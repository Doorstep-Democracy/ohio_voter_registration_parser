"""
tools/generate_narratives.py
────────────────────────────
Reads existing chart JSON from docs/data/ and writes a {slug}_narrative.json
for every jurisdiction at the specified level(s).

Prose is produced by the per-level template registry in
tools/narrative/templates.py -- deterministic, hallucination-proof, and fast.
After template generation, optional LLM enrichment (--llm / --llm-batch)
writes a plain-English captain briefing via tools/narrative/llm_enricher.py.

Cache-skip strategy
───────────────────
Each output JSON stores a `metrics_hash` field -- a 16-char SHA-256 prefix of
the input metrics + template version.  On subsequent runs, if the hash matches
the existing file's hash, the file is skipped.  Bumping TEMPLATE_VERSION or
PER_LEVEL_CONFIG_VERSION in templates.py auto-invalidates all cached narratives
on the next run (no --overwrite needed).

Usage (CLI)
───────────
  # All 88 counties (default level)
  python tools/generate_narratives.py --templated

  # Targeted county run
  python tools/generate_narratives.py Hamilton Franklin --templated

  # All Hamilton precincts
  python tools/generate_narratives.py --level precinct Hamilton --templated

  # All cities
  python tools/generate_narratives.py --level city --templated

  # Every level in sequence
  python tools/generate_narratives.py --all-levels --templated

  # Inspect output without writing
  python tools/generate_narratives.py Hamilton --templated --dry-run

  # Force regeneration despite unchanged metrics
  python tools/generate_narratives.py --templated --overwrite

  # Template + LLM sync enrichment (one API call per jurisdiction)
  python tools/generate_narratives.py Hamilton --templated --llm

  # Template + LLM Batch API enrichment (50% cheaper, waits for completion)
  python tools/generate_narratives.py --all-levels --templated --llm-batch

  # Force re-enrich even if captain_metrics_hash is current
  python tools/generate_narratives.py Hamilton --templated --llm --llm-force

Usage (programmatic -- called by ohio_voter_pipeline.py)
────────────────────────────────────────────────────────
  from tools.generate_narratives import run_for_levels
  ok, skipped, failed = run_for_levels(['county', 'precinct'])
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from pathlib import Path

# ---- Registry import ---------------------------------------------------------
from .templates import (
    LEVELS,
    TEMPLATE_VERSION,
    PER_LEVEL_CONFIG_VERSION,
    build_metrics_for_level,
    build_narrative,
    metrics_hash as _metrics_hash,
)
from .llm_enricher import (  # gracefully no-ops if ANTHROPIC_API_KEY absent
    captain_hash as _captain_hash,
    enrich_one as _enrich_one,
    enrich_batch as _enrich_batch,
    write_captain_narrative as _write_captain_narrative,
    is_captain_fresh as _is_captain_fresh,
)
from .officeholders import (
    # Deterministic; reads local/source/County Data Files/*/*_roster_officers.csv
    # (written by scrape_ohio_roster.PY, sourced from ohioroster.ohiosos.gov).
    # No LLM/API calls. To refresh officeholder data, re-run that scraper --
    # not anything in this file. See officeholders.py module docstring.
    build_officeholders_by_slug as _build_officeholders_by_slug,
)

# ---- Paths -------------------------------------------------------------------
ROOT     = Path(__file__).resolve().parent.parent.parent  # project root
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
# The output root comes from the single resolver, so narratives follow
# ELECTION_DATA_OUT together with the chart JSON they are built from.
from pipeline.paths import DATA_DIR, DOCS_DIR  # noqa: E402
MANIFEST = DOCS_DIR / 'manifest.json'

# ---- Constants ---------------------------------------------------------------
# Label written to generated_by field when the template registry produced the
# prose.  Matches the locked decision from the planning session (2026-05-26).
GENERATED_BY_TEMPLATED = 'templated-v1'

# Jurisdiction levels that are neither county nor precinct.  Used by the
# pipeline to request narrative generation after jurisdictional_groupings.py
# has written its chart JSON.
NON_COUNTY_LEVELS = [l for l in LEVELS if l not in ('county', 'precinct')]

log = logging.getLogger(__name__)


# ---- Utilities ---------------------------------------------------------------

def county_slug(name: str) -> str:
    """
    Produce the filesystem slug for a county name.
    'Van Wert' -> 'van_wert'.  Matches the pipeline/voter_data_cleaner.py convention.
    """
    return name.lower().replace(' ', '_').replace("'", '')


def _load_json(path: Path) -> dict | None:
    """Return parsed JSON dict, or None if the file is missing or malformed."""
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except Exception as exc:
        log.debug('Could not parse %s: %s', path.name, exc)
        return None


# ---- County-list extraction --------------------------------------------------

def load_county_list() -> list[str]:
    """
    Return the 88 Ohio county proper-cased names from manifest.json.

    manifest.json is occasionally written truncated mid-JSON by the pipeline
    (the file contains ~65k chart entries and can exceed write-buffer limits).
    When json.loads() fails, fall back to regex extraction of the allCounties
    array, which always appears near the top of the file and is reliably intact.
    """
    raw = MANIFEST.read_text(encoding='utf-8')
    try:
        d = json.loads(raw)
        counties = (
            d.get('processedCounties')
            or d.get('allCounties')
            or d.get('counties')
            or []
        )
        if counties:
            return counties
    except json.JSONDecodeError:
        pass  # fall through to regex

    m = re.search(r'"allCounties"\s*:\s*(\[.*?\])', raw, re.DOTALL)
    if m:
        log.debug('manifest.json truncated; used regex fallback for county list')
        return json.loads(m.group(1))

    raise RuntimeError(
        'Cannot extract county list from manifest.json -- '
        'file may be completely malformed.'
    )


# ---- Per-level enumeration ---------------------------------------------------

def enumerate_county(filter_names: list[str] | None = None) -> list[dict]:
    """
    Return one entry per county: {'slug': str, 'county': str}.
    filter_names restricts to those county names (proper-cased, as in manifest).
    """
    all_counties = load_county_list()
    if filter_names:
        all_counties = [c for c in all_counties if c in filter_names]
    return [{'slug': county_slug(c), 'county': c} for c in all_counties]


def enumerate_precinct(filter_counties: list[str] | None = None) -> list[dict]:
    """
    Walk each county's {county_slug}_precinct_index.json and yield one entry
    per precinct.

    Precinct index format (written by pipeline/voter_data_cleaner.py):
      { 'county': 'Hamilton',
        'precincts': [
          {'name': 'ADDYSTON A', 'safe_name': 'addyston_a', ...}, ...
        ] }

    Returns entries:
      { 'slug':          '{county_slug}_precinct_{safe_name}',
        'county_slug':   str,
        'county':        str (proper-cased),
        'precinct':      str (display name from index),
        'precinct_safe': str (safe_name from index) }
    """
    all_counties = load_county_list()
    if filter_counties:
        all_counties = [c for c in all_counties if c in filter_counties]

    entries = []
    for county_name in all_counties:
        cs  = county_slug(county_name)
        idx = _load_json(DATA_DIR / f'{cs}_precinct_index.json')
        if not idx:
            continue
        for p in idx.get('precincts', []):
            safe = p.get('safe_name', '')
            if not safe:
                continue
            entries.append({
                'slug':          f'{cs}_precinct_{safe}',
                'county_slug':   cs,
                'county':        county_name,
                'precinct':      p.get('name', safe),
                'precinct_safe': safe,
            })
    return entries


def enumerate_level(level: str) -> list[dict]:
    """
    For district/city/township/school/court levels, glob
    docs/data/{level}/*_party_affiliation.json to enumerate all jurisdictions.

    The slug is the filename stem minus '_party_affiliation' (e.g. '01' for
    state senate district 1, 'akron_city' for the city of Akron).

    Returns entries: {'slug': str, 'level': str, 'out_dir': Path}
    """
    level_dir = DATA_DIR / level
    if not level_dir.exists():
        log.debug('No data directory for level %r -- skipping', level)
        return []
    entries = []
    for pa_file in sorted(level_dir.glob('*_party_affiliation.json')):
        slug = pa_file.name.replace('_party_affiliation.json', '')
        entries.append({'slug': slug, 'level': level, 'out_dir': level_dir})
    return entries


# ---- Data loaders ------------------------------------------------------------

def _load_county_jsons(slug: str) -> tuple:
    """
    Load (party, generation, party_decade) chart JSONs for a county slug.
    County level renders both generation distribution and decade trend.
    """
    return (
        _load_json(DATA_DIR / f'{slug}_party_affiliation.json'),
        _load_json(DATA_DIR / f'{slug}_generation_distribution.json'),
        _load_json(DATA_DIR / f'{slug}_party_by_decade.json'),
    )


def _load_precinct_jsons(entry: dict) -> tuple:
    """
    Load (party, None, party_decade) for a precinct entry from enumerate_precinct().
    Generation distribution is not rendered for precincts (small n, noisy signal).
    """
    cs, ps = entry['county_slug'], entry['precinct_safe']
    return (
        _load_json(DATA_DIR / f'{cs}_precinct_{ps}_party.json'),
        None,
        _load_json(DATA_DIR / f'{cs}_precinct_{ps}_party_by_decade.json'),
    )


def _load_level_jsons(level: str, slug: str) -> tuple:
    """
    Load (party, None, party_decade) for any district/city/township/etc. slug.
    Generation distribution files are not produced for these levels by the
    current pipeline; templates.py omits the generation sentence gracefully.
    """
    level_dir = DATA_DIR / level
    return (
        _load_json(level_dir / f'{slug}_party_affiliation.json'),
        None,
        _load_json(level_dir / f'{slug}_party_by_decade.json'),
    )


# ---- Output path resolution --------------------------------------------------

def out_path_for(level: str, slug: str) -> Path:
    """
    Resolve the output narrative JSON path for any level/slug combination.

      county, precinct -> docs/data/{slug}_narrative.json
      (all other)      -> docs/data/{level}/{slug}_narrative.json
    """
    if level in ('county', 'precinct'):
        return DATA_DIR / f'{slug}_narrative.json'
    return DATA_DIR / level / f'{slug}_narrative.json'


# ---- Cache validation --------------------------------------------------------

def _cache_hit(out_path: Path, new_hash: str) -> bool:
    """
    Return True if the existing output JSON already has new_hash as its
    metrics_hash field -- meaning neither the input data nor the template
    version changed since the last run.  Bumping TEMPLATE_VERSION or
    PER_LEVEL_CONFIG_VERSION in templates.py produces a new hash that will
    not match any existing file, so the cache auto-invalidates statewide.
    """
    existing = _load_json(out_path)
    if not existing:
        return False
    return existing.get('metrics_hash') == new_hash


# ---- Core per-jurisdiction builder -------------------------------------------

def _process_one(
    *,
    level:         str,
    slug:          str,
    party_json:    dict | None,
    gen_json:      dict | None,
    pd_json:       dict | None,
    parent_county: str | None,
    out_path:      Path,
    overwrite:     bool,
    dry_run:       bool,
    llm:           bool = False,
    llm_force:     bool = False,
    officeholders: dict | None = None,
) -> str:
    """
    Build and write (or print) the narrative JSON for one jurisdiction.
    Returns 'ok', 'skipped', or 'failed'.

    Steps:
      1. Build a level-aware metrics dict via build_metrics_for_level().
      2. Compute metrics_hash (includes officeholders).  Skip if unchanged.
      3. Generate prose via build_narrative() (template registry; deterministic).
      4. Write output JSON (or print to stdout in --dry-run mode).
      5. If llm=True, call enrich_one() and append narrative_captain to the file.

    officeholders is sourced only at the county level today, by
    officeholders.py reading local/source/County Data Files/*/*_roster_officers.csv
    (written by scrape_ohio_roster.PY, from ohioroster.ohiosos.gov). This
    function never fetches or computes officeholder data itself -- the
    caller (_run_county) passes it in already-built. If officeholder text
    in the output looks stale, the fix is re-running scrape_ohio_roster.PY,
    not editing anything in this file. Other levels pass None, which
    renders the existing "data not yet available" placeholder for every
    office slot -- there is no roster-site source for those levels.
    """
    # 1. Assemble level-aware metrics from raw chart JSON.
    metrics = build_metrics_for_level(
        level=level,
        party_json=party_json,
        decade_json=None,           # decade_trend derived from party_decade_json
        generation_json=gen_json,
        party_decade_json=pd_json,
        parent_county=parent_county,
        geography_counties=None,    # district->counties map is Workstream 2
        source=f'{level}/{slug}',
    )
    if metrics is None:
        log.debug('No usable metrics for %s/%s -- skipping', level, slug)
        return 'failed'

    # 2. Cache check: skip if metrics + officeholders + template versions are
    #    all unchanged. officeholders must be included here -- otherwise a
    #    newly-available officeholder (or one who leaves office) would never
    #    invalidate a previously-cached narrative.
    mhash = _metrics_hash(metrics, officeholders)
    if not overwrite and not dry_run and _cache_hit(out_path, mhash):
        log.info('Skip %s (metrics_hash unchanged)', out_path.name)
        return 'skipped'

    # 3. Generate prose. officeholders=None -> all slots render placeholder text.
    narrative = build_narrative(metrics, officeholders=officeholders)

    # 4a. Dry-run: print to stdout without writing.
    if dry_run:
        print(f"\n{'='*60}")
        print(f"[{level}] {slug}")
        print(narrative)
        return 'ok'

    # 4b. Write output JSON with full provenance fields.
    out = {
        'geography':         level,
        'level':             level,
        'jurisdiction_name': metrics['name'],
        'updated':           metrics['data_as_of'],
        'generated_by':      GENERATED_BY_TEMPLATED,
        'template_version':  TEMPLATE_VERSION,
        'config_version':    PER_LEVEL_CONFIG_VERSION,
        'metrics_hash':      mhash,
        'narrative':         narrative,
    }
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps(out, indent=2, ensure_ascii=False),
        encoding='utf-8',
    )
    log.info('Wrote %s', out_path.name)

    # 5. Optional LLM sync enrichment.
    if llm and not dry_run:
        if not llm_force and _is_captain_fresh(out, metrics):
            log.info('Skip LLM (captain_hash unchanged) %s', out_path.name)
        else:
            captain_text = _enrich_one(metrics)
            if captain_text:
                _write_captain_narrative(out_path, out, captain_text, metrics)
                log.info('LLM enriched %s', out_path.name)
            else:
                log.warning('LLM enrichment returned None for %s', out_path.name)

    return 'ok'


# ---- Level-specific runners --------------------------------------------------

def _run_county(
    filter_names: list[str] | None,
    overwrite: bool,
    dry_run: bool,
    llm: bool = False,
    llm_force: bool = False,
) -> tuple[int, int, int]:
    """Generate narrative JSONs for all (or filtered) counties."""
    # Loaded once for the whole run (~30ms for all 88 counties), not per
    # jurisdiction -- mirrors load_county_list()'s single manifest.json read.
    # Source of this data: local/source/County Data Files/*/*_roster_officers.csv,
    # written by scrape_ohio_roster.PY from ohioroster.ohiosos.gov. Purely a
    # local CSV read here -- deterministic, no LLM/API calls. To refresh
    # (e.g. after a new election cycle), re-run that scraper; this call will
    # pick up whatever is newest in the CSVs the next time this runs.
    officeholders_by_slug = _build_officeholders_by_slug()

    ok = skipped = failed = 0
    for entry in enumerate_county(filter_names):
        slug = entry['slug']
        party, gen, pd = _load_county_jsons(slug)
        if party is None:
            log.warning('No party data for county %s -- skipping', slug)
            failed += 1
            continue
        result = _process_one(
            level='county', slug=slug,
            party_json=party, gen_json=gen, pd_json=pd,
            parent_county=None,
            out_path=out_path_for('county', slug),
            overwrite=overwrite, dry_run=dry_run,
            llm=llm, llm_force=llm_force,
            officeholders=officeholders_by_slug.get(slug),
        )
        ok      += result == 'ok'
        skipped += result == 'skipped'
        failed  += result == 'failed'
    return ok, skipped, failed


def _run_precinct(
    filter_counties: list[str] | None,
    overwrite: bool,
    dry_run: bool,
    llm: bool = False,
    llm_force: bool = False,
) -> tuple[int, int, int]:
    """
    Generate narrative JSONs for all precincts across all (or filtered) counties.
    Missing precinct party files are logged at DEBUG -- they are expected during
    partial pipeline runs -- to avoid noise in the run log.
    """
    ok = skipped = failed = 0
    for entry in enumerate_precinct(filter_counties):
        party, gen, pd = _load_precinct_jsons(entry)
        if party is None:
            log.debug('No party data for precinct %s -- skipping', entry['slug'])
            failed += 1
            continue
        result = _process_one(
            level='precinct', slug=entry['slug'],
            party_json=party, gen_json=gen, pd_json=pd,
            parent_county=entry['county'],
            out_path=out_path_for('precinct', entry['slug']),
            overwrite=overwrite, dry_run=dry_run,
            llm=llm, llm_force=llm_force,
        )
        ok      += result == 'ok'
        skipped += result == 'skipped'
        failed  += result == 'failed'
    return ok, skipped, failed


def _run_level(
    level: str,
    overwrite: bool,
    dry_run: bool,
    llm: bool = False,
    llm_force: bool = False,
) -> tuple[int, int, int]:
    """Generate narrative JSONs for all jurisdictions at a district/city/etc. level."""
    ok = skipped = failed = 0
    for entry in enumerate_level(level):
        slug = entry['slug']
        party, gen, pd = _load_level_jsons(level, slug)
        if party is None:
            log.debug('No party data for %s/%s -- skipping', level, slug)
            failed += 1
            continue
        result = _process_one(
            level=level, slug=slug,
            party_json=party, gen_json=gen, pd_json=pd,
            parent_county=None,
            out_path=out_path_for(level, slug),
            overwrite=overwrite, dry_run=dry_run,
            llm=llm, llm_force=llm_force,
        )
        ok      += result == 'ok'
        skipped += result == 'skipped'
        failed  += result == 'failed'
    return ok, skipped, failed


def _dispatch_level(
    level: str,
    filter_names: list[str] | None,
    overwrite: bool,
    dry_run: bool,
    llm: bool = False,
    llm_force: bool = False,
) -> tuple[int, int, int]:
    """Route a single level string to its runner function."""
    if level == 'county':
        return _run_county(filter_names, overwrite, dry_run, llm=llm, llm_force=llm_force)
    if level == 'precinct':
        return _run_precinct(filter_names, overwrite, dry_run, llm=llm, llm_force=llm_force)
    return _run_level(level, overwrite, dry_run, llm=llm, llm_force=llm_force)


# ---- Batch LLM enrichment (post-template pass) --------------------------------

def _run_llm_batch(
    levels: list[str],
    filter_names: list[str] | None,
    llm_force: bool,
) -> None:
    """
    Collect metrics for all jurisdictions across the given levels, submit one
    Anthropic Batch API call, poll until complete, and write results.

    This path collects all work first, submits once, then writes results in a
    second pass -- giving 50% cost reduction vs the per-jurisdiction sync path.
    """
    pending: list[dict] = []

    def _collect(level, slug, party_json, gen_json, pd_json, parent_county, out_path):
        metrics = build_metrics_for_level(
            level=level, party_json=party_json,
            decade_json=None, generation_json=gen_json,
            party_decade_json=pd_json, parent_county=parent_county,
        )
        if metrics is None:
            return
        existing = _load_json(out_path) or {}
        if not llm_force and _is_captain_fresh(existing, metrics):
            log.info('Skip LLM batch (captain_hash unchanged) %s', out_path.name)
            return
        metrics['slug'] = slug
        metrics['_out_path'] = out_path
        pending.append(metrics)

    for level in levels:
        if level == 'county':
            for entry in enumerate_county(filter_names):
                slug = entry['slug']
                party, gen, pd = _load_county_jsons(slug)
                if party:
                    _collect(level, slug, party, gen, pd, None, out_path_for(level, slug))
        elif level == 'precinct':
            for entry in enumerate_precinct(filter_names):
                party, gen, pd = _load_precinct_jsons(entry)
                if party:
                    _collect(level, entry['slug'], party, gen, pd,
                             entry['county'], out_path_for(level, entry['slug']))
        else:
            for entry in enumerate_level(level):
                slug = entry['slug']
                party, gen, pd = _load_level_jsons(level, slug)
                if party:
                    _collect(level, slug, party, gen, pd, None, out_path_for(level, slug))

    if not pending:
        log.info('[llm-batch] Nothing to enrich.')
        return

    log.info('[llm-batch] Submitting %d jurisdictions to Batch API...', len(pending))

    # Strip internal path key before sending to API (not JSON-serialisable).
    path_map = {m['slug']: m.pop('_out_path') for m in pending}
    results = _enrich_batch(pending)

    written = 0
    for m in pending:
        slug = m['slug']
        captain_text = results.get(slug)
        if not captain_text:
            continue
        out_path = path_map[slug]
        existing = _load_json(out_path) or {}
        _write_captain_narrative(out_path, existing, captain_text, m)
        written += 1

    log.info('[llm-batch] Wrote %d captain narratives.', written)


# ---- Public API (for pipeline import) ----------------------------------------

def run_for_levels(
    levels: list[str],
    filter_names: list[str] | None = None,
    overwrite: bool = False,
    llm: bool = False,
    llm_batch: bool = False,
    llm_force: bool = False,
) -> tuple[int, int, int]:
    """
    Programmatic entry point called by ohio_voter_pipeline.py._narrative_phase().

    Runs the templated narrative generator for each level, then optionally
    enriches with LLM captain briefings (sync or batch).

    Args:
        levels:       List of level strings from LEVELS, e.g. ['county', 'precinct'].
        filter_names: County names to process; None means all 88 counties.
        overwrite:    If True, regenerate even when metrics_hash is unchanged.
        llm:          Enrich each jurisdiction synchronously after templating.
        llm_batch:    Collect all jurisdictions and submit one Batch API call
                      after templating completes (50% cheaper; waits for completion).
        llm_force:    Re-enrich even when captain_metrics_hash is current.

    Returns:
        (total_ok, total_skipped, total_failed) summed across all levels.

    Example:
        from tools.generate_narratives import run_for_levels
        ok, sk, fa = run_for_levels(['county', 'precinct'], filter_names=['Hamilton'],
                                    llm=True)
    """
    total_ok = total_skipped = total_failed = 0
    for level in levels:
        ok, sk, fa = _dispatch_level(
            level, filter_names, overwrite, dry_run=False,
            llm=llm and not llm_batch,
            llm_force=llm_force,
        )
        log.info(
            '[narrative] %-35s  ok:%-5d  skipped:%-5d  failed:%d',
            level, ok, sk, fa,
        )
        total_ok      += ok
        total_skipped += sk
        total_failed  += fa

    if llm_batch:
        _run_llm_batch(levels, filter_names, llm_force)

    return total_ok, total_skipped, total_failed


# ---- CLI ---------------------------------------------------------------------

def main() -> None:
    # --dry-run prints narrative prose straight to stdout, which can contain
    # non-ASCII characters (e.g. U+2212 minus sign in percentage deltas).
    # The Windows console's cp1252 default can't encode those and raises
    # UnicodeEncodeError, so force UTF-8 stdout regardless of host console.
    sys.stdout.reconfigure(encoding='utf-8')
    logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s')

    parser = argparse.ArgumentParser(
        description=(
            'Generate templated prose narratives for Ohio voter dashboard '
            'jurisdictions at any level (county, precinct, city, district, etc.).'
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        'names', nargs='*',
        metavar='NAME',
        help=(
            'County names (for --level county or precinct) or jurisdiction '
            'slugs to process.  Default: all jurisdictions for the level.'
        ),
    )
    parser.add_argument(
        '--level', default='county', choices=list(LEVELS),
        help='Jurisdiction level to generate narratives for.  Default: county.',
    )
    parser.add_argument(
        '--all-levels', action='store_true',
        help='Run all %(count)d levels in sequence (overrides --level).' % {'count': len(LEVELS)},
    )
    parser.add_argument(
        '--templated', action='store_true',
        help=(
            'Use the deterministic template registry to generate prose.  '
            'Required for offline / no-API-key runs.'
        ),
    )
    parser.add_argument(
        '--llm', action='store_true',
        help=(
            'After templating, enrich each jurisdiction with an LLM captain '
            'briefing (synchronous, one API call per jurisdiction).  '
            'Requires ANTHROPIC_API_KEY.'
        ),
    )
    parser.add_argument(
        '--llm-batch', action='store_true',
        help=(
            'After templating, submit all jurisdictions to the Anthropic '
            'Batch API in one call (50%% cheaper than --llm; waits for '
            'completion before exiting).  Requires ANTHROPIC_API_KEY.'
        ),
    )
    parser.add_argument(
        '--llm-force', action='store_true',
        help=(
            'Re-enrich with LLM even when captain_metrics_hash is current.  '
            'Normally not needed -- bump ENRICHER_VERSION in llm_enricher.py '
            'to auto-invalidate statewide.'
        ),
    )
    parser.add_argument(
        '--dry-run', action='store_true',
        help='Print generated narratives to stdout; do not write any files.',
    )
    parser.add_argument(
        '--overwrite', action='store_true',
        help=(
            'Ignore the metrics_hash cache and regenerate all files.  '
            'Normally not needed -- bumping TEMPLATE_VERSION in templates.py '
            'auto-invalidates the cache statewide.'
        ),
    )
    args = parser.parse_args()

    # Guard: --templated is required for any file-writing run.
    if not args.templated and not args.dry_run:
        parser.error(
            '--templated is required.  '
            'Run with --templated [--llm | --llm-batch] to generate narratives.'
        )

    levels = list(LEVELS) if args.all_levels else [args.level]
    filter_names = args.names or None

    llm_sync  = getattr(args, 'llm', False) and not getattr(args, 'llm_batch', False)
    llm_batch = getattr(args, 'llm_batch', False)
    llm_force = getattr(args, 'llm_force', False)

    for level in levels:
        log.info('=== level: %s ===', level)
        ok, skipped, failed = _dispatch_level(
            level, filter_names, args.overwrite, args.dry_run,
            llm=llm_sync, llm_force=llm_force,
        )
        if not args.dry_run:
            log.info(
                '[%s] done -- ok:%d  skipped:%d  failed:%d',
                level, ok, skipped, failed,
            )

    if llm_batch and not args.dry_run:
        _run_llm_batch(levels, filter_names, llm_force)


if __name__ == '__main__':
    main()
