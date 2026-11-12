"""
jurisdictional_groupings.py
════════════════════════════════

Generate voter cohort summaries for all Ohio jurisdictions present in SWVF:
  - Cities
  - Townships
  - Villages
  - School Districts (Local, City, Exempted Village)
  - State Senate Districts
  - State Representative Districts
  - Congressional Districts
  - County Court Districts
  - Municipal Court Districts
  - Court of Appeals Districts

Aggregates pre-classified, enriched voter data by jurisdiction and exports:
  - JSON (6 chart types per jurisdiction, schema identical to precinct/county)
  - Excel (multi-sheet workbook, one sheet per jurisdiction type)

Reuses COHORT_SLICES, export logic, and chart formatting from pipeline/voter_data_cleaner.py.
Single-threaded main process reads cached enriched parquet once; ThreadPoolExecutor
distributes aggregation across 8 workers per jurisdiction type.

Output:
  docs/data/{jurisdiction_type}/{jurisdiction_slug}_*.json  (6 files per jurisdiction)
  output/jurisdictional_groupings_{format}_{timestamp}.xlsx | .json
"""

import concurrent.futures
import json
import logging
import re
import sys
import time
from datetime import date as date_t, datetime
from pathlib import Path

import polars as pl
import pandas as pd
import xlsxwriter

try:
    import orjson as _orjson
except ImportError:
    _orjson = None


# ─────────────────────────────────────────────────────────────────────────────
# Paths and constants (reuse from pipeline.voter_data_cleaner)
# ─────────────────────────────────────────────────────────────────────────────

# Ensure pipeline/ is on sys.path so sibling modules can be imported as bare names
# whether this script is run directly or imported as part of the pipeline package.
_pipeline_dir = str(Path(__file__).resolve().parent)
if _pipeline_dir not in sys.path:
    sys.path.insert(0, _pipeline_dir)

# Every managed location comes from the single resolver, pipeline/paths.py
# (CLAUDE.md section 5). The names stay module globals here so callers and
# tests that read or patch e.g. `jurisdictional_groupings.ENRICHED_CACHE` keep working.
from paths import (  # noqa: E402,F401
    BASE_DIR,
    DATA_DIR,
    DOCS_DIR,
    ENRICHED_CACHE,
    LOGS_DIR,
    OUTPUT_DIR,
    PARQUET_DIR,
    PARQUET_ENRICHED_DIR,
    parquet_glob,
)
PARQUET_ENRICHED_DIR.mkdir(parents=True, exist_ok=True)
CLASSIFIER_SRC       = BASE_DIR / "pipeline" / "voter_data_cleaner.py"

LOGS_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)

# ─────────────────────────────────────────────────────────────────────────────
# Logging setup
# ─────────────────────────────────────────────────────────────────────────────

def setup_logger(name='groupings', log_dir=LOGS_DIR):
    """Create timestamped logger for this run."""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = log_dir / f'{name}_{timestamp}.log'

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # File handler (debug level)
    fh = logging.FileHandler(log_file, encoding='utf-8')
    fh.setLevel(logging.DEBUG)

    # Console handler (info level)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)

    # Formatter
    fmt = logging.Formatter(
        '%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    fh.setFormatter(fmt)
    ch.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(ch)

    # Verbose defaults: run context + native-crash tracebacks in this log.
    # Rationale: voter_data_cleaner.setup_logging() docstring, "Logging policy".
    import voter_data_cleaner as _v2
    _v2.enable_crash_log(log_file, logger)
    _v2.log_run_context(logger)

    return logger, log_file


def _split_by_freshness(jurisdictions, sentinel_for, cutoff: float):
    """Partition jurisdictions for resume into (current, stale, missing).

    current -- sentinel exists and is newer than `cutoff`: reuse, skip.
    stale   -- sentinel exists but is not newer than `cutoff`: regenerate.
    missing -- no sentinel: generate.

    Why a cutoff and not existence: until 2026-11-11 resume skipped any
    jurisdiction whose sentinel existed. Files written under the old 7-cohort
    taxonomy (May to July 2026) were then skipped as "already exported" by
    every later regeneration, so the public site mixed data vintages (repo
    evaluation P4). The caller sets `cutoff` to max(enriched cache mtime, this
    module's mtime): a file older than the data or code that should produce it
    is stale. Crash-resume still works, since files written earlier in an
    interrupted run are newer than both.
    """
    current, stale, missing = set(), [], []
    for j in jurisdictions:
        s = sentinel_for(j)
        if not s.exists():
            missing.append(j)
        elif s.stat().st_mtime > cutoff:
            current.add(j)
        else:
            stale.append(j)
    return current, stale, missing


# ─────────────────────────────────────────────────────────────────────────────
# Cohort definitions (copied from pipeline/voter_data_cleaner.py)
# ─────────────────────────────────────────────────────────────────────────────

COHORT_SLICES = [
    ('PURE_R',          'Pure R',                   '#ef4444'),
    ('UNC_LAPSED_R',    'UNC – Lapsed R',           '#fca5a5'),
    ('MIXED_ACTIVE',    'Mixed – Active',           '#f59e0b'),
    ('MIXED_LAPSED',    'Mixed – Lapsed',           '#a78bfa'),
    ('MINOR_PARTY',     'Minor Party',              '#14b8a6'),
    ('UNC_NONPARTISAN', 'UNC – Non-partisan Ballot', '#c4b5cd'),
    ('UNC_NO_PRIMARY',  'UNC – No Primary',         '#9ca3af'),
    ('UNC_LAPSED_D',    'UNC – Lapsed D',           '#93c5fd'),
    ('PURE_D',          'Pure D',                   '#3b82f6'),
]

COHORT_STACK_MAP = {
    'PURE_R':          'r_pure',
    'UNC_LAPSED_R':    'unc_r',
    'MIXED_ACTIVE':    'unc_mid',
    'MIXED_LAPSED':    'unc_mid',
    'MINOR_PARTY':     'unc_mid',
    'UNC_NONPARTISAN': 'unc_mid',
    'UNC_NO_PRIMARY':  'unc_mid',
    'UNC_LAPSED_D':    'unc_d',
    'PURE_D':          'd_pure',
}

UNC_SHADOW_COLORS = {
    'UNC_LAPSED_R': '#fca5a5',
    'MIXED_ACTIVE': '#f59e0b',
    'MIXED_LAPSED': '#a78bfa',
    'UNC_NONPARTISAN': '#c4b5cd',
    'UNC_NO_PRIMARY': '#9ca3af',
    'UNC_LAPSED_D': '#93c5fd',
}

CHART_COLORS = {
    'bar':      '#3b82f6',
    'generation': '#8b5cf6',
}

# Canonical generation order — must match voter_data_cleaner._GEN_ORDER
# and the labels written into the `Generation` column.
_GEN_ORDER = ['Silent/Greatest', 'Baby Boomers', 'Gen X', 'Millennials', 'Gen Z', 'Gen Alpha']

GENERATION_RANGES = {
    'Silent': (1928, 1945),
    'Baby Boomers': (1946, 1964),
    'Generation X': (1965, 1980),
    'Millennials': (1981, 1996),
    'Generation Z': (1997, 2012),
    'Gen Alpha': (2013, 2025),
}


# ─────────────────────────────────────────────────────────────────────────────
# Jurisdiction definitions
# ─────────────────────────────────────────────────────────────────────────────

JURISDICTIONS = {
    'cities': {
        'column': 'CITY',
        'display': 'City',
        # A2: group on a resolved-city column (see _add_resolved_city_column)
        # so zero-CITY counties (Cuyahoga: municipality via WARD/residential)
        # get bundles; populated-CITY counties stay byte-identical.
        'row_resolver': 'city',
    },
    'townships': {
        'column': 'TOWNSHIP',
        'display': 'Township',
        'county_scoped': True,  # 152/810 names collide across counties (Washington Twp x23)
    },
    'villages': {
        'column': 'VILLAGE',
        'display': 'Village',
        'county_scoped': True,  # 37/645 names collide across counties
    },
    'local_school_districts': {
        'column': 'LOCAL_SCHOOL_DISTRICT',
        'display': 'Local School District',
    },
    'city_school_districts': {
        'column': 'CITY_SCHOOL_DISTRICT',
        'display': 'City School District',
    },
    'exempted_vill_school_districts': {
        'column': 'EXEMPTED_VILL_SCHOOL_DISTRICT',
        'display': 'Exempted Village School District',
    },
    'state_senate_districts': {
        'column': 'STATE_SENATE_DISTRICT',
        'display': 'State Senate District',
    },
    'state_rep_districts': {
        'column': 'STATE_REPRESENTATIVE_DISTRICT',
        'display': 'State Representative District',
    },
    'congressional_districts': {
        'column': 'CONGRESSIONAL_DISTRICT',
        'display': 'Congressional District',
    },
    'county_court_districts': {
        'column': 'COUNTY_COURT_DISTRICT',
        'display': 'County Court District',
    },
    'municipal_court_districts': {
        'column': 'MUNICIPAL_COURT_DISTRICT',
        'display': 'Municipal Court District',
        'county_scoped': True,  # BELLEVUE (22/39/72), VERMILLION (22/47) — distinct courts
    },
    'court_of_appeals': {
        'column': 'COURT_OF_APPEALS',
        'display': 'Court of Appeals District',
    },
    # 'wards' is intentionally NOT a generic config entry. WARD names are neither
    # globally unique -- 16 values collide across counties (five different
    # 'FIRST WARD's, etc., 2026-07-09 measurement) -- nor reliably city-prefixed
    # ('FIRST WARD', 'WEST WARD'), so the (county, name) composite path cannot key
    # them safely. Canonical ward entities are municipality-scoped (parent place +
    # ward value), built by build_ward_entities() via the single ward resolver
    # voter_data_cleaner._ward_map_per_county. See PLAN_SUBCOUNTY_JURISDICTIONS.md
    # Part W1 and CLAUDE.md 4 (ward-uniqueness correction).
}


# ─────────────────────────────────────────────────────────────────────────────
# Aggregation and export functions
# ─────────────────────────────────────────────────────────────────────────────

def _slugify(name: str) -> str:
    """Convert jurisdiction name to URL-safe slug.

    Collapses any run of non-alphanumeric characters (spaces, punctuation,
    stray whitespace) to a single underscore and strips leading/trailing
    underscores — matching _precinct_safe_name in voter_data_cleaner.py and
    the frontend's countyToSlug/cityNameToSlug (v2.js). A naive one-for-one
    replace() left a source CITY value with a trailing space (e.g.
    "COLUMBUS CITY ") slugged to "columbus_city_", producing a double
    underscore once export_jurisdiction_json appended "_<chart_type>.json".
    """
    if name is None or (isinstance(name, float)):
        return 'unknown'
    return re.sub(r'[^a-z0-9]+', '_', str(name).lower()).strip('_')


def _cn_key(county_number) -> str:
    """Normalize COUNTY_NUMBER to zero-padded 2-char string for OHIO_COUNTIES lookup.

    Hive-partitioned parquet exposes COUNTY_NUMBER as Int64 (e.g. 1), but
    OHIO_COUNTIES uses zero-padded string keys ('01').  Always route through
    this helper rather than calling str() directly.
    """
    return str(county_number).zfill(2)


# Chart-type file family written per jurisdiction by export_jurisdiction_json(),
# plus the narrative file written separately by tools/narrative/generate_narratives.py.
# Single source of truth so the prune step below can never drift from what a
# regen run actually owns.
_CHART_TYPE_SUFFIXES = [
    'party_affiliation', 'decade_distribution', 'party_by_decade',
    'generation_distribution', 'party_by_generation', 'unc_shadow', 'narrative',
]


def _prune_ghost_family_files(type_dir: Path, valid_slugs: set, logger: logging.Logger) -> int:
    """Delete stale-slug files whose slug matches no jurisdiction in the current data.

    Guards against the class of bug behind A6 (2026-07-10): a _slugify() fix, or
    a jurisdiction's underlying data changing (e.g. a resolved county), leaves old
    slugged bundles orphaned forever, since export_jurisdiction_json only ever
    writes new files. Scoped to the chart-type + narrative file family so it never
    touches index.json or unrelated files. Resume-safe: valid_slugs is computed
    from the CURRENT full jurisdiction list (not `results`), so a resumed/skipped
    jurisdiction's still-current files are never removed.
    """
    if not type_dir.exists():
        return 0
    suffixes = sorted((f'_{ct}.json' for ct in _CHART_TYPE_SUFFIXES), key=len, reverse=True)
    removed = 0
    for fpath in type_dir.iterdir():
        if fpath.name == 'index.json':
            continue
        matched_suffix = next((s for s in suffixes if fpath.name.endswith(s)), None)
        if matched_suffix is None:
            continue
        slug = fpath.name[:-len(matched_suffix)]
        if slug not in valid_slugs:
            fpath.unlink()
            removed += 1
            logger.info(f'Pruned ghost file (stale slug not in current jurisdiction set): {fpath.name}')
    return removed


def _dump_json(data: dict, filepath: Path, logger: logging.Logger):
    """Write JSON with orjson if available, else stdlib json."""
    filepath.parent.mkdir(parents=True, exist_ok=True)
    if _orjson:
        with open(filepath, 'wb') as f:
            f.write(_orjson.dumps(data, option=_orjson.OPT_INDENT_2))
    else:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
    logger.debug(f'Wrote {filepath.name}')


def _add_resolved_city_column(df: pl.DataFrame, logger: logging.Logger) -> pl.DataFrame:
    """Add `_resolved_city`, the grouping key for the CITY stats bundles (A2).

    Value = the raw CITY string verbatim when populated; else, ONLY for counties
    that populate CITY for zero rows (Cuyahoga-style, where municipality is encoded
    via WARD prefix / RESIDENTIAL_CITY and CITY is blank for every voter), the
    single place resolver's dominant city for that precinct, re-suffixed ' CITY'
    so its slug matches the raw-CITY bundle convention ('CLEVELAND CITY' ->
    cleveland_city, mirroring the raw 'KETTERING CITY').

    Counties that populate CITY for even one row are left untouched: their
    _resolved_city equals CITY (populated rows) or null (blanks, dropped exactly
    as before), so every such county's bundles are byte-identical. This preserves
    the two-totals hero contract (CLAUDE.md 4) -- a blank-CITY row in a county that
    DOES use CITY is never silently reassigned to a city hero. Within a fully
    blank-CITY county precincts do not straddle municipalities, so the precinct-
    dominant resolver is row-accurate there and matches the geo tree's city nodes
    (this is why the parent handoff's 'do not use _dominant_city_per_precinct'
    warning does not apply once the fallback is gated to zero-CITY counties).
    """
    import voter_data_cleaner as _v2
    if 'CITY' not in df.columns or 'PRECINCT_NAME' not in df.columns \
            or 'COUNTY_NUMBER' not in df.columns:
        if 'CITY' in df.columns:
            return df.with_columns(pl.col('CITY').alias('_resolved_city'))
        return df

    zero_city_counties = (
        df.group_by('COUNTY_NUMBER')
          .agg((pl.col('CITY').str.strip_chars() != '').any().alias('_has_city'))
          .filter(~pl.col('_has_city'))['COUNTY_NUMBER']
          .to_list()
    )

    fb_rows = []
    for cn in zero_city_counties:
        cdf = df.filter(pl.col('COUNTY_NUMBER') == cn)
        for pname, city in _v2._dominant_city_per_precinct(cdf).items():
            fb_rows.append((cn, pname, f'{city} CITY'))

    if fb_rows:
        fb = pl.DataFrame(
            fb_rows,
            schema=['COUNTY_NUMBER', 'PRECINCT_NAME', '_fallback_city'],
            orient='row',
        ).with_columns(pl.col('COUNTY_NUMBER').cast(df.schema['COUNTY_NUMBER']))
        df = df.join(fb, on=['COUNTY_NUMBER', 'PRECINCT_NAME'], how='left')
    else:
        df = df.with_columns(pl.lit(None, dtype=pl.Utf8).alias('_fallback_city'))

    df = df.with_columns(
        pl.when(pl.col('CITY').str.strip_chars().fill_null('') != '')
          .then(pl.col('CITY'))
          .otherwise(pl.col('_fallback_city'))
          .alias('_resolved_city')
    ).drop('_fallback_city')

    if fb_rows:
        logger.info(
            f'  cities (A2): resolved-city fallback active for '
            f'{len(zero_city_counties)} zero-CITY counties, '
            f'{len(fb_rows)} precinct->city mappings'
        )
    return df


def aggregate_jurisdiction(
    df: pl.DataFrame,
    jurisdiction_key: str,
    jurisdiction_name: str | None,
    jurisdiction_type: str,
    election_cols: list[str],
    today: str,
    logger: logging.Logger,
    county_name: str | None = None,
) -> dict:
    """
    Aggregate voter data for a single jurisdiction and return chart data.

    Returns dict with keys: party_affiliation, decade_distribution, party_by_decade,
    party_by_generation, unc_shadow, cohort_counts
    """

    if jurisdiction_name is None or (isinstance(jurisdiction_name, float)):
        return {}

    # County-scoped types (townships, villages, municipal courts) prefix the
    # slug with county and qualify the display name.  Required because the
    # same name denotes legally distinct entities across counties (e.g. 23
    # Washington Townships in Ohio).  Non-scoped callers pass county_name=None
    # and behave exactly as before.
    if county_name:
        slug         = f'{_slugify(county_name)}_{_slugify(jurisdiction_name)}'
        display_name = f'{jurisdiction_name} ({county_name} Co.)'
    else:
        slug         = _slugify(jurisdiction_name)
        display_name = str(jurisdiction_name)
        
        if jurisdiction_type in ('Local School District', 'City School District', 'Exempted Village School District'):
            import re
            m = re.search(r'\s*\([^)]*\)$', display_name)
            if m:
                extracted_county = m.group(0).strip(' ()')
                base_name = re.sub(r'\s*\([^)]*\)$', '', display_name)
                display_name = f'{base_name.title()} ({extracted_county.title()} Co.)'

    note = f'Analysis run {today} — Ohio Secretary of State SWVF voter file'

    # Subset to this jurisdiction.  When called via the county-scoped path,
    # the caller has already pre-filtered the partition to a single
    # (county, name) slice, so this filter is a no-op on the correct subset.
    subset = df.filter(pl.col(jurisdiction_key) == jurisdiction_name)
    if subset.height == 0:
        logger.warning(f'Empty subset: {jurisdiction_type} {display_name}')
        return {}

    result = {
        'slug':              slug,
        'jurisdiction_type': jurisdiction_type,
        'jurisdiction_name': display_name,
        'county':            county_name,
        'voter_count':       subset.height,
    }

    # ── Party affiliation (doughnut) ──────────────────────────────────────────
    if 'cohort_family' in subset.columns:
        cohort_counts_df = (
            subset.group_by('cohort_family').agg(pl.len().alias('n'))
        )
        cmap = dict(zip(cohort_counts_df['cohort_family'].to_list(),
                        cohort_counts_df['n'].to_list()))

        pa_labels = [lbl   for _, lbl, _ in COHORT_SLICES]
        pa_colors = [color for _, _, color in COHORT_SLICES]
        pa_counts = [int(cmap.get(fam, 0)) for fam, _, _ in COHORT_SLICES]
        pa_total  = sum(pa_counts)
        pa_pcts   = [int(round(100 * c / pa_total)) if pa_total > 0 else 0 for c in pa_counts]

        result['party_affiliation'] = {
            'title': 'Party Affiliation',
            'type': 'doughnut',
            'chartConfig': {
                'labels': [f'{lbl} — {cnt:,} ({pct}%)'
                          for lbl, cnt, pct in zip(pa_labels, pa_counts, pa_pcts)],
                'datasets': [{
                    'data': pa_counts,
                    'backgroundColor': pa_colors,
                }],
            },
            'note': note + ' — partisan spectrum: affiliated + behavioral cohorts.',
        }

        result['cohort_counts'] = {fam: int(cmap.get(fam, 0)) for fam, _, _ in COHORT_SLICES}

    # ── Decade distribution (bar) ─────────────────────────────────────────────
    if 'Decade' in subset.columns:
        decade_df = (
            subset.group_by('Decade')
                  .agg(pl.len().alias('count'))
                  .sort('Decade')
        )
        result['decade_distribution'] = {
            'title': 'Voter Age Distribution by Birth Decade',
            'type': 'bar',
            'chartConfig': {
                'labels': [f'{d}s' for d in decade_df['Decade'].to_list()],
                'datasets': [{
                    'label': 'Registered Voters',
                    'data': decade_df['count'].to_list(),
                    'backgroundColor': CHART_COLORS['bar'],
                    'borderRadius': 4,
                }],
            },
            'note': note,
        }

    # ── Party × Decade (stacked bar) ──────────────────────────────────────────
    if 'cohort_family' in subset.columns and 'Decade' in subset.columns:
        party_decade_df = (
            subset.group_by(['Decade', 'cohort_family'])
                  .agg(pl.len().alias('count'))
                  .sort('Decade')
        )

        decades = sorted(party_decade_df['Decade'].unique().to_list())
        decade_labels = [f'{d}s' for d in decades]

        datasets = []
        for fam, lbl, color in COHORT_SLICES:
            data = []
            for decade in decades:
                row = party_decade_df.filter(
                    (pl.col('Decade') == decade) & (pl.col('cohort_family') == fam)
                )
                data.append(int(row['count'].sum()) if row.height > 0 else 0)

            datasets.append({
                'label': lbl,
                'data': data,
                'backgroundColor': color,
                'stack': COHORT_STACK_MAP[fam],
            })

        result['party_by_decade'] = {
            'title': 'Party Affiliation by Birth Decade',
            'type': 'bar',
            'chartConfig': {
                'labels': decade_labels,
                'datasets': datasets,
            },
            'note': note,
        }

    # ── Generation distribution (bar) ─────────────────────────────────────────
    # Uses the canonical `Generation` column built by the cleaner (Pew bounds),
    # mirroring decade_distribution. Empty generations are dropped from labels.
    if 'Generation' in subset.columns:
        gen_df = (
            subset.group_by('Generation')
                  .agg(pl.len().alias('count'))
        )
        gmap = dict(zip(gen_df['Generation'].to_list(), gen_df['count'].to_list()))
        gen_labels = [g for g in _GEN_ORDER if gmap.get(g)]
        result['generation_distribution'] = {
            'title': 'Voter Distribution by Generation',
            'type': 'bar',
            'chartConfig': {
                'labels': gen_labels,
                'datasets': [{
                    'label': 'Registered Voters',
                    'data': [int(gmap.get(g, 0)) for g in gen_labels],
                    'backgroundColor': CHART_COLORS['bar'],
                    'borderRadius': 4,
                }],
            },
            'note': note,
        }

    # ── Party × Generation (stacked bar) ──────────────────────────────────────
    if 'cohort_family' in subset.columns and 'Generation' in subset.columns:
        party_gen_df = (
            subset.group_by(['Generation', 'cohort_family'])
                  .agg(pl.len().alias('count'))
        )
        gen_order = [g for g in _GEN_ORDER
                     if party_gen_df.filter(pl.col('Generation') == g).height > 0]

        datasets = []
        for fam, lbl, color in COHORT_SLICES:
            data = []
            for gen in gen_order:
                row = party_gen_df.filter(
                    (pl.col('Generation') == gen) & (pl.col('cohort_family') == fam)
                )
                data.append(int(row['count'].sum()) if row.height > 0 else 0)

            datasets.append({
                'label': lbl,
                'data': data,
                'backgroundColor': color,
                'stack': COHORT_STACK_MAP[fam],
            })

        result['party_by_generation'] = {
            'title': 'Party Affiliation by Generation',
            'type': 'bar',
            'chartConfig': {
                'labels': gen_order,
                'datasets': datasets,
            },
            'note': note,
        }

    # ── UNC Shadow (stacked bar) ──────────────────────────────────────────────
    if 'cohort_family' in subset.columns:
        unc_families = ['UNC_LAPSED_R', 'MIXED_ACTIVE', 'MIXED_LAPSED',
                        'UNC_NONPARTISAN', 'UNC_NO_PRIMARY', 'UNC_LAPSED_D']
        unc_labels = [lbl for fam, lbl, _ in COHORT_SLICES if fam in unc_families]
        unc_colors = [color for fam, _, color in COHORT_SLICES if fam in unc_families]

        unc_counts = []
        for fam in unc_families:
            cnt = int(subset.filter(pl.col('cohort_family') == fam).height)
            unc_counts.append(cnt)

        result['unc_shadow'] = {
            'title': 'UNC Shadow Partisanship',
            'type': 'doughnut',
            'chartConfig': {
                'labels': [f'{lbl} — {cnt:,}' for lbl, cnt in zip(unc_labels, unc_counts)],
                'datasets': [{
                    'data': unc_counts,
                    'backgroundColor': unc_colors,
                }],
            },
            'note': note,
        }

    return result


def export_jurisdiction_json(
    results: dict,
    jurisdiction_type: str,
    logger: logging.Logger,
):
    """Export aggregated jurisdiction data to JSON files."""

    type_dir = DATA_DIR / jurisdiction_type.lower().replace(' ', '_')
    type_dir.mkdir(parents=True, exist_ok=True)

    for result in results:
        if not result:
            continue

        slug = result.get('slug')
        if not slug:
            continue

        # Write each chart type as separate JSON
        for chart_type in ['party_affiliation', 'decade_distribution', 'party_by_decade',
                          'generation_distribution', 'party_by_generation', 'unc_shadow']:
            if chart_type in result:
                chart_data = result[chart_type]
                chart_data['geography'] = jurisdiction_type
                chart_data['jurisdiction_name'] = result['jurisdiction_name']
                chart_data['updated'] = result.get('today', date_t.today().isoformat())

                filepath = type_dir / f'{slug}_{chart_type}.json'
                _dump_json(chart_data, filepath, logger)



# Enriched-cache freshness and writing: voter_data_cleaner owns the single
# _cache_is_fresh() and _write_cache_atomic(). This module used to keep copies;
# its writer skipped the provenance sidecar (snapshot date + affiliation
# window), so a cache rebuilt on this path left the sidecar describing an
# older snapshot.



def export_jurisdiction_index(results, jurisdiction_type_key, logger):
    """Write docs/data/{type}/index.json listing all slugs + metadata for the type.
    Called from main() after the JSON export loop so the dashboard can populate
    its filter dropdowns without re-running the full aggregation.
    """
    config     = JURISDICTIONS.get(jurisdiction_type_key, {})
    type_dir   = DATA_DIR / config.get('display', jurisdiction_type_key).lower().replace(' ', '_')
    PRIMARY_SUFFIX = '_party_affiliation.json'
    CHART_TYPES    = ['party_affiliation', 'decade_distribution', 'party_by_decade',
                      'generation_distribution', 'party_by_generation', 'unc_shadow']

    entries = []
    if not type_dir.exists():
        logger.warning(f'Index export: dir not found {type_dir}')
        return

    pa_files = sorted(f for f in type_dir.iterdir() if f.name.endswith(PRIMARY_SUFFIX))
    for fpath in pa_files:
        slug = fpath.name[:-len(PRIMARY_SUFFIX)]
        import json as _json
        d = _json.loads(fpath.read_text(encoding='utf-8'))
        display_name = d.get('jurisdiction_name') or slug
        try:
            voter_count = sum(d['chartConfig']['datasets'][0]['data'])
        except Exception:
            voter_count = 0

        county = None
        county_slug = None
        if config.get('county_scoped'):
            m = re.search(r'\((\w[\w\s]+)\s+Co\.\)$', display_name)
            if m:
                county = m.group(1)
                county_slug = county.lower()
        bare_name = re.sub(r'\s*\(\w[\w\s]+\s+Co\.\)$', '', display_name).strip()
        available = [ct for ct in CHART_TYPES if (type_dir / f'{slug}_{ct}.json').exists()]

        entries.append({
            'slug':         slug,
            'name':         bare_name,
            'display_name': display_name,
            'county':       county,
            'county_slug':  county_slug,
            'voter_count':  voter_count,
            'charts':       available,
        })

    if config.get('county_scoped'):
        entries.sort(key=lambda e: (e['county'] or '', e['name']))
    else:
        entries.sort(key=lambda e: e['name'])

    _dump_json(entries, type_dir / 'index.json', logger)
    logger.info(f'index.json written: {len(entries)} entries for {jurisdiction_type_key}')


def build_ward_entities(df, election_cols, today, logger):
    """Build municipality-scoped ward entities and export docs/data/ward/.

    Replaces the falsified 'globally unique WARD' model. Each entity is keyed by
    (parent place, WARD value): the five colliding 'FIRST WARD's split by their
    real parent city, while a city-ward spanning counties (ALLIANCE WARD 2 in
    Mahoning + Stark) merges into one entity with both counties' voters. Parent
    derivation and slugging are delegated to
    voter_data_cleaner._ward_map_per_county -- the single ward resolver
    (CLAUDE.md 5) also used by the precinct-index ward stamp, so a precinct's
    ward_slug and these entity slugs can never diverge. Wards carry the same
    6 chart types as township/village; their narrative is written afterwards by
    tools/narrative/generate_narratives.py (level 'ward').
    """
    import voter_data_cleaner as _v2

    type_dir = DATA_DIR / 'ward'

    # Full-tree replacement. The old bare-slug files (first_ward, 1st_ward, ...)
    # silently merged unrelated municipalities; clear them so git sees a clean
    # swap. They are tracked in docs/data/ward/, hence recoverable via git
    # checkout if this run is ever aborted mid-way.
    if type_dir.exists():
        removed = sum(1 for f in type_dir.glob('*.json') if (f.unlink() or True))
        logger.info('Ward tree: cleared %d stale bare-slug files', removed)
    type_dir.mkdir(parents=True, exist_ok=True)

    if 'WARD' not in df.columns:
        logger.warning('No WARD column present; skipping ward entities')
        return

    county_name_by_slug = {_slugify(nm): nm for nm in _v2.OHIO_COUNTIES.values()}

    annotated = []
    desc_by_slug: dict = {}
    counties_by_slug: dict = {}   # slug -> {county_slug: voter_count}
    excluded_abuse = 0

    for cn_frame in df.partition_by('COUNTY_NUMBER', include_key=True):
        cn = _cn_key(cn_frame['COUNTY_NUMBER'][0])
        county_name = _v2.OHIO_COUNTIES.get(cn, 'Unknown')
        county_slug = _slugify(county_name)

        wm = _v2._ward_map_per_county(cn_frame, county_slug)
        if not wm:
            continue

        rows = []
        for ward_value, d in wm.items():
            if d['abuse']:
                excluded_abuse += 1
                continue
            rows.append((ward_value, d['slug']))
            desc_by_slug.setdefault(d['slug'], {
                'slug':              d['slug'],
                'name':              d['ward_name'],
                'parent_type':       d['parent_type'],
                'parent_name':       d['parent_name'],
                'parent_place_slug': d['parent_place_slug'],
            })
            counties_by_slug.setdefault(d['slug'], {})
        if not rows:
            continue

        map_df = pl.DataFrame(
            {'WARD': [r[0] for r in rows], '_ward_slug': [r[1] for r in rows]}
        )
        # inner join keeps only ward-holding, non-abuse rows; select the minimal
        # columns the chart aggregation needs (cohort_family, Decade, Generation)
        # to keep the statewide concat small. Dropping Generation here is what
        # left every ward without its two generation charts (evaluation P7). Must collapse internal whitespace the same way
        # _ward_map_per_county does (map_df's WARD keys are already collapsed) --
        # strip_chars() alone only trims leading/trailing space, so a raw value
        # like Licking's 'CITY OF NEWARK  WD2' (irregular internal spacing) would
        # never match map_df's collapsed 'CITY OF NEWARK WD2' key, silently
        # dropping the entire ward from the join.
        sub = (
            cn_frame.with_columns(
                pl.col('WARD').str.strip_chars().str.replace_all(r'\s+', ' ').alias('WARD')
            )
                    .join(map_df, on='WARD', how='inner')
                    .select(['cohort_family', 'Decade', 'Generation', '_ward_slug'])
        )
        cc = sub.group_by('_ward_slug').agg(pl.len().alias('n'))
        for s, n in zip(cc['_ward_slug'].to_list(), cc['n'].to_list()):
            counties_by_slug[s][county_slug] = counties_by_slug[s].get(county_slug, 0) + int(n)
        annotated.append(sub)

    if not annotated:
        logger.warning('No ward entities built')
        return

    big = pl.concat(annotated, how='diagonal_relaxed')
    logger.info('Ward entities: %d rows -> %d entities (%d township-name abuse values excluded)',
                big.height, len(desc_by_slug), excluded_abuse)

    results = []
    index_entries = []
    for part in big.partition_by('_ward_slug', include_key=True):
        slug = part['_ward_slug'][0]
        desc = desc_by_slug[slug]
        keyed = part.with_columns(pl.lit(slug).alias('_entity'))
        r = aggregate_jurisdiction(keyed, '_entity', slug, 'Ward',
                                   election_cols, today, logger, None)
        if not r:
            continue

        ccounts = counties_by_slug.get(slug, {})
        county_slugs = [c for c, _ in sorted(ccounts.items(), key=lambda kv: (-kv[1], kv[0]))]
        primary_slug = county_slugs[0] if county_slugs else None
        primary_name = county_name_by_slug.get(primary_slug)
        display_name = f"{desc['name']} ({desc['parent_name']} {desc['parent_type'].title()})"

        r['slug'] = slug
        r['jurisdiction_type'] = 'Ward'
        r['jurisdiction_name'] = display_name
        r['county'] = primary_name
        r['today'] = today
        results.append(r)

        charts = [ct for ct in ('party_affiliation', 'decade_distribution',
                                'party_by_decade', 'generation_distribution',
                                'party_by_generation', 'unc_shadow') if ct in r]
        index_entries.append({
            'slug':              slug,
            'name':              desc['name'],
            'display_name':      display_name,
            'parent_type':       desc['parent_type'],
            'parent_name':       desc['parent_name'],
            'parent_place_slug': desc['parent_place_slug'],
            'county':            primary_name,
            'county_slug':       primary_slug,
            'county_slugs':      county_slugs,
            'voter_count':       int(r.get('voter_count', 0)),
            'charts':            charts,
        })

    export_jurisdiction_json(results, 'Ward', logger)
    index_entries.sort(key=lambda e: (e['parent_name'] or '', e['name']))
    _dump_json(index_entries, type_dir / 'index.json', logger)
    logger.info('Ward entities: wrote %d entities + index.json', len(index_entries))



def main(
    jurisdictions_to_process: list[str] | None = None,
    output_format: str = 'json',
    logger: logging.Logger | None = None,
):
    """
    Main entry point: load enriched parquet, aggregate all jurisdictions, export.

    Args:
        jurisdictions_to_process: List of jurisdiction type keys (e.g., ['cities', 'townships']).
                                 If None, processes all.
        output_format: 'json', 'xlsx', or 'both'
        logger: Optional logger; creates one if not provided.
    """

    if logger is None:
        logger, log_file = setup_logger()
        logger.info(f'Log file: {log_file}')

    start_time = time.time()
    today = date_t.today().isoformat()

    # Determine which jurisdictions to process. 'wards' is a pseudo-type routed to
    # the municipality-scoped builder (build_ward_entities), never the generic loop.
    if jurisdictions_to_process is None:
        process_wards = True
        jurisdictions_to_process = list(JURISDICTIONS.keys())
    else:
        process_wards = 'wards' in jurisdictions_to_process
        jurisdictions_to_process = [j for j in jurisdictions_to_process if j != 'wards']

    logger.info(f'Processing {len(jurisdictions_to_process)} jurisdiction types: {jurisdictions_to_process}')
    logger.info(f'Output format: {output_format}')

    # Load enriched voter data -- use persistent cache when fresh
    import voter_data_cleaner as _v2
    if _v2._cache_is_fresh(logger):
        logger.info('Loading enriched voter data from persistent cache...')
        df = pl.read_parquet(ENRICHED_CACHE)
        logger.info(f'Loaded from cache: {df.height:,} rows x {df.width} columns')
    else:
        if not PARQUET_DIR.exists():
            logger.error(f'Parquet cache not found at {PARQUET_DIR}')
            logger.error('Run ohio_voter_pipeline.py option 1 to generate it.')
            return False
        logger.info('Loading voter parquet cache...')
        df = pl.read_parquet(parquet_glob(PARQUET_DIR), hive_partitioning=True)
        logger.info(f'Loaded {df.height:,} rows x {df.width} columns')
        logger.info('Enriching voter data (cohort classification + demographics)...')
        df = _v2.clean_voter_data(df, logger)
        logger.info(f'Enriched: {df.height:,} rows x {df.width} columns')
        logger.info('Writing enriched voter data to persistent cache...')
        _v2._write_cache_atomic(df, logger)

    # Get election columns (any column matching PRIMARY-*, GENERAL-*, SPECIAL-* pattern)
    election_cols = [col for col in df.columns if col.split('-')[0] in ['PRIMARY', 'GENERAL', 'SPECIAL']]
    logger.info(f'Found {len(election_cols)} election history columns')

    # Resume cutoff: an existing output file is reused only if it is newer than
    # both the enriched cache it was aggregated from and this module's source
    # (output shape is defined here). See _split_by_freshness().
    resume_cutoff = max(ENRICHED_CACHE.stat().st_mtime, Path(__file__).stat().st_mtime)
    logger.info(
        'Resume cutoff %s: max(enriched cache %s, %s source %s)',
        _v2._fmt_mtime(resume_cutoff), _v2._fmt_mtime(ENRICHED_CACHE.stat().st_mtime),
        Path(__file__).name, _v2._fmt_mtime(Path(__file__).stat().st_mtime),
    )

    # Aggregate each jurisdiction type
    jurisdiction_results = {}

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:

        for jurisdiction_type_key in jurisdictions_to_process:
            if jurisdiction_type_key not in JURISDICTIONS:
                logger.warning(f'Unknown jurisdiction type: {jurisdiction_type_key}')
                continue

            config        = JURISDICTIONS[jurisdiction_type_key]
            column        = config['column']
            display       = config['display']
            county_scoped = config.get('county_scoped', False)

            # Skip if column not in dataframe
            if column not in df.columns:
                logger.warning(f'Column {column} not in dataframe; skipping {jurisdiction_type_key}')
                continue

            # A2: cities group on the resolved-city column so Cuyahoga-style
            # zero-CITY counties get bundles; populated-CITY counties stay
            # byte-identical. Computed once, lazily, on first row_resolver type.
            if config.get('row_resolver') == 'city':
                if '_resolved_city' not in df.columns:
                    df = _add_resolved_city_column(df, logger)
                if '_resolved_city' in df.columns:
                    column = '_resolved_city'

            # ── Collect valid jurisdiction keys ──────────────────────────────
            # Filters: nulls, floats, blank strings, and single-character
            # artifacts (e.g. "/" rural-voter placeholders that cause timeouts).
            #
            # County-scoped types build composite (county_number, name) keys
            # so that "Washington Township" in 23 different counties is treated
            # as 23 distinct jurisdictions rather than merged into one phantom.
            if county_scoped:
                pairs_df = (
                    df.select(['COUNTY_NUMBER', column])
                      .filter(pl.col(column).is_not_null())
                      .unique()
                )
                jurisdictions = sorted([
                    (_cn_key(row[0]), row[1]) for row in pairs_df.iter_rows()
                    if row[1] is not None
                    and not isinstance(row[1], float)
                    and len(str(row[1]).strip()) > 1
                ])
            else:
                raw_values = df[column].unique().to_list()
                jurisdictions = sorted([
                    j for j in raw_values
                    if j is not None
                    and not isinstance(j, float)
                    and len(str(j).strip()) > 1
                ])

            logger.info(f'Aggregating {jurisdiction_type_key} ({column}): {len(jurisdictions)} jurisdictions')

            # ── Resume: skip jurisdictions already exported AND current ──────
            # Sentinel file: {slug}_party_affiliation.json.  Exists ⇒ all other
            # chart files for that jurisdiction were written in the same call
            # (export_jurisdiction_json writes the bundle together).  Existence
            # alone is not enough: see _split_by_freshness() for the cutoff.
            type_dir = DATA_DIR / display.lower().replace(' ', '_')

            def _slug_for(jur_key) -> str:
                if county_scoped:
                    cn, jn = jur_key
                    cname  = _v2.OHIO_COUNTIES.get(cn, 'unknown')
                    return f'{_slugify(cname)}_{_slugify(str(jn))}'
                return _slugify(str(jur_key))

            def _sentinel_for(jur_key) -> Path:
                return type_dir / f'{_slug_for(jur_key)}_party_affiliation.json'

            # Clear-before-write: prune any file whose slug matches none of the
            # jurisdictions currently present in the data. Must run against the
            # full `jurisdictions` list (pre resume-skip) -- see module docstring
            # on _prune_ghost_family_files for why.
            _pruned = _prune_ghost_family_files(
                type_dir, {_slug_for(j) for j in jurisdictions}, logger,
            )
            if _pruned:
                logger.info(f'  {jurisdiction_type_key}: pruned {_pruned} ghost files before regen')

            if type_dir.exists():
                current, stale, missing = _split_by_freshness(
                    jurisdictions, _sentinel_for, resume_cutoff,
                )
                logger.info(
                    f'  Resume {jurisdiction_type_key}: {len(current)} current (skipped), '
                    f'{len(stale)} stale (older than cutoff, regenerating), '
                    f'{len(missing)} missing'
                )
                if stale:
                    oldest = min(_sentinel_for(j).stat().st_mtime for j in stale)
                    logger.debug(
                        f'    stale examples (oldest {_v2._fmt_mtime(oldest)}): '
                        f'{[_slug_for(j) for j in stale[:10]]}'
                    )
                jurisdictions = [j for j in jurisdictions if j not in current]

            if not jurisdictions:
                logger.info(f'  {jurisdiction_type_key}: fully exported, skipping')
                continue

            # ── Pre-partition: single pass via partition_by ──────────────────
            # County-scoped types partition by [COUNTY_NUMBER, column] so each
            # slice contains exactly the voters of one (county, name) pair.
            print(f'  Pre-partitioning {len(jurisdictions)} {jurisdiction_type_key}...', flush=True)
            if county_scoped:
                _raw_parts = df.partition_by(['COUNTY_NUMBER', column], maintain_order=False, include_key=True)
                _all_parts: dict = {}
                for _frame in _raw_parts:
                    _cn   = _frame['COUNTY_NUMBER'][0]
                    _name = _frame[column][0]
                    if _name is not None and not isinstance(_name, float):
                        _all_parts[(_cn_key(_cn), _name)] = _frame
                partition_map = {
                    pair: _all_parts[pair]
                    for pair in jurisdictions
                    if pair in _all_parts
                }
            else:
                _raw_parts = df.partition_by(column, maintain_order=False, include_key=True)
                _all_parts: dict = {}
                for _frame in _raw_parts:
                    _key = _frame[column][0]
                    if _key is not None and not isinstance(_key, float):
                        _all_parts[str(_key)] = _frame
                partition_map = {
                    name: _all_parts[str(name)]
                    for name in jurisdictions
                    if str(name) in _all_parts
                }

            # ── Submit one task per jurisdiction ─────────────────────────────
            # Workers receive their pre-sliced frame and (for scoped types) the
            # county_name needed to construct county-qualified slugs and labels.
            futures_for_type = {}
            for jur_key in jurisdictions:
                if county_scoped:
                    cn, jn      = jur_key
                    county_name = _v2.OHIO_COUNTIES.get(cn, 'Unknown')
                    future = executor.submit(
                        aggregate_jurisdiction,
                        partition_map[jur_key],
                        column, jn, display, election_cols, today, logger,
                        county_name,
                    )
                else:
                    future = executor.submit(
                        aggregate_jurisdiction,
                        partition_map[jur_key],
                        column, jur_key, display, election_cols, today, logger,
                        None,
                    )
                futures_for_type[jur_key] = future

            # Collect results and print a rolling progress counter so the
            # terminal does not appear frozen during long aggregation passes.
            results = []
            total = len(jurisdictions)
            for idx, jur_key in enumerate(jurisdictions, 1):
                if county_scoped:
                    cn, jn       = jur_key
                    progress_lbl = f'{_v2.OHIO_COUNTIES.get(cn, "?")}/{jn}'
                else:
                    progress_lbl = str(jur_key)
                print(
                    f'\r  [{idx:>5}/{total}] {jurisdiction_type_key}: '
                    f'{progress_lbl[:45]:<45}',
                    end='', flush=True,
                )
                try:
                    result = futures_for_type[jur_key].result(timeout=120)
                    if result:
                        result['today'] = today
                        results.append(result)
                except concurrent.futures.TimeoutError:
                    logger.error(
                        f'Timeout (>120s) aggregating {jurisdiction_type_key} / {jur_key}'
                    )
                except Exception as e:
                    logger.error(
                        f'Error aggregating {jurisdiction_type_key} / {jur_key}: {e}'
                    )
            print()  # newline after rolling progress line
            logger.info(f'  {jurisdiction_type_key}: {len(results)}/{total} aggregated successfully')

            jurisdiction_results[jurisdiction_type_key] = {
                'config': config,
                'results': results,
            }

    # Export
    if output_format in ['json', 'both']:
        logger.info('Exporting JSON...')
        for jurisdiction_type_key, data in jurisdiction_results.items():
            config = data['config']
            results = data['results']
            export_jurisdiction_json(results, config['display'], logger)
            export_jurisdiction_index(results, jurisdiction_type_key, logger)
        logger.info('JSON export complete')

    # Ward entities: municipality-scoped, cross-county-aware; own export + index.
    if process_wards and output_format in ['json', 'both']:
        logger.info('Building municipality-scoped ward entities...')
        build_ward_entities(df, election_cols, today, logger)

    elapsed = time.time() - start_time
    logger.info(f'Done in {elapsed:.1f}s')

    # A reading at the end of the run, not a peak (see _memory_line).
    logger.info('Memory at end: %s', _v2._memory_line())

    return True


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
        description='Aggregate Ohio voter data by jurisdictions present in SWVF.'
    )
    parser.add_argument(
        '--jurisdictions',
        type=str,
        default=None,
        help='Comma-separated list of jurisdiction types to process. '
             'Default: all. Options: cities, townships, villages, '
             'local_school_districts, city_school_districts, exempted_vill_school_districts, '
             'state_senate_districts, state_rep_districts, congressional_districts, '
             'county_court_districts, municipal_court_districts, court_of_appeals, wards'
    )
    parser.add_argument(
        '--format',
        choices=['json', 'xlsx', 'both'],
        default='json',
        help='Output format (default: json)'
    )

    args = parser.parse_args()

    jurisdictions_to_process = None
    if args.jurisdictions:
        jurisdictions_to_process = [j.strip() for j in args.jurisdictions.split(',')]

    logger, log_file = setup_logger()
    logger.info(f'Log file: {log_file}')

    success = main(
        jurisdictions_to_process=jurisdictions_to_process,
        output_format=args.format,
        logger=logger
    )

    sys.exit(0 if success else 1)
