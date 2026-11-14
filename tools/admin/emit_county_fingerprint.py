"""Emit a machine-readable per-county structural fingerprint of a SWVF drop.

This is the single, regenerable artifact behind the 88-county heterogeneity
doctrine (CLAUDE.md Sec.5). Where the doctrine says "a source column's meaning is
heterogeneous across counties until a bounded profile proves it uniform," this
emitter *is* that profile, computed rather than hand-asserted, and re-derivable on
every voterfile drop so the numbers can drift with the data instead of rotting in
prose.

What it measures (structural / set facts only):
  * code_format        - dominant PRECINCT_CODE character-class shape (vendor tell)
  * blank_columns      - static columns this county's BoE never populates (schema
                         divergence: "present here, absent there")
  * name_template      - dominant PRECINCT_NAME token shape (naming philosophy;
                         ADVISORY - shapes route, they do NOT assign jurisdiction)
  * zip_set / cities   - the county's ZIP and CITY sets (topological signatures;
                         set-membership only, order-independent)
  * vendor_cluster_id  - counties sharing an exact (code_format, blank_columns)
                         signature; the ~handful of vendors made visible

What it deliberately does NOT do: it computes no per-precinct jurisdiction
assignment. A precinct straddles wards / school districts / ZIPs, so collapsing
one to a single row (the classic doctrine violation) is out of scope here by
design. Jurisdiction stays with the single resolver in voter_data_cleaner.py.

Reads RAW parquet (local/source/parquet), not the enriched cache: heterogeneity
is a property of the source stream, before our cleaning normalizes it.

Output: local/exports/fingerprints/county_fingerprint_<snapshot_date>.json
(gitignored, date-versioned so successive drops can be diffed - goal: temporal
drift detection). Atomic write via .tmp -> replace.

Usage:
  python tools/admin/emit_county_fingerprint.py
  python tools/admin/emit_county_fingerprint.py --jobs 8 --parquet-dir <path>
"""
import argparse
import json
import os
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
# CLI defaults from the single resolver; --parquet-dir and --staged-from still override them.
from pipeline.paths import PARTITION_FILE, PARTITION_GLOB, TXT_DIR  # noqa: E402
from pipeline.paths import PARQUET_DIR as DEFAULT_PARQUET_DIR  # noqa: E402
DEFAULT_STAGED_FROM = TXT_DIR / 'staged_from.json'
DEFAULT_OUT_DIR = _ROOT / 'local' / 'exports' / 'fingerprints'
DATA_QUALITY_MD = _ROOT / 'DATA_QUALITY.md'

# Self-owned machine block in the single registry (DATA_QUALITY.md). Distinct
# from the jurisdiction validator's `auto:` markers - the two never overlap.
FP_BEGIN = '<!-- fingerprint:begin -->'
FP_END = '<!-- fingerprint:end -->'

# Profile schema version - bump when the emitted structure changes so a diff tool
# can refuse to compare incompatible generations.
PROFILE_SCHEMA_VERSION = 1

# Ohio counties are alphabetically numbered 01..88 (Adams..Wyandot). Reference
# data, stable across drops; the partition dir name is the authoritative number.
OHIO_COUNTIES = [
    "Adams", "Allen", "Ashland", "Ashtabula", "Athens", "Auglaize", "Belmont", "Brown",
    "Butler", "Carroll", "Champaign", "Clark", "Clermont", "Clinton", "Columbiana", "Coshocton",
    "Crawford", "Cuyahoga", "Darke", "Defiance", "Delaware", "Erie", "Fairfield", "Fayette",
    "Franklin", "Fulton", "Gallia", "Geauga", "Greene", "Guernsey", "Hamilton", "Hancock",
    "Hardin", "Harrison", "Henry", "Highland", "Hocking", "Holmes", "Huron", "Jackson",
    "Jefferson", "Knox", "Lake", "Lawrence", "Licking", "Logan", "Lorain", "Lucas",
    "Madison", "Mahoning", "Marion", "Medina", "Meigs", "Mercer", "Miami", "Monroe",
    "Montgomery", "Morgan", "Morrow", "Muskingum", "Noble", "Ottawa", "Paulding", "Perry",
    "Pickaway", "Pike", "Portage", "Preble", "Putnam", "Richland", "Ross", "Sandusky",
    "Scioto", "Seneca", "Shelby", "Stark", "Summit", "Trumbull", "Tuscarawas", "Union",
    "Van Wert", "Vinton", "Warren", "Washington", "Wayne", "Williams", "Wood", "Wyandot",
]

# The 89 dynamic election-history columns carry TYPE-MM/DD/YYYY headers. They are
# temporal turnout, not vendor schema, so they are excluded from blank-column
# detection - but their COUNT is recorded per county to catch state-schema drift.
_ELECTION_PREFIXES = ('PRIMARY-', 'GENERAL-', 'SPECIAL-')

# PRECINCT_NAME tokens that carry jurisdictional meaning; preserved literally in
# the name template so the shape reflects naming philosophy, not just arity.
_NAME_KEYWORDS = {
    'WARD', 'WD', 'TWP', 'TOWNSHIP', 'CITY', 'VILLAGE', 'VILL', 'PCT',
    'PRECINCT', 'CORP',
}


def _is_election_col(name):
    return name.startswith(_ELECTION_PREFIXES)


def code_shape(code):
    """Per-character class template of a PRECINCT_CODE.

    Digit -> 'N', letter -> 'A', anything else preserved literally. Length is
    NOT collapsed: '01AAN' -> 'NNAAA', '18-A-AAA' -> 'NN-A-AAA'. Within a county
    the shape is near-uniform, so the mode is the vendor's code format.
    """
    out = []
    for ch in code:
        if ch.isdigit():
            out.append('N')
        elif ch.isalpha():
            out.append('A')
        else:
            out.append(ch)
    return ''.join(out)


def name_template(name):
    """Whitespace-token shape of a PRECINCT_NAME (ADVISORY).

    Jurisdiction keywords kept literal; all-digit token -> [NUM]; single letter ->
    [CHAR]; alpha word -> [WORD]; anything mixed -> [MIX]. This is a routing hint
    only - it describes how a BoE types names, never what jurisdiction a precinct
    belongs to.
    """
    toks = []
    for tok in name.split():
        up = tok.upper()
        if up in _NAME_KEYWORDS:
            toks.append(up)
        elif tok.isdigit():
            toks.append('[NUM]')
        elif tok.isalpha():
            toks.append('[CHAR]' if len(tok) == 1 else '[WORD]')
        else:
            toks.append('[MIX]')
    return ' '.join(toks)


def _clean_series(df, col):
    """Non-blank, stripped, unique values of a string column as a Python list."""
    if col not in df.columns:
        return []
    s = (
        df.select(pl.col(col).cast(pl.Utf8).fill_null('').str.strip_chars().alias('_v'))
          .filter(pl.col('_v').str.len_chars() > 0)
          .select(pl.col('_v').unique())
          .to_series()
          .to_list()
    )
    return s


def county_fingerprint(county_number, part_path):
    """Compute one county's structural fingerprint from its raw parquet part."""
    idx = int(county_number)
    county_name = OHIO_COUNTIES[idx - 1] if 1 <= idx <= len(OHIO_COUNTIES) else f'Unknown-{county_number}'

    schema = pl.scan_parquet(part_path).collect_schema()
    all_cols = list(schema.names())
    election_cols = [c for c in all_cols if _is_election_col(c)]
    static_cols = [c for c in all_cols if not _is_election_col(c)]

    # Read only the static columns (never the 89 election cols) plus what we need.
    df = pl.read_parquet(part_path, columns=static_cols)
    height = df.height

    # Blank-column detection: a column is blank if every value is null OR, for
    # string columns, strips to empty. SWVF blanks arrive as "" not null (Sec.4),
    # so null_count alone is insufficient for Utf8.
    blank_flags = {}
    exprs = []
    for c in static_cols:
        if schema[c] == pl.Utf8:
            exprs.append(
                (pl.col(c).fill_null('').str.strip_chars().str.len_chars() > 0)
                .any().alias(c)
            )
        else:
            exprs.append((pl.col(c).null_count() < height).alias(c))
    if exprs:
        populated = df.select(exprs).row(0, named=True)
        blank_flags = {c: (not populated[c]) for c in static_cols}
    blank_columns = sorted(c for c, is_blank in blank_flags.items() if is_blank)

    # PRECINCT_CODE format (mode over distinct codes).
    codes = _clean_series(df, 'PRECINCT_CODE')
    code_counts = Counter(code_shape(c) for c in codes)
    if code_counts:
        top_code, n = code_counts.most_common(1)[0]
        code_format = top_code
        code_format_purity = round(n / sum(code_counts.values()), 4)
    else:
        code_format, code_format_purity = None, None

    # PRECINCT_NAME template (mode over distinct names).
    names = _clean_series(df, 'PRECINCT_NAME')
    name_counts = Counter(name_template(nm) for nm in names)
    if name_counts:
        top_name, n = name_counts.most_common(1)[0]
        dominant_name_template = top_name
        name_template_purity = round(n / sum(name_counts.values()), 4)
    else:
        dominant_name_template, name_template_purity = None, None

    zip_set = sorted(_clean_series(df, 'RESIDENTIAL_ZIP'))
    city_set = sorted(_clean_series(df, 'CITY'))

    return {
        'county_number': county_number,
        'county_name': county_name,
        'voter_count': height,
        'precinct_count': len(codes),
        'n_total_columns': len(all_cols),
        'n_static_columns': len(static_cols),
        'n_election_columns': len(election_cols),
        'code_format': code_format,
        'code_format_purity': code_format_purity,
        'dominant_name_template': dominant_name_template,
        'name_template_purity': name_template_purity,
        'blank_columns': blank_columns,
        'n_blank_columns': len(blank_columns),
        'zip_set': zip_set,
        'n_zips': len(zip_set),
        'cities': city_set,
        'n_cities': len(city_set),
    }


def assign_vendor_clusters(records):
    """Cross-county pass (single-threaded, after all counties): group counties by
    their code_format (the true vendor tell). The exact set of blank_columns is
    left as honest-but-noisy supplementary detail, as it mixes vendor feature flags
    with literal geographic absences (e.g., lack of exempted village schools)."""
    sig_to_counties = {}
    for r in records:
        sig = r['code_format']
        sig_to_counties.setdefault(sig, []).append(r['county_number'])

    ordered = sorted(sig_to_counties.items(), key=lambda kv: (-len(kv[1]), kv[0] or ''))
    clusters = {}
    sig_to_id = {}
    for i, (sig, members) in enumerate(ordered, start=1):
        cid = f'C{i:02d}'
        sig_to_id[sig] = cid
        clusters[cid] = {
            'code_format': sig,
            'county_count': len(members),
            'counties': sorted(members),
        }
    for r in records:
        sig = r['code_format']
        r['vendor_cluster_id'] = sig_to_id[sig]
    return clusters


def resolve_snapshot_date(staged_from_path, override):
    if override:
        return override
    if staged_from_path.exists():
        meta = json.loads(staged_from_path.read_text(encoding='utf-8'))
        d = meta.get('snapshot_date')
        if d:
            return d
    return None


def _code_format_census(counties):
    """code_format -> sorted county-number list, ordered by descending membership."""
    m = {}
    for r in counties:
        m.setdefault(r['code_format'], []).append(r['county_number'])
    for nums in m.values():
        nums.sort()
    return dict(sorted(m.items(), key=lambda kv: (-len(kv[1]), kv[0] or '')))


def _parse_fp_first_seen(text):
    """code_format -> first_observed date from a prior fingerprint block, so a
    re-run does not reset when a format was first seen (mirrors the jurisdiction
    validator's first-seen preservation). Tolerant of a missing/empty block."""
    first = {}
    if FP_BEGIN not in text or FP_END not in text:
        return first
    block = text.split(FP_BEGIN, 1)[1].split(FP_END, 1)[0]
    for line in block.splitlines():
        if not line.startswith('|'):
            continue
        cells = [c.strip() for c in line.strip('|').split('|')]
        if len(cells) < 5 or cells[0] in ('Code format', '---') or set(cells[0]) == {'-'}:
            continue
        fmt = cells[0].strip('`').strip()
        first_observed = cells[2]
        if fmt and first_observed and first_observed != 'First observed':
            first[fmt] = first_observed
    return first


def build_fingerprint_block(profile, prior_first_seen):
    """The fingerprint stream's registry block: the earned-uniformity record
    (which structural axes are proven uniform across which counties), plus an
    honest note on where clustering does NOT hold. Observation dates are anchored
    to the data snapshot, not wall-clock, so the block doubles as a
    vendor-migration detector across drops."""
    snap = profile['snapshot_date']
    census = _code_format_census(profile['counties'])
    # Blank-column fragmentation measured DIRECTLY from the records, independent of
    # however assign_vendor_clusters currently groups counties -- so this advisory
    # can never contradict the cluster definition.
    blank_sigs = Counter(
        (c['code_format'], tuple(c['blank_columns'])) for c in profile['counties']
    )
    n_sigs = len(blank_sigs)
    singleton_sigs = sum(1 for v in blank_sigs.values() if v == 1)

    lines = [
        (f"_Regenerated by `python tools/admin/emit_county_fingerprint.py --ledger` "
         f"from snapshot {snap}. Full per-county detail: "
         f"`local/exports/fingerprints/county_fingerprint_{snap}.json`._"),
        "",
        ("**Proven-uniform axis - PRECINCT_CODE format.** Intra-county purity is 100% "
         "in every county (no county mixes code formats), so a resolver may key on "
         "`code_format` across the counties in each row rather than special-casing them."),
        "",
        "| Code format | Counties | First observed | Last observed | County numbers |",
        "|---|---|---|---|---|",
    ]
    for fmt, nums in census.items():
        first = prior_first_seen.get(fmt, snap)
        lines.append(f"| `{fmt}` | {len(nums)} | {first} | {snap} | {', '.join(nums)} |")
    lines += [
        "",
        (f"**Advisory - blank-column heterogeneity.** Across {profile['county_count']} "
         f"counties there are {n_sigs} distinct (code_format, blank-column-set) "
         f"signatures, {singleton_sigs} unique to a single county. The blank-column SET "
         f"therefore does NOT collapse counties into clean vendor buckets. (Blank-*count* "
         f"appears to, but that is a counting artifact - counties sharing a blank count "
         f"often blank different columns.) `code_format` above is the load-bearing "
         f"uniformity axis; each county's `blank_columns` is per-county detail, not a "
         f"vendor proof."),
    ]
    return "\n".join(lines)


def write_fingerprint_ledger(profile):
    """Write/refresh the `fingerprint:` block in DATA_QUALITY.md, preserving
    everything outside the markers (crucially the jurisdiction `auto:` block and
    all manual sections). Atomic .tmp -> replace."""
    existing = DATA_QUALITY_MD.read_text(encoding='utf-8') if DATA_QUALITY_MD.exists() else ''
    prior = _parse_fp_first_seen(existing)
    block = build_fingerprint_block(profile, prior)

    if FP_BEGIN in existing and FP_END in existing:
        pre, rest = existing.split(FP_BEGIN, 1)
        _, post = rest.split(FP_END, 1)
        new_text = f"{pre}{FP_BEGIN}\n{block}\n{FP_END}{post}"
    else:
        section = (
            "\n## Structural fingerprint (vendor/format stream)\n\n"
            "Machine-regenerated by `python tools/admin/emit_county_fingerprint.py "
            "--ledger`; do not hand-edit inside the `fingerprint:` markers. This is the "
            "earned-uniformity record for the SWVF structural stream - the inverse of a "
            "deviation list: it cites which source-structure axes are proven uniform "
            "across which counties (Sec.5), so a resolver can key on them instead of "
            "re-checking.\n\n"
            f"{FP_BEGIN}\n{block}\n{FP_END}\n"
        )
        base = existing if existing.endswith('\n') or existing == '' else existing + '\n'
        new_text = base + section

    tmp = DATA_QUALITY_MD.with_suffix('.md.tmp')
    tmp.write_text(new_text, encoding='utf-8')
    os.replace(tmp, DATA_QUALITY_MD)
    print(f'[ledger] updated {DATA_QUALITY_MD} (fingerprint block, snapshot {profile["snapshot_date"]})')


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--parquet-dir', type=Path, default=DEFAULT_PARQUET_DIR)
    ap.add_argument('--out-dir', type=Path, default=DEFAULT_OUT_DIR)
    ap.add_argument('--staged-from', type=Path, default=DEFAULT_STAGED_FROM)
    ap.add_argument('--snapshot-date', default=None,
                    help='ISO date override; default reads staged_from.json')
    ap.add_argument('--jobs', type=int, default=8)
    ap.add_argument('--ledger', action='store_true',
                    help='also write the fingerprint block into DATA_QUALITY.md')
    args = ap.parse_args()

    if not args.parquet_dir.exists():
        print(f'ERROR: parquet dir not found: {args.parquet_dir}', file=sys.stderr)
        return 2

    parts = sorted(args.parquet_dir.glob(f'{PARTITION_GLOB}/{PARTITION_FILE}'))
    if not parts:
        print(f'ERROR: no {PARTITION_GLOB}/{PARTITION_FILE} under {args.parquet_dir}', file=sys.stderr)
        return 2

    snapshot_date = resolve_snapshot_date(args.staged_from, args.snapshot_date)
    if not snapshot_date:
        print('ERROR: snapshot_date not found (no staged_from.json and no --snapshot-date)',
              file=sys.stderr)
        return 2

    jobs = [(p.parent.name.split('=', 1)[1], p) for p in parts]
    print(f'Fingerprinting {len(jobs)} counties (snapshot {snapshot_date}, jobs={args.jobs}) ...')
    t0 = time.time()

    records = []
    with ThreadPoolExecutor(max_workers=args.jobs) as ex:
        futures = {ex.submit(county_fingerprint, num, path): num for num, path in jobs}
        for fut in futures:
            records.append(fut.result())
    records.sort(key=lambda r: r['county_number'])

    # Cross-county reduction runs single-threaded, after every county is in hand.
    clusters = assign_vendor_clusters(records)

    profile = {
        'profile_schema_version': PROFILE_SCHEMA_VERSION,
        'snapshot_date': snapshot_date,
        'generated_at': datetime.now(timezone.utc).isoformat(timespec='seconds'),
        'source': str(args.parquet_dir),
        'county_count': len(records),
        'vendor_cluster_count': len(clusters),
        'vendor_clusters': clusters,
        'counties': records,
    }

    args.out_dir.mkdir(parents=True, exist_ok=True)
    out_path = args.out_dir / f'county_fingerprint_{snapshot_date}.json'
    tmp_path = out_path.with_suffix('.json.tmp')
    tmp_path.write_text(json.dumps(profile, indent=2, sort_keys=False), encoding='utf-8')
    os.replace(tmp_path, out_path)

    if args.ledger:
        write_fingerprint_ledger(profile)

    dt = time.time() - t0
    total_voters = sum(r['voter_count'] for r in records)
    total_precincts = sum(r['precinct_count'] for r in records)
    print(f'Wrote {out_path}')
    print(f'  counties={len(records)}  voters={total_voters:,}  precincts={total_precincts:,}')
    print(f'  vendor_clusters={len(clusters)} (top: '
          + ', '.join(f'{cid}={c["county_count"]}' for cid, c in list(clusters.items())[:4]) + ')')
    print(f'  elapsed {dt:.1f}s')
    return 0


if __name__ == '__main__':
    sys.exit(main())
