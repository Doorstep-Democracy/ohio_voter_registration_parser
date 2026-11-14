"""
export_crosser_targets.py
==========================
Axis A Phase 3 (Linear DOO-7) - subscriber export v1, the "crosser" export.

Produces one CSV per county (or all 88, with --all-counties) listing every
"non-locked crosser" in that county: a voter who has voted in both parties'
primaries at different times and whose recent ballot history does not show
them settled back into one party. This is item 1, the top-ranked product, on
the Axis A subscriber-tier stack rank (an operator working document kept under
the gitignored local/context/scope/, not in this repository). County parties and
campaigns buy this list to target persuadable voters rather than straight
turnout/base voters.

What a row contains (BUYER_STATIC_COLUMNS + election history + COMPUTED_COLUMNS)
------------------------------------------------------------------------------
The CSV columns are an explicit whitelist, never "whatever the cache holds":

    the 46 static SWVF columns, verbatim, in file order (BUYER_STATIC_COLUMNS)
    the SWVF election-history columns, verbatim, chronological (resolved by
        pipeline.voter_data_cleaner.election_cols_from_names, never hardcoded)
    COUNTY_NAME      - added here; the SWVF carries a county number, no name
    fresh_defector   - did this voter's MOST RECENT primary ballot happen to be
                       the party switch itself, within the last two years?
                       (stack-rank item 2, from axis_a_segments.fresh_defector)
    voted_last_even_november_general
                     - did this voter cast a counted ballot in the most recent
                       November, even-year general election? A turnout hygiene
                       flag riding along on this export, as the stack rank's
                       item 5 write-up describes.

Every column the pipeline derives (cohort, lean_score, switch_count,
partisan_ballot_sequence, ...) is left out by construction. switch_count in
particular is banned on every surface (it undercounts crossing ~11x), and
TIER_LINE_FIELDS is asserted absent from every file written.

Where the rows come from
------------------------
The enriched cache (pipeline.paths.ENRICHED_CACHE), which carries the crosser
classification produced once by classify_all_voters_primary_history(). Unlike
tools/export/export_unc_targets.py this script never reads the raw SWVF text:
re-deriving the classification would be a second copy of logic that already has
one home. Segment predicates come from tools/scoring/axis_a_segments.py.

Before reading, the script asks the pipeline's single freshness resolver
(_cache_is_fresh) whether the cache is newer than the raw partitions and the
classifier source. A stale cache is refused, not rebuilt: an export must never
start a statewide pipeline rebuild as a side effect, so this script does not go
through load_enriched_voters (which rebuilds on a stale cache and holds the
statewide frame in memory). It reads the cache lazily, one county at a time,
projected to the columns it writes.

Provenance
----------
The cache records the snapshot date and the PARTY_AFFILIATION window it was
built under (R.C. 3513.19; CLAUDE.md section 4). Affiliation decides which
cohort a crosser lands in, so two exports are only comparable if their windows
match. Both values are required in the source file's metadata and are written,
with the anchor column and row counts, to a provenance JSON next to each CSV.

Output layout
-------------
    local/exports/Crosser_Targets/
        57_Montgomery/
            montgomery_CROSSER_targets.csv
            montgomery_CROSSER_targets.provenance.json
        ...

Usage
-----
    python tools/export/export_crosser_targets.py --county 57 --county-name montgomery
    python tools/export/export_crosser_targets.py --all-counties

    --county        Zero-padded county number (e.g. 57). Required unless --all-counties.
    --county-name   Slug used in the output filename (e.g. montgomery). Required with --county.
    --all-counties  Export every county in the source, one CSV each.
    --parquet       Read this enriched parquet instead of the managed cache. The
                    freshness check applies to the managed cache only; provenance
                    metadata is still required.
    --output-dir    Root output directory. Defaults to local/exports.

Security: local/ is gitignored (CLAUDE.md section 9). Voter rows are public
record, but bulk export files stay local by convention, like UNC_Exports/.
"""

from __future__ import annotations

import argparse
import json
import logging
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from pipeline.paths import COUNTY_COUNT, ENRICHED_CACHE  # noqa: E402
from pipeline.voter_data_cleaner import (  # noqa: E402
    OHIO_COUNTIES,
    _cache_is_fresh,
    _county_slice,
    build_col_meta,
    election_cols_from_names,
)
from tools.scoring.axis_a_segments import fresh_defector, non_locked_crossers  # noqa: E402

DEFAULT_OUTPUT_DIR = _ROOT / 'local' / 'exports'
EXPORT_SUBDIR = 'Crosser_Targets'

#: The 46 static SWVF columns, verbatim and in file order. The buyer-facing
#: half of the column whitelist; the static half of the official layout.
BUYER_STATIC_COLUMNS: tuple[str, ...] = (
    'SOS_VOTERID', 'COUNTY_ID', 'LAST_NAME', 'FIRST_NAME', 'MIDDLE_NAME', 'SUFFIX', 'DATE_OF_BIRTH',
    'REGISTRATION_DATE', 'VOTER_STATUS', 'PARTY_AFFILIATION', 'RESIDENTIAL_ADDRESS1',
    'RESIDENTIAL_SECONDARY_ADDR', 'RESIDENTIAL_CITY', 'RESIDENTIAL_STATE', 'RESIDENTIAL_ZIP',
    'RESIDENTIAL_ZIP_PLUS4', 'RESIDENTIAL_COUNTRY', 'RESIDENTIAL_POSTALCODE', 'MAILING_ADDRESS1',
    'MAILING_SECONDARY_ADDRESS', 'MAILING_CITY', 'MAILING_STATE', 'MAILING_ZIP', 'MAILING_ZIP_PLUS4',
    'MAILING_COUNTRY', 'MAILING_POSTAL_CODE', 'CAREER_CENTER', 'CITY', 'CITY_SCHOOL_DISTRICT',
    'COUNTY_COURT_DISTRICT', 'CONGRESSIONAL_DISTRICT', 'COURT_OF_APPEALS', 'EDU_SERVICE_CENTER_DISTRICT',
    'EXEMPTED_VILL_SCHOOL_DISTRICT', 'LIBRARY', 'LOCAL_SCHOOL_DISTRICT', 'MUNICIPAL_COURT_DISTRICT',
    'PRECINCT_NAME', 'PRECINCT_CODE', 'STATE_BOARD_OF_EDUCATION', 'STATE_REPRESENTATIVE_DISTRICT',
    'STATE_SENATE_DISTRICT', 'TOWNSHIP', 'VILLAGE', 'WARD', 'COUNTY_NUMBER',
)

#: Columns this script computes and appends at the right of every row.
COMPUTED_COLUMNS: tuple[str, ...] = ('COUNTY_NAME', 'fresh_defector', 'voted_last_even_november_general')

#: Trajectory fields that must never appear in a buyer file (Phase 3 handoff
#: section 7). The whitelist already excludes them; this is the second check.
TIER_LINE_FIELDS: tuple[str, ...] = (
    'switch_count', 'partisan_ballot_sequence', 'ever_crossed',
    'primary_participation_rate', 'regular_primaries_eligible', 'primaries_skipped',
)

#: Metadata keys the source parquet must carry (written by _write_cache_atomic).
REQUIRED_PROVENANCE_KEYS: tuple[str, ...] = ('provenance_snapshot_date', 'affiliation_window_years')


# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────

def _build_logger() -> logging.Logger:
    """Stdout logger in export_unc_targets.py's format. Idempotent: calling it
    twice in one process does not attach a second handler (which would print
    every line twice), and it does not propagate to the root logger."""
    logger = logging.getLogger('crosser_export')
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter('%(asctime)s  %(levelname)-8s  %(message)s',
                                               datefmt='%H:%M:%S'))
        logger.addHandler(handler)
    return logger


# ─────────────────────────────────────────────────────────────────────────────
# Source checks: freshness and provenance
# ─────────────────────────────────────────────────────────────────────────────

def check_source(parquet_path: Path, logger: logging.Logger) -> dict:
    """
    Decide whether `parquet_path` may be exported from, and return its
    provenance as {'provenance_snapshot_date': str, 'affiliation_window_years':
    list[int]}.

    Freshness. When `parquet_path` is the managed cache (pipeline.paths.
    ENRICHED_CACHE), the pipeline's single freshness resolver _cache_is_fresh()
    must report it newer than the raw partitions and the classifier source; a
    stale cache raises RuntimeError rather than being exported or rebuilt. Any
    other path was named explicitly with --parquet: freshness is not judged
    (there is nothing to judge it against) and a WARNING says so.

    Provenance. Affiliation is a function of the snapshot, not of the voter
    (CLAUDE.md section 4), and it decides which cohort, and so which lean
    thresholds, a crosser gets. A file without its snapshot date and affiliation
    window cannot be compared with any other export, so both metadata keys are
    required; a missing key raises RuntimeError.
    """
    if not parquet_path.exists():
        raise FileNotFoundError(f'enriched parquet not found: {parquet_path}')

    if parquet_path.resolve() == ENRICHED_CACHE.resolve():
        if not _cache_is_fresh(logger):
            raise RuntimeError(
                f'enriched cache {ENRICHED_CACHE} is stale (older than the raw partitions or the '
                'classifier source). Rebuild it with the pipeline, then rerun this export. The '
                'export never rebuilds the cache itself.'
            )
    else:
        logger.warning('Source given with --parquet (%s): freshness is not checked for a '
                       'non-managed file.', parquet_path)

    meta = pl.read_parquet_metadata(parquet_path)
    missing = [k for k in REQUIRED_PROVENANCE_KEYS if k not in meta]
    if missing:
        raise RuntimeError(
            f'{parquet_path} has no {", ".join(missing)} metadata. The export needs the snapshot '
            'date and affiliation window the classification was built under; rebuild the '
            'enriched cache with the current pipeline.'
        )
    provenance = {
        'provenance_snapshot_date': meta['provenance_snapshot_date'],
        'affiliation_window_years': json.loads(meta['affiliation_window_years']),
    }
    logger.info('Source snapshot %s, affiliation window %s',
                provenance['provenance_snapshot_date'], provenance['affiliation_window_years'])
    return provenance


# ─────────────────────────────────────────────────────────────────────────────
# Column resolution
# ─────────────────────────────────────────────────────────────────────────────

def resolve_even_november_general(schema_names: list[str], lf: pl.LazyFrame,
                                  logger: logging.Logger) -> str:
    """
    Return the most recent November, even-year GENERAL column, e.g.
    'GENERAL-11/05/2024': the anchor for voted_last_even_november_general.

    NEVER the chronologically latest general without that filter. In the
    2026-07-04 snapshot the latest is GENERAL-11/04/2025, an odd-year municipal
    general; anchoring on it understated general turnout about 8x in the Axis A
    Phase 2 measurement (220,273 vs 1,790,384 statewide).

    Columns are listed from the schema by election_cols_from_names() and typed by
    build_col_meta(), the pipeline's single election-column resolver. Only date
    and type are used here, so build_col_meta is handed a zero-row frame of those
    columns: no voter row is read to choose the anchor. Raises RuntimeError if no
    such column exists.
    """
    election_cols = election_cols_from_names(schema_names)
    meta = build_col_meta(lf.select(election_cols).head(0).collect(), election_cols)
    even_nov = [m for m in meta
                if m.type_code == 'GENERAL' and m.date.month == 11 and m.date.year % 2 == 0]
    if not even_nov:
        raise RuntimeError('no November even-year GENERAL column in the source')
    anchor = max(even_nov, key=lambda m: m.date).col
    logger.info('Even-November general anchor resolved: %s', anchor)
    return anchor


def buyer_columns(schema_names: list[str]) -> list[str]:
    """
    The exact, ordered column list of every CSV this script writes:
    BUYER_STATIC_COLUMNS, then the election-history columns in date order, then
    COMPUTED_COLUMNS.

    Raises RuntimeError if the source lacks a static column (an older or
    differently shaped cache) or if a TIER_LINE_FIELDS name would be written.
    The second check cannot fire with the lists as defined; it exists so a
    future edit that widens the whitelist fails here rather than in a buyer's
    inbox.
    """
    missing = [c for c in BUYER_STATIC_COLUMNS if c not in schema_names]
    if missing:
        raise RuntimeError(f'source is missing static SWVF column(s): {missing}')
    columns = list(BUYER_STATIC_COLUMNS) + election_cols_from_names(schema_names) + list(COMPUTED_COLUMNS)
    leaked = [c for c in columns if c in TIER_LINE_FIELDS]
    if leaked:
        raise RuntimeError(f'tier-line field(s) in the buyer column list: {leaked}')
    return columns


def load_county_name_map(lf: pl.LazyFrame, *, require_all: bool) -> dict[str, str]:
    """
    {COUNTY_NUMBER: filename slug} for every county present in the source, from
    pipeline.voter_data_cleaner.OHIO_COUNTIES (the SWVF has no county-name
    column). Values are whitespace-stripped before lookup. The slug is the
    lowercased name with spaces as underscores ('Van Wert' -> 'van_wert').

    Loud by design. A COUNTY_NUMBER with no OHIO_COUNTIES entry raises
    ValueError: --all-counties prunes every county directory not in this map,
    so a county silently skipped here would also have its previous output
    deleted. With require_all (the managed cache), fewer than
    pipeline.paths.COUNTY_COUNT counties also raises.
    """
    present = (
        lf.select(pl.col('COUNTY_NUMBER').str.strip_chars().unique())
        .collect()
        .get_column('COUNTY_NUMBER')
        .to_list()
    )
    unknown = sorted(str(c) for c in present if c not in OHIO_COUNTIES)
    if unknown:
        raise ValueError(f'COUNTY_NUMBER value(s) with no OHIO_COUNTIES entry: {unknown}')
    if require_all and len(present) != COUNTY_COUNT:
        raise ValueError(f'source holds {len(present)} counties, expected {COUNTY_COUNT}')
    return {c: OHIO_COUNTIES[c].strip().lower().replace(' ', '_') for c in present}


# ─────────────────────────────────────────────────────────────────────────────
# Build one county's rows
# ─────────────────────────────────────────────────────────────────────────────

def build_crosser_export(lf: pl.LazyFrame, general_col: str, county_number: str,
                         columns: list[str]) -> pl.DataFrame:
    """
    One county's non-locked crossers (axis_a_segments.non_locked_crossers, item
    1), with fresh_defector (item 2) and voted_last_even_november_general (item
    5's test) computed, projected to exactly `columns`.

    The county filter is pipeline.voter_data_cleaner._county_slice, the single
    spelling of "rows for one county" (strip, then compare), and COUNTY_NUMBER is
    written stripped. The predicates are imported, never restated here.

    voted_last_even_november_general is not zero_floor_reachable(): that
    function answers a question about voters with no partisan primary ballot,
    and every row here has several. What carries over is its test, "a counted
    ballot in the anchor general" (a non-blank cell). A blank cell means no
    COUNTED ballot, not "did not vote", so False is a floor (EOM Ch. 4 pp.
    4-149/4-150; see the axis_a_segments module docstring).

    Raises AssertionError if the result's columns differ from `columns`, or if
    any row has a missing COUNTY_NUMBER or one other than `county_number`.
    """
    df = (
        _county_slice(lf, county_number)
        .filter(non_locked_crossers())
        .with_columns(
            pl.col('COUNTY_NUMBER').str.strip_chars(),
            fresh_defector().alias('fresh_defector'),
            (pl.col(general_col) != '').fill_null(False).alias('voted_last_even_november_general'),
            pl.lit(OHIO_COUNTIES[county_number]).alias('COUNTY_NAME'),
        )
        .select(columns)
        .collect()
    )
    if df.columns != columns:
        raise AssertionError(f'export columns differ from the whitelist: {df.columns}')
    if df.height:
        counties = df.get_column('COUNTY_NUMBER').unique().to_list()
        if counties != [county_number]:
            raise AssertionError(f'expected COUNTY_NUMBER {county_number!r} on every row, found {counties!r}')
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Writing
# ─────────────────────────────────────────────────────────────────────────────

def county_dirname(county_number: str) -> str:
    """'##_CountyName' directory for one county, e.g. '57_Montgomery'. The one
    place the rule lives; writing and pruning both call it."""
    return f'{county_number}_{OHIO_COUNTIES[county_number].replace(" ", "_")}'


def _write_atomic_text(path: Path, text: str) -> None:
    tmp = path.with_name(path.name + '.tmp')
    tmp.write_text(text, encoding='utf-8')
    tmp.replace(path)


def write_county_csv(df: pl.DataFrame, county_number: str, county_name: str, output_dir: Path,
                     provenance: dict, logger: logging.Logger) -> Path:
    """
    Write one county's CSV and its provenance JSON under
    output_dir/Crosser_Targets/##_CountyName/, each via a .tmp file and
    Path.replace() so a reader never sees a half-written file.

    A county with no qualifying rows still gets a header-only CSV. Skipping it
    would leave an earlier run's file in place, which a reader would take as
    current. Returns the CSV path. Raises AssertionError if a TIER_LINE_FIELDS
    column is about to be written.
    """
    leaked = [c for c in df.columns if c in TIER_LINE_FIELDS]
    if leaked:
        raise AssertionError(f'refusing to write tier-line field(s): {leaked}')

    subdir = output_dir / EXPORT_SUBDIR / county_dirname(county_number)
    subdir.mkdir(parents=True, exist_ok=True)
    out_path = subdir / f'{county_name}_CROSSER_targets.csv'
    tmp_path = out_path.with_name(out_path.name + '.tmp')
    df.write_csv(tmp_path)
    tmp_path.replace(out_path)

    record = {
        **provenance,
        'county_number': county_number,
        'county_name': OHIO_COUNTIES[county_number],
        'rows': df.height,
        'fresh_defector_rows': int(df.get_column('fresh_defector').sum() or 0),
        'voted_last_even_november_general_rows':
            int(df.get_column('voted_last_even_november_general').sum() or 0),
        'columns': df.columns,
        'generated_at_utc': datetime.now(timezone.utc).isoformat(timespec='seconds'),
    }
    _write_atomic_text(out_path.with_name(out_path.stem + '.provenance.json'),
                       json.dumps(record, indent=2))
    if df.height:
        logger.info('Written: %s  (%s rows)', out_path, f'{df.height:,}')
    else:
        logger.warning('No non-locked crossers in county %s; wrote a header-only CSV.', county_name)
    return out_path


def prune_stale_county_dirs(crosser_targets_dir: Path, valid_county_numbers: list[str],
                            logger: logging.Logger) -> int:
    """
    Delete '##_' county directories under crosser_targets_dir whose county
    number is not in valid_county_numbers (the counties present in the source
    this run), logging each one. Anything not shaped '##_...' is never touched.

    The same pattern as jurisdictional_groupings._prune_ghost_family_files(): a
    county that drops out of the source must not leave an old directory that a
    reader would take as current. Only the --all-counties path calls this; a
    --county run has no full set to compare against.
    """
    if not crosser_targets_dir.exists():
        return 0
    removed = 0
    for entry in crosser_targets_dir.iterdir():
        name = entry.name
        if not entry.is_dir() or len(name) < 3 or not name[:2].isdigit() or name[2] != '_':
            continue
        if name[:2] not in valid_county_numbers:
            shutil.rmtree(entry)
            removed += 1
            logger.info('Pruned stale county directory (county not in current set): %s', name)
    return removed


# ─────────────────────────────────────────────────────────────────────────────
# Entrypoint
# ─────────────────────────────────────────────────────────────────────────────

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Export the Axis A non-locked-crosser target list from the enriched voter cache.'
    )
    parser.add_argument('--county', default=None,
                        help='Zero-padded county number (e.g. 57). Required unless --all-counties.')
    parser.add_argument('--county-name', default=None,
                        help='County slug for the output filename (e.g. montgomery). Required with --county.')
    parser.add_argument('--all-counties', action='store_true',
                        help='Export every county in the source, one CSV per county.')
    parser.add_argument('--parquet', default=None,
                        help=f'Enriched parquet to read. Defaults to the managed cache ({ENRICHED_CACHE}).')
    parser.add_argument('--output-dir', default=None,
                        help=f'Root output directory. Defaults to {DEFAULT_OUTPUT_DIR}.')
    args = parser.parse_args(argv)
    if args.all_counties and args.county:
        parser.error('--county and --all-counties are mutually exclusive.')
    if not args.all_counties and not args.county:
        parser.error('One of --county or --all-counties is required.')
    if args.county and not args.county_name:
        parser.error('--county-name is required when --county is given.')
    return args


def run_export(parquet_path: Path, output_dir: Path, logger: logging.Logger, *,
               county: str | None = None, county_name: str | None = None,
               all_counties: bool = False) -> int:
    """Check the source, resolve the anchor and the column whitelist, then write
    one county or all of them. Returns the total number of rows written."""
    provenance = check_source(parquet_path, logger)
    lf = pl.scan_parquet(parquet_path)
    schema_names = lf.collect_schema().names()
    general_col = resolve_even_november_general(schema_names, lf, logger)
    columns = buyer_columns(schema_names)
    provenance = {**provenance, 'source': str(parquet_path),
                  'source_mtime_utc': datetime.fromtimestamp(parquet_path.stat().st_mtime, timezone.utc)
                  .isoformat(timespec='seconds'),
                  'anchor_general_column': general_col}

    if all_counties:
        managed = parquet_path.resolve() == ENRICHED_CACHE.resolve()
        name_map = load_county_name_map(lf, require_all=managed)
        counties = sorted(name_map)
        logger.info('Exporting %d counties.', len(counties))
        total = 0
        for county_number in counties:
            df = build_crosser_export(lf, general_col, county_number, columns)
            write_county_csv(df, county_number, name_map[county_number], output_dir, provenance, logger)
            total += df.height
        pruned = prune_stale_county_dirs(output_dir / EXPORT_SUBDIR, counties, logger)
        if pruned:
            logger.info('Pruned %d stale county director%s.', pruned, 'y' if pruned == 1 else 'ies')
        logger.info('Done. %s crosser rows across %d counties.', f'{total:,}', len(counties))
        return total

    county_number = county.strip().zfill(2)
    if county_number not in OHIO_COUNTIES:
        raise ValueError(f'--county {county!r} is not an Ohio county number')
    df = build_crosser_export(lf, general_col, county_number, columns)
    write_county_csv(df, county_number, county_name.strip().lower(), output_dir, provenance, logger)
    logger.info('Done. %s crosser rows for county %s.', f'{df.height:,}', county_number)
    return df.height


def main(argv: list[str] | None = None) -> None:
    logger = _build_logger()
    args = parse_args(argv)
    parquet_path = Path(args.parquet) if args.parquet else ENRICHED_CACHE
    output_dir = Path(args.output_dir) if args.output_dir else DEFAULT_OUTPUT_DIR
    logger.info('Axis A Crosser Export (DOO-7)  source=%s  output=%s', parquet_path, output_dir)
    run_export(parquet_path, output_dir, logger, county=args.county,
               county_name=args.county_name, all_counties=args.all_counties)


if __name__ == '__main__':
    main()
