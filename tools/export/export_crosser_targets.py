"""
export_crosser_targets.py
==========================
Axis A Phase 3 (Linear DOO-7) — subscriber export v1, the "crosser" export.

Produces one CSV per county (or all 88, with --all-counties) listing every
"non-locked crosser" in that county: a voter who has voted in both parties'
primaries at different times and whose recent ballot history does not show
them settled back into one party. This is the top-ranked, highest-priority
product on the Axis A subscriber-tier stack rank
(local/context/scope/trajectory_axis_a_stack_rank.md, item 1) — county
parties and campaigns buy this list to target persuadable voters rather than
straight turnout/base voters.

Every row also carries two extra columns beyond the voter's normal SWVF data:

    fresh_defector   - did this voter's MOST RECENT primary ballot happen
                        to be the party-switch itself, within the last two
                        years? A recency signal: freshly-switched voters
                        likely need a different script/contact strategy than
                        someone who crossed parties a decade ago.

    voted_last_even_november_general
                      - did this voter cast a counted ballot in the most
                        recent big (November, even-year) general election
                        (e.g. a presidential or gubernatorial year)? A
                        general-election turnout hygiene flag, riding along
                        on this export exactly as the stack rank's item 5
                        write-up describes ("indirectly every buyer of items
                        1-4, whose lists gain a reachable/inert flag").

Unlike tools/export/export_unc_targets.py (which reads the raw SWVF_*.txt
flat files and classifies voters itself), this script reads the ENRICHED
PARQUET (local/source/parquet_enriched/enriched_voters.parquet). The crosser
classification (ever_crossed, crossover_class, partisan_ballot_sequence, etc.)
is produced once by the pipeline's classify_all_voters_primary_history() and
lives only in that parquet -- re-deriving it here from raw text would be a
second, divergent copy of logic that already has exactly one home
(tools/scoring/axis_a_segments.py), which this script imports rather than
reimplements.

Security: local/exports/ is gitignored (see CLAUDE.md section 9). Voter rows
are public record, but bulk export files stay local per existing convention,
same as export_unc_targets.py's own UNC_Exports/ output.

Usage
-----
    Single county:
        python export_crosser_targets.py --county 57 --county-name montgomery

    All 88 counties in one run:
        python export_crosser_targets.py --all-counties

    --county            Zero-padded county number (e.g. 57 for Montgomery).
                         Required unless --all-counties is given.
    --county-name       Slug used in the output filename (e.g. montgomery).
                         Required with --county; ignored with --all-counties
                         (the county's own name is looked up automatically).
    --all-counties      Export every county found in the parquet, one CSV
                         each. Mutually exclusive with --county.
    --parquet           Path to the enriched parquet. Defaults to
                         local/source/parquet_enriched/enriched_voters.parquet.
    --output-dir        Root output directory. Defaults to local/exports
                         relative to the project root.
"""

import argparse
import logging
import sys
from pathlib import Path

import polars as pl


# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────

def _build_logger() -> logging.Logger:
    """Build a stdout logger matching export_unc_targets.py's format exactly,
    so log output from either export script reads consistently in a terminal
    or a captured run log."""
    logger = logging.getLogger('crosser_export')
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter('%(asctime)s  %(levelname)-8s  %(message)s',
                                           datefmt='%H:%M:%S'))
    logger.addHandler(handler)
    return logger


# ─────────────────────────────────────────────────────────────────────────────
# County-name lookup (for --all-counties, where no --county-name is given)
# ─────────────────────────────────────────────────────────────────────────────

def load_county_name_map(lf: pl.LazyFrame, logger: logging.Logger) -> dict[str, str]:
    """
    Build a {COUNTY_NUMBER: slug} map for every county actually present in
    the parquet's COUNTY_NUMBER column, using
    pipeline.voter_data_cleaner.OHIO_COUNTIES -- the pipeline's own, single
    COUNTY_NUMBER-to-name lookup (an Ohio SOS-sourced dict, not derived from
    voter rows, since the SWVF carries a county NUMBER on every row but no
    county NAME column at all). Reimplementing that 88-entry table here
    instead of importing it would be a second, driftable copy of something
    that already has exactly one home.

    The slug is lowercased with spaces replaced by underscores (e.g.
    "Van Wert" -> "van_wert"), matching the --county-name convention
    export_unc_targets.py already uses (e.g. "montgomery"), so filenames stay
    consistent between both export scripts.
    """
    from pipeline.voter_data_cleaner import OHIO_COUNTIES

    present = (
        lf.select('COUNTY_NUMBER')
        .unique()
        .collect()
        .get_column('COUNTY_NUMBER')
        .to_list()
    )
    name_map = {}
    for county_number in present:
        county_name = OHIO_COUNTIES.get(county_number)
        if county_name is None:
            logger.warning('COUNTY_NUMBER %r in parquet has no OHIO_COUNTIES entry -- skipping.',
                           county_number)
            continue
        name_map[county_number] = county_name.strip().lower().replace(' ', '_')
    logger.info('Resolved county-name map for %d counties.', len(name_map))
    return name_map


# ─────────────────────────────────────────────────────────────────────────────
# Election-column resolution (even-November general anchor)
# ─────────────────────────────────────────────────────────────────────────────

def resolve_even_november_general(lf: pl.LazyFrame, logger: logging.Logger) -> str:
    """
    Find the most recent November, even-year GENERAL election column, e.g.
    'GENERAL-11/05/2024'. This is the column used to test whether a voter
    showed up for a big general election -- the kind of election most likely
    to have a presidential or major statewide race on the ballot, and
    therefore the highest-turnout, most-representative test of "does this
    voter still show up at all."

    THE ONE THING THIS FUNCTION MUST NEVER DO: pick the chronologically
    LATEST general column without filtering for November/even-year first.
    The Axis A Phase 2 validation (local/context/scope/axis_a_segment_sizes.md
    section 4) measured that the latest column in the 2026-07-04 snapshot is
    GENERAL-11/04/2025, a low-turnout ODD-YEAR municipal general -- anchoring
    on it understates general-election turnout by roughly 8x (220,273 vs
    1,790,384 statewide) because most voters simply don't turn out for sleepy
    local elections. Always filter to month == 11 and year % 2 == 0, then
    take the most recent of THOSE, exactly as
    local/patches/verify_axis_a_segments.py does.

    Election columns are never hardcoded by name or date anywhere in this
    project (CLAUDE.md section 4/5, the single-resolver law) because the set
    of election columns grows with every new election added to the state
    voter file. This function instead calls identify_election_cols() and
    build_col_meta() -- the pipeline's own resolvers -- on a bounded sample
    of rows, and lets those resolvers tell it which columns exist and what
    each one means.
    """
    from pipeline.voter_data_cleaner import build_col_meta, identify_election_cols

    schema_probe = lf.head(1).collect()
    election_cols = identify_election_cols(schema_probe)
    # build_col_meta profiles the actual ballot codes in each column, so it
    # needs real rows -- a bounded sample is sufficient, not the full 7.9M-row
    # frame (CLAUDE.md's bounded-read rule for exploration, not the export
    # itself, which does need full per-row data -- see the handoff section 2).
    sample = lf.select(election_cols).head(200_000).collect()
    meta = build_col_meta(sample, election_cols)

    even_nov_generals = [
        m for m in meta
        if m.type_code == 'GENERAL' and m.date.month == 11 and m.date.year % 2 == 0
    ]
    if not even_nov_generals:
        logger.error('No November even-year GENERAL columns resolved from the parquet.')
        sys.exit(1)

    anchor = even_nov_generals[-1].col
    logger.info('Even-November general anchor resolved: %s', anchor)
    return anchor


# ─────────────────────────────────────────────────────────────────────────────
# Build the crosser export for one county's rows
# ─────────────────────────────────────────────────────────────────────────────

def build_crosser_export(lf: pl.LazyFrame, general_col: str) -> pl.LazyFrame:
    """
    Filter the enriched-voter LazyFrame down to non-locked crossers and
    attach the two computed columns this export sells: fresh_defector and
    voted_last_even_november_general.

    WHY THE PREDICATES COME FROM axis_a_segments.py, NOT FROM HERE
    ----------------------------------------------------------------
    non_locked_crossers() and fresh_defector() are imported from
    tools.scoring.axis_a_segments, the single resolver for every Axis A
    segment definition (CLAUDE.md section 5). Re-writing either predicate's
    logic inline in this export script -- even if it looked identical today
    -- would create a second copy that could silently drift from the
    original the next time axis_a_segments.py changes. That exact failure
    mode (two copies of "how do you resolve a voter's city," one correct and
    one stale) went undetected for weeks earlier in this project and
    mislabeled 56% of precinct pairs -- this project does not repeat that
    mistake for crossing logic.

    WHY voted_last_even_november_general IS NOT zero_floor_reachable()
    ----------------------------------------------------------------
    axis_a_segments.py also defines zero_floor_reachable() /
    zero_floor_inert(), but those two functions are written for a
    COMPLETELY DIFFERENT population: voters with ZERO partisan primary
    ballots ever. Every voter in this export is a crosser, and crossing
    requires having cast primary ballots -- so calling zero_floor_reachable()
    on this export's rows would always evaluate to False, not because the
    answer is meaningfully "no" but because the underlying question (are you
    a zero-primary voter who still shows up to generals?) doesn't apply to
    anyone here.

    What this export reuses instead is the TEST behind that function, not
    the function itself: "did this voter cast a counted ballot in the most
    recent even-November general?" That test was proven useful against
    zero-floor voters first (Axis A Phase 2), and the stack rank's own
    write-up for that idea (item 5, "Who pays") says it was always meant to
    travel onto other exports as a bonus hygiene column -- this is that
    column, computed the same way, on a different population, by design.

    Full reasoning: local/context/handoffs/HANDOFF_AXIS_A_PHASE3_EXPORT.md,
    section 4.

    COUNTED-BALLOT RULE
    --------------------
    A blank cell in `general_col` means "no COUNTED ballot on record," not
    "definitely did not vote" -- a rejected provisional or an uncounted
    absentee leaves the same blank as never showing up. Every row this
    export marks voted_last_even_november_general = True is therefore a
    reliable positive; every False is a floor, not a certainty, on that
    voter's true participation. See axis_a_segments.py's module docstring
    for the full statutory citation (EOM Ch. 4, pp. 4-149/4-150).
    """
    from tools.scoring.axis_a_segments import fresh_defector, non_locked_crossers

    return (
        lf.filter(non_locked_crossers())
        .with_columns([
            fresh_defector().alias('fresh_defector'),
            (pl.col(general_col) != '').alias('voted_last_even_november_general'),
        ])
    )


# ─────────────────────────────────────────────────────────────────────────────
# Write one county's CSV
# ─────────────────────────────────────────────────────────────────────────────

def write_county_csv(df: pl.DataFrame, county_name: str, output_dir: Path,
                     logger: logging.Logger) -> None:
    """
    Write one county's crosser rows to
    local/exports/Crosser_Targets/{county_name}_CROSSER_targets.csv, mirroring
    the UNC_Exports/<Class>/{county}_<CLASS>_targets.csv layout
    export_unc_targets.py already uses, so both export families sit under a
    predictable, matching directory shape.

    Writes are atomic: the CSV is written to a sibling .tmp path first, then
    swapped into place with Path.replace(), so a reader can never observe a
    half-written file even if this process is interrupted mid-write
    (CLAUDE.md section 5's atomic-write rule). export_unc_targets.py itself
    predates that rule and writes directly; this script does not repeat that
    gap.
    """
    subdir = output_dir / 'Crosser_Targets'
    subdir.mkdir(parents=True, exist_ok=True)

    if df.is_empty():
        logger.warning('No non-locked crossers found for county %s -- skipping.', county_name)
        return

    out_path = subdir / f'{county_name}_CROSSER_targets.csv'
    tmp_path = out_path.with_suffix('.csv.tmp')

    df.write_csv(tmp_path)
    tmp_path.replace(out_path)

    logger.info('Written: %s  (%s rows)', out_path, f'{len(df):,}')


# ─────────────────────────────────────────────────────────────────────────────
# Entrypoint
# ─────────────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Export the Axis A non-locked-crosser target list from the enriched voter parquet.'
    )
    parser.add_argument(
        '--county', default=None,
        help='Zero-padded county number (e.g. 57 for Montgomery). Required unless --all-counties.',
    )
    parser.add_argument(
        '--county-name', default=None,
        help='County slug used in the output filename (e.g. montgomery). Required with --county.',
    )
    parser.add_argument(
        '--all-counties', action='store_true',
        help='Export every county found in the parquet, one CSV per county.',
    )
    parser.add_argument(
        '--parquet', default=None,
        help='Path to the enriched parquet. Defaults to '
             'local/source/parquet_enriched/enriched_voters.parquet.',
    )
    parser.add_argument(
        '--output-dir', default=None,
        help='Root output directory. Defaults to local/exports next to the project root.',
    )
    args = parser.parse_args()

    if args.all_counties and args.county:
        parser.error('--county and --all-counties are mutually exclusive.')
    if not args.all_counties and not args.county:
        parser.error('One of --county or --all-counties is required.')
    if args.county and not args.county_name:
        parser.error('--county-name is required when --county is given.')

    return args


def main() -> None:
    logger = _build_logger()
    args = parse_args()

    root = Path(__file__).resolve().parent.parent.parent
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

    parquet_path = Path(args.parquet) if args.parquet else \
        root / 'local/source/parquet_enriched/enriched_voters.parquet'
    output_dir = Path(args.output_dir) if args.output_dir else root / 'local/exports'

    if not parquet_path.exists():
        logger.error('Parquet not found: %s', parquet_path)
        sys.exit(1)

    logger.info('=' * 60)
    logger.info('Axis A Crosser Export (DOO-7)')
    logger.info('Parquet : %s', parquet_path)
    logger.info('Output  : %s', output_dir)
    logger.info('=' * 60)

    lf = pl.scan_parquet(parquet_path)

    general_col = resolve_even_november_general(lf, logger)

    if args.all_counties:
        name_map = load_county_name_map(lf, logger)
        counties = sorted(name_map.keys())
        logger.info('Exporting all %d counties found in parquet.', len(counties))
        total_rows = 0
        for county_number in counties:
            county_name = name_map[county_number]
            county_lf = lf.filter(pl.col('COUNTY_NUMBER') == county_number)
            export_df = build_crosser_export(county_lf, general_col).collect()
            write_county_csv(export_df, county_name, output_dir, logger)
            total_rows += len(export_df)
        logger.info('Done. %s total crosser rows across %d counties.',
                     f'{total_rows:,}', len(counties))
    else:
        county_number = args.county.strip().zfill(2)
        county_name = args.county_name.strip().lower()
        county_lf = lf.filter(pl.col('COUNTY_NUMBER') == county_number)
        export_df = build_crosser_export(county_lf, general_col).collect()
        write_county_csv(export_df, county_name, output_dir, logger)
        logger.info('Done. %s crosser rows for county %s.',
                     f'{len(export_df):,}', county_name)


if __name__ == '__main__':
    main()
