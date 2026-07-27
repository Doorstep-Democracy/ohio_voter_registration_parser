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

Output layout: one '##_CountyName' subdirectory per county (matching the
convention this repo already uses elsewhere for per-county file
organization, e.g. local/source/Geo/57_Montgomery/), each holding that
county's CSV:

    local/exports/Crosser_Targets/
        57_Montgomery/
            montgomery_CROSSER_targets.csv
        25_Franklin/
            franklin_CROSSER_targets.csv
        ...

Every row also carries an explicit COUNTY_NUMBER and COUNTY_NAME column
(the latter added by this script, since the raw SWVF has no county-name
column at all) -- county identity is never something a reader of these
files has to infer, whether from the row or the folder it's in. See
build_crosser_export()'s docstring for the guarantee this enforces.

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

def build_crosser_export(lf: pl.LazyFrame, general_col: str, county_number: str,
                         logger: logging.Logger) -> pl.DataFrame:
    """
    Filter the enriched-voter LazyFrame down to non-locked crossers, attach
    the two computed columns this export sells (fresh_defector and
    voted_last_even_november_general), and GUARANTEE every row carries an
    explicit, human-readable county identifier.

    WHY THIS FUNCTION COLLECTS AND ASSERTS, RATHER THAN RETURNING A LAZYFRAME
    ----------------------------------------------------------------------
    Every row of every exported CSV must be identifiable by county on sight
    -- these files are opened directly by a human (a buyer, or in public
    review), not joined against other tables where a missing key would be
    someone else's problem to notice. COUNTY_NUMBER survives into every row
    today only because it happens to be one of the SWVF source columns this
    script preserves verbatim -- true by construction, but never checked as
    a real guarantee. This function makes it a checked guarantee: it
    collects here (once, per county, which is already required before
    write_county_csv can inspect df.is_empty()) and asserts COUNTY_NUMBER is
    present, has zero nulls, and -- since this function is always called
    with rows already filtered to one county -- has exactly one distinct
    value, matching the county_number the caller asked for. Any violation
    raises immediately (CLAUDE.md section 5's loud-failures rule); a export
    tool that silently shipped a row with no county attached would be a
    worse failure than a crashed run.

    COUNTY_NAME is added as its own explicit column (via OHIO_COUNTIES,
    the same single resolver load_county_name_map() already uses) so a
    human opening the CSV cold sees the county by name, not just by the
    SWVF's bare numeric code -- the raw voter file has no county-name
    column at all.

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
    from pipeline.voter_data_cleaner import OHIO_COUNTIES
    from tools.scoring.axis_a_segments import fresh_defector, non_locked_crossers

    county_display_name = OHIO_COUNTIES.get(county_number)
    if county_display_name is None:
        raise ValueError(
            f'COUNTY_NUMBER {county_number!r} has no OHIO_COUNTIES entry -- '
            'cannot attach a county name to this export.'
        )

    df = (
        lf.filter(non_locked_crossers())
        .with_columns([
            fresh_defector().alias('fresh_defector'),
            (pl.col(general_col) != '').alias('voted_last_even_november_general'),
            pl.lit(county_display_name).alias('COUNTY_NAME'),
        ])
        .collect()
    )

    if df.is_empty():
        # Nothing to assert against -- an empty county is handled by the
        # caller (write_county_csv logs and skips), not an error here.
        return df

    if 'COUNTY_NUMBER' not in df.columns:
        raise AssertionError(
            'COUNTY_NUMBER missing from export columns -- the county '
            'identifier guarantee this script exists to uphold has broken.'
        )
    null_count = df.get_column('COUNTY_NUMBER').null_count()
    if null_count:
        raise AssertionError(
            f'{null_count} row(s) have a null COUNTY_NUMBER -- every row '
            'in this export must carry an explicit county identifier.'
        )
    distinct = df.get_column('COUNTY_NUMBER').unique().to_list()
    if distinct != [county_number]:
        raise AssertionError(
            f'Expected every row to carry COUNTY_NUMBER == {county_number!r}, '
            f'found {distinct!r} -- the county filter upstream is not doing '
            'what this function assumes.'
        )

    return df


# ─────────────────────────────────────────────────────────────────────────────
# Write one county's CSV
# ─────────────────────────────────────────────────────────────────────────────

def county_dirname(county_number: str) -> str:
    """
    Build the '##_CountyName' directory name this export uses for one county,
    e.g. county_dirname('57') -> '57_Montgomery'.

    This is the single place that naming rule lives -- write_county_csv() and
    _prune_stale_county_dirs() both call it, so the two can never disagree
    about what a valid directory name looks like (the same single-resolver
    principle CLAUDE.md section 5 already requires for jurisdiction logic).

    THE CONVENTION, AND WHY IT MATTERS HERE
    ----------------------------------------
    Matches the shape this repo already uses for per-county file
    organization elsewhere -- e.g. local/source/Geo/57_Montgomery/ (zero-
    padded county number, underscore, county name in ITS NATURAL CASE, not
    lowercased or slugified). Adopted here specifically so a human doing
    public/buyer review can identify a county from the folder structure
    alone, without opening a file -- reinforcing, at the file-organization
    level, the same guarantee build_crosser_export() enforces at the row
    level: county identity is never something a reader has to infer.
    """
    from pipeline.voter_data_cleaner import OHIO_COUNTIES

    county_display_name = OHIO_COUNTIES.get(county_number)
    if county_display_name is None:
        raise ValueError(f'COUNTY_NUMBER {county_number!r} has no OHIO_COUNTIES entry.')
    return f'{county_number}_{county_display_name.replace(" ", "_")}'


def write_county_csv(df: pl.DataFrame, county_number: str, county_name: str,
                     output_dir: Path, logger: logging.Logger) -> None:
    """
    Write one county's crosser rows to
    local/exports/Crosser_Targets/{##_CountyName}/{county_name}_CROSSER_targets.csv
    -- a '##_CountyName' subdirectory per county (see county_dirname()),
    containing the CSV named with the existing lowercase-slug convention
    export_unc_targets.py already uses (e.g. 'montgomery_CROSSER_targets.csv'
    inside '57_Montgomery/'). Only the directory layer is new; the filename
    convention inside it is unchanged from the original build.

    Writes are atomic: the CSV is written to a sibling .tmp path first, then
    swapped into place with Path.replace(), so a reader can never observe a
    half-written file even if this process is interrupted mid-write
    (CLAUDE.md section 5's atomic-write rule). export_unc_targets.py itself
    predates that rule and writes directly; this script does not repeat that
    gap.
    """
    subdir = output_dir / 'Crosser_Targets' / county_dirname(county_number)
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
# Stale-directory pruning
# ─────────────────────────────────────────────────────────────────────────────

def prune_stale_county_dirs(crosser_targets_dir: Path, valid_county_numbers: list[str],
                            logger: logging.Logger) -> int:
    """
    Delete '##_CountyName' subdirectories under crosser_targets_dir whose
    county number is not in valid_county_numbers.

    WHY THIS EXISTS
    -----------------
    This export only ever WRITES; nothing before this function ever deleted
    an old directory. If a future run has fewer qualifying counties than a
    prior run -- e.g. a classifier change shrinks some county's non-locked-
    crosser population, or a county's OHIO_COUNTIES name spelling changes,
    changing county_dirname()'s output -- the old directory for that county
    would stay on disk forever: orphaned, and silently inconsistent with
    what a buyer or public reviewer would assume is the current export.

    This is the exact bug class pipeline/jurisdictional_groupings.py's
    _prune_ghost_family_files() was built to prevent (2026-07-10, 2,465
    stale files from a slug-format change that export logic never cleaned
    up) -- this function is that same pattern, applied to this script's own
    output directory instead. Modeled on it directly: scope to a known file
    family (here, '##_CountyName' directories directly under
    crosser_targets_dir, nothing else), compute the current valid set from
    LIVE data (valid_county_numbers is the set of counties actually present
    in the parquet right now, not which counties happened to produce
    non-empty output this run -- a county with zero crossers this run is
    still a valid, current county, and its momentarily-missing directory is
    expected write_county_csv() is_empty() behavior, not staleness), and log
    every deletion individually rather than deleting silently.

    ONLY CALL THIS FROM THE --all-counties PATH
    ----------------------------------------------
    A single-county (--county) run has no "current full set" to compare
    against -- calling this after a single-county run would delete every
    OTHER county's directory, which is never what a --county invocation
    means. main() enforces this by only calling prune_stale_county_dirs()
    inside the --all-counties branch.
    """
    if not crosser_targets_dir.exists():
        return 0

    removed = 0
    for entry in crosser_targets_dir.iterdir():
        if not entry.is_dir():
            continue
        # A '##_CountyName' directory name always starts with a 2-digit
        # county number followed by an underscore -- anything else under
        # this root is not part of this export's file family and must never
        # be touched (e.g. an unrelated directory a future script adds).
        prefix = entry.name[:3]
        if len(prefix) != 3 or not prefix[:2].isdigit() or prefix[2] != '_':
            continue
        county_number = entry.name[:2]
        if county_number not in valid_county_numbers:
            import shutil
            shutil.rmtree(entry)
            removed += 1
            logger.info('Pruned stale county directory (county not in current set): %s',
                       entry.name)

    return removed


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
            export_df = build_crosser_export(county_lf, general_col, county_number, logger)
            write_county_csv(export_df, county_number, county_name, output_dir, logger)
            total_rows += len(export_df)

        # Prune AFTER all counties are written, using the CURRENT full set
        # (`counties`, resolved above from live parquet data) as the valid
        # set -- never from which counties happened to write non-empty
        # output this run. Only reachable from --all-counties: see
        # prune_stale_county_dirs()'s docstring for why a --county run must
        # never call this.
        crosser_targets_dir = output_dir / 'Crosser_Targets'
        pruned = prune_stale_county_dirs(crosser_targets_dir, counties, logger)
        if pruned:
            logger.info('Pruned %d stale county director%s.',
                       pruned, 'y' if pruned == 1 else 'ies')

        logger.info('Done. %s total crosser rows across %d counties.',
                     f'{total_rows:,}', len(counties))
    else:
        county_number = args.county.strip().zfill(2)
        county_name = args.county_name.strip().lower()
        county_lf = lf.filter(pl.col('COUNTY_NUMBER') == county_number)
        export_df = build_crosser_export(county_lf, general_col, county_number, logger)
        write_county_csv(export_df, county_number, county_name, output_dir, logger)
        logger.info('Done. %s crosser rows for county %s.',
                     f'{len(export_df):,}', county_name)


if __name__ == '__main__':
    main()
