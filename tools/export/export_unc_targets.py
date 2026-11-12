"""
export_unc_targets.py
═════════════════════
Standalone prototype script — no edits to existing scripts, no GitHub push.

Reads one or more SWVF_*.txt flat files, filters to a single county, then
emits six county-level CSV target lists covering both the unaffiliated (UNC)
cohort classified by lifetime primary ballot history and the currently
affiliated D and R cohorts:

    UNC_Exports/Lifetime_D/{county}_LIFETIME_D_targets.csv
    UNC_Exports/Lifetime_R/{county}_LIFETIME_R_targets.csv
    UNC_Exports/Mixed/{county}_MIXED_targets.csv
    UNC_Exports/No_History/{county}_NO_HISTORY_targets.csv
    UNC_Exports/Current_D/{county}_CURRENT_D_targets.csv
    UNC_Exports/Current_R/{county}_CURRENT_R_targets.csv

For UNC outputs, four computed columns are appended on the right:
total_primaries, d_primaries, r_primaries, unc_class. The Current_D /
Current_R outputs preserve all source columns verbatim and append the same
three count columns (total_primaries, d_primaries, r_primaries) so downstream
scoring tools can compute consistent features across all six cohorts.

Security: UNC_Exports/ must be listed in .gitignore before running.
PII is never written outside of UNC_Exports/.

Usage
─────
    python export_unc_targets.py \\
        --county 57 \\
        --county-name montgomery \\
        --input "local/source/State Voter Files/SWVF_45_66.txt" \\
        [--input "local/source/State Voter Files/SWVF_1_22.txt" ...] \\
        [--output-dir "local/exports"]

    --county        Zero-padded county number (e.g. 57 for Montgomery).
    --county-name   Slug used in output filenames (e.g. montgomery).
    --input         Path(s) to SWVF_*.txt file(s). Repeat flag for multiple files.
                    Only the file(s) containing your county need to be listed,
                    but listing all four is safe — the county filter is applied
                    at scan time so non-matching rows are never loaded.
    --output-dir    Root output directory. Defaults to ./UNC_Exports relative
                    to this script's location.
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
    logger = logging.getLogger('unc_export')
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter('%(asctime)s  %(levelname)-8s  %(message)s',
                                           datefmt='%H:%M:%S'))
    logger.addHandler(handler)
    return logger


# ─────────────────────────────────────────────────────────────────────────────
# Load
# ─────────────────────────────────────────────────────────────────────────────

def load_county(txt_files: list[Path], county_number: str,
                logger: logging.Logger) -> pl.DataFrame:
    """
    Scan one or more SWVF_*.txt files and return only rows matching
    county_number. All columns are loaded as plain strings (infer_schema=False)
    to prevent Polars from miscasting election participation values.
    """
    frames: list[pl.LazyFrame] = []

    for path in txt_files:
        if not path.exists():
            logger.error('File not found: %s', path)
            sys.exit(1)

        logger.debug('Scanning %s', path.name)

        lf = pl.scan_csv(
            path,
            separator=',',
            quote_char='"',
            infer_schema=False,       # keep all cols as Utf8
            encoding='utf8-lossy',    # handle Windows-1252 chars in voter names
            ignore_errors=True,       # skip malformed rows, do not halt
        )

        # Push county filter into the scan — only matching rows are decoded
        lf = lf.filter(
            pl.col('COUNTY_NUMBER').str.strip_chars() == county_number.strip()
        )

        frames.append(lf)

    if not frames:
        logger.error('No input files provided.')
        sys.exit(1)

    logger.info('Collecting %d file(s) filtered to county %s ...', len(frames), county_number)
    df = pl.concat(frames).collect()
    logger.info('Loaded: %s rows × %d columns', f'{len(df):,}', len(df.columns))
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Classify
# ─────────────────────────────────────────────────────────────────────────────

def identify_primary_cols(columns: list[str]) -> list[str]:
    """Return column names matching the SWVF primary election format: PRIMARY-MM/DD/YYYY."""
    return [c for c in columns if c.startswith('PRIMARY-')]


def classify_unc(df: pl.DataFrame, primary_cols: list[str],
                 logger: logging.Logger) -> pl.DataFrame:
    """
    Filter to UNC voters (PARTY_AFFILIATION == '') and classify by lifetime
    primary ballot history using Polars sum_horizontal (single vectorised pass).

    SWVF primary participation values:
        'D'  — Democrat party primary ballot
        'R'  — Republican party primary ballot
        'X'  — non-partisan primary (judicial, local issues)
        'L'/'G'/'C'/'S' — minor party ballots
        ''   — did not participate

    Classification logic:
        LIFETIME_D  : at least one D primary, zero R primaries
        LIFETIME_R  : at least one R primary, zero D primaries
        MIXED       : both D and R primaries in history
        NO_HISTORY  : no primary participation at all

    Returns the full original DataFrame (all source columns) with four
    computed columns appended. All four classes are returned.
    """
    party_col = 'PARTY_AFFILIATION' if 'PARTY_AFFILIATION' in df.columns else 'PARTYAFFIL'

    # Isolate UNCs only before building the wide expression list
    unc = df.filter(pl.col(party_col).str.strip_chars() == '')
    logger.info('UNC voters identified: %s of %s total', f'{len(unc):,}', f'{len(df):,}')

    if not primary_cols:
        logger.warning('No PRIMARY-* columns found — cannot classify. Exiting.')
        sys.exit(1)

    logger.info('Primary election columns found: %d', len(primary_cols))

    # Build one expression per primary column for D, R, and any participation.
    # sum_horizontal collapses each set across all columns in one vectorised pass.
    d_exprs:   list[pl.Expr] = []
    r_exprs:   list[pl.Expr] = []
    any_exprs: list[pl.Expr] = []

    for col in primary_cols:
        val = pl.col(col).str.strip_chars()
        d_exprs.append(  (val == 'D').cast(pl.Int32))
        r_exprs.append(  (val == 'R').cast(pl.Int32))
        any_exprs.append((val != '').cast(pl.Int32))

    unc = unc.with_columns([
        pl.sum_horizontal(d_exprs).alias('d_primaries'),
        pl.sum_horizontal(r_exprs).alias('r_primaries'),
        pl.sum_horizontal(any_exprs).alias('total_primaries'),
    ])

    # Single when/then chain — one columnar expression, not a Python loop
    unc = unc.with_columns(
        pl.when(pl.col('total_primaries') == 0)
          .then(pl.lit('NO_HISTORY'))
          .when((pl.col('d_primaries') > 0) & (pl.col('r_primaries') == 0))
          .then(pl.lit('LIFETIME_D'))
          .when((pl.col('r_primaries') > 0) & (pl.col('d_primaries') == 0))
          .then(pl.lit('LIFETIME_R'))
          .otherwise(pl.lit('MIXED'))
          .alias('unc_class')
    )

    for label in ['LIFETIME_D', 'LIFETIME_R', 'MIXED', 'NO_HISTORY']:
        count = len(unc.filter(pl.col('unc_class') == label))
        logger.info('  %-12s : %s', label, f'{count:,}')

    return unc


# ─────────────────────────────────────────────────────────────────────────────
# Export
# ─────────────────────────────────────────────────────────────────────────────

def export_csvs(targets: pl.DataFrame, county_name: str,
                output_dir: Path, logger: logging.Logger) -> None:
    """
    Split UNC targets by unc_class and write one CSV per class into its
    respective subdirectory. All source columns are written verbatim;
    computed columns (total_primaries, d_primaries, r_primaries, unc_class)
    are appended at the right.
    """
    subdir_map = {
        'LIFETIME_D': output_dir / 'Lifetime_D',
        'LIFETIME_R': output_dir / 'Lifetime_R',
        'MIXED':      output_dir / 'Mixed',
        'NO_HISTORY': output_dir / 'No_History',
    }

    for class_name, subdir in subdir_map.items():
        subdir.mkdir(parents=True, exist_ok=True)

        subset = targets.filter(pl.col('unc_class') == class_name)

        if subset.is_empty():
            logger.warning('No %s voters found for county %s — skipping.', class_name, county_name)
            continue

        filename = f'{county_name}_{class_name}_targets.csv'
        out_path = subdir / filename

        subset.write_csv(out_path)
        logger.info('Written: %s  (%s rows)', out_path, f'{len(subset):,}')


def export_current_affiliated(df: pl.DataFrame, primary_cols: list[str],
                               county_name: str, output_dir: Path,
                               logger: logging.Logger) -> None:
    """
    Emit two CSVs covering currently affiliated D and R voters, regardless of
    primary participation history. PARTY_AFFILIATION values 'D' and 'R' are
    behaviour-derived (set when the voter most recently pulled a partisan
    primary ballot within the prior two calendar years per Ohio EOM Ch.15).

    Adds the same three count columns used in UNC outputs so downstream
    scoring can derive consistent features across cohorts.
    """
    party_col = 'PARTY_AFFILIATION' if 'PARTY_AFFILIATION' in df.columns else 'PARTYAFFIL'

    d_exprs = []
    r_exprs = []
    any_exprs = []
    for col in primary_cols:
        val = pl.col(col).str.strip_chars()
        d_exprs.append((val == 'D').cast(pl.Int32))
        r_exprs.append((val == 'R').cast(pl.Int32))
        any_exprs.append((val != '').cast(pl.Int32))

    enriched = df.with_columns([
        pl.sum_horizontal(d_exprs).alias('d_primaries') if d_exprs else pl.lit(0).alias('d_primaries'),
        pl.sum_horizontal(r_exprs).alias('r_primaries') if r_exprs else pl.lit(0).alias('r_primaries'),
        pl.sum_horizontal(any_exprs).alias('total_primaries') if any_exprs else pl.lit(0).alias('total_primaries'),
    ])

    cohort_map = {
        'CURRENT_D': ('D', output_dir / 'Current_D'),
        'CURRENT_R': ('R', output_dir / 'Current_R'),
    }

    for class_name, (party_value, subdir) in cohort_map.items():
        subdir.mkdir(parents=True, exist_ok=True)
        subset = enriched.filter(
            pl.col(party_col).str.strip_chars() == party_value
        )
        if subset.is_empty():
            logger.warning('No %s voters found for county %s -- skipping.', class_name, county_name)
            continue

        filename = f'{county_name}_{class_name}_targets.csv'
        out_path = subdir / filename
        subset.write_csv(out_path)
        logger.info('Written: %s  (%s rows)', out_path, f'{len(subset):,}')


# ---------------------------------------------------------------------------
# Universal-cohort exports (Pure_D/R + D/R_Crossover + All_Affiliated_D/R)
# ---------------------------------------------------------------------------

def export_cohort_targets(df: pl.DataFrame, primary_cols: list[str],
                           county_name: str, output_dir: Path,
                           logger: logging.Logger) -> None:
    """
    Write per-cohort CSVs for currently-affiliated D and R voters using the
    universal classifier defined in pipeline/voter_data_cleaner.py.

    Outputs (one CSV per cohort, full source columns + cohort scoring fields):
        All_Affiliated_D/{county}_ALL_AFFILIATED_D_targets.csv
        Pure_D/{county}_PURE_D_targets.csv
        D_Crossover/{county}_D_CROSSOVER_targets.csv
            (sorted by lean_score descending - most D-leaning first)
        ... mirror for R ...
    """
    # Import lazily so this script remains usable even if v2 is unavailable.
    import sys as _sys
    _root = Path(__file__).resolve().parent.parent.parent
    if str(_root) not in _sys.path:
        _sys.path.insert(0, str(_root))
    from pipeline import voter_data_cleaner as v2

    classified = v2.classify_all_voters_primary_history(
        df, primary_cols, logger,
        reference_date=v2.get_snapshot_date(logger),
    )

    # Join cohort fields onto df
    enriched = df.join(
        classified.select([
            'SOS_VOTERID',
            'd_primaries', 'r_primaries', 'x_primaries',
            'total_primaries', 'partisan_primaries',
            'lean_score', 'confidence', 'recent_5yr_lean',
            'last_three_party', 'years_since_last_partisan',
            'switch_count',
            'cohort', 'cohort_family', 'crossover_class',
            'is_new_registrant',
        ]),
        on='SOS_VOTERID',
        how='left',
    )

    # All_Affiliated_D / All_Affiliated_R = PURE + CROSSOVER family
    # Pure_D / Pure_R                     = PURE only
    # D_Crossover / R_Crossover           = CROSSOVER only (sorted by lean_score)
    cohort_specs = [
        ('ALL_AFFILIATED_D', output_dir / 'All_Affiliated_D',
         pl.col('cohort_family').is_in(['PURE_D', 'CROSSOVER_D']),
         None),
        ('ALL_AFFILIATED_R', output_dir / 'All_Affiliated_R',
         pl.col('cohort_family').is_in(['PURE_R', 'CROSSOVER_R']),
         None),
        ('PURE_D', output_dir / 'Pure_D',
         pl.col('cohort_family') == 'PURE_D',
         None),
        ('PURE_R', output_dir / 'Pure_R',
         pl.col('cohort_family') == 'PURE_R',
         None),
        ('D_CROSSOVER', output_dir / 'D_Crossover',
         pl.col('cohort_family') == 'CROSSOVER_D',
         ('lean_score', True)),    # descending == most D-leaning at top
        ('R_CROSSOVER', output_dir / 'R_Crossover',
         pl.col('cohort_family') == 'CROSSOVER_R',
         ('lean_score', False)),   # ascending == most R-leaning at top
    ]

    for cohort_name, subdir, predicate, sort_spec in cohort_specs:
        subdir.mkdir(parents=True, exist_ok=True)
        subset = enriched.filter(predicate)
        if subset.is_empty():
            logger.warning('No %s voters for %s -- skipping.', cohort_name, county_name)
            continue
        if sort_spec is not None:
            sort_col, desc = sort_spec
            subset = subset.sort(sort_col, descending=desc, nulls_last=True)
        out_path = subdir / f'{county_name}_{cohort_name}_targets.csv'
        subset.write_csv(out_path)
        logger.info('Written: %s  (%s rows)', out_path, f'{len(subset):,}')


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Export UNC and Current D/R target lists from Ohio SWVF data.'
    )
    parser.add_argument(
        '--input', dest='inputs', action='append', required=True, metavar='FILE',
        help='Path to SWVF_*.txt file. Repeat for multiple files.',
    )
    parser.add_argument(
        '--county', required=True,
        help='Zero-padded county number (e.g. 57 for Montgomery).',
    )
    parser.add_argument(
        '--county-name', required=True,
        help='County slug used in output filenames (e.g. montgomery).',
    )
    parser.add_argument(
        '--output-dir', default=None,
        help='Root output directory. Defaults to UNC_Exports/ next to this script.',
    )
    return parser.parse_args()


def main() -> None:
    logger = _build_logger()
    args   = parse_args()

    script_dir = Path(__file__).resolve().parent.parent.parent
    output_dir = Path(args.output_dir) if args.output_dir else script_dir / 'local/exports'

    txt_files    = [Path(p) for p in args.inputs]
    county_num   = args.county.strip().zfill(2)
    county_name  = args.county_name.strip().lower()

    logger.info('=' * 60)
    logger.info('UNC + Current D/R Cohort Export')
    logger.info('County : %s (%s)', county_name, county_num)
    logger.info('Output : %s', output_dir)
    logger.info('=' * 60)

    # 1. Load
    df = load_county(txt_files, county_num, logger)

    # 2. Identify primary columns from the actual file headers
    primary_cols = identify_primary_cols(df.columns)

    # 3. Classify UNCs
    targets = classify_unc(df, primary_cols, logger)

    # 4. Export UNC cohorts
    export_csvs(targets, county_name, output_dir, logger)

    # 5. Export full cohort taxonomy (Pure_D/R, D/R_Crossover, All_Affiliated_D/R)
    export_cohort_targets(df, primary_cols, county_name, output_dir, logger)

    logger.info('Done.')


if __name__ == '__main__':
    main()
