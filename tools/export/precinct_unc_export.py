"""
precinct_unc_export.py
──────────────────────
For every precinct in all 88 Ohio counties, compute:
  - total registered voters
  - affiliated D count
  - UNC Lifetime-D count  (unaffiliated voters whose entire primary history is D ballots)
  - combined D + UNC Lifetime-D count and percentage of precinct total

Output: one CSV per county → UNC_Exports/Precinct_Summary/<county_name>_precinct_d_unc.csv
        one statewide rollup  → UNC_Exports/Precinct_Summary/ohio_precinct_d_unc.csv

Usage:
    python precinct_unc_export.py [--no-parquet] [--county 57]

Options:
    --no-parquet    Force CSV ingestion even if Parquet cache exists
    --county N      Process a single county (zero-padded or plain, e.g. 57 or 057)
"""

import argparse
import sys
import time
from pathlib import Path

import polars as pl

# ── Paths ─────────────────────────────────────────────────────────────────────

BASE_DIR    = Path(__file__).resolve().parent.parent.parent
SOURCE_DIR  = BASE_DIR / "local" / "source"  # PATCH: Rerouted to local/ workspace
TXT_DIR     = SOURCE_DIR / "State Voter Files"
PARQUET_DIR = SOURCE_DIR / "parquet"
OUT_DIR     = BASE_DIR / "local" / "exports" / "Precinct_Summary"  # PATCH: Rerouted to local/ workspace

# ── Import shared helpers from existing pipeline ──────────────────────────────

sys.path.insert(0, str(BASE_DIR))
from pipeline import voter_data_cleaner as v2

OHIO_COUNTIES   = v2.OHIO_COUNTIES          # dict[str, str]  '01' → 'Adams'
setup_logging   = v2.setup_logging
build_parquet   = v2.build_parquet_cache
load_parquet    = v2.load_voter_files_parquet
load_csv        = v2.load_voter_files
clean_data      = v2.clean_voter_data
identify_cols   = v2.identify_election_cols
classify_unc    = v2.classify_unc_primary_history

# ── Core logic ────────────────────────────────────────────────────────────────

def build_precinct_summary(
    df:           pl.DataFrame,
    primary_cols: list[str],
    logger,
) -> pl.DataFrame:
    """
    Return a DataFrame with one row per (COUNTY_NUMBER, PRECINCT_NAME):

        county_number | county_name | precinct_name
        total | affiliated_d | unc_lifetime_d | d_plus_unc | pct_d_plus_unc
    """
    # ── Universal classifier (all voters, not just UNC) ──────────────────
    classify_all = v2.classify_all_voters_primary_history
    classified   = classify_all(df, primary_cols, logger)

    # Join cohort_family + is_new_registrant onto the source df.  Other cohort
    # columns (lean_score, etc.) are not needed for this aggregation.
    df_c = df.join(
        classified.select(['SOS_VOTERID', 'cohort_family', 'is_new_registrant']),
        on='SOS_VOTERID',
        how='left',
    )

    # 1. Total voters per precinct
    totals = (
        df_c
        .group_by(['COUNTY_NUMBER', 'PRECINCT_NAME'])
        .agg(pl.len().alias('total'))
    )

    # 2. Cohort counts per precinct (single grouping pass with conditional sums)
    cohort_counts = (
        df_c
        .group_by(['COUNTY_NUMBER', 'PRECINCT_NAME'])
        .agg([
            (pl.col('cohort_family') == 'PURE_D').sum().cast(pl.Int64).alias('pure_d'),
            (pl.col('cohort_family') == 'CROSSOVER_D').sum().cast(pl.Int64).alias('d_crossover'),
            (pl.col('cohort_family') == 'UNC_LIFETIME_D').sum().cast(pl.Int64).alias('unc_lifetime_d'),
            (pl.col('cohort_family') == 'PURE_R').sum().cast(pl.Int64).alias('pure_r'),
            (pl.col('cohort_family') == 'CROSSOVER_R').sum().cast(pl.Int64).alias('r_crossover'),
            (pl.col('cohort_family') == 'UNC_LIFETIME_R').sum().cast(pl.Int64).alias('unc_lifetime_r'),
            (
                (pl.col('cohort_family').is_in(['PURE_D', 'CROSSOVER_D']))
                & pl.col('is_new_registrant').fill_null(False)
            ).sum().cast(pl.Int64).alias('new_registrants_d'),
            (
                (pl.col('cohort_family').is_in(['PURE_R', 'CROSSOVER_R']))
                & pl.col('is_new_registrant').fill_null(False)
            ).sum().cast(pl.Int64).alias('new_registrants_r'),
        ])
    )

    # 4. Join + derive composite columns
    summary = (
        totals
        .join(cohort_counts, on=['COUNTY_NUMBER', 'PRECINCT_NAME'], how='left')
        .with_columns([
            (pl.col('pure_d') + pl.col('d_crossover')).alias('total_d_affiliated'),
            (pl.col('pure_r') + pl.col('r_crossover')).alias('total_r_affiliated'),
        ])
        .with_columns([
            # Backward-compat aliases
            pl.col('total_d_affiliated').alias('registered_d'),
            pl.col('total_r_affiliated').alias('registered_r'),
            (pl.col('pure_d') + pl.col('unc_lifetime_d')).alias('pure_d_plus_unc_lifetime_d'),
            (pl.col('pure_r') + pl.col('unc_lifetime_r')).alias('pure_r_plus_unc_lifetime_r'),
        ])
        .with_columns([
            # Legacy d_plus_unc = pure_d + unc_lifetime_d (per refactor decision)
            pl.col('pure_d_plus_unc_lifetime_d').alias('d_plus_unc'),
        ])
        .with_columns([
            (
                (pl.col('d_plus_unc') / pl.col('total') * 100)
                .round(1)
                .alias('pct_d_plus_unc')
            )
        ])
        .with_columns(
            pl.col('COUNTY_NUMBER')
              .replace(OHIO_COUNTIES, default='Unknown')
              .alias('county_name')
        )
        .rename({
            'COUNTY_NUMBER': 'county_number',
            'PRECINCT_NAME': 'precinct_name',
        })
        .select([
            'county_number', 'county_name', 'precinct_name',
            'total',
            'pure_d', 'd_crossover', 'total_d_affiliated', 'registered_d',
            'unc_lifetime_d', 'pure_d_plus_unc_lifetime_d',
            'new_registrants_d',
            'pure_r', 'r_crossover', 'total_r_affiliated', 'registered_r',
            'unc_lifetime_r', 'pure_r_plus_unc_lifetime_r',
            'new_registrants_r',
            'd_plus_unc', 'pct_d_plus_unc',
        ])
        .sort(['county_number', 'precinct_name'])
    )

    return summary


def write_county_files(summary: pl.DataFrame, logger) -> list[Path]:
    """Write one CSV per county; return list of written paths."""
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    written = []

    for county_num in sorted(summary['county_number'].unique().to_list()):
        county_name = OHIO_COUNTIES.get(county_num.strip().zfill(2), f'county_{county_num}')
        slug        = county_name.lower().replace(' ', '_')
        out_path    = OUT_DIR / f"{slug}_precinct_d_unc.csv"

        county_df = summary.filter(pl.col('county_number') == county_num)
        county_df.write_csv(out_path)
        written.append(out_path)

    logger.info('Wrote %d county CSV files → %s', len(written), OUT_DIR)
    return written


def write_statewide_file(summary: pl.DataFrame, logger) -> Path:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUT_DIR / "ohio_precinct_d_unc.csv"
    summary.write_csv(out_path)
    logger.info('Wrote statewide rollup → %s', out_path)
    return out_path


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='Export D + UNC Lifetime-D counts per precinct.')
    parser.add_argument('--no-parquet', action='store_true',
                        help='Force CSV ingestion; skip Parquet cache')
    parser.add_argument('--county', type=str, default=None,
                        help='Single county number to process (e.g. 57)')
    args = parser.parse_args()

    logger = setup_logging('precinct_export')
    logger.info('=' * 60)
    logger.info('PRECINCT D + UNC LIFETIME-D EXPORT')
    logger.info('=' * 60)

    use_parquet  = not args.no_parquet
    county_filter = args.county.strip().zfill(2) if args.county else None

    txt_files = sorted(TXT_DIR.glob('SWVF_*.txt'))
    if not txt_files:
        logger.error('No SWVF_*.txt files found in %s', TXT_DIR)
        sys.exit(1)

    t0 = time.perf_counter()

    # ── Load ──────────────────────────────────────────────────────────────────
    if use_parquet:
        build_parquet(txt_files, logger=logger)
        logger.info('Loading from Parquet cache ...')
        df = load_parquet(county_number=county_filter, logger=logger)
    else:
        logger.info('Loading from CSV (--no-parquet) ...')
        df = load_csv(txt_files, logger=logger)
        if county_filter:
            df = df.filter(pl.col('COUNTY_NUMBER').str.strip_chars().str.zfill(2) == county_filter)

    logger.info('Loaded %s rows', f'{len(df):,}')

    # ── Clean + identify election columns ─────────────────────────────────────
    df = clean_data(df, logger)
    election_cols = identify_cols(df)
    primary_cols  = [c for c in election_cols if c.startswith('PRIMARY-')]
    logger.info('%d primary columns identified', len(primary_cols))

    # ── Normalize PARTY_AFFILIATION ───────────────────────────────────────────
    if 'PARTY_AFFILIATION' not in df.columns and 'PARTYAFFIL' in df.columns:
        df = df.rename({'PARTYAFFIL': 'PARTY_AFFILIATION'})

    # ── Build summary ─────────────────────────────────────────────────────────
    logger.info('Building precinct summary ...')
    summary = build_precinct_summary(df, primary_cols, logger)

    total_precincts = len(summary)
    total_d_unc     = summary['d_plus_unc'].sum()
    total_voters    = summary['total'].sum()
    logger.info(
        'Summary: %s precincts | %s D+UNC voters of %s total (%.1f%%)',
        f'{total_precincts:,}',
        f'{total_d_unc:,}',
        f'{total_voters:,}',
        total_d_unc / total_voters * 100 if total_voters else 0,
    )

    # ── Write output ──────────────────────────────────────────────────────────
    if county_filter:
        # Single county — write just that file
        write_county_files(summary, logger)
    else:
        # All 88 counties — write per-county files + statewide rollup
        write_county_files(summary, logger)
        write_statewide_file(summary, logger)

    elapsed = time.perf_counter() - t0
    logger.info('Done in %.1f s', elapsed)
    logger.info('Output directory: %s', OUT_DIR)


if __name__ == '__main__':
    main()
