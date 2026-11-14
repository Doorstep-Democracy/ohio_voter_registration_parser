#!/usr/bin/env python3
"""
Regenerate *_city_summary.json for all counties using the fixed
build_city_summary() which groups on the CITY column instead of
PRECINCT_NAME prefix-matching.

Does NOT touch precinct charts, cohort data, or any other pipeline output.
Run from the project root: python tools/admin/regen_city_summary.py
"""
import json
import logging
import sys
from datetime import date
from pathlib import Path

import polars as pl

# Add project root to sys.path so pipeline package is importable.
_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
from pipeline import voter_data_cleaner as _v2
from pipeline.paths import DATA_DIR, county_partition
from pipeline.paths import PARQUET_DIR as PAR_DIR

logging.basicConfig(level=logging.INFO, format='%(message)s')
log = logging.getLogger(__name__)

today = date.today().isoformat()

SLUG_TO_NUM = {
    name.lower().replace(' ', '_'): num
    for num, name in _v2.OHIO_COUNTIES.items()
}
NUM_TO_SLUG = {v: k for k, v in SLUG_TO_NUM.items()}


def regen_county(county_num: str, county_name: str, slug: str) -> None:
    par_path = PAR_DIR / county_partition(county_num)
    if not par_path.exists():
        log.warning('  [skip] %s — no parquet', slug)
        return

    # Load the columns the single place resolver behind build_city_summary()
    # needs. VILLAGE / WARD / TOWNSHIP are required so villages resolve to their
    # own place type and drop out of the city summary; without them a village
    # falls through to the RESIDENTIAL_CITY postal fallback and re-enters as a
    # bogus "city". RESIDENTIAL_CITY is the last-resort grouping key for the ~19
    # counties where CITY is blank (100% populated there — confirmed by scan).
    df = pl.read_parquet(
        par_path,
        columns=['PRECINCT_NAME', 'VOTER_STATUS', 'CITY', 'RESIDENTIAL_CITY',
                 'VILLAGE', 'WARD', 'TOWNSHIP'],
    )
    # COUNTY_NUMBER is a Hive partition key — add it back as a literal
    df = df.with_columns(pl.lit(county_num).alias('COUNTY_NUMBER'))

    city_df = _v2.build_city_summary(df)

    city_rows = []
    for row in city_df.iter_rows(named=True):
        city_rows.append([
            row.get('COUNTY_NUMBER', ''),
            row.get('City', ''),
            f"{row.get('ACTIVE', 0):,}",
            f"{row.get('CONFIRMATION', 0):,}",
            f"{row.get('Total Registered', 0):,}",
            str(row.get('Precincts', '')),
            row.get('Est. Unregistered', 'N/A'),
        ])

    out = {
        'title':     'Registration by City / Township',
        'county':    county_name,
        'geography': 'city',
        'type':      'table',
        'updated':   today,
        'note':      (
            f'Analysis run {today} — Ohio Secretary of State SWVF voter file. '
            'City derived from CITY column (registered-address municipality); '
            'falls back to RESIDENTIAL_CITY for counties where CITY is blank. '
            'Cross-county cities show separate rows per county.'
        ),
        'headers': ['County #', 'City / Township', 'Active', 'Confirmation',
                    'Total Registered', 'Precincts', 'Est. Unregistered'],
        'rows': city_rows,
    }

    dest = DATA_DIR / f'{slug}_city_summary.json'
    dest.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding='utf-8')
    log.info('  %s: %d city/township rows', slug, len(city_rows))


def main():
    counties = sorted(_v2.OHIO_COUNTIES.items())  # ('01', 'Adams'), ...
    log.info('Regenerating city_summary.json for %d counties...', len(counties))
    for num, name in counties:
        slug = name.lower().replace(' ', '_')
        regen_county(num, name, slug)
    log.info('\nDone.')


if __name__ == '__main__':
    main()
