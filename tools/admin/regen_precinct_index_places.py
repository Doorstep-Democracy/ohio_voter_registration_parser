#!/usr/bin/env python3
"""
One-shot backfill: stamp place_type / place_name / place_slug AND
ward_slug / ward_name onto every <county>_precinct_index.json and rewrite each
entry's `city` to cities-only, using the single resolvers
voter_data_cleaner._place_per_precinct and ._ward_map_per_county. Then rebuild
the derived city_summary (villages removed) and city_county_map.json.

ward_slug is the precinct's DOMINANT ward's canonical entity slug (majority of
its ward-holding voters), matching what jurisdictional_groupings.build_ward_entities
emits, so the geo tree nests each precinct under the same ward entity. Precincts
with no ward voters -- or whose dominant ward is township-name abuse -- get null.

This mirrors what a full pipeline run now emits natively (export_precinct_charts
was patched to stamp the same fields), so it exists only to backfill the current
committed data without regenerating the ~78k per-precinct chart files. All other
index-entry fields are preserved unchanged, and entries are re-emitted in the
exact key order export_precinct_charts produces, so a future full pipeline run
yields byte-identical files (zero extra git churn).

Place resolution reads per-county slices from local/source/parquet (partitioned
by COUNTY_NUMBER); it needs only raw SWVF jurisdiction columns, no enrichment.

Run from the project root:
    python tools/admin/regen_precinct_index_places.py
    python tools/admin/regen_precinct_index_places.py --county montgomery   # verify one county (derived rebuilds auto-skipped)
"""
import argparse
import json
import logging
import sys
from pathlib import Path

import polars as pl

_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
from pipeline import voter_data_cleaner as _v
from pipeline.ohio_voter_pipeline import _build_city_county_map
from tools.admin import regen_city_summary

from pipeline.paths import DATA_DIR, county_partition  # noqa: E402
from pipeline.paths import PARQUET_DIR as PAR_DIR  # noqa: E402

logging.basicConfig(level=logging.INFO, format='%(message)s')
log = logging.getLogger(__name__)

# Raw SWVF jurisdiction columns the place resolver consumes.
PLACE_COLS = ['PRECINCT_NAME', 'CITY', 'VILLAGE', 'WARD', 'TOWNSHIP', 'RESIDENTIAL_CITY']

# Leading keys in export_precinct_charts' native order; the new place_* and
# ward_* keys sit immediately after `city`, then every remaining existing field
# follows in place.
_LEAD_KEYS = ('name', 'safe_name', 'city', 'place_type', 'place_name', 'place_slug',
              'ward_slug', 'ward_name')


def stamp_county(county_num: str, county_name: str, slug: str) -> bool:
    """Rewrite one county's precinct index with place stamps. Returns True on
    success. Reports any place-less precincts (should be zero -- the 'no Other
    bucket' guarantee)."""
    idx_path = DATA_DIR / f'{slug}_precinct_index.json'
    if not idx_path.exists():
        log.warning('  [skip] %s -- no precinct index', slug)
        return False
    par_path = PAR_DIR / county_partition(county_num)
    if not par_path.exists():
        log.warning('  [skip] %s -- no parquet', slug)
        return False

    df = pl.read_parquet(par_path, columns=PLACE_COLS)
    places = _v._place_per_precinct(df)
    ward_map = _v._ward_map_per_county(df, slug)
    dominant_ward = _v._dominant_per_precinct(df, 'WARD') if 'WARD' in df.columns else {}

    idx = json.loads(idx_path.read_text(encoding='utf-8'))
    entries = idx.get('precincts', [])

    placeless = []
    new_entries = []
    for e in entries:
        place = places.get(e.get('name'))
        if place:
            _dw = dominant_ward.get(e.get('name'))
            _wdesc = ward_map.get(_dw) if _dw else None
            _ward_slug = _wdesc['slug'] if _wdesc and not _wdesc['abuse'] else None
            rebuilt = {
                'name':       e.get('name'),
                'safe_name':  e.get('safe_name'),
                'city':       place['name'] if place['type'] == 'city' else None,
                'place_type': place['type'],
                'place_name': place['name'],
                'place_slug': _v._place_slug(place, slug),
                'ward_slug':  _ward_slug,
                'ward_name':  _dw if _ward_slug else None,
            }
        else:
            placeless.append(e.get('name'))
            rebuilt = {k: (e.get(k) if k in ('name', 'safe_name') else None)
                       for k in _LEAD_KEYS}
        # Preserve every remaining field in its existing order.
        for k, val in e.items():
            if k not in rebuilt:
                rebuilt[k] = val
        new_entries.append(rebuilt)

    idx['precincts'] = new_entries
    _v._dump_json(idx, idx_path, log)

    counts: dict = {}
    for e in new_entries:
        counts[e['place_type']] = counts.get(e['place_type'], 0) + 1
    log.info('  %s: %d precincts %s%s', slug, len(new_entries), counts,
             f'  PLACELESS={placeless}' if placeless else '')
    return not placeless


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--county', help='limit to one county slug (e.g. montgomery); '
                                     'derived rebuilds are auto-skipped')
    args = ap.parse_args()

    counties = sorted(_v.OHIO_COUNTIES.items())  # ('01', 'Adams'), ...
    if args.county:
        target = args.county.lower()
        counties = [(n, nm) for n, nm in counties
                    if nm.lower().replace(' ', '_') == target]
        if not counties:
            log.error('Unknown county slug: %s', args.county)
            return 2

    log.info('Stamping place fields on %d county precinct index(es)...', len(counties))
    stamped = clean = 0
    for num, name in counties:
        slug = name.lower().replace(' ', '_')
        ok_exists = stamp_county(num, name, slug)
        if (DATA_DIR / f'{slug}_precinct_index.json').exists():
            stamped += 1
        if ok_exists:
            clean += 1
    log.info('Stamped %d index(es); %d fully place-resolved.', stamped, clean)

    # Derived rebuilds must see all 88 updated indexes to stay consistent, so
    # they run only on a full pass. A single-county run is for verification.
    if args.county:
        log.info('\n(single-county run -- skipping city_summary / city_county_map rebuild)')
    else:
        log.info('\nRebuilding city_summary (villages removed)...')
        regen_city_summary.main()
        log.info('\nRebuilding city_county_map.json...')
        _build_city_county_map(logger=log)
    log.info('\nDone.')
    return 0


if __name__ == '__main__':
    sys.exit(main())
