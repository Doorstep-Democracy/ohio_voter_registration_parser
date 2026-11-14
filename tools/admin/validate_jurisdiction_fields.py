"""Validate the jurisdiction-vs-postal city fields in an enriched voter parquet.

Run this whenever a new SWVF drop is ingested. It confirms the per-county field
coverage that the pipeline's municipality resolver
(`_dominant_city_per_precinct` in pipeline/voter_data_cleaner.py) depends on, and
flags any precinct that would be mislabeled by a postal-city fallback.

Background -- Ohio SWVF official file layout:
  https://www6.ohiosos.gov/ords/f?p=111:2  (also mirrored in
  local/source/Voter_File_Layout.md)
The voter file carries TWO unrelated location families:
    * Postal address:  RESIDENTIAL_CITY / RESIDENTIAL_ZIP ... -> USPS mail
      delivery only. NOT a legal jurisdiction.
    * Authoritative jurisdiction: CITY / VILLAGE / WARD / TOWNSHIP / PRECINCT ->
      the Board of Elections' assignment of where a voter legally votes. CITY is
      blank for voters in an unincorporated township (they vote in NO city);
      their TOWNSHIP column names the township.
Conflating them (backfilling a blank CITY from RESIDENTIAL_CITY) mislabels
township precincts with their post-office city -- e.g. Montgomery's
WASHINGTON TWP F (CITY='', TOWNSHIP='WASHINGTON TOWNSHIP') gets tagged KETTERING
because its mail is delivered from the Kettering post office.

The resolver therefore uses a jurisdiction HIERARCHY, not a postal fallback:
  CITY -> VILLAGE -> WARD-prefix -> TOWNSHIP(=not a city) -> RESIDENTIAL_CITY.
Bounded all-county checks established why: 69 counties populate CITY; 16 of the
remaining blank-CITY counties populate TOWNSHIP~100%; Cuyahoga (18) encodes the
municipality in its WARD prefix ('CLEVELAND WARD 7') and PRECINCT_NAME; only
Wyandot (88) has no authoritative column at all, so postal is a true last resort
there -- 1 county, not 19.

What this checks, per county:
  1. CITY coverage        -- % of voters with a non-blank authoritative CITY.
  2. Hierarchy coverage   -- for blank-CITY counties, the % populated of each
     fallback column (VILLAGE / WARD / TOWNSHIP) and whether ANY authoritative
     column covers the county. Counties with none are the true postal-last-resort
     set (expected: just Wyandot).
  3. Name conflicts       -- precincts whose NAME is a township/village but whose
     postal RESIDENTIAL_CITY is a different municipality (the mislabel risk a
     postal fallback would reintroduce).

Read-only (except --ledger). Bounded output. Exit code is non-zero under
--strict if any resolver-CONFIRMED postal mislabel exists (section [3b],
B1 2026-07-10) or a MUST-be-zero structural check fails, so this can gate
a pipeline run in CI.
"""
from __future__ import annotations
import argparse
import json
import re
import sys
from datetime import date
from pathlib import Path

import polars as pl

_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
if str(Path(__file__).resolve().parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
from pipeline import voter_data_cleaner as _v  # noqa: E402
# Shared per-county profiling primitives (the "88-county assumption" toolkit,
# CLAUDE.md 5). COUNTY_NAME is the single canonical roster, consumed here --
# never re-declared. nonblank / coverage_by_county / iter_county_frames are the
# lifted profiling verbs; profile_column is the "profile before you assume" call.
from data_profile import (  # noqa: E402
    OHIO_COUNTIES as COUNTY_NAME,
    coverage_by_county,
    iter_county_frames,
    nonblank as _nonblank,
)
# Section [10]: the per-county x per-rule encoding census (the systemic guard
# from the 2026-07-11 root-cause session). Lives in its own module so it is
# runnable standalone; the validator runs it on every drop.
from encoding_census import run_census, summarize as census_summarize  # noqa: E402

# Locations come from the single resolver. DEFAULT_PARQUET stays the --parquet
# default, so the flag still overrides it; DOCS_DATA follows ELECTION_DATA_OUT.
from pipeline.paths import DATA_DIR as DOCS_DATA  # noqa: E402
from pipeline.paths import ENRICHED_CACHE as DEFAULT_PARQUET  # noqa: E402
WARD_INDEX_JSON = DOCS_DATA / 'ward' / 'index.json'
DATA_QUALITY_MD = _ROOT / 'DATA_QUALITY.md'
LEDGER_BEGIN = '<!-- auto:begin -->'
LEDGER_END = '<!-- auto:end -->'
PLACE_WARD_COLS = ['COUNTY_NUMBER', 'PRECINCT_NAME', 'CITY', 'VILLAGE', 'WARD', 'TOWNSHIP',
                   'RESIDENTIAL_CITY']

# Measured 2026-07-09/10 baselines (PLAN_SUBCOUNTY_JURISDICTIONS.md Parts W1-W4).
# --strict fails if these GROW (regression), not on the baseline count itself --
# each is a per-drop measurement re-checked on every new SWVF partition.
SPLIT_PRECINCT_BASELINE = 43
# 4 -> 18 on 2026-07-11: the rule-4 token gate (encoding_census.py) stopped
# phantom-city parents, so Stark's 14 township-in-WARD values now resolve to
# township parents and are correctly counted as abuse (WARD='JACKSON' et al.);
# same fix turned Lucas WARD='WASHINGTON TOWNSHIP' township-parented. This is
# the fix REVEALING existing schema abuse, not state-data growth.
WARD_ABUSE_BASELINE = 18
# A5 (2026-07-04 snapshot, row-accurate CITY basis): counties where ANY city
# has <100% WARD coverage, INCLUDING at-large-everywhere cities -- broader
# than [6]'s DEFECT classification. Informational, not strict-gated.
WARD_GAP_COUNTY_BASELINE = 29

# COUNTY_NAME (number->name, '01'->'Adams') is imported above from data_profile,
# which sources it from voter_data_cleaner.OHIO_COUNTIES -- the single canonical
# roster (CLAUDE.md 5, single-source rule). Ohio-specific facts baked into that
# canonical table -- 88 counties, numbered 01-88 alphabetically by statute,
# COUNTY_NUMBER a 2-char zero-padded string -- are the state seam to revisit when
# a second state is added; do not re-declare the roster here.

# A county is a true "postal last resort" only if NO authoritative jurisdiction
# column (CITY/VILLAGE/WARD/TOWNSHIP) covers it -- i.e. any_auth_cov below this
# threshold. A merely-blank CITY does NOT qualify (TOWNSHIP/WARD usually cover
# it). Tunable. Expected to select exactly Wyandot (88) on the current file.
POSTAL_ONLY_CITY_COVERAGE = 0.10
TWP_VILLAGE_RE = r'\bTWP\b|\bTOWNSHIP\b|\bVILLAGE\b|\bVILL\b'

# _nonblank and coverage_by_county are imported from data_profile (the shared
# profiling toolkit); see the import block above.


def township_postal_conflicts(lf: pl.LazyFrame) -> pl.DataFrame:
    """Precincts whose NAME is a township/village, CITY is blank, but the postal
    RESIDENTIAL_CITY carries a municipality -- the mislabel set."""
    twp = lf.filter(
        pl.col('PRECINCT_NAME').str.to_uppercase().str.contains(TWP_VILLAGE_RE)
        & ~_nonblank('CITY')
        & _nonblank('RESIDENTIAL_CITY')
    )
    return (
        twp.group_by(['COUNTY_NUMBER', 'PRECINCT_NAME'])
           .agg(
               pl.len().alias('voters'),
               pl.col('RESIDENTIAL_CITY').mode().first().alias('postal_city'),
               pl.col('TOWNSHIP').mode().first().alias('township'),
           )
           .sort(['COUNTY_NUMBER', 'voters'], descending=[False, True])
           .collect()
    )


def classify_township_conflicts(lf: pl.LazyFrame, conflicts: pl.DataFrame,
                                postal_set: set) -> dict:
    """[3b] Resolve every [3] conflict precinct through the real place
    resolver (_v._place_per_precinct) and split the benign majority from
    resolver-CONFIRMED mislabels (B1, operator decision 2026-07-10).

    A [3] row is postal-vs-jurisdiction NOISE when the resolver lands on an
    authoritative column anyway: township (TOWNSHIP column / precinct-name
    token), village (VILLAGE column), or a city carried by authoritative
    CITY/WARD rows. The last bucket exists because Cuyahoga names its
    municipalities inside WARD ('BAY VILLAGE WARD 2') and 'VILLAGE' there is
    part of the municipality's legal name (Bay Village is a CITY), matching
    the [3] name-token regex by accident -- RESIDENTIAL_CITY is never
    consulted for those precincts.

    A row is a CONFIRMED mislabel only when the precinct has NO
    authoritative-column rows at all, so the resolver genuinely falls
    through to RESIDENTIAL_CITY, in a county that HAS authoritative columns
    (postal-last-resort counties are excluded: postal is correct-by-necessity
    there). 2026-07-04 measurement: 1,778 conflicts -> 1,558 township +
    204 village + 16 authoritative-WARD city, 0 confirmed. --strict gates on
    the confirmed subset only; confirmed rows feed the POSTAL-MISLABEL
    ledger IDs in build_ledger_rows."""
    out: dict = {'township': 0, 'village': 0, 'city_auth': 0,
                 'confirmed': [], 'unresolved': []}
    high = conflicts.filter(~pl.col('COUNTY_NUMBER').is_in(sorted(postal_set)))
    if high.height == 0:
        return out
    target = set(high['COUNTY_NUMBER'].to_list())
    for num, cname, slug, sub in iter_county_frames(lf, PLACE_WARD_COLS):
        if num not in target:
            continue
        place = _v._place_per_precinct(sub)
        auth_expr = pl.lit(False)
        for c in ('CITY', 'VILLAGE', 'WARD', 'TOWNSHIP'):
            if c in sub.columns:
                auth_expr = auth_expr | _nonblank(c)
        auth_by_p = dict(
            sub.group_by('PRECINCT_NAME')
               .agg(auth_expr.sum().alias('auth'))
               .iter_rows())
        for r in high.filter(pl.col('COUNTY_NUMBER') == num).iter_rows(named=True):
            p = r['PRECINCT_NAME']
            res = place.get(p)
            entry = {'county_num': num, 'county_name': cname, 'precinct': p,
                     'voters': r['voters'], 'postal_city': r['postal_city'],
                     'resolved': res['name'] if res else None}
            if res is None:
                out['unresolved'].append(entry)
            elif res['type'] in ('township', 'village'):
                out[res['type']] += 1
            elif int(auth_by_p.get(p, 0) or 0) > 0:
                out['city_auth'] += 1
            else:
                out['confirmed'].append(entry)
    return out


def village_in_township_candidates(lf: pl.LazyFrame) -> pl.DataFrame:
    """KNOWN-LIMITATION flag (manual review). Precincts whose name is NOT a
    township token, but whose dominant TOWNSHIP column value is a COMPOSITE of
    two place words (e.g. Wyandot's NEVADA precinct -> TOWNSHIP='NEVADA ANTRIM':
    the incorporated Village of Nevada split across Antrim & Eden townships).

    The resolver maps these to None ('not a city') because the TOWNSHIP column is
    populated -- which UNDER-CLAIMS a real incorporated village. We deliberately
    do NOT auto-correct (the composite-township pattern is rare and hard to
    detect without false positives -- see the 2026-06 decision). Instead we
    surface candidates loudly so a human can confirm whether the village should
    roll up. Heuristic is intentionally tight (composite township starting with
    the precinct's first word, second word NOT a township token) to minimize
    noise; review every row, it is not authoritative."""
    dom = (lf.filter(_nonblank('TOWNSHIP'))
             .group_by(['COUNTY_NUMBER', 'PRECINCT_NAME', 'TOWNSHIP'])
             .agg(pl.len().alias('n'))
             .sort('n', descending=True)
             .group_by(['COUNTY_NUMBER', 'PRECINCT_NAME'])
             .agg(pl.first('TOWNSHIP').alias('township'),
                  pl.sum('n').alias('voters'))
             .collect())
    import re
    rows = []
    for r in dom.iter_rows(named=True):
        pn = (r['PRECINCT_NAME'] or '').strip().upper()
        tw = (r['township'] or '').strip().upper()
        if not pn or not tw:
            continue
        # Skip precincts already correctly handled as townships by name.
        if re.search(TWP_VILLAGE_RE, pn) or re.search(r'\sTS$', pn):
            continue
        toks = tw.split()
        first = pn.split()[0]
        # composite: 'NEVADA ANTRIM' -> starts with precinct's first word, has a
        # distinct second word, and is not spelled as a township.
        if (len(toks) >= 2 and toks[0] == first
                and 'TOWNSHIP' not in tw and 'TWP' not in tw
                and not tw.endswith('TOWNSHP')):  # common misspelling = real twp
            rows.append(r)
    return pl.DataFrame(rows) if rows else pl.DataFrame(
        schema={'COUNTY_NUMBER': pl.Utf8, 'PRECINCT_NAME': pl.Utf8,
                'township': pl.Utf8, 'voters': pl.Int64})


# _per_county_frames was lifted to data_profile.iter_county_frames(lf, cols) --
# now parameterized on the column list so any stream can reuse it. This module's
# callers pass PLACE_WARD_COLS (the jurisdiction columns the place/ward resolvers
# need). The county-slug is produced inline by iter_county_frames.


def _bundle_missing_path(place: dict, county_slug: str) -> Path | None:
    """Path to the place's stats bundle if it does NOT exist on disk, else
    None. Checks a single representative chart file (party_affiliation) --
    a bundle is written atomically per-place (CLAUDE.md 5), so one file's
    presence implies the rest."""
    slug = _v._place_slug(place, county_slug)
    if place['type'] == 'city':
        p = DOCS_DATA / 'city' / f'{slug}_city_party_affiliation.json'
    else:
        p = DOCS_DATA / place['type'] / f'{slug}_party_affiliation.json'
    return None if p.exists() else p


def check_place_completeness(lf: pl.LazyFrame, show: int) -> int:
    """[5] Place resolution completeness (Part 6): every precinct must resolve
    to exactly one (type, name) via the single place resolver
    (_v._place_per_precinct) -- the data-layer guarantee behind 'no Other
    precincts bucket'. Secondary warn-only check: does each resolved place's
    stats bundle actually exist on disk (surfaces rare postal-fallback /
    composite-township cases where a tree node would render but 404)."""
    print(f'\n[5] Place resolution completeness')
    placeless: list[tuple[str, str]] = []
    missing_bundles: list[tuple[str, str, str]] = []
    for num, cname, slug, sub in iter_county_frames(lf, PLACE_WARD_COLS):
        place = _v._place_per_precinct(sub)
        precincts = set(sub['PRECINCT_NAME'].drop_nulls().unique().to_list())
        for p in sorted(precincts - place.keys()):
            placeless.append((cname, p))
        seen_slugs = set()
        for p in place.values():
            key = (p['type'], p['name'])
            if key in seen_slugs:
                continue
            seen_slugs.add(key)
            missing = _bundle_missing_path(p, slug)
            if missing is not None:
                missing_bundles.append((cname, f"{p['type']}:{p['name']}", str(missing)))

    print(f'    place-less precincts (MUST be zero): {len(placeless)}')
    for cname, p in placeless[:show]:
        print(f'      {cname:<14} {p}')
    if len(placeless) > show:
        print(f'      ... +{len(placeless) - show} more (raise --show to see all)')

    print(f'    resolved places with no stats bundle on disk (warn-only): '
          f'{len(missing_bundles)}')
    for cname, label, path in missing_bundles[:show]:
        print(f'      {cname:<14} {label:<30} missing {path}')

    return len(placeless)


def check_ward_coverage(lf: pl.LazyFrame) -> list[dict]:
    """[6] Ward coverage matrix: per (municipality, county), % of that city's
    voters with a non-blank WARD, counted on the ROW-ACCURATE basis: literal
    CITY == value row matches (normalized via _v._normalize_city_name), never
    the dominant-place-per-precinct sweep (A4, 2026-07-10). The sweep counted
    every voter in a precinct whose DOMINANT place is the city, pulling
    blank-CITY and other-city rows from mixed precincts into the count --
    Kettering/Greene read ~3,100 where the row-accurate figure is 540
    (181 in BEAVERCREEK 090 + 359 in SUGARCREEK 151, 2026-07-04 snapshot).
    Cities that appear only via WARD-prefix or postal resolution (blank-CITY
    counties, e.g. Cuyahoga) carry no literal CITY rows and drop out of this
    matrix -- their ward coverage is definitionally carried by WARD itself.

    Classification (locked policy): 0% in every county the city appears in
    -> INFO 'at-large' (no code change); anything else uneven (partial
    coverage in a county, or populated in one county and blank in another --
    the Kettering/Greene case) -> DEFECT. We never impute the gap (ward
    boundaries aren't derivable from the voter file).

    Also prints the A5 summary: count of counties where ANY city has <100%
    WARD coverage, INCLUDING at-large-everywhere cities -- broader than the
    DEFECT classification (baseline WARD_GAP_COUNTY_BASELINE, re-measured
    per drop, informational)."""
    print(f'\n[6] Ward coverage matrix (per municipality, across counties; '
          f'row-accurate CITY-row basis)')
    # city_name -> county_slug -> (county_num, county_name, nonblank_ward_voters, total_voters)
    by_city: dict[str, dict[str, tuple[str, str, int, int]]] = {}
    for num, cname, slug, sub in iter_county_frames(lf, PLACE_WARD_COLS):
        rows = sub.select(['CITY', 'WARD']).with_columns(
            pl.col('CITY').str.strip_chars().alias('_c'),
            pl.col('WARD').str.strip_chars().alias('_w'),
        ).filter(pl.col('_c').is_not_null() & (pl.col('_c') != ''))
        if rows.height == 0:
            continue
        agg = rows.group_by('_c').agg(
            pl.len().alias('total'),
            (pl.col('_w').is_not_null() & (pl.col('_w') != '')).sum().alias('nb'),
        )
        for r in agg.iter_rows(named=True):
            city = _v._normalize_city_name(r['_c'])
            if not city:
                continue
            # two raw values can normalize to the same city ('KETTERING CITY'
            # / 'KETTERING') -- accumulate, never overwrite.
            _, _, nb0, tot0 = by_city.setdefault(city, {}).get(
                slug, (num, cname, 0, 0))
            by_city[city][slug] = (num, cname, nb0 + r['nb'], tot0 + r['total'])

    defects: list[dict] = []
    info_at_large = 0
    for city, counties in sorted(by_city.items()):
        covered = [c for c, (_, _, nb, tot) in counties.items() if nb > 0]
        if not covered:
            info_at_large += 1
            continue
        multi = len(counties) > 1
        for c, (num, cname, nb, tot) in sorted(counties.items()):
            cov = nb / tot if tot else 0.0
            if 0 < cov < 0.95:
                defects.append({'kind': 'partial', 'city': city, 'county_num': num,
                                 'county_name': cname, 'county_slug': c,
                                 'coverage': cov, 'nb': nb, 'total': tot})
            elif cov == 0 and multi:
                defects.append({'kind': 'cross_county_gap', 'city': city, 'county_num': num,
                                 'county_name': cname, 'county_slug': c,
                                 'coverage': 0.0, 'nb': nb, 'total': tot})

    print(f'    at-large (0% everywhere) municipalities: {info_at_large}  (INFO, not a defect)')
    print(f'    DEFECT-classified gaps: {len(defects)}')
    for d in defects:
        if d['kind'] == 'partial':
            print(f"      {d['city']} / {d['county_slug']}: partial WARD coverage "
                  f"{d['coverage']:.1%} ({d['nb']}/{d['total']} voters)")
        else:
            print(f"      {d['city']} / {d['county_slug']}: 0% WARD, populated in "
                  f"another county ({d['total']} voters under-counted)")

    # A5: coverage-gap county summary, broader than DEFECT (includes at-large).
    gap_counties = {c for counties in by_city.values()
                    for c, (_, _, nb, tot) in counties.items() if nb < tot}
    print(f'    counties where ANY city has <100% WARD coverage '
          f'(incl. at-large cities): {len(gap_counties)}  '
          f'(baseline: {WARD_GAP_COUNTY_BASELINE})')
    return defects


def check_split_precincts(lf: pl.LazyFrame, show: int) -> list:
    """[7] Split precincts (H2): precincts whose ward-holding voters span more
    than one WARD value. Baseline 43 statewide (2026-07-09 measurement) --
    fail --strict if it grows. Also enumerates ward entities that are never
    any precinct's dominant ward (the stamp-less tail: row-accurate page
    exists, but no tree node), by diffing the stamped ward_slug set in the
    committed precinct indexes against docs/data/ward/index.json."""
    print(f'\n[7] Split precincts + stamp-less ward entities')
    splits = []
    for num, cname, slug, sub in iter_county_frames(lf, PLACE_WARD_COLS):
        if 'WARD' not in sub.columns:
            continue
        rows = sub.select(['PRECINCT_NAME', 'WARD']).with_columns(
            pl.col('WARD').str.strip_chars().alias('_w')
        ).filter(pl.col('_w') != '')
        if rows.height == 0:
            continue
        per_prec = (rows.group_by(['PRECINCT_NAME', '_w']).agg(pl.len().alias('n'))
                        .group_by('PRECINCT_NAME').agg(pl.col('_w').n_unique().alias('nwards'),
                                                        pl.col('n').sum().alias('voters')))
        for r in per_prec.filter(pl.col('nwards') > 1).iter_rows(named=True):
            splits.append((cname, r['PRECINCT_NAME'], r['nwards'], r['voters']))

    print(f'    precincts spanning >1 ward: {len(splits)}  (baseline: 43)')
    for cname, pname, n, voters in splits[:show]:
        print(f'      {cname:<14} {pname:<26} {n} wards ({voters:,} voters)')
    if len(splits) > show:
        print(f'      ... +{len(splits) - show} more (raise --show to see all)')

    stamped_slugs: set[str] = set()
    for idx_path in DOCS_DATA.glob('*_precinct_index.json'):
        idx = json.loads(idx_path.read_text(encoding='utf-8'))
        for entry in idx.get('precincts', []):
            ws = entry.get('ward_slug')
            if ws:
                stamped_slugs.add(ws)

    all_slugs = {e['slug'] for e in json.loads(WARD_INDEX_JSON.read_text(encoding='utf-8'))} \
        if WARD_INDEX_JSON.exists() else set()
    stampless = sorted(all_slugs - stamped_slugs)
    print(f'    ward entities with no precinct stamped as their dominant ward: '
          f'{len(stampless)}  (baseline: 7)')
    for s in stampless[:show]:
        print(f'      {s}')

    return splits


def check_ward_collision_safety(lf: pl.LazyFrame) -> bool:
    """[8] Collision safety: no two ward entities share a slug, and every
    precinct-stamped ward_slug resolves to an existing entity (the
    'first_ward silently merges 5 municipalities' class of bug can never
    regress unnoticed). Current truth: 915 stamped slugs subset of 922
    entities (7 stamp-less, see [7])."""
    print(f'\n[8] Ward collision safety')
    if not WARD_INDEX_JSON.exists():
        print('    [skip] docs/data/ward/index.json not found')
        return True
    entries = json.loads(WARD_INDEX_JSON.read_text(encoding='utf-8'))
    slugs = [e['slug'] for e in entries]
    dupes = sorted({s for s in slugs if slugs.count(s) > 1})
    print(f'    duplicate entity slugs (MUST be zero): {len(dupes)}')
    for d in dupes:
        print(f'      {d}')

    slug_set = set(slugs)
    stamped_slugs: set[str] = set()
    for idx_path in DOCS_DATA.glob('*_precinct_index.json'):
        idx = json.loads(idx_path.read_text(encoding='utf-8'))
        for entry in idx.get('precincts', []):
            ws = entry.get('ward_slug')
            if ws:
                stamped_slugs.add(ws)
    orphans = sorted(stamped_slugs - slug_set)
    print(f'    stamped ward_slug values with no matching entity (MUST be zero): '
          f'{len(orphans)}')
    for o in orphans[:20]:
        print(f'      {o}')
    print(f'    stamped slugs: {len(stamped_slugs)}  entities: {len(slug_set)}  '
          f'(subset check: {"OK" if not orphans else "FAIL"})')
    return not dupes and not orphans


def check_ward_schema_abuse(lf: pl.LazyFrame) -> list[dict]:
    """[9] Schema abuse: WARD values that are not real wards -- either the
    value itself is a township name (Lucas stuffs 'WASHINGTON TOWNSHIP' etc.
    into WARD) or the dominant resolved parent is a township (Stark's
    WARD='CANTON' on 8,182 township voters; a 2-voter BELLEVUE WARD 4
    fragment in a township-tokened precinct). Mirrors
    _v._ward_map_per_county's own 'regex OR township-parent' exclusion --
    this validates against that rule, not the plan's original prose.
    Baseline: 4 distinct WARD values statewide."""
    print(f'\n[9] Ward schema abuse (excluded from ward entities)')
    abuse_rows: list[dict] = []
    for num, cname, slug, sub in iter_county_frames(lf, PLACE_WARD_COLS):
        ward_map = _v._ward_map_per_county(sub, slug)
        if not ward_map:
            continue
        counts = None
        for ward_value, desc in ward_map.items():
            if not desc['abuse']:
                continue
            if counts is None:
                counts = (sub.select(pl.col('WARD').str.strip_chars().alias('_w'))
                             .group_by('_w').agg(pl.len().alias('n')))
            n = counts.filter(pl.col('_w') == ward_value)['n'].to_list()
            abuse_rows.append({'county_num': num, 'county_name': cname,
                                'ward_value': ward_value,
                                'parent_type': desc['parent_type'],
                                'voters': n[0] if n else 0})

    distinct_values = sorted({(r['county_name'], r['ward_value']) for r in abuse_rows})
    print(f'    distinct abused WARD values: {len(distinct_values)}  (baseline: 4)')
    for r in abuse_rows:
        print(f"      {r['county_name']:<12} WARD={r['ward_value']:<20} "
              f"parent_type={r['parent_type']:<10} ({r['voters']:,} voters)")
    return abuse_rows


def check_earned_uniformity(lf: pl.LazyFrame) -> list[dict]:
    """[11] Detect cross-county city merges that fail the earned-uniformity guard."""
    from pipeline.voter_data_cleaner import _dominant_per_precinct, _dominant_city_per_precinct
    from pipeline.ohio_voter_pipeline import EARNED_UNIFORMITY_ALLOWLIST
    df = lf.select(['COUNTY_NUMBER', 'PRECINCT_NAME', 'CITY', 'VILLAGE', 'WARD', 'TOWNSHIP', 'RESIDENTIAL_CITY', 
                    'LOCAL_SCHOOL_DISTRICT', 'EXEMPTED_VILL_SCHOOL_DISTRICT', 'CITY_SCHOOL_DISTRICT', 'MUNICIPAL_COURT_DISTRICT']).collect()
    
    city_counties = {}
    city_identities = {}
    
    for (county_number,), cdf in df.group_by(['COUNTY_NUMBER']):
        county_slug = COUNTY_NAME[county_number].lower().replace(' ', '_')
        cities = _dominant_city_per_precinct(cdf)
        dom_local = _dominant_per_precinct(cdf, 'LOCAL_SCHOOL_DISTRICT')
        dom_ev = _dominant_per_precinct(cdf, 'EXEMPTED_VILL_SCHOOL_DISTRICT')
        dom_city = _dominant_per_precinct(cdf, 'CITY_SCHOOL_DISTRICT')
        dom_muni = _dominant_per_precinct(cdf, 'MUNICIPAL_COURT_DISTRICT')
        
        for pr, city in cities.items():
            if not city: continue
            city = city.strip().upper()
            city_counties.setdefault(city, set()).add(county_slug)
            idents = city_identities.setdefault(city, {}).setdefault(county_slug, set())
            
            for field, dom_dict in [('LOCAL_SCHOOL_DISTRICT', dom_local), 
                                  ('EXEMPTED_VILL_SCHOOL_DISTRICT', dom_ev), 
                                  ('CITY_SCHOOL_DISTRICT', dom_city), 
                                  ('MUNICIPAL_COURT_DISTRICT', dom_muni)]:
                v = dom_dict.get(pr)
                if v:
                    import re
                    val = re.sub(r'\s*\([^)]*\)$', '', v.strip().upper())
                    idents.add(f"{field}:{val}")
                    
    violations = []
    
    for city, slugs in sorted(city_counties.items()):
        slugs_list = sorted(slugs)
        if len(slugs_list) > 1 and city not in EARNED_UNIFORMITY_ALLOWLIST:
            county_sets = [city_identities[city][s] for s in slugs_list]
            shared = set.intersection(*county_sets) if county_sets else set()
            if not shared:
                violations.append({
                    'city': city,
                    'counties': slugs_list,
                })
    return violations



def check_school_district_collisions(lf: pl.LazyFrame) -> list[dict]:
    """[12] Detect school districts spanning multiple counties without a disambiguating county suffix."""
    df = lf.select(['COUNTY_NUMBER', 'LOCAL_SCHOOL_DISTRICT', 'EXEMPTED_VILL_SCHOOL_DISTRICT']).collect()
    
    violations = []
    for col in ['LOCAL_SCHOOL_DISTRICT', 'EXEMPTED_VILL_SCHOOL_DISTRICT']:
        if col not in df.columns: continue
        
        # Group by district name
        for district, cdf in df.drop_nulls(subset=[col]).group_by([col]):
            dist_name = str(district[0]).strip().upper()
            if not dist_name: continue
            
            counties = sorted(cdf['COUNTY_NUMBER'].unique().to_list())
            if len(counties) > 1 and '(' not in dist_name:
                violations.append({
                    'district_type': col,
                    'district_name': dist_name,
                    'counties': [COUNTY_NAME.get(c, c) for c in counties]
                })
                
    return violations


def _gap_slug(s: str) -> str:
    return re.sub(r'[^A-Z0-9]+', '-', s.upper()).strip('-')

def build_ledger_rows(ward_defects: list[dict], abuse_rows: list[dict],
                       splits: list,
                       postal_mislabels: list[dict] | None = None,
                       uniformity_violations: list[dict] | None = None,
                       sd_violations: list[dict] | None = None) -> list[dict]:
    """Part W7: turn this run's measured gaps into stable-ID ledger rows for
    DATA_QUALITY.md. One row per distinct state-data defect; IDs are stable
    across drops (county number + defect specifics) so the manual section can
    reference them durably. Never fabricates a boundary or imputes a count --
    every row is a direct measurement from this drop's enriched parquet."""
    rows: list[dict] = []
    for d in ward_defects:
        gap_id = f"WARD-GAP-{d['county_num']}-{_gap_slug(d['city'])}"
        cls = ('cross-county ward gap' if d['kind'] == 'cross_county_gap'
               else 'partial ward coverage')
        voters = d['total'] - d['nb'] if d['kind'] == 'cross_county_gap' else d['nb']
        detail = (f"{d['coverage']:.1%} coverage" if d['kind'] == 'partial'
                  else '0% coverage, populated in another county')
        rows.append({
            'gap_id': gap_id, 'county': d['county_name'], 'municipality': d['city'],
            'defect_class': cls, 'affected_voters': d['total'],
            'detail': detail,
        })
    for r in abuse_rows:
        gap_id = f"WARD-ABUSE-{r['county_num']}-{_gap_slug(r['ward_value'])}"
        rows.append({
            'gap_id': gap_id, 'county': r['county_name'], 'municipality': r['ward_value'],
            'defect_class': 'schema abuse (township name in WARD)',
            'affected_voters': r['voters'],
            'detail': f"resolves to {r['parent_type']} parent",
        })
    for m in (postal_mislabels or []):
        gap_id = f"POSTAL-MISLABEL-{m['county_num']}-{_gap_slug(m['precinct'])}"
        rows.append({
            'gap_id': gap_id, 'county': m['county_name'],
            'municipality': m['postal_city'],
            'defect_class': ('postal fall-through (no authoritative '
                             'jurisdiction column populated)'),
            'affected_voters': m['voters'],
            'detail': (f"precinct {m['precinct']}: CITY/VILLAGE/WARD/"
                       f"TOWNSHIP blank; only RESIDENTIAL_CITY populated"),
        })
    for v in (uniformity_violations or []):
        gap_id = f"UNEARNED-MERGE-{_gap_slug(v['city'])}"
        counties_str = ', '.join(v['counties'])
        rows.append({
            'gap_id': gap_id, 'county': counties_str, 'municipality': v['city'],
            'defect_class': 'unearned multi-county city merge',
            'affected_voters': 0,
            'detail': f"spans {counties_str} but shares no school/court district identity",
        })
    for v in (sd_violations or []):
        gap_id = f"SD-COLLISION-{_gap_slug(v['district_name'])}"
        counties_str = ', '.join(v['counties'])
        rows.append({
            'gap_id': gap_id, 'county': counties_str, 'municipality': v['district_name'],
            'defect_class': f"school district collision ({v['district_type']})",
            'affected_voters': 0,
            'detail': f"spans {counties_str} but lacks state (COUNTY) disambiguation suffix",
        })
    if splits:
        total_voters = sum(v for *_, v in splits)
        rows.append({
            'gap_id': 'WARD-SPLIT-PRECINCTS', 'county': 'statewide',
            'municipality': f'{len(splits)} precincts (linked list, see validator [7])',
            'defect_class': 'split precinct (dominant-ward approximation)',
            'affected_voters': total_voters,
            'detail': f'{len(splits)} precincts span >1 ward',
        })
    return rows


def _parse_existing_ledger(text: str) -> dict[str, str]:
    """gap_id -> first_observed, read from a previous auto-table so re-runs
    don't reset the 'first seen' date. Tolerant of a missing/empty table."""
    first_seen: dict[str, str] = {}
    for line in text.splitlines():
        if not line.startswith('|'):
            continue
        cells = [c.strip() for c in line.strip('|').split('|')]
        if len(cells) < 6 or cells[0] in ('Gap ID', '---') or set(cells[0]) == {'-'}:
            continue
        gap_id, first_observed = cells[0], cells[4]
        if gap_id and first_observed:
            first_seen[gap_id] = first_observed
    return first_seen


def write_ledger(rows: list[dict], today: str) -> None:
    """Write/refresh the auto section of DATA_QUALITY.md between LEDGER_BEGIN
    / LEDGER_END markers, preserving everything outside them (the manual
    status section is human-owned, per Part W7) and preserving each gap's
    first-observed date across runs."""
    existing = DATA_QUALITY_MD.read_text(encoding='utf-8') if DATA_QUALITY_MD.exists() else ''
    prior_first_seen = _parse_existing_ledger(existing)

    header = ('| Gap ID | County | Municipality | Defect class | First observed | '
               'Last observed | Affected voters | Detail |\n'
               '|---|---|---|---|---|---|---|---|\n')
    lines = [header.rstrip('\n')]
    for r in sorted(rows, key=lambda r: r['gap_id']):
        first_observed = prior_first_seen.get(r['gap_id'], today)
        lines.append(f"| {r['gap_id']} | {r['county']} | {r['municipality']} | "
                      f"{r['defect_class']} | {first_observed} | {today} | "
                      f"{r['affected_voters']:,} | {r['detail']} |")
    table = '\n'.join(lines)

    if LEDGER_BEGIN in existing and LEDGER_END in existing:
        pre, rest = existing.split(LEDGER_BEGIN, 1)
        _, post = rest.split(LEDGER_END, 1)
        new_text = f'{pre}{LEDGER_BEGIN}\n{table}\n{LEDGER_END}{post}'
    else:
        new_text = (
            '# DATA_QUALITY.md — Ohio SWVF state-data defect ledger\n\n'
            'Measured gaps in the state-provided voter file (missing WARD '
            'coverage, township-name schema abuse in WARD, split precincts) '
            'that are Board-of-Elections/SoS data defects, not repo bugs. '
            'The table below is regenerated by '
            '`python tools/admin/validate_jurisdiction_fields.py --ledger`; '
            'edit only the manual section beneath it.\n\n'
            f'{LEDGER_BEGIN}\n{table}\n{LEDGER_END}\n\n'
            '## Manual status\n\n'
            'Never touched by the tool. Add one row per gap ID when a report '
            'is filed.\n\n'
            '| Gap ID | Report filed | SoS/BoE contact or ticket | Status | '
            'Resolved in drop |\n'
            '|---|---|---|---|---|\n'
        )
    tmp = DATA_QUALITY_MD.with_suffix('.md.tmp')
    tmp.write_text(new_text, encoding='utf-8')
    tmp.replace(DATA_QUALITY_MD)
    print(f'\n[ledger] wrote {len(rows)} rows to {DATA_QUALITY_MD}')


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--parquet', type=Path, default=DEFAULT_PARQUET)
    ap.add_argument('--show', type=int, default=25,
                    help='max conflict rows to print')
    ap.add_argument('--strict', action='store_true',
                    help='exit non-zero if any name conflict exists')
    ap.add_argument('--skip-ward', action='store_true',
                    help='skip sections [5]-[10] (slower: per-county resolver passes)')
    ap.add_argument('--ledger', action='store_true',
                    help='write/refresh the auto section of DATA_QUALITY.md (Part W7)')
    args = ap.parse_args()

    if not args.parquet.exists():
        print(f'ERROR: parquet not found: {args.parquet}', file=sys.stderr)
        return 2

    lf = pl.scan_parquet(args.parquet)
    cols = lf.collect_schema().names()
    need = {'COUNTY_NUMBER', 'PRECINCT_NAME', 'CITY', 'RESIDENTIAL_CITY',
            'TOWNSHIP'}
    missing = need - set(cols)
    if missing:
        print(f'ERROR: parquet missing required columns: {sorted(missing)}',
              file=sys.stderr)
        return 2

    def name(n: str) -> str:
        return COUNTY_NAME.get(n, n)

    print('=' * 70)
    print('JURISDICTION FIELD VALIDATION')
    print(f'  source: {args.parquet}')
    print('=' * 70)

    cov = coverage_by_county(lf)
    # The TRUE postal-last-resort set: no authoritative column covers the county.
    # (Not "CITY blank" -- a blank CITY is usually covered by TOWNSHIP/WARD.)
    postal_only = cov.filter(pl.col('any_auth_cov') < POSTAL_ONLY_CITY_COVERAGE)
    print(f'\n[1] CITY coverage: {cov.height} counties present')
    weak = cov.filter(pl.col('city_cov') < 0.50).sort('city_cov')
    print(f'    counties with < 50% authoritative CITY coverage: {weak.height}')
    print('    (low CITY is normal -- these counties encode municipality via '
          'TOWNSHIP/VILLAGE/WARD; see [2])')
    for r in weak.head(40).to_dicts():
        print(f"      {name(r['COUNTY_NUMBER']):<14} "
              f"CITY {r['city_cov']*100:5.1f}%  "
              f"RES {r.get('residential_city_cov', 0)*100:5.1f}%  "
              f"({r['voters']:,} voters)")

    print(f'\n[2] Jurisdiction-hierarchy coverage for blank-CITY counties')
    print('    For each county with < 50% CITY, the fallback columns the '
          'resolver relies on:')
    print(f"      {'county':<14} {'CITY':>6} {'VILL':>6} {'WARD':>6} "
          f"{'TWP':>6} {'ANY-AUTH':>9}")
    for r in weak.sort('any_auth_cov').head(40).to_dicts():
        print(f"      {name(r['COUNTY_NUMBER']):<14} "
              f"{r.get('city_cov', 0)*100:5.1f}% "
              f"{r.get('village_cov', 0)*100:5.1f}% "
              f"{r.get('ward_cov', 0)*100:5.1f}% "
              f"{r.get('township_cov', 0)*100:5.1f}% "
              f"{r.get('any_auth_cov', 0)*100:8.1f}%")
    print(f'\n    -> TRUE postal-last-resort counties (no authoritative column, '
          f'ANY-AUTH < {POSTAL_ONLY_CITY_COVERAGE*100:.0f}%): {postal_only.height}')
    print('       Only these legitimately fall through to RESIDENTIAL_CITY. '
          'Expected: Wyandot (88) only.')
    for r in postal_only.to_dicts():
        print(f"      {name(r['COUNTY_NUMBER']):<14} "
              f"ANY-AUTH {r['any_auth_cov']*100:4.1f}%  "
              f"RES {r.get('residential_city_cov', 0)*100:5.1f}%")

    conflicts = township_postal_conflicts(lf)
    print(f'\n[3] Township/Village precincts mislabeled via postal city: '
          f'{conflicts.height}')
    print('    (NAME is a township/village, CITY blank, postal city populated)')
    for r in conflicts.head(args.show).to_dicts():
        print(f"      {name(r['COUNTY_NUMBER']):<12} "
              f"{r['PRECINCT_NAME']:<26} postal={r['postal_city']:<16} "
              f"({r['voters']:,} voters)")
    if conflicts.height > args.show:
        print(f'      ... +{conflicts.height - args.show} more '
              f'(raise --show to see all)')

    # B1 (operator decision 2026-07-10): --strict gates on the resolver-
    # CONFIRMED subset from [3b], not the raw non-postal-county conflict
    # count -- the raw count (1,778 on 2026-07-04 data) is dominated by
    # precincts the resolver's TOWNSHIP/VILLAGE/WARD steps already handle,
    # exactly as [3]'s own NOTE states.
    postal_set = set(postal_only['COUNTY_NUMBER'].to_list())
    cls = classify_township_conflicts(lf, conflicts, postal_set)
    confirmed = cls['confirmed']
    benign = cls['township'] + cls['village'] + cls['city_auth']
    print(f'\n[3b] Resolver outcome for the [3] conflicts (B1 --strict basis)')
    print(f'    benign -- resolver lands on an authoritative column (WARN,')
    print(f'    tracks the postal-fallback risk surface only): {benign}')
    print(f"      township: {cls['township']}   village: {cls['village']}   "
          f"city via authoritative CITY/WARD rows: {cls['city_auth']}")
    if cls['unresolved']:
        print(f"    unresolved by the place resolver (see [5]): "
              f"{len(cls['unresolved'])}")
    print(f'    CONFIRMED postal mislabels -- resolver falls through to')
    print(f'    RESIDENTIAL_CITY in a county WITH authoritative columns')
    print(f'    (gates --strict): {len(confirmed)}')
    for m in confirmed[:args.show]:
        print(f"      {m['county_name']:<12} {m['precinct']:<26} "
              f"postal={m['postal_city']:<16} ({m['voters']:,} voters)")

    vit = village_in_township_candidates(lf)
    print('\n' + '!' * 70)
    print(f'[4] KNOWN LIMITATION -- village-in-township (MANUAL REVIEW): '
          f'{vit.height}')
    print('!' * 70)
    print('    These precincts resolve to None ("not a city") because their')
    print('    TOWNSHIP column is populated -- but the value is a COMPOSITE place')
    print('    code (e.g. NEVADA -> "NEVADA ANTRIM"), so a real incorporated')
    print('    village may be UNDER-CLAIMED. By decision (2026-06) we do NOT auto-')
    print('    correct this; review each row and assign the village manually if')
    print('    warranted. See memory: city-resolution-village-in-township.')
    for r in vit.sort('COUNTY_NUMBER').to_dicts():
        print(f"      {name(r['COUNTY_NUMBER']):<12} "
              f"{r['PRECINCT_NAME']:<22} township={r['township']!r:<22} "
              f"({r['voters']:,} voters)  -> currently None")

    placeless_count = 0
    ward_defects: list[str] = []
    splits: list = []
    collision_ok = True
    abuse_rows: list = []
    rule4_no_token = 0
    uniformity_violations = []
    sd_violations = []
    if not args.skip_ward:
        placeless_count = check_place_completeness(lf, args.show)
        ward_defects = check_ward_coverage(lf)
        splits = check_split_precincts(lf, args.show)
        collision_ok = check_ward_collision_safety(lf)
        abuse_rows = check_ward_schema_abuse(lf)

        # [10] Encoding census: per-county resolver rule-precedence assertions
        # (encoding_census.py). RULE4-NO-TOKEN is a structural invariant --
        # the resolver's own token gate makes it impossible unless the
        # resolver regresses (2026-07-11 fix; pre-fix baseline 166 precincts,
        # 153,833 voters, 4 counties). The other checks are informational
        # per-drop measurements of known approximations.
        print('\n[10] ENCODING CENSUS (rule-precedence assertions, all 88)')
        census = run_census(lf)
        census_summarize(census, args.show)
        rule4_no_token = sum(1 for v in census['violations']
                             if v['check'] == 'RULE4-NO-TOKEN')

        print('\n[11] EARNED-UNIFORMITY GUARD')
        uniformity_violations = check_earned_uniformity(lf)
        if not uniformity_violations:
            print('  OK (all multi-county cities share district identity or are allowlisted)')
        else:
            for v in uniformity_violations:
                print(f"  FAIL: {v['city']} spans {', '.join(v['counties'])} but has no shared district identity")

        print('\n[12] SCHOOL DISTRICT GUARD')
        sd_violations = check_school_district_collisions(lf)
        if not sd_violations:
            print('  OK (all multi-county local/exempted school districts have a disambiguating county suffix)')
        else:
            for v in sd_violations:
                print(f"  FAIL: {v['district_name']} ({v['district_type']}) spans {', '.join(v['counties'])} without suffix")

    print('\n[ledger] assembling defects for DATA_QUALITY.md...')
    if args.ledger:
        if args.skip_ward:
            print('ERROR: --ledger requires sections [5]-[9] (drop --skip-ward)',
                  file=sys.stderr)
            return 2
        ledger_rows = build_ledger_rows(ward_defects, abuse_rows, splits,
                                        confirmed, uniformity_violations,
                                        sd_violations)
        write_ledger(ledger_rows, date.today().isoformat())

    print(f'\n[SUMMARY]')
    print(f'  township/postal name conflicts total:                 '
          f'{conflicts.height}')
    print(f'  resolver-benign (WARN; county has auth columns):       '
          f'{benign}')
    print(f'  CONFIRMED postal mislabels (gates --strict):           '
          f'{len(confirmed)}')
    print(f'  true postal-last-resort counties (no auth col):        '
          f'{postal_only.height}  (expected: 1 = Wyandot)')
    print('  NOTE: the resolver maps these conflicts to None (= "not a city")')
    print('        via the TOWNSHIP step, so they are NOT mislabeled at runtime.')
    print('        This count tracks the postal-fallback risk surface over time.')
    if not args.skip_ward:
        print(f'  place-less precincts (MUST be zero):                   {placeless_count}')
        print(f'  ward coverage DEFECTs (state-data gaps, tracked in '
              f'DATA_QUALITY.md): {len(ward_defects)}')
        print(f'  split precincts:                                       '
              f'{len(splits)}  (baseline {SPLIT_PRECINCT_BASELINE})')
        print(f'  ward collision safety:                                 '
              f'{"OK" if collision_ok else "FAIL"}')
        distinct_abuse = len({(r['county_name'], r['ward_value']) for r in abuse_rows})
        print(f'  ward schema-abuse values:                              '
              f'{distinct_abuse}  (baseline {WARD_ABUSE_BASELINE})')
        print(f'  census RULE4-NO-TOKEN precincts (MUST be zero):        '
              f'{rule4_no_token}')

    if args.strict:
        failures = []
        if confirmed:
            failures.append(f'{len(confirmed)} resolver-confirmed postal '
                             f'mislabels (fall-through to RESIDENTIAL_CITY)')
        if placeless_count:
            failures.append(f'{placeless_count} place-less precincts (must be zero)')
        if not collision_ok:
            failures.append('ward slug collision/orphan check failed (must be zero)')
        if rule4_no_token:
            failures.append(f'{rule4_no_token} census RULE4-NO-TOKEN precincts '
                             f'(resolver token-gate regression; must be zero)')
        if uniformity_violations:
            failures.append(f'{len(uniformity_violations)} unearned multi-county merges (must be zero or allowlisted)')
        if sd_violations:
            failures.append(f'{len(sd_violations)} school district collisions (must be zero)')
        if len(splits) > SPLIT_PRECINCT_BASELINE:
            failures.append(f'split precincts grew to {len(splits)} '
                             f'(baseline {SPLIT_PRECINCT_BASELINE})')
        distinct_abuse = len({(r['county_name'], r['ward_value']) for r in abuse_rows})
        if distinct_abuse > WARD_ABUSE_BASELINE:
            failures.append(f'ward schema-abuse values grew to {distinct_abuse} '
                             f'(baseline {WARD_ABUSE_BASELINE})')
        if failures:
            print('\nFAIL:', file=sys.stderr)
            for f in failures:
                print(f'  - {f}', file=sys.stderr)
            return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
