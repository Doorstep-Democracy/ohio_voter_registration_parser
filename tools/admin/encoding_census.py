"""Per-county x per-rule ENCODING CENSUS for the SWVF place resolver.

The systemic guard behind the "88-county assumption" (CLAUDE.md 5): the SWVF is
88 sovereign Boards of Elections under one fixed schema, and every historical
jurisdiction bug (DATA_QUALITY.md; the 2026-07-11 root-cause handoff) came from
a resolver-rule ORDERING assumption that held in the counties examined first
and failed silently in the long tail. This census converts that failure mode
into a report: for every county it replays the REAL place resolver
(pipeline.voter_data_cleaner._place_per_precinct -- never a copy; single-
resolver rule, CLAUDE.md 5), reads each precinct's claiming rule from the
resolver's own 'rule' attribution, and asserts each rule's precedence
assumptions, emitting violators ranked by voter count.

Checks (per precinct, aggregated per county):
  RULE4-NO-TOKEN   rule 4 claimed a precinct whose dominant WARD value
                   passes NEITHER of the resolver's own gates (township
                   token / municipality-prefix token, both imported from
                   voter_data_cleaner). Structurally zero since the
                   2026-07-11 token-gate fix; a nonzero count means the
                   resolver regressed. Pre-fix baseline: 166 precincts,
                   153,833 voters, 4 counties (Stark WARD='JACKSON' et al.).
  RULE4-OVER-TWP   rule 4 claimed a precinct whose dominant TOWNSHIP is also
                   populated -- rule 5 had an answer and lost on precedence.
                   Informational: legitimate city-ward precincts can carry a
                   minority township value.
  CITY-MINORITY-TWP    a city/village-typed precinct where >= threshold of
                       rows are township-shaped (TOWNSHIP populated, CITY and
                       VILLAGE blank). The one-precinct-one-node dominance
                       approximation is accepted (two-totals, CLAUDE.md 4);
                       this measures its per-county exposure.
  TWP-MINORITY-CITY    the inverse: a township-typed precinct with city rows.
  POSTAL-FALLTHROUGH   rule 6 (postal RESIDENTIAL_CITY) claimed a precinct in
                       a county that HAS authoritative coverage -- postal should
                       be the last resort of the no-auth tail only.

Also emits the per-county encoding-mechanism profile (which fields are
populated, WARD token convention, township tokens in precinct names) -- the
"6 mechanisms across 88 counties" tabulation from the 2026-07-11 root-cause
analysis, kept live so a future SWVF drop that moves a county between
mechanism classes is visible immediately.

Read-only. Bounded output. Importable: validate_jurisdiction_fields.py runs
run_census() as its section [10] so every new SWVF drop flags violators
automatically. CLI:
    python tools/admin/encoding_census.py [--parquet PATH] [--show N]
                                          [--json OUT.json]
"""
from __future__ import annotations
import argparse
import json
import sys
from pathlib import Path

import polars as pl

_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
if str(Path(__file__).resolve().parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
from pipeline.voter_data_cleaner import (  # noqa: E402
    _dominant_per_precinct,
    _place_per_precinct,
    _TOWNSHIP_NAME_RE,
    _WARD_CITY_PREFIX_RE,
)
from data_profile import (  # noqa: E402
    OHIO_COUNTIES,
    coverage_by_county,
    iter_county_frames,
    nonblank,
)

# The --parquet default, from the single resolver; the flag still overrides it.
from pipeline.paths import ENRICHED_CACHE as DEFAULT_PARQUET  # noqa: E402
CENSUS_COLS = ['COUNTY_NUMBER', 'PRECINCT_NAME', 'CITY', 'VILLAGE', 'WARD',
               'TOWNSHIP', 'RESIDENTIAL_CITY']
# Rule 4's gates are IMPORTED from the resolver (_WARD_CITY_PREFIX_RE,
# _TOWNSHIP_NAME_RE) -- never re-declared here. A private census copy is how a
# checker drifts from the thing it checks (single-source rule, CLAUDE.md 5).
# A county "has authoritative coverage" above this any_auth_cov; below it,
# postal fall-through is the expected last resort, not a violation. Mirrors
# validate_jurisdiction_fields.POSTAL_ONLY_CITY_COVERAGE.
AUTH_COVERAGE_FLOOR = 0.10
# Minority-rows exposure threshold for the dominance checks.
DOMINANCE_MINORITY_THRESHOLD = 0.10

RULE_LABEL = {
    1: 'precinct-twp-token', 2: 'CITY', 3: 'VILLAGE',
    4: 'WARD-prefix', 5: 'TOWNSHIP-col', 6: 'postal',
}


def census_county(df: pl.DataFrame, county_num: str, county_name: str,
                  county_has_auth: bool) -> tuple[list[dict], list[dict]]:
    """Census one county: (per-rule tallies, violations). df must carry the
    CENSUS_COLS present in the stream (COUNTY_NUMBER partition-key column
    re-added by the caller's scan; CLAUDE.md 5)."""
    place = _place_per_precinct(df)
    dom_ward = _dominant_per_precinct(df, 'WARD') if 'WARD' in df.columns else {}
    dom_twp = (_dominant_per_precinct(df, 'TOWNSHIP')
               if 'TOWNSHIP' in df.columns else {})

    shape = (
        df.group_by('PRECINCT_NAME')
          .agg(
              pl.len().alias('voters'),
              nonblank('CITY').sum().alias('city_rows'),
              (nonblank('TOWNSHIP') & ~nonblank('CITY') & ~nonblank('VILLAGE'))
                  .sum().alias('twp_rows'),
          )
    )
    stats = {r['PRECINCT_NAME']: r for r in shape.iter_rows(named=True)}

    tally: dict[int, dict] = {}
    violations: list[dict] = []

    def flag(check: str, pname: str, voters: int, detail: str) -> None:
        violations.append({
            'check': check, 'county_num': county_num, 'county': county_name,
            'precinct': pname, 'voters': voters, 'detail': detail,
        })

    for pname, resolved in place.items():
        st = stats.get(pname)
        voters = st['voters'] if st else 0
        rule = resolved['rule']
        t = tally.setdefault(rule, {'precincts': 0, 'voters': 0})
        t['precincts'] += 1
        t['voters'] += voters

        if rule == 4:
            ward_val = dom_ward.get(pname, '')
            gated = (_TOWNSHIP_NAME_RE.search(ward_val.upper())
                     or _WARD_CITY_PREFIX_RE.search(ward_val))
            if not gated:
                flag('RULE4-NO-TOKEN', pname, voters,
                     f"WARD={ward_val!r} -> phantom city {resolved['name']!r}")
            if pname in dom_twp:
                flag('RULE4-OVER-TWP', pname, voters,
                     f"WARD={ward_val!r} outranked TOWNSHIP={dom_twp[pname]!r}")
        if rule == 6 and county_has_auth:
            flag('POSTAL-FALLTHROUGH', pname, voters,
                 f"postal city {resolved['name']!r} in an auth-covered county")
        if st and voters:
            if (resolved['type'] in ('city', 'village')
                    and st['twp_rows'] / voters >= DOMINANCE_MINORITY_THRESHOLD):
                flag('CITY-MINORITY-TWP', pname, st['twp_rows'],
                     f"{st['twp_rows']}/{voters} township-shaped rows in "
                     f"{resolved['type']} {resolved['name']!r}")
            elif (resolved['type'] == 'township'
                    and st['city_rows'] / voters >= DOMINANCE_MINORITY_THRESHOLD):
                flag('TWP-MINORITY-CITY', pname, st['city_rows'],
                     f"{st['city_rows']}/{voters} city rows in township "
                     f"{resolved['name']!r}")

    per_rule = [{'county_num': county_num, 'county': county_name, 'rule': r,
                 'rule_label': RULE_LABEL[r], **v}
                for r, v in sorted(tally.items())]
    return per_rule, violations


def mechanism_profile(lf: pl.LazyFrame) -> pl.DataFrame:
    """Per-county encoding-mechanism classification (the 4B tabulation, live).
    Coverage from data_profile.coverage_by_county plus two value-level
    signals: the WARD token rate and township tokens in precinct names."""
    cov = coverage_by_county(lf)
    ward_tok = (
        lf.filter(nonblank('WARD'))
          .group_by('COUNTY_NUMBER')
          .agg(
              pl.len().alias('ward_rows'),
              pl.col('WARD').str.contains(_WARD_CITY_PREFIX_RE.pattern)
                  .sum().alias('ward_tok_rows'),
          )
          .collect()
    )
    out = (
        cov.join(ward_tok, on='COUNTY_NUMBER', how='left')
           .with_columns(
               (pl.col('ward_tok_rows') / pl.col('ward_rows'))
                   .fill_null(0.0).alias('ward_tok_rate'))
    )

    def classify(r: dict) -> str:
        m = []
        if r['city_cov'] >= 0.05:
            m.append('CITY')
        if r['ward_cov'] >= 0.05:
            m.append('WARD-prefix' if r['ward_tok_rate'] >= 0.5
                      else 'WARD-tokenless')
        if r['any_auth_cov'] < AUTH_COVERAGE_FLOOR:
            m.append('POSTAL-only')
        return '+'.join(m) if m else 'precinct/twp-only'

    rows = out.sort('COUNTY_NUMBER').to_dicts()
    return pl.DataFrame({
        'COUNTY_NUMBER': [r['COUNTY_NUMBER'] for r in rows],
        'county': [OHIO_COUNTIES.get(r['COUNTY_NUMBER'], '?') for r in rows],
        'mechanism': [classify(r) for r in rows],
        'city_cov': [round(r['city_cov'], 3) for r in rows],
        'ward_cov': [round(r['ward_cov'], 3) for r in rows],
        'township_cov': [round(r['township_cov'], 3) for r in rows],
        'any_auth_cov': [round(r['any_auth_cov'], 3) for r in rows],
        'ward_tok_rate': [round(r['ward_tok_rate'], 3) for r in rows],
    })


def run_census(lf: pl.LazyFrame) -> dict:
    """Full 88-county census. Returns {'per_rule': DataFrame,
    'violations': list[dict] (ranked by voters desc),
    'mechanisms': DataFrame}. Single-threaded per-county loop by design
    (CLAUDE.md 5)."""
    mech = mechanism_profile(lf)
    auth_ok = {r['COUNTY_NUMBER']: r['any_auth_cov'] >= AUTH_COVERAGE_FLOOR
               for r in mech.iter_rows(named=True)}

    per_rule_rows: list[dict] = []
    violations: list[dict] = []
    for num, cname, _slug, df in iter_county_frames(lf, CENSUS_COLS):
        pr, vio = census_county(df, num, cname, auth_ok.get(num, True))
        per_rule_rows.extend(pr)
        violations.extend(vio)

    violations.sort(key=lambda v: -v['voters'])
    return {
        'per_rule': pl.DataFrame(per_rule_rows),
        'violations': violations,
        'mechanisms': mech,
    }


def summarize(result: dict, show: int = 15) -> None:
    """Bounded ASCII console report (cp1252-safe, CLAUDE.md 6)."""
    mech = result['mechanisms']
    print('\n[census] per-county encoding mechanisms '
          f'({mech.height} counties):')
    tally = (mech.group_by('mechanism').agg(pl.len().alias('n'))
                 .sort('n', descending=True))
    for r in tally.iter_rows(named=True):
        members = mech.filter(pl.col('mechanism') == r['mechanism'])
        sample = ', '.join(members['county'].head(4).to_list())
        more = f', +{members.height - 4} more' if members.height > 4 else ''
        print(f"    {r['mechanism']:<28} {r['n']:>3}  ({sample}{more})")

    print('\n[census] resolver rule usage (voters claimed per rule):')
    pr = result['per_rule']
    for r in (pr.group_by(['rule', 'rule_label'])
                .agg(pl.col('precincts').sum(), pl.col('voters').sum())
                .sort('rule').iter_rows(named=True)):
        print(f"    rule {r['rule']} {r['rule_label']:<20} "
              f"{r['precincts']:>6,} precincts  {r['voters']:>10,} voters")

    print('\n[census] rule-precedence violations, ranked by voters:')
    by_check: dict[str, dict] = {}
    for v in result['violations']:
        agg = by_check.setdefault(v['check'],
                                  {'n': 0, 'voters': 0, 'counties': set()})
        agg['n'] += 1
        agg['voters'] += v['voters']
        agg['counties'].add(v['county'])
    if not by_check:
        print('    none')
    for check, agg in sorted(by_check.items(), key=lambda kv: -kv[1]['voters']):
        print(f"    {check:<20} {agg['n']:>5} precincts  "
              f"{agg['voters']:>10,} voters  "
              f"{len(agg['counties'])} counties")
    shown = result['violations'][:show]
    if shown:
        print(f'\n    top {len(shown)} violating precincts:')
        for v in shown:
            print(f"      {v['check']:<18} {v['county']:<12} "
                  f"{v['precinct']:<28} {v['voters']:>7,}  {v['detail']}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--parquet', type=Path, default=DEFAULT_PARQUET)
    ap.add_argument('--show', type=int, default=15,
                    help='max violating precincts to print')
    ap.add_argument('--json', type=Path, default=None,
                    help='write the full violation list + mechanisms to JSON')
    args = ap.parse_args()
    if not args.parquet.exists():
        print(f'ERROR: parquet not found: {args.parquet}', file=sys.stderr)
        return 2

    lf = pl.scan_parquet(args.parquet)
    result = run_census(lf)
    print('=' * 70)
    print('ENCODING CENSUS (88-county resolver rule-precedence assertions)')
    print(f'  source: {args.parquet}')
    print('=' * 70)
    summarize(result, args.show)

    if args.json:
        payload = {
            'mechanisms': result['mechanisms'].to_dicts(),
            'per_rule': result['per_rule'].to_dicts(),
            'violations': result['violations'],
        }
        args.json.write_text(json.dumps(payload, indent=1), encoding='utf-8')
        print(f'\n[census] wrote JSON: {args.json}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
