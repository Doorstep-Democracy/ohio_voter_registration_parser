"""
Precinct persuadability scoring from voter-file data alone.

WHAT THIS IS
------------
A per-precinct ranking of where partisan contest and unexpressed electorate
coincide -- i.e. where canvassing time plausibly changes an outcome, as opposed
to where a result is already settled. Built entirely from Ohio SWVF registration
and primary-ballot history. No election results are used (see LIMITS).

WHY IT IS NOT A SUM OF INDIVIDUAL SCORES
----------------------------------------
The obvious design -- score each voter's persuadability, average per precinct --
is not supportable by this data, and the profile that killed it is worth
recording (Montgomery County, 2026-07-23):

  * `crossover_class` is NULL for 86.4% of voters (311,534 / 360,416)
  * `switch_count > 0` covers 0.85% of voters (3,049) -- ~8 per precinct
  * `lean_score` is NULL for 46.3% -- by construction, everyone with zero
    partisan primaries, which is exactly the population of interest

Individual-level swing signal exists for well under 1% of the electorate, and
the ~46% who are most interesting have no partisan signal at all. Persuadability
here is therefore a property of a PLACE (its composition), never a claim about
a person. This matches what the data can actually support; see
local/context/scope/partisan_cohort_semantics_and_map_parameters.md.

THE TWO AXES
------------
Measured independently because they are empirically independent --
corr(closeness, unexpressed) = 0.019 across Montgomery's 381 precincts, with
all four quadrants populated (89/101/102/89 on a median split). One combined
number would destroy the distinction that makes the output actionable.

  closeness    1 - |D-R| / (D+R)    Among voters who have EXPRESSED a side,
                                    how balanced is the precinct? 1.0 = tied.
  unexpressed  U / total            Share with no usable partisan signal.

The quadrants mean different things and imply different work:

  tight  + high unexpressed  -> BATTLEGROUND. Contested and lots of upside.
  tight  + low  unexpressed  -> TURNOUT FIGHT. Contested, everyone has a side.
  lopsided + high unexpressed -> REGISTRATION/BASE. Not a persuasion target;
                                a mobilisation or long-term growth target.
  lopsided + low  unexpressed -> SETTLED. Deprioritise for persuasion.

This is the distinction the choropleth cannot make: on the map, a genuinely
divided precinct and a merely-unexpressed one are the same grey.

LIMITS -- READ BEFORE USING TO DIRECT PEOPLE
--------------------------------------------
1. Ohio has NO party registration. Every cohort is derived from which primary
   ballot a voter requested (open primary, public record). "Pure R" is a
   consistent past ACT, not an identity or membership.
2. Primary behaviour is not general-election behaviour. This data cannot see
   November choices at all.
3. This ranks PLACES. It says nothing about any household or individual, and
   must not be used to infer one.
4. No election results are incorporated yet. Until they are, "persuadable" is a
   structural inference (contested composition), not a validated prediction.
   Results ingestion is the planned next step.

USAGE
-----
    python tools/scoring/persuadability_score.py --county 57
    python tools/scoring/persuadability_score.py --county 57 --csv out.csv
    python tools/scoring/persuadability_score.py --all-counties --level county
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parent.parent.parent
ENRICHED = ROOT / 'local' / 'source' / 'parquet_enriched' / 'enriched_voters.parquet'
COUNTIES_GEOJSON = ROOT / 'docs' / 'data' / 'state_map' / 'counties.geojson'


def county_names() -> dict[str, str]:
    """Map zero-padded COUNTY_NUMBER -> county name.

    Ohio numbers counties alphabetically (Adams=01 ... Wyandot=88). There is no
    shared constant for this in the repo and the published geojson carries names
    but no number, so the mapping is derived rather than hardcoded -- a 89th
    copy of the county list is one more thing to drift.

    The ordering is not assumed: verified 2026-07-23 by matching each county's
    published `total_voters` against its enriched row count, 88/88 within 1%
    (the residual is the OTHER cohort the published export drops).
    """
    if not COUNTIES_GEOJSON.exists():
        return {}
    import json
    gj = json.loads(COUNTIES_GEOJSON.read_text(encoding='utf-8'))
    names = sorted(f['properties']['name'] for f in gj['features'])
    return {f'{i + 1:02d}': n for i, n in enumerate(names)}

# Cohort -> side. Mirrors the published 7-slice export (COHORT_SLICES in
# pipeline/voter_data_cleaner.py) but is stated here explicitly rather than
# inherited, because this tool makes a DIFFERENT choice about OTHER (below).
R_COHORTS = ('PURE_R', 'UNC_LAPSED_R')
D_COHORTS = ('PURE_D', 'UNC_LAPSED_D')
# Everyone with no usable one-sided partisan signal. CROSSOVER_* are affiliated
# voters with opposing history -- genuinely cross-pressured, so they belong with
# the unexpressed rather than counted for a side.
U_COHORTS = ('CROSSOVER_R', 'CROSSOVER_D', 'UNC_MIXED',
             # Turned out, but only ever for the non-partisan ballot. Split
             # out of UNC_MIXED when the behaviour taxonomy was un-stapled
             # from the time taxonomy; they have no partisan signal at all,
             # so they are unexpressed by definition. Omitting them here
             # would silently drop 11,248 Montgomery voters from the
             # denominator rather than raising.
             'UNC_NONPARTISAN',
             'UNC_NO_PRIMARY',
             # Libertarian, Green and other parties: no D-vs-R signal, so
             # unexpressed on this axis. Before MINOR_PARTY existed these
             # voters were cohort OTHER (see below).
             'MINOR_PARTY')

# The classifier's .otherwise() fallthrough. Until 2026-11 it held every
# non-blank, non-R, non-D affiliation (795 Montgomery voters, 0.22%), which the
# published charts dropped; those voters are now MINOR_PARTY (in U_COHORTS
# above) and the classifier raises if any row still lands in OTHER. other_n
# should therefore be 0; it is kept so a regression shows as a non-zero column
# instead of a silently smaller denominator.
OTHER_COHORT = 'OTHER'

# Base-size shrinkage. A precinct's closeness is estimated from its expressed
# partisans; a small base makes an extreme value (0.0 or 1.0) unreliable. We
# shrink closeness toward the county mean by n/(n+K). Montgomery's partisan
# bases are uniform (min 111, median 405) so this barely moves anything there,
# but precinct sizing is a per-county property (ORC 3501.18 sizes precincts by
# equipment capacity, not population), so the correction must exist for
# counties where it does matter.
SHRINK_K = 150


def load_precincts(county: str | None, level: str) -> pl.DataFrame:
    """Aggregate voters to the requested geography with cohort side counts."""
    if not ENRICHED.exists():
        sys.exit(f'ERROR: enriched cache not found: {ENRICHED}\n'
                 'Run the pipeline first.')

    lf = pl.scan_parquet(ENRICHED)
    if county is not None:
        lf = lf.filter(pl.col('COUNTY_NUMBER') == county)

    group_cols = ['COUNTY_NUMBER'] if level == 'county' else ['COUNTY_NUMBER', 'PRECINCT_NAME']

    agg = (
        lf.group_by(group_cols)
          .agg([
              pl.col('cohort').is_in(R_COHORTS).sum().alias('r_side'),
              pl.col('cohort').is_in(D_COHORTS).sum().alias('d_side'),
              pl.col('cohort').is_in(U_COHORTS).sum().alias('unexpressed_n'),
              (pl.col('cohort') == 'UNC_NO_PRIMARY').sum().alias('no_primary_n'),
              (pl.col('cohort') == OTHER_COHORT).sum().alias('other_n'),
              pl.len().alias('total_roll'),
          ])
          .collect()
    )
    if agg.is_empty():
        sys.exit('ERROR: no rows matched. Check --county (zero-padded string, e.g. "57").')
    return agg


def score(df: pl.DataFrame) -> pl.DataFrame:
    """Add the two axes and the composite score.

    Derived columns are computed in explicit stages from already-materialised
    float columns. Chaining these into one with_columns over freshly-aggregated
    integer aliases produced silently wrong values (closeness ~ -9.5e6) during
    development -- integer division inside a when/then resolved against the
    aggregation aliases rather than the intended floats.
    """
    df = df.with_columns([
        pl.col('r_side').cast(pl.Float64).alias('_r'),
        pl.col('d_side').cast(pl.Float64).alias('_d'),
        pl.col('unexpressed_n').cast(pl.Float64).alias('_u'),
        pl.col('no_primary_n').cast(pl.Float64).alias('_np'),
    ])
    df = df.with_columns([
        (pl.col('_r') + pl.col('_d')).alias('partisan_n'),
    ])
    df = df.with_columns([
        (pl.col('partisan_n') + pl.col('_u')).alias('scored_n'),
    ])
    df = df.filter(pl.col('scored_n') > 0)

    df = df.with_columns([
        pl.when(pl.col('partisan_n') > 0)
          .then(1.0 - ((pl.col('_d') - pl.col('_r')).abs() / pl.col('partisan_n')))
          .otherwise(None).alias('closeness_raw'),
        (pl.col('_u') / pl.col('scored_n')).alias('unexpressed_share'),
        (pl.col('_np') / pl.col('scored_n')).alias('no_primary_share'),
        ((pl.col('_d') - pl.col('_r')) / pl.col('scored_n')).alias('lean_total'),
        pl.when(pl.col('partisan_n') > 0)
          .then((pl.col('_d') - pl.col('_r')) / pl.col('partisan_n'))
          .otherwise(None).alias('lean_partisan'),
    ])

    # Shrink toward the mean so a small partisan base cannot manufacture a
    # perfect 1.000 tie and top the ranking.
    mean_close = df['closeness_raw'].mean()
    df = df.with_columns([
        (pl.col('closeness_raw') * (pl.col('partisan_n') / (pl.col('partisan_n') + SHRINK_K))
         + pl.lit(mean_close) * (pl.lit(float(SHRINK_K)) / (pl.col('partisan_n') + SHRINK_K))
         ).alias('closeness')
    ])

    # Composite: the geometric mean of the two axes. Deliberately multiplicative
    # -- a precinct must score on BOTH to rank. An additive blend would let a
    # 0.82-unexpressed / lopsided precinct (the Brookville failure mode) climb
    # on one axis alone, which is the exact error this tool exists to prevent.
    df = df.with_columns([
        (pl.col('closeness') * pl.col('unexpressed_share')).sqrt().alias('persuadability')
    ])

    med_c = df['closeness'].median()
    med_u = df['unexpressed_share'].median()
    df = df.with_columns([
        pl.when((pl.col('closeness') >= med_c) & (pl.col('unexpressed_share') >= med_u))
          .then(pl.lit('BATTLEGROUND'))
          .when((pl.col('closeness') >= med_c) & (pl.col('unexpressed_share') < med_u))
          .then(pl.lit('TURNOUT'))
          .when((pl.col('closeness') < med_c) & (pl.col('unexpressed_share') >= med_u))
          .then(pl.lit('REGISTRATION'))
          .otherwise(pl.lit('SETTLED')).alias('quadrant')
    ])

    keep = [c for c in df.columns if not c.startswith('_')]
    return df.select(keep).sort('persuadability', descending=True)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--county', help='zero-padded county number, e.g. 57 for Montgomery')
    ap.add_argument('--all-counties', action='store_true', help='score every county')
    ap.add_argument('--level', choices=['precinct', 'county'], default='precinct')
    ap.add_argument('--top', type=int, default=20, help='rows to print (default 20)')
    ap.add_argument('--csv', help='write full results to this path')
    args = ap.parse_args()

    if not args.county and not args.all_counties:
        ap.error('specify --county NN or --all-counties')

    level = 'county' if args.all_counties and args.level == 'county' else args.level
    df = score(load_precincts(None if args.all_counties else args.county, level))

    names = county_names()
    if level == 'county' and names:
        df = df.with_columns(
            pl.col('COUNTY_NUMBER').replace_strict(names, default=None).alias('county_name')
        )
    label = 'PRECINCT_NAME' if level == 'precinct' else 'county_name'
    if label not in df.columns:
        label = 'COUNTY_NUMBER'

    where = ''
    if not args.all_counties:
        where = f' (county {args.county}'
        where += f' - {names[args.county]})' if args.county in names else ')'
    print(f'{len(df)} {level}s scored{where}\n')
    print(f'{"":<22}{"close":>7}{"unexp":>7}{"score":>7}  {"quadrant":<13}{"R":>6}{"D":>6}{"U":>7}')
    print('-' * 78)
    for r in df.head(args.top).to_dicts():
        print(f'{str(r[label])[:22]:<22}'
              f'{r["closeness"]:>7.3f}{r["unexpressed_share"]:>7.3f}'
              f'{r["persuadability"]:>7.3f}  {r["quadrant"]:<13}'
              f'{r["r_side"]:>6}{r["d_side"]:>6}{r["unexpressed_n"]:>7}')

    counts = df.group_by('quadrant').agg(pl.len().alias('n')).sort('n', descending=True)
    print('\nquadrant distribution:')
    for r in counts.to_dicts():
        print(f'  {r["quadrant"]:<14}{r["n"]:>5}')

    if args.csv:
        out = Path(args.csv)
        out.parent.mkdir(parents=True, exist_ok=True)
        df.write_csv(out)
        print(f'\nwrote {len(df)} rows -> {out}')


if __name__ == '__main__':
    main()
