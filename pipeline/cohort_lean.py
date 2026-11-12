"""Party-lean arithmetic over a published party-affiliation chart, keyed by cohort label.

Every consumer that turns a ``*_party_affiliation.json`` chart into R / unaffiliated / D
shares goes through ``cohort_sides()``: the narrative templates, both map builders, and
(by a mirrored ``COHORT_SIDE`` table) the dashboard in ``docs/assets/v2.js``.

Why labels and not positions: the chart's ``data`` array follows ``COHORT_SLICES`` order,
and that order changed when the UNC - Non-partisan Ballot cohort was inserted at index 4
(and again when Minor Party took index 4 in 2026-11). Position-based sums kept running
and silently counted UNC - No Primary as Democratic.
Matching on labels makes the arithmetic independent of order, and requiring the exact
canonical label set makes any future change to the taxonomy fail loudly here instead of
producing plausible wrong numbers downstream.

The label set must match ``COHORT_SLICES`` in ``pipeline/voter_data_cleaner.py`` and the
``COHORT_SIDE`` table in ``docs/assets/v2.js``; ``tools/tests/test_cohort_lean.py`` pins
all three together.
"""
from __future__ import annotations

# Canonical cohort label -> partisan side. 'R' and 'D' are the lean numerator; 'UNC'
# counts toward the total only. Order mirrors COHORT_SLICES for readability; lookups
# never depend on it.
COHORT_SIDE: dict[str, str] = {
    'Pure R':                    'R',
    'UNC – Lapsed R':            'R',
    'Mixed – Active':            'UNC',
    'Mixed – Lapsed':            'UNC',
    'Minor Party':               'UNC',
    'UNC – Non-partisan Ballot': 'UNC',
    'UNC – No Primary':          'UNC',
    'UNC – Lapsed D':            'D',
    'Pure D':                    'D',
}

# Some writers append the count and share to each label: "Pure R — 3,808 (3%)".
# The separator is an em dash (U+2014); cohort names themselves use an en dash (U+2013),
# so splitting on the em dash never cuts a cohort name.
_COUNT_SUFFIX_SEP = ' — '


def normalize_label(label: object) -> str:
    """Strip an optional ' — N (p%)' count suffix, leaving the canonical cohort name."""
    return str(label).split(_COUNT_SUFFIX_SEP)[0].strip()


def cohort_sides(chart_config: dict, source: str) -> dict:
    """Sum a party-affiliation chart into partisan sides.

    Returns ``{'r', 'unc', 'd', 'pure_r', 'pure_d', 'total'}`` as numbers.
    Raises ``ValueError`` unless the chart carries exactly the canonical cohort labels,
    once each: a 7-cohort file (pre-2026-07-25 output), the 3-label statewide file, or an
    unknown cohort are all rejected rather than summed.
    """
    labels = chart_config['labels']
    data = chart_config['datasets'][0]['data']
    if len(labels) != len(data):
        raise ValueError(f'{source}: {len(labels)} labels but {len(data)} data values')

    names = [normalize_label(l) for l in labels]
    expected = set(COHORT_SIDE)
    seen = set(names)
    if len(names) != len(seen) or seen != expected:
        missing = sorted(expected - seen)
        extra = sorted(seen - expected)
        raise ValueError(
            f'{source}: cohort labels do not match the canonical {len(expected)}-cohort set '
            f'(missing={missing}, unexpected={extra}, count={len(names)})'
        )

    # Keep the counts' own type (ints in every published chart): a float total
    # renders as "123,527.0 registered voters" in narrative prose.
    by_name = {n: (v or 0) for n, v in zip(names, data)}
    sides = {'R': 0, 'UNC': 0, 'D': 0}
    for name, value in by_name.items():
        sides[COHORT_SIDE[name]] += value
    return {
        'r': sides['R'],
        'unc': sides['UNC'],
        'd': sides['D'],
        'pure_r': by_name['Pure R'],
        'pure_d': by_name['Pure D'],
        'total': sides['R'] + sides['UNC'] + sides['D'],
    }
