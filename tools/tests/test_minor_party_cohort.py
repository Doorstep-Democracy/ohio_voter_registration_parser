"""MINOR_PARTY cohort and the slice-coverage assertion in the universal classifier.

Regression for evaluation item P5: voters with a non-R/D affiliation (Libertarian, 'L')
fell to cohort OTHER, which no chart slice lists, so every party chart and county total
dropped them (11,904 statewide, 2026-10-24 snapshot) while the precinct index still
counted them. Minor-party primary ballots (G, C, ...) were also counted as non-partisan
X ballots, labelling those voters "never a party ballot".

Synthetic frame only: runs on a fresh checkout with no local/ data.
"""
import logging
from datetime import date

import pytest

pl = pytest.importorskip("polars")
from pipeline.voter_data_cleaner import COHORT_SLICES, classify_all_voters_primary_history

P20, P26 = "PRIMARY-03/17/2020", "PRIMARY-05/05/2026"

# (voter id, PARTY_AFFILIATION, 2020 ballot, 2026 ballot, expected cohort)
CASES = [
    ("L_ONLY",       "L", "",  "L", "MINOR_PARTY"),     # the P5 gap
    ("L_EX_D",       "L", "D", "L", "MINOR_PARTY"),     # affiliation is most recent: L
    ("N_AFFIL",      "N", "",  "",  "MINOR_PARTY"),     # the one N in the statewide file
    ("UNC_GREEN",    "",  "G", "",  "MINOR_PARTY"),     # was UNC_NONPARTISAN
    ("UNC_GREEN_X",  "",  "G", "X", "MINOR_PARTY"),     # a party ballot beats issues-only
    ("UNC_X",        "",  "X", "",  "UNC_NONPARTISAN"),
    ("UNC_R_THEN_G", "",  "R", "G", "UNC_LAPSED_R"),    # any D/R ballot keeps the D/R ladder
    ("PURE_R",       "R", "",  "R", "PURE_R"),
    ("PURE_D",       "D", "D", "D", "PURE_D"),
    ("NEVER",        "",  "",  "",  "UNC_NO_PRIMARY"),
]


def _frame(cases):
    return pl.DataFrame({
        "SOS_VOTERID":       [c[0] for c in cases],
        "PARTY_AFFILIATION": [c[1] for c in cases],
        "REGISTRATION_DATE": ["2010-01-01"] * len(cases),
        P20:                 [c[2] for c in cases],
        P26:                 [c[3] for c in cases],
    })


def _classify(df):
    return classify_all_voters_primary_history(
        df, [P20, P26], logging.getLogger("test"), reference_date=date(2026, 10, 24))


def test_cohorts():
    out = _classify(_frame(CASES))
    got = dict(zip(out["SOS_VOTERID"], out["cohort"]))
    assert got == {c[0]: c[4] for c in CASES}


def test_every_voter_lands_in_a_slice():
    """The invariant the charts rely on: slice counts sum to the row count."""
    out = _classify(_frame(CASES))
    families = {fam for fam, _, _ in COHORT_SLICES}
    assert set(out["cohort_family"]) <= families
    assert out.height == len(CASES)


def test_minor_party_ballots_are_not_counted_as_nonpartisan():
    out = _classify(_frame(CASES)).filter(pl.col("SOS_VOTERID") == "UNC_GREEN_X")
    assert (out["x_primaries"][0], out["m_primaries"][0]) == (1, 1)


def test_null_affiliation_raises_instead_of_dropping():
    """Blank is '' in the SWVF. A null would land outside every slice: fail loudly."""
    df = _frame(CASES).with_columns(
        pl.when(pl.col("SOS_VOTERID") == "NEVER").then(None)
          .otherwise(pl.col("PARTY_AFFILIATION")).alias("PARTY_AFFILIATION"))
    with pytest.raises(ValueError, match="outside COHORT_SLICES for 1 voters"):
        _classify(df)
