"""Axis A segment predicates (tools/scoring/axis_a_segments.py, DOO-7).

Run against the real cohort classifier on hand-built histories
(tools/tests/axis_a_fixture.py), so each predicate is checked against the
columns the pipeline actually produces.
"""

import polars as pl
import pytest

from tools.scoring import axis_a_segments as seg
from tools.tests.axis_a_fixture import VOTERS, enriched_frame


@pytest.fixture(scope="module")
def enriched():
    return enriched_frame()


def _flags(df, expr):
    return dict(zip(df.get_column("SOS_VOTERID"), df.select(expr.alias("x")).get_column("x")))


def test_non_locked_crossers_truth_table(enriched):
    got = _flags(enriched, seg.non_locked_crossers())
    assert got == {vid: spec[4] for vid, spec in VOTERS.items()}


def test_fresh_defector_truth_table(enriched):
    got = _flags(enriched, seg.fresh_defector())
    assert got == {vid: spec[5] for vid, spec in VOTERS.items()}


def test_predicates_never_return_null(enriched):
    """A NULL crossover_class (every cohort but CROSSOVER_* and UNC_MIXED) must
    read False, so the predicates are safe to sum, not only to filter on."""
    out = enriched.select(seg.non_locked_crossers().alias("n"), seg.fresh_defector().alias("f"))
    assert out.null_count().row(0) == (0, 0)


def test_minor_party_crosser_is_excluded(enriched):
    """An L-affiliated voter with D and R ballots crossed, but is cohort
    MINOR_PARTY with no class, so is not a non-locked crosser."""
    v05 = enriched.filter(pl.col("SOS_VOTERID") == "V05").row(0, named=True)
    assert v05["ever_crossed"] is True
    assert v05["cohort"] == "MINOR_PARTY"
    assert v05["crossover_class"] is None
    assert _flags(enriched, seg.non_locked_crossers())["V05"] is False


def test_minor_party_ballot_stays_out_of_sequence(enriched):
    v06 = enriched.filter(pl.col("SOS_VOTERID") == "V06").row(0, named=True)
    assert v06["partisan_ballot_sequence"] == "RD"
    assert v06["m_primaries"] == 1


def test_consistent_low_frequency_needs_tenure_and_a_ballot(enriched):
    got = _flags(enriched, seg.consistent_low_frequency(tenure_floor=5))
    assert got["V08"] is True        # never crossed, partisan ballots, long tenure
    assert got["V10"] is False       # no partisan ballot
    assert got["V01"] is False       # crossed


def test_zero_floor_split(enriched):
    reachable = _flags(enriched, seg.zero_floor_reachable("GENERAL-11/05/2024"))
    inert = _flags(enriched, seg.zero_floor_inert(["GENERAL-11/08/2022", "GENERAL-11/05/2024"]))
    assert reachable["V10"] is False and inert["V10"] is True
    assert not any(v for k, v in reachable.items() if k != "V10")


def test_zero_floor_inert_requires_columns():
    with pytest.raises(ValueError):
        seg.zero_floor_inert([])


def test_crossing_turf_rate_is_an_aggregate(enriched):
    rates = enriched.group_by("COUNTY_NUMBER").agg(seg.crossing_turf_rate().alias("r"))
    by_county = dict(zip(rates.get_column("COUNTY_NUMBER"), rates.get_column("r")))
    assert by_county["09"] == pytest.approx(1 / 3)
