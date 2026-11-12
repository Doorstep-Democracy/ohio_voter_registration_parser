"""County export: each worker takes its own slice of the statewide frame.

run_ohio_analysis used to filter all 88 counties out of the statewide frame
before starting the thread pool, which held a second full copy of the data for
the whole export stage. The worker now slices on entry. These tests pin that
the slice is the same rows in the same order as before, that the worker exports
exactly that slice, and that the driver no longer slices up front.
"""

from __future__ import annotations

import inspect
import logging

import polars as pl
from polars.testing import assert_frame_equal

from pipeline import voter_data_cleaner as vdc

STATEWIDE = pl.DataFrame({
    "SOS_VOTERID": [f"OH{i}" for i in range(9)],
    # Padded values on purpose: the county filter compares the stripped value.
    "COUNTY_NUMBER": ["01", "25", " 01", "57 ", "25", "01", "57", " 25 ", "88"],
    "cohort": list("ABCDEFGHI"),
})


def _old_upfront_slice(df: pl.DataFrame, county_num: str) -> pl.DataFrame:
    """The expression run_ohio_analysis used before the change, verbatim."""
    return df.filter(pl.col("COUNTY_NUMBER").str.strip_chars() == county_num)


def test_worker_slice_equals_the_old_upfront_slice_for_every_county():
    counties = sorted(STATEWIDE["COUNTY_NUMBER"].str.strip_chars().unique().to_list())
    assert counties == ["01", "25", "57", "88"]
    total = 0
    for county_num in counties:
        new = vdc._county_slice(STATEWIDE, county_num)
        assert_frame_equal(new, _old_upfront_slice(STATEWIDE, county_num))
        total += new.height
    assert total == STATEWIDE.height          # every row lands in exactly one county


def test_slice_keeps_frame_order():
    assert vdc._county_slice(STATEWIDE, "01")["SOS_VOTERID"].to_list() == ["OH0", "OH2", "OH5"]


def test_slice_of_an_absent_county_is_empty_with_the_same_schema():
    empty = vdc._county_slice(STATEWIDE, "99")
    assert empty.height == 0 and empty.schema == STATEWIDE.schema


def test_worker_exports_its_own_slice_of_the_shared_frame(monkeypatch):
    seen = {}

    def _export_json(county_name, county_df, election_cols, logger, **kwargs):
        seen["json"] = (county_name, county_df, kwargs)

    def _export_shadow(county_name, county_df, primary_cols, logger, **kwargs):
        seen["shadow"] = (county_name, county_df)

    monkeypatch.setattr(vdc, "export_json", _export_json)
    monkeypatch.setattr(vdc, "export_unc_shadow_json", _export_shadow)
    monkeypatch.setattr(vdc, "_unc_classified_from_enriched_df", lambda df: "classified")

    result = vdc._export_county_worker(("25", STATEWIDE, [], True))

    assert result == (vdc.OHIO_COUNTIES["25"], None)
    expected = _old_upfront_slice(STATEWIDE, "25")
    assert_frame_equal(seen["json"][1], expected)
    assert_frame_equal(seen["shadow"][1], expected)
    assert seen["json"][2]["include_precinct_charts"] is True
    assert seen["json"][2]["unc_classified"] == "classified"


def test_worker_reports_a_failure_as_text_and_does_not_raise(monkeypatch):
    def _boom(*args, **kwargs):
        raise RuntimeError("disk full")

    monkeypatch.setattr(vdc, "export_json", _boom)
    monkeypatch.setattr(vdc, "_unc_classified_from_enriched_df", lambda df: "classified")
    name, err = vdc._export_county_worker(("57", STATEWIDE, [], False))
    assert name == vdc.OHIO_COUNTIES["57"] and err == "disk full"


def test_driver_does_not_slice_counties_up_front():
    """Guard against the second copy coming back: the statewide entry point
    builds tasks from the shared frame and never filters it itself."""
    src = inspect.getsource(vdc.run_ohio_analysis)
    assert ".filter(" not in src
    assert "_export_county_worker" in src
