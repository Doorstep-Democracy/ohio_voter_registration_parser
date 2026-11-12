"""load_enriched_voters: cache-first ordering and projection (voter_data_cleaner).

The ordering is the fix for OPEN-001 incident 2: on a fresh enriched cache the
raw parquet must not be loaded at all, because holding the raw and enriched
frames together cost ~21 GB. These tests pin that with synthetic frames; no
voter data is read.
"""

from __future__ import annotations

import inspect
import logging

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from pipeline import voter_data_cleaner as vdc

LOG = logging.getLogger("test_enriched_loader")

# Column order mirrors the real frame's shape: static columns, then election
# columns in chronological order, then derived columns.
ENRICHED = pl.DataFrame({
    "SOS_VOTERID": ["OH1", "OH2", "OH3"],
    "COUNTY_NUMBER": ["01", "01", "25"],
    "PRIMARY-03/07/2000": ["R", "", "D"],
    "GENERAL-11/07/2000": ["X", "X", ""],
    "PRIMARY-05/05/2026": ["", "D", "D"],
    "cohort": ["A", "B", "C"],
})


def _forbid(name):
    def _raise(*args, **kwargs):
        raise AssertionError(f"{name} must not be called on this path")
    return _raise


@pytest.fixture
def fresh_cache(tmp_path, monkeypatch):
    """A fresh enriched cache on disk; every raw-side function is forbidden."""
    cache = tmp_path / "enriched_voters.parquet"
    ENRICHED.write_parquet(cache)
    monkeypatch.setattr(vdc, "ENRICHED_CACHE", cache)
    monkeypatch.setattr(vdc, "_cache_is_fresh", lambda logger=None: True)
    monkeypatch.setattr(vdc, "load_voter_files_parquet", _forbid("load_voter_files_parquet"))
    monkeypatch.setattr(vdc, "load_voter_files", _forbid("load_voter_files"))
    monkeypatch.setattr(vdc, "clean_voter_data", _forbid("clean_voter_data"))
    monkeypatch.setattr(vdc, "_write_cache_atomic", _forbid("_write_cache_atomic"))
    return cache


def test_cache_hit_never_loads_raw(fresh_cache, monkeypatch):
    built = []
    monkeypatch.setattr(vdc, "build_parquet_cache", lambda txt_files, logger=None: built.append(txt_files))
    df = vdc.load_enriched_voters(LOG, txt_files=["a.txt"])
    assert_frame_equal(df, ENRICHED)
    # The raw parquet cache is still ensured first: freshness is judged against it.
    assert built == [["a.txt"]]


def test_cache_hit_without_txt_files_does_not_build_the_raw_cache(fresh_cache, monkeypatch):
    monkeypatch.setattr(vdc, "build_parquet_cache", _forbid("build_parquet_cache"))
    assert_frame_equal(vdc.load_enriched_voters(LOG), ENRICHED)


def test_projection_reads_only_requested_columns_in_frame_order(fresh_cache):
    # Requested out of order on purpose: election columns must come back in the
    # frame's chronological order, because callers derive that order from df.columns.
    wanted = ["cohort", "PRIMARY-05/05/2026", "COUNTY_NUMBER", "PRIMARY-03/07/2000"]
    df = vdc.load_enriched_voters(LOG, columns=wanted)
    assert df.columns == ["COUNTY_NUMBER", "PRIMARY-03/07/2000", "PRIMARY-05/05/2026", "cohort"]
    assert_frame_equal(df, ENRICHED.select(df.columns))


def test_projection_naming_a_missing_column_raises_and_names_it(fresh_cache):
    with pytest.raises(KeyError) as err:
        vdc.load_enriched_voters(LOG, columns=["COUNTY_NUMBER", "NOT_A_COLUMN", "ALSO_MISSING"])
    assert "NOT_A_COLUMN" in str(err.value) and "ALSO_MISSING" in str(err.value)


def test_cache_miss_loads_raw_cleans_and_writes_the_whole_frame(monkeypatch):
    calls = []
    raw = ENRICHED.drop("cohort")
    monkeypatch.setattr(vdc, "_cache_is_fresh", lambda logger=None: False)
    monkeypatch.setattr(vdc, "build_parquet_cache", lambda txt_files, logger=None: calls.append("build"))
    monkeypatch.setattr(vdc, "load_voter_files", _forbid("load_voter_files"))

    def _load(logger=None):
        calls.append("load_raw")
        return raw

    def _clean(df, logger):
        calls.append("clean")
        assert df is raw
        return ENRICHED

    written = []

    def _write(df, logger):
        calls.append("write")
        written.append(df)

    monkeypatch.setattr(vdc, "load_voter_files_parquet", _load)
    monkeypatch.setattr(vdc, "clean_voter_data", _clean)
    monkeypatch.setattr(vdc, "_write_cache_atomic", _write)

    df = vdc.load_enriched_voters(LOG, txt_files=["a.txt"], columns=["cohort", "SOS_VOTERID"])
    assert calls == ["build", "load_raw", "clean", "write"]
    # The cache is written whole; only the returned frame is projected.
    assert_frame_equal(written[0], ENRICHED)
    assert df.columns == ["SOS_VOTERID", "cohort"]


def test_csv_miss_without_txt_files_raises(monkeypatch):
    monkeypatch.setattr(vdc, "_cache_is_fresh", lambda logger=None: False)
    with pytest.raises(ValueError):
        vdc.load_enriched_voters(LOG, use_parquet=False)


def test_run_ohio_analysis_loads_through_the_cache_first_loader():
    """Guard against the ordering regressing: the statewide entry point must
    not call a raw loader itself."""
    src = inspect.getsource(vdc.run_ohio_analysis)
    assert "load_enriched_for_export(" in src
    assert "load_voter_files_parquet(" not in src
    assert "load_voter_files(" not in src


def test_both_loaders_share_the_one_cache_first_decision():
    """_resolve_enriched is the only place freshness is checked and the only
    place a raw loader is called; the two public loaders go through it."""
    for loader in (vdc.load_enriched_voters, vdc.load_enriched_for_export):
        src = inspect.getsource(loader)
        assert "_resolve_enriched(" in src
        assert "_cache_is_fresh(" not in src
        assert "load_voter_files_parquet(" not in src and "clean_voter_data(" not in src
