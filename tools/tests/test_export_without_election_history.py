"""The county export produces identical files without election history in memory.

run_ohio_analysis loads the enriched frame WITHOUT its election columns
(load_enriched_for_export), reads those columns from the cache a batch at a
time to compute participation, and exports from a frame that never held them.

The end-to-end test here runs the real export path twice on synthetic data and
compares every file written, byte for byte:

  legacy   clean -> participation on the full frame -> export the full frame
           (election columns present throughout, as before this change)
  current  the cleaned frame written as a cache -> load_enriched_for_export
           -> participation through read_cells -> export

It covers clean_voter_data, the cohort classifier, add_voter_participation,
_export_county_worker, export_json with precinct charts and
export_unc_shadow_json. Input is tools/tests/synthetic_swvf.py; no voter data
is read and nothing is written outside tmp_path.
"""

from __future__ import annotations

import gc
import logging
import weakref
from datetime import date

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from pipeline import voter_data_cleaner as vdc
from tools.tests import synthetic_swvf

LOG = logging.getLogger("test_export_without_election_history")
LOG.addHandler(logging.NullHandler())

SNAPSHOT = date(2026, 10, 24)


@pytest.fixture(scope="module")
def cleaned() -> pl.DataFrame:
    """Synthetic raw frame through the real cleaner and classifier."""
    mp = pytest.MonkeyPatch()
    mp.setattr(vdc, "get_snapshot_date", lambda logger=None: SNAPSHOT)
    try:
        return vdc.clean_voter_data(synthetic_swvf.raw_frame(), LOG)
    finally:
        mp.undo()


def _export(frame: pl.DataFrame, election_cols: list[str], out_dir, monkeypatch) -> dict:
    """Run the county worker for every county in `frame`; return {relative path: bytes}."""
    data = out_dir / "data"
    data.mkdir(parents=True)
    monkeypatch.setattr(vdc, "DOCS_DIR", out_dir)
    monkeypatch.setattr(vdc, "DATA_DIR", data)
    for county in sorted(frame["COUNTY_NUMBER"].unique().to_list()):
        name, err = vdc._export_county_worker((county, frame, election_cols, True))
        assert err is None, f"{name}: {err}"
    return {p.relative_to(out_dir).as_posix(): p.read_bytes() for p in sorted(out_dir.rglob("*")) if p.is_file()}


def _use_cache(cleaned: pl.DataFrame, tmp_path, monkeypatch):
    cache = tmp_path / "enriched_voters.parquet"
    cleaned.write_parquet(cache)
    monkeypatch.setattr(vdc, "ENRICHED_CACHE", cache)
    monkeypatch.setattr(vdc, "_cache_is_fresh", lambda logger=None: True)
    return cache


# ---- the fixture is worth trusting --------------------------------------------

def test_synthetic_frame_reaches_the_cohorts_and_columns_the_export_reads(cleaned):
    families = set(cleaned["cohort_family"].unique().to_list())
    assert {"PURE_R", "PURE_D", "MINOR_PARTY", "UNC_NO_PRIMARY"} <= families, families
    assert len(families) >= 6
    assert len(vdc.identify_election_cols(cleaned)) == len(synthetic_swvf.ELECTION_COLUMNS)
    assert cleaned.height == 6 * 60


# ---- the loader ---------------------------------------------------------------

def test_cache_hit_returns_the_frame_without_election_columns(cleaned, tmp_path, monkeypatch):
    _use_cache(cleaned, tmp_path, monkeypatch)
    for forbidden in ("load_voter_files_parquet", "load_voter_files", "clean_voter_data"):
        monkeypatch.setattr(vdc, forbidden, lambda *a, **k: (_ for _ in ()).throw(AssertionError("raw path used")))

    frame, election_cols, read_cells = vdc.load_enriched_for_export(LOG)

    assert election_cols == vdc.identify_election_cols(cleaned)
    assert not set(election_cols) & set(frame.columns)
    assert_frame_equal(frame, cleaned.drop(election_cols))
    # read_cells returns exactly the columns asked for, in that order, row-aligned.
    wanted = [election_cols[5], election_cols[0]]
    assert_frame_equal(read_cells(wanted), cleaned.select(wanted))


def _rebuild_into(cache, full: pl.DataFrame):
    """Stand-in for _resolve_enriched on the rebuild path: write the cache from
    the full frame, then return that frame."""
    def _resolve(logger, txt_files, use_parquet, before_load=None):
        full.write_parquet(cache)
        return full
    return _resolve


def test_rebuild_returns_the_same_three_things_reading_cells_from_the_cache(cleaned, tmp_path, monkeypatch):
    cache = tmp_path / "enriched_voters.parquet"
    monkeypatch.setattr(vdc, "ENRICHED_CACHE", cache)
    monkeypatch.setattr(vdc, "_resolve_enriched", _rebuild_into(cache, cleaned))
    frame, election_cols, read_cells = vdc.load_enriched_for_export(LOG)
    assert election_cols == vdc.identify_election_cols(cleaned)
    assert not set(election_cols) & set(frame.columns)
    assert_frame_equal(frame, cleaned.drop(election_cols))
    wanted = [election_cols[5], election_cols[0]]
    assert_frame_equal(read_cells(wanted), cleaned.select(wanted))
    # The cells come from the file, not from a frame held in memory.
    cache.unlink()
    with pytest.raises(FileNotFoundError):
        read_cells(wanted)


def test_rebuild_does_not_keep_the_full_frame_alive(cleaned, tmp_path, monkeypatch):
    # A read_cells bound to the full frame (rebuilt.select) held every election
    # column in memory until participation was computed.
    cache = tmp_path / "enriched_voters.parquet"
    monkeypatch.setattr(vdc, "ENRICHED_CACHE", cache)
    full = cleaned.clone()
    released = weakref.ref(full)
    monkeypatch.setattr(vdc, "_resolve_enriched", _rebuild_into(cache, full))
    del full
    frame, election_cols, read_cells = vdc.load_enriched_for_export(LOG)
    monkeypatch.undo()                       # the stand-in closure also held the frame
    gc.collect()
    assert released() is None
    assert getattr(read_cells, "__self__", None) is None
    assert_frame_equal(read_cells(election_cols[:3]), cleaned.select(election_cols[:3]))


def test_participation_after_a_rebuild_equals_participation_on_the_full_frame(cleaned, tmp_path, monkeypatch):
    cache = tmp_path / "enriched_voters.parquet"
    monkeypatch.setattr(vdc, "ENRICHED_CACHE", cache)
    monkeypatch.setattr(vdc, "_resolve_enriched", _rebuild_into(cache, cleaned))
    election_cols = vdc.identify_election_cols(cleaned)
    full = vdc.add_voter_participation(cleaned, election_cols, LOG).drop(election_cols)
    frame, cols, read_cells = vdc.load_enriched_for_export(LOG)
    assert_frame_equal(vdc.add_voter_participation(frame, cols, LOG, read_cells=read_cells), full)


def test_a_frame_without_cohort_family_is_refused(cleaned, monkeypatch):
    monkeypatch.setattr(vdc, "_resolve_enriched",
                        lambda logger, txt_files, use_parquet, before_load=None: cleaned.drop("cohort_family"))
    with pytest.raises(RuntimeError, match="cohort_family"):
        vdc.load_enriched_for_export(LOG)


# ---- participation and column metadata through read_cells ----------------------

def test_participation_through_read_cells_equals_participation_on_the_full_frame(cleaned, tmp_path, monkeypatch):
    election_cols = vdc.identify_election_cols(cleaned)
    full = vdc.add_voter_participation(cleaned, election_cols, LOG)

    _use_cache(cleaned, tmp_path, monkeypatch)
    frame, cols, read_cells = vdc.load_enriched_for_export(LOG)
    lean = vdc.add_voter_participation(frame, cols, LOG, read_cells=read_cells)

    assert_frame_equal(lean, full.drop(election_cols))


def test_col_meta_through_read_cells_equals_col_meta_on_the_full_frame(cleaned):
    election_cols = vdc.identify_election_cols(cleaned)
    assert (vdc.build_col_meta(cleaned.drop(election_cols), election_cols, LOG, read_cells=cleaned.select)
            == vdc.build_col_meta(cleaned, election_cols, LOG))


def test_misaligned_cells_are_refused(cleaned):
    election_cols = vdc.identify_election_cols(cleaned)
    short = cleaned.head(10)
    with pytest.raises(ValueError, match="row-aligned"):
        vdc.add_voter_participation(cleaned.drop(election_cols), election_cols, LOG, read_cells=short.select)


# ---- end to end: identical files ----------------------------------------------

def test_export_files_are_identical_with_and_without_election_history(cleaned, tmp_path, monkeypatch):
    election_cols = vdc.identify_election_cols(cleaned)

    legacy_frame = vdc.add_voter_participation(cleaned, election_cols, LOG)
    legacy = _export(legacy_frame, election_cols, tmp_path / "legacy", monkeypatch)

    _use_cache(cleaned, tmp_path, monkeypatch)
    frame, cols, read_cells = vdc.load_enriched_for_export(LOG)
    frame = vdc.add_voter_participation(frame, cols, LOG, read_cells=read_cells)
    assert not set(cols) & set(frame.columns)
    current = _export(frame, cols, tmp_path / "current", monkeypatch)

    assert legacy, "the export wrote nothing"
    assert sorted(current) == sorted(legacy)
    differing = [path for path in legacy if legacy[path] != current[path]]
    assert not differing, f"{len(differing)} of {len(legacy)} files differ: {differing[:10]}"
    # Both counties, with precinct charts and the UNC shadow file.
    assert any(p.startswith("data/adams_precinct_") for p in legacy)
    assert "data/franklin_unc_shadow.json" in legacy and "data/adams_party_affiliation.json" in legacy
