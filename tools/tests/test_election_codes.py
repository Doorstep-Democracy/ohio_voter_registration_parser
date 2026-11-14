"""Compact election codes (DOO-184, M6): equivalence, strictness, loaders, cache, estimator.

Election cells are cast from String to a 1-byte pl.Enum at load. These tests pin that
every classifier and participation output is identical on the String and Enum paths,
that a value outside the profiled code set stops the run, and that the enriched cache
is still written as String (operator decision 2A).

Synthetic frames only: runs on a fresh checkout with no local/ data.
"""
import json
import logging
from datetime import date

import pytest

pl = pytest.importorskip("polars")
from polars.testing import assert_frame_equal  # noqa: E402

import election_codes_fixture as fx  # noqa: E402
from pipeline import memory_budget as mb  # noqa: E402
from pipeline import paths  # noqa: E402
from pipeline import voter_data_cleaner as vdc  # noqa: E402

# voter_data_cleaner imports election_codes as a sibling module (pipeline/ on sys.path);
# import it the same way so exception classes and helpers are the same objects.
import election_codes as ec  # noqa: E402

LOG = logging.getLogger("test")
COLS = fx.ELECTION_COLS


@pytest.fixture(scope="module")
def frames():
    s = fx.string_frame()
    return s, vdc.to_codes(s, COLS)


# -- the type ------------------------------------------------------------------

def test_enum_is_one_byte_physical():
    """memory_budget charges compact cells at CELL_BYTES['Enum'] = 1 byte; a Polars
    upgrade that widens the physical type must fail here, not in a paging rebuild."""
    s = pl.Series(["D", "", "X"], dtype=vdc.ELECTION_CODE_DTYPE)
    assert s.to_physical().dtype == pl.UInt8
    assert mb.COMPACT_ELECTION_CELL_BYTES == mb.CELL_BYTES["Enum"] == 1.0


def test_cast_strips_and_keeps_blank_as_a_category(frames):
    s, e = frames
    assert all(e.schema[c] == vdc.ELECTION_CODE_DTYPE for c in COLS)
    pad = e.filter(pl.col("SOS_VOTERID") == "E_PAD_D")["PRIMARY-05/03/2022"].to_list()
    assert pad == ["D"]
    blank = e.filter(pl.col("SOS_VOTERID") == "E_BLANK_ALL").select(COLS).row(0)
    assert blank == ("",) * len(COLS)
    assert e.select(pl.sum_horizontal(pl.col(COLS).is_null())).to_series().sum() == 0


def test_to_codes_is_idempotent(frames):
    _, e = frames
    assert_frame_equal(vdc.to_codes(e, COLS), e)


def test_code_expr_rejects_other_dtypes():
    with pytest.raises(TypeError, match="expected String or"):
        vdc.code_expr("PRIMARY-05/03/2022", pl.Int32)


# -- equivalence: String path vs Enum path -------------------------------------

def test_classifier_outputs_identical(frames):
    s, e = frames
    run = lambda df: vdc.classify_all_voters_primary_history(  # noqa: E731
        df, fx.PRIMARY_COLS, LOG, reference_date=fx.REFERENCE_DATE)
    a, b = run(s), run(e)
    assert_frame_equal(a, b)
    # Every edge case resolved to the cohort the String path gives it, and none
    # fell outside the chart slices.
    assert set(b["cohort_family"]) <= {fam for fam, _, _ in vdc.COHORT_SLICES}
    got = dict(zip(b["SOS_VOTERID"], b["cohort"]))
    assert got["E_MINOR_ONLY"] == "MINOR_PARTY"
    assert got["E_L_AFF"] == "MINOR_PARTY"
    assert got["E_PAD_D"] != "UNC_NO_PRIMARY"


def test_minor_party_count_means_non_empty_and_not_drx(frames):
    _, e = frames
    out = vdc.classify_all_voters_primary_history(
        e, fx.PRIMARY_COLS, LOG, reference_date=fx.REFERENCE_DATE)
    by_id = {r["SOS_VOTERID"]: r for r in out.iter_rows(named=True)}
    for code in ["L", "G", "C", "S", "N", "E"]:
        assert (by_id[f"E_CODE_{code}"]["m_primaries"], by_id[f"E_CODE_{code}"]["x_primaries"]) == (1, 0)
    assert by_id["E_CODE_X"]["x_primaries"] == 1 and by_id["E_CODE_X"]["m_primaries"] == 0


def test_col_meta_identical(frames):
    s, e = frames
    assert vdc.build_col_meta(s, COLS, LOG) == vdc.build_col_meta(e, COLS, LOG)


@pytest.mark.parametrize("source", ["frame", "read_cells_string", "read_cells_enum"])
def test_participation_identical(frames, source):
    s, e = frames
    want = vdc.add_voter_participation(s, COLS, LOG).drop(COLS)
    if source == "frame":
        got = vdc.add_voter_participation(e, COLS, LOG).drop(COLS)
    else:
        cells = s if source == "read_cells_string" else e
        got = vdc.add_voter_participation(e.drop(COLS), COLS, LOG, read_cells=cells.select)
    assert_frame_equal(got, want)


def test_election_participation_summaries_identical(frames):
    s, e = frames
    a = vdc.build_election_participation(vdc.add_voter_participation(s, COLS, LOG), COLS, LOG)
    b = vdc.build_election_participation(vdc.add_voter_participation(e, COLS, LOG), COLS, LOG)
    for x, y in zip(a, b):
        # The by-type summary comes from an unordered group_by (not changed here).
        assert_frame_equal(x, y, check_row_order=False)


# -- strict: an unseen code stops the run --------------------------------------

def _with_unseen(df):
    return df.with_columns(
        # Row 0, so it lands in every partition split below.
        pl.when(pl.col("SOS_VOTERID") == "E_BLANK_ALL").then(pl.lit("Q"))
          .otherwise(pl.col("GENERAL-11/03/2020")).alias("GENERAL-11/03/2020"))


def test_unseen_code_raises_naming_column_county_and_value():
    df = _with_unseen(fx.string_frame(n_random=50))
    with pytest.raises(vdc.UnknownElectionCodeError) as err:
        vdc.to_codes(df, COLS)
    msg = str(err.value)
    assert "GENERAL-11/03/2020" in msg and "['Q']" in msg and "['57']" in msg
    assert "PRIMARY-" not in msg


def test_unseen_code_stops_clean_voter_data():
    df = _with_unseen(fx.string_frame(n_random=50))
    with pytest.raises(vdc.UnknownElectionCodeError):
        vdc.clean_voter_data(df, LOG)


def test_nonstring_election_column_is_refused():
    df = pl.DataFrame({"SOS_VOTERID": ["a"], "PRIMARY-05/03/2022": [1]})
    with pytest.raises(TypeError, match="cannot cast to codes"):
        vdc.to_codes(df, ["PRIMARY-05/03/2022"])


# -- loaders --------------------------------------------------------------------

def _write_partitions(tmp_path, df):
    """Two hive partitions laid out as build_parquet_cache writes them: no COUNTY_NUMBER
    column inside the files, the key in the directory name."""
    pdir = tmp_path / "parquet"
    raw = df.drop("REGDATE_DT", "COUNTY_NUMBER")
    for num, part in ((1, raw.head(20)), (12, raw.tail(30))):
        f = paths.county_partition_file(num, pdir)
        f.parent.mkdir(parents=True)
        part.write_parquet(f)
    return pdir


def test_full_load_matches_hive_read(tmp_path):
    pdir = _write_partitions(tmp_path, fx.string_frame(n_random=50))
    old = pl.read_parquet(vdc.parquet_glob(pdir), hive_partitioning=True)
    old = old.with_columns(pl.col("COUNTY_NUMBER").cast(pl.String).str.zfill(2))
    new = vdc.load_voter_files_parquet(parquet_dir=pdir, logger=LOG)
    assert new.columns == old.columns
    assert all(new.schema[c] == vdc.ELECTION_CODE_DTYPE for c in COLS)
    assert_frame_equal(vdc.to_codes(old, COLS), new)


def test_county_load_casts_codes(tmp_path):
    pdir = _write_partitions(tmp_path, fx.string_frame(n_random=50))
    df = vdc.load_voter_files_parquet(parquet_dir=pdir, county_number="12", logger=LOG)
    assert df.height == 30 and df["COUNTY_NUMBER"].unique().to_list() == ["12"]
    assert all(df.schema[c] == vdc.ELECTION_CODE_DTYPE for c in COLS)


def test_full_load_names_the_partition_with_an_unseen_code(tmp_path):
    pdir = _write_partitions(tmp_path, _with_unseen(fx.string_frame(n_random=50)))
    with pytest.raises(vdc.UnknownElectionCodeError, match="'Q'"):
        vdc.load_voter_files_parquet(parquet_dir=pdir, logger=LOG)


def test_csv_load_casts_codes(tmp_path):
    df = fx.string_frame(n_random=40).drop("REGDATE_DT")
    paths = []
    for i, part in enumerate((df.head(20), df.tail(36))):
        p = tmp_path / f"SWVF_{i}.txt"
        part.write_csv(p)
        paths.append(p)
    got = vdc.load_voter_files(paths, logger=LOG)
    assert all(got.schema[c] == vdc.ELECTION_CODE_DTYPE for c in COLS)
    # Compare against the same CSV round trip read as String, then cast.
    want = pl.concat([pl.read_csv(p, infer_schema=False) for p in paths])
    assert_frame_equal(got, vdc.to_codes(want, COLS))


# -- enriched cache: still String on disk ---------------------------------------

def test_cache_written_as_string_and_frame_left_compact(tmp_path, monkeypatch, frames):
    _, e = frames
    cache = tmp_path / "enriched_voters.parquet"
    monkeypatch.setattr(vdc, "ENRICHED_CACHE", cache)
    monkeypatch.setattr(vdc, "get_snapshot_date", lambda logger=None: date(2026, 10, 24))
    vdc._write_cache_atomic(e, LOG)
    schema = pl.read_parquet_schema(cache)
    assert all(schema[c] == pl.String for c in COLS)
    assert_frame_equal(pl.read_parquet(cache), ec.to_strings(e, COLS))
    meta = pl.read_parquet_metadata(cache)
    assert meta["provenance_snapshot_date"] == "2026-10-24"
    assert json.loads(meta["affiliation_window_years"])
    assert all(e.schema[c] == vdc.ELECTION_CODE_DTYPE for c in COLS)


# -- estimator ------------------------------------------------------------------

def test_rebuild_estimate_charges_compact_cells():
    rows, raw_cols, n_el, largest = 8_000_000, 137, 91, 1_000_000
    est = mb.estimate_rebuild(rows, raw_cols, n_el, largest)
    parts = dict(est.parts)
    non_election = mb.frame_bytes(rows, ["String"] * (raw_cols - n_el))
    assert parts["raw frame"] == non_election + int(rows * n_el * 1.0)
    assert parts["partition cast"] == int(largest * n_el * mb.ELECTION_CELL_BYTES)
    # The cache is written as String: charged at a full String copy of the election cells.
    assert parts["cache write"] == int(rows * n_el * mb.ELECTION_CELL_BYTES)
    raw, enriched = parts["raw frame"], parts["enriched frame"]
    assert est.peak == max(raw + parts["partition cast"],
                           raw + enriched + parts["working"],
                           raw + enriched + parts["cache write"])
    # Statewide shape (8.0M rows, 91 election columns): well under the all-String estimate.
    string_raw = mb.frame_bytes(rows, ["String"] * (raw_cols - n_el), election_cols=n_el)
    old_peak = string_raw + int(string_raw * 1.1) + int(string_raw * mb.WORKING_FACTOR)
    assert est.peak < 0.6 * old_peak


def test_rebuild_estimate_without_partition_sizes_is_conservative():
    est = mb.estimate_rebuild(8_000_000, 137, 91)
    assert dict(est.parts)["partition cast"] == int(8_000_000 * 91 * mb.ELECTION_CELL_BYTES)
