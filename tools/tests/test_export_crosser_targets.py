"""Crosser export (tools/export/export_crosser_targets.py, DOO-7), end to end on
hand-built histories run through the real classifier. No real voter data; all
output goes to pytest's tmp_path."""

import json

import polars as pl
import pytest

from tools.export import export_crosser_targets as exp
from tools.tests import synthetic_swvf
from tools.tests.axis_a_fixture import EXPORTED_57, PROVENANCE, SHUFFLED_ELECTION_COLUMNS, write_enriched


@pytest.fixture()
def source(tmp_path):
    path = tmp_path / "enriched.parquet"
    write_enriched(path)
    return path


@pytest.fixture()
def logger():
    return exp._build_logger()


def _csv(out, dirname, slug):
    return pl.read_csv(out / "Crosser_Targets" / dirname / f"{slug}_CROSSER_targets.csv",
                       infer_schema_length=0)


def test_static_whitelist_matches_the_swvf_layout():
    assert list(exp.BUYER_STATIC_COLUMNS) == synthetic_swvf.STATIC_COLUMNS


def test_single_county_rows_and_flags(source, tmp_path, logger):
    out = tmp_path / "out"
    assert exp.run_export(source, out, logger, county="57", county_name="montgomery") == 6
    df = _csv(out, "57_Montgomery", "montgomery")
    assert df.get_column("SOS_VOTERID").to_list() == EXPORTED_57
    flags = {r["SOS_VOTERID"]: (r["fresh_defector"], r["voted_last_even_november_general"])
             for r in df.iter_rows(named=True)}
    assert flags["V01"] == ("true", "true")
    assert flags["V02"] == ("false", "true")
    assert flags["V07"] == ("true", "false")      # voted only the odd-year general
    assert set(df.get_column("COUNTY_NAME")) == {"Montgomery"}


def test_columns_are_exactly_the_whitelist(source, tmp_path, logger):
    out = tmp_path / "out"
    exp.run_export(source, out, logger, county="57", county_name="montgomery")
    header = _csv(out, "57_Montgomery", "montgomery").columns
    elections = sorted(SHUFFLED_ELECTION_COLUMNS, key=lambda c: (c[-4:], c.split("-")[1]))
    assert header == list(exp.BUYER_STATIC_COLUMNS) + elections + list(exp.COMPUTED_COLUMNS)
    assert not set(header) & set(exp.TIER_LINE_FIELDS)
    assert "switch_count" not in header and "cohort" not in header and "lean_score" not in header


def test_anchor_is_latest_even_november_regardless_of_column_order(source, logger):
    lf = pl.scan_parquet(source)
    assert exp.resolve_even_november_general(lf.collect_schema().names(), lf, logger) == "GENERAL-11/05/2024"


def test_provenance_sidecar(source, tmp_path, logger):
    out = tmp_path / "out"
    exp.run_export(source, out, logger, county="57", county_name="montgomery")
    record = json.loads((out / "Crosser_Targets" / "57_Montgomery" /
                         "montgomery_CROSSER_targets.provenance.json").read_text(encoding="utf-8"))
    assert record["provenance_snapshot_date"] == "2026-10-24"
    assert record["affiliation_window_years"] == [2024, 2025, 2026]
    assert record["anchor_general_column"] == "GENERAL-11/05/2024"
    assert (record["rows"], record["fresh_defector_rows"]) == (6, 3)


def test_all_counties_prunes_stale_dirs_and_county_run_does_not(source, tmp_path, logger):
    out = tmp_path / "out"
    stale = out / "Crosser_Targets" / "99_Nowhere"
    stale.mkdir(parents=True)
    exp.run_export(source, out, logger, county="57", county_name="montgomery")
    assert stale.exists()
    assert exp.run_export(source, out, logger, all_counties=True) == 7
    assert not stale.exists()
    assert sorted(p.name for p in (out / "Crosser_Targets").iterdir()) == ["09_Butler", "57_Montgomery"]


def test_county_filter_strips_whitespace(tmp_path, logger):
    path = tmp_path / "padded.parquet"
    write_enriched(path)
    padded = pl.read_parquet(path).with_columns(pl.col("COUNTY_NUMBER") + " ")
    padded.write_parquet(path, metadata=PROVENANCE)
    out = tmp_path / "out"
    assert exp.run_export(path, out, logger, county="57", county_name="montgomery") == 6
    assert set(_csv(out, "57_Montgomery", "montgomery").get_column("COUNTY_NUMBER")) == {"57"}


def test_empty_county_writes_header_only(source, tmp_path, logger):
    out = tmp_path / "out"
    assert exp.run_export(source, out, logger, county="01", county_name="adams") == 0
    df = _csv(out, "01_Adams", "adams")
    assert df.height == 0 and "fresh_defector" in df.columns


def test_unknown_county_number_fails_loudly(tmp_path, logger):
    path = tmp_path / "bad.parquet"
    write_enriched(path)
    bad = pl.read_parquet(path).with_columns(pl.lit("99").alias("COUNTY_NUMBER"))
    bad.write_parquet(path, metadata=PROVENANCE)
    with pytest.raises(ValueError, match="99"):
        exp.run_export(path, tmp_path / "out", logger, all_counties=True)


def test_missing_provenance_is_refused(tmp_path, logger):
    path = tmp_path / "noprov.parquet"
    write_enriched(path, metadata=None)
    with pytest.raises(RuntimeError, match="affiliation_window_years"):
        exp.run_export(path, tmp_path / "out", logger, county="57", county_name="montgomery")


def test_stale_managed_cache_is_refused(source, tmp_path, logger, monkeypatch):
    monkeypatch.setattr(exp, "ENRICHED_CACHE", source)
    monkeypatch.setattr(exp, "_cache_is_fresh", lambda _logger=None: False)
    with pytest.raises(RuntimeError, match="stale"):
        exp.run_export(source, tmp_path / "out", logger, county="57", county_name="montgomery")


def test_managed_cache_must_hold_every_county(source, tmp_path, logger, monkeypatch):
    monkeypatch.setattr(exp, "ENRICHED_CACHE", source)
    monkeypatch.setattr(exp, "_cache_is_fresh", lambda _logger=None: True)
    with pytest.raises(ValueError, match="expected 88"):
        exp.run_export(source, tmp_path / "out", logger, all_counties=True)


def test_tier_line_column_is_never_written(tmp_path, logger):
    df = pl.DataFrame({"SOS_VOTERID": ["V01"], "switch_count": [1]})
    with pytest.raises(AssertionError, match="switch_count"):
        exp.write_county_csv(df, "57", "montgomery", tmp_path, {}, logger)
