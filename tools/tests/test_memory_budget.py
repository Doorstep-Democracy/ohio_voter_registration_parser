"""Memory preflight: pipeline/memory_budget.py and its wiring into the pipeline.

Three layers, all on synthetic data:

  1. The pure decision logic, fed made-up machines (16, 32, 64 GB).
  2. The wiring: a stage that does not fit is refused BEFORE anything loads, on
     the cache-hit path, the rebuild path and the first build from text.
  3. End to end: run_ohio_analysis from a synthetic enriched cache, through the
     preflight, the loader, participation, the worker pool, the statewide pass
     and the manifest.

The byte figures the estimates produce are provisional (see the module
docstring of pipeline/memory_budget.py), so these tests pin the logic and the
ordering, not particular GB values.
"""

from __future__ import annotations

import json
import logging
from datetime import date

import polars as pl
import pytest

from pipeline import memory_budget as mb
from pipeline import ohio_voter_pipeline as ovp
from pipeline import run_integrity as ri
from pipeline import voter_data_cleaner as vdc
from tools.tests import synthetic_swvf

LOG = logging.getLogger("test_memory_budget")
LOG.addHandler(logging.NullHandler())

GIB = mb.GIB


def _machine(total_gb: float, available_gb: float, swap_gb: float = 32, cpus: int = 8) -> mb.Machine:
    return mb.Machine(total=int(total_gb * GIB), available=int(available_gb * GIB),
                      swap_total=int(swap_gb * GIB), cpu_count=cpus)


# A statewide-shaped frame: the dtype mix of the enriched cache without its
# election columns, 8 million rows, the largest county holding 11% of them.
STATEWIDE_DTYPES = ["String"] * 49 + ["Int32"] * 12 + ["Float64"] * 8 + ["Date"] + ["Boolean"] * 2
STATEWIDE = dict(rows=8_000_000, frame_dtypes=STATEWIDE_DTYPES, election_cols=91,
                 largest_share=0.11, batch=8)


def _export(workers: int) -> mb.Estimate:
    return mb.estimate_export(workers=workers, **STATEWIDE)


# ---- 1. settings and budget ----------------------------------------------------

def test_default_settings_are_auto_with_a_measured_budget():
    s = mb.MemorySettings()
    assert (s.mode, s.budget_gb, s.max_workers, s.margin) == ("auto", None, 0, 0.15)


@pytest.mark.parametrize("bad", [
    dict(mode="per-county"), dict(mode=""), dict(budget_gb=0), dict(budget_gb=-4),
    dict(max_workers=-1), dict(margin=1.0), dict(margin=-0.1),
])
def test_invalid_settings_are_rejected(bad):
    with pytest.raises(ValueError):
        mb.MemorySettings(**bad)


def test_budget_is_available_memory_less_the_margin_and_never_counts_swap():
    small_swap = mb.make_budget(_machine(32, 20, swap_gb=2), mb.MemorySettings())
    huge_swap = mb.make_budget(_machine(32, 20, swap_gb=76), mb.MemorySettings())
    assert small_swap.bytes == huge_swap.bytes == int(20 * GIB * 0.85)
    assert not small_swap.forced


def test_forced_budget_replaces_the_measurement():
    budget = mb.make_budget(_machine(64, 40), mb.MemorySettings(budget_gb=6))
    assert budget.bytes == 6 * GIB and budget.forced


# ---- 1. estimates --------------------------------------------------------------

def test_frame_bytes_sums_per_dtype_cell_costs():
    assert mb.frame_bytes(1000, ["Int32", "Float64", "Boolean"]) == int(1000 * (4 + 8 + 0.125))
    assert mb.frame_bytes(1000, [], election_cols=3) == int(1000 * 3 * mb.ELECTION_CELL_BYTES)
    # An unknown dtype is charged, not ignored.
    assert mb.frame_bytes(10, ["Struct"]) == int(10 * mb.UNKNOWN_CELL_BYTES)


def test_export_estimate_charges_the_larger_of_the_batch_and_the_slices():
    one, eight = _export(1), _export(8)
    parts_one, parts_eight = dict(one.parts), dict(eight.parts)
    assert parts_eight["county slices"] == 8 * parts_one["county slices"]
    assert parts_one["frame"] == parts_eight["frame"]
    for est, parts in ((one, parts_one), (eight, parts_eight)):
        assert est.peak == (parts["frame"] + parts["working"]
                            + max(parts["election batch"], parts["county slices"]))
    assert eight.peak > one.peak


def test_rebuild_costs_more_than_the_export_it_feeds():
    assert mb.estimate_rebuild(8_000_000, 137, 91).peak > _export(8).peak


# ---- 1. workers ----------------------------------------------------------------

def test_default_workers_follow_the_cpu_count_up_to_the_cap():
    assert mb.choose_workers(4) == 4
    assert mb.choose_workers(8) == 8
    assert mb.choose_workers(32) == mb.MAX_DEFAULT_WORKERS
    assert mb.choose_workers(0) == 1


def test_an_explicit_worker_count_is_used_as_given():
    assert mb.choose_workers(4, requested=12) == 12


def test_workers_are_lowered_until_the_slices_fit():
    budget = _export(3).peak
    assert mb.fit_workers(_export, budget, start=8) == 3
    assert mb.fit_workers(_export, _export(8).peak, start=8) == 8
    # Not even one worker fits: fit_workers bottoms out at 1 and enforce decides.
    assert mb.fit_workers(_export, 1, start=8) == 1


# ---- 1. the decision on made-up machines ---------------------------------------

def _decide(machine: mb.Machine, settings: mb.MemorySettings = mb.MemorySettings()):
    """What _preflight_statewide_export does, on a made-up machine."""
    budget = mb.make_budget(machine, settings)
    start = mb.choose_workers(machine.cpu_count, settings.max_workers)
    workers = start if settings.max_workers else mb.fit_workers(_export, budget.bytes, start)
    mb.enforce(_export(workers), budget, settings, LOG)
    return workers


def test_a_64_gb_machine_with_most_of_it_free_runs_with_eight_workers():
    assert _decide(_machine(64, 50)) == 8


def test_a_16_gb_machine_is_refused_before_loading():
    with pytest.raises(mb.MemoryBudgetError) as err:
        _decide(_machine(16, 9))
    text = str(err.value)
    assert "Nothing was loaded" in text and "county export" in text
    assert "--memory-mode eager" in text and "subset of counties" in text
    assert "never counted" in text


def test_a_machine_in_between_runs_with_fewer_workers():
    available = _export(2).peak / 0.85 / GIB + 0.01
    assert _decide(_machine(32, available)) == 2


def test_eager_mode_runs_a_stage_that_does_not_fit_and_says_so(caplog):
    with caplog.at_level(logging.WARNING, logger=LOG.name):
        _decide(_machine(16, 9), mb.MemorySettings(mode="eager"))
    assert any("OVER BUDGET" in r.getMessage() for r in caplog.records)


def test_low_available_memory_refuses_even_on_a_large_machine():
    # Total RAM is not the budget: what other programs hold is not ours to use.
    with pytest.raises(mb.MemoryBudgetError):
        _decide(_machine(64, 8))


def test_budget_line_reports_the_machine_the_budget_and_the_parts(caplog):
    budget = mb.make_budget(_machine(64, 50, swap_gb=32), mb.MemorySettings())
    with caplog.at_level(logging.INFO, logger=LOG.name):
        mb.enforce(_export(8), budget, mb.MemorySettings(), LOG)
    line = caplog.records[-1].getMessage()
    for expected in ("RAM total 64.0 GB", "available 50.0 GB", "swap 32.0 GB (not counted)",
                     "budget 42.5 GB", "county slices", "fits"):
        assert expected in line, line


def _swap_blocked():
    raise RuntimeError("PdhAddEnglishCounterW failed")


def test_unknown_swap_total_is_reported_as_unknown_and_changes_no_budget(monkeypatch, caplog):
    # Windows with performance counters blocked: psutil.swap_memory() raises.
    monkeypatch.setattr(mb.psutil, "swap_memory", _swap_blocked)
    machine = mb.read_machine()
    assert machine.swap_total is None
    known = mb.Machine(machine.total, machine.available, 32 * GIB, machine.cpu_count)
    assert (mb.make_budget(machine, mb.MemorySettings()).bytes
            == mb.make_budget(known, mb.MemorySettings()).bytes)

    budget = mb.make_budget(machine, mb.MemorySettings(budget_gb=64))
    with caplog.at_level(logging.INFO, logger=LOG.name):
        mb.enforce(_export(8), budget, mb.MemorySettings(), LOG)
    line = caplog.records[-1].getMessage()
    assert "swap unknown (not counted)" in line, line


def test_swap_reader_passes_on_every_other_error():
    class _Broken:
        @staticmethod
        def swap_memory():
            raise OSError("not the blocked-counter case")

    with pytest.raises(OSError):
        mb.swap_total_bytes(_Broken)


# ---- 2. wiring: shape readers --------------------------------------------------

def test_text_shape_estimates_rows_from_a_sample(tmp_path):
    header = ",".join(f'"{c}"' for c in ["SOS_VOTERID", "COUNTY_NUMBER", "PRIMARY-03/07/2000", "GENERAL-11/07/2000"])
    lines = [f'"OH{i:09d}","{i % 88 + 1:02d}","R","X"' for i in range(5000)]
    path = tmp_path / "SWVF_1_22.txt"
    path.write_text(header + "\n" + "\n".join(lines) + "\n", encoding="utf-8", newline="\n")
    rows, cols, election = vdc._text_shape([path])
    assert cols == 4 and election == 2
    assert abs(rows - 5000) <= 50


# ---- 2. wiring: refusal happens before the load --------------------------------

@pytest.fixture(scope="module")
def cleaned() -> pl.DataFrame:
    mp = pytest.MonkeyPatch()
    mp.setattr(vdc, "get_snapshot_date", lambda logger=None: date(2026, 10, 24))
    try:
        return vdc.clean_voter_data(synthetic_swvf.raw_frame(), LOG)
    finally:
        mp.undo()


@pytest.fixture
def cached(cleaned, tmp_path, monkeypatch):
    """A fresh synthetic enriched cache; raw partitions described by counts only."""
    cache = tmp_path / "enriched_voters.parquet"
    cleaned.write_parquet(cache)
    monkeypatch.setattr(vdc, "ENRICHED_CACHE", cache)
    monkeypatch.setattr(vdc, "_cache_is_fresh", lambda logger=None: True)
    monkeypatch.setattr(vdc, "_raw_partition_rows", lambda: {"p01": 240, "p25": 120})
    monkeypatch.setattr(vdc, "_raw_columns", lambda: (68, 22))
    return cache


def _policy(monkeypatch, **settings):
    monkeypatch.setattr(vdc.memory_budget, "_ACTIVE", mb.MemorySettings(**settings))


def _forbid_timer(monkeypatch):
    """Every load in the cleaner runs inside _timer; make entering one a failure."""
    def _no_timer(logger, step):
        raise AssertionError(f"a stage started before the preflight refused the run: {step}")
    monkeypatch.setattr(vdc, "_timer", _no_timer)


def test_cache_hit_export_is_refused_before_the_cache_is_read(cached, monkeypatch):
    _policy(monkeypatch, budget_gb=1e-9)
    _forbid_timer(monkeypatch)
    with pytest.raises(vdc.memory_budget.MemoryBudgetError, match="county export"):
        vdc.load_enriched_for_export(
            LOG, before_load=lambda fresh: vdc._preflight_statewide_export(LOG, fresh))


def test_rebuild_is_refused_before_the_raw_cache_is_read(cached, monkeypatch):
    monkeypatch.setattr(vdc, "_cache_is_fresh", lambda logger=None: False)
    _policy(monkeypatch, budget_gb=1e-9)
    _forbid_timer(monkeypatch)
    with pytest.raises(vdc.memory_budget.MemoryBudgetError, match="rebuild"):
        vdc.load_enriched_for_export(
            LOG, before_load=lambda fresh: vdc._preflight_statewide_export(LOG, fresh))


def test_groupings_is_refused_before_its_frame_is_read(cached, monkeypatch):
    _policy(monkeypatch, budget_gb=1e-9)
    _forbid_timer(monkeypatch)
    with pytest.raises(vdc.memory_budget.MemoryBudgetError, match="groupings"):
        vdc.load_enriched_voters(
            LOG, columns=["COUNTY_NUMBER", "cohort_family"],
            before_load=lambda fresh: vdc._preflight_groupings(LOG, fresh, ["COUNTY_NUMBER", "cohort_family"]))


def test_first_build_from_text_is_refused_before_the_text_is_read(tmp_path, monkeypatch):
    txt = tmp_path / "SWVF_1_22.txt"
    txt.write_text('"SOS_VOTERID","COUNTY_NUMBER"\n' + '"OH1","01"\n' * 200, encoding="utf-8", newline="\n")
    _policy(monkeypatch, budget_gb=1e-9)
    _forbid_timer(monkeypatch)
    with pytest.raises(vdc.memory_budget.MemoryBudgetError, match="from text"):
        vdc.build_parquet_cache([txt], parquet_dir=tmp_path / "parquet", logger=LOG)


def test_preflight_passes_and_returns_the_worker_count(cached, monkeypatch):
    _policy(monkeypatch, budget_gb=64)
    monkeypatch.setattr(vdc.memory_budget, "read_machine", lambda: _machine(64, 50, cpus=6))
    assert vdc._preflight_statewide_export(LOG, True) == 6
    # An explicit count is used as given, by argument or by policy.
    assert vdc._preflight_statewide_export(LOG, True, requested_workers=3) == 3
    _policy(monkeypatch, budget_gb=64, max_workers=2)
    assert vdc._preflight_statewide_export(LOG, True) == 2


def test_eager_mode_goes_ahead_over_budget(cached, monkeypatch, caplog):
    _policy(monkeypatch, budget_gb=1e-9, mode="eager")
    with caplog.at_level(logging.WARNING, logger=LOG.name):
        assert vdc._preflight_statewide_export(LOG, True) >= 1
    assert any("OVER BUDGET" in r.getMessage() for r in caplog.records)


def test_a_rebuild_with_no_raw_partitions_skips_the_preflight_and_says_so(cached, monkeypatch, caplog):
    monkeypatch.setattr(vdc, "_raw_partition_rows", lambda: {})
    _policy(monkeypatch, budget_gb=1e-9)
    with caplog.at_level(logging.WARNING, logger=LOG.name):
        assert vdc._preflight_statewide_export(LOG, False) >= 1
    assert any("preflight skipped" in r.getMessage() for r in caplog.records)


# ---- 2. wiring: command line ---------------------------------------------------

def test_command_line_defaults_and_values():
    args = ovp.parse_args([])
    assert (args.memory_mode, args.memory_budget_gb, args.max_workers) == ("auto", None, 0)
    args = ovp.parse_args(["--memory-mode", "eager", "--memory-budget-gb", "12.5", "--max-workers", "4"])
    assert (args.memory_mode, args.memory_budget_gb, args.max_workers) == ("eager", 12.5, 4)


@pytest.mark.parametrize("argv", [
    ["--memory-mode", "per-county"], ["--memory-budget-gb", "0"], ["--memory-budget-gb", "-3"],
    ["--memory-budget-gb", "lots"], ["--max-workers", "0"], ["--max-workers", "-2"], ["--max-workers", "1.5"],
])
def test_command_line_rejects_bad_values(argv, capsys):
    with pytest.raises(SystemExit) as err:
        ovp.parse_args(argv)
    assert err.value.code == 2
    capsys.readouterr()


def test_a_refusal_ends_the_run_with_exit_code_3_and_the_failure_marker(monkeypatch, caplog, capsys):
    def _refuse(*args, **kwargs):
        raise ovp.memory_budget.MemoryBudgetError("Not enough memory for the county export: ...")

    monkeypatch.setattr(ovp, "_dispatch", _refuse)
    with caplog.at_level(logging.ERROR, logger="voter_analysis"):
        with pytest.raises(SystemExit) as err:
            ovp._dispatch_guarded("1", [])
    assert err.value.code == 3
    assert any(ri.FAILED_MARKER in r.getMessage() for r in caplog.records)
    assert "RUN REFUSED" in capsys.readouterr().out


# ---- 3. end to end -------------------------------------------------------------

def test_statewide_run_from_a_synthetic_cache(cached, tmp_path, monkeypatch):
    out = tmp_path / "docs"
    (out / "data").mkdir(parents=True)
    monkeypatch.setattr(vdc, "DOCS_DIR", out)
    monkeypatch.setattr(vdc, "DATA_DIR", out / "data")
    monkeypatch.setattr(vdc, "LOGS_DIR", tmp_path / "logs")
    monkeypatch.setattr(vdc, "get_snapshot_date", lambda logger=None: date(2026, 10, 24))
    monkeypatch.setattr(vdc, "build_parquet_cache", lambda txt_files, logger=None: None)
    for raw in ("load_voter_files_parquet", "load_voter_files", "clean_voter_data"):
        monkeypatch.setattr(vdc, raw, lambda *a, **k: (_ for _ in ()).throw(AssertionError("raw path used")))
    _policy(monkeypatch, budget_gb=64, max_workers=3)

    vdc.run_ohio_analysis([], use_parquet=True, include_precinct_charts=True)

    written = {p.relative_to(out).as_posix() for p in out.rglob("*") if p.is_file()}
    for expected in ("data/adams_party_affiliation.json", "data/franklin_party_affiliation.json",
                     "data/adams_unc_shadow.json", "manifest.json"):
        assert expected in written, sorted(written)[:20]
    state_slug = vdc.STATEWIDE_NAME.lower().replace(" ", "_")
    assert f"data/{state_slug}_party_affiliation.json" in written
    assert any(p.startswith("data/adams_precinct_") for p in written)

    # The statewide count is the sum of the county counts (the additive contract).
    def _total(slug: str) -> int:
        chart = json.loads((out / "data" / f"{slug}_party_affiliation.json").read_text(encoding="utf-8"))
        return sum(chart["chartConfig"]["datasets"][0]["data"])

    assert _total(state_slug) == _total("adams") + _total("franklin") == 6 * 60
