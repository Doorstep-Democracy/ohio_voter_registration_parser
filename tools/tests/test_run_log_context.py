"""Run-log context: machine fingerprint, UTC-offset timestamps, one run id per process.

These give a later investigation fixed points: what the machine was, when each
line was written in absolute time, and which log files belong to one run. They
detect nothing by themselves and make no claim about hardware health.
"""

from __future__ import annotations

import getpass
import logging
import platform
import re
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

import pipeline.jurisdictional_groupings as jg
from pipeline import run_integrity as ri
from pipeline import voter_data_cleaner as vdc

ISO_OFFSET_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2}")


# ---- timestamps ---------------------------------------------------------------

def test_formatter_writes_iso_8601_with_utc_offset_for_the_record_time():
    record = logging.LogRecord("t", logging.INFO, __file__, 1, "msg", None, None)
    record.created = 1_794_465_247.0
    stamp = vdc.IsoOffsetFormatter("%(asctime)s %(message)s").format(record).split(" ")[0]
    assert ISO_OFFSET_RE.fullmatch(stamp), stamp
    parsed = datetime.fromisoformat(stamp)
    assert parsed.utcoffset() is not None
    # Same instant, whatever the local zone is.
    assert parsed.astimezone(timezone.utc).timestamp() == record.created


def test_log_file_name_format_still_parses_for_the_previous_run_check():
    # The line timestamp changed; the file-name stamp run_integrity reads did not.
    assert ri._LOG_NAME_RE.search("voter_analysis_20261112_013407_pipeline.log").group(1) == "20261112_013407"


# ---- run id -------------------------------------------------------------------

def test_first_claim_sets_the_run_id_and_later_claims_read_it_back(monkeypatch):
    monkeypatch.setattr(vdc, "_RUN_ID", None)
    assert vdc.claim_run_id("20261112_013407") == "20261112_013407"
    assert vdc.claim_run_id("20261112_020000") == "20261112_013407"


def test_excepthook_failure_line_carries_the_run_id(monkeypatch, caplog):
    import sys

    monkeypatch.setattr(sys, "excepthook", lambda *a: None)
    logger = logging.getLogger("test_run_log_context.hook")
    ri.install_excepthook(logger, run_id="20261112_013407")
    with caplog.at_level(logging.ERROR, logger=logger.name):
        sys.excepthook(RuntimeError, RuntimeError("boom"), None)
    failed = [r.getMessage() for r in caplog.records if ri.FAILED_MARKER in r.getMessage()]
    assert failed == [f"{ri.FAILED_MARKER}: uncaught RuntimeError: boom (run id 20261112_013407)"]


# ---- machine fingerprint ------------------------------------------------------

def test_fingerprint_fields_and_config_id_shape():
    fp = vdc.machine_fingerprint()
    assert set(fp) == {"os", "cpu", "cpu_count", "ram_total_gb", "swap_total_gb", "config_id", "boot_time"}
    assert re.fullmatch(r"[0-9a-f]{12}", fp["config_id"])
    assert fp["ram_total_gb"] > 0 and fp["cpu_count"] >= 1
    assert datetime.fromisoformat(fp["boot_time"]).utcoffset() is not None
    assert vdc.machine_fingerprint()["config_id"] == fp["config_id"]      # stable across calls


def test_fingerprint_names_no_host_and_no_user():
    values = " ".join(str(v) for v in vdc.machine_fingerprint().values()).lower()
    for private in (platform.node(), getpass.getuser()):
        if len(private) >= 3:
            assert private.lower() not in values


def _fake_psutil(ram_bytes: int, swap_bytes: int):
    return SimpleNamespace(
        virtual_memory=lambda: SimpleNamespace(total=ram_bytes, available=ram_bytes // 2),
        swap_memory=lambda: SimpleNamespace(total=swap_bytes),
        boot_time=lambda: 1_794_000_000.0,
    )


def test_config_id_changes_with_installed_ram_but_not_with_swap_size(monkeypatch):
    gib = 2**30
    monkeypatch.setattr(vdc, "_psutil", _fake_psutil(64 * gib, 32 * gib))
    base = vdc.machine_fingerprint()
    monkeypatch.setattr(vdc, "_psutil", _fake_psutil(64 * gib, 76 * gib))     # pagefile grew
    grown = vdc.machine_fingerprint()
    monkeypatch.setattr(vdc, "_psutil", _fake_psutil(32 * gib, 32 * gib))     # a kit was removed
    halved = vdc.machine_fingerprint()

    assert grown["config_id"] == base["config_id"]
    assert grown["swap_total_gb"] == 76.0 and base["swap_total_gb"] == 32.0
    assert halved["config_id"] != base["config_id"]
    assert halved["ram_total_gb"] == 32.0


# ---- the lines reach the logs -------------------------------------------------

def test_run_context_logs_run_id_and_machine_lines(caplog, monkeypatch):
    monkeypatch.setattr(vdc, "_RUN_ID", "20261112_013407")
    logger = logging.getLogger("test_run_log_context.ctx")
    with caplog.at_level(logging.INFO, logger=logger.name):
        vdc.log_run_context(logger)
    text = "\n".join(r.getMessage() for r in caplog.records)
    assert "run id 20261112_013407, started " in text and " UTC" in text
    fp = vdc.machine_fingerprint()
    assert f"machine config id {fp['config_id']}, booted " in text
    assert f"{fp['cpu_count']} logical CPUs" in text


def test_groupings_log_carries_the_process_run_id_and_offset_timestamps(tmp_path):
    # setup_logger imports the cleaner by its bare module name; read the run id
    # from that same module object.
    import voter_data_cleaner as bare_vdc

    name = "groupings_runid_test"
    logger, log_file = jg.setup_logger(name=name, log_dir=tmp_path)
    try:
        run_id = bare_vdc.claim_run_id("unused")
        for handler in logger.handlers:
            handler.flush()
        lines = log_file.read_text(encoding="utf-8").splitlines()
    finally:
        for handler in list(logger.handlers):
            handler.close()
            logger.removeHandler(handler)
    assert any(f"run id {run_id}," in line for line in lines)
    assert lines and all(ISO_OFFSET_RE.match(line) for line in lines if "Run context" in line)
    assert re.fullmatch(r"\d{8}_\d{6}", run_id)
