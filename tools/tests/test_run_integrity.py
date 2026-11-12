"""pipeline/run_integrity.py: outputs are flushed and validated before PIPELINE COMPLETE,
and damage or an abnormal end from a previous run is reported loudly at the next start."""
import logging
import os
import sys
import time
from datetime import datetime

import pytest

from pipeline import run_integrity as ri

LOGGER = "test_run_integrity"


def _log():
    return logging.getLogger(LOGGER)


def _write(path, payload: bytes, mtime: float | None = None):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    if mtime is not None:
        os.utime(path, (mtime, mtime))
    return path


# ── validate_file ────────────────────────────────────────────────────────────

@pytest.mark.parametrize("name,payload,expect", [
    ("ok.json", b'{"a": 1}', None),
    ("empty.json", b"", "empty"),
    ("zero.json", b"\x00" * 656, "zero-filled"),
    ("tail.json", b'{"a": 1}' + b"\x00" * 40, "NUL bytes"),
    ("bad.json", b'{"a": ', "invalid JSON"),
    ("page.htm", b"<p>ok</p>", None),
    ("img.png", b"\x89PNG\x00\x00data", None),          # binary: NULs are normal
    ("blank.png", b"\x00" * 64, "zero-filled"),         # but all-NUL is not
])
def test_validate_file(tmp_path, name, payload, expect):
    why = ri.validate_file(_write(tmp_path / name, payload))
    if expect is None:
        assert why is None
    else:
        assert why and expect in why


# ── seal_outputs ─────────────────────────────────────────────────────────────

def test_seal_reports_only_this_runs_bad_files(tmp_path, caplog):
    root = tmp_path / "out"
    start = time.time()
    _write(root / "old_bad.json", b"\x00" * 10, mtime=start - 3600)   # before the run: ignored
    _write(root / "a" / "good.json", b"[1, 2]")
    bad = _write(root / "a" / "zeroed_narrative.json", b"\x00" * 600)
    with caplog.at_level(logging.INFO, logger=LOGGER):
        problems = ri.seal_outputs(root, start, _log())
    assert list(problems) == [bad]
    assert any(r.levelno == logging.ERROR and "Output seal FAILED" in r.getMessage() for r in caplog.records)
    assert any("2 file(s)" in r.getMessage() for r in caplog.records)


def test_seal_clean_run_logs_no_error(tmp_path, caplog):
    root = tmp_path / "out"
    start = time.time()
    _write(root / "x.json", b"{}")
    with caplog.at_level(logging.INFO, logger=LOGGER):
        assert ri.seal_outputs(root, start, _log()) == {}
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]


# ── check_previous_run ───────────────────────────────────────────────────────

def _prev_log(logs, started: float, body: str):
    stamp = datetime.fromtimestamp(started).strftime("%Y%m%d_%H%M%S")
    return _write(logs / f"voter_analysis_{stamp}_pipeline.log", body.encode("utf-8"))


def test_previous_run_that_died_is_an_error(tmp_path, caplog):
    logs, root = tmp_path / "logs", tmp_path / "out"
    _prev_log(logs, time.time() - 600, "INFO  computing participation metrics ...\n")
    current = _prev_log(logs, time.time() + 1, "")
    root.mkdir()
    with caplog.at_level(logging.INFO, logger=LOGGER):
        ri.check_previous_run(logs, current, root, _log())
    errors = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
    assert any("did NOT end with PIPELINE COMPLETE" in m and "participation" in m for m in errors)


def test_damage_after_a_complete_run_is_found(tmp_path, caplog):
    """The 2026-11-11 case: the log says complete, a crash afterwards zero-filled files."""
    logs, root = tmp_path / "logs", tmp_path / "out"
    _prev_log(logs, time.time() - 600, "INFO  PIPELINE COMPLETE: choice=4\n")
    current = _prev_log(logs, time.time() + 1, "")
    zeroed = _write(root / "municipal_court_district" / "x_narrative.json", b"\x00" * 656)
    _write(root / "fine.json", b"{}")
    _write(root / "untouched.json", b"\x00", mtime=time.time() - 7200)  # before that run
    with caplog.at_level(logging.INFO, logger=LOGGER):
        problems = ri.check_previous_run(logs, current, root, _log())
    assert list(problems) == [zeroed]
    assert any("FOUND DAMAGED OUTPUT" in r.getMessage() for r in caplog.records if r.levelno == logging.ERROR)


def test_no_previous_log_is_not_an_error(tmp_path, caplog):
    logs = tmp_path / "logs"
    current = _prev_log(logs, time.time(), "")
    with caplog.at_level(logging.INFO, logger=LOGGER):
        assert ri.check_previous_run(logs, current, tmp_path, _log()) == {}
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]


# ── recheck ──────────────────────────────────────────────────────────────────

def test_recheck_separates_repaired_removed_and_still_bad(tmp_path, caplog):
    root = tmp_path / "out"
    repaired = _write(root / "r.json", b"{}")
    still = _write(root / "s.json", b"\x00" * 5)
    removed = root / "gone.json"
    flagged = {repaired: "zero-filled", still: "zero-filled", removed: "zero-filled"}
    with caplog.at_level(logging.INFO, logger=LOGGER):
        left = ri.recheck(flagged, root, _log())
    assert list(left) == [still]
    assert any("1 repaired by this run, 1 removed, 1 still bad" in r.getMessage() for r in caplog.records)


# ── excepthook ───────────────────────────────────────────────────────────────

def test_uncaught_exception_reaches_the_log(monkeypatch, caplog):
    seen = []
    monkeypatch.setattr(sys, "excepthook", lambda *a: seen.append(a))
    ri.install_excepthook(_log())
    try:
        raise ValueError("cohort labels do not match")
    except ValueError:
        exc_info = sys.exc_info()
    with caplog.at_level(logging.INFO, logger=LOGGER):
        sys.excepthook(*exc_info)
    msgs = [(r.levelno, r.getMessage()) for r in caplog.records]
    assert (logging.CRITICAL, "Uncaught exception") in msgs
    assert any(lvl == logging.ERROR and m.startswith("PIPELINE FAILED: uncaught ValueError") for lvl, m in msgs)
    assert seen, "the original hook still runs, so console output is unchanged"
