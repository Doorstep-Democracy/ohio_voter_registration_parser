"""Stage memory lines and the output-root line in the run log (voter_data_cleaner).

Every _timer exit must carry RSS, VMS and system-available memory, on success
and on failure: the failure line is the one a post-mortem reads. The run
context must state the output root, so a redirected run is visible in its log.
"""

from __future__ import annotations

import logging
import re

import pytest

from pipeline import voter_data_cleaner as vdc

MEMORY_RE = re.compile(r"RSS=[\d,]+ MB  VMS=[\d,]+ MB  avail=[\d,]+ MB")


def test_memory_line_reports_rss_vms_and_available():
    line = vdc._memory_line()
    assert MEMORY_RE.fullmatch(line), line
    rss_mb = int(line.split("RSS=")[1].split(" MB")[0].replace(",", ""))
    assert rss_mb > 0


def test_timer_logs_memory_on_success(caplog):
    logger = logging.getLogger("test_stage_memory.ok")
    with caplog.at_level(logging.INFO, logger=logger.name):
        with vdc._timer(logger, "a stage"):
            pass
    done = [r.getMessage() for r in caplog.records if "a stage" in r.getMessage() and "(" in r.getMessage()]
    assert len(done) == 1
    assert MEMORY_RE.search(done[0]), done[0]


def test_timer_logs_memory_on_failure_and_does_not_swallow(caplog):
    logger = logging.getLogger("test_stage_memory.fail")
    with caplog.at_level(logging.INFO, logger=logger.name):
        with pytest.raises(RuntimeError):
            with vdc._timer(logger, "a failing stage"):
                raise RuntimeError("boom")
    errors = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 1
    assert "RuntimeError: boom" in errors[0]
    assert MEMORY_RE.search(errors[0]), errors[0]


def test_run_context_states_the_output_root(caplog):
    logger = logging.getLogger("test_stage_memory.ctx")
    with caplog.at_level(logging.INFO, logger=logger.name):
        vdc.log_run_context(logger)
    lines = [r.getMessage() for r in caplog.records if "output root" in r.getMessage()]
    assert len(lines) == 1
    assert str(vdc.DOCS_DIR) in lines[0]
    # The test process runs without the override, so the default must be named.
    assert ("override" in lines[0]) == vdc.OUTPUT_OVERRIDDEN
