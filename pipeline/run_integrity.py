"""Run-output integrity: make "PIPELINE COMPLETE" mean the outputs are on disk and valid,
and report loudly, at the next start, anything a crash left behind.

Why this exists (2026-11-11)
────────────────────────────
A menu-[4] run logged ``PIPELINE COMPLETE`` and returned the shell prompt normally.
About 50 seconds later the machine bugchecked (0x12B, KNOWN_ISSUES.md OPEN-001). The
last 40 narrative files the run had written were still in the Windows write cache and
came back zero-filled after the reboot. Nothing reported it: the process had already
exited, and a machine crash cannot be logged by anything running on that machine.
The operator found out only because a later check parsed every file.

Requirement (CLAUDE.md §5): failures are loud, never silent. Three checks implement it:

1. ``seal_outputs`` (end of run, before the completion line): every file under the
   output root modified since the run started is flushed to disk (``os.fsync``) and
   validated. Only if all pass does the pipeline log ``PIPELINE COMPLETE``; otherwise
   it logs ``PIPELINE FAILED`` with the file list and exits non-zero. After the flush,
   a crash can no longer erase what the run reported as written.
2. ``check_previous_run`` (start of run): reads the previous pipeline log and reports
   an ERROR if it does not end with ``PIPELINE COMPLETE`` (the run died: native crash,
   machine crash, kill). It then validates every output file modified since that run
   started, so files damaged after a "complete" run are found even with no log trace.
3. ``recheck`` (end of run): the startup findings are re-validated and repeated next to
   the completion line, so they are not lost in scrollback. Files the run rewrote
   correctly are reported as repaired; anything still bad fails the run.

Validation is format-aware but cheap: non-empty; no NUL bytes in text formats (a
zero-filled or NUL-tailed file is the write-cache-loss signature); JSON and GeoJSON
must parse. See also the "Logging policy" in ``voter_data_cleaner.setup_logging()``.
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
from datetime import datetime
from pathlib import Path

COMPLETE_MARKER = 'PIPELINE COMPLETE'
FAILED_MARKER = 'PIPELINE FAILED'

_TEXT_SUFFIXES = {'.json', '.geojson', '.htm', '.html', '.js', '.css', '.md', '.txt', '.csv', '.svg'}
_JSON_SUFFIXES = {'.json', '.geojson'}
_LOG_NAME_RE = re.compile(r'voter_analysis_(\d{8}_\d{6})_pipeline\.log$')


def validate_file(path: Path) -> str | None:
    """Return None if the file looks intact, else a short reason."""
    raw = path.read_bytes()
    if not raw:
        return 'empty'
    if raw.count(0) == len(raw):
        return f'zero-filled ({len(raw)} bytes of NUL)'
    suffix = path.suffix.lower()
    if suffix in _TEXT_SUFFIXES and b'\x00' in raw:
        return f'NUL bytes from offset {raw.index(0)} of {len(raw)}'
    if suffix in _JSON_SUFFIXES:
        try:
            json.loads(raw)
        except ValueError as exc:  # the parse failure IS the finding being reported
            return f'invalid JSON: {exc}'[:160]
    return None


def _files_modified_since(root: Path, since: float) -> list[Path]:
    return [p for p in root.rglob('*') if p.is_file() and p.stat().st_mtime >= since]


def _report(problems: dict[Path, str], root: Path, logger: logging.Logger, header: str) -> None:
    logger.error('%s: %d file(s)', header, len(problems))
    for p, why in list(problems.items())[:50]:
        logger.error('    %s -- %s', p.relative_to(root), why)
    if len(problems) > 50:
        logger.error('    ... and %d more (all listed at DEBUG)', len(problems) - 50)
        for p, why in list(problems.items())[50:]:
            logger.debug('    %s -- %s', p.relative_to(root), why)


def seal_outputs(root: Path, run_start: float, logger: logging.Logger) -> dict[Path, str]:
    """Flush to disk and validate every file under `root` modified since `run_start`.

    Returns {path: reason} for files that failed validation (empty dict = all good).
    """
    t0 = time.time()
    files = _files_modified_since(root, run_start)
    problems: dict[Path, str] = {}
    for p in files:
        with open(p, 'r+b') as fh:  # write access is required for FlushFileBuffers on Windows
            os.fsync(fh.fileno())
        why = validate_file(p)
        if why:
            problems[p] = why
    logger.info(
        'Output seal: %d file(s) under %s modified this run flushed to disk and validated '
        'in %.1fs; %d problem(s)', len(files), root, time.time() - t0, len(problems),
    )
    if problems:
        _report(problems, root, logger, 'Output seal FAILED -- invalid files written this run')
    return problems


def _log_start(log_path: Path) -> float | None:
    m = _LOG_NAME_RE.search(log_path.name)
    if not m:
        return None
    return datetime.strptime(m.group(1), '%Y%m%d_%H%M%S').timestamp()


def check_previous_run(logs_dir: Path, current_log: Path, root: Path,
                       logger: logging.Logger) -> dict[Path, str]:
    """Report how the previous pipeline run ended and validate what changed since it began.

    Returns {path: reason} for invalid output files found (empty dict = none).
    """
    previous = sorted(
        p for p in logs_dir.glob('voter_analysis_*_pipeline.log')
        if p.resolve() != current_log.resolve() and _log_start(p) is not None
    )
    if not previous:
        logger.info('Previous-run check: no earlier pipeline log in %s', logs_dir)
        return {}
    prev = previous[-1]
    with open(prev, 'rb') as fh:
        fh.seek(max(0, prev.stat().st_size - 4096))
        tail = fh.read().decode('utf-8', errors='replace')
    if COMPLETE_MARKER in tail:
        logger.info('Previous-run check: %s ended with %s', prev.name, COMPLETE_MARKER)
    elif FAILED_MARKER in tail:
        logger.error('Previous-run check: %s ended with %s (see that log)', prev.name, FAILED_MARKER)
    else:
        last = [l for l in tail.splitlines() if l.strip()][-1:] or ['(empty log)']
        logger.error(
            'Previous-run check: %s did NOT end with %s -- that run died (native crash, '
            'machine crash or kill). Its last log line: %s', prev.name, COMPLETE_MARKER, last[0][:200],
        )

    since = _log_start(prev)
    t0 = time.time()
    files = _files_modified_since(root, since)
    problems = {p: why for p in files if (why := validate_file(p))}
    logger.info(
        'Previous-run check: validated %d output file(s) modified since %s in %.1fs; %d problem(s)',
        len(files), prev.name, time.time() - t0, len(problems),
    )
    if problems:
        _report(problems, root, logger,
                'Previous-run check FOUND DAMAGED OUTPUT (likely lost writes from a crash)')
    return problems


def install_excepthook(logger: logging.Logger) -> None:
    """Write any uncaught exception, with its traceback, into the run log.

    Python prints uncaught tracebacks to stderr only, so a run that died on an
    exception left a log that simply stopped. The hook logs it at CRITICAL followed by
    the failure marker (so check_previous_run reports it as failed, not as vanished),
    then defers to the original hook so the console output is unchanged.
    """
    import sys
    original = sys.excepthook

    def _hook(exc_type, exc, tb):
        if not issubclass(exc_type, KeyboardInterrupt):
            logger.critical('Uncaught exception', exc_info=(exc_type, exc, tb))
        logger.error('%s: uncaught %s: %s', FAILED_MARKER, exc_type.__name__, exc)
        original(exc_type, exc, tb)

    sys.excepthook = _hook


def recheck(startup_problems: dict[Path, str], root: Path,
            logger: logging.Logger) -> dict[Path, str]:
    """Re-validate files flagged at startup; return those still bad, repeat the findings."""
    if not startup_problems:
        return {}
    # A flagged file that no longer exists was removed by the run (ghost pruning), not
    # left damaged; it is reported as removed, not as still bad.
    removed = [p for p in startup_problems if not p.exists()]
    still_bad = {p: why for p in startup_problems if p.exists() and (why := validate_file(p))}
    repaired = len(startup_problems) - len(still_bad) - len(removed)
    logger.warning(
        'Startup check had flagged %d damaged file(s): %d repaired by this run, '
        '%d removed, %d still bad',
        len(startup_problems), repaired, len(removed), len(still_bad),
    )
    if still_bad:
        _report(still_bad, root, logger, 'STILL DAMAGED after this run (not regenerated by this menu choice)')
    return still_bad
