"""
voter_data_cleaner.py
═════════════════════
Voter data cleaner, analyser, and exporter for the Ohio Secretary of State
statewide voter file format (SWVF_*.txt, 135 columns, ~7.9M rows across 4 files).

WHAT THIS SCRIPT DOES
─────────────────────
1.  Loads one or more SWVF_*.txt files using Polars (not pandas) for speed.
    Polars uses Apache Arrow memory layout and SIMD parallelism; it handles
    Ohio-scale data (~4–5 GB uncompressed) in roughly 1/5 the time and memory
    that pandas would require.

2.  Optionally filters to a single county during load, using chunked streaming
    so only the matching rows are ever materialized in RAM.

3.  Cleans and enriches the data:
      - Parses DATE_OF_BIRTH and REGISTRATION_DATE into proper date types.
      - Derives birth-decade cohorts for age-cohort analysis.
      - Normalises PARTY_AFFILIATION ('R'/'D'/'') into display labels (REP/DEM/UNC).
      - Computes per-voter participation metrics (elections eligible, elections voted,
        turnout rate, frequency class) using Polars' sum_horizontal — a single pass
        over all 89 election columns rather than a Python for-loop.

4.  Builds summary tables (county totals, decade distributions, party cross-tabs,
    district breakdowns, precinct tables) as small Polars DataFrames.

5.  Writes two output formats from the same summary data:
      a. Excel workbook (.xlsx) using xlsxwriter — formatted, multi-sheet.
      b. JSON files into docs/data/ — exact schema consumed by index.html / charts.js.
         The HTML dashboard reads these files directly; no data conversion step needed.

6.  Logs all major steps, timings, and errors to both console and a timestamped
    file in logs/ so failed runs can be debugged without re-running everything.

SCHEMA CHANGES FROM v1 (Montgomery County CSV) → v2 (State SWVF)
──────────────────────────────────────────────────────────────────
v1 column               v2 column
─────────────────────── ─────────────────────────────────────────
SOSIDNUM / CNTYIDNUM    SOS_VOTERID / COUNTY_NUMBER / COUNTY_ID
LASTN, FIRSTN           LAST_NAME, FIRST_NAME
BIRTHYEAR  (int)        DATE_OF_BIRTH  (YYYY-MM-DD string → parsed)
REGDATE  (MM/DD/YYYY)   REGISTRATION_DATE  (YYYY-MM-DD string → parsed)
VOTERSTAT               VOTER_STATUS  ('ACTIVE' | 'CONFIRMATION')
PARTYAFFIL (REP/DEM/UNC) PARTY_AFFILIATION  ('R' | 'D' | '')
STNUM+STDIR+STNAME+APT  RESIDENTIAL_ADDRESS1  (pre-combined single field)
U.S. CONGRESS           CONGRESSIONAL_DISTRICT
STATE SENATE            STATE_SENATE_DISTRICT
STATE HOUSE             STATE_REPRESENTATIVE_DISTRICT
SCHOOL DIST             LOCAL_SCHOOL_DISTRICT
CO BD OF EDUC           STATE_BOARD_OF_EDUCATION
Election cols: YYYYMMDDX  PRIMARY-MM/DD/YYYY | GENERAL-... | SPECIAL-...
Participation values: non-empty str  →  'X' (in-person) | 'R' (absentee) | 'D' (provisional) | ''
"""

# ─────────────────────────────────────────────────────────────────────────────
# Imports
# ─────────────────────────────────────────────────────────────────────────────

import concurrent.futures
import json
from dataclasses import dataclass
try:
    import orjson as _orjson
except ImportError:
    _orjson = None
# psutil is a declared dependency and a hard import: stage memory lines are
# the evidence for OPEN-001, and must fail loudly rather than go missing.
import psutil as _psutil
_psutil_proc = _psutil.Process()
import hashlib
import logging
import os
import platform
import re
import sys
import time
from datetime import date as date_t
from datetime import datetime, timezone
from pathlib import Path

import polars as pl                  # fast columnar processing — replaces pandas for analysis
import pandas as pd                  # used ONLY for xlsxwriter compatibility at write time
import xlsxwriter                    # noqa: F401 — imported for engine availability check


# ─────────────────────────────────────────────────────────────────────────────
# Paths and constants
# ─────────────────────────────────────────────────────────────────────────────

# Every managed location comes from the single resolver, pipeline/paths.py
# (CLAUDE.md section 5). The names stay module globals here so callers and
# tests that read or patch e.g. `voter_data_cleaner.ENRICHED_CACHE` keep working.
# pipeline/ goes on sys.path so the bare sibling import resolves whether this
# module is loaded as `voter_data_cleaner` or `pipeline.voter_data_cleaner`.
_PIPELINE_DIR = str(Path(__file__).resolve().parent)
if _PIPELINE_DIR not in sys.path:
    sys.path.insert(0, _PIPELINE_DIR)
from paths import (  # noqa: E402,F401
    BASE_DIR,
    DATA_DIR,
    DOCS_DIR,
    ENRICHED_CACHE,
    LOGS_DIR,
    OUTPUT_ENV_VAR,
    OUTPUT_OVERRIDDEN,
    PARQUET_DIR,
    PARQUET_ENRICHED_DIR,
    PARTITION_KEY,
    parquet_glob,
)
import memory_budget  # noqa: E402  (sibling module; preflight before heavy loads)
PARQUET_ENRICHED_DIR.mkdir(parents=True, exist_ok=True)
CLASSIFIER_SRC       = Path(__file__)

# Voter frequency thresholds (fraction of eligible elections in which voter participated).
FREQ_HIGH = 0.75   # ≥75% → "Frequent"
FREQ_LOW  = 0.25   # <25% → "Infrequent";  25–74% → "Moderate"

# District fields that appear as breakdown sheets in the Excel workbook.
# These match column names in the SWVF state file exactly.
DISTRICT_FIELDS = [
    'CONGRESSIONAL_DISTRICT',
    'STATE_SENATE_DISTRICT',
    'STATE_REPRESENTATIVE_DISTRICT',
    'LOCAL_SCHOOL_DISTRICT',
    'STATE_BOARD_OF_EDUCATION',
]

# Raw party codes from the state file → human-readable display labels.
# Blank string means the voter has not declared a party affiliation.
PARTY_MAP: dict[str, str] = {
    'R': 'REP',
    'D': 'DEM',
    '':  'UNC',
}
TOP_PARTIES = ['REP', 'DEM', 'UNC']   # columns shown explicitly; everything else → "Other"

ELECTION_TYPE_LABELS: dict[str, str] = {
    'PRIMARY': 'Primary',
    'GENERAL': 'General',
    'SPECIAL': 'Special',
}

# Colors used in the web dashboard JSON (Chart.js backgroundColor values).
# These must match the existing sample JSON files so no CSS changes are needed.
CHART_COLORS = {
    'REP':   '#ef4444',   # red
    'DEM':   '#3b82f6',   # blue
    'UNC':   '#9ca3af',   # grey
    'Other': '#f59e0b',   # amber
    'bar':   '#3b82f6',   # default bar fill
}

# Excel header colors
_DARK_BLUE = '#366092'
_MED_BLUE  = '#4472C4'
_TITLE_BG  = '#D9E1F2'

# Regex that matches the state-format election participation column names.
# Examples: "PRIMARY-03/07/2000", "GENERAL-11/04/2025", "SPECIAL-08/08/2023"
ELEC_RE = re.compile(r'^(PRIMARY|GENERAL|SPECIAL)-(\d{2}/\d{2}/\d{4})$')

MANIFEST_PATH = BASE_DIR / 'download_manifest.json'



def _fmt_mtime(ts: float) -> str:
    """Render an mtime for logs: local time to the second."""
    return datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')


def _cache_is_fresh(logger: logging.Logger | None = None) -> bool:
    """Return True iff enriched cache is newer than raw partitions AND classifier.

    Single freshness resolver for the enriched cache (jurisdictional_groupings
    calls this one rather than keeping a copy). Logs the verdict and the three
    timestamps it compared, per the logging policy in setup_logging(): a cache
    decision that is not in the run log cannot be audited after the fact.
    """
    log = logger or logging.getLogger('voter_analysis')
    if not ENRICHED_CACHE.exists():
        log.info('Enriched cache freshness: STALE -- %s does not exist', ENRICHED_CACHE)
        return False
    cache_mt = ENRICHED_CACHE.stat().st_mtime
    raw_partitions = list(PARQUET_DIR.glob("COUNTY_NUMBER=*"))
    if not raw_partitions:
        log.info('Enriched cache freshness: STALE -- no raw partitions under %s', PARQUET_DIR)
        return False
    newest_raw    = max(raw_partitions, key=lambda p: p.stat().st_mtime)
    latest_raw    = newest_raw.stat().st_mtime
    classifier_mt = CLASSIFIER_SRC.stat().st_mtime
    fresh = cache_mt > max(latest_raw, classifier_mt)
    log.info(
        'Enriched cache freshness: %s -- cache %s vs newest raw partition %s (%s) '
        'and classifier source %s (%s)',
        'FRESH' if fresh else 'STALE', _fmt_mtime(cache_mt), _fmt_mtime(latest_raw),
        newest_raw.name, _fmt_mtime(classifier_mt), CLASSIFIER_SRC.name,
    )
    return fresh


def _write_cache_atomic(df: pl.DataFrame, logger: logging.Logger) -> None:
    """Atomic tmp-then-replace write so a crash cannot corrupt the cache."""
    import json

    # Strict: the cache carries the affiliation window, so it must carry the
    # true snapshot date. get_snapshot_date() raises rather than substituting
    # today, which would stamp a historical rebuild with a window it never had.
    snap = get_snapshot_date(logger)
    iso_date = snap.isoformat()
    window = affiliation_window_years(snap)

    # PARTY_AFFILIATION is only comparable between two files whose windows
    # match (R.C. 3513.19). Recording the window makes that testable instead
    # of something a reader has to remember; see CLAUDE.md sec 4.
    provenance = {
        'provenance_snapshot_date': iso_date,
        'affiliation_window_years': window,
        'affiliation_rule': (
            'R.C. 3513.19: most recent partisan primary ballot within the '
            'current calendar year plus the previous two. Steps at calendar-'
            'year boundaries; boards refresh after each primary. Two '
            'snapshots with different windows are NOT directly comparable '
            'on PARTY_AFFILIATION.'
        ),
    }

    tmp = ENRICHED_CACHE.with_suffix(".parquet.tmp")
    df.write_parquet(tmp, compression="zstd",
                     metadata={k: json.dumps(v) if not isinstance(v, str) else v
                               for k, v in provenance.items()})
    tmp.replace(ENRICHED_CACHE)

    sidecar = ENRICHED_CACHE.with_suffix(".provenance.json")
    sidecar_tmp = sidecar.with_suffix(".json.tmp")
    sidecar_tmp.write_text(json.dumps(provenance, indent=2), encoding='utf-8')
    sidecar_tmp.replace(sidecar)

    logger.info("Enriched cache written: %s", ENRICHED_CACHE)


def get_snapshot_date(logger: 'logging.Logger | None' = None) -> date_t:
    """
    The staged snapshot's own date, as a ``date``. STRICT: raises if nothing
    is staged.

    This is the classifier's time origin (``reference_date``) — the "as of"
    point for decay weights and ``years_since_last_partisan``. Unlike
    get_source_date(), it has NO fallback to today: a silent fallback would
    make a re-run of an OLD snapshot compute as if it were current, which
    turns the snapshot-over-time series into an artifact (every snapshot
    would share one decay origin and real movement would vanish). Loud
    failure over silent degradation (CLAUDE.md sec 5).

    Source of truth is the snapshot FOLDER's ISO name, resolved by
    snapshot_store (the single snapshot resolver) — not file mtimes (gzip
    mtimes are zeroed) and not the election columns (those give the last
    ELECTION date, weeks before the file's release).
    """
    import snapshot_store
    staged = snapshot_store.staged_info()
    if not staged or not staged.get('snapshot_date'):
        raise RuntimeError(
            'No staged snapshot: cannot determine the classifier reference '
            'date. Stage one first (python pipeline/ohio_voter_pipeline.py '
            '--snapshot YYYY-MM-DD). Refusing to fall back to today\'s date, '
            'which would silently mis-date a historical snapshot.'
        )
    iso = staged['snapshot_date']
    try:
        snap = date_t.fromisoformat(iso)
    except ValueError as exc:
        raise RuntimeError(
            f'Staged snapshot_date {iso!r} is not an ISO date (YYYY-MM-DD); '
            'the snapshot folder name is the authoritative source.'
        ) from exc
    if logger:
        logger.info('Classifier reference date (staged snapshot): %s', snap)
    return snap


def affiliation_window_years(snapshot_date: date_t) -> list[int]:
    """
    The calendar years whose primaries can set PARTY_AFFILIATION in a file
    captured on ``snapshot_date``.

    Ohio R.C. 3513.19 (Election Official Manual, "PARTY AFFILIATION"):
    affiliation is the MOST RECENT partisan primary ballot within the
    CURRENT calendar year plus the previous two. The window is bounded by
    calendar years, not a rolling 24 months — it steps on January 1 (a whole
    year drops out at once) and boards refresh affiliation after each
    primary.

    This matters for snapshot-over-snapshot comparison: two files whose
    windows differ are NOT directly comparable on PARTY_AFFILIATION, because
    voters can change affiliation with no behaviour change at all (their
    qualifying ballot simply aged out). Stamped into provenance so the
    incomparability is visible rather than inferred.
    """
    y = snapshot_date.year
    return [y - 2, y - 1, y]


def get_source_date(logger: logging.Logger) -> str:
    """
    Extract the effective source-file date as ``YYYYMMDD``.

    Precedence:
      1. The staged snapshot's own date (``snapshot_store.staged_info()`` ->
         ``staged_from.json``) -- the true source-file date.
      2. The most recent ``last_modified`` in ``download_manifest.json``.
      3. Today's date (last resort).
    Each fallback logs a loud warning so a wrong output date is never silent.
    """
    # 1 -- staged snapshot date (authoritative source-file date).
    import snapshot_store
    staged = snapshot_store.staged_info()
    if staged and staged.get('snapshot_date'):
        snap_date = staged['snapshot_date']
        logger.info('Source-file date from staged snapshot: %s', snap_date)
        return snap_date.replace('-', '')
    logger.warning('No staged snapshot (staged_from.json) -- falling back to '
                   'download_manifest.json for the source date')

    if not MANIFEST_PATH.exists():
        logger.warning('download_manifest.json not found — falling back to today\'s date '
                       'for output filenames (source-file date is unavailable)')
        return date_t.today().strftime('%Y%m%d')

    try:
        raw = json.loads(MANIFEST_PATH.read_text(encoding='utf-8'))
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning('download_manifest.json could not be read (%s) — '
                       'falling back to today\'s date', exc)
        return date_t.today().strftime('%Y%m%d')

    # Collect all last_modified values from per-file entries
    dates: list[datetime] = []
    for key, val in raw.items():
        if not isinstance(val, dict):
            continue
        lm = val.get('last_modified')
        if not lm or lm == 'unknown':
            continue
        # HTTP Last-Modified format: "Thu, 24 Apr 2026 18:30:00 GMT"
        for fmt in ('%a, %d %b %Y %H:%M:%S %Z', '%a, %d %b %Y %H:%M:%S GMT'):
            try:
                dates.append(datetime.strptime(lm, fmt))
                break
            except ValueError:
                continue

    if not dates:
        logger.warning('No parseable last_modified dates in download_manifest.json — '
                       'falling back to today\'s date')
        return date_t.today().strftime('%Y%m%d')

    source_dt = max(dates)
    logger.info('Source-file date from manifest: %s', source_dt.strftime('%Y-%m-%d'))
    return source_dt.strftime('%Y%m%d')

# Ohio county number → county name.
# County numbers run 01–88 and are zero-padded strings in the voter file.
# Ohio SOS official county numbering: 01–88, strictly alphabetical by county name.
# This is NOT geographic — it is the state's administrative index.
# Numbers are zero-padded to two digits and used as:
#   - Hive partition keys in the Parquet cache  (COUNTY_NUMBER=57/)
#   - SWVF COUNTY_NUMBER field values
#   - manifest slug lookups and dashboard data file prefixes
# Source: Ohio Secretary of State Statewide Voter File documentation.
# Display name of the statewide aggregate. export_json slugs it to
# 'ohio_(statewide)', the file prefix the dashboard's 'state' level and the
# landing page's headline count read (docs/assets/v2.js STATE_SLUG).
STATEWIDE_NAME = 'Ohio (Statewide)'

OHIO_COUNTIES: dict[str, str] = {
    '01': 'Adams',       '02': 'Allen',       '03': 'Ashland',     '04': 'Ashtabula',
    '05': 'Athens',      '06': 'Auglaize',    '07': 'Belmont',     '08': 'Brown',
    '09': 'Butler',      '10': 'Carroll',     '11': 'Champaign',   '12': 'Clark',
    '13': 'Clermont',    '14': 'Clinton',     '15': 'Columbiana',  '16': 'Coshocton',
    '17': 'Crawford',    '18': 'Cuyahoga',    '19': 'Darke',       '20': 'Defiance',
    '21': 'Delaware',    '22': 'Erie',        '23': 'Fairfield',   '24': 'Fayette',
    '25': 'Franklin',    '26': 'Fulton',      '27': 'Gallia',      '28': 'Geauga',
    '29': 'Greene',      '30': 'Guernsey',    '31': 'Hamilton',    '32': 'Hancock',
    '33': 'Hardin',      '34': 'Harrison',    '35': 'Henry',       '36': 'Highland',
    '37': 'Hocking',     '38': 'Holmes',      '39': 'Huron',       '40': 'Jackson',
    '41': 'Jefferson',   '42': 'Knox',        '43': 'Lake',        '44': 'Lawrence',
    '45': 'Licking',     '46': 'Logan',       '47': 'Lorain',      '48': 'Lucas',
    '49': 'Madison',     '50': 'Mahoning',    '51': 'Marion',      '52': 'Medina',
    '53': 'Meigs',       '54': 'Mercer',      '55': 'Miami',       '56': 'Monroe',
    '57': 'Montgomery',  '58': 'Morgan',      '59': 'Morrow',      '60': 'Muskingum',
    '61': 'Noble',       '62': 'Ottawa',      '63': 'Paulding',    '64': 'Perry',
    '65': 'Pickaway',    '66': 'Pike',        '67': 'Portage',     '68': 'Preble',
    '69': 'Putnam',      '70': 'Richland',    '71': 'Ross',        '72': 'Sandusky',
    '73': 'Scioto',      '74': 'Seneca',      '75': 'Shelby',      '76': 'Stark',
    '77': 'Summit',      '78': 'Trumbull',    '79': 'Tuscarawas',  '80': 'Union',
    '81': 'Van Wert',    '82': 'Vinton',      '83': 'Warren',      '84': 'Washington',
    '85': 'Wayne',       '86': 'Williams',    '87': 'Wood',        '88': 'Wyandot',
}


# ─────────────────────────────────────────────────────────────────────────────
# Logging setup
# ─────────────────────────────────────────────────────────────────────────────

# File objects faulthandler writes to. faulthandler holds only the file descriptor,
# so the Python file object must stay referenced for the life of the process.
_CRASH_LOG_HANDLES: list = []


def enable_crash_log(log_path: Path, logger: logging.Logger) -> None:
    """Route native-crash tracebacks (faulthandler) into this run's log file.

    See the logging policy in setup_logging() for why. Re-enabling points the
    handler at the newest run log, so a crash lands in the log of the phase
    that was running (pipeline vs. groupings).
    """
    import faulthandler
    handle = open(log_path, 'a', encoding='utf-8')
    faulthandler.enable(file=handle, all_threads=True)
    _CRASH_LOG_HANDLES.append(handle)
    logger.debug('faulthandler enabled -> %s', log_path)


class IsoOffsetFormatter(logging.Formatter):
    """Log formatter whose %(asctime)s is ISO 8601 local time with its UTC offset,
    e.g. 2026-11-12T01:34:07-05:00.

    The offset is what lets a log line be matched against OS event logs (kept
    in UTC) and against logs from other machines without guessing a time zone
    or a daylight-saving state. It is computed per record, so a run that
    crosses a DST change stays correct. `datefmt` is ignored.
    """

    def formatTime(self, record, datefmt=None):
        return datetime.fromtimestamp(record.created).astimezone().isoformat(timespec='seconds')


_RUN_ID: str | None = None


def claim_run_id(stamp: str) -> str:
    """Return this process's run id, setting it to `stamp` if none is set yet.

    The run id is the YYYYMMDD_HHMMSS stamp of the first log file the process
    opens, so it equals the stamp in that log's file name. Every later log of
    the same process (the groupings log) records the same id, which is what the
    logs of one run are joined on. First caller wins; later stamps are ignored.
    """
    global _RUN_ID
    if _RUN_ID is None:
        _RUN_ID = stamp
    return _RUN_ID


def machine_fingerprint() -> dict:
    """Platform-neutral description of the machine this run is on.

    Sources: `platform`, os.cpu_count and psutil only. No OS-specific query,
    and nothing that identifies a person or a host: no hostname, user name or
    path. It records what the machine was, not whether it is healthy.

    config_id   12 hex chars of SHA-256 over (os, cpu, cpu_count, ram_total_gb).
                A different id in two run logs means the OS build, the CPU or
                the installed RAM changed between them. Swap is left out on
                purpose: a dynamically sized pagefile changes total swap from
                run to run with no configuration change. Limit: replacing DIMMs
                with others of the same total size does not change the id.
    swap_total_gb  None when the OS will not report it
                (memory_budget.swap_total_bytes).
    boot_time   When the machine last started, with UTC offset. A different
                value in two consecutive run logs means a reboot or a crash
                happened between them.
    Sizes are GiB (2**30 bytes), labelled GB like the MB figures in _memory_line.
    """
    fp = {
        'os':           platform.platform(),
        'cpu':          platform.processor() or platform.machine(),
        'cpu_count':    os.cpu_count(),
        'ram_total_gb': round(_psutil.virtual_memory().total / 2**30, 1),
    }
    fp['config_id'] = hashlib.sha256(
        '|'.join(str(fp[k]) for k in ('os', 'cpu', 'cpu_count', 'ram_total_gb')).encode('utf-8')
    ).hexdigest()[:12]
    swap = memory_budget.swap_total_bytes(_psutil)
    fp['swap_total_gb'] = None if swap is None else round(swap / 2**30, 1)
    fp['boot_time'] = (datetime.fromtimestamp(_psutil.boot_time()).astimezone()
                       .isoformat(timespec='seconds'))
    return fp


def log_run_context(logger: logging.Logger) -> None:
    """Record what produced this run: run id, process, interpreter, libraries,
    code, memory, output root and machine fingerprint.

    See the logging policy in setup_logging() for why each item is here.
    The output-root line states whether ELECTION_DATA_OUT redirected this run;
    it is the first thing to check when output lands somewhere unexpected.
    """
    import subprocess
    run = claim_run_id(datetime.now().strftime('%Y%m%d_%H%M%S'))
    logger.info('Run context: run id %s, started %s UTC', run,
                datetime.now(timezone.utc).isoformat(timespec='seconds'))
    logger.info('Run context: pid=%d argv=%s cwd=%s', os.getpid(), sys.argv, Path.cwd())
    logger.info(
        'Run context: python %s (%s), polars %s, polars thread pool %d',
        sys.version.split()[0], sys.executable, pl.__version__, pl.thread_pool_size(),
    )
    sha = subprocess.run(['git', '-C', str(BASE_DIR), 'rev-parse', 'HEAD'],
                         capture_output=True, text=True, check=False)
    branch = subprocess.run(['git', '-C', str(BASE_DIR), 'rev-parse', '--abbrev-ref', 'HEAD'],
                            capture_output=True, text=True, check=False)
    if sha.returncode == 0 and branch.returncode == 0:
        logger.info('Run context: git %s on %s', sha.stdout.strip(), branch.stdout.strip())
    else:
        logger.info('Run context: git commit not recorded (%s)',
                    (sha.stderr or branch.stderr).strip()[:200])
    vm = _psutil.virtual_memory()
    logger.info('Run context: RAM total %.1f GB, available %.1f GB at start',
                vm.total / 2**30, vm.available / 2**30)
    fp = machine_fingerprint()
    swap = 'unknown' if fp['swap_total_gb'] is None else f"{fp['swap_total_gb']:.1f} GB"
    logger.info('Run context: machine %s | %s | %d logical CPUs | RAM %.1f GB | swap %s',
                fp['os'], fp['cpu'], fp['cpu_count'], fp['ram_total_gb'], swap)
    logger.info('Run context: machine config id %s, booted %s', fp['config_id'], fp['boot_time'])
    logger.info('Run context: output root %s (%s)', DOCS_DIR,
                f'{OUTPUT_ENV_VAR} override' if OUTPUT_OVERRIDDEN else 'default, the published tree')


def setup_logging(run_label: str = '') -> logging.Logger:
    """
    Configure a logger that writes to both:
      - Console  (INFO level — concise progress for interactive use)
      - Log file (DEBUG level — full detail for post-run debugging)

    The log file is timestamped so each run produces its own file, making
    it easy to compare across runs or share a failing run's log for review.

    Logging policy (verbose by default, added 2026-11-11)
    ─────────────────────────────────────────────────────
    Each item below exists because a real run went wrong and the log could not
    say why. The log file is the record of a run; keep these on by default.

    * Run context at start (log_run_context): pid, argv, Python and polars
      versions, polars thread pool, git commit and branch, total and available
      RAM. Two full-state runs died at ~21 GB RSS (2026-10-30 and 2026-11-11,
      KNOWN_ISSUES.md OPEN-001) with no record of what memory was free, which
      code was running, or which libraries were loaded.
    * Native crash tracebacks (enable_crash_log): faulthandler writes the
      Python stack of every thread into this log file on a segfault or access
      violation. The 2026-11-11 crash (0xc0000005 in VCRUNTIME140.dll) left no
      traceback and no log line; it was found only in the Windows event log.
    * Freshness verdicts (_cache_is_fresh and the groupings resume check):
      every reuse-or-rebuild decision logs FRESH/STALE with the timestamps it
      compared. The groupings resume check once skipped 3,062 outdated
      7-cohort files as "already exported" on every regeneration (May to
      November 2026), silently mixing data vintages on the public site.
    * Completion line (ohio_voter_pipeline._dispatch): "PIPELINE COMPLETE" is
      the last line of a successful run. A native crash returns the shell
      prompt as if the run had finished quickly; a log without that line
      ended abnormally, however it looked in the terminal.

    * Machine fingerprint (machine_fingerprint): OS build, CPU, logical CPU
      count, total RAM and swap, boot time and a short config id, from
      platform and psutil only. The November 2026 crashes had to be matched
      to the machine's state from memory; the log now records what the
      machine was. The fingerprint holds no hostname, user name or path.
    * Timestamps with UTC offset (IsoOffsetFormatter) and one run id per
      process (claim_run_id), written into every log of the run and onto the
      completion line. Matching the 2026-11-11 crash to the OS event log,
      which is kept in UTC, meant inferring the offset; and the pipeline and
      groupings logs of one run had nothing in common to join on.

    Returns the root logger configured with both handlers.
    """
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    timestamp    = datetime.now().strftime('%Y%m%d_%H%M%S')
    claim_run_id(timestamp)   # first log of the process: its stamp is the run id
    suffix       = f'_{run_label}' if run_label else ''
    log_path     = LOGS_DIR / f'voter_analysis_{timestamp}{suffix}.log'

    logger = logging.getLogger('voter_analysis')

    # If the logger already has handlers it was configured earlier this session
    # (e.g. Cell 1 re-run in the notebook without a kernel restart).  Reuse it
    # so we don't create a new log file and duplicate console output on every run.
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)   # capture everything; handlers filter individually

    # ── Console handler: INFO and above, no timestamps (they clutter the terminal) ──
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter('%(levelname)-8s  %(message)s'))
    logger.addHandler(console_handler)

    # ── File handler: DEBUG and above, full timestamps for forensic debugging ──
    file_handler = logging.FileHandler(log_path, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(IsoOffsetFormatter(
        '%(asctime)s  %(levelname)-8s  %(funcName)s:%(lineno)d  %(message)s',
    ))
    logger.addHandler(file_handler)

    logger.info('Log file: %s', log_path)
    enable_crash_log(log_path, logger)
    log_run_context(logger)
    return logger


def _memory_line() -> str:
    """Process and system memory for one log line, in MB, from psutil only.

    RSS   resident set: physical pages this process holds now.
    VMS   virtual size. On Windows psutil reports pagefile-backed private
          commit here, the figure the commit limit applies to. OPEN-001
          incident 2 failed on commit while RSS still fit, so RSS alone
          does not show that failure coming.
    avail system memory obtainable without paging, the number a memory
          budget is measured against.

    A point-in-time reading at the moment of the call, not a peak: memory
    freed inside a stage is not visible here. The allocator can also keep
    freed pages, so RSS may not fall after a large frame is released.
    """
    mem = _psutil_proc.memory_info()
    avail = _psutil.virtual_memory().available
    return (f'RSS={mem.rss / 1_048_576:,.0f} MB  VMS={mem.vms / 1_048_576:,.0f} MB  '
            f'avail={avail / 1_048_576:,.0f} MB')


def _timer(logger: logging.Logger, step: str):
    """
    Context manager that logs how long a labelled step took and the memory
    reading at its end (_memory_line), on success and on failure.

    Usage:
        with _timer(log, "loading voter files"):
            df = load_voter_files(...)
    """
    class _T:
        def __enter__(self):
            self._start = time.perf_counter()
            logger.info('► %s ...', step)
            return self
        def __exit__(self, exc_type, exc_val, exc_tb):
            elapsed = time.perf_counter() - self._start
            mem_str = _memory_line()
            if exc_type:
                logger.error('✗ %s failed after %.1f s — %s: %s  %s',
                             step, elapsed, exc_type.__name__, exc_val, mem_str)
            else:
                logger.info('  ✓ %s  (%.1f s)  %s', step, elapsed, mem_str)
            return False   # do not suppress exceptions
    return _T()


# ─────────────────────────────────────────────────────────────────────────────
# Election-column helpers
# ─────────────────────────────────────────────────────────────────────────────

def identify_election_cols(df: pl.DataFrame) -> list[str]:
    """
    Find and sort all election participation columns in the DataFrame.

    The state voter file has one column per election held since 2000,
    named "TYPE-MM/DD/YYYY" (e.g. "GENERAL-11/04/2025").  Participation
    values are: 'X' (in-person), 'R' (absentee), 'D' (provisional), '' (did not vote).

    Returns columns sorted chronologically by election date so participation
    metrics accumulate in the correct order.
    """
    return election_cols_from_names(df.columns)


def election_cols_from_names(names) -> list[str]:
    """Election participation column names among `names`, in chronological order.

    Works from names alone, so the election columns of a parquet file can be
    listed from its schema without loading a row. identify_election_cols is
    this applied to a frame's columns; there is no second matcher.
    """
    cols = [c for c in names if ELEC_RE.match(c)]
    # Sort key routes through the resolver: parse_election_meta is the only place
    # an election column name is decomposed into a date (single-resolver law).
    cols.sort(key=lambda c: parse_election_meta(c)[0])
    return cols


def parse_election_meta(col: str) -> tuple[date_t | None, str | None, str | None]:
    """
    Decompose an election column name into its component parts.

    'PRIMARY-03/07/2000' → (date(2000, 3, 7), 'PRIMARY', 'Primary')

    Returns (None, None, None) if the column name doesn't match the expected pattern.
    This can happen if the SOS adds a new election type not in ELECTION_TYPE_LABELS.
    """
    m = ELEC_RE.match(col)
    if not m:
        return None, None, None
    type_code, date_str = m.groups()
    try:
        elec_date = datetime.strptime(date_str, '%m/%d/%Y').date()
    except ValueError:
        return None, None, None
    return elec_date, type_code, ELECTION_TYPE_LABELS.get(type_code, type_code)


@dataclass(frozen=True)
class ColMeta:
    """Fully resolved metadata for one election column.

    The two semantic axes (is_regular, partisan_capable) are the whole point of
    the A0 collapse: neither can be read from the column NAME (the SoS labels
    both the Aug-2022 partisan primary and a Sept-2011 municipal issue election
    'PRIMARY'). They come from date-position + a bounded profile of the column's
    observed ballot codes, computed once by build_col_meta().

    Fields:
      col              raw column name (e.g. 'PRIMARY-08/02/2022')
      date             parsed election date
      type_code        'PRIMARY' | 'GENERAL' | 'SPECIAL'
      is_regular       on every precinct's ballot on a fixed cycle: the first
                       Mar-May primary of its year, or a November general.
                       Drives the participation-eligibility denominator.
      partisan_capable the election offered party ballots (>=1 D or R cast).
                       Drives the cohort partisan signal. Aug-2022 is
                       partisan_capable=True, is_regular=False; a Sept-2011
                       issue-only primary is partisan_capable=False.
      rule             human-readable tag naming the rule that classified this
                       column, committed to the reference table as work-shown.
    """
    col:              str
    date:             date_t
    type_code:        str
    is_regular:       bool
    partisan_capable: bool
    rule:             str


def build_col_meta(
    df:            pl.DataFrame,
    election_cols: list[str],
    logger:        'logging.Logger | None' = None,
    *,
    read_cells=None,
) -> list[ColMeta]:
    """THE single election-column resolver (CLAUDE.md sec 5 single-resolver law).

    Given the voter DataFrame and its election columns, returns one ColMeta per
    column, chronologically ordered, carrying date + type (from the name, via
    parse_election_meta) AND is_regular + partisan_capable (from date-position +
    a bounded per-column ballot-code profile). Every downstream consumer — the
    cohort classifier, the participation engine, the Excel summaries — obtains
    date/type/regular/partisan facts ONLY from here. There is no second parser.

    read_cells: optional callable(list of names) -> DataFrame of those election
    columns. Give it when `df` does not hold the election columns itself (the
    county export: load_enriched_for_export). The ballot-code profile is then
    taken _PARTICIPATION_BATCH columns at a time. Without it the profile reads
    `df` in one pass, as before.

    partisan_capable: profiled, not named. For each column we count D and R
    ballots in a single vectorised pass; partisan_capable == (n_D + n_R >= 1).

    is_regular: date-position over the resolved type. The regular spine is one
    primary per calendar year (the earliest PRIMARY in Mar-May of that year)
    plus the November general. Off-cycle primaries, non-November generals, and
    every SPECIAL are excluded. On the current Ohio snapshot this yields
    25 regular primaries + 27 November generals = 52 regular columns.
    """
    parsed: list[tuple[str, date_t, str]] = []
    for c in election_cols:
        elec_date, type_code, _ = parse_election_meta(c)
        if elec_date is None:
            if logger:
                logger.warning('build_col_meta: unparseable election column "%s" '
                               '- excluded from metadata', c)
            continue
        parsed.append((c, elec_date, type_code))
    parsed.sort(key=lambda t: t[1])

    # -- partisan_capable: bounded ballot-code profile, one columnar pass ------
    # Count D and R per column across all rows; a column with >=1 partisan
    # ballot offered party primaries. This is the ONLY place ballot codes are
    # read to determine election meaning.
    partisan_capable: dict[str, bool] = {}
    if parsed:
        cols = [c for c, _, _ in parsed]

        def _partisan_counts(frame: pl.DataFrame, names: list[str]) -> tuple:
            return frame.select([
                ((pl.col(c).str.strip_chars() == 'D') |
                 (pl.col(c).str.strip_chars() == 'R')).sum().alias(c)
                for c in names
            ]).row(0)

        if read_cells is None:
            counts = _partisan_counts(df, cols)
        else:
            counts = ()
            for start in range(0, len(cols), _PARTICIPATION_BATCH):
                batch = cols[start:start + _PARTICIPATION_BATCH]
                counts += _partisan_counts(read_cells(batch), batch)
        for (c, _, _), n_partisan in zip(parsed, counts):
            partisan_capable[c] = (n_partisan or 0) >= 1

    # -- is_regular: date-position over the resolved type ---------------------
    # First PRIMARY (by date) held in each calendar year, if it falls Mar-May,
    # is that year's regular primary. November generals are regular. All else
    # (off-cycle primaries, non-Nov generals, SPECIALs) is not.
    first_primary_of_year: dict[int, date_t] = {}
    for c, d, type_code in parsed:
        if type_code == 'PRIMARY':
            cur = first_primary_of_year.get(d.year)
            if cur is None or d < cur:
                first_primary_of_year[d.year] = d

    out: list[ColMeta] = []
    for c, d, type_code in parsed:
        pc = partisan_capable.get(c, False)
        if type_code == 'SPECIAL':
            is_regular = False
            rule = 'SPECIAL -> not regular'
        elif type_code == 'GENERAL':
            is_regular = (d.month == 11)
            rule = ('GENERAL in November -> regular' if is_regular
                    else 'GENERAL off-November -> special')
        elif type_code == 'PRIMARY':
            is_first = (first_primary_of_year.get(d.year) == d)
            is_regular = is_first and d.month in (3, 4, 5)
            if is_regular:
                rule = 'PRIMARY, first Mar-May of year -> regular'
            elif not is_first:
                rule = 'PRIMARY, not first of year -> off-cycle special'
            else:
                rule = 'PRIMARY outside Mar-May -> off-cycle special'
        else:
            is_regular = False
            rule = f'{type_code} -> not regular'
        out.append(ColMeta(
            col=c, date=d, type_code=type_code,
            is_regular=is_regular, partisan_capable=pc, rule=rule,
        ))

    if logger:
        n_reg = sum(1 for m in out if m.is_regular)
        n_partisan = sum(1 for m in out if m.partisan_capable)
        logger.info('build_col_meta: %d election columns resolved '
                    '(%d regular, %d partisan-capable)',
                    len(out), n_reg, n_partisan)
    return out


def write_col_meta_reference(
    col_meta: list[ColMeta],
    out_path: 'Path',
) -> None:
    """Commit the resolver's classification table as human-readable work-shown.

    One row per election column: col, date, type, is_regular, partisan_capable,
    and the rule that tagged it. This is the free-tier audit artifact — every
    column's classification is verifiable by eye against the committed table.
    """
    lines = [
        '# Election-column classification table (A0 resolver output)',
        '#',
        '# Generated by pipeline/voter_data_cleaner.py:build_col_meta / '
        'write_col_meta_reference.',
        '# Each row is one election column tagged on two axes by the SINGLE '
        'election-metadata',
        '# resolver. is_regular / partisan_capable are NOT read from the column '
        'name: they come',
        '# from date-position + a bounded per-column ballot-code profile '
        '(CLAUDE.md sec 5).',
        '#',
        f'# Columns: {len(col_meta)}  |  '
        f'regular: {sum(1 for m in col_meta if m.is_regular)}  |  '
        f'partisan_capable: {sum(1 for m in col_meta if m.partisan_capable)}',
        '',
    ]
    header = ('| ' + f'{"column":<22}' + ' | ' + f'{"date":<10}' + ' | '
              + f'{"type":<8}' + ' | ' + f'{"is_regular":<10}' + ' | '
              + f'{"partisan_capable":<16}' + ' | rule |')
    sep = ('|' + '-' * 24 + '|' + '-' * 12 + '|' + '-' * 10 + '|'
           + '-' * 12 + '|' + '-' * 18 + '|' + '-' * 6 + '|')
    lines.append(header)
    lines.append(sep)
    for m in col_meta:
        lines.append(
            f'| {m.col:<22} | {m.date.isoformat():<10} | {m.type_code:<8} | '
            f'{str(m.is_regular):<10} | {str(m.partisan_capable):<16} | {m.rule} |'
        )
    out_path.write_text('\n'.join(lines) + '\n', encoding='utf-8')



# ─────────────────────────────────────────────────────────────────────────────
# Parquet cache  (one-time conversion; fast loads on every subsequent run)
# ─────────────────────────────────────────────────────────────────────────────

def build_parquet_cache(
    txt_files:   list[Path],
    parquet_dir: Path | None = None,
    logger:      logging.Logger | None = None,
) -> Path:
    """
    Convert SWVF_*.txt → Hive-partitioned Parquet on first run.

    Layout: source/parquet/COUNTY_NUMBER=01/, COUNTY_NUMBER=02/, ...

    Idempotent: skips conversion if all 88 partitions already exist.
    Subsequent loads via load_voter_files_parquet() skip CSV entirely —
    load time drops from ~4 min to under 60 s at Ohio scale.
    """
    import pyarrow as pa
    import pyarrow.dataset as ds

    log  = logger or logging.getLogger('voter_analysis')
    pdir = parquet_dir or PARQUET_DIR
    pdir.mkdir(parents=True, exist_ok=True)

    existing = {p.name for p in pdir.iterdir() if p.is_dir()}
    expected = {f'COUNTY_NUMBER={n:02d}' for n in range(1, 89)}
    if existing >= expected:
        log.info('Parquet cache complete (%d partitions) — skipping.', len(existing))
        return pdir

    log.info('Parquet cache missing %d partition(s). Building from %d txt file(s).',
             len(expected - existing), len(txt_files))
    # The build below reads the whole raw frame into memory: check it fits first.
    _preflight_text_build(txt_files, log)

    with _timer(log, 'read CSV for Parquet conversion'):
        df = pl.concat([
            pl.scan_csv(p, separator=',', quote_char='"',
                        infer_schema=False, encoding='utf8-lossy',
                        ignore_errors=True)
            for p in txt_files
        ]).collect()
        log.info('  %s rows loaded', f'{len(df):,}')

    with _timer(log, 'write Hive-partitioned Parquet'):
        ds.write_dataset(
            df.to_arrow(),
            base_dir=str(pdir),
            format='parquet',
            partitioning=ds.partitioning(
                pa.schema([pa.field('COUNTY_NUMBER', pa.string())]),
                flavor='hive',
            ),
            file_options=ds.ParquetFileFormat().make_write_options(compression='snappy'),
            existing_data_behavior='overwrite_or_ignore',
        )
        n = sum(1 for p in pdir.iterdir() if p.is_dir())
        log.info('  %d county partitions written to %s', n, pdir)

    return pdir


def load_voter_files_parquet(
    parquet_dir:   Path | None = None,
    county_number: str | None  = None,
    logger:        logging.Logger | None = None,
) -> pl.DataFrame:
    """Load from Hive-partitioned Parquet. Requires build_parquet_cache() first."""
    log  = logger or logging.getLogger('voter_analysis')
    pdir = parquet_dir or PARQUET_DIR

    if not pdir.exists():
        raise FileNotFoundError(f'Parquet cache not found at {pdir}. Run build_parquet_cache() first.')

    if county_number:
        cnum = county_number.strip().zfill(2)
        part = pdir / f'COUNTY_NUMBER={cnum}'
        if not part.exists():
            part = pdir / f'COUNTY_NUMBER={int(cnum)}'
        if not part.exists():
            raise FileNotFoundError(f'No Parquet partition for county {cnum}.')
        log.info('Loading Parquet partition: %s', part.name)
        df = pl.read_parquet(parquet_glob(part))
        if 'COUNTY_NUMBER' not in df.columns:
            df = df.with_columns(pl.lit(cnum).alias('COUNTY_NUMBER'))
    else:
        log.info('Loading full Parquet cache: %s', pdir)
        df = pl.read_parquet(parquet_glob(pdir), hive_partitioning=True)

    # PyArrow may infer COUNTY_NUMBER as integer from the Hive partition key.
    # Cast to String so all downstream str.strip_chars() calls work correctly.
    if df['COUNTY_NUMBER'].dtype != pl.String:
        df = df.with_columns(
            pl.col('COUNTY_NUMBER').cast(pl.String).str.zfill(2)
        )

    log.info('Loaded %s rows x %d columns from Parquet', f'{len(df):,}', len(df.columns))
    return df


# ─────────────────────────────────────────────────────────────────────────────
# File loading  (Polars — lazy scan + optional county filter)
# ─────────────────────────────────────────────────────────────────────────────

def load_voter_files(
    txt_files:     list[Path],
    county_number: str | None   = None,
    logger:        logging.Logger | None = None,
) -> pl.DataFrame:
    """
    Load and concatenate SWVF_*.txt files using Polars lazy scanning.

    WHY LAZY SCANNING:
    Polars' scan_csv() builds a query plan without reading the file.  When we
    call .collect() the query optimizer pushes the county filter down to the
    file reader so only matching rows are ever decoded — on a 1.4 GB file this
    can reduce RAM usage by 50–98 % depending on the county size.

    WHY infer_schema=False:
    All 135 columns are treated as plain strings on load.  This avoids Polars
    mis-casting election participation columns ('X'/'R'/'D'/'') as booleans or
    integers, and prevents type errors on columns with mixed content.
    Typed columns (DATE_OF_BIRTH, REGISTRATION_DATE, BIRTHYEAR) are cast
    explicitly in clean_voter_data().

    Args:
        txt_files:     Ordered list of SWVF_*.txt paths (typically 4 files).
        county_number: Zero-padded county string, e.g. '57' for Montgomery.
                       Pass None to load all 88 counties (full Ohio).
        logger:        Caller's logger instance; falls back to print if None.
    """
    log = logger or logging.getLogger('voter_analysis')
    frames: list[pl.LazyFrame] = []

    for path in txt_files:
        log.debug('Scanning %s', path.name)

        # scan_csv returns a LazyFrame — nothing is read yet
        lf = pl.scan_csv(
            path,
            separator=',',
            quote_char='"',
            infer_schema=False,      # all cols as Utf8/String until explicitly cast
            encoding='utf8-lossy',   # Polars only accepts 'utf8' or 'utf8-lossy';
                                     # utf8-lossy silently replaces invalid UTF-8 bytes
                                     # (Windows-1252 chars in voter names) with U+FFFD
            ignore_errors=True,      # malformed rows are skipped, not fatal
        )

        if county_number:
            # Push filter into the scan so only matching rows are decoded.
            # str.strip() handles any accidental leading/trailing whitespace
            # in the COUNTY_NUMBER field.
            lf = lf.filter(
                pl.col('COUNTY_NUMBER').str.strip_chars() == county_number
            )

        frames.append(lf)

    if not frames:
        raise ValueError('No voter files provided to load_voter_files()')

    # Concatenate all lazy frames, then collect (execute) the full query plan.
    # Polars will parallelise reads across files on multi-core systems.
    log.info('Collecting %d file(s)%s ...',
             len(frames),
             f' filtered to county {county_number}' if county_number else ' (full state)')

    combined = pl.concat(frames).collect()

    log.info('Loaded: %s rows × %d columns', f'{len(combined):,}', len(combined.columns))
    return combined


# ─────────────────────────────────────────────────────────────────────────────
# Data cleaning and enrichment  (Polars)
# ─────────────────────────────────────────────────────────────────────────────

def clean_voter_data(df: pl.DataFrame, logger: logging.Logger) -> pl.DataFrame:
    """
    Validate, parse, and enrich the raw voter DataFrame.

    Transformations applied:
      DATE_OF_BIRTH     → BIRTHYEAR (Int32) + Decade (Int32) for cohort analysis
      REGISTRATION_DATE → REGDATE_DT (Date) for eligibility comparisons
      PARTY_AFFILIATION → PARTY_LABEL (Utf8: REP/DEM/UNC/Other) for cross-tabs
      VOTER_STATUS      → normalised to uppercase, trimmed

    Rows with unparseable birth dates or birth years outside 1900–current year
    are dropped.  All other rows are retained regardless of status.
    """
    initial_count = len(df)

    # ── Birth year: parse full date, extract year, derive decade ──────────────
    # DATE_OF_BIRTH is "YYYY-MM-DD" in the state file.
    # We derive BIRTHYEAR as an Int32 so we can do arithmetic (// 10 * 10 for decade).
    df = df.with_columns([
        pl.col('DATE_OF_BIRTH')
          .str.to_date(format='%Y-%m-%d', strict=False)   # strict=False → null on failure
          .dt.year()
          .alias('BIRTHYEAR'),
    ])

    # Drop rows where birth year could not be parsed or is out of plausible range.
    # This removes test records, data-entry errors, and placeholder entries.
    current_year = date_t.today().year
    invalid_mask = ~(
        pl.col('BIRTHYEAR').is_not_null() &
        pl.col('BIRTHYEAR').is_between(1900, current_year)
    )
    invalid_rows = df.filter(invalid_mask)

    if len(invalid_rows) > 0:
        # Write bad rows to a CSV error file instead of looping through them in Python.
        # iter_rows() on tens-of-thousands of invalid records + per-row logger.warning()
        # calls are extremely slow (blocks on disk I/O each iteration) and caused silent
        # process death on full-Ohio 7.9M-row runs.  A single Polars write_csv() is
        # ~1000× faster and produces a file analysts can inspect directly.
        error_dir = Path(__file__).resolve().parent.parent / 'local' / 'working' / 'errors'
        error_dir.mkdir(parents=True, exist_ok=True)
        error_file = error_dir / f'invalid_birthyear_{logger.name}.csv'
        invalid_rows.select('SOS_VOTERID', 'DATE_OF_BIRTH', 'BIRTHYEAR').write_csv(error_file)
        logger.warning(
            'Dropped %d rows with invalid birth years (out of range 1900–%d) — '
            'details: %s',
            len(invalid_rows), current_year, error_file,
        )

    df = df.filter(~invalid_mask)

    # Create decade cohort column: 1987 → 1980, 2003 → 2000, etc.
    df = df.with_columns([
        (pl.col('BIRTHYEAR') // 10 * 10).alias('Decade'),
    ])

    # Create generational cohort column using Pew Research Center delineations.
    # Boundaries:
    #   Silent/Greatest : ≤ 1945
    #   Baby Boomers    : 1946–1964
    #   Generation X    : 1965–1980
    #   Millennials     : 1981–1996
    #   Generation Z    : 1997–2012
    #   Gen Alpha       : ≥ 2013  (too young to vote but kept for completeness)
    #   Unknown         : null BIRTHYEAR (already filtered, but defensive)
    # Source: Pew Research Center (2019) — https://pewrsr.ch/2HFp9rq
    df = df.with_columns([
        pl.when(pl.col('BIRTHYEAR') <= 1945).then(pl.lit('Silent/Greatest'))
          .when(pl.col('BIRTHYEAR') <= 1964).then(pl.lit('Baby Boomers'))
          .when(pl.col('BIRTHYEAR') <= 1980).then(pl.lit('Gen X'))
          .when(pl.col('BIRTHYEAR') <= 1996).then(pl.lit('Millennials'))
          .when(pl.col('BIRTHYEAR') <= 2012).then(pl.lit('Gen Z'))
          .otherwise(pl.lit('Gen Alpha'))
          .alias('Generation'),
    ])

    # ── Registration date: parse to a native Date type ────────────────────────
    # REGISTRATION_DATE is "YYYY-MM-DD" in the state file.
    # Stored as a Polars Date so we can compare directly with election date objects.
    # Null means the registration date is missing — those voters will have 0
    # eligible elections in the participation calculation.
    df = df.with_columns([
        pl.col('REGISTRATION_DATE')
          .str.to_date(format='%Y-%m-%d', strict=False)
          .alias('REGDATE_DT'),
    ])

    null_reg = df.filter(pl.col('REGDATE_DT').is_null()).height
    if null_reg:
        logger.warning('%d voters have no parseable registration date — '
                       'they will show 0 eligible elections', null_reg)

    # ── Party label: map raw code to display string ───────────────────────────
    # The state file uses single-character codes: 'R', 'D', or '' (unaffiliated).
    # We normalise to REP/DEM/UNC for consistency with the web dashboard colors
    # and Excel cross-tabs.  Any unexpected code becomes 'Other'.
    df = df.with_columns([
        pl.col('PARTY_AFFILIATION')
          .str.strip_chars()
          .replace_strict(PARTY_MAP, default='Other')
          .alias('PARTY_LABEL'),
    ])

    # ── Voter status: normalise whitespace ────────────────────────────────────
    df = df.with_columns([
        pl.col('VOTER_STATUS').str.strip_chars().str.to_uppercase().alias('VOTER_STATUS'),
    ])

    # ── Universal cohort classifier ───────────────────────────────────────────
    # Attach cohort assignments + decay-weighted lean scoring + new-registrant
    # flag onto every voter row.  The classifier joins back on SOS_VOTERID, so
    # downstream code can filter or group on cohort_family without re-running.
    primary_cols_local = identify_primary_cols(identify_election_cols(df))
    if primary_cols_local:
        classified = classify_all_voters_primary_history(
            df, primary_cols_local, logger,
            reference_date=get_snapshot_date(logger),
        )
        join_cols  = [c for c in classified.columns if c != 'SOS_VOTERID']
        df = df.join(
            classified.select(['SOS_VOTERID'] + join_cols),
            on='SOS_VOTERID',
            how='left',
        )
        logger.info('Cohort classifier attached: %d cohort columns', len(join_cols))

    logger.info('Clean complete: %s valid records', f'{len(df):,}')
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Participation metrics  (Polars — single pass via sum_horizontal)
# ─────────────────────────────────────────────────────────────────────────────

# Election columns evaluated per pass in add_voter_participation. It bounds the
# transient memory of that step to one batch of Boolean flags; 8 matches the
# default worker count, so a batch is evaluated in one parallel pass.
_PARTICIPATION_BATCH = 8


def add_voter_participation(
    df:            pl.DataFrame,
    election_cols: list[str],
    logger:        logging.Logger,
    *,
    read_cells=None,
) -> pl.DataFrame:
    """
    Compute per-voter participation metrics across every election column.

    Memory shape (OPEN-001; this step took RSS from 26 to 47 GB on the
    2026-11-12 statewide run). Each election column yields two Boolean flags,
    eligible and cast, each evaluated once and reused for the all-elections
    and the generals-only counts. Columns are processed _PARTICIPATION_BATCH at
    a time and folded into four Int32 running totals, so the transient cost is
    one batch of flags plus the totals. The previous form built one Int32
    expression per column per count and handed all of them to sum_horizontal
    at once, with the cast test evaluated twice per column.

    read_cells: optional callable(list of names) -> DataFrame of those election
    columns, row-aligned with `df`. Give it when `df` does not hold the
    election columns (load_enriched_for_export); each batch is then read
    through it and released. Without it the columns are taken from `df`.
    Raises ValueError if a batch does not have exactly as many rows as `df`.

    Semantics are unchanged from that form, including the exact cast test
    (str.strip_chars() != ''); tools/tests/test_participation_equivalence.py
    compares the two on randomized frames.

    Columns added (all-elections, backward-compatible names):
      Elections_Eligible — number of elections held after the voter's registration date
      Elections_Voted    — of those, number where the voter participated
      Turnout_Rate       — Elections_Voted / Elections_Eligible  (null if 0 eligible)

    Columns added (generals-only):
      General_Eligible      — general elections held after the voter's registration date
      General_Voted         — of those, generals where the voter participated
      General_Turnout_Rate  — General_Voted / General_Eligible  (null if 0 eligible)

    Derived:
      Voter_Frequency    — categorical label based on General_Turnout_Rate vs FREQ thresholds

    A voter is "eligible" for an election if their REGDATE_DT is on or before
    that election's date.  A voter "voted" if they were eligible AND their
    participation value is non-empty (X, R, or D).
    """
    # Single resolver: date/type/regular facts come ONLY from build_col_meta,
    # never from the column name here. The "generals" set below is the resolved
    # type_code == 'GENERAL', not a startswith test.
    col_meta = build_col_meta(df, election_cols, logger, read_cells=read_cells)
    read = read_cells or df.select
    logger.info('Computing participation over %d election columns (%d generals), %d per batch ...',
                len(col_meta), sum(m.type_code == 'GENERAL' for m in col_meta),
                _PARTICIPATION_BATCH)

    # Voter_Frequency is based on the generals-only rate so that the 75%
    # "Frequent" threshold is reachable: a voter attending every general since
    # 2000 scores ~100%, not ~28% diluted by every primary and special.
    count_cols = ('Elections_Eligible', 'Elections_Voted', 'General_Eligible', 'General_Voted')
    totals = {name: pl.zeros(df.height, dtype=pl.Int32, eager=True) for name in count_cols}
    reg = pl.col('REGDATE_DT')

    def _count_true(frame: pl.DataFrame, names: list[str]) -> pl.Series:
        """Row-wise count of True across `names` as Int32. The flags hold no nulls."""
        return frame.select(
            pl.sum_horizontal([pl.col(n).cast(pl.Int32) for n in names])
        ).to_series()

    for start in range(0, len(col_meta), _PARTICIPATION_BATCH):
        batch = col_meta[start:start + _PARTICIPATION_BATCH]
        idx = range(len(batch))
        # eligible: registered on or before the election date.
        # cast:     the cell holds a ballot code (non-null and non-blank).
        # Both are Boolean and never null: a null registration date or a null
        # cell makes the left operand False, and False & null is False.
        cells = read([m.col for m in batch])
        if cells.height != df.height:
            raise ValueError(
                f'election cells have {cells.height:,} rows but the frame has {df.height:,}: '
                'read_cells is not row-aligned with the frame'
            )
        # hstack requires equal heights, which the check above guarantees.
        flags = df.select(
            [(reg.is_not_null() & (reg <= pl.lit(m.date))).alias(f'_elig_{i}')
             for i, m in enumerate(batch)]
        ).hstack(cells.select(
            [(pl.col(m.col).is_not_null() & (pl.col(m.col).str.strip_chars() != '')).alias(f'_cast_{i}')
             for i, m in enumerate(batch)]
        ))
        del cells
        # voted: eligible AND cast. A ballot recorded before the registration
        # date is therefore not counted. REGISTRATION_DATE resets on cancellation
        # and reactivation, so such ballots exist (CLAUDE.md section 4).
        flags = flags.with_columns(
            [(pl.col(f'_elig_{i}') & pl.col(f'_cast_{i}')).alias(f'_voted_{i}') for i in idx]
        )
        generals = [i for i in idx if batch[i].type_code == 'GENERAL']
        totals['Elections_Eligible'] = totals['Elections_Eligible'] + _count_true(flags, [f'_elig_{i}' for i in idx])
        totals['Elections_Voted'] = totals['Elections_Voted'] + _count_true(flags, [f'_voted_{i}' for i in idx])
        if generals:
            totals['General_Eligible'] = totals['General_Eligible'] + _count_true(flags, [f'_elig_{i}' for i in generals])
            totals['General_Voted'] = totals['General_Voted'] + _count_true(flags, [f'_voted_{i}' for i in generals])
        del flags

    # A frame with no GENERAL column keeps General_* at 0, which the frequency
    # label below reports as 'No Eligible Elections'.
    df = df.with_columns([totals[name].alias(name) for name in count_cols])

    # Turnout rate: voted / eligible.  Divide-by-zero → null (not 0), which
    # correctly propagates to 'No Eligible Elections' in the frequency label.
    df = df.with_columns([
        (
            pl.col('Elections_Voted').cast(pl.Float64) /
            pl.when(pl.col('Elections_Eligible') == 0)
              .then(None)
              .otherwise(pl.col('Elections_Eligible'))
        ).round(4).alias('Turnout_Rate'),
        (
            pl.col('General_Voted').cast(pl.Float64) /
            pl.when(pl.col('General_Eligible') == 0)
              .then(None)
              .otherwise(pl.col('General_Eligible'))
        ).round(4).alias('General_Turnout_Rate'),
    ])

    # Frequency label: based on generals-only turnout rate so the ≥75%
    # threshold is meaningful.  A voter who participates in every general
    # election since registration now correctly scores as "Frequent".
    df = df.with_columns([
        pl.when(pl.col('General_Eligible') == 0)
          .then(pl.lit('No Eligible Elections'))
          .when(pl.col('General_Turnout_Rate') >= FREQ_HIGH)
          .then(pl.lit('Frequent (≥75%)'))
          .when(pl.col('General_Turnout_Rate') >= FREQ_LOW)
          .then(pl.lit('Moderate (25–74%)'))
          .otherwise(pl.lit('Infrequent (<25%)'))
          .alias('Voter_Frequency')
    ])

    eligible_count = df.filter(pl.col('Elections_Eligible') > 0).height
    gen_eligible   = df.filter(pl.col('General_Eligible') > 0).height
    logger.info('Participation complete: %s voters had ≥1 eligible election '
                '(%s with ≥1 eligible general)',
                f'{eligible_count:,}', f'{gen_eligible:,}')
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Summary builders  (Polars → small DataFrames; cheap to convert to pandas)
# ─────────────────────────────────────────────────────────────────────────────

def build_county_summary(df: pl.DataFrame) -> pl.DataFrame:
    """
    One row per county: total voters, active count, party breakdown.
    Used as the top-level sheet in the Ohio-wide Excel workbook where
    dumping all 7.9M raw rows would exceed Excel's row limit.
    """
    # Total voters per county
    totals = (
        df.group_by('COUNTY_NUMBER')
          .agg(pl.len().alias('Total Voters'))
    )

    # Active voters (status == 'ACTIVE')
    active = (
        df.filter(pl.col('VOTER_STATUS') == 'ACTIVE')
          .group_by('COUNTY_NUMBER')
          .agg(pl.len().alias('Active Voters'))
    )

    # Party breakdown per county using pivot
    party_pivot = (
        df.group_by(['COUNTY_NUMBER', 'PARTY_LABEL'])
          .agg(pl.len().alias('n'))
          .pivot(on='PARTY_LABEL', index='COUNTY_NUMBER', values='n', aggregate_function='sum')
    )

    # Join all pieces together on COUNTY_NUMBER
    summary = (
        totals
        .join(active,       on='COUNTY_NUMBER', how='left')
        .join(party_pivot,  on='COUNTY_NUMBER', how='left')
        .with_columns([
            # Add human-readable county name from our lookup dict
            pl.col('COUNTY_NUMBER')
              .replace_strict(OHIO_COUNTIES, default='Unknown')
              .alias('County Name'),
        ])
        .sort('Total Voters', descending=True)
    )

    return summary


def build_decade_summary(df: pl.DataFrame) -> pl.DataFrame:
    """
    Voter counts grouped by birth decade.
    Drives both the Decade Summary Excel sheet and the web dashboard bar chart.
    """
    return (
        df.group_by('Decade')
          .agg(pl.len().alias('Voter Count'))
          .sort('Decade')
    )


# Ordered list used for sorting generation rows chronologically.
_GEN_ORDER = ['Silent/Greatest', 'Baby Boomers', 'Gen X', 'Millennials', 'Gen Z', 'Gen Alpha']


def build_generation_summary(df: pl.DataFrame) -> pl.DataFrame:
    """
    Voter counts grouped by generational cohort (Pew Research Center boundaries).
    Rows are returned in chronological birth-order.
    """
    order_df = pl.DataFrame({'Generation': _GEN_ORDER,
                             '_sort': list(range(len(_GEN_ORDER)))})
    summary = (
        df.group_by('Generation')
          .agg(pl.len().alias('Voter Count'))
    )
    return (
        summary.join(order_df, on='Generation', how='left')
               .sort('_sort')
               .drop('_sort')
    )


def build_generation_crosstab(df: pl.DataFrame, logger: logging.Logger) -> pl.DataFrame:
    """
    Party affiliation × generational cohort cross-tabulation.
    Mirrors the structure of the decade cross-tab in build_party_crosstabs().
    """
    order_df = pl.DataFrame({'Generation': _GEN_ORDER,
                             '_sort': list(range(len(_GEN_ORDER)))})
    try:
        raw = (
            df.group_by(['Generation', 'PARTY_LABEL'])
              .agg(pl.len().alias('count'))
              .pivot(on='PARTY_LABEL', index='Generation',
                     values='count', aggregate_function='sum')
        )
        # Ensure all expected party columns exist
        for party in TOP_PARTIES + ['Other']:
            if party not in raw.columns:
                raw = raw.with_columns(pl.lit(0).cast(pl.Int64).alias(party))
        total = raw.select(pl.col(c) for c in TOP_PARTIES + ['Other']).sum_horizontal()
        raw = raw.with_columns(total.alias('Total'))
        result = (
            raw.join(order_df, on='Generation', how='left')
               .sort('_sort')
               .drop('_sort')
               .rename({'Generation': 'Generational Cohort'})
        )
        return result
    except Exception:
        logger.warning('build_generation_crosstab: could not build cross-tab', exc_info=True)
        return pl.DataFrame()


def build_election_participation(
    df:            pl.DataFrame,
    election_cols: list[str],
    logger:        logging.Logger,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """
    Build three summary DataFrames for the Participation Excel sheet:

      election_df — one row per election: eligible count, votes cast, turnout rate
      type_df     — aggregated by election type (Primary / General / Special)
      freq_df     — voter frequency distribution (Frequent / Moderate / Infrequent)

    These are computed directly from the per-voter REGDATE_DT and election columns
    rather than from the derived Elections_Eligible/Voted columns, because the
    per-election breakdown needs election-specific eligibility counts.

    NOTE: This step iterates over 89 election columns in Python.  For Ohio-scale
    data (~7.9M rows) this is the slowest part of the analysis — each column
    requires a Polars filter + aggregation.  It runs in roughly 30–60 seconds on
    an 8-core machine.  A future optimisation would be to unpivot the election
    columns into a long format and aggregate in one pass.
    """
    logger.info('Building election-level participation summary (%d elections) ...',
                len(election_cols))

    rows = []
    total = len(election_cols)
    for i, col in enumerate(election_cols, 1):
        # Log progress every 10 elections so the user knows the loop is running.
        # This step can take 30–60 s on Ohio-wide data; without it the process
        # looks hung during a long silent pause.
        if i % 10 == 0 or i == total:
            logger.info('  Participation by election: %d / %d', i, total)

        elec_date, type_code, type_label = parse_election_meta(col)
        if elec_date is None:
            continue

        elig_mask = (
            pl.col('REGDATE_DT').is_not_null() &
            (pl.col('REGDATE_DT') <= pl.lit(elec_date))
        )
        n_elig  = df.filter(elig_mask).height
        n_voted = df.filter(
            elig_mask &
            pl.col(col).is_not_null() &
            (pl.col(col).str.strip_chars() != '')
        ).height
        rate = n_voted / n_elig if n_elig > 0 else None

        rows.append({
            'Election': col,
            'Date':     elec_date.strftime('%Y-%m-%d'),
            'Type':     type_label,
            'Eligible': n_elig,
            'Voted':    n_voted,
            'Rate':     f'{rate:.1%}' if rate is not None else 'N/A',
            '_rate_raw': rate,   # retained for type aggregation, dropped before output
        })

    election_df = pl.DataFrame(rows)

    # Aggregate by election type (sum eligible/voted, average rate)
    type_df = (
        election_df.group_by('Type')
          .agg([
              pl.len().alias('Elections'),
              pl.col('Eligible').mean().round(0).cast(pl.Int64).alias('Avg Eligible'),
              pl.col('Voted').mean().round(0).cast(pl.Int64).alias('Avg Voted'),
              pl.col('_rate_raw').mean().alias('_avg_rate'),
          ])
          .with_columns([
              pl.col('_avg_rate').map_elements(
                  lambda r: f'{r:.1%}' if r is not None else 'N/A',
                  return_dtype=pl.Utf8,
              ).alias('Avg Rate'),
          ])
          .drop('_avg_rate')
    )

    election_df = election_df.drop('_rate_raw')

    # Voter frequency distribution
    freq_order = ['Frequent (≥75%)', 'Moderate (25–74%)', 'Infrequent (<25%)', 'No Eligible Elections']
    freq_counts = df.group_by('Voter_Frequency').agg(pl.len().alias('Voter Count'))
    # Ensure all categories present even if count is zero
    all_cats    = pl.DataFrame({'Voter_Frequency': freq_order})
    freq_df = (
        all_cats
        .join(freq_counts, on='Voter_Frequency', how='left')
        .with_columns(pl.col('Voter Count').fill_null(0))
        .with_columns([
            (pl.col('Voter Count') / len(df) * 100)
              .round(2)
              .cast(pl.Utf8)
              # KNOWN DEFECT, kept on purpose: str.join is an aggregation. It joins the
              # whole column into one '%'-separated string and broadcasts it, so every
              # row of this sheet's Percent column holds the same long string. The
              # intent was a per-row '12.5%'. This line only replaces the deprecated
              # spelling (str.concat) with the identical current one; the output is
              # unchanged. Fixing the value is a separate, output-changing decision.
              .str.join('%')
              .alias('Percent')
        ])
        .rename({'Voter_Frequency': 'Frequency Class'})
    )

    logger.info('Election participation summary complete: %d elections', len(election_df))
    return election_df, type_df, freq_df


def build_district_breakdown(
    df:            pl.DataFrame,
    election_cols: list[str],
    logger:        logging.Logger,
) -> tuple[dict[str, pl.DataFrame], str | None]:
    """
    Per-district breakdown for each field in DISTRICT_FIELDS.

    For each district field (Congressional, Senate, House, School, Board of Ed),
    produces a table with total voters, party breakdown, status breakdown, and
    turnout in the most recent General election.

    Returns (district_tables_dict, most_recent_general_col_name).
    """
    logger.info('Building district breakdowns ...')

    # Identify the most recent General election column for turnout calculation.
    # General elections are the most comparable across districts.
    # Single resolver: 'general' is resolved type_code, not a name startswith.
    _gen_meta     = [m for m in build_col_meta(df, election_cols, logger)
                     if m.type_code == 'GENERAL']
    most_recent_g = _gen_meta[-1].col if _gen_meta else None

    # Pre-compute eligibility and voted columns for the most recent General election
    # so we don't recompute for every district field.
    if most_recent_g:
        g_date, _, _ = parse_election_meta(most_recent_g)
        df = df.with_columns([
            (
                pl.col('REGDATE_DT').is_not_null() &
                (pl.col('REGDATE_DT') <= pl.lit(g_date))
            ).cast(pl.Int32).alias('_elig_g'),

            (
                (pl.col('REGDATE_DT').is_not_null() & (pl.col('REGDATE_DT') <= pl.lit(g_date))) &
                pl.col(most_recent_g).is_not_null() &
                (pl.col(most_recent_g).str.strip_chars() != '')
            ).cast(pl.Int32).alias('_voted_g'),
        ])
    else:
        df = df.with_columns([
            pl.lit(0).cast(pl.Int32).alias('_elig_g'),
            pl.lit(0).cast(pl.Int32).alias('_voted_g'),
        ])

    district_tables: dict[str, pl.DataFrame] = {}

    for field in DISTRICT_FIELDS:
        if field not in df.columns:
            logger.warning('District field "%s" not found in voter file — skipping', field)
            continue

        # Group by district value, computing all metrics in one aggregation pass
        agg = (
            df.with_columns(
                pl.col(field).fill_null('Unknown').str.strip_chars().alias('_district')
            )
            .group_by(['_district', 'PARTY_LABEL', 'VOTER_STATUS'])
            .agg([
                pl.len().alias('_n'),
                pl.col('_elig_g').sum().alias('_elig_g'),
                pl.col('_voted_g').sum().alias('_voted_g'),
            ])
        )

        # Pivot party and status into separate columns
        totals    = agg.group_by('_district').agg(pl.col('_n').sum().alias('Total Voters'))
        party_ct  = (
            agg.group_by(['_district', 'PARTY_LABEL'])
               .agg(pl.col('_n').sum())
               .pivot(on='PARTY_LABEL', index='_district', values='_n', aggregate_function='sum')
        )
        # Explicit aggregation instead of pivot — fixed schema regardless of
        # what VOTER_STATUS values exist in the statewide file.
        status_ct = (
            agg.group_by('_district')
               .agg([
                   pl.col('VOTER_STATUS').eq('ACTIVE').mul(pl.col('_n')).sum().cast(pl.Int64).alias('ACTIVE'),
                   pl.col('VOTER_STATUS').eq('CONFIRMATION').mul(pl.col('_n')).sum().cast(pl.Int64).alias('CONFIRMATION'),
               ])
        )
        g_agg = (
            agg.group_by('_district')
               .agg([
                   pl.col('_elig_g').sum().alias('Eligible (General)'),
                   pl.col('_voted_g').sum().alias('Voted (General)'),
               ])
               .with_columns([
                   pl.when(pl.col('Eligible (General)') > 0)
                     .then(pl.col('Voted (General)').cast(pl.Float64) /
                           pl.col('Eligible (General)').cast(pl.Float64))
                     .otherwise(None)
                     .round(4)
                     .map_elements(
                         lambda r: f'{r:.1%}' if r is not None else 'N/A',
                         return_dtype=pl.Utf8,
                     )
                     .alias('Turnout%')
               ])
        )

        # Rename party columns to include prefix for clarity in Excel
        for p in TOP_PARTIES + ['Other']:
            if p not in party_ct.columns:
                party_ct = party_ct.with_columns(pl.lit(0).cast(pl.Int32).alias(p))
        party_ct = party_ct.rename({p: f'Party: {p}' for p in TOP_PARTIES + ['Other']
                                    if p in party_ct.columns})

        out = (
            totals
            .join(party_ct,  on='_district', how='left')
            .join(status_ct, on='_district', how='left')
            .join(g_agg,     on='_district', how='left')
            .rename({'_district': field})
            .sort('Total Voters', descending=True)
        )

        district_tables[field] = out
        logger.debug('District breakdown for %s: %d districts', field, len(out))

    # Drop the temporary general-election helper columns
    df = df.drop(['_elig_g', '_voted_g'])

    logger.info('District breakdown complete: %d field(s)', len(district_tables))
    return district_tables, most_recent_g


def build_party_crosstabs(df: pl.DataFrame, logger: logging.Logger) -> tuple:
    """
    Three crosstabs used in the Party Cross-tabs Excel sheet and web dashboard:

      by_congress — party × Congressional district
      by_decade   — party × birth decade cohort
      by_status   — party × voter registration status
    """
    logger.info('Building party cross-tabs ...')

    def crosstab(group_col: str, label: str) -> pl.DataFrame:
        """Pivot party counts for the given grouping column."""
        return (
            df.with_columns(
                pl.col(group_col).fill_null('Unknown').str.strip_chars().alias('_grp')
            )
            .group_by(['_grp', 'PARTY_LABEL'])
            .agg(pl.len().alias('n'))
            .pivot(on='PARTY_LABEL', index='_grp', values='n', aggregate_function='sum')
            .rename({'_grp': label})
        )

    by_congress = crosstab('CONGRESSIONAL_DISTRICT', 'Congressional District')
    by_decade   = crosstab('Decade',                 'Birth Decade')

    # by_status uses explicit aggregation instead of the generic crosstab pivot
    # so the column schema is fixed regardless of unexpected VOTER_STATUS values.
    by_status = (
        df.group_by('PARTY_LABEL')
          .agg([
              pl.col('VOTER_STATUS').eq('ACTIVE').sum().cast(pl.Int64).alias('ACTIVE'),
              pl.col('VOTER_STATUS').eq('CONFIRMATION').sum().cast(pl.Int64).alias('CONFIRMATION'),
          ])
          .with_columns(
              (pl.col('ACTIVE') + pl.col('CONFIRMATION')).alias('Total')
          )
          .sort('Total', descending=True)
          .rename({'PARTY_LABEL': 'Voter Status'})
    )

    return by_congress, by_decade, by_status


def build_precinct_summary(df: pl.DataFrame) -> pl.DataFrame:
    """
    Precinct-level active/inactive counts for the table chart in the web dashboard.

    Uses explicit .eq() aggregations instead of a pivot so the output schema is
    fixed regardless of what VOTER_STATUS values exist in the source file (the
    statewide file may contain RETIRED, DELETED, or other values beyond ACTIVE /
    CONFIRMATION).  This also includes COUNTY_NUMBER so city-level aggregation
    and future cross-county merges can group by city name across county lines.

    Est. Unregistered is intentionally set to 'N/A' — this column will be
    populated in a future step once Census adult-population data by precinct is
    integrated.  The column is included now so the JSON schema is stable.
    """
    return (
        df.with_columns(
            pl.col('PRECINCT_NAME').fill_null('Unknown').str.strip_chars().alias('PRECINCT_NAME')
        )
        .group_by(['COUNTY_NUMBER', 'PRECINCT_NAME'])
        .agg([
            pl.col('VOTER_STATUS').eq('ACTIVE').sum().cast(pl.Int64).alias('ACTIVE'),
            pl.col('VOTER_STATUS').eq('CONFIRMATION').sum().cast(pl.Int64).alias('CONFIRMATION'),
        ])
        .with_columns([
            (pl.col('ACTIVE') + pl.col('CONFIRMATION')).alias('Total Registered'),
            pl.lit('N/A').alias('Est. Unregistered'),
        ])
        .sort(['Total Registered', 'PRECINCT_NAME'], descending=[True, False])
    )


# Precinct-ID suffixes that should be stripped to recover the city/township name.
# Pattern: optional separator (space or dash), then one or more digits, optionally
# followed by a dash and a letter — e.g. "3-E", "14", "5-D".  Also matches a
# lone trailing letter after a separator — e.g. "MIAMI TWP R" → "MIAMI TWP",
def build_city_summary(df: pl.DataFrame) -> pl.DataFrame:
    """
    Aggregate voter registration to city/township level using the CITY column
    (voter's registered address municipality) as the primary grouping key.

    COUNTY_NUMBER is preserved in the output so that when multiple counties are
    processed (Phase 3 statewide), cities that span county lines (e.g. Kettering
    across Montgomery and Greene) can be identified and merged by downstream logic
    without any changes to this function.

    Schema:
        COUNTY_NUMBER   — Ohio SOS county code (string)
        City            — extracted municipality name
        ACTIVE          — active registered voters
        CONFIRMATION    — confirmation-status voters
        Total Registered
        Precincts       — number of distinct precincts contributing to this city
        Est. Unregistered — placeholder; requires Census integration
    """
    base = df.with_columns([
        pl.col('PRECINCT_NAME').fill_null('Unknown').str.strip_chars().alias('PRECINCT_NAME'),
        pl.col('COUNTY_NUMBER').fill_null('??').str.strip_chars().alias('COUNTY_NUMBER'),
    ])

    # Resolve municipality with the SHARED jurisdiction-hierarchy resolver, so
    # this summary, the precinct index, and city_county_map can never drift:
    #   CITY -> VILLAGE -> WARD-prefix -> TOWNSHIP (= not a city) -> postal.
    # Townships resolve to None and are intentionally excluded (they are not
    # cities). _dominant_city_per_precinct keys by PRECINCT_NAME within a county
    # slice; build_city_summary may receive multiple counties, so map per
    # (county, precinct).
    city_by_precinct = {}
    for (cnum,), sub in base.group_by(['COUNTY_NUMBER'], maintain_order=True):
        for pname, city in _dominant_city_per_precinct(sub).items():
            city_by_precinct[(cnum, pname)] = city

    mapping = pl.DataFrame(
        [{'COUNTY_NUMBER': c, 'PRECINCT_NAME': p, 'City': v}
         for (c, p), v in city_by_precinct.items()],
        schema={'COUNTY_NUMBER': pl.Utf8, 'PRECINCT_NAME': pl.Utf8, 'City': pl.Utf8},
    )

    return (
        base.join(mapping, on=['COUNTY_NUMBER', 'PRECINCT_NAME'], how='inner')
        .group_by(['COUNTY_NUMBER', 'City'])
        .agg([
            pl.col('VOTER_STATUS').eq('ACTIVE').sum().cast(pl.Int64).alias('ACTIVE'),
            pl.col('VOTER_STATUS').eq('CONFIRMATION').sum().cast(pl.Int64).alias('CONFIRMATION'),
            pl.col('PRECINCT_NAME').n_unique().alias('Precincts'),
        ])
        .with_columns([
            (pl.col('ACTIVE') + pl.col('CONFIRMATION')).alias('Total Registered'),
            pl.lit('N/A').alias('Est. Unregistered'),
        ])
        .sort('Total Registered', descending=True)
    )


# ─────────────────────────────────────────────────────────────────────────────
# JSON export  (writes to docs/data/ — consumed directly by index.html)
# ─────────────────────────────────────────────────────────────────────────────

def _dump_json(obj: dict, path: Path, logger: logging.Logger):
    """Write a dict as formatted JSON; log the result.

    Uses orjson when available (3-10x faster, GIL-releasing, direct bytes
    write to NVMe buffer).  Falls back to stdlib json if orjson is not
    installed.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    if _orjson is not None:
        path.write_bytes(_orjson.dumps(obj, option=_orjson.OPT_INDENT_2 | _orjson.OPT_NON_STR_KEYS))
    else:
        path.write_text(json.dumps(obj, indent=2, default=str), encoding='utf-8')
    logger.debug('JSON written: %s  (%d bytes)', path.name, path.stat().st_size)


# ─────────────────────────────────────────────────────────────────────────────
# UNC shadow-partisanship classifier  (Polars-native, format-agnostic)
# ─────────────────────────────────────────────────────────────────────────────

# UNC shadow classification colors — intentionally lighter than the full-
# saturation party colors so the visual distinction between "affiliated" and
# "behaviorally inferred" is immediately apparent.
UNC_SHADOW_COLORS = {
    'LIFETIME_D':   '#60a5fa',   # blue-400  — lighter tint of DEM #3b82f6 (blue-500)
    'LIFETIME_R':   '#f87171',   # red-400   — lighter tint of REP #ef4444 (red-500)
    'MIXED':        '#a78bfa',   # violet-400 — crossed party lines
    'NO_HISTORY':   '#9ca3af',   # grey-400   — no primary participation on record
}

UNC_SHADOW_LABELS = {
    'LIFETIME_D':  'Lifetime D',
    'LIFETIME_R':  'Lifetime R',
    'MIXED':       'Mixed / Crossover',
    'NO_HISTORY':  'No Primary History',
}

# 9-cohort partisan-spectrum taxonomy — single source of truth for chart exports.
# ORDER IS THE SPECTRUM: charts render this list left-to-right, R -> D, so a
# slice's position asserts where that behaviour sits partisan-wise.
# UNC_NONPARTISAN sits in the neutral centre beside UNC_NO_PRIMARY: both are
# zero-partisan-signal populations. It must NOT sit next to the Mixed slices,
# which would repeat the very conflation this taxonomy was split to fix.
# MINOR_PARTY (Libertarian, Green and other non-R/D affiliation or primary
# ballot) sits at the exact centre: it is on neither side of the D/R
# spectrum. Before it existed those voters fell to cohort OTHER, which no
# slice listed, so every chart dropped them (11,904 statewide).
#
# NOTE (known duplication, CLAUDE.md sec 5): this list is mirrored in
# jurisdictional_groupings.py, serve/roster_api.py and
# tools/export/precinct_party_export.py — each with its own labels/colours for
# its surface. All four must change together. Collapsing them to one source of
# truth is a queued follow-up handoff.
COHORT_SLICES = [
    ('PURE_R',          'Pure R',                   '#ef4444'),
    ('UNC_LAPSED_R',    'UNC – Lapsed R',           '#fca5a5'),
    ('MIXED_ACTIVE',    'Mixed – Active',           '#f59e0b'),
    ('MIXED_LAPSED',    'Mixed – Lapsed',           '#a78bfa'),
    ('MINOR_PARTY',     'Minor Party',              '#14b8a6'),
    ('UNC_NONPARTISAN', 'UNC – Non-partisan Ballot', '#c4b5cd'),
    ('UNC_NO_PRIMARY',  'UNC – No Primary',         '#9ca3af'),
    ('UNC_LAPSED_D',    'UNC – Lapsed D',           '#93c5fd'),
    ('PURE_D',          'Pure D',                   '#3b82f6'),
]

COHORT_STACK_MAP = {
    'PURE_R':          'r_pure',
    'UNC_LAPSED_R':    'unc_r',
    'MIXED_ACTIVE':    'unc_mid',
    'MIXED_LAPSED':    'unc_mid',
    'MINOR_PARTY':     'unc_mid',
    'UNC_NONPARTISAN': 'unc_mid',
    'UNC_NO_PRIMARY':  'unc_mid',
    'UNC_LAPSED_D':    'unc_d',
    'PURE_D':          'd_pure',
}

UNC_SHADOW_NOTE = (
    'Shadow partisanship inferred from primary ballot history only. '
    'Ohio uses an open primary: voters may cross party lines in any election. '
    'These labels reflect behavioral pattern, not registration status. '
    'Voters with ballots in both D and R primaries are classified as Mixed.'
)


def identify_primary_cols(election_cols: list[str]) -> list[str]:
    """
    Filter a list of election columns down to primary-only columns.

    Accepts the output of identify_election_cols() — sorted chronologically.
    The filter is deliberately the only state-specific logic: swap this
    predicate for a different state's naming convention and the classifier
    itself needs no changes.

    Ohio SWVF format: 'PRIMARY-MM/DD/YYYY'

    Thin filter over the resolver's name-tag: a column is a primary iff
    parse_election_meta resolves its type_code to 'PRIMARY'. No name parsing
    happens here — the ELEC_RE / date decomposition lives only in the resolver.
    The data-dependent axes (is_regular, partisan_capable) are consumed inside
    the classifier via build_col_meta, not here.
    """
    return [c for c in election_cols if parse_election_meta(c)[1] == 'PRIMARY']


def _assert_cohort_coverage(frame: pl.DataFrame) -> None:
    """Raise unless every voter's cohort_family is one of the COHORT_SLICES families.

    Every chart, hero total and lean figure sums the slices, so a family outside
    them is a voter silently dropped from all of them. That happened: until
    MINOR_PARTY existed, non-R/D affiliations fell to cohort OTHER and 11,904
    voters statewide (2026-10-24 snapshot) vanished from every published party
    chart while the precinct index still counted them. Fail here instead, where
    the cause is visible (CLAUDE.md sec 5, loud failures).
    """
    families = [fam for fam, _, _ in COHORT_SLICES]
    bad = (
        frame.filter(~pl.col('cohort_family').is_in(families)
                     | pl.col('cohort_family').is_null())
             .group_by(['cohort', 'cohort_family'])
             .agg(pl.len().alias('n'))
             .sort('n', descending=True)
    )
    if bad.height:
        raise ValueError(
            f'cohort_family outside COHORT_SLICES for {int(bad["n"].sum()):,} voters; '
            f'every chart would drop them. (cohort, cohort_family, n): {bad.rows()}'
        )


def classify_all_voters_primary_history(
    df:             pl.DataFrame,
    primary_cols:   list[str],
    logger:         logging.Logger,
    reference_date: date_t,
) -> pl.DataFrame:
    """
    Universal voter classifier — applies cohort taxonomy + decay-weighted
    crossover scoring to ALL voters (not just UNC).

    Cohorts are evaluated top-down with first-match-wins semantics.  See
    CLAUDE.md / refactor doc for the full taxonomy table.  Output is one row
    per voter keyed on SOS_VOTERID, joinable back onto the source df.

    ``reference_date`` is REQUIRED (no default): it is the snapshot's own
    date and sets the decay origin. A default would silently let a re-run of
    an old snapshot compute as if it were today, collapsing every snapshot
    onto one time origin. Callers pass ``get_snapshot_date(logger)``.

    Ballot counts run over the PARTISAN-CAPABLE primary spine from
    build_col_meta() — the columns where at least one D or R ballot was
    actually cast — not over every column named "PRIMARY". Off-cycle
    issue-only primaries offer no party ballot, so they can neither add to
    d/r_primaries nor create a crossing. The cohort ladder and
    ever_crossed therefore read one identical column set.

    Returns a DataFrame with columns:
        SOS_VOTERID                  Utf8
        d_primaries, r_primaries     Int32  (count of partisan ballots)
        x_primaries                  Int32  (non-partisan ballots: code X)
        m_primaries                  Int32  (minor-party ballots: not in {D, R, X, ''})
        total_primaries              Int32  (any non-empty ballot)
        partisan_primaries           Int32  (= d + r)
        lean_score                   Float64 nullable  decay-weighted partisan lean
        confidence                   Float64 nullable  |lean| * n / (n + 2)
        recent_5yr_lean              Float64 nullable  same math, last 5 years only
        last_three_party             Utf8     DDD/DD/D/RRR/RR/R/MIX/NONE
        years_since_last_partisan    Float64 nullable
        switch_count                 Int32   adjacent D->R or R->D transitions
                                             (LEGACY - undercounts ~11x; use
                                             ever_crossed. Kept for callers.)
        ever_crossed                 Boolean voted BOTH parties' primaries at
                                             any point (compacted subsequence)
        partisan_ballot_sequence     Utf8    e.g. 'DDRD' - partisan ballots
                                             oldest-first; auditable by eye
        cohort                       Utf8    PURE_R/PURE_D/CROSSOVER_R/CROSSOVER_D/
                                             UNC_LAPSED_R/UNC_LAPSED_D/UNC_MIXED/
                                             UNC_NONPARTISAN/UNC_NO_PRIMARY/
                                             MINOR_PARTY
        cohort_family                Utf8    rolled-up family
        crossover_class              Utf8 nullable  LOCKED_D/LEAN_D/TRUE_MIXED/...
        regular_primaries_eligible   Int32   regular primaries since registering
        primary_participation_rate   Float64 nullable  partisan / eligible
        primaries_skipped            Int32   eligible - partisan (floored at 0)
        is_new_registrant            Boolean  REGISTRATION_DATE >= 2024-01-01
    """
    from math import exp

    party_col = 'PARTY_AFFILIATION' if 'PARTY_AFFILIATION' in df.columns else 'PARTYAFFIL'

    # -- Empty-primary-cols edge case ----------------------------------------
    if not primary_cols:
        logger.warning('classify_all_voters_primary_history: no primary columns - '
                       'returning minimal frame keyed on SOS_VOTERID')
        out = df.select([
            pl.col('SOS_VOTERID'),
            pl.lit(0).cast(pl.Int32).alias('d_primaries'),
            pl.lit(0).cast(pl.Int32).alias('r_primaries'),
            pl.lit(0).cast(pl.Int32).alias('x_primaries'),
            pl.lit(0).cast(pl.Int32).alias('m_primaries'),
            pl.lit(0).cast(pl.Int32).alias('total_primaries'),
            pl.lit(0).cast(pl.Int32).alias('partisan_primaries'),
            pl.lit(None).cast(pl.Float64).alias('lean_score'),
            pl.lit(None).cast(pl.Float64).alias('confidence'),
            pl.lit(None).cast(pl.Float64).alias('recent_5yr_lean'),
            pl.lit('NONE').alias('last_three_party'),
            pl.lit(None).cast(pl.Float64).alias('years_since_last_partisan'),
            pl.lit(0).cast(pl.Int32).alias('switch_count'),
            # Schema parity with the main path: this frame is joined onto
            # voter rows exactly like the full one, so a missing column here
            # surfaces as a ColumnNotFoundError while processing some OTHER
            # county. Dtypes must match, not just names.
            pl.lit(False).alias('ever_crossed'),
            pl.lit('').alias('partisan_ballot_sequence'),
            pl.lit(0).cast(pl.Int32).alias('regular_primaries_eligible'),
            pl.lit(None).cast(pl.Float64).alias('primary_participation_rate'),
            pl.lit(0).cast(pl.Int32).alias('primaries_skipped'),
        ])
        party = pl.col(party_col).str.strip_chars() if party_col in df.columns else pl.lit('')
        out = out.with_columns(party.alias('_party'))
        cohort_expr = (
            pl.when(pl.col('_party') == 'R').then(pl.lit('PURE_R'))
              .when(pl.col('_party') == 'D').then(pl.lit('PURE_D'))
              .when(~pl.col('_party').is_in(['R', 'D', ''])).then(pl.lit('MINOR_PARTY'))
              .otherwise(pl.lit('UNC_NO_PRIMARY'))
              .alias('cohort')
        )
        out = out.with_columns(cohort_expr).drop('_party')
        family_map = {
            'PURE_R':          'PURE_R',
            'PURE_D':          'PURE_D',
            'UNC_NO_PRIMARY':  'UNC_NO_PRIMARY',
            'UNC_NONPARTISAN': 'UNC_NONPARTISAN',
            'MIXED_ACTIVE':    'MIXED_ACTIVE',
            'MIXED_LAPSED':    'MIXED_LAPSED',
            'MINOR_PARTY':     'MINOR_PARTY',
        }
        out = out.with_columns(
            pl.col('cohort').replace(family_map).alias('cohort_family')
        )
        out = out.with_columns(pl.lit(None).cast(pl.Utf8).alias('crossover_class'))
        if 'REGISTRATION_DATE' in df.columns:
            reg = df.select([pl.col('SOS_VOTERID'),
                             pl.col('REGISTRATION_DATE')]).clone()
            if reg.schema['REGISTRATION_DATE'] != pl.Date:
                reg = reg.with_columns(
                    pl.col('REGISTRATION_DATE')
                      .str.to_date(format='%Y-%m-%d', strict=False)
                      .alias('REGISTRATION_DATE')
                )
            reg = reg.with_columns(
                (pl.col('REGISTRATION_DATE') >= pl.lit(date_t(2024, 1, 1)))
                  .fill_null(False)
                  .alias('is_new_registrant')
            ).select(['SOS_VOTERID', 'is_new_registrant'])
            out = out.join(reg, on='SOS_VOTERID', how='left')
        else:
            out = out.with_columns(pl.lit(False).alias('is_new_registrant'))
        _assert_cohort_coverage(out)
        return out

    logger.info('  classify_all_voters_primary_history: %s voters x %d primary columns',
                f'{len(df):,}', len(primary_cols))

    # -- Resolve primary-column metadata via the SINGLE resolver ------------
    # No inline parse loop: build_col_meta is the sole decomposer of election
    # column names into (date, type, is_regular, partisan_capable). We keep the
    # (col, date) pairs the downstream expression builders expect.
    _resolved = build_col_meta(df, primary_cols, logger)

    # THE COUNT SPINE: partisan-capable columns only. A primary where no D or
    # R ballot was cast (an off-cycle issue-only primary) offered no party
    # ballot, so it cannot contribute a partisan count and cannot constitute a
    # party crossing. Filtering here means the cohort ladder, switch/crossing
    # detection and lean scoring all read ONE identical column set — the
    # alternative (counts over every "PRIMARY"-named column) lets a voter be
    # d>=1 AND r>=1 while ever_crossed is False.
    col_meta: list[tuple[str, date_t]] = [
        (m.col, m.date) for m in _resolved if m.partisan_capable
    ]

    # The REGULAR spine is a different question: which elections was a voter
    # expected at, on a fixed cycle (A0's is_regular). Used only as the
    # participation denominator below, never for ballot counts.
    regular_primary_meta: list[tuple[str, date_t]] = [
        (m.col, m.date) for m in _resolved if m.is_regular
    ]

    logger.info('  spine: %d partisan-capable of %d primary cols '
                '(%d regular primaries for the participation denominator)',
                len(col_meta), len(_resolved), len(regular_primary_meta))

    col_meta_newest_first = sorted(col_meta, key=lambda x: x[1], reverse=True)
    col_meta_chrono       = sorted(col_meta, key=lambda x: x[1])

    weights:   dict[str, float] = {}
    is_recent: dict[str, bool]  = {}
    for c, d in col_meta:
        years_since = (reference_date - d).days / 365.25
        w = exp(-0.15 * years_since)
        if d.year % 4 == 0 and d.month == 3:
            w *= 1.25
        weights[c]   = w
        is_recent[c] = years_since <= 5.0

    # -- Build expressions for D / R / X / total counts ---------------------
    d_exprs:   list[pl.Expr] = []
    r_exprs:   list[pl.Expr] = []
    x_exprs:   list[pl.Expr] = []
    m_exprs:   list[pl.Expr] = []
    any_exprs: list[pl.Expr] = []

    for c, _ in col_meta:
        v = pl.col(c).str.strip_chars()
        d_exprs.append((v == 'D').cast(pl.Int32))
        r_exprs.append((v == 'R').cast(pl.Int32))
        # X is the non-partisan (issues-only) ballot. Any other non-empty code
        # is a minor-party ballot (L, G, C, S, N, E seen statewide). Counting
        # those as X labelled minor-party voters "never a party ballot"
        # (DATA_QUALITY.md X-PRIMARIES-MINORPARTY-CONFLATION).
        x_exprs.append((v == 'X').cast(pl.Int32))
        m_exprs.append(((v != '') & ~v.is_in(['D', 'R', 'X'])).cast(pl.Int32))
        any_exprs.append((v != '').cast(pl.Int32))

    # -- Lean numerator/denominator + recent-5yr variant --------------------
    lean_num_exprs:   list[pl.Expr] = []
    lean_den_exprs:   list[pl.Expr] = []
    recent_num_exprs: list[pl.Expr] = []
    recent_den_exprs: list[pl.Expr] = []

    for c, _ in col_meta:
        v = pl.col(c).str.strip_chars()
        w = weights[c]
        ballot_val = (
            pl.when(v == 'D').then(pl.lit(1.0 * w))
              .when(v == 'R').then(pl.lit(-1.0 * w))
              .otherwise(pl.lit(0.0))
        )
        partisan_w = (
            pl.when((v == 'D') | (v == 'R')).then(pl.lit(w))
              .otherwise(pl.lit(0.0))
        )
        lean_num_exprs.append(ballot_val)
        lean_den_exprs.append(partisan_w)
        if is_recent[c]:
            recent_num_exprs.append(ballot_val)
            recent_den_exprs.append(partisan_w)

    # -- switch_count (LEGACY): adjacent partisan flips ----------------------
    # Compares column N to column N+1 directly, so it only sees a crossing
    # when the two partisan ballots land in CONSECUTIVE primary columns. A
    # voter who went D in 2016, skipped 2018, then R in 2020 has an empty
    # cell between them and is missed. Since most voters skip primaries this
    # undercounts crossers ~11x (Montgomery: 3,342 vs 37,809 real). Retained
    # only because existing callers read the column; ever_crossed below is
    # the correct measure — prefer it.
    switch_exprs: list[pl.Expr] = []
    chrono_cols = [c for c, _ in col_meta_chrono]
    for prev_col, next_col in zip(chrono_cols, chrono_cols[1:]):
        v1 = pl.col(prev_col).str.strip_chars()
        v2 = pl.col(next_col).str.strip_chars()
        diff_partisan = (
            ((v1 == 'D') & (v2 == 'R')) | ((v1 == 'R') & (v2 == 'D'))
        ).cast(pl.Int32)
        switch_exprs.append(diff_partisan)

    # -- partisan_ballot_sequence: the COMPACTED subsequence (A4) -----------
    # Concatenate partisan letters oldest-first, emitting '' for a skipped or
    # non-partisan ballot. Skips vanish, so 'D__R' compacts to 'DR' and a
    # crossing becomes a plain substring test. The column is deliberately
    # human-readable: a reviewer can compare 'DDRD' against a voter's raw
    # ballots by eye without rerunning anything.
    seq_letter_exprs: list[pl.Expr] = []
    for c, _ in col_meta_chrono:
        v = pl.col(c).str.strip_chars()
        seq_letter_exprs.append(
            pl.when(v == 'D').then(pl.lit('D'))
              .when(v == 'R').then(pl.lit('R'))
              .otherwise(pl.lit(''))
        )

    # -- years_since_last_partisan ------------------------------------------
    yslp_exprs: list[pl.Expr] = []
    for c, d in col_meta_newest_first:
        years_since = (reference_date - d).days / 365.25
        v = pl.col(c).str.strip_chars()
        ex = (
            pl.when((v == 'D') | (v == 'R')).then(pl.lit(years_since))
              .otherwise(pl.lit(None, dtype=pl.Float64))
        )
        yslp_exprs.append(ex)

    # -- regular_primaries_eligible: the A2 participation denominator -------
    # NOT a constant. "How many regular primaries has THIS voter had the
    # chance to vote in" — so a 2024 registrant is not accountable for 2004.
    # Counting per voter is what makes primary_participation_rate mean "of
    # the ones you could have voted in" rather than "of all history".
    #
    # The anchor is min(REGISTRATION_DATE, first observed ballot), NOT
    # REGISTRATION_DATE alone. Ohio keeps a voter's registration date through
    # name and address changes, but a cancellation followed by reactivation
    # assigns a NEW one while the ballot history is retained (EOM ch.4,
    # "Registration Date"). Measured on Montgomery: 39,234 voters (10.9%)
    # have ballots predating their registration date, median 9.5 years
    # earlier. Anchoring on the registration date alone would credit those
    # voters with fewer eligible primaries than they demonstrably voted in,
    # producing participation rates above 1.0 (4,335 voters). A cast ballot
    # is proof of eligibility at that date, so it is the safer bound.
    #
    # The 1900-01-01 sentinel (2 rows in Montgomery) is a placeholder, not a
    # registration; it would otherwise make those voters eligible for every
    # primary ever held. Voters with neither a valid date nor any ballot get
    # 0 eligible and a NULL rate — undefined, not 0%.
    eligible_exprs: list[pl.Expr] = []
    if regular_primary_meta:
        _anchor: pl.Expr | None = None
        if 'REGISTRATION_DATE' in df.columns:
            _regcol = pl.col('REGISTRATION_DATE')
            _regdate = (
                _regcol if df.schema.get('REGISTRATION_DATE') == pl.Date
                else _regcol.str.to_date(format='%Y-%m-%d', strict=False)
            )
            _anchor = (
                pl.when(_regdate <= pl.lit(date_t(1900, 1, 2)))
                  .then(pl.lit(None, dtype=pl.Date))
                  .otherwise(_regdate)
            )

        # Earliest election (any type, any ballot) this voter shows up in.
        # Uses the full resolved set, not just regular primaries: turning out
        # for ANY election proves registration was already active.
        _first_ballot_exprs = [
            pl.when(pl.col(m.col).str.strip_chars() != '')
              .then(pl.lit(m.date))
              .otherwise(pl.lit(None, dtype=pl.Date))
            for m in _resolved
        ]
        if _first_ballot_exprs:
            _first_ballot = pl.min_horizontal(_first_ballot_exprs)
            _anchor = (_first_ballot if _anchor is None
                       else pl.min_horizontal([_anchor, _first_ballot]))

        if _anchor is not None:
            for _c, _d in regular_primary_meta:
                eligible_exprs.append(
                    (_anchor <= pl.lit(_d)).fill_null(False).cast(pl.Int32)
                )

    # -- Project to working frame ------------------------------------------
    # Carries the partisan-capable spine (the count/lean columns) PLUS any
    # remaining resolved primary column, because the first-ballot anchor above
    # reads every election: turning out for an issue-only primary still proves
    # the registration was active then. A column referenced by an expression
    # but absent from this projection raises ColumnNotFoundError at collect
    # time — it exists in the source parquet but never reaches the frame
    # (CLAUDE.md sec 7).
    _spine_cols = [c for c, _ in col_meta]
    _extra_cols = [m.col for m in _resolved if m.col not in set(_spine_cols)]
    base_select = [pl.col('SOS_VOTERID'),
                   pl.col(party_col).str.strip_chars().alias('_party')]
    if 'REGISTRATION_DATE' in df.columns:
        base_select.append(pl.col('REGISTRATION_DATE'))
    work = df.select(base_select
                     + [pl.col(c) for c in _spine_cols]
                     + [pl.col(c) for c in _extra_cols])

    work = work.with_columns([
        pl.sum_horizontal(d_exprs).alias('d_primaries'),
        pl.sum_horizontal(r_exprs).alias('r_primaries'),
        pl.sum_horizontal(x_exprs).alias('x_primaries'),
        pl.sum_horizontal(m_exprs).alias('m_primaries'),
        pl.sum_horizontal(any_exprs).alias('total_primaries'),
        (pl.sum_horizontal(lean_num_exprs) if lean_num_exprs
            else pl.lit(0.0)).alias('_lean_num'),
        (pl.sum_horizontal(lean_den_exprs) if lean_den_exprs
            else pl.lit(0.0)).alias('_lean_den'),
        (pl.sum_horizontal(recent_num_exprs) if recent_num_exprs
            else pl.lit(0.0)).alias('_recent_num'),
        (pl.sum_horizontal(recent_den_exprs) if recent_den_exprs
            else pl.lit(0.0)).alias('_recent_den'),
        (pl.sum_horizontal(switch_exprs) if switch_exprs
            else pl.lit(0).cast(pl.Int32)).alias('switch_count'),
        (pl.min_horizontal(yslp_exprs) if yslp_exprs
            else pl.lit(None).cast(pl.Float64)).alias('years_since_last_partisan'),
        (pl.concat_str(seq_letter_exprs) if seq_letter_exprs
            else pl.lit('')).alias('partisan_ballot_sequence'),
        (pl.sum_horizontal(eligible_exprs) if eligible_exprs
            else pl.lit(0).cast(pl.Int32)).alias('regular_primaries_eligible'),
    ])

    # ever_crossed: both parties appear anywhere in the compacted sequence.
    # Substring tests on the compacted string, so skipped primaries between
    # the two ballots are irrelevant (that is the whole switch_count bug).
    work = work.with_columns(
        (pl.col('partisan_ballot_sequence').str.contains('D')
         & pl.col('partisan_ballot_sequence').str.contains('R'))
        .alias('ever_crossed')
    )

    work = work.with_columns([
        (pl.col('d_primaries') + pl.col('r_primaries')).alias('partisan_primaries'),
        pl.when(pl.col('_lean_den') > 0)
          .then(pl.col('_lean_num') / pl.col('_lean_den'))
          .otherwise(None)
          .alias('lean_score'),
        pl.when(pl.col('_recent_den') > 0)
          .then(pl.col('_recent_num') / pl.col('_recent_den'))
          .otherwise(None)
          .alias('recent_5yr_lean'),
    ])

    # -- A2 participation: rate + skipped over the regular spine ------------
    # Numerator and denominator MUST span the same column set. Counting
    # partisan ballots over all 35 partisan-capable columns while the
    # denominator covers only the 25 regular ones lets off-cycle turnout push
    # the ratio above 1.0 (4,377 Montgomery voters did exactly that). So the
    # numerator here counts partisan ballots cast IN REGULAR PRIMARIES only —
    # deliberately NOT the `partisan_primaries` column, which spans the wider
    # spine and stays the right measure for cohort assignment.
    #
    # Null (not 0.0) when a voter was eligible for nothing: a voter who
    # registered after the last regular primary has an UNDEFINED rate, not a
    # 0% one, and averaging a fabricated zero would drag every aggregate down.
    regular_partisan_exprs = [
        pl.col(m.col).str.strip_chars().is_in(['D', 'R']).cast(pl.Int32)
        for m in _resolved if m.is_regular
    ]
    work = work.with_columns(
        (pl.sum_horizontal(regular_partisan_exprs) if regular_partisan_exprs
         else pl.lit(0).cast(pl.Int32)).alias('_regular_partisan')
    )
    work = work.with_columns([
        pl.when(pl.col('regular_primaries_eligible') > 0)
          .then(pl.col('_regular_partisan').cast(pl.Float64)
                / pl.col('regular_primaries_eligible').cast(pl.Float64))
          .otherwise(None)
          .alias('primary_participation_rate'),
        (pl.col('regular_primaries_eligible') - pl.col('_regular_partisan'))
          .cast(pl.Int32).alias('primaries_skipped'),
    ])
    work = work.with_columns([
        pl.when(pl.col('partisan_primaries') > 0)
          .then(
              pl.col('lean_score').abs()
              * pl.col('partisan_primaries').cast(pl.Float64)
              / (pl.col('partisan_primaries').cast(pl.Float64) + 2.0)
          )
          .otherwise(None)
          .alias('confidence'),
    ])

    # -- last_three_party: concat partisan letters newest-first, take first 3
    if col_meta_newest_first:
        partisan_letter_exprs = []
        for c, _ in col_meta_newest_first:
            v = pl.col(c).str.strip_chars()
            ex = (
                pl.when(v == 'D').then(pl.lit('D'))
                  .when(v == 'R').then(pl.lit('R'))
                  .otherwise(pl.lit(''))
            )
            partisan_letter_exprs.append(ex)
        work = work.with_columns(
            pl.concat_str(partisan_letter_exprs).str.slice(0, 3).alias('_last3raw')
        )
    else:
        work = work.with_columns(pl.lit('').alias('_last3raw'))

    work = work.with_columns(
        pl.when(pl.col('_last3raw') == 'DDD').then(pl.lit('DDD'))
          .when(pl.col('_last3raw') == 'RRR').then(pl.lit('RRR'))
          .when(pl.col('_last3raw').str.len_chars() == 0).then(pl.lit('NONE'))
          .when(pl.col('_last3raw') == 'D').then(pl.lit('D'))
          .when(pl.col('_last3raw') == 'R').then(pl.lit('R'))
          .when(pl.col('_last3raw') == 'DD').then(pl.lit('DD'))
          .when(pl.col('_last3raw') == 'RR').then(pl.lit('RR'))
          .otherwise(pl.lit('MIX'))
          .alias('last_three_party')
    )

    # -- cohort assignment: 6-cohort schema (top-down, first-match-wins) ----
    # Pure = NEVER opposing primary, ever. Crossovers = Mixed. X-only UNCs = Mixed.
    cohort_expr = (
        pl.when((pl.col('_party') == 'R') & (pl.col('d_primaries') == 0))
          .then(pl.lit('PURE_R'))
        .when((pl.col('_party') == 'D') & (pl.col('r_primaries') == 0))
          .then(pl.lit('PURE_D'))
        .when((pl.col('_party') == 'R') & (pl.col('d_primaries') >= 1))
          .then(pl.lit('CROSSOVER_R'))
        .when((pl.col('_party') == 'D') & (pl.col('r_primaries') >= 1))
          .then(pl.lit('CROSSOVER_D'))
        .when((pl.col('_party') == '') & (pl.col('d_primaries') == 0) & (pl.col('r_primaries') >= 1))
          .then(pl.lit('UNC_LAPSED_R'))
        .when((pl.col('_party') == '') & (pl.col('r_primaries') == 0) & (pl.col('d_primaries') >= 1))
          .then(pl.lit('UNC_LAPSED_D'))
        # Minor-party voters: affiliated with a party other than R or D (the
        # SWVF carries L, and once N), or unaffiliated with a minor-party
        # primary ballot and no D or R ballot. Evaluated BEFORE
        # UNC_NONPARTISAN: a Green ballot is a party ballot, not an
        # issues-only one. An unaffiliated voter with any D/R ballot stays on
        # the lapsed/mixed ladder. A null affiliation matches nothing here,
        # falls to OTHER, and _assert_cohort_coverage raises.
        .when(~pl.col('_party').is_in(['R', 'D', '']))
          .then(pl.lit('MINOR_PARTY'))
        .when((pl.col('_party') == '') & (pl.col('m_primaries') >= 1) &
              (pl.col('d_primaries') == 0) & (pl.col('r_primaries') == 0))
          .then(pl.lit('MINOR_PARTY'))
        # Never cast a partisan ballot, but DID turn out for non-partisan
        # ones (issues, levies, judicial). Evaluated BEFORE the mixed branch:
        # first-match-wins means order is the control flow here. Previously
        # these voters were labelled UNC_MIXED alongside genuine crossers,
        # which put ~11.2k Montgomery voters who have never expressed ANY
        # party preference into a slice that means "crossed party lines".
        .when((pl.col('_party') == '') & (pl.col('x_primaries') >= 1) &
              (pl.col('d_primaries') == 0) & (pl.col('r_primaries') == 0))
          .then(pl.lit('UNC_NONPARTISAN'))
        # Genuine mixers only: ballots in BOTH parties' primaries.
        .when((pl.col('_party') == '') & (pl.col('d_primaries') >= 1) & (pl.col('r_primaries') >= 1))
          .then(pl.lit('UNC_MIXED'))
        .when((pl.col('_party') == '') & (pl.col('total_primaries') == 0))
          .then(pl.lit('UNC_NO_PRIMARY'))
        .otherwise(pl.lit('OTHER'))
        .alias('cohort')
    )
    work = work.with_columns(cohort_expr)

    # The public family axis is BEHAVIOUR (did you cross party lines?), and
    # Active/Lapsed is a TIME qualifier on that behaviour (are you currently
    # affiliated?). Both MIXED_* families now contain only voters who really
    # did vote both parties; voters with no partisan ballot at all get their
    # own family rather than being folded into "Mixed".
    family_map = {
        'PURE_R':          'PURE_R',
        'PURE_D':          'PURE_D',
        'CROSSOVER_R':     'MIXED_ACTIVE',  # currently affiliated, opposing history
        'CROSSOVER_D':     'MIXED_ACTIVE',
        'UNC_LAPSED_R':    'UNC_LAPSED_R',
        'UNC_LAPSED_D':    'UNC_LAPSED_D',
        'UNC_MIXED':       'MIXED_LAPSED',  # UNC who voted BOTH parties
        'UNC_NONPARTISAN': 'UNC_NONPARTISAN',  # turned out, never a party ballot
        'UNC_NO_PRIMARY':  'UNC_NO_PRIMARY',
        'MINOR_PARTY':     'MINOR_PARTY',
        'OTHER':           'OTHER',  # unreachable by design; the assertion below raises
    }
    work = work.with_columns(
        pl.col('cohort').replace(family_map).alias('cohort_family')
    )
    _assert_cohort_coverage(work)

    # -- crossover_class: applies only to R_CROSSOVER, D_CROSSOVER, UNC_MIXED
    # Tightened thresholds for affiliated crossovers (LOCKED 0.5, LEAN 0.3);
    # UNC_MIXED uses 0.4/0.2 (legacy mixed-lean predictor logic).
    crossover_class_expr = (
        pl.when(~pl.col('cohort').is_in(['CROSSOVER_R', 'CROSSOVER_D', 'UNC_MIXED']))
          .then(pl.lit(None, dtype=pl.Utf8))
        .when(pl.col('cohort') == 'UNC_MIXED')
          .then(
              pl.when((pl.col('lean_score') >= 0.40)
                      & (pl.col('last_three_party') == 'DDD')
                      & (pl.col('years_since_last_partisan') <= 6.0))
                .then(pl.lit('LOCKED_D'))
              .when((pl.col('lean_score') <= -0.40)
                      & (pl.col('last_three_party') == 'RRR')
                      & (pl.col('years_since_last_partisan') <= 6.0))
                .then(pl.lit('LOCKED_R'))
              .when(pl.col('lean_score') >= 0.20).then(pl.lit('LEAN_D'))
              .when(pl.col('lean_score') <= -0.20).then(pl.lit('LEAN_R'))
              .otherwise(pl.lit('TRUE_MIXED'))
          )
        .otherwise(
              pl.when((pl.col('lean_score') >= 0.50)
                      & (pl.col('last_three_party') == 'DDD')
                      & (pl.col('years_since_last_partisan') <= 6.0))
                .then(pl.lit('LOCKED_D'))
              .when((pl.col('lean_score') <= -0.50)
                      & (pl.col('last_three_party') == 'RRR')
                      & (pl.col('years_since_last_partisan') <= 6.0))
                .then(pl.lit('LOCKED_R'))
              .when(pl.col('lean_score') >= 0.30).then(pl.lit('LEAN_D'))
              .when(pl.col('lean_score') <= -0.30).then(pl.lit('LEAN_R'))
              .otherwise(pl.lit('TRUE_MIXED'))
        )
        .alias('crossover_class')
    )
    work = work.with_columns(crossover_class_expr)

    # -- is_new_registrant ---------------------------------------------------
    if 'REGISTRATION_DATE' in work.columns:
        if work.schema['REGISTRATION_DATE'] != pl.Date:
            work = work.with_columns(
                pl.col('REGISTRATION_DATE')
                  .str.to_date(format='%Y-%m-%d', strict=False)
                  .alias('_regdate_d')
            )
        else:
            work = work.with_columns(
                pl.col('REGISTRATION_DATE').alias('_regdate_d')
            )
        work = work.with_columns(
            (pl.col('_regdate_d') >= pl.lit(date_t(2024, 1, 1)))
              .fill_null(False)
              .alias('is_new_registrant')
        )
    else:
        work = work.with_columns(pl.lit(False).alias('is_new_registrant'))

    out = work.select([
        'SOS_VOTERID',
        'd_primaries', 'r_primaries', 'x_primaries', 'm_primaries',
        'total_primaries', 'partisan_primaries',
        'lean_score', 'confidence', 'recent_5yr_lean',
        'last_three_party', 'years_since_last_partisan',
        'switch_count', 'ever_crossed', 'partisan_ballot_sequence',
        'regular_primaries_eligible', 'primary_participation_rate',
        'primaries_skipped',
        'cohort', 'cohort_family', 'crossover_class',
        'is_new_registrant',
    ])
    return out


def _unc_classified_from_enriched_df(df: pl.DataFrame) -> 'pl.DataFrame | None':
    """
    Fast path: build the unc_classified frame directly from a df that already
    has cohort columns attached (i.e. after clean_voter_data() ran the universal
    classifier).  Avoids re-running classify_all_voters_primary_history() in
    workers that receive a pre-enriched IPC payload.

    Returns None if cohort_family is not in df.columns.
    Returns columns: SOS_VOTERID, COUNTY_NUMBER, PRECINCT_NAME,
                     d_primaries, r_primaries, total_primaries, unc_class
    """
    if 'cohort_family' not in df.columns:
        return None

    unc_families = ['UNC_LAPSED_D', 'UNC_LAPSED_R', 'MIXED_ACTIVE', 'MIXED_LAPSED',
                    'UNC_NONPARTISAN', 'UNC_NO_PRIMARY']
    unc_class_map = {
        'UNC_LAPSED_D':  'LIFETIME_D',
        'UNC_LAPSED_R':  'LIFETIME_R',
        'MIXED_ACTIVE':  'MIXED',
        'MIXED_LAPSED':  'MIXED',
        # Legacy 4-value schema has no non-partisan slot; NO_HISTORY here
        # means "no PARTISAN history", which is exactly this cohort. The
        # distinction (turned out for issues vs never turned out) survives
        # in cohort_family for consumers that read the 8-slice taxonomy.
        'UNC_NONPARTISAN': 'NO_HISTORY',
        'UNC_NO_PRIMARY': 'NO_HISTORY',
    }
    unc = (
        df.filter(pl.col('cohort_family').is_in(unc_families))
          .with_columns(
              pl.col('cohort_family').replace(unc_class_map).alias('unc_class')
          )
    )
    keep_candidates = ['SOS_VOTERID', 'COUNTY_NUMBER', 'PRECINCT_NAME',
                       'd_primaries', 'r_primaries', 'total_primaries', 'unc_class']
    keep = [c for c in keep_candidates if c in unc.columns]
    return unc.select(keep)


def classify_unc_primary_history(
    df:           pl.DataFrame,
    primary_cols: list[str],
    logger:       logging.Logger,
    reference_date: 'date_t | None' = None,
) -> pl.DataFrame:
    """
    Backward-compat wrapper - delegates to classify_all_voters_primary_history,
    filters to UNC families, and remaps cohort names to the legacy schema
    (LIFETIME_D / LIFETIME_R / MIXED / NO_HISTORY).

    ``reference_date`` defaults to the staged snapshot's date via
    get_snapshot_date() — which RAISES if nothing is staged rather than
    silently substituting today. The parameter exists so a caller already
    holding the date can avoid re-reading staged_from.json, not to allow an
    arbitrary one.

    Returns columns: SOS_VOTERID, COUNTY_NUMBER, PRECINCT_NAME,
                     d_primaries, r_primaries, total_primaries, unc_class
    """
    if reference_date is None:
        reference_date = get_snapshot_date(logger)
    all_classified = classify_all_voters_primary_history(
        df, primary_cols, logger, reference_date=reference_date)

    unc_families = ['UNC_LAPSED_D', 'UNC_LAPSED_R', 'MIXED_ACTIVE', 'MIXED_LAPSED',
                    'UNC_NONPARTISAN', 'UNC_NO_PRIMARY']
    unc = all_classified.filter(pl.col('cohort_family').is_in(unc_families))

    unc_class_map = {
        'UNC_LAPSED_D':  'LIFETIME_D',
        'UNC_LAPSED_R':  'LIFETIME_R',
        'MIXED_ACTIVE':  'MIXED',
        'MIXED_LAPSED':  'MIXED',
        # Legacy 4-value schema has no non-partisan slot; NO_HISTORY here
        # means "no PARTISAN history", which is exactly this cohort. The
        # distinction (turned out for issues vs never turned out) survives
        # in cohort_family for consumers that read the 8-slice taxonomy.
        'UNC_NONPARTISAN': 'NO_HISTORY',
        'UNC_NO_PRIMARY': 'NO_HISTORY',
    }
    unc = unc.with_columns(
        pl.col('cohort_family').replace(unc_class_map).alias('unc_class')
    )

    extra_cols = [c for c in ['COUNTY_NUMBER', 'PRECINCT_NAME'] if c in df.columns]
    if extra_cols:
        unc = unc.join(
            df.select(['SOS_VOTERID'] + extra_cols),
            on='SOS_VOTERID',
            how='left',
        )

    keep = [c for c in ['SOS_VOTERID', 'COUNTY_NUMBER', 'PRECINCT_NAME',
                         'd_primaries', 'r_primaries', 'total_primaries', 'unc_class']
            if c in unc.columns]
    return unc.select(keep)


def export_unc_shadow_json(
    county_name:    str,
    df:             pl.DataFrame,
    primary_cols:   list[str],
    logger:         logging.Logger,
    unc_classified: 'pl.DataFrame | None' = None,
) -> bool:
    """
    Classify UNC voters and write {slug}_unc_shadow.json for the dashboard.

    Returns True if the file was written, False if skipped (e.g. no UNC voters
    or no primary columns).  The caller uses the return value to decide whether
    to add a manifest section for this county.

    JSON schema (stacked bar):
      type: 'bar'
      chartOptions: { scales: { x: { stacked: true }, y: { stacked: true } } }
      chartConfig.labels: ['Unaffiliated (UNC)']   — single x-axis group
      chartConfig.datasets: four datasets, one per classification
    """
    slug  = county_name.lower().replace(' ', '_')
    today = date_t.today().isoformat()
    out   = DATA_DIR / f'{slug}_unc_shadow.json'

    if not primary_cols:
        logger.info('  UNC shadow: no primary columns — skipping %s', county_name)
        return False

    classified = unc_classified if unc_classified is not None else classify_unc_primary_history(df, primary_cols, logger)
    if classified.is_empty():
        logger.info('  UNC shadow: no UNC voters — skipping %s', county_name)
        return False

    counts = (
        classified
        .group_by('unc_class')
        .agg(pl.len().alias('n'))
    )
    count_map: dict[str, int] = dict(zip(
        counts['unc_class'].to_list(),
        counts['n'].to_list(),
    ))

    order = ['LIFETIME_D', 'LIFETIME_R', 'MIXED', 'NO_HISTORY']
    total_unc = sum(count_map.values())

    datasets = []
    for cls in order:
        n = count_map.get(cls, 0)
        datasets.append({
            'label':           UNC_SHADOW_LABELS[cls],
            'data':            [n],
            'backgroundColor': UNC_SHADOW_COLORS[cls],
            'borderRadius':    4,
        })

    _dump_json({
        'title':       'UNC Voter Primary History',
        'county':      county_name,
        'geography':   'county',
        'type':        'bar',
        'updated':     today,
        'note':        UNC_SHADOW_NOTE,
        'totalUnc':    total_unc,
        'chartOptions': {
            'scales': {
                'x': {'stacked': True},
                'y': {'stacked': True},
            },
            'plugins': {
                'tooltip': {
                    'callbacks': {}   # overridden by charts.js defaults
                }
            },
        },
        'chartConfig': {
            'labels':   ['Unaffiliated (UNC)'],
            'datasets': datasets,
        },
    }, out, logger)

    logger.info('  UNC shadow JSON: %s  (total UNC=%s)', out.name, f'{total_unc:,}')
    return True


def export_json(
    county_name:             str,
    df:                      pl.DataFrame,
    election_cols:           list[str],
    logger:                  logging.Logger,
    update_manifest:         bool = True,
    unc_classified:          'pl.DataFrame | None' = None,
    include_precinct_charts: bool = False,
    write_precinct_summary:  bool = True,
):
    """
    Write four JSON files into docs/data/ matching the exact schema expected by
    charts.js / index.html.  These replace the sample data files shipped with
    the dashboard.

    The web dashboard reads these files at runtime via fetch() — the Python
    script and the browser share the same data without any intermediate conversion.
    When you run an analysis for a new county, the dashboard automatically gets
    that county's data the next time the page loads.

    Files written:
      {slug}_decade_distribution.json  — bar chart: voters by birth decade
      {slug}_party_affiliation.json    — doughnut: party breakdown
      {slug}_party_by_decade.json      — grouped bar: party × decade
      {slug}_precinct_summary.json     — table: precinct active/inactive counts
    """
    slug    = county_name.lower().replace(' ', '_')
    today   = date_t.today().isoformat()
    note    = f'Analysis run {today} — Ohio Secretary of State SWVF voter file'

    # ── Decade distribution (bar chart) ───────────────────────────────────────
    decade_df = (
        df.group_by('Decade')
          .agg(pl.len().alias('count'))
          .sort('Decade')
    )
    _dump_json({
        'title':     'Voter Age Distribution by Birth Decade',
        'county':    county_name,
        'geography': 'county',
        'type':      'bar',
        'updated':   today,
        'note':      note,
        'chartConfig': {
            'labels': [f'{d}s' for d in decade_df['Decade'].to_list()],
            'datasets': [{
                'label':           'Registered Voters',
                'data':            decade_df['count'].to_list(),
                'backgroundColor': CHART_COLORS['bar'],
                'borderRadius':    4,
            }],
        },
    }, DATA_DIR / f'{slug}_decade_distribution.json', logger)

    # ── Party affiliation (doughnut chart) ────────────────────────────────────
    # 9-slice partisan-spectrum layout based on cohort_family.  Falls back to
    # the legacy PARTY_LABEL doughnut when the universal classifier output is
    # not present.  COHORT_SLICES + COHORT_STACK_MAP are module-level constants.
    meta_block: dict | None = None
    if 'cohort_family' in df.columns:
        cohort_counts_df = (
            df.group_by('cohort_family').agg(pl.len().alias('n'))
        )
        cmap = dict(zip(cohort_counts_df['cohort_family'].to_list(),
                        cohort_counts_df['n'].to_list()))
        pa_labels = [lbl   for _, lbl, _ in COHORT_SLICES]
        pa_colors = [color for _, _, color in COHORT_SLICES]
        pa_counts = [int(cmap.get(fam, 0)) for fam, _, _ in COHORT_SLICES]
        pa_note   = note + ' — partisan spectrum: affiliated + behavioral cohorts.'

        # New-registrant counts by family (for dashboard meta block)
        if 'is_new_registrant' in df.columns:
            new_reg_df = (
                df.filter(pl.col('is_new_registrant'))
                  .group_by('cohort_family')
                  .agg(pl.len().alias('n'))
            )
            nm = dict(zip(new_reg_df['cohort_family'].to_list(),
                          new_reg_df['n'].to_list()))
            new_r   = int(nm.get('PURE_R', 0))
            new_d   = int(nm.get('PURE_D', 0))
            new_unc = (int(nm.get('UNC_LAPSED_R', 0))
                       + int(nm.get('UNC_LAPSED_D', 0))
                       + int(nm.get('MIXED_ACTIVE', 0))
                       + int(nm.get('MIXED_LAPSED', 0))
                       + int(nm.get('UNC_NONPARTISAN', 0))
                       + int(nm.get('UNC_NO_PRIMARY', 0)))
            meta_block = {
                'new_registrants_r':   new_r,
                'new_registrants_d':   new_d,
                'new_registrants_unc': new_unc,
            }
    else:
        # Legacy fallback: PARTY_LABEL doughnut + optional unc_classified split
        party_df = (
            df.group_by('PARTY_LABEL')
              .agg(pl.len().alias('count'))
              .sort('count', descending=True)
        )
        if unc_classified is not None and not unc_classified.is_empty():
            shadow_counts = (
                unc_classified.group_by('unc_class').agg(pl.len().alias('n'))
            )
            sc = dict(zip(shadow_counts['unc_class'].to_list(),
                           shadow_counts['n'].to_list()))
            non_unc_map = {row['PARTY_LABEL']: row['count']
                           for row in party_df.iter_rows(named=True)
                           if row['PARTY_LABEL'] != 'UNC'}
            pa_labels, pa_colors, pa_counts = [], [], []
            for reg_party, unc_cls, unc_label, unc_color in [
                ('REP', 'LIFETIME_R', 'UNC – Lifetime R', UNC_SHADOW_COLORS['LIFETIME_R']),
                ('DEM', 'LIFETIME_D', 'UNC – Lifetime D', UNC_SHADOW_COLORS['LIFETIME_D']),
            ]:
                if reg_party in non_unc_map:
                    pa_labels.append(reg_party)
                    pa_colors.append(CHART_COLORS.get(reg_party, '#6366f1'))
                    pa_counts.append(non_unc_map[reg_party])
                pa_labels.append(unc_label)
                pa_colors.append(unc_color)
                pa_counts.append(sc.get(unc_cls, 0))
            pa_labels += ['UNC – Mixed', 'UNC – No History']
            pa_colors += [UNC_SHADOW_COLORS['MIXED'], UNC_SHADOW_COLORS['NO_HISTORY']]
            pa_counts += [sc.get('MIXED', 0), sc.get('NO_HISTORY', 0)]
            for other_party, other_count in non_unc_map.items():
                if other_party not in ('REP', 'DEM'):
                    pa_labels.append(other_party)
                    pa_colors.append(CHART_COLORS.get(other_party, '#6366f1'))
                    pa_counts.append(other_count)
            pa_note = note + ' — UNC split by primary ballot history; see methodology note.'
        else:
            pa_labels = party_df['PARTY_LABEL'].to_list()
            pa_colors = [CHART_COLORS.get(p, '#6366f1') for p in pa_labels]
            pa_counts = party_df['count'].to_list()
            pa_note   = note
    party_payload = {
        'title':     'Party Affiliation Breakdown',
        'county':    county_name,
        'geography': 'county',
        'type':      'doughnut',
        'updated':   today,
        'note':      pa_note,
        'chartConfig': {
            'labels': pa_labels,
            'datasets': [{
                'data':            pa_counts,
                'backgroundColor': pa_colors,
                'borderWidth':     2,
                'borderColor':     'transparent',
            }],
        },
    }
    if meta_block is not None:
        party_payload['meta'] = meta_block
    _dump_json(party_payload, DATA_DIR / f'{slug}_party_affiliation.json', logger)

    # ── Party × Decade (stacked bar chart) ────────────────────────────────────
    # Pivot on cohort_family so labels/colors/order match the doughnut.
    # Falls back to the legacy PARTY_LABEL+unc_classified shape only if
    # cohort_family is not present on df (unenriched callers).
    if 'cohort_family' in df.columns:
        cross = (
            df.filter(pl.col('Decade').is_not_null())
              .group_by(['Decade', 'cohort_family'])
              .agg(pl.len().alias('n'))
              .pivot(on='cohort_family', index='Decade',
                     values='n', aggregate_function='sum')
              .sort('Decade')
        )
        decade_labels = [f'{d}s' for d in cross['Decade'].to_list()]
        decade_vals   = cross['Decade'].to_list()
        datasets      = []
        for fam, lbl, color in COHORT_SLICES:
            if fam in cross.columns:
                vals = [int(v or 0) for v in cross[fam].to_list()]
            else:
                vals = [0] * len(decade_vals)
            datasets.append({
                'label':           lbl,
                'data':            vals,
                'backgroundColor': color,
                'borderRadius':    3,
                'stack':           COHORT_STACK_MAP[fam],
            })
    else:
        # Legacy fallback — original PARTY_LABEL pivot + unc_classified split.
        cross = (
            df.group_by(['Decade', 'PARTY_LABEL'])
              .agg(pl.len().alias('count'))
              .pivot(on='PARTY_LABEL', index='Decade', values='count', aggregate_function='sum')
              .sort('Decade')
        )
        decade_labels = [f'{d}s' for d in cross['Decade'].to_list()]
        datasets      = []
        for party, stack_id in [('REP', 'rep'), ('DEM', 'dem'), ('Other', 'other')]:
            if party not in cross.columns:
                continue
            datasets.append({
                'label':           party,
                'data':            [v or 0 for v in cross[party].to_list()],
                'backgroundColor': CHART_COLORS.get(party, '#6366f1'),
                'borderRadius':    3,
                'stack':           stack_id,
            })
        if unc_classified is not None and not unc_classified.is_empty():
            unc_decade = (
                unc_classified
                .join(df.select(['SOS_VOTERID', 'Decade']), on='SOS_VOTERID', how='left')
                .group_by(['Decade', 'unc_class'])
                .agg(pl.len().alias('n'))
                .pivot(on='unc_class', index='Decade', values='n', aggregate_function='sum')
                .sort('Decade')
            )
            decade_vals = cross['Decade'].to_list()
            for cls, label, color in [
                ('LIFETIME_D', 'UNC – Lifetime D', UNC_SHADOW_COLORS['LIFETIME_D']),
                ('LIFETIME_R', 'UNC – Lifetime R', UNC_SHADOW_COLORS['LIFETIME_R']),
                ('MIXED',      'UNC – Mixed',       UNC_SHADOW_COLORS['MIXED']),
                ('NO_HISTORY', 'UNC – No History',  UNC_SHADOW_COLORS['NO_HISTORY']),
            ]:
                if cls in unc_decade.columns:
                    dmap = dict(zip(unc_decade['Decade'].to_list(),
                                    unc_decade[cls].to_list()))
                    vals = [int(dmap.get(d, 0) or 0) for d in decade_vals]
                else:
                    vals = [0] * len(decade_vals)
                datasets.append({
                    'label':           label,
                    'data':            vals,
                    'backgroundColor': color,
                    'borderRadius':    3,
                    'stack':           'unc',
                })
        else:
            if 'UNC' in cross.columns:
                datasets.append({
                    'label':           'UNC',
                    'data':            [v or 0 for v in cross['UNC'].to_list()],
                    'backgroundColor': CHART_COLORS['UNC'],
                    'borderRadius':    3,
                    'stack':           'unc',
                })
    _dump_json({
        'title':     'Party Affiliation by Birth Decade',
        'county':    county_name,
        'geography': 'county',
        'type':      'bar',
        'updated':   today,
        'note':      note,
        'chartConfig': {
            'labels':   decade_labels,
            'datasets': datasets,
        },
    }, DATA_DIR / f'{slug}_party_by_decade.json', logger)

    # ── Generation distribution (bar chart) ───────────────────────────────────
    gen_df = build_generation_summary(df)
    _dump_json({
        'title':     'Voter Age Distribution by Generation',
        'county':    county_name,
        'geography': 'county',
        'type':      'bar',
        'updated':   today,
        'note':      note + ' — Pew Research Center generational boundaries',
        'chartConfig': {
            'labels': gen_df['Generation'].to_list(),
            'datasets': [{
                'label':           'Registered Voters',
                'data':            gen_df['Voter Count'].to_list(),
                'backgroundColor': CHART_COLORS['bar'],
                'borderRadius':    4,
            }],
        },
    }, DATA_DIR / f'{slug}_generation_distribution.json', logger)

    # ── Party × Generation (stacked bar chart) ────────────────────────────────
    # Pivot on cohort_family so labels/colors/order match the doughnut.
    if 'cohort_family' in df.columns:
        gen_cross = (
            df.filter(pl.col('Generation').is_not_null())
              .group_by(['Generation', 'cohort_family'])
              .agg(pl.len().alias('n'))
              .pivot(on='cohort_family', index='Generation',
                     values='n', aggregate_function='sum')
        )
        order_df = pl.DataFrame({'Generation': _GEN_ORDER,
                                 '_sort': list(range(len(_GEN_ORDER)))})
        gen_cross = (
            gen_cross.join(order_df, on='Generation', how='left')
                     .sort('_sort')
                     .drop('_sort')
        )
        gen_labels   = gen_cross['Generation'].to_list()
        gen_datasets = []
        for fam, lbl, color in COHORT_SLICES:
            if fam in gen_cross.columns:
                vals = [int(v or 0) for v in gen_cross[fam].fill_null(0).to_list()]
            else:
                vals = [0] * len(gen_labels)
            gen_datasets.append({
                'label':           lbl,
                'data':            vals,
                'backgroundColor': color,
                'borderRadius':    2,
                'stack':           COHORT_STACK_MAP[fam],
            })
    else:
        # Legacy fallback — original PARTY_LABEL pivot + unc_classified split.
        gen_cross = (
            df.group_by(['Generation', 'PARTY_LABEL'])
              .agg(pl.len().alias('count'))
              .pivot(on='PARTY_LABEL', index='Generation',
                     values='count', aggregate_function='sum')
        )
        order_df = pl.DataFrame({'Generation': _GEN_ORDER,
                                 '_sort': list(range(len(_GEN_ORDER)))})
        gen_cross = (
            gen_cross.join(order_df, on='Generation', how='left')
                     .sort('_sort')
                     .drop('_sort')
        )
        gen_labels   = gen_cross['Generation'].to_list()
        gen_datasets = []
        for party, stack_id in [('REP', 'rep'), ('DEM', 'dem'), ('Other', 'other')]:
            color = CHART_COLORS.get(party, '#888888')
            gen_datasets.append({
                'label':           party,
                'data':            gen_cross[party].fill_null(0).to_list() if party in gen_cross.columns else [],
                'backgroundColor': color,
                'borderRadius':    2,
                'stack':           stack_id,
            })
        if unc_classified is not None and not unc_classified.is_empty():
            unc_gen = (
                unc_classified
                .join(df.select(['SOS_VOTERID', 'Generation']), on='SOS_VOTERID', how='left')
                .group_by(['Generation', 'unc_class'])
                .agg(pl.len().alias('n'))
                .pivot(on='unc_class', index='Generation', values='n', aggregate_function='sum')
            )
            sort_df = pl.DataFrame({'Generation': _GEN_ORDER, '_sort': list(range(len(_GEN_ORDER)))})
            unc_gen = (
                unc_gen.join(sort_df, on='Generation', how='left').sort('_sort').drop('_sort')
            )
            for cls, label, color in [
                ('LIFETIME_D', 'UNC – Lifetime D', UNC_SHADOW_COLORS['LIFETIME_D']),
                ('LIFETIME_R', 'UNC – Lifetime R', UNC_SHADOW_COLORS['LIFETIME_R']),
                ('MIXED',      'UNC – Mixed',       UNC_SHADOW_COLORS['MIXED']),
                ('NO_HISTORY', 'UNC – No History',  UNC_SHADOW_COLORS['NO_HISTORY']),
            ]:
                if cls in unc_gen.columns:
                    gmap  = dict(zip(unc_gen['Generation'].to_list(), unc_gen[cls].to_list()))
                    vals  = [int(gmap.get(g, 0) or 0) for g in gen_labels]
                else:
                    vals = [0] * len(gen_labels)
                gen_datasets.append({
                    'label':           label,
                    'data':            vals,
                    'backgroundColor': color,
                    'borderRadius':    2,
                    'stack':           'unc',
                })
        else:
            color = CHART_COLORS.get('UNC', '#888888')
            gen_datasets.append({
                'label':           'UNC',
                'data':            gen_cross['UNC'].fill_null(0).to_list() if 'UNC' in gen_cross.columns else [],
                'backgroundColor': color,
                'borderRadius':    2,
                'stack':           'unc',
            })
    _dump_json({
        'title':     'Party Affiliation by Generation',
        'county':    county_name,
        'geography': 'county',
        'type':      'bar',
        'updated':   today,
        'note':      note + ' — Pew Research Center generational boundaries',
        'chartConfig': {
            'labels':   gen_labels,
            'datasets': gen_datasets,
        },
    }, DATA_DIR / f'{slug}_party_by_generation.json', logger)

    # ── Primary participation rate by generation (bar chart) ─────────────────
    # Aggregates the A2 classifier's per-voter regular-primary fields
    # (regular_primaries_eligible / primary_participation_rate) — computed
    # once in classify_all_voters_primary_history() and joined onto df in
    # clean_voter_data(). Do NOT recompute a denominator here; a second
    # denominator over a different column set is exactly the §5 violation
    # this repair exists to fix. primary_participation_rate is NULL (not 0.0)
    # for voters with regular_primaries_eligible == 0 — Polars mean() already
    # skips nulls, so no fill_null(0) before aggregating.
    if 'primary_participation_rate' in df.columns:
        part_gen = (
            df.filter(pl.col('Generation').is_not_null())
              .group_by('Generation')
              .agg(
                  pl.col('primary_participation_rate').mean().alias('rate'),
                  pl.col('primary_participation_rate').is_not_null().sum().alias('n_eligible'),
              )
        )
        order_df = pl.DataFrame({'Generation': _GEN_ORDER,
                                 '_sort': list(range(len(_GEN_ORDER)))})
        part_gen = (
            part_gen.join(order_df, on='Generation', how='left')
                    .sort('_sort')
                    .drop('_sort')
        )
        part_labels = part_gen['Generation'].to_list()
        # Chart payload is 0-100 (percentY render option expects a percent
        # scale); the underlying primary_participation_rate field stays 0-1
        # per its documented convention (§4) — this rescale is display-only.
        part_rates  = [round(r * 100, 2) if r is not None else None
                       for r in part_gen['rate'].to_list()]
        _dump_json({
            'title':     'Primary Participation Rate by Generation',
            'county':    county_name,
            'geography': 'county',
            'type':      'bar',
            'updated':   today,
            'note':      (note + ' — share of the regular primaries (specials '
                          "excluded) you've been eligible for "
                          'since registering in which you took a party ballot. '
                          'Pew Research Center generational '
                          'boundaries. Generations with no eligible voters show as '
                          'a gap, not zero.'),
            'chartConfig': {
                'labels': part_labels,
                'datasets': [{
                    'label':           'Primary Participation Rate',
                    'data':            part_rates,
                    'backgroundColor': CHART_COLORS['bar'],
                    'borderRadius':    4,
                }],
            },
        }, DATA_DIR / f'{slug}_participation_by_generation.json', logger)

    # ── Precinct summary (table) ──────────────────────────────────────────────
    # Rows name the precinct only, which identifies it within one county. A
    # statewide table would list ~8,900 names without their counties, so the
    # statewide export skips it (write_precinct_summary=False).
    if write_precinct_summary:
        precinct_df = build_precinct_summary(df)
        rows = []
        for row in precinct_df.iter_rows(named=True):
            rows.append([
                row.get('PRECINCT_NAME', ''),
                f"{row.get('ACTIVE', 0):,}",
                f"{row.get('CONFIRMATION', 0):,}",
                f"{row.get('Total Registered', 0):,}",
                row.get('Est. Unregistered', 'N/A'),
            ])
        _dump_json({
            'title':     'Registration by Precinct',
            'county':    county_name,
            'geography': 'precinct',
            'type':      'table',
            'updated':   today,
            'note':      note + ' — Est. Unregistered requires Census integration (future)',
            'headers':   ['Precinct', 'Active', 'Confirmation', 'Total Registered', 'Est. Unregistered'],
            'rows':      rows,
        }, DATA_DIR / f'{slug}_precinct_summary.json', logger)

    # ── City/township summary (table) ─────────────────────────────────────────
    city_df = build_city_summary(df)
    city_rows = []
    for row in city_df.iter_rows(named=True):
        city_rows.append([
            row.get('COUNTY_NUMBER', ''),
            row.get('City', ''),
            f"{row.get('ACTIVE', 0):,}",
            f"{row.get('CONFIRMATION', 0):,}",
            f"{row.get('Total Registered', 0):,}",
            str(row.get('Precincts', '')),
            row.get('Est. Unregistered', 'N/A'),
        ])
    _dump_json({
        'title':     'Registration by City / Township',
        'county':    county_name,
        'geography': 'city',
        'type':      'table',
        'updated':   today,
        'note':      note + ' — City extracted from precinct name. Cross-county cities show separate rows per county until Phase 3 statewide merge.',
        'headers':   ['County #', 'City / Township', 'Active', 'Confirmation', 'Total Registered', 'Precincts', 'Est. Unregistered'],
        'rows':      city_rows,
    }, DATA_DIR / f'{slug}_city_summary.json', logger)

    logger.info('JSON export complete for %s County (%s)', county_name, slug)

    # ── Per-precinct chart files (opt-in) ─────────────────────────────────────
    if include_precinct_charts:
        export_precinct_charts(county_name, slug, df, election_cols, unc_classified, logger)

    if update_manifest:
        _update_manifest(county_name, slug, today, logger)


def _precinct_safe_name(precinct_name: str) -> str:
    """Convert a precinct display name to a filesystem-safe slug."""
    s = precinct_name.lower()
    s = re.sub(r'[^a-z0-9]+', '_', s)
    s = re.sub(r'_+', '_', s)
    return s.strip('_')


_CITY_SUFFIX_RE = re.compile(r'\s+(?:CITY|VILLAGE|CITY CORP|CORP)$', re.IGNORECASE)
# A township token in PRECINCT_NAME means 'not a city' (used when the
# TOWNSHIP column is blank, e.g. Wyandot's 'ANTRIM TS'). ' TS' is a
# Wyandot-only trailing abbreviation (verified statewide: 13 precincts,
# all trailing, no collisions). TWP/TOWNSHIP are the general markers.
_TOWNSHIP_NAME_RE = re.compile(r'\bTWP\b|\bTOWNSHIP\b|\sTS$')
# Cuyahoga (county 18) appends a sub-precinct suffix ('-00-A' ... '-00-I')
# to PRECINCT_NAME. In rule 1's blank-TOWNSHIP fallback that would fragment
# one township into N per-suffix fakes (Olmsted Township -> 9). Strip a
# recognized suffix before using the precinct name as the township name.
# Shape verified statewide 2026-07-11: matches only 10 Cuyahoga sub-precincts,
# zero real township names (0 of 810 distinct TOWNSHIP values).
_SUBPRECINCT_SUFFIX_RE = re.compile(r'-\d{2}-[A-Z]$')

# Rule 4's city-prefix gate (2026-07-11 encoding census, encoding_census.py
# RULE4-NO-TOKEN). A WARD value declares a municipality ONLY when a recognized
# separator token follows the name: ' WARD' (the common shape), ' DISTRICT'
# (Cuyahoga 'MAPLE HEIGHTS DISTRICT 3'), optionally preceded by ' COUNCIL'
# (Franklin 'COLUMBUS COUNCIL DISTRICT 7'). A tokenless value (Stark 'JACKSON',
# Sandusky 'CLYDE CITY') is NOT a city declaration -- pre-fix the split
# returned the whole string and minted phantom cities for 153,833 voters
# across 4 counties.
_WARD_CITY_PREFIX_RE = re.compile(r'\s+(?:COUNCIL\s+)?(?:WARD|DISTRICT)\b')


def _normalize_city_name(name: str) -> str:
    """Strip Ohio municipal-type suffixes so 'KETTERING CITY' -> 'KETTERING'."""
    return _CITY_SUFFIX_RE.sub('', name.strip()).strip() if name else ''


def _dominant_per_precinct(df: pl.DataFrame, value_col: str) -> dict:
    """PRECINCT_NAME -> most-frequent non-blank value of value_col.
    
    TODO: This lossy "dominant" compression is a hacky workaround that must be revisited.
    The problem stems from the fact that precincts often get consolidated or split at
    separate cadences than other political subdivisions, such as Municipal court districts,
    which are not districts intended for voter management. Congressional redistricting is
    governed by Article XIX of the Ohio Constitution, while census and population shifts
    trigger ORC 3501.18(A): "each precinct shall contain a number of electors, not to exceed
    one thousand four hundred, that the board of elections determines to be a reasonable
    number after taking into consideration the type and amount of available equipment...".
    Because BOEs draw precinct lines around equipment capacity rather than strict civic
    boundaries, single precincts routinely bisect multiple jurisdictions, leading to split
    resolutions and exact mathematical ties (e.g., Milford A, Fultonham, Windham Twp B)
    that are arbitrarily collapsed here.
    """
    work = (
        df.select([
            pl.col('PRECINCT_NAME'),
            pl.col(value_col).str.strip_chars().alias('_v'),
        ])
        .filter(pl.col('_v').is_not_null() & (pl.col('_v').str.len_chars() > 0))
    )
    if work.is_empty():
        return {}
    dominant = (
        work.group_by(['PRECINCT_NAME', '_v'])
            .agg(pl.len().alias('n'))
            .sort(['PRECINCT_NAME', 'n'], descending=[False, True])
            .group_by('PRECINCT_NAME')
            .first()
    )
    return {row['PRECINCT_NAME']: row['_v'] for row in dominant.iter_rows(named=True)}


def _place_per_precinct(df: pl.DataFrame) -> dict:
    """
    Map PRECINCT_NAME -> {'type': 'city'|'village'|'township', 'name': <str>}
    for one county's df, resolving EVERY precinct to exactly one place (never
    None). This is the single authoritative place resolver (CLAUDE.md 5); the
    city resolver _dominant_city_per_precinct below is a thin derivation of it,
    so villages and townships can never leak into the city summary /
    city_county_map again (the historical postal-city and village-as-city bugs).

    The Ohio SWVF carries two unrelated location families:
      * Authoritative jurisdiction: CITY, VILLAGE, WARD, TOWNSHIP, PRECINCT --
        the Board of Elections' assignment of where a voter legally votes.
      * Postal address: RESIDENTIAL_CITY -- USPS mail delivery only, the LAST
        resort, never a jurisdiction override.

    Resolution order per precinct (first match wins), mirroring the 4
    hierarchy, but recording a TYPE + display name instead of collapsing
    townships to None / villages to city:
      1. PRECINCT_NAME township token (TWP / TOWNSHIP / trailing ' TS') ->
         township. Name = dominant TOWNSHIP column value, falling back to the
         precinct name token when TOWNSHIP is blank (e.g. Wyandot 'ANTRIM TS').
         Runs first so a MINORITY city/village value in an unincorporated
         precinct (e.g. Wyandot's Village of Kirby inside 'JACKSON TS') cannot
         claim it.
      2. CITY     -> city    (normalized via _normalize_city_name).
      3. VILLAGE  -> village. Name = the RAW VILLAGE value, keeping the
         ' VILLAGE' suffix so _place_slug matches the village bundle filename
         (e.g. 'NEW LEBANON VILLAGE' -> montgomery_new_lebanon_village).
      4. WARD -> place, token-gated (2026-07-11 census fix): a value
         carrying a township token is the township restated in WARD (Lucas
         'WASHINGTON TOWNSHIP'); a municipality prefix before ' WARD' /
         ' DISTRICT' (optional ' COUNCIL') is a city (Cuyahoga
         'CLEVELAND WARD 7', 'MAPLE HEIGHTS DISTRICT 3'); a tokenless value
         (Stark 'JACKSON', Sandusky 'CLYDE CITY') is NO claim -- it falls
         through to rules 5/6. Pre-fix, tokenless values minted phantom
         cities for 153,833 voters in 4 counties (encoding_census.py
         RULE4-NO-TOKEN). Ward-entity handling itself is out of scope here.
      5. TOWNSHIP column populated -> township (column-based counterpart to
         step 1, for counties that populate TOWNSHIP but use a non-township
         precinct name).
      6. RESIDENTIAL_CITY postal last resort -> city.

    Run tools/admin/validate_jurisdiction_fields.py on every new SWVF drop to
    confirm the per-county coverage this logic relies on.
    """
    cols = df.columns
    if 'PRECINCT_NAME' not in cols:
        return {}

    township_by_p = _dominant_per_precinct(df, 'TOWNSHIP') if 'TOWNSHIP' in cols else {}
    resolved: dict = {}

    def fill(col: str, place_type: str, rule: int, transform=None):
        """Record (place_type, name, rule) from the dominant non-blank value of
        col, but only for precincts not already claimed by a higher-priority
        rule. 'rule' is provenance for the encoding census (CLAUDE.md 5): it
        names WHICH precedence rule claimed the precinct, so per-county
        rule-precedence assertions never re-derive resolver logic."""
        if col not in cols:
            return
        for pname, val in _dominant_per_precinct(df, col).items():
            if pname in resolved:
                continue
            name = transform(val) if transform else val
            if name:
                resolved[pname] = {'type': place_type, 'name': name, 'rule': rule}

    # 1: PRECINCT_NAME township token -> township (highest priority).
    for pname in df['PRECINCT_NAME'].drop_nulls().unique().to_list():
        if _TOWNSHIP_NAME_RE.search(pname.upper()):
            tname = (township_by_p.get(pname)
                     or _SUBPRECINCT_SUFFIX_RE.sub('', pname.strip()).strip())
            resolved.setdefault(pname,
                                {'type': 'township', 'name': tname, 'rule': 1})

    # 2: CITY (normalized).
    fill('CITY', 'city', 2, _normalize_city_name)
    # 3: VILLAGE -> its own type, RAW value (keep suffix for slug parity).
    fill('VILLAGE', 'village', 3)
    # 4: WARD -> place, token-gated (2026-07-11 census fix; see the
    #    docstring's rule-4 bullet and encoding_census.py RULE4-NO-TOKEN).
    if 'WARD' in cols:
        for pname, val in _dominant_per_precinct(df, 'WARD').items():
            if pname in resolved:
                continue
            if _TOWNSHIP_NAME_RE.search(val.upper()):
                # 4a: the township restated in WARD (schema abuse; mirrors
                # _ward_map_per_county's guard). Name from the TOWNSHIP
                # column when populated, else the WARD value itself.
                tname = township_by_p.get(pname) or val.strip()
                resolved[pname] = {'type': 'township', 'name': tname,
                                   'rule': 4}
            elif _WARD_CITY_PREFIX_RE.search(val):
                # 4b: municipality prefix before the separator token.
                raw_prefix = _WARD_CITY_PREFIX_RE.split(val, maxsplit=1)[0]
                if 'VILLAGE' in raw_prefix.upper():
                    resolved[pname] = {'type': 'village', 'name': raw_prefix.strip(), 'rule': 4}
                else:
                    name = _normalize_city_name(raw_prefix)
                    if name:
                        resolved[pname] = {'type': 'city', 'name': name,
                                           'rule': 4}
            # 4c: tokenless -> no claim; rules 5/6 decide.
    # 5: TOWNSHIP column populated -> township.
    for pname, tname in township_by_p.items():
        resolved.setdefault(pname,
                            {'type': 'township', 'name': tname, 'rule': 5})
    # 6: RESIDENTIAL_CITY postal last resort -> city.
    fill('RESIDENTIAL_CITY', 'city', 6, _normalize_city_name)

    return resolved


def _place_slug(place: dict, county_slug: str) -> str:
    """Routing slug for a resolved place, matching the jurisdictional_groupings
    bundle filenames. Cities use the bare name slug (the frontend appends
    '_city', v2.js:262); villages and townships use the county-prefixed slug
    (e.g. 'montgomery_washington_township'). county_slug is the pipeline's
    county slug, identical to _slugify(county_name) for Ohio county names."""
    name_slug = _precinct_safe_name(place['name'])
    if place['type'] == 'city':
        return name_slug
    return f'{county_slug}_{name_slug}'


def _ward_parent_slugs(parent_type: str, parent_name: str, county_slug: str) -> tuple:
    """(route_slug, bundle_slug) for a ward's parent place. route_slug matches the
    precinct-index place_slug (bare for cities); bundle_slug is the ward-entity
    prefix and matches the data/{city,village,township} bundle filename -- cities
    carry the '_city' suffix the frontend appends (v2.js:262), villages/townships
    use the county-prefixed slug verbatim."""
    name_slug = _precinct_safe_name(parent_name)
    if parent_type == 'city':
        return name_slug, f'{name_slug}_city'
    return f'{county_slug}_{name_slug}', f'{county_slug}_{name_slug}'


def _ward_map_per_county(df: pl.DataFrame, county_slug: str) -> dict:
    """Map each non-blank WARD value in one county's df to its canonical ward
    entity descriptor. The single ward resolver (CLAUDE.md 5): the precinct-index
    ward stamp (export_precinct_charts) and the ward entity builder
    (jurisdictional_groupings.build_ward_entities) both derive their slugs here,
    so a precinct's stamped ward_slug can never point to a non-existent entity.

    Parent municipality = the DOMINANT resolved place among the ward's voter rows,
    via _place_per_precinct -- NEVER parsed from the ward string (which breaks on
    'CINTI WARD 1', 'HUBER HTS ...', and the un-prefixed 'FIRST WARD'). Because the
    parent is a real resolved place, the five cross-county 'FIRST WARD' collisions
    split by their distinct parent cities while a city-ward spanning counties
    (ALLIANCE WARD 2 in Mahoning + Stark) yields one slug in both.

    Returns { ward_value: {
        'ward_name', 'parent_type', 'parent_name', 'parent_place_slug',
        'slug', 'abuse' } }.

    'abuse' flags a WARD value that is itself a township name resolving to a
    township parent (Lucas stuffs 'WASHINGTON TOWNSHIP' etc. into WARD); callers
    exclude abuse entries -- they are the township restated, not a ward.
    """
    cols = df.columns
    if 'WARD' not in cols or 'PRECINCT_NAME' not in cols:
        return {}

    place = _place_per_precinct(df)  # precinct -> {type, name}

    ward_rows = (
        df.select([
            pl.col('PRECINCT_NAME'),
            # Some counties enter WARD with irregular internal spacing (e.g.
            # Erie's 'VERMILION  WARD 1', Licking's 'CITY OF NEWARK  WD2') --
            # collapse runs of whitespace so the raw value is stable both as a
            # ward_name display string and as this function's grouping key.
            pl.col('WARD').str.strip_chars().str.replace_all(r'\s+', ' ').alias('_ward'),
        ])
        .filter(pl.col('_ward') != '')
    )
    if ward_rows.height == 0:
        return {}

    prec_names = ward_rows['PRECINCT_NAME'].unique().to_list()
    pmap = pl.DataFrame({
        'PRECINCT_NAME': prec_names,
        '_ptype': [place[p]['type'] if place.get(p) else None for p in prec_names],
        '_pname': [place[p]['name'] if place.get(p) else None for p in prec_names],
    })
    counts = (
        ward_rows.join(pmap, on='PRECINCT_NAME', how='left')
                 .filter(pl.col('_ptype').is_not_null())
                 # deterministic dominant: rows desc, then name/type asc for ties
                 .group_by(['_ward', '_ptype', '_pname'])
                 .agg(pl.len().alias('n'))
                 .sort(['_ward', 'n', '_pname', '_ptype'],
                       descending=[False, True, False, False])
    )

    out: dict = {}
    for row in counts.group_by('_ward', maintain_order=True).first().iter_rows(named=True):
        ward_value  = row['_ward']
        parent_type = row['_ptype']
        parent_name = row['_pname']
        route_slug, bundle_slug = _ward_parent_slugs(parent_type, parent_name, county_slug)
        slug = f'{bundle_slug}_{_precinct_safe_name(ward_value)}'
        # Not a real ward (townships have no wards), excluded in two shapes:
        #  * the WARD value is itself a township name -- guard on the value,
        #    since Lucas's 'WASHINGTON TOWNSHIP' resolves to a BOGUS city via
        #    the WARD-prefix fallback (rule 4), which a parent test would miss;
        #  * the dominant resolved parent is a township -- Stark stuffs the
        #    township name 'CANTON' into WARD (distinct from Canton CITY's real
        #    CANTON WARD 1-9), and stray city-ward voters in a township-tokened
        #    precinct (2 'BELLEVUE WARD 4' voters in GROTON TWP) resolve there.
        abuse = bool(_TOWNSHIP_NAME_RE.search(ward_value.upper())) or parent_type == 'township'
        out[ward_value] = {
            'ward_name':         ward_value,
            'parent_type':       parent_type,
            'parent_name':       parent_name,
            'parent_place_slug': route_slug,
            'slug':              slug,
            'abuse':             abuse,
        }
    return out


def _dominant_city_per_precinct(df: pl.DataFrame) -> dict:
    """Thin city-only derivation of _place_per_precinct (CLAUDE.md 5): keeps
    only type=='city' places, returning PRECINCT_NAME -> normalized city name.
    Townships AND villages resolve to their own place types and are therefore
    ABSENT here; callers (build_city_summary, _build_city_county_map) treat an
    absent precinct as 'not a city', so villages no longer leak into the city
    layer. All resolution semantics live in _place_per_precinct."""
    return {p: place['name']
            for p, place in _place_per_precinct(df).items()
            if place['type'] == 'city'}


def export_precinct_charts(
    county_name:    str,
    slug:           str,
    df:             pl.DataFrame,
    election_cols:  list[str],
    unc_classified: 'pl.DataFrame | None',
    logger:         logging.Logger,
):
    """
    For every unique PRECINCT_NAME in df, write per-precinct party doughnut
    and UNC shadow stacked-bar JSON files, then write a precinct index JSON.

    Output files (example slug = 'montgomery', precinct 'KETTERING 1-A'):
      montgomery_precinct_kettering_1_a_party.json
      montgomery_precinct_kettering_1_a_unc.json
      montgomery_precinct_index.json
    """
    today = date_t.today().isoformat()

    if 'PRECINCT_NAME' not in df.columns:
        logger.info('  Precinct charts: no PRECINCT_NAME column — skipping %s', county_name)
        return

    precinct_names = sorted(df['PRECINCT_NAME'].drop_nulls().unique().to_list())
    if not precinct_names:
        logger.info('  Precinct charts: no precincts found — skipping %s', county_name)
        return

    # Build a lookup: SOS_VOTERID → unc_class (for fast per-precinct filtering)
    unc_map: dict = {}
    if unc_classified is not None and not unc_classified.is_empty():
        unc_map = dict(zip(
            unc_classified['SOS_VOTERID'].to_list(),
            unc_classified['unc_class'].to_list(),
        ))

    index_entries = []

    # Per-precinct authoritative place (city/village/township) via the single
    # place resolver. Drives the dashboard's geo tree grouping, the city
    # summary, and the cross-county city_county_map. Emitting it here makes a
    # full rerun self-sufficient — no post-hoc index patch.
    place_by_precinct = _place_per_precinct(df)
    # Per-precinct dominant WARD + the county's canonical ward map (single ward
    # resolver). ward_slug is the precinct's dominant ward's canonical entity
    # slug, so the geo tree nests each precinct under the same ward entity that
    # jurisdictional_groupings.build_ward_entities emits.
    ward_map = _ward_map_per_county(df, slug)
    dominant_ward = _dominant_per_precinct(df, 'WARD') if 'WARD' in df.columns else {}
    dominant_local_school = _dominant_per_precinct(df, 'LOCAL_SCHOOL_DISTRICT') if 'LOCAL_SCHOOL_DISTRICT' in df.columns else {}
    dominant_ev_school = _dominant_per_precinct(df, 'EXEMPTED_VILL_SCHOOL_DISTRICT') if 'EXEMPTED_VILL_SCHOOL_DISTRICT' in df.columns else {}
    dominant_city_school = _dominant_per_precinct(df, 'CITY_SCHOOL_DISTRICT') if 'CITY_SCHOOL_DISTRICT' in df.columns else {}
    dominant_muni_court = _dominant_per_precinct(df, 'MUNICIPAL_COURT_DISTRICT') if 'MUNICIPAL_COURT_DISTRICT' in df.columns else {}

    for precinct_name in precinct_names:
        safe_name = _precinct_safe_name(precinct_name)
        pdf = df.filter(pl.col('PRECINCT_NAME') == precinct_name)
        total = len(pdf)

        # ── Party doughnut ────────────────────────────────────────────────────
        # 9-cohort partisan-spectrum layout — mirrors export_json() exactly so
        # precinct, county, ward, and district charts are visually identical.
        p_unc_ids = set(pdf['SOS_VOTERID'].to_list()) if 'SOS_VOTERID' in pdf.columns else set()
        if 'cohort_family' in pdf.columns:
            cmap = dict(zip(*pdf.group_by('cohort_family')
                              .agg(pl.len().alias('n'))
                              .select(['cohort_family', 'n'])
                              .to_dict(as_series=False)
                              .values()))
            pa_labels = [lbl   for _, lbl, _ in COHORT_SLICES]
            pa_colors = [color for _, _, color in COHORT_SLICES]
            pa_counts = [int(cmap.get(fam, 0)) for fam, _, _ in COHORT_SLICES]
        else:
            # Legacy fallback: PARTY_LABEL doughnut + optional unc_classified split
            party_df = (
                pdf.group_by('PARTY_LABEL')
                   .agg(pl.len().alias('count'))
                   .sort('count', descending=True)
            )
            p_unc_rows = [(vid, cls) for vid, cls in unc_map.items() if vid in p_unc_ids]
            if p_unc_rows and unc_classified is not None:
                p_unc_df = unc_classified.filter(pl.col('SOS_VOTERID').is_in(list(p_unc_ids)))
                sc_counts = p_unc_df.group_by('unc_class').agg(pl.len().alias('n'))
                sc = dict(zip(sc_counts['unc_class'].to_list(), sc_counts['n'].to_list()))
                non_unc_map = {row['PARTY_LABEL']: row['count']
                               for row in party_df.iter_rows(named=True)
                               if row['PARTY_LABEL'] != 'UNC'}
                pa_labels, pa_colors, pa_counts = [], [], []
                for reg_party, unc_cls, unc_label, unc_color in [
                    ('REP', 'LIFETIME_R', 'UNC – Lifetime R', UNC_SHADOW_COLORS['LIFETIME_R']),
                    ('DEM', 'LIFETIME_D', 'UNC – Lifetime D', UNC_SHADOW_COLORS['LIFETIME_D']),
                ]:
                    if reg_party in non_unc_map:
                        pa_labels.append(reg_party)
                        pa_colors.append(CHART_COLORS.get(reg_party, '#6366f1'))
                        pa_counts.append(non_unc_map[reg_party])
                    pa_labels.append(unc_label)
                    pa_colors.append(unc_color)
                    pa_counts.append(sc.get(unc_cls, 0))
                pa_labels += ['UNC – Mixed', 'UNC – No History']
                pa_colors += [UNC_SHADOW_COLORS['MIXED'], UNC_SHADOW_COLORS['NO_HISTORY']]
                pa_counts += [sc.get('MIXED', 0), sc.get('NO_HISTORY', 0)]
                for other_party, other_count in non_unc_map.items():
                    if other_party not in ('REP', 'DEM'):
                        pa_labels.append(other_party)
                        pa_colors.append(CHART_COLORS.get(other_party, '#6366f1'))
                        pa_counts.append(other_count)
            else:
                pa_labels = party_df['PARTY_LABEL'].to_list()
                pa_colors = [CHART_COLORS.get(p, '#6366f1') for p in pa_labels]
                pa_counts = party_df['count'].to_list()

        _dump_json({
            'title':     f'Party Affiliation — {precinct_name}',
            'county':    county_name,
            'precinct':  precinct_name,
            'geography': 'precinct-detail',
            'type':      'doughnut',
            'updated':   today,
            'chartConfig': {
                'labels': pa_labels,
                'datasets': [{
                    'data':            pa_counts,
                    'backgroundColor': pa_colors,
                    'borderWidth':     2,
                    'borderColor':     'transparent',
                }],
            },
        }, DATA_DIR / f'{slug}_precinct_{safe_name}_party.json', logger)

        # ── UNC shadow stacked bar ────────────────────────────────────────────
        if unc_classified is not None and not unc_classified.is_empty() and p_unc_ids:
            p_unc_df2 = unc_classified.filter(pl.col('SOS_VOTERID').is_in(list(p_unc_ids)))
            if not p_unc_df2.is_empty():
                sc2_counts = p_unc_df2.group_by('unc_class').agg(pl.len().alias('n'))
                sc2 = dict(zip(sc2_counts['unc_class'].to_list(), sc2_counts['n'].to_list()))
                total_unc2 = sum(sc2.values())
                order = ['LIFETIME_D', 'LIFETIME_R', 'MIXED', 'NO_HISTORY']
                unc_datasets = []
                for cls in order:
                    n = sc2.get(cls, 0)
                    unc_datasets.append({
                        'label':           UNC_SHADOW_LABELS[cls],
                        'data':            [n],
                        'backgroundColor': UNC_SHADOW_COLORS[cls],
                        'borderRadius':    4,
                    })
                _dump_json({
                    'title':     f'UNC Primary History — {precinct_name}',
                    'county':    county_name,
                    'precinct':  precinct_name,
                    'geography': 'precinct-detail',
                    'type':      'bar',
                    'updated':   today,
                    'note':      UNC_SHADOW_NOTE,
                    'totalUnc':  total_unc2,
                    'chartOptions': {
                        'scales': {
                            'x': {'stacked': True},
                            'y': {'stacked': True},
                        },
                        'plugins': {'tooltip': {'callbacks': {}}},
                    },
                    'chartConfig': {
                        'labels':   ['Unaffiliated (UNC)'],
                        'datasets': unc_datasets,
                    },
                }, DATA_DIR / f'{slug}_precinct_{safe_name}_unc.json', logger)

        # ── Decade distribution (bar) ─────────────────────────────────────────
        decade_url = None
        if 'Decade' in pdf.columns:
            p_decade_df = (
                pdf.filter(pl.col('Decade').is_not_null())
                   .group_by('Decade')
                   .agg(pl.len().alias('count'))
                   .sort('Decade')
            )
            if not p_decade_df.is_empty():
                _dump_json({
                    'title':     f'Voter Age Distribution by Birth Decade — {precinct_name}',
                    'county':    county_name,
                    'precinct':  precinct_name,
                    'geography': 'precinct-detail',
                    'type':      'bar',
                    'updated':   today,
                    'chartConfig': {
                        'labels': [f'{d}s' for d in p_decade_df['Decade'].to_list()],
                        'datasets': [{
                            'label':           'Registered Voters',
                            'data':            p_decade_df['count'].to_list(),
                            'backgroundColor': CHART_COLORS['bar'],
                            'borderRadius':    4,
                        }],
                    },
                }, DATA_DIR / f'{slug}_precinct_{safe_name}_decade.json', logger)
                decade_url = f'data/{slug}_precinct_{safe_name}_decade.json'

        # ── Generation distribution (bar) ─────────────────────────────────────
        generation_url = None
        if 'Generation' in pdf.columns:
            p_gen_summary = build_generation_summary(pdf)
            if not p_gen_summary.is_empty():
                _dump_json({
                    'title':     f'Voter Age Distribution by Generation — {precinct_name}',
                    'county':    county_name,
                    'precinct':  precinct_name,
                    'geography': 'precinct-detail',
                    'type':      'bar',
                    'updated':   today,
                    'note':      'Pew Research Center generational boundaries',
                    'chartConfig': {
                        'labels': p_gen_summary['Generation'].to_list(),
                        'datasets': [{
                            'label':           'Registered Voters',
                            'data':            p_gen_summary['Voter Count'].to_list(),
                            'backgroundColor': CHART_COLORS['bar'],
                            'borderRadius':    4,
                        }],
                    },
                }, DATA_DIR / f'{slug}_precinct_{safe_name}_generation.json', logger)
                generation_url = f'data/{slug}_precinct_{safe_name}_generation.json'

        # ── Party × Decade (stacked bar) ──────────────────────────────────────
        # Pivot on cohort_family so labels/colors match the doughnut. Falls back
        # to PARTY_LABEL pivot only if cohort_family is missing on pdf.
        party_decade_url = None
        if 'Decade' in pdf.columns and 'cohort_family' in pdf.columns:
            p_cross = (
                pdf.filter(pl.col('Decade').is_not_null())
                   .group_by(['Decade', 'cohort_family'])
                   .agg(pl.len().alias('n'))
                   .pivot(on='cohort_family', index='Decade',
                          values='n', aggregate_function='sum')
                   .sort('Decade')
            )
            if not p_cross.is_empty():
                p_decade_labels = [f'{d}s' for d in p_cross['Decade'].to_list()]
                p_decade_vals   = p_cross['Decade'].to_list()
                p_datasets = []
                for fam, lbl, color in COHORT_SLICES:
                    if fam in p_cross.columns:
                        vals = [int(v or 0) for v in p_cross[fam].to_list()]
                    else:
                        vals = [0] * len(p_decade_vals)
                    p_datasets.append({
                        'label':           lbl,
                        'data':            vals,
                        'backgroundColor': color,
                        'borderRadius':    3,
                        'stack':           COHORT_STACK_MAP[fam],
                    })
                _dump_json({
                    'title':     f'Party Affiliation by Birth Decade — {precinct_name}',
                    'county':    county_name,
                    'precinct':  precinct_name,
                    'geography': 'precinct-detail',
                    'type':      'bar',
                    'updated':   today,
                    'chartConfig': {
                        'labels':   p_decade_labels,
                        'datasets': p_datasets,
                    },
                }, DATA_DIR / f'{slug}_precinct_{safe_name}_party_by_decade.json', logger)
                party_decade_url = f'data/{slug}_precinct_{safe_name}_party_by_decade.json'
        elif 'Decade' in pdf.columns and 'PARTY_LABEL' in pdf.columns:
            # Legacy fallback path — original PARTY_LABEL + unc_classified shape.
            p_cross = (
                pdf.filter(pl.col('Decade').is_not_null())
                   .group_by(['Decade', 'PARTY_LABEL'])
                   .agg(pl.len().alias('count'))
                   .pivot(on='PARTY_LABEL', index='Decade',
                          values='count', aggregate_function='sum')
                   .sort('Decade')
            )
            if not p_cross.is_empty():
                p_decade_labels = [f'{d}s' for d in p_cross['Decade'].to_list()]
                p_datasets = []
                for party, stack_id in [('REP', 'rep'), ('DEM', 'dem'), ('Other', 'other')]:
                    if party not in p_cross.columns:
                        continue
                    p_datasets.append({
                        'label':           party,
                        'data':            [v or 0 for v in p_cross[party].to_list()],
                        'backgroundColor': CHART_COLORS.get(party, '#6366f1'),
                        'borderRadius':    3,
                        'stack':           stack_id,
                    })
                if (unc_classified is not None
                        and not unc_classified.is_empty()
                        and p_unc_ids):
                    p_unc_decade = (
                        unc_classified
                        .filter(pl.col('SOS_VOTERID').is_in(list(p_unc_ids)))
                        .join(pdf.select(['SOS_VOTERID', 'Decade']),
                              on='SOS_VOTERID', how='left')
                        .filter(pl.col('Decade').is_not_null())
                        .group_by(['Decade', 'unc_class'])
                        .agg(pl.len().alias('n'))
                        .pivot(on='unc_class', index='Decade',
                               values='n', aggregate_function='sum')
                        .sort('Decade')
                    )
                    p_decade_vals = p_cross['Decade'].to_list()
                    for cls, label, color in [
                        ('LIFETIME_D', 'UNC – Lifetime D', UNC_SHADOW_COLORS['LIFETIME_D']),
                        ('LIFETIME_R', 'UNC – Lifetime R', UNC_SHADOW_COLORS['LIFETIME_R']),
                        ('MIXED',      'UNC – Mixed',       UNC_SHADOW_COLORS['MIXED']),
                        ('NO_HISTORY', 'UNC – No History',  UNC_SHADOW_COLORS['NO_HISTORY']),
                    ]:
                        if cls in p_unc_decade.columns:
                            dmap = dict(zip(p_unc_decade['Decade'].to_list(),
                                            p_unc_decade[cls].to_list()))
                            vals = [int(dmap.get(d, 0) or 0) for d in p_decade_vals]
                        else:
                            vals = [0] * len(p_decade_vals)
                        p_datasets.append({
                            'label':           label,
                            'data':            vals,
                            'backgroundColor': color,
                            'borderRadius':    3,
                            'stack':           'unc',
                        })
                else:
                    if 'UNC' in p_cross.columns:
                        p_datasets.append({
                            'label':           'UNC',
                            'data':            [v or 0 for v in p_cross['UNC'].to_list()],
                            'backgroundColor': CHART_COLORS['UNC'],
                            'borderRadius':    3,
                            'stack':           'unc',
                        })
                _dump_json({
                    'title':     f'Party Affiliation by Birth Decade — {precinct_name}',
                    'county':    county_name,
                    'precinct':  precinct_name,
                    'geography': 'precinct-detail',
                    'type':      'bar',
                    'updated':   today,
                    'chartConfig': {
                        'labels':   p_decade_labels,
                        'datasets': p_datasets,
                    },
                }, DATA_DIR / f'{slug}_precinct_{safe_name}_party_by_decade.json', logger)
                party_decade_url = f'data/{slug}_precinct_{safe_name}_party_by_decade.json'

        # ── Party × Generation (stacked bar) ──────────────────────────────────
        # Pivot on cohort_family so labels/colors match the doughnut. Falls back
        # to PARTY_LABEL pivot only if cohort_family is missing on pdf.
        party_generation_url = None
        if 'Generation' in pdf.columns and 'cohort_family' in pdf.columns:
            p_gen_cross = (
                pdf.filter(pl.col('Generation').is_not_null())
                   .group_by(['Generation', 'cohort_family'])
                   .agg(pl.len().alias('n'))
                   .pivot(on='cohort_family', index='Generation',
                          values='n', aggregate_function='sum')
            )
            if not p_gen_cross.is_empty():
                p_order_df = pl.DataFrame({'Generation': _GEN_ORDER,
                                           '_sort': list(range(len(_GEN_ORDER)))})
                p_gen_cross = (
                    p_gen_cross.join(p_order_df, on='Generation', how='left')
                               .sort('_sort')
                               .drop('_sort')
                )
                p_gen_labels   = p_gen_cross['Generation'].to_list()
                p_gen_datasets = []
                for fam, lbl, color in COHORT_SLICES:
                    if fam in p_gen_cross.columns:
                        vals = [int(v or 0) for v in p_gen_cross[fam].fill_null(0).to_list()]
                    else:
                        vals = [0] * len(p_gen_labels)
                    p_gen_datasets.append({
                        'label':           lbl,
                        'data':            vals,
                        'backgroundColor': color,
                        'borderRadius':    2,
                        'stack':           COHORT_STACK_MAP[fam],
                    })
                _dump_json({
                    'title':     f'Party Affiliation by Generation — {precinct_name}',
                    'county':    county_name,
                    'precinct':  precinct_name,
                    'geography': 'precinct-detail',
                    'type':      'bar',
                    'updated':   today,
                    'note':      'Pew Research Center generational boundaries',
                    'chartConfig': {
                        'labels':   p_gen_labels,
                        'datasets': p_gen_datasets,
                    },
                }, DATA_DIR / f'{slug}_precinct_{safe_name}_party_by_generation.json', logger)
                party_generation_url = f'data/{slug}_precinct_{safe_name}_party_by_generation.json'
        elif 'Generation' in pdf.columns and 'PARTY_LABEL' in pdf.columns:
            # Legacy fallback path — original PARTY_LABEL + unc_classified shape.
            p_gen_cross = (
                pdf.group_by(['Generation', 'PARTY_LABEL'])
                   .agg(pl.len().alias('count'))
                   .pivot(on='PARTY_LABEL', index='Generation',
                          values='count', aggregate_function='sum')
            )
            if not p_gen_cross.is_empty():
                p_order_df = pl.DataFrame({'Generation': _GEN_ORDER,
                                           '_sort': list(range(len(_GEN_ORDER)))})
                p_gen_cross = (
                    p_gen_cross.join(p_order_df, on='Generation', how='left')
                               .sort('_sort')
                               .drop('_sort')
                )
                p_gen_labels   = p_gen_cross['Generation'].to_list()
                p_gen_datasets = []
                for party, stack_id in [('REP', 'rep'), ('DEM', 'dem'), ('Other', 'other')]:
                    color = CHART_COLORS.get(party, '#888888')
                    p_gen_datasets.append({
                        'label':           party,
                        'data':            (p_gen_cross[party].fill_null(0).to_list()
                                            if party in p_gen_cross.columns else
                                            [0] * len(p_gen_labels)),
                        'backgroundColor': color,
                        'borderRadius':    2,
                        'stack':           stack_id,
                    })
                if (unc_classified is not None
                        and not unc_classified.is_empty()
                        and p_unc_ids):
                    p_unc_gen = (
                        unc_classified
                        .filter(pl.col('SOS_VOTERID').is_in(list(p_unc_ids)))
                        .join(pdf.select(['SOS_VOTERID', 'Generation']),
                              on='SOS_VOTERID', how='left')
                        .group_by(['Generation', 'unc_class'])
                        .agg(pl.len().alias('n'))
                        .pivot(on='unc_class', index='Generation',
                               values='n', aggregate_function='sum')
                    )
                    p_sort_df = pl.DataFrame({'Generation': _GEN_ORDER,
                                              '_sort': list(range(len(_GEN_ORDER)))})
                    p_unc_gen = (
                        p_unc_gen.join(p_sort_df, on='Generation', how='left')
                                 .sort('_sort')
                                 .drop('_sort')
                    )
                    for cls, label, color in [
                        ('LIFETIME_D', 'UNC – Lifetime D', UNC_SHADOW_COLORS['LIFETIME_D']),
                        ('LIFETIME_R', 'UNC – Lifetime R', UNC_SHADOW_COLORS['LIFETIME_R']),
                        ('MIXED',      'UNC – Mixed',       UNC_SHADOW_COLORS['MIXED']),
                        ('NO_HISTORY', 'UNC – No History',  UNC_SHADOW_COLORS['NO_HISTORY']),
                    ]:
                        if cls in p_unc_gen.columns:
                            gmap = dict(zip(p_unc_gen['Generation'].to_list(),
                                            p_unc_gen[cls].to_list()))
                            vals = [int(gmap.get(g, 0) or 0) for g in p_gen_labels]
                        else:
                            vals = [0] * len(p_gen_labels)
                        p_gen_datasets.append({
                            'label':           label,
                            'data':            vals,
                            'backgroundColor': color,
                            'borderRadius':    2,
                            'stack':           'unc',
                        })
                else:
                    color = CHART_COLORS.get('UNC', '#888888')
                    p_gen_datasets.append({
                        'label':           'UNC',
                        'data':            (p_gen_cross['UNC'].fill_null(0).to_list()
                                            if 'UNC' in p_gen_cross.columns else
                                            [0] * len(p_gen_labels)),
                        'backgroundColor': color,
                        'borderRadius':    2,
                        'stack':           'unc',
                    })
                _dump_json({
                    'title':     f'Party Affiliation by Generation — {precinct_name}',
                    'county':    county_name,
                    'precinct':  precinct_name,
                    'geography': 'precinct-detail',
                    'type':      'bar',
                    'updated':   today,
                    'note':      'Pew Research Center generational boundaries',
                    'chartConfig': {
                        'labels':   p_gen_labels,
                        'datasets': p_gen_datasets,
                    },
                }, DATA_DIR / f'{slug}_precinct_{safe_name}_party_by_generation.json', logger)
                party_generation_url = f'data/{slug}_precinct_{safe_name}_party_by_generation.json'

        place = place_by_precinct.get(precinct_name)
        place_city = place['name'] if place and place['type'] == 'city' else None
        _dw = dominant_ward.get(precinct_name)
        _wdesc = ward_map.get(_dw) if _dw else None
        _ward_slug = _wdesc['slug'] if _wdesc and not _wdesc['abuse'] else None
        index_entries.append({
            'name':              precinct_name,
            'safe_name':         safe_name,
            'city':              place_city,
            'place_type':        place['type'] if place else None,
            'place_name':        place['name'] if place else None,
            'place_slug':        _place_slug(place, slug) if place else None,
            'ward_slug':         _ward_slug,
            'ward_name':         _dw if _ward_slug else None,
            'local_school_district':         dominant_local_school.get(precinct_name),
            'exempted_vill_school_district': dominant_ev_school.get(precinct_name),
            'city_school_district':          dominant_city_school.get(precinct_name),
            'municipal_court_district':      dominant_muni_court.get(precinct_name),
            'precinct_code':     (pdf['PRECINCT_CODE'][0]
                                  if 'PRECINCT_CODE' in pdf.columns and total
                                  else None),
            'total':             total,
            'partyUrl':          f'data/{slug}_precinct_{safe_name}_party.json',
            'uncUrl':            f'data/{slug}_precinct_{safe_name}_unc.json',
            'decadeUrl':         decade_url,
            'generationUrl':     generation_url,
            'partyDecadeUrl':    party_decade_url,
            'partyGenerationUrl': party_generation_url,
        })

    _dump_json({
        'county':    county_name,
        'precincts': index_entries,
    }, DATA_DIR / f'{slug}_precinct_index.json', logger)

    _prune_orphan_precinct_files(slug, [e['safe_name'] for e in index_entries], logger)

    logger.info(
        '  Precinct charts: wrote %d precincts for %s', len(index_entries), county_name
    )


# Per-precinct file kinds: {county_slug}_precinct_{safe_name}_{kind}.json.
# 'narrative' is written later by tools/narrative/generate_narratives.py.
PRECINCT_FILE_KINDS = ('party', 'unc', 'decade', 'generation', 'party_by_decade',
                       'party_by_generation', 'narrative')


def _prune_orphan_precinct_files(slug: str, safe_names: list, logger: logging.Logger) -> None:
    """
    Delete this county's per-precinct files whose precinct is not in the index
    just written. Precincts are renamed and merged between snapshots, and this
    export only ever wrote files, so a vanished precinct's files stayed
    published at their old vintage (2026-11-12: 8 precincts in 4 counties,
    56 files, last written 2026-07-25). Matching against the exact names the
    index implies avoids parsing safe names, which contain underscores.
    The files are tracked in git, so a wrong prune is recoverable.
    """
    if not safe_names:
        raise ValueError(f'{slug}: precinct index is empty; refusing to prune its precinct files')
    expected = {f'{slug}_precinct_{s}_{k}.json' for s in safe_names for k in PRECINCT_FILE_KINDS}
    expected |= {f'{slug}_precinct_index.json', f'{slug}_precinct_summary.json'}
    orphans = sorted(p for p in DATA_DIR.glob(f'{slug}_precinct_*.json')
                     if p.name not in expected)
    for p in orphans:
        p.unlink()
    if orphans:
        logger.info('  Precinct charts: removed %d file(s) of precincts no longer in the %s index: %s',
                    len(orphans), slug, [p.name for p in orphans])


def _update_manifest(
    county_name: str,
    slug:        str,
    today:       str,
    logger:      logging.Logger,
):
    """
    Update docs/manifest.json to include the newly analysed county.

    The manifest is the index that charts.js uses to populate the county
    dropdown and build the section list.  We add the county to the counties
    list AND processedCounties (which the dropdown actually reads), then
    upsert this county's section entries.  Existing entries for other
    counties are preserved unchanged.

    For statewide runs, prefer _write_manifest_bulk() — calling this in a
    per-county loop is O(N²) in disk I/O. This function exists for
    single-county runs and as a fallback.
    """
    manifest_path = DOCS_DIR / 'manifest.json'
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
    else:
        # Bootstrap a fresh manifest if none exists
        manifest = {
            'title':             'Ohio Voter Registration Analysis',
            'description':       'Voter registration analysis for {county}.',
            'dataNote':          '',
            'counties':          [],
            'processedCounties': [],
            'sections':          [],
        }

    # Always overwrite dataNote with a real-data message so the "Sample data"
    # banner in the web dashboard is cleared after the first analysis run.
    import snapshot_store
    staged = snapshot_store.staged_info() or {}
    iso_date = staged.get('snapshot_date', today)

    manifest['dataNote'] = (
        f'Data sourced from Ohio Secretary of State statewide voter file. '
        f'Last updated {today}.'
    )
    manifest['provenanceSnapshotDate'] = iso_date

    # Add county to both lists. `counties` is legacy (kept for back-compat with
    # any older code paths). `processedCounties` is what charts.js actually reads
    # to populate the county dropdown — it MUST be kept in sync.
    if county_name not in manifest.get('counties', []):
        manifest.setdefault('counties', []).append(county_name)
    if county_name not in manifest.get('processedCounties', []):
        manifest.setdefault('processedCounties', []).append(county_name)
        manifest['processedCounties'].sort()

    # Define the four sections this county contributes.
    # IDs are namespaced by slug to avoid collisions between counties.
    new_sections = [
        {
            'id':          f'{slug}-decade-distribution',
            'title':       'Voter Age Distribution by Birth Decade',
            'navLabel':    'Age Cohorts',
            'description': 'Count of registered voters grouped by birth decade.',
            'county':      county_name,
            'geography':   'county',
            'dataUrl':     f'data/{slug}_decade_distribution.json',
        },
        {
            'id':          f'{slug}-party-affiliation',
            'title':       'Party Affiliation Breakdown',
            'navLabel':    'Party',
            'description': 'Distribution of voters by party affiliation.',
            'county':      county_name,
            'geography':   'county',
            'dataUrl':     f'data/{slug}_party_affiliation.json',
        },
        {
            'id':          f'{slug}-party-by-decade',
            'title':       'Party Affiliation by Birth Decade',
            'navLabel':    'Party × Age',
            'description': 'Party affiliation within each birth decade cohort.',
            'county':      county_name,
            'geography':   'county',
            'dataUrl':     f'data/{slug}_party_by_decade.json',
        },
        {
            'id':          f'{slug}-generation-distribution',
            'title':       'Voter Age Distribution by Generation',
            'navLabel':    'Generations',
            'description': 'Count of registered voters grouped by generational cohort (Pew Research Center).',
            'county':      county_name,
            'geography':   'county',
            'dataUrl':     f'data/{slug}_generation_distribution.json',
        },
        {
            'id':          f'{slug}-party-by-generation',
            'title':       'Party Affiliation by Generation',
            'navLabel':    'Party × Generation',
            'description': 'Party affiliation within each generational cohort.',
            'county':      county_name,
            'geography':   'county',
            'dataUrl':     f'data/{slug}_party_by_generation.json',
        },
        {
            'id':          f'{slug}-precinct-summary',
            'title':       'Registration by Precinct',
            'navLabel':    'Precincts',
            'description': 'Precinct-level active and confirmation voter counts.',
            'county':      county_name,
            'geography':   'precinct',
            'dataUrl':     f'data/{slug}_precinct_summary.json',
        },
        {
            'id':          f'{slug}-city-summary',
            'title':       'Registration by City / Township',
            'navLabel':    'Cities',
            'description': 'City and township level totals aggregated from precinct names. Cross-county cities appear as separate rows until Phase 3 statewide merge.',
            'county':      county_name,
            'geography':   'city',
            'dataUrl':     f'data/{slug}_city_summary.json',
        },
        {
            'id':          f'{slug}-unc-shadow',
            'title':       'UNC Voter Primary History',
            'navLabel':    'UNC Shadow',
            'description': (
                'Unaffiliated voters segmented by lifetime primary ballot pattern. '
                'Lifetime D/R = voted exclusively in one party\'s primaries. '
                'Mixed = crossed party lines at least once. '
                'Shadow partisanship is inferred from behavior, not registration status.'
            ),
            'county':      county_name,
            'geography':   'county',
            'dataUrl':     f'data/{slug}_unc_shadow.json',
        },
        {
            'id':          f'{slug}-precinct-index',
            'title':       'Precinct Detail Index',
            'navLabel':    'Precinct Charts',
            'description': 'Per-precinct party and UNC shadow charts.',
            'county':      county_name,
            'geography':   'precinct-index',
            'dataUrl':     f'data/{slug}_precinct_index.json',
        },
    ]

    # Replace any existing sections for this county — identified by slug prefix in
    # the id OR by dataUrl pointing to this slug's data files.  The dataUrl check
    # removes legacy entries that pre-date slug-prefixed IDs so they can't
    # re-accumulate on subsequent runs.
    def _is_stale(s):
        sid  = s.get('id', '')
        url  = s.get('dataUrl', '')
        return sid.startswith(slug + '-') or (f'/{slug}_' in url) or (f'data/{slug}_' in url)

    existing_sections = [s for s in manifest.get('sections', []) if not _is_stale(s)]
    manifest['sections'] = existing_sections + new_sections

    manifest_path.write_text(json.dumps(manifest, indent=2), encoding='utf-8')
    logger.info(
        'manifest.json updated — %d counties, %d processed',
        len(manifest.get('counties', [])),
        len(manifest.get('processedCounties', [])),
    )


def _write_manifest_bulk(
    processed_counties: list[str],
    today:              str,
    logger:             logging.Logger,
):
    """
    Write docs/manifest.json once for an entire statewide export run.

    Replaces the prior per-county loop that re-read and re-wrote the manifest
    88 times (O(N²) disk I/O, log spam, and historically prone to leaving
    `processedCounties` stale because the per-county writer only touched the
    legacy `counties` field).

    Both `counties` and `processedCounties` are set to the exact list of
    counties whose JSON files were successfully written this run. Section
    entries for those counties are upserted; entries for counties NOT in this
    run are preserved (so a single-county re-run does not wipe the rest).
    """
    manifest_path = DOCS_DIR / 'manifest.json'
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
    else:
        manifest = {
            'title':             'Ohio Voter Registration Analysis',
            'description':       'Voter registration analysis for {county}.',
            'dataNote':          '',
            'counties':          [],
            'processedCounties': [],
            'sections':          [],
        }

    import snapshot_store
    staged = snapshot_store.staged_info() or {}
    iso_date = staged.get('snapshot_date', today)

    manifest['dataNote'] = (
        f'Data sourced from Ohio Secretary of State statewide voter file. '
        f'Last updated {today}.'
    )
    manifest['provenanceSnapshotDate'] = iso_date

    # Merge: union the run's counties with anything already on disk, then sort.
    # This keeps prior runs' work intact if someone re-runs a partial set.
    existing_counties = set(manifest.get('counties', []))
    existing_processed = set(manifest.get('processedCounties', []))
    all_counties  = sorted(existing_counties  | set(processed_counties))
    all_processed = sorted(existing_processed | set(processed_counties))
    manifest['counties']          = all_counties
    manifest['processedCounties'] = all_processed

    # Rebuild section list: drop any section whose slug matches a county in
    # this run (so rerunning a county replaces its sections cleanly), then
    # append fresh sections for every county in this run.
    run_slugs = {c.lower().replace(' ', '_') for c in processed_counties}

    def _belongs_to_run(s):
        sid = s.get('id', '')
        url = s.get('dataUrl', '')
        for slug in run_slugs:
            if sid.startswith(slug + '-') or (f'/{slug}_' in url) or (f'data/{slug}_' in url):
                return True
        return False

    kept_sections = [s for s in manifest.get('sections', []) if not _belongs_to_run(s)]
    new_sections  = []
    for county_name in processed_counties:
        slug = county_name.lower().replace(' ', '_')
        new_sections.extend(_sections_for_county(county_name, slug))
    # A section whose data file is gone would send charts.js (v1.html) to a
    # 404, e.g. the statewide precinct summary, which is no longer written.
    sections = kept_sections + new_sections
    manifest['sections'] = [s for s in sections
                            if (DOCS_DIR / s.get('dataUrl', '')).is_file()]
    if len(manifest['sections']) != len(sections):
        logger.info('manifest.json: dropped %d section(s) whose data file is missing: %s',
                    len(sections) - len(manifest['sections']),
                    sorted(s.get('dataUrl') for s in sections if s not in manifest['sections']))

    manifest_path.write_text(json.dumps(manifest, indent=2), encoding='utf-8')
    logger.info(
        'manifest.json written — %d counties, %d processed, %d sections',
        len(manifest['counties']),
        len(manifest['processedCounties']),
        len(manifest['sections']),
    )


def _sections_for_county(county_name: str, slug: str) -> list[dict]:
    """Return the list of section descriptors charts.js needs for one county."""
    return [
        {
            'id':          f'{slug}-decade-distribution',
            'title':       'Voter Age Distribution by Birth Decade',
            'navLabel':    'Age Cohorts',
            'description': 'Count of registered voters grouped by birth decade.',
            'county':      county_name,
            'geography':   'county',
            'dataUrl':     f'data/{slug}_decade_distribution.json',
        },
        {
            'id':          f'{slug}-party-affiliation',
            'title':       'Party Affiliation Breakdown',
            'navLabel':    'Party',
            'description': 'Distribution of voters by party affiliation.',
            'county':      county_name,
            'geography':   'county',
            'dataUrl':     f'data/{slug}_party_affiliation.json',
        },
        {
            'id':          f'{slug}-party-by-decade',
            'title':       'Party Affiliation by Birth Decade',
            'navLabel':    'Party × Age',
            'description': 'Party affiliation within each birth decade cohort.',
            'county':      county_name,
            'geography':   'county',
            'dataUrl':     f'data/{slug}_party_by_decade.json',
        },
        {
            'id':          f'{slug}-generation-distribution',
            'title':       'Voter Age Distribution by Generation',
            'navLabel':    'Generations',
            'description': 'Count of registered voters grouped by generational cohort (Pew Research Center).',
            'county':      county_name,
            'geography':   'county',
            'dataUrl':     f'data/{slug}_generation_distribution.json',
        },
        {
            'id':          f'{slug}-party-by-generation',
            'title':       'Party Affiliation by Generation',
            'navLabel':    'Party × Generation',
            'description': 'Party affiliation within each generational cohort.',
            'county':      county_name,
            'geography':   'county',
            'dataUrl':     f'data/{slug}_party_by_generation.json',
        },
        {
            'id':          f'{slug}-precinct-summary',
            'title':       'Registration by Precinct',
            'navLabel':    'Precincts',
            'description': 'Precinct-level active and confirmation voter counts.',
            'county':      county_name,
            'geography':   'precinct',
            'dataUrl':     f'data/{slug}_precinct_summary.json',
        },
        {
            'id':          f'{slug}-city-summary',
            'title':       'Registration by City / Township',
            'navLabel':    'Cities',
            'description': 'City and township level totals aggregated from precinct names. Cross-county cities appear as separate rows until Phase 3 statewide merge.',
            'county':      county_name,
            'geography':   'city',
            'dataUrl':     f'data/{slug}_city_summary.json',
        },
        {
            'id':          f'{slug}-unc-shadow',
            'title':       'UNC Voter Primary History',
            'navLabel':    'UNC Shadow',
            'description': (
                'Unaffiliated voters segmented by lifetime primary ballot pattern. '
                'Lifetime D/R = voted exclusively in one party\'s primaries. '
                'Mixed = crossed party lines at least once. '
                'Shadow partisanship is inferred from behavior, not registration status.'
            ),
            'county':      county_name,
            'geography':   'county',
            'dataUrl':     f'data/{slug}_unc_shadow.json',
        },
        {
            'id':          f'{slug}-precinct-index',
            'title':       'Precinct Detail Index',
            'navLabel':    'Precinct Charts',
            'description': 'Per-precinct party and UNC shadow charts.',
            'county':      county_name,
            'geography':   'precinct-index',
            'dataUrl':     f'data/{slug}_precinct_index.json',
        },
    ]


# ─────────────────────────────────────────────────────────────────────────────
# Excel output  (Polars → pandas → xlsxwriter)
# ─────────────────────────────────────────────────────────────────────────────
# We convert only the small summary DataFrames to pandas here — the raw
# 7.9M-row frame never touches pandas.  The conversion cost is negligible
# because summaries are at most a few thousand rows.

def _hdr_fmt(wb, bg: str = _MED_BLUE):
    return wb.add_format({
        'bold': True, 'font_color': '#FFFFFF', 'bg_color': bg,
        'align': 'center', 'valign': 'vcenter', 'font_name': 'Arial', 'font_size': 11,
    })

def _title_fmt(wb):
    return wb.add_format({
        'bold': True, 'font_color': '#1F3864', 'bg_color': _TITLE_BG,
        'font_name': 'Arial', 'font_size': 12,
    })

def _write_section(writer, sheet, df_pd, start_row, title, hdr_fmt, t_fmt, col_widths=None):
    """
    Write a titled table block to an Excel sheet.  Accepts a pandas DataFrame
    (converted from Polars upstream).  Returns the next free row index.
    """
    if title:
        if sheet not in writer.sheets:
            df_pd.head(0).to_excel(writer, sheet_name=sheet, startrow=start_row + 1, index=False)
        writer.sheets[sheet].write(start_row, 0, title, t_fmt)

    df_pd.to_excel(writer, sheet_name=sheet, startrow=start_row + 1, index=False)
    ws = writer.sheets[sheet]
    for i, col in enumerate(df_pd.columns):
        ws.write(start_row + 1, i, str(col), hdr_fmt)

    if col_widths:
        items = col_widths.items() if isinstance(col_widths, dict) else col_widths
        for ci, w in items:
            ws.set_column(ci, ci, w)

    return start_row + 1 + 1 + len(df_pd) + 2   # title + header + rows + blank gap


def build_workbook(
    df:            pl.DataFrame,
    election_cols: list[str],
    output_path:   Path,
    county_name:   str,
    include_raw:   bool,
    logger:        logging.Logger,
):
    """
    Assemble the Excel workbook.

    include_raw=True   → county analysis: include a raw Voter Data sheet
                         (safe at ~300K rows; Excel's limit is ~1M rows)
    include_raw=False  → Ohio-wide analysis: include a County Summary sheet instead
                         (7.9M rows would exceed Excel's limit and take hours to write)
    """
    logger.info('Creating workbook: %s', output_path.name)

    with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
        wb = writer.book

        # ── Sheet 1: Raw data OR County Summary ───────────────────────────────
        if include_raw:
            logger.info('  Writing Voter Data sheet ...')
            # Exclude the 89 election columns from the raw sheet — they'd make it
            # unreadable (89 single-char columns after the demographic fields).
            # Election history is covered by the Participation summary sheet.
            display_cols = [c for c in df.columns if not ELEC_RE.match(c)]
            df_raw_pd    = df.select(display_cols).to_pandas()
            df_raw_pd.to_excel(writer, sheet_name='Voter Data', index=False)
            ws  = writer.sheets['Voter Data']
            hf  = _hdr_fmt(wb, bg=_DARK_BLUE)
            for i, c in enumerate(df_raw_pd.columns):
                ws.write(0, i, c, hf)
                ws.set_column(i, i, min(max(len(c), 10), 32))
            ws.freeze_panes(1, 0)
            logger.info('  ✓ Voter Data: %s rows, %d columns',
                        f'{len(df_raw_pd):,}', len(display_cols))

        else:
            logger.info('  Writing County Summary sheet ...')
            county_sum_pd = build_county_summary(df).to_pandas()
            _write_section(writer, 'County Summary', county_sum_pd, 0,
                           'Voter Registration by County',
                           _hdr_fmt(wb), _title_fmt(wb),
                           col_widths={0: 12, 1: 16, 2: 14, 3: 14})
            writer.sheets['County Summary'].set_column(0, 12, 14)
            logger.info('  ✓ County Summary: %d counties', len(county_sum_pd))

        # ── Sheet 2: Decade Summary ───────────────────────────────────────────
        logger.info('  Writing Decade Summary sheet ...')
        decade_pd = build_decade_summary(df).to_pandas()
        decade_pd.to_excel(writer, sheet_name='Decade Summary', index=False)
        ws  = writer.sheets['Decade Summary']
        hf  = _hdr_fmt(wb)
        for i, c in enumerate(decade_pd.columns):
            ws.write(0, i, c, hf)
        ws.set_column(0, 0, 15)
        ws.set_column(1, 1, 15)
        # Embed bar chart
        chart = wb.add_chart({'type': 'column'})
        n     = len(decade_pd)
        chart.add_series({
            'name':       'Voter Count',
            'categories': ['Decade Summary', 1, 0, n, 0],
            'values':     ['Decade Summary', 1, 1, n, 1],
            'fill':       {'color': _MED_BLUE},
        })
        chart.set_title({'name': f'Voters by Birth Decade — {county_name}'})
        chart.set_x_axis({'name': 'Birth Decade'})
        chart.set_y_axis({'name': 'Voter Count', 'num_format': '#,##0'})
        chart.set_size({'width': 480, 'height': 300})
        ws.insert_chart('D2', chart)
        logger.info('  ✓ Decade Summary: %d cohorts', len(decade_pd))

        # ── Sheet 2b: Generation Summary ─────────────────────────────────────
        logger.info('  Writing Generation Summary sheet ...')
        gen_pd = build_generation_summary(df).to_pandas()
        gen_pd.to_excel(writer, sheet_name='Generation Summary', index=False)
        ws  = writer.sheets['Generation Summary']
        hf  = _hdr_fmt(wb)
        for i, c in enumerate(gen_pd.columns):
            ws.write(0, i, c, hf)
        ws.set_column(0, 0, 22)
        ws.set_column(1, 1, 15)
        # Embed bar chart
        chart = wb.add_chart({'type': 'column'})
        n     = len(gen_pd)
        chart.add_series({
            'name':       'Voter Count',
            'categories': ['Generation Summary', 1, 0, n, 0],
            'values':     ['Generation Summary', 1, 1, n, 1],
            'fill':       {'color': _MED_BLUE},
        })
        chart.set_title({'name': f'Voters by Generation — {county_name}'})
        chart.set_x_axis({'name': 'Generation'})
        chart.set_y_axis({'name': 'Voter Count', 'num_format': '#,##0'})
        chart.set_size({'width': 480, 'height': 300})
        ws.insert_chart('D2', chart)
        logger.info('  ✓ Generation Summary: %d cohorts', len(gen_pd))

        # ── Sheet 3: Participation ────────────────────────────────────────────
        logger.info('  Writing Participation sheet ...')
        hf, tf = _hdr_fmt(wb), _title_fmt(wb)
        election_df, type_df, freq_df = build_election_participation(df, election_cols, logger)
        row = 0
        row = _write_section(writer, 'Participation', election_df.to_pandas(), row,
                             'Election Participation — by Election', hf, tf,
                             col_widths={0: 30, 1: 14, 2: 12, 3: 12, 4: 10, 5: 10})
        row = _write_section(writer, 'Participation', type_df.to_pandas(), row,
                             'Participation Summary — by Type', hf, tf,
                             col_widths={0: 16, 1: 12, 2: 14, 3: 12, 4: 12})
        _write_section(writer, 'Participation', freq_df.to_pandas(), row,
                       'Voter Frequency Distribution', hf, tf,
                       col_widths={0: 30, 1: 14, 2: 10})
        writer.sheets['Participation'].freeze_panes(2, 0)
        logger.info('  ✓ Participation: %d elections', len(election_df))

        # ── Sheet 4: District Breakdown ───────────────────────────────────────
        logger.info('  Writing District Breakdown sheet ...')
        hf, tf = _hdr_fmt(wb), _title_fmt(wb)
        dist_tables, most_recent_g = build_district_breakdown(df, election_cols, logger)
        row = 0
        for field, tbl in dist_tables.items():
            row = _write_section(writer, 'District Breakdown', tbl.to_pandas(), row,
                                 f'Breakdown — {field}', hf, tf)
        ws = writer.sheets.get('District Breakdown')
        if ws:
            ws.set_column(0, 0, 38)
            ws.set_column(1, 20, 16)
        logger.info('  ✓ District Breakdown: %d field(s)', len(dist_tables))

        # ── Sheet 5: Party Cross-tabs ─────────────────────────────────────────
        logger.info('  Writing Party Cross-tabs sheet ...')
        hf, tf = _hdr_fmt(wb), _title_fmt(wb)
        by_congress, by_decade, by_status = build_party_crosstabs(df, logger)
        by_generation = build_generation_crosstab(df, logger)
        row = 0
        row = _write_section(writer, 'Party Cross-tabs', by_congress.to_pandas(), row,
                             'Party × Congressional District', hf, tf, col_widths={0: 35})
        row = _write_section(writer, 'Party Cross-tabs', by_decade.to_pandas(), row,
                             'Party × Birth Decade', hf, tf, col_widths={0: 16})
        row = _write_section(writer, 'Party Cross-tabs', by_status.to_pandas(), row,
                             'Party × Voter Status', hf, tf, col_widths={0: 25})
        if len(by_generation) > 0:
            _write_section(writer, 'Party Cross-tabs', by_generation.to_pandas(), row,
                           'Party × Generation (Pew Research Center)', hf, tf, col_widths={0: 28})
        ws = writer.sheets.get('Party Cross-tabs')
        if ws:
            ws.set_column(1, 30, 12)
        logger.info('  ✓ Party Cross-tabs')

    size_mb = output_path.stat().st_size / 1e6
    logger.info('Workbook saved: %s  (%.1f MB)', output_path.name, size_mb)


# ─────────────────────────────────────────────────────────────────────────────
# Public entry points  (called by ohio_voter_pipeline.py)
# ─────────────────────────────────────────────────────────────────────────────

def _require_columns(wanted: list[str] | None, available: list[str], where: str) -> list[str] | None:
    """Return `wanted` re-ordered to the frame's own column order, or None for "all".

    Frame order is kept because callers derive the election-column list from
    df.columns, and that order is chronological and load-bearing. Raises
    KeyError naming every missing column: a caller must never proceed on a
    narrower frame than it asked for.
    """
    if wanted is None:
        return None
    have = set(available)
    missing = sorted(set(wanted) - have)
    if missing:
        raise KeyError(f'{len(missing)} requested column(s) not in the {where}: {missing}')
    want = set(wanted)
    return [c for c in available if c in want]


# Columns add_voter_participation appends: four Int32 counts, two Float64 rates,
# one String label. Used only to size the export frame before it exists.
_PARTICIPATION_DTYPES = ['Int32'] * 4 + ['Float64'] * 2 + ['String']
# Stand-in for the classifier's derived columns when sizing a frame that has
# not been built yet (a rebuild). Provisional, like the rest of the estimate.
_DERIVED_DTYPES_ESTIMATE = ['Float64'] * 26


def _parquet_rows(path: Path) -> int:
    """Row count of one parquet file, from its footer. No data page is read."""
    import pyarrow.parquet as pq
    return pq.ParquetFile(path).metadata.num_rows


def _raw_partition_rows() -> dict[str, int]:
    """Rows per raw-cache partition directory, from parquet footers only."""
    return {
        part.name: sum(_parquet_rows(f) for f in part.rglob('*.parquet'))
        for part in sorted(PARQUET_DIR.glob(f'{PARTITION_KEY}=*')) if part.is_dir()
    }


def _raw_columns() -> tuple[int, int]:
    """(column count, election column count) of the raw cache, from the footer of
    its first file. The Hive partition key is counted even though it is a
    directory name and not a column inside the files."""
    first = next((f for part in sorted(PARQUET_DIR.glob(f'{PARTITION_KEY}=*'))
                  for f in sorted(part.rglob('*.parquet'))), None)
    if first is None:
        return 0, 0
    names = list(pl.read_parquet_schema(first))
    return len(names) + (0 if PARTITION_KEY in names else 1), len(election_cols_from_names(names))


def _text_shape(txt_files: list[Path]) -> tuple[int, int, int]:
    """(estimated rows, columns, election columns) of the raw text files.

    Reads each file's header line and its first 2 MiB only: rows are estimated
    as file size / average line length in that sample. Good to a few percent
    for a file whose rows are alike, which is all a memory estimate needs.
    """
    rows, names = 0.0, []
    for path in txt_files:
        size = path.stat().st_size
        with open(path, 'rb') as fh:
            sample = fh.read(2 * 1024 * 1024)
        header, _, body = sample.partition(b'\n')
        if not names:
            names = [n.strip().strip('"') for n in header.decode('utf-8', errors='replace').split(',')]
        lines = body.count(b'\n')
        if lines:
            rows += (size - len(header) - 1) / (len(body) / lines)
    return int(rows), len(names), len(election_cols_from_names(names))


def _memory_context() -> tuple:
    """(settings, budget, cpu_count) for a preflight: the active policy and the
    machine as it is right now."""
    settings = memory_budget.active()
    machine = memory_budget.read_machine()
    return settings, memory_budget.make_budget(machine, settings), machine.cpu_count


def _preflight_text_build(txt_files: list[Path], logger: logging.Logger) -> None:
    """Enforce the memory budget before the raw parquet cache is built from text.
    That build reads the whole raw frame into memory, and it is the first thing
    a fresh clone does. Raises memory_budget.MemoryBudgetError when it does not fit."""
    settings, budget, _ = _memory_context()
    rows, cols, n_election = _text_shape(txt_files)
    if not rows:
        logger.warning('Memory budget: could not size the text files; preflight skipped')
        return
    memory_budget.enforce(memory_budget.estimate_text_build(rows, cols, n_election),
                          budget, settings, logger)


def _preflight_rebuild(logger: logging.Logger, settings, budget) -> tuple[int, int, int]:
    """Enforce the budget for an enriched-cache rebuild. Returns (rows, raw
    columns, election columns) of the raw cache, (0, 0, 0) when it has no
    partitions to size the run from."""
    rows = sum(_raw_partition_rows().values())
    raw_cols, n_election = _raw_columns()
    if not rows:
        logger.warning('Memory budget: no raw parquet partitions to size the rebuild from; '
                       'preflight skipped')
        return 0, 0, 0
    memory_budget.enforce(memory_budget.estimate_rebuild(rows, raw_cols, n_election),
                          budget, settings, logger)
    return rows, raw_cols, n_election


def _preflight_statewide_export(logger: logging.Logger, fresh: bool, requested_workers: int = 0) -> int:
    """Size the county export, pick its worker count, and enforce the memory
    budget. Called by the loader after the cache-first decision and before
    anything is loaded. Returns the worker count to use.

    Shape comes from parquet footers only: rows and dtypes of the enriched
    cache when it is fresh, of the raw cache when it has to be rebuilt (in
    which case the rebuild is enforced first). The largest county's share of
    rows bounds what one worker can hold.

    Workers: an explicit request (argument, or --max-workers) is used as given.
    Otherwise the count starts at the logical CPU count capped at 8 and is
    lowered until the estimate fits; if it does not fit at one worker the run
    is refused (mode auto) or goes ahead with a warning (mode eager).

    Raises memory_budget.MemoryBudgetError.
    """
    settings, budget, cpu_count = _memory_context()
    requested = requested_workers or settings.max_workers
    start = memory_budget.choose_workers(cpu_count, requested)

    by_part = _raw_partition_rows()
    total = sum(by_part.values())
    largest_share = (max(by_part.values()) / total) if total else 1.0

    if fresh:
        schema = pl.read_parquet_schema(ENRICHED_CACHE)
        election = set(election_cols_from_names(list(schema)))
        rows = _parquet_rows(ENRICHED_CACHE)
        frame_dtypes = [str(t) for c, t in schema.items() if c not in election] + _PARTICIPATION_DTYPES
        n_election = len(election)
    else:
        rows, raw_cols, n_election = _preflight_rebuild(logger, settings, budget)
        if not rows:
            return start
        frame_dtypes = (['String'] * (raw_cols - n_election)
                        + _DERIVED_DTYPES_ESTIMATE + _PARTICIPATION_DTYPES)

    def estimate_for(workers: int):
        return memory_budget.estimate_export(rows, frame_dtypes, n_election, largest_share,
                                             workers, _PARTICIPATION_BATCH)

    workers = start if requested else memory_budget.fit_workers(estimate_for, budget.bytes, start)
    if workers != start:
        logger.info('Memory budget: %d worker(s) instead of %d so the county slices fit', workers, start)
    memory_budget.enforce(estimate_for(workers), budget, settings, logger)
    return workers


def _preflight_groupings(logger: logging.Logger, fresh: bool, columns: list[str]) -> None:
    """Enforce the memory budget for jurisdictional groupings before its frame
    is loaded: the projected frame plus one partitioned copy of it. On a stale
    cache the rebuild is enforced first. Raises memory_budget.MemoryBudgetError."""
    settings, budget, _ = _memory_context()
    if fresh:
        schema = pl.read_parquet_schema(ENRICHED_CACHE)
        rows = _parquet_rows(ENRICHED_CACHE)
        dtypes = [str(schema[c]) for c in columns if c in schema]
    else:
        rows, _, _ = _preflight_rebuild(logger, settings, budget)
        if not rows:
            return
        dtypes = ['String'] * len(columns)
    memory_budget.enforce(memory_budget.estimate_groupings(rows, dtypes), budget, settings, logger)


def _resolve_enriched(
    logger:      logging.Logger,
    txt_files:   list[Path] | None,
    use_parquet: bool,
    before_load=None,
) -> pl.DataFrame | None:
    """Make the cache-first decision once. The only place it is made.

    Returns None when ENRICHED_CACHE is fresh: the caller reads what it needs
    from the file and nothing has been loaded. Otherwise rebuilds (load raw,
    clean, write the cache atomically) and returns the full enriched frame.

    Order is the contract: freshness is decided BEFORE anything is loaded.
    Loading raw first and then replacing it with the cache held ~21 GB twice
    and was the trigger for OPEN-001 incident 2 (2026-11-11). On a rebuild the
    raw and enriched frames do overlap while clean_voter_data runs; that is the
    cost of a rebuild, paid once per snapshot or classifier change.

    txt_files   SWVF source files. With use_parquet the raw parquet cache is
                built from them first (idempotent), because freshness is judged
                against the raw partitions. Required for a CSV rebuild.
    before_load callable(fresh: bool), run after the decision and before any
                load. This is where a caller enforces its memory budget
                (_preflight_*); whatever it raises stops the run with nothing
                loaded.

    Raises FileNotFoundError (parquet rebuild, no raw cache), ValueError (CSV
    rebuild without txt_files), and anything before_load raises.
    """
    if use_parquet and txt_files is not None:
        build_parquet_cache(txt_files, logger=logger)

    fresh = _cache_is_fresh(logger)
    if before_load is not None:
        before_load(fresh)
    if fresh:
        return None

    if use_parquet:
        with _timer(logger, 'loading voter files (Parquet)'):
            raw = load_voter_files_parquet(logger=logger)
    else:
        if txt_files is None:
            raise ValueError('enriched cache is stale and use_parquet is False: txt_files is required')
        with _timer(logger, 'loading voter files (CSV)'):
            raw = load_voter_files(txt_files, logger=logger)
    with _timer(logger, 'cleaning voter data'):
        df = clean_voter_data(raw, logger)
    del raw
    logger.info('Writing enriched voter data to persistent cache...')
    _write_cache_atomic(df, logger)
    return df


def load_enriched_voters(
    logger:      logging.Logger,
    *,
    txt_files:   list[Path] | None = None,
    use_parquet: bool = True,
    columns:     list[str] | None = None,
    before_load=None,
) -> pl.DataFrame:
    """Return the statewide enriched frame, cache-first (see _resolve_enriched).

    columns     Projection of the returned frame, in the frame's own column
                order. On a cache hit only these columns are read from disk.
                On a rebuild the cache is still written whole. Raises KeyError
                naming any requested column the frame lacks (_require_columns).

    Callers that only need election history to compute participation should
    use load_enriched_for_export instead, which never holds it.
    before_load is passed to _resolve_enriched.
    """
    rebuilt = _resolve_enriched(logger, txt_files, use_parquet, before_load)
    if rebuilt is None:
        ordered = _require_columns(columns, list(pl.read_parquet_schema(ENRICHED_CACHE)), 'enriched cache')
        with _timer(logger, 'loading enriched cache'):
            df = pl.read_parquet(ENRICHED_CACHE, columns=ordered)
            if ordered is not None:
                df = df.select(ordered)
        logger.info('Loaded from enriched cache: %s rows x %d columns%s', f'{df.height:,}', df.width,
                    '' if ordered is None else ' (projected)')
        return df

    ordered = _require_columns(columns, rebuilt.columns, 'enriched frame')
    return rebuilt if ordered is None else rebuilt.select(ordered)


def load_enriched_for_export(
    logger:      logging.Logger,
    *,
    txt_files:   list[Path] | None = None,
    use_parquet: bool = True,
    before_load=None,
):
    """Return (frame, election_cols, read_cells) for the county export.

    frame          the enriched frame WITHOUT its election-history columns.
    election_cols  the names of those columns, chronological.
    read_cells     callable(list of names) -> DataFrame of exactly those
                   election columns, row-aligned with `frame`. Pass it to
                   add_voter_participation, then drop the reference.

    Why: the export reads election cells for one thing, the participation
    counts. Everything after that works from the cohort and participation
    columns. Election history is 91 String columns, about 13 GB statewide, more
    than the rest of the frame put together, and the export stages used to
    carry it unread through every county slice and the statewide pass.

    Cache hit   frame is read from ENRICHED_CACHE without those columns, and
                read_cells reads the requested ones from the same file, so at
                most one batch of them is ever in memory. Both reads return
                rows in file order, which is what keeps them aligned.
    Rebuild     the classifier needs every election column, so the full frame
                exists anyway. read_cells selects from it and keeps it alive;
                dropping the read_cells reference releases it.

    Raises RuntimeError if the frame has no cohort_family column: without it
    the export would fall back to re-classifying from election cells that are
    no longer there. before_load is passed to _resolve_enriched.
    """
    rebuilt = _resolve_enriched(logger, txt_files, use_parquet, before_load)
    if rebuilt is None:
        names = list(pl.read_parquet_schema(ENRICHED_CACHE))
        election_cols = election_cols_from_names(names)
        skip = set(election_cols)
        keep = [c for c in names if c not in skip]
        cache = ENRICHED_CACHE
        with _timer(logger, 'loading enriched cache (without election history)'):
            frame = pl.read_parquet(cache, columns=keep).select(keep)

        def read_cells(cols: list[str]) -> pl.DataFrame:
            return pl.read_parquet(cache, columns=cols).select(cols)

        logger.info('Loaded from enriched cache: %s rows x %d columns; %d election columns '
                    'left on disk and read in batches', f'{frame.height:,}', frame.width,
                    len(election_cols))
    else:
        election_cols = identify_election_cols(rebuilt)
        frame = rebuilt.drop(election_cols)
        read_cells = rebuilt.select
        logger.info('Enriched frame rebuilt: %s rows x %d columns plus %d election columns, '
                    'held until participation is computed', f'{frame.height:,}', frame.width,
                    len(election_cols))

    if 'cohort_family' not in frame.columns:
        raise RuntimeError(
            'enriched frame has no cohort_family column; the county export cannot run '
            'without the cohort classifier output (see load_enriched_for_export)'
        )
    return frame, election_cols, read_cells


def _county_slice(df: pl.DataFrame, county_num: str) -> pl.DataFrame:
    """Rows of `df` for one county, in frame order. `county_num` is the stripped,
    zero-padded COUNTY_NUMBER value. The single spelling of the county filter."""
    return df.filter(pl.col('COUNTY_NUMBER').str.strip_chars() == county_num)


def _export_county_worker(args: tuple) -> tuple:
    """
    Thread worker for ThreadPoolExecutor.
    Receives (county_num, statewide_df, election_cols, include_precinct_charts).

    Memory: the worker filters its own county out of the shared statewide frame
    and drops the slice when it returns, so the slices resident at any moment
    are those of the running workers (at most max_workers), never all 88.
    The statewide frame carries no election-history columns by this point
    (load_enriched_for_export), so neither do the slices.
    Slicing every county before the pool started held a second full copy of
    the frame for the whole export stage (OPEN-001: 44.3 GB RSS with 8 threads).

    Threads share the main process address space: no IPC serialisation, no
    pipe handles, no WinError 1450. The statewide frame is pre-enriched (cohort
    columns attached by clean_voter_data) and is only read here; Polars frames
    are reference-counted and safe to read from several threads.
    orjson releases the GIL during serialisation so threads overlap on I/O.
    """
    county_num, statewide_df, election_cols, include_precinct_charts = args
    county_name = OHIO_COUNTIES.get(county_num.strip(), f'County {county_num}')
    logger = logging.getLogger('voter_analysis')
    try:
        county_df = _county_slice(statewide_df, county_num)
        primary_cols = identify_primary_cols(election_cols)

        # Fast path: build unc_classified from pre-enriched frame (no classifier re-run).
        unc_classified = _unc_classified_from_enriched_df(county_df)
        if unc_classified is None and primary_cols:
            unc_classified = classify_unc_primary_history(county_df, primary_cols, logger)

        export_json(county_name, county_df, election_cols, logger,
                    update_manifest=False, unc_classified=unc_classified,
                    include_precinct_charts=include_precinct_charts)
        export_unc_shadow_json(county_name, county_df, primary_cols, logger,
                               unc_classified=unc_classified)
        return (county_name, None)
    except Exception as exc:
        return (county_name, str(exc))


def run_ohio_analysis(
    txt_files:               list[Path],
    use_parquet:             bool = True,
    max_workers:             int  = 0,
    include_precinct_charts: bool = False,
):
    """
    Export web dashboard JSON for all Ohio counties.

    Writes JSON to docs/data/ and updates docs/manifest.json.
    No Excel output — completely independent of the spreadsheet path.
    Use run_ohio_excel() separately if you need the workbook.

    Args:
        txt_files:               SWVF_*.txt source files.
        use_parquet:             Build/use Hive-partitioned Parquet cache. Default True.
        max_workers:             Worker threads for the county export. 0 = derive from the
                                 CPU count and the memory budget (--max-workers, then
                                 _preflight_statewide_export).

    Raises memory_budget.MemoryBudgetError, before anything is loaded, when the
    estimated peak does not fit the memory budget and the memory mode is auto.
        include_precinct_charts: Also write per-precinct party + UNC JSON files.
                                 Significantly increases output file count and run time.
    """
    logger = setup_logging('ohio')
    logger.info('=' * 60)
    logger.info('OHIO STATEWIDE  —  web dashboard JSON export')
    logger.info('=' * 60)

    try:
        # ── 1. Load + enrich, cache-first, without election history ──────────────
        # The frame carries no election columns; read_cells hands them to the
        # participation step a batch at a time (see load_enriched_for_export).
        # The memory preflight runs inside the loader, after the cache-first
        # decision and before any load; it also fixes the worker count.
        plan: dict = {}
        df, election_cols, read_cells = load_enriched_for_export(
            logger, txt_files=txt_files, use_parquet=use_parquet,
            before_load=lambda fresh: plan.update(
                workers=_preflight_statewide_export(logger, fresh, max_workers)),
        )
        logger.info('%d election columns: %s to %s',
                    len(election_cols), election_cols[0], election_cols[-1])

        with _timer(logger, 'computing participation metrics'):
            df = add_voter_participation(df, election_cols, logger, read_cells=read_cells)
        # After a rebuild read_cells is the last reference to the full frame.
        del read_cells

        # ── 3. Parallel county JSON export ────────────────────────────────────
        counties_present = sorted(df['COUNTY_NUMBER'].str.strip_chars().unique().to_list())
        logger.info('Exporting %d counties; each worker slices its own county ...',
                    len(counties_present))

        # One task per county. Every task carries the same statewide frame by
        # reference; the worker takes its county slice when it starts and drops
        # it when it returns (see _export_county_worker).
        tasks = [(county_num, df, election_cols, include_precinct_charts)
                 for county_num in counties_present]

        # ThreadPoolExecutor: no IPC pipes, no WinError 1450.
        # orjson releases the GIL during serialisation so threads overlap on I/O.
        # The count was fixed by the memory preflight (_preflight_statewide_export).
        n_workers = plan['workers']
        logger.info('Launching %d thread worker(s) ...', n_workers)

        processed: list[str] = []
        failed:    list[tuple] = []

        with _timer(logger, f'parallel JSON export ({n_workers} threads)'):
            with concurrent.futures.ThreadPoolExecutor(max_workers=n_workers) as pool:
                futures = {pool.submit(_export_county_worker, t): t[0] for t in tasks}
                for fut in concurrent.futures.as_completed(futures):
                    county_num = futures[fut]
                    try:
                        county_name, err = fut.result()
                        if err:
                            logger.error('FAIL %s: %s', county_name, err)
                            failed.append((county_name, err))
                        else:
                            logger.info('  OK  %s', county_name)
                            processed.append(county_name)
                    except Exception as exc:
                        cname = OHIO_COUNTIES.get(county_num.strip(), county_num)
                        logger.error('FAIL %s: %s', cname, exc)
                        failed.append((cname, str(exc)))

        # ── 3b. Statewide aggregate (evaluation M10) ──────────────────────────────
        # export_json over the whole frame: the same resolver as the counties,
        # so the statewide charts carry the same 9 cohort labels. The landing
        # page's headline count and the dashboard's "Ohio" view read these.
        # No county slice is alive by now: each worker dropped its own on return.
        # Not added to `processed`: the manifest's county lists name counties only.
        del tasks, futures
        with _timer(logger, 'statewide JSON export'):
            export_json(STATEWIDE_NAME, df, election_cols, logger,
                        update_manifest=False, write_precinct_summary=False)
            export_unc_shadow_json(STATEWIDE_NAME, df, identify_primary_cols(election_cols),
                                   logger, unc_classified=_unc_classified_from_enriched_df(df))
        state_slug = STATEWIDE_NAME.lower().replace(' ', '_')
        # Written by earlier versions; skipped now (see export_json).
        (DATA_DIR / f'{state_slug}_precinct_summary.json').unlink(missing_ok=True)

        # ── 4. Write manifest once — no race condition, no O(N²) I/O ──────────
        today = date_t.today().isoformat()
        _write_manifest_bulk(sorted(processed), today, logger)

        if failed:
            logger.warning('%d county export(s) failed: %s', len(failed), [f[0] for f in failed])

        logger.info('=' * 60)
        logger.info('SUCCESS  (%d counties, %d failed)', len(processed), len(failed))
        logger.info('=' * 60)

    except memory_budget.MemoryBudgetError:
        # A refusal, not a crash: enforce() already logged the numbers.
        raise
    except Exception:
        logger.exception('Unhandled error in run_ohio_analysis')
        raise


def run_county_subset(
    txt_files:               list[Path],
    county_numbers:          list[str],
    use_parquet:             bool = True,
    include_precinct_charts: bool = False,
):
    """
    Export web dashboard JSON for a specific subset of counties.

    Identical pipeline to run_ohio_analysis() but loads only the requested
    Parquet partitions instead of all 88.  Runs counties sequentially —
    no parallel worker pool — since the subset is typically small (1–5).

    Requires the Parquet cache to exist.  Run option [1] or [2] first if
    starting from a fresh clone with only the raw SWVF txt files.

    Args:
        txt_files:               SWVF_*.txt source files (used to build cache if absent).
        county_numbers:          Zero-padded county number strings, e.g. ['57', '29'].
        use_parquet:             Must be True; included for API consistency.
        include_precinct_charts: Also write per-precinct party + UNC JSON files.
    """
    logger = setup_logging('county_subset')
    logger.info('=' * 60)
    logger.info('COUNTY SUBSET  —  web dashboard JSON export')
    logger.info('Counties: %s', ', '.join(
        f"{OHIO_COUNTIES.get(n, n)} ({n})" for n in county_numbers
    ))
    logger.info('=' * 60)

    try:
        # ── 1. Ensure Parquet cache exists ────────────────────────────────────
        build_parquet_cache(txt_files, logger=logger)

        # ── 2. Load + clean each county sequentially ──────────────────────────
        election_cols = None
        processed: list[str] = []
        failed:    list[tuple] = []

        for county_num in county_numbers:
            county_name = OHIO_COUNTIES.get(county_num.strip(), f'County {county_num}')
            logger.info('─' * 40)
            logger.info('Processing %s (%s) ...', county_name, county_num)

            try:
                with _timer(logger, f'load {county_name} (Parquet)'):
                    county_df = load_voter_files_parquet(
                        county_number=county_num, logger=logger
                    )

                with _timer(logger, f'clean {county_name}'):
                    county_df = clean_voter_data(county_df, logger)

                if election_cols is None:
                    election_cols = identify_election_cols(county_df)
                    logger.info('%d election columns identified', len(election_cols))
                else:
                    # Re-identify in case this county's slice has a different column set
                    ec = identify_election_cols(county_df)
                    if ec:
                        election_cols = ec

                with _timer(logger, f'participation metrics {county_name}'):
                    county_df = add_voter_participation(county_df, election_cols, logger)

                primary_cols   = identify_primary_cols(election_cols)
                unc_classified = (
                    classify_unc_primary_history(county_df, primary_cols, logger)
                    if primary_cols else None
                )

                with _timer(logger, f'export JSON {county_name}'):
                    export_json(
                        county_name, county_df, election_cols, logger,
                        update_manifest=True,
                        unc_classified=unc_classified,
                        include_precinct_charts=include_precinct_charts,
                    )
                    export_unc_shadow_json(
                        county_name, county_df, primary_cols, logger,
                        unc_classified=unc_classified,
                    )

                processed.append(county_name)
                logger.info('  OK  %s', county_name)

            except Exception as exc:
                logger.error('FAIL %s: %s', county_name, exc)
                failed.append((county_name, str(exc)))

        if failed:
            logger.warning('%d county export(s) failed: %s', len(failed), [f[0] for f in failed])

        logger.info('=' * 60)
        logger.info('SUCCESS  (%d counties, %d failed)', len(processed), len(failed))
        logger.info('=' * 60)

    except Exception:
        logger.exception('Unhandled error in run_county_subset')
        raise


def run_ohio_excel(
    txt_files:   list[Path],
    output_path: Path,
    use_parquet: bool = True,
):
    """
    Write a summary Excel workbook for all of Ohio.

    Completely independent of run_ohio_analysis() — call only when the
    Excel deliverable is needed. Does not touch JSON or the dashboard.
    """
    logger = setup_logging('ohio_excel')
    logger.info('=' * 60)
    logger.info('OHIO STATEWIDE  —  Excel export')
    logger.info('=' * 60)

    try:
        if use_parquet and PARQUET_DIR.exists():
            with _timer(logger, 'loading voter files (Parquet)'):
                df = load_voter_files_parquet(logger=logger)
        else:
            with _timer(logger, 'loading voter files (CSV)'):
                df = load_voter_files(txt_files, logger=logger)

        with _timer(logger, 'cleaning voter data'):
            df = clean_voter_data(df, logger)

        election_cols = identify_election_cols(df)

        with _timer(logger, 'computing participation metrics'):
            df = add_voter_participation(df, election_cols, logger)

        with _timer(logger, 'writing Excel workbook'):
            build_workbook(df, election_cols, output_path,
                           county_name=STATEWIDE_NAME,
                           include_raw=False,
                           logger=logger)
        logger.info('Workbook saved: %s', output_path)

    except Exception:
        logger.exception('Unhandled error in run_ohio_excel')
        raise


def run_county_analysis(
    txt_files:     list[Path],
    county_number: str,
    output_path:   Path,
    use_parquet:   bool = True,
):
    """
    Single-county web dashboard JSON + Excel export.

    Prefers Parquet cache when available; falls back to CSV scan.
    """
    county_number = county_number.zfill(2)
    county_name   = OHIO_COUNTIES.get(county_number, f'County {county_number}')
    logger        = setup_logging(f'county_{county_number}')

    logger.info('=' * 60)
    logger.info('COUNTY %s (%s)', county_number, county_name)
    logger.info('=' * 60)

    try:
        if use_parquet and PARQUET_DIR.exists():
            with _timer(logger, f'loading county {county_number} (Parquet)'):
                df = load_voter_files_parquet(county_number=county_number, logger=logger)
        else:
            with _timer(logger, f'loading county {county_number} (CSV)'):
                df = load_voter_files(txt_files, county_number=county_number, logger=logger)

        if df.is_empty():
            logger.error('No records found for county %s — check county number and file range',
                         county_number)
            return

        with _timer(logger, 'cleaning voter data'):
            df = clean_voter_data(df, logger)

        election_cols = identify_election_cols(df)
        logger.info('%d election columns: %s → %s',
                    len(election_cols), election_cols[0], election_cols[-1])

        with _timer(logger, 'computing participation metrics'):
            df = add_voter_participation(df, election_cols, logger)

        primary_cols   = identify_primary_cols(election_cols)
        unc_classified = (
            classify_unc_primary_history(df, primary_cols, logger)
            if primary_cols else None
        )

        with _timer(logger, 'exporting JSON'):
            export_json(
                county_name, df, election_cols, logger,
                update_manifest=True,
                unc_classified=unc_classified,
            )
            export_unc_shadow_json(
                county_name, df, primary_cols, logger,
                unc_classified=unc_classified,
            )

        with _timer(logger, 'writing Excel workbook'):
            build_workbook(df, election_cols, output_path,
                           county_name=county_name,
                           include_raw=False,
                           logger=logger)
        logger.info('Workbook saved: %s', output_path)

    except Exception:
        logger.exception('Unhandled error in run_county_analysis(%s)', county_number)
        raise


# ══════════════════════════════════════════════════════════════════════════════
# Entry point — invoked directly by ohio_voter_pipeline.py
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == '__main__':
    import sys as _sys
    # Allow quick smoke-tests: python pipeline/voter_data_cleaner.py --test
    if '--test' in _sys.argv:
        _logger = setup_logging('smoke_test')
        _logger.info('Smoke test: OHIO_COUNTIES has %d entries', len(OHIO_COUNTIES))
        assert len(OHIO_COUNTIES) == 88, f'Expected 88 counties, got {len(OHIO_COUNTIES)}'
        nums = sorted(OHIO_COUNTIES.keys())
        assert nums[0] == '01' and nums[-1] == '88', f'Numbering out of range: {nums[0]}…{nums[-1]}'
        assert OHIO_COUNTIES['57'] == 'Montgomery', \
            f"County 57 should be Montgomery, got {OHIO_COUNTIES['57']}"
        assert 'strictly alphabetical by county name' in open(__file__, encoding='utf-8').read(), \
            'OHIO_COUNTIES comment block missing'
        assert callable(globals().get('run_county_subset')), \
            'run_county_subset not found'
        _logger.info('All smoke-test assertions passed.')
    else:
        print('voter_data_cleaner.py is a library — import it or run with --test')
