"""
memory_budget.py
================
Preflight for the heavy pipeline stages: measure the machine, estimate the
stage's peak memory from the shape of the data, and either go ahead or refuse
before anything is loaded. Standard library and psutil only; no OS-specific
call, so the same code runs on Windows, Linux and macOS.

Why (KNOWN_ISSUES.md OPEN-001)
    A run that does not fit used to start anyway, work for minutes, and then
    die inside Polars' native code with an access violation or an abort. That
    is not a catchable MemoryError, so the only place a clear failure can be
    produced is here, before the load.

Rules
    * The budget is AVAILABLE PHYSICAL memory minus a margin (default 15%).
      Available physical memory is the binding number on every OS: Windows
      refuses commit then pages, Linux overcommits then OOM-kills, macOS
      compresses then swaps.
    * Swap / pagefile is reported and never counted. A run that fits only by
      paging is the slow failure this module exists to prevent; the pagefile is
      a safety net for overshoot, not capacity.
    * The estimate is built from the data's own shape (row counts and dtypes
      read from parquet footers by the caller) times a per-dtype cell cost. No
      state-specific row count is baked in.
    * Refusal happens before load, names the numbers, and says what to do.

PROVISIONAL NUMBERS
    CELL_BYTES, ELECTION_CELL_BYTES and WORKING_FACTOR are estimates from dtype
    layout and a single statewide run (2026-11-12, taken on a machine with a
    faulty memory kit). They have not been calibrated against a clean
    measured run; that is DOO-187. Treat a refusal near the line as "probably",
    and use --memory-mode eager or --memory-budget-gb to override.

Run settings are process-wide (activate / active): one run has one memory
policy, set once by the command line, read by every stage.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Iterable

import psutil

GIB = 2**30

DEFAULT_MARGIN = 0.15
MODES = ("auto", "eager")

# Worker threads when the caller does not ask for a number: the logical CPU
# count, capped here. Eight is the only count this pipeline has been run with;
# more threads would each hold one more county slice for an unmeasured gain.
MAX_DEFAULT_WORKERS = 8

# Approximate resident bytes per cell, by Polars dtype name. String is a
# 16-byte view plus out-of-line data for values longer than 12 bytes; 24 is a
# rounded allowance for name and address columns.
CELL_BYTES = {
    "String": 24.0, "Utf8": 24.0, "Categorical": 4.0, "Enum": 1.0, "Boolean": 0.125,
    "Int8": 1.0, "UInt8": 1.0, "Int16": 2.0, "UInt16": 2.0, "Int32": 4.0, "UInt32": 4.0,
    "Int64": 8.0, "UInt64": 8.0, "Float32": 4.0, "Float64": 8.0, "Date": 4.0, "Datetime": 8.0,
}
UNKNOWN_CELL_BYTES = 16.0

# An election-history cell is a String holding zero or one character: the view
# and little else. 17.8 B/cell was observed on the 2026-11 statewide runs.
ELECTION_CELL_BYTES = 18.0

# Transient working memory (group-bys, joins, serialisation buffers, allocator
# slack) allowed on top of the resident frames, as a fraction of the main frame.
WORKING_FACTOR = 0.5


class MemoryBudgetError(RuntimeError):
    """The stage's estimated peak does not fit the memory budget. Raised before
    anything is loaded; the message states the numbers and the options."""


# ---- settings -----------------------------------------------------------------

@dataclass(frozen=True)
class MemorySettings:
    """One run's memory policy.

    mode         'auto'  refuse when the estimate exceeds the budget.
                 'eager' always run; an estimate over budget is logged as a
                         warning. For when the estimate is known to be wrong.
    budget_gb    Use this budget (GiB) instead of measuring available memory.
                 For tests, shared machines, and reproducing another machine.
    max_workers  Thread count for parallel stages. 0 derives it (choose_workers).
    margin       Fraction of available memory kept in reserve. Ignored when
                 budget_gb is given.
    """
    mode: str = "auto"
    budget_gb: float | None = None
    max_workers: int = 0
    margin: float = DEFAULT_MARGIN

    def __post_init__(self) -> None:
        if self.mode not in MODES:
            raise ValueError(f"memory mode must be one of {MODES}, got {self.mode!r}")
        if self.budget_gb is not None and not self.budget_gb > 0:
            raise ValueError(f"memory budget must be greater than 0 GB, got {self.budget_gb!r}")
        if self.max_workers < 0:
            raise ValueError(f"max workers must be 0 (derive) or at least 1, got {self.max_workers!r}")
        if not 0 <= self.margin < 1:
            raise ValueError(f"margin must be in [0, 1), got {self.margin!r}")


_ACTIVE = MemorySettings()


def activate(settings: MemorySettings) -> None:
    """Set the process-wide memory policy. Called once, by the command line."""
    global _ACTIVE
    _ACTIVE = settings


def active() -> MemorySettings:
    """The process-wide memory policy (defaults: auto, measured budget, derived workers)."""
    return _ACTIVE


# ---- the machine and the budget ------------------------------------------------

@dataclass(frozen=True)
class Machine:
    """Memory and CPU facts, in bytes. `available` is what can be used without
    paging right now; it already excludes what other programs hold. `swap_total`
    is None when the OS will not report it (see swap_total_bytes)."""
    total: int
    available: int
    swap_total: int | None
    cpu_count: int


def swap_total_bytes(source=psutil) -> int | None:
    """Total swap / pagefile in bytes, or None when the OS will not report it.

    On Windows psutil.swap_memory() also reads a performance counter for the
    used percentage, and raises RuntimeError where counters are blocked
    (restricted shells, some service accounts). Only the total is wanted and
    swap is never counted in the budget, so an unreadable total is reported as
    unknown. It is never reported as 0, which would read as "no swap".

    source   the psutil-like module to ask. The cleaner passes its own binding,
             so both callers share this one reader.
    """
    try:
        return source.swap_memory().total
    except RuntimeError:  # the blocked counter; any other error is a real fault
        return None


def read_machine() -> Machine:
    """Read the machine through psutil and os.cpu_count."""
    vm = psutil.virtual_memory()
    return Machine(total=vm.total, available=vm.available,
                   swap_total=swap_total_bytes(), cpu_count=os.cpu_count() or 1)


@dataclass(frozen=True)
class Budget:
    """Bytes a stage may use. `forced` is True when the number came from
    MemorySettings.budget_gb and not from the machine."""
    machine: Machine
    margin: float
    bytes: int
    forced: bool


def make_budget(machine: Machine, settings: MemorySettings) -> Budget:
    """available x (1 - margin), or the forced budget. Swap is never added."""
    if settings.budget_gb is not None:
        return Budget(machine, 0.0, int(settings.budget_gb * GIB), True)
    return Budget(machine, settings.margin, int(machine.available * (1 - settings.margin)), False)


# ---- estimates ----------------------------------------------------------------

@dataclass(frozen=True)
class Estimate:
    """A stage's estimated peak, with the parts it was summed from (for the log)."""
    label: str
    peak: int
    parts: tuple


def frame_bytes(rows: int, dtypes: Iterable[str], election_cols: int = 0) -> int:
    """Approximate resident size of a frame: rows x the per-dtype cell cost of
    each column in `dtypes`, plus `election_cols` election-history columns."""
    per_row = sum(CELL_BYTES.get(str(d).split("(")[0], UNKNOWN_CELL_BYTES) for d in dtypes)
    return int(rows * (per_row + election_cols * ELECTION_CELL_BYTES))


def estimate_export(rows: int, frame_dtypes: Iterable[str], election_cols: int,
                    largest_share: float, workers: int, batch: int) -> Estimate:
    """County export from a fresh enriched cache.

    frame      the enriched frame without election history, resident throughout.
    cells      one batch of election columns while participation is computed.
    slices     one county slice per worker; bounded above by every worker
               holding a county the size of the largest (`largest_share` of rows).
    The batch and the slices are never alive together, so only the larger counts.
    """
    frame = frame_bytes(rows, frame_dtypes)
    cells = frame_bytes(rows, (), election_cols=min(batch, election_cols))
    slices = int(frame * largest_share * workers)
    working = int(frame * WORKING_FACTOR)
    return Estimate("county export", frame + max(cells, slices) + working,
                    (("frame", frame), ("election batch", cells), ("county slices", slices),
                     ("working", working)))


def estimate_rebuild(rows: int, raw_cols: int, election_cols: int) -> Estimate:
    """Rebuilding the enriched cache: the raw frame (every column String) and the
    enriched frame it becomes are both resident while the classifier runs."""
    raw = frame_bytes(rows, ["String"] * (raw_cols - election_cols), election_cols=election_cols)
    enriched = int(raw * 1.1)          # raw columns plus the derived ones
    working = int(raw * WORKING_FACTOR)
    return Estimate("enriched-cache rebuild", raw + enriched + working,
                    (("raw frame", raw), ("enriched frame", enriched), ("working", working)))


def estimate_groupings(rows: int, frame_dtypes: Iterable[str]) -> Estimate:
    """Jurisdictional groupings: the projected frame plus one partitioned copy
    of it (partition_by materialises every partition for a jurisdiction type)."""
    frame = frame_bytes(rows, frame_dtypes)
    working = int(frame * WORKING_FACTOR)
    return Estimate("jurisdictional groupings", 2 * frame + working,
                    (("frame", frame), ("partitioned copy", frame), ("working", working)))


def estimate_text_build(rows: int, cols: int, election_cols: int) -> Estimate:
    """Building the raw parquet cache from the text files: the whole raw frame is
    read into memory once, then written out partitioned."""
    raw = frame_bytes(rows, ["String"] * (cols - election_cols), election_cols=election_cols)
    working = int(raw * WORKING_FACTOR)
    return Estimate("raw parquet build from text", raw + working,
                    (("raw frame", raw), ("working", working)))


# ---- decisions ----------------------------------------------------------------

def choose_workers(cpu_count: int, requested: int = 0) -> int:
    """Thread count for a parallel stage. An explicit request wins; otherwise the
    logical CPU count capped at MAX_DEFAULT_WORKERS. Always at least 1."""
    if requested:
        return max(1, requested)
    return max(1, min(cpu_count or 1, MAX_DEFAULT_WORKERS))


def fit_workers(estimate_for, budget_bytes: int, start: int) -> int:
    """Largest worker count <= `start` whose estimate fits the budget, else 1.
    `estimate_for(workers)` returns an Estimate. Fewer workers hold fewer county
    slices, so a run that does not fit at `start` may fit lower."""
    for workers in range(max(1, start), 0, -1):
        if estimate_for(workers).peak <= budget_bytes:
            return workers
    return 1


def _gb(n: int) -> str:
    return f"{n / GIB:.1f} GB"


def describe(estimate: Estimate, budget: Budget) -> str:
    """One log line: the machine, the budget and the estimate, with its parts."""
    m = budget.machine
    source = "forced" if budget.forced else f"{budget.margin:.0%} of available kept in reserve"
    parts = ", ".join(f"{name} {_gb(size)}" for name, size in estimate.parts)
    swap = "swap unknown" if m.swap_total is None else f"swap {_gb(m.swap_total)}"
    return (f"RAM total {_gb(m.total)}, available {_gb(m.available)}, {swap} "
            f"(not counted); budget {_gb(budget.bytes)} ({source}); "
            f"{estimate.label} estimated peak {_gb(estimate.peak)} [{parts}]")


def refusal_message(estimate: Estimate, budget: Budget) -> str:
    """What the user sees when a stage is refused."""
    m = budget.machine
    return (
        f"Not enough memory for the {estimate.label}: it needs about {_gb(estimate.peak)} and the "
        f"budget is {_gb(budget.bytes)} ({_gb(m.available)} of {_gb(m.total)} RAM is available now"
        + ("" if budget.forced else f", {budget.margin:.0%} kept in reserve")
        + "; the pagefile or swap is never counted). Nothing was loaded. Options: "
        "(1) close other programs and run again; "
        "(2) run a subset of counties (menu choice 5, or --choice 5 --counties ...); "
        "(3) force this run with --memory-mode eager if you know the estimate is too high "
        "(it may page heavily or fail). The estimate is provisional; see pipeline/memory_budget.py."
    )


def enforce(estimate: Estimate, budget: Budget, settings: MemorySettings, logger) -> None:
    """Log the budget line, then return (fits, or mode 'eager') or raise
    MemoryBudgetError (mode 'auto' and the estimate exceeds the budget)."""
    line = describe(estimate, budget)
    if estimate.peak <= budget.bytes:
        logger.info("Memory budget: %s; fits", line)
        return
    if settings.mode == "eager":
        logger.warning("Memory budget: %s; OVER BUDGET, continuing because memory mode is eager", line)
        return
    logger.error("Memory budget: %s; REFUSED", line)
    raise MemoryBudgetError(refusal_message(estimate, budget))
