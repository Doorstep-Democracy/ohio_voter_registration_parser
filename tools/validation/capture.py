"""In-memory capture of everything the pipeline would write for one county.

WHAT THIS IS FOR
----------------
The consolidation refactor needs a fast answer to "did my edit change the
output?". Running the pipeline to find out is slow for a reason that has
nothing to do with the analysis: writing ~83,000 small JSON files is
filesystem-bound. The compute underneath is not the bottleneck -- a statewide
88-county group-by over 7.9M rows measures in tenths of a second because Polars
pushes predicates down through columnar parquet.

So this harness runs the real export code paths and intercepts the writes.
Every call to ``_dump_json`` is captured into a dict keyed by the path the file
WOULD have been written to, relative to ``docs/data/``. No file is created, no
directory is made, nothing under ``docs/`` is touched.

WHY IT PATCHES RATHER THAN REIMPLEMENTS
---------------------------------------
The obvious alternative -- reimplement the payload construction here and
compare that -- would validate the harness against itself. This runs
``export_json`` and ``export_unc_shadow_json`` exactly as
``_export_county_worker`` does in production, in the same order, with the same
arguments. If the production sequence changes, this harness changes with it or
it is wrong, and that coupling is deliberate.

Both ``pipeline/voter_data_cleaner.py`` and ``pipeline/jurisdictional_groupings.py``
define their OWN ``_dump_json`` (itself an instance of the duplication the
consolidation work is meant to fix -- see CLAUDE.md section 5). Both are
patched, so jurisdiction-scope writes are captured too if the caller reaches
them.

SPEED
-----
``load_county_frame`` reads the enriched cache with a pushed-down county filter
rather than re-running the classifier. This matters: ``run_county_subset``
(voter_data_cleaner.py:4485-4490) loads the raw partition and calls
``clean_voter_data`` every time, while ``run_ohio_analysis`` (:4368-4375) reads
``ENRICHED_CACHE`` when it is fresh and skips cleaning entirely -- so the
"cheap" single-county entry point actually pays the expensive classifier cost.
This harness takes the cache path when it can and says so in the returned
metadata, because a validation loop you run after every edit has to be seconds,
not minutes.

SAFETY
------
* Writes are intercepted, so nothing lands on disk even on an exception.
* ``update_manifest=False`` is passed regardless, so ``docs/manifest.json`` is
  never touched even if patching were somehow bypassed.
* The frame is read-only; no parquet is rewritten.
"""

from __future__ import annotations

import contextlib
import logging
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import polars as pl  # noqa: E402

from pipeline import jurisdictional_groupings as _jg  # noqa: E402
from pipeline import voter_data_cleaner as _v  # noqa: E402


@dataclass
class CaptureResult:
    """Everything one county's export produced, plus how it was produced.

    Attributes:
        county_number:  Zero-padded county number, e.g. '57'.
        county_name:    Display name from ``OHIO_COUNTIES``, e.g. 'Montgomery'.
        payloads:       Map of output path (relative to ``docs/data/``, POSIX
                        separators) to the decoded payload dict that would have
                        been written there.
        source:         'enriched-cache' or 'clean-voter-data' -- which load
                        path was taken. Recorded because the two are supposed to
                        be equivalent and a mismatch between them is itself a
                        finding worth seeing.
        rows:           Row count of the county frame.
        seconds:        Wall-clock for the whole capture, load included.
    """

    county_number: str
    county_name: str
    payloads: dict[str, dict] = field(default_factory=dict)
    source: str = ""
    rows: int = 0
    seconds: float = 0.0

    def summary(self) -> str:
        """One-line human summary, for CLI output."""
        return (
            f"{self.county_name} ({self.county_number}): {len(self.payloads)} payload(s) "
            f"from {self.rows:,} rows via {self.source} in {self.seconds:.1f}s"
        )


@contextlib.contextmanager
def capture_writes(data_dir: Path | None = None) -> Iterator[dict[str, dict]]:
    """Patch every ``_dump_json`` so writes are collected instead of performed.

    Args:
        data_dir: The directory paths are made relative to when building keys.
                  Defaults to ``voter_data_cleaner.DATA_DIR``. Paths that fall
                  outside it are keyed by their absolute form, so a write to an
                  unexpected location is visible rather than silently rebased.

    Yields:
        The dict being filled: output path -> payload. It is populated as the
        wrapped code runs, so it can be inspected after the ``with`` block.

    Note:
        Patching is restored in a ``finally``, so an exception inside the block
        still leaves the modules untouched. This is why the harness is safe to
        run repeatedly in one process.
    """
    base = Path(data_dir) if data_dir is not None else Path(_v.DATA_DIR)
    captured: dict[str, dict] = {}

    def _fake_dump(obj: Any, path: Path, logger: logging.Logger) -> None:
        path = Path(path)
        try:
            key = path.resolve().relative_to(Path(base).resolve()).as_posix()
        except ValueError:
            key = path.as_posix()
        captured[key] = obj

    originals = [(_v, _v._dump_json), (_jg, _jg._dump_json)]
    try:
        for module, _ in originals:
            module._dump_json = _fake_dump
        yield captured
    finally:
        for module, original in originals:
            module._dump_json = original


def load_county_frame(
    county_number: str,
    logger: logging.Logger,
    *,
    prefer_cache: bool = True,
) -> tuple[pl.DataFrame, str]:
    """Load one county's enriched voter frame as cheaply as correctness allows.

    Args:
        county_number: Zero-padded county number, e.g. '57'. Callers may pass
                       '7'; it is padded here.
        logger:        Pipeline logger, passed through to the loaders.
        prefer_cache:  When True (default) read ``ENRICHED_CACHE`` with a
                       pushed-down county filter. When False, or when the cache
                       is absent, fall back to loading the raw partition and
                       running ``clean_voter_data`` -- the same thing
                       ``run_county_subset`` does.

    Returns:
        ``(frame, source)`` where source is 'enriched-cache' or
        'clean-voter-data'.

    Raises:
        ValueError: If the county has no rows, which almost always means a
                    wrong county number rather than an empty county.
    """
    county_number = str(county_number).zfill(2)
    cache = Path(_v.ENRICHED_CACHE)

    if prefer_cache and cache.exists():
        frame = (
            pl.scan_parquet(cache)
            .filter(pl.col("COUNTY_NUMBER").str.strip_chars() == county_number)
            .collect()
        )
        if frame.height:
            return frame, "enriched-cache"
        logger.warning(
            "county %s absent from enriched cache; falling back to clean_voter_data",
            county_number,
        )

    raw = _v.load_voter_files_parquet(county_number=county_number, logger=logger)
    if raw.is_empty():
        raise ValueError(f"no rows for county {county_number} in the parquet cache")
    return _v.clean_voter_data(raw, logger), "clean-voter-data"


def capture_county(
    county_number: str,
    *,
    include_precinct_charts: bool = True,
    prefer_cache: bool = True,
    logger: logging.Logger | None = None,
) -> CaptureResult:
    """Run one county's full JSON export in memory and return what it produced.

    The call sequence mirrors ``voter_data_cleaner._export_county_worker``
    exactly -- participation metrics, primary column identification, the
    enriched-frame fast path for ``unc_classified`` with the classifier as
    fallback, then ``export_json`` and ``export_unc_shadow_json``. Any drift
    between this and production makes the comparison meaningless, so keep them
    in step.

    Args:
        county_number:           Zero-padded county number, e.g. '57'.
        include_precinct_charts: Capture the per-precinct files as well as the
                                 county-scope ones. True by default because the
                                 precinct scope is where the consolidation does
                                 most of its work and where the duplicate
                                 write-site question lived.
        prefer_cache:            See ``load_county_frame``.
        logger:                  Optional logger; a quiet one is made if absent.

    Returns:
        A ``CaptureResult``. Nothing is written to disk.
    """
    logger = logger or _quiet_logger()
    county_number = str(county_number).zfill(2)
    county_name = _v.OHIO_COUNTIES.get(county_number, f"County {county_number}")

    started = time.perf_counter()
    frame, source = load_county_frame(county_number, logger, prefer_cache=prefer_cache)

    election_cols = _v.identify_election_cols(frame)
    frame = _v.add_voter_participation(frame, election_cols, logger)
    primary_cols = _v.identify_primary_cols(election_cols)

    unc_classified = _v._unc_classified_from_enriched_df(frame)
    if unc_classified is None and primary_cols:
        unc_classified = _v.classify_unc_primary_history(frame, primary_cols, logger)

    with capture_writes() as payloads:
        _v.export_json(
            county_name,
            frame,
            election_cols,
            logger,
            update_manifest=False,
            unc_classified=unc_classified,
            include_precinct_charts=include_precinct_charts,
        )
        _v.export_unc_shadow_json(
            county_name,
            frame,
            primary_cols,
            logger,
            unc_classified=unc_classified,
        )

    return CaptureResult(
        county_number=county_number,
        county_name=county_name,
        payloads=dict(payloads),
        source=source,
        rows=frame.height,
        seconds=time.perf_counter() - started,
    )


def county_slug(county_name: str) -> str:
    """Slug for a county's output filenames, e.g. 'Van Wert' -> 'van_wert'.

    Mirrors the convention at ``voter_data_cleaner.py:3994``
    (``county_name.lower().replace(' ', '_')``). It is duplicated here rather
    than imported because the pipeline builds it inline and exposes no helper --
    which is itself an instance of the single-resolver problem this refactor is
    addressing. When ``pipeline/paths.py`` lands with a slug helper, delete this
    and import that one.
    """
    return county_name.lower().replace(" ", "_")


def _quiet_logger() -> logging.Logger:
    """A logger that stays out of the way during a fast validation loop.

    The pipeline logs at INFO per county and per precinct; at capture volume
    that is thousands of lines competing with the comparison output. WARNING
    keeps genuine problems visible without the narration.
    """
    logger = logging.getLogger("validation.capture")
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
        logger.addHandler(handler)
    logger.setLevel(logging.WARNING)
    return logger
