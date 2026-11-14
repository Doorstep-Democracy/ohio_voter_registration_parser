"""
snapshot_store.py
=================
Single resolver for SOS voter-file snapshots (CLAUDE.md section 5: never
duplicate discovery/staging logic).  Every consumer -- the interactive
pipeline (`ohio_voter_pipeline.py`), the wrapper
(`ohio_voter_pipeline_wrapper.py`), and any future temporal engine (not yet
written) -- imports this module rather than
re-deriving where snapshots live or how one gets staged.

Layout this module owns
-----------------------
    local/source/
      snapshots/                     <- the ONLY place discovery looks
        YYYY-MM-DD/                   <- one SOS drop, named by its file date
          SWVF_1_22.txt.gz           <- exactly these 4 names = "complete"
          SWVF_23_44.txt.gz
          SWVF_45_66.txt.gz
          SWVF_67_88.txt.gz
          provenance.json            <- optional (download metadata)
      State Voter Files/             <- staging cache: decompressed .txt of
        SWVF_*.txt                      exactly ONE snapshot (analysis reads this)
        staged_from.json             <- identity of what is currently staged

A folder under snapshots/ that does not match ^\\d{4}-\\d{2}-\\d{2}$ or lacks
the full 4-name set is INCOMPLETE and not selectable.  Staging is a cache of
one snapshot at a time; `staged_from.json` is its identity and restaging
overwrites atomically.

Console strings are ASCII-only (cp1252 console; a Unicode glyph crashes the
process).  Failures are loud -- no try/except as control flow.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import re
import shutil
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

# ── Config (resolved from __file__, never CWD) ────────────────────────────────
# Shared locations come from the single resolver, pipeline/paths.py. pipeline/
# goes on sys.path so the bare sibling import resolves under either import style.
_PIPELINE_DIR = str(Path(__file__).resolve().parent)
if _PIPELINE_DIR not in sys.path:
    sys.path.insert(0, _PIPELINE_DIR)
from paths import BASE_DIR, ENRICHED_CACHE, PARQUET_DIR, SOURCE_DIR, TXT_DIR  # noqa: E402,F401

SNAPSHOTS_DIR = SOURCE_DIR / "snapshots"
STAGING_DIR   = TXT_DIR   # the staging cache IS the staged raw-voter-file directory
STAGED_FROM   = STAGING_DIR / "staged_from.json"

# PARQUET_DIR and ENRICHED_CACHE are downstream caches keyed off the staged
# snapshot. They are not owned by this module but are invalidated here, because
# stage() is the only place that knows a *different* snapshot just replaced the
# staged one.


def _invalidate_downstream_caches(prev_snapshot_date: str | None, new_snapshot_date: str) -> None:
    """
    Delete the raw + enriched parquet caches when they were built from a
    different (or unknown) snapshot than the one now being staged.

    build_parquet_cache() is idempotent on partition EXISTENCE only, never on
    which snapshot produced them -- without this, restaging a newer snapshot
    silently leaves analysis reading the old one under the new date stamp.
    Both caches are gitignored and fully reconstructable from snapshots/*.gz,
    so deleting them is safe; the next pipeline run rebuilds from the newly
    staged .txt files.
    """
    if prev_snapshot_date == new_snapshot_date:
        return  # restaging the same snapshot; caches (if any) are still valid

    reason = (
        f"no prior snapshot marker (legacy state)" if prev_snapshot_date is None
        else f"staged snapshot changed {prev_snapshot_date} -> {new_snapshot_date}"
    )

    removed_any = False
    if PARQUET_DIR.exists():
        partition_dirs = [p for p in PARQUET_DIR.iterdir() if p.is_dir()]
        for p in partition_dirs:
            shutil.rmtree(p)
        if partition_dirs:
            removed_any = True
            print(f"  Invalidated {len(partition_dirs)} raw parquet partition(s) "
                  f"under {PARQUET_DIR} ({reason}). Gitignored + reconstructable "
                  f"from snapshots/*.gz.")

    if ENRICHED_CACHE.exists():
        ENRICHED_CACHE.unlink()
        removed_any = True
        print(f"  Invalidated enriched cache {ENRICHED_CACHE} ({reason}). "
              f"Gitignored + reconstructable from snapshots/*.gz.")

    if not removed_any:
        print(f"  No existing parquet cache to invalidate ({reason}).")


# The 4 canonical gz names that define a "complete" snapshot.
SWVF_GZ_NAMES = [
    "SWVF_1_22.txt.gz",
    "SWVF_23_44.txt.gz",
    "SWVF_45_66.txt.gz",
    "SWVF_67_88.txt.gz",
]

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


# ── Snapshot descriptor ───────────────────────────────────────────────────────
@dataclass(frozen=True)
class SnapshotInfo:
    """One discovered snapshot folder under snapshots/."""
    date:        str    # "YYYY-MM-DD" (folder name)
    path:        Path   # snapshots/<date>/
    complete:    bool   # all 4 SWVF_GZ_NAMES present
    total_bytes: int    # sum of the present gz file sizes
    is_staged:   bool   # this snapshot is the one currently in STAGING_DIR

    @property
    def gz_paths(self) -> list[Path]:
        """Absolute paths to the 4 canonical gz files (whether present or not)."""
        return [self.path / name for name in SWVF_GZ_NAMES]


# ── Discovery ─────────────────────────────────────────────────────────────────
def _snapshot_complete(folder: Path) -> bool:
    return all((folder / name).is_file() for name in SWVF_GZ_NAMES)


def _snapshot_bytes(folder: Path) -> int:
    return sum((folder / name).stat().st_size
               for name in SWVF_GZ_NAMES if (folder / name).is_file())


def list_snapshots() -> list[SnapshotInfo]:
    """
    Discover every YYYY-MM-DD folder under snapshots/, newest first.

    Non-date-named folders and files are ignored, so sibling caches
    (parquet/, Geo/, County Data Files/, ...) never leak into the list.
    """
    if not SNAPSHOTS_DIR.exists():
        return []

    staged = staged_info()
    staged_date = staged.get("snapshot_date") if staged else None

    snaps: list[SnapshotInfo] = []
    for child in SNAPSHOTS_DIR.iterdir():
        if not child.is_dir() or not _DATE_RE.match(child.name):
            continue
        snaps.append(SnapshotInfo(
            date=child.name,
            path=child,
            complete=_snapshot_complete(child),
            total_bytes=_snapshot_bytes(child),
            is_staged=(child.name == staged_date),
        ))

    snaps.sort(key=lambda s: s.date, reverse=True)
    return snaps


def resolve(selector: str) -> SnapshotInfo:
    """
    Resolve a selector to a complete SnapshotInfo, or raise (loud).

    Accepts "latest" (newest complete snapshot) or an exact "YYYY-MM-DD".
    """
    snaps = list_snapshots()
    if not snaps:
        raise FileNotFoundError(
            f"No snapshots found under {SNAPSHOTS_DIR}. Add a dated folder with "
            f"the 4 SWVF_*.txt.gz files (see snapshots/README.md)."
        )

    if selector == "latest":
        for snap in snaps:  # already newest-first
            if snap.complete:
                return snap
        raise FileNotFoundError(
            f"No COMPLETE snapshot under {SNAPSHOTS_DIR} (need all 4 of "
            f"{', '.join(SWVF_GZ_NAMES)})."
        )

    if not _DATE_RE.match(selector):
        raise ValueError(
            f"Snapshot selector {selector!r} is not 'latest' or 'YYYY-MM-DD'."
        )

    match = next((s for s in snaps if s.date == selector), None)
    if match is None:
        available = ", ".join(s.date for s in snaps) or "(none)"
        raise FileNotFoundError(
            f"Snapshot {selector} not found under {SNAPSHOTS_DIR}. "
            f"Available: {available}"
        )
    if not match.complete:
        raise FileNotFoundError(
            f"Snapshot {selector} is INCOMPLETE (needs all 4 of "
            f"{', '.join(SWVF_GZ_NAMES)}); refusing to stage."
        )
    return match


# ── Decompression (moved here from ohio_voter_pipeline.py) ─────────────────────
def decompress_gz(gz_path: Path, out_dir: Path) -> Path:
    """
    Decompress gz_path into out_dir; return the path to the decompressed file.

    Writes to a .tmp sibling and atomically replaces the target so a partial
    decompression can never masquerade as a complete staged file.
    """
    out_path = out_dir / gz_path.name.removesuffix(".gz")
    tmp_path = out_path.with_suffix(out_path.suffix + ".tmp")
    print(f"  Decompressing -> {out_path.name} ...", end="", flush=True)
    with gzip.open(gz_path, "rb") as f_in, open(tmp_path, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out, length=8 * 1024 * 1024)
    tmp_path.replace(out_path)
    size = out_path.stat().st_size
    print(f"\r  OK {out_path.name}  ({size/1e9:.2f} GB)              ")
    return out_path


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _atomic_write_json(dest: Path, payload: dict) -> None:
    tmp = dest.with_suffix(dest.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(dest)


# ── Staging ───────────────────────────────────────────────────────────────────
def staged_info() -> dict | None:
    """Return the parsed staged_from.json, or None if nothing is staged."""
    if not STAGED_FROM.exists():
        return None
    return json.loads(STAGED_FROM.read_text(encoding="utf-8"))


def _staged_txt_paths() -> list[Path]:
    """The 4 decompressed .txt paths in the staging dir (order matches SWVF_GZ_NAMES)."""
    return [STAGING_DIR / name.removesuffix(".gz") for name in SWVF_GZ_NAMES]


def _staging_matches(snap: SnapshotInfo) -> bool:
    """
    True when the staging dir already holds exactly `snap`: staged_from.json
    records this date AND every recorded txt file is present at its recorded size.
    """
    info = staged_info()
    if not info or info.get("snapshot_date") != snap.date:
        return False
    recorded = info.get("staged_txt", {})
    if set(recorded) != {p.name for p in _staged_txt_paths()}:
        return False
    for txt in _staged_txt_paths():
        if not txt.is_file() or txt.stat().st_size != recorded.get(txt.name):
            return False
    return True


def stage(snap: SnapshotInfo, force: bool = False) -> list[Path]:
    """
    Decompress `snap`'s 4 gz files into STAGING_DIR and write staged_from.json.

    Idempotent: if the staging dir already holds exactly this snapshot (matching
    staged_from.json date and all txt sizes), skip the work and return the
    existing paths.  Pass force=True to restage regardless.

    Returns the 4 decompressed .txt paths (order matches SWVF_GZ_NAMES).
    """
    if not snap.complete:
        raise FileNotFoundError(
            f"Refusing to stage INCOMPLETE snapshot {snap.date} "
            f"(needs all 4 of {', '.join(SWVF_GZ_NAMES)})."
        )

    # Capture the snapshot the CURRENT parquet cache was (presumably) built
    # from -- i.e. whatever staged_from.json said before this call overwrites
    # it -- so a snapshot change can be detected even though staged_from.json
    # itself is about to be replaced below.
    _prev_info = staged_info()
    _prev_snapshot_date = _prev_info.get("snapshot_date") if _prev_info else None

    if not force and _staging_matches(snap):
        print(f"  Snapshot {snap.date} already staged, skipping decompression.")
        return _staged_txt_paths()

    STAGING_DIR.mkdir(parents=True, exist_ok=True)

    print(f"  Staging snapshot {snap.date} -> {STAGING_DIR}")
    txt_paths: list[Path] = []
    gz_sha: dict[str, str] = {}
    for gz_path in snap.gz_paths:
        gz_sha[gz_path.name] = _sha256(gz_path)
        txt_paths.append(decompress_gz(gz_path, STAGING_DIR))

    payload = {
        "snapshot_date": snap.date,
        "staged_at":     datetime.now().isoformat(timespec="seconds"),
        "source_gz":     gz_sha,
        "staged_txt":    {p.name: p.stat().st_size for p in txt_paths},
    }
    _atomic_write_json(STAGED_FROM, payload)
    print(f"  OK staged_from.json written ({snap.date})")

    _invalidate_downstream_caches(_prev_snapshot_date, snap.date)

    return txt_paths


# ── Table rendering (shared by --list-snapshots and the interactive menu) ──────
def format_snapshot_table(snaps: list[SnapshotInfo] | None = None) -> str:
    """Return an ASCII table of discovered snapshots (newest first)."""
    if snaps is None:
        snaps = list_snapshots()
    if not snaps:
        return f"  (no snapshots under {SNAPSHOTS_DIR})"

    newest_complete = next((s.date for s in snaps if s.complete), None)
    lines = [f"  {'#':>2}  {'DATE':<12} {'STATUS':<10} {'SIZE':>9}  MARKERS",
             f"  {'-'*2}  {'-'*12} {'-'*10} {'-'*9}  {'-'*18}"]
    for i, s in enumerate(snaps, 1):
        status  = "complete" if s.complete else "INCOMPLETE"
        size    = f"{s.total_bytes/1e6:.0f} MB" if s.total_bytes else "-"
        markers = []
        if s.is_staged:
            markers.append("staged")
        if s.date == newest_complete:
            markers.append("newest")
        lines.append(f"  {i:>2}  {s.date:<12} {status:<10} {size:>9}  {', '.join(markers)}")
    return "\n".join(lines)
