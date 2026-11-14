"""
paths.py
========
Single resolver for the pipeline's filesystem layout (CLAUDE.md section 5).
Every managed location is defined here once; every other module imports it.

Layout (relative to the repo root; resolved from __file__, never the CWD)
    docs/                                   DOCS_DIR     public web root
    docs/data/                              DATA_DIR     compiled dashboard JSON
    local/source/                           SOURCE_DIR   raw inputs (gitignored)
    local/source/<state txt dir>/           TXT_DIR      staged raw voter files
    local/source/parquet/                   PARQUET_DIR  Hive partitions, COUNTY_NUMBER=NN
    local/source/parquet_enriched/          PARQUET_ENRICHED_DIR
        enriched_voters.parquet             ENRICHED_CACHE
    local/logs/                             LOGS_DIR
    local/output/                           OUTPUT_DIR

Invariants
    * Import has no filesystem side effects. Nothing is created here; the
      module that owns a directory creates it.
    * Inputs and caches under local/ are never redirected.
    * Only the output root (DOCS_DIR, with DATA_DIR under it) can move.

Output override
    ELECTION_DATA_OUT=<dir> replaces DOCS_DIR for the process, so a trial run
    cannot write into the published tree. The value is resolved to an absolute
    path and must be an existing directory, else NotADirectoryError at import.
    It is never created: a typo must fail, not scatter voter-derived JSON into
    a new location. Child processes inherit it through the environment.
    OUTPUT_OVERRIDDEN reports whether it is active; log it at run start.
    With the override set DOCS_DIR is outside the repo by design. The
    conformance suite's inside-the-repo check covers the default layout only.

State
    STATE selects per-state layout names. Only "OH" is registered. An
    unregistered state raises KeyError at import rather than guessing a path.

Troubleshooting
    Output landed somewhere unexpected -> check the "Output root" line at the
    top of the run log, then the ELECTION_DATA_OUT value in that shell.
    A script still writes to docs/data under the override -> it builds its own
    path instead of importing this module (tools/tests/conformance lists them).
"""

from __future__ import annotations

import os
from pathlib import Path

OUTPUT_ENV_VAR = "ELECTION_DATA_OUT"

DEFAULT_STATE = "OH"
STATE = DEFAULT_STATE

# state -> name of the staged raw-voter-file directory under SOURCE_DIR.
_TXT_DIRNAME_BY_STATE = {"OH": "State Voter Files"}

# Hive partition key of the raw parquet cache. It is a directory-name key, not
# a column inside the parquet files (CLAUDE.md section 5).
PARTITION_KEY = "COUNTY_NUMBER"

BASE_DIR = Path(__file__).resolve().parent.parent


def resolve_output_root(base_dir: Path, env: dict[str, str] | None = None) -> tuple[Path, bool]:
    """Return (output root, overridden?) for `base_dir`.

    Default: base_dir/docs, False. With ELECTION_DATA_OUT set: that directory
    resolved absolute, True. Raises NotADirectoryError when the override names
    anything that is not an existing directory; the directory is never created.
    `env` defaults to os.environ and exists so tests can inject a mapping.
    """
    raw = (os.environ if env is None else env).get(OUTPUT_ENV_VAR, "").strip()
    if not raw:
        return base_dir / "docs", False
    root = Path(raw).expanduser().resolve()
    if not root.is_dir():
        raise NotADirectoryError(
            f"{OUTPUT_ENV_VAR}={raw!r} resolves to {root}, which is not an existing "
            "directory. Create it first or unset the variable; it is never created "
            "automatically."
        )
    return root, True


DOCS_DIR, OUTPUT_OVERRIDDEN = resolve_output_root(BASE_DIR)
DATA_DIR = DOCS_DIR / "data"

LOGS_DIR = BASE_DIR / "local" / "logs"
OUTPUT_DIR = BASE_DIR / "local" / "output"

SOURCE_DIR = BASE_DIR / "local" / "source"
TXT_DIR = SOURCE_DIR / _TXT_DIRNAME_BY_STATE[STATE]
PARQUET_DIR = SOURCE_DIR / "parquet"
PARQUET_ENRICHED_DIR = SOURCE_DIR / "parquet_enriched"
ENRICHED_CACHE = PARQUET_ENRICHED_DIR / "enriched_voters.parquet"


def county_partition(county_number: int | str) -> str:
    """Canonical raw-cache partition directory name: 'COUNTY_NUMBER=NN'.

    int 7, '7' and '07' all give 'COUNTY_NUMBER=07', matching the zero-padded
    directories on disk. Raises ValueError for a non-numeric or non-positive
    value: a malformed county number must not resolve to a real-looking path.
    """
    number = int(str(county_number).strip())
    if number < 1:
        raise ValueError(f"county number must be >= 1, got {county_number!r}")
    return f"{PARTITION_KEY}={number:02d}"


def parquet_glob(directory: Path) -> str:
    """Recursive glob string matching every parquet file under `directory`.

    The one spelling of this pattern. Pass PARQUET_DIR for the whole cache
    (read with hive_partitioning=True) or a single partition directory.
    """
    return str(directory) + "/**/*.parquet"
