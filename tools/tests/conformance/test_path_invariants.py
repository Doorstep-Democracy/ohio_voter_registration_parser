"""CONFORMANCE 5 -- the resolver's own paths are structurally sound.

Everything else in this suite polices where paths are DEFINED. This one
polices whether the surviving definitions are CORRECT: rooted in the repo,
consistently nested, portable across Windows and POSIX, and independent of the
current working directory.

The CWD test is the one worth reading twice. CLAUDE.md section 6 says "do not
rely on CWD" precisely because relying on it has bitten before, and the failure
is invisible from inside a normal test run -- pytest happens to run from the
repo root, so a CWD-dependent constant resolves correctly right up until a
scheduled task or an SSH session runs the same code from somewhere else. It is
checked by launching a subprocess from a temp directory, because reloading the
module in-process would not re-evaluate module-level constants the same way.

EXPECTED STATUS TODAY: SKIPPED -- pipeline/paths.py does not exist yet. These
become the acceptance criteria for DOO-86.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys

import pytest

from conftest import ROOT

PATHS_MODULE = ROOT / "pipeline" / "paths.py"

# Constants pipeline/paths.py is expected to export. Kept here rather than
# discovered, so that quietly dropping one is a test failure and not a silent
# reduction in coverage.
REQUIRED_CONSTANTS = ("BASE_DIR", "DOCS_DIR", "DATA_DIR", "SOURCE_DIR", "PARQUET_DIR", "ENRICHED_CACHE")


def _paths_module():
    if not PATHS_MODULE.exists():
        pytest.skip("pipeline/paths.py does not exist yet (DOO-86)")
    sys.path.insert(0, str(ROOT))
    try:
        from pipeline import paths  # type: ignore

        return paths
    except Exception as exc:  # pragma: no cover
        pytest.fail(f"pipeline/paths.py exists but does not import: {exc}")


def test_exports_the_required_constants():
    paths = _paths_module()
    missing = [name for name in REQUIRED_CONSTANTS if not hasattr(paths, name)]
    assert not missing, f"pipeline/paths.py is missing required constants: {missing}"


def test_all_paths_are_absolute_and_inside_the_repo():
    paths = _paths_module()
    problems = []
    for name in REQUIRED_CONSTANTS:
        value = getattr(paths, name, None)
        if value is None:
            continue
        if not value.is_absolute():
            problems.append(f"{name} is not absolute: {value}")
            continue
        if ".." in value.parts:
            problems.append(f"{name} contains a '..' traversal: {value}")
        try:
            value.relative_to(ROOT)
        except ValueError:
            problems.append(f"{name} resolves outside the repo root: {value}")
    assert not problems, "\n".join(problems)


def test_directory_nesting_is_consistent():
    """DATA_DIR under DOCS_DIR under BASE_DIR, PARQUET_DIR under SOURCE_DIR.
    Cheap, and it catches the copy-paste error where one constant is rebuilt
    from the root instead of from its parent and the two silently diverge.
    """
    paths = _paths_module()
    assert paths.DOCS_DIR.is_relative_to(paths.BASE_DIR), "DOCS_DIR must sit under BASE_DIR"
    assert paths.DATA_DIR.is_relative_to(paths.DOCS_DIR), "DATA_DIR must sit under DOCS_DIR"
    assert paths.SOURCE_DIR.is_relative_to(paths.BASE_DIR), "SOURCE_DIR must sit under BASE_DIR"
    assert paths.PARQUET_DIR.is_relative_to(paths.SOURCE_DIR), "PARQUET_DIR must sit under SOURCE_DIR"


def test_no_hardcoded_drive_letters_or_absolute_roots():
    """A literal D:\\ or /home/ in the resolver makes the repo unclonable
    anywhere else -- and this project is worked from Windows, from an SSH
    session, and from a Linux sandbox.
    """
    if not PATHS_MODULE.exists():
        pytest.skip("pipeline/paths.py does not exist yet (DOO-86)")
    src = PATHS_MODULE.read_text(encoding="utf-8", errors="replace")
    bad = []
    for token in (":\\", ":/", "/home/", "/Users/", "/mnt/", "/sessions/"):
        if token in src:
            bad.append(token)
    # A drive letter can also appear as a raw string; catch the common shapes.
    assert not bad, f"pipeline/paths.py contains machine-specific path fragments: {bad}"


def test_paths_are_independent_of_cwd(tmp_path):
    """Resolve the constants from a foreign working directory in a clean
    subprocess and require identical values. This is the assertion CLAUDE.md
    section 6 has only ever had as prose.
    """
    if not PATHS_MODULE.exists():
        pytest.skip("pipeline/paths.py does not exist yet (DOO-86)")
    program = (
        "import json;from pipeline import paths;"
        "print(json.dumps({n: str(getattr(paths, n)) for n in "
        f"{list(REQUIRED_CONSTANTS)!r} if hasattr(paths, n)}}))"
    )
    env = dict(os.environ, PYTHONPATH=str(ROOT))
    from_root = subprocess.run(
        [sys.executable, "-c", program], cwd=str(ROOT), env=env, capture_output=True, text=True
    )
    from_elsewhere = subprocess.run(
        [sys.executable, "-c", program], cwd=str(tmp_path), env=env, capture_output=True, text=True
    )
    assert from_root.returncode == 0, f"import failed from repo root:\n{from_root.stderr}"
    assert from_elsewhere.returncode == 0, (
        "pipeline.paths failed to import from a foreign working directory -- "
        f"this is CWD dependence:\n{from_elsewhere.stderr}"
    )
    a, b = json.loads(from_root.stdout), json.loads(from_elsewhere.stdout)
    differing = {k: (a[k], b[k]) for k in a if a[k] != b.get(k)}
    assert not differing, f"path constants change with the working directory: {differing}"
