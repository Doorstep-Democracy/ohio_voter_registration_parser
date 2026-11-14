"""CONFORMANCE 6 -- docstrings, comments and docs do not reference things that
no longer exist.

This is the docstring/README half of the question "was everything adjusted".
It cannot verify that a docstring is TRUE -- that a rewritten function still
does what its prose claims -- but it can verify that every module, file and
path a docstring names actually exists. That catches the entire rename class
of staleness, which is the class this repo has already suffered.

The live example, and the reason this test exists: pipeline/voter_data_cleaner.py
was formerly voter_data_cleaner_v2.py at the repo root. The rename left
references behind in docstrings and comments across seven files, and -- worse --
two live module-level imports (tools/export/precinct_party_export.py,
tools/export/precinct_unc_export.py) plus a self-test in voter_data_cleaner.py
that all import a module which no longer exists. Those three are not stale
prose; they are broken code that fails at import, and nothing in the existing
test suite touches them.

EXPECTED STATUS TODAY: RED on both the dead-module scan and the broken-import
scan.
"""

from __future__ import annotations

import ast
import re

from conftest import ROOT, is_allowlisted, iter_source_files, parse_module, rel

# Modules known to have been renamed or removed. Add to this set whenever a
# module is renamed, in the same commit as the rename -- that is what turns a
# rename into a mechanically enforced migration instead of a scavenger hunt.
DEAD_MODULES = {
    "voter_data_cleaner_v2": "renamed to pipeline/voter_data_cleaner.py",
}

# Documentation scanned for stale file references. Deliberately limited to the
# committed, human-maintained docs: local/context/ is gitignored session
# history and is SUPPOSED to contain references to files as they were at the
# time, so policing it would be wrong as well as noisy.
DOC_FILES = (
    "README.md",
    "CLAUDE.md",
    "DATA_QUALITY.md",
    "GEMINI.md",
    ".claude/rules/frontend.md",
)

# Files permitted to mention a dead module: changelogs and this suite, which
# has to name the thing it detects.
DEAD_MODULE_ALLOWLIST = ("tools/tests/conformance/",)

PY_PATH_RE = re.compile(r"\b((?:pipeline|tools|serve|docs)/[A-Za-z0-9_./-]+\.py)\b")


def _mentions(path, needle: str) -> list[tuple[int, str]]:
    out = []
    for i, line in enumerate(path.read_text(encoding="utf-8", errors="replace").splitlines(), 1):
        if needle in line:
            out.append((i, line.strip()[:120]))
    return out


def test_no_source_file_references_a_dead_module():
    hits: list[str] = []
    for path in iter_source_files():
        relpath = rel(path)
        if relpath.startswith(DEAD_MODULE_ALLOWLIST):
            continue
        for module, reason in DEAD_MODULES.items():
            for lineno, text in _mentions(path, module):
                hits.append(f"{relpath}:{lineno}  ({reason})\n        {text}")
    if hits:
        raise AssertionError(
            f"{len(hits)} reference(s) to a renamed or removed module remain:\n"
            + "\n".join(f"    {h}" for h in hits)
        )


def test_no_source_file_imports_a_dead_module():
    """The subset of the above that is not cosmetic. A stale docstring is
    untidy; a stale import is a module that cannot load, and it will not be
    caught by any test that never imports it.
    """
    broken: list[str] = []
    for path in iter_source_files():
        relpath = rel(path)
        if relpath.startswith(DEAD_MODULE_ALLOWLIST):
            continue
        tree = parse_module(path)
        if tree is None:
            continue
        for node in ast.walk(tree):
            names: list[str] = []
            if isinstance(node, ast.Import):
                names = [a.name.split(".")[0] for a in node.names]
            elif isinstance(node, ast.ImportFrom):
                names = [(node.module or "").split(".")[0]]
            elif (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "__import__"
                and node.args
                and isinstance(node.args[0], ast.Constant)
            ):
                names = [str(node.args[0].value)]
            for name in names:
                if name in DEAD_MODULES:
                    broken.append(f"{relpath}:{getattr(node, 'lineno', 0)}  imports {name}")
    if broken:
        raise AssertionError(
            "Live imports of a module that no longer exists (these files cannot "
            "load at all):\n" + "\n".join(f"    {b}" for b in broken)
        )


def test_documented_python_paths_exist():
    """Every pipeline/tools/serve/docs .py path named in the committed docs must
    exist on disk. This is what keeps README and CLAUDE.md honest through a
    refactor that moves files around.
    """
    missing: list[str] = []
    for doc in DOC_FILES:
        doc_path = ROOT / doc
        if not doc_path.exists():
            continue
        for i, line in enumerate(doc_path.read_text(encoding="utf-8", errors="replace").splitlines(), 1):
            for match in PY_PATH_RE.findall(line):
                if not (ROOT / match).exists():
                    missing.append(f"{doc}:{i}  references missing file {match}")
    if missing:
        raise AssertionError(
            "Documentation references Python files that do not exist:\n"
            + "\n".join(f"    {m}" for m in missing)
        )


def test_module_docstrings_do_not_name_missing_files():
    """Same check, applied to module docstrings rather than markdown. A module
    whose own docstring points at a file that was deleted is the most common
    residue of a partial migration.
    """
    missing: list[str] = []
    for path in iter_source_files():
        relpath = rel(path)
        if is_allowlisted(relpath):
            continue
        tree = parse_module(path)
        if tree is None:
            continue
        doc = ast.get_docstring(tree)
        if not doc:
            continue
        # Deduplicated: a docstring naming the same missing file eight times is
        # one defect, and eight identical failure lines hide the other seven
        # files underneath them.
        for match in sorted(set(PY_PATH_RE.findall(doc))):
            if not (ROOT / match).exists():
                missing.append(f"{relpath}: docstring references missing file {match}")
    if missing:
        raise AssertionError(
            "Module docstrings reference Python files that do not exist:\n"
            + "\n".join(f"    {m}" for m in missing)
        )
