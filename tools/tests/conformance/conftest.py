"""Shared collectors for the conformance suite.

These helpers are deliberately stdlib-only (ast / pathlib / re / json). The
conformance suite must run on a fresh clone with no Polars, no DuckDB, and no
parquet cache present, because its job is to police source-tree structure --
not data. Anything here that needs real data must degrade to pytest.skip with
an explicit reason, never to a silent pass.

Path resolution follows CLAUDE.md section 6: resolve from the file, never CWD.
This module lives at tools/tests/conformance/, so the repo root is parents[3].
"""

from __future__ import annotations

import ast
import json
import re
from dataclasses import dataclass
from pathlib import Path

import pytest

# tools/tests/conformance/conftest.py -> conformance -> tests -> tools -> ROOT
ROOT = Path(__file__).resolve().parents[3]

# Directories scanned for Python source. Kept narrow on purpose: local/ is
# gitignored scratch, docs/ is the published web root, .venv is vendored.
SCAN_DIRS = ("pipeline", "tools", "serve")

# Paths never treated as offenders.
#   pipeline/paths.py  -- the single resolver this suite exists to protect
#   this directory     -- the tests necessarily mention the patterns they detect
#   tools/admin/fingerprint_audit.py -- excluded 2026-11-13 (DOO-86). Its four
#       flagged sites are audit output files named *.parquet under its own
#       local/working/fingerprint_audit/ folder; the "parquet" family matches the
#       file suffix, not the managed raw cache. Moving a one-off audit folder into
#       pipeline/paths.py would grow the resolver for no consumer. Cost: a future
#       managed path added to this one file would go undetected.
ALLOWLIST_RELPATHS = frozenset(
    {
        "pipeline/paths.py",
        "tools/tests/conformance/conftest.py",
        "tools/admin/fingerprint_audit.py",
    }
)
ALLOWLIST_DIR_PREFIXES = ("tools/tests/conformance/",)

EXCLUDE_DIR_NAMES = frozenset({"__pycache__", ".venv", ".git", "node_modules", ".pytest_cache"})

# The managed path families. A string-token join that matches one of these is
# a path this project has decided belongs to exactly one resolver.
MANAGED_PATTERNS = (
    ("docs_data", re.compile(r"(?=.*\bdocs\b)(?=.*\bdata\b)")),
    ("parquet_enriched", re.compile(r"parquet_enriched|enriched_voters")),
    ("parquet", re.compile(r"\bparquet\b")),
    ("txt_source", re.compile(r"state voter files")),
    ("source_root", re.compile(r"(?=.*\blocal\b)(?=.*\bsource\b)")),
)

# Partition-key construction. One canonical form, one resolver.
PARTITION_TOKEN = "COUNTY_NUMBER="
PARTITION_DIR_RE = re.compile(r"^COUNTY_NUMBER=\d{2}$")

# Families the consolidation actually covers, and which the suite ASSERTS on.
# `source_root` is detected and reported but not asserted: local/source/ also
# holds Geo shapefiles, precinct_keys and Official Docs, which are outside this
# refactor. Asserting on it would make the gate fail for reasons the work was
# never scoped to fix -- and a gate that fails for out-of-scope reasons is a
# gate people learn to ignore.
CORE_FAMILIES = frozenset({"docs_data", "parquet", "parquet_enriched", "txt_source"})

MANIFEST_PATH = Path(__file__).resolve().parent / "scope_manifest.json"

VALID_BUCKETS = frozenset({"safe-mechanical", "needs-judgment", "broken-or-dead", ""})
VALID_STATUSES = frozenset({"pending", "converted", "excluded"})


@dataclass(frozen=True)
class Finding:
    """One detected site. `scope` is 'module' for a module-level assignment,
    'inline' for one built inside a function body -- the distinction the
    original audit missed on its first pass (see README, Test 1)."""

    relpath: str
    lineno: int
    name: str
    family: str
    scope: str
    snippet: str

    def __str__(self) -> str:  # pragma: no cover - diagnostic only
        return f"{self.relpath}:{self.lineno}  {self.name} [{self.family}/{self.scope}]  {self.snippet}"


def rel(path: Path) -> str:
    """Repo-relative, forward-slashed. Stable across Windows and POSIX so the
    manifest and the failure output can be compared by eye and by string."""
    return path.resolve().relative_to(ROOT).as_posix()


def is_allowlisted(relpath: str) -> bool:
    return relpath in ALLOWLIST_RELPATHS or relpath.startswith(ALLOWLIST_DIR_PREFIXES)


def iter_source_files() -> list[Path]:
    """Every .py file under SCAN_DIRS, excluding caches and vendored trees."""
    out: list[Path] = []
    for d in SCAN_DIRS:
        base = ROOT / d
        if not base.is_dir():
            continue
        for p in base.rglob("*.py"):
            if EXCLUDE_DIR_NAMES.intersection(p.parts):
                continue
            out.append(p)
    return sorted(out)


def parse_module(path: Path) -> ast.Module | None:
    """Parse, tolerating unreadable/unparseable files. A parse failure is
    reported by test_scope_manifest.py rather than crashing every test."""
    try:
        return ast.parse(path.read_text(encoding="utf-8", errors="replace"), filename=str(path))
    except SyntaxError:
        return None


def _string_tokens(node: ast.AST) -> list[str]:
    """Every string constant in a subtree, in source order. Catches
    BASE_DIR / 'docs' / 'data' just as well as a bare 'docs/data' literal."""
    return [n.value for n in ast.walk(node) if isinstance(n, ast.Constant) and isinstance(n.value, str)]


def _classify(tokens: list[str]) -> str | None:
    if not tokens:
        return None
    joined = " ".join(tokens).lower()
    for family, pattern in MANAGED_PATTERNS:
        if pattern.search(joined):
            return family
    return None


# Names whose subtrees are pytest temp dirs, not managed project paths. A
# fixture building tmp_path / 'parquet' is correct code, not a second copy.
TEMP_FIXTURE_NAMES = frozenset({"tmp_path", "tmpdir", "tmp_path_factory", "tmpdir_factory"})


def _uses_temp_fixture(node: ast.AST) -> bool:
    return any(isinstance(n, ast.Name) and n.id in TEMP_FIXTURE_NAMES for n in ast.walk(node))


def _is_path_expression(node: ast.AST) -> bool:
    """True when the assignment's own right-hand side builds a filesystem path.

    Checked on the top-level node only, deliberately. Testing descendants
    instead would flag `df = pl.read_parquet(str(PARQUET_DIR) + '/**/*.parquet')`
    -- a data read, not a path definition -- and would flag any function call
    whose docstring or argument merely mentions the word parquet.
    """
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Div):
        return True
    if isinstance(node, ast.Call):
        func = node.func
        name = func.id if isinstance(func, ast.Name) else getattr(func, "attr", "")
        # os.path.join is the pre-pathlib idiom (build_montgomery_demo.py used
        # it until that script was removed on 2026-11-13). Omitting it is how a
        # scanner reports a clean tree that isn't one.
        return name in {"Path", "PurePath", "PosixPath", "WindowsPath", "join"}
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return "/" in node.value or "\\" in node.value
    if isinstance(node, ast.JoinedStr):
        return any(
            isinstance(v, ast.Constant) and isinstance(v.value, str) and ("/" in v.value or "\\" in v.value)
            for v in node.values
        )
    return False


def _assign_targets(node: ast.AST) -> list[str]:
    if isinstance(node, ast.Assign):
        return [t.id for t in node.targets if isinstance(t, ast.Name)]
    if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
        return [node.target.id]
    return []


def _snippet(node: ast.AST) -> str:
    try:
        text = ast.unparse(node)
    except Exception:  # pragma: no cover - defensive
        return "<unparseable>"
    text = " ".join(text.split())
    return text if len(text) <= 110 else text[:107] + "..."


def collect_path_constants(paths: list[Path] | None = None) -> list[Finding]:
    """Every assignment that constructs a managed path, anywhere in the file.

    Both module-level constants and inline construction inside a function are
    reported. Restricting to module level is exactly the mistake that made the
    first audit pass undercount (ohio_voter_pipeline.py builds its docs/data
    path inside a function body, invisible to a DATA_DIR-name grep).
    """
    findings: list[Finding] = []
    for path in paths if paths is not None else iter_source_files():
        relpath = rel(path)
        if is_allowlisted(relpath):
            continue
        tree = parse_module(path)
        if tree is None:
            continue
        module_level_nodes = set(id(n) for n in tree.body)

        # Per-module symbol table: name -> the string tokens its path is built
        # from. Needed because the path is often assembled across statements --
        # DOCS = BASE / "docs" then DATA_DIR = DOCS / "data". Neither line alone
        # contains both tokens, so a per-expression matcher sees nothing while a
        # second copy of docs/data sits in plain sight.
        symbols: dict[str, list[str]] = {}
        assigns = sorted(
            (n for n in ast.walk(tree) if isinstance(n, (ast.Assign, ast.AnnAssign))),
            key=lambda n: getattr(n, "lineno", 0),
        )
        reported_lines: set[int] = set()
        for node in assigns:
            value = node.value
            if value is None:
                continue
            names = _assign_targets(node) or ["<unnamed>"]
            if not _is_path_expression(value) or _uses_temp_fixture(value):
                continue
            tokens = list(_string_tokens(value))
            for ref in ast.walk(value):
                if isinstance(ref, ast.Name) and ref.id in symbols:
                    tokens = symbols[ref.id] + tokens
            for name in names:
                symbols[name] = tokens
            family = _classify(tokens)
            if family is None:
                continue
            scope = "module" if id(node) in module_level_nodes else "inline"
            lineno = getattr(node, "lineno", 0)
            reported_lines.add(lineno)
            for name in names:
                findings.append(
                    Finding(
                        relpath=relpath,
                        lineno=lineno,
                        name=name,
                        family=family,
                        scope=scope,
                        snippet=_snippet(node),
                    )
                )

        # Orphan literals: a managed path written as a bare string somewhere
        # other than an assignment -- a dict value, a call argument. Requires a
        # separator and no whitespace so prose that quotes a path is not flagged.
        for node in ast.walk(tree):
            if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
                continue
            text = node.value
            lineno = getattr(node, "lineno", 0)
            if lineno in reported_lines or "/" not in text or any(ch.isspace() for ch in text):
                continue
            family = _classify([text])
            if family is None:
                continue
            findings.append(
                Finding(
                    relpath=relpath,
                    lineno=lineno,
                    name="<literal>",
                    family=family,
                    scope="literal",
                    snippet=_snippet(node),
                )
            )
    return sorted(findings, key=lambda f: (f.relpath, f.lineno))


def _partition_literal(node: ast.AST) -> bool:
    """True when this node builds a partition NAME, not merely mentions one.

    The discriminator is whitespace. A partition name and a partition glob are
    single tokens ('COUNTY_NUMBER=07', 'COUNTY_NUMBER=*/part-0.parquet'); a
    docstring or an error message that happens to quote one is prose and always
    carries spaces. Without this rule the suite flags voter_data_cleaner.py's
    module docstring and emit_county_fingerprint.py's stderr string, neither of
    which is a construction site.
    """
    if isinstance(node, ast.Constant):
        return (
            isinstance(node.value, str)
            and PARTITION_TOKEN in node.value
            and not any(ch.isspace() for ch in node.value)
        )
    if isinstance(node, ast.JoinedStr):
        parts = [v.value for v in node.values if isinstance(v, ast.Constant) and isinstance(v.value, str)]
        if not any(PARTITION_TOKEN in p for p in parts):
            return False
        return not any(ch.isspace() for p in parts for ch in p)
    return False


def collect_partition_strings(paths: list[Path] | None = None) -> list[Finding]:
    """Every literal or f-string that builds a 'COUNTY_NUMBER=' partition name."""
    findings: list[Finding] = []
    for path in paths if paths is not None else iter_source_files():
        relpath = rel(path)
        if is_allowlisted(relpath):
            continue
        tree = parse_module(path)
        if tree is None:
            continue
        # An f-string reports twice under ast.walk -- once as the JoinedStr and
        # once as its literal Constant part. Record the f-string lines first and
        # let the JoinedStr own them, so counts are sites, not node visits.
        joined_lines = {
            getattr(n, "lineno", 0)
            for n in ast.walk(tree)
            if isinstance(n, ast.JoinedStr) and _partition_literal(n)
        }
        for node in ast.walk(tree):
            hit = False
            if isinstance(node, ast.JoinedStr):
                hit = _partition_literal(node)
            elif isinstance(node, ast.Constant) and isinstance(node.value, str):
                hit = _partition_literal(node) and getattr(node, "lineno", 0) not in joined_lines
            if hit:
                findings.append(
                    Finding(
                        relpath=relpath,
                        lineno=getattr(node, "lineno", 0),
                        name=PARTITION_TOKEN,
                        family="partition",
                        scope="literal",
                        snippet=_snippet(node),
                    )
                )
    return sorted(findings, key=lambda f: (f.relpath, f.lineno))


def load_manifest() -> dict:
    if not MANIFEST_PATH.exists():
        pytest.fail(f"scope manifest missing: {rel(MANIFEST_PATH)}")
    return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


def manifest_relpaths(manifest: dict | None = None) -> set[str]:
    m = manifest if manifest is not None else load_manifest()
    return {entry["path"] for entry in m["files"]}


def report(findings: list[Finding], header: str) -> str:
    """Failure output is the deliverable here -- these tests are meant to be
    read, not merely to go red. Group by file, count by family."""
    lines = [header, ""]
    by_file: dict[str, list[Finding]] = {}
    for f in findings:
        by_file.setdefault(f.relpath, []).append(f)
    for relpath in sorted(by_file):
        lines.append(f"  {relpath}")
        for f in by_file[relpath]:
            lines.append(f"      line {f.lineno:>5}  {f.name:<24} [{f.family}/{f.scope}]")
            lines.append(f"                    {f.snippet}")
    families: dict[str, int] = {}
    for f in findings:
        families[f.family] = families.get(f.family, 0) + 1
    lines.append("")
    lines.append(f"  distinct files : {len(by_file)}")
    lines.append(f"  total sites    : {len(findings)}")
    lines.append(f"  by family      : {families}")
    return "\n".join(lines)


# Fixtures ------------------------------------------------------------------


@pytest.fixture(scope="session")
def source_files() -> list[Path]:
    return iter_source_files()


@pytest.fixture(scope="session")
def path_constant_findings() -> list[Finding]:
    return collect_path_constants()


@pytest.fixture(scope="session")
def partition_findings() -> list[Finding]:
    return collect_partition_strings()


@pytest.fixture(scope="session")
def manifest() -> dict:
    return load_manifest()
