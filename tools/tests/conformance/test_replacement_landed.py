"""CONFORMANCE 3 -- converted files actually import the resolver.

Test 1 proves no second copy remains. On its own that is satisfiable by simply
deleting a constant and inlining the literal at each use site, which passes the
letter of the rule and defeats its purpose. This test is the complement: a file
marked converted must import from pipeline.paths, so the constant was replaced
rather than merely removed.

EXPECTED STATUS TODAY: GREEN but vacuous -- no file is marked converted yet.
It acquires teeth one file at a time as the refactor lands, which is the point:
each conversion is independently verified rather than the batch being trusted.
"""

from __future__ import annotations

import ast

from conftest import CORE_FAMILIES, ROOT, collect_path_constants, parse_module

RESOLVER_MODULES = {"pipeline.paths", "paths"}


def _imports_resolver(tree: ast.Module) -> bool:
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and (node.module or "") in RESOLVER_MODULES:
            return True
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name in RESOLVER_MODULES:
                    return True
    return False


def test_converted_files_import_the_resolver(manifest):
    converted = [e["path"] for e in manifest["files"] if e.get("status") == "converted"]
    offenders: list[str] = []
    for relpath in converted:
        tree = parse_module(ROOT / relpath)
        if tree is None:
            offenders.append(f"{relpath}: does not parse")
            continue
        if not _imports_resolver(tree):
            offenders.append(f"{relpath}: marked converted but does not import pipeline.paths")
    if offenders:
        raise AssertionError(
            "Converted files must import the resolver, not just drop their copy:\n"
            + "\n".join(f"    {o}" for o in offenders)
        )


def test_converted_files_have_no_remaining_copies(manifest):
    """Belt and braces: a file can import the resolver and still keep its old
    constant beside it, which is the worst of both -- two live definitions, one
    of them now shadowed and silently unused.
    """
    converted = {e["path"] for e in manifest["files"] if e.get("status") == "converted"}
    if not converted:
        return
    residue = [
        f for f in collect_path_constants() if f.relpath in converted and f.family in CORE_FAMILIES
    ]
    if residue:
        from conftest import report

        raise AssertionError(report(residue, "Files marked converted still define a managed path:"))
