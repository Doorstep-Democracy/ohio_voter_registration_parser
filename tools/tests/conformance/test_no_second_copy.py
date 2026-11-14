"""CONFORMANCE 1 -- no managed path is constructed outside pipeline/paths.py.

This is the enforcement mechanism for CLAUDE.md section 5 (single-resolver
logic, as amended 2026-08-01 to cover infrastructure constants). The rule
existed as prose and nine naming conventions appeared anyway; a rule with a
failing test behind it is a different kind of object.

EXPECTED STATUS TODAY: RED. It goes green when the path-consolidation work
lands, and it is the definition of "done" for that work -- not a markdown
checklist, not a reviewer's recollection.
"""

from __future__ import annotations

from conftest import CORE_FAMILIES, report


def test_no_managed_path_constructed_outside_resolver(path_constant_findings):
    core = [f for f in path_constant_findings if f.family in CORE_FAMILIES]
    extended = [f for f in path_constant_findings if f.family not in CORE_FAMILIES]
    if not core:
        return
    detail = report(core, "Managed path constructed outside pipeline/paths.py:")
    if extended:
        ext_files = sorted({f.relpath for f in extended})
        detail += (
            "\n\n  Not asserted, shown for scope awareness -- "
            f"{len(extended)} site(s) in {len(ext_files)} file(s) build a local/source/\n"
            "  path outside the four core families (Geo, precinct_keys, Official Docs):\n"
            + "\n".join(f"      {p}" for p in ext_files)
        )
    raise AssertionError(detail)


def test_no_inline_path_construction_in_pipeline(path_constant_findings):
    """Inline construction inside a function body is the variant that hides from
    a constant-name grep -- it is how the original audit undercounted on its
    first pass. Asserted separately from module-level so the failure output
    names the harder-to-find class on its own.
    """
    inline = [
        f
        for f in path_constant_findings
        if f.scope == "inline" and f.family in CORE_FAMILIES and f.relpath.startswith("pipeline/")
    ]
    if inline:
        raise AssertionError(report(inline, "Managed path built inline inside a pipeline function:"))


def test_no_bare_path_literals(path_constant_findings):
    """A managed path written as a bare string in a dict value or call argument.
    Carries no constant name at all, so every name-based grep misses it, and it
    drifts silently because nothing links it to the value it duplicates.
    """
    literals = [f for f in path_constant_findings if f.scope == "literal" and f.family in CORE_FAMILIES]
    if literals:
        raise AssertionError(report(literals, "Managed path hardcoded as a bare string literal:"))
