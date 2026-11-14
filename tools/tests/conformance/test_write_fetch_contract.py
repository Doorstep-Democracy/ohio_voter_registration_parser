"""CONFORMANCE 7 -- every JSON file the frontend fetches has a pipeline writer.

The consolidation changes the pipeline write side (DOO-88), the narrative read
side (DOO-89), the validator (DOO-90) and the frontend fetch side (DOO-91).
The handoff's own risk note is that a half-migrated state breaks the pipeline
and frontend contracts simultaneously. This test makes that state detectable in
seconds instead of in a browser.

Method: reduce both sides to the set of file-name SUFFIXES they deal in --
'_party_by_decade.json' and so on -- because the prefix is a runtime slug on
both sides and cannot be compared statically. Then assert that every suffix
v2.js fetches is one the pipeline writes.

Only that direction is asserted. Written-but-not-fetched is reported as
information, not failure: the pipeline legitimately writes files consumed by
tools rather than by the browser, and a gate that fires on a legitimate case is
a gate that gets switched off.

EXPECTED STATUS TODAY: GREEN. Its job is to go red during the migration, at the
exact moment the two halves disagree.
"""

from __future__ import annotations

import re

import pytest

from conftest import ROOT

V2_JS = ROOT / "docs" / "assets" / "v2.js"
WRITERS = (
    ROOT / "pipeline" / "voter_data_cleaner.py",
    ROOT / "pipeline" / "jurisdictional_groupings.py",
    ROOT / "pipeline" / "ohio_voter_pipeline.py",
    ROOT / "tools" / "narrative" / "generate_narratives.py",
    ROOT / "tools" / "admin" / "build_precinct_map.py",
    ROOT / "tools" / "admin" / "build_state_map.py",
)

# Fetched names with no pipeline writer by design: emitted by admin tools, by
# the map builders, or served from elsewhere. Curate this list deliberately --
# every entry is a claim that something outside pipeline/ produces the file.
FETCH_ALLOWLIST = {
    # Jurisdiction-type indexes and the dashboard manifest.
    "index.json",
    "manifest.json",
    # docs/data/state_map/ geometry. Emitted by tools/admin/build_precinct_map.py
    # and build_state_map.py under names assembled at runtime (per chamber, per
    # county), so the literal filename never appears in the writer's source and
    # cannot be matched statically. Verified present on disk 2026-08-01.
    "counties.geojson",
    "congress.geojson",
    "house.geojson",
    "senate.geojson",
    "_detail.geojson",
}

SUFFIX_RE = re.compile(r"[A-Za-z0-9_]*\.(?:json|geojson)")


def _suffixes(text: str) -> set[str]:
    """Reduce every JSON-ish token to its trailing _<name>.json form.

    'data/${slug}_precinct_${safe}_party_by_decade.json' and
    f'{slug}_precinct_{safe_name}_party_by_decade.json' both reduce to
    '_party_by_decade.json', which is the unit the two sides actually agree on.
    """
    out: set[str] = set()
    for match in SUFFIX_RE.finditer(text):
        token = match.group(0)
        if not token or token.startswith("."):
            continue
        # `await res.json()` is a method call, not a filename. A real filename
        # is followed by a quote, a backtick or whitespace -- never '('.
        if text[match.end() : match.end() + 1] == "(":
            continue
        out.add(token)
    return out


def _read(path):
    if not path.exists():
        pytest.skip(f"missing {path}")
    return path.read_text(encoding="utf-8", errors="replace")


def test_frontend_fetches_have_pipeline_writers():
    fetched = _suffixes(_read(V2_JS))
    written: set[str] = set()
    for writer in WRITERS:
        written |= _suffixes(_read(writer))

    def tail(name: str) -> str:
        # Compare on the last two underscore-separated tokens so a slug prefix
        # baked into a literal on one side does not create a false mismatch.
        parts = name.rsplit("_", 2)
        return "_".join(parts[-2:]) if len(parts) >= 2 else name

    written_tails = {tail(w) for w in written}
    orphans = sorted(
        f
        for f in fetched
        if f not in FETCH_ALLOWLIST and f not in written and tail(f) not in written_tails
    )
    if orphans:
        raise AssertionError(
            "docs/assets/v2.js fetches JSON that no pipeline writer emits.\n"
            "Either the frontend moved ahead of the pipeline (half-migration) or\n"
            "the file comes from a tool and belongs in FETCH_ALLOWLIST:\n"
            + "\n".join(f"    {o}" for o in orphans)
        )


def test_report_pipeline_outputs_the_frontend_never_fetches(capsys):
    """Informational only -- never fails. Printed so that the consolidation can
    see, at a glance, which outputs exist solely for tools and therefore do not
    constrain the frontend schema.
    """
    fetched = _suffixes(_read(V2_JS))
    written: set[str] = set()
    for writer in WRITERS:
        written |= _suffixes(_read(writer))
    unfetched = sorted(written - fetched)
    with capsys.disabled():
        print(f"\n[info] pipeline writes {len(written)} JSON name(s); v2.js fetches {len(fetched)}")
        if unfetched:
            print("[info] written but never fetched by the frontend:")
            for name in unfetched:
                print(f"          {name}")
