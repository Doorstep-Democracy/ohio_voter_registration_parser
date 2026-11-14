"""CONFORMANCE 2 -- the scope manifest is complete, valid, and self-verifying.

The handoff carried its file inventory as a prose table, and the count was
derived by adding two overlapping lists (13 + 17 = 30, actual union 27, actual
detected 33). A prose inventory cannot be checked by a machine and drifts the
moment anyone adds a file.

scope_manifest.json replaces the table. The load-bearing assertion is
MANIFEST is a SUPERSET of DETECTED: if the detectors find a file the manifest
does not list, the scope is wrong -- either a new offender was introduced, or
the original audit missed one. Either way it must be triaged, not discovered
halfway through the refactor.

EXPECTED STATUS TODAY: GREEN (manifest was generated from the detectors).
It turns RED the first time someone adds a path constant without registering
it, which is precisely the drift this suite exists to catch.
"""

from __future__ import annotations

from conftest import (
    CORE_FAMILIES,
    VALID_BUCKETS,
    VALID_STATUSES,
    collect_partition_strings,
    collect_path_constants,
    manifest_relpaths,
)


def test_manifest_covers_every_detected_file(manifest):
    listed = manifest_relpaths(manifest)
    detected = {f.relpath for f in collect_path_constants() if f.family in CORE_FAMILIES}
    detected |= {f.relpath for f in collect_partition_strings()}
    missing = sorted(detected - listed)
    if missing:
        raise AssertionError(
            "Files construct a managed path or partition key but are not in "
            "scope_manifest.json.\nAdd them with a bucket and a reason, or fix the file:\n"
            + "\n".join(f"    {p}" for p in missing)
        )


def test_manifest_entries_are_well_formed(manifest):
    problems: list[str] = []
    seen: set[str] = set()
    for entry in manifest["files"]:
        path = entry.get("path", "<missing path>")
        if path in seen:
            problems.append(f"{path}: duplicate entry")
        seen.add(path)
        if entry.get("bucket") not in VALID_BUCKETS:
            problems.append(f"{path}: bucket {entry.get('bucket')!r} not in {sorted(VALID_BUCKETS)}")
        if entry.get("status") not in VALID_STATUSES:
            problems.append(f"{path}: status {entry.get('status')!r} not in {sorted(VALID_STATUSES)}")
        if entry.get("status") in {"converted", "excluded"} and not entry.get("notes"):
            problems.append(f"{path}: status {entry['status']} requires a note giving the reason")
    if problems:
        raise AssertionError("Malformed manifest entries:\n" + "\n".join(f"    {p}" for p in problems))


def test_manifest_files_exist(manifest):
    from conftest import ROOT

    missing = [e["path"] for e in manifest["files"] if not (ROOT / e["path"]).exists()]
    if missing:
        raise AssertionError(
            "Manifest lists files that do not exist (renamed or deleted without "
            "updating scope):\n" + "\n".join(f"    {p}" for p in missing)
        )


def test_every_file_is_classified(manifest):
    """DOO-85's deliverable, expressed as a gate. Unclassified files are the
    ones that get swept into a mechanical pass by accident.

    EXPECTED STATUS TODAY: RED until the classification pass is done.
    """
    unclassified = [e["path"] for e in manifest["files"] if not e.get("bucket")]
    if unclassified:
        raise AssertionError(
            f"{len(unclassified)} of {len(manifest['files'])} files are unclassified "
            "(bucket is empty).\nClassify each as safe-mechanical, needs-judgment, or "
            "broken-or-dead:\n" + "\n".join(f"    {p}" for p in unclassified)
        )


def test_conversion_is_complete(manifest):
    """The completeness gate. Green only when every in-scope file has actually
    been converted or explicitly excluded with a reason -- which is the question
    "is all the code touched" asked as a command rather than a re-read.

    EXPECTED STATUS TODAY: RED.
    """
    pending = [e["path"] for e in manifest["files"] if e.get("status") == "pending"]
    if pending:
        raise AssertionError(
            f"{len(pending)} of {len(manifest['files'])} in-scope files still pending:\n"
            + "\n".join(f"    {p}" for p in pending)
        )
