"""CONFORMANCE 4 -- one canonical COUNTY_NUMBER= partition form, one resolver.

The repo already proved this drifts. voter_data_cleaner.py builds
f'COUNTY_NUMBER={cnum}' on line 731 and f'COUNTY_NUMBER={int(cnum)}' on line
733 -- twelve lines apart, in the same function, with different zero-padding.
The on-disk partitions are zero-padded (COUNTY_NUMBER=01), so the int() form
mis-globs for counties 1-9 whenever cnum arrives as an int.

Two assertions here, and the second matters more than the first: it is not
enough to centralize the construction, the centralized form has to match what
is actually on disk. Picking the wrong side of the existing inconsistency would
centralize the bug.

EXPECTED STATUS TODAY: RED for the site scan (13 sites in 9 files); SKIPPED for
the resolver behaviour tests until pipeline/paths.py exists.
"""

from __future__ import annotations

import re

import pytest

from conftest import PARTITION_DIR_RE, ROOT, report

PARQUET_DIR_CANDIDATE = ROOT / "local" / "source" / "parquet"


def _county_partition():
    try:
        from pipeline.paths import county_partition  # type: ignore
    except Exception as exc:  # ModuleNotFoundError, ImportError, AttributeError
        pytest.skip(f"pipeline.paths.county_partition not available yet (DOO-86/DOO-92): {exc}")
    return county_partition


def test_no_partition_string_built_outside_resolver(partition_findings):
    if partition_findings:
        raise AssertionError(
            report(
                partition_findings,
                "COUNTY_NUMBER= partition names are built outside county_partition():",
            )
        )


def test_partition_form_is_canonical_and_type_insensitive():
    """The bug this suite is named after. int 7, str '7' and str '07' must all
    produce the identical partition name, or the same county resolves to two
    different directories depending on which caller got there first.
    """
    county_partition = _county_partition()
    forms = {county_partition(7), county_partition("7"), county_partition("07")}
    assert forms == {"COUNTY_NUMBER=07"}, (
        "county_partition must normalise int/str/zero-padded input to one "
        f"zero-padded form; got {sorted(forms)}"
    )


def test_partition_form_matches_on_disk():
    """Centralising on a form that does not match the filesystem would be a
    silent, total read failure. Skipped when the cache is absent (fresh clone),
    never assumed.
    """
    county_partition = _county_partition()
    if not PARQUET_DIR_CANDIDATE.is_dir():
        pytest.skip(f"no parquet cache at {PARQUET_DIR_CANDIDATE} -- cannot compare to disk")
    on_disk = sorted(p.name for p in PARQUET_DIR_CANDIDATE.iterdir() if p.is_dir())
    if not on_disk:
        pytest.skip("parquet cache present but holds no partitions")
    malformed = [n for n in on_disk if not PARTITION_DIR_RE.match(n)]
    assert not malformed, f"partition directories do not match COUNTY_NUMBER=NN: {malformed}"
    for name in on_disk:
        number = int(name.split("=", 1)[1])
        assert county_partition(number) == name, (
            f"county_partition({number}) produced {county_partition(number)!r} "
            f"but the directory on disk is {name!r}"
        )


def test_expected_partition_set_is_not_a_bare_range():
    """voter_data_cleaner.py:682 hardcodes range(1, 89) -- '88 counties' as a
    bare literal. It is the check that would have to notice a second state's
    partitions colliding with Ohio's, and as written it cannot.
    """
    src = (ROOT / "pipeline" / "voter_data_cleaner.py").read_text(encoding="utf-8", errors="replace")
    hits = [
        (i + 1, line.strip())
        for i, line in enumerate(src.splitlines())
        if re.search(r"range\(\s*1\s*,\s*89\s*\)", line)
    ]
    if hits:
        raise AssertionError(
            "Hardcoded 1-88 county range still present; route through a per-state "
            "lookup instead:\n" + "\n".join(f"    voter_data_cleaner.py:{n}  {t}" for n, t in hits)
        )
