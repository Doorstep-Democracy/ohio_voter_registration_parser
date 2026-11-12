"""Groupings resume reuses an output file only when it is newer than its inputs, and
freshness decisions are logged. Rationale: jurisdictional_groupings._split_by_freshness
and the "Logging policy" in voter_data_cleaner.setup_logging()."""
import logging
import os

import pytest

pytest.importorskip("polars")
pytest.importorskip("pandas")

from pipeline import jurisdictional_groupings as jg  # noqa: E402
from pipeline import voter_data_cleaner as vdc  # noqa: E402

CUTOFF = 1_800_000_000.0  # arbitrary fixed epoch for the synthetic files


def _sentinels(tmp_path, ages):
    """Create one sentinel per name; ages are seconds relative to CUTOFF (None = absent)."""
    for name, delta in ages.items():
        if delta is None:
            continue
        p = tmp_path / f"{name}_party_affiliation.json"
        p.write_text("{}", encoding="utf-8")
        os.utime(p, (CUTOFF + delta, CUTOFF + delta))
    return lambda j: tmp_path / f"{j}_party_affiliation.json"


def test_split_by_freshness_partitions_three_ways(tmp_path):
    sentinel_for = _sentinels(tmp_path, {"new": 60, "old": -86_400, "gone": None})
    current, stale, missing = jg._split_by_freshness(["new", "old", "gone"], sentinel_for, CUTOFF)
    assert current == {"new"}
    assert stale == ["old"]
    assert missing == ["gone"]


def test_file_exactly_at_cutoff_is_stale(tmp_path):
    """Strictly newer is required: a file from the same instant as its input is not trusted."""
    sentinel_for = _sentinels(tmp_path, {"edge": 0})
    current, stale, _ = jg._split_by_freshness(["edge"], sentinel_for, CUTOFF)
    assert not current and stale == ["edge"]


def test_existence_alone_no_longer_skips(tmp_path):
    """The regression: every existing file used to count as 'already exported'."""
    sentinel_for = _sentinels(tmp_path, {f"j{i}": -1 for i in range(5)})
    current, stale, _ = jg._split_by_freshness([f"j{i}" for i in range(5)], sentinel_for, CUTOFF)
    assert current == set() and len(stale) == 5


def test_cache_freshness_verdict_is_logged(caplog):
    with caplog.at_level(logging.INFO, logger="test_freshness"):
        verdict = vdc._cache_is_fresh(logging.getLogger("test_freshness"))
    lines = [r.getMessage() for r in caplog.records if "Enriched cache freshness" in r.getMessage()]
    assert len(lines) == 1
    assert ("FRESH" if verdict else "STALE") in lines[0]


def test_groupings_has_no_private_freshness_copy():
    """voter_data_cleaner owns the enriched-cache freshness check (single resolver)."""
    assert not hasattr(jg, "_cache_is_fresh")
