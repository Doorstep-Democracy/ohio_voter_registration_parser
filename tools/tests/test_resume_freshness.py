"""The enriched-cache freshness check has one owner (voter_data_cleaner) and logs its
verdict. Groupings resume tests are added with jurisdictional_groupings._split_by_freshness."""
import logging
import os

import pytest

pytest.importorskip("polars")
pytest.importorskip("pandas")

from pipeline import jurisdictional_groupings as jg  # noqa: E402
from pipeline import voter_data_cleaner as vdc  # noqa: E402

def test_cache_freshness_verdict_is_logged(caplog):
    with caplog.at_level(logging.INFO, logger="test_freshness"):
        verdict = vdc._cache_is_fresh(logging.getLogger("test_freshness"))
    lines = [r.getMessage() for r in caplog.records if "Enriched cache freshness" in r.getMessage()]
    assert len(lines) == 1
    assert ("FRESH" if verdict else "STALE") in lines[0]


def test_groupings_has_no_private_freshness_copy():
    """voter_data_cleaner owns the enriched-cache freshness check (single resolver)."""
    assert not hasattr(jg, "_cache_is_fresh")
