"""
Pass B — Step 0 cache helper tests.

Tests _cache_is_fresh() and _write_cache_atomic() behavior in both
pipeline/voter_data_cleaner.py and jurisdictional_groupings.
Requires polars; skips automatically if not installed.
"""
import logging
import os
import time
from datetime import date
from pathlib import Path

import pytest

polars = pytest.importorskip("polars")
from pipeline import voter_data_cleaner as _v2
from pipeline import jurisdictional_groupings as _jg


# ── _cache_is_fresh() ────────────────────────────────────────────────────────

@pytest.fixture(params=[_v2, _jg], ids=["v2", "jg"])
def mod(request):
    return request.param


def _make_partition(parquet_dir: Path, name="COUNTY_NUMBER=01"):
    p = parquet_dir / name
    p.mkdir(parents=True, exist_ok=True)
    return p


class TestCacheIsFresh:
    # voter_data_cleaner owns the single _cache_is_fresh(); jurisdictional_groupings
    # delegates to it (its copy was removed 2026-11-11), so only one module is tested.
    @pytest.fixture
    def mod(self):
        return _v2

    def test_missing_cache_returns_false(self, mod, tmp_path, monkeypatch):
        monkeypatch.setattr(mod, "ENRICHED_CACHE", tmp_path / "cache.parquet")
        monkeypatch.setattr(mod, "PARQUET_DIR",    tmp_path / "parquet")
        monkeypatch.setattr(mod, "CLASSIFIER_SRC", tmp_path / "classifier.py")
        assert mod._cache_is_fresh() is False

    def test_no_raw_partitions_returns_false(self, mod, tmp_path, monkeypatch):
        cache = tmp_path / "cache.parquet"; cache.touch()
        empty = tmp_path / "parquet"; empty.mkdir()
        monkeypatch.setattr(mod, "ENRICHED_CACHE", cache)
        monkeypatch.setattr(mod, "PARQUET_DIR",    empty)
        monkeypatch.setattr(mod, "CLASSIFIER_SRC", tmp_path / "c.py"); (tmp_path / "c.py").touch()
        assert mod._cache_is_fresh() is False

    def test_raw_newer_than_cache_returns_false(self, mod, tmp_path, monkeypatch):
        pdir = tmp_path / "parquet"; pdir.mkdir()
        part = _make_partition(pdir)
        clf  = tmp_path / "c.py"; clf.touch()
        cache = tmp_path / "cache.parquet"; cache.touch()
        # make raw partition newer than cache
        t = cache.stat().st_mtime + 2
        os.utime(part, (t, t))
        os.utime(clf,  (cache.stat().st_mtime - 1, cache.stat().st_mtime - 1))
        monkeypatch.setattr(mod, "ENRICHED_CACHE", cache)
        monkeypatch.setattr(mod, "PARQUET_DIR",    pdir)
        monkeypatch.setattr(mod, "CLASSIFIER_SRC", clf)
        assert mod._cache_is_fresh() is False

    def test_classifier_newer_than_cache_returns_false(self, mod, tmp_path, monkeypatch):
        """Critical: cohort taxonomy changes must bust the cache."""
        pdir = tmp_path / "parquet"; pdir.mkdir()
        part = _make_partition(pdir)
        cache = tmp_path / "cache.parquet"; cache.touch()
        clf   = tmp_path / "c.py"; clf.touch()
        # make classifier newer than cache
        t = cache.stat().st_mtime + 2
        os.utime(clf,  (t, t))
        os.utime(part, (cache.stat().st_mtime - 1, cache.stat().st_mtime - 1))
        monkeypatch.setattr(mod, "ENRICHED_CACHE", cache)
        monkeypatch.setattr(mod, "PARQUET_DIR",    pdir)
        monkeypatch.setattr(mod, "CLASSIFIER_SRC", clf)
        assert mod._cache_is_fresh() is False

    def test_cache_newest_returns_true(self, mod, tmp_path, monkeypatch):
        pdir = tmp_path / "parquet"; pdir.mkdir()
        part = _make_partition(pdir)
        clf  = tmp_path / "c.py"; clf.touch()
        time.sleep(0.05)
        cache = tmp_path / "cache.parquet"; cache.touch()  # newest
        monkeypatch.setattr(mod, "ENRICHED_CACHE", cache)
        monkeypatch.setattr(mod, "PARQUET_DIR",    pdir)
        monkeypatch.setattr(mod, "CLASSIFIER_SRC", clf)
        assert mod._cache_is_fresh() is True

    def test_uses_max_of_raw_and_classifier(self, mod, tmp_path, monkeypatch):
        """Cache newer than raw but older than classifier → still stale."""
        pdir = tmp_path / "parquet"; pdir.mkdir()
        part = _make_partition(pdir)
        t0   = time.time() - 10
        os.utime(part, (t0, t0))
        cache = tmp_path / "cache.parquet"; cache.touch()
        t1 = cache.stat().st_mtime + 2
        clf = tmp_path / "c.py"; clf.touch()
        os.utime(clf, (t1, t1))  # classifier newer than cache
        monkeypatch.setattr(mod, "ENRICHED_CACHE", cache)
        monkeypatch.setattr(mod, "PARQUET_DIR",    pdir)
        monkeypatch.setattr(mod, "CLASSIFIER_SRC", clf)
        assert mod._cache_is_fresh() is False


# ── _write_cache_atomic() ────────────────────────────────────────────────────

class TestWriteCacheAtomic:
    # voter_data_cleaner owns the single _write_cache_atomic(); jurisdictional_groupings
    # delegates to it (its copy, which wrote no provenance sidecar, was removed
    # 2026-11-12), so only one module is tested.
    @pytest.fixture
    def mod(self):
        return _v2

    def test_groupings_keeps_no_copy(self):
        assert not hasattr(_jg, "_write_cache_atomic")
        assert not hasattr(_jg, "_cache_is_fresh")

    def test_writes_provenance_sidecar(self, mod, tmp_path, monkeypatch):
        import json
        cache = tmp_path / "cache.parquet"
        monkeypatch.setattr(mod, "ENRICHED_CACHE", cache)
        mod._write_cache_atomic(polars.DataFrame({"x": [1]}), logging.getLogger("test"))
        side = json.loads(cache.with_suffix(".provenance.json").read_text(encoding="utf-8"))
        assert side["provenance_snapshot_date"] == "2026-10-24"
        assert "affiliation_window_years" in side

    @pytest.fixture(autouse=True)
    def _pin_snapshot_date(self, monkeypatch):
        # voter_data_cleaner._write_cache_atomic stamps the provenance sidecar
        # with get_snapshot_date(), which reads the staged snapshot under
        # local/ and raises when none is staged (a fresh checkout / CI).
        # These tests exercise the atomic write, not snapshot staging, so pin
        # the date instead of depending on whatever is staged on this machine.
        monkeypatch.setattr(_v2, "get_snapshot_date",
                            lambda logger=None: date(2026, 10, 24))

    def test_creates_parquet_at_target_path(self, mod, tmp_path, monkeypatch):
        cache = tmp_path / "cache.parquet"
        monkeypatch.setattr(mod, "ENRICHED_CACHE", cache)
        df = polars.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        mod._write_cache_atomic(df, logging.getLogger("test"))
        assert cache.exists()

    def test_no_tmp_file_left_after_success(self, mod, tmp_path, monkeypatch):
        cache = tmp_path / "cache.parquet"
        monkeypatch.setattr(mod, "ENRICHED_CACHE", cache)
        df = polars.DataFrame({"x": [1]})
        mod._write_cache_atomic(df, logging.getLogger("test"))
        assert not cache.with_suffix(".parquet.tmp").exists()

    def test_written_parquet_is_readable(self, mod, tmp_path, monkeypatch):
        cache = tmp_path / "cache.parquet"
        monkeypatch.setattr(mod, "ENRICHED_CACHE", cache)
        df = polars.DataFrame({"col1": [10, 20], "col2": ["a", "b"]})
        mod._write_cache_atomic(df, logging.getLogger("test"))
        df2 = polars.read_parquet(cache)
        assert df2.height == 2
        assert set(df2.columns) == {"col1", "col2"}
