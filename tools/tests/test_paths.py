"""pipeline/paths.py: the ELECTION_DATA_OUT output override and the partition / glob helpers.

The structural invariants of the default layout (absolute, inside the repo, CWD
independent) are asserted by tools/tests/conformance/test_path_invariants.py.
This file covers behaviour: where output goes when the override is set, and
that a bad override stops the process instead of writing somewhere new.

Subprocesses are used wherever module-level constants matter, because the
override is read once at import and an in-process reload would not re-evaluate
the modules that already imported the constants.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from pipeline import paths

ROOT = Path(__file__).resolve().parents[2]

# Every module a pipeline run writes dashboard output through. The two map
# builders are run by the pipeline as subprocesses and have no package, so they
# are loaded by file path.
_PROBE = r"""
import importlib, importlib.util, json, sys
out = {}
for name in ("pipeline.voter_data_cleaner", "pipeline.jurisdictional_groupings",
             "pipeline.ohio_voter_pipeline", "tools.narrative.generate_narratives"):
    mod = importlib.import_module(name)
    out[name] = str(mod.DATA_DIR)
for rel in ("tools/admin/build_state_map.py", "tools/admin/build_precinct_map.py"):
    spec = importlib.util.spec_from_file_location(rel.replace("/", "_")[:-3], rel)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    out[rel] = str(mod.OUT_DIR)
    out[rel + ":manifest"] = str(mod.MANIFEST_PATH)
from pipeline import paths
out["PARQUET_DIR"] = str(paths.PARQUET_DIR)
out["ENRICHED_CACHE"] = str(paths.ENRICHED_CACHE)
out["OUTPUT_OVERRIDDEN"] = paths.OUTPUT_OVERRIDDEN
print("PROBE_JSON=" + json.dumps(out))
"""


def _run_probe(env_value: str | None) -> subprocess.CompletedProcess:
    env = dict(os.environ, PYTHONPATH=str(ROOT), PYTHONIOENCODING="utf-8")
    env.pop(paths.OUTPUT_ENV_VAR, None)
    if env_value is not None:
        env[paths.OUTPUT_ENV_VAR] = env_value
    return subprocess.run(
        [sys.executable, "-c", _PROBE], cwd=str(ROOT), env=env,
        capture_output=True, text=True, encoding="utf-8", errors="replace",
    )


def _probe_result(proc: subprocess.CompletedProcess) -> dict:
    assert proc.returncode == 0, proc.stderr[-2000:]
    line = next(l for l in proc.stdout.splitlines() if l.startswith("PROBE_JSON="))
    return json.loads(line[len("PROBE_JSON="):])


# ---- resolve_output_root: pure function, injected environment -----------------

def test_default_output_root_is_docs_under_base(tmp_path):
    root, overridden = paths.resolve_output_root(tmp_path, env={})
    assert root == tmp_path / "docs"
    assert overridden is False


def test_blank_override_is_ignored(tmp_path):
    root, overridden = paths.resolve_output_root(tmp_path, env={paths.OUTPUT_ENV_VAR: "   "})
    assert root == tmp_path / "docs"
    assert overridden is False


def test_override_resolves_to_the_existing_directory(tmp_path):
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    root, overridden = paths.resolve_output_root(tmp_path, env={paths.OUTPUT_ENV_VAR: str(scratch)})
    assert root == scratch.resolve()
    assert root.is_absolute()
    assert overridden is True


def test_override_naming_a_missing_directory_raises_and_creates_nothing(tmp_path):
    missing = tmp_path / "does_not_exist"
    with pytest.raises(NotADirectoryError):
        paths.resolve_output_root(tmp_path, env={paths.OUTPUT_ENV_VAR: str(missing)})
    assert not missing.exists()


def test_override_naming_a_file_raises(tmp_path):
    a_file = tmp_path / "file.txt"
    a_file.write_text("x", encoding="utf-8")
    with pytest.raises(NotADirectoryError):
        paths.resolve_output_root(tmp_path, env={paths.OUTPUT_ENV_VAR: str(a_file)})


# ---- the override reaches every writer, and only the output root moves --------

def test_every_pipeline_writer_follows_the_override(tmp_path):
    scratch = tmp_path / "scratch_out"
    scratch.mkdir()
    got = _probe_result(_run_probe(str(scratch)))
    assert got.pop("OUTPUT_OVERRIDDEN") is True
    inputs = {k: got.pop(k) for k in ("PARQUET_DIR", "ENRICHED_CACHE")}
    outside = {k: v for k, v in got.items() if not Path(v).is_relative_to(scratch.resolve())}
    assert not outside, f"these writers ignore {paths.OUTPUT_ENV_VAR}: {outside}"
    # Inputs and caches are never redirected.
    moved = {k: v for k, v in inputs.items() if not Path(v).is_relative_to(ROOT)}
    assert not moved, f"the override moved an input or cache path: {moved}"


def test_without_the_override_every_writer_uses_the_published_tree():
    got = _probe_result(_run_probe(None))
    assert got.pop("OUTPUT_OVERRIDDEN") is False
    for key in ("PARQUET_DIR", "ENRICHED_CACHE"):
        got.pop(key)
    docs = ROOT / "docs"
    outside = {k: v for k, v in got.items() if not Path(v).is_relative_to(docs)}
    assert not outside, f"default output paths left docs/: {outside}"


def test_a_missing_override_directory_stops_the_process_at_import(tmp_path):
    missing = tmp_path / "typo_dir"
    proc = _run_probe(str(missing))
    assert proc.returncode != 0
    assert "NotADirectoryError" in proc.stderr
    assert not missing.exists()


# ---- helpers ------------------------------------------------------------------

def test_county_partition_is_zero_padded_and_type_insensitive():
    # Expected names are built from PARTITION_KEY: a literal partition name here
    # would itself be a second copy (tools/tests/conformance polices that).
    key = paths.PARTITION_KEY
    assert {paths.county_partition(7), paths.county_partition("7"),
            paths.county_partition("07"), paths.county_partition(" 7 ")} == {f"{key}=07"}
    assert paths.county_partition(88) == f"{key}=88"


@pytest.mark.parametrize("bad", [0, -1, "0", "abc", ""])
def test_county_partition_rejects_malformed_numbers(bad):
    with pytest.raises(ValueError):
        paths.county_partition(bad)


def test_parquet_glob_is_the_recursive_pattern(tmp_path):
    pattern = paths.parquet_glob(tmp_path)
    assert pattern.startswith(str(tmp_path))
    tail = pattern[len(str(tmp_path)):]
    assert tail.split("/") == ["", "**", "*.parquet"]


def test_expected_partitions_cover_every_county_once():
    parts = paths.expected_partitions()
    assert len(parts) == paths.COUNTY_COUNT == paths.COUNTY_COUNT_BY_STATE["OH"]
    assert paths.county_partition(1) in parts
    assert paths.county_partition(paths.COUNTY_COUNT) in parts
    assert paths.county_partition(paths.COUNTY_COUNT + 1) not in parts


def test_partition_glob_matches_partition_directories_only(tmp_path):
    names = [paths.county_partition(1), paths.county_partition(paths.COUNTY_COUNT)]
    for name in names:
        (tmp_path / name).mkdir()
    (tmp_path / "not_a_partition").mkdir()
    assert sorted(p.name for p in tmp_path.glob(paths.PARTITION_GLOB)) == sorted(names)
