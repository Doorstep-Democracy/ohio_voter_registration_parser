"""Published-data vintage and completeness gate (evaluation M5, P4, P7, M10).

One full rebuild must leave every published family on one run, with nothing
left over from an earlier one:

* Chart and narrative files stamp the run date in ``updated``.
  docs/manifest.json records that date in ``dataNote`` ("Last updated
  YYYY-MM-DD") and the voter-file snapshot in ``provenanceSnapshotDate``.
* The landing-map layers bake in lean and total_voters from the party JSON, so
  state_map/provenance.json stamps the snapshot, per layer.
* Every entity in a level's index.json has all of its chart files and its
  narrative, and no precinct file outlives its precinct.

Each check compares like with like (run date to run date, snapshot to
snapshot). The large families are read as deterministic strides, so the gate
runs in seconds rather than parsing ~84k files.
"""
import json
import re
from pathlib import Path

import pytest

from pipeline.paths import DATA_DIR as DATA
from pipeline.paths import DOCS_DIR as DOCS
from pipeline.voter_data_cleaner import PRECINCT_FILE_KINDS, STATEWIDE_NAME

ROOT = Path(__file__).resolve().parent.parent.parent
STATE_MAP = DATA / "state_map"
STATE_SLUG = STATEWIDE_NAME.lower().replace(" ", "_")

UPDATED = re.compile(rb'"updated":\s*"(\d{4}-\d{2}-\d{2})"')

# Root-level files for each county and for the statewide aggregate.
COUNTY_KINDS = ("party_affiliation", "decade_distribution", "party_by_decade",
                "generation_distribution", "party_by_generation",
                "participation_by_generation", "unc_shadow", "city_summary")
# Per-entity files in every docs/data/<level>/ that has an index.json.
ENTITY_KINDS = ("party_affiliation", "decade_distribution", "party_by_decade",
                "generation_distribution", "party_by_generation", "unc_shadow",
                "narrative")


@pytest.fixture(scope="module")
def manifest():
    path = DOCS / "manifest.json"
    if not path.exists():
        pytest.skip("docs/manifest.json not present")
    return json.loads(path.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def run_date(manifest):
    m = re.search(r"Last updated (\d{4}-\d{2}-\d{2})", manifest.get("dataNote", ""))
    assert m, "manifest dataNote carries no 'Last updated YYYY-MM-DD'"
    return m.group(1)


def _county_slug(name):
    return name.lower().replace(" ", "_")


def _stamp(path):
    m = UPDATED.search(path.read_bytes())
    return m.group(1).decode() if m else None


def _stride(paths, n):
    paths = sorted(paths)
    return paths[::max(1, len(paths) // n)]


def _stale(paths, run_date):
    out = {}
    for p in paths:
        s = _stamp(p)
        if s != run_date:
            out[p.relative_to(DATA).as_posix()] = s
    return out


def _levels(manifest):
    return [p.name for p in sorted(DATA.iterdir()) if (p / "index.json").is_file()]


def test_county_files_carry_the_run(manifest, run_date):
    paths, missing = [], []
    for name in manifest["processedCounties"]:
        for kind in COUNTY_KINDS + ("narrative",):
            p = DATA / f"{_county_slug(name)}_{kind}.json"
            (paths if p.exists() else missing).append(p)
    stale = _stale(paths, run_date)
    assert not missing, f"missing county files: {[p.name for p in missing][:10]}"
    assert not stale, f"county files not from run {run_date}: {dict(list(stale.items())[:10])}"


def test_statewide_files_carry_the_run(run_date):
    paths = [DATA / f"{STATE_SLUG}_{kind}.json" for kind in COUNTY_KINDS]
    missing = [p.name for p in paths if not p.exists()]
    assert not missing, f"missing statewide files: {missing}"
    stale = _stale(paths, run_date)
    assert not stale, f"statewide files not from run {run_date}: {stale}"
    # A statewide precinct table would name ~8,900 precincts without counties.
    assert not (DATA / f"{STATE_SLUG}_precinct_summary.json").exists()


def test_precinct_files_carry_the_run(run_date):
    paths = [p for p in DATA.glob("*_precinct_*.json")
             if not p.name.endswith(("_precinct_index.json", "_precinct_summary.json"))]
    stale = _stale(_stride(paths, 600), run_date)
    assert not stale, f"precinct files not from run {run_date}: {dict(list(stale.items())[:10])}"


def test_no_orphan_precinct_files(manifest):
    expected = set()
    for name in manifest["processedCounties"]:
        slug = _county_slug(name)
        index = json.loads((DATA / f"{slug}_precinct_index.json").read_text(encoding="utf-8"))
        expected |= {f"{slug}_precinct_{e['safe_name']}_{k}.json"
                     for e in index["precincts"] for k in PRECINCT_FILE_KINDS}
        expected |= {f"{slug}_precinct_index.json", f"{slug}_precinct_summary.json"}
    orphans = sorted(p.name for p in DATA.glob("*_precinct_*.json") if p.name not in expected)
    assert not orphans, f"{len(orphans)} precinct files for precincts in no index: {orphans[:10]}"


def test_jurisdiction_levels_carry_the_run(manifest, run_date):
    stale = {}
    for level in _levels(manifest):
        paths = [p for p in (DATA / level).glob("*.json") if p.name != "index.json"]
        stale.update(_stale(_stride(paths, 120), run_date))
    assert not stale, f"jurisdiction files not from run {run_date}: {dict(list(stale.items())[:10])}"


def test_every_indexed_entity_has_every_chart_and_narrative(manifest):
    missing = {}
    for level in _levels(manifest):
        index = json.loads((DATA / level / "index.json").read_text(encoding="utf-8"))
        for e in index:
            for kind in ENTITY_KINDS:
                if not (DATA / level / f"{e['slug']}_{kind}.json").exists():
                    key = f"{level}:{kind}"
                    missing[key] = missing.get(key, 0) + 1
    assert not missing, f"indexed entities missing files (level:kind -> count): {missing}"


def test_manifest_sections_point_at_files(manifest):
    dangling = [s["dataUrl"] for s in manifest.get("sections", [])
                if not (DOCS / s["dataUrl"]).is_file()]
    assert not dangling, f"manifest sections with no data file: {dangling[:10]}"


def test_map_layers_carry_the_snapshot(manifest):
    snap = manifest["provenanceSnapshotDate"]
    prov = json.loads((STATE_MAP / "provenance.json").read_text(encoding="utf-8"))
    assert prov.get("provenance_snapshot_date") == snap, (
        f"state_map/provenance.json is for snapshot {prov.get('provenance_snapshot_date')!r}, "
        f"manifest is {snap!r}")
    layers = prov.get("layers", {})
    stale = {p.stem: (layers.get(p.stem) or {}).get("provenance_snapshot_date")
             for p in STATE_MAP.glob("*.geojson")}
    stale = {k: v for k, v in stale.items() if v != snap}
    assert not stale, f"map layers not built from snapshot {snap}: {stale}"


def test_statewide_total_equals_the_county_sum(manifest):
    """The landing headline and the Ohio view read the statewide party chart.
    It must count the same voters as the 88 county charts (evaluation P4)."""
    def total(path):
        return sum(json.loads(path.read_text(encoding="utf-8"))["chartConfig"]["datasets"][0]["data"])
    counties = sum(total(DATA / f"{_county_slug(n)}_party_affiliation.json")
                   for n in manifest["processedCounties"])
    state = total(DATA / f"{STATE_SLUG}_party_affiliation.json")
    assert state == counties, f"statewide party chart counts {state:,}; the counties sum to {counties:,}"
