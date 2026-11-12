"""
Build the Montgomery County precinct choropleth layers: an overview (used for
the full county choropleth) and a finer-tolerance detail layer (used only by
the dashboard's zoomed selection inset).

Dissolves the Montgomery County BoE's 2022 precinct polygons (497 multipart
rows -> 381 precincts), joins them to the voter-file precinct index through an
explicit name normalizer, and writes a simplified GeoJSON to
docs/data/state_map/precincts_montgomery.geojson for the precinct-level
choropleth, then a second time at finer tolerance to
docs/data/state_map/precincts_montgomery_detail.geojson for the inset -- the
85m overview simplification reads as faceted rectangles when one precinct
fills the inset panel. Both runs share one pipeline (dissolve -> crosswalk ->
hard-fail gate -> party-lean join -> simplify -> write -> provenance);
see run_pass() and HANDOFF_6_PRECINCT_INSET_DETAIL.md for full spec.

Scope is deliberately Montgomery-only: precinct boundaries are published by 88
sovereign county Boards of Elections in whatever format each chooses
(CLAUDE.md SS5, format + provenance divergence). Do NOT generalize the
normalizer below to another county without a fresh per-county profile.

Lean and total_voters come from the pipeline's own party-affiliation JSON --
never from the source GDB's TOTAL_VOTERS / DEM_VOTERS / REP_VOTERS columns,
which are an independent tally that would silently contradict pipeline numbers.

See local/context/handoffs/HANDOFF_5_PRECINCT_PILOT.md for the overview spec.

Usage:
    uv run python tools/admin/build_precinct_map.py
"""

import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import geopandas as gpd
import shapely.geometry

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
from pipeline.cohort_lean import cohort_sides  # noqa: E402

GEO_DIR = REPO_ROOT / "local" / "source" / "Geo"
GDB_PATH = GEO_DIR / "57_Montgomery" / "GeoDataBase Files" / "MC_ELECTIONS.gdb"
GDB_LAYER = "precinct_2022_polygon"
GDB_VINTAGE = "2022 precinct plan (Montgomery County BoE)"

DOCS_DATA = REPO_ROOT / "docs" / "data"
PRECINCT_INDEX = DOCS_DATA / "montgomery_precinct_index.json"
MANIFEST_PATH = REPO_ROOT / "docs" / "manifest.json"
OUT_DIR = DOCS_DATA / "state_map"
OUT_PATH = OUT_DIR / "precincts_montgomery.geojson"
PROV_PATH = OUT_DIR / "provenance.json"

COUNTY_SLUG = "montgomery"
LAYER_NAME = "precincts_montgomery"
EXPECTED_PRECINCTS = 381
TOLERANCE = 0.001

MAX_TARGET_KB = 300
HARD_FAIL_KB = 600

# Detail pass: fetched lazily, one precinct at a time, for the zoomed
# selection inset -- never on initial page load, so the 300 KB statewide
# budget does not apply. It still needs a sane ceiling since a caller could
# in principle pull the whole county at once.
DETAIL_LAYER_NAME = "precincts_montgomery_detail"
DETAIL_OUT_PATH = OUT_DIR / "precincts_montgomery_detail.geojson"
DETAIL_TOLERANCE = 0.0001
DETAIL_HARD_FAIL_KB = 2500

# Village-in-township exception list. New Lebanon straddles Jackson and Perry
# townships, so the BoE keeps both tokens where the voter data keeps only an
# abbreviated prefix. Keys are RAW VNAME strings (note the underscore) and are
# substituted BEFORE the abbreviation rules below -- running TOWNSHIP -> TWP
# first defeats the lookup. An auditable 3-entry list beats another regex.
OVERRIDE = {
    'PERRY TOWNSHIP_NEW LEBANON':     'PER/NEW LEBANON',
    'JACKSON TOWNSHIP_NEW LEBANON A': 'JACK/NEW LEBANON-A',
    'JACKSON TOWNSHIP_NEW LEBANON B': 'JACK/NEW LEBANON-B',
}
ABBR = [(r'\bJACKSON\b', 'JACK'), (r'\bPERRY\b', 'PER'),
        (r'\bWEST\b', 'W'),       (r'\bTOWNSHIP\b', 'TWP')]


def fail(msg):
    print("FAIL: " + msg)
    sys.exit(1)


def round_coords(coords):
    if isinstance(coords, (list, tuple)):
        if coords and isinstance(coords[0], (int, float)):
            return [round(float(c), 5) for c in coords]
        return [round_coords(c) for c in coords]
    return coords


def make_feature(geometry, properties):
    mapped = shapely.geometry.mapping(geometry)
    geom_out = {"type": mapped["type"], "coordinates": round_coords(mapped["coordinates"])}
    return {"type": "Feature", "properties": properties, "geometry": geom_out}


def party_lean(data, source_label):
    if not data or "chartConfig" not in data:
        fail(f"malformed party-affiliation JSON: {source_label}")
    sides = cohort_sides(data["chartConfig"], source_label)
    total = sides["total"]
    if total <= 0:
        fail(f"non-positive total_voters for {source_label}: {total}")
    lean = round((sides["d"] - sides["r"]) / total, 4)
    if not (-1 <= lean <= 1):
        fail(f"lean out of [-1,1] range for {source_label}: {lean}")
    return lean, int(total)


def load_party_json(path, join_id):
    if not path.exists():
        fail(f"missing party-affiliation JSON for id '{join_id}': {path}")
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def norm(s):
    s = OVERRIDE.get(s.strip(), s).upper().strip()
    for a, b in ABBR:
        s = re.sub(a, b, s)
    s = re.sub(r'[^A-Z0-9]', '', s)               # drop punctuation/space
    s = re.sub(r'(?<=[A-Z])0+(?=[0-9])', '', s)   # CLAYTON01A -> CLAYTON1A
    return s


def load_precinct_index():
    if not PRECINCT_INDEX.exists():
        fail(f"missing precinct index: {PRECINCT_INDEX}")
    with open(PRECINCT_INDEX, encoding="utf-8") as f:
        index = json.load(f)
    precincts = index.get("precincts") or []
    if not precincts:
        fail(f"precinct index has no 'precincts' array: {PRECINCT_INDEX}")
    if len(precincts) != EXPECTED_PRECINCTS:
        fail(f"precinct index count = {len(precincts)}, expected {EXPECTED_PRECINCTS}")
    return precincts


def read_geometry():
    if not GDB_PATH.exists():
        fail(f"missing precinct GDB: {GDB_PATH}")
    print(f"Reading {GDB_LAYER} from {GDB_PATH.name} ...")
    gdf = gpd.read_file(GDB_PATH, layer=GDB_LAYER)
    raw_rows = len(gdf)
    if "VNAME" not in gdf.columns:
        fail(f"layer {GDB_LAYER} has no VNAME column; columns={list(gdf.columns)}")
    gdf = gdf.dissolve(by="VNAME").reset_index()
    print(f"  dissolve(by='VNAME'): {raw_rows} multipart rows -> {len(gdf)} precincts")
    if len(gdf) != EXPECTED_PRECINCTS:
        fail(f"dissolved precinct count = {len(gdf)}, expected {EXPECTED_PRECINCTS}")
    return gdf.to_crs(4326), raw_rows


def build_crosswalk(gdf, precincts):
    # Normalize both sides and join on the result.
    geo_keys = {}
    for vname in gdf["VNAME"]:
        key = norm(str(vname))
        if key in geo_keys:
            fail(f"normalizer collision on geometry side: '{vname}' and "
                 f"'{geo_keys[key]}' both normalize to '{key}'")
        geo_keys[key] = vname

    idx_keys = {}
    for p in precincts:
        key = norm(str(p["name"]))
        if key in idx_keys:
            fail(f"normalizer collision on index side: '{p['name']}' and "
                 f"'{idx_keys[key]['name']}' both normalize to '{key}'")
        idx_keys[key] = p

    only_geo = sorted(geo_keys[k] for k in set(geo_keys) - set(idx_keys))
    only_idx = sorted(idx_keys[k]["name"] for k in set(idx_keys) - set(geo_keys))
    matched = len(set(geo_keys) & set(idx_keys))

    print(f"Crosswalk match: {matched}/{EXPECTED_PRECINCTS}")
    if only_geo or only_idx:
        print(f"  unmatched on geometry side ({len(only_geo)}): {only_geo}")
        print(f"  unmatched on index side ({len(only_idx)}): {only_idx}")
        fail("crosswalk incomplete -- every precinct must match on both sides")
    if matched != EXPECTED_PRECINCTS:
        fail(f"matched {matched}, expected {EXPECTED_PRECINCTS}")

    return {vname: idx_keys[norm(str(vname))] for vname in gdf["VNAME"]}


def run_pass(gdf, raw_rows, crosswalk, rows, tolerance, out_path, layer_name,
             max_target_kb, hard_fail_kb):
    """Simplify at `tolerance` and write one GeoJSON + provenance entry.

    `gdf`/`raw_rows`/`crosswalk`/`rows` are shared across passes -- the
    dissolve, crosswalk, and party-lean join do not depend on simplify
    tolerance, so re-deriving them per pass would risk the two outputs
    silently drifting (e.g. a crosswalk edge case resolving differently on a
    second GDB read) instead of sharing one join.
    """
    generated = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    bounds = [float(round(x, 5)) for x in gdf.total_bounds]  # [W,S,E,N]

    simplified = [(geom.simplify(tolerance, preserve_topology=True), props) for geom, props in rows]
    fc = {
        "type": "FeatureCollection",
        "layer": layer_name,
        "generated": generated,
        "bounds": bounds,
        "features": [make_feature(geom, props) for geom, props in simplified],
    }
    text = json.dumps(fc, separators=(",", ":"))

    size_kb = len(text.encode("utf-8")) / 1024
    if size_kb <= max_target_kb:
        status = "OK"
    elif size_kb <= hard_fail_kb:
        status = f"WARN (over {max_target_kb}KB target)"
    else:
        status = "HARD-FAIL"
    print(f"{out_path.name} size = {size_kb:.1f} KB [{status}] tolerance={tolerance}")
    if size_kb > hard_fail_kb:
        fail(f"{out_path.name} exceeds {hard_fail_kb}KB hard ceiling: {size_kb:.1f} KB. "
             "Increase simplify tolerance and rerun.")

    feature_count = len(fc["features"])
    if feature_count != EXPECTED_PRECINCTS:
        fail(f"{out_path.name}: wrote {feature_count} features, expected {EXPECTED_PRECINCTS}")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8", newline="\n") as f:
        f.write(text)
    print(f"Wrote {out_path} ({size_kb:.1f} KB, {feature_count} features)")

    write_provenance(layer_name, generated, raw_rows, feature_count, size_kb, tolerance)


def run_build():
    precincts = load_precinct_index()
    gdf, raw_rows = read_geometry()
    crosswalk = build_crosswalk(gdf, precincts)

    rows = []
    for vname, geom in zip(gdf["VNAME"], gdf.geometry):
        p = crosswalk[vname]
        safe_name = p["safe_name"]
        label = f"{COUNTY_SLUG}:{safe_name}"
        party_path = DOCS_DATA / f"{COUNTY_SLUG}_precinct_{safe_name}_party.json"
        lean, total = party_lean(load_party_json(party_path, label), label)
        rows.append((geom, {
            "safe_name": safe_name,
            "name": p["name"],
            "lean": lean,
            "total_voters": total,
        }))

    run_pass(gdf, raw_rows, crosswalk, rows, TOLERANCE, OUT_PATH, LAYER_NAME,
              MAX_TARGET_KB, HARD_FAIL_KB)
    run_pass(gdf, raw_rows, crosswalk, rows, DETAIL_TOLERANCE, DETAIL_OUT_PATH,
              DETAIL_LAYER_NAME, DETAIL_HARD_FAIL_KB, DETAIL_HARD_FAIL_KB)


def write_provenance(layer_name, generated, raw_rows, feature_count, size_kb, tolerance):
    if not PROV_PATH.exists():
        fail(f"missing provenance file: {PROV_PATH} (run build_state_map.py first)")
    with open(PROV_PATH, encoding="utf-8") as f:
        provenance = json.load(f)
    with open(MANIFEST_PATH, encoding="utf-8") as f:
        snapshot = json.load(f).get("provenanceSnapshotDate")
    # These layers merge into the file build_state_map.py writes; refuse to
    # merge into one stamped for a different snapshot.
    if not snapshot or provenance.get("provenance_snapshot_date") != snapshot:
        fail(f"provenance.json is for snapshot {provenance.get('provenance_snapshot_date')!r}, "
             f"manifest is {snapshot!r}: run build_state_map.py first")

    provenance.setdefault("layers", {})[layer_name] = {
        "provenance_snapshot_date": snapshot,
        "source": str(GDB_PATH.relative_to(REPO_ROOT)).replace("\\", "/"),
        "source_layer": GDB_LAYER,
        "plan_vintage": GDB_VINTAGE,
        "vintage_note": (
            "precinct geometry is 2022 vintage -- it differs from the voter "
            "file's snapshot vintage"
        ),
        "generated": generated,
        "raw_rows": raw_rows,
        "dissolve_by": "VNAME",
        "feature_count": feature_count,
        "matched_count": f"{feature_count}/{EXPECTED_PRECINCTS}",
        "party_data_join": (
            "docs/data/montgomery_precinct_<safe_name>_party.json; the source "
            "GDB's TOTAL_VOTERS/DEM_VOTERS/REP_VOTERS columns are an "
            "independent tally and are deliberately unused"
        ),
        "name_overrides": dict(OVERRIDE),
        "name_abbreviations": [[a, b] for a, b in ABBR],
        "simplify_tolerance": tolerance,
        "output_size_kb": round(size_kb, 1),
        "county_coverage": "1/88 (Montgomery only)",
    }
    if layer_name == DETAIL_LAYER_NAME:
        provenance["layers"][layer_name]["usage"] = (
            "fetched lazily by the dashboard only when the zoomed selection "
            "inset opens for a precinct-level selection; not loaded on "
            "initial page load and not subject to the overview's 300KB "
            "statewide budget"
        )

    with open(PROV_PATH, "w", encoding="utf-8", newline="\n") as f:
        json.dump(provenance, f, indent=2)
        f.write("\n")
    print(f"Updated {PROV_PATH}")


if __name__ == "__main__":
    run_build()
