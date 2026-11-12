"""
Build choropleth-ready GeoJSON layers for the Ohio statewide landing map.

Reads county + district boundary geometry, joins already-published
party-affiliation cohort data (docs/data/*_party_affiliation.json), and
writes small simplified GeoJSON files to docs/data/state_map/ for the
landing-page choropleth. See
local/context/handoffs/HANDOFF_1_SONNET_STATE_MAP_DATA.md for full spec.

Usage:
    uv run python tools/admin/build_state_map.py
    uv run python tools/admin/build_state_map.py --check-congress-vintage
"""

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import geopandas as gpd
import polars as pl
import shapely.geometry

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
from pipeline.cohort_lean import cohort_sides  # noqa: E402

GEO_DIR = REPO_ROOT / "local" / "source" / "Geo"
COUNTIES_GPKG = GEO_DIR / "ohio_census_consolidated.gpkg"
SOS_DIR = GEO_DIR / "SOS District Maps"

DOCS_DATA = REPO_ROOT / "docs" / "data"
OUT_DIR = DOCS_DATA / "state_map"
MANIFEST_PATH = REPO_ROOT / "docs" / "manifest.json"

ENRICHED_PARQUET = REPO_ROOT / "local" / "source" / "parquet_enriched" / "enriched_voters.parquet"
RAW_PARQUET = REPO_ROOT / "local" / "source" / "parquet" / "raw_voters.parquet"

MAX_TARGET_KB = 300
HARD_FAIL_KB = 600

# Ohio SOS official county numbering: 01-88, strictly alphabetical by county
# name. Mirrors pipeline/voter_data_cleaner.py OHIO_COUNTIES -- duplicated
# here (not imported) so this tool has no runtime dependency on pipeline/.
OHIO_COUNTIES = {
    '01': 'Adams',       '02': 'Allen',       '03': 'Ashland',     '04': 'Ashtabula',
    '05': 'Athens',      '06': 'Auglaize',    '07': 'Belmont',     '08': 'Brown',
    '09': 'Butler',      '10': 'Carroll',     '11': 'Champaign',   '12': 'Clark',
    '13': 'Clermont',    '14': 'Clinton',     '15': 'Columbiana',  '16': 'Coshocton',
    '17': 'Crawford',    '18': 'Cuyahoga',    '19': 'Darke',       '20': 'Defiance',
    '21': 'Delaware',    '22': 'Erie',        '23': 'Fairfield',   '24': 'Fayette',
    '25': 'Franklin',    '26': 'Fulton',      '27': 'Gallia',      '28': 'Geauga',
    '29': 'Greene',      '30': 'Guernsey',    '31': 'Hamilton',    '32': 'Hancock',
    '33': 'Hardin',      '34': 'Harrison',    '35': 'Henry',       '36': 'Highland',
    '37': 'Hocking',     '38': 'Holmes',      '39': 'Huron',       '40': 'Jackson',
    '41': 'Jefferson',   '42': 'Knox',        '43': 'Lake',        '44': 'Lawrence',
    '45': 'Licking',     '46': 'Logan',       '47': 'Lorain',      '48': 'Lucas',
    '49': 'Madison',     '50': 'Mahoning',    '51': 'Marion',      '52': 'Medina',
    '53': 'Meigs',       '54': 'Mercer',      '55': 'Miami',       '56': 'Monroe',
    '57': 'Montgomery',  '58': 'Morgan',      '59': 'Morrow',      '60': 'Muskingum',
    '61': 'Noble',       '62': 'Ottawa',      '63': 'Paulding',    '64': 'Perry',
    '65': 'Pickaway',    '66': 'Pike',        '67': 'Portage',     '68': 'Preble',
    '69': 'Putnam',      '70': 'Richland',    '71': 'Ross',        '72': 'Sandusky',
    '73': 'Scioto',      '74': 'Seneca',      '75': 'Shelby',      '76': 'Stark',
    '77': 'Summit',      '78': 'Trumbull',    '79': 'Tuscarawas',  '80': 'Union',
    '81': 'Van Wert',    '82': 'Vinton',      '83': 'Warren',      '84': 'Washington',
    '85': 'Wayne',       '86': 'Williams',    '87': 'Wood',        '88': 'Wyandot',
}

COUNTY_TOLERANCE = 0.003

DISTRICT_LAYERS = {
    "house": {
        "shapefile": SOS_DIR / "2024-2032-hd-shapefile.zip",
        "expected_count": 99,
        "dtype": "state_representative_district",
        "index_dir": "state_representative_district",
        "plan_vintage": "2024-2032 adopted plan",
        "tolerance": 0.002,
    },
    "senate": {
        "shapefile": SOS_DIR / "2024-2032-sd-shapefile.zip",
        "expected_count": 33,
        "dtype": "state_senate_district",
        "index_dir": "state_senate_district",
        "plan_vintage": "2024-2032 adopted plan",
        "tolerance": 0.002,
    },
    "congress": {
        "shapefile": SOS_DIR / "uscongressionaldistricts-2026-2032-adopted-2025-10-31-shapefiles.zip",
        "expected_count": 15,
        "dtype": "congressional_district",
        "index_dir": "congressional_district",
        "plan_vintage": "2026-2032 adopted plan",
        "tolerance": 0.002,
    },
}


def fail(msg):
    print("FAIL: " + msg)
    sys.exit(1)


def county_slug(name):
    # Byte-identical to countyToSlug() in docs/assets/v2.js:211-213.
    return re.sub(r'[^a-z0-9]+', '_', name.lower()).strip('_')


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


def load_manifest_county_slugs():
    if not MANIFEST_PATH.exists():
        fail(f"manifest not found: {MANIFEST_PATH}")
    with open(MANIFEST_PATH, encoding="utf-8") as f:
        manifest = json.load(f)
    names = manifest.get("processedCounties") or manifest.get("allCounties") or []
    if not names:
        fail("manifest.json has no processedCounties/allCounties")
    return {county_slug(n) for n in names}


def build_counties(manifest_slugs):
    print("Reading counties layer...")
    gdf = gpd.read_file(COUNTIES_GPKG, layer="counties")
    gdf = gdf.to_crs(4326)
    if len(gdf) != 88:
        fail(f"counties feature count = {len(gdf)}, expected 88")

    gdf["slug"] = gdf["NAME"].apply(county_slug)
    slugs = gdf["slug"].tolist()
    if len(set(slugs)) != len(slugs):
        dupes = sorted({s for s in slugs if slugs.count(s) > 1})
        fail(f"duplicate county slugs: {dupes}")

    slug_set = set(slugs)
    if slug_set != manifest_slugs:
        missing = manifest_slugs - slug_set
        extra = slug_set - manifest_slugs
        fail(f"county slug set mismatch vs manifest.json. missing={sorted(missing)} extra={sorted(extra)}")

    print("PASS counties: feature_count=88, id_set matches manifest.json, no duplicates")

    bounds = [float(round(x, 5)) for x in gdf.total_bounds]  # [minx,miny,maxx,maxy] == [W,S,E,N]

    rows = []
    for slug, name, geom in zip(gdf["slug"], gdf["NAME"], gdf.geometry):
        party_path = DOCS_DATA / f"{slug}_party_affiliation.json"
        data = load_party_json(party_path, slug)
        lean, total = party_lean(data, slug)
        props = {"name": name, "slug": slug, "lean": lean, "total_voters": total}
        rows.append((geom, props))

    return rows, bounds


def build_district_layer(layer_name, config):
    print(f"Reading {layer_name} layer...")
    gdf = gpd.read_file("zip://" + str(config["shapefile"]))
    gdf = gdf.to_crs(4326)
    expected = config["expected_count"]
    if len(gdf) != expected:
        fail(f"{layer_name} feature count = {len(gdf)}, expected {expected}")

    gdf["district_id"] = gdf["DISTRICT"].apply(lambda v: str(int(str(v).strip())).zfill(2))
    ids = gdf["district_id"].tolist()
    if len(set(ids)) != len(ids):
        dupes = sorted({i for i in ids if ids.count(i) > 1})
        fail(f"duplicate {layer_name} ids: {dupes}")

    index_path = DOCS_DATA / config["index_dir"] / "index.json"
    if not index_path.exists():
        fail(f"missing index.json for {layer_name}: {index_path}")
    with open(index_path, encoding="utf-8") as f:
        index = json.load(f)
    index_slugs = {row["slug"] for row in index}

    id_set = set(ids)
    if id_set != index_slugs:
        missing = index_slugs - id_set
        extra = id_set - index_slugs
        fail(f"{layer_name} id set mismatch vs index.json. missing={sorted(missing)} extra={sorted(extra)}")

    print(f"PASS {layer_name}: feature_count={expected}, id_set matches index.json, no duplicates")

    rows = []
    for did, geom in zip(gdf["district_id"], gdf.geometry):
        party_path = DOCS_DATA / config["index_dir"] / f"{did}_party_affiliation.json"
        data = load_party_json(party_path, f"{layer_name}:{did}")
        lean, total = party_lean(data, f"{layer_name}:{did}")
        props = {
            "id": did,
            "dtype": config["dtype"],
            "name": f"District {int(did)}",
            "lean": lean,
            "total_voters": total,
        }
        rows.append((geom, props))

    return rows


def simplify_rows(rows, tolerance):
    return [(geom.simplify(tolerance, preserve_topology=True), props) for geom, props in rows]


def serialize_layer(layer_name, rows, bounds, generated):
    features = [make_feature(geom, props) for geom, props in rows]
    fc = {
        "type": "FeatureCollection",
        "layer": layer_name,
        "generated": generated,
        "bounds": bounds,
        "features": features,
    }
    return json.dumps(fc, separators=(",", ":"))


def run_build():
    generated = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    manifest_slugs = load_manifest_county_slugs()

    county_rows, bounds = build_counties(manifest_slugs)

    district_rows = {}
    for layer_name, config in DISTRICT_LAYERS.items():
        district_rows[layer_name] = build_district_layer(layer_name, config)

    tolerances = {"counties": COUNTY_TOLERANCE}
    tolerances.update({name: cfg["tolerance"] for name, cfg in DISTRICT_LAYERS.items()})

    # Simplify + serialize all four layers and check every gate (including
    # the size ceiling) BEFORE writing any output file.
    serialized = {
        "counties": serialize_layer(
            "counties", simplify_rows(county_rows, tolerances["counties"]), bounds, generated
        ),
    }
    for layer_name in DISTRICT_LAYERS:
        serialized[layer_name] = serialize_layer(
            layer_name, simplify_rows(district_rows[layer_name], tolerances[layer_name]), bounds, generated
        )

    sizes_kb = {}
    for layer_name, text in serialized.items():
        size_kb = len(text.encode("utf-8")) / 1024
        sizes_kb[layer_name] = size_kb
        if size_kb <= MAX_TARGET_KB:
            status = "OK"
        elif size_kb <= HARD_FAIL_KB:
            status = "WARN (over 300KB target)"
        else:
            status = "HARD-FAIL"
        print(f"{layer_name}.geojson size = {size_kb:.1f} KB [{status}] tolerance={tolerances[layer_name]}")

    over_hard = {name: round(kb, 1) for name, kb in sizes_kb.items() if kb > HARD_FAIL_KB}
    if over_hard:
        fail(f"file(s) exceed {HARD_FAIL_KB}KB hard ceiling: {over_hard}. Increase simplify tolerance and rerun.")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for layer_name, text in serialized.items():
        out_path = OUT_DIR / f"{layer_name}.geojson"
        with open(out_path, "w", encoding="utf-8", newline="\n") as f:
            f.write(text)
        print(f"Wrote {out_path} ({sizes_kb[layer_name]:.1f} KB)")

    provenance = {
        "generated": generated,
        "party_data_join": (
            "docs/data/<county_slug>_party_affiliation.json (counties); "
            "docs/data/<dtype>/<id>_party_affiliation.json (districts); "
            "manifest snapshot 2026-07-14 per current docs/manifest.json"
        ),
        "layers": {
            "counties": {
                "source": str(COUNTIES_GPKG.relative_to(REPO_ROOT)).replace("\\", "/"),
                "layer": "counties",
                "note": "full-resolution TIGER/Line county geometry (not generalized cb_*_500k)",
                "feature_count": len(county_rows),
                "simplify_tolerance": tolerances["counties"],
                "output_size_kb": round(sizes_kb["counties"], 1),
            },
        },
    }
    for layer_name, config in DISTRICT_LAYERS.items():
        provenance["layers"][layer_name] = {
            "source": str(config["shapefile"].relative_to(REPO_ROOT)).replace("\\", "/"),
            "plan_vintage": config["plan_vintage"],
            "feature_count": len(district_rows[layer_name]),
            "simplify_tolerance": tolerances[layer_name],
            "output_size_kb": round(sizes_kb[layer_name], 1),
        }

    prov_path = OUT_DIR / "provenance.json"
    with open(prov_path, "w", encoding="utf-8", newline="\n") as f:
        json.dump(provenance, f, indent=2)
        f.write("\n")
    print(f"Wrote {prov_path}")


def check_congress_vintage():
    print("Running congressional plan vintage check (voter file vs 2026-2032 adopted shapefile)...")
    parquet_path = ENRICHED_PARQUET if ENRICHED_PARQUET.exists() else RAW_PARQUET
    if not parquet_path.exists():
        fail(f"no parquet source found at {ENRICHED_PARQUET} or {RAW_PARQUET}")

    print(f"  voter-file crosswalk from {parquet_path.relative_to(REPO_ROOT)}")
    df = (
        pl.scan_parquet(str(parquet_path))
        .select(["COUNTY_NUMBER", "CONGRESSIONAL_DISTRICT"])
        .group_by(["COUNTY_NUMBER", "CONGRESSIONAL_DISTRICT"])
        .agg(pl.len().alias("n"))
        .filter(pl.col("n") >= 1000)
        .collect()
    )
    voter_pairs = set()
    voter_count_by_pair = {}
    for row in df.iter_rows(named=True):
        cnum = row["COUNTY_NUMBER"]
        cname = OHIO_COUNTIES.get(cnum)
        if cname is None:
            fail(f"unknown COUNTY_NUMBER in parquet: {cnum!r}")
        pair = (county_slug(cname), row["CONGRESSIONAL_DISTRICT"])
        voter_pairs.add(pair)
        voter_count_by_pair[pair] = row["n"]
    print(f"  {len(voter_pairs)} voter-file (county, district) pairs with >= 1000 voters")

    print("  geometric crosswalk: intersecting counties x congress (unsimplified geometry)")
    counties_gdf = gpd.read_file(COUNTIES_GPKG, layer="counties").to_crs(4326)
    counties_gdf["slug"] = counties_gdf["NAME"].apply(county_slug)
    congress_cfg = DISTRICT_LAYERS["congress"]
    congress_gdf = gpd.read_file("zip://" + str(congress_cfg["shapefile"])).to_crs(4326)
    congress_gdf["district_id"] = congress_gdf["DISTRICT"].apply(lambda v: str(int(str(v).strip())).zfill(2))

    overlay = gpd.overlay(
        counties_gdf[["slug", "geometry"]],
        congress_gdf[["district_id", "geometry"]],
        how="intersection",
    )
    overlay_proj = overlay.to_crs(5070)  # CONUS Albers equal-area, for a real km^2 threshold
    overlay["area_km2"] = overlay_proj.geometry.area / 1_000_000

    filtered = overlay[overlay["area_km2"] > 1.0]
    geo_pairs = {(row["slug"], row["district_id"]) for _, row in filtered.iterrows()}
    area_by_pair = {(row["slug"], row["district_id"]): row["area_km2"] for _, row in filtered.iterrows()}
    print(f"  {len(geo_pairs)} geometric (county, district) pairs with > 1 km^2 overlap")

    only_voter = voter_pairs - geo_pairs
    only_geo = geo_pairs - voter_pairs

    if not only_voter and not only_geo:
        print("PASS: voter-file and geometric county<->congressional-district crosswalks match.")
        print("Congressional plan vintage: 2026-2032 adopted plan matches voter file.")
        return True

    print("MISMATCH: voter-file and geometric crosswalks disagree.")
    print(f"  pairs in voter file but not geometry ({len(only_voter)}):")
    for pair in sorted(only_voter):
        print(f"    {pair}  voters={voter_count_by_pair.get(pair)}")
    print(f"  pairs in geometry but not voter file ({len(only_geo)}):")
    for pair in sorted(only_geo):
        print(f"    {pair}  overlap_area_km2={area_by_pair.get(pair):.2f}")
    print("The voter file's CONGRESSIONAL_DISTRICT column may reflect a different")
    print("redistricting plan than the 2026-2032 adopted shapefile on disk. A small")
    print("overlap_area_km2 (a few km^2) on a geometry-only pair is usually a TIGER/")
    print("Line-vs-SOS-shapefile boundary sliver, not a real plan mismatch -- check")
    print("whether that county has ANY voters in that district before concluding")
    print("this is a genuine vintage problem.")
    print("DECISION POINT for the user -- see HANDOFF_1_SONNET_STATE_MAP_DATA.md.")
    return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check-congress-vintage", action="store_true")
    args = parser.parse_args()

    if args.check_congress_vintage:
        ok = check_congress_vintage()
        sys.exit(0 if ok else 1)

    run_build()


if __name__ == "__main__":
    main()
