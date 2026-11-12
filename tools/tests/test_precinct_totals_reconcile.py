"""Published precinct totals reconcile with the party charts built from the same rows.

Regression for evaluation item P5. `export_precinct_charts` writes a precinct's index
`total` as its row count and its party chart as per-cohort counts, in one loop. When a
cohort fell outside the chart slices (non-R/D affiliation -> OTHER), the index kept
counting those voters and the chart dropped them: Montgomery's index summed to 365,310
and its party charts to 364,521, a gap that survived a full regeneration.

Both checks are the same scope (one precinct; one county), so they must be equal. This is
not CLAUDE.md sec 4's "two totals" case, which compares a geo-tree node against a
jurisdiction's own row-accurate total.
"""
import json
from pathlib import Path

import pytest

DOCS = Path(__file__).resolve().parent.parent.parent / "docs"
DATA = DOCS / "data"


def _chart_sum(path: Path) -> int:
    cfg = json.loads(path.read_text(encoding="utf-8"))["chartConfig"]
    return int(sum(v or 0 for v in cfg["datasets"][0]["data"]))


def _indexes():
    found = sorted(DATA.glob("*_precinct_index.json")) if DATA.exists() else []
    if not found:
        pytest.skip("docs/data precinct indexes not present")
    return found


def test_precinct_index_total_equals_party_chart_sum():
    bad = {}
    for idx in _indexes():
        for p in json.loads(idx.read_text(encoding="utf-8"))["precincts"]:
            party = DOCS / p["partyUrl"]
            got = _chart_sum(party)
            if got != p["total"]:
                county = idx.name[: -len("_precinct_index.json")]
                bad.setdefault(county, []).append((p["name"], p["total"], got))
    summary = {c: f"{len(v)} precincts, first {v[0]}" for c, v in bad.items()}
    assert not bad, f"index total != party chart sum (name, index, chart) by county: {summary}"


def test_county_party_chart_equals_sum_of_precinct_totals():
    bad = {}
    for idx in _indexes():
        county = idx.name[: -len("_precinct_index.json")]
        hero = DATA / f"{county}_party_affiliation.json"
        if not hero.exists():
            continue
        index_sum = sum(p["total"] for p in json.loads(idx.read_text(encoding="utf-8"))["precincts"])
        chart = _chart_sum(hero)
        if chart != index_sum:
            bad[county] = (index_sum, chart)
    assert not bad, f"(precinct index sum, county party chart) by county: {bad}"
