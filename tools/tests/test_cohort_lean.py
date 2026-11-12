"""Party lean is computed by cohort label, against one canonical 9-cohort set.

Pins together the three places the cohort taxonomy is spelled out for lean arithmetic:
the pipeline writers' COHORT_SLICES, pipeline/cohort_lean.py, and the dashboard's
COHORT_SIDE table in docs/assets/v2.js. Also gates the published data: every party chart
under docs/data must carry the canonical label set, so a partial regeneration that leaves
an older cohort scheme on disk is caught here instead of on the live site.
"""
import json
import re
from pathlib import Path

import pytest

from pipeline.cohort_lean import COHORT_SIDE, cohort_sides, normalize_label

ROOT = Path(__file__).resolve().parent.parent.parent
V2_JS = ROOT / "docs" / "assets" / "v2.js"
DOCS_DATA = ROOT / "docs" / "data"
# The statewide file predates the cohort taxonomy (UNC/REP/DEM) and is not read for lean;
# replacing it is tracked separately.
STATEWIDE_PARTY = DOCS_DATA / "ohio_(statewide)_party_affiliation.json"


def _chart(pairs):
    return {"labels": [l for l, _ in pairs], "datasets": [{"data": [v for _, v in pairs]}]}


CANONICAL = [
    ("Pure R", 100),
    ("UNC – Lapsed R", 50),
    ("Mixed – Active", 10),
    ("Mixed – Lapsed", 20),
    ("Minor Party", 5),
    ("UNC – Non-partisan Ballot", 30),
    ("UNC – No Primary", 400),
    ("UNC – Lapsed D", 70),
    ("Pure D", 90),
]


# ── taxonomy contract ────────────────────────────────────────────────────────

def test_side_table_matches_cleaner_cohort_slices():
    pytest.importorskip("polars")
    from pipeline.voter_data_cleaner import COHORT_SLICES
    assert {lbl for _, lbl, _ in COHORT_SLICES} == set(COHORT_SIDE)


def test_side_table_matches_groupings_cohort_slices():
    pytest.importorskip("polars")
    pytest.importorskip("pandas")
    from pipeline.jurisdictional_groupings import COHORT_SLICES
    assert {lbl for _, lbl, _ in COHORT_SLICES} == set(COHORT_SIDE)


def test_js_side_table_matches_python():
    src = V2_JS.read_text(encoding="utf-8")
    block = re.search(r"const COHORT_SIDE = \{(.*?)\};", src, re.S)
    assert block, "COHORT_SIDE table not found in v2.js"
    js = dict(re.findall(r"'([^']+)':\s*'(R|UNC|D)'", block.group(1)))
    assert js == COHORT_SIDE


# ── positional mirrors ───────────────────────────────────────────────────────
# These tables are read by INDEX (ribbon segment N -> entry N), so a set check is not
# enough: order must equal COHORT_SLICES order. The captain table drifted once, to 7
# entries against 8 slices, which sent every tap after index 4 to the wrong roster.

def _slices():
    pytest.importorskip("polars")
    from pipeline.voter_data_cleaner import COHORT_SLICES
    return COHORT_SLICES


def _js_array(src, name):
    block = re.search(rf"const {name} = \[(.*?)\];", src, re.S)
    assert block, f"{name} not found"
    return re.findall(r"'([^']+)'", block.group(1))


def test_js_label_and_color_arrays_follow_slice_order():
    src = V2_JS.read_text(encoding="utf-8")
    slices = _slices()
    assert _js_array(src, "COHORT_LABELS") == [lbl for _, lbl, _ in slices]
    assert _js_array(src, "COHORT_COLORS") == [c for _, _, c in slices]


def test_captain_cohorts_follow_slice_order():
    src = (ROOT / "docs" / "captain" / "captain-mode.js").read_text(encoding="utf-8")
    block = re.search(r"const COHORTS = \[(.*?)\];", src, re.S)
    assert block, "COHORTS not found in captain-mode.js"
    slugs = re.findall(r"slug:\s*'([A-Z_]+)'", block.group(1))
    assert slugs == [fam for fam, _, _ in _slices()]


def test_roster_api_cohorts_follow_slice_order():
    # Parsed, not imported: serve/ pulls in web dependencies the test suite does not need.
    import ast
    tree = ast.parse((ROOT / "serve" / "roster_api.py").read_text(encoding="utf-8"))
    consts = {t.id: node.value for node in tree.body if isinstance(node, ast.Assign)
              for t in node.targets if isinstance(t, ast.Name)}
    families = [fam for fam, _, _ in _slices()]
    assert ast.literal_eval(consts["VALID_COHORTS"]) == set(families)
    assert [s for s, _, _ in ast.literal_eval(consts["COHORT_SPEC"])] == families


# ── arithmetic ───────────────────────────────────────────────────────────────

def test_sums_by_side():
    s = cohort_sides(_chart(CANONICAL), "synthetic")
    assert (s["r"], s["unc"], s["d"]) == (150, 465, 160)
    assert (s["pure_r"], s["pure_d"]) == (100, 90)
    assert s["total"] == 775
    # Integer counts stay integers: a float total printed "123,527.0 registered voters".
    assert all(isinstance(s[k], int) for k in ("r", "unc", "d", "pure_r", "pure_d", "total"))


def test_order_independent():
    """The bug this module exists for: an inserted cohort shifted every later index."""
    shuffled = list(reversed(CANONICAL))
    assert cohort_sides(_chart(shuffled), "reversed") == cohort_sides(_chart(CANONICAL), "canonical")


def test_count_suffix_is_stripped():
    suffixed = [(f"{l} — {v:,} (1%)", v) for l, v in CANONICAL]
    assert cohort_sides(_chart(suffixed), "suffixed") == cohort_sides(_chart(CANONICAL), "bare")
    assert normalize_label("UNC – Lapsed R — 4,911 (4%)") == "UNC – Lapsed R"


def test_rejects_eight_cohort_scheme():
    """The pre-2026-11 scheme had no Minor Party slice and silently dropped those voters."""
    eight = [p for p in CANONICAL if p[0] != "Minor Party"]
    with pytest.raises(ValueError, match="missing=\\['Minor Party'\\]"):
        cohort_sides(_chart(eight), "eight")


def test_rejects_seven_cohort_scheme():
    seven = [p for p in CANONICAL if p[0] not in ("UNC – Non-partisan Ballot", "Minor Party")]
    with pytest.raises(ValueError, match="UNC – Non-partisan Ballot"):
        cohort_sides(_chart(seven), "seven")


def test_rejects_unknown_and_duplicate_labels():
    unknown = CANONICAL[:-1] + [("Pure Green", 1)]
    with pytest.raises(ValueError, match="Pure Green"):
        cohort_sides(_chart(unknown), "unknown")
    dup = CANONICAL[:-1] + [("Pure R", 1)]
    with pytest.raises(ValueError):
        cohort_sides(_chart(dup), "duplicate")


def test_narrative_error_names_the_file():
    """A failing chart must be identifiable: the title alone ('Party Affiliation') is not."""
    from tools.narrative.templates import build_metrics_for_level
    eight = [p for p in CANONICAL if p[0] != "Minor Party"]
    party = {"title": "Party Affiliation", "chartConfig": _chart(eight)}
    with pytest.raises(ValueError, match="township/adams_bratton_township"):
        build_metrics_for_level("township", party, source="township/adams_bratton_township")


def test_rejects_length_mismatch():
    chart = _chart(CANONICAL)
    chart["datasets"][0]["data"].pop()
    with pytest.raises(ValueError, match="data values"):
        cohort_sides(chart, "short")


def test_real_county_file_sums_to_its_own_total():
    path = DOCS_DATA / "montgomery_party_affiliation.json"
    if not path.exists():
        pytest.skip("published Montgomery chart not present")
    cfg = json.loads(path.read_text(encoding="utf-8"))["chartConfig"]
    s = cohort_sides(cfg, path.name)
    assert s["total"] == sum(float(v or 0) for v in cfg["datasets"][0]["data"])
    # The positional formula read No Primary as Democratic: on this file it would put
    # more than half of all voters on the D side. By label, D is a minority share.
    assert s["d"] / s["total"] < 0.5


# ── published-data gate ──────────────────────────────────────────────────────

def test_every_published_party_chart_uses_canonical_cohorts():
    """Fails while any level was left on an older cohort scheme by a partial regen."""
    if not DOCS_DATA.exists():
        pytest.skip("docs/data not present")
    expected = set(COHORT_SIDE)
    stale = {}
    for p in DOCS_DATA.rglob("*party_affiliation.json"):
        if p == STATEWIDE_PARTY:
            continue
        labels = json.loads(p.read_text(encoding="utf-8"))["chartConfig"]["labels"]
        if {normalize_label(l) for l in labels} != expected or len(labels) != len(expected):
            level = p.relative_to(DOCS_DATA).parts[0] if p.parent != DOCS_DATA else "(root)"
            stale[level] = stale.get(level, 0) + 1
    assert not stale, f"party charts not on the canonical cohort set, by level: {stale}"
