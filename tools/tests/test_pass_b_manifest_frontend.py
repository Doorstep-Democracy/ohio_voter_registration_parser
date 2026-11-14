"""
Pass B — manifest.json jurisdictionScopes tests.

Validates the jurisdictionScopes registry in manifest.json (12 jurisdiction
types, county-scoping flags, dirName/index.json linkage against docs/data/).

History: this file originally also covered the V1 frontend (docs/index.html,
docs/charts.js) — a dropdown-select UI (jur-type-select / jur-county-select /
jur-name-select) driven by charts.js symbols like _setupJurisdictionControls.
That UI was replaced by the tree-nav hierarchy in docs/index.htm +
assets/v2.js; docs/index.html no longer exists and docs/charts.js is
unreferenced dead code. Those tests were removed rather than fixed. Two
charts.js tests (atomic .tmp write, classifier-mtime cache freshness) were
actually testing pipeline cache behavior via string-grep on source text —
duplicated, more rigorously, by test_pass_b_cache.py's monkeypatched
behavioral tests — so they were dropped rather than repointed.
"""
import json
from pathlib import Path

import pytest

from pipeline.paths import DATA_DIR, DOCS_DIR as DOCS

BASE     = Path(__file__).resolve().parent.parent.parent
MANIFEST = DOCS / "manifest.json"

COUNTY_SCOPED_KEYS = {"townships", "villages", "municipal_court_districts"}
EXPECTED_TYPE_KEYS = {
    "townships", "villages", "municipal_court_districts",
    "cities", "local_school_districts", "city_school_districts",
    "exempted_vill_school_districts", "state_senate_districts",
    "state_rep_districts", "congressional_districts",
    "county_court_districts", "court_of_appeals",
}


@pytest.fixture(scope="module")
def manifest():
    return json.loads(MANIFEST.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def jur_scopes(manifest):
    return manifest.get("jurisdictionScopes", [])


class TestManifestJurisdictionScopes:
    def test_jurisdiction_scopes_key_exists(self, manifest):
        assert "jurisdictionScopes" in manifest, "manifest.json missing 'jurisdictionScopes'"

    def test_all_12_types_present(self, jur_scopes):
        keys = {s["key"] for s in jur_scopes}
        missing = EXPECTED_TYPE_KEYS - keys
        assert not missing, f"jurisdictionScopes missing types: {missing}"

    def test_each_scope_has_required_fields(self, jur_scopes):
        required = {"key", "display", "displayPlural", "dirName", "countyScoped"}
        for sc in jur_scopes:
            missing = required - set(sc.keys())
            assert not missing, f"scope {sc.get('key')} missing fields {missing}"

    def test_county_scoped_flags_correct(self, jur_scopes):
        for sc in jur_scopes:
            expected = sc["key"] in COUNTY_SCOPED_KEYS
            assert sc["countyScoped"] == expected, \
                f"{sc['key']}: countyScoped should be {expected}, got {sc['countyScoped']}"

    def test_dir_names_match_actual_directories(self, jur_scopes):
        for sc in jur_scopes:
            d = DATA_DIR / sc["dirName"]
            assert d.is_dir(), f"dirName {sc['dirName']!r} for {sc['key']} not found in docs/data/"

    def test_index_json_exists_for_each_scope(self, jur_scopes):
        for sc in jur_scopes:
            idx = DATA_DIR / sc["dirName"] / "index.json"
            assert idx.exists(), f"Missing index.json for {sc['key']} ({sc['dirName']})"
