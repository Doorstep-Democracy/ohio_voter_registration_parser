"""
Pass B — index.json structure tests.

Validates the per-type index.json files written to docs/data/{type}/
by build_jur_indexes.py / export_jurisdiction_index().
"""
import json

import pytest

from pipeline.paths import DATA_DIR as DOCS_DATA
COUNTY_SCOPED = {"township", "village", "municipal_court_district"}
REQUIRED_KEYS = {"slug", "name", "display_name", "county", "county_slug",
                 "voter_count", "charts"}
KNOWN_TYPES = {
    "city", "city_school_district", "congressional_district",
    "county_court_district", "court_of_appeals_district",
    "exempted_village_school_district", "local_school_district",
    "municipal_court_district", "state_representative_district",
    "state_senate_district", "township", "village",
}


def _load_index(type_dir_name: str) -> list:
    return json.loads((DOCS_DATA / type_dir_name / "index.json").read_text(encoding="utf-8"))


@pytest.fixture(params=sorted(KNOWN_TYPES))
def type_name(request):
    return request.param


class TestIndexJsonExists:
    def test_index_file_present(self, type_name):
        assert (DOCS_DATA / type_name / "index.json").exists(), \
            f"Missing docs/data/{type_name}/index.json"

    def test_index_is_nonempty_array(self, type_name):
        idx = _load_index(type_name)
        assert isinstance(idx, list) and len(idx) > 0, \
            f"{type_name}/index.json must be a non-empty array"


class TestIndexEntrySchema:
    def test_required_keys_present(self, type_name):
        idx = _load_index(type_name)
        for entry in idx[:10]:
            missing = REQUIRED_KEYS - set(entry.keys())
            assert not missing, f"{type_name}: {entry.get('slug')} missing keys {missing}"

    def test_slug_is_nonempty_string(self, type_name):
        for entry in _load_index(type_name)[:10]:
            assert isinstance(entry["slug"], str) and len(entry["slug"]) > 0

    def test_voter_count_positive(self, type_name):
        for entry in _load_index(type_name)[:10]:
            assert entry["voter_count"] > 0, \
                f"{type_name}: {entry['slug']} has voter_count={entry['voter_count']}"

    def test_charts_list_nonempty(self, type_name):
        for entry in _load_index(type_name)[:10]:
            assert isinstance(entry["charts"], list) and len(entry["charts"]) > 0, \
                f"{type_name}: {entry['slug']} has empty charts list"

    def test_party_affiliation_in_charts(self, type_name):
        """Every jurisdiction must have at least the primary chart."""
        for entry in _load_index(type_name)[:10]:
            assert "party_affiliation" in entry["charts"], \
                f"{type_name}: {entry['slug']} missing party_affiliation chart"


class TestCountyScopingInIndex:
    def test_county_scoped_entries_have_county(self, type_name):
        if type_name not in COUNTY_SCOPED:
            pytest.skip("not county-scoped")
        for entry in _load_index(type_name):
            assert isinstance(entry["county"], str) and len(entry["county"]) > 0, \
                f"{type_name}: {entry['slug']} missing county field"
            assert isinstance(entry["county_slug"], str) and len(entry["county_slug"]) > 0

    def test_non_scoped_entries_have_null_county(self, type_name):
        if type_name in COUNTY_SCOPED:
            pytest.skip("county-scoped — county is expected")
        for entry in _load_index(type_name)[:20]:
            assert entry["county"] is None, \
                f"{type_name}: non-scoped entry {entry['slug']} has county={entry['county']}"
            assert entry["county_slug"] is None

    def test_county_scoped_display_name_includes_county(self, type_name):
        if type_name not in COUNTY_SCOPED:
            pytest.skip("not county-scoped")
        import re
        pattern = re.compile(r"\(.+ Co\.\)$")
        for entry in _load_index(type_name)[:20]:
            assert pattern.search(entry["display_name"]), \
                f"{type_name}: {entry['slug']} display_name missing ' (X Co.)': {entry['display_name']}"


class TestIndexSlugConsistency:
    def test_slugs_match_actual_chart_files(self, type_name):
        type_dir = DOCS_DATA / type_name
        for entry in _load_index(type_name)[:5]:
            slug = entry["slug"]
            pa_file = type_dir / f"{slug}_party_affiliation.json"
            assert pa_file.exists(), \
                f"{type_name}: slug {slug!r} has no party_affiliation file"

    def test_no_duplicate_slugs(self, type_name):
        idx = _load_index(type_name)
        slugs = [e["slug"] for e in idx]
        dupes = [s for s in slugs if slugs.count(s) > 1]
        assert not dupes, f"{type_name}: duplicate slugs {set(dupes)}"
