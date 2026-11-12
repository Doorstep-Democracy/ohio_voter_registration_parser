import json
import logging
from pathlib import Path
import pytest
from pipeline import ohio_voter_pipeline

def test_build_city_county_map_collisions(tmp_path, monkeypatch):
    # Point the module's DATA_DIR (imported from pipeline/paths.py) at tmp_path
    data_dir = tmp_path / "docs" / "data"
    data_dir.mkdir(parents=True)
    monkeypatch.setattr(ohio_voter_pipeline, "DATA_DIR", data_dir)
    
    # 1. Synthetic same-class collision with NO shared identity -> warned/excluded
    c1_index = {
        "precincts": [
            {"city": "FAKE_COLLISION", "local_school_district": "DISTRICT A"}
        ]
    }
    c2_index = {
        "precincts": [
            {"city": "FAKE_COLLISION", "local_school_district": "DISTRICT B"}
        ]
    }
    (data_dir / "county1_precinct_index.json").write_text(json.dumps(c1_index))
    (data_dir / "county2_precinct_index.json").write_text(json.dumps(c2_index))
    
    # 2. Positively-shared real -> still merge
    c3_index = {
        "precincts": [
            {"city": "SHARED_REAL", "municipal_court_district": "COURT X"}
        ]
    }
    c4_index = {
        "precincts": [
            {"city": "SHARED_REAL", "municipal_court_district": "COURT X"}
        ]
    }
    (data_dir / "county3_precinct_index.json").write_text(json.dumps(c3_index))
    (data_dir / "county4_precinct_index.json").write_text(json.dumps(c4_index))
    
    # 3. Allowlisted name -> merge regardless of identity data
    c5_index = {
        "precincts": [
            {"city": "WESTERVILLE", "city_school_district": "WESTERVILLE CITY SD"}
        ]
    }
    c6_index = {
        "precincts": [
            {"city": "WESTERVILLE", "city_school_district": ""}
        ]
    }
    (data_dir / "county5_precinct_index.json").write_text(json.dumps(c5_index))
    (data_dir / "county6_precinct_index.json").write_text(json.dumps(c6_index))
    
    logger = logging.getLogger("test")
    out = ohio_voter_pipeline._build_city_county_map(logger)
    
    # Assertions
    # FAKE_COLLISION should be excluded from `out` because it failed the guard
    assert "FAKE_COLLISION" not in out
    
    # SHARED_REAL should be merged
    assert "SHARED_REAL" in out
    assert sorted(out["SHARED_REAL"]) == ["county3", "county4"]
    
    # WESTERVILLE should be merged despite no shared identity because it is allowlisted
    assert "WESTERVILLE" in out
    assert sorted(out["WESTERVILLE"]) == ["county5", "county6"]
