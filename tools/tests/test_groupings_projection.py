"""jurisdictional_groupings loads only GROUPING_COLUMNS from the enriched cache.

Three things are pinned here, all on synthetic frames:

  1. The projection is exactly the columns the grouping code reads.
  2. No grouping or resolver function names an enriched column outside it. The
     columns are read behind `if col in df.columns` guards, so a missing one
     would drop a chart or change a place resolution silently; this test is
     what makes "add a column read" and "add it to GROUPING_COLUMNS" one change.
  3. Output is identical whether the functions get the full frame or the
     projected one, i.e. the 140-odd unloaded columns never influenced output.
"""

from __future__ import annotations

import ast
import inspect
import logging
import textwrap

import polars as pl

import pipeline.jurisdictional_groupings as jg
from pipeline import voter_data_cleaner as vdc

LOG = logging.getLogger("test_groupings_projection")
LOG.addHandler(logging.NullHandler())

# The enriched cache's non-election columns (names only; read from the parquet
# footer on 2026-11-12). Used to recognise a column name inside function source.
ENRICHED_NON_ELECTION_COLUMNS = frozenset("""
SOS_VOTERID COUNTY_ID LAST_NAME FIRST_NAME MIDDLE_NAME SUFFIX DATE_OF_BIRTH REGISTRATION_DATE
VOTER_STATUS PARTY_AFFILIATION RESIDENTIAL_ADDRESS1 RESIDENTIAL_SECONDARY_ADDR RESIDENTIAL_CITY
RESIDENTIAL_STATE RESIDENTIAL_ZIP RESIDENTIAL_ZIP_PLUS4 RESIDENTIAL_COUNTRY RESIDENTIAL_POSTALCODE
MAILING_ADDRESS1 MAILING_SECONDARY_ADDRESS MAILING_CITY MAILING_STATE MAILING_ZIP MAILING_ZIP_PLUS4
MAILING_COUNTRY MAILING_POSTAL_CODE CAREER_CENTER CITY CITY_SCHOOL_DISTRICT COUNTY_COURT_DISTRICT
CONGRESSIONAL_DISTRICT COURT_OF_APPEALS EDU_SERVICE_CENTER_DISTRICT EXEMPTED_VILL_SCHOOL_DISTRICT
LIBRARY LOCAL_SCHOOL_DISTRICT MUNICIPAL_COURT_DISTRICT PRECINCT_NAME PRECINCT_CODE
STATE_BOARD_OF_EDUCATION STATE_REPRESENTATIVE_DISTRICT STATE_SENATE_DISTRICT TOWNSHIP VILLAGE WARD
COUNTY_NUMBER BIRTHYEAR Decade Generation REGDATE_DT PARTY_LABEL d_primaries r_primaries x_primaries
m_primaries total_primaries partisan_primaries lean_score confidence recent_5yr_lean last_three_party
years_since_last_partisan switch_count ever_crossed partisan_ballot_sequence regular_primaries_eligible
primary_participation_rate primaries_skipped cohort cohort_family crossover_class is_new_registrant
""".split())

EXPECTED_GROUPING_COLUMNS = {
    "COUNTY_NUMBER", "PRECINCT_NAME", "WARD", "RESIDENTIAL_CITY",
    "cohort_family", "Decade", "Generation",
    "CITY", "TOWNSHIP", "VILLAGE", "LOCAL_SCHOOL_DISTRICT", "CITY_SCHOOL_DISTRICT",
    "EXEMPTED_VILL_SCHOOL_DISTRICT", "STATE_SENATE_DISTRICT", "STATE_REPRESENTATIVE_DISTRICT",
    "CONGRESSIONAL_DISTRICT", "COUNTY_COURT_DISTRICT", "MUNICIPAL_COURT_DISTRICT", "COURT_OF_APPEALS",
}

# Every function the groupings step runs a voter frame through.
FRAME_READERS = (
    jg.main, jg.aggregate_jurisdiction, jg._add_resolved_city_column, jg.build_ward_entities,
    vdc._place_per_precinct, vdc._ward_map_per_county, vdc._dominant_per_precinct,
    vdc._dominant_city_per_precinct,
)


# ---- synthetic data -----------------------------------------------------------

def _row(i, county, precinct, **kw):
    families = [fam for fam, _, _ in jg.COHORT_SLICES]
    base = {
        "COUNTY_NUMBER": county, "PRECINCT_NAME": precinct,
        "CITY": "", "VILLAGE": "", "WARD": "", "TOWNSHIP": "", "RESIDENTIAL_CITY": "",
        "LOCAL_SCHOOL_DISTRICT": "", "CITY_SCHOOL_DISTRICT": "", "EXEMPTED_VILL_SCHOOL_DISTRICT": "",
        "STATE_SENATE_DISTRICT": "", "STATE_REPRESENTATIVE_DISTRICT": "", "CONGRESSIONAL_DISTRICT": "",
        "COUNTY_COURT_DISTRICT": "", "MUNICIPAL_COURT_DISTRICT": "", "COURT_OF_APPEALS": "",
        "cohort_family": families[i % len(families)],
        "Decade": 1940 + 10 * (i % 7),
        "Generation": jg._GEN_ORDER[i % 5],
        # Columns the groupings step never reads. Present in the full frame only.
        "SOS_VOTERID": f"OH{i:07d}", "LAST_NAME": f"LAST{i}", "FIRST_NAME": f"FIRST{i}",
        "RESIDENTIAL_ADDRESS1": f"{i} MAIN ST", "PARTY_AFFILIATION": "RD"[i % 2],
        "PRIMARY-03/07/2000": ["R", "D", "", "X"][i % 4],
        "GENERAL-11/07/2000": ["X", ""][i % 2],
        "PRIMARY-05/05/2026": ["", "D", "R"][i % 3],
        "lean_score": (i % 11) / 10.0,
    }
    base.update(kw)
    return base


def _full_frame() -> pl.DataFrame:
    rows, i = [], 0
    # County 01 populates CITY: a city with two wards, a township, a village.
    for ward in ("ALPHA WARD 1", "ALPHA  WARD 2"):          # irregular spacing on purpose
        for _ in range(9):
            rows.append(_row(i, "01", "ALPHA 1-A" if "1" in ward else "ALPHA 2-A",
                             CITY="ALPHA CITY", WARD=ward, RESIDENTIAL_CITY="ALPHA",
                             CITY_SCHOOL_DISTRICT="ALPHA CITY SD", STATE_SENATE_DISTRICT="14",
                             STATE_REPRESENTATIVE_DISTRICT="90", CONGRESSIONAL_DISTRICT="02",
                             MUNICIPAL_COURT_DISTRICT="ALPHA", COURT_OF_APPEALS="04"))
            i += 1
    for _ in range(7):
        rows.append(_row(i, "01", "BETA TWP", TOWNSHIP="BETA TOWNSHIP", RESIDENTIAL_CITY="ALPHA",
                         LOCAL_SCHOOL_DISTRICT="BETA LOCAL SD (ADAMS)", STATE_SENATE_DISTRICT="14",
                         STATE_REPRESENTATIVE_DISTRICT="90", CONGRESSIONAL_DISTRICT="02",
                         COUNTY_COURT_DISTRICT="ADAMS", COURT_OF_APPEALS="04"))
        i += 1
    for _ in range(5):
        rows.append(_row(i, "01", "GAMMA", VILLAGE="GAMMA VILLAGE", RESIDENTIAL_CITY="GAMMA",
                         EXEMPTED_VILL_SCHOOL_DISTRICT="GAMMA EVSD", STATE_SENATE_DISTRICT="14",
                         STATE_REPRESENTATIVE_DISTRICT="91", CONGRESSIONAL_DISTRICT="02",
                         COURT_OF_APPEALS="04"))
        i += 1
    # County 18 populates CITY for zero rows: municipality is encoded in WARD.
    for _ in range(11):
        rows.append(_row(i, "18", "CLEVELAND 07-A", WARD="CLEVELAND WARD 7", RESIDENTIAL_CITY="CLEVELAND",
                         CITY_SCHOOL_DISTRICT="CLEVELAND MUNICIPAL SD", STATE_SENATE_DISTRICT="21",
                         STATE_REPRESENTATIVE_DISTRICT="10", CONGRESSIONAL_DISTRICT="11",
                         MUNICIPAL_COURT_DISTRICT="CLEVELAND", COURT_OF_APPEALS="08"))
        i += 1
    # A township with the same name in the second county: county-scoped types must not merge.
    for _ in range(6):
        rows.append(_row(i, "18", "BETA TWP", TOWNSHIP="BETA TOWNSHIP", RESIDENTIAL_CITY="CLEVELAND",
                         STATE_SENATE_DISTRICT="21", STATE_REPRESENTATIVE_DISTRICT="10",
                         CONGRESSIONAL_DISTRICT="11", COURT_OF_APPEALS="08"))
        i += 1
    return pl.DataFrame(rows)


def _projected(full: pl.DataFrame) -> pl.DataFrame:
    return full.select([c for c in full.columns if c in jg.GROUPING_COLUMNS])


def _read_tree(root) -> dict:
    return {p.relative_to(root).as_posix(): p.read_bytes() for p in sorted(root.rglob("*.json"))}


# ---- 1. the projection itself -------------------------------------------------

def test_grouping_columns_is_exactly_the_expected_set():
    assert set(jg.GROUPING_COLUMNS) == EXPECTED_GROUPING_COLUMNS
    assert len(jg.GROUPING_COLUMNS) == len(set(jg.GROUPING_COLUMNS))


def test_every_configured_jurisdiction_column_is_loaded():
    missing = {cfg["column"] for cfg in jg.JURISDICTIONS.values()} - set(jg.GROUPING_COLUMNS)
    assert not missing


# ---- 2. no function reads outside it ------------------------------------------

def test_no_frame_reader_names_an_enriched_column_outside_the_projection():
    offenders = {}
    for func in FRAME_READERS:
        tree = ast.parse(textwrap.dedent(inspect.getsource(func)))
        body = tree.body[0]
        doc = ast.get_docstring(body)
        named = {
            node.value for node in ast.walk(body)
            if isinstance(node, ast.Constant) and isinstance(node.value, str) and node.value != doc
        }
        outside = (named & ENRICHED_NON_ELECTION_COLUMNS) - set(jg.GROUPING_COLUMNS)
        if outside:
            offenders[func.__qualname__] = sorted(outside)
    assert not offenders, (
        "these functions name enriched columns that jurisdictional_groupings does not load; "
        f"add them to GROUPING_COLUMNS: {offenders}"
    )


# ---- 3. projected output equals full-frame output -----------------------------

def test_fixture_exercises_columns_outside_the_projection():
    full = _full_frame()
    assert set(full.columns) - set(jg.GROUPING_COLUMNS), "fixture must carry unloaded columns"
    assert set(jg.GROUPING_COLUMNS) <= set(full.columns)


def test_aggregate_jurisdiction_is_unchanged_by_projection():
    full, proj = _full_frame(), _projected(_full_frame())
    election_cols = [c for c in full.columns if c.split("-")[0] in ("PRIMARY", "GENERAL", "SPECIAL")]
    compared = 0
    for cfg in jg.JURISDICTIONS.values():
        column = cfg["column"]
        for value in sorted(v for v in full[column].unique().to_list() if v):
            scoped = "Adams" if cfg.get("county_scoped") else None
            a = jg.aggregate_jurisdiction(full, column, value, cfg["display"], election_cols, "2026-11-12", LOG, scoped)
            b = jg.aggregate_jurisdiction(proj, column, value, cfg["display"], [], "2026-11-12", LOG, scoped)
            assert a and a == b, (column, value)
            compared += 1
    assert compared >= 12


def test_resolved_city_is_unchanged_by_projection():
    full, proj = _full_frame(), _projected(_full_frame())
    a = jg._add_resolved_city_column(full, LOG)["_resolved_city"].to_list()
    b = jg._add_resolved_city_column(proj, LOG)["_resolved_city"].to_list()
    assert a == b
    # The zero-CITY county resolved through WARD, the populated one kept CITY verbatim.
    assert "CLEVELAND CITY" in a and "ALPHA CITY" in a


def test_ward_entities_are_unchanged_by_projection(tmp_path, monkeypatch):
    full, proj = _full_frame(), _projected(_full_frame())
    trees = []
    for name, frame, election_cols in (("full", full, ["PRIMARY-03/07/2000"]), ("proj", proj, [])):
        out = tmp_path / name
        out.mkdir()
        monkeypatch.setattr(jg, "DATA_DIR", out)
        jg.build_ward_entities(frame, election_cols, "2026-11-12", LOG)
        trees.append(_read_tree(out))
    assert trees[0], "fixture produced no ward files"
    assert trees[0] == trees[1]
    # Three real wards (two in ALPHA, one in CLEVELAND), six charts each, plus the index.
    assert len(trees[0]) == 3 * 6 + 1


# ---- main() asks the loader for the projection and nothing else ---------------

def test_main_loads_only_grouping_columns_through_the_shared_loader(tmp_path, monkeypatch):
    # jg.main imports the cleaner by its bare module name, which is a different
    # module object from pipeline.voter_data_cleaner. Patch the one main() uses.
    import voter_data_cleaner as bare_vdc

    calls = []

    def _loader(logger, *, txt_files=None, use_parquet=True, columns=None):
        calls.append(columns)
        return _projected(_full_frame())

    cache = tmp_path / "enriched_voters.parquet"
    cache.write_bytes(b"")
    out = tmp_path / "out"
    out.mkdir()
    monkeypatch.setattr(bare_vdc, "load_enriched_voters", _loader)
    monkeypatch.setattr(jg, "ENRICHED_CACHE", cache)
    monkeypatch.setattr(jg, "DATA_DIR", out)

    assert jg.main(jurisdictions_to_process=["cities", "townships"], output_format="json", logger=LOG) is True

    assert calls == [list(jg.GROUPING_COLUMNS)]
    tree = _read_tree(out)
    assert "city/alpha_city_party_affiliation.json" in tree
    assert "city/cleveland_city_party_affiliation.json" in tree
    # County-scoped: the two BETA TOWNSHIPs stay separate entities.
    assert "township/adams_beta_township_party_affiliation.json" in tree
    assert "township/cuyahoga_beta_township_party_affiliation.json" in tree
