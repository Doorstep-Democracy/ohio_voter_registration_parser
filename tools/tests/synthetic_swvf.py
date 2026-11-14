"""Synthetic SWVF-shaped voter rows for end-to-end tests. No real voter data.

raw_frame() returns a small all-String frame with the 46 static SWVF columns
(names as the Ohio Secretary of State emits them) plus a set of election
columns, shaped so the whole export path can run on it: clean_voter_data, the
cohort classifier, add_voter_participation, export_json with precinct charts
and export_unc_shadow_json.

It is deterministic for a given seed and uses only `random.Random`, so two
processes (or two versions of the pipeline) given the same seed build
byte-identical input. Names, addresses and ids are generated placeholders.
"""

from __future__ import annotations

import random
from datetime import date, timedelta

import polars as pl

STATIC_COLUMNS = [
    "SOS_VOTERID", "COUNTY_ID", "LAST_NAME", "FIRST_NAME", "MIDDLE_NAME", "SUFFIX", "DATE_OF_BIRTH",
    "REGISTRATION_DATE", "VOTER_STATUS", "PARTY_AFFILIATION", "RESIDENTIAL_ADDRESS1",
    "RESIDENTIAL_SECONDARY_ADDR", "RESIDENTIAL_CITY", "RESIDENTIAL_STATE", "RESIDENTIAL_ZIP",
    "RESIDENTIAL_ZIP_PLUS4", "RESIDENTIAL_COUNTRY", "RESIDENTIAL_POSTALCODE", "MAILING_ADDRESS1",
    "MAILING_SECONDARY_ADDRESS", "MAILING_CITY", "MAILING_STATE", "MAILING_ZIP", "MAILING_ZIP_PLUS4",
    "MAILING_COUNTRY", "MAILING_POSTAL_CODE", "CAREER_CENTER", "CITY", "CITY_SCHOOL_DISTRICT",
    "COUNTY_COURT_DISTRICT", "CONGRESSIONAL_DISTRICT", "COURT_OF_APPEALS", "EDU_SERVICE_CENTER_DISTRICT",
    "EXEMPTED_VILL_SCHOOL_DISTRICT", "LIBRARY", "LOCAL_SCHOOL_DISTRICT", "MUNICIPAL_COURT_DISTRICT",
    "PRECINCT_NAME", "PRECINCT_CODE", "STATE_BOARD_OF_EDUCATION", "STATE_REPRESENTATIVE_DISTRICT",
    "STATE_SENATE_DISTRICT", "TOWNSHIP", "VILLAGE", "WARD", "COUNTY_NUMBER",
]

ELECTION_COLUMNS = [
    "PRIMARY-03/07/2000", "GENERAL-11/07/2000", "SPECIAL-02/06/2001", "PRIMARY-05/08/2001",
    "GENERAL-11/06/2001", "PRIMARY-05/07/2002", "GENERAL-11/05/2002", "SPECIAL-08/05/2003",
    "GENERAL-11/04/2003", "PRIMARY-03/02/2004", "GENERAL-11/02/2004", "PRIMARY-05/03/2022",
    "PRIMARY-08/02/2022", "GENERAL-11/08/2022", "PRIMARY-05/02/2023", "SPECIAL-08/08/2023",
    "GENERAL-11/07/2023", "PRIMARY-03/19/2024", "GENERAL-11/05/2024", "PRIMARY-05/06/2025",
    "GENERAL-11/04/2025", "PRIMARY-05/05/2026",
]

# (county number, precinct, jurisdiction fields). Two counties so county-scoped
# logic has something to separate; a city with wards, a township and a village.
PRECINCTS = [
    ("01", "ALPHA 1-A", {"CITY": "ALPHA CITY", "WARD": "ALPHA WARD 1", "RESIDENTIAL_CITY": "ALPHA",
                         "CITY_SCHOOL_DISTRICT": "ALPHA CITY SD", "MUNICIPAL_COURT_DISTRICT": "ALPHA"}),
    ("01", "ALPHA 2-A", {"CITY": "ALPHA CITY", "WARD": "ALPHA WARD 2", "RESIDENTIAL_CITY": "ALPHA",
                         "CITY_SCHOOL_DISTRICT": "ALPHA CITY SD", "MUNICIPAL_COURT_DISTRICT": "ALPHA"}),
    ("01", "BETA TWP", {"TOWNSHIP": "BETA TOWNSHIP", "RESIDENTIAL_CITY": "ALPHA",
                        "LOCAL_SCHOOL_DISTRICT": "BETA LOCAL SD (ADAMS)", "COUNTY_COURT_DISTRICT": "ADAMS"}),
    ("01", "GAMMA", {"VILLAGE": "GAMMA VILLAGE", "RESIDENTIAL_CITY": "GAMMA",
                     "EXEMPTED_VILL_SCHOOL_DISTRICT": "GAMMA EVSD"}),
    ("25", "DELTA 1-A", {"CITY": "DELTA CITY", "WARD": "DELTA WARD 1", "RESIDENTIAL_CITY": "DELTA",
                         "CITY_SCHOOL_DISTRICT": "DELTA CITY SD", "MUNICIPAL_COURT_DISTRICT": "DELTA"}),
    ("25", "BETA TWP", {"TOWNSHIP": "BETA TOWNSHIP", "RESIDENTIAL_CITY": "DELTA",
                        "LOCAL_SCHOOL_DISTRICT": "BETA LOCAL SD (FRANKLIN)"}),
]

_DISTRICTS = {
    "01": {"CONGRESSIONAL_DISTRICT": "02", "STATE_SENATE_DISTRICT": "14",
           "STATE_REPRESENTATIVE_DISTRICT": "90", "COURT_OF_APPEALS": "04", "STATE_BOARD_OF_EDUCATION": "10"},
    "25": {"CONGRESSIONAL_DISTRICT": "03", "STATE_SENATE_DISTRICT": "15",
           "STATE_REPRESENTATIVE_DISTRICT": "01", "COURT_OF_APPEALS": "10", "STATE_BOARD_OF_EDUCATION": "06"},
}

# Voter archetypes: (PARTY_AFFILIATION, primary-ballot weights, turnout). Chosen
# so every cohort family is reachable, including minor-party and non-partisan.
_ARCHETYPES = [
    ("R", {"R": 0.8, "": 0.2}, 0.8),
    ("D", {"D": 0.8, "": 0.2}, 0.8),
    ("", {"R": 0.3, "": 0.7}, 0.5),
    ("", {"D": 0.3, "": 0.7}, 0.5),
    ("", {"R": 0.25, "D": 0.25, "": 0.5}, 0.6),
    ("", {"X": 0.4, "": 0.6}, 0.4),
    ("", {"": 1.0}, 0.2),
    ("L", {"L": 0.5, "": 0.5}, 0.5),
    ("R", {"R": 0.4, "D": 0.2, "": 0.4}, 0.7),
    ("D", {"D": 0.4, "R": 0.2, "": 0.4}, 0.7),
]


def _election_date(name: str) -> date:
    month, day, year = name.split("-", 1)[1].split("/")
    return date(int(year), int(month), int(day))


def _pick(rng: random.Random, weights: dict) -> str:
    roll, acc = rng.random(), 0.0
    for value, weight in weights.items():
        acc += weight
        if roll < acc:
            return value
    return next(reversed(weights))


def raw_frame(seed: int = 7, voters_per_precinct: int = 60,
              election_columns: list[str] | None = None) -> pl.DataFrame:
    """All-String synthetic raw frame, static columns then election columns."""
    rng = random.Random(seed)
    elections = list(election_columns or ELECTION_COLUMNS)
    rows = []
    serial = 0
    for county, precinct, places in PRECINCTS:
        for _ in range(voters_per_precinct):
            serial += 1
            party, ballot_weights, turnout = _ARCHETYPES[serial % len(_ARCHETYPES)]
            born = date(1930, 1, 1) + timedelta(days=rng.randrange(0, 28_000))
            registered = max(born + timedelta(days=18 * 366),
                             date(1996, 1, 1) + timedelta(days=rng.randrange(0, 11_000)))
            row = {c: "" for c in STATIC_COLUMNS}
            row.update({
                "SOS_VOTERID": f"OH{serial:010d}", "COUNTY_ID": f"{county}{serial:07d}",
                "LAST_NAME": f"LAST{serial}", "FIRST_NAME": f"FIRST{serial}",
                "DATE_OF_BIRTH": born.isoformat(),
                # A few registration dates are blank: those voters have no eligible elections.
                "REGISTRATION_DATE": "" if serial % 37 == 0 else registered.isoformat(),
                "VOTER_STATUS": "CONFIRMATION" if serial % 9 == 0 else "ACTIVE",
                "PARTY_AFFILIATION": party,
                "RESIDENTIAL_ADDRESS1": f"{serial} MAIN ST", "RESIDENTIAL_STATE": "OH",
                "RESIDENTIAL_ZIP": f"45{serial % 1000:03d}",
                "PRECINCT_NAME": precinct, "PRECINCT_CODE": f"{county}-{precinct[:3]}",
                "COUNTY_NUMBER": county,
            })
            row.update(places)
            row.update(_DISTRICTS[county])
            for name in elections:
                voted = rng.random() < turnout and _election_date(name) >= registered
                if not voted:
                    row[name] = ""
                elif name.startswith("PRIMARY"):
                    row[name] = _pick(rng, ballot_weights) or "X"
                else:
                    row[name] = "X"
            rows.append(row)
    return pl.DataFrame(rows, schema={c: pl.String for c in STATIC_COLUMNS + elections})
