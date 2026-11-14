"""Hand-built voter histories for the Axis A tests (DOO-7). No real voter data.

Eleven voters in two counties, each chosen to land on one edge of the segment
definitions, run through the real cohort classifier so the tests check the
predicates against what the pipeline produces, not against assumed columns.

Election columns are deliberately out of date order: the even-November anchor
must not depend on column order.
"""

from __future__ import annotations

from datetime import date

import polars as pl

from pipeline.voter_data_cleaner import (
    classify_all_voters_primary_history,
    identify_election_cols,
    identify_primary_cols,
)
from tools.tests.synthetic_swvf import STATIC_COLUMNS

SHUFFLED_ELECTION_COLUMNS = [
    "GENERAL-11/04/2025", "PRIMARY-05/03/2016", "GENERAL-11/05/2024", "PRIMARY-05/08/2018",
    "PRIMARY-05/03/2022", "GENERAL-11/08/2022", "PRIMARY-03/19/2024", "PRIMARY-05/06/2025",
    "PRIMARY-05/05/2026", "GENERAL-11/03/2020", "PRIMARY-03/17/2020",
]
_PRIMARIES = ["PRIMARY-05/03/2016", "PRIMARY-05/08/2018", "PRIMARY-03/17/2020", "PRIMARY-05/03/2022",
              "PRIMARY-03/19/2024", "PRIMARY-05/06/2025", "PRIMARY-05/05/2026"]
REFERENCE_DATE = date(2026, 10, 24)
PROVENANCE = {"provenance_snapshot_date": "2026-10-24", "affiliation_window_years": "[2024, 2025, 2026]"}

# id: (county, party, primary ballots oldest-first, general columns voted,
#      expected non_locked_crossers, expected fresh_defector)
VOTERS = {
    "V01": ("57", "R", "DD....R", ["GENERAL-11/05/2024"], True, True),    # fresh D->R
    "V02": ("57", "R", "DDR....", ["GENERAL-11/05/2024"], True, False),   # crossed 2020: not recent
    "V03": ("57", "D", "DRRD..D", [], True, False),                       # latest pair DD
    "V04": ("57", "", "DRDR...", ["GENERAL-11/05/2024"], True, False),    # UNC_MIXED
    "V05": ("57", "L", "DRDR..R", ["GENERAL-11/05/2024"], False, False),  # MINOR_PARTY: no class
    "V06": ("57", "R", "GR....D", ["GENERAL-11/05/2024"], True, True),    # G stays out of the sequence
    "V07": ("57", "D", "R.....D", ["GENERAL-11/04/2025"], True, True),    # odd-year general only
    "V08": ("57", "R", "RRRRR.R", ["GENERAL-11/05/2024"], False, False),  # never crossed
    "V09": ("09", "R", "D.....R", ["GENERAL-11/05/2024"], True, True),    # second county
    "V10": ("09", "D", ".......", [], False, False),                      # zero floor
    "V11": ("09", "", "XXXXXDX", [], False, False),                       # UNC_LAPSED_D
}
EXPORTED_57 = ["V01", "V02", "V03", "V04", "V06", "V07"]


def raw_frame() -> pl.DataFrame:
    rows = []
    for vid, (county, party, ballots, generals, _, _) in VOTERS.items():
        row = {c: "" for c in STATIC_COLUMNS + SHUFFLED_ELECTION_COLUMNS}
        row.update({"SOS_VOTERID": vid, "COUNTY_ID": vid, "COUNTY_NUMBER": county,
                    "PARTY_AFFILIATION": party, "REGISTRATION_DATE": "2010-01-01",
                    "VOTER_STATUS": "ACTIVE", "LAST_NAME": "TEST", "FIRST_NAME": vid})
        for col, code in zip(_PRIMARIES, ballots):
            row[col] = "" if code == "." else code
        for col in generals:
            row[col] = "X"
        rows.append(row)
    return pl.DataFrame(rows, schema={c: pl.String for c in STATIC_COLUMNS + SHUFFLED_ELECTION_COLUMNS})


def enriched_frame() -> pl.DataFrame:
    """raw_frame() with the classifier's columns joined on, as the cache holds them."""
    import logging

    raw = raw_frame()
    primaries = identify_primary_cols(identify_election_cols(raw))
    classified = classify_all_voters_primary_history(
        raw, primaries, logging.getLogger("axis_a_fixture"), reference_date=REFERENCE_DATE)
    return raw.join(classified, on="SOS_VOTERID", how="left")


def write_enriched(path, *, metadata: dict | None = PROVENANCE) -> None:
    enriched_frame().write_parquet(path, metadata=metadata)
