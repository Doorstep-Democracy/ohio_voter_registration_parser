"""Synthetic voter frame for the compact election-code tests (DOO-184, M6).

Election cells are String, as the raw parquet holds them. The frame covers every
code in the 88-county profile (DATA_QUALITY.md ELECTION-CELL-CODE-SET), blanks,
padded values such as ' D', a ballot cast before the registration date, an
unaffiliated voter whose only ballot is minor-party, and an L-affiliated voter.
A seeded random block adds volume so cross-column logic (crossings, last three
ballots, decay weights) is exercised on many combinations.

No local/ data is read; the frame is built from the seed alone.
"""
from __future__ import annotations

import random
from datetime import date

import polars as pl

ELECTION_COLS = [
    "PRIMARY-03/15/2016", "GENERAL-11/08/2016",
    "PRIMARY-05/08/2018", "GENERAL-11/06/2018",
    "PRIMARY-09/10/2019",
    "PRIMARY-03/17/2020", "GENERAL-11/03/2020",
    "PRIMARY-05/03/2022", "GENERAL-11/08/2022",
    "SPECIAL-08/08/2023",
    "PRIMARY-03/19/2024", "GENERAL-11/05/2024",
]
PRIMARY_COLS = [c for c in ELECTION_COLS if c.startswith("PRIMARY-")]
REFERENCE_DATE = date(2026, 10, 24)

_CODES = ["", "X", "D", "R", "L", "G", "C", "S", "N", "E", " D", "R ", " X", " L "]
_WEIGHTS = [55, 15, 10, 10, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]
_AFFILIATIONS = ["", "D", "R", "L", "N"]

# (voter id, PARTY_AFFILIATION, REGISTRATION_DATE, {column: code}); unnamed columns are ''.
EDGE_CASES = [
    ("E_BLANK_ALL", "", "2010-01-01", {}),
    ("E_PAD_D", "", "2010-01-01", {"PRIMARY-05/03/2022": " D"}),
    ("E_PRE_REG", "D", "2023-01-01", {"PRIMARY-03/15/2016": "D", "GENERAL-11/08/2016": "X"}),
    ("E_MINOR_ONLY", "", "2010-01-01", {"PRIMARY-03/17/2020": "G"}),
    ("E_L_AFF", "L", "2010-01-01", {"PRIMARY-03/19/2024": "L"}),
    ("E_SENTINEL", "", "1900-01-01", {"GENERAL-11/03/2020": "X"}),
    ("E_CROSS", "R", "2000-01-01", {"PRIMARY-03/15/2016": "D", "PRIMARY-03/19/2024": "R"}),
] + [
    (f"E_CODE_{code}", "", "2010-01-01", {"PRIMARY-05/08/2018": code})
    for code in ["D", "R", "X", "L", "G", "C", "S", "N", "E"]
]


def string_frame(n_random: int = 3000, seed: int = 184) -> pl.DataFrame:
    """The fixture with String election cells, edge cases first."""
    rng = random.Random(seed)
    rows: list[dict] = []
    for vid, aff, reg, cells in EDGE_CASES:
        rows.append({"SOS_VOTERID": vid, "PARTY_AFFILIATION": aff, "REGISTRATION_DATE": reg,
                     **{c: cells.get(c, "") for c in ELECTION_COLS}})
    for i in range(n_random):
        reg = ("1900-01-01" if rng.random() < 0.005
               else f"{rng.randint(1990, 2025)}-{rng.randint(1, 12):02d}-{rng.randint(1, 28):02d}")
        rows.append({
            "SOS_VOTERID": f"R{i:05d}",
            "PARTY_AFFILIATION": rng.choice(_AFFILIATIONS),
            "REGISTRATION_DATE": reg,
            **{c: rng.choices(_CODES, _WEIGHTS)[0] for c in ELECTION_COLS},
        })
    df = pl.DataFrame(rows, schema={"SOS_VOTERID": pl.String, "PARTY_AFFILIATION": pl.String,
                                    "REGISTRATION_DATE": pl.String,
                                    **{c: pl.String for c in ELECTION_COLS}})
    return df.with_columns(
        pl.col("REGISTRATION_DATE").str.to_date("%Y-%m-%d", strict=False).alias("REGDATE_DT"),
        pl.lit("57").alias("COUNTY_NUMBER"),
    )
