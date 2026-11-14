"""add_voter_participation: the batched Boolean form equals the form it replaced.

_reference_add_voter_participation below is the function exactly as it stood at
commit 1b71d79543 (one Int32 expression per election column per count, all
handed to sum_horizontal at once). It is frozen here as the oracle. Do not
edit it; if the participation semantics are ever changed on purpose, delete
this file in that change and say so.

The comparison is assert_frame_equal on whole frames: values, dtypes, column
order. Frames are randomized from a fixed seed and built to hit the edges:
null registration dates, registration on an election date, ballots cast before
registration, blank, whitespace-only and null cells, minor-party codes, two
elections on one date, election columns out of chronological order, and more
columns than one batch holds.
"""

from __future__ import annotations

import logging
import random
from datetime import date, timedelta

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from pipeline import voter_data_cleaner as vdc
from pipeline.voter_data_cleaner import FREQ_HIGH, FREQ_LOW, build_col_meta  # noqa: F401  (used by the reference)

LOG = logging.getLogger("test_participation_equivalence")
LOG.addHandler(logging.NullHandler())


# ---- frozen reference (verbatim from 1b71d79543) -------------------------------

def _reference_add_voter_participation(
    df:            pl.DataFrame,
    election_cols: list[str],
    logger:        logging.Logger,
) -> pl.DataFrame:
    """
    Compute per-voter participation metrics across all 89 election columns.

    WHY sum_horizontal INSTEAD OF A PYTHON LOOP:
    The pandas v1 approach looped over each election column in Python, creating
    a series per column and accumulating.  With 89 columns × 7.9M rows that's
    ~700M boolean evaluations executed serially.

    Polars' sum_horizontal() evaluates all 89 expressions in a single columnar
    pass using SIMD instructions and thread-level parallelism.  Expect ~10–20×
    faster execution compared to the pandas loop on Ohio-scale data.

    Columns added (all-elections, backward-compatible names):
      Elections_Eligible — number of elections held after the voter's registration date
      Elections_Voted    — of those, number where the voter participated
      Turnout_Rate       — Elections_Voted / Elections_Eligible  (null if 0 eligible)

    Columns added (generals-only):
      General_Eligible      — general elections held after the voter's registration date
      General_Voted         — of those, generals where the voter participated
      General_Turnout_Rate  — General_Voted / General_Eligible  (null if 0 eligible)

    Derived:
      Voter_Frequency    — categorical label based on General_Turnout_Rate vs FREQ thresholds

    A voter is "eligible" for an election if their REGDATE_DT is on or before
    that election's date.  A voter "voted" if they were eligible AND their
    participation value is non-empty (X, R, or D).
    """
    logger.info('Building participation metric expressions for %d election columns ...',
                len(election_cols))

    # Build one Polars expression per election column for each of:
    #   elig  — 1 if voter was registered on/before election date, else 0
    #   voted — 1 if voter was eligible AND has a non-empty participation value
    # All expressions are Int32 so sum_horizontal produces an integer total.
    #
    # Two parallel sets are built: all-elections and generals-only.
    # Voter_Frequency is based on the generals-only rate so that the ≥75%
    # "Frequent" threshold is reachable (a voter attending every general
    # since 2000 scores ~100% instead of ~28% when diluted by 89 elections).
    elig_exprs:  list[pl.Expr] = []
    voted_exprs: list[pl.Expr] = []
    gen_elig_exprs:  list[pl.Expr] = []
    gen_voted_exprs: list[pl.Expr] = []

    # Single resolver: date/type/regular facts come ONLY from build_col_meta,
    # never from the column name here. The "generals" set below is the resolved
    # type_code == 'GENERAL', not a startswith test.
    col_meta = build_col_meta(df, election_cols, logger)

    for m in col_meta:
        # Polars stores Python date objects directly in comparisons with Date columns
        elig = (
            pl.col('REGDATE_DT').is_not_null() &
            (pl.col('REGDATE_DT') <= pl.lit(m.date))
        ).cast(pl.Int32)

        voted = (
            elig.cast(pl.Boolean) &                     # must be eligible
            pl.col(m.col).is_not_null() &               # participation value must exist
            (pl.col(m.col).str.strip_chars() != '')      # and be non-empty (X / R / D)
        ).cast(pl.Int32)

        elig_exprs.append(elig)
        voted_exprs.append(voted)

        if m.type_code == 'GENERAL':
            gen_elig_exprs.append(elig)
            gen_voted_exprs.append(voted)

    logger.info('  %d total election expressions, %d generals-only',
                len(elig_exprs), len(gen_elig_exprs))

    # sum_horizontal adds across all expressions row-by-row, fully vectorised.
    # This replaces the 89-iteration Python for-loop from v1.
    df = df.with_columns([
        pl.sum_horizontal(elig_exprs).alias('Elections_Eligible'),
        pl.sum_horizontal(voted_exprs).alias('Elections_Voted'),
    ])

    # Generals-only metrics
    if gen_elig_exprs:
        df = df.with_columns([
            pl.sum_horizontal(gen_elig_exprs).alias('General_Eligible'),
            pl.sum_horizontal(gen_voted_exprs).alias('General_Voted'),
        ])
    else:
        # Edge case: no GENERAL- columns in the data at all
        df = df.with_columns([
            pl.lit(0).cast(pl.Int32).alias('General_Eligible'),
            pl.lit(0).cast(pl.Int32).alias('General_Voted'),
        ])

    # Turnout rate: voted / eligible.  Divide-by-zero → null (not 0), which
    # correctly propagates to 'No Eligible Elections' in the frequency label.
    df = df.with_columns([
        (
            pl.col('Elections_Voted').cast(pl.Float64) /
            pl.when(pl.col('Elections_Eligible') == 0)
              .then(None)
              .otherwise(pl.col('Elections_Eligible'))
        ).round(4).alias('Turnout_Rate'),
        (
            pl.col('General_Voted').cast(pl.Float64) /
            pl.when(pl.col('General_Eligible') == 0)
              .then(None)
              .otherwise(pl.col('General_Eligible'))
        ).round(4).alias('General_Turnout_Rate'),
    ])

    # Frequency label: based on generals-only turnout rate so the ≥75%
    # threshold is meaningful.  A voter who participates in every general
    # election since registration now correctly scores as "Frequent".
    df = df.with_columns([
        pl.when(pl.col('General_Eligible') == 0)
          .then(pl.lit('No Eligible Elections'))
          .when(pl.col('General_Turnout_Rate') >= FREQ_HIGH)
          .then(pl.lit('Frequent (≥75%)'))
          .when(pl.col('General_Turnout_Rate') >= FREQ_LOW)
          .then(pl.lit('Moderate (25–74%)'))
          .otherwise(pl.lit('Infrequent (<25%)'))
          .alias('Voter_Frequency')
    ])

    eligible_count = df.filter(pl.col('Elections_Eligible') > 0).height
    gen_eligible   = df.filter(pl.col('General_Eligible') > 0).height
    logger.info('Participation complete: %s voters had ≥1 eligible election '
                '(%s with ≥1 eligible general)',
                f'{eligible_count:,}', f'{gen_eligible:,}')
    return df


# ---- synthetic frames ---------------------------------------------------------

ELECTIONS = [
    "PRIMARY-03/07/2000", "GENERAL-11/07/2000", "SPECIAL-02/06/2001", "PRIMARY-05/08/2001",
    "GENERAL-11/06/2001", "PRIMARY-05/07/2002", "GENERAL-11/05/2002", "SPECIAL-08/05/2003",
    "GENERAL-11/04/2003", "PRIMARY-03/02/2004", "GENERAL-11/02/2004", "PRIMARY-08/02/2022",
    "GENERAL-11/08/2022", "PRIMARY-05/02/2023", "SPECIAL-05/02/2023", "SPECIAL-08/08/2023",
    "GENERAL-11/07/2023", "GENERAL-11/04/2025", "PRIMARY-05/05/2026",
]
CELLS = ["", "", "", " ", "X", "R", "D", "L", " X ", None]


def _frame(seed: int, rows: int, columns: list[str], null_reg_rate: float = 0.1) -> pl.DataFrame:
    rng = random.Random(seed)
    election_dates = [vdc.parse_election_meta(c)[0] for c in columns]
    reg = []
    for _ in range(rows):
        roll = rng.random()
        if roll < null_reg_rate:
            reg.append(None)
        elif roll < null_reg_rate + 0.2 and election_dates:
            reg.append(rng.choice(election_dates))          # registered exactly on an election date
        else:
            reg.append(date(1995, 1, 1) + timedelta(days=rng.randrange(0, 11_700)))
    data = {"SOS_VOTERID": [f"OH{i:06d}" for i in range(rows)],
            "REGDATE_DT": pl.Series("REGDATE_DT", reg, dtype=pl.Date)}
    shuffled = columns[:]
    rng.shuffle(shuffled)                                   # frame order is not chronological
    for c in shuffled:
        data[c] = pl.Series(c, [rng.choice(CELLS) for _ in range(rows)], dtype=pl.String)
    return pl.DataFrame(data)


def _both(df: pl.DataFrame):
    cols = vdc.identify_election_cols(df)
    return _reference_add_voter_participation(df, cols, LOG), vdc.add_voter_participation(df, cols, LOG)


# ---- equivalence --------------------------------------------------------------

@pytest.mark.parametrize("seed", range(6))
def test_matches_reference_on_randomized_frames(seed):
    ref, new = _both(_frame(seed, rows=500, columns=ELECTIONS))
    assert_frame_equal(ref, new)


@pytest.mark.parametrize("batch", [1, 3, 8, 100])
def test_result_does_not_depend_on_batch_size(batch, monkeypatch):
    df = _frame(11, rows=300, columns=ELECTIONS)
    monkeypatch.setattr(vdc, "_PARTICIPATION_BATCH", batch)
    ref, new = _both(df)
    assert_frame_equal(ref, new)


def test_fixture_spans_more_than_one_batch():
    assert len(ELECTIONS) > 2 * vdc._PARTICIPATION_BATCH


def test_matches_reference_without_any_general_election():
    columns = [c for c in ELECTIONS if not c.startswith("GENERAL")]
    ref, new = _both(_frame(21, rows=200, columns=columns))
    assert_frame_equal(ref, new)
    assert new["General_Eligible"].sum() == 0
    assert set(new["Voter_Frequency"].unique().to_list()) == {"No Eligible Elections"}


def test_matches_reference_when_every_registration_date_is_null():
    ref, new = _both(_frame(31, rows=120, columns=ELECTIONS, null_reg_rate=1.0))
    assert_frame_equal(ref, new)
    assert new["Elections_Eligible"].sum() == 0 and new["Elections_Voted"].sum() == 0


def test_count_columns_keep_their_dtype_and_position():
    df = _frame(41, rows=50, columns=ELECTIONS)
    _, new = _both(df)
    added = new.columns[len(df.columns):]
    assert added == ["Elections_Eligible", "Elections_Voted", "General_Eligible", "General_Voted",
                     "Turnout_Rate", "General_Turnout_Rate", "Voter_Frequency"]
    for name in added[:4]:
        assert new.schema[name] == pl.Int32


# ---- hand-checked cases: the definition, independent of the reference ----------

def test_hand_checked_voter():
    columns = ["PRIMARY-03/07/2000", "GENERAL-11/07/2000", "GENERAL-11/06/2001", "SPECIAL-08/05/2003"]
    df = pl.DataFrame({
        "SOS_VOTERID": ["early", "boundary", "late", "unregistered"],
        "REGDATE_DT": pl.Series([date(1999, 1, 1), date(2000, 11, 7), date(2002, 1, 1), None], dtype=pl.Date),
        "PRIMARY-03/07/2000": ["R", "D", "X", "X"],     # 'boundary' and 'late' cast it before registering
        "GENERAL-11/07/2000": ["X", "X", "", "X"],
        "GENERAL-11/06/2001": ["", " ", None, "X"],     # blank, whitespace, null: none is a ballot
        "SPECIAL-08/05/2003": ["X", "L", "X", "X"],
    })
    out = vdc.add_voter_participation(df, columns, LOG)
    got = {r["SOS_VOTERID"]: (r["Elections_Eligible"], r["Elections_Voted"], r["General_Eligible"], r["General_Voted"])
           for r in out.iter_rows(named=True)}
    assert got == {
        "early":        (4, 3, 2, 1),
        "boundary":     (3, 2, 2, 1),   # eligible from its own registration day on
        "late":         (1, 1, 0, 0),
        "unregistered": (0, 0, 0, 0),
    }
