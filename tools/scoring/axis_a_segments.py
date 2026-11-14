"""
axis_a_segments.py
==================

Single-snapshot ("Axis A") product segments for the subscriber tier.

WHAT THIS IS
------------
One named Polars expression per sellable segment, and nothing else. No CLI, no
I/O, no aggregation -- callers scan the enriched parquet themselves and apply
these predicates. This module is the single resolver for "who is a fresh
defector" and its four siblings: per CLAUDE.md section 5, a segment defined
inline at a call site is a divergent copy, and divergent copies are how the
postal-city bug survived weeks undetected.

Every segment traces to a numbered item in the Axis A subscriber-tier stack rank
(the ranking authority, held in the operator's working notes -- not in this
repo). Item numbers are cited per function so the mapping survives without it.
Nothing here DEPENDS on that document: every statutory claim below carries its
own R.C. / O.A.C. / EOM citation, and every data-quality claim names its ledger
entry in the repo-tracked DATA_QUALITY.md.

WHAT THESE FIELDS MAY NEVER CLAIM
---------------------------------
Every predicate here describes COUNTED BALLOTS, never engagement. This is a
statutory boundary, not a stylistic caution.

Ohio tracks a per-voter "Last Activity Type" with exactly seven codes -- VOT
(voting), ABR (absentee request), REG (registering), UPD (updating registration,
including updates with no changes), BMV (BMV interaction), PET (petition
signature), CON (confirmation-notice response) -- tie-broken in that order when
several fall on one day (O.A.C. 111:3-11-01(B)(1)-(9); R.C. 3503.15(C)(11); Ohio
Election Official Manual Ch. 4 section 4.04, pp. 4-96/4-98).

Only VOT is derivable from the SWVF election-history columns. Last Activity Type
itself is not in the statewide file -- it exists only in county files. A voter
with a completely empty ballot history can therefore be fully active in the
state's eyes because they renewed a licence at the BMV or signed a verified
petition.

Consequence for every function below: "has never cast a partisan primary ballot"
is a statement about ballots. It is never a statement about civic engagement,
persuadability-in-principle, or whether a voter is reachable. A segment that
claims otherwise is making a claim this data cannot support.

THE COUNTED-BALLOT RULE
-----------------------
A blank election cell means "no COUNTED ballot," not "did not attempt to vote."
Under the DATA Act (per EOM Ch. 4, "Voter History," pp. 4-149/4-150) voting
history is assigned only where the voter signed the pollbook, timely returned a
counted absentee (incl. UOCAVA/FWAB), voted early in person and the ballot was
counted, or cast a COUNTED provisional. A rejected ballot leaves a blank
indistinguishable from a non-vote.

Every count produced here is therefore a FLOOR on the true behaviour. The bias
has one direction only -- it shrinks the observed universe, never inflates it --
which is why the value claims survive it: a segment measured at N contains at
least N qualifying voters. Note also that a counted BLANK ballot does receive
history, so a GENERAL mark is not proof that any particular race was voted.

MEASURED FIGURES LIVE ELSEWHERE, DELIBERATELY
---------------------------------------------
Segment sizes, county shares, and reset rates are NOT recorded in these
docstrings. They belong to a snapshot, not to a definition, and this project has
already been bitten: the 8A prose quoted 152,060 for a cohort that read 155,254
after a regen landed mid-analysis. Current figures live in the stack-rank
deliverable and in DATA_QUALITY.md. The one exception is the Phase 1 gate anchor
in ``non_locked_crossers``, which is a reproducibility check, not a product
claim -- and it is stamped with the snapshot it was measured against.

FIELD PROVENANCE
----------------
All fields consumed here are produced by ``classify_all_voters_primary_history``
in ``pipeline/voter_data_cleaner.py`` -- the single resolver -- and survive a
pipeline rerun. None originates in a patch script.
"""

from __future__ import annotations

import polars as pl

# ---------------------------------------------------------------------------
# Shared vocabulary
# ---------------------------------------------------------------------------

#: ``crossover_class`` values that represent a crosser who is NOT locked back
#: into one party. Assigned in ``voter_data_cleaner.py`` (the crossover_class
#: block of ``classify_all_voters_primary_history``) with two
#: threshold regimes: UNC_MIXED uses LOCKED at |lean| >= 0.40 and LEAN at
#: |lean| >= 0.20; affiliated crossovers (CROSSOVER_R/CROSSOVER_D) use the
#: tighter 0.50 / 0.30. Both regimes emit this same label vocabulary.
NON_LOCKED_CLASSES: tuple[str, ...] = ("LEAN_R", "LEAN_D", "TRUE_MIXED")

#: Tenure floor for ``consistent_low_frequency``. Part of the definition, not a
#: caller's option -- see that function's docstring.
CONSISTENT_TENURE_FLOOR: int = 10

#: Recency window, in years, for ``fresh_defector``.
FRESH_DEFECTOR_YEARS: float = 2.0


# ---------------------------------------------------------------------------
# Item 1 -- the anchor segment
# ---------------------------------------------------------------------------

def non_locked_crossers() -> pl.Expr:
    """
    Voters with demonstrated cross-party primary behaviour who have not settled
    back into a single party. Stack-rank item 1 -- the top-ranked product.

    Definition: ``ever_crossed`` AND ``crossover_class`` in
    :data:`NON_LOCKED_CLASSES`.

    WHY THE CLASS FILTER IS PART OF THE DEFINITION
    ----------------------------------------------
    ``ever_crossed`` alone includes LOCKED_D and LOCKED_R -- voters who crossed
    historically but whose recent behaviour has re-consolidated. Selling those
    as persuadable is the error this segment exists to avoid.

    THE ARITHMETIC DOES NOT CLOSE, AND THAT IS CORRECT
    --------------------------------------------------
    ``crossers - locked != non_locked``. ``crossover_class`` is assigned only to
    cohorts CROSSOVER_R / CROSSOVER_D / UNC_MIXED (the crossover_class block of
    ``classify_all_voters_primary_history``); every other cohort gets NULL. A
    residue of ``ever_crossed`` voters therefore carries no class at all and is
    excluded here -- they are UNCLASSIFIABLE, not non-locked, and asserting
    either lean about them would be unfounded.

    One known source of that residue is minor-party affiliation. A voter
    registered ``L`` (Libertarian) who has cast both D and R primary ballots is
    cohort MINOR_PARTY (before the minor-party cohort existed, OTHER), so their
    class is NULL and they are not in this segment although ``ever_crossed`` is
    True. Whether such voters belong in the product is a product decision, not
    one this predicate makes silently. Measured 2026-07-26 on the 2026-07-04
    snapshot: 2,609 residue rows statewide, and the inverse (a class set while
    ``ever_crossed`` is False) was exactly zero. Not re-measured since.

    A NULL class yields False here, never NULL, so the expression is safe to sum
    or to emit as a boolean column, not only to filter on.

    COUNTED-BALLOT RULE
    -------------------
    Crossing is understated: a voter whose only opposite-party ballot was
    rejected reads as never having crossed (module docstring). The bias shrinks
    this universe, so the item 1 value claim survives -- the real persuadable
    population is at least this large.

    GATE ANCHOR
    -----------
    Returned 536,143 rows statewide against the enriched cache built from the
    2026-07-04 snapshot (cache mtime 2026-07-25 10:29, 7,932,119 rows,
    affiliation window [2024, 2025, 2026]), measured 2026-07-26. It has NOT been
    re-measured on any later snapshot. Re-measure before treating it as a gate
    against any other cache: a gate that silently passes against a stale
    reference is worse than no gate.

    The figure is a property of the snapshot, not only of voter behaviour.
    Which cohort a crosser lands in (CROSSOVER_* or UNC_MIXED) follows current
    PARTY_AFFILIATION, and the two cohorts use different lean thresholds, so the
    population moves when the affiliation window steps on 1 January (R.C.
    3513.19; CLAUDE.md section 4) even if no voter casts another ballot.
    """
    return (
        pl.col("ever_crossed") & pl.col("crossover_class").is_in(NON_LOCKED_CLASSES)
    ).fill_null(False)


# ---------------------------------------------------------------------------
# Item 2 -- recency tiering
# ---------------------------------------------------------------------------

def fresh_defector(years: float = FRESH_DEFECTOR_YEARS) -> pl.Expr:
    """
    Crossers whose MOST RECENT partisan ballot was itself the cross, within
    ``years``. Stack-rank item 2 (sequence shape / recency).

    Definition (STRICT reading, pinned): ``ever_crossed`` AND the final two
    characters of ``partisan_ballot_sequence`` differ AND
    ``years_since_last_partisan <= years``.

    ``partisan_ballot_sequence`` is oldest-first and compacted (non-votes removed),
    so a differing final pair means the voter's latest partisan act was a switch
    away from their prior ballot -- 'DDR' qualifies, 'DRR' does not.

    THE REJECTED ALTERNATIVE -- DO NOT SILENTLY REOPEN THIS
    -------------------------------------------------------
    A LOOSE reading was considered and rejected: ``ever_crossed`` AND
    ``years_since_last_partisan <= years``, i.e. "crossed at some point, and has
    voted a partisan primary recently." That is a different and much larger
    population -- it counts a voter who crossed in 2008 and has voted straight-D
    since as a fresh defector, which is precisely the LOCKED behaviour item 1
    already excludes.

    The two differ by ~4.6x on Montgomery at years=2.0 (17,485 loose vs 3,810
    strict, measured 2026-07-26). The strict reading is what the stack-rank
    actually measured for item 2, so it is what the Phase 2 reconciliation
    checks against. Changing this constant changes the product; changing the
    READING invalidates the Phase 2 gate and must be a documented decision, not
    an edit.

    COUNTED-BALLOT RULE
    -------------------
    Doubly understated: a rejected ballot can both hide a cross AND make the
    voter's apparent "most recent partisan ballot" older than it truly was.
    Direction is still one-way -- this is a floor.
    """
    seq = pl.col("partisan_ballot_sequence")
    crossed_on_latest = (seq.str.len_chars() >= 2) & (
        seq.str.slice(-1, 1) != seq.str.slice(-2, 1)
    )
    return (
        pl.col("ever_crossed")
        & crossed_on_latest
        & (pl.col("years_since_last_partisan") <= years)
    ).fill_null(False)


# ---------------------------------------------------------------------------
# Item 3 -- turf aggregate (NOT a voter-level predicate)
# ---------------------------------------------------------------------------

def crossing_turf_rate() -> pl.Expr:
    """
    Share of a geography's voters who have ever crossed. Stack-rank item 3
    (geographic clustering).

    NOT A VOTER-LEVEL PREDICATE. This is an aggregate expression intended for
    use inside ``.group_by(...).agg(...)`` over a geographic key -- county or
    precinct. Applying it row-wise is a category error: crossing is a property
    of a person, crossing RATE is a property of a place.

    Returns the mean of ``ever_crossed`` cast to float, i.e. the crossing share
    within the group.

    COUNTED-BALLOT RULE
    -------------------
    Aggregate rates inherit the counted-ballot undercount, but they inherit it
    UNIFORMLY only to the extent that ballot-rejection rates are uniform across
    geographies -- which is not established. Per CLAUDE.md section 5 (the
    88-county assumption), do not assume it: a county whose crossing rate is an
    outlier gets profiled before it gets sold as turf.

    OPEN FALSIFIER
    --------------
    The stack-rank left item 3's decisive test unrun: whether precinct-level
    variance in this rate exceeds binomial sampling noise. If it does not,
    crossing is a county-level phenomenon and precinct turf packets are not a
    sellable product form. Run before shipping anything precinct-scoped.
    """
    return pl.col("ever_crossed").cast(pl.Float64).mean()


# ---------------------------------------------------------------------------
# Item 4 -- participation stratification
# ---------------------------------------------------------------------------

def consistent_low_frequency(tenure_floor: int = CONSISTENT_TENURE_FLOOR) -> pl.Expr:
    """
    Voters with a consistent one-party record who nonetheless vote primaries
    rarely, restricted to those with enough registration tenure for "rarely" to
    mean anything. Stack-rank item 4 (participation stratification).

    Definition: never crossed, has cast at least one partisan ballot, and was
    eligible for at least ``tenure_floor`` regular primaries.

    THE TENURE FILTER IS PART OF THE DEFINITION, NOT THE CALLER'S JOB
    ----------------------------------------------------------------
    Without it this segment is a registration-tenure artifact: a voter who
    registered last year and has voted one primary has a low participation rate
    for a reason that has nothing to do with commitment. The stack-rank is
    explicit that only the filtered version is sellable. Exposing an unfiltered
    variant would recreate exactly the divergent-copy problem this module exists
    to prevent.

    REGISTRATION-DATE RESET -- THE FIGURE IS A FLOOR, NOT A CEILING
    ---------------------------------------------------------------
    ``regular_primaries_eligible`` is anchored at
    ``min(REGISTRATION_DATE, first observed ballot)`` because Ohio preserves the
    original registration date through name and address changes but RESETS it on
    cancellation followed by reactivation (R.C. 3503.15(C)(9)(b)(ii); EOM Ch. 4
    p. 4-89). Ledgered as REGDATE-REACTIVATION-RESET in DATA_QUALITY.md.

    Resets SHORTEN apparent tenure. Voters whose true tenure clears
    ``tenure_floor`` but whose reset registration date does not are excluded
    here. The segment is therefore a floor -- it under-selects, never
    over-selects. Registration dates are additionally displaced around elections
    by the statutory blackout period (R.C. 3503.15(C)(9)(b)).

    NO MEAN OF ``primary_participation_rate``
    -----------------------------------------
    Do not summarise this segment with a mean rate. The field is bimodal with a
    large exact-zero floor; its population mean describes almost nobody. Report
    the floor share plus the conditional distribution given rate > 0.
    """
    return (
        ~pl.col("ever_crossed")
        & (pl.col("regular_primaries_eligible") >= tenure_floor)
        & (pl.col("partisan_ballot_sequence").str.len_chars() > 0)
    )


# ---------------------------------------------------------------------------
# Item 5 -- zero-floor split (list hygiene)
# ---------------------------------------------------------------------------

def _zero_floor() -> pl.Expr:
    """Eligible for at least one regular primary, but has cast zero partisan
    ballots. Shared base for the reachable/inert split."""
    return (pl.col("regular_primaries_eligible") > 0) & (
        pl.col("partisan_ballot_sequence").str.len_chars() == 0
    )


def zero_floor_reachable(general_col: str) -> pl.Expr:
    """
    Zero-partisan-primary voters who DID cast a counted ballot in the given
    general election. Stack-rank item 5, reachable half.

    ``general_col`` is a GENERAL-* column name. Resolve it via ``build_col_meta``
    in ``pipeline/voter_data_cleaner.py`` -- never hardcode a date, since the
    election-column set grows with every election.

    DO NOT PASS "THE LATEST GENERAL" -- PASS A NOVEMBER EVEN-YEAR GENERAL
    --------------------------------------------------------------------
    The chronologically last GENERAL column is frequently an odd-year municipal
    general with a fraction of the turnout, and it silently produces a segment
    ~8x too small. Measured 2026-07-26 (2026-07-04 snapshot): anchoring on
    GENERAL-11/04/2025 yields
    220,273 reachable statewide, while GENERAL-11/05/2024 yields 1,790,384.
    Filter ``build_col_meta`` output to ``date.month == 11 and date.year % 2 == 0``
    and take the most recent. This bit the Phase 1 verification harness on its
    first run.

    Blank cells arrive as empty strings, not nulls (CLAUDE.md section 4), so the
    test is an emptiness test, not a null test.

    WHAT "REACHABLE" MEANS AND DOES NOT MEAN
    ----------------------------------------
    It means: this voter has a counted general-election ballot on record despite
    no partisan primary signal. They belong in general-election persuasion and
    GOTV universes.

    It does NOT mean the voter engaged with any particular race. A counted BLANK
    ballot receives voting history (EOM Ch. 4 pp. 4-149/4-150), so a GENERAL mark
    is proof of a counted ballot and nothing more.
    """
    return _zero_floor() & (pl.col(general_col) != "")


def zero_floor_inert(general_cols: list[str]) -> pl.Expr:
    """
    Zero-partisan-primary voters with no counted ballot in ANY of the supplied
    general elections. Stack-rank item 5, inert half.

    ``general_cols`` should be the recent generals under consideration, resolved
    via ``build_col_meta`` -- never hardcoded.

    "INERT" IS BALLOT-INERT. IT IS NEVER "DISENGAGED"
    -------------------------------------------------
    This is the module's central boundary (see module docstring) and item 5 is
    where it is easiest to violate. These voters have no counted ballots in the
    supplied window. They may be renewing licences at the BMV, signing verified
    petitions, or responding to confirmation notices -- all of which the state
    counts as activity (O.A.C. 111:3-11-01(B); EOM Ch. 4 section 4.04) and none
    of which is visible in the SWVF.

    Item 5 is defensible EXACTLY BECAUSE it claims only ballot behaviour. The
    adjacent question -- separating "never will" from "not yet" -- requires Last
    Activity Type, is unanswerable in this file, and was discarded as D1 in the
    stack rank. Any in-SWVF answer to it would be a violation of this rule
    dressed up as analysis.

    Deprioritising this half for door programs is a defensible operational call.
    Describing them as disengaged voters is not.
    """
    if not general_cols:
        raise ValueError("general_cols must name at least one GENERAL-* column")
    no_ballot = pl.all_horizontal([pl.col(c) == "" for c in general_cols])
    return _zero_floor() & no_ballot
