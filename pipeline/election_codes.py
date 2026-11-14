"""
election_codes.py
=================
Single resolver for the compact election-history code type (DOO-184, M6).

The 91 election columns hold one short ballot code per voter per election. As
String each cell costs 16 bytes resident (Polars string view); as a pl.Enum over
the code set it costs 1 (UInt8 physical). Statewide that is about 10 GiB.

Code set
    Fixed, from the per-county profile of the 2026-10-24 snapshot (88 counties,
    91 columns; DATA_QUALITY.md row ELECTION-CELL-CODE-SET): '' (did not vote),
    D and R (party primary ballot), X (non-partisan ballot), and the minor-party
    codes L G C S N E. '' is a category, not null.

Strict
    A value outside the set raises UnknownElectionCodeError naming the county,
    column and values (operator decision 3, DOO-184). Before M6 such a value
    counted silently as a minor-party ballot. No code is ever mapped to another.

Scope
    In memory only (operator decision 2A). Loaders cast with to_codes(); the
    enriched cache is written as String via to_strings(), so cache readers are
    unchanged. Readers that must accept either dtype build their expressions
    with code_expr().
"""

from __future__ import annotations

from collections.abc import Iterable

import polars as pl

ELECTION_CODES: tuple[str, ...] = ('', 'D', 'R', 'X', 'L', 'G', 'C', 'S', 'N', 'E')
ELECTION_CODE_DTYPE = pl.Enum(list(ELECTION_CODES))
# Cell costs are memory_budget's (CELL_BYTES['Enum']); the 1-byte physical size
# it assumes is pinned by tools/tests/test_election_codes.py.

_COUNTY_COL = 'COUNTY_NUMBER'


class UnknownElectionCodeError(ValueError):
    """An election cell holds a value outside ELECTION_CODES."""


def is_code_dtype(dtype: pl.DataType) -> bool:
    """True when `dtype` is the compact election-code Enum."""
    return dtype == ELECTION_CODE_DTYPE


def code_expr(name: str, dtype: pl.DataType) -> pl.Expr:
    """The stripped ballot code of column `name`, for a column of type `dtype`.

    Enum cells were stripped when they were cast, so they are read as they are.
    String cells are stripped here, exactly as every reader did before M6.
    Comparisons such as == 'D', != '' and is_in([...]) then behave identically
    on both. Raises TypeError for any other dtype.
    """
    if is_code_dtype(dtype):
        return pl.col(name)
    if dtype == pl.String:
        return pl.col(name).str.strip_chars()
    raise TypeError(f'election column {name!r} has dtype {dtype}; expected String or '
                    f'{ELECTION_CODE_DTYPE}')


def check_codes(df: pl.DataFrame, cols: Iterable[str], where: str = '') -> None:
    """Raise UnknownElectionCodeError if any String cell of `cols` is outside the set.

    Values are compared after stripping, as to_codes() casts them. Nulls are
    allowed (they stay null). The message names each offending column, its
    values, and the counties holding them when the frame has COUNTY_NUMBER,
    otherwise `where`.
    """
    str_cols = [c for c in cols if df.schema[c] == pl.String]
    if not str_cols:
        return
    allowed = list(ELECTION_CODES)

    def bad(c: str) -> pl.Expr:
        v = pl.col(c).str.strip_chars()
        return v.is_not_null() & ~v.is_in(allowed)

    flags = df.select([bad(c).any().alias(c) for c in str_cols]).row(0, named=True)
    offending = [c for c in str_cols if flags[c]]
    if not offending:
        return
    lines = []
    for c in offending:
        rows = df.filter(bad(c))
        values = sorted(rows[c].str.strip_chars().unique().to_list())
        counties = (sorted(rows[_COUNTY_COL].cast(pl.String).unique().to_list())
                    if _COUNTY_COL in df.columns else [where or 'unknown'])
        lines.append(f'  {c}: values {values} in {rows.height:,} cells, county {counties}')
    raise UnknownElectionCodeError(
        f'election cells outside the code set {list(ELECTION_CODES)}'
        f'{" (" + where + ")" if where else ""}:\n' + '\n'.join(lines)
        + '\nAdd a profiled code to ELECTION_CODES and DATA_QUALITY.md '
          '(ELECTION-CELL-CODE-SET) only after checking its meaning with the county.'
    )


def to_codes(df: pl.DataFrame, cols: Iterable[str], where: str = '') -> pl.DataFrame:
    """Return `df` with election `cols` stripped and cast to ELECTION_CODE_DTYPE.

    Strict: raises UnknownElectionCodeError before casting if any value is
    outside the set. Columns already of the code dtype pass through, so the
    call is idempotent. Raises TypeError for a column of any other dtype.
    """
    cols = list(cols)
    for c in cols:
        if not (is_code_dtype(df.schema[c]) or df.schema[c] == pl.String):
            raise TypeError(f'election column {c!r} has dtype {df.schema[c]}; cannot cast to codes')
    check_codes(df, cols, where)
    casts = [pl.col(c).str.strip_chars().cast(ELECTION_CODE_DTYPE)
             for c in cols if df.schema[c] == pl.String]
    return df.with_columns(casts) if casts else df


def string_casts(schema: pl.Schema, cols: Iterable[str]) -> list[pl.Expr]:
    """Expressions casting each code-dtype column of `cols` back to String.

    Used eagerly by to_strings() and lazily by the enriched-cache writer, which
    streams the cast rather than applying it to the whole frame. Streaming still
    costs up to one String copy of the election columns (measured 2026-11-14);
    memory_budget.estimate_rebuild charges it.
    """
    return [pl.col(c).cast(pl.String) for c in cols if is_code_dtype(schema[c])]


def to_strings(df: pl.DataFrame, cols: Iterable[str]) -> pl.DataFrame:
    """Return `df` with every code-dtype column of `cols` cast back to String."""
    casts = string_casts(df.schema, cols)
    return df.with_columns(casts) if casts else df
