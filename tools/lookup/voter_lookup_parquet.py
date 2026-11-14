"""voter_lookup_parquet.py -- global substring search across the enriched Parquet cache.

Renamed 2026-07 from voter_lookup.py to voter_lookup_parquet.py to disambiguate
from the sibling voter_lookup_source.py (renamed from raw_voter_lookup.py, same
date), which runs the identical search against the raw SWVF_*.txt files instead.
The old name didn't say which storage layer it hit; if anything still imports
this module by its old filename/path, update the reference -- the run_global_
parquet_search() function name is unchanged.

2026-07: added optional --last/--first field-scoped filters. The original
positional `term` is a single literal substring checked against EVERY column
(pl.any_horizontal(...str.contains(...))) -- it has no wildcard support and
cannot AND two separate name parts together, so a nickname/legal-name mismatch
(e.g. "Zach" on the voter file vs. a search for "Zachary") could miss a real
match, and "Zach Roberts" as one term never matches because FIRST_NAME and
LAST_NAME are separate columns. --last/--first solve that: --last is a PREFIX
match against LAST_NAME (so "Roberts" also catches "Robertson", matching the
substring behavior elsewhere in this script); --first is a SUBSTRING match
against FIRST_NAME (so "Zach" alone catches both "ZACH" and "ZACHARY"). Both
are optional and AND together with each other and with `term` when given
together, so `--last Roberts --first Zach` finds Zach/Zachary Roberts without
needing to know which form the voter file uses.

Also fixed the same date: stdout is reconfigured to UTF-8 on startup. The
console summary print (jurisdictional combinations table) previously crashed
with UnicodeEncodeError under the Windows cp1252 console default whenever a
matched row contained a non-ASCII character (e.g. a diacritic in a city
name) -- the search itself and the CSV export both completed fine, only the
on-screen summary raised. Pre-existing bug, unrelated to the --last/--first
addition; fixed here because it blocked verifying the new filters end to end.
"""
import polars as pl
from pathlib import Path
from datetime import datetime
import argparse
import sys

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
from pipeline.paths import PARQUET_DIR, PARTITION_GLOB  # noqa: E402

# Windows console default (cp1252) crashes on non-ASCII output (e.g. a
# diacritic in a jurisdiction name) -- reconfigure stdout to UTF-8 so the
# console summary print never raises UnicodeEncodeError mid-run, after the
# search has already completed (CLAUDE.md "Console encoding (Windows)").
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

def run_global_parquet_search(term: str = None, parquet_dir: str = None,
                               last: str = None, first: str = None):
    # 1. Handle Prompting
    # A field-scoped search (--last/--first) stands on its own -- only prompt
    # for a free-text term when NEITHER field filter was given.
    if not term and not last and not first:
        term = input("Enter search term: ").strip()
    if not term and not last and not first:
        print("No search term provided. Exiting.")
        return

    # Resolve paths
    script_dir = Path(__file__).parent.resolve()
    project_root = script_dir.parent.parent
    if parquet_dir is None:
        base_path = PARQUET_DIR
    else:
        base_path = Path(parquet_dir).resolve()

    if not base_path.exists():
        print(f"Error: Parquet directory '{base_path}' not found.")
        return

    term_upper = term.upper() if term else None
    last_upper = last.upper() if last else None
    first_upper = first.upper() if first else None

    label_parts = []
    if term:
        label_parts.append(term)
    if last:
        label_parts.append(f"last={last}")
    if first:
        label_parts.append(f"first={first}")
    label = " ".join(label_parts)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_label = label.replace(" ", "_").replace("=", "-")
    output_file = project_root / f"search_{safe_label}_{timestamp}.csv"

    print(f"\nGLOBAL PARQUET SEARCH")
    print(f"Term:   '{label}'")
    print(f"Source: {base_path}")
    print("-" * 60)

    partitions = list(base_path.glob(PARTITION_GLOB))
    if not partitions:
        print(f"No county partitions found in {base_path}")
        return

    all_matches = []

    for p in sorted(partitions):
        c_num = p.name.split("=")[1]
        try:
            lf = pl.scan_parquet(p)

            # Build the AND-composed filter from whichever of term/last/first
            # were supplied. Field filters require the target column to exist
            # in this partition's schema; term (global) always applies if given.
            clauses = []
            if term_upper:
                clauses.append(
                    pl.any_horizontal(
                        pl.all().cast(pl.String).str.to_uppercase().str.contains(term_upper)
                    )
                )
            if last_upper and "LAST_NAME" in lf.collect_schema().names():
                clauses.append(
                    pl.col("LAST_NAME").cast(pl.String).str.to_uppercase()
                      .str.starts_with(last_upper)
                )
            if first_upper and "FIRST_NAME" in lf.collect_schema().names():
                clauses.append(
                    pl.col("FIRST_NAME").cast(pl.String).str.to_uppercase()
                      .str.contains(first_upper)
                )

            if not clauses:
                continue
            filter_expr = clauses[0]
            for c in clauses[1:]:
                filter_expr = filter_expr & c

            match = lf.filter(filter_expr).collect()

            if not match.is_empty():
                match = match.with_columns(pl.lit(c_num).alias("COUNTY_NUMBER"))
                all_matches.append(match)
                print(f"  County {c_num}: Found {len(match):,} matches")

        except Exception as e:
            continue

    if not all_matches:
        print(f"\nNo records found matching '{label}'.")
        return

    # Combine results
    final_df = pl.concat(all_matches)
    
    # --- RESTORED SUMMARY VIEW ---
    print("-" * 60)
    print(f"TOTAL MATCHES FOUND: {len(final_df):,}")
    
    # Show Top combinations (Jurisdictional focus for quick sanity check)
    print("\nTOP JURISDICTIONAL COMBINATIONS:")
    juris_cols = [c for c in ["CITY", "TOWNSHIP", "VILLAGE", "PRECINCT_NAME"] if c in final_df.columns]
    summary = final_df.group_by(["COUNTY_NUMBER"] + juris_cols).agg(pl.len().alias("Voters")).sort("Voters", descending=True)
    with pl.Config(tbl_rows=20, tbl_width_chars=120):
        print(summary.head(20))

    # Show County Breakdown
    print("\nBREAKDOWN BY COUNTY:")
    county_sum = final_df.group_by("COUNTY_NUMBER").agg(pl.len().alias("Voters")).sort("Voters", descending=True)
    print(county_sum)
    
    # --- FILE EXPORT ---
    print("-" * 60)
    print(f"Writing all 135 columns for {len(final_df):,} records to: {output_file.name}")
    try:
        final_df.write_csv(output_file)
        print("Export Complete.")
    except Exception as e:
        print(f"Error writing CSV: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Global search across all columns in Parquet files.")
    parser.add_argument("term", nargs="?", help="Search term (optional, will prompt if omitted)")
    parser.add_argument("--dir", default=None, help="Path to parquet directory")
    parser.add_argument("--last", default=None,
                         help="Filter to LAST_NAME starting with this (prefix match, e.g. --last Roberts)")
    parser.add_argument("--first", default=None,
                         help="Filter to FIRST_NAME containing this (substring match, e.g. --first Zach "
                              "also matches ZACHARY). Combine with --last for a real name search: "
                              "--last Roberts --first Zach")

    args = parser.parse_args()
    run_global_parquet_search(args.term, args.dir, args.last, args.first)
