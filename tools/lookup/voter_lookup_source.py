"""voter_lookup_source.py -- global substring search across the raw SWVF_*.txt files.

Renamed 2026-07 from raw_voter_lookup.py to voter_lookup_source.py to pair with
the sibling voter_lookup_parquet.py (renamed from voter_lookup.py, same date),
which runs the identical search against the cleaned Parquet cache instead. Use
this one when checking whether a record was dropped or altered somewhere in
pipeline processing -- it reads the untouched original source files. If anything
still imports this module by its old filename/path, update the reference -- the
run_global_raw_search() function name is unchanged.
"""
import polars as pl
from pathlib import Path
from datetime import datetime
import argparse
import sys

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
from pipeline.paths import TXT_DIR  # noqa: E402

def run_global_raw_search(term: str = None, raw_dir: str = None):
    # 1. Handle Prompting
    if not term:
        term = input("Enter search term: ").strip()
    if not term:
        print("No search term provided. Exiting.")
        return

    # Resolve paths
    script_dir = Path(__file__).parent.resolve()
    project_root = script_dir.parent.parent
    if raw_dir is None:
        raw_path = TXT_DIR
    else:
        raw_path = Path(raw_dir).resolve()

    if not raw_path.exists():
        print(f"Error: Raw directory '{raw_path}' not found.")
        return

    term_upper = term.upper()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = project_root / f"search_raw_{term.replace(' ', '_')}_{timestamp}.csv"

    print(f"\nGLOBAL RAW TEXT SEARCH")
    print(f"Term:   '{term}'")
    print(f"Source: {raw_path}")
    print("-" * 60)

    files = list(raw_path.glob("SWVF_*.txt"))
    if not files:
        print(f"No SWVF_*.txt files found in {raw_path}")
        return

    all_matches = []
    
    for f in sorted(files):
        print(f"  Scanning {f.name}...")
        try:
            lf = pl.scan_csv(
                f, 
                separator=",", 
                quote_char='"', 
                infer_schema_length=0, 
                ignore_errors=True,
                encoding="utf8-lossy"
            )
            
            filter_expr = pl.any_horizontal(
                pl.all().fill_null("").str.to_uppercase().str.contains(term_upper)
            )
            
            match = lf.filter(filter_expr).collect(engine='streaming')
            
            if not match.is_empty():
                match = match.with_columns(pl.lit(f.name).alias("SOURCE_FILE"))
                all_matches.append(match)
                print(f"    -> Found {len(match):,} records")
                
        except Exception as e:
            continue

    if not all_matches:
        print(f"\nNo records found containing '{term}' in raw files.")
        return

    # Combine results
    final_df = pl.concat(all_matches)
    
    # --- RESTORED SUMMARY VIEW ---
    print("-" * 60)
    print(f"TOTAL MATCHES FOUND: {len(final_df):,}")
    
    print("\nTOP JURISDICTIONAL COMBINATIONS:")
    juris_cols = [c for c in ["CITY", "TOWNSHIP", "VILLAGE", "PRECINCT_NAME"] if c in final_df.columns]
    summary = final_df.group_by(["COUNTY_NUMBER"] + juris_cols).agg(pl.len().alias("Voters")).sort("Voters", descending=True)
    with pl.Config(tbl_rows=20, tbl_width_chars=120):
        print(summary.head(20))

    print("\nBREAKDOWN BY SOURCE FILE:")
    file_sum = final_df.group_by("SOURCE_FILE").agg(pl.len().alias("Voters")).sort("SOURCE_FILE")
    print(file_sum)

    # --- FILE EXPORT ---
    print("-" * 60)
    print(f"Writing all 135 columns for {len(final_df):,} records to: {output_file.name}")
    try:
        final_df.write_csv(output_file)
        print("Export Complete.")
    except Exception as e:
        print(f"Error writing CSV: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Global search across all columns in raw SWVF text files.")
    parser.add_argument("term", nargs="?", help="Search term (optional, will prompt if omitted)")
    parser.add_argument("--dir", default=None, help="Path to raw SWVF files directory")
    
    args = parser.parse_args()
    run_global_raw_search(args.term, args.dir)
