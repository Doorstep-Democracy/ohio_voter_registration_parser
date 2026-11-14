"""
jurisdiction_collisions_report.py
──────────────────────────────────
Detect jurisdiction name collisions that would corrupt output if slugs are
derived from jurisdiction name alone (without county scoping).

For each jurisdiction type, reports any value that appears in more than one
COUNTY_NUMBER partition — i.e., a name collision that a naive group_by on the
name column alone would silently merge into one file.

Outputs a markdown report to local/working/collision_report.md.

Run:
    uv run python tools/admin/jurisdiction_collisions_report.py
"""

import sys
from datetime import datetime
from pathlib import Path
import polars as pl

BASE_DIR    = Path(__file__).resolve().parent.parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))
from pipeline.paths import PARQUET_DIR, parquet_glob  # noqa: E402
REPORT_PATH = BASE_DIR / "local" / "working" / "collision_report.md"

JURISDICTION_COLUMNS = {
    "cities":                           "CITY",
    "townships":                        "TOWNSHIP",
    "villages":                         "VILLAGE",
    "local_school_districts":           "LOCAL_SCHOOL_DISTRICT",
    "city_school_districts":            "CITY_SCHOOL_DISTRICT",
    "exempted_vill_school_districts":   "EXEMPTED_VILL_SCHOOL_DISTRICT",
    "state_senate_districts":           "STATE_SENATE_DISTRICT",
    "state_rep_districts":              "STATE_REPRESENTATIVE_DISTRICT",
    "congressional_districts":          "CONGRESSIONAL_DISTRICT",
    "county_court_districts":           "COUNTY_COURT_DISTRICT",
    "municipal_court_districts":        "MUNICIPAL_COURT_DISTRICT",
    "court_of_appeals":                 "COURT_OF_APPEALS",
}

# Columns we actually need — avoid loading 135-col frame just for this check
LOAD_COLS = ["COUNTY_NUMBER"] + list(JURISDICTION_COLUMNS.values())


def main():
    print(f"Loading parquet from {PARQUET_DIR} ...")
    df = pl.read_parquet(
        parquet_glob(PARQUET_DIR),
        columns=LOAD_COLS,
        hive_partitioning=True,
    )
    print(f"Loaded {df.height:,} rows")
    print(f"Writing report to {REPORT_PATH} ...\n")

    lines = []
    lines.append("# Jurisdiction Name Collision Report")
    lines.append(f"\nGenerated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  ")
    lines.append(f"Source: `{PARQUET_DIR}`  ")
    lines.append(f"Rows loaded: {df.height:,}\n")
    lines.append("A **collision** means the same jurisdiction name appears in voters")
    lines.append("from more than one county partition. Name-only slugs would silently")
    lines.append("merge these into a single output file.\n")
    lines.append("---\n")

    any_collisions = False

    for jurisdiction_type, col in JURISDICTION_COLUMNS.items():
        if col not in df.columns:
            lines.append(f"## {jurisdiction_type}\n")
            lines.append(f"> SKIP — column `{col}` not found in parquet.\n")
            print(f"[SKIP] {jurisdiction_type}: column {col!r} not in parquet")
            continue

        filtered = (
            df.select([col, "COUNTY_NUMBER"])
            .filter(
                pl.col(col).is_not_null()
                & (pl.col(col).cast(pl.String).str.strip_chars() != "")
            )
        )

        total_unique = filtered.select(col).unique().height

        cross_county = (
            filtered
            .unique()
            .group_by(col)
            .agg(
                pl.col("COUNTY_NUMBER").n_unique().alias("n_counties"),
                pl.col("COUNTY_NUMBER").sort().alias("counties"),
            )
            .filter(pl.col("n_counties") > 1)
            .sort("n_counties", descending=True)
        )

        lines.append(f"## {jurisdiction_type}")
        lines.append(f"\nColumn: `{col}` — {total_unique} unique values\n")

        if cross_county.height == 0:
            lines.append(f"✅ **No collisions.**\n")
            print(f"[OK]      {jurisdiction_type:<40} {total_unique:>5} unique — no collisions")
        else:
            any_collisions = True
            lines.append(
                f"⚠️ **{cross_county.height} of {total_unique} names span multiple counties.**\n"
            )
            lines.append("| Name | Counties (n) | County numbers |")
            lines.append("|------|:---:|---|")
            for row in cross_county.iter_rows(named=True):
                name        = str(row[col]).strip()
                n           = row["n_counties"]
                county_list = ", ".join(str(c).zfill(2) for c in row["counties"])
                lines.append(f"| {name} | {n} | {county_list} |")
            lines.append("")
            print(
                f"[COLLIDE] {jurisdiction_type:<40} {cross_county.height:>5} / {total_unique}"
                f" names span multiple counties"
            )

    lines.append("---\n")
    if any_collisions:
        lines.append(
            "**Result:** Collisions detected. The groupings script must use a composite key "
            "(`COUNTY_NUMBER` + name) for county-scoped types, or confirm that cross-county "
            "presence reflects a single real jurisdiction (e.g. legislative districts, "
            "multi-county cities)."
        )
        print("\nRESULT: Collisions detected — see local/working/collision_report.md")
    else:
        lines.append("**Result:** No collisions. Name-only slugs are safe for all jurisdiction types.")
        print("\nRESULT: No collisions.")

    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")
    print(f"Report written to {REPORT_PATH}")


if __name__ == "__main__":
    main()
