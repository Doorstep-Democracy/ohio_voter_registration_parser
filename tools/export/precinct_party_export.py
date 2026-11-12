"""
precinct_party_export.py
════════════════════════
Export a multi-tab Excel workbook of voter names + addresses + all source
columns, split by partisan cohort, for a chosen county or a single precinct.

Tab layout (7-tab partisan spectrum — colours match dashboard doughnut):
  Pure_R          #ef4444   affiliated R, zero D primary ballots ever
  UNC_Lapsed_R    #fca5a5   unaffiliated, all primaries were R ballots
  Mixed_Active    #f59e0b   currently affiliated crossover voters
  Mixed_Lapsed    #a78bfa   unaffiliated, mixed or X-only primary history
  UNC_No_Primary  #9ca3af   unaffiliated, no primary participation ever
  UNC_Lapsed_D    #93c5fd   unaffiliated, all primaries were D ballots
  Pure_D          #3b82f6   affiliated D, zero R primary ballots ever
  Summary         #1e293b   row counts per tab

Cohort assignment uses classify_all_voters_primary_history() (pipeline/voter_data_cleaner.py).
cohort_family is the segmentation key; internal cohort column preserved for
future proprietary crossover analysis but not surfaced in output tabs.

Output:  UNC_Exports/Workbooks/{County}_{Precinct}_voters.xlsx
                             or {County}_all_voters.xlsx

Usage:
    python precinct_party_export.py

Security: UNC_Exports/ is in .gitignore — no PII leaves that directory.
"""

import logging
import sys
import time
from pathlib import Path

import polars as pl

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR    = Path(__file__).resolve().parent.parent.parent
PARQUET_DIR = BASE_DIR / "local" / "source" / "parquet"  # PATCH: Rerouted to local/ workspace
OUT_DIR     = BASE_DIR / "local" / "exports" / "Workbooks"  # PATCH: Rerouted to local/ workspace

# ── Import shared helpers ─────────────────────────────────────────────────────
sys.path.insert(0, str(BASE_DIR))
from pipeline import voter_data_cleaner as v2

OHIO_COUNTIES  = v2.OHIO_COUNTIES
identify_cols  = v2.identify_election_cols
classify_unc   = v2.classify_unc_primary_history
classify_all   = v2.classify_all_voters_primary_history

# ── Tab definitions ───────────────────────────────────────────────────────────
# 7-tab partisan-spectrum layout based on cohort_family (universal classifier).
# Mirrors the public dashboard taxonomy exactly — no decay scoring surfaced.
TABS = [
    # (tab_name,           tab_colour,  filter_col,      cohort_family value)
    ("Pure_R",           "#ef4444",   "cohort_family", "PURE_R"),
    ("UNC_Lapsed_R",     "#fca5a5",   "cohort_family", "UNC_LAPSED_R"),
    ("Mixed_Active",     "#f59e0b",   "cohort_family", "MIXED_ACTIVE"),
    ("Mixed_Lapsed",     "#a78bfa",   "cohort_family", "MIXED_LAPSED"),
    ("UNC_Nonpartisan",  "#c4b5cd",   "cohort_family", "UNC_NONPARTISAN"),
    ("UNC_No_Primary",   "#9ca3af",   "cohort_family", "UNC_NO_PRIMARY"),
    ("UNC_Lapsed_D",     "#93c5fd",   "cohort_family", "UNC_LAPSED_D"),
    ("Pure_D",           "#3b82f6",   "cohort_family", "PURE_D"),
]

# ── Logging ───────────────────────────────────────────────────────────────────
def _build_logger() -> logging.Logger:
    log = logging.getLogger("precinct_party_export")
    log.setLevel(logging.DEBUG)
    if not log.handlers:
        h = logging.StreamHandler(sys.stdout)
        h.setFormatter(logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s",
                                         datefmt="%H:%M:%S"))
        log.addHandler(h)
    return log

# ── Data loading ──────────────────────────────────────────────────────────────
def _load_county(county_num: str, logger) -> pl.DataFrame:
    """Load one county from Parquet cache."""
    part = PARQUET_DIR / f"COUNTY_NUMBER={county_num}"
    if not part.exists():
        logger.error("Parquet partition not found: %s", part)
        sys.exit(1)
    logger.info("Loading parquet partition: %s", part)
    df = pl.read_parquet(part)
    logger.info("  %s rows × %s columns loaded", f"{len(df):,}", len(df.columns))
    return df

# ── Precinct listing ──────────────────────────────────────────────────────────
def _list_precincts(df: pl.DataFrame) -> list[str]:
    return sorted(df["PRECINCT_NAME"].drop_nulls().unique().to_list())

# ── Excel writer ──────────────────────────────────────────────────────────────
# ── Generation boundaries (Pew Research) ─────────────────────────────────────
# Source: https://www.pewresearch.org/short-reads/2019/01/17/where-millennials-end-and-generation-z-begins/
GENERATIONS = [
    ("Silent",      1928, 1945),
    ("Boomers",     1946, 1964),
    ("Gen X",       1965, 1980),
    ("Millennials", 1981, 1996),
    ("Gen Z",       1997, 2012),
]
DECADES = [
    ("1920s", 1920, 1929),
    ("1930s", 1930, 1939),
    ("1940s", 1940, 1949),
    ("1950s", 1950, 1959),
    ("1960s", 1960, 1969),
    ("1970s", 1970, 1979),
    ("1980s", 1980, 1989),
    ("1990s", 1990, 1999),
    ("2000s", 2000, 2012),
]

# Cohort colours aligned with TABS order
COHORT_COLORS = [t[1] for t in TABS]   # ["#ef4444", "#fca5a5", ...]


def _xl_col(n: int) -> str:
    """Convert 0-based column index to Excel column letter (A, B, ... Z, AA ...)."""
    result = ""
    while True:
        result = chr(ord("A") + n % 26) + result
        n = n // 26 - 1
        if n < 0:
            break
    return result


def _countifs_dob_cohort(tab: str, dob_col: str, yr_lo: int, yr_hi: int,
                          cf_col: str, cf_val: str, max_row: int) -> str:
    """
    COUNTIFS formula counting rows on `tab` where:
      - LEFT(dob_col, 4)*1 is between yr_lo and yr_hi (inclusive)
      - cf_col == cf_val
    DATE_OF_BIRTH is stored as YYYY-MM-DD text, so we use YEAR-like text math
    via an array-style SUMPRODUCT to avoid helper columns.
    """
    dob_range = f"'{tab}'!{dob_col}2:{dob_col}{max_row}"
    cf_range  = f"'{tab}'!{cf_col}2:{cf_col}{max_row}"
    return (
        f'=SUMPRODUCT('
        f'(VALUE(LEFT({dob_range},4))>={yr_lo})*'
        f'(VALUE(LEFT({dob_range},4))<={yr_hi})*'
        f'({cf_range}="{cf_val}"))'
    )


def _write_workbook(
    df:           "pl.DataFrame",
    primary_cols: list,
    out_path:     "Path",
    logger,
    precinct_mode: bool = False,
) -> None:
    import xlsxwriter  # imported here so the rest of the script works without it

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    logger.info("Classifying voters with universal cohort classifier ...")
    classified = classify_all(df, primary_cols, logger)

    score_cols = [
        "cohort", "cohort_family", "crossover_class",
        "lean_score", "confidence", "recent_5yr_lean",
        "last_three_party", "years_since_last_partisan",
        "switch_count",
        "d_primaries", "r_primaries", "x_primaries",
        "total_primaries", "partisan_primaries",
        "is_new_registrant",
    ]
    score_cols = [c for c in score_cols if c in classified.columns]
    df_enriched = df.join(
        classified.select(["SOS_VOTERID"] + score_cols),
        on="SOS_VOTERID",
        how="left",
    )

    # ── Segment frames by cohort_family ───────────────────────────────────────
    frames: dict = {}
    for tab_name, _, filter_col, cohort_value in TABS:
        frames[tab_name] = df_enriched.filter(pl.col(filter_col) == cohort_value)
        logger.info("  %-20s %s rows", tab_name, f"{len(frames[tab_name]):,}")

    # ── Open workbook ─────────────────────────────────────────────────────────
    logger.info("Writing workbook → %s", out_path)
    wb = xlsxwriter.Workbook(str(out_path), {"constant_memory": False})

    header_fmt = wb.add_format({
        "bold": True, "bg_color": "#1e293b", "font_color": "#e2e8f0",
        "border": 1, "border_color": "#334155",
        "text_wrap": False,
    })
    cell_fmt = wb.add_format({"border": 0, "text_wrap": False})
    int_fmt  = wb.add_format({"border": 0, "num_format": "#,##0"})

    def _is_numeric_col(series: "pl.Series") -> bool:
        return series.dtype in (pl.Int8, pl.Int16, pl.Int32, pl.Int64,
                                 pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64,
                                 pl.Float32, pl.Float64)

    # ── Write data tabs ───────────────────────────────────────────────────────
    DROP_ALWAYS = [
        "lean_score", "confidence", "recent_5yr_lean",
        "crossover_class", "last_three_party",
        "years_since_last_partisan", "switch_count",
    ]
    # Track column positions for COUNTIFS references
    tab_col_map: dict = {}   # tab_name -> {"cohort_family": "C", "DATE_OF_BIRTH": "E", max_row: N}

    for tab_name, tab_colour, _, _ in TABS:
        frame = frames[tab_name]

        drop = [c for c in DROP_ALWAYS if c in frame.columns]
        if drop:
            frame = frame.drop(drop)

        ws = wb.add_worksheet(tab_name)
        ws.set_tab_color(tab_colour)
        ws.freeze_panes(1, 0)
        ws.autofilter(0, 0, 0, len(frame.columns) - 1)

        cols = frame.columns
        col_series = {c: frame[c] for c in cols}

        # Record column letters for COUNTIFS formulas
        cf_letter  = _xl_col(cols.index("cohort_family")) if "cohort_family" in cols else None
        dob_letter = _xl_col(cols.index("DATE_OF_BIRTH")) if "DATE_OF_BIRTH" in cols else None
        tab_col_map[tab_name] = {
            "cohort_family": cf_letter,
            "DATE_OF_BIRTH": dob_letter,
            "max_row": len(frame) + 1,
        }

        # Header row
        for ci, col in enumerate(cols):
            ws.write(0, ci, col, header_fmt)
            sample = col_series[col].head(200).cast(pl.Utf8).drop_nulls().to_list()
            max_w  = max([len(col)] + [len(str(v)) for v in sample])
            ws.set_column(ci, ci, min(max_w + 2, 40))

        # Data rows
        numeric_flags = {c: _is_numeric_col(col_series[c]) for c in cols}
        for ri, row_tuple in enumerate(frame.iter_rows(), start=1):
            for ci, (col, val) in enumerate(zip(cols, row_tuple)):
                if val is None:
                    ws.write_blank(ri, ci, None, cell_fmt)
                elif numeric_flags[col]:
                    ws.write_number(ri, ci, val, int_fmt)
                else:
                    ws.write_string(ri, ci, str(val), cell_fmt)

        logger.info("    wrote %s data rows to tab '%s'", f"{len(frame):,}", tab_name)

    # ── Summary tab ───────────────────────────────────────────────────────────
    ws_sum = wb.add_worksheet("Summary")
    ws_sum.set_tab_color("#1e293b")

    sum_hdr = wb.add_format({"bold": True, "bg_color": "#1e293b",
                              "font_color": "#e2e8f0", "border": 1})
    sum_lbl = wb.add_format({"bold": True, "border": 0})
    sum_num = wb.add_format({"num_format": "#,##0", "border": 0})
    pct_fmt = wb.add_format({"num_format": "0.0%", "border": 0})

    # ── Section A: Cohort counts (rows 0–8) ───────────────────────────────────
    ws_sum.write(0, 0, "Cohort",      sum_hdr)
    ws_sum.write(0, 1, "Voter Count", sum_hdr)
    ws_sum.write(0, 2, "% of Total",  sum_hdr)
    ws_sum.set_column(0, 0, 22)
    ws_sum.set_column(1, 1, 14)
    ws_sum.set_column(2, 2, 12)

    cohort_count_cells = {}   # tab_name -> "B{row}" for chart series refs
    for ri, (tab_name, _, _, _) in enumerate(TABS, start=1):
        ws_sum.write_string(ri, 0, tab_name)
        ws_sum.write_number(ri, 1, len(frames[tab_name]), sum_num)
        cohort_count_cells[tab_name] = ri   # 0-based row index for xlsxwriter

    total_row = len(TABS) + 1   # 0-based row 8
    ws_sum.write_string(total_row, 0, "TOTAL", sum_lbl)
    ws_sum.write_number(total_row, 1,
                        sum(len(frames[t]) for t, *_ in TABS), sum_num)

    # % of total formulas
    total_b = f"B{total_row + 1}"   # Excel 1-based
    for ri in range(1, len(TABS) + 1):
        ws_sum.write_formula(ri, 2, f"=IF({total_b}=0,0,B{ri+1}/{total_b})", pct_fmt)

    # ── Section B: Age by Decade grid (rows 10–19, cols 0–7) ─────────────────
    DECADE_ROW_START = total_row + 2   # row index 10 (0-based)
    ws_sum.write(DECADE_ROW_START, 0, "Age by Decade", sum_lbl)

    # Header: cohort names across columns 1–7
    for ci, (tab_name, _, _, _) in enumerate(TABS, start=1):
        ws_sum.write(DECADE_ROW_START, ci, tab_name, sum_hdr)
    ws_sum.write(DECADE_ROW_START, len(TABS) + 1, "TOTAL", sum_hdr)

    decade_data_rows = {}  # decade_label -> row_index
    for di, (decade_label, yr_lo, yr_hi) in enumerate(DECADES):
        ri = DECADE_ROW_START + 1 + di
        decade_data_rows[decade_label] = ri
        ws_sum.write_string(ri, 0, decade_label, sum_lbl)
        row_total_parts = []
        for ci, (tab_name, _, _, cf_val) in enumerate(TABS, start=1):
            info = tab_col_map.get(tab_name, {})
            dob_col = info.get("DATE_OF_BIRTH")
            cf_col  = info.get("cohort_family")
            max_row = info.get("max_row", 2)
            if dob_col and cf_col and max_row > 1:
                formula = _countifs_dob_cohort(
                    tab_name, dob_col, yr_lo, yr_hi, cf_col, cf_val, max_row)
                ws_sum.write_formula(ri, ci, formula, sum_num)
            else:
                ws_sum.write_number(ri, ci, 0, sum_num)
            xl_row = ri + 1   # Excel 1-based
            xl_col = _xl_col(ci)
            row_total_parts.append(f"{xl_col}{xl_row}")
        ws_sum.write_formula(ri, len(TABS) + 1,
                             "=" + "+".join(row_total_parts), sum_num)

    # ── Section C: Age by Generation grid (rows after decades + 2) ───────────
    GEN_ROW_START = DECADE_ROW_START + len(DECADES) + 2
    ws_sum.write(GEN_ROW_START, 0, "Age by Generation", sum_lbl)
    ws_sum.write_comment(
        GEN_ROW_START, 0,
        "Generation boundaries per Pew Research Center.\n"
        "Source: https://www.pewresearch.org/short-reads/2019/01/17/"
        "where-millennials-end-and-generation-z-begins/",
        {"x_scale": 2.5, "y_scale": 1.5},
    )

    for ci, (tab_name, _, _, _) in enumerate(TABS, start=1):
        ws_sum.write(GEN_ROW_START, ci, tab_name, sum_hdr)
    ws_sum.write(GEN_ROW_START, len(TABS) + 1, "TOTAL", sum_hdr)

    gen_data_rows = {}
    for gi, (gen_label, yr_lo, yr_hi) in enumerate(GENERATIONS):
        ri = GEN_ROW_START + 1 + gi
        gen_data_rows[gen_label] = ri
        ws_sum.write_string(ri, 0, gen_label, sum_lbl)
        row_total_parts = []
        for ci, (tab_name, _, _, cf_val) in enumerate(TABS, start=1):
            info = tab_col_map.get(tab_name, {})
            dob_col = info.get("DATE_OF_BIRTH")
            cf_col  = info.get("cohort_family")
            max_row = info.get("max_row", 2)
            if dob_col and cf_col and max_row > 1:
                formula = _countifs_dob_cohort(
                    tab_name, dob_col, yr_lo, yr_hi, cf_col, cf_val, max_row)
                ws_sum.write_formula(ri, ci, formula, sum_num)
            else:
                ws_sum.write_number(ri, ci, 0, sum_num)
            xl_row = ri + 1
            xl_col = _xl_col(ci)
            row_total_parts.append(f"{xl_col}{xl_row}")
        ws_sum.write_formula(ri, len(TABS) + 1,
                             "=" + "+".join(row_total_parts), sum_num)

    # ── Charts (precinct mode only) ────────────────────────────────────────────
    if precinct_mode:
        SN = "Summary"   # sheet name shorthand

        # Chart 1: Doughnut — cohort counts
        ch_donut = wb.add_chart({"type": "doughnut"})
        ch_donut.set_title({"name": "Party Affiliation"})
        ch_donut.set_hole_size(40)
        ch_donut.add_series({
            "name":       "Voters",
            "categories": [SN, 1, 0, len(TABS), 0],
            "values":     [SN, 1, 1, len(TABS), 1],
            "points": [
                {"fill": {"color": color}} for _, color, _, _ in TABS
            ],
            "data_labels": {"none": True},
        })
        ch_donut.set_legend({"position": "right"})
        ch_donut.set_size({"width": 480, "height": 320})
        CHART_COL = len(TABS) + 3   # place charts to the right of data grids
        ws_sum.insert_chart(0, CHART_COL, ch_donut, {"x_offset": 0, "y_offset": 0})

        # Chart 2: Age by Decade — clustered bar
        ch_decade = wb.add_chart({"type": "bar", "subtype": "clustered"})
        ch_decade.set_title({"name": "Age by Decade"})
        ch_decade.set_x_axis({"name": "Voter Count"})
        ch_decade.set_y_axis({"name": "Decade"})
        for ci, (tab_name, color, _, _) in enumerate(TABS, start=1):
            ch_decade.add_series({
                "name":       tab_name,
                "categories": [SN, DECADE_ROW_START + 1, 0,
                               DECADE_ROW_START + len(DECADES), 0],
                "values":     [SN, DECADE_ROW_START + 1, ci,
                               DECADE_ROW_START + len(DECADES), ci],
                "fill":       {"color": color},
                "gap":        50,
            })
        ch_decade.set_legend({"position": "bottom"})
        ch_decade.set_size({"width": 600, "height": 360})
        ws_sum.insert_chart(11, CHART_COL, ch_decade, {"x_offset": 0, "y_offset": 0})

        # Chart 3: Age by Generation — clustered bar
        ch_gen = wb.add_chart({"type": "bar", "subtype": "clustered"})
        ch_gen.set_title({"name": "Age by Generation"})
        ch_gen.set_x_axis({"name": "Voter Count"})
        ch_gen.set_y_axis({"name": "Generation"})
        for ci, (tab_name, color, _, _) in enumerate(TABS, start=1):
            ch_gen.add_series({
                "name":       tab_name,
                "categories": [SN, GEN_ROW_START + 1, 0,
                               GEN_ROW_START + len(GENERATIONS), 0],
                "values":     [SN, GEN_ROW_START + 1, ci,
                               GEN_ROW_START + len(GENERATIONS), ci],
                "fill":       {"color": color},
                "gap":        50,
            })
        ch_gen.set_legend({"position": "bottom"})
        ch_gen.set_size({"width": 600, "height": 320})
        ws_sum.insert_chart(32, CHART_COL, ch_gen, {"x_offset": 0, "y_offset": 0})

        # Chart 4: Party × Decade — stacked bar (7 cohorts)
        ch_pdec = wb.add_chart({"type": "bar", "subtype": "stacked"})
        ch_pdec.set_title({"name": "Party × Decade"})
        ch_pdec.set_x_axis({"name": "Voter Count"})
        ch_pdec.set_y_axis({"name": "Decade"})
        for ci, (tab_name, color, _, _) in enumerate(TABS, start=1):
            ch_pdec.add_series({
                "name":       tab_name,
                "categories": [SN, DECADE_ROW_START + 1, 0,
                               DECADE_ROW_START + len(DECADES), 0],
                "values":     [SN, DECADE_ROW_START + 1, ci,
                               DECADE_ROW_START + len(DECADES), ci],
                "fill":       {"color": color},
            })
        ch_pdec.set_legend({"position": "bottom"})
        ch_pdec.set_size({"width": 600, "height": 360})
        ws_sum.insert_chart(53, CHART_COL, ch_pdec, {"x_offset": 0, "y_offset": 0})

        # Chart 5: Party × Generation — stacked bar (7 cohorts)
        ch_pgen = wb.add_chart({"type": "bar", "subtype": "stacked"})
        ch_pgen.set_title({"name": "Party × Generation"})
        ch_pgen.set_x_axis({"name": "Voter Count"})
        ch_pgen.set_y_axis({"name": "Generation"})
        for ci, (tab_name, color, _, _) in enumerate(TABS, start=1):
            ch_pgen.add_series({
                "name":       tab_name,
                "categories": [SN, GEN_ROW_START + 1, 0,
                               GEN_ROW_START + len(GENERATIONS), 0],
                "values":     [SN, GEN_ROW_START + 1, ci,
                               GEN_ROW_START + len(GENERATIONS), ci],
                "fill":       {"color": color},
            })
        ch_pgen.set_legend({"position": "bottom"})
        ch_pgen.set_size({"width": 600, "height": 320})
        ws_sum.insert_chart(74, CHART_COL, ch_pgen, {"x_offset": 0, "y_offset": 0})

    wb.close()
    logger.info("Workbook saved → %s  (%.1f MB)",
                out_path, out_path.stat().st_size / 1_048_576)

# ── Interactive menu ──────────────────────────────────────────────────────────
def _pick_county(logger) -> tuple[str, str]:
    """Return (county_num_zero_padded, county_name)."""
    print("\n  Ohio counties (enter number 1-88):")
    for num, name in sorted(OHIO_COUNTIES.items(), key=lambda x: int(x[0])):
        print(f"    {int(num):>3}  {name}")
    while True:
        raw = input("\n  County number: ").strip()
        padded = raw.zfill(2)
        if padded in OHIO_COUNTIES:
            return padded, OHIO_COUNTIES[padded]
        print(f"  ✗ '{raw}' not recognised — try again.")

def main() -> None:
    logger = _build_logger()
    print("\n╔══════════════════════════════════════════╗")
    print("║   Ohio Voter Party Export — Excel        ║")
    print("╚══════════════════════════════════════════╝")
    print("\n  Export mode:")
    print("    1  Single precinct  →  one workbook")
    print("    2  Whole county     →  one workbook")

    while True:
        mode = input("\n  Choice [1/2]: ").strip()
        if mode in ("1", "2"):
            break
        print("  Enter 1 or 2.")

    county_num, county_name = _pick_county(logger)
    t0 = time.perf_counter()

    logger.info("Loading county %s (%s) …", county_num, county_name)
    df = _load_county(county_num, logger)
    primary_cols = [c for c in identify_cols(df) if c.startswith("PRIMARY-")]

    if mode == "1":
        precincts = _list_precincts(df)
        print(f"\n  Precincts in {county_name} ({len(precincts)} total):")
        for i, p in enumerate(precincts, 1):
            print(f"    {i:>4}  {p}")
        while True:
            raw = input("\n  Precinct number from list above: ").strip()
            try:
                idx = int(raw) - 1
                if 0 <= idx < len(precincts):
                    precinct = precincts[idx]
                    break
            except ValueError:
                pass
            print("  Enter the number shown on the left.")

        df = df.filter(pl.col("PRECINCT_NAME") == precinct)
        logger.info("Filtered to precinct '%s': %s rows", precinct, f"{len(df):,}")

        safe_precinct = precinct.replace(" ", "_").replace("/", "-")
        out_path = OUT_DIR / f"{county_name}_{safe_precinct}_voters.xlsx"
    else:
        out_path = OUT_DIR / f"{county_name}_all_voters.xlsx"

    _write_workbook(df, primary_cols, out_path, logger, precinct_mode=(mode == "1"))
    logger.info("Done in %.1f s", time.perf_counter() - t0)
    print(f"\n  ✓ Workbook written to:\n    {out_path}\n")

if __name__ == "__main__":
    main()
