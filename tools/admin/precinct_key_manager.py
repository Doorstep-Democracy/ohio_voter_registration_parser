"""
precinct_key_manager.py
══════════════════════
Unified utility for sourcing and aggregating Ohio precinct keys.

This script combines the functionality of the vtrapp scraper and the Excel 
aggregator. It allows for the extraction of precinct metadata directly from 
county Board of Elections portals and the compilation of those keys into a 
single master reference workbook.

Functions:
──────────
1. SCRAPE: Extracts the "Beginning Precinct" (cmb_precfrom) dropdown from 
   Ohio county BoE vtrapp pages. 
   Source: https://lookup.boe.ohio.gov/vtrapp/{county}/vtrreport.aspx
   
   Sub-field rule: last token is a sub iff it matches ^[A-Z]$ or ^\\d+-[A-Z]$
   Everything else after the leading code is treated as part of the name.

2. AGGREGATE: Compiles all individual {county}_precincts.csv files into a 
   single multi-tab Excel workbook with formatted headers, auto-filters, 
   and frozen panes.

Output:
───────
- CSVs: ./source/precinct_keys/{county}_precincts.csv
- XLSX: ./precinct_keys_master.xlsx

Usage:
──────
    python tools/precinct_key_manager.py

Requires: curl_cffi, beautifulsoup4, xlsxwriter
"""

import csv
import random
import re
import sys
import time
import logging
from pathlib import Path

# Third-party imports
try:
    from curl_cffi import requests
    from curl_cffi.requests.exceptions import HTTPError, RequestException, Timeout
    from bs4 import BeautifulSoup
    import xlsxwriter
except ImportError as e:
    print(f"Missing dependency: {e}")
    print("Please run: pip install curl_cffi beautifulsoup4 xlsxwriter")
    sys.exit(1)

# ── Paths ─────────────────────────────────────────────────────────────────────
# BASE_DIR is the project root — script lives two levels deep in tools/admin/.
BASE_DIR = Path(__file__).resolve().parent.parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))
from pipeline.paths import county_partition_file  # noqa: E402
COUNTY_DATA_DIR = BASE_DIR / "local" / "source" / "County Data Files"
LOG_DIR  = BASE_DIR / "local" / "working"
MASTER_XLSX = COUNTY_DATA_DIR / "precinct_keys_master.xlsx"

COUNTY_DATA_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.FileHandler(COUNTY_DATA_DIR / "vtrapp_scrape.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

# ── Scraping Configuration ───────────────────────────────────────────────────

SUBSCRIBER_COUNTIES = [
    "adams",      "allen",      "ashland",    "ashtabula",  "athens",
    "auglaize",   "belmont",    "brown",      "carroll",    "champaign",
    "clark",      "clinton",    "columbiana", "coshocton",  "darke",
    "defiance",   "delaware",   "erie",       "fairfield",  "fayette",
    "fulton",     "gallia",     "geauga",     "greene",     "guernsey",
    "hardin",     "harrison",   "highland",   "hocking",    "holmes",
    "huron",      "jackson",    "jefferson",  "knox",       "lake",
    "lawrence",   "licking",    "logan",      "lorain",     "lucas",
    "madison",    "mahoning",   "marion",     "medina",     "meigs",
    "miami",      "monroe",     "montgomery", "morgan",     "muskingum",
    "noble",      "paulding",   "perry",      "pickaway",   "pike",
    "portage",    "preble",     "putnam",     "richland",   "ross",
    "scioto",     "seneca",     "shelby",     "summit",     "tuscarawas",
    "union",      "vanwert",    "vinton",     "warren",     "washington",
    "wayne",      "williams",
]

BASE_URL = "https://lookup.boe.ohio.gov/vtrapp/{county}/vtrreport.aspx"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/147.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Sec-Fetch-Site": "none",
}

DELAY_MEAN, DELAY_SIGMA = 2.5, 0.8
MAX_RETRIES = 3
_SUB_RE = re.compile(r"^(?:[A-Z]|\d+-[A-Z])$")
_NOT_SUBSCRIBED = object()

class BlockedError(Exception):
    """Raised on HTTP 403."""

# ── Scraping Logic ───────────────────────────────────────────────────────────

def human_delay():
    delay = max(1.2, min(5.0, random.gauss(DELAY_MEAN, DELAY_SIGMA)))
    time.sleep(delay)

def parse_label(text: str) -> tuple[str, str, str]:
    parts = text.strip().split()
    if not parts: return "", "", ""
    code = parts[0]
    if len(parts) == 1: return code, "", ""
    if len(parts) >= 3 and _SUB_RE.match(parts[-1]):
        return code, " ".join(parts[1:-1]), parts[-1]
    return code, " ".join(parts[1:]), ""

def fetch_county(session, county: str):
    url = BASE_URL.format(county=county)
    retries = 0
    for attempt in range(MAX_RETRIES):
        try:
            resp = session.get(url, headers=HEADERS, timeout=20)
            resp.raise_for_status()
            if HEADERS.get("Sec-Fetch-Site") == "none":
                HEADERS["Sec-Fetch-Site"] = "same-origin"
            break
        except HTTPError as exc:
            if exc.response.status_code == 403: raise BlockedError(county)
            return None, f"HTTP Error {exc.response.status_code}"
        except Exception:
            retries += 1
            if attempt + 1 < MAX_RETRIES: time.sleep(4 * (2 ** attempt))
            else: return None, f"Failed/Timeout after {MAX_RETRIES} retries"

    soup = BeautifulSoup(resp.text, "html.parser")
    if "does not subscribe" in soup.get_text().lower(): return _NOT_SUBSCRIBED, "Not Subscribed to Enhanced Web Features"

    select = soup.find("select", {"id": "cmb_precfrom"}) or soup.find("select", {"name": "cmb_precfrom"})
    if not select: return None, "No Dropdown Found on Page"

    rows = []
    for opt in select.find_all("option"):
        val = opt.get("value", "").strip()
        if not val: continue
        label = opt.get_text(strip=True)
        # Note: Following current cleanup directive, we only keep county, code, and label
        rows.append({
            "county": county,
            "precinct_code": val,
            "precinct_label": label
        })
    log.info(f"  {len(rows)} precincts")
    status_msg = "Success" if retries == 0 else f"Success (after {retries} retries)"
    return rows, status_msg

def generate_from_parquet(county: str):
    """Fallback generator for non-Triad counties: extract precinct keys directly from local Parquet data."""
    import pandas as pd
    OHIO_COUNTIES = [
        "Adams", "Allen", "Ashland", "Ashtabula", "Athens", "Auglaize", "Belmont", "Brown", 
        "Butler", "Carroll", "Champaign", "Clark", "Clermont", "Clinton", "Columbiana", "Coshocton", 
        "Crawford", "Cuyahoga", "Darke", "Defiance", "Delaware", "Erie", "Fairfield", "Fayette", 
        "Franklin", "Fulton", "Gallia", "Geauga", "Greene", "Guernsey", "Hamilton", "Hancock", 
        "Hardin", "Harrison", "Henry", "Highland", "Hocking", "Holmes", "Huron", "Jackson", 
        "Jefferson", "Knox", "Lake", "Lawrence", "Licking", "Logan", "Lorain", "Lucas", 
        "Madison", "Mahoning", "Marion", "Medina", "Meigs", "Mercer", "Miami", "Monroe", 
        "Montgomery", "Morgan", "Morrow", "Muskingum", "Noble", "Ottawa", "Paulding", "Perry", 
        "Pickaway", "Pike", "Portage", "Preble", "Putnam", "Richland", "Ross", "Sandusky", 
        "Scioto", "Seneca", "Shelby", "Stark", "Summit", "Trumbull", "Tuscarawas", "Union", 
        "Van Wert", "Vinton", "Warren", "Washington", "Wayne", "Williams", "Wood", "Wyandot"
    ]
    county_map = {c.lower().replace(" ", ""): f"{i+1:02d}" for i, c in enumerate(OHIO_COUNTIES)}
    
    county_num = county_map.get(county)
    if not county_num: return None
    
    parquet_path = county_partition_file(county_num)
    if not parquet_path.exists(): return None
    
    try:
        df = pd.read_parquet(parquet_path, columns=['PRECINCT_CODE', 'PRECINCT_NAME'])
        df = df.drop_duplicates(subset=['PRECINCT_CODE']).dropna(subset=['PRECINCT_CODE'])
        
        rows = []
        for _, row in df.iterrows():
            rows.append({
                "county": county,
                "vtrapp_web_code": "",
                "vtrapp_web_label": "",
                "PRECINCT_CODE": str(row['PRECINCT_CODE']).strip(),
                "PRECINCT_NAME": str(row['PRECINCT_NAME']).strip()
            })
        log.info(f"  {len(rows)} precincts extracted from parquet")
        return rows
    except Exception as e:
        log.error(f"Failed to read parquet for {county}: {e}")
        return None

def normalize_scraped(s):
    s = str(s).upper()
    s = re.sub(r'^\d+\s+', '', s)
    s = re.sub(r'[^A-Z0-9]', '', s)
    return s

def normalize_pq(s):
    s = str(s).upper()
    s = re.sub(r'[^A-Z0-9]', '', s)
    return s

def combine_web_and_parquet(county_str: str, web_rows: list):
    import pandas as pd
    web_df = pd.DataFrame(web_rows)
    web_df = web_df.rename(columns={'precinct_code': 'vtrapp_web_code', 'precinct_label': 'vtrapp_web_label'})
    web_df['norm_key'] = web_df['vtrapp_web_label'].apply(normalize_scraped)
    
    OHIO_COUNTIES = [
        "Adams", "Allen", "Ashland", "Ashtabula", "Athens", "Auglaize", "Belmont", "Brown", 
        "Butler", "Carroll", "Champaign", "Clark", "Clermont", "Clinton", "Columbiana", "Coshocton", 
        "Crawford", "Cuyahoga", "Darke", "Defiance", "Delaware", "Erie", "Fairfield", "Fayette", 
        "Franklin", "Fulton", "Gallia", "Geauga", "Greene", "Guernsey", "Hamilton", "Hancock", 
        "Hardin", "Harrison", "Henry", "Highland", "Hocking", "Holmes", "Huron", "Jackson", 
        "Jefferson", "Knox", "Lake", "Lawrence", "Licking", "Logan", "Lorain", "Lucas", 
        "Madison", "Mahoning", "Marion", "Medina", "Meigs", "Mercer", "Miami", "Monroe", 
        "Montgomery", "Morgan", "Morrow", "Muskingum", "Noble", "Ottawa", "Paulding", "Perry", 
        "Pickaway", "Pike", "Portage", "Preble", "Putnam", "Richland", "Ross", "Sandusky", 
        "Scioto", "Seneca", "Shelby", "Stark", "Summit", "Trumbull", "Tuscarawas", "Union", 
        "Van Wert", "Vinton", "Warren", "Washington", "Wayne", "Williams", "Wood", "Wyandot"
    ]
    county_num = {c.lower().replace(" ", ""): f"{i+1:02d}" for i, c in enumerate(OHIO_COUNTIES)}.get(county_str)
    # An unknown county leaves county_num None; keep that on the not-found branch.
    parquet_path = county_partition_file(county_num) if county_num else None
    
    if parquet_path is None or not parquet_path.exists():
        web_df['PRECINCT_CODE'] = ""
        web_df['PRECINCT_NAME'] = ""
        out_cols = ['county', 'vtrapp_web_code', 'vtrapp_web_label', 'PRECINCT_CODE', 'PRECINCT_NAME']
        return web_df[out_cols].to_dict('records')
        
    pq_df = pd.read_parquet(parquet_path, columns=['PRECINCT_CODE', 'PRECINCT_NAME'])
    pq_df = pq_df.drop_duplicates(subset=['PRECINCT_CODE']).dropna(subset=['PRECINCT_CODE'])
    pq_df['PRECINCT_CODE'] = pq_df['PRECINCT_CODE'].astype(str).str.strip()
    pq_df['PRECINCT_NAME'] = pq_df['PRECINCT_NAME'].astype(str).str.strip()
    pq_df['norm_key'] = pq_df['PRECINCT_NAME'].apply(normalize_pq)
    
    joined = pd.merge(web_df, pq_df, on='norm_key', how='outer')
    joined['county'] = county_str
    joined = joined.fillna("")
    
    out_cols = ['county', 'vtrapp_web_code', 'vtrapp_web_label', 'PRECINCT_CODE', 'PRECINCT_NAME']
    return joined[out_cols].to_dict('records')

def run_scraper():
    """Executes the CSV export logic (formerly scrape_vtrapp_precincts.py) for all 88 counties."""
    OHIO_COUNTIES = [
        "Adams", "Allen", "Ashland", "Ashtabula", "Athens", "Auglaize", "Belmont", "Brown", 
        "Butler", "Carroll", "Champaign", "Clark", "Clermont", "Clinton", "Columbiana", "Coshocton", 
        "Crawford", "Cuyahoga", "Darke", "Defiance", "Delaware", "Erie", "Fairfield", "Fayette", 
        "Franklin", "Fulton", "Gallia", "Geauga", "Greene", "Guernsey", "Hamilton", "Hancock", 
        "Hardin", "Harrison", "Henry", "Highland", "Hocking", "Holmes", "Huron", "Jackson", 
        "Jefferson", "Knox", "Lake", "Lawrence", "Licking", "Logan", "Lorain", "Lucas", 
        "Madison", "Mahoning", "Marion", "Medina", "Meigs", "Mercer", "Miami", "Monroe", 
        "Montgomery", "Morgan", "Morrow", "Muskingum", "Noble", "Ottawa", "Paulding", "Perry", 
        "Pickaway", "Pike", "Portage", "Preble", "Putnam", "Richland", "Ross", "Sandusky", 
        "Scioto", "Seneca", "Shelby", "Stark", "Summit", "Trumbull", "Tuscarawas", "Union", 
        "Van Wert", "Vinton", "Warren", "Washington", "Wayne", "Williams", "Wood", "Wyandot"
    ]
    county_map = {c.lower().replace(" ", ""): (f"{i+1:02d}", c) for i, c in enumerate(OHIO_COUNTIES)}
    ALL_COUNTIES = [c.lower().replace(" ", "") for c in OHIO_COUNTIES]
    
    triad_targets = [c for c in ALL_COUNTIES if c in SUBSCRIBER_COUNTIES]
    parquet_targets = [c for c in ALL_COUNTIES if c not in SUBSCRIBER_COUNTIES]
    
    random.shuffle(triad_targets)
    random.shuffle(parquet_targets)
    
    targets = parquet_targets + triad_targets

    if not targets:
        return True

    log.info(f"Extracting {len(targets)} counties...")
    session = requests.Session(impersonate="chrome131")
    
    status_records = []
    success_count = 0
    try:
        for i, county in enumerate(targets, 1):
            log.info(f"[{i}/{len(targets)}] {county}")
            status_msg = ""
            
            if county in SUBSCRIBER_COUNTIES:
                raw_rows, status_msg = fetch_county(session, county)
                # If cached/already fetched or currently fetching, we must merge
                if raw_rows and raw_rows is not _NOT_SUBSCRIBED:
                    rows = combine_web_and_parquet(county, raw_rows)
                else:
                    rows = raw_rows
            else:
                rows = generate_from_parquet(county)
                status_msg = "Parquet Extraction (Non-Triad)"
                
            status_records.append({"county": county, "status": status_msg})
                
            if rows and rows is not _NOT_SUBSCRIBED:
                county_num, proper_name = county_map[county]
                folder_name = f"{county_num}_{proper_name}"
                file_name = f"{county_num}_{proper_name}_precinct_key.csv"
                out_dir_path = COUNTY_DATA_DIR / folder_name
                out_dir_path.mkdir(parents=True, exist_ok=True)
                path = out_dir_path / file_name
                
                with path.open("w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=["county", "vtrapp_web_code", "vtrapp_web_label", "PRECINCT_CODE", "PRECINCT_NAME"])
                    writer.writeheader()
                    writer.writerows(rows)
                success_count += 1
                
            if i < len(targets) and county in SUBSCRIBER_COUNTIES: 
                human_delay()
    except BlockedError:
        log.error("Scrape aborted due to 403 Forbidden.")
        return False
        
    status_path = COUNTY_DATA_DIR / "scrape_status.csv"
    with status_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["county", "status"])
        writer.writeheader()
        writer.writerows(status_records)
    
    log.info(f"Extraction complete. {success_count} new files created/updated.")
    return True

# ── Aggregation Logic ────────────────────────────────────────────────────────

def run_aggregation():
    """Executes the Excel workbook logic (formerly aggregate_precinct_keys.py)"""
    csv_files = sorted(list(COUNTY_DATA_DIR.glob("*/*_precinct_key.csv")))
    if not csv_files:
        log.error("No CSV files found to aggregate.")
        return False

    log.info(f"Aggregating {len(csv_files)} files into {MASTER_XLSX.name}...")
    workbook = xlsxwriter.Workbook(str(MASTER_XLSX))
    header_fmt = workbook.add_format({'bold': True, 'bg_color': '#D7E4BC', 'border': 1})
    
    status_path = COUNTY_DATA_DIR / "scrape_status.csv"
    if status_path.exists():
        ws = workbook.add_worksheet("Scrape Status")
        try:
            with status_path.open("r", encoding="utf-8") as f:
                reader = csv.reader(f)
                for r_idx, row in enumerate(reader):
                    for c_idx, val in enumerate(row):
                        ws.write(r_idx, c_idx, val, header_fmt if r_idx == 0 else None)
            ws.set_column(0, 0, 15)
            ws.set_column(1, 1, 60)
        except Exception as e:
            log.error(f"Error indexing {status_path.name}: {e}")
    
    for csv_file in csv_files:
        sheet_name = csv_file.parent.name[:31]
        ws = workbook.add_worksheet(sheet_name)
        try:
            with csv_file.open("r", encoding="utf-8") as f:
                reader = csv.reader(f)
                for r_idx, row in enumerate(reader):
                    for c_idx, val in enumerate(row):
                        ws.write(r_idx, c_idx, val, header_fmt if r_idx == 0 else None)
            ws.autofilter(0, 0, 0, 2)
            ws.freeze_panes(1, 0)
            ws.set_column(0, 1, 15)
            ws.set_column(2, 2, 40)
        except Exception as e:
            log.error(f"Error indexing {csv_file.name}: {e}")

    workbook.close()
    log.info(f"Successfully created {MASTER_XLSX.name}")
    return True

# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("\n  Ohio Precinct Key Manager")
    print("  " + "═" * 25)
    print("  1. Export CSVs Only (Scrape)")
    print("  2. Master Workbook Only (Aggregate)")
    print("  3. Both (Scrape & Aggregate)")
    print("  Q. Quit")
    
    choice = input("\n  Select [1-3, Q]: ").strip().upper()
    
    if choice == "1":
        run_scraper()
    elif choice == "2":
        run_aggregation()
    elif choice == "3":
        if run_scraper():
            run_aggregation()
    elif choice == "Q":
        sys.exit(0)
    else:
        print("Invalid choice.")

if __name__ == "__main__":
    main()
