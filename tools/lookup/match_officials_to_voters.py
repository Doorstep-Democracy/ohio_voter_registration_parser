"""match_officials_to_voters.py — link electedofficials.csv rows to SOS voter records.

For each official in the CSV that has a home address (HZIPCODE), queries the
enriched parquet to find their voter registration record.  Uses a two-pass
strategy: exact last+first match via DuckDB, then rapidfuzz near-miss for
name variations (e.g. MAT vs MATHIAS, JD vs JAMES).  Zip code is used as a
tiebreaker when multiple records match on name.

Output: local/working/officials_voter_links.json
  Each entry includes: csv_key, name, office, sos_voterid, voter_status,
  party_affiliation, match_method, match_score, address_verified.

Notable cross-checks this surfaces:
  - Karl Keith (County Auditor + DAYTON 8-A D captain)
  - Mat Heck (County Prosecutor + DAYTON 1-B filer)
  - Tom Herner (HD-37 D challenger + WASTWP-Z filer)
  - Joshua Umbaugh (LIB captain MIAMISBURG 2-B + HD-40 L challenger)

Usage:
    python tools/lookup/match_officials_to_voters.py
    python tools/lookup/match_officials_to_voters.py --county 57     # Montgomery only
    python tools/lookup/match_officials_to_voters.py --verbose
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

ROOT       = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from pipeline.paths import ENRICHED_CACHE as PARQUET  # noqa: E402

CSV_PATH   = ROOT / "local" / "source" / "electedofficials.csv"
OUTPUT_DIR = ROOT / "local" / "working"
OUTPUT     = OUTPUT_DIR / "officials_voter_links.json"


def _load_deps() -> tuple:
    try:
        import duckdb
        from rapidfuzz import fuzz, process
        return duckdb, fuzz, process
    except ImportError as e:
        print(f"ERROR: missing dependency — {e}", file=sys.stderr)
        print("Run: uv add duckdb rapidfuzz", file=sys.stderr)
        sys.exit(1)


def _official_key(row: dict) -> str:
    return f"{row['LASTN']}, {row['FIRSTN']} ({row['OFCDESC']})"


def _full_name_from_parquet(row: dict) -> str:
    parts = [row.get("FIRST_NAME", ""), row.get("MIDDLE_NAME", ""), row.get("LAST_NAME", "")]
    return " ".join(p for p in parts if p).upper()


def match_officials(county_filter: int | None, verbose: bool) -> list[dict]:
    duckdb, fuzz, process = _load_deps()

    officials = list(csv.DictReader(CSV_PATH.open(encoding="utf-8")))
    # Only process rows that have a home zip (local officials we can locate)
    addressable = [r for r in officials if r.get("HZIPCODE", "").strip()]
    if county_filter:
        # Filter by zip codes likely to be in the target county — rough heuristic;
        # exact county filtering happens inside the parquet query below.
        pass  # zip filter not reliable; rely on parquet county_number column

    print(f"Officials CSV: {len(officials)} rows total, {len(addressable)} with home zip")

    con = duckdb.connect()

    # Pre-load county 57 (or all) voter name+address data into a temp table for speed.
    county_clause = f"AND county_number = {county_filter}" if county_filter else ""
    con.execute(f"""
        CREATE TABLE voters AS
        SELECT
            SOS_VOTERID,
            UPPER(LAST_NAME)  AS last_name,
            UPPER(FIRST_NAME) AS first_name,
            UPPER(COALESCE(MIDDLE_NAME, '')) AS middle_name,
            VOTER_STATUS,
            PARTY_AFFILIATION,
            RESIDENTIAL_ZIP,
            COUNTY_NUMBER
        FROM read_parquet('{PARQUET}')
        WHERE VOTER_STATUS IN ('ACTIVE', 'CONFIRMATION')
        {county_clause}
    """)

    results = []
    unmatched = []

    for row in addressable:
        last  = row["LASTN"].strip().upper()
        first = row["FIRSTN"].strip().upper()
        zipcode = row["HZIPCODE"].strip().zfill(5)
        office = row["OFCDESC"].strip()
        party  = row["PARTY"].strip()

        # Pass 1: exact last + first prefix (handles MAT vs MATHIAS, JD vs JAMES DAVID)
        candidates = con.execute("""
            SELECT SOS_VOTERID, last_name, first_name, middle_name,
                   VOTER_STATUS, PARTY_AFFILIATION, RESIDENTIAL_ZIPCODE, county_number
            FROM voters
            WHERE last_name = ?
              AND first_name LIKE ? || '%'
            ORDER BY RESIDENTIAL_ZIPCODE = ? DESC, last_name
            LIMIT 20
        """, [last, first[:3], zipcode]).fetchall()

        cols = ["SOS_VOTERID","last_name","first_name","middle_name",
                "VOTER_STATUS","PARTY_AFFILIATION","RESIDENTIAL_ZIPCODE","county_number"]
        candidates = [dict(zip(cols, c)) for c in candidates]

        matched     = None
        method      = None
        score       = None
        addr_verified = False

        if len(candidates) == 1:
            matched = candidates[0]
            method  = "exact_prefix"
            score   = 100
            addr_verified = (matched["RESIDENTIAL_ZIPCODE"] == zipcode)

        elif len(candidates) > 1:
            # Prefer zip-code match
            zip_hits = [c for c in candidates if c["RESIDENTIAL_ZIPCODE"] == zipcode]
            if len(zip_hits) == 1:
                matched = zip_hits[0]
                method  = "exact_prefix+zip"
                score   = 100
                addr_verified = True
            else:
                # Pass 2: rapidfuzz on full name string
                query_name = f"{first} {last}"
                choice_map = {
                    f"{c['first_name']} {c['middle_name']} {c['last_name']}".strip(): c
                    for c in (zip_hits or candidates)
                }
                best = process.extractOne(
                    query_name,
                    choice_map.keys(),
                    scorer=fuzz.token_sort_ratio,
                    score_cutoff=80,
                )
                if best:
                    matched = choice_map[best[0]]
                    method  = "fuzzy"
                    score   = best[1]
                    addr_verified = (matched["RESIDENTIAL_ZIPCODE"] == zipcode)

        if matched:
            entry = {
                "csv_key":          _official_key(row),
                "last":             last,
                "first":            first,
                "office":           office,
                "party_csv":        party,
                "sos_voterid":      matched["SOS_VOTERID"],
                "voter_status":     matched["VOTER_STATUS"],
                "party_affiliation":matched["PARTY_AFFILIATION"],
                "county_number":    matched["county_number"],
                "match_method":     method,
                "match_score":      score,
                "address_verified": addr_verified,
            }
            results.append(entry)
            if verbose:
                av = "addr-OK" if addr_verified else "addr-MISMATCH"
                print(f"  MATCH [{method}/{score}] {_official_key(row)} -> {matched['SOS_VOTERID']} [{av}]")
        else:
            unmatched.append(_official_key(row))

    print(f"\nMatched: {len(results)}/{len(addressable)}")
    if unmatched:
        print(f"Unmatched ({len(unmatched)}):")
        for u in unmatched:
            print(f"  {u}")

    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Link elected officials to SOS voter records.")
    parser.add_argument("--county",  type=int, default=57, help="County number filter (default 57)")
    parser.add_argument("--all-counties", action="store_true", help="Search all 88 counties (slow)")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    county = None if args.all_counties else args.county

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    results = match_officials(county_filter=county, verbose=args.verbose)

    OUTPUT.write_text(json.dumps(results, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\nWrote {OUTPUT} ({OUTPUT.stat().st_size:,} bytes, {len(results)} records)")


if __name__ == "__main__":
    main()
