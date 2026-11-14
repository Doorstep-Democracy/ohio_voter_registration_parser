"""ingest_elected_officials.py — populate serve/officials.json from electedofficials.csv.

Reads the Ohio SOS elected officials CSV (standard BoE format) and merges
TOWNSHIP, VILLAGE, LOCAL_SCHOOL_DISTRICT, CITY_SCHOOL_DISTRICT, and WARD
data into officials.json.
Existing district-level entries (CONGRESSIONAL_DISTRICT, STATE_SENATE_DISTRICT,
STATE_REPRESENTATIVE_DISTRICT) are preserved unchanged.

OFCDESC parsing extracts seat type:
  "CLAYTON CITY COUNCIL WARD 1"      -> ward=1, parquet key "CLAYTON WARD 1"
  "KETTERING CITY COUNCIL DISTRICT 2"-> ward=2, parquet key "KETTERING WARD 2"
  "HUBER HEIGHTS CITY COUNCIL AT LARGE" -> at_large
  "BUTLER TOWNSHIP TRUSTEE"          -> trustee (at-large)
  "BROOKVILLE CITY MAYOR"            -> mayor

Ward-keyed entries (keys match parquet WARD column) go into the WARD section.
At-large and mayor entries go into WARD under an "AT_LARGE" sub-key using the
pattern "{CITY} AT LARGE" and "{CITY} MAYOR" — these don't exist as parquet
WARD values but are needed so the frontend can display city-wide officeholders
alongside ward-specific reps.

Usage:
    python tools/admin/ingest_elected_officials.py                  # dry run
    python tools/admin/ingest_elected_officials.py --write          # apply changes
    python tools/admin/ingest_elected_officials.py --county 57      # filter by county in CSV
    python tools/admin/ingest_elected_officials.py --csv path/to/other.csv --write
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from pathlib import Path

ROOT           = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from pipeline.paths import DATA_DIR  # noqa: E402

DEFAULT_CSV    = ROOT / "local" / "source" / "electedofficials.csv"
OFFICIALS_JSON = ROOT / "serve" / "officials.json"
WARD_INDEX_JSON = DATA_DIR / "ward" / "index.json"

# ── DISTNAME normalization maps ───────────────────────────────────────────────

# CSV "CITY OF X" -> parquet WARD key prefix
# Parquet uses abbreviated forms in some cases.
CITY_PREFIX: dict[str, str] = {
    "CITY OF BROOKVILLE":   "BROOKVILLE",
    "CITY OF CENTERVILLE":  "CENTERVILLE",
    "CITY OF CLAYTON":      "CLAYTON",
    "CITY OF DAYTON":       "DAYTON",
    "CITY OF ENGLEWOOD":    "ENGLEWOOD",
    "CITY OF GERMANTOWN":   "GERMANTOWN",
    "CITY OF HUB HEIGHTS":  "HUBER HTS",      # parquet abbreviation
    "CITY OF KETTERING":    "KETTERING",
    "CITY OF MIAMISBURG":   "MIAMISBURG",
    "CITY OF MORAINE":      "MORAINE",
    "CITY OF OAKWOOD":      "OAKWOOD",
    "CITY OF RIVERSIDE":    "RIVERSIDE",
    "CITY OF SPRINGBORO":   "SPRINGBORO",
    "CITY OF TROTWOOD":     "TROTWOOD",
    "CITY OF UNION":        "UNION",
    "CITY OF VANDALIA":     "VANDALIA",
    "CITY OF W CARROLLTON": "W CARROLLTON",
}

# Township: CSV DISTNAME -> parquet TOWNSHIP value (exact match for Montgomery Co.)
TOWNSHIP_MAP: dict[str, str] = {
    "BUTLER TOWNSHIP":    "BUTLER TOWNSHIP",
    "CLAY TOWNSHIP":      "CLAY TOWNSHIP",
    "CLEARCREEK TOWNSHIP":"CLEARCREEK TOWNSHIP",
    "GERMAN TOWNSHIP":    "GERMAN TOWNSHIP",
    "HARRISON TOWNSHIP":  "HARRISON TOWNSHIP",
    "JACKSON TOWNSHIP":   "JACKSON TOWNSHIP",
    "JEFFERSON TOWNSHIP": "JEFFERSON TOWNSHIP",
    "MIAMI TOWNSHIP":     "MIAMI TOWNSHIP",
    "PERRY TOWNSHIP":     "PERRY TOWNSHIP",
    "WASHINGTON TOWNSHIP":"WASHINGTON TOWNSHIP",
}

# Village: CSV DISTNAME -> parquet VILLAGE value
VILLAGE_MAP: dict[str, str] = {
    "VILL OF FARMERSVILLE":  "FARMERSVILLE VILLAGE",
    "VILL 0F NEW LEBANON":   "NEW LEBANON VILLAGE",   # CSV has typo "0F"
    "VILL OF NEW LEBANON":   "NEW LEBANON VILLAGE",
    "VILL OF PHILLIPSBURG":  "PHILLIPSBURG VILLAGE",
    "VILLAGE OF VERONA":     "VERONA VILLAGE",
    "VILL OF CARLISLE":      "CARLISLE VILLAGE",       # not in parquet county 57; included for completeness
}

# School districts split into two mutually-exclusive parquet columns (handoff
# §3a). LOCAL_SCHOOL_DISTRICT and CITY_SCHOOL_DISTRICT are separate columns in
# enriched_voters.parquet; a voter is in at most one. Without the split, CSD
# board members land in LOCAL_SCHOOL_DISTRICT and the frontend join misses them.

# CSV DISTNAME -> parquet LOCAL_SCHOOL_DISTRICT value (rural/township "local" SDs)
LSD_MAP: dict[str, str] = {
    "BROOKVILLE LSD":      "BROOKVILLE LOCAL SD (MONTG)",
    "CARLISLE LSD":        "CARLISLE LOCAL SD (WARREN)",
    "JEFFERSON TWP LSD":   "JEFFERSON TOWNSHIP LOCAL SD (MONTG)",
    "MAD RIVER LSD":       "MAD RIVER LOCAL SD (MONTG)",
    "NEW LEBANON LSD":     "NEW LEBANON LOCAL SD (MONTG)",
    "NORTHRIDGE LSD":      "NORTHRIDGE LOCAL SD (MONTG)",
    "PREBLE-SHAWNEE LSD":  "PREBLE SHAWNEE LOCAL SD (PREBLE)",
    "TRI-COUNTY NORTH LSD":"TRI-COUNTY NORTH LSD (PREBLE)",
    "VALLEY VIEW LSD":     "VALLEY VIEW LOCAL SD (MONTG)",
}

# CSV DISTNAME -> parquet CITY_SCHOOL_DISTRICT value (municipal "city" SDs)
CSD_MAP: dict[str, str] = {
    "CENTERVILLE CSD":     "CENTERVILLE CITY SD",
    "DAYTON CSD":          "DAYTON CITY SD",
    "HUBER HEIGHTS CSD":   "HUBER HEIGHTS CITY SD",
    "KETTERING CSD":       "KETTERING CITY SD",
    "MIAMISBURG CSD":      "MIAMISBURG CITY SD",
    "NORTHMONT CSD":       "NORTHMONT CITY SD",
    "OAKWOOD CSD":         "OAKWOOD CITY SD",
    "SPRINGBORO CCSD":     "SPRINGBORO COMMUNITY CITY SD",
    "TROTWOOD-MADISON CSD":"TROTWOOD-MADISON CITY SD",
    "VANDALIA-BUTLER CSD": "VANDALIA-BUTLER CITY SD",
    "WEST CARROLLTON CSD": "WEST CARROLLTON CITY SD",
}
# BEAVERCREEK LSD and FAIRBORN CSD removed: they are Greene County (29), not
# Montgomery (57). Restore with county gating if expanding multi-county (§3a).

# Parquet WARD values that actually exist (ward-specific seats only, no at-large)
PARQUET_WARD_PREFIXES = {
    "CLAYTON", "DAYTON", "HUBER HTS", "KETTERING",
    "MIAMISBURG", "MORAINE", "TROTWOOD",
}

def load_ward_name_to_slug(county_scope: str = "montgomery") -> dict[str, str]:
    """WARD raw value -> canonical ward_slug, from the W1 ward index
    (data/ward/index.json), scoped to entities whose county_slugs include
    county_scope. Ward *names* are not globally unique (e.g. bare 'FIRST WARD'
    collides across 5 counties, CLAUDE.md 4), so an unscoped lookup could
    silently resolve to the wrong municipality's entity; scoping to this
    ingester's county keeps the join unambiguous. Raises if a name collides
    even within scope (would indicate a genuine entity-builder bug)."""
    if not WARD_INDEX_JSON.exists():
        return {}
    entries = json.loads(WARD_INDEX_JSON.read_text(encoding="utf-8"))
    out: dict[str, str] = {}
    for e in entries:
        if county_scope not in (e.get("county_slugs") or []):
            continue
        name = e["name"]
        if name in out and out[name] != e["slug"]:
            raise ValueError(
                f"ward name {name!r} resolves to multiple slugs within "
                f"{county_scope!r} scope: {out[name]!r} vs {e['slug']!r}"
            )
        out[name] = e["slug"]
    return out


# ── OFCDESC seat parsing ──────────────────────────────────────────────────────

_WARD_RE   = re.compile(r'\bWARD\s+(\d+)\b',     re.IGNORECASE)
_DIST_RE   = re.compile(r'\bDISTRICT\s+(\d+)\b', re.IGNORECASE)
_ATLRG_RE  = re.compile(r'\bAT[\s-]+LARGE\b',    re.IGNORECASE)
_MAYOR_RE  = re.compile(r'\bMAYOR\b',             re.IGNORECASE)
_COMMIS_RE = re.compile(r'\bCOMMISSION\b',        re.IGNORECASE)


def parse_seat(ofcdesc: str, city_prefix: str) -> tuple[str, str]:
    """Return (parquet_key, seat_label) for a CITY row.

    parquet_key: exact match to parquet WARD column if ward-specific,
                 or "{prefix} AT LARGE" / "{prefix} MAYOR" for city-wide seats.
    seat_label:  human-readable office description.
    """
    m = _WARD_RE.search(ofcdesc)
    if m:
        n = m.group(1)
        return f"{city_prefix} WARD {n}", f"Ward {n}"

    m = _DIST_RE.search(ofcdesc)
    if m:
        n = m.group(1)
        # Normalize DISTRICT -> WARD to match parquet
        return f"{city_prefix} WARD {n}", f"District {n} (Ward {n})"

    if _ATLRG_RE.search(ofcdesc) or _COMMIS_RE.search(ofcdesc):
        return f"{city_prefix} AT LARGE", "At Large"

    if _MAYOR_RE.search(ofcdesc):
        return f"{city_prefix} MAYOR", "Mayor"

    # Generic council entry with no ward designation -> treat as at-large
    return f"{city_prefix} AT LARGE", "At Large"


def make_incumbent(row: dict) -> dict:
    name_parts = [row["FIRSTN"].strip(), row["MIDDLEN"].strip(), row["LASTN"].strip()]
    if row.get("SUFFIXN", "").strip():
        name_parts.append(row["SUFFIXN"].strip())
    name = " ".join(p for p in name_parts if p)
    party = row.get("PARTY", "").strip() or None
    entry = {"name": name, "party": party}
    if party is None:
        # PARTY == "" in the SOS CSV means the office is nonpartisan by charter
        # (school board, municipal court judge) -- NOT merely "party unknown".
        # Flag it so the serve layer can tell charter-nonpartisan from missing data.
        entry["nonpartisan"] = True
    return entry


# ── Merge logic ───────────────────────────────────────────────────────────────

def build_sections(rows: list[dict]) -> dict:
    """Build WARD, TOWNSHIP, VILLAGE, LOCAL_SCHOOL_DISTRICT dicts from CSV rows."""
    ward: dict        = {}
    township: dict    = {}
    village: dict     = {}
    school: dict      = {}
    city_school: dict = {}
    unmapped: list    = []
    ward_name_to_slug = load_ward_name_to_slug()
    unmatched_ward_seats: list = []

    for row in rows:
        dtype    = row["DISTTYPE"]
        distname = row["DISTNAME"].strip()
        ofcdesc  = row["OFCDESC"].strip()

        if dtype == "TOWN":
            parquet_key = TOWNSHIP_MAP.get(distname)
            if not parquet_key:
                unmapped.append(f"TOWN/{distname}")
                continue
            office = ofcdesc  # e.g. "BUTLER TOWNSHIP TRUSTEE"
            # Multiple trustees per township -> collect into list
            if parquet_key not in township:
                township[parquet_key] = {
                    "office":    "Township Trustee",
                    "term_exp":  row.get("TERMEXP", "").strip() or None,
                    "incumbents": [],
                    "challengers": [],
                }
            township[parquet_key]["incumbents"].append(make_incumbent(row))

        elif dtype == "VILL":
            parquet_key = VILLAGE_MAP.get(distname)
            if not parquet_key:
                unmapped.append(f"VILL/{distname}")
                continue
            # Villages can have mayor + multiple council members
            if parquet_key not in village:
                village[parquet_key] = {"offices": [], "challengers": []}
            village[parquet_key]["offices"].append({
                "office":    ofcdesc,
                "term_exp":  row.get("TERMEXP", "").strip() or None,
                "incumbent": make_incumbent(row),
            })

        elif dtype == "SCHOOL":
            # Route to the correct mutually-exclusive parquet column (§3a):
            # local "LSD" districts vs. municipal "CSD" districts.
            lsd_key = LSD_MAP.get(distname)
            csd_key = CSD_MAP.get(distname)
            if lsd_key:
                target, parquet_key = school, lsd_key
            elif csd_key:
                target, parquet_key = city_school, csd_key
            else:
                unmapped.append(f"SCHOOL/{distname}")
                continue
            if parquet_key not in target:
                target[parquet_key] = {
                    "office":    "Board of Education Member",
                    "incumbents": [],
                    "challengers": [],
                }
            target[parquet_key]["incumbents"].append({
                **make_incumbent(row),
                "term_exp": row.get("TERMEXP", "").strip() or None,
            })

        elif dtype == "CITY":
            city_prefix = CITY_PREFIX.get(distname)
            if not city_prefix:
                unmapped.append(f"CITY/{distname}")
                continue
            parquet_key, seat_label = parse_seat(ofcdesc, city_prefix)
            if parquet_key not in ward:
                is_ward_specific = (city_prefix in PARQUET_WARD_PREFIXES
                                     and "AT LARGE" not in parquet_key
                                     and "MAYOR" not in parquet_key)
                ward_slug = ward_name_to_slug.get(parquet_key) if is_ward_specific else None
                if is_ward_specific and ward_slug is None:
                    unmatched_ward_seats.append(f"{ofcdesc} (DISTNAME={distname}, key={parquet_key})")
                ward[parquet_key] = {
                    "office":      ofcdesc,
                    "seat":        seat_label,
                    "parquet_match": is_ward_specific,
                    "ward_slug":   ward_slug,
                    "incumbents":  [],
                    "challengers": [],
                }
            ward[parquet_key]["incumbents"].append(make_incumbent(row))

    if unmapped:
        print(f"  WARN: {len(unmapped)} unmapped DISTNAME values: {unmapped[:10]}")
    if unmatched_ward_seats:
        print(f"  WARN: {len(unmatched_ward_seats)} ward-specific seats did not "
              f"resolve to a data/ward/index.json entity: {unmatched_ward_seats}")

    return {
        "WARD": ward,
        "TOWNSHIP": township,
        "VILLAGE": village,
        "LOCAL_SCHOOL_DISTRICT": school,
        "CITY_SCHOOL_DISTRICT": city_school,
    }


# ── Main ──────────────────────────────────────────────────────────────────────

# -- district incumbents (A5): derived from the CSV holder, not hand-entry -------

# CSV DISTTYPE -> officials.json district section. These are intentionally the
# rows ingest's CITY/TOWN/VILL/SCHOOL filter excludes; here we own them so the
# district sections stop being hand-typed (and stop being wrong -- see handoff
# section 2b, the Plummer/Huffman mislabel).
DISTRICT_SECTION = {
    "USCONG": "CONGRESSIONAL_DISTRICT",
    "SENATE": "STATE_SENATE_DISTRICT",
    "HOUSE":  "STATE_REPRESENTATIVE_DISTRICT",
}
_ORD_RE = re.compile(r"(\d+)\s*(?:ST|ND|RD|TH)?\s+DISTRICT", re.IGNORECASE)
_NAME_SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "v"}


def _display_name(s: str) -> str:
    """Title-case an ALL-CAPS CSV name with minimal special-casing."""
    out = []
    for w in s.split():
        wl = w.lower()
        if wl in {"ii", "iii", "iv", "v"}:
            out.append(w.upper())
        elif wl.startswith("mc") and len(w) > 2:
            out.append("Mc" + w[2:].capitalize())
        else:
            out.append(w.capitalize())
    return " ".join(out)


def _district_incumbent(row: dict) -> dict:
    """CSV holder as a display entry: first + last (+ suffix), middle dropped.

    The operator-canonical form is first+last (e.g. HD-37 'James Young', not the
    middle-name-laden 'James Thomas Young'); the middle name is kept only inside
    _same_person matching, where the nickname (Tom <- Thomas) lives.
    """
    bits = [row["FIRSTN"].strip(), row["LASTN"].strip()]
    if row.get("SUFFIXN", "").strip():
        bits.append(row["SUFFIXN"].strip())
    entry = {"name": _display_name(" ".join(b for b in bits if b)),
             "party": (row.get("PARTY", "").strip() or None)}
    if entry["party"] is None:
        entry["nonpartisan"] = True
    return entry


def _full_csv_name(row: dict) -> str:
    """Full CSV name incl. middle, for _same_person matching only (the display
    name drops the middle, but the middle carries the go-by name: 'Thomas'->'Tom')."""
    bits = [row["FIRSTN"], row.get("MIDDLEN", ""), row["LASTN"], row.get("SUFFIXN", "")]
    return " ".join(b.strip() for b in bits if b.strip())


def _tokens(name: str) -> list[str]:
    return [p for p in re.sub(r"[^a-z ]", " ", name.lower()).split() if p not in _NAME_SUFFIXES]


def _same_person(a: str, b: str) -> bool:
    """Same registrant heuristic: identical last name AND a shared given-name
    initial across first OR middle names. Handles nickname-vs-legal (Phil/Philip)
    and goes-by-middle-name (James Thomas Young -> 'Tom Young'). Single-county
    scope; rare same-last+shared-initial collisions are acceptable here.
    """
    pa, pb = _tokens(a), _tokens(b)
    if len(pa) < 2 or len(pb) < 2 or pa[-1] != pb[-1]:
        return False
    return bool({p[0] for p in pa[:-1]} & {p[0] for p in pb[:-1]})


def _district_office_label(section: str, key: str) -> str:
    n = int(key)
    if section == "CONGRESSIONAL_DISTRICT":
        return f"U.S. House (OH-{n})"
    if section == "STATE_SENATE_DISTRICT":
        return f"Ohio Senate (District {n})"
    return f"Ohio House (District {n})"


def build_district_sections(rows: list[dict], candidates: dict) -> dict:
    """Build the three district sections, fully derived (zero hand-entry).

    incumbent       <- the current CSV officeholder of the seat (source-stamped);
    challengers     <- candidates.json filers for the seat, minus the incumbent;
    currently_holds <- stamped on a challenger who already holds another seat
                       (the 'incumbent runs for higher office' case, e.g. Plummer
                       holds HD-39 while challenging for SD-5);
    incumbent_of_this_race <- the petition-marked filer (or null); the Stage-8
                       reconciliation gate fails if it disagrees with the CSV holder.
    """
    holders: dict = {}
    for row in rows:
        section = DISTRICT_SECTION.get(row["DISTTYPE"])
        if not section:
            continue
        m = _ORD_RE.search(row["OFCDESC"])
        if not m:
            continue
        holders[(section, f"{int(m.group(1)):02d}")] = row

    # (seat -> full CSV name) for matching the currently_holds cross-reference.
    holder_full = {sk: _full_csv_name(r) for sk, r in holders.items()}

    seats = set(holders)
    for section in DISTRICT_SECTION.values():
        for key in candidates.get(section, {}) or {}:
            seats.add((section, key))

    out: dict = {s: {} for s in DISTRICT_SECTION.values()}
    for (section, key) in sorted(seats):
        row = holders.get((section, key))
        inc = _district_incumbent(row) if row is not None else None
        inc_full = _full_csv_name(row) if row is not None else None

        filers = (candidates.get(section, {}).get(key, {}) or {}).get("candidates", [])
        marked = None
        challengers = []
        for c in filers:
            if c.get("is_incumbent"):
                marked = c
            if inc_full and _same_person(c["name"], inc_full):
                continue  # the incumbent re-filing is not their own challenger
            ch = {"name": c["name"], "party": c.get("party")}
            if c.get("nonpartisan"):
                ch["nonpartisan"] = True
            held = next((sk for sk, hn in holder_full.items()
                         if sk != (section, key) and _same_person(c["name"], hn)), None)
            if held:
                ch["currently_holds"] = {
                    "section": held[0], "key": held[1],
                    "office": _district_office_label(*held),
                }
            challengers.append(ch)

        reconciled = (marked is None) or (
            inc_full is not None and _same_person(marked["name"], inc_full)
        )
        # When the ballot filer reconciles to the CSV holder, the ballot name is
        # the public-facing name (matches Ballotpedia / the officeholder's site,
        # e.g. 'Tom Young' for legal 'James Young', 'Mike Turner' for 'Michael
        # Turner'). Display the ballot name; keep the CSV legal name on record.
        if inc and marked and reconciled and marked["name"] != inc["name"]:
            inc["legal_name"] = inc["name"]
            inc["name"] = marked["name"]
        out[section][key] = {
            "office": _district_office_label(section, key),
            "incumbent": inc,
            "incumbent_source": "electedofficials.csv" if inc else None,
            "incumbent_of_this_race": (
                {"name": marked["name"], "party": marked.get("party")} if marked else None
            ),
            "challengers": challengers,
            "reconciled": reconciled,
        }
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Populate officials.json from electedofficials.csv.")
    parser.add_argument("--csv",   type=Path, default=DEFAULT_CSV, help="Path to electedofficials.csv")
    parser.add_argument("--write", action="store_true", help="Write changes (default: dry run)")
    args = parser.parse_args()

    if not args.csv.exists():
        print(f"ERROR: CSV not found: {args.csv}", file=sys.stderr)
        sys.exit(1)

    rows = list(csv.DictReader(args.csv.open(encoding="utf-8")))
    relevant_types = {"CITY", "TOWN", "VILL", "SCHOOL"}
    relevant_rows  = [r for r in rows if r["DISTTYPE"] in relevant_types]
    print(f"CSV: {len(rows)} rows, {len(relevant_rows)} relevant (CITY/TOWN/VILL/SCHOOL)")

    new_sections = build_sections(relevant_rows)

    # Report counts
    for sec, data in new_sections.items():
        parquet_matched = sum(1 for v in data.values() if isinstance(v, dict) and v.get("parquet_match", True) and not str(list(data.keys())[0] if data else "").startswith("_"))
        print(f"  {sec}: {len(data)} entries")

    if not args.write:
        print("\nDry run. Pass --write to apply. Sample output:")
        for sec, data in new_sections.items():
            sample_key = next(iter(data), None)
            if sample_key:
                print(f"  {sec}/{sample_key!r}: {json.dumps(data[sample_key], indent=4)[:200]}")
        return

    # Load existing officials.json and merge
    existing = json.loads(OFFICIALS_JSON.read_text(encoding="utf-8"))

    # A5: replace the hand-typed district sections with CSV-derived ones.
    district_rows = [r for r in rows if r["DISTTYPE"] in DISTRICT_SECTION]
    candidates = json.loads((ROOT / "serve" / "candidates.json").read_text(encoding="utf-8"))
    for section, data in build_district_sections(district_rows, candidates).items():
        existing[section] = data
        conflicts = [k for k, v in data.items() if not v.get("reconciled")]
        if conflicts:
            print(f"  WARN: {section} marker/CSV conflict on {conflicts} -- Stage 8 will fail")

    for section, data in new_sections.items():
        if isinstance(existing.get(section), dict) and "_note" in existing[section]:
            # Replace stub
            existing[section] = data
        else:
            # Merge: new entries win, keep existing district-level data
            existing.setdefault(section, {}).update(data)

    OFFICIALS_JSON.write_text(
        json.dumps(existing, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    print(f"\nWrote {OFFICIALS_JSON} ({OFFICIALS_JSON.stat().st_size:,} bytes)")


if __name__ == "__main__":
    main()
