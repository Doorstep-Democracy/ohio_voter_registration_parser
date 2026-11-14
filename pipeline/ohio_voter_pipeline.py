"""
ohio_voter_pipeline.py
─────────────────────
1. Optionally checks the Ohio SOS voter file page for updated file dates
2. Downloads any new/updated .gz archives (if scrape succeeded)
3. Decompresses them into source/State Voter Files/
4. Prompts for the next analysis step

Usage:  python ohio_voter_pipeline.py

Note: The Parquet cache is a hard requirement for single-county and
multi-county runs.  On first use, run option [1] or [2] to build it from
the raw SWVF txt files (one-time, ~4 min).  All subsequent runs load from
the cache in under 60 s.
"""

import argparse
import gzip
import json
import shutil
import sys
from datetime import date, datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ── Config ────────────────────────────────────────────────────────────────────

# Ensure pipeline/ is on sys.path (bare sibling imports) and project root
# is on sys.path (from tools.narrative import ...) regardless of invocation style.
_PIPELINE_DIR = Path(__file__).resolve().parent
for _p in [str(_PIPELINE_DIR), str(_PIPELINE_DIR.parent)]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

# Every managed location comes from the single resolver (CLAUDE.md section 5).
from paths import BASE_DIR, DATA_DIR, PARQUET_DIR, SOURCE_DIR, TXT_DIR  # noqa: E402,F401
MANIFEST   = BASE_DIR / "download_manifest.json"

# Single resolver for snapshot discovery/staging (CLAUDE.md section 5).
import snapshot_store  # noqa: E402  (needs the sys.path setup above)
import memory_budget   # noqa: E402  (memory policy for the run; see --memory-mode)

SOS_URL    = "https://www6.ohiosos.gov/ords/f?p=VOTERFTP:STWD"
SOS_BASE   = "https://www6.ohiosos.gov"

# ── Deferred narrative-module loader ────────────────────────────────────────────

def _load_generate_narratives():
    """
    Import and return the generate_narratives module.

    The import is deferred because the narrative stage is optional and pulls
    in heavy template machinery only used at run time.
    Callers needing the NON_COUNTY_LEVELS constant access it as
    `_load_generate_narratives().NON_COUNTY_LEVELS`.
    """
    from tools.narrative import generate_narratives as _gn
    return _gn


# ── Cross-county city map ───────────────────────────────────────────────────────

EARNED_UNIFORMITY_ALLOWLIST = {
    'CANAL WINCHESTER', 'CENTERVILLE', 'COLUMBUS', 'DUBLIN',
    'HUBER HEIGHTS', 'MILFORD', 'PICKERINGTON', 'REYNOLDSBURG', 'WESTERVILLE'
}

def _build_city_county_map(logger=None):
    """
    Build docs/data/city_county_map.json: { "CITY NAME": ["county_slug", ...] }.

    Cities span county lines (e.g. Kettering covers Montgomery and Greene; the
    Greene-side precincts are SUGARCREEK 151 / BEAVERCREEK 090, NOT
    KETTERING-prefixed). Each precinct index carries a per-precinct `city` field
    (from the SWVF CITY / RESIDENTIAL_CITY column); this scans every committed
    *_precinct_index.json and inverts it to city -> set(counties).

    Keyed on the uppercased city name so the dashboard can match
    prec.city.toUpperCase() directly and fan out across all listed counties.

    Reads only docs/data/ (already-written indexes); needs no source files.
    Called at the end of every branch that rebuilds precinct indexes.
    """
    import json
    data_dir = DATA_DIR
    city_counties: dict[str, set[str]] = {}
    city_identities: dict[str, dict[str, set[str]]] = {}
    
    indexes = sorted(data_dir.glob('*_precinct_index.json'))
    for idx_path in indexes:
        slug = idx_path.name[:-len('_precinct_index.json')]
        try:
            idx = json.loads(idx_path.read_text(encoding='utf-8'))
        except (OSError, ValueError) as e:
            if logger:
                logger.warning('city_county_map: skip %s (%s)', idx_path.name, e)
            continue
        for prec in idx.get('precincts', []):
            city = (prec.get('city') or '').strip().upper()
            if city:
                city_counties.setdefault(city, set()).add(slug)
                idents = city_identities.setdefault(city, {}).setdefault(slug, set())
                for f in ['local_school_district', 'exempted_vill_school_district', 'city_school_district', 'municipal_court_district']:
                    if prec.get(f):
                        import re
                        val = re.sub(r'\s*\([^)]*\)$', '', prec[f].strip().upper())
                        idents.add(f"{f}:{val}")

    out = {}
    flagged = []
    
    for city, slugs in sorted(city_counties.items()):
        slugs_list = sorted(slugs)
        if len(slugs_list) > 1 and city not in EARNED_UNIFORMITY_ALLOWLIST:
            # Earned uniformity: intersection of identity sets across these counties must be non-empty
            county_sets = [city_identities[city][s] for s in slugs_list]
            shared = set.intersection(*county_sets) if county_sets else set()
            if not shared:
                msg = f"Earned-Uniformity Guard: City '{city}' spans counties {slugs_list} but shares no school/court district. Excluding from merge."
                if logger:
                    logger.warning(msg)
                flagged.append(city)
                continue
        out[city] = slugs_list

    dest = data_dir / 'city_county_map.json'
    dest.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding='utf-8')
    if logger:
        cross = sum(1 for v in out.values() if len(v) > 1)
        logger.info('city_county_map: %d cities (%d cross-county) from %d indexes -> %s',
                    len(out), cross, len(indexes), dest.name)
    return out


SWVF_NAMES = list(snapshot_store.SWVF_GZ_NAMES)  # canonical 4-name set

REQ_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; ohio-voter-pipeline/1.0)"}
TIMEOUT     = 60


# ── Manifest ──────────────────────────────────────────────────────────────────

def load_manifest() -> dict:
    return json.loads(MANIFEST.read_text()) if MANIFEST.exists() else {}


def save_manifest(data: dict):
    MANIFEST.write_text(json.dumps(data, indent=2, default=str))


# ── Scraping ──────────────────────────────────────────────────────────────────

def scrape_download_links() -> dict[str, str]:
    """
    Fetch the SOS voter file page and return {filename: url} for the 4 SWVF gz files.
    Falls back to constructing URLs from any href containing 'SWVF'.
    """
    print(f"Fetching {SOS_URL} ...")
    try:
        resp = requests.get(SOS_URL, headers=REQ_HEADERS, timeout=TIMEOUT)
        resp.raise_for_status()
    except requests.RequestException as e:
        raise RuntimeError(f"Could not reach SOS page: {e}")

    soup  = BeautifulSoup(resp.text, "html.parser")
    links = {}

    for tag in soup.find_all("a", href=True):
        href = tag["href"]
        for name in SWVF_NAMES:
            stem = name.removesuffix(".gz")
            if name in href or stem in href:
                url = href if href.startswith("http") else SOS_BASE + href
                links[name] = url
                break
        else:
            if "SWVF" in href.upper():
                url  = href if href.startswith("http") else SOS_BASE + href
                name = Path(href.split("?")[0]).name
                if not name.endswith(".gz"):
                    name += ".gz"
                if name not in links:
                    links[name] = url

    return links


def head_last_modified(url: str) -> str:
    """Return Last-Modified header value, or 'unknown'."""
    try:
        r = requests.head(url, headers=REQ_HEADERS, timeout=TIMEOUT, allow_redirects=True)
        return r.headers.get("Last-Modified", "unknown")
    except Exception:
        return "unknown"


# ── Download & decompress ─────────────────────────────────────────────────────

def download_file(url: str, dest: Path):
    """Stream download with a simple progress counter."""
    with requests.get(url, headers=REQ_HEADERS, stream=True, timeout=TIMEOUT) as r:
        r.raise_for_status()
        total    = int(r.headers.get("content-length", 0))
        received = 0
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=2 * 1024 * 1024):
                f.write(chunk)
                received += len(chunk)
                if total:
                    pct = received / total * 100
                    print(f"\r  {dest.name}  {pct:.0f}%  ({received/1e6:.0f}/{total/1e6:.0f} MB)",
                          end="", flush=True)
    print(f"\r  OK {dest.name}  ({received/1e6:.0f} MB)                    ")


# decompress_gz now lives in snapshot_store (single resolver); re-exported
# here for any caller still importing it from this module.
from snapshot_store import decompress_gz  # noqa: E402,F401


# ── County lookup helpers ─────────────────────────────────────────────────────

def _get_ohio_counties() -> dict[str, str]:
    """Return the OHIO_COUNTIES dict from voter_data_cleaner without a full import."""
    import voter_data_cleaner as v2
    return v2.OHIO_COUNTIES   # dict[str, str]  e.g. {'01': 'Adams', ...}


def print_county_list():
    """Print all 88 Ohio counties with their official state numbers, 3-up."""
    counties = _get_ohio_counties()
    items    = sorted(counties.items(), key=lambda kv: int(kv[0]))
    cols     = 3
    col_w    = 22
    print()
    print("  Ohio counties — official SOS numbering (alphabetical, 01–88)")
    print("  " + "─" * (cols * col_w))
    for i in range(0, len(items), cols):
        row = items[i:i + cols]
        line = "  ".join(f"  {num}  {name:<{col_w - 6}}" for num, name in row)
        print(line)
    print()


def resolve_counties(raw: str) -> list[str] | None:
    """
    Parse a comma-separated string of county numbers and/or names.
    Returns a sorted list of zero-padded two-digit county number strings,
    or None if the input contained unresolvable tokens (after prompting user).

    Supports:
      - Numbers:      "57"  "057"  "57, 29"
      - Exact names:  "Montgomery"  "montgomery"
      - Partials:     "mont" → Montgomery (prompts if ambiguous)
      - Mixed:        "57, Greene, 29"
    """
    counties   = _get_ohio_counties()
    name_to_num = {v.lower(): k for k, v in counties.items()}
    resolved   = []

    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue

        # ── Numeric input ──────────────────────────────────────────────────
        if token.isdigit():
            num = token.zfill(2)
            if num in counties:
                resolved.append(num)
            else:
                print(f"  ✗ No county with number {token}. Use [L] to list all counties.")
                return None
            continue

        # ── Exact name match (case-insensitive) ────────────────────────────
        lower = token.lower()
        if lower in name_to_num:
            resolved.append(name_to_num[lower])
            continue

        # ── Partial name match ─────────────────────────────────────────────
        matches = [(num, name) for num, name in counties.items()
                   if lower in name.lower()]

        if len(matches) == 1:
            resolved.append(matches[0][0])
            print(f"  → Resolved '{token}' to {matches[0][1]} ({matches[0][0]})")
            continue

        if len(matches) == 0:
            print(f"  ✗ No county matching '{token}'. Use [L] to list all counties.")
            return None

        # Ambiguous — ask the user to pick
        print(f"\n  '{token}' matches {len(matches)} counties:")
        for i, (num, name) in enumerate(matches, 1):
            print(f"    [{i}]  {num}  {name}")
        pick = input(f"  Enter number (1–{len(matches)}): ").strip()
        if pick.isdigit() and 1 <= int(pick) <= len(matches):
            resolved.append(matches[int(pick) - 1][0])
        else:
            print("  ✗ Invalid selection. Please re-enter county.")
            return None

    return sorted(set(resolved)) if resolved else None


# ── Prompts ───────────────────────────────────────────────────────────────────

def prompt_check_for_updates() -> bool:
    """Ask whether to attempt scraping the SOS page for new voter files."""
    print()
    print("  Check Ohio SOS website for updated voter files?")
    print("  Note: the SOS site blocks automated requests with 403/CAPTCHA.")
    print("  [Y]  Yes — attempt scrape (may fail silently)")
    print("  [N]  No  — skip to analysis with files already on disk  (default; press Enter)")
    print()
    ans = input("  Check for updates? (Y/N) [N]: ").strip().upper()
    return ans == "Y"


def prompt_precinct_charts() -> bool:
    """Ask whether to also generate per-precinct chart JSON files."""
    print()
    print("  Per-precinct chart JSON (party doughnut + UNC shadow per precinct):")
    print("  [Y]  Yes — build precinct-level charts for selected counties")
    print("  [N]  No  — county-level charts only  (default; press Enter)")
    print()
    ans = input("  Include precinct charts? (Y/N) [N]: ").strip().upper()
    return ans == "Y"


def prompt_county_selection() -> list[str] | None:
    """
    Prompt the user to enter one or more counties by number or name.
    Returns a list of resolved zero-padded county number strings, or None to cancel.
    Loops until valid input is provided or the user cancels.
    """
    print()
    print("  Enter county number(s) or name(s), comma-separated.")
    print("  Examples:  57              → Montgomery only")
    print("             57, 29          → Montgomery and Greene")
    print("             Montgomery      → same as 57")
    print("             mont            → resolves to Montgomery (clarifies if ambiguous)")
    print("             57, Greene, 31  → mix of numbers and names")
    print("  [L] to list all 88 counties   [0] to cancel")
    print()

    while True:
        raw = input("  County: ").strip()
        if not raw:
            continue
        if raw.upper() == "L":
            print_county_list()
            continue
        if raw == "0":
            return None

        result = resolve_counties(raw)
        if result:
            counties = _get_ohio_counties()
            names    = [counties.get(n, n) for n in result]
            print(f"\n  Selected: {', '.join(f'{n} ({num})' for n, num in zip(names, result))}")
            confirm = input("  Proceed? (Y/N) [Y]: ").strip().upper()
            if confirm in ("", "Y"):
                return result
            # User said no — loop back
        # resolve_counties already printed the error; loop back


def prompt_next_step(txt_files: list[Path]) -> tuple[str, bool, list[str] | None]:
    """
    Returns (choice, include_precincts, county_numbers).
    county_numbers is None for full-Ohio runs, a list of strings for targeted runs.
    """
    total_rows_est = sum(f.stat().st_size for f in txt_files) // 140
    parquet_dir    = PARQUET_DIR
    parquet_ready  = parquet_dir.exists() and any(parquet_dir.iterdir())
    cache_label    = "  ✓ Parquet cache ready (fast load)" if parquet_ready else "  ○ No Parquet cache yet (will build on first run)"

    print(f"\n{'='*60}")
    print(f"  {len(txt_files)} voter file(s) ready  (~{total_rows_est:,.0f} records estimated)")
    for f in txt_files:
        print(f"    {f.name}  {f.stat().st_size/1e9:.2f} GB")
    print(f"  {cache_label}")
    print("="*60)
    print()
    print("  Next step:")
    print("  [1]  Full rebuild — counties + precincts + all jurisdictions → JSON  (default)")
    print("  [2]  Full rebuild → JSON + Excel workbook")
    print("  [3]  Counties + precincts only → JSON  (skip jurisdictional groupings: cities, townships, wards, districts)")
    print("  [4]  Jurisdictional groupings only → JSON  (cities, townships, districts, etc.)")
    print("  [5]  Selected counties only → JSON")
    print("  [L]  List all 88 counties with official state numbers")
    print("  [0]  Exit")
    print()

    while True:
        choice = input("  Choice (1/2/3/4/5/L/0) [1]: ").strip().upper() or "1"

        if choice == "L":
            print_county_list()
            continue

        if choice == "0":
            return "0", False, None

        if choice in ("1", "2"):
            # Full rebuild: county + precinct + all jurisdictions
            include_precincts = prompt_precinct_charts()
            return choice, include_precincts, None

        if choice == "3":
            # Counties + precincts only — no jurisdictional groupings
            include_precincts = prompt_precinct_charts()
            return "3", include_precincts, None

        if choice == "4":
            # Jurisdictional groupings only — all 12 types
            return "4", False, None

        if choice == "5":
            # Selected counties only
            county_nums = prompt_county_selection()
            if county_nums is None:
                # User cancelled — re-show main menu
                continue
            include_precincts = prompt_precinct_charts()
            return "5", include_precincts, county_nums

        print("  Invalid choice. Enter 1, 2, 3, 4, 5, L, or 0.")


# ── Main ──────────────────────────────────────────────────────────────────────

# -- Snapshot selection --------------------------------------------------------

def _parse_last_modified_date(lm: str) -> str | None:
    """Parse an HTTP Last-Modified header value to 'YYYY-MM-DD', or None."""
    if not lm or lm == "unknown":
        return None
    for fmt in ("%a, %d %b %Y %H:%M:%S %Z", "%a, %d %b %Y %H:%M:%S GMT"):
        try:
            return datetime.strptime(lm, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def prompt_snapshot_selection() -> snapshot_store.SnapshotInfo:
    """
    Show discovered snapshots and prompt which to process.
    Enter accepts the default (newest complete snapshot).
    """
    snaps = snapshot_store.list_snapshots()
    complete = [s for s in snaps if s.complete]
    if not complete:
        print("\n  No COMPLETE snapshot found under:")
        print(f"    {snapshot_store.SNAPSHOTS_DIR}")
        print("  Add a dated folder with the 4 SWVF_*.txt.gz files "
              "(see snapshots/README.md).")
        sys.exit(1)

    default = complete[0]  # newest complete
    print(f"\n{'='*60}")
    print("  AVAILABLE SNAPSHOTS")
    print("=" * 60)
    print(snapshot_store.format_snapshot_table(snaps))
    print()
    while True:
        raw = input(f"  Snapshot to process (# or YYYY-MM-DD) [{default.date}]: ").strip()
        if not raw:
            return default
        if raw.isdigit():
            idx = int(raw) - 1
            if 0 <= idx < len(snaps):
                pick = snaps[idx]
            else:
                print("  Invalid row number.")
                continue
        else:
            pick = next((s for s in snaps if s.date == raw), None)
            if pick is None:
                print("  No snapshot with that date.")
                continue
        if not pick.complete:
            print(f"  {pick.date} is INCOMPLETE; choose a complete snapshot.")
            continue
        return pick


def _run_update_check() -> None:
    """
    Best-effort SOS scrape + download into snapshots/<date>/ with provenance.json.
    The SOS site blocks automated requests (403/CAPTCHA); this stays best-effort
    and never raises into the caller.
    """
    manifest = load_manifest()
    try:
        links = scrape_download_links()
    except RuntimeError as e:
        print(f"  WARN: could not reach SOS page: {e}")
        print("        Continuing with snapshots already on disk.")
        return
    if not links:
        print("  WARN: no SWVF download links found (page layout may have changed).")
        print(f"        Check manually: {SOS_URL}")
        return
    print(f"  Found {len(links)} download link(s).")

    to_download: dict[str, tuple[str, str]] = {}
    for name, url in links.items():
        remote_dt = head_last_modified(url)
        cached_dt = manifest.get(name, {}).get("last_modified")
        if remote_dt == "unknown" or remote_dt != cached_dt:
            to_download[name] = (url, remote_dt)
    if not to_download:
        print("  All remote files current; nothing to download.")
        return

    # Date the snapshot by the newest parseable Last-Modified, else today.
    parsed = [d for d in (_parse_last_modified_date(dt) for _, dt in to_download.values()) if d]
    snap_date = max(parsed) if parsed else date.today().strftime("%Y-%m-%d")
    dest_dir = snapshot_store.SNAPSHOTS_DIR / snap_date
    dest_dir.mkdir(parents=True, exist_ok=True)
    print(f"\n  Downloading into {dest_dir}\n")

    sha: dict[str, str] = {}
    last_modified: dict[str, str] = {}
    for name, (url, remote_dt) in to_download.items():
        gz_path = dest_dir / name
        try:
            download_file(url, gz_path)
        except Exception as e:
            print(f"  FAIL download {name}: {e}")
            continue
        sha[name] = snapshot_store._sha256(gz_path)
        last_modified[name] = remote_dt
        manifest.setdefault(name, {}).update({
            "last_modified": remote_dt,
            "downloaded":    datetime.now().isoformat(),
            "gz_path":       str(gz_path),
        })

    prov = {
        "source_url":    SOS_URL,
        "last_modified": last_modified,
        "sha256":        sha,
        "downloaded_at": datetime.now().isoformat(timespec="seconds"),
        "downloaded_by": "ohio_voter_pipeline.scrape",
    }
    snapshot_store._atomic_write_json(dest_dir / "provenance.json", prov)
    manifest["last_run"] = datetime.now().isoformat()
    save_manifest(manifest)
    print(f"\n  Downloaded snapshot {snap_date}; provenance.json written.")


# -- Argument parsing ----------------------------------------------------------

def _positive_float(text: str) -> float:
    """argparse type: a number greater than 0."""
    value = float(text)
    if not value > 0:
        raise argparse.ArgumentTypeError(f"must be greater than 0, got {text!r}")
    return value


def _positive_int(text: str) -> int:
    """argparse type: an integer of at least 1."""
    value = int(text)
    if value < 1:
        raise argparse.ArgumentTypeError(f"must be at least 1, got {text!r}")
    return value


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="ohio_voter_pipeline.py",
        description="Ohio SWVF snapshot pipeline: discover, stage, and analyze.",
    )
    p.add_argument("--list-snapshots", action="store_true",
                   help="Print the discovered snapshot table and exit.")
    sel = p.add_mutually_exclusive_group()
    sel.add_argument("--snapshot", metavar="YYYY-MM-DD",
                     help="Process this exact snapshot (implies headless).")
    sel.add_argument("--latest", action="store_true",
                     help="Process the newest complete snapshot (implies headless).")
    p.add_argument("--choice", choices=["1", "2", "3", "4", "5"],
                   help="Menu choice to run, bypassing the interactive menu (implies headless).")
    p.add_argument("--counties", metavar="LIST",
                   help='Comma-separated county numbers, e.g. "57,29" (required with --choice 5).')
    p.add_argument("--precinct-charts", action="store_true",
                   help="Also build per-precinct chart JSON (choices 1/2/3/5).")
    p.add_argument("--no-update-check", action="store_true",
                   help="Skip the interactive SOS update-check prompt.")
    p.add_argument("--memory-mode", choices=list(memory_budget.MODES), default="auto",
                   help="auto (default): refuse a stage whose estimated peak does not fit "
                        "available memory, before loading anything. eager: always run.")
    p.add_argument("--memory-budget-gb", type=_positive_float, default=None, metavar="GB",
                   help="Use this memory budget in GB and do not measure available memory.")
    p.add_argument("--max-workers", type=_positive_int, default=0, metavar="N",
                   help="Worker threads for parallel stages. Default: logical CPUs, at most 8, "
                        "lowered if the county slices would not fit the memory budget.")
    return p.parse_args(argv)


# -- Main ----------------------------------------------------------------------

def main(args: argparse.Namespace | None = None):
    if args is None:
        args = parse_args()

    # One memory policy for the whole run, read by every stage (memory_budget.active()).
    memory_budget.activate(memory_budget.MemorySettings(
        mode=args.memory_mode, budget_gb=args.memory_budget_gb, max_workers=args.max_workers))

    # --list-snapshots: print and exit before any prompt or network call.
    if args.list_snapshots:
        print(snapshot_store.format_snapshot_table())
        return

    # Any explicit selection/choice flag implies headless: never call input().
    headless = bool(args.snapshot or args.latest or args.choice)

    if args.choice == "5" and not args.counties:
        print('  --choice 5 requires --counties "57,29".')
        sys.exit(2)

    print("=" * 60)
    print("  OHIO VOTER FILE PIPELINE")
    print("=" * 60)

    # 1 -- optional SOS update check (interactive only; never in headless).
    if not headless and not args.no_update_check and prompt_check_for_updates():
        _run_update_check()
    elif not headless:
        print("  Skipping SOS scrape -- using snapshots already on disk.")

    # 2 -- snapshot selection.
    if args.snapshot:
        snap = snapshot_store.resolve(args.snapshot)
    elif args.latest or headless:
        snap = snapshot_store.resolve("latest")
    else:
        snap = prompt_snapshot_selection()

    print(f"\n  Selected snapshot: {snap.date}")

    # 3 -- stage (decompress) the chosen snapshot into State Voter Files/.
    #      Idempotent: already-staged snapshots skip decompression.
    txt_files = sorted(snapshot_store.stage(snap))

    # 4 -- menu choice + dispatch.
    if args.choice:
        choice = args.choice
        include_precincts = args.precinct_charts
        county_nums = None
        if choice == "5":
            county_nums = resolve_counties(args.counties)
            if not county_nums:
                print(f"  Could not resolve counties from: {args.counties!r}")
                sys.exit(2)
        _dispatch_guarded(choice, txt_files, include_precincts, county_nums)
    else:
        choice, include_precincts, county_nums = prompt_next_step(txt_files)
        if choice == "0":
            print("  Exiting.")
            return
        _dispatch_guarded(choice, txt_files, include_precincts, county_nums)


def _dispatch_guarded(*args, **kwargs) -> None:
    """Run _dispatch; report a memory-budget refusal as a refusal.

    A refused run loaded nothing and wrote nothing. It ends with exit code 3
    and, in the run log, the failure marker followed by
    run_integrity.REFUSED_MARKER. The next run's previous-run check reads that
    and passes over this run, so it still checks from the last run that ran.
    Any other exception propagates to the excepthook as before.
    """
    try:
        _dispatch(*args, **kwargs)
    except memory_budget.MemoryBudgetError as exc:
        import logging
        import run_integrity
        logging.getLogger('voter_analysis').error(
            '%s: %s: %s', run_integrity.FAILED_MARKER, run_integrity.REFUSED_MARKER, exc)
        print(f"\n  RUN REFUSED. {exc}")
        sys.exit(3)


def _build_maps(logger, include_precincts: bool) -> None:
    """
    Rebuild the landing-map GeoJSON layers in docs/data/state_map/ from the
    party JSON this run just wrote. The layers bake in lean and total_voters,
    so a run that skips this leaves the map on an older snapshot (evaluation
    M5). Reads only docs/data/, docs/manifest.json and local/source/Geo/.

    The Montgomery precinct layers join the per-precinct party files, so they
    are rebuilt only when this run wrote those files; otherwise they keep
    their old stamp and the vintage gate reports them.

    Each builder runs as a subprocess, which keeps geopandas out of the
    pipeline's address space. A builder whose gates fail exits non-zero, and
    that raises here.
    """
    import os
    import subprocess
    # UTF-8 both ways and errors='replace': a stray non-ASCII byte from a
    # builder must not raise at the very end of a multi-hour run.
    env = {**os.environ, 'PYTHONIOENCODING': 'utf-8'}
    builders = [BASE_DIR / 'tools' / 'admin' / 'build_state_map.py']
    if include_precincts:
        builders.append(BASE_DIR / 'tools' / 'admin' / 'build_precinct_map.py')
    for script in builders:
        res = subprocess.run([sys.executable, str(script)], cwd=BASE_DIR, env=env,
                             capture_output=True, text=True, encoding='utf-8',
                             errors='replace')
        for line in (res.stdout + res.stderr).splitlines():
            logger.info('[maps] %s', line)
        if res.returncode != 0:
            raise RuntimeError(f'{script.name} failed (exit {res.returncode}); '
                               f'see the [maps] lines above')


def _dispatch(
    choice:            str,
    txt_files:         list[Path],
    include_precincts: bool            = False,
    county_nums:       list[str] | None = None,
):
    """Route the user's menu choice to the appropriate analysis functions.

    "1" — Full rebuild: county/precinct JSON then all 12 jurisdictional groupings,
          narratives, and the landing-map layers.
    "2" — Same as "1" plus an Excel workbook export.
    "3" — Counties + precincts only; jurisdictional groupings not run.
    "4" — Jurisdictional groupings only (all 12 types); county rebuild skipped.
    "5" — Selected counties + precincts; no jurisdictional groupings.
    """
    import voter_data_cleaner as v2

    _log     = v2.setup_logging('pipeline')
    src_date = v2.get_source_date(_log)

    # Output integrity (pipeline/run_integrity.py): log uncaught exceptions into the
    # run log, report how the previous run ended, and validate every output changed
    # since it began -- before this run writes anything.
    import time as _time
    import run_integrity
    run_start = _time.time()
    # setup_logging above claimed the run id; this call only reads it back.
    run_id = v2.claim_run_id(datetime.now().strftime('%Y%m%d_%H%M%S'))
    run_integrity.install_excepthook(_log, run_id=run_id)

    def _end_stamp() -> str:
        """Run id, UTC wall clock and elapsed seconds for the closing log line."""
        ended = datetime.now(timezone.utc).isoformat(timespec='seconds')
        return f'run id {run_id}, ended {ended} UTC, elapsed {_time.time() - run_start:.0f} s'

    current_log = Path(next(h.baseFilename for h in _log.handlers if hasattr(h, 'baseFilename')))
    startup_problems = run_integrity.check_previous_run(v2.LOGS_DIR, current_log, v2.DOCS_DIR, _log)

    if choice in ("1", "2"):
        # Step A — build county + precinct chart JSON for all 88 counties.
        v2.run_ohio_analysis(txt_files, use_parquet=True,
                             include_precinct_charts=include_precincts)

        # Narrative phase A: generate county (+ precinct) narrative JSONs
        # immediately after the county chart-JSON export completes.
        # include_precincts mirrors the user's choice from the prompt.
        _narrative_phase(
            levels=['county', 'precinct'] if include_precincts else ['county'],
            v2=v2,
            county_names=None,  # all 88 counties
            logger=_log,
        )

        # Step B — build chart JSON for all 12 jurisdictional types:
        #   cities, townships, villages, local/city/exempted school districts,
        #   state senate/rep/congressional districts, county/municipal court
        #   districts, and court of appeals.
        import jurisdictional_groupings as jg
        jg_logger, _ = jg.setup_logger()
        jg_logger.info("Running jurisdictional groupings — all 12 types...")
        jg.main(jurisdictions_to_process=None, output_format='json', logger=jg_logger)

        if choice == "2":
            # Optional Excel workbook (large — peaks at ~45 GB RSS during export).
            out = BASE_DIR / f"ohio_analysis_src{src_date}.xlsx"
            v2.run_ohio_excel(txt_files, output_path=out, use_parquet=True)

        # Narrative phase B: generate narratives for all 12 jurisdictional
        # grouping levels after jg.main() has written their chart JSON.
        # NON_COUNTY_LEVELS excludes 'county' and 'precinct' (handled above).
        _NCL = _load_generate_narratives().NON_COUNTY_LEVELS
        _narrative_phase(levels=_NCL, v2=v2, county_names=None, logger=_log)

        # Cross-county city map — inverts the freshly-written precinct indexes.
        _build_city_county_map(logger=_log)

        # Landing-map layers: last, because they bake in the county, district
        # and precinct party JSON written above and the manifest's snapshot.
        _build_maps(logger=_log, include_precincts=include_precincts)

    elif choice == "3":
        # Counties + precincts only — identical to the county pass in option 1
        # but jurisdictional_groupings.py is not invoked.
        v2.run_ohio_analysis(txt_files, use_parquet=True,
                             include_precinct_charts=include_precincts)

        # Narrative phase: county (+ precinct) only — no jurisdictional groupings.
        _narrative_phase(
            levels=['county', 'precinct'] if include_precincts else ['county'],
            v2=v2,
            county_names=None,
            logger=_log,
        )

        # Cross-county city map — inverts the freshly-written precinct indexes.
        _build_city_county_map(logger=_log)

    elif choice == "4":
        # Jurisdictional groupings only — useful when county JSON is already
        # up-to-date and only city/township/district data needs refreshing.
        import jurisdictional_groupings as jg
        logger, _ = jg.setup_logger()
        logger.info("Running jurisdictional groupings — all 12 types...")
        jg.main(jurisdictions_to_process=None, output_format='json', logger=logger)

        # Narrative phase: all non-county, non-precinct levels.
        # County/precinct narratives are not regenerated here because the
        # county chart JSON was not rebuilt in this branch.
        _NCL = _load_generate_narratives().NON_COUNTY_LEVELS
        _narrative_phase(levels=_NCL, v2=v2, county_names=None, logger=_log)

    elif choice == "5" and county_nums:
        # Targeted run for one or more counties.  Useful for spot-checking a
        # county after a classifier change without rebuilding all 88.
        v2.run_county_subset(
            txt_files,
            county_numbers=county_nums,
            use_parquet=True,
            include_precinct_charts=include_precincts,
        )

        # Narrative phase: only the selected counties (+ their precincts).
        # county_nums are zero-padded number strings; resolve to names via
        # v2.OHIO_COUNTIES (dict[str, str], e.g. {'57': 'Montgomery', ...}).
        selected_names = [
            v2.OHIO_COUNTIES[n] for n in county_nums if n in v2.OHIO_COUNTIES
        ]
        _narrative_phase(
            levels=['county', 'precinct'] if include_precincts else ['county'],
            v2=v2,
            county_names=selected_names,
            logger=_log,
        )

    # Seal: flush this run's outputs to disk and validate them, then repeat the
    # startup findings. PIPELINE COMPLETE is logged only when both are clean, so it
    # means "outputs are on disk and valid", not merely "the process got here".
    # A log without either marker ended abnormally even if the terminal looked
    # normal. Rationale: pipeline/run_integrity.py and voter_data_cleaner.setup_logging().
    sealed_bad = run_integrity.seal_outputs(v2.DOCS_DIR, run_start, _log)
    still_bad = run_integrity.recheck(startup_problems, v2.DOCS_DIR, _log)
    if sealed_bad or still_bad:
        _log.error(
            '%s: choice=%s, source date %s, %s -- %d invalid file(s) written this run, '
            '%d damaged file(s) from before this run still present (listed above)',
            run_integrity.FAILED_MARKER, choice, src_date, _end_stamp(),
            len(sealed_bad), len(still_bad),
        )
        sys.exit(1)
    _log.info('%s: choice=%s, source date %s, %s',
              run_integrity.COMPLETE_MARKER, choice, src_date, _end_stamp())

# ── Narrative phase ───────────────────────────────────────────────────────────

def _narrative_phase(
    levels: list[str],
    v2,
    county_names: list[str] | None = None,
    logger=None,
) -> None:
    """
    Generate templated prose narrative JSON files for the given levels.

    Called at the end of each _dispatch() branch after the chart-JSON export
    has completed.  Uses the same _timer + psutil RSS logging pattern as the
    rest of the pipeline so performance is visible in the run log.

    Args:
        levels:       List of LEVELS values to generate, e.g. ['county', 'precinct'].
        v2:           The pipeline.voter_data_cleaner module (already imported by _dispatch).
        county_names: Optional list of county names (proper-cased) to restrict
                      county/precinct enumeration.  None means all 88 counties.
        logger:       Logger instance.  Defaults to a pipeline logger if None.

    Design notes:
        - tools/ has no __init__.py, so we insert it onto sys.path and import
          generate_narratives directly.  The import is deferred to first call.
        - Failures are loud (raise), not silently swallowed, per
          feedback_pipeline_safety.md.  The narrative stage is non-critical
          for the voter-file analysis itself, so we log the error and continue
          rather than aborting the entire pipeline run.
        - The LLM API path is Workstream 2; only the template registry is used.
    """
    _gn = _load_generate_narratives()

    _log = logger or v2.setup_logging('narrative')
    level_label = ', '.join(levels)

    with v2._timer(_log, f'narrative generation ({level_label})'):
        ok, skipped, failed = _gn.run_for_levels(
            levels=levels,
            filter_names=county_names,
            overwrite=False,  # cache-skip: only regenerate when metrics change
        )
    _log.info(
        '[narrative] %s — ok:%d  skipped:%d  failed:%d',
        level_label, ok, skipped, failed,
    )


if __name__ == '__main__':
    main(parse_args())
