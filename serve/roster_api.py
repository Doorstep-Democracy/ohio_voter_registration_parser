#!/usr/bin/env python3
"""
roster_api.py — gated PII-roster backend (PROTOTYPE).

Serves named-voter rosters (first/last/address/city/precinct + last-voted
dates) filtered by jurisdiction and cohort. This is the *gated* counterpart to
the public aggregate dashboard: it reads PII straight from the enriched parquet
and is intended to run ONLY on localhost behind a token. Nothing it returns may
ever be written into docs/ or committed.

Design decisions (see CLAUDE.md + the prototyping discussion):

  * Reads ``local/source/parquet_enriched/enriched_voters.parquet`` — the same
    cache the weekly pipeline rebuilds. We never re-run cleaning here. The path
    is config-driven (``ROSTER_ENRICHED_CACHE``) so a hosted deploy can point at
    a different mount without a code change; the localhost default is unchanged.
  * Weekly-refresh aware: the loaded frame is cached in-process, but its source
    mtime is checked on every request. When the pipeline rewrites the parquet
    (atomic .replace), the next request transparently reloads it. No restart.
  * "Last voted primary/general" are DERIVED at load time from the 89 election
    columns (``PRIMARY-MM/DD/YYYY`` / ``GENERAL-MM/DD/YYYY``). Those columns are
    not chronologically ordered and are sparse (a value exists only if the voter
    cast that ballot), so we parse the date out of each column NAME and keep the
    max non-empty one per voter. Done once per load, not per query.
  * Auth: a single bearer token from ROSTER_TOKEN. Prototype-grade — replace
    with real session auth before this is exposed beyond localhost. The token
    is REQUIRED (startup fails loud otherwise) whenever ROSTER_HOST binds to a
    non-loopback address (e.g. a Tailnet IP) — see `_require_token_if_remote()`.
    Binding to 127.0.0.1/localhost/::1 keeps working with no token, for the
    zero-config local demo.
  * Stdlib http.server only — zero new deps. Swap for FastAPI/uvicorn when the
    gated web app is built for real.

Config (env vars; all optional, sensible localhost defaults):
    ROSTER_HOST            bind address (default 127.0.0.1)
    ROSTER_PORT            bind port (default 8000)
    ROSTER_TOKEN           bearer token; REQUIRED if ROSTER_HOST is non-loopback
    ROSTER_ENRICHED_CACHE  path to enriched_voters.parquet (default under local/)
    ROSTER_DB_PATH         path to captain.db (read by captain_db.py)

Run (local demo, no token needed):
    .venv/Scripts/python.exe serve/roster_api.py
    # then:  GET http://127.0.0.1:8000/roster?level=county&id=01&cohort=PURE_R

Run (non-loopback / Tailnet — token mandatory):
    ROSTER_HOST=100.x.y.z ROSTER_TOKEN=<strong-secret> .venv/Scripts/python.exe serve/roster_api.py
    # then:  GET http://100.x.y.z:8000/roster?level=county&id=01&cohort=PURE_R
    #        Authorization: Bearer <strong-secret>

Moving to a hosted service later is a config change only: point ROSTER_HOST at
the host's bind address, set ROSTER_TOKEN, and set ROSTER_ENRICHED_CACHE /
ROSTER_DB_PATH at the hosted paths — no code edit required.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
import threading
from datetime import date, datetime, timezone
from functools import lru_cache
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse, parse_qs

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent))  # serve/ as import root

import polars as pl

import captain_db  # SQLite write tier — captain identity, touches, walk lists

BASE_DIR = Path(__file__).resolve().parent.parent
# Config-driven: ROSTER_ENRICHED_CACHE overrides the localhost default so a
# hosted deploy can point at a different mount with zero code change.
ENRICHED_CACHE = Path(os.environ.get(
    "ROSTER_ENRICHED_CACHE",
    str(BASE_DIR / "local" / "source" / "parquet_enriched" / "enriched_voters.parquet"),
))
# As of 2026-06-21 the UI is docs/index.htm + captain/captain-mode.js — this
# API is JSON-only. (Earlier prototype served preview.html / pc.html here; those
# routes were removed when the dashboard absorbed the captain experience.)

HOST = os.environ.get("ROSTER_HOST", "127.0.0.1")
PORT = int(os.environ.get("ROSTER_PORT", "8000"))
TOKEN = os.environ.get("ROSTER_TOKEN", "")

# Loopback addresses that keep the zero-config local demo working without a
# token. Anything else (a LAN IP, a Tailnet IP, 0.0.0.0, a public hostname) is
# "serving non-locally" and the token gate below applies.
LOOPBACK_HOSTS = {"127.0.0.1", "localhost", "::1"}


def _is_loopback(host: str) -> bool:
    return host in LOOPBACK_HOSTS


def require_token_if_remote(host: str, token: str) -> None:
    """Fail loud at startup if we're about to serve real PII off a
    non-loopback interface (Tailnet, LAN, etc.) without a token configured.

    This is the re-enabled ROSTER_TOKEN gate per the seeder handoff (§6):
    the localhost demo must keep working with no token, but the moment this
    process binds somewhere reachable off-box, an unset token is a
    misconfiguration, not a degraded mode — so this is a hard exit, not a
    warning. No try/except: a missing token on a remote bind must not be
    silently tolerated.
    """
    if _is_loopback(host):
        return
    if not token:
        print(f"ERROR: ROSTER_HOST={host!r} is non-loopback but ROSTER_TOKEN is unset.")
        print("Refusing to serve real PII off localhost without auth.")
        print("Set ROSTER_TOKEN to a strong secret before binding non-locally.")
        sys.exit(1)


# Password storage: PBKDF2-HMAC-SHA256 (stdlib; OWASP-recommended iteration
# count). The stored string is self-describing ("pbkdf2_sha256$<iters>$<salt>
# $<hash>") so the iteration count can be raised later without invalidating
# existing hashes. Replaces the original unsalted single-round sha256 — swapped
# 2026-07-05 while zero accounts were activated, so no migration was needed.
_PBKDF2_ITERATIONS = 600_000


def _hash_password(password: str) -> str:
    salt = secrets.token_bytes(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, _PBKDF2_ITERATIONS)
    return f"pbkdf2_sha256${_PBKDF2_ITERATIONS}${salt.hex()}${dk.hex()}"


def _verify_password(password: str, stored: str) -> bool:
    """Constant-time check against a stored _hash_password() string. No login
    endpoint calls this yet — it exists so the future login/session work uses
    the matching verifier instead of re-hashing ad hoc."""
    parts = (stored or "").split("$")
    if len(parts) != 4 or parts[0] != "pbkdf2_sha256":
        return False
    _, iters, salt_hex, hash_hex = parts
    dk = hashlib.pbkdf2_hmac(
        "sha256", password.encode("utf-8"), bytes.fromhex(salt_hex), int(iters)
    )
    return secrets.compare_digest(dk.hex(), hash_hex)


# ── Append-only PII access log (SOC 2 CC7; runbook Sec7 item 11 / F3) ────────
# One line per request to a PII-bearing route: timestamp, token id, endpoint,
# precinct/seat scope, result. Append mode only — never read-modify-write
# (CLAUDE.md Sec8). Lives under local/ (gitignored), never near docs/.
PII_LOG_DIR = BASE_DIR / "local" / "logs"
PII_LOG_PATH = PII_LOG_DIR / "pii_access.log"


def _token_id(token: str) -> str:
    """Never log the raw bearer token — a stable short hash identifies which
    token was used without the log itself becoming a credential leak."""
    if not token:
        return "none-localhost"
    return hashlib.sha256(token.encode("utf-8")).hexdigest()[:12]


def _append_pii_log(entry: dict) -> None:
    PII_LOG_DIR.mkdir(parents=True, exist_ok=True)
    with open(PII_LOG_PATH, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry) + "\n")


# ── Precinct display-name -> DISTNAME resolver (runbook Sec7 item 7) ────────
# The manual /captain form collects a precinct DISPLAY name (e.g. "KETTERING
# 1-A"), but captain_db.upsert_seat's seat_key is built from DISTNAME (the BoE
# ballot code, e.g. "0010") — the same identity the seeder uses. Without this
# resolution step, a manually-created captain gets a SECOND seat next to the
# seeder's for the same precinct (D1 finding). Reuses officials_common.py's
# crosswalk — no new resolver logic (CLAUDE.md Sec5, single-resolver hygiene).
_crosswalk_cache: dict[str, object] = {}


def _resolve_dist_name(county_number: str, precinct_name: str) -> str | None:
    tools_admin = str((BASE_DIR / "tools" / "admin").resolve())
    if tools_admin not in sys.path:
        sys.path.insert(0, tools_admin)
    from officials_common import COUNTY_SLUG, load_precinct_crosswalk  # local import

    slug = COUNTY_SLUG.get(county_number)
    if slug is None:
        return None
    xwalk = _crosswalk_cache.get(slug)
    if xwalk is None:
        xwalk = load_precinct_crosswalk(slug)
        _crosswalk_cache[slug] = xwalk
    return xwalk.resolve_dist_name(precinct_name)

# The 9 cohort_family values that back the 9 chart slices. A roster request
# carries the cohort the user clicked; we validate against this set so a typo'd
# cohort yields an empty roster, not an unfiltered name dump. A slice missing
# here is not a cosmetic gap: the captain taps its bar and gets an empty
# roster back, so this set must track COHORT_SLICES exactly.
VALID_COHORTS = {
    "PURE_R", "UNC_LAPSED_R", "MIXED_ACTIVE", "MIXED_LAPSED", "MINOR_PARTY",
    "UNC_NONPARTISAN", "UNC_NO_PRIMARY", "UNC_LAPSED_D", "PURE_D",
}

# Ordered cohort spec: (slug, human label, chart color). Order is the
# partisan-spectrum left-to-right used by the dashboard charts. The human label
# is what a precinct captain reads; the slug never surfaces to them. Colors match
# COHORT_SLICES in pipeline/voter_data_cleaner.py so the bar a captain taps is the
# same color as the roster header they get back.
COHORT_SPEC = [
    ("PURE_R",          "Solid Republican",       "#ef4444"),
    ("UNC_LAPSED_R",    "Lapsed Republican",      "#fca5a5"),
    ("MIXED_ACTIVE",    "Mixed – Active",         "#f59e0b"),
    ("MIXED_LAPSED",    "Mixed – Lapsed",         "#a78bfa"),
    # Libertarian, Green and other non-R/D parties, by affiliation or ballot.
    ("MINOR_PARTY",     "Minor Party",            "#14b8a6"),
    # Turned out, but only ever for the non-partisan ballot (issues, levies,
    # judicial). Plain-language label: a captain needs "what did they do",
    # not the taxonomy slug.
    ("UNC_NONPARTISAN", "Issues Only (No Party Ballot)", "#c4b5cd"),
    ("UNC_NO_PRIMARY",  "No Primary History",     "#9ca3af"),
    ("UNC_LAPSED_D",    "Lapsed Democrat",        "#93c5fd"),
    ("PURE_D",          "Solid Democrat",         "#3b82f6"),
]
COHORT_LABELS = {slug: label for slug, label, _ in COHORT_SPEC}

# Jurisdiction levels the field needs. Each maps the request's ``level`` to the
# parquet column it filters on. ``county`` filters on COUNTY_NUMBER (zero-padded
# 2-digit). ``precinct`` additionally needs a county to disambiguate (precinct
# names collide across counties — see CLAUDE.md county-scoped note).
LEVEL_COLUMN = {
    "county": "COUNTY_NUMBER",
    "precinct": "PRECINCT_NAME",
    "congressional_district": "CONGRESSIONAL_DISTRICT",
    "state_senate_district": "STATE_SENATE_DISTRICT",
    "state_representative_district": "STATE_REPRESENTATIVE_DISTRICT",
    "township": "TOWNSHIP",
    "village": "VILLAGE",
    "city": "CITY",
}

# Jurisdiction columns surfaced on every roster row so a canvasser in the field
# sees all the districts a voter sits in, not just the one they filtered by.
JURISDICTION_COLS = [
    "PRECINCT_NAME", "TOWNSHIP", "VILLAGE", "WARD",
    "CONGRESSIONAL_DISTRICT", "STATE_SENATE_DISTRICT",
    "STATE_REPRESENTATIVE_DISTRICT", "LOCAL_SCHOOL_DISTRICT",
]

_ELEC_RE = re.compile(r"^(PRIMARY|GENERAL)-(\d{2})/(\d{2})/(\d{4})$")

_lock = threading.Lock()
_loaded: dict[str, object] = {"mtime": None, "df": None}


def _derive_last_voted(lf: pl.LazyFrame, cols: list[str], kind: str) -> pl.Expr:
    """Build an expr giving the most-recent date (ISO str) the voter cast a
    ``kind`` ('PRIMARY'|'GENERAL') ballot. A cell is "voted" when non-empty.

    The date lives in the column NAME, so we fold across the relevant columns
    keeping the max name-date wherever the cell is populated. Voters with no such
    ballot get null.
    """
    dated = []
    for c in cols:
        m = _ELEC_RE.match(c)
        if not m or m.group(1) != kind:
            continue
        _, mm, dd, yyyy = m.groups()
        iso = f"{yyyy}-{mm}-{dd}"
        # when this ballot was cast (cell non-empty/non-null), contribute its date
        dated.append(
            pl.when(pl.col(c).cast(pl.Utf8).str.strip_chars().str.len_chars() > 0)
              .then(pl.lit(iso))
              .otherwise(None)
        )
    if not dated:
        return pl.lit(None, dtype=pl.Utf8).alias(f"last_{kind.lower()}")
    return pl.max_horizontal(dated).alias(f"last_{kind.lower()}")


def _load(path: Path) -> pl.DataFrame:
    """Load the enriched parquet, projecting only roster-relevant columns and
    attaching derived last-voted dates. Heavy; called once per cache mtime."""
    schema_names = list(pl.scan_parquet(path).collect_schema().names())
    elec_cols = [c for c in schema_names if _ELEC_RE.match(c)]

    base_cols = [
        "SOS_VOTERID", "FIRST_NAME", "LAST_NAME",
        "RESIDENTIAL_ADDRESS1", "CITY", "RESIDENTIAL_CITY", "RESIDENTIAL_ZIP",
        "COUNTY_NUMBER", "cohort_family",
        "PRECINCT_CODE", "PARTY_AFFILIATION",
        # Generation comes pre-computed from the pipeline (BIRTHYEAR -> bucket).
        # The captain UI hits this as a secondary facet (Pure D Millennials, etc.)
        "Generation",
    ]
    keep = [c for c in dict.fromkeys(base_cols + JURISDICTION_COLS) if c in schema_names]

    lf = pl.scan_parquet(path)
    df = (
        lf.select(keep + elec_cols)
          .with_columns([
              _derive_last_voted(lf, elec_cols, "PRIMARY"),
              _derive_last_voted(lf, elec_cols, "GENERAL"),
              # CITY is blank in ~19 counties; fall back to residential city.
              # Both arrive as empty strings (not null), so normalize "" → null
              # before coalescing, or the empty CITY would shadow RESIDENTIAL_CITY.
              pl.coalesce([
                  pl.col("CITY").cast(pl.Utf8).str.strip_chars()
                    .replace("", None),
                  pl.col("RESIDENTIAL_CITY").cast(pl.Utf8).str.strip_chars()
                    .replace("", None),
              ]).alias("_city"),
          ])
          .drop(elec_cols)
          .collect()
    )
    return df


def get_df() -> pl.DataFrame:
    """Return the roster frame, reloading transparently if the weekly pipeline
    has rewritten the parquet since we last loaded it."""
    if not ENRICHED_CACHE.exists():
        raise FileNotFoundError(
            f"enriched cache missing: {ENRICHED_CACHE} — run the pipeline first"
        )
    mtime = ENRICHED_CACHE.stat().st_mtime
    with _lock:
        if _loaded["mtime"] != mtime:
            _loaded["df"] = _load(ENRICHED_CACHE)
            _loaded["mtime"] = mtime
        return _loaded["df"]  # type: ignore[return-value]


def filter_voter_ids(level: str, jid: str, cohort: str | None, county: str | None,
                     generation: str | None = None) -> list[str]:
    """Return SOS_VOTERIDs matching the given filter — the seed set for a new
    walk list. Pulled out separately from _roster_frame so the caller can avoid
    the row materialization cost (we only need ids, not addresses + dates)."""
    matched = _roster_frame(level, jid, cohort, county, generation=generation)
    if "SOS_VOTERID" not in matched.columns:
        return []
    return [str(v) for v in matched["SOS_VOTERID"].to_list() if v is not None]


def _slugify(s: str) -> str:
    """Normalize a name for slug-vs-slug comparison. Lowercases, then collapses
    any run of non-alphanumeric characters into a single underscore. This is the
    server-side mirror of the client's URL-slug generator: 'KETTERING 1-A' and
    'kettering_1_a' both become 'kettering_1_a'. The captain UI passes the slug
    from location.search straight through, so the comparison needs to be
    punctuation-agnostic on both sides — the parquet name may use hyphens,
    spaces, apostrophes, or slashes depending on the county."""
    return re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")


def _roster_frame(level: str, jid: str, cohort: str | None, county: str | None,
                  generation: str | None = None) -> pl.DataFrame:
    """Apply the jurisdiction + (cohort | generation) filter and return the
    matched frame, sorted by name. Shared by the JSON roster and the file
    exports so both see exactly the same set of voters.

    Exactly one of cohort/generation should be provided. Both is allowed
    (intersection) but the captain UI ships single-facet for this iteration.
    """
    df = get_df()
    col = LEVEL_COLUMN[level]
    flt = pl.lit(True)
    if cohort:
        flt = flt & (pl.col("cohort_family") == cohort)
    if generation and "Generation" in df.columns:
        flt = flt & (pl.col("Generation").cast(pl.Utf8) == generation)
    if level == "county":
        flt = flt & (pl.col("COUNTY_NUMBER").cast(pl.Utf8).str.zfill(2) == jid.zfill(2))
    else:
        # Slug-vs-slug match: collapse punctuation on both sides so the URL slug
        # the captain UI sends ("kettering_1_a") matches the parquet name
        # ("KETTERING 1-A"). Mirrors _slugify().
        jid_slug = _slugify(jid)
        col_slug = (
            pl.col(col).cast(pl.Utf8).str.to_lowercase()
              .str.replace_all(r"[^a-z0-9]+", "_")
              .str.strip_chars("_")
        )
        flt = flt & (col_slug == jid_slug)
        # precinct/township/village names collide across counties — scope by county
        if county is not None and "COUNTY_NUMBER" in df.columns:
            flt = flt & (pl.col("COUNTY_NUMBER").cast(pl.Utf8).str.zfill(2) == county.zfill(2))
    return df.filter(flt).sort(["LAST_NAME", "FIRST_NAME"])


# Columns exported to CSV/XLSX, in order: (frame column, header label).
EXPORT_COLUMNS = [
    ("LAST_NAME", "Last Name"),
    ("FIRST_NAME", "First Name"),
    ("RESIDENTIAL_ADDRESS1", "Address"),
    ("_city", "City"),
    ("RESIDENTIAL_ZIP", "ZIP"),
    ("last_primary", "Last Primary"),
    ("last_general", "Last General"),
    ("PRECINCT_NAME", "Precinct"),
    ("WARD", "Ward"),
    ("CONGRESSIONAL_DISTRICT", "U.S. Congress"),
    ("STATE_SENATE_DISTRICT", "State Senate"),
    ("STATE_REPRESENTATIVE_DISTRICT", "State House"),
]


def query_roster(level: str, jid: str, cohort: str | None, county: str | None,
                 limit: int, offset: int, generation: str | None = None) -> dict:
    """Filter the roster frame by jurisdiction + cohort and/or generation,
    and shape rows for the UI. At least one of cohort/generation is required —
    an unscoped name dump would be antithetical to the gated-access posture."""
    if level not in LEVEL_COLUMN:
        return {"error": f"unknown level '{level}'", "valid": sorted(LEVEL_COLUMN)}
    if not cohort and not generation:
        return {"error": "cohort or generation required"}
    if cohort and cohort not in VALID_COHORTS:
        return {"error": f"unknown cohort '{cohort}'", "valid": sorted(VALID_COHORTS)}

    matched = _roster_frame(level, jid, cohort, county, generation=generation)
    total = matched.height

    page = matched.slice(offset, limit)

    rows = []
    for r in page.iter_rows(named=True):
        rows.append({
            "sos_voterid": r.get("SOS_VOTERID"),
            "first": r.get("FIRST_NAME"),
            "last": r.get("LAST_NAME"),
            "address": r.get("RESIDENTIAL_ADDRESS1"),
            "city": r.get("_city"),
            "zip": r.get("RESIDENTIAL_ZIP"),
            "last_primary": r.get("last_primary"),
            "last_general": r.get("last_general"),
            "cohort": r.get("cohort_family"),
            "jurisdictions": {
                k.lower(): r.get(k) for k in JURISDICTION_COLS if k in page.columns
            },
        })

    return {
        "level": level, "id": jid,
        "cohort": cohort, "generation": generation, "county": county,
        "total": total, "offset": offset, "limit": limit,
        "returned": len(rows), "rows": rows,
        "generated": date.today().isoformat(),
    }


def precinct_summary(precinct: str, county: str) -> dict:
    """Per-cohort counts for one precinct, in chart order with labels + colors.

    This is the data behind the bar chart the captain taps. Returning the full
    COHORT_SPEC (even zero-count cohorts) keeps the chart shape stable across
    precincts so the captain always sees the same 7 bars in the same order.
    """
    df = get_df()
    flt = (pl.col("PRECINCT_NAME").cast(pl.Utf8) == precinct)
    if "COUNTY_NUMBER" in df.columns:
        flt = flt & (pl.col("COUNTY_NUMBER").cast(pl.Utf8).str.zfill(2) == county.zfill(2))
    p = df.filter(flt)
    counts = dict(
        p.group_by("cohort_family").agg(pl.len().alias("n"))
         .iter_rows()
    )
    bars = [
        {"cohort": slug, "label": label, "color": color, "count": int(counts.get(slug, 0))}
        for slug, label, color in COHORT_SPEC
    ]
    return {
        "precinct": precinct, "county": county,
        "total": p.height, "bars": bars,
        "generated": date.today().isoformat(),
    }


def _export_frame(level: str, jid: str, cohort: str | None, county: str | None,
                  generation: str | None = None) -> pl.DataFrame:
    """The exact rows an export contains: the full matched filter projected to
    EXPORT_COLUMNS with human headers. No pagination — exports are complete."""
    matched = _roster_frame(level, jid, cohort, county, generation=generation)
    src_cols = [c for c, _ in EXPORT_COLUMNS if c in matched.columns]
    rename = {c: lbl for c, lbl in EXPORT_COLUMNS if c in matched.columns}
    return matched.select(src_cols).rename(rename)


def export_csv(level: str, jid: str, cohort: str | None, county: str | None,
               generation: str | None = None) -> bytes:
    return _export_frame(level, jid, cohort, county, generation=generation).write_csv().encode("utf-8")


def export_xlsx(level: str, jid: str, cohort: str | None, county: str | None,
                generation: str | None = None) -> bytes:
    """Build a printable .xlsx via xlsxwriter (per CLAUDE.md, the mandated path
    for stakeholder spreadsheet output). Frozen header row, auto-width, and the
    cohort label in the sheet name so a printed walk-list is self-identifying."""
    import io
    import xlsxwriter  # local import: only needed when an xlsx is actually requested

    frame = _export_frame(level, jid, cohort, county, generation=generation)
    label = COHORT_LABELS.get(cohort, cohort) if cohort else (generation or "Roster")
    buf = io.BytesIO()
    wb = xlsxwriter.Workbook(buf, {"in_memory": True})
    # Sheet name: Excel caps at 31 chars and forbids []:*?/\
    ws = wb.add_worksheet(label[:31].replace("/", "-") or "Roster")
    hdr = wb.add_format({"bold": True, "bg_color": "#366092", "font_color": "#FFFFFF", "border": 1})
    cell = wb.add_format({"border": 1})

    headers = frame.columns
    for c, h in enumerate(headers):
        ws.write(0, c, h, hdr)
    for r, row in enumerate(frame.iter_rows(), start=1):
        for c, val in enumerate(row):
            ws.write(r, c, "" if val is None else str(val), cell)

    # Auto-width: header vs. longest value, capped so the address column stays sane.
    for c, h in enumerate(headers):
        col_vals = frame[h].cast(pl.Utf8).fill_null("")
        width = min(max(len(h), int(col_vals.str.len_chars().max() or 0)) + 2, 40)
        ws.set_column(c, c, width)
    ws.freeze_panes(1, 0)
    wb.close()
    return buf.getvalue()


class Handler(BaseHTTPRequestHandler):
    # Routes that read or write voter/captain PII -- every response through
    # these paths gets one append-only audit line (runbook Sec7 item 11/F3).
    # /health is deliberately excluded (no PII). _send() is the single funnel
    # every route returns through, so this is the one place logging has to
    # hook in (mirrors the project's esc()-as-single-funnel convention).
    _PII_ROUTE_PREFIXES = (
        "/roster", "/export", "/precinct-summary", "/captain", "/touches",
        "/walk-list", "/activate",
    )

    def _is_pii_route(self, path: str) -> bool:
        return any(path == p or path.startswith(p + "/") for p in self._PII_ROUTE_PREFIXES)

    def _send(self, code: int, payload: dict):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        # Localhost dashboard origin; tighten before any non-local deploy.
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Authorization")
        self.end_headers()
        self.wfile.write(body)
        path = urlparse(self.path).path
        if self._is_pii_route(path):
            _append_pii_log({
                "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "token_id": _token_id(TOKEN),
                "endpoint": path,
                "scope": getattr(self, "_pii_scope", None),
                "status": code,
                "result": "ok" if code < 400 else "error",
            })
            self._pii_scope = None

    def _authed(self) -> bool:
        if not TOKEN:
            # Only reachable when ROSTER_HOST is loopback: main() calls
            # require_token_if_remote() at startup and exits before
            # serve_forever() if a non-loopback bind has no token. So an
            # unset TOKEN here always means "localhost demo" -> open.
            return True
        auth = self.headers.get("Authorization", "")
        return auth == f"Bearer {TOKEN}"

    def do_OPTIONS(self):
        # CORS preflight. Browser sends OPTIONS before POST/PUT with a JSON body.
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, PUT, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Authorization, Content-Type")
        self.send_header("Access-Control-Max-Age", "600")
        self.end_headers()

    def _read_body(self) -> dict:
        """Parse JSON body for POST/PUT. Returns {} for empty bodies or anything
        unparseable — caller validates required fields itself."""
        length = int(self.headers.get("Content-Length") or "0")
        if not length:
            return {}
        raw = self.rfile.read(length)
        try:
            data = json.loads(raw.decode("utf-8"))
            return data if isinstance(data, dict) else {}
        except (json.JSONDecodeError, UnicodeDecodeError):
            return {}

    def _send_file(self, body: bytes, content_type: str, filename: str):
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)
        # Exports bypass _send()'s JSON funnel but are the most PII-dense
        # response this API returns (full named rosters) -- log them too.
        path = urlparse(self.path).path
        if self._is_pii_route(path):
            _append_pii_log({
                "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "token_id": _token_id(TOKEN),
                "endpoint": path,
                "scope": getattr(self, "_pii_scope", None),
                "status": 200,
                "result": "ok",
            })
            self._pii_scope = None

    # ── Captain-write endpoints (SQLite tier in captain_db) ──────────────
    # GET   /captain/me              — current captain or {"captain": null}
    # POST  /captain                 — create captain (name/email/phone/precinct)
    # POST  /activate                — self-service activation (v_id + PIN)
    # POST  /activate/override       — staff-assisted activation (v_id, no PIN;
    #                                   for holders with no matchable voter record)
    # GET   /touches?sos_voterid=    — touch history for one voter
    # POST  /touches                 — log a new touch
    # POST  /walk-list               — find-or-create walk list for a filter
    # GET   /walk-list/{id}/progress — counts + last-touched
    # GET   /walk-list/{id}/statuses — sos_voterid -> status map
    # PUT   /walk-list/{id}/voter/{sos}/status — set queued/done/skip
    _GET_ROUTES = {"/health", "/roster", "/precinct-summary", "/export",
                   "/captain/me", "/touches"}
    # Walk-list GET routes are dynamic (/walk-list/{id}/...), checked separately.

    def do_GET(self):
        parsed = urlparse(self.path)
        # JSON-only API. The captain UI is docs/index.htm + captain-mode.js.
        if parsed.path == "/health":
            return self._send(200, {"ok": True, "cache_exists": ENRICHED_CACHE.exists()})
        is_walk_list_get = parsed.path.startswith("/walk-list/")
        if parsed.path not in self._GET_ROUTES and not is_walk_list_get:
            return self._send(404, {"error": "not found"})
        if not self._authed():
            return self._send(401, {"error": "unauthorized"})

        q = parse_qs(parsed.query)

        if parsed.path == "/captain/me":
            return self._send(200, {"captain": captain_db.get_current_captain()})

        if parsed.path == "/touches":
            sos = q.get("sos_voterid", [""])[0]
            if not sos:
                return self._send(400, {"error": "sos_voterid required"})
            self._pii_scope = f"sos:{sos}"
            return self._send(200, {"touches": captain_db.list_touches(sos)})

        if is_walk_list_get:
            return self._handle_walk_list_get(parsed.path)

        if parsed.path == "/export":
            level = q.get("level", ["county"])[0]
            jid = q.get("id", [""])[0]
            cohort = q.get("cohort", [""])[0] or None
            generation = q.get("generation", [""])[0] or None
            county = q.get("county", [None])[0]
            fmt = q.get("format", ["csv"])[0].lower()
            if level not in LEVEL_COLUMN:
                return self._send(400, {"error": f"unknown level '{level}'"})
            if not cohort and not generation:
                return self._send(400, {"error": "cohort or generation required"})
            if cohort and cohort not in VALID_COHORTS:
                return self._send(400, {"error": f"unknown cohort '{cohort}'"})
            tag = cohort or generation or "roster"
            stem = f"{jid}_{tag}_{date.today().isoformat()}".replace(" ", "_").replace("/", "-")
            self._pii_scope = f"{level}:{jid}"
            if fmt == "xlsx":
                body = export_xlsx(level, jid, cohort, county, generation=generation)
                return self._send_file(
                    body,
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    f"{stem}.xlsx",
                )
            body = export_csv(level, jid, cohort, county, generation=generation)
            return self._send_file(body, "text/csv; charset=utf-8", f"{stem}.csv")

        if parsed.path == "/precinct-summary":
            precinct = q.get("precinct", [""])[0]
            county = q.get("county", [""])[0]
            if not precinct or not county:
                return self._send(400, {"error": "precinct and county required"})
            self._pii_scope = f"precinct:{precinct}"
            return self._send(200, precinct_summary(precinct, county))

        level = (q.get("level", ["county"])[0])
        jid = q.get("id", [""])[0]
        cohort = q.get("cohort", [""])[0] or None
        generation = q.get("generation", [""])[0] or None
        county = q.get("county", [None])[0]
        try:
            limit = min(int(q.get("limit", ["100"])[0]), 1000)
            offset = max(int(q.get("offset", ["0"])[0]), 0)
        except ValueError:
            return self._send(400, {"error": "limit/offset must be integers"})

        self._pii_scope = f"{level}:{jid}"
        result = query_roster(level, jid, cohort, county, limit, offset, generation=generation)
        code = 400 if "error" in result else 200
        self._send(code, result)

    # ── Walk-list GET routes ─────────────────────────────────────────────
    def _handle_walk_list_get(self, path: str):
        # /walk-list/{id}/progress | /walk-list/{id}/statuses | /walk-list/{id}/touches
        parts = path.strip("/").split("/")
        # parts = ["walk-list", "{id}", "<sub>"]
        if len(parts) != 3:
            return self._send(404, {"error": "not found"})
        try:
            wl_id = int(parts[1])
        except ValueError:
            return self._send(400, {"error": "walk_list_id must be integer"})
        self._pii_scope = f"walk_list:{wl_id}"
        sub = parts[2]
        if sub == "progress":
            return self._send(200, captain_db.walk_list_progress(wl_id))
        if sub == "statuses":
            return self._send(200, {"statuses": captain_db.walk_list_statuses(wl_id)})
        if sub == "touches":
            return self._send(200, {"touches": captain_db.list_touches_for_walk_list(wl_id)})
        return self._send(404, {"error": "not found"})

    # ── Captain-write POSTs ──────────────────────────────────────────────
    def do_POST(self):
        parsed = urlparse(self.path)
        if not self._authed():
            return self._send(401, {"error": "unauthorized"})
        body = self._read_body()

        if parsed.path == "/activate":
            # v_id is the SEAT's opaque v_id (opaque_hash of county|dist_name|
            # party -- captain_schema_v2.md S1), from the QR / activation link.
            # It is NOT a SOS_VOTERID: that anchor was superseded because it
            # rotates on holder change, orphaning the account every rotation.
            v_id = body.get("v_id", "")
            pin = body.get("pin", "")
            password = body.get("new_password", "")
            if not v_id or not pin or not password:
                return self._send(400, {"error": "v_id, pin, and new_password are required"})
            self._pii_scope = f"v_id:{v_id}"

            seat = captain_db.get_seat_by_v_id(v_id)
            if seat is None:
                return self._send(404, {"error": "Seat not found"})
            holder = captain_db.get_active_holder(seat["seat_id"])
            if holder is None:
                return self._send(404, {"error": "No holder on record for this seat"})
            # PIN verification: SOS_VOTERID last-4 (schema S6 "PIN / SOS last-4").
            # Phone-based PIN is not available until the PII sheet is supplied
            # (HANDOFF S3 interim path -- phone is null for all v1 captains).
            if not holder.get("sos_voterid") or pin != str(holder["sos_voterid"])[-4:]:
                return self._send(400, {"error": "PIN does not match this seat's holder on record"})
            if holder.get("password_hash"):
                return self._send(400, {"error": "Account already activated"})

            pw_hash = _hash_password(password)
            try:
                captain_db.attach_login(holder_term_id=holder["holder_term_id"], password_hash=pw_hash)
            except ValueError as e:
                return self._send(400, {"error": str(e)})
            captain_db.set_seat_status(seat["seat_id"], "filled")

            return self._send(201, {
                "message": "Activated successfully",
                "captain": captain_db.get_captain_view(seat["seat_id"]),
                # Random per-activation token. NOTE: nothing validates it
                # server-side yet — real session auth is still the hosted-tier
                # task — but a hardcoded literal credential must never ship.
                "token": secrets.token_urlsafe(24),
            })

        if parsed.path == "/activate/override":
            # Staff-assisted activation: the captain calls the office, a human
            # verifies them conversationally, staff issues the password
            # directly. No `pin` field -- this is NOT the self-service path,
            # so it does not depend on holder_term.sos_voterid being set (the
            # gap the plan's Part 4 exists to close: newly-seated appointees /
            # NCOA-mover captains with no matchable voter record still get an
            # activation route). Same auth gate as every other do_POST route
            # (self._authed() above); the "staff" trust boundary today is
            # "whoever holds ROSTER_TOKEN" -- a real staff-role check is
            # session-auth-tier work (runbook Sec7 item 4), out of scope here.
            v_id = body.get("v_id", "")
            password = body.get("new_password", "")
            if not v_id or not password:
                return self._send(400, {"error": "v_id and new_password are required"})
            self._pii_scope = f"v_id:{v_id}"

            seat = captain_db.get_seat_by_v_id(v_id)
            if seat is None:
                return self._send(404, {"error": "Seat not found"})
            holder = captain_db.get_active_holder(seat["seat_id"])
            if holder is None:
                return self._send(404, {"error": "No holder on record for this seat"})

            pw_hash = _hash_password(password)
            try:
                # attach_login's own "already activated" guard gives us
                # first-activation-only, no-reset behavior for free (the
                # scope the plan/runbook B3-ii/operator decisions settled on).
                captain_db.attach_login(holder_term_id=holder["holder_term_id"], password_hash=pw_hash)
            except ValueError as e:
                return self._send(400, {"error": str(e)})
            captain_db.set_seat_status(seat["seat_id"], "filled")

            return self._send(201, {
                "message": "Activated successfully (staff override)",
                "captain": captain_db.get_captain_view(seat["seat_id"]),
                "token": secrets.token_urlsafe(24),
            })

        if parsed.path == "/captain":
            display_name = body.get("display_name", "")
            email = body.get("email", "")
            phone = body.get("phone", "")
            precinct_county = body.get("precinct_county", "")
            precinct_name = body.get("precinct_name", "")
            for label, val in [("display_name", display_name), ("email", email),
                               ("phone", phone), ("precinct_county", precinct_county),
                               ("precinct_name", precinct_name)]:
                if not val or not str(val).strip():
                    return self._send(400, {"error": f"{label} required"})
            self._pii_scope = f"{precinct_county.strip()}:{precinct_name.strip()}"
            # Reduce the human-typed display name to the seeder's DISTNAME
            # code before keying the seat -- upsert_seat's seat_key is
            # (county, dist_name, party); passing the display name straight
            # through here created a SECOND seat next to the seeder's for the
            # same precinct (D1 finding, runbook Sec7 item 7).
            dist_name = _resolve_dist_name(precinct_county.strip(), precinct_name.strip())
            if dist_name is None:
                return self._send(400, {
                    "error": f"could not resolve precinct '{precinct_name}' in county "
                             f"'{precinct_county}' to a DISTNAME code",
                })
            seat = captain_db.upsert_seat(
                county_number=precinct_county.strip(), dist_name=dist_name,
                party="D", display_name=precinct_name.strip(), status="filled",
            )
            captain_db.start_holder_term(
                seat_id=seat["seat_id"], display_name=display_name.strip(),
                email=email.strip(), phone=phone.strip(), origin="appointed",
                claimed=True,
            )
            return self._send(201, {"captain": captain_db.get_captain_view(seat["seat_id"])})

        if parsed.path == "/touches":
            try:
                seat_id = int(body.get("captain_id", 0))
            except (ValueError, TypeError):
                return self._send(400, {"error": "captain_id required"})
            holder = captain_db.get_active_holder(seat_id)
            if holder is None:
                return self._send(400, {"error": "no active holder for this seat"})
            self._pii_scope = f"sos:{body.get('sos_voterid', '')}"
            try:
                t = captain_db.log_touch(
                    sos_voterid=body.get("sos_voterid", ""),
                    seat_id=seat_id,
                    holder_term_id=holder["holder_term_id"],
                    precinct_county=body.get("precinct_county", ""),
                    precinct_name=body.get("precinct_name", ""),
                    kind=body.get("kind", ""),
                    outcome=body.get("outcome") or None,
                    notes=body.get("notes") or None,
                )
            except (ValueError, TypeError) as e:
                return self._send(400, {"error": str(e)})
            return self._send(201, {"touch": t})

        if parsed.path == "/walk-list":
            # The body carries the filter the captain just clicked. We resolve
            # the seed voter set against the parquet (same filter the /roster
            # endpoint uses), then find-or-create the SQLite walk list.
            try:
                seat_id = int(body.get("captain_id", 0))
            except (ValueError, TypeError):
                return self._send(400, {"error": "captain_id required"})
            precinct = body.get("precinct_name", "")
            county = body.get("precinct_county", "")
            cohort = body.get("cohort") or None
            generation = body.get("generation") or None
            if not precinct or not county:
                return self._send(400, {"error": "precinct_name and precinct_county required"})
            if not cohort and not generation:
                return self._send(400, {"error": "cohort or generation required"})
            self._pii_scope = f"{county}:{precinct}"
            # Compose filter_kind/value/label so re-clicks are idempotent.
            if cohort and generation:
                fk, fv = "cohort_generation", f"{cohort}|{generation}"
                label = body.get("filter_label") or f"{cohort} {generation}"
            elif cohort:
                fk, fv = "cohort", cohort
                label = body.get("filter_label") or cohort
            else:
                fk, fv = "generation", generation  # type: ignore[assignment]
                label = body.get("filter_label") or generation  # type: ignore[assignment]
            holder = captain_db.get_active_holder(seat_id)
            try:
                seeds = filter_voter_ids("precinct", precinct, cohort, county, generation=generation)
                wl = captain_db.find_or_create_walk_list(
                    seat_id=seat_id,
                    created_by_holder_term=holder["holder_term_id"] if holder else None,
                    precinct_county=county, precinct_name=precinct,
                    filter_kind=fk, filter_value=fv, filter_label=label,
                    seed_voter_ids=seeds,
                )
            except (ValueError, TypeError) as e:
                return self._send(400, {"error": str(e)})
            return self._send(200, {"walk_list": wl, "seeded": len(seeds)})

        return self._send(404, {"error": "not found"})

    # ── Walk-status PUT ──────────────────────────────────────────────────
    def do_PUT(self):
        parsed = urlparse(self.path)
        if not self._authed():
            return self._send(401, {"error": "unauthorized"})
        # /walk-list/{id}/voter/{sos_voterid}/status
        parts = parsed.path.strip("/").split("/")
        if len(parts) == 5 and parts[0] == "walk-list" and parts[2] == "voter" and parts[4] == "status":
            try:
                wl_id = int(parts[1])
            except ValueError:
                return self._send(400, {"error": "walk_list_id must be integer"})
            sos = parts[3]
            body = self._read_body()
            status = body.get("status", "")
            self._pii_scope = f"walk_list:{wl_id}:sos:{sos}"
            try:
                captain_db.set_walk_status(walk_list_id=wl_id, sos_voterid=sos, status=status)
            except ValueError as e:
                return self._send(400, {"error": str(e)})
            return self._send(200, {"ok": True})
        return self._send(404, {"error": "not found"})

    def log_message(self, fmt, *args):  # quieter console
        pass


def main():
    if not ENRICHED_CACHE.exists():
        print("ERROR: enriched parquet not found at")
        print(f"  {ENRICHED_CACHE}")
        print()
        print("Run the pipeline first:")
        print("  python pipeline/ohio_voter_pipeline.py")
        print("Select option [1] when prompted (full Ohio -> dashboard JSON).")
        sys.exit(1)
    # Loud gate: a non-loopback bind (Tailnet/LAN) with no token is a hard
    # exit, not a warning — see require_token_if_remote() docstring.
    require_token_if_remote(HOST, TOKEN)
    if not TOKEN:
        print("WARNING: ROSTER_TOKEN unset - API is OPEN. Localhost-only use.")
    print(f"roster_api -> http://{HOST}:{PORT}  (cache: {ENRICHED_CACHE})")
    print("  GET /health")
    print("  GET /roster?level=county&id=01&cohort=PURE_R&limit=50")
    ThreadingHTTPServer((HOST, PORT), Handler).serve_forever()


if __name__ == "__main__":
    main()
