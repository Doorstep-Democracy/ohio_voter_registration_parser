# Tools Directory

This directory contains standalone utilities, maintenance scripts, and specialized data exporters for the Ohio Voter Registration project. These tools are separate from the core pipeline logic but provide essential functionality for data acquisition, verification, and stakeholder delivery.

Several subfolders have grown large enough to carry their own per-script README — this file gives the map and the highlights; follow the links for full detail (inputs/outputs/usage for every script).

## 🛠 Unified Management

### `precinct_key_manager.py`
*   **Purpose:** The central utility for sourcing and aggregating Ohio precinct keys.
*   **Function:** Combines scraping and aggregation into a single interface. 
    *   **Scrape Mode:** Extracts "Beginning Precinct" dropdowns from BoE `vtrapp` portals using `curl_cffi` and `BeautifulSoup`.
    *   **Aggregate Mode:** Compiles all county CSVs into a master Excel workbook with formatted headers and frozen panes.
*   **Menu Options:** 1. Export CSVs only, 2. Workbook only, 3. Both.
*   **Output:** CSVs in `local/source/precinct_keys/` and `precinct_keys_master.xlsx` in the root.

## 🧹 Maintenance & Cleaning

### `clean_precinct_keys.py`
*   **Purpose:** A surgical utility to clean redundant columns from the scraped precinct CSVs.
*   **Function:** Removes the `name` and `sub` columns and preserves the `precinct_label` as the source of truth across all 88 counties.

## 🗳️ `admin/` — Officials, Captains & Candidates Pipeline

The staged build (Stages 3 → 8) that turns BoE filings and the SOS elected-officials CSV into the `serve/*.json` files the captain dashboard reads, plus the schema/jurisdiction validation gates and the precinct-key scraper. 18 scripts — full breakdown in **[`admin/README.md`](admin/README.md)**. For the officials/captains/candidates chain specifically, **[`admin/OFFICIALS_PIPELINE_GUIDE.md`](admin/OFFICIALS_PIPELINE_GUIDE.md)** is the plain-language run-order guide.

Highlights:
*   **`match_to_voters.py`** — the single voter-matching resolver (`match_entity`) every other officials/captains script reuses rather than reimplementing.
*   **`validate_officials.py`** — Stage 8 integrity gate across all `serve/*.json`; run after any producer/consumer change.
*   **`seed_quorum_registry.py`** — seeds the precinct-captain seat/holder registry for the quorum app via a per-county adapter seam.
*   **`admin/validate_jurisdiction_fields.py`** — data-validation gate for the municipality fields in a new SWVF drop. Confirms the per-county field coverage that the pipeline's city resolver (`_dominant_city_per_precinct`) depends on, and flags precincts a postal-city fallback would mislabel.
    *   **Why it exists:** The Ohio SWVF ([official layout](https://www6.ohiosos.gov/ords/f?p=111:2)) carries two unrelated location families — authoritative jurisdiction (`CITY`/`VILLAGE`/`WARD`/`TOWNSHIP`) and postal address (`RESIDENTIAL_CITY`). Backfilling a blank `CITY` from the postal column mislabels township precincts with their post-office city (e.g. `WASHINGTON TWP F` → `KETTERING`). The resolver instead walks a hierarchy: `CITY → VILLAGE → WARD-prefix → TOWNSHIP (= not a city) → RESIDENTIAL_CITY` (last resort).
    *   **Reports:** (1) per-county CITY coverage; (2) hierarchy-coverage table for blank-CITY counties showing which fallback column covers them, and the *true* postal-last-resort set (counties with no authoritative column — currently only Wyandot); (3) township/village precincts a postal fallback would mislabel.
    *   **Usage:** `python tools/admin/validate_jurisdiction_fields.py [--strict] [--show N]`. `--strict` exits non-zero on HIGH-severity mislabels so it can gate a pipeline run in CI.
*   **`validate_schema.py`** *(renamed 2026-07 from `dump_schema.py`, to match the `validate_*` naming of the other two gates below)* — generates the structural inventory blocks inside `schema/*.md` docs; `--check` gates on drift.
*   **`run_city_groupings.py`** / **`regen_city_summary.py`** — direct runners that regenerate city-level chart JSON in `docs/data/` without going through the full pipeline menu.

## 📊 `export/` — Stakeholder Deliverables

Excel workbooks, target-list CSVs, and cloud uploads for human consumption — reads raw voter-file records (public record, not PII by itself — see the terminology note in `export/README.md`), writes only to gitignored `local/exports/`. Full detail in **[`export/README.md`](export/README.md)**.

*   **`precinct_party_export.py`** — 7-tab partisan-cohort Excel workbook (with embedded charts in precinct mode), for a chosen county or single precinct. Output: `local/exports/Workbooks/{County}_{Precinct}_voters.xlsx` or `{County}_all_voters.xlsx`.
*   **`precinct_unc_export.py`** — D + UNC Lifetime-D voter counts per precinct across all 88 counties, plus a statewide rollup. Output: `local/exports/Precinct_Summary/`.
*   **`export_unc_targets.py`** — prototype target-list exporter (UNC lifetime cohorts + current D/R + full cohort taxonomy) straight from raw SWVF files.
*   **`upload_to_gdrive.py`** — one-off OAuth2 uploader that pushes `docs/cohort_definitions.md` to Google Drive as a native Doc.

## 🔍 `lookup/` — Find One Voter

Read-only, ad hoc search and cross-reference tools — the "find one voter" toolbox. Full detail in **[`lookup/README.md`](lookup/README.md)**.

*   **`voter_lookup_parquet.py`** *(renamed 2026-07 from `voter_lookup.py`, to name the storage layer it searches and pair with `voter_lookup_source.py` below)* — global, case-insensitive substring search across every column of the enriched Parquet cache, all counties. Writes matches to a timestamped CSV. **This is the one to reach for when you need to find a specific voter.**
    *   **Usage:** `python tools/lookup/voter_lookup_parquet.py "SEARCH TERM"` (prompts interactively if omitted).
*   **`voter_lookup_source.py`** *(renamed 2026-07 from `raw_voter_lookup.py`)* — the same global search, but against the raw `SWVF_*.txt` source files instead of the cleaned Parquet — useful for checking whether a record was dropped or altered somewhere in pipeline processing.
*   **`match_officials_to_voters.py`** — links `electedofficials.csv` rows (that carry a home zip) to their voter record via DuckDB + rapidfuzz. An earlier, narrower-scoped sibling of `admin/match_to_voters.py`'s single resolver.

## 🧮 `scoring/` — Lean Prediction Engine

Analyzes voter primary ballot history to estimate partisan lean for unaffiliated and crossover voters. Full detail in **[`scoring/README.md`](scoring/README.md)**.

*   **`mixed_lean_predictor.py`** — the core logic engine; recency-weighted exponential decay over primary ballot history, bucketed into `LEAN_D` / `LEAN_R` / `TRUE_MIXED`.
*   **`run_lean_predictor_all_cohorts.py`** — primary driver; discovers target CSVs under `local/exports/` and runs the predictor in parallel via `ProcessPoolExecutor`.
*   **`run_mixed_lean_predictor_all_counties.py`** — statewide "Mixed" cohort runner.
*   **`unc_lifetime_d_predictor.py`** — prediction model specific to the Lifetime-D cohort.

## 📝 `narrative/` — Dashboard Narratives & Captain Briefings

Turns aggregate jurisdiction stats into human-readable text for the dashboard, in two layers: a deterministic template system and an optional LLM enrichment pass. Full detail in **[`narrative/README.md`](narrative/README.md)**.

*   **`templates.py`** — deterministic, statistically-accurate narrative generation (no LLM).
*   **`llm_enricher.py`** — transforms template output into plain-English "captain briefings" via `claude-haiku-4-5` (Batch API, ~$1 for a full-state run); gracefully no-ops if no API key is set — never blocks the pipeline.
*   **`generate_narratives.py`** — the driver that writes `*_narrative.json` for the dashboard.

## ✅ `tests/` — Pytest Suite & Standalone Reports

Cache-freshness unit tests, generated-file slug/naming hygiene, and manifest/index schema checks for the jurisdiction build. Full detail in **[`tests/README.md`](tests/README.md)**. Run with `pytest tools/tests/`.

*   **`test_voter_data_cleaner.py`** — regression tests for the `_dominant_city_per_precinct` jurisdiction hierarchy (the fix for the postal-city mislabeling bug).
*   **`test_pass_b_index.py`** / **`test_pass_b_manifest_frontend.py`** — schema checks for `docs/data/{type}/index.json` and `docs/manifest.json`'s `jurisdictionScopes` registry.

## 📓 Other

### `voter_analysis.ipynb`
*   **Purpose:** Active Jupyter notebook for exploratory data analysis (EDA) and prototyping.
*   **Function:** Used for testing new classification logic and generating ad-hoc visualizations.
