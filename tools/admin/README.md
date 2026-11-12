# tools/admin/

Admin/pipeline utilities for the officials/captains/candidates build, precinct-key sourcing,
and one-off maintenance scripts. Most scripts here follow a staged pipeline (Stage 3 -> 8)
documented in each file's docstring; `officials_common.py` is the shared library every stage
imports from (single-resolver hygiene, CLAUDE.md section 5).

## Officials / Captains / Candidates pipeline

Producer stages write to `local/working/` (intermediate) or `serve/*.json` (final, served to
the dashboard). Run order: 3 -> 4 -> 5 -> 6 -> 7 -> 8, per county.

### `officials_common.py`
Shared library, not a runnable script. Provides:
- **Inputs:** none directly; wraps `local/source/precinct_keys/*.csv` and `enriched_voters.parquet`.
- **Functions:** `COUNTY_NUMBER`/`COUNTY_SLUG` lookups, `load_precinct_crosswalk()` (ballot number ->
  exact parquet `PRECINCT_NAME`), `load_jurisdiction_crosswalk()` (precinct -> CD/SD/HD containment),
  `normalize_name()` / `name_from_parts()` (ALL-CAPS BoE names -> display case), `atomic_write_json()`
  (`.tmp` -> `os.replace`, never a partial file).
- **Outputs:** none (library only). Every other pipeline script imports from here to avoid
  duplicating the precinct/name resolvers.

### `parse_central_committee.py` (Stage 3)
- **Function:** Dispatches over three adapters (CSV rows in `electedofficials.csv`, a BoE PDF parsed
  by physical line geometry via PyMuPDF, or a manual JSON override) to extract central-committee
  (precinct captain) filings for one county+party.
- **Inputs:** `local/source/electedofficials.csv`, county PDF filings under
  `local/source/County Data Files/{county}/`, or `local/patches/captain_override_{county}_{party}.json`.
- **Outputs:** `local/working/{county_code}_captain_filings_{county}_{party}.json` (intermediate schema
  shared by all three adapters).
- **Usage:** `python tools/admin/parse_central_committee.py --county montgomery --all-parties`

### `build_precinct_captains.py` (Stage 5)
- **Function:** Merges the three Stage-3 party files (D/L/R) per precinct into contested /
  uncontested / vacant / data_pending status buckets.
- **Inputs:** `local/working/*_captain_filings_{county}_{D,L,R}.json`, the precinct crosswalk.
- **Outputs:** `serve/precinct_captains.json` (keyed by exact parquet `PRECINCT_NAME`; read by the
  captain UI and roster API).
- **Usage:** `python tools/admin/build_precinct_captains.py --county montgomery`

### `parse_candidate_petitions.py` (Stage 4)
- **Function:** Parses the BoE candidate petition report (converted to markdown first) into flat
  general-election candidate filings, splitting multi-candidate stacked table rows and classifying
  office sections by regex.
- **Inputs:** `local/source/County Data Files/{county}/*Candidate-Petition-Report*.md` (produced by
  `pdf_to_markdown.py`).
- **Outputs:** `local/working/{county_code}_candidate_filings_{county}.json`.
- **Usage:** `python tools/admin/parse_candidate_petitions.py --county montgomery`

### `build_candidates.py` (Stage 6)
- **Function:** Groups Stage-4 filings into district/county/statewide/judicial sections matching
  `officials.json`'s key structure.
- **Inputs:** `local/working/*_candidate_filings_{county}.json`.
- **Outputs:** `serve/candidates.json` (2026 general-election candidate roster).
- **Usage:** `python tools/admin/build_candidates.py --county montgomery`

### `ingest_elected_officials.py`
- **Function:** Populates `serve/officials.json` WARD/TOWNSHIP/VILLAGE/school-district sections from
  the SOS elected officials CSV, and fully derives the three district sections (Congressional/Senate/
  House incumbents + challengers) by cross-referencing `candidates.json`.
- **Inputs:** `local/source/electedofficials.csv`, `serve/candidates.json`.
- **Outputs:** `serve/officials.json` (merged in place; dry-run by default, `--write` to apply).
- **Usage:** `python tools/admin/ingest_elected_officials.py --write`

### `match_to_voters.py` (Stage 7)
- **Function:** The single voter-matching resolver (`match_entity`) used across the whole pipeline.
  Cross-references every officials/captains/candidates filer against the enriched voter parquet
  (two-pass exact-prefix + rapidfuzz matching, disambiguated by zip or precinct), attaches a partisan
  lean profile, and joins in the human-curated confirmation ledger.
- **Inputs:** `serve/officials.json`, `serve/precinct_captains.json`, `serve/candidates.json`,
  `enriched_voters.parquet`, `serve/voter_match_confirmations.json`.
- **Outputs:** `serve/partisan_profiles.json`.
- **Usage:** `python tools/admin/match_to_voters.py --county montgomery [--verbose]`

### `validate_officials.py` (Stage 8)
- **Function:** Integrity gate for the whole officials pipeline — bounded checks on precinct-captain
  keys/statuses, DEM filing gaps, person name/party well-formedness, partisan-profile label vocabulary,
  confirmation-ledger shape, district incumbency reconciliation against a fresh CSV re-derivation, and
  schema-drift (delegates to `validate_schema.py`, renamed 2026-07 from `dump_schema.py`).
- **Inputs:** all `serve/*.json` files, `electedofficials.csv`.
- **Outputs:** none (stdout report only); non-zero exit code on any failed check.
- **Usage:** `python tools/admin/validate_officials.py --county montgomery`

### `captain_match_report.py`
- **Function:** One-off P0 measurement report: of the 209 Democratic precinct captains (identified by
  `OFCDESC` prefix, not the unreliable `PARTY` column), how many resolve to an exact/fuzzy/no voter
  match. Reuses `match_to_voters.py`'s resolver rather than reimplementing matching. Also cross-checks
  a secondary residential-zip roster (`Copy of Dem CC.xlsx`) as corroboration only.
- **Inputs:** `local/source/County Data Files/57_Montgomery/electedofficials.csv`, the precinct
  crosswalk, `enriched_voters.parquet`, optionally `Other/Copy of Dem CC.xlsx`.
- **Outputs:** `local/working/captain_match_report_{county}.json` + a human-readable `.txt` companion.
- **Usage:** `python tools/admin/captain_match_report.py [--verbose]`

### `seed_quorum_registry.py`
- **Function:** Seeds the seat/holder-term registry for one county's precinct captains via a
  per-county `CountyAdapter` (Montgomery implemented; other counties raise `NotImplementedError`).
  Runs a CT3 validation gate (HALT on count mismatch, missing fields, or duplicate precinct captain),
  matches each captain to a voter record via `match_to_voters.py`'s resolver, writes seat/holder_term
  rows into `captain_db.py`'s SQLite tier, and emits the quorum app's `Captain[]` JSON.
- **Inputs:** `local/source/County Data Files/{county}/electedofficials.csv`, `enriched_voters.parquet`,
  `serve/captain_db.sqlite` (via `captain_db.py`).
- **Outputs:** `local/working/quorum_registry_{date}.json` (gitignored; copy to
  `quorum/src/mockRegistry.json` before an event); optionally writes SQLite rows (`--no-db` to skip).
- **Usage:** `.venv/Scripts/python.exe tools/admin/seed_quorum_registry.py [--no-db] [--verbose]`

## Schema & validation

### `validate_schema.py`
*(renamed 2026-07 from `dump_schema.py`, to align its verb prefix with the other two
validation gates in this folder — `validate_officials.py` and
`validate_jurisdiction_fields.py`. Function names (`generate_blocks()`, `check_drift()`)
and the `--check` flag are unchanged. `validate_officials.py`'s import was updated to
match; if anything else imports this by the old filename/path, update the reference.)*
- **Function:** Generates the structural inventory (column names/dtypes, JSON key shapes with dynamic
  keys collapsed to `<key>`) that lives inside marker-delimited blocks in `schema/*.md` docs. Hand-written
  semantic annotations outside the markers are preserved untouched. `validate_officials.py` imports
  `check_drift()` to gate on staleness.
- **Inputs:** `enriched_voters.parquet` schema, `serve/officials.json`, `serve/precinct_captains.json`,
  `serve/candidates.json`, `serve/partisan_profiles.json` (skipped if not yet produced).
- **Outputs:** generated blocks inside `schema/enriched/enriched_voters.md` and `schema/serve/*.md`.
- **Usage:** `python tools/admin/validate_schema.py` (write) or `--check` (exit 1 if stale, no writes).
- **Host note:** must run on Windows/Claude Code CLI — the Cowork sandbox serves byte-capped reads and
  cannot reliably read the parquet.

### `validate_jurisdiction_fields.py`
- **Function:** Validates the jurisdiction-vs-postal city fields in the enriched voter parquet, per
  county: `CITY` coverage, jurisdiction-hierarchy fallback coverage (VILLAGE/WARD/TOWNSHIP), and
  precincts whose name implies a township/village but would be mislabeled by a postal-city fallback.
  Also surfaces (non-blocking) "village-in-township" candidates for manual review. This is the
  data-validation gate mentioned in the project `CLAUDE.md` — run on every new SWVF drop.
- **Inputs:** `local/source/parquet_enriched/enriched_voters.parquet` (or `--parquet` override).
- **Outputs:** none (stdout report only); `--strict` exits non-zero if any HIGH-severity mislabel exists.
- **Usage:** `python tools/admin/validate_jurisdiction_fields.py [--strict] [--show N]`

### `jurisdiction_collisions_report.py`
- **Function:** Standalone report (moved from `tools/tests/test_jurisdiction_collisions.py`,
  whose `test_` name made pytest try to collect it). Detects jurisdiction name collisions: the
  same name (e.g. a TOWNSHIP or VILLAGE value) under more than one `COUNTY_NUMBER` partition,
  which would silently merge into one output file if a grouping script slugged on name alone
  instead of a `(COUNTY_NUMBER, name)` composite key. Checks all 12 jurisdiction-type columns.
- **Inputs:** `local/source/parquet/**/*.parquet` (hive-partitioned by `COUNTY_NUMBER`).
- **Outputs:** `local/working/collision_report.md` (colliding names + county lists per
  jurisdiction type); also prints an OK/COLLIDE summary per type to stdout.
- **Usage:** `uv run python tools/admin/jurisdiction_collisions_report.py`

## Precinct key sourcing

### `precinct_key_manager.py`
- **Function:** Interactive menu combining two roles: (1) scrape the "Beginning Precinct" dropdown from
  each subscribed county's Ohio BoE `vtrapp` lookup page to get precinct code/label pairs, and
  (2) aggregate all per-county CSVs into one multi-tab Excel workbook.
- **Inputs:** county BoE `vtrapp` web pages (network fetch); existing
  `local/source/precinct_keys/{county}_precincts.csv` files for aggregation.
- **Outputs:** `local/source/precinct_keys/{county}_precincts.csv` (one per county) and
  `local/source/precinct_keys/precinct_keys_master.xlsx`.
- **Usage:** `python tools/admin/precinct_key_manager.py` (interactive: 1=scrape, 2=aggregate, 3=both).
- **Requires:** `curl_cffi`, `beautifulsoup4`, `xlsxwriter`.

### `clean_precinct_keys.py`
- **Function:** Strips scraped precinct CSVs down to `county`, `precinct_code`, `precinct_label`
  columns only, validating the expected 5-column header before overwriting.
- **Inputs:** `local/source/precinct_keys/*_precincts.csv`.
- **Outputs:** the same files, rewritten in place with only the 3 kept columns.
- **Usage:** `python tools/admin/clean_precinct_keys.py`

## City/jurisdiction regeneration

### `regen_city_summary.py`
- **Function:** Regenerates `*_city_summary.json` for all 88 counties using the corrected
  `build_city_summary()` (groups on the `CITY` column, falling back to `RESIDENTIAL_CITY` only where
  `CITY` is blank) instead of the old `PRECINCT_NAME` prefix-matching approach. Does not touch precinct
  charts, cohort data, or any other pipeline output.
- **Inputs:** `local/source/parquet/COUNTY_NUMBER={n}/` partitions.
- **Outputs:** `docs/data/{county_slug}_city_summary.json` per county.
- **Usage:** `python tools/regen_city_summary.py` (run from project root)

### `run_city_groupings.py`
- **Function:** Thin direct runner that invokes `pipeline.jurisdictional_groupings.main()` for just
  the `cities` jurisdiction level, bypassing the interactive pipeline menu.
- **Inputs:** whatever `jurisdictional_groupings.py`'s city-level processing reads (enriched parquet).
- **Outputs:** city-level chart JSON files in `docs/data/`.
- **Usage:** `python tools/admin/run_city_groupings.py`

## Format conversion

### `pdf_to_markdown.py`
- **Function:** Batch-converts BoE filing PDFs to markdown via `pymupdf4llm`, with a pre-pass that
  detects checkbox/checkmark vector graphics (e.g. incumbent markers on petition forms) and burns
  `[x]`/`[ ]` text into the page before conversion, so the markdown captures checkbox state that would
  otherwise be lost. Skips PDFs with an up-to-date `.md` already present unless `--force`.
- **Inputs:** one or more PDF file paths, or `--all` to convert everything under
  `local/source/County Data Files/`.
- **Outputs:** `<same directory>/<basename>.md` per PDF.
- **Usage:** `python tools/admin/pdf_to_markdown.py path/to/file.pdf [--force]` or `--all`
- **Requires:** `pymupdf` (`fitz`), `pymupdf4llm`.

### `HTMLbook_convert.py`
- **Function:** Standalone HTML-to-Markdown converter for O'Reilly-style HTMLBook source (structural
  sections, textboxes, tables, images) — not part of the voter pipeline; a general-purpose document
  conversion utility using BeautifulSoup.
- **Inputs:** an HTMLBook-format `.html` file (positional arg).
- **Outputs:** a `.md` file (positional arg) — verbatim structural markdown copy.
- **Usage:** `python tools/admin/HTMLbook_convert.py input.html output.md`
- **Requires:** `beautifulsoup4`.

## Empty / stub

### `__init__.py`
Empty — makes `tools/admin/` importable as a package (used by sibling scripts' relative imports).
