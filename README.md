# Ohio Voter Registration Analysis

Download, clean, and analyse the Ohio Secretary of State statewide voter file (SWVF).
Outputs interactive web dashboard JSON for all 88 counties and optional Excel workbooks.

## What's in this repo

**Pipeline (`pipeline/`)**

| File | Purpose |
|---|---|
| `pipeline/ohio_voter_pipeline.py` | Main entry point — interactive menu for statewide or targeted county analysis |
| `pipeline/voter_data_cleaner.py` | Core engine — Polars ingestion, cohort classifier, participation metrics, JSON + Excel export |
| `pipeline/ohio_voter_pipeline_wrapper.py` | Programmatic wrapper around the pipeline for scripted runs |
| `pipeline/jurisdictional_groupings.py` | Aggregates all 12 jurisdiction types; handles county-scoped slug logic |

**Exporters (`tools/export/`)**

| File | Purpose |
|---|---|
| `tools/export/precinct_unc_export.py` | Standalone export: partisan cohort counts per precinct, all 88 counties |
| `tools/export/precinct_party_export.py` | Interactive export: 8-tab partisan-spectrum Excel workbook by county or precinct |
| `tools/export/export_unc_targets.py` | Export cohort-segmented voter targeting CSVs (Pure R/D, Crossover, UNC subclasses) |
| `tools/export/upload_to_gdrive.py` | Upload deliverables to Google Drive |

**Lookup (`tools/lookup/`)**

| File | Purpose |
|---|---|
| `tools/lookup/voter_lookup_parquet.py` | Parquet-backed individual voter lookup |
| `tools/lookup/voter_lookup_source.py` | Raw text file voter lookup (pre-Parquet) |

**Admin (`tools/admin/`)**

| File | Purpose |
|---|---|
| `tools/admin/precinct_key_manager.py` | Scrape and aggregate Ohio BoE precinct keys |
| `tools/admin/clean_precinct_keys.py` | Remove redundant columns from scraped precinct CSVs |
| `tools/admin/regen_city_summary.py` | Repair operator: regenerate `*_city_summary.json` from Parquet |
| `tools/admin/run_city_groupings.py` | Bypass shim: run city jurisdictional groupings without full pipeline menu |

**Narrative (`tools/narrative/`)**

| File | Purpose |
|---|---|
| `tools/narrative/generate_narratives.py` | Pipeline-integrated runner: templated + optional LLM narrative generation |
| `tools/narrative/templates.py` | Deterministic per-level template engine |
| `tools/narrative/llm_enricher.py` | Optional Anthropic API enrichment for captain briefings |

**Scoring (`tools/scoring/`)**

| File | Purpose |
|---|---|
| `tools/scoring/mixed_lean_predictor.py` | Decay-weighted lean predictor for UNC MIXED cohort |
| `tools/scoring/run_lean_predictor_all_cohorts.py` | Batch runner — lean predictor across all cohorts |
| `tools/scoring/run_mixed_lean_predictor_all_counties.py` | Batch runner — lean predictor across all 88 counties |
| `tools/scoring/unc_lifetime_d_predictor.py` | Lifetime Democratic lean predictor for UNC voters |

**Dashboard**

| Path | Purpose |
|---|---|
| `docs/` | Web dashboard (HTML + Chart.js) with county, city, and precinct drill-down |
| `docs/data/` | ~65k pre-generated JSON payloads, one per geography |
| `docs/manifest.json` | Dashboard index — geography list, cohort metadata, routing |

## Requirements

- Python 3.14+
- [uv](https://docs.astral.sh/uv/) for dependency management

Key libraries: Polars (vectorized out-of-core processing), PyArrow (Parquet), xlsxwriter/openpyxl (Excel), DuckDB, requests/beautifulsoup4 (optional SOS scrape).

### Memory

A full-state run works on about 8 million voter rows. Before each heavy stage the pipeline
checks how much physical memory is free and estimates what the stage needs. If the stage
won't fit, the run stops before loading anything and tells you what it needs, what's
available and what you can do about it.

- The check uses free physical memory with 15% held back. It never counts the pagefile or
  swap: a run that only fits by paging is slow and can fail partway.
- The estimate comes from the row counts and column types of your own data, read from the
  Parquet file footers. It's provisional. We haven't calibrated it against a measured run yet.
- Before this check existed, a full-state run peaked at 53.5 GB (measured 2026-11-12). We
  haven't measured the peak since the memory work landed, so there's no tested minimum to
  quote yet.
- A run on a subset of counties (`--choice 5 --counties "57,29"`) needs far less.

The run log records the decision on one line, in this shape:

```text
Memory budget: RAM total <n> GB, available <n> GB, swap <n> GB (not counted); budget <n> GB (15% of available kept in reserve); county export estimated peak <n> GB [frame <n> GB, election batch <n> GB, county slices <n> GB, working <n> GB]; fits
```

A refused run ends with exit code 3 and the line `RUN REFUSED`, followed by the numbers.

| Flag | Effect |
|---|---|
| `--memory-mode auto` | Default. Refuse a stage that doesn't fit. |
| `--memory-mode eager` | Run anyway. Use it when you know the estimate is too high. |
| `--memory-budget-gb N` | Use `N` GB as the budget and skip the measurement. |
| `--max-workers N` | Worker threads. Default: one per logical CPU, at most 8, fewer if the county slices wouldn't fit. |

### Trial runs

To send a run's output somewhere other than `docs/`, set `ELECTION_DATA_OUT` to a folder
that already exists. The pipeline won't create it, and it stops if the folder is missing.
The run log states which output folder is in use.

```powershell
$env:ELECTION_DATA_OUT = "C:\temp\trial-out"
python pipeline/ohio_voter_pipeline.py --choice 5 --counties "57"
```

## Quick start

### 1. Set up the environment

```powershell
uv sync
```

### 2. Obtain the source files

The Ohio SOS blocks automated downloads with 403/CAPTCHA. Download the four SWVF archives
manually from the [Ohio SOS voter file page](https://www6.ohiosos.gov/ords/f?p=VOTERFTP:STWD)
and place them in `local/source/State Voter Files/`:

```
SWVF_1_22.txt.gz
SWVF_23_44.txt.gz
SWVF_45_66.txt.gz
SWVF_67_88.txt.gz
```

### 3. Build the Parquet cache and run the analysis

```powershell
python pipeline/ohio_voter_pipeline.py
```

On first run, select option **[1]** or **[2]**. The pipeline will decompress the source files,
build a Hive-partitioned Parquet cache (`local/source/parquet/COUNTY_NUMBER=NN/`), then export
dashboard JSON for all 88 counties. Subsequent runs load from the Parquet cache (~4 seconds
for all 7.9M rows) instead of re-parsing the raw text files.

**Menu options:**

| Option | Action |
|---|---|
| `1` | Full Ohio (88 counties) → web dashboard JSON |
| `2` | Full Ohio (88 counties) → web dashboard JSON + Excel workbook |
| `3` | Selected counties only → web dashboard JSON (by number or name) |
| `L` | List all 88 counties with official SOS numbers |
| `0` | Exit |

For targeted runs (option 3), enter county numbers or names — partial name matching and
comma-separated multi-county selection are supported. Example: `57, greene, 76`.

### 4. Export a precinct or county voter workbook

```powershell
python tools/export/precinct_party_export.py
```

Interactive menu: choose county + precinct (single workbook) or whole county. Output lands in
`local/exports/Workbooks/`. Tabs follow the 8-cohort partisan spectrum (see Cohort Taxonomy below).

### 5. View the dashboard

The dashboard supports three scope tiers: **County** (default), **City**, and **Precinct**.
Precinct drill-down shows per-precinct cohort composition for every precinct in the selected county.

**Easiest: use the live site.**
[https://precincts.info](https://precincts.info) — fully rendered, no setup.

**Run it locally** if you want to develop against it or unlock captain mode (see step 6).
You cannot just double-click `docs/app.htm` — modern browsers block `fetch()` over
`file://` URLs, so the dashboard JSON won't load. You need a tiny static server. Python's
built-in one is enough:

```powershell
cd docs
python -m http.server 8001
```

Open [http://localhost:8001](http://localhost:8001) — this is the new landing page (`docs/index.htm`) with the interactive statewide map. The dashboard itself now lives at [http://localhost:8001/app.htm](http://localhost:8001/app.htm) (`docs/app.htm`); any old deep link that carries dashboard params (e.g. `?level=county&id=hamilton`) redirects there automatically. Leave the terminal running; stop with `Ctrl+C`.

### 6. Captain mode (optional — real voter rosters)

The public dashboard shows aggregates only. **Captain mode** is an overlay on the same
dashboard that lets you click any chart segment in a precinct view and get the actual list
of voters in that cohort (name, address, last-voted dates), plus walk-list and door-touch
tracking. The roster data stays on your machine; nothing about captain mode touches `docs/`
or the network.

**Prerequisites:** you must have completed steps 2 and 3 above. Captain mode reads the
enriched parquet (`local/source/parquet_enriched/enriched_voters.parquet`) that the pipeline
builds. If that file is missing, `serve/roster_api.py` refuses to start and prints the
pipeline command you need to run. If the API is somehow running without the parquet, the
dashboard shows an amber banner instead of activating click handlers.

**Run two terminals:**

```powershell
# Terminal 1 — static dashboard (same as step 5)
cd docs
python -m http.server 8001
```

```powershell
# Terminal 2 — roster API on port 8000
$env:ROSTER_TOKEN = "dev-secret"
python serve/roster_api.py
```

Stop either server with `Ctrl+C`.

**First-run note (Windows):** Defender Firewall prompts on the first listening-socket
bind for a given Python interpreter. You will see *one* "Allow access" dialog per
server, per interpreter (a venv Python and a system Python prompt separately). Click
"Allow access" once and the rule persists. If a terminal seems frozen waiting for the
server to start, check for a Defender dialog hidden behind your other windows.

Now reload [http://localhost:8001](http://localhost:8001). The page will detect the API on
`127.0.0.1:8000` and inject a banner: *"Self-hosted — real voter data"*. Drill into any
county → precinct, then click a slice of the cohort ribbon, doughnut, or generation chart
to open the roster panel.

**Why two ports?** The static server and the API both default to 8000. We put the dashboard
on 8001 to avoid the collision; the API stays on 8000 because `captain-mode.js` hardcodes
`http://127.0.0.1:8000` as its default backend (override with `?captainApi=http://...`).

**What gets written.** Captain identity, walk lists, and door-touch logs go to
`local/captain.db` (SQLite, gitignored). It's created automatically on first write. The
enriched parquet is read-only.

## Output

- **Web dashboard** — `docs/app.htm` with Chart.js charts; data in `docs/data/` and `docs/manifest.json` (the root `docs/index.htm` is the landing page)
- **Excel workbook** — `ohio_analysis_YYYY-MM-DD.xlsx` (county summary) or `county_NN_analysis_YYYY-MM-DD.xlsx`
  - Sheets: Decade Summary, Participation, District Breakdown, Party Cross-tabs
  - Ohio-wide builds use a County Summary sheet (raw 7.9M rows exceeds Excel's row limit)
- **Precinct workbook** — `local/exports/Workbooks/{County}_{Precinct}_voters.xlsx` — all voter columns + cohort scoring
- **Target CSVs** — `local/exports/Pure_D/`, `local/exports/D_Crossover/`, etc. — one file per county per cohort
- **Parquet cache** — `local/source/parquet/COUNTY_NUMBER=NN/` — fast reload for subsequent runs
- **Error log** — `local/working/errors/invalid_birthyear_<run>.csv` — rows dropped for unparseable birth dates
- **Run logs** — `local/logs/voter_analysis_YYYYMMDD_HHMMSS.log`

## Cohort taxonomy

`PARTY_AFFILIATION` in the SWVF is behavior-derived and lagged — it reflects only the past two calendar years of primary participation, not lifetime history. The universal classifier (`classify_all_voters_primary_history`) applies the full 26-year election history to every voter and assigns a cohort and cohort_family.

### Affiliated voters (affiliated R or D)

| Cohort family | Definition | Dashboard color |
|---|---|---|
| `PURE_R` | Affiliated R + zero D primary ballots ever | `#ef4444` |
| `CROSSOVER_R` | Affiliated R + has voted both primaries historically | `#f87171` |
| `PURE_D` | Affiliated D + zero R primary ballots ever | `#3b82f6` |
| `CROSSOVER_D` | Affiliated D + has voted both primaries historically | `#60a5fa` |

`R_NEW` / `D_NEW` (registered within ~2 years, no primary history) roll into `PURE_R` / `PURE_D` with `is_new_registrant = True`.

### Unaffiliated voters (UNC)

| Cohort family | Definition | Dashboard color |
|---|---|---|
| `UNC_LIFETIME_R` | All primaries were R ballots | `#fca5a5` |
| `UNC_LIFETIME_D` | All primaries were D ballots | `#93c5fd` |
| `UNC_MIXED` | Has voted both party primaries | `#a78bfa` |
| `UNC_NO_HISTORY` | No primary participation on record | `#9ca3af` |

### Crossover scoring

Voters in `CROSSOVER_R`, `CROSSOVER_D`, and `UNC_MIXED` receive decay-weighted lean scores:

- `lean_score` ∈ [-1, +1]: recency-weighted ratio of D vs R primary ballots (decay λ=0.15, presidential-year 1.25× boost)
- `crossover_class`: `LOCKED_D` / `LEAN_D` / `TRUE_MIXED` / `LEAN_R` / `LOCKED_R`
  - Affiliated crossover thresholds: LOCKED ±0.50, LEAN ±0.30
  - UNC MIXED thresholds: LOCKED ±0.40, LEAN ±0.20
- Auxiliary: `confidence`, `recent_5yr_lean`, `last_three_party`, `switch_count`, `years_since_last_partisan`

## Analysis methodology

**Party affiliation** is behavior-derived, not self-declared. Per Ohio EOM Ch. 15, affiliation
reflects which party primary a voter participated in within the past two calendar years.
Election history columns are used as ground truth for lifetime trajectory analysis.

**Voter status** in the SWVF is either `ACTIVE` or `CONFIRMATION`. `CONFIRMATION` collapses
multiple internal substatus codes (NCOA movers, BMV/SSA mismatches, supplemental list maintenance).
High CONFIRMATION density combined with sparse recent election history is a concrete anomaly signal.

**Generational cohorts** follow [Pew Research Center definitions](https://pewrsr.ch/2HFp9rq):

| Generation | Birth years |
|---|---|
| Silent / Greatest | ≤ 1945 |
| Baby Boomers | 1946–1964 |
| Generation X | 1965–1980 |
| Millennials | 1981–1996 |
| Generation Z | 1997–2012 |
| Gen Alpha | ≥ 2013 |

## What stays out of git

`local/source/`, `local/working/`, `local/exports/`, `*.xlsx`, `*.csv`, `*.txt`, and `local/logs/` are excluded via `.gitignore`.
Raw SWVF files contain PII and must never be committed. Aggregated dashboard JSON in `docs/data/`
contains no individual voter records and is safe to publish.

## Roadmap

- **Phase 2** — Roll-up aggregation: Wards, School Districts, Legislative Districts
- **Phase 3** — Diff engine + GIS: demographic deltas between subregions and parent geographies; precinct choropleth maps via Census TIGER shapefiles
- **Phase 4** — Dark data: unregistered resident matching via property records, Census blocks, USPS NCOA
- **Phase 5** — Temporal tracking: registration momentum, turnout anomaly detection, SaaS delivery

## License

MIT — Matthew F Reyes, 2026

---

## `pipeline/voter_data_cleaner.py` — core engine reference

This module is not normally called directly. `pipeline/ohio_voter_pipeline.py` is the intended entry point. The functions below are documented for scripted or programmatic use.

### Smoke test

```powershell
python pipeline/voter_data_cleaner.py --test
```

Validates that `OHIO_COUNTIES` has all 88 entries and spot-checks county numbering. Exits with a non-zero code on failure.

### Public entry points

**`run_ohio_analysis(txt_files, use_parquet, max_workers, include_precinct_charts)`**

Exports web dashboard JSON for all 88 counties. Writes to `docs/data/` and updates `docs/manifest.json`. Does not produce Excel output.

- `txt_files` — list of `SWVF_*.txt` paths
- `use_parquet` — build / reuse Hive-partitioned Parquet cache (default `True`; strongly recommended)
- `max_workers` — thread-pool size; `0` = auto
- `include_precinct_charts` — also write per-precinct party + UNC JSON files (significantly increases file count and run time; prompt `[Y]` in the pipeline menu)

**`run_county_subset(txt_files, county_numbers, use_parquet, include_precinct_charts)`**

Same pipeline as `run_ohio_analysis()` but loads only the requested Parquet partitions. Requires the Parquet cache to exist. `county_numbers` is a list of zero-padded strings, e.g. `['57', '29']`.

**`run_ohio_excel(txt_files, output_path, use_parquet)`**

Writes a summary Excel workbook for all of Ohio. Completely independent of the JSON / dashboard path — call only when the Excel deliverable is needed.

**`run_county_analysis(txt_files, county_number, output_path, use_parquet)`**

Single-county combined run: web dashboard JSON + Excel workbook. `county_number` is a zero-padded string or bare integer; the function normalises it.

### Key build functions (importable)

| Function | Input | Output |
|---|---|---|
| `build_city_summary(df)` | County-scoped Polars DataFrame | City / township aggregation. Municipality is resolved by the SWVF jurisdiction hierarchy (`CITY → VILLAGE → WARD-prefix → TOWNSHIP = not-a-city → RESIDENTIAL_CITY` as last resort), never by postal city alone — see `_dominant_city_per_precinct` and `tools/admin/validate_jurisdiction_fields.py` |
| `build_county_summary(df)` | County-scoped DataFrame | Active / Confirmation totals and cohort breakdown |
| `build_precinct_summary(df)` | County-scoped DataFrame | Per-precinct active / confirmation totals |
| `build_parquet_cache(txt_files)` | SWVF `.txt` paths | Hive-partitioned Parquet at `local/source/parquet/COUNTY_NUMBER=NN/` |
| `classify_all_voters_primary_history(df)` | Full statewide DataFrame | Adds `cohort_family`, `cohort`, `lean_score`, crossover columns |

### Targeted post-pipeline utilities

These scripts update specific outputs without rerunning the full pipeline:

```powershell
# Regenerate all *_city_summary.json from parquet (fast; skips chart generation)
python tools/admin/regen_city_summary.py
```

### Data flow

```
SWVF_*.txt.gz
    └─▶ build_parquet_cache()   →  local/source/parquet/COUNTY_NUMBER=NN/
            └─▶ load_voter_files_parquet()
                    └─▶ clean_voter_data()
                            └─▶ classify_all_voters_primary_history()
                                    ├─▶ export_county_json()   →  docs/data/*.json
                                    └─▶ build_workbook()       →  *.xlsx
```

### Important constraints

- Uses **Polars**, not Pandas, for all core operations. Functions returning `pl.DataFrame` must not be passed to Pandas-expecting code without explicit conversion.
- `COUNTY_NUMBER` is a **Hive partition key** in the Parquet layout, not a column inside the files. Add it back as a literal if needed: `df.with_columns(pl.lit('57').alias('COUNTY_NUMBER'))`.
- `CITY` column in SWVF is blank for ~19 counties (Cuyahoga, Holmes, Sandusky, and others). `build_city_summary()` falls back to precinct-name prefix extraction for those counties.
- Never call `build_city_summary()` using `PRECINCT_NAME` as a proxy for municipality. The CITY column is the authoritative source; precinct boundaries do not map 1:1 to municipal boundaries.
