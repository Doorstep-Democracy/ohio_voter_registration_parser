# tools/tests/

Pytest suite covering pipeline cache correctness, generated
`docs/data/` file/slug hygiene, and manifest/index schema consistency for the "Pass B" jurisdiction
build. Run with `pytest tools/tests/` from the project root (or target a single file).

### `conftest.py`
- **Function:** Pytest fixture-less bootstrap — inserts the project root onto `sys.path` so test
  modules can `import pipeline...` regardless of pytest's invocation directory.
- **Inputs / Outputs:** none; auto-loaded by pytest for every test in this directory.

### `test_pass_b_cache.py`
- **Function:** Unit tests (parametrized across both `pipeline.voter_data_cleaner` and
  `pipeline.jurisdictional_groupings`, which each carry their own copy of the same cache-freshness
  logic) for `_cache_is_fresh()` and `_write_cache_atomic()`. Covers: missing cache, no raw
  partitions, raw-partition newer than cache, **classifier source file newer than cache** (the
  critical case — a cohort-taxonomy code change must invalidate the cache even if the raw data
  didn't move), and atomic write leaving no `.tmp` file behind.
- **Inputs:** synthetic `tmp_path` fixtures (no real data); monkeypatches each module's
  `ENRICHED_CACHE`/`PARQUET_DIR`/`CLASSIFIER_SRC` module-level paths.
- **Outputs:** none (assertions only).
- **Requires:** `polars` (auto-skipped via `pytest.importorskip` if absent).

### `test_voter_data_cleaner.py`
- **Function:** Unit tests for the municipality-resolution helpers in
  `pipeline/voter_data_cleaner.py`: `_normalize_city_name` (strips CITY/VILLAGE/CORP suffixes),
  `_precinct_safe_name` (slugification), `_TOWNSHIP_NAME_RE` (detects township-token precinct
  names), and `_dominant_city_per_precinct` — the jurisdiction-hierarchy resolver itself. The
  hierarchy tests are regression tests for specific real-county bugs: CITY column always wins over
  postal `RESIDENTIAL_CITY`; a township-named precinct never resolves to a city even when postal
  data says otherwise; a township-name token outranks a minority village value; postal is only used
  as a true last resort when no authoritative column exists at all.
- **Inputs:** small inline `pl.DataFrame` fixtures built per-test.
- **Outputs:** none (assertions only).

### `test_data_slug_naming.py`
- **Function:** Filename/slug hygiene check over already-generated `docs/data/city/*.json` files.
  Catches two bug classes found in a 2026-07 incident: double underscores from un-stripped
  whitespace in a source name, and raw punctuation characters that the frontend's own
  `cityNameToSlug`/`countyToSlug` functions would never produce — either causes a silent 404 and a
  "No summary data found" page with no surfaced error. Deliberately scoped to `docs/data/city/`
  only (other jurisdiction types use a separate, pre-existing `name_(county)` convention that's
  out of scope here). One aggregating test rather than one-per-file, since the directory holds
  files across all 88 counties.
- **Inputs:** `docs/data/city/*.json` filenames only (reads no file contents — public, no-PII,
  no pipeline run required).
- **Outputs:** none (assertions only); test is a no-op (empty file list) if the directory doesn't exist.

### `test_pass_b_index.py`
- **Function:** Validates the per-jurisdiction-type `index.json` files written to
  `docs/data/{type}/` by the jurisdiction-index builder. Parametrized across all 12 known
  jurisdiction types. Checks: file exists and is a non-empty array; each entry (sampled, first
  10/20) has the required keys (`slug`, `name`, `display_name`, `county`, `county_slug`,
  `voter_count`, `charts`); `voter_count` is positive; `charts` includes `party_affiliation`;
  county-scoped types (township/village/municipal_court_district) carry non-null `county`/
  `county_slug` and a `display_name` ending in `(X Co.)`, non-scoped types carry null county
  fields; slugs have no duplicates and resolve to an actual `{slug}_party_affiliation.json` file.
- **Inputs:** `docs/data/{type}/index.json` for each of the 12 jurisdiction types.
- **Outputs:** none (assertions only).

### `test_pass_b_manifest_frontend.py`
- **Function:** Validates the `jurisdictionScopes` registry inside `docs/manifest.json` — all 12
  type keys present, each scope has the required fields (`key`, `display`, `displayPlural`,
  `dirName`, `countyScoped`), the `countyScoped` flag matches the actual county-scoped set, each
  scope's `dirName` exists under `docs/data/`, and each scope has a corresponding `index.json`.
  Docstring notes this file used to also test the retired V1 dropdown frontend
  (`docs/index.html`/`charts.js`, replaced by the tree-nav `index.htm`/`assets/v2.js`); those tests
  were deleted rather than repointed, and two cache-freshness tests were dropped as duplicates of
  `test_pass_b_cache.py`'s more rigorous monkeypatched versions.
- **Inputs:** `docs/manifest.json`, `docs/data/` directory structure.
- **Outputs:** none (assertions only).

## CI (`.github/workflows/ci.yml`)

Two Python jobs, both on Python 3.14 (the `requires-python` floor) with dependencies installed
from the committed `uv.lock` (`uv sync --frozen`; `pytest` is in the `dev` dependency group):

- **Python test suite (blocking).** `uv run pytest tools/tests --ignore=tools/tests/conformance`.
  A PR cannot merge red. A CI checkout has no `local/` (gitignored), so every test here must pass
  from committed files alone (`docs/data/`, `docs/manifest.json`, source). A test that needs
  `local/` data must `pytest.skip()` with an explicit reason naming the missing path, not fail
  or error.
- **Conformance (non-blocking).** `uv run pytest tools/tests/conformance` with
  `continue-on-error: true`. Those tests are red by design until the path consolidation lands
  (see `conformance/README.md`). The job runs so the failure count is visible on every PR. When
  the last conformance test turns green, fold it into the blocking job.

Run the blocking set locally the same way: `uv run pytest tools/tests --ignore=tools/tests/conformance -q`.
