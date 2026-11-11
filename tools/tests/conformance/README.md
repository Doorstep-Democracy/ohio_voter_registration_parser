# Conformance suite — structural gates for the path-consolidation work

This directory holds **conformance** tests, as distinct from the behavioural
tests in `tools/tests/`. The difference is the whole reason it exists:

* A **behavioural** test proves *nothing broke*. Byte-identical regeneration,
  a green `pytest tools/tests/`, a clean import — all of these pass happily
  while a refactor is only two-thirds done.
* A **conformance** test proves *everything was done*. It fails while a single
  file still holds its own copy of a managed path, while a single docstring
  still names a module that was renamed away, while a single file in scope is
  still unclassified.

The path-consolidation work (`HANDOFF_DATA_DIR_CONSOLIDATION_AND_STATE_READINESS.md`,
Linear DOO-85 through DOO-92) is a 33-file refactor whose only failure mode is
partial completion. A refactor that converts 25 of 33 files passes every
behavioural gate in the repo and leaves the exact disease it was meant to cure.
This suite is the gate that catches that.

## The one rule that governs everything here

**These tests were written *before* the refactor, and most of them are RED
today. That is intentional and load-bearing.**

A conformance test authored *after* a refactor, which passes the first time it
runs, has never been demonstrated to detect anything. It is a claim, not a
check. Written first and watched to fail, each test proves it can see the
condition it exists to police — and then the definition of "done" for the
refactor becomes *the suite goes green*, rather than a reviewer's recollection
of a markdown checklist.

Do not "fix" a red test here by weakening it. Fix the code it is pointing at,
or — if the finding is genuinely out of scope — add it to that test's explicit
allowlist **with a written reason**, which is itself reviewable.

## Running

```powershell
cd "D:\vibe\election-data"
.venv\Scripts\python.exe -m pytest tools/tests/conformance -q
```

Stdlib only — no Polars, no DuckDB, no parquet cache, no network. The suite
runs on a fresh clone in about five seconds, so it is cheap enough to run on
every commit of the refactor rather than once at the end. Tests that genuinely
need data (the on-disk partition comparison) `skip` with an explicit reason
rather than passing silently.

## Baseline as of 2026-08-01 (pre-refactor)

```
11 failed, 7 passed, 7 skipped
```

The seven skips are all `pipeline/paths.py` does not exist yet — they are the
acceptance criteria for DOO-86 and will start executing the moment it lands.

**2026-11-11:** `8 failed, 10 passed, 7 skipped`. Evaluation items P10 and P15
cleared three `test_stale_references.py` tests (section 6). CI runs this
directory as a non-blocking job until it is green (`tools/tests/README.md`, "CI").

Detector output at baseline, which is also the corrected scope figure:

| Detector | Result |
|---|---|
| Managed path constructed outside a resolver | **100 sites in 32 files** (48 module-level, 43 inline, 9 bare literals) |
| — of which built inline inside a `pipeline/` function | 9 |
| `COUNTY_NUMBER=` partition names built by hand | **13 sites in 9 files** |
| `local/source/` paths outside the four core families | 54 sites in 23 files (reported, not asserted) |
| Files in `scope_manifest.json` | **33** |

For context on that last number: the handoff states 30 (13 + 17 from two
overlapping lists); the true union of those two lists is 27; the AST detectors
find 33. The three counts differ because the handoff's lists were assembled by
name-based greps, which miss inline construction, `os.path.join`, bare string
literals, and paths assembled across two statements. See "What the detectors
catch that a grep does not" below.

---

## The tests, one by one

### 1. `test_no_second_copy.py` — no managed path is built outside the resolver

**What it does.** Parses every `.py` file under `pipeline/`, `tools/` and
`serve/` into an AST and finds every assignment whose right-hand side
constructs a filesystem path belonging to one of four managed families:
`docs_data`, `parquet`, `parquet_enriched`, `txt_source`. Any such site outside
`pipeline/paths.py` is a failure.

**Why it exists.** CLAUDE.md §5 (single-resolver logic) was amended on
2026-08-01 to cover infrastructure constants, because nine different naming
conventions for two conceptual paths had already appeared under the old
wording. A rule that lives only in prose produced that outcome. This test is
what converts §5 from a convention into an invariant.

**Three assertions, deliberately separate** so the failure output names each
class on its own:

* `test_no_managed_path_constructed_outside_resolver` — the headline check,
  across all scopes.
* `test_no_inline_path_construction_in_pipeline` — paths built *inside a
  function body* in `pipeline/`. This is the variant that hides from a
  constant-name grep, and it is exactly how the original audit undercounted on
  its first pass (`ohio_voter_pipeline.py` builds its `docs/data` path inline,
  invisible to a `DATA_DIR`-name search).
* `test_no_bare_path_literals` — a managed path written as a raw string in a
  dict value or call argument, carrying no constant name at all. Nothing links
  it to the value it duplicates, so it drifts in total silence.

**How to make it green.** Land `pipeline/paths.py`, convert each file, mark it
`converted` in the manifest. The test goes green on the last one, not the first.

**Allowlist.** `pipeline/paths.py` itself and this directory (these tests must
name the patterns they detect). Defined in `conftest.ALLOWLIST_RELPATHS`.

---

### 2. `test_scope_manifest.py` — the scope list is complete and self-verifying

**What it does.** Replaces the handoff's prose file-inventory table with
`scope_manifest.json`, and then polices that file five ways.

The load-bearing assertion is `test_manifest_covers_every_detected_file`:
**the manifest must be a superset of what the detectors find.** If a file
constructs a managed path and is not listed, either someone added a new copy
or the original audit missed one — both need triage, and neither should be
discovered halfway through the refactor.

**Why it exists.** The handoff's inventory was a markdown table whose total was
computed by adding two lists that overlap by six files. A machine cannot check
a markdown table, and the moment anyone adds a file the table is wrong and
nothing says so. Moving the inventory into JSON with a test behind it makes
"is all the code touched?" a command you run instead of a document you re-read.

**The five assertions.**

* `test_manifest_covers_every_detected_file` — superset rule, above.
* `test_manifest_entries_are_well_formed` — no duplicates; `bucket` and
  `status` drawn from their enums; anything marked `converted` or `excluded`
  must carry a written note. An exclusion without a reason is indistinguishable
  from an oversight.
* `test_manifest_files_exist` — catches a file renamed or deleted without the
  scope being updated.
* `test_every_file_is_classified` — **this is DOO-85 expressed as a gate.**
  Red until every entry has a bucket: `safe-mechanical`, `needs-judgment`, or
  `broken-or-dead`. Unclassified files are the ones that get swept into a
  mechanical pass by accident.
* `test_conversion_is_complete` — **the completeness gate.** Red until every
  in-scope file is `converted` or explicitly `excluded` with a reason.

**On the third bucket.** The handoff has two buckets. `broken-or-dead` is
added because the repo contains files that *cannot currently be imported* (see
test 6) — those must be *decided about*, not converted. Leaving them alone is a
legitimate answer; silently repairing them during a path refactor is not,
because that resurrects untested code while looking like cleanup.

**Note on the manifest's provenance.** `scope_manifest.json` was generated by
running the detectors in this suite, so `test_manifest_covers_every_detected_file`
passes today by construction. Its value is prospective: it catches the first
file added after this date without registration. The manifest is a derived
artifact — regenerate it deliberately, never edit it to silence a failure.

---

### 3. `test_replacement_landed.py` — converted files actually import the resolver

**What it does.** For every file marked `converted` in the manifest, asserts
that it imports from `pipeline.paths`, and that it no longer defines a managed
path of its own.

**Why it exists.** Test 1 alone is satisfiable by deleting a constant and
inlining the literal at each use site — passing the letter of the rule while
defeating its purpose entirely. This is the complement: the constant must have
been *replaced*, not merely *removed*. The second assertion catches the
opposite residue: a file that imports the resolver and keeps its old constant
beside it, which is the worst of both — two live definitions, one silently
shadowed.

**Status.** Green but vacuous today, because nothing is marked `converted`. It
acquires teeth one file at a time as the refactor lands, which is the intent:
each conversion is verified individually rather than the batch being trusted.

---

### 4. `test_partition_strings.py` — one canonical `COUNTY_NUMBER=` form

**What it does.** Four assertions covering both the *location* and the
*correctness* of partition-key construction.

* `test_no_partition_string_built_outside_resolver` — no `COUNTY_NUMBER=`
  literal or f-string anywhere except the resolver. 13 sites in 9 files today.
* `test_partition_form_is_canonical_and_type_insensitive` — `county_partition(7)`,
  `county_partition("7")` and `county_partition("07")` must all yield
  `COUNTY_NUMBER=07`.
* `test_partition_form_matches_on_disk` — the canonical form must equal the
  directory names actually present in `local/source/parquet/`. Skips when the
  cache is absent; never assumes.
* `test_expected_partition_set_is_not_a_bare_range` — flags the hardcoded
  `range(1, 89)` at `voter_data_cleaner.py:682`.

**Why the second and third assertions matter more than the first.** The repo
already proved this drifts: `voter_data_cleaner.py` builds
`f'COUNTY_NUMBER={cnum}'` on line 731 and `f'COUNTY_NUMBER={int(cnum)}'` on
line 733 — twelve lines apart, same function, different zero-padding. The
partitions on disk are zero-padded, so the `int()` form mis-globs for counties
1–9 whenever `cnum` arrives as an integer. **Centralising on the wrong side of
that inconsistency would centralise the bug** and make it uniform instead of
sporadic. Comparing against the real directory names is the only check that
catches that, and it is why this test reaches for the filesystem at all.

**Related.** `tools/tests/test_pass_b_cache.py:27` hardcodes `"COUNTY_NUMBER=01"`
as a fixture. It is in the manifest; update it in the same commit as the helper
or it quietly tests a shape that no longer matches production.

---

### 5. `test_path_invariants.py` — the surviving definitions are structurally sound

**What it does.** Everything above polices *where* paths are defined. This
polices whether what remains is *correct*: exports the expected constants; every
path absolute, inside the repo, free of `..`; `DATA_DIR` under `DOCS_DIR` under
`BASE_DIR`, `PARQUET_DIR` under `SOURCE_DIR`; no machine-specific fragments
(`D:\`, `/home/`, `/mnt/`, `/sessions/`) baked into the resolver.

**The assertion worth reading twice** is `test_paths_are_independent_of_cwd`.
CLAUDE.md §6 says "do not rely on CWD" precisely because relying on it has bitten
before, and the failure is invisible from inside a normal test run: pytest
happens to execute from the repo root, so a CWD-dependent constant resolves
correctly right up until a scheduled task, an SSH session or a different shell
runs the same code from somewhere else. The test launches a **subprocess from a
temp directory** and requires byte-identical constants, because reloading the
module in-process would not re-evaluate module-level constants the same way.
This is §6's rule as an executable check for the first time.

**Design note.** `REQUIRED_CONSTANTS` is written out explicitly rather than
discovered by introspection, so that quietly dropping one is a test failure
instead of a silent reduction in coverage.

**Status.** All skipped — `pipeline/paths.py` does not exist yet. These *are*
the acceptance criteria for DOO-86.

---

### 6. `test_stale_references.py` — docs and docstrings name only things that exist

**What it does.** Four assertions, covering the documentation half of "was
everything adjusted".

* `test_no_source_file_references_a_dead_module` — no source file mentions a
  module in `DEAD_MODULES` (currently `voter_data_cleaner_v2`, renamed to
  `pipeline/voter_data_cleaner.py`).
* `test_no_source_file_imports_a_dead_module` — the subset that is not
  cosmetic: files that *import* a module which no longer exists.
* `test_documented_python_paths_exist` — every `pipeline/`, `tools/`, `serve/`
  or `docs/` `.py` path named in `README.md`, `CLAUDE.md`, `DATA_QUALITY.md`,
  `GEMINI.md` or `.claude/rules/frontend.md` must exist on disk.
* `test_module_docstrings_do_not_name_missing_files` — the same check applied
  to module docstrings, deduplicated per file.

**Status (2026-11-11).** The first three pass: every `voter_data_cleaner_v2`
import and mention was repointed, and the README rows for the renamed lookup
tools and the removed `archive_state.py` were fixed. The fourth still fails on
four module docstrings that name pre-reorganisation paths.

**Why it exists, concretely.** The `voter_data_cleaner_v2` → `voter_data_cleaner`
rename left references behind across seven files. Three of them are not stale
prose but **broken code**: `tools/export/precinct_party_export.py:44` and
`tools/export/precinct_unc_export.py:39` import the old module at module level,
and `pipeline/voter_data_cleaner.py` (line 4768 by 2026-11) does the same in its `--test` path.
Those two export scripts cannot load at all, and nothing in the existing test
suite touches them — the only surviving artefacts are stale `.pyc` files in
`__pycache__/`, which are not importable without their source.

That matters far beyond tidiness. It is a **worked example of the exact failure
this refactor risks**: a module-boundary change that silently orphaned its
callers, in this codebase, in files that are themselves on the conversion list,
undetected long enough to be forgotten. It also means "run the tests, confirm
the refactor is inert" would prove nothing about them.

**At baseline this test also found genuine documentation rot**, unrelated to
the refactor and worth fixing on its own: `README.md` names three tools that do
not exist (`tools/lookup/voter_lookup.py`, `tools/lookup/raw_voter_lookup.py`,
`tools/admin/archive_state.py`), and several module docstrings still cite
pre-reorganisation paths (`tools/generate_narratives.py`,
`tools/regen_city_summary.py`, `tools/precinct_key_manager.py`,
`pipeline/temporal/snapshot_state.py`).

**Maintaining `DEAD_MODULES`.** Add an entry in the *same commit* as any
rename. That is what turns a rename from a scavenger hunt into a mechanically
enforced migration.

**Scope note.** `local/context/` is deliberately **not** scanned. It is
gitignored session history and is *supposed* to reference files as they were at
the time; policing it would be both wrong and noisy.

**Acknowledged limit.** This verifies that a docstring names things that
*exist*. It cannot verify that a docstring is *accurate* — that a rewritten
function still does what its prose claims. That remains human review, and it is
the one place worth spending a deliberate read-through rather than pretending
automation covers it.

---

### 7. `test_write_fetch_contract.py` — every fetched JSON has a writer

**What it does.** Reduces both sides of the pipeline/frontend contract to the
set of filename *suffixes* they deal in (`_party_by_decade.json`, etc. — the
prefix is a runtime slug on both sides and cannot be compared statically), then
asserts that every suffix `docs/assets/v2.js` fetches is one a pipeline writer
emits.

**Why it exists.** The consolidation splits across four issues — pipeline write
side (DOO-88), narrative read side (DOO-89), validator (DOO-90), frontend
(DOO-91). The handoff's own stated risk is that a half-migrated state breaks the
pipeline and frontend contracts *simultaneously*. This test makes that state
detectable in five seconds instead of in a browser, which is what allows those
four issues to be landed and reverted independently rather than as one
synchronised flip.

**Only one direction is asserted.** Written-but-not-fetched is *reported*, never
failed: the pipeline legitimately writes files consumed by tools rather than by
the browser, and a gate that fires on a legitimate case is a gate people learn
to switch off. `test_report_pipeline_outputs_the_frontend_never_fetches` prints
that side as information.

**Allowlist.** `FETCH_ALLOWLIST` holds the jurisdiction-type `index.json`, the
dashboard `manifest.json`, and the four `state_map/*.geojson` files. The geojson
entries are there because `build_precinct_map.py` and `build_state_map.py`
assemble those filenames at runtime (per chamber, per county), so the literal
never appears in the writer's source and cannot be matched statically — all four
were verified present on disk on 2026-08-01. **Every allowlist entry is a claim
that something outside `pipeline/` produces the file; curate it deliberately.**

**Status.** Green today. Its job is to go red during the migration, at the
precise moment the two halves disagree.

---

## What the detectors catch that a grep does not

This is why the three scope counts differ, and it is the part worth
internalising before trusting any future inventory:

1. **Inline construction inside a function** — `data_dir = BASE_DIR / 'docs' / 'data'`
   in a function body has no module-level constant to grep for. 43 of the 100
   core sites are inline.
2. **`os.path.join`** — the pre-pathlib idiom, still live in
   `build_montgomery_demo.py`. A `/`-operator matcher alone reports a clean tree
   that isn't one.
3. **Bare string literals** — a path in a dict value with no variable name
   at all (`captain_match_report.py:352`). 9 sites.
4. **Paths assembled across statements** — `DOCS = BASE / "docs"` then
   `DATA_DIR = DOCS / "data"`. Neither line contains both tokens, so a
   per-expression matcher sees nothing. `conftest` carries a per-module symbol
   table specifically for this.
5. **False positives a naive matcher produces** — `parser = argparse.ArgumentParser(description='... Parquet files.')`
   and `pl.read_parquet(...)` are not path definitions; `tmp_path / 'parquet'`
   in a fixture is correct code. The detector tests the *top-level* node of the
   assignment and skips pytest temp fixtures, which is what keeps the signal
   usable.

## Files in this directory

| File | Role |
|---|---|
| `conftest.py` | Detectors (`collect_path_constants`, `collect_partition_strings`), family definitions, allowlists, shared fixtures. Stdlib only. |
| `scope_manifest.json` | The machine-readable scope. Derived from the detectors; regenerate deliberately, never hand-edit to silence a test. |
| `test_no_second_copy.py` | Test 1 |
| `test_scope_manifest.py` | Test 2 |
| `test_replacement_landed.py` | Test 3 |
| `test_partition_strings.py` | Test 4 |
| `test_path_invariants.py` | Test 5 |
| `test_stale_references.py` | Test 6 |
| `test_write_fetch_contract.py` | Test 7 |

## What this suite does not cover

Stated so the gaps are visible rather than implied:

* **Output equivalence.** Whether the consolidated JSON carries the same data as
  the six files it replaces is a *behavioural* question needing a separate
  comparator run against real data. Not here.
* **Docstring accuracy.** See test 6's limit above.
* **Frontend runtime behaviour.** Test 7 checks that a writer exists for every
  fetch; it does not check that the payload *shape* is what the render layer
  expects. That is the key-set assertion in the consolidation plan, and it needs
  the new schema to exist first.
* **`local/source/` beyond the four core families.** 54 sites across 23 files
  touch `local/source/` for Geo shapefiles, `precinct_keys` and Official Docs.
  Reported by test 1, deliberately not asserted — asserting would make the gate
  fail for reasons this refactor was never scoped to fix, and a gate that fails
  for out-of-scope reasons is a gate people learn to ignore.
