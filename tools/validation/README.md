# Validation harness — fast equivalence checking for the consolidation refactor

Three modules that answer one question in seconds instead of minutes: **did my
edit change what the pipeline emits?**

```powershell
cd "D:\vibe\election-data"
.venv\Scripts\python.exe -m tools.validation.compare_committed --county 82
.venv\Scripts\python.exe -m tools.validation.compare_committed --quartet
```

Nothing here writes to `docs/` or `local/source/`. Every pipeline write is
intercepted in memory.

## The two ideas this rests on

**1. The baseline is already in git.** `docs/data/` as committed *is* the
golden output from the last full pipeline run. You never need to run the old
code to produce a comparison baseline — only the new code, diffed against what
is already there. That halves every check from two runs to one.

**2. The expensive part of a pipeline run is writing files, not computing.**
A statewide 88-county group-by over 7.9M rows measures in tenths of a second,
because Polars pushes predicates down through columnar parquet. Writing ~83,000
small JSON files does not. So the harness runs the real export code and
intercepts `_dump_json`, producing the complete output for a county with zero
filesystem I/O.

Together those turn the iteration loop from "regenerate and diff a directory
tree" into "run the exporter in memory and compare dicts".

## Where this sits

| Layer | What it proves | Cost |
|---|---|---|
| `tools/tests/` | Units behave | seconds |
| `tools/tests/conformance/` | The refactor is **complete** — no second copies, no stale docstrings, every file converted | seconds |
| **`tools/validation/`** | The output is **equivalent** to what is committed | seconds |
| Real pipeline run to a scratch dir | File count, byte size, commit speed, human-readable structure | minutes |
| Statewide run | Everything, once, before the final commit | long |

The bottom two tiers cannot be replaced by this harness and should not be
skipped. In particular, **the success criteria you actually care about — fewer
files, faster commits, a readable structure — are only observable in a real run
that writes to disk.** This harness validates that the *content* survived; a
scratch-directory run validates that the *shape* improved.

## Modules

### `capture.py` — run the export, keep the output in memory

`capture_county("57")` returns a `CaptureResult` whose `payloads` maps each
output path (relative to `docs/data/`) to the dict that would have been written
there.

* **It patches rather than reimplements.** Reimplementing payload construction
  here would validate the harness against itself. Instead it calls
  `export_json` and `export_unc_shadow_json` exactly as
  `_export_county_worker` does in production — same order, same arguments,
  same `unc_classified` fast path. If the production sequence changes, this
  must change with it. That coupling is deliberate.
* **Both `_dump_json` functions are patched.** `voter_data_cleaner.py` and
  `jurisdictional_groupings.py` each define their own — itself an instance of
  the duplication this refactor exists to fix.
* **Restoration is in a `finally`.** An exception inside the block still leaves
  the modules untouched, so the harness is safe to run repeatedly in one
  process and safe to use from a notebook or REPL.
* **Writes cannot escape.** Paths outside `docs/data/` are captured under their
  absolute name rather than silently rebased, so a write to an unexpected
  location shows up in the results instead of vanishing. `update_manifest=False`
  is passed regardless, so `docs/manifest.json` is never touched even if
  patching were somehow bypassed.

**The speed note that matters.** `load_county_frame` reads `ENRICHED_CACHE`
with a pushed-down county filter. This is not a micro-optimisation:
`run_county_subset` (`voter_data_cleaner.py:4485-4490`) loads the raw partition
and calls `clean_voter_data` every time, while `run_ohio_analysis`
(`:4368-4375`) reads the enriched cache when fresh and skips cleaning entirely.
The "cheap" single-county entry point therefore pays the full classifier cost
that the statewide path avoids. Which load path was taken is recorded in
`CaptureResult.source`.

### The classifier is not county-decomposable — read this before using `--no-cache`

Measured during the 2026-08-01 rehearsal, and it changes how any regeneration
gate for this refactor must be built.

Running the enriched-cache path for Vinton reproduces the committed output
exactly: **130 of 130 files MATCH**. Running the same county through
`clean_voter_data` instead — which is what `run_county_subset` does — produces
**30 of 130 DIFFER**.

The cause is not the reference date (`get_snapshot_date` is strict and reads the
staged snapshot folder, so both paths share a time origin). It is that
`classify_all_voters_primary_history` counts ballots over the **partisan-capable
primary spine** — the columns where at least one D or R ballot was actually
cast — and that spine is derived from *the frame it is given*:

| Frame | Partisan-capable primaries (of 46) |
|---|---|
| Statewide | **36** |
| Vinton alone | **27** |

Nine primaries — `PRIMARY-09/08/2009`, `09/29/2009`, `07/13/2010`, `09/07/2010`,
`09/13/2016`, `09/12/2017`, `09/10/2019`, `09/14/2021`, `05/06/2025`, mostly
September municipal primaries — had zero D/R ballots *in Vinton* but D/R ballots
elsewhere in Ohio. With the narrower spine, non-partisan ballots on those
columns stop being counted, and exactly **14 voters move from `UNC_NONPARTISAN`
(88 → 74) to `UNC_NO_PRIMARY` (3,079 → 3,093)**. Every other cohort, plus
`PARTY_LABEL` and `Generation`, is byte-identical.

Three consequences:

1. **`--no-cache` is a diagnostic, not an equivalence check.** Its differences
   are expected. Do not treat them as regressions.
2. **A "regenerate one county, expect byte-identical" gate run through
   `run_county_subset` would fail for reasons unrelated to the refactor.** Any
   such gate must go through the enriched cache, as this harness does.
3. **Two production entry points disagree about a county's cohorts.**
   `run_ohio_analysis` and `run_county_subset` are presumed interchangeable and
   are not. Whether the statewide spine is the *correct* one is a real semantic
   question — it means a county's cohort assignments depend on other counties'
   behaviour, which sits awkwardly beside the 88-county heterogeneity law in
   CLAUDE.md §5 — and it deserves its own issue rather than being resolved
   inside a path refactor.

### `normalize.py` — strip volatile fields, then deep-diff

Every payload carries `"updated": "<today>"` and a `"note"` beginning
`"Analysis run <today>"`. Regenerate identical data with identical code
tomorrow and every file differs. Without normalisation a comparison reports
100% mismatch and teaches you nothing — and a check that always fails is a
check people stop running.

**Normalisation is targeted, not a blanket date strip, and that is a safety
property rather than fussiness.** The obvious implementation — regex every
`YYYY-MM-DD` to a placeholder — would be actively dangerous here, because dates
appear in these payloads as *content*: election columns are `PRIMARY-03/07/2000`
style, chart labels carry election dates, and the UNC shadow charts label
datasets by election. A blanket strip would mask a genuine regression where the
wrong election's data landed in a chart. So `VOLATILE_KEYS` is an explicit,
reviewable list, and adding to it is a visible act.

`--strict` disables normalisation entirely. Use it for the final pre-commit
comparison, where even date churn should be visible.

`diff()` reports dotted paths (`chartConfig.datasets[0].data[1]: 2 -> 99`)
rather than dumping structures, because a chart payload contains arrays of
hundreds of integers and an unstructured diff of one is unreadable. It stops
after `limit` differences: a payload that has diverged in shape produces
thousands of leaf differences and the first few carry the diagnosis.

### `compare_committed.py` — the CLI

Four verdicts per file:

| Verdict | Meaning |
|---|---|
| `MATCH` | Identical after normalisation |
| `DIFFER` | Both exist, content differs — dotted-path diff shown |
| `MISSING` | Captured now, absent from `docs/data/` — a **new** output |
| `EXTRA` | Present in `docs/data/`, not captured — a **removed** output |

`MISSING` and `EXTRA` are separated from `DIFFER` on purpose. The consolidation
is *supposed* to change which files exist, and "the shape changed as designed"
must never be confused with "the numbers changed by accident".

Exit code is 0 when everything matches, 1 otherwise, so it can gate a commit.

## The county quartet

`--quartet` runs four counties chosen for coverage of documented traps rather
than for speed. Together they cost a few percent of a statewide run:

| County | Why |
|---|---|
| 57 Montgomery | Wards; Washington Township's straddling precincts (the tree-node vs hero-total case, CLAUDE.md §4) |
| 29 Greene | Shares Kettering with Montgomery — the cross-county city behind the over-count bug |
| 76 Stark | `WARD='CANTON'` township abuse; Alliance's ward spanning two counties |
| 82 Vinton | Smallest county; the fast smoke test |

One county is not enough and eighty-eight is unnecessary. If a fifth is ever
warranted, Lucas is the candidate — it stuffs township names into `WARD` too.

## Read this before trusting a result

**Establish the oracle first.** This tool answers *"does my edit change the
output relative to what is committed?"* — not *"is the output correct?"*. If
`docs/data/` was produced by an older version of the code, then a comparison
after your edit is measuring two changes at once.

So run it on a **clean tree, before any edits**:

```powershell
.venv\Scripts\python.exe -m tools.validation.compare_committed --quartet --json local\working\baseline.json
```

* All `MATCH` → the baseline is live, and every later difference is yours.
* Differences on a clean tree → the committed tree is stale. Rebase the
  baseline (regenerate and commit) *before* starting the refactor, or you will
  spend a day chasing a difference that predates your work.

Two known-legitimate sources of difference on a clean tree, worth recognising
so they are not mistaken for staleness: the enriched cache and the committed
`docs/data/` must come from the same snapshot (both were 2026-07-25 when this
was written), and `--no-cache` runs re-derive the classifier, which should
agree with the cache but is worth confirming once rather than assuming.

## Scope limits

* **Jurisdiction subdirectories are not compared.** `ward/`, `township/` and
  the other twelve are written by `jurisdictional_groupings` in a separate pass
  this harness does not invoke. `capture_writes` *would* capture them if a
  caller reached them, but `compare_county` deliberately does not scan those
  directories for `EXTRA` files, or the entire subtree would be reported as
  removed on every run. Extending to jurisdiction scope means adding the
  groupings call to `capture_county` — do it when DOO-88 touches that file.
* **Content only, not shape.** File count, byte totals and commit speed need a
  real run. See the tier table above.
* **Correctness is not equivalence.** A bug faithfully preserved from the
  committed tree reports `MATCH`. This gates *regressions*, not *defects*.
## Rehearsal results, 2026-08-01

Run against the real repo before any refactor began, read-only:

| Check | Result |
|---|---|
| `--county 82` (Vinton, 8,021 rows) | **130 of 130 MATCH**, 1.6s, exit 0 |
| Load from enriched cache | 1.9s for the county slice |
| `--strict` | 129 DIFFER, all date churn only — normalisation is doing exactly its job and nothing more |
| `--no-cache` | 30 DIFFER — see the county-decomposability section above |
| Spurious `EXTRA` verdicts | 21 on first run, all `_narrative.json`; fixed by `NOT_WRITTEN_BY_THIS_HARNESS` |

**The committed `docs/data/` tree is a live oracle.** Today's unmodified code
reproduces it exactly for Vinton, so the baseline does not need rebasing before
the refactor starts, and every difference from here is yours. Confirm this on
the quartet before relying on it — Vinton is one county of 88 and the smallest.

Also verified: `normalize.py` ignores date churn while catching value changes,
label changes (`Pure R` → `REP`, which is what a legacy fallback firing would
look like), and key removals; `capture_writes` restores both patched modules
cleanly, including after an exception, and captures writes aimed outside
`docs/data/` under their absolute path rather than silently rebasing them.
