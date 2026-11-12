# KNOWN_ISSUES.md — tracked engineering issues

Not a data-heterogeneity ledger (see `DATA_QUALITY.md` for per-county SWVF
deviations). This file tracks defects and architectural limitations in the
pipeline/tooling code itself — the kind of thing a contributor cloning this
repo needs to know before relying on a given code path.

---

## OPEN-001 — Full-state analysis path loads the entire SWVF eagerly; no memory floor documented or enforced

**Status:** open, undiagnosed root cause for the triggering incident; fix direction undecided.
**Area:** `pipeline/voter_data_cleaner.py` — `load_voter_files_parquet()`, `clean_voter_data()`.
**First observed:** 2026-10-30.

### What happens

`load_voter_files_parquet()` ([voter_data_cleaner.py:742](pipeline/voter_data_cleaner.py#L742))
reads the full statewide Hive-partitioned Parquet cache eagerly:

```python
df = pl.read_parquet(str(pdir) + '/**/*.parquet', hive_partitioning=True)
```

This materializes all 88 counties — ~8.0M rows x 137 columns — into a single
resident `DataFrame` in one call, rather than scanning lazily
(`pl.scan_parquet`) and pushing a per-county filter down, or looping per
county the way the Hive partition layout is structured to support.
`clean_voter_data()` ([voter_data_cleaner.py:833](pipeline/voter_data_cleaner.py#L833))
then runs a full column-derivation pass (Generation, Decade, jurisdiction
resolution, etc.) against that entire in-memory frame — each derived column
adds a new buffer on top of the existing resident set, so peak RSS during
cleaning runs measurably higher than the post-load baseline.

This is in tension with the "out-of-core" framing `CLAUDE.md` §2 uses for the
Polars/DuckDB stack choice — for the full-state path specifically, the code
is not out-of-core.

### Triggering incident (2026-10-30, menu choice [1])

- Parquet load completed in 4.5s; log recorded `RSS=20,896 MB` immediately
  after ([local/logs/voter_analysis_20261030_010610_pipeline.log](local/logs/voter_analysis_20261030_010610_pipeline.log)).
- `clean_voter_data()` started, logged one `WARNING` (54 dropped invalid-birth-year
  rows) at `01:06:17`, then the process terminated with **no further log
  output, no Python traceback, and no `python.exe` process left running.**
- Windows Event Viewer showed **no trace of a cause**: no
  `Microsoft-Windows-Resource-Exhaustion-Detector` event, no Application
  Error/Hang record for `python.exe`, no Windows Defender operational-log
  action, no Kernel-Power sleep/shutdown event anywhere near `01:06:17`.
  Both the PowerShell host and VS Code stayed up throughout — so the kill
  was not a terminal/IDE crash either.
- The operating machine had recently had RAM downgraded 64 GB → 48 GB
  (slower clocks too) pending an RMA reinstall — raised as a plausible
  contributing factor, but **not confirmed**: a genuine Windows memory-pressure
  kill normally leaves a Resource-Exhaustion-Detector event, and none was
  found. Root cause of this specific silent exit remains unresolved.

### Why it matters beyond this one machine

Independent of what killed this particular run, the eager full-state load is
a real portability concern for this repo being public on GitHub: anyone
cloning it on a 16–32 GB machine and running the default menu choice (full
statewide rebuild) is working with a materially different memory budget than
the ~20.9 GB-plus-headroom this pipeline was built and tuned against, with no
documented minimum-RAM requirement and no guard that fails loudly if the
budget isn't there.

### Flagged directions (not yet decided or scoped)

1. **Louder failure state.** Per `CLAUDE.md` §5's own "loud failures over
   silent degradation" principle, a resource-exhaustion exit should not be
   indistinguishable from a clean one. Worth a watchdog/heartbeat or an
   explicit memory check that turns a silent kill into a raised, logged
   `MemoryError`-equivalent.
2. **Real memory management for the full-state path.** Switch
   `load_voter_files_parquet()` to `pl.scan_parquet()` + a per-county (or
   chunked) processing loop for the statewide case, so peak resident memory
   stops scaling with all 88 counties at once — consistent with the
   partition layout `build_parquet_cache()` already writes.
3. **System-configuration-aware strategy selection.** Detect available RAM
   at startup and choose eager-whole-state vs. chunked-per-county
   automatically (with a visible warning when falling back to the slower
   path), rather than assuming one fixed hardware profile.

None of these have been scoped into an implementation plan yet; this entry
exists so the tradeoff is visible before the next full-state run is
attempted on a smaller machine.

### Follow-up (2026-10-30, after RAM restored)

Same full-state run (menu choice [1]), same machine, RAM reinstalled. This
time `clean_voter_data()` completed the stage that previously died silently —
85.5s, peak `RSS=25,686 MB` — and the pipeline proceeded past it to writing
the enriched cache and computing participation metrics. This is
**corroborating, not conclusive** evidence for the memory-pressure
hypothesis: the failure didn't recur once headroom was restored, but Windows
still never logged a cause for the original kill, so the exact mechanism
stays unconfirmed. Doesn't change the flagged directions above — the eager
full-state load is still the structural issue regardless of what killed this
one run.

### Second incident (2026-11-11, menu choice [1], headless)

Command: `ohio_voter_pipeline.py --snapshot 2026-10-24 --choice 1
--precinct-charts --no-update-check`, branch `fix/lean-by-label`. Polars 1.40.1
and Python 3.14.4 unchanged since 2026-05-14.

- Log ([local/logs/voter_analysis_20261111_084303_pipeline.log](local/logs/voter_analysis_20261111_084303_pipeline.log))
  ends at `08:43:21` on `add_voter_participation` "Building participation
  metric expressions", RSS ~21 GB on entry. On the successful 10-30 run this
  step peaked at `RSS=39,892 MB`.
- Unlike the first incident, Windows **did** record a cause: Application
  Error 1000 at `08:43:30`, `python.exe` faulting in `VCRUNTIME140.dll`,
  exception `0xc0000005` (access violation). A native crash: no Python
  traceback, no log line, and the shell prompt returns as if the run had
  finished quickly. Nothing was written to `docs/data`.
- An immediate rerun with `python -X faulthandler` got past the same point.
  Intermittent, not reproduced; mechanism unconfirmed (memory pressure during
  the ~2x RSS growth in this step and a native race are both open).

Adds to the flagged directions:

4. **Crash-visible default logging.** Enable `faulthandler` by default
   (in the pipeline entry point, writing to the run's log file, not only
   stderr), and log a final "pipeline complete" line so a missing one
   identifies an abnormal exit. Today a native crash is indistinguishable
   from a short successful run.
5. **Memory profile of `add_voter_participation`.** It roughly doubles
   resident memory (21 GB -> 40 GB); candidate for the same lazy / per-county
   treatment as direction 2.

**Direction 4 done (2026-11-11, branch `fix/lean-by-label`).** Every run log now
starts with run context (versions, git commit, total/available RAM), receives
`faulthandler` tracebacks on a native crash (verified with a deliberate
segfault), logs each enriched-cache FRESH/STALE verdict with its timestamps,
and ends with `PIPELINE COMPLETE`. Rationale lives in the
`voter_data_cleaner.setup_logging()` docstring. Directions 1-3 and 5 remain
open.

### Third incident (2026-11-11 ~09:36, system bugcheck after a completed run)

Menu choice [4] logged `PIPELINE COMPLETE` at `09:36:23` (groupings peak
`RSS=49.76 GB` of 68.6 GB total); the machine bugchecked and rebooted by
`09:37:14`: stop `0x12B` FAULTY_HARDWARE_CORRUPTED_PAGE, minidump
`C:\Windows\Minidump\111126-12828-01.dmp`, no WHEA-Logger events. The last 40
files written (municipal/county court and appeals narratives) were left
zero-filled by the lost write cache and were regenerated.

The System log holds **11 bugchecks since 2026-09-03** with varied stop codes,
most memory-related: `0x1A` MEMORY_MANAGEMENT, `0x4E` PFN_LIST_CORRUPT, `0x19`
BAD_POOL_HEADER, `0xC2` BAD_POOL_CALLER (x2), `0x12B`, plus `0x7E`/`0x1E`/`0x50`
access violations, `0x133`, `0xEF`. Varied memory-corruption codes are
consistent with unstable RAM or memory timings (the RAM was swapped and
reinstalled around the RMA noted above) and also with a faulty driver; not
diagnosed. A future session should start with a memory test (MemTest86 or
Windows Memory Diagnostic, XMP off) before attributing any pipeline crash to
the code, then verify outputs after every full run, since a crash right after
a run can zero-fill files the log reports as written.
