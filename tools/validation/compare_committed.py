"""Compare a freshly captured county export against the committed docs/data.

THE ORACLE
----------
``docs/data/`` as committed to git IS the baseline. There is no need to run the
old code to produce one -- it is already there, byte for byte, from the last
full pipeline run. That halves every comparison from two runs to one, and it is
the single cheapest accelerator available for this refactor.

The consequence worth stating plainly: this tool answers "does my edit change
the output relative to what is committed?", not "is the output correct?". If
the committed tree was produced by an older version of the code, then the first
thing to establish -- before trusting any result here -- is whether TODAY's
unmodified code reproduces it. Run this on a clean tree first. A clean tree
that already reports differences means the baseline is stale, and every later
comparison is measuring two changes at once.

WHAT IT REPORTS
---------------
Per output file, one of four verdicts:

    MATCH      identical after normalisation
    DIFFER     both exist, content differs (dotted-path diff is shown)
    MISSING    captured now, absent from docs/data/ -- a NEW output
    EXTRA      present in docs/data/, not captured -- a REMOVED output

MISSING and EXTRA are the interesting ones during consolidation: the whole
point of the work is that the set of files changes. They are reported
separately from DIFFER so "the shape changed as designed" never gets confused
with "the numbers changed by accident".

USAGE
-----
    .venv\\Scripts\\python.exe -m tools.validation.compare_committed --county 57
    .venv\\Scripts\\python.exe -m tools.validation.compare_committed --county 57 --county 29
    .venv\\Scripts\\python.exe -m tools.validation.compare_committed --quartet
    .venv\\Scripts\\python.exe -m tools.validation.compare_committed --county 82 --strict

``--quartet`` runs the four counties chosen to exercise the documented
heterogeneity traps rather than merely to be fast; see ``QUARTET`` below.

Exit code is 0 when every file matches, 1 otherwise, so it can gate a commit.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline import voter_data_cleaner as _v  # noqa: E402
from tools.validation.capture import CaptureResult, capture_county, county_slug  # noqa: E402
from tools.validation.normalize import diff, normalise  # noqa: E402

# Counties chosen for coverage of known traps, not for size. Together they cost
# a few percent of a statewide run while exercising every jurisdiction edge
# case this project has already been burned by:
#
#   57 Montgomery -- wards, and Washington Township's straddling precincts
#                    (the tree-node vs hero-total case in CLAUDE.md section 4)
#   29 Greene     -- shares Kettering with Montgomery; the cross-county city
#                    that produced the over-count bug
#   76 Stark      -- WARD='CANTON' township abuse, and Alliance's ward spanning
#                    two counties
#   82 Vinton     -- smallest county; the fast smoke test
QUARTET = ("57", "29", "76", "82")

# Committed outputs produced by writers this harness does not invoke. Excluded
# from the EXTRA scan so that "we no longer emit this file" stays a meaningful
# signal instead of a standing 21-line false positive per county.
NOT_WRITTEN_BY_THIS_HARNESS = ("_narrative.json",)


@dataclass
class FileVerdict:
    """One output file's comparison result.

    Attributes:
        relpath:  Path relative to ``docs/data/``.
        verdict:  'MATCH', 'DIFFER', 'MISSING' or 'EXTRA'.
        details:  Dotted-path differences, populated for DIFFER only.
    """

    relpath: str
    verdict: str
    details: list[str]


@dataclass
class CountyReport:
    """All verdicts for one county, plus the capture metadata behind them."""

    capture: CaptureResult
    verdicts: list[FileVerdict]

    def counts(self) -> dict[str, int]:
        """Verdict name -> number of files with that verdict."""
        out: dict[str, int] = {}
        for verdict in self.verdicts:
            out[verdict.verdict] = out.get(verdict.verdict, 0) + 1
        return out

    @property
    def clean(self) -> bool:
        """True when every captured file matched its committed counterpart."""
        return all(v.verdict == "MATCH" for v in self.verdicts)


def committed_files_for(slug: str, data_dir: Path) -> set[str]:
    """Every committed file belonging to one county, relative to ``data_dir``.

    Matches the flat county-scope and precinct-scope files (``{slug}_*.json``)
    at the top level, minus the outputs of writers this harness does not run:

    * ``*_narrative.json`` -- produced by ``tools/narrative/generate_narratives.py``
      on an independent, LLM-driven cadence. Narrative stays a separate file
      through the consolidation by design, so counting it here would report
      every county's narratives as EXTRA on every run and bury the real
      findings. Verified against Vinton: 21 of 21 spurious EXTRA verdicts were
      narrative files.
    * Jurisdiction-type subdirectories (``ward/``, ``township/`` and the other
      twelve) -- written by ``jurisdictional_groupings`` in a separate pass this
      harness does not invoke. Not globbed at all, for the same reason.

    Args:
        slug:     County slug, e.g. 'montgomery'.
        data_dir: The ``docs/data`` directory.

    Returns:
        Set of file names relative to ``data_dir``.
    """
    if not data_dir.is_dir():
        return set()
    return {
        p.name
        for p in data_dir.glob(f"{slug}_*.json")
        if p.is_file() and not p.name.endswith(tuple(NOT_WRITTEN_BY_THIS_HARNESS))
    }


def compare_county(
    county_number: str,
    *,
    include_precinct_charts: bool = True,
    prefer_cache: bool = True,
    strict: bool = False,
    diff_limit: int = 12,
) -> CountyReport:
    """Capture one county and compare every payload with the committed file.

    Args:
        county_number:           Zero-padded county number.
        include_precinct_charts: Include per-precinct outputs. On for the real
                                 check; off for a fast county-scope-only smoke.
        prefer_cache:            Use the enriched cache fast path.
        strict:                  Disable volatile-field normalisation, so
                                 generation dates count as differences.
        diff_limit:              Maximum differences reported per file.

    Returns:
        A ``CountyReport``.
    """
    capture = capture_county(
        county_number,
        include_precinct_charts=include_precinct_charts,
        prefer_cache=prefer_cache,
    )
    data_dir = Path(_v.DATA_DIR)
    slug = county_slug(capture.county_name)

    verdicts: list[FileVerdict] = []
    seen: set[str] = set()

    for relpath, payload in sorted(capture.payloads.items()):
        seen.add(relpath)
        on_disk = data_dir / relpath
        if not on_disk.exists():
            verdicts.append(FileVerdict(relpath, "MISSING", []))
            continue
        try:
            committed = json.loads(on_disk.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            verdicts.append(FileVerdict(relpath, "DIFFER", [f"could not read committed file: {exc}"]))
            continue
        differences = diff(
            normalise(committed, strict=strict),
            normalise(payload, strict=strict),
            limit=diff_limit,
        )
        verdicts.append(
            FileVerdict(relpath, "MATCH" if not differences else "DIFFER", differences)
        )

    # Files the pipeline used to write for this county and no longer does.
    # Only meaningful when precinct charts were captured; otherwise every
    # precinct file would be reported as removed.
    if include_precinct_charts:
        for name in sorted(committed_files_for(slug, data_dir) - seen):
            verdicts.append(FileVerdict(name, "EXTRA", []))

    return CountyReport(capture=capture, verdicts=verdicts)


def print_report(report: CountyReport, *, show: int = 10, verbose: bool = False) -> None:
    """Print one county's report to stdout.

    Args:
        report:  The report to print.
        show:    Maximum number of non-MATCH files listed in detail. A
                 half-migrated tree produces thousands; the first few carry the
                 diagnosis and the rest are the same story repeated.
        verbose: Also list MATCH files.
    """
    counts = report.counts()
    print()
    print(report.capture.summary())
    print("  " + "  ".join(f"{k}={v}" for k, v in sorted(counts.items())))

    listed = 0
    for verdict in report.verdicts:
        if verdict.verdict == "MATCH" and not verbose:
            continue
        if listed >= show:
            remaining = sum(1 for v in report.verdicts if v.verdict != "MATCH") - listed
            if remaining > 0:
                print(f"  ... and {remaining} more non-matching file(s)")
            break
        print(f"  [{verdict.verdict}] {verdict.relpath}")
        for line in verdict.details:
            print(f"        {line}")
        listed += 1


def main(argv: list[str] | None = None) -> int:
    """CLI entry point. Returns the process exit code."""
    parser = argparse.ArgumentParser(
        description=(
            "Compare freshly captured county output against the committed "
            "docs/data/ tree. Writes nothing."
        )
    )
    parser.add_argument(
        "--county",
        action="append",
        default=[],
        metavar="NN",
        help="County number, repeatable. e.g. --county 57 --county 29",
    )
    parser.add_argument(
        "--quartet",
        action="store_true",
        help=f"Run the heterogeneity quartet: {', '.join(QUARTET)}",
    )
    parser.add_argument(
        "--no-precinct-charts",
        action="store_true",
        help="County-scope only. Much faster; skips the precinct payloads.",
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Load via clean_voter_data instead of the enriched cache. NOT an "
        "equivalence check -- the classifier is not county-decomposable, so this "
        "path legitimately differs from committed output. See README.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Do not normalise volatile fields; generation dates count as differences.",
    )
    parser.add_argument("--show", type=int, default=10, help="Non-matching files to list per county.")
    parser.add_argument("--diff-limit", type=int, default=12, help="Differences shown per file.")
    parser.add_argument("--verbose", action="store_true", help="List matching files too.")
    parser.add_argument(
        "--json",
        type=Path,
        metavar="PATH",
        help="Also write the full report as JSON (use local/, never docs/).",
    )
    args = parser.parse_args(argv)

    counties = list(args.county)
    if args.quartet:
        counties.extend(c for c in QUARTET if c not in counties)
    if not counties:
        parser.error("give at least one --county, or --quartet")

    reports: list[CountyReport] = []
    for county in counties:
        report = compare_county(
            county,
            include_precinct_charts=not args.no_precinct_charts,
            prefer_cache=not args.no_cache,
            strict=args.strict,
            diff_limit=args.diff_limit,
        )
        reports.append(report)
        print_report(report, show=args.show, verbose=args.verbose)

    total: dict[str, int] = {}
    for report in reports:
        for key, value in report.counts().items():
            total[key] = total.get(key, 0) + value
    print()
    print("TOTAL  " + "  ".join(f"{k}={v}" for k, v in sorted(total.items())))

    if args.json:
        args.json.parent.mkdir(parents=True, exist_ok=True)
        args.json.write_text(
            json.dumps(
                {
                    "counties": [
                        {
                            "county_number": r.capture.county_number,
                            "county_name": r.capture.county_name,
                            "source": r.capture.source,
                            "rows": r.capture.rows,
                            "seconds": round(r.capture.seconds, 2),
                            "counts": r.counts(),
                            "files": [
                                {"path": v.relpath, "verdict": v.verdict, "details": v.details}
                                for v in r.verdicts
                                if v.verdict != "MATCH"
                            ],
                        }
                        for r in reports
                    ],
                    "total": total,
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        print(f"report written: {args.json}")

    return 0 if all(r.clean for r in reports) else 1


if __name__ == "__main__":
    raise SystemExit(main())
