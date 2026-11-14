"""
Filename/slug hygiene across generated docs/data/city/ chart files.

Catches the class of bug found 2026-07: a fragile slugify() producing a
double underscore (e.g. "columbus_city__decade_distribution.json") or a
raw punctuation character (e.g. "washington_c.h._city_party_affiliation.json")
that the frontend's own slug functions (cityNameToSlug/countyToSlug in
docs/assets/v2.js, _precinct_safe_name in voter_data_cleaner.py) would
never generate. The frontend requests the clean slug, the mismatched file
just 404s, and the jurisdiction's whole page silently shows "No summary
data found" with no error surfaced anywhere else.

Scoped to docs/data/city/ only, not the whole docs/data/ tree: several
other jurisdiction types (local_school_district/, exempted_village_
school_district/, ...) use a distinct, pre-existing "name_(county)"
filename convention (thousands of files) that predates this check and is
a separate, out-of-scope concern — see the PR discussion. Widening this
test to cover those without first confirming whether the frontend
actually depends on that convention would make CI red for unrelated
reasons.

Deliberately does not depend on docs/manifest.json or docs/index.htm —
this only reads already-generated, already-public docs/data/city/ files,
so it needs no PII and no pipeline run to check. A single aggregating
test (rather than one test per file) since docs/data/ as a whole holds
tens of thousands of files across 88 counties; per-file parametrization
would make this check itself the slow part of CI.
"""
import re

from pipeline.paths import DATA_DIR

CITY_DATA = DATA_DIR / "city"

_VALID_SLUG_RE = re.compile(r'^[a-z0-9_]+$')


def _all_data_json_files():
    if not CITY_DATA.exists():
        return []
    return sorted(CITY_DATA.glob("*.json"))


def test_no_data_filenames_have_naming_bugs():
    double_underscore = []
    bad_chars = []
    for path in _all_data_json_files():
        stem = path.stem
        if "__" in stem:
            double_underscore.append(str(path.relative_to(CITY_DATA)))
        elif not _VALID_SLUG_RE.match(stem):
            bad_chars.append(str(path.relative_to(CITY_DATA)))

    problems = []
    if double_underscore:
        problems.append(
            f"{len(double_underscore)} file(s) with a double underscore "
            f"(likely un-stripped whitespace in the source name): "
            + ", ".join(double_underscore[:10])
            + (" ..." if len(double_underscore) > 10 else "")
        )
    if bad_chars:
        problems.append(
            f"{len(bad_chars)} file(s) with characters outside [a-z0-9_] "
            "(won't match the frontend's own slug functions): "
            + ", ".join(bad_chars[:10])
            + (" ..." if len(bad_chars) > 10 else "")
        )
    assert not problems, "\n".join(problems)
