"""Normalisation of volatile fields, so a diff shows real differences only.

WHY THIS EXISTS
---------------
Every chart payload the pipeline writes carries the date it was generated:

    "updated": "2026-07-25"
    "note":    "Analysis run 2026-07-25 - Ohio Secretary of State SWVF voter file"

Regenerate the same county tomorrow from identical data and identical code and
every single file differs. Without normalisation a comparison run reports
100% mismatch and teaches you nothing, so the natural next step is to stop
running it -- which is how an equivalence check quietly becomes decorative.

WHY IT IS TARGETED AND NOT A BLANKET DATE STRIP
-----------------------------------------------
The obvious implementation -- regex every ``\\d{4}-\\d{2}-\\d{2}`` in every
string to a placeholder -- is wrong here, and dangerously so. Dates appear in
this project's payloads as *content*, not just as metadata: election column
names are ``PRIMARY-03/07/2000`` style, chart labels carry election dates, and
the UNC shadow charts label datasets by election. A blanket strip would mask a
genuine regression where the wrong election's data landed in a chart.

So normalisation is keyed to an explicit, reviewable list of volatile field
names (``VOLATILE_KEYS``) plus one prefix rule for the ``note`` field. Adding a
key to that list is a deliberate act that says "this field is expected to
change on every run", and it should be visible in a diff when someone does it.

STRICT MODE
-----------
``normalise(payload, strict=True)`` disables all of it. Use strict mode for the
final pre-commit comparison, where you want to see even the date churn, and
non-strict for the fast iteration loop.
"""

from __future__ import annotations

import re
from typing import Any

# Field names whose value is expected to differ on every run purely because
# time passed. Anything not on this list is compared verbatim.
VOLATILE_KEYS: frozenset[str] = frozenset(
    {
        "updated",       # date_t.today().isoformat() stamped into every chart payload
        "generated",     # used by some index/manifest writers
        "generated_at",
        "built",
        "build_date",
        "snapshot_date_stamped",
    }
)

PLACEHOLDER = "<NORMALISED>"

# The 'note' field is not wholly volatile -- it carries real content after the
# date prefix ("... - Ohio Secretary of State SWVF voter file", "... - Pew
# Research Center generational boundaries"). Only the leading "Analysis run
# <date>" segment is stripped, so a change to the substantive tail still shows
# up as a difference.
_ANALYSIS_RUN_PREFIX = re.compile(r"^Analysis run \d{4}-\d{2}-\d{2}")


def normalise_value(key: str, value: Any) -> Any:
    """Normalise one key/value pair. Returns the value unchanged unless the key
    is known-volatile or the value is a ``note`` carrying a run-date prefix.

    Args:
        key:   The JSON object key this value was found under.
        value: The value itself.

    Returns:
        The value, or a normalised stand-in for it.
    """
    if key in VOLATILE_KEYS:
        return PLACEHOLDER
    if key == "note" and isinstance(value, str):
        return _ANALYSIS_RUN_PREFIX.sub("Analysis run " + PLACEHOLDER, value)
    return value


def normalise(obj: Any, *, strict: bool = False) -> Any:
    """Recursively normalise a decoded JSON payload.

    Args:
        obj:    A dict, list, or scalar decoded from JSON (or captured from the
                pipeline before serialisation -- both shapes work, which is the
                point: the capture harness yields dicts and the committed file
                yields dicts, so they compare directly).
        strict: When True, return the object unchanged. Volatile fields then
                participate in the comparison like any other, which is what you
                want for a final pre-commit check.

    Returns:
        A new structure with volatile fields replaced. The input is not mutated.
    """
    if strict:
        return obj
    if isinstance(obj, dict):
        return {k: normalise(normalise_value(k, v), strict=strict) for k, v in obj.items()}
    if isinstance(obj, list):
        return [normalise(v, strict=strict) for v in obj]
    return obj


def diff(
    expected: Any,
    actual: Any,
    *,
    path: str = "",
    limit: int = 40,
) -> list[str]:
    """Deep-compare two decoded JSON structures and describe the differences.

    Produces dotted paths into the structure rather than a raw dump, because a
    chart payload is a nested object with arrays of hundreds of integers and an
    unstructured diff of one is unreadable.

    Args:
        expected: The committed payload (the baseline / oracle).
        actual:   The freshly captured payload.
        path:     Internal -- the dotted path accumulated so far.
        limit:    Stop after this many differences. A payload that has genuinely
                  diverged in shape produces thousands of leaf differences, and
                  the first few are the informative ones.

    Returns:
        A list of human-readable difference descriptions, empty when equal.
    """
    out: list[str] = []

    def _walk(exp: Any, act: Any, at: str) -> None:
        if len(out) >= limit:
            return
        if type(exp) is not type(act) and not (
            isinstance(exp, (int, float)) and isinstance(act, (int, float))
        ):
            out.append(f"{at or '<root>'}: type {type(exp).__name__} -> {type(act).__name__}")
            return
        if isinstance(exp, dict):
            for key in sorted(set(exp) | set(act)):
                sub = f"{at}.{key}" if at else key
                if key not in exp:
                    out.append(f"{sub}: added (value {act[key]!r})")
                elif key not in act:
                    out.append(f"{sub}: removed (was {exp[key]!r})")
                else:
                    _walk(exp[key], act[key], sub)
                if len(out) >= limit:
                    return
            return
        if isinstance(exp, list):
            if len(exp) != len(act):
                out.append(f"{at}: length {len(exp)} -> {len(act)}")
                return
            for i, (e, a) in enumerate(zip(exp, act)):
                _walk(e, a, f"{at}[{i}]")
                if len(out) >= limit:
                    return
            return
        if exp != act:
            out.append(f"{at or '<root>'}: {exp!r} -> {act!r}")

    _walk(expected, actual, path)
    if len(out) >= limit:
        out.append(f"... (stopped at {limit} differences)")
    return out
