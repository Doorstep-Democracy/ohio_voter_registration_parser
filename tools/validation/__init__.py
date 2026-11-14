"""Validation harness for the docs/data consolidation refactor.

This package holds read-only tooling that answers one question quickly:
*does the pipeline still emit the same content it emitted before my edit?*

It is deliberately separate from three neighbouring things:

* ``tools/tests/`` -- behavioural unit tests, run by pytest, no real data.
* ``tools/tests/conformance/`` -- structural gates proving the refactor is
  COMPLETE (every file converted, no second copies, no stale docstrings).
* ``tools/admin/validate_jurisdiction_fields.py`` -- the data-quality gate run
  on every new SWVF drop, which validates the DATA, not a code change.

This package validates EQUIVALENCE: new code against committed output. See
``tools/validation/README.md`` for the full rationale and the tiered ladder it
is meant to sit in.

Nothing here writes to ``docs/`` or to ``local/source/``. The capture harness
intercepts every write in memory.
"""
