"""
tools/narrative/llm_enricher.py
═══════════════════════════════════════════════════════════════════════════════
LLM-powered narrative enrichment for precinct captain briefs.

PURPOSE
───────
The deterministic template system (templates.py) produces statistically
accurate but clinical prose. This module takes those same structured
metrics and rewrites them into plain-English "neighbor briefings" — the
kind of thing a precinct captain can read on their phone before knocking
on a door and immediately understand who they're talking to.

Example of the difference:

  TEMPLATE:  "Anderson A Precinct has 614 registered voters, with 62.2%
              unaffiliated or lacking primary history. Republican-leaning
              voters make up 23.5% of registrants and Democratic-leaning
              voters account for 28.2%..."

  LLM:       "Just over 6 in 10 of your neighbors haven't picked a party
              primary — they're genuinely persuadable. Democrats have a
              small edge, but the real opportunity is that younger voters
              here lean noticeably bluer than longtime residents, which
              means turnout among newer arrivals is your best lever."

ARCHITECTURE
────────────
The module has two code paths:

  1. enrich_one(metrics) → str
       Synchronous single-jurisdiction call. Fast, simple. Good for
       on-demand enrichment of one precinct or county at a time (e.g.
       an API endpoint serving the dashboard).

  2. enrich_batch(metrics_list) → dict[slug, str]
       Asynchronous batch enrichment via the Anthropic Batch API.
       Submits up to 100,000 requests in one shot, polls until done,
       and returns a slug→narrative map. Used by the pipeline for
       bulk generation. Cost is 50% of real-time pricing.

Both paths accept the same dict shape produced by build_metrics_for_level()
and return the same plain-English string that the pipeline stores under the
"narrative_captain" key in each *_narrative.json file.

CACHING
───────
The cache key is the same metrics_hash already used by generate_narratives.py.
If an existing narrative JSON already has a "narrative_captain" field whose
"captain_metrics_hash" matches the current hash, we skip the API call. This
means the LLM is only invoked when the underlying data has actually changed
— not on every pipeline run.

GRACEFUL DEGRADATION
────────────────────
If the Anthropic API key is missing, the model call fails, or the module
is imported in an environment without the anthropic package, every function
silently returns None. The caller (generate_narratives.py) then falls back
to the templated narrative. This means the LLM path is purely additive
and never blocks a pipeline run.

MODEL CHOICE
────────────
We use claude-haiku-4-5 for single calls and batch submissions. Rationale:

  • Input to the model is tiny: ~200 tokens of structured metrics.
  • Output is short: 2-4 sentences (~80-120 tokens).
  • The task is straightforward prose rewriting, not reasoning.
  • Ohio has ~10,000+ precincts across 88 counties; Haiku's cost
    advantage (~5× cheaper than Opus) matters at that scale.
  • Haiku 4.5 supports streaming, prompt caching, and the Batch API.

The system prompt is cached via prompt_caching so repeated calls within
the cache TTL (5 minutes default) only pay ~0.1× for the system content.

PRIVACY / PII GUARANTEE
───────────────────────
The metrics dict passed to the LLM contains ONLY aggregate statistics —
percentages, voter counts, trend direction, and jurisdiction names. It is
derived from the same data that the template engine uses, which is already
fully anonymized before it reaches this layer. No individual voter data,
addresses, names, or SOS voter IDs are ever sent to the API.

USAGE (programmatic)
────────────────────
  from tools.narrative.llm_enricher import enrich_one, enrich_batch

  # Single jurisdiction (sync)
  captain_text = enrich_one(metrics)

  # Full county batch (async, returns when all done)
  import asyncio
  results = asyncio.run(enrich_batch(metrics_list))

USAGE (CLI — dry-run a single precinct)
───────────────────────────────────────
  python -m tools.narrative.llm_enricher --slug hamilton_precinct_addyston_a

ENVIRONMENT
───────────
  ANTHROPIC_API_KEY   Required for all LLM calls. If absent, functions
                       return None silently.

DEPENDENCIES
────────────
  anthropic >= 0.40.0   (pip install anthropic)
  The rest of the imports are stdlib or already used by the pipeline.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

# ── Model and version constants ───────────────────────────────────────────────
# We freeze the model string here so a bump to a newer Haiku automatically
# invalidates any cached captain narratives (the model name flows into the
# cache key via ENRICHER_VERSION).
_MODEL = "claude-haiku-4-5"

# Bump this string whenever you change the system prompt, user prompt
# template, or model. That causes captain_metrics_hash values written by
# the old version to no longer match, triggering automatic re-generation
# on the next pipeline run (same pattern as TEMPLATE_VERSION in templates.py).
ENRICHER_VERSION = "v1"

# ── Prompt constants ──────────────────────────────────────────────────────────
# The system prompt is cached via the Anthropic prompt-caching feature
# (cache_control on the last system content block). Because the system prompt
# never changes across requests in a given process, the first call pays the
# cache-write premium (~1.25×) and all subsequent calls within the 5-minute
# TTL pay only ~0.1×. This is especially valuable during pipeline batch runs
# that submit thousands of single-call requests in a tight loop.

_SYSTEM_PROMPT = """\
You are a data interpreter who briefs precinct captains — local political \
volunteers who knock on doors and make phone calls to their neighbors. You \
receive structured voter registration statistics for a specific jurisdiction \
(precinct, county, city, or district in Ohio) and write a plain-English \
neighborhood briefing.

RULES:
1. Write exactly 2-3 sentences. No bullet points. No headers.
2. Speak to the captain directly, as if you are handing them a note before \
they go out. Use "your neighbors," "your precinct," "your community."
3. Translate statistics into human terms: instead of "UNC – No Primary: 54%," \
say "more than half of your neighbors have never picked a party primary."
4. Highlight the single biggest opportunity for a Democratic organizer: \
persuadable unaffiliated voters, a generational shift, or an untapped base.
5. If the precinct leans Republican, be honest — but still identify the best \
opening (e.g., unaffiliated voters, young registrants trending blue).
6. Never use jargon: no "cohort," "net lean," "UNC," "lapsed," "Pure D/R." \
Use plain words: "neighbors who've never voted in a primary," "long-time \
Republicans," "younger voters."
7. Never invent facts not in the data. If a trend is absent, don't mention one.
8. Do not start with "In" or "The." Start with a vivid, direct observation."""

# The user prompt template is a Python f-string. We extract the most
# captain-relevant facts from the metrics dict and present them clearly.
# We deliberately omit raw percentages for sub-cohorts (Pure R, Mixed-Active,
# etc.) that would require explanation — the LLM gets the high-level picture
# it needs to write good briefings without data overload.
_USER_PROMPT_TEMPLATE = """\
Jurisdiction: {name} ({level}{parent_str})
Total registered voters: {total_voters:,}
Democratic-leaning voters: {d_lean_pct}%
Republican-leaning voters: {r_lean_pct}%
Voters with no party primary on record: {unc_pct}%
Net partisan lean: {net_lean:+.1f} percentage points (D−R){trend_str}{small_n_warning}

Write the captain's briefing now."""


# ── Hash helper ───────────────────────────────────────────────────────────────
def captain_hash(metrics: dict) -> str:
    """
    Produce a 16-character hex hash that uniquely identifies a metrics dict
    combined with the current enricher version.

    This is used as the "captain_metrics_hash" field stored in the output JSON.
    If the data changes (pipeline re-run with new SWVF source) or we bump
    ENRICHER_VERSION (new prompt, new model), the hash changes and the next
    pipeline run will re-generate the LLM narrative.

    The hash intentionally excludes the resulting narrative text — we're
    hashing the *inputs* to detect staleness, not the *output*.
    """
    payload = {
        "metrics": metrics,
        "enricher_v": ENRICHER_VERSION,
        "model": _MODEL,
    }
    blob = json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()[:16]


# ── Prompt builder ────────────────────────────────────────────────────────────
def _build_user_prompt(metrics: dict) -> str:
    """
    Convert a metrics dict (as returned by build_metrics_for_level) into
    the plain-text user prompt we send to the LLM.

    We purposely extract only the data points that are relevant for a
    captain-facing briefing. Deep sub-cohort percentages (Pure R vs
    Mixed-Lapsed etc.) are not included — the captain briefing needs the
    30-second summary, not the full data table.
    """
    p = metrics["party"]
    level = metrics.get("level", "jurisdiction")
    parent = metrics.get("parent_county")

    # Build the optional parent county parenthetical, e.g. " in Hamilton County"
    parent_str = f" in {parent} County" if parent and level not in ("county",) else ""

    # Build the optional generational trend sentence.
    # We only include this if the templates layer already decided it was
    # statistically meaningful (abs(younger - older) >= 1.5 pp).
    trend = metrics.get("trend")
    if trend:
        direction = trend["direction"]  # "bluer" or "redder"
        younger = trend["younger"]      # net D-R lean for newest cohort
        older = trend["older"]          # net D-R lean for older cohort
        trend_str = (
            f"\nGenerational trend: registration is getting {direction} — "
            f"newer registrants lean {younger:+.1f}% D−R vs {older:+.1f}% "
            f"for the 1950s–60s cohorts."
        )
    else:
        trend_str = ""

    # Flag small precincts so the model doesn't over-interpret noisy numbers.
    small_n_warning = (
        "\n⚠ Small sample — interpret with caution." if metrics.get("small_n") else ""
    )

    return _USER_PROMPT_TEMPLATE.format(
        name=metrics["name"],
        level=level,
        parent_str=parent_str,
        total_voters=metrics["total_voters"],
        d_lean_pct=p["d_lean_pct"],
        r_lean_pct=p["r_lean_pct"],
        unc_pct=p["unc_pct"],
        net_lean=p["net_lean"],
        trend_str=trend_str,
        small_n_warning=small_n_warning,
    )


# ── API client factory ────────────────────────────────────────────────────────
def _get_client():
    """
    Return an Anthropic client, or None if the package is not installed
    or the API key is missing.

    We do the import lazily here rather than at module top-level so that
    importing llm_enricher in environments without anthropic installed
    (e.g., during templated-only pipeline runs) does not raise ImportError.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        log.debug("ANTHROPIC_API_KEY not set — LLM enrichment disabled.")
        return None
    try:
        import anthropic  # noqa: PLC0415 — intentional lazy import
        return anthropic.Anthropic(api_key=api_key)
    except ImportError:
        log.debug("anthropic package not installed — LLM enrichment disabled.")
        return None


# ── Single-jurisdiction synchronous enrichment ────────────────────────────────
def enrich_one(metrics: dict) -> Optional[str]:
    """
    Generate a captain-facing plain-English briefing for one jurisdiction.

    Args:
        metrics: The dict returned by build_metrics_for_level(). Must have at
                 minimum: name, level, total_voters, party{d_lean_pct,
                 r_lean_pct, unc_pct, net_lean}.

    Returns:
        A 2-3 sentence plain-English string, or None if the API call could
        not be completed (missing key, network error, etc.).

    The system prompt is marked with cache_control so that repeated calls
    within the 5-minute Anthropic cache TTL are billed at ~0.1× the normal
    input token rate for the cached portion.

    This function is intentionally synchronous. The pipeline calls it from
    a ThreadPoolExecutor (see ohio_voter_pipeline.py), which means the
    GIL is released during the network I/O and multiple precincts can be
    enriched concurrently without needing async code.
    """
    client = _get_client()
    if client is None:
        return None

    user_prompt = _build_user_prompt(metrics)

    try:
        response = client.messages.create(
            model=_MODEL,
            max_tokens=256,  # 2-3 sentences is well under 256 tokens
            system=[
                {
                    "type": "text",
                    "text": _SYSTEM_PROMPT,
                    # Cache the system prompt. On the first call this costs
                    # ~1.25× but every call within 5 minutes after pays ~0.1×.
                    # At pipeline scale (thousands of precincts per run) this
                    # pays back quickly.
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            messages=[{"role": "user", "content": user_prompt}],
        )
        # Extract the text from the first content block. The model is
        # instructed to produce only plain text (no tool use, no thinking).
        text_blocks = [b.text for b in response.content if b.type == "text"]
        if not text_blocks:
            log.warning("LLM returned no text blocks for %s", metrics.get("name"))
            return None

        captain_text = text_blocks[0].strip()
        log.debug(
            "Enriched %s/%s (tokens in=%d out=%d)",
            metrics.get("level"),
            metrics.get("name"),
            response.usage.input_tokens,
            response.usage.output_tokens,
        )
        return captain_text

    except Exception as exc:  # broad catch — never block the pipeline
        log.warning(
            "LLM enrichment failed for %s/%s: %s",
            metrics.get("level"),
            metrics.get("name"),
            exc,
        )
        return None


# ── Batch enrichment ──────────────────────────────────────────────────────────
def enrich_batch(
    metrics_list: list[dict],
    *,
    poll_interval_s: int = 30,
    max_wait_s: int = 3600,
) -> dict[str, str]:
    """
    Submit a batch of jurisdiction metrics to the Anthropic Batch API and
    return a mapping of {slug → captain_narrative_text}.

    The Batch API processes requests asynchronously at 50% of real-time
    pricing. For a full Ohio run (~10,000 precincts) this reduces LLM cost
    significantly compared to individual synchronous calls.

    Args:
        metrics_list: List of metrics dicts. Each must contain a "slug" key
                      (the jurisdiction's filesystem slug, e.g.
                      "hamilton_precinct_addyston_a"). This is used as the
                      batch custom_id so we can map results back.
        poll_interval_s: How often (seconds) to poll for batch completion.
                         Default 30s — the API docs suggest batches complete
                         within 1 hour but often much faster.
        max_wait_s: Maximum total seconds to wait before giving up. Default
                    3600 (1 hour). Callers that need a hard deadline can
                    lower this; the function returns whatever results are
                    available at timeout.

    Returns:
        Dict mapping slug → captain narrative text for every successfully
        processed request. Slugs that errored or timed out are absent from
        the dict; callers should fall back to the templated narrative.

    Raises:
        Nothing — all exceptions are caught and logged. Returns empty dict
        on total failure.

    Note on the "slug" key convention:
        The metrics dict produced by build_metrics_for_level() does NOT
        include a slug field — it's a pure data container. Callers of
        enrich_batch() must inject the slug before passing in, e.g.:
            metrics["slug"] = entry["slug"]
        The generate_narratives.py integration does this automatically.
    """
    client = _get_client()
    if client is None:
        return {}

    if not metrics_list:
        return {}

    # ── Import batch-specific types lazily ────────────────────────────────────
    try:
        from anthropic.types.message_create_params import MessageCreateParamsNonStreaming
        from anthropic.types.messages.batch_create_params import Request as BatchRequest
    except ImportError as exc:
        log.warning("Could not import Anthropic batch types: %s", exc)
        return {}

    # ── Build batch request list ───────────────────────────────────────────────
    # Each BatchRequest has a custom_id (the slug) and a params dict that
    # mirrors what we'd pass to messages.create() in the sync path.
    requests = []
    for m in metrics_list:
        slug = m.get("slug")
        if not slug:
            log.warning("metrics entry missing 'slug' key — skipping in batch")
            continue

        requests.append(
            BatchRequest(
                custom_id=slug,
                params=MessageCreateParamsNonStreaming(
                    model=_MODEL,
                    max_tokens=256,
                    system=[
                        {
                            "type": "text",
                            "text": _SYSTEM_PROMPT,
                            # Prompt caching applies to Batch API too.
                            # Each item in the batch that shares the same
                            # system prefix will hit the cache after the
                            # first few items are processed.
                            "cache_control": {"type": "ephemeral"},
                        }
                    ],
                    messages=[
                        {"role": "user", "content": _build_user_prompt(m)}
                    ],
                ),
            )
        )

    if not requests:
        return {}

    log.info("[llm_enricher] Submitting batch of %d requests...", len(requests))

    # ── Submit batch ──────────────────────────────────────────────────────────
    try:
        batch = client.messages.batches.create(requests=requests)
    except Exception as exc:
        log.error("[llm_enricher] Batch creation failed: %s", exc)
        return {}

    log.info("[llm_enricher] Batch ID: %s  status: %s", batch.id, batch.processing_status)

    # ── Poll for completion ───────────────────────────────────────────────────
    deadline = time.monotonic() + max_wait_s
    while True:
        if time.monotonic() >= deadline:
            log.warning(
                "[llm_enricher] Batch %s timed out after %ds — returning partial results.",
                batch.id,
                max_wait_s,
            )
            break

        batch = client.messages.batches.retrieve(batch.id)

        if batch.processing_status == "ended":
            log.info(
                "[llm_enricher] Batch %s complete. succeeded=%d errored=%d",
                batch.id,
                batch.request_counts.succeeded,
                batch.request_counts.errored,
            )
            break

        log.debug(
            "[llm_enricher] Batch %s: processing=%d ...",
            batch.id,
            batch.request_counts.processing,
        )
        time.sleep(poll_interval_s)

    # ── Collect results ────────────────────────────────────────────────────────
    results: dict[str, str] = {}
    try:
        for result in client.messages.batches.results(batch.id):
            if result.result.type != "succeeded":
                log.debug(
                    "[llm_enricher] Batch item %s: %s",
                    result.custom_id,
                    result.result.type,
                )
                continue
            msg = result.result.message
            text_blocks = [b.text for b in msg.content if b.type == "text"]
            if text_blocks:
                results[result.custom_id] = text_blocks[0].strip()
    except Exception as exc:
        log.error("[llm_enricher] Failed to retrieve batch results: %s", exc)

    log.info("[llm_enricher] Collected %d/%d enriched narratives.", len(results), len(requests))
    return results


# ── Output JSON helper ─────────────────────────────────────────────────────────
def write_captain_narrative(
    out_path: Path,
    existing_json: dict,
    captain_text: str,
    metrics: dict,
) -> None:
    """
    Write (or update) a *_narrative.json file to include the LLM-generated
    captain briefing alongside the existing templated narrative.

    The output JSON gains two new fields:
      "narrative_captain"        — the LLM-generated plain-English text
      "captain_metrics_hash"     — cache key so we can skip re-generation
      "captain_generated_by"     — model + enricher version tag for provenance

    Existing fields (narrative, metrics_hash, generated_by, etc.) are
    preserved unchanged so the dashboard can use either narrative type.

    Args:
        out_path:      Path where the JSON file lives (already exists).
        existing_json: The already-loaded dict from that file.
        captain_text:  The LLM-generated narrative text.
        metrics:       The metrics dict (for generating the cache hash).
    """
    updated = dict(existing_json)  # shallow copy preserves all existing fields
    updated["narrative_captain"] = captain_text
    updated["captain_metrics_hash"] = captain_hash(metrics)
    updated["captain_generated_by"] = f"{_MODEL}/{ENRICHER_VERSION}"

    out_path.write_text(
        json.dumps(updated, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def is_captain_fresh(existing_json: dict, metrics: dict) -> bool:
    """
    Return True if the existing narrative JSON already has an up-to-date
    captain narrative — meaning we do NOT need to call the LLM again.

    "Up to date" means the stored captain_metrics_hash matches what we'd
    compute for the current metrics dict + enricher version. If the source
    data changed (new pipeline run with new SWVF) or we bumped
    ENRICHER_VERSION, the hash won't match and we regenerate.
    """
    stored_hash = existing_json.get("captain_metrics_hash")
    if not stored_hash:
        return False  # field absent — never been enriched
    return stored_hash == captain_hash(metrics)


# ── CLI entry point ────────────────────────────────────────────────────────────
def _cli_dry_run(slug: str) -> None:
    """
    Quick CLI dry-run: load the narrative JSON for a given slug,
    reconstruct the metrics, and print the LLM-generated captain briefing
    to stdout without writing anything.

    Usage:
        python -m tools.narrative.llm_enricher --slug hamilton_precinct_addyston_a
        python -m tools.narrative.llm_enricher --slug hamilton

    This is useful for:
      • Tuning the system prompt before a full batch run
      • Demoing the feature to stakeholders
      • Debugging unexpected output for a specific precinct
    """
    import sys  # noqa: PLC0415

    # Locate the narrative JSON for this slug.
    # We look in docs/data/ first (county/precinct), then docs/data/*/ (districts).
    root = Path(__file__).resolve().parent.parent.parent
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    from pipeline.paths import DATA_DIR  # noqa: PLC0415
    data_dir = DATA_DIR

    candidate = data_dir / f"{slug}_narrative.json"
    if not candidate.exists():
        # Try subdirectories
        matches = list(data_dir.rglob(f"{slug}_narrative.json"))
        if not matches:
            print(f"ERROR: No narrative JSON found for slug '{slug}'", file=sys.stderr)
            sys.exit(1)
        candidate = matches[0]

    existing = json.loads(candidate.read_text(encoding="utf-8"))

    # Re-derive the level so we can call build_metrics_for_level correctly.
    # The narrative JSON stores 'level' and all the source chart metrics are
    # at the standard paths — but for a CLI dry-run we just use the narrative
    # JSON's stored metrics (they're embedded as prose, not raw values).
    # Instead, we reconstruct from the party JSON.
    level = existing.get("level", "county")

    # Attempt to load the party affiliation JSON for this slug and rebuild metrics.
    try:
        # Insert tools/ on path so narrative.templates resolves.
        tools_dir = str(Path(__file__).resolve().parent.parent)
        if tools_dir not in sys.path:
            sys.path.insert(0, tools_dir)
        from narrative.templates import build_metrics_for_level  # noqa: PLC0415

        party_path = data_dir / f"{slug}_party_affiliation.json"
        if not party_path.exists():
            party_path = candidate.parent / f"{slug}_party_affiliation.json"
        if not party_path.exists():
            # For precincts, path is docs/data/{county_slug}_precinct_{safe}_party.json
            parts = slug.split("_precinct_")
            if len(parts) == 2:
                county_slug, precinct_safe = parts
                party_path = data_dir / f"{county_slug}_precinct_{precinct_safe}_party.json"

        if not party_path.exists():
            print(
                f"ERROR: Cannot find party JSON for slug '{slug}' — "
                "cannot rebuild metrics for LLM call.",
                file=sys.stderr,
            )
            sys.exit(1)

        party_json = json.loads(party_path.read_text(encoding="utf-8"))
        metrics = build_metrics_for_level(
            level=level,
            party_json=party_json,
            parent_county=existing.get("jurisdiction_name") if level == "precinct" else None,
        )

        if metrics is None:
            print("ERROR: build_metrics_for_level returned None.", file=sys.stderr)
            sys.exit(1)

        # Inject the slug so enrich_one has it for logging (optional).
        metrics["slug"] = slug

        print(f"\n{'='*60}")
        print(f"[{level}] {metrics['name']}")
        print(f"{'='*60}")
        print("TEMPLATED:\n", existing.get("narrative", "(none)"))
        print(f"\n{'─'*60}")
        print("LLM CAPTAIN BRIEF:")

        result = enrich_one(metrics)
        if result:
            print(result)
        else:
            print("(API call returned None — check ANTHROPIC_API_KEY)")

    except Exception as exc:
        print(f"ERROR during dry-run: {exc}", file=sys.stderr)
        raise


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    parser = argparse.ArgumentParser(
        description="LLM enricher dry-run: print a captain briefing for one jurisdiction."
    )
    parser.add_argument(
        "--slug",
        required=True,
        help=(
            "Jurisdiction slug to enrich, e.g. 'hamilton' (county) or "
            "'hamilton_precinct_addyston_a' (precinct)."
        ),
    )
    args = parser.parse_args()
    _cli_dry_run(args.slug)
