# LLM Enricher — Captain Briefings for Precinct Volunteers

**File:** `tools/narrative/llm_enricher.py`

The deterministic template system in `templates.py` produces statistically accurate narratives. This module transforms those same metrics into plain-English *captain briefings* — the kind of note a precinct volunteer can read on their phone before knocking on a door.

---

## The Problem It Solves

**Template output** (clinical, data-first):
> "Anderson A Precinct has 614 registered voters, with 62.2% unaffiliated or lacking primary history. Republican-leaning voters make up 23.5% of registrants and Democratic-leaning voters account for 28.2%, a modest Democratic lean of 4.7 percentage points."

**Captain briefing** (human-scale, action-oriented):
> "Just over 6 in 10 of your neighbors haven't picked a party primary — they're genuinely persuadable. Democrats have a small edge, but younger voters here lean noticeably bluer than longtime residents, which means turnout among newer arrivals is your best lever."

Both are derived from the same aggregate statistics. Only the framing changes.

---

## Architecture

```
build_metrics_for_level()     ← existing templates.py output
        │
        ▼
  enrich_one(metrics)          ← sync, single jurisdiction
        or
  enrich_batch(metrics_list)   ← async, Batch API, 50% discount
        │
        ▼
  write_captain_narrative()    ← adds fields to *_narrative.json
```

### Two code paths

| Path | Function | When to use |
|---|---|---|
| Sync (single call) | `enrich_one(metrics)` | On-demand: one precinct, dashboard API endpoint |
| Batch API | `enrich_batch(metrics_list)` | Bulk: full county or full state pipeline run |

Both accept the same `metrics` dict shape and return the same `str` type. The batch path returns a `dict[slug → str]`.

---

## Setup

### 1. Install the Anthropic SDK

```powershell
.venv\Scripts\pip install "anthropic>=0.40.0"
```

### 2. Set your API key

```powershell
$env:ANTHROPIC_API_KEY = "sk-ant-..."
```

Add to your `.env` file (never commit) or set it in your shell profile for persistent access.

### 3. Verify

```powershell
python -m tools.narrative.llm_enricher --slug hamilton
```

---

## CLI Usage

The `--slug` flag takes any jurisdiction slug. The command loads the existing `*_narrative.json`, rebuilds the metrics from the source party affiliation JSON, calls the LLM, and prints both narratives side-by-side — without writing anything.

```powershell
# County
python -m tools.narrative.llm_enricher --slug hamilton

# Precinct
python -m tools.narrative.llm_enricher --slug hamilton_precinct_addyston_a

# Congressional district
python -m tools.narrative.llm_enricher --slug oh_01
```

Use this to:
- Tune the system prompt before a full batch run
- Demo the feature to stakeholders
- Debug unexpected output for a specific jurisdiction

---

## Programmatic Usage

### Single jurisdiction (synchronous)

```python
from tools.narrative.llm_enricher import enrich_one, write_captain_narrative, is_captain_fresh
from tools.narrative.templates import build_metrics_for_level
import json
from pathlib import Path

# Load source data (already happens in generate_narratives.py)
party_json = json.loads(Path("docs/data/hamilton_party_affiliation.json").read_text())
metrics = build_metrics_for_level("county", party_json)

# Check cache first
narrative_path = Path("docs/data/hamilton_narrative.json")
existing = json.loads(narrative_path.read_text())

if not is_captain_fresh(existing, metrics):
    captain_text = enrich_one(metrics)
    if captain_text:
        write_captain_narrative(narrative_path, existing, captain_text, metrics)
```

### Full county batch (Batch API)

```python
from tools.narrative.llm_enricher import enrich_batch, write_captain_narrative, captain_hash
import json
from pathlib import Path

# Build a metrics list with slug injected into each entry
metrics_list = []
for entry in precinct_entries:
    m = build_metrics_for_level("precinct", entry["party_json"])
    m["slug"] = entry["slug"]
    metrics_list.append(m)

# Filter to only stale entries
stale = [
    m for m in metrics_list
    if not is_captain_fresh(load_json(m["slug"]), m)
]

# Submit batch — polls until done (up to 1 hour)
results = enrich_batch(stale)

# Write results
for m in stale:
    slug = m["slug"]
    if slug in results:
        path = Path(f"docs/data/{slug}_narrative.json")
        existing = json.loads(path.read_text())
        write_captain_narrative(path, existing, results[slug], m)
```

---

## Output JSON Schema

After enrichment, `*_narrative.json` files gain three new fields alongside the existing ones:

```json
{
  "geography": "precinct",
  "level": "precinct",
  "jurisdiction_name": "Addyston A",
  "updated": "2026-05-09",
  "generated_by": "templated-v1",
  "template_version": "v2",
  "config_version": "v1",
  "metrics_hash": "d3fdcd54da43a662",
  "narrative": "Addyston A Precinct (precinct in Hamilton County) has 525 registered voters...",

  "narrative_captain": "Just over half your neighbors have never voted in a party primary — they're your most reachable audience. The precinct tilts slightly Democratic, but the bigger story is that newer registrants here lean considerably bluer than longtime residents, so first-time and young voters are your highest-leverage contacts.",
  "captain_metrics_hash": "a1b2c3d4e5f67890",
  "captain_generated_by": "claude-haiku-4-5/v1"
}
```

The original `narrative` and `metrics_hash` fields are preserved unchanged. The dashboard can use either field depending on the audience.

---

## Caching and Staleness

The `captain_metrics_hash` field is a 16-character SHA-256 hash of:
- The metrics dict (aggregate statistics only)
- `ENRICHER_VERSION` (version string in `llm_enricher.py`)
- The model name (`claude-haiku-4-5`)

If any of these change — new pipeline run with updated source data, a bumped prompt version, or a model upgrade — the stored hash won't match the current computed hash. `is_captain_fresh()` detects this and the pipeline re-generates only stale entries.

This is the same pattern used by `metrics_hash` in the template system.

---

## Cost Notes

| Dimension | Detail |
|---|---|
| Model | `claude-haiku-4-5` — cheapest capable Claude model |
| Input per call | ~200 tokens (structured metrics) |
| Output per call | ~80–120 tokens (2-3 sentences) |
| Prompt caching | System prompt cached at `cache_control: ephemeral`; hits after first write pay ~0.1× input rate |
| Batch API discount | 50% off real-time pricing for `enrich_batch()` calls |
| Cache skipping | `is_captain_fresh()` avoids API calls for unchanged jurisdictions |
| Ohio scale | ~10,000 precincts × (200 input + 100 output) tokens at Haiku batch pricing ≈ **under $1 for a full-state run** |

The Batch API path is strongly recommended for full county or full state runs. The sync path is for on-demand enrichment (single precincts, API endpoints, CLI dry-runs).

---

## Privacy and PII Guarantee

The `metrics` dict sent to the LLM contains **only aggregate statistics**:

- Total registered voter count
- Party lean percentages (D, R, unaffiliated)
- Net partisan lean (percentage points)
- Generational trend direction (if present)
- Jurisdiction name and level

No individual voter data is ever included. The metrics dict is the same sanitized aggregate structure used by the template engine — individual SWVF records are collapsed into counts before this layer is ever reached. SOS voter IDs, addresses, names, and election-history detail for specific voters never enter the enrichment pipeline.

---

## Graceful Degradation

If `ANTHROPIC_API_KEY` is not set, the `anthropic` package is not installed, or any API call fails:

- `enrich_one()` returns `None`
- `enrich_batch()` returns `{}`
- `write_captain_narrative()` is never called
- The pipeline continues with the existing templated `narrative` field

The LLM path is purely additive. It never blocks a pipeline run.

---

## Integration with `generate_narratives.py`

The `--llm` / `--llm-batch` flags are wired into `generate_narratives.py`. On each run the pipeline:

1. Builds templated narratives (existing behavior)
2. Checks `is_captain_fresh()` for each jurisdiction
3. Calls `enrich_one()` (sync, `--llm`) or queues for `enrich_batch()` (`--llm-batch`) on stale jurisdictions
4. Writes updated JSON via `write_captain_narrative()`

As of this writing, no production run has used either flag: `ANTHROPIC_API_KEY` is unset and the `anthropic` package is not installed in `.venv`, so every `docs/data/*_narrative.json` file has only the templated `narrative` field, never `narrative_captain`. The integration is dormant, not unbuilt — see "Setup" above to activate it.

---

## Version Bumping

To force re-generation of all captain narratives (e.g., after a system prompt improvement):

1. Bump `ENRICHER_VERSION` in `llm_enricher.py` (e.g., `"v1"` → `"v2"`)
2. Run the pipeline — all `captain_metrics_hash` values will no longer match, triggering fresh LLM calls for every jurisdiction

This is intentional. The hash encodes both the data *and* the prompt version, so a better prompt automatically invalidates stale output.
