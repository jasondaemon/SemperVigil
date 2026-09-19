# Private incident deconstruction

September 19, 2026: locally implemented, including cached multi-source compilation;
not deployed or model-evaluated.

This is the extraction and structured-compilation slice of the living report. It
does not complete narrative synthesis, claim qualification, semantic corrections,
or publication. The public quotation pilot is unchanged.

## Execution path

The authenticated `POST /admin/api/events/{id}/private-review` endpoint accepts
`deconstruct: true` together with `aliases`, a source-backed `scope`, and
`article_id`. It rejects combining this mode with `paired`. Admission uses the
existing `event_review_private` job, queue cap, low priority, deduplication and
single local LLM worker. The admin never performs inference. Jobs and their HTML
attachments use the existing dashboard/viewer. No dedicated extraction button
has been added yet; admission is API-only.

Defaults are disabled. Both admin and worker need an upgraded image before
setting `SV_EVENT_DECONSTRUCTION_ENABLED=1`. Existing review/model/scope flags
are also required. Set `SV_EVENT_DECONSTRUCTION_PROFILE_ID` to a separate profile
whose system prompt exactly equals `event_deconstruction.SYSTEM_PROMPT`, user
template is `{{input}}`, and model/provider match the existing local model.
No cloud fallback or new model is permitted. Parameters: temperature 0,
max_input_chars 15000, max_tokens 512-1536. Actual model quality and output-budget
adequacy remain unmeasured; do not enable automated admission on this basis.

The worker sends one complete bounded source and an exact incident anchor. It
holds oversized sources before inference rather than truncating silently.
At most eight proposed claims cover overview, initial access, attack path,
impact, recovery and attribution. Claims include exact source spans and optional
incident/disclosure dates with explicit precision. Unknown dates remain null.
Existing claim validation is reused. A structural pass does NOT establish
incident relevance, entailment, date support, independence or truth.

The HTML clearly labels all claims unreviewed, groups readable statements by
section, shows supporting quotes and dated milestone proposals, and explicitly
labels unfilled sections as extraction gaps, not facts about the incident.
Escaping prevents source/model HTML from executing. Packet, source results and
HTML are immutable private log artifacts. No approval receipt is produced, so
the quotation approval path cannot promote these drafts. No events, feeds,
publication pointers or dirty-build state are written.

## Current limits and next gate

- Manual API pilot only; no automatic scheduler admission. Persistent extraction
  reuse binds the entire source, incident scope, prompt and pinned generation
  configuration. Unrelated event timestamps or new sources do not repeat existing
  inference. Every cached claim is reconstructed against current evidence; corrupt
  cache entries fail rather than silently falling back. Cache hits are shown in
  the existing job result. Cached empty extractions also avoid repeated calls.
- After extracting one source, the same job compiles all matching current cached
  drafts without extra inference. Changed/missing source versions are not copied
  from old drafts. Revisions ignore source ordering and event-only timestamp churn.
  The `changes` helper describes added, withdrawn and retained claim records; it
  does not infer which competing claim is true or claim to adjudicate a correction.
- Compilation is structured paragraphs with citations, not yet coherent model
  synthesis. Conflicting assertions remain unreviewed rather than being silently
  reconciled. Coverage identifies included, pending, over-budget, omitted and
  truncated sources. There is no public version of this path.
- Full sources above the bounded context budget are held. Chunking requires its
  own relevance and missing-context evaluation before use.
- Drafts are historical snapshot reviews, not assertions of current DB freshness.
- Model statements/dates can be semantically wrong despite exact citations.
  Tests explicitly demonstrate that structural success is never approval.
- Next: provision the dedicated profile through the supported API, perform a
  bounded private Vercel pilot, evaluate actual claims against sources, then add
  qualified multi-source narrative synthesis and semantic correction handling.
  Retain the current public report until those gates pass.

## Verification and troubleshooting

Run `.cache/mcp-venv/bin/python -m pytest -q tests/offline/test_event_deconstruction.py`
from the repository. Tests include real worker dispatch with mocked inference,
job-owned artifact reading, publication isolation, disabled admission, exact
citations, unsafe markup, malformed output, stale anchors and context bounds.
This does not invoke Hugo, a live model, or production DB.

`private_deconstruction_disabled`: verify the separate enablement on admin and
worker. `private_review_prompt_mismatch` or `private_review_profile_budget`:
profile does not match the pinned contract. `deconstruction_source_over_budget`:
source is deliberately held; do not raise model context or truncate to bypass.
`deconstruction_quote_not_unique`: model quote is absent or ambiguous; inspect
the source rather than fuzzy matching. Invalid model output fails the job with
its existing one-attempt policy, not a generic summary fallback.

Rollback: leave the feature disabled. No migration or production change is
required for this local implementation.
