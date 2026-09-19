# Private incident deconstruction

September 19, 2026: private pilot deployed (admin `69e76a6`, LLM worker
`c4f8ddf`). See STABILIZATION_VERIFICATION.md for measured model outcomes.
This is not enabled for automatic narrative publication.

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
adequacy require real-pilot evaluation; do not enable automated admission on this basis.

The worker sends one complete bounded source and an exact incident anchor. It
holds oversized sources before inference rather than truncating silently.
At most eight proposed claims cover overview, initial access, attack path,
impact, recovery and attribution. Claims include exact source spans and optional
incident/disclosure dates with explicit precision. Unknown dates remain null.
The local transport now uses schema-constrained generation with exact source
sentence choices and allowed sections. The model returns only a date value or
null; code derives precision and validates the calendar date. This avoids both
inconsistent duplicate fields and the installed Ollama grammar parser's crash
on regex escapes. No regex/conditional grammar is sent to inference.
Both schema and source input count toward the existing 15KB budget. Sources with
no unique bounded sentences are explicitly held, not treated as an empty success.
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
- The dedicated v2 profile is provisioned through the supported API. A real
  three-source Vercel pilot produced 17 claims; unchanged-source replay used no
  inference. Actual review found mismatched supporting quotations, overconfident
  attribution, and loss of the sensitive/unprotected-secret distinction. No
  overview or dated milestones were extracted. Structural success is not reader
  acceptance. The earlier v1 profile is disabled.
- Next: passage-bound claim support evaluation and uncertainty/correction tests,
  then qualified multi-source narrative synthesis. Retain the current public
  report until those gates pass. Do not increase model/context/concurrency to
  hide these failures or auto-enroll the legacy candidate backlog.
  The local audit contract is now implemented in `event_claim_support.py`; see
  EVENT_CLAIM_SUPPORT.md. It is not wired into production or model-validated yet.

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

Rollback: set the separate deconstruction flag to 0, render/diff, and roll only
admin/LLM worker after draining their work. The original private quotation flow
and public report do not require this flag. There is no schema migration or public
content rollback. Keep the existing single-LLM lane and retained images.
