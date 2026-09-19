# Private claim support audit

September 19, 2026: local library and regression tests only. Not deployed, not
connected to the admin queue, and not evaluated with the actual model yet.
Existing private extraction and public quotation publication are unchanged.

## Why a separate audit

The production extraction pilot returned exact source quotations paired with
unsupported statements. Presence of a quotation is not entailment. The support
module audits six dimensions independently: incident relevance, quoted support,
attribution, uncertainty, date support and surrounding-source qualifications.
It does not rewrite claims, substitute another citation or invent corrections.

One bounded request contains all claims from one current source, the full source,
and the incident scope. At most eight claims are possible under the extraction
contract. The prompt, input and output schema share the existing 15KB budget;
oversized requests fail before inference with no silent truncation. No regex or
conditional schema is sent to the installed model. Empty extractions need no call.

All six dimensions must be returned for every supplied claim exactly once.
One unsupported dimension yields `reject`; otherwise any uncertainty yields
`hold`. All supported yields `model_supported`, NEVER approved or confirmed.
Even agreement from a second call to the same model is not independent evidence.
Every result remains `public_eligible: false` and produces no publication receipt.

## Code and local verification

`event_claim_support.request_for(packet, scope, source)` reconstructs the stored
extraction against current source/scope/config before building an audit request.
`assess(packet, scope, source, complete, root)` makes at most one model callback,
or reuses a separately stored immutable result. The callback receives the full
request including system text and response schema; it must use a separately pinned
support profile and the existing single-LLM worker, not run in the admin process.
`render` validates the result again and shows original statements, citations and
dimension outcomes in escaped private HTML.

Cache identity includes extraction claims/config, full source, incident scope,
support prompt/schema and checker generation. Event timestamp churn does not
repeat inference. Modified evidence or mismatched cache records fail; changed valid
claims/profile require a new audit. Rejections are cached too. Source extraction
and support caches are distinct. The module performs no database or public writes.

Run:

```sh
.cache/mcp-venv/bin/python -m pytest -q tests/offline/test_event_claim_support.py
```

Tests cover all six rejection/uncertainty dimensions, exact response inventory,
unknown IDs, malformed/duplicate output, stale inputs, cache reuse/tampering,
symlink refusal, budget refusal, empty-source abstention and HTML escaping.
These are contract tests with mocked completions, NOT measured model accuracy.
Current verification: 32 audit tests pass; full offline suite 849 passed, one
Linux-only skip on macOS and existing deprecation warnings. PostgreSQL and
JavaScript suites were not rerun: no schema or frontend changes in this slice.

## Next integration and release gate

1. Add explicit default-disabled private audit admission to existing review jobs,
   pin a separate profile and preserve dashboard visibility, queue cap and low
   priority. Do not silently double every extraction's inference work.
2. Build a fixed evaluation cohort from the actual pilot errors and correct claims:
   mismatched citation, uncertain actor identity, sensitive/unprotected secrets,
   advice versus completed recovery, and dated/undated milestones. Preserve gold
   expectations before running the model; unknown is held, not counted as a pass.
3. Run through the existing serial worker and record per-dimension false accepts,
   false rejects, holds, latency and cache replay. No direct inference bypass.
4. Only then consider a synthesis input gate; audit output alone must never
   auto-promote a report. Material source changes invalidate previous decisions.

Rollback currently requires no production action: nothing in runtime imports this
module. Do not describe this checkpoint as deployed semantic validation.
