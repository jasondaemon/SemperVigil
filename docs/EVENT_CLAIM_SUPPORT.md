# Private claim support audit

September 19, 2026: queue integration deployed to admin `eff906d`; the single-claim
worker revision is `3148e06`, platform `023f17c`. Private API admission is enabled;
v2 real evaluation completed and FAILED quality acceptance. Existing
private extraction and public quotation publication are unchanged.

## Queue admission

The existing authenticated `POST /admin/api/events/{id}/private-review` accepts
`aliases`, `scope`, `article_id`, and `audit_source` (the 64-character extraction
cache digest, without `.json`). This mode cannot be combined with paired review
or deconstruction. Only an existing, current source extraction can be audited;
the job revalidates its source/scope/claim/config binding before inference.
Missing/stale extraction fails rather than silently generating another one.

The existing `event_review_private` job runs in the serial local LLM lane, with
existing low priority, queue cap, deduplication and one-attempt job policy. Results
and private HTML use the existing Jobs dashboard and authenticated artifact API.
Admission currently uses the API, not a dedicated dashboard button. The admin
never reads source bodies or performs inference during admission.

Defaults: `SV_EVENT_CLAIM_SUPPORT_ENABLED=0`, empty
`SV_EVENT_CLAIM_SUPPORT_PROFILE_ID`. Enable only with a dedicated profile whose
system prompt equals this module's prompt, user template `{{input}}`, same local
model/provider, no fallback, temperature 0, max input 15000, max tokens 512-1536.
V2 pilot profile: `51ab710c-fe80-59fb-897e-0653ab078f8b`.

## Why a separate audit

The production extraction pilot returned exact source quotations paired with
unsupported statements. Presence of a quotation is not entailment. The support
module audits six dimensions independently: incident relevance, quoted support,
attribution, uncertainty, date support and surrounding-source qualifications.
It does not rewrite claims, substitute another citation or invent corrections.

V1's batch failed real evaluation: one false acceptance, three false rejections,
and repeated incorrect null-date judgments. It is not an accepted quality gate.
V2 sends one claim at a time with the full source and incident scope. Null date
means no date assertion; code fixes that dimension instead of asking the model.
At most eight claims are possible under the extraction
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
`assess(packet, scope, source, complete, root)` makes one serialized callback per
uncached claim, or reuses its immutable result. Completed claims survive a later
call's failure, so resuming does not repeat them. The callback receives the full
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
Current verification: full offline suite 856 passed, one
Linux-only skip on macOS and existing deprecation warnings. PostgreSQL and
JavaScript suites were not rerun: no schema or frontend changes in this slice.

## Next integration and release gate

Measured v2 outcome against the unchanged 17-case cohort: five bad claims rejected,
seven bad claims held, all five supported claims also held. Zero false accepts in
this small cohort does not establish a useful classifier: it accepts nothing.
In particular, repeated uncertainty holds and some missed entailment failures
remain. Model time: 50.444s across 17 calls (about 2.97s/call), versus 41.882s for
three batch calls. This is not ready to gate automatic narrative publication.

The next experiment must distinguish support for a source's assertion from
independent factual certainty. Evaluate quote entailment as a separate narrow
question before combining attribution/context decisions. Do not fix this by
relabeling holds as passes, weakening golden expectations, dropping the uncertainty
gate, or increasing resources without evidence. No further prompt iterations
were run after the two documented experiments.

1. Explicit default-disabled admission and pinned profile are implemented and
   deployed. This does not silently double every extraction's inference work.
2. A fixed cohort is committed in `tests/fixtures/event_claim_support_pilot.json`,
   with five supported and twelve rejected expectations frozen before inference.
   Extend future cohorts to cover:
   mismatched citation, uncertain actor identity, sensitive/unprotected secrets,
   advice versus completed recovery, and dated/undated milestones. Preserve gold
   expectations before running the model; unknown is held, not counted as a pass.
3. Both versions ran through the existing serial worker; repeat the same measured
   evaluation for any proposed correction. Record per-dimension false accepts,
   false rejects, holds, latency and cache replay. No direct inference bypass.
4. Only then consider a synthesis input gate; audit output alone must never
   auto-promote a report. Material source changes invalidate previous decisions.

Rollback: disable the separate support flag and drain/roll only admin and LLM
worker. Existing extraction/publication do not require this mode. Keep prior image
tags. There is no DB migration or public content rollback. Do not interpret
successful job execution as validated semantic accuracy.
