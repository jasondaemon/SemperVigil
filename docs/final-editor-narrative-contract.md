# Whole-source final editor, narrative v1

This candidate is disabled by default. The active release path prioritizes accurate substantive evolving reports; ATT&CK integration is excluded. Previously committed catalog/mapping workflows and historical publications remain separate and unchanged.

Opt in with `SV_EVENT_REPORT_V2_FINAL_EDITOR_CONFIG` containing exactly:

```json
{"workflow":"whole-source-final-editor-narrative-v1","model":"CAPABLE_CONFIGURED_EDITOR","reasoning_effort":"high","max_completion_tokens":12000,"context_overrides":{}}
```

Existing v2 event scope, generation enablement, immutable cohort policy, expiry, run limits, cumulative reservation accounting and qualified-predecessor gates still apply. No new default scope or production activation is introduced. Model-name distinction is an enforced legacy policy constraint, not proof of statistical or organizational independence. It remains the default. The owner-authorized new narrative workflow may explicitly set `SV_EVENT_REPORT_V2_FINAL_EDITOR_MODEL_POLICY=narrative-source-checking-separate-invocations-v1`. This opt-in is bound into generation/runtime identities and the immutable snapshot; admission, reconstruction and promotion require that exact new-workflow authority for same-model use. The two authentic invocations retain different writer/editor prompts, schemas and response receipts. Lineage labels the result as same-model source-checking editing with no model diversity. The setting does not relax ordinary review-only/legacy guards or credential and separated publication controls. No production activation is implied.

Exactly two paid requests: narrative writer and whole-source final editor (journal phases writer/review). Writer and editor receive complete article bodies without catalog candidates or technique fields. Existing previous-report mapping arrays are omitted as application-owned continuity adaptation, while prose and citations remain. The editor returns a final narrative report plus a decision about that returned report. Native validation binds unresolved issue IDs to returned final paragraphs, including newly added paragraphs. Final prose may correct actor/action/object, attribution, completion state, dates, certainty, scope and omissions while preserving developed paragraphs and explicit assessments. Readiness depends on material unresolved issues; locator and editorial warnings do not block. Deterministic provenance checks do not establish semantic entailment.

Wire schemas exclude mapping fields. The application supplies empty mappings only for internal/public compatibility. Active public bundles use null catalog and empty resolved mappings, authenticated by the new final-editor qualification lineage. Any model taxonomy output, substituted review-only response or manual prose derivative is rejected. Old publication readers continue using their original catalog and review contracts.

Original responses remain immutable. Reconstruct the final artifact from both authentic request/response receipts and the exact source snapshot. Record original draft, final content, response, source/context and profile hashes. Report content hash is separate from artifact lineage hash; unchanged prose can keep its content hash. Edited prose receives a fresh qualification, with no old approval transfer. The immutable final derivative is inserted once, never updated. Invalid/truncated/refused output, unresolved material issues, changed sources/predecessor/runtime/policy, missing model context or insufficient budget holds the run. Unknown HTTP outcomes retain their reservation; interrupted jobs cannot replay. There is no third rescue request.

The final-editor completion cap must cover a full report plus reasoning. The retained Sol/high verdict-only probe used 3,083 reasoning tokens; retained writer outputs used up to 3,608 completion tokens. A proposed 12,000 completion cap supplies bounded headroom rather than recycling the 6,000 verdict cap. Exact reservation is tokenizer(request)+512+cap. At either request boundary, both selected configured model context and cumulative run/cohort budgets must admit the complete request before HTTP. The cap is not a promise of successful output; cap exhaustion holds.

An explicit case allowance can use `context_overrides:{"evt_e2b587f96d75":32768}`. It raises only that event's complete-context admission; all other events retain 24,000. The final-editor workflow refuses delta omission and never truncates articles. The measured lean FBI packet retains all 16 source bodies and is 26,677 tokens; exact narrative writer reservation is 35,852. Configured Sol context is 1,050,000. Any second-stage reservation must be computed from the actual draft; a proposed single-run 100,000 ceiling would hold if cumulative exact reservations exceed it. Source attribution reconciliation, fresh evidence audit and predecessor qualification remain independent blockers. This allowance has not been activated.

Synthetic PostgreSQL integration proves structural lifecycle constraints through actual separated admission/promotion roles and a real newly linked fixture article on the same event. It is not hosted-model quality evidence or a production new-article demonstration. The next proposed paid tests use retained CEVA and Zammad drafts, omit mapping arrays only, preserve all prose/citations/sources and make one editor request each without writer regeneration. They test CEVA agency/notification correction and Zammad factual speed/date-precision correction, plus rich useful final prose and explicit uncertainty. Zammad's historical invalid writer remains evaluation input, not a qualified production continuation. All final artifacts need independent full-source adjudication before any deployment decision.

Historical ordinary v2 publications are validated against original generation prompt identity. New runs pin writer/reviewer prompt hashes and generator version inside their immutable input snapshot. Published reads use that original pin rather than today's prompt text; current admission/promotion still requires today's exact prompts. Older pre-pin publications require an exact prompt pair archived from trusted repository ancestor versions AND an authentic non-revoked existing publication. Unknown pairs, changed receipts/pins and unpublished pre-pin artifacts fail closed. This historical-read rule does not confer new publication authority. Ordinary old-generation PostgreSQL regressions cover both pre-pin and pinned reports, unchanged qualification/export, current-admission rejection and prompt tampering; imported continuations are not used for these tests.

### Bounded transport and unknown usage

The narrative workflow pins `narrative-hard-transport-deadline-v1` into the
snapshot, generation identity and runtime identity. Defaults are 180 seconds for
the writer, 240 seconds for the final editor and a 600-second overall window
starting with the first durable call journal. Optional
`SV_EVENT_REPORT_V2_TRANSPORT_POLICY` accepts exactly `workflow`,
`writer_seconds`, `editor_seconds`, `overall_seconds`: per-call bounds are
180–240 seconds and the overall bound is 360–600 seconds. The actual editor
profile is checked before a call journal or HTTP; the provider's existing
60-second default cannot silently govern a Sol/high/12,000-token editor.

The native router executes one synchronous Chat Completions request in a
separate process. Credentials travel through stdin; captured diagnostics are
never returned. The parent enforces an absolute deadline with process termination
and reaping, including trickling response bodies, and works in worker threads.
The remaining overall window is recalculated after credential/freshness checks
immediately before transport. Native no-retry behavior and immutable journal,
reservation and publication gates remain in force. Legacy workflows retain their
existing transport behavior; no global provider timeout is changed.

These limits are conservative engineering bounds, not measured latency
percentiles: a prior verdict-only Sol/high/6,000 review took 40.215 seconds, while
the retained full-editor Sol/high/12,000 attempt exhausted the provider's
60-second socket timeout. A longer deadline has not yet been demonstrated to
produce an accurate report. Timeout after transport starts retains unknown
usage and the full outstanding reservation; stopping the client cannot prove
that the provider stopped generation or incurred no charge. Only the existing
instrumented pre-HTTP authority boundary can prove zero transport and release
an outstanding reservation; lifetime admission remains consumed.

Acceptance: reject 60-second actual editor profiles; complete one local normal
response; terminate header stalls and continually trickling responses under a
worker thread without retry; clip transport to the remaining overall window;
hold expired windows before HTTP; retain unknown transport reservations;
preserve historical exports and same-model workflow restrictions. No production
enablement or paid evaluation is implied by these tests.
