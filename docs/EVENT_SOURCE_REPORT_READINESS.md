# Bounded whole-source report readiness

Source comparisons use the actual published bundle. Source-report publications
retain their immutable run snapshot even when publication used a qualified held
derivative. Legacy publications have a known baseline only if every recorded
article evidence version still matches its original stored body; otherwise the
baseline is explicitly unavailable. Unpublished accepted runs cannot override a
public predecessor.

Optional `SV_EVENT_SOURCE_REPORT_COHORT_ID` and
`SV_EVENT_SOURCE_REPORT_COHORT_TOKENS` must be supplied together. Each call reserves
against the serialized sum of charged and outstanding tokens across that cohort.
Unknown transport outcomes stay reserved; a cohort cannot silently change limits.
No migration, model concurrency change or expanded publication authority is needed.
Automatic enrollment requires an explicit cohort. Defaults remain disabled.

`event.source_report.approved` is a bounded explicit list of reviewed run IDs.
When the report path is enabled, the scheduler can hand these to the existing
separate approval-admission and promotion jobs. Acceptance alone does not grant
publication authority; immutable output, freshness and predecessor checks remain.
Empty approval/enrollment lists authorize nothing.

The addition-only lexical novelty gate ignores recognized syndicated-page footer
chrome. It does not modify stored evidence or model context, and never filters
corrections to already-linked source bodies. This is not semantic novelty analysis.
Debounced snapshots freeze at admission; source bursts hold stale jobs before paid
calls and permit admission of a fresh snapshot. They are not silently coalesced.

An optional bounded analyst question supplies a focus, not a conclusion. Original
primary disclosures take precedence for company beliefs and qualifications;
secondary disagreement must remain attributed. Older newly incorporated sources
are not later incident developments. Reviewer evaluation still covers the entire
packet and report, including inference rationale and confidence.

Local verification: full offline suite 1,260 passed, two skipped; actual disposable
PostgreSQL tests cover public derivative baselines, legacy-version mismatch,
cohort reservations, unknown transport, and debounce bursts (16 passed).
Initial readiness image `b7b7481` was deployed to the four scoped workloads.
Primary evidence ingestion/linking completed, but preflight exposed an incorrect
assumption that all retained publication workflows hash their bare bundle. The
local follow-up instead invokes each retained workflow's existing resolver and
revision validator. The actual legacy bundle and all three original article
versions passed read-only validation. Follow-up image `5518745` is now deployed;
all four scoped rollouts passed after affected-queue drain, with rendered/live
agreement. Unrelated local-model work was preserved. The complete four-source
packet passed preflight with a known single older-source addition and no omission.
The one-time proof executor selected the orchestrator rather than the hosted
worker and failed during local provider-secret loading, before HTTP. The run is
held, its job failed, no output/review/approval exists, and its 10,115-token
reservation remains retained. The configured hosted worker passed read-only
credential readiness. No secret was copied and the terminal attempt was not
replayed. Explicit audited pre-transport recovery is still required before the
bounded two-call proof. Public predecessors and current page/feed hashes are
unchanged; global admission remains disabled. This is an operator proof-routing
failure, not an assertion that the ordinary hosted worker lacks credentials.
No autonomous living-report acceptance claim follows from these local tests.

## Explicit zero-HTTP recovery

Migration 070 adds an immutable parent/child recovery audit. Operator-only
`recover_pretransport` permits one retry of a first-writer local credential
readiness failure, with unchanged source/configuration/public predecessor. It
refuses completed or uncertain transport, paid output, unproven errors, stale
inputs and descendants. The original terminal run, job, call and journal remain;
only the proven unused token reservation is reconciled through the audited
operation. The child has a normal parent-linked job, the original cohort ceiling
and two-call/no-correction limit. Recovery is not a source or incident development.
There is no automatic recovery scheduler or exposed public endpoint.

New runtime readiness failures carry a structured pre-HTTP receipt. Older missing
master-key failures require an explicitly authorized operator attestation backed
by saved request/journal identity, original executor/loader code identities,
missing-key observation and deterministic local reproduction. Exception text
alone is insufficient. The operator must verify the evidence before admission.
No new secret copying, role grants or credential expansion is needed.

`JournaledExecutor` checks client readiness in the exact worker context, allows
two calls under a summed reservation ceiling and writes atomic per-run journals.
It distinguishes local pre-transport failure from uncertain transport and cannot
replay an existing journal. Responses are committed before parsing/control return.
Offline tests cover readiness reaching no HTTP, mixed router stdout, both complete
receipts, reservation/call ceilings, replay refusal and uncertainty classification.
Actual PostgreSQL tests run recovery through generation, review, journal and
immutable persistence using fixtures, including legacy attestation and refusal.
Verification: 1,263 offline passed, two skipped; 20 PostgreSQL tests passed.
Deployment and the recovered live proof remain pending.
