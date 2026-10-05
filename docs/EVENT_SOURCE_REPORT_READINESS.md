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

Local verification: full offline suite 1,259 passed, two skipped; actual disposable
PostgreSQL tests cover public derivative baselines, legacy-version mismatch,
cohort reservations, unknown transport, and debounce bursts. Deployment and a
bounded two-call primary-evidence correction proof remain separate pending gates.
No autonomous living-report acceptance claim follows from these local tests.
