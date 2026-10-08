# Initial analyst reports and reassessment fairness

This candidate addresses two verified October 8, 2026 failures. An audited FBI
fallback remains held with no derived child. The historical recovery selector
chooses that same artifact every tick, and its held result previously returned
before the 23 active reassessment cases were considered. Separately, qualified
source-report successor admission requires an existing public report; a newly
confirmed draft instead receives the local Article Summary overview. Oracle
Health demonstrated that mismatch despite two linked complete source articles.

This is source implementation for independent review. It does not enroll all
active cases, deploy a runtime, make hosted requests, or authorize an ASOS run.
The test model responses are synthetic: gate tests do not establish model fidelity
or the editorial quality of the eventual Oracle report.

## Changes

`event_reassessment_automation.py` stores a terminal receipt for an unchanged
held fallback, keyed by composition and the latest successful audit job. Original
composition, ledger and audit remain intact. An explicit new audit identity can
be considered once; ordinary ticks do not retry the closed identity. Held recovery
and held active-case results no longer terminate a scheduler turn. A turn still
stops after actual progression. A separate last-checked settings record rotates
active cases before phase/priority ordering, so 25 pending cases cannot indefinitely
exclude later cases. It does not modify their semantic snapshot or updated date.

`SV_EVENT_REASSESSMENT_AUTOMATION_EVENT_IDS` is optional JSON. Absence retains
existing global behavior; `[]` pauses this coordinator; a list restricts active
case selection and disables global historical recovery. It does not pause
ingestion, article summaries, source scraping, builds, or daily JSON exports.
Other research coordinators retain their own controls and must be inspected before
a bounded rollout. Fixing fairness must not unexpectedly admit all 23 cases.

`event_report_initial.py` and `event_report_v2_policy.py` add an explicit
`admission_kind: initial_report` policy branch. It requires one scoped event,
one lifetime run, one concurrent run, an explicit final editor, active confirmed
draft state, matching entity, no public pointer and no historical public revision.
The policy freezes entity, system, incident window and original-source quote
anchors. Anchors establish locator integrity, not semantic correctness. An
independent reviewer must verify that the proposed identity matches the sources.
Rechecks cover source identity, entity/state, expiry, runtime identity, generation,
policy and public predecessor before transport and publication.

`event_source_reports_v2.py` uses the existing complete-source writer/final-editor
path. It freezes initial-report provenance and supplies the proposed incident
identity and all original source text to both calls. Legacy overview prose is not
passed as continuity. Each call reserves before HTTP; actual usage is charged and
uncertain transport remains reserved. A held attempt consumes the cohort lifetime
slot even if little or no output was produced. Changed evidence does not grant a
replacement run. No automatic repair, third call, allowance or cohort rotation is
introduced. `worker.py` skips competing legacy Event summary generation only for
the explicitly managed initial case; other local summaries remain available even
when the selected narrative policy is malformed.

`event_report_contract_v2.py` excludes `what_changed` from first-report writer and
editor schemas and checks that locally. The initial report has no prior report to
compare. `event_source_report_publication_v2.py` verifies predecessor-free initial
provenance and displays an application-owned notice distinguishing incident dates
from source publication dates. Existing approval admission, separate promotion,
immutable reconstruction, export and final activation authorization are reused.
Existing qualified-successor admission and historical report reconstruction retain
their previous semantics. ATT&CK stays disabled through the narrative profile.

`enrichment/diagnostics.py` and `worker.py` retain bounded search-result decisions:
rank, public locator, title, score/components, save/discard outcome and reason.
The first 50 entries are retained with complete outcome counts and an omitted
count. Diagnostic URLs omit userinfo, query and fragment; bodies, snippets and
headers are not added. Scoring, thresholds, fetching and promotion are unchanged.
This makes future zero-save searches explainable without another paid search.
It cannot retroactively reconstruct the discarded Oracle search results.

## Analyst report content contract

The overview orients the reader to the affected organization/system, supported
mechanism, sequence, consequences, response and essential uncertainty. Developed
detail sections add mechanics, dated milestones, discriminating evidence,
population/data scope, response status or reasoned implications. Paraphrasing the
overview is not additional coverage. Headings and word counts are not quality
quotas; omit a section when the source set cannot support distinct value.

Every material finding and assessment premise cites a supplied original source.
Passage quotes are navigational anchors, not the entire evidence universe. Both
passes read complete cited articles. Source domains do not establish independent
confirmation. Preserve company beliefs, secondary attribution, contradictions,
affected populations, notification status and temporal precision. Publication
dates are not incident dates. Findings remain separate from explicitly labelled
assessments and gaps with confidence, rationale, premises and limits. Unknowns
remain specific; missing reporting is not proof that an action did not happen.

The final editor returns the entire final report plus a review of that exact
report. Only unresolved material errors block; stylistic and locator warnings
are nonblocking. Same-model writer/editor operation is explicitly described as
separate source-checking invocations, not independent model consensus. A valid
schema, matching quote or ready response alone does not demonstrate analyst
usefulness. Human/source-grounded canary review remains necessary.

Initial reports have no `what_changed`. Successors compare the actual published
baseline and separate corrections, new developments and newly incorporated old
evidence. Application-owned provenance identifies the exact source packet,
generation and publication history. Public pages must render the qualified final
report, and feed/index/daily projections must preserve existing unrelated records.

## Acceptance evidence

- Offline tests cover explicit first-report authority, required editor/single-case
  scope, identity/history/anchor rejection, no-work pause, restricted recovery,
  holds yielding to viable work, legacy summary isolation, first-report change
  rejection and bounded enrichment decisions without extra fetching.
- PostgreSQL scheduler tests reopen the database to prove terminal receipt
  persistence, preserve original audit/composition bytes, require a new audit
  identity for reconsideration, stop after one actual advance, and visit all
  30 pending cases across successive 25-case batches without editing case identity.
- PostgreSQL initial tests exercise complete identical evidence packets in writer
  and editor, exact two-call completion, separate admission/promotion principals,
  predecessor-free publication, native export and activation preserving an older
  event, no ATT&CK, source/entity changes between calls, disabled/expired authority,
  historical publication, failed/unknown/empty responses, retained unknown usage,
  spent lifetime slots and rejection of other events.
- Existing native/successor/import/budget/publication regressions must pass alongside
  these tests. These use disposable PostgreSQL and synthetic provider responses,
  never the production database or hosted models.

## Proposed Oracle-only production proof, after independent review

Scope exactly `evt_9b81883fcdf0`, Oracle Health legacy Cerner patient-data incident.
The historical incident window is January 22 through April 1, 2025, discovery
February 20, 2025, as attributed in source `S36976`. October 2026 reporting concerns
an impact estimate, not a newly occurring October breach. The near-20-million
figure remains attributed reporting; secondary coverage does not make it a
confirmed distinct-person count. Keep other Oracle cloud/SSO/LDAP, E-Business Suite
and Clop incidents separate. Do not turn a quoted actor alias into confirmed
attribution or classify the source commentator Blackfog as the affected platform.

Create a fresh explicitly identified initial-report cohort only after review and
runtime source/generation preflight. It has 70,000 lifetime/per-run tokens,
one run, one concurrent run, two calls maximum, 300-second debounce, and expiry
12 hours after activation. Writer: configured `gpt-5.6-sol`, reasoning `none`,
6,000 completion tokens. Final editor: `gpt-5.6-sol`, reasoning `high`, 12,000
completion tokens, explicit separate-invocation policy. Hard deadlines: writer
180 seconds, editor 240 seconds, overall 600 seconds. The model names are the
existing configured profile identifiers and must pass actual worker preflight.

Pause the legacy paid reassessment coordinator with its explicit empty scope
before resuming the scheduler. Inspect/drain existing event work naturally and
verify other paid coordinator gates. Preserve expired CEVA/Microsoft/Astrana
policies without extending, resetting or reusing them. No retry or scope expansion
follows a failed/held run. Record both actual usage and outstanding unknown
reservations, keep durable requests/responses, and review report fidelity before
asserting the trial succeeded. Deploy and reconcile manifests only from the
reviewed immutable commit while preserving unrelated local platform changes.

ASOS `evt_99691ed24e2a` is excluded from this proof. Its October 8 articles were
already linked to the existing October 6 draft. The July customer credential-
stuffing incident is separate from the October employee-credential/social-
engineering report. The current Snowflake-focused draft title is not confirmation
of platform compromise. Vendor statements, secondary observations about Simon AI
and alleged sample data need their own attribution and limits. Exact vendor,
actor, MFA state, exfiltration volume and affected count remain unresolved unless
later primary evidence supplies them.
