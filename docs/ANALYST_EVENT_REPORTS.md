# Analyst Event report contract

## Whole-source report and deterministic revision provenance — October 5

The default-disabled `SV_EVENT_SOURCE_REPORT_ENABLED=0` path uses one complete
source-context writer and one independent whole-report reviewer. A configured
optional correction changes only flagged item IDs, followed by one verification;
four phases are the absolute maximum. Existing article summaries, feeds and
existing published Event revisions retain their contracts. The legacy contract
below remains the normal generation path. The source-report contract is deployed
default-disabled; one explicitly approved derivative completed the scoped gates.

Each material paragraph carries exact source quotes. Local checks bind those
quotes to article text offsets, enforce finding/assessment confidence metadata,
reject exact repeated paragraphs, check timeline date ordering and reject stale
source/configuration/predecessor snapshots. Previous reports are continuity, not
evidence. Complete sources are supplied normally; oversized histories retain all
changed and previously cited evidence with an explicit omitted-source manifest,
or hold without truncating articles. Report coverage is not evidence novelty:
`update_reason`, `evidence_delta` and `previous_report_coverage` distinguish
generator refresh from new or corrected source material. Unknown baselines never
label all existing sources new.

Calls reserve tokenizer-measured request plus output allowance before transmission.
Raw responses and provider usage persist before parsing; failed/unknown attempts
retain reservation and cannot replay. Transport elapsed time is retained in new
response receipts. The new qualification adapter verifies exact persisted writer
and reviewer output before using the existing separate approval/promotion roles
and atomic public-pointer/release guards. No fact-ledger identifiers are fabricated.

### Acceptance checkpoint and deterministic derivative

The bounded four-call public-source proof corrected unsupported removal chronology
and the claim that a profile-picture detail was absent from the prior report. The
whole-report verifier returned ready with no issues, but manual inspection held
the report: its change section and rationale still called unchanged source material
new evidence and new articles despite an explicitly empty evidence delta. This is
a semantic false-negative in revision metadata, not a citation-integrity failure.
The supported deterministic `event-report-generator-metadata-projection-v1`
excludes only generated `what_changed` items for a generator refresh with a known,
empty evidence delta. It never rewrites retained prose, quotations or rationales.
The immutable derivative records raw report/review and projected report hashes,
removed item IDs, policy, reason, source-snapshot identity and exact review scope.
The raw report, raw model artifacts and original metadata hold remain intact;
qualification authorizes the verified derivative, not the omitted false claim.

Future generator-only requests forbid `what_changed` in their output schema.
Application-owned provenance renders: "Report revised using the existing source
set; no new sources were added." Unknown evidence baselines get an explicit
unavailable-baseline notice instead. Evidence-change updates may retain substantive
model-written changes tied to actual source additions/corrections. The prompts
explicitly encourage justified analyst assessments with supporting rationale and
intelligence gaps for sparse evidence, without forced speculation.

Freshness, exact model-response integrity and all retained source spans are
revalidated at derivative qualification, separate approval, promotion, export and
activation. Completed model artifacts and derivative records have immutable
database guards. A retained substantive reviewer issue or unrelated hold cannot
be bypassed by projecting revision metadata. Existing publication gates and the
previous public revision remain the fallback on failure.

All unflagged paragraphs were preserved exactly and all 21 quotes validated. The
two added phases reserved 17,984 tokens against an 18,000-token allowance and used
13,578 actual tokens; their transport times were 14.155 and 3.545 seconds. The
four successful phases used 25,268 actual tokens. An older lost-response attempt
remains held with unknown actual usage; durable remote journaling fixes subsequent
mixed-output recovery without claiming to reconstruct the already-lost response.
The seven retained items contain 17 exact source spans; their reviewed body is
unchanged. Restricted-role PostgreSQL tests exercise qualification of an explicitly
held raw report, deterministic projection, normal approval/promotion and rendering,
plus false-notice rejection and completed-artifact immutability. No additional
hosted calls are needed for the deterministic derivative. Deployment remains
default-disabled, with a one-Event publication admission scope and no automatic
cohort enrollment.

### First scoped live publication

The [Microsoft report](https://cybernews.jasondaemon.net/events/evt_8d136739e530/)
completed normal separate-role approval/promotion and scheduler-admitted build,
then guarded atomic activation on October 5. Its seven body items, 17 exact source
spans, two publisher links and deterministic revision notice were verified on the
public page and Events index. The public report fragment hash matched the active
guarded release manifest; the scoped Helm/live diff was empty. Raw artifacts and
their metadata hold remain intact. No additional hosted call or access expansion
was used for publication. Historical daily JSON and the untouched comparison Event
were byte-identical; 18 public HTTP/markup/asset/sample-JSON checks passed.

This is one sparse-source synthesis, not a forensic reconstruction or varied-case
acceptance. The two news articles rely on common reporting, so publisher count is
not proof of independent investigation. Initial access, attribution and financial
loss remain unresolved. Generic possible access routes are not incident findings;
no missing exploit, IOC, mitigation or quantified impact was invented. The body
has six findings and one intelligence gap, not a demonstrated standalone analyst
assessment. Backend source spans do not yet provide an inline reader passage UI.
Evidence-change narratives and automatic living-report updates still need varied
validation. Global report generation stays disabled; comparison-case refreshes
await review of this first public result. Local gates: 1,257 offline tests passed
(two skipped) and 13 disposable PostgreSQL integration tests passed.

The one new dependency is the MIT-licensed tokenizer `tiktoken` (GPL-compatible).
Use the dependency-aware `SourceReport.Dockerfile` overlay with a verified runtime
base, pinned tokenizer, exact source revision and original runtime user; the
source-only overlay is not appropriate for this dependency addition.

### Running the local gates

```sh
python -m pytest -q
SV_TEST_DB_URL=postgresql://localhost/disposable_report_tests python -m pytest --run-db-tests -q tests/test_event_source_reports_postgres.py
```

Never use the production database as `SV_TEST_DB_URL`. For a held run, inspect
persisted response, review, exact quotes and reservations; do not reset its paid
call ledger. Public revision and source freshness checks remain mandatory.

## Legacy composition contract

Event composition v11 keeps the existing evidence ledger, independent audit,
qualification, publication, and static JSON boundaries. It changes only new
composition requests and their reader-facing rendering. Existing immutable Event
revisions retain their recorded workflow and remain reproducible.

## Reader contract

An Event report starts with a short executive overview. Detail sections then add
evidence-backed information instead of restating that overview:

- attack vector: delivery or initial access;
- attack path: post-access activity and progression;
- timeline: dated milestones in normalized chronological order while preserving the
  source's displayed date and precision;
- impact: operational, data, financial, safety, or downstream consequences;
- response and recovery: investigation, containment, remediation, restoration, and
  current state;
- mitigations: source-supported defensive guidance, never presented as completed
  response;
- attribution: who made the attribution and its qualified basis;
- open questions: material unknowns that remain unresolved.

Only dimensions supported by accepted facts are requested. A supported dimension
must be covered in the first generated draft; absent evidence remains an empty
section. The validator rejects identical and near-identical recycled prose. It does
not impose a word target and cannot authorize invented detail.

Every generated paragraph is either a `sourced_finding` or an
`analyst_assessment`. Sourced findings carry no confidence label. Analyst assessments
must be explicitly framed as assessments, cite all supporting facts, and use high,
moderate, or low confidence. The independent support audit checks that the confidence
and conclusion do not exceed the cited evidence. Unsupported detail may still be
deleted by the existing one-pass remediation; completeness never overrides safety.

## Evolution and compatibility

The public revision footer retains deterministic added, superseded, and disputed
counts. Corrective and additive revisions also render the available changed fact
statements with source citations in a `What changed` section. No daily feed JSON,
article summary, scraper, Event-index schema, URL, publication pointer, or build
contract changes.

## Acceptance gates

- all material prose cites active accepted facts;
- every evidence-backed detail dimension appears in the initial draft;
- sourced findings have null confidence and assessments have an explicit confidence;
- duplicated or near-duplicated narrative is rejected;
- timeline milestones are chronologically ordered after conservative date parsing;
- support audit and publication integrity validation remain mandatory;
- audit remediation may remove unsupported detail but may not add evidence or claims;
- older workflow revisions render and publish under their recorded contracts;
- article ingestion, summaries, daily JSON, Event index, and static-site checks pass.

Roll out behind the existing Event composition enablement and serial hosted-model
queue. Canary one evidence-rich private Event first, inspect section usefulness,
assessment calibration, audit outcomes, latency, and token use, then qualify a public
revision only through the normal promotion and atomic release path.
