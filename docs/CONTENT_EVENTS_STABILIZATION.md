# Article-to-Events stabilization path

Date: 2026-09-19

Status: authoritative implementation sequence for article enrichment, event
identification, curation and narrative publication. S1 passage-bound extraction
and the S2 reviewed sidecar are deployed. Two frozen revisions are accepted and
one is held; automatic evidence admission remains blocked. The private S3
candidate sidecar is deployed, and one WaterPlum campaign candidate is enrolled
without creating an Event. Current article summaries, daily downloads and public
Events remain served.

## Outcome

Keep publishing the existing per-article summaries without interruption while
building a separate, versioned evidence path that can support conservative event
identification and useful living incident reports.

The target flow is:

```text
stored article text
  |-- current summary job --> current article/feed publication (unchanged)
  `-- shadow evidence job --> accepted article evidence
                               --> incident candidates
                               --> curated event ledger
                               --> narrative revision
                               --> existing guarded publication path
```

The shadow branch is not allowed to erase or delay the live branch. A failed
evidence extraction leaves the published summary and the last authorized Event
unchanged.

## Release invariants

Every implementation slice must preserve these properties:

- Existing article summary prompts, writes, feed fields and build triggers remain
  unchanged until a separately measured replacement is approved.
- Daily article/CVE JSON paths, IDs, field types, counts and null behavior remain
  compatible with downstream consumers.
- The local inference lane stays serialized. Fresh article summaries, article
  context and CVE enrichment take precedence over experimental or Event work.
- Evidence work does not request a site build. Only a newly authorized public
  Event revision uses the existing builder-owned export and atomic activation.
- Source or model failure preserves the last good article output and last
  authorized Event. Revocation and known-invalid content use the existing explicit
  withdrawal path rather than silently retaining bad content.
- No historical backfill is automatic. New-article canaries precede bounded,
  operator-initiated repair.
- No same-model judge is added to the mandatory path. Code validates identity,
  structure, bounds and references; a finite reviewed corpus measures meaning.

## Data boundary

Do not overload `summary_llm` or the existing context payload with publication
authority. Add a durable sidecar evidence revision keyed by:

- article ID and exact stored-text hash;
- extraction workflow and schema version;
- provider, model, prompt and parameter fingerprint;
- creation status: `unreviewed`, `accepted`, `held` or `superseded`.

Each proposed fact references one or more deterministic passage IDs. Passage IDs
and exact text spans are created by code before inference. The model selects IDs
and supplies a concise statement, fact kind, date role, attribution and uncertainty;
it does not reproduce quotations. Code attaches the exact source text afterward.

An accepted revision is immutable. A changed article text or generation
configuration creates a new revision and makes the prior revision ineligible for
new Event updates without deleting it. Public summaries continue using their
existing storage during this work.

## Staged implementation

### S0 - Protect and measure the live path

Deliverables:

- A release check that proves representative current and historical daily JSON,
  news/CVE rendering, search, metrics and existing Event URLs still work.
- Queue telemetry that separates fresh article/CVE work from lower-priority Event
  work and reports actual provider attempts and latency.
- A frozen article-quality corpus containing the three inspected articles plus a
  small blind set covering allegations, incident versus publication dates,
  quantities, mitigations and incomplete reporting.

Exit gate: baseline is reproducible, no unexplained fresh-content backlog exists,
and rollback images/configuration are identified. This stage changes no content.

### S1 - Passage-bound article evidence in shadow

Implementation status: contract, private queue and real-model comparison complete.
The model selects stable passage IDs; code attaches exact source text/offsets and
binds uncertainties to passages. The model still confused advisory/incident dates,
omitted material facts and inconsistently classified allegations. S1 failed its
semantic exit gate; do not enable automatic admission.

Deliverables:

- Deterministic paragraph/passage segmentation with stable IDs and bounded context.
- A private queued extraction that returns passage IDs and typed facts, then has
  code attach exact spans. It writes only sidecar revisions and telemetry.
- Source/configuration freshness checks, idempotent replay and one-attempt behavior.
- A human-readable comparison of source, current summary/context and candidate
  evidence for the frozen corpus.

Exit gate:

- zero unsupported material claims, invented dates or lost allegation qualifiers;
- every accepted fact resolves to its exact current source passage;
- required facts in the frozen corpus have at least 85% coverage, with omissions
  shown as holds rather than invented fill;
- unchanged replay uses zero inference and changed text creates a new revision;
- no article, Event, build state or public JSON is changed by the trial.

If the local model misses this gate, stop. Decide explicitly between a stronger
model, narrower extraction scope or editorial review; do not add recursive judges.

### S2 - Reviewed evidence canary

Deliverables:

- Default-disabled suggestions for newly stored full-text articles only.
- Lower priority than fresh summary/context/CVE work, with admission paused while
  those queues are pending or the measured inference budget is exhausted.
- Explicit accept/hold/reject curation before a sidecar revision becomes eligible
  for incident matching, plus visible evidence and reasons in the existing admin
  jobs view. Nothing is selected by default.

Exit gate: seven days of new articles show no summary publication delay, queue
growth, resource regression or public-output change. Evidence acceptance and
coverage stay within the S1 bounds after review. No broad historical backfill yet.
Removing routine review requires a separately evaluated stronger model or policy;
job success is not sufficient.

### S3 - Conservative incident identification

Deliverables:

- Candidate signals derived from accepted facts, explicit CVE IDs, named campaigns,
  affected products/organizations, actors and incident references.
- Incident identity separate from a vendor/entity and from article publication
  date. A shared company, CVE or product alone cannot merge incidents.
- A reviewable candidate state machine: `suggested`, `held`, `rejected`, `enrolled`.
  Decisions and reasons are durable and idempotent.
- Admin views for candidate evidence, conflicting signals and source coverage.

Exit gate: a reviewed historical set rejects cross-incident contamination and
duplicate incidents while finding the known target incidents. No candidate is
published or merged automatically during this stage.

### S4 - Versioned event curation and composition

Deliverables:

- A durable event ledger of accepted article facts, source versions, passage IDs,
  assertion status and explicit predecessor/supersession relationships.
- Deterministic retention of unchanged facts; only changed/new sources invoke
  extraction. Contradictions and ambiguous corrections are held for review.
- One composition call per material ledger revision producing an overview, attack
  vector/path, timeline, impact, containment/recovery, open questions and change
  summary. Every material statement references ledger facts.
- A reader-page preview plus compact coverage, omission, conflict, call-count and
  latency appendix in the existing admin workflow.

Exit gate: one initial report, one additive update, one correction and one unchanged
replay pass the frozen whole-report review; a blind second incident passes without
prompt edits. Unchanged replay causes zero model calls and no build.

### S5 - Controlled narrative publication

Deliverables:

- A versioned narrative bundle and qualification distinct from the current
  quotation-only format.
- First report requires explicit review. Initially, ambiguous corrections,
  incident merges/splits, source withdrawals and material impact changes also
  require review.
- Reuse the existing immutable revisions, public pointer, authorization checks,
  matching HTML/Events JSON export, builder API and atomic release switch.
- Stable Event URLs and a visible revision/change history.

Exit gate: one API-driven production publish and subsequent update preserve all
daily downloads and existing pages; failed generation retains the prior authorized
report; revocation/withdrawal removes only the intended Event output. The canary
then runs for seven days before another event is enrolled.

### S6 - Expansion and cleanup

Only after the canary succeeds:

- enroll a small number of additional incidents and measure discovery precision;
- allow low-risk additive updates within the evaluated policy while retaining
  review for corrections and identity changes;
- run bounded operator-requested historical evidence repair;
- inventory and retire legacy candidate/report writers only after proving no
  public pointer, source link or rollback path depends on them.

The old candidate backlog is not a source of truth and is not bulk-promoted or
deleted as part of stabilization.

## Immediate implementation order

1. Completed: freeze the S0 corpus and acceptance rubric from stored articles.
2. Completed: deploy deterministic passage segmentation and passage-ID selection
   in the private article queue.
3. Completed: deploy immutable reviewed evidence. Two frozen revisions were
   accepted; the allegation-typing failure was held rather than normalized away.
4. Completed for the canary: deterministic, non-publishing incident candidates from
   accepted revisions only, with explicit enrollment/hold/reject states and
   stale-evidence ineligibility.
5. Next: build the private versioned Event ledger from the enrolled WaterPlum
   candidate before adding any narrative call or publication authority.

This order deliberately postpones prompt changes to the live summary publisher.
Better evidence may later feed a revised per-article summary, but that becomes a
separate side-by-side release with feed compatibility and reader-quality checks.

## Operational scorecard

Track these numbers for every stage:

| Measure | Stabilization expectation |
| --- | --- |
| Fresh summary age | No regression from the pre-canary baseline |
| Fresh summary/CVE queue | Evidence/Event admission pauses while work is pending |
| Evidence provider attempts | One per changed source; zero for unchanged replay |
| Evidence structural acceptance | Report accepted, held and failed separately |
| Critical factual errors | Zero in the release corpus |
| Required-fact coverage | At least 85% in the release corpus |
| Candidate precision | No known cross-incident merge in the reviewed set |
| Event composition calls | One per material ledger revision |
| Unchanged event update | Zero inference, zero public build |
| Failed generation | Last authorized public output remains available |
| Public contract | Daily JSON and existing URLs remain compatible |

Passing tests or completing a job is not a quality result. Each gate requires the
reader-facing artifact, its source coverage and measured operational cost.
