# Events architecture reset

Date: 2026-09-19. Baseline: application `fa59d95`, deployed LLM worker `d80f98a`.
Status: architecture assessment and recommended next implementation sequence.
No application, profile, deployment, scheduling, or production data changes made
for this review. No inference requested. This document supersedes the earlier
claim-auditor iteration sequence, not the existing publication safety contracts.

## Decision

Keep the reliable publication foundation. Stop developing the same-model,
per-claim auditor as a prerequisite to report writing. Prove one complete,
readable, incrementally maintained report with a much smaller workflow before
adding more infrastructure or enrolling more events.

Recommended path: **stored source evidence -> bounded source extraction ->
versioned evidence ledger -> one report composition -> publication policy ->
existing immutable revision/export/activation path**.

This is not a claim that evidence-first extraction guarantees truth. It reduces
the number of model tasks and makes their errors inspectable. Quality must be
measured on whole reports and updates, including an unseen incident. If the
current model cannot meet that test, stop rather than add more model judges.

## What the system actually does

| Path | Current behavior | Missing for the requested feature |
| --- | --- | --- |
| Legacy Events | Article classification creates/links candidates and queues `event_report_llm`; report consumes article summaries/context and promoted web snippets. | Incident-specific identity, exact evidence grounding, correction semantics, safe narrative qualification. |
| Managed public pilot | Explicitly enrolled scope admits short attributed excerpts, promotes an immutable revision, and triggers the normal build. | Coherent incident narrative, useful timeline, recovery/impact deconstruction, changed-source maintenance. |
| Private deconstruction | One bounded source call proposes up to eight claims; current cached sources compile into a private report. | Reliable citations/coverage, composition, durable maintained ledger and a narrative publication contract. |
| Private support audit | Up to two additional model calls per claim; returns suggestions only. | Reliable semantic judgments. It has failed the real cohort and cannot authorize publication. |

These are parallel paths, not one nearly finished pipeline. The support audit
does not feed the existing quotation publisher, and successful private jobs do
not advance a narrative publication pointer.

Read-only production observations during this review:
- Admin, orchestrator, LLM worker and builder each have one ready replica.
- LLM worker concurrency and max inflight are both one. Ollama reports
  Qwen 2.5 7B / 16,384 context, 6.3 GB loaded, 100% GPU. No capacity change needed
  to perform the proposed bounded experiment; throughput remains unproven.
- Jobs requested in the preceding 24 hours: 33 succeeded legacy derivations,
  four succeeded legacy reports, three succeeded promotions; private reviews had
  47 successes, nine failures and one cancellation. Job success is not report quality.
- Joined inference records for those private jobs: 101 records / 443.260 seconds.
  Legacy inference records did not appear in that join; do not infer zero cost.
  This is application-model telemetry, NOT Codex token/credit consumption.
- Three managed public revisions belong to one event. Its pointer is still
  `c44cc56060cba571615cc697fc2d4930edacacdb94bbedf92104d9ce46a90252`.

## Findings, ordered by impact

### 1. We optimized a checker, not the reader's product

`event_claim_support.assess` can add 16 calls to one eight-claim extraction.
It uses the same model as extraction, not independent verification. V4's original
17 cases contain one false acceptance and four false rejections; the additional
five-case holdout contains one false hold. Tests proving cache integrity and
publication isolation do not make these judgments reliable.

The staged plan's existing execution policy explicitly said not to add a model
call per claim/validator. Implementation diverged from that cost constraint.
Keep the fixtures as regression evidence, but remove this auditor from the
proposed production critical path. Retain it as frozen diagnostic tooling; no
new revisions/profiles until a separate, justified experiment requires one.

### 2. Publication is deliberately quotation-only

`event_projection.prepare` accepts a quote qualification, and `event_render.resolve`
reconstructs that exact format. The renderer hardcodes unknown incident dates and
an attributed-quotation summary. A better private paragraph cannot pass this
contract merely by flipping `public_eligible`. A versioned narrative contract
and explicit qualification policy are missing work, not a configuration issue.

Reuse source locking, predecessor checks, restricted principals, immutable
revisions, revocation, output verification and atomic activation. Do not reuse
quote qualification identities to authorize generated prose.

### 3. Living-event maintenance is incomplete

`event_review.snapshot` selects the first 12 linked articles by ascending ID and
marks further links truncated. `event_automation.candidates` rejects incomplete
snapshots, and `advance` holds changes to previously quoted documents. Those are
safe pilot limits, not an incremental event history design.

`event_deconstruction.changes` reports added/withdrawn claim IDs, not semantic
corrections. IDs bind the source request, so editing a document can replace all
its claim IDs. There is no implemented rule saying which old assertion is
corrected, remains supported elsewhere, or is disputed.

Use a paginated source-version inventory plus bounded changed-source work. Keep
unchanged evidence references in a durable ledger, not in every model prompt.
Never raise the snapshot/model limits to make a whole lifetime fit in one job.

### 4. Discovery can contaminate the report before writing begins

Legacy derivation keys events by kind and entity (`event:{kind}:{entity}`), not
an independently identified incident, and uses publication/ingestion date when an
incident date is unknown. The conservative `event_matching.match_incident` helper
has no runtime caller. The new pilot only has explicitly enrolled scopes.

Keep general discovery out of the first report milestone. Subsequently treat
retrieval as candidate generation, not an automatic merge. An explicit incident
reference or evaluated scoped match is needed; a shared company, CVE or system
alone is not enough. Do not bulk-promote or delete the candidate backlog.

### 5. The older generator is not a safe shortcut

`worker._event_report_profile` can fall back to an article-summary profile.
`_build_event_report_input` includes old summaries/narrative; `_parse_event_report_output`
accepts raw text as overview and can backfill timeline rows from other sections
using a common incident date. These explain why restoring old output is not a
correctness solution. Leave legacy serving intact, but prevent its writer from
being an alternate writer for newly managed narrative events at cutover.

### 6. Limits and documentation need one authoritative interpretation

A job has `max_attempts=1`, but router `_http_request` can retry timeout requests
twice. One application inference record need not equal one provider attempt.
The byte budget is not a token budget; source text plus quote-enum schema also
duplicates input. Smaller bounded evidence avoids that duplication; no silent
truncation or larger model context should be assumed.

Several documents contain historical "not deployed" statements below newer
overrides. This review and the tracker define the next sequence. Historical
logs remain evidence, not competing instructions. No more rollout merely to
change a prompt before a bounded whole-report experiment passes.

## Keep, simplify, retire later

| Component | Decision |
| --- | --- |
| Shared serial queue and existing Jobs dashboard | Keep. Extend existing job results for report coverage, cost and holds; no new queue/dashboard. |
| Exact source snapshots, offsets, versioned cache | Keep. Evidence remains untrusted reporting; hashes prove identity, not truth. |
| Revision/source locking and public-pointer controls | Keep. These solve real stale-write and authority problems. |
| Builder-owned exports and atomic activation | Keep untouched. API-driven build only; no extra per-source build. |
| Private extraction and compilation | Simplify into evidence-first source work plus one composition; reuse storage/cache primitives. |
| V1-V4 support judge and paired review variants | Freeze experiments. Not mandatory steps in the new report path. Preserve fixtures/history. |
| Quotation pilot | Keep serving during development. Do not present it as the target narrative product. |
| Legacy derivation/report writers | No new reliance. Disable per managed event during controlled cutover, then retire only after inventory and rollback testing. |
| MCP / skills | Retain as optional internal access/procedure adapters. Do not add them to worker execution or treat them as better reasoning. |
| New research agents, embeddings, model upgrades | Defer. None fixes the unproven report workflow or is needed for its first test. |

## Minimal report workflow

1. **Identify changed evidence without inference.** Inventory source membership,
   suppression and text versions for one enrolled incident. Distinguish material
   source changes from timestamp churn. Keep the initial demonstration bounded;
   versioned pagination is required before lifting its source cap.
2. **Extract once per changed source.** Select stable passage IDs before proposing
   concise facts. Retain full relevant paragraphs/antecedents within the budget,
   attribution, qualifiers, date text and precision. No duplicated full-sentence
   enum. Missing/oversized context becomes a visible hold, not an empty success.
   IDs and typed spans make references checkable; they do not prove entailment.
3. **Maintain the ledger deterministically where possible.** Store source version,
   passage IDs, section, assertion status, proposed fact and optional date, plus
   predecessor/supersession references. Exact duplicates need no new prose.
   Changed-source facts lose current eligibility pending reevaluation; a model
   cannot silently resolve disagreement or declare independent corroboration.
4. **Compose once per material event revision.** Give the writer a bounded current
   ledger, not prior prose or whole article history. Produce concise overview and
   section paragraphs with evidence IDs per statement. Timeline and change list
   are rendered from structured dated evidence/diffs. No invention to fill a
   section. If the required ledger does not fit, hold with visible coverage;
   do not silently drop inconvenient facts. No call for unchanged evidence.
5. **Qualify separately, then reuse publication.** Validate references, versions,
   membership, unsafe content, schema and budget in code. Evaluate factual meaning
   through a finite pre-release human-adjudicated corpus and whole-report review,
   not another mandatory call to the same model. First public revision gets an
   explicit review; later unattended admission is restricted to the evaluated
   scope/update policy. Broad discovery and ambiguous corrections remain held.

The writing role remains substantive: the reader should get a coherent incident
deconstruction, not a renamed quote digest. Structural validation cannot certify
arbitrary prose. If a fully unattended workflow cannot meet measured accuracy
with this model, say so. A stronger model or ongoing editorial review is a later
explicit tradeoff, not an automatic fallback. No claim of AdSense eligibility.

### Publication and correction boundary

Add a versioned narrative bundle/qualification only after the private report
experiment succeeds. Reuse the existing transaction/export design, but extend
the format dispatch deliberately; this is not a generic prose field on a quote
record. A durable current ledger/revision must not depend on disposable HTML logs.
Keep stable event URLs and all daily-feed JSON contracts unchanged.

Unchanged still-authorized reports remain live during failed generation. A
revoked or known-invalid report must not be retained under that rule: use the
existing withdrawal/authorization path, with an evaluated corrected revision or
safe withdrawn state. Today `publish.write_events_authorized_snapshot` raises
`event_export_withheld` for a held managed event; a stale event can therefore
hold a shared site build. Correction/withdrawal handling must be demonstrated
before enabling unattended narrative updates. Never bypass the activation guard
or redesign Hugo to conceal this issue.

## One bounded experiment, then a decision

No inference was run during this architecture review. Proposed next experiment:

| Scenario | Maximum logical model calls | Acceptance |
| --- | --- | --- |
| Vercel initial report, four complete sources | 4 extraction + 1 composition | Readable overview, attack vector/path, concrete impact and recovery where sources support them; explicit gaps; every material statement traceable. |
| One new source with material recovery information | 1 extraction + 1 composition | New detail added, old supported details retained, meaningful change list. |
| One source correction | 1 extraction + 1 composition | Old assertion superseded/withdrawn, uncertainty preserved, correction shown; never append contradictory facts as jointly confirmed. |
| Unchanged replay, timestamp-only churn, duplicate job | 0 | Identical content identity; zero model calls, no new event export/build request. |
| Blind second incident, at most three sources | 3 extraction + 1 composition | Same factual/readability bar without incident-specific prompt edits. |
| One malformed-structure repair across the entire experiment | 1 | Optional; not a retry to obtain a preferred factual answer. |

**Total ceiling: 14 provider attempts**, counting timeouts and retries, not just
successful completions. The table assumes one complete bounded source per call;
an oversized source must be held, not silently split outside this allowance.
Initial experiment deadline: 10 minutes cumulative model time, no more than
90 seconds per attempt. These are proposed stop limits, not measured performance
promises. Enforce them in an Events-specific request/admission path before running
the experiment; do not change global article/CVE retry policy. A client timeout
must not release the single inference slot while the provider is still generating.

No automatic cloud fallback, parallelism, history backfill or continuous repair.
Do not admit pilot work while fresh summary work is pending. Interrupted work
resumes only from recorded remaining budget, with completed source calls cached.

The full V4 audit alone consumed 26 calls / 43.009 seconds for four sources,
excluding extraction. The proposed initial report is five calls, including
extraction and writing. Fewer calls is a design bound, NOT a predicted latency
or measured accuracy improvement; composition may take longer than each audit.

Freeze expected source facts before the run. Review all output statements for
material errors and the predefined must-cover facts for omissions. Required
negative cases include unrelated roundup paragraphs, same-company/different
incident, allegations, protected/unprotected categories, advice versus completed
recovery, unknown dates and embedded instructions. All critical cases must pass;
holds on required supported facts do not count as correct coverage. Review the
blind incident without changing the prompt or grading rubric afterward.

**Stop rule:** one unsupported material claim/date, mixed incident, harmful loss
of qualification, incorrect correction or budget overrun fails the gate. Record
the result and make an architecture/capability decision; do not start a V5-V10
judge loop. No production schema or presentation expansion before this gate.

## Trackable implementation sequence

| Slice | Deliverable | Exit / release condition |
| --- | --- | --- |
| R0: review | This assessment and tracker reset | Complete; documentation only. |
| R1: private vertical slice | Evidence-first extraction + one readable report + update/correction/replay fixture, using the existing job lane | Execute the bounded experiment above. No public output. Stop on failure; no new infrastructure first. |
| R2: controlled publication | Versioned narrative policy/bundle, durable ledger, explicit first-report review, writer ownership and withdrawal tests | Disposable PostgreSQL races; matching HTML/Events JSON; existing public/feed regressions; one API-driven production publish. |
| R3: maintenance canary | One enrolled event, bounded incremental inventory and coalesced updates | Seven days with observable no-change reuse, update/correction behavior and no fresh-summary or build regression. |
| R4: expand only if useful | Small additional enrollment, discovery evaluation and admin exception controls | Independent report quality holds; otherwise retain last authorized coverage and stop expansion. |

R1 must deliver the report, not another diagnostic screen. Its first review
artifact includes the actual proposed reader page and a compact coverage/cost
appendix. No new broad admin UI, MCP tools, dashboards, or infrastructure phases
are prerequisites. Later production admission should enforce the original plan's
provisional <=10% rolling inference-time allocation, with measured baseline and
fresh-work backpressure; it is not currently implemented by queue serialization.

## Evidence navigation

- [Legacy derivation/report/profile/parser](../src/sempervigil/worker.py):
  `_handle_derive_events_from_articles`, `_event_report_profile`,
  `_parse_event_report_output`, `_handle_event_report_llm`.
- [Quotation automation](../src/sempervigil/event_automation.py): `advance`, `candidates`.
- [Snapshot cap](../src/sempervigil/event_review.py): `snapshot`, `MAX_DOCUMENTS`.
- [Extraction](../src/sempervigil/event_deconstruction.py): `response_format`,
  `compile_report`, `changes`; [audit](../src/sempervigil/event_claim_support.py): `assess`.
- [Publication format](../src/sempervigil/event_projection.py),
  [renderer](../src/sempervigil/event_render.py),
  [transaction](../src/sempervigil/event_publication_store.py),
  [export holds](../src/sempervigil/publish.py),
  [release](../src/sempervigil/event_release.py),
  [activation](../src/sempervigil/event_activation.py).
- [Unwired matcher](../src/sempervigil/event_matching.py),
  [transport attempts](../src/sempervigil/llm/router.py): `_http_request`.
- [Frozen evaluation results](EVENT_CLAIM_SUPPORT.md),
  [original execution policy](EVENTS_STAGED_IMPLEMENTATION_PLAN.md),
  [MCP boundary](EVENTS_MCP_ARCHITECTURE.md).

No claim is made that a documentation review fixes any of these gaps. Next work
is judged by the R1 end-to-end report and finite experiment, not additional test
counts or successful review jobs.
