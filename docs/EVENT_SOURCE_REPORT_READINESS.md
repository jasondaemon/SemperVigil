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
Recovery image `5b7cb42` is deployed to all four scoped workloads, migration 070
is installed, and the fresh scoped rendered/live diff is zero. The immutable
audit released the original proven-unused 10,115-token reservation and admitted
one parent-linked child. Its one completed writer used 8,050 tokens, but exact
quote validation held the candidate (`event_report_quote_not_in_source`) before
review: a cited source comma was changed to a period. No reviewer, correction,
approval or publication followed. The original terminal attempt remains intact;
global admission is disabled and both event public predecessors are unchanged.
This supersedes the pending recovery/reservation status above. The candidate is
not an accepted analyst report, and no automatic retry is authorized.

## Passage provenance, not model transcription

Citation resolution now returns source ID, original offsets, canonical original
passage and a stable content-bound passage anchor. Exact citations need no mapping.
A unique whitespace/separator-punctuation-only match records the generated quote
and original passage explicitly; generated report text and raw response remain
unchanged. Case, lexical, negation, numeric and identifier differences, and
ambiguous normalized matches still hold. Internal punctuation and operators are
significant. This mapping is provenance, not a factual-support determination;
whole-context review is still required. No prompt/schema expansion or writer
replay is needed for the existing candidate.

An operator-only preserved-review continuation permits one review of the exact
completed writer body held on citation resolution, under existing freshness and
budget gates. It refuses existing review attempts and leaves the original failed
job and held run state intact; it neither corrects nor publishes. This helper and
resolver were loaded ephemerally in the configured hosted worker for the scoped
review, without a deployment or persistent runtime patch. Running images remain
`5b7cb42`; the source changes are not yet deployed.

The Astrana reviewer used the complete four-source packet, exact previous report,
known single-source delta, unchanged writer body, provenance mappings and explicit
manual review questions. Its 10,278 tokens plus the writer's 8,050 total 18,328;
summed reservations were 22,015, below 24,000. It returned four substantive issues:
independence inflation, filing-date errors in P116/P118, and incomplete current
notification status. The run remains held with `substantive_review_issues` and
zero outstanding reservation. No additional writer/correction call, approval,
promotion, deployment or publication followed. Manual concerns not flagged by the
reviewer (including financial-expectation coverage) are not thereby cleared.

## Explicit final-two-phase allowance

Migration 071 adds an immutable operator allowance for correction/verification
only. The original cohort limit and snapshot remain unchanged; final-phase costs
are separately capped by summed reservations and audited with original budget,
prior usage and report/review identities. The run budget is extended only through
that audited grant, not a status/job reset. The four-call database ordinal limit,
unique phase constraints, freshness checks and durable journal prevent a fifth
attempt. Compact correction returns only flagged item objects; application-owned
assembly preserves all other items, title and kind. Generic instructions preserve
primary/derivative attribution, materiality versus filing dates and completed
versus ongoing/intended response actions. Final verification reads all sources
and the entire assembled candidate.

The explicitly authorized Astrana final pair preflight reserved at most 21,390
tokens including a growth margin, below the additional 22,000 allowance. Actual
summed reservations were 20,827; usage was 9,188 correction + 8,507 verification =
17,695, bringing all four calls to 36,023 actual tokens with zero outstanding
reservation. P112/P113/P115/P117 and title/kind were preserved unchanged. The final
review held P118 because selected passages omit the materiality-date and
notification premises, while explicitly recognizing support elsewhere in the full
primary source. No fifth call, new deployment or publication followed. Runtime
remains 5b7cb42; migration 071 and its audit were applied for this bounded operation.
Local tests: 1,278 offline passed/two skipped, 21 disposable PostgreSQL tests passed.

## Source-level review contract and independent shadow

New generation snapshots identify the whole-cited-source review contract. Factual
support uses complete cited sources, while the entire packet is checked for
contradictions and missing qualifications. Exact or canonically resolved passages
remain provenance/navigation anchors, not exhaustive fact containers. Review returns
blocking substantive `issues` separately from nonblocking `locator_warnings`;
`ready` depends only on substantive issues. Stored legacy reviews retain their
original interpretation and holds. No previous issue was retroactively reclassified.

Writer guidance now explicitly preserves company beliefs, appropriately typed and
placed assessments, unreported action dates, distinct detail sections and material
financial/notification qualifications. Held-out breach, vulnerability, campaign and
law-enforcement fixtures test warning/issue contract separation, not automated
semantic truth detection. Publication reconstructs compact corrections from the
immutable original writer plus flagged item patch and checks exact assembled equality
against persisted report, then checks the immutable verification output. Legacy
span storage preserves original offsets/quotes and bundle identity; only absence of
new passage-anchor metadata is accepted. Actual Microsoft immutable publication
material passed this updated adapter read-only.

`SV_EVENT_SOURCE_REPORT_WRITER_MODEL` optionally selects an already-enabled OpenAI
writer; review stays fixed to the configured baseline model. The generator version
records writer and reviewer contract identity. Defaults/global admission do not
change. The independently authorized archived Astrana shadow selected gpt-5.6-sol
writer and gpt-5.6-luna reviewer, reasoning low, no correction and 24,000 summed
reservation ceiling. Conservative preflight bound was 22,345. The writer alone
reserved 10,306 and used 9,401 tokens (6,201 prompt plus 3,200 reasoning completion),
returned empty content with finish_reason length after 36,438 ms, and held before
review. No complete report exists; no reviewer, retry or repair was run. There is
no valid quality comparison or isolated model-superiority result. No configured
pricing columns were available, so no monetary cost was inferred.

The prior Astrana four-call hold remains unchanged. No source deployment, publication
or persistent generation enablement followed the failed shadow. Microsoft, Astrana
and the immediately preceding daily-feed bytes/pointers remain unchanged.
Verification: 1,283 offline tests passed/two skipped before the final model-selector
fixture; 22 disposable PostgreSQL tests passed, including compact reconstruction,
unchecked prose substitution refusal and legacy derivative span identity.

## Explicit per-phase configuration / Sol-none pair

`SV_EVENT_SOURCE_REPORT_PHASE_CONFIG` accepts bounded per-phase reasoning effort
and total completion caps. Requests and exact token reservations use those values;
generation identity includes all phase settings. Invalid effort/cap/unknown-field
configurations refuse before transport. Writer model selection stays separate;
the fixed reviewer is unchanged. Changing provider model defaults alone does not
override this explicit report-path contract. No global enablement is introduced.

The approved independent pair used Sol/none/6,000 and Luna/low/2,400 on the same
archived complete source packet, prior report, actual delta and analyst question.
Conservative preflight bound was 28,747 under 29,000. Exact reservations were
13,107 writer and 11,877 reviewer (24,984 total); usage was 9,059 + 9,101 = 18,160
tokens, zero outstanding. Writer returned 2,857 visible completion tokens with zero
reasoning in 29,476 ms; reviewer completed in 5,835 ms, ready=true/no substantive
issues, with one nonblocking locator warning about the subsidiary-identification
sentence elsewhere in the same cited source. Public-rate cost estimate is $0.090899,
including observed cache-write tokens; this is not an invoice.

Independent manual quality review held the otherwise model-accepted candidate:
P17 conflates spoofed caller-ID presentation with possession of a telephone number;
P18 labels undated detection strictly before September 22 without a supported day
bound. Raw output and ready reviewer verdict are preserved, not edited or cleared.
No correction/retry, source deployment, push or publication followed. Both earlier
Astrana holds remain intact; Microsoft/page/feed pointers and bytes are unchanged,
runtime is 5b7cb42 and global generation remains 0. Pre-call verification was 1,292
offline passed/two skipped and 22 disposable PostgreSQL tests passed; explicit
per-phase model-selector, identity and budget-refusal tests also pass.

The specifically approved final pair used an immutable manual-issue allowance,
Sol/none/1,000 correction and Luna/low/2,400 verification. This final-phase override
has a separate generation identity tied to the original generator and does not
rewrite either prior request/configuration or reviewer verdict. Correction scope
can restrict metadata fields; P18's body, citations, type and rationale were unchanged.
Preflight 23,504 was below the additional 24,000; exact final-pair reservations
were 22,832 and actual usage 18,400. All four calls total 36,560 actual tokens,
zero outstanding. Final review is ready with no issues or locator warnings.
P17 now distinguishes fabricated identity/caller-ID signals from demonstrated
employee authorization/control failure and labels independent verification as a
defensive inference. P18 is undated. All unflagged items are exactly preserved.
P26's October 5 label is the assessment cutoff for supplied evidence, not a claim
that the company investigation was independently verified still ongoing then.
Manual source/integrity review passed; both Astrana candidate and the existing
Microsoft immutable derivative passed the publication adapter read-only.
Source/privacy review passed; source commit/image `b4cd117` was pushed and built
from verified retained dependency bases. All four schedulable nodes received both
image digests. The four-workload rollout drains admission and active work first;
normal Astrana publication remains pending. No further model calls are authorized
on this candidate. Full offline verification: 1,296 passed, two skipped; actual
disposable PostgreSQL verification: 23 passed. Estimated public-rate cost of all
four calls is $0.146544, including observed cache-write tokens, not an invoice.
Existing separated approval/promotion/activation roles received only SELECT on
the immutable allowance audit table; no new credentials or publication-write
authority were added. Global report generation stays disabled.

## Astrana normal publication verified

October 5 approval/promotion `job_293b94633c1f453eb2f33bb3969056ae` and
scheduler-admitted build `job_fd04e01fc44f4008b4a94e973e8d92ec` succeeded.
Guarded release `20261005212350` publishes Astrana revision
`6d45e27d67c63152e2f9d3c72bd9c125725b7b742a2596a6ede68881cb773fb1`.
All 13 item texts match the accepted report; the public fragment matches its
activation manifest. Four source links include the SEC filing; update framing
identifies it as older newly incorporated evidence, not a later development.
Events JSON retains publication history; prior revision remains stored.
Microsoft page/pointer and immediately preceding historical-feed bytes are
unchanged. All 18 HTTP/markup/assets/sampled-feed checks passed. No new model
calls were made, and global generation remains disabled.

This supervised correction does not establish autonomous living-report readiness.
Varied-case evaluation, semantic source independence/novelty, bounded enrollment,
monitoring policy and operational alerts remain. Public chrome's “Independent
sources” counts supplied sources, not verified investigative independence;
“Living incident report” does not demonstrate enabled automatic updates.
Visible revision-history UX and passage inspection remain separate work.

## Unattended readiness audit and bounded enablement proposal

October 5 follow-up used no new hosted requests. Global admission remains 0;
`event.source_report.enrolled` and `.approved` are empty; no cohort or per-phase
model override is configured. The current runtime scope is Microsoft only.
Existing unrelated daily research enrollment is unchanged. Do not describe this
state as autonomous source-report publication.

### Verified boundaries and blockers

| Stage | Verified behavior | Remaining control before autonomous enrollment |
| --- | --- | --- |
| Intake/link | Existing ingestion and web-source validation feed linked raw articles; report snapshots freeze membership, metadata, bodies and suppression. Research is explicitly enrolled and interval-bounded (`event_living_research.py:21,79`). | Report cohort budget does not cover research validation or article-summary calls. Do not add fresh research in the first rollout. Existing web validation can use lexical fallback unless REQUIRE_LLM is set; verify incident-matching policy for enrolled routes (`worker.py:7212,7315,7418`). |
| Novelty/debounce | Exact-body deduplication, conservative lexical addition filtering, existing-body corrections/removals and metadata changes. Snapshot admitted with default 300-second debounce (`event_source_reports.py:189,301`). | No semantic independence/novelty proof. Cited-body correction may intentionally withhold stale managed report at export; uncited additions retain prior valid report. Expose this distinction and keep the immutable fallback. |
| Generation | Complete context or hold, saved prior/public baseline, per-phase model/config identity, exact pre-HTTP reservations, two-call default and max_attempts=1 (`event_source_reports.py:423,693`). | Scheduler currently calls submit with its hard-coded 24,000 default; the successful complete Astrana first pair reserved 24,984. Add validated per-run scheduler budget and explicit Sol-none/Luna-low settings before enrollment. |
| Publication | Separate approval admission, promotion, current-source/predecessor checks, immutable response reconstruction and guarded manifest activation (`event_source_report_publication.py:109,192`; `event_activation.py:110`). | Accepted output is not automatically approved: tick only publishes IDs in `event.source_report.approved` (`event_source_reports.py:738`). Implement an explicit, bounded, audited automatic-approval policy for eligible successor runs; do not invent IDs in settings or bypass roles. |
| Scheduler isolation | One admission per pass; per-event pending suppression and transaction/advisory locks. | Source-report tick errors are not isolated before build admission (`orchestrator.py:440,448`). A bad enrollment, unavailable source or exhausted cohort can abort the tick. Add per-event rollback/hold receipts and continue unrelated ingestion/build admission. |
| Failure/accounting | Review, quote/schema, freshness, budget and uncertain-transport failures hold; no ordinary replay of identical held requests; immutable response and reservation retained. | A held report can be a succeeded worker job containing a held result, so failed-job counts alone miss it (`worker.py:5086`; `event_source_reports.py:733`). Add a read-only report-status view and bounded alert/receipt outbox integrated into existing daily review. No unlimited repair or automatic reservation release. |

### First bounded operational policy (proposed, not enabled)

Use exactly Microsoft `evt_8d136739e530` and Astrana `evt_eef963338f1a`, not all
legacy Events or a 42-case backfill. Consume only newly linked evidence from
existing intake. Do not trigger generator-only backfill, enroll new research,
change article ingestion/summaries, or alter the daily feed pipeline.

The first window is 48 hours, at most one successor run per Event and two runs
total, one report job active globally, 300-second debounce. Require an immutable
cohort ID with a lifetime 64,000-token ceiling, 32,000 reserved/actual tokens per
run, Sol/none/6,000 writer and fixed Luna/low/2,400 independent review. Two calls
maximum per run; no automatic correction or extra allowance. Unknown transport
consumes its reservation. Complete context that cannot fit holds, never truncates.
Window expiry, admitted-run count, queue concurrency and scheduler per-run budget
are additional controls to implement, not capabilities implied by today's token
cohort. Do not rotate cohort IDs to reset spending automatically.

At the recorded public rates, 64,000 tokens at the maximum selected marginal
rate ($20/million) bounds report-only nominal token cost at $1.28; choose a
$1.50 report-only operator ceiling and reject provider/model/rate changes rather
than silently widening it. This is a conservative rate estimate, not an invoice
or bound on unrelated existing ingestion expenditure. Actual dollar enforcement
needs a recorded rate snapshot and preflight/post-call accounting. Provider cache
write accounting and uncertain usage remain visible. Do not claim an all-system
budget until validation/search/summary stages have separate budget coverage.

Automatic successor approval requires: explicit two-event policy and unexpired
cohort; current complete source snapshot; exact raw-response/assembled-report
identity; ready review with no substantive issues (initial policy also holds
locator warnings); current public predecessor; immutable qualification receipt.
Approval submits through the existing admission role, promotion job and normal
dirty-state build. It never directly writes public pointers or activates a release.
Source changes after approval hold at promotion/export/activation and cannot be
overridden. Initial publication of newly discovered Events remains out of scope
until separately evaluated: these two examples validate supervised updates only.

Expose per-event last checked/enrollment expiry, run/status/reason, actual writer
and reviewer models, phase/total usage, reserved unknown tokens, cohort remaining,
estimated rate-based spend, approval/promotion/build/release IDs and last valid
revision. Alert once per stable run/reason on held/incomplete/refused output,
uncertain transport, budget or 80% cohort threshold, stale source/configuration,
approval/promotion/build/activation failure and an accepted-but-unpublished run
older than 15 minutes. Route receipts to the existing daily review; add no new
external automation. Alert text excludes source bodies, credentials and raw HTTP
logs. Never equate succeeded job, ready model verdict or domain count with quality,
publication or source independence. Failure stops that report, not core ingestion.

Rollback: stop admission to this cohort, drain active hosted/build jobs, disable
the report flag/enrollment, preserve immutable attempts/reservations and return
to the last validated revision through normal qualification/release gates. Do not
delete holds, silently release unknown spend, revert unrelated historical feeds,
or force a stale report over a corrected/suppressed cited source.

### Deterministic acceptance evidence

Full existing offline suite: 1,296 passed, two skipped (five existing warnings).
Disposable PostgreSQL report suite: 34 passed, no hosted requests. Four kind
fixtures (breach, compromise, law enforcement, vulnerability) now traverse saved
fixture writer/review responses, restricted approval/promotion, public baseline,
rendering and simulated manifest/index/page verification. Exact fragment tampering
is refused. Qualified held derivatives retain original raw artifacts and baseline.
Each non-derivative kind also incorporates an older primary-source correction with
complete two-source context, prior report and exact known new-source delta, then
normally promotes a successor; stale prior admission is refused.

New PostgreSQL checks prove concurrent admission creates one run/job, concurrent
runs serialize their shared cohort reservation before transport, stale
generation reaches no provider, process death after saved writer/uncertain review
does not replay on restart and keeps its reservation, and accepted scheduler output
does not bypass the explicit approved list. Existing checks cover debounce bursts,
budget refusal before transport, unknown transport/cohort exhaustion, immutable
artifacts, terminal four-call repair and exact legacy evidence baselines. Existing
offline checks cover article matching, footer/navigation-only additions, source
corrections/removals, caller/source certainty contracts and configuration scope.
These fixtures prove mechanical boundaries, not semantic incident quality,
independent corroboration, real provider variance or an automatically scheduled
end-to-end production flow. Real Microsoft/Astrana publication was supervised.

The separate Hugo label change uses neutral “Source-backed incident report”,
“Sources” and the actual report publication time, with no unverified automation
or source-independence claim. Six isolated Hugo tests cover safe defaults,
publication versus source date, absent metadata, present-only section navigation
and unchanged body. Analyst assessment/what-changed navigation is included.

An additional operator-helper audit found `JournaledExecutor.__call__` selects
`self.phases[len(self.reservations)-1]` before appending the reservation, reversing
the two phase labels in its journal (`event_source_report_executor.py:28`). The
authoritative database call phases and request/response bodies are correct; normal
unattended `run` does not use this optional helper. Preserve historical receipts;
fix future helper phase attribution with explicit tests before reusing it. Do not
rewrite old journals or replay paid requests to repair descriptive metadata.

The label-only deployment completed after discovery/build drain, with admission
restored, no image changes and scoped deployment diff0. Hugo commit `ab283dd9`
is pushed; guarded normal build `job_5a4f1a5551574bf1ae08bde7f897b523` succeeded
and activated release `20261005213346`. Both live pages have factual source and
publication labels; Astrana assessment/update navigation targets exist. Both
reviewed report fragment hashes and revision pointers are unchanged, including
all 13 Astrana items. Microsoft whole-page bytes intentionally changed only with
the generic presentation update. Historical feed bytes remain stable and all18
public checks pass. No new model calls, report enrollment, cohort, credentials,
external automation or changes to core ingestion were made.

## Implemented bounded automatic successor controls (not enabled at checkpoint)

Source `e86c67e` implements the two-event pilot; `ed367b2` additionally refuses
the new status endpoint unless admin authentication is configured. Both commits
are pushed. Runtime is still the prior `b4cd117`; no pilot policy, cohort or
enrollment is enabled yet. Parent requested a safe milestone before deployment.

Pilot JSON freezes ID, exact events, UTC start/expiry (at most 48 hours), two total
runs, one queued/running run at a time, 32,000 tokens per run and 64,000 cohort
capacity. Admission locks the cohort and reserves each run's full lifetime
capacity, including held/unknown attempts; each Event has at most one slot.
Before transport, summed call reservations must fit the run ceiling. Two calls,
no repair, recovery, extra allowance, automatic retry or generator backfill.
Unknown transport remains reserved. Only genuine changes against a verified
public predecessor may enter this policy; unchanged evidence is a no-op.

Accepted pilot successors no longer need an approved-run list. The scheduler
checks generation freshness then submits normal restricted approval with an
audited policy/generation/source/report/review identity. Promotion reconstructs
responses and independently rechecks current source/predecessor, policy/expiry,
exact two successful model phases, review readiness, citations/types and budget.
It binds the stored generation identity to the receipt without reading provider
tables. Existing admission/promotion roles have no model/provider-table SELECT;
no new role, grants, credentials, migration or publication-write authority needed.
The normal worker marks the build dirty; manifest/export/activation remain guarded.

Report-tick failures roll back and record sanitized bounded last-tick receipts
without aborting core ingestion/build admission. Authenticated read-only
`/admin/api/events/{event_id}/source-report-status` exposes policy/expiry, actual
models, phase usage/call caps, charged/outstanding/admission capacity, conservative
report-only cost bounds, stable alert keys, hold/promotion/build failures and
accepted-unpublished age. Alerts are visible through existing operator access,
not a new messaging/monitor service. The journal helper now records correct
future phase labels; historical journals are preserved.

Verification: 50 real disposable PostgreSQL tests across both report suites,
including 16 new pilot tests. Fake provider only; real queue/response persistence,
restricted-role autoapproval/promotion and worker dirty-build handoff. Cases include
expiry, quota, concurrency, restarted unknown reservation, stale generation/source,
source bursts, refusal/review holds, tampered report/budget/predecessor, generation
receipt alteration and out-of-cohort/generator-backfill refusal. Promotion tests
explicitly revoke provider/model-table read privileges. Full offline verification
and final source/privacy check precede rollout; no paid calls were made here.

Final source-only images are built on docker45 from verified retained bases:
ingest `ed367b2` digest
`002699c035672f2b438af48ad73618cfa2e2bfc4279fceeb37d201672e8682ca` (1000:1000),
builder `ed367b2` digest
`c2044552819c20e57c1a417c0e00456345420e57fe7477052a1b302593487c97` (retained root).
Intermediate `e86c67e` images were imported on nodes42/46/47/52 but not deployed;
import the final `ed367b2` images before rollout. Admin also needs the new source
image to expose the authenticated status endpoint. Preserve all unrelated dirty
site/platform files. ConfigMap report keys are chart-wired; ordinary environment
updates require controlled pod recreation (do not assume envFrom hot reload).
Do not use stale private four-workload/tag scripts without updating scope/hash.

Enable only after final imports, source checks, drain and rollout verification.
Record verified UTC enable/start and exactly +48-hour expiry in the immutable
policy; configure exact Microsoft/Astrana scope, Sol/none/6,000 and Luna/low/2,400,
cohort64,000/run32,000/max2/maxConcurrent1. Add no research enrollment. Verify
unchanged evidence admits zero jobs/calls and last-tick/status/alerts are visible.
Preserve both public revisions and fragments. A real new-source unattended cycle
remains unobserved; never force one with synthetic production evidence or paid
proof calls. This checkpoint is not a broad-production completion claim.
