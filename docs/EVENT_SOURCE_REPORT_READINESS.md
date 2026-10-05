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
