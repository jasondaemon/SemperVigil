# Incident scoping after the real-model pilot

## Evidence and decision

Three bounded cohorts used the existing serialized local model. V3 fixed response
completeness and improved the eight provisional cases to 6/8, but still included
an unrelated Commission staff/MDM incident and generic Odido background. This is
not a sufficient semantic publication gate. Do not continue changing wording or
reduce the test expectations to make the pilot appear successful.

The current input combines a legacy event title, company aliases and linked
articles. Legacy links already mix incidents. A title is not an independently
established incident scope, and the assessor cannot establish that scope simply by
agreeing with its own earlier output. The next implementation must carry a concrete
source-grounded incident definition across retrieval, assessment and revisions.

## Next bounded implementation

1. Introduce a private immutable scope declaration identifying a source document
   version and exact anchor passage. Any entity, affected-system or incident-specific
   reference fields must point to exact source spans. A company alias alone is not
   an incident reference. Dates keep their explicit role and precision.
2. Validate the declaration against the snapshot before constructing model input.
   Missing, changed or ambiguous anchors produce a hold/error, not a fallback to
   the first article. The assessment response cannot select or approve its own
   trusted scope. A model-generated scope remains a proposal until independently
   qualified; a caller-supplied scope is not automatically trustworthy either.
3. Supply the exact anchor alongside bounded candidate quotations. Keep one model
   invocation per current private job, existing model/provider and token limits.
   First evaluate whether explicit comparison fixes the observed cross-incident
   cases before introducing more calls or automatic admission.
4. Use the existing conservative incident-reference matcher only after reference
   provenance and authority are established. Do not treat shared company names,
   a CVE ID alone, copied article titles, or arbitrary overlapping words as proof
   that two passages concern one incident. Conflicting references abstain.
5. Retain all rejected/held/omitted evidence in private review with an explicit
   reason and coverage counts. Never hide negative cases from evaluation merely
   by changing retrieval. Keep the current provisional cases and add changed-anchor,
   repeated-organization, roundup and reference-conflict cases.

The source-anchor and private-job integration is deployed for a bounded private
pilot. Existing matching and evidence modules remain foundations; structural
validation is not semantic approval. The first integration remains private and
disabled by default.

## Implementation contract

`event_scope.propose` binds one 35-1,200-character anchor to its full stored
document version and event ID. Two to four distinct source-span focus roles
identify the entity and an affected system, attack mechanism or reported
reference. Exact quotes, offsets, title and URL are revalidated from the current
worker snapshot before inference. Role labels are proposals, not an entailment
test or verified chronology. Source changes invalidate the proposal; a report-only
event timestamp change does not change its identity. The broader assessment
request still includes the private packet version and is not yet suitable as an
automatic report-refresh fingerprint.

Admin accepts an optional `scope` object on the existing authenticated private
review endpoint, bounded to 6,000 JSON bytes. Admission checks metadata and its
hash without reading article bodies or invoking inference. The worker checks
actual source provenance. The separate `SV_EVENT_REVIEW_SCOPE_ENABLED=1` flag and
`SV_EVENT_REVIEW_SCOPE_PROFILE_ID` are required; the latter must use the exact
scoped system prompt, existing local provider/model, and existing bounded params.
Missing/incorrect configuration fails closed instead of silently dropping scope.
No automatic admission or public write is added. The existing job type remains
visible in the dashboard, with the same low priority and single-attempt policy.

Requests keep the existing round-robin candidate selection, 12-item/12,000-byte
maximum and one model call. Scope is separately included as comparison evidence.
The cache includes scoped request identity and refuses a valid assessment for a
different scope planted in the expected entry. Private HTML shows the anchor,
source citation, document version and explicit unqualified status; human choices
still start at Hold. Scope is retained inside the immutable assessment JSON.

The three real v3 request hashes and private HTML hashes remain byte-identical
under the local code when scope is absent. New offline tests cover source changes,
forged metadata, exact spans, default-disabled admission, profile mismatch,
stale-source rejection before model invocation, one-call reuse, escaping and
scope-aware quality-case pins. Scoped jobs have now been admitted through live
admin, but their real-model results remain pending. Independent qualification and
automatic publication remain unimplemented.

### Scoped pilot prepared

The chart now exposes the separate scope flag/profile with disabled defaults;
rendered overrides are tested. Three explicitly selected anchors retain all eight
v3 case labels unchanged in `assessment-*-scoped-v1.json`. Candidate coverage stays
12 per event; total request bytes are 11,165 (Odido), 10,650 (Vercel), and 10,731
(Commission), including system text. Source choices are provisional assistant
review, not independent qualification:

- Odido: article 21505, characters 694-833; customer contact system breach.
- Vercel: article 26194, characters 0-555; Context.ai compromise and Workspace access.
- Commission: article 25303, characters 0-168; Trivy/API-key/AWS compromise.

The unused scoped profile `fe0ae074-117d-560f-8350-83c9da87ed67` and prompt
`9ca763f0-f8af-5a8c-9e15-5238d530c8f6` were created/read back through admin. Same local
provider/model, temperature zero, 1,024 output tokens, 12,000 input characters, no
fallback/schema. Existing v3 profile and stage routing remain unchanged. No scoped
inference or public report has been requested at this preparation checkpoint.

### Deployment checkpoint

Admin and LLM worker now run source image `6141d61`; chart controls are committed
in `1015bea`, environment values in platform `39b6dd2`. Three scoped jobs are
queued at the unchanged private priority; exact IDs and rollback are recorded in
`STABILIZATION_VERIFICATION.md`. Do not duplicate or reprioritize the pilot.
Existing v3 profile/cache/attachments continue unchanged. Private scope proposals
and model suggestions still cannot authorize publication.

## Automation and publication boundary

New-event discovery should retain its originating evidence and proposed scope,
not just a generated title. An independently qualified seed can support automated
updates without manual review of every new article. Legacy mixed events must not
be silently assigned a scope from their first or latest linked article; prepare
auditable scope/split proposals instead. The qualification policy itself needs
implementation and evaluation before unattended publication is enabled.

Changed-evidence admission must fingerprint source versions, scope and generation
policy, not report-written timestamps. The current private packet includes the
legacy event `updated_at`, which is also changed by report writes: reusing that
unchanged in an automatic publish/refresh loop could cause self-triggered churn.
The local scoped-cache correction below addresses inference reuse, not automatic
admission or publication; the running pilot still uses its original deployed code.
Coalesce pending jobs, reuse unchanged assessments, limit inference by measured
capacity and pause when fresh-content budgets are exceeded. Current `llm_runs`
coverage is incomplete, so it is not yet a complete automated budget meter.

Validated immutable revisions and a publication pointer must remain separate from
model suggestions and incident lifecycle. Failed assessment, stale evidence or
scope changes preserve the last validated public revision. Public exports and daily
JSON remain compatible and publish only through the existing platform API and
atomic release process. The current private artifacts are not public revisions.

## Timestamp-independent scoped reuse (local, not deployed)

The scoped cache now keys the full bounded source snapshot, scope, exact model
input/system text and guarded generation identity, excluding only the event's
report-written `updated_at`. It still includes full documents, aliases, event ID
and title, omission/truncation coverage, and every source metadata field. Changes
outside the model's excerpt invalidate reuse too. Non-scoped v3 behavior is
unchanged, as are all request formats and quality-case hashes.

A hit revalidates the stored assessment against its original timestamp and the
current exact source snapshot, then maps its validated decisions onto the current
packet's passage IDs through the normal response validator. No stale packet IDs
are returned. Stored corruption fails before inference. Existing exact-snapshot
scoped entries can seed the new cache without another model call; no bulk cache
rewrite or deletion occurs. Scope proposals and returned assessments remain private.

Twelve additional tests cover timestamp-only reuse, correct rebinding, scope/cache
tampering, changed full sources, coverage/title/alias/model invalidation and legacy
entry reuse. 518 offline tests pass. This removes an unnecessary model call from
future scoped refreshes; it does not prevent all legacy report/build churn, establish
semantic correctness, or make private artifacts safe for public publication.

## Scoped cohort result and next bounded test

All three scoped jobs completed structurally, one call each, total 18,117 ms.
Commission improved to 4/4: all three staff/MDM passages excluded, Trivy/AWS
reporting retained. Odido remained 2/3: one company-services description excluded,
another similar description incorrectly included. Vercel fell to 0/1: the relevant
early breach report was incorrectly excluded. Total is still 6/8; no publication.
The anchors improved incident separation but did not establish reliable passage
classification. Do not adjust the expected answers or declare the feature ready.

Next, implement an explicit source-level private assessment option: one selected
article per queued job, at most its four bounded passages plus the same source
anchor. Keep the current scoped prompt/profile/model/response validator, input
limit, output cap and single-job policy. This changes task granularity, not prompt
wording or publication criteria, and matches the staged plan's per-article evidence
extraction approach. Whether it improves quality remains a testable hypothesis.

Preserve the complete packet and its excluded/unassessed coverage in private
review, rather than hiding unwanted cases. Unknown or unavailable selected sources
must fail before inference. Pin selected article identity in request/assessment/
cache metadata; an unrelated source's cached decisions must not be accepted.
The same eight positive/negative expectations span seven source jobs:
Odido 21505 and 22331; Vercel 26190; Commission 21216, 21218, 23638 and 25303.
Evaluate their union, requiring every case once and rejecting mixed snapshots or
scope versions. Do not treat missing coverage or all-Hold as success.

Do not enqueue until the new path and aggregate checker pass offline tests and a
targeted release is verified. At most those seven calls for this diagnostic cohort;
no automatic source-job expansion or public writes. A result that still misses the
mandatory cases remains unfit for unattended publication. Full-source cache
dependency narrowing, independent scope qualification, claim extraction and
transactional publication revisions remain later integration work.

## Source-level implementation checkpoint (local, not deployed)

The authenticated private-review API now accepts optional integer `article_id`
alongside `aliases` and `scope`. A source selection requires scope; no coercion of
strings, floats or booleans. The worker validates the source against its bounded
snapshot before inference. Each request selects at most four candidate passages
from that source and retains the full packet and omitted-passage counts. Existing
requests without the option keep their workflow, hashes and behavior unchanged.

The same scoped prompt/profile and one-call budget apply. Source ID and the new
`event-source-assessment-v1` workflow bind the immutable assessment. Cache identity
separates sources even when their model inputs are identical, while retaining the
tested report-timestamp rebinding. There is no automatic fan-out or public write.

Run the local gate with `.cache/mcp-venv/bin/python -m pytest tests/offline -q`.
556 tests pass, including 27 new source-path and aggregate-evaluation checks.
The three real v3 request hashes also remain unchanged. Queue/API integration is
tested offline; no new production jobs, model calls or deployment occurred.

`tools/check-event-assessment.py --source-bundle --packet PACKET --assessment BUNDLE
--cases CASES` evaluates a pinned per-event source cohort. Bundle metadata requires
the guarded generation identity on every assessment. Cases pin packet, event,
scope, generation, source request hashes and unchanged passage expectations. The
checker rejects duplicate/missing sources, mixed generation/scope/snapshot,
duplicate cases and unassessed cases; all-Hold fails positive expectations.
It is an operator diagnostic, not a cryptographic attestation or publication gate.

Next: prepare the seven exact source requests and fixture pins using the current
guarded generation identity; verify all eight existing expectations unchanged.
Then render/diff a targeted admin/LLM release, preserving the ordinary model lane,
and admit the seven-job cohort. Do not mistake the offline implementation for a
successful real-model result. The prior seven PostgreSQL and 19 JS checks are from
the preceding slices, not rerun in this source-only checkpoint.

### Source cohort pinned

The seven source requests are pinned in `assessment-*-source-v1.json`, with all
eight labels and allowed decisions identical to the scoped cohort. Guarded profile
preflight returned generation identity
`58bedc2903733c7b9f840791cc27583f0b0538167c397742249974fb275aa4e1` without inference.
Inputs contain three or four passages and 4,887-5,942 bytes including system text.
557 offline tests pass. Source runtime image `e93b6f0` is prepared; targeted
admin/LLM server-side spec comparisons show only container/init-container images
changing from `6141d61`. Rollout and actual model results are not yet verified.

Update: targeted `e93b6f0` rollout verified, platform `aadba3a`; all seven live
request hashes and guarded generation identities matched before admission. Jobs
are queued at unchanged priority. Exact IDs, public checks, cache/attachment
verification and rollback are in `STABILIZATION_VERIFICATION.md`. Semantic quality
is still pending; no public permission follows from a completed private job.

## Source cohort evaluated

All seven jobs attempted exactly one call: 15,021 ms total. Six produced validated
private artifacts. Odido now passes 3/3 and Commission 4/4, with all previous labels
unchanged. Vercel failed `invalid_assessment_item`: the model emitted `id:` instead
of `id` for one row. No artifact or cached valid assessment was accepted for that
job. Its raw p1 decision also excludes the pinned positive, so fixing the key alone
would not resolve the semantic failure. Do not normalize malformed output, count
the failed case as a pass, retry blindly, or publish the partial cohort.

There are two distinct next issues: enforce the exact output shape at generation
where the installed provider supports it, and preserve enough source context to
compare an early generic incident report with a later root-cause account. The
current Vercel positive describes the initial incident without the Context.ai
detail in the anchor. Exact quotations elsewhere in the same stored documents
carry additional context that the four-candidate snippets do not send. Stored
HTML/outbound-reference provenance is absent for these sampled articles, so do
not invent shared advisory links or treat model guesses as trusted references.

Before another model cohort, inspect the installed provider's constrained-output
capability and design a bounded source-pair evidence input. Keep the same local
model and single-call serialization; measure the request budget against actual
context capacity rather than silently truncating or raising limits. Preserve the
existing workflows/profile/cache and all eight cases. If required evidence cannot
fit or establish the relationship, hold explicitly. A paired-input test remains a
hypothesis, not an established fix, and must not unlock public reporting by itself.

### Installed capability and budget evidence

Read-only inspection confirms installed LiteLLM 1.77.7 maps OpenAI-compatible
`response_format.type=json_schema` to Ollama's schema-valued `format`, in both
completion and chat adapters. This is a capability observation, not a successful
end-to-end constrained-generation test. Ollama still reports the same Qwen model,
100% GPU and 16,384-token context. Seven source calls used 1,030-1,296 actual prompt
tokens and 77-100 output tokens each; no context-pressure evidence in this cohort.

A candidate Vercel paired-input layout retains both full stored texts (no silent
truncation), exact candidate quotes, the same scope, and untrusted-source treatment.
Removing duplicated 200-character windows and repeating document metadata yields
14,701 bytes including the existing system text, before explicit pair-role IDs.
It does not fit the current 12,000-byte contract. A separate opt-in, versioned
15,000-byte diagnostic mode/profile is a bounded next option, not a global limit
increase; keep 1,024 output tokens and the same single local model. Measure actual
tokens on the one failed Vercel source before considering broader use.

Some other source pairs exceed that budget (one Commission pair alone is 22,420
serialized bytes). Such requests must reject/hold without truncating, hiding
coverage, or falling back silently. Keep existing smaller successful modes intact.
Do not describe a successful one-source diagnostic as a homogeneous all-case
qualification. Pin new generation and request identities, test strict schema
transport and application validation, then admit at most that one diagnostic call.
