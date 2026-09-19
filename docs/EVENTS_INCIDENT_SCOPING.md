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
Coalesce pending jobs, reuse unchanged assessments, limit inference by measured
capacity and pause when fresh-content budgets are exceeded. Current `llm_runs`
coverage is incomplete, so it is not yet a complete automated budget meter.

Validated immutable revisions and a publication pointer must remain separate from
model suggestions and incident lifecycle. Failed assessment, stale evidence or
scope changes preserve the last validated public revision. Public exports and daily
JSON remain compatible and publish only through the existing platform API and
atomic release process. The current private artifacts are not public revisions.
