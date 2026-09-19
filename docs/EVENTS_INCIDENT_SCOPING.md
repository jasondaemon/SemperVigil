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

This is a design checkpoint, not a deployed scoping system. Existing matching and
evidence modules are foundations; their structural validation is not semantic
approval. The first integration remains private and disabled by default.

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
