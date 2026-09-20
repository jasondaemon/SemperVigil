# Private Event Narrative Composition

Event narrative composition is a private, review-gated step between an accepted
Event ledger revision and any future publication workflow. It does not write the
legacy `events` tables, alter article summaries, generate feed JSON, mark a build
dirty, invoke Hugo, or authorize publication.

## Admission and execution

- An operator may queue composition only for an accepted, lineage-current Event
  ledger revision.
- The feature is default-disabled and requires
  `SV_EVENT_LEDGER_COMPOSITION_ENABLED=1`.
- One deduplicated `event_ledger_compose` job runs on the existing serial
  `llm_local` worker with one provider attempt and no repair, retry, or fallback.
- The request identity binds the accepted revision, active local model/profile,
  prompt, schema, and generation settings. A changed ledger or configuration
  invalidates the work rather than replaying it.
- Superseded and conflicting facts are excluded before inference. Input is capped
  at 48 KB and output at 24 KB.

## Evidence contract

The model receives only active ledger facts and their retained exact passages.
Every narrative item must cite one or more active fact IDs. The validator rejects:

- unknown, superseded, or conflicting fact references;
- timeline dates not explicitly present on a cited fact;
- unresolved questions not grounded in an allegation or uncertainty fact;
- additional fields or an incomplete structured response.

The sections are overview, attack vector, attack path, timeline, impact,
response/recovery, mitigations, attribution, and open questions. Unsupported
sections remain empty. The deterministic ledger change record is attached by
code, not composed by the model.

Fact references provide traceability, not semantic proof. A human must inspect
the narrative against the displayed fact IDs and exact ledger before acceptance.

## Review and revision behavior

Generated records start as `unreviewed` and always carry
`public_eligible=false`. Admin review may accept, hold, or reject them privately.
Only one accepted composition may exist per ledger. A later accepted material
ledger revision makes the prior composition stale; stale compositions cannot be
accepted. A new accepted composition supersedes the previous accepted one
without deleting history.

The operator surfaces are:

- `Content > Event Ledgers`: queue one private narrative from an accepted ledger;
- `Content > Event Narratives`: inspect citations and accept, hold, or reject the
  result.

No action in either surface publishes an Event.

## Verification boundary

Offline checks cover active-fact filtering, schema enforcement, exact dates,
question uncertainty, default-disable behavior, one-attempt execution, replay
rejection, worker registration, and queue routing. The disposable PostgreSQL
lifecycle covers migration, private storage and review, plus stale lineage after
an additive ledger revision.

The first production canary must consume exactly one local model call, create no
public Event/build/feed change, and remain unreviewed until its evidence quality
is inspected. Publication remains a separate future gate.
