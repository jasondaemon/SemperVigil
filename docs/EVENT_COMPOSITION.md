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
  `openai` worker using `gpt-5.6-luna`, with one provider attempt and no repair,
  retry, or fallback.
- The request identity binds the accepted revision, hosted model/provider,
  prompt, schema, and generation settings. A changed ledger or configuration
  invalidates the work rather than replaying it.
- Superseded and conflicting facts are excluded before inference. Input is capped
  at 48 KB and output at 24 KB.

## Evidence contract

The model receives only active ledger facts, not raw articles or excluded facts.
To keep the editorial task small, each request assigns stable
request-local aliases (`F01`, `F02`, and so on); code maps those aliases back to
immutable fact IDs before storage. The model writes natural prose inside the
fixed Event sections, and every item must cite one or more active aliases. Each
fact carries deterministic allowed sections from the accepted ledger. Code, not
the model, restores fact IDs and explicit timeline labels. The validator rejects:

- unknown, superseded, or conflicting fact references;
- sections not allowed by every cited fact;
- timeline dates not explicitly present on a cited fact;
- omission of any explicitly dated fact from the timeline;
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

Accepted, current narratives expose a separate explicit publication action. It
records a composition-specific qualification through the restricted admission
role and queues the existing one-attempt promotion worker. The builder then
exports the immutable composition revision and the activation guard rechecks the
accepted ledger, composition, evidence revisions, article availability, and Event
membership before the atomic switch. Review alone never publishes an Event.

## Verification boundary

Offline checks cover active-fact filtering, schema enforcement, exact dates,
question uncertainty, default-disable behavior, one-attempt execution, replay
rejection, worker registration, and queue routing. The disposable PostgreSQL
lifecycle covers migration, private storage and review, plus stale lineage after
an additive ledger revision.

The first production canary must consume exactly one OpenAI call, create no
public Event/build/feed change, and remain unreviewed until its evidence quality
is inspected. At published September 2026 rates, a representative 5,000-token
input and 1,500-token output is approximately $0.003. Permanent revision-based
deduplication prevents scheduler polling from repeating that charge. Publication
remains a separate future gate.
