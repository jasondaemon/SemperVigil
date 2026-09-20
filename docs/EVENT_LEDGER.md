# Private Event Ledger

The private Event ledger is the reviewed boundary between accepted article
evidence and any future Event narrative. It does not read or write the legacy
`events` table, invoke an LLM, enqueue a build, or authorize publication.

## Admission

Only an `enrolled` incident candidate backed by an `accepted` article-evidence
revision can enter a ledger. Every revision has relational lineage to its source
candidate, evidence revision and article. Acceptance rechecks that lineage and
fails closed if a candidate is no longer enrolled or evidence was superseded.

Ledger content preserves each accepted fact's:

- immutable fact ID and exact statement;
- assertion kind and explicit date wording/role;
- article, candidate and evidence-revision identity;
- exact retained passage text and offsets;
- deterministic section tags used for inspection, not inference.

`public_eligible` is always false.

## Revisions and review

Content revisions are immutable. Review metadata may move a revision through
`proposed`, `held`, `accepted`, `rejected`, `superseded`, or `withdrawn`.
Only one open and one accepted revision may exist per ledger.

Supported material changes are:

- `initial`: first enrolled source;
- `additive`: another enrolled source and its accepted facts;
- `correction`: an operator explicitly identifies prior fact IDs superseded by
  new accepted evidence;
- `conflict`: an operator explicitly identifies prior fact IDs that conflict
  with new accepted evidence.

The system does not infer corrections or contradictions. Unchanged candidate
replay reuses the current revision and creates no row, model call or build.
Withdrawal removes the accepted state without deleting history.

## Operator surface

`Content > Incident Candidates` creates an initial private ledger or adds an
enrolled candidate to an existing ledger. `Content > Event Ledgers` shows exact
facts, lineage health, changes, and review controls. Correction and conflict
proposals are available through the authenticated admin API so their exact fact
IDs must be supplied deliberately.

## Verification

The offline suite covers deterministic classification and confirms publication
is not a ledger decision. The disposable PostgreSQL lifecycle covers initial,
unchanged replay, additive update, correction, conflict/hold/adjudication,
superseded-source detection and withdrawal while asserting no writes to legacy
Events or LLM-run records.

Narrative composition is now implemented as a separate default-disabled,
single-attempt private review gate. It is documented in
[`EVENT_COMPOSITION.md`](EVENT_COMPOSITION.md). Public publication remains a
separate future gate.

## Production canary

The accepted WaterPlum ledger revision contains 20 accepted facts and 20 exact
passages from the enrolled source revision. Its lineage is current. Replaying the
same candidate reused the accepted revision and left job count, LLM-run count,
legacy Event rows, Event-article links and public output unchanged.
