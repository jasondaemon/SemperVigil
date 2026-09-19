# Human-reviewed Events staging pilot

September 19, 2026: implemented and tested locally, **not deployed or enabled**.
This closes the explicit approval-to-worker handoff, not the automatic Events
feature or the final publication coordinator. It adds no production schema,
credentials, model calls, builds, or public content changes.

## Operator workflow

Once the release gates below are met, the Jobs table offers **Review quotes for
staging** on completed scoped private reviews with immutable receipts. The link
is absent while `SV_EVENT_HUMAN_APPROVAL_ENABLED=0` (the default).

The screen displays the exact incident scope and anchor, original source context,
candidate quotations and model relevance suggestions, including not-assessed
passages. Nothing is preselected. The operator selects 1-12 quotations and explicitly
confirms incident relevance and suitability for attributed publication. A model
`include` decision is never used as approval. An operator may approve a held or
unassessed passage after reading its context; that decision is attributed to the
human, not retroactively described as a model-quality success.

The admin validates the request, records an immutable decision, and enqueues
`event_promote_reviewed`. It does not run inference, render site content or build
Hugo. The task appears in the Fetch dashboard group and Jobs type filter. It runs
through the existing fetch worker, with no extra runner or inference concurrency.

Successful promotion reports `publication_status=awaiting_export` and
`public_eligible=false`: the revision is staged in the trusted pointer store, not
proof of a live page. The currently deployed legacy exporter does not consume it.
The coordinated exporter/manifest/activation integration remains a release gate.

This manual pilot is a temporary qualification path. The intended automated
policy-based qualification and model-quality work remain on the roadmap; this
does not impose permanent manual review on the final architecture.

## Authority and data integrity

- Authentication is mandatory even when legacy admin routes would allow missing
  token configuration. Approval POSTs require a custom header and matching Origin
  for cookie-based browser requests. Token-authenticated API clients can omit
  Origin but must still send `X-SV-Event-Approval: 1`.
- The server loads only the completed job's immutable receipt, HTML and packet,
  using bounded regular-file reads without following symlinks. No client path,
  invented quote, reviewer identity, model decision or qualification object is
  accepted. Generation provenance and a complete scoped packet are required.
- Selected IDs resolve to exact server-side source spans. The qualification binds
  full source versions, incident scope, exact quotations and a versioned human
  policy. Shared-token identity is recorded honestly as `authenticated-admin`,
  not a fabricated personal identity. Multi-user identity/auditing is future work.
- Current source locks protect admission; qualification, queued job and immutable
  approval record commit together. The enqueue helper's new `commit=False` is
  used only by this owned transaction; all existing callers retain default commit
  behavior. An insertion failure rolls back all three records.
- A global admission lock limits this manual pilot to ten pending tasks. Identical
  decisions return their original job, including after completion; they do not
  silently re-enqueue failed, canceled or already completed work. Use the existing
  Jobs controls for an explicit retry. Rechecks still apply on every retry.
- Promotion uses a different role, rechecks current evidence and qualification
  revocation, and performs the existing pointer compare-and-swap. A generic queued
  job containing an invented approval ID cannot mint approval. Queue payloads
  contain only an approval ID, never private source bodies.
- Exact quotations remain attributed, with unknown incident dates and source
  independence. No legacy narrative is merged into the qualified output.

## Configuration and schema

```dotenv
SV_EVENT_HUMAN_APPROVAL_ENABLED=0
SV_EVENT_APPROVAL_DB_URL=
SV_EVENT_PROMOTION_DB_URL=
```

The admin and the affected fetch worker both need the explicit feature flag when
the pilot is enabled. Provision credentials only in their intended workloads;
neither path falls back to `SV_DB_URL`.

The schema in `event_approval.SCHEMA` is additive and explicit, not startup DDL.
It depends on `event_publication_store.SCHEMA`. Approval records have a one-way
immutable audit trigger and reference their qualification and task. Retain those
task records with the approval audit; a future job-retention policy must account
for this foreign key rather than deleting the audit or bypassing its trigger.

Admission role: SELECT on evidence, publication and approval/job tables; UPDATE
on events/articles/memberships for existing source locks; INSERT on qualifications,
approvals and jobs. It must have **no** publication-pointer or revision write
privileges. Promotion role: SELECT on approvals and qualifications, plus the
existing promotion grants on source locks, revisions and pointers. It must have
**no** approval or qualification write privileges. The application explicitly
checks these privilege boundaries. Provision and test roles before enabling.

These are boundaries for the new code paths, not a claim that an entire legacy
worker pod is isolated from its existing broad application credentials. Broader
credential isolation requires a separate deployment/security change.

## Verification

```sh
.cache/mcp-venv/bin/python -m pytest -q tests/offline
node --test tests/js/*.test.cjs
```

728 offline tests and 26 JavaScript tests pass. The explicitly disposable PostgreSQL
suite passes 11 tests, including an end-to-end human approval, atomic rollback,
queued task, restricted-role promotion, stale-source refusal, idempotent retry,
matched page/JSON export, audit immutability and revoked-qualification refusal.
API/template and DOM interaction tests pass; this is not a live browser or
production pilot verification.

No production `init_db`, direct Hugo invocation, LLM request, migration or rollout
was used. The temporary PostgreSQL container/tunnel are removed after testing.

## Remaining release gates

1. Provision additive schema and separate roles through a reviewed migration and
   environment-specific platform configuration. Keep flags off during rollout.
2. Wire all affected exporters coherently to the qualified path, including holds
   and withdrawals, without legacy fallback. Bind exact output to the complete
   activation manifest and verify current evidence at the correct release boundary.
3. Enable the already approved guarded release switch only with that coordinator.
   Preserve the existing incremental feed/archive and atomic publication process.
4. Run an explicitly reviewed production pilot through the normal build API.
   Verify URLs, page/JSON parity, desktop/mobile controls and unchanged public
   feed downloads before widening admission.

For a stale/invalid approval response, refresh and review the new evidence rather
than forcing old selections. Disabled/unconfigured responses require provisioning,
not using a general database credential. A successful task still says awaiting
export until the separate publication coordinator is implemented.
