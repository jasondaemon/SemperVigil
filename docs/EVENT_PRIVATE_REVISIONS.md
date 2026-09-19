# Private revision receipts

Status: deployed to admin/LLM as `78a0739`; end-to-end receipt pilot pending.
Existing opt-in private
review guards remain the configuration boundary. No new queue, model profile,
database migration, publication pointer or build behavior.

Every model-assisted `event_review_private` job now creates an immutable receipt
after its existing packet, assessment and HTML are saved. It binds the event ID,
exact packet version, complete validated assessment, guarded generation identity,
HTML filename and full SHA256. The job result exposes only a bounded descriptor.
Repeated identical runs reuse the same receipt and preserve its modification time.
Files stay private with mode 0600 alongside existing private artifacts.

The receipt always says `proposal_only` and `public_eligible: false`. Outstanding
gates are explicit: incident qualification, evidence qualification, current-input
transaction and publication authorization. An unversioned test/custom completion
also records missing generation provenance. A hash is not an approval or proof of
source truth. This is an auditable handoff for a future revision/publication path,
not that path itself. It has no predecessor chain, public pointer or database CAS.

The Jobs table offers **Download revision evidence** only for valid descriptors.
The authenticated endpoint is `/admin/api/jobs/{job_id}/private-revision`. It fails
closed when admin authentication is unconfigured, the job is incomplete, a legacy
job has no receipt, or the artifact is missing/altered. File reads reject symlinks,
special files, oversized data and client-selected paths. The receipt is bound to
the job's event, packet and separately verified HTML. Responses are attachments
with private/no-store, sandbox CSP and nosniff headers. No public JSON changes.

Local checks:

```sh
python3 -m pytest -q tests/offline/test_private_revision.py
node --test tests/js/private_review_result.test.cjs
```

Latest full gate: 598 offline tests and 21 JavaScript tests passed, plus JavaScript
syntax validation. Integration PostgreSQL tests were not rerun; this adds no SQL.
The paired diagnostics completed before rollout. The receipt verification job is
`job_aefc9fa9885b40e584eaa4e917c9dd20`; cache availability and unchanged generation
were verified before admission. Do not duplicate or reprioritize it.

Release: after current diagnostic completion, render/diff and drain before a
targeted admin/LLM-only rollout. Verify one existing assessment-cache hit produces
a receipt without inference, download authorization/integrity, and public checks.
Retain prior images. Rollback leaves harmless private receipt files on disk; old
workers/UI ignore the additive result field. No backfill is required.

Troubleshooting: older completed jobs legitimately return 404 for this endpoint.
Do not synthesize generation identity or rewrite their records to make a download
appear. A new bounded private job can reuse its compatible assessment cache after
deployment. Immutable conflict or corrupted bytes should fail, not be overwritten.

## Database snapshot handoff (local only, disabled)

`event_revision_store` adds an opt-in worker integration, controlled by
`SV_EVENT_PRIVATE_REVISION_STORE=0` (default in code/chart). Disabled mode does
not open a write connection. Only model-assisted jobs with a validated receipt
can enter the enabled path; the admin does not execute this work.

The proposed `event_private_revisions` table stores canonical full packet and
receipt JSON together, bounded at 3,000,000 and 65,536 bytes, keyed by event and
revision. It references the existing event. A dedicated short transaction performs
insert-if-absent then verifies existing bytes; matching duplicate/concurrent writes
reuse the original row/time, and mismatches fail without overwrite. Connection,
lock and statement timeouts are bounded. No report, event field, publish flag or
public pointer is written. Missing generation provenance is refused.

This is snapshot history, **not** a claim that sources are still current. Current
source locking, independent qualification, predecessor chains and public pointer
promotion remain separate pending steps. Public rendering must not read these
proposal rows as approved content.

Schema DDL is defined by `event_revision_store.SCHEMA` for the isolated test and
future controlled migration. It is **not registered in startup migrations and is
never executed by the worker**. Do not enable the flag in production before a
reviewed additive migration and targeted rollout. Missing tables should fail the
opt-in operation, not trigger automatic schema initialization or fallback writes.

Local verification includes real PostgreSQL concurrent admission and duplicate
checks on an explicitly named disposable database, plus the prior seven PG gates:

```sh
SV_TEST_DB_URL=postgresql://localhost/sempervigil_test \
  python3 -m pytest --run-db-tests -q tests/test_investigation_postgres.py
python3 -m pytest -q tests/offline
```

The disposable container and its loopback tunnel were removed after testing. No
production schema or data was changed. Rollback while disabled needs no data work;
future enablement must retain immutable snapshots and turn admission off first.
