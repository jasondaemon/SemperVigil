# Private revision receipts

Status: implemented and tested locally; not deployed. Existing opt-in private
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
Production remains on the prior runtime while five paired diagnostics wait behind
ordinary work. Do not restart that worker merely to ship this local slice.

Release: after current diagnostic completion, render/diff and drain before a
targeted admin/LLM-only rollout. Verify one existing assessment-cache hit produces
a receipt without inference, download authorization/integrity, and public checks.
Retain prior images. Rollback leaves harmless private receipt files on disk; old
workers/UI ignore the additive result field. No backfill is required.

Troubleshooting: older completed jobs legitimately return 404 for this endpoint.
Do not synthesize generation identity or rewrite their records to make a download
appear. A new bounded private job can reuse its compatible assessment cache after
deployment. Immutable conflict or corrupted bytes should fail, not be overwritten.
