# Private revision receipts

Status: deployed to admin/LLM as `78a0739`; end-to-end receipt pilot verified.
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
The paired diagnostics completed before rollout. Receipt verification job
`job_aefc9fa9885b40e584eaa4e917c9dd20` succeeded using its cache, with zero model calls.
Both authenticated downloads passed integrity/header checks. Jobs filtering,
coverage labels and download links were verified in the browser with no console
errors. Do not duplicate the job. See the dated verification notes for hashes.

Release: after current diagnostic completion, render/diff and drain before a
targeted admin/LLM-only rollout. Verify one existing assessment-cache hit produces
a receipt without inference, download authorization/integrity, and public checks.
Retain prior images. Rollback leaves harmless private receipt files on disk; old
workers/UI ignore the additive result field. No backfill is required.

The release procedure above is now complete for receipts. It does not authorize
deployment of the later local-only database or projection components below.

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

## Current-source transaction window (local, no promotion caller yet)

`locked_current_snapshot` supplies the short transaction boundary for a future
qualified pointer change. It rejects omitted/truncated packets and requires a
fresh non-autocommit connection. It checks that the immediate, validated event
foreign key exists, locks the event, existing memberships and source rows with
NOWAIT, then recollects and compares the full evidence dataset. Only the event's
bookkeeping timestamp is excluded; title, membership, text, URLs, feed dates and
coverage remain part of the comparison. The production FK prerequisite was
confirmed read-only; no production locks or writes were taken by this helper.

NOWAIT conflicts must abort/defer the prospective promotion, not preempt ingestion
or invoke automatic retries. Statement and idle-in-transaction timeouts are bounded.
No network work, inference, or build is permitted inside the yielded block. A
caller must still establish independent qualification and compare/update the
expected publication predecessor in that same transaction. No such public caller
or pointer is enabled yet. Merely entering this context is never approval.

Nine disposable PostgreSQL tests now pass, including concurrent source-body edits,
membership insert/delete attempts, stale-source rejection, harmless bookkeeping
updates, missing-FK rejection and unsafe autocommit rejection. All 609 offline
tests pass. The private persistence test also reran against the stricter receipt
validator. Disposable container and tunnel were removed again after verification.

## Quote-only projection preparation (local, no publishing caller)

`event_projection.prepare` accepts a source packet, incident scope and a separate
qualification record. Its default empty trusted-qualification set refuses all
records. A future caller must load permitted identities from an independently
authorized store, never from model output or a client-supplied trust list. Digest
membership is a boundary supplied by that caller, not authentication implemented
by this module. No qualification evaluator or trusted store is implemented yet.

The bounded qualification records exact source and scope versions, versioned
human/policy reviewer identity and exact quotation spans. Preparation rejects
stale/full-context changes, incomplete packets, altered quotes, duplicate or
overlapping spans, arbitrary prose and model-role reviewers. Output uses only
attributed quotations and source metadata, keeps unrepresented sources visible,
leaves origin independence and incident dates unknown, and labels feed_day only
as feed metadata. It never copies legacy narrative fields. An optional predecessor
changes content identity but is not a transactional compare-and-swap.

Returned projections remain not_promoted/public_eligible=false. Independent
qualification, durable storage, transactional pointer promotion, secure rendering
and publication authorization are still required. No runtime imports, production
configuration, model calls, migrations or public files changed in this slice.
Run `python3 -m pytest -q tests/offline/test_event_projection.py` for the 21 new
checks; all 630 offline checks pass. The previous nine PostgreSQL and 21 JavaScript
gates were not rerun for this pure-data addition.

## Qualified Markdown export branch (local, not enabled)

`write_events_markdown` now accepts optional `qualified_revisions` and
`promoted_revision_ids` maps. Existing callers supply neither and retain identical
legacy bytes. A future caller must obtain the latter from a trusted publication
pointer store, not legacy event metadata, a model response or a client-supplied
mapping. There is no such production caller or pointer store yet.

`event_render.render` reconstructs the exact projection from its full packet,
scope, independent qualification and predecessor, then compares the resulting
identity and event to the expected pointer. It does not itself approve a
qualification. It emits only source-backed quotations and explicit coverage/
unknown-date/unknown-independence notices. Markdown, HTML and shortcode punctuation
in source text is encoded; source URLs are separately encoded. Rendering never
fetches a URL or invokes Hugo. The qualified branch skips all old summary,
narrative, timeline, CVE and product fields, preserving the existing stable slug.

All input renders and pointer checks complete before file replacements or pruning.
Invalid, missing, duplicate or unmatched pointers fail without replacing prior
pages; identical qualified output retains bytes, inode and timestamp. This does
not make the source-directory writes a multi-file transaction: the existing
build/atomic publication process is still required.

Ten new checks pass, with 640 total offline tests and unchanged legacy checksum.
No deploy or build occurred. Before enablement: add trusted pointer storage and
qualification, a matching qualified Events JSON index path, full-output preflight,
real platform-driven Hugo/browser validation and a bounded publication pilot.
The daily feed JSON contract is not modified by this branch.
