# Database remediation

## September 29, 2026: first guarded release

This release does not change news acquisition, inference, concurrency, daily JSON
fields, Event qualification, Hugo commands, or release activation.

- Published legacy composition upgrades retain a durable hold in settings, keyed
  by public revision, composition/audit job material and policy. Unchanged holds
  are skipped. New evidence/revisions, audit results or policy versions invalidate
  the hold. Genuine exceptions still roll back; read work is explicitly finished.
  Removing `event.public_composition_upgrade.hold.<event_id>` permits an explicit
  retry without modifying the accepted/public content. Bump `HOLD_POLICY` when
  changing upgrade validation behavior not represented by the input fingerprint.
- Feed inventory computes the same base MD5 before materialization rather than
  spilling full article/CVE JSON into temporary storage. Dependency fragments,
  final signatures, dates, public fields and archive selection remain identical.
- Migration 061 adds `(source_id, started_at DESC)` for latest source-run lookups.
  On established large databases, create the same index concurrently before
  rollout; startup then only records the migration. The index is additive and
  compatible with old images.
- Database connections identify their component using `SV_DB_APPLICATION_NAME`,
  otherwise the container hostname. No global autocommit or memory-policy change.

Validation must compare old/new signatures in one read-only database snapshot,
observe ordinary new-article builds, and compare rolling rollback/temp-write
deltas. Cumulative counters alone cannot prove an improvement.

### Production validation

Deployed application image `683a993` to only the orchestrator and build worker;
platform commit `800ff3c` records their tags. Scoped server-side manifest diff
contained only image changes, and rendered/live configuration matched afterward.

- Read-only, repeatable-read comparison: all 5,098 date signatures matched.
  Inventory timings were 3.539 seconds before and 3.086 seconds after.
- Production statement statistics: approximately 235 MB versus 42 MB temporary
  writes per inventory call, an 82% reduction. This is not a claim that the
  all-history scan has been eliminated.
- Latest source-run lookup uses the new valid index: 0.099 ms measured execution.
- Eight unchanged composition holds are now skipped instead of repeatedly
  attempting promotion and rolling back. Evidence and publication gates remain.
- Supported API build `job_644e1807c41040e38942cd22563388d7` succeeded in about
  52 seconds and activated release `20260929102537`.
- The ordinary scheduler then admitted
  `job_8e3be47c50e54ce5900e1fb5896623ec`; it succeeded in about 72 seconds.
  Article 36304, acquired at 10:28 UTC after deployment, reached the daily JSON.
- All 19 public checks passed, including historical JSON and independent metrics
  freshness. The checker now recognizes the existing CyberNews metrics label.
- Offline tests: 1,157 passed, two skipped, five existing deprecation warnings.
  Kubernetes readiness, including etcd, passed; all nodes reported no memory
  pressure. No application memory settings were changed.
- A 7-minute-29-second post-hold sample measured 67 rollbacks and 3,078 commits
  (2.13% of transactions), below the initial approximately 4.55% sustained rate.
  This is shorter than the 15-minute alert window and does not establish that
  every rollback cause is resolved or that the alert has cleared.

Unrelated platform working-tree changes were preserved, not included in this
deployment or commit. Old image tags remain available for rollback.

## Next stage: durable dirty-day tracking

The first release still computes an all-history inventory. Do not describe it as
fully incremental database work. A follow-up must transactionally invalidate all
export dependencies, including deletes/date moves and shared metadata changes;
generation-aware acknowledgements must retain updates arriving during export.
Use the current fingerprint implementation as a low-frequency reconciliation and
parity oracle. Do not remove historical JSON, EPSS refreshes, or safety gates to
reduce the workload. This stage needs separate concurrency and mutation coverage.

Further work: audit read-only connection lifetimes, optimize enrichment eligibility
joins using measured plans, and investigate monitoring collector connection reuse.
Keep `shared_buffers` and global `work_mem` unchanged; scoped memory tuning requires
a measured query plan and concurrent-memory budget.

## Source-only packaging without Docker

`tools/source-overlay.py` appends a source-only OCI layer to a verified,
single-platform containerd export. It preserves existing dependency layers and
runtime configuration, records source revision provenance, and replaces only
`/app/src`. It refuses missing provenance, unsupported digests and manifest lists.
Test/import the resulting image before scoped Deployment rollout. Do not use it
for dependency changes. No code is copied into running production containers.

Rollback: restore affected image tags and rerun the supported build admission
path. Hold settings and the additive index are safe to retain with old images.
