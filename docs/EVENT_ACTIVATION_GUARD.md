# Events release activation guard

Status: locally implemented and tested, **not deployed or enabled**. The user
explicitly approved this narrowly scoped activation-path change on September 19,
2026. It does not authorize arbitrary build changes or autonomous publication.

**Current integration:** see [EVENT_RELEASE_COORDINATION.md](EVENT_RELEASE_COORDINATION.md).
The builder now prepares an output-bound version-two manifest named
`event-publication.json`. The live activation entry point requires version two,
checks current source evidence and rendered outputs, and refuses the authority-only
version-one format described below. The older format remains in transaction unit
tests only. Deployment and API-driven pilot verification are still pending.

## Behavior

`SV_EVENT_ACTIVATION_CHECK` defaults to `0`. In that state `hugo-build.sh` runs its
original `ln -sfn` release switch, without importing Python or contacting the
database. Hugo arguments, build attempts, caches, mounts, feed history, source
content and concurrency are unchanged. Invalid flag values refuse activation.

With the flag exactly `1`, `sempervigil.event_activation` reads the candidate
release's `.event-publication.json`. Missing, malformed, oversized, duplicate-key,
non-regular and symlink manifests fail closed. The manifest is bounded to 16 KiB
and 20 managed events, including deliberate withdrawals:

```json
{
  "workflow": "event-release-authorization-v1",
  "revisions": {"event-id": "64-character-current-revision-digest"},
  "withdrawn": {}
}
```

The example digest is a placeholder; actual values must be 64 lowercase hex
characters. Withdrawal values also identify the expected current pointer, not a
reason string. Active and withdrawn IDs cannot overlap. The combined inventory
must equal **all** stored public pointers, not merely IDs selected by a caller.

Immediately before switching, a dedicated READ COMMITTED transaction:

1. Rejects a database principal able to write qualification records.
2. Takes a NOWAIT SHARE lock on the pointer table, preventing changes or additions
   to the managed inventory while the switch runs.
3. Requires the qualification revocation trigger and locks managed event rows
   in stable order, coordinating with promotion and one-way revocation.
4. Rereads authority after locking. Superseded/missing pointers, revoked active
   qualifications, broken references and non-reproducible revisions refuse the
   switch. Explicit removals do not reauthorize a revoked report.
5. Holds these locks through the existing local `ln -sfn` operation. Locks are
   **not** held during Hugo, inference or content rendering. Contention fails
   immediately rather than waiting behind other workers.

The DB connection timeout is three seconds; statements have a three-second
timeout, idle transactions five seconds, and the switch subprocess two seconds.
This is not a distributed transaction between PostgreSQL and the filesystem:
an exceptional connection/process failure after a successful switch can report
failure even though the link moved. Inspect the current link after such a failure;
never blindly retry an unguarded switch. Revocation *after* activation still needs
a subsequent withdrawal publication. This hook does not remove already-live pages.

## Credentials and release gates

`SV_EVENT_ACTIVATION_DB_URL` must be provisioned separately. There is no fallback
to the broad application credential, schema creation, or production migration.
The activation principal needs SELECT on publication tables and events, UPDATE
on events for row locking, and UPDATE on pointers for the table lock. It must
have no qualification write privileges. Credentials are never logged.

**Do not enable this flag yet.** The publication coordinator must first bind the
manifest to its exact qualified page/JSON export, including removing withdrawn
pages and index entries, and independently validate current evidence. The guard
checks publication authority, not rendered-file integrity, source freshness, or
semantic quality. A hand-written manifest is not an approved publication path.
No production caller emits this manifest yet. All affected exporters must be
released coherently so legacy writers cannot overwrite qualified content.

## Validation and troubleshooting

Offline tests exercise the real extracted shell switch branch, not Hugo:

```sh
.cache/mcp-venv/bin/python -m pytest -q tests/offline/test_event_activation.py
sh -n tools/hugo-build.sh
```

The explicitly disposable PostgreSQL suite tests successful authorization,
lock-protected activation, concurrent revocation/pointer update refusal,
superseded revision rejection and withdrawal after revocation. It does not use
or initialize the production database. See `tests/test_investigation_postgres.py`.

After the remaining coordinator gates are implemented, request representative
builds through the platform API only. Never invoke Hugo directly. Missing
manifest/credentials, authorization drift or lock contention must fail the build
before the switch; do not disable the guard merely to force that candidate live.
Failed candidates remain available for diagnosis and normal later retention
cleanup. The existing live release remains untouched on authorization refusal.

Deployment rollback must retain the previously verified image/configuration.
Once qualified publication is enabled, bypassing the guard is not a safe rollback;
stop admissions and restore a verified release through the approved process.
