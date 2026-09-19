# Legacy event report stale-write guard

Status: tested locally, not deployed. No schema, model, prompt, build command,
public format, selection or concurrency change.

## Finding

`_handle_event_report_llm` reads the event before inference. Previously,
`update_event_report` then read `meta_json` and unconditionally overwrote it after
adding the new report. An event edit during inference could make the generated
report stale. An edit between the metadata read and write could be lost entirely.

## Narrow correction

The worker captures the starting event `updated_at` and passes it to the writer.
Missing versions skip before inference. A changed version skips the write and
does not mark the site dirty. The final database update compares both the complete
original `meta_json` and original `updated_at`, using PostgreSQL null-safe equality.
Only one affected row counts as success. A competing edit after the read survives
instead of being replaced by old metadata. Existing callers without an explicit
starting version still receive the read/write race protection.

Normal report structure, attribution metadata and successful build-dirty behavior
are unchanged. The worker returns `stale_or_unavailable_event` when it cannot
install its result. It does not blindly retry stale content or add inference.
An unrelated event touch may conservatively defer a report; later admission needs
coalesced current-evidence scheduling, not replay of the obsolete candidate.

## Verification

Eleven offline cases cover normal metadata retention, changed starting versions,
concurrent metadata/timestamp edits, deletion, null metadata, missing versions,
worker propagation and build-dirty suppression. The full offline suite has 529
passing tests.

Seven targeted tests pass against a separate disposable PostgreSQL 18 database,
including a second connection changing metadata between read and conditional
write. The first attempt used the retrieval fixture's private schema, while legacy
storage explicitly checks `public.events`; it correctly returned unavailable.
The final test creates only a small synthetic `public.events` in the named-disposable
database and drops it afterwards, exercising the actual storage guard and SQL.
No production database, application initialization or migration was used.

## Boundaries

This is not immutable revision storage, a source-version publication transaction,
or semantic approval. Article/evidence changes that do not touch the event are
not detected by the starting event timestamp. Same-version metadata races are
detected by the final full-metadata comparison. The legacy parser and its content
validation limitations remain; do not use this guard to qualify new public reports.

The new Events path still needs immutable candidates, independently qualified
scope/evidence, and a separately activated publication pointer that preserves its
last accepted value on any failed or stale assessment. Keep it gated until those
checks and the real-model evaluation pass.
