# Event Markdown reuse and failure handling

## Status

September 19: locally tested, not deployed. This changes only the existing event
Markdown writer, not its publication selection, content, model calls, daily JSON,
event index JSON, Hugo commands, concurrency, or public activation process.

Call-path inspection found the writer is invoked by `_publish_events` during CVE
synchronization and explicit event rebuilds. It is not invoked by every Hugo build.
Previously it removed every event Markdown file except `_index.md` before rendering
the replacement set, then rewrote even byte-identical pages.

## Behavior

- Render the complete input before replacing any page.
- Preserve bytes, inode and modification time when a desired page is identical.
- Atomically replace changed individual files through the existing text writer.
- Remove obsolete Markdown only after all desired writes succeed; retain `_index.md`
  and non-Markdown files. An intentionally empty list still withdraws generated
  event pages, as before.
- Fail on missing event IDs, unsafe/reserved filenames, case-colliding slugs, or
  symlink/nonregular desired targets instead of deleting existing content first.
- Surface cleanup failures instead of silently accepting a stale page.

No new configuration or database schema is introduced. A non-ASCII filename remains
supported when it contains only word characters, dots and hyphens and fits the
filename bound. Live read-only inspection found all 11 currently selected published
events use safe event-ID slugs or the same ID fallback. This does not establish
that every future caller is valid.

## Verification and limits

The regression suite pins a rich synthetic page's SHA-256 from the prior writer,
checks unchanged inode/mtime preservation, one-page changes, withdrawal/index
retention, and render/write/cleanup failure paths. It also covers unsafe/duplicate
slugs and static symlink rejection.

```sh
python3 -m pytest tests/offline/test_event_export_reuse.py -q
```

This is not a transactional directory swap or a lock shared by all exporters and
Hugo. A later write failure can leave some newly written source files beside old
ones, although stale pages are not pruned on that failure. Concurrent writers and
publication-level snapshot/locking guarantees require separate work; the existing
external atomic public-release process is unchanged. No production build-time
improvement is claimed from these local tests.

Before deployment, identify every live caller image, compare rendered manifests,
and validate a platform-API build with page/content parity and public availability
checks. Do not assume replacing only the builder deploys this writer: orchestration
and fetch-stage calls also use it. Keep this change separate from private model
prompt rollouts. Rollback is the prior caller image; no data migration is needed.
