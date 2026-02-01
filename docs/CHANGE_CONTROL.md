# Change Control Log

## Purpose
This log records all changes that affect build/publish/serve stability, docker-compose mounts, permissions, and Hugo build mechanics.

## Scope (Must Be Logged)
Changes MUST be logged if they touch any of:

- docker-compose.yml volumes/mounts for data/site-src/site-public
- web container user/permissions or nginx document root/ports
- tools/hugo-build.sh (locks, retries, config override, cache/resource dir behavior)
- publish pipeline semantics that affect Hugo reads (atomic write behavior)
- directory creation/permission “fixers” or background chmod/chown behavior
- build scheduler cadence/backoff and any job claiming rules that impact build frequency

## Prohibited Changes Without Explicit Approval

- Reintroducing hardcoded `/nfs` paths in docker-compose.yml
- Using unsupported Hugo flags (e.g., `--resourceDir`)
- Adding rsync/staging/complex rebuild steps unless tied to a reproducible failure + logs

## Required Entry Format (Template)

```
- Date: YYYY-MM-DD HH:MM (local)
- Author: <name or Codex>
- Summary: <1–2 lines>
- Motivation / Problem:
- Files changed:
  - <path>
- Risk / Impact: low | med | high
- Verification steps run:
  - <command>
- Outcome: success | failure
- Rollback plan:
```

## Contributor Rules (Codex)
If you (Codex) propose edits affecting pipeline stability, you must:
1) paste the relevant logs/errors that justify the change, and
2) update CHANGE_CONTROL.md in the same PR/commit.

---

## Entries

- Date: 2026-01-31 00:00 (local)
- Author: Codex
- Summary: Stabilized Hugo build/serve pipeline and publishing writes.
- Motivation / Problem: Hugo builds failed due to unsupported flags and non-atomic writes during publish. NFS/Synology mounts required portable defaults and predictable permissions.
- Files changed:
  - tools/hugo-build.sh
  - src/sempervigil/utils.py
  - src/sempervigil/publish.py
  - src/sempervigil/pipelines/daily_brief.py
  - docker-compose.yml
  - docs/BUILD_PIPELINE.md
- Risk / Impact: med
- Verification steps run:
  - docker compose up -d --build
  - docker compose exec builder_scheduler sh -lc 'ls -la /site/index.html'
  - docker compose exec web sh -lc 'ls -la /usr/share/nginx/html/index.html'
  - curl -I http://localhost:${SV_WEB_PORT:-8080}/
- Outcome: success
- Rollback plan: Revert listed files to prior versions and rerun verification steps.
