# Build + Serve Pipeline (Stable Invariant)

## Source of Truth (Must Not Change)
The build + serve pipeline described in this document is **stable** and **must not change**.
Any modification to mounts, build orchestration, or Hugo invocation **must** be explicitly requested and verified with logs.

---

## Pipeline Flow

```
ingest → publish (atomic writes) → hugo build → web serve
```

- **Ingest** produces normalized articles/events.
- **Publish** writes Hugo inputs using **atomic writes** (no partial files).
- **Hugo build** reads from `/site-src`, writes to `/site`.
- **Web serve** serves `/site` via nginx.

---

## Mount Expectations (Invariant)

PVC-backed mounts are the default:

- `${SV_DATA_DIR:-./data}` → `/data`
- `${SV_SITE_SRC_DIR:-./site-src}` → `/site-src`
- `${SV_SITE_PUBLIC_DIR:-./site-public}` → `/site` (builder) and `/usr/share/nginx/html` (web)

The live cluster uses PVC claims provisioned by `nfs-csi`.
**Never** hardcode legacy NFS-era paths in `docker-compose.yml` or deployment manifests.

---

## Permissions Expectations (Invariant)

- `web` runs as user `${SV_UID:-1000}:${SV_GID:-1000}` to read `/usr/share/nginx/html`.
- Do **not** add chmod/chown loops or “fix perms” daemons.

---

## Hugo Invocation Expectations (Invariant)

- **Do not use** `--resourceDir` (unsupported by our Hugo build).
- `resourceDir` **must** be set via a **build-only config override** generated under `/data`.
- Hugo caches/modules/resources live under `/data` (not under `/site-src`).

---

## Synology / Staging Gotchas

Any config staging or copy/sync step **must** exclude:

- `@eaDir/`
- `.DS_Store`
- `._*` (AppleDouble)

This prevents build failures and permission errors on Synology-backed volumes.

---

## Serving Expectations (Invariant)

- nginx listens on **8080** inside the container.
- Published as `${SV_WEB_PORT:-8080}`.
- Document root is `/usr/share/nginx/html` (same mount as `${SV_SITE_PUBLIC_DIR}`).

---

## DO NOT CHANGE (Guardrails)

**Do not** make any of the following changes unless explicitly requested:

- Do not change docker-compose mounts for `data`, `site-src`, or `site-public` without updating docs and running verification.
- Do not reintroduce legacy NFS-era paths in `docker-compose.yml` or the k8s manifests.
- Do not change `tools/hugo-build.sh` behavior (locks/retries/atomic-write assumptions) without a concrete failing log + explicit approval.
- Do not add rsync staging or build dir relocations unless a reproducible issue requires it.

---

## Change Control
Any pipeline-affecting change **must** include a new entry in `docs/CHANGE_CONTROL.md`.

---

## Allowed Changes (Must Not Alter Pipeline)

These are safe **only if they do not modify the pipeline**:

- Hugo templates/layouts/static assets
- Event schema/content generation
- UI formatting
- New pages/sections
- Additional JSON data outputs

---

## How to Verify (Exact Commands)

### Incremental feed freshness (prepared September 2026)

The staged upgrade branch uses database content fingerprints rather than only
record counts and maximum update timestamps. Fingerprints include feed fields,
linked products/vendors/versions, threat actors and aliases, event keys, article
tags, source names/icon availability, and KEV membership/due dates. EPSS's public
checked-at field is included; unrelated job/check timestamps are excluded.

All dates are inspected, but only changed/missing days are serialized. Historical
JSON stays outside Hugo input. Normal refresh prioritizes recent dates and repairs
up to `SV_FEED_ARCHIVE_BACKGROUND_DAYS` historical dates (default 25). A five-second
background budget is checked between days, not inside an individual day. Remaining
work is reported as `deferred` and resumes on a subsequent refresh. Set the limit
to 0 to pause background catch-up; the explicit archive job remains available.
Existing manifests without content signatures are migrated through the same
bounded process. Do not seed signatures for unverified old files.

The database scan adds measured work even when nothing needs regeneration; initial
read-only observations were 2.68-4.17 seconds. This is not a deployed build-time
measurement. Monitor full job duration after release. Use the platform API to
request a build; never run Hugo directly for validation.

Multiple worker handlers also call the feed exporter. Release all affected writer
images coherently; a builder-only rollout would leave old writers able to replace
complete files with partial results. Runtime 889b2de was deployed September 18
with background catch-up disabled; see STABILIZATION_VERIFICATION.md for evidence.

Archive day/manifest writes hold a shared filesystem lock. Retain this lock across
inventory and replacement; public reads do not acquire it. Rollback artifacts
must live outside Hugo data inputs (including `/data` in production). The guarded
release snapshot is under `/log/release-snapshots/`, not a recurring backup.

Offline verification: `python3 -m pytest -m offline --strict-markers -q`.

### Existing container checks

```bash
docker compose up -d --build
docker compose exec build_worker sh -lc 'ls -la /site/index.html'
docker compose exec web sh -lc 'ls -la /usr/share/nginx/html/index.html'
curl -I http://localhost:${SV_WEB_PORT:-8080}/
```
