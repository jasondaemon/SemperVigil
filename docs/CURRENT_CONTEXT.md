# SemperVigil — Current Context (2026-02-19)

This doc is the **single source of truth** for the current running state, operational behavior, and known pitfalls.
Use it first when starting a new chat or debugging issues.

---

## Runtime Topology

- **Postgres**: `sempervigil-db` (internal).
- **Admin API/UI**: `sempervigil-admin` on port `SV_ADMIN_PORT` (default 8001).
- **Workers**:
  - `worker_fetch` (ingest, content fetch, CVE sync/KEV, source health, events rebuild, web enrich/promote)
  - `worker_llm` (summarize/context, article/CVE enrichment, event derivation, event report)
  - `worker_openai` (dedicated `build_daily_brief`)
- **Builder**:
  - `builder` (one-shot, profile `build`)
  - `build_worker` (always-on, claims admitted `build_site` jobs)
- **Web**: nginx serving `/site-public` on `SV_WEB_PORT` (default 8080).

Key volumes (NFS):
- `/nfs/sempervigil/site-src` -> Hugo source
- `/nfs/sempervigil/site-public` -> Hugo output
- `/nfs/sempervigil/data` -> runtime data (articles, CVEs, reports)
- `/nfs/sempervigil/log` -> logs (admin/worker/build)

The same share is mounted as:
- `/Volumes/docker/sempervigil` on macOS (SMB)
- `/nfs/sempervigil` on the server (NFS)

---

## Build System Behavior

- **Only build when a job exists**: use `build_site` job via admin UI or `sempervigil jobs enqueue build_site`.
- **Single-build at a time**: admin enqueue refuses if a build is already queued/running.
- **Scheduler poll interval**: `SV_BUILDER_POLL_SECONDS` (default 60).
- **Debounce**: `SV_BUILD_DEBOUNCE_SECONDS` (default 60).

If builds are too frequent or CPU-pegged, verify both values in `.env`.

---

## Publishing Model (DB-backed)

- **Per-article markdown is disabled by default** (`SV_ENABLE_ARTICLE_MARKDOWN=false`).
- Front page uses data files in `site-src/data/`:
  - `data/articles/today.json`
  - `data/articles/recent.json`
  - `data/cves/today.json`
  - `data/cves/recent.json`
- Daily briefs are published at `/daily-briefs/YYYY-MM-DD/` and loaded on the homepage via feed index + day JSON.

**Homepage rendering**: `site-src/layouts/partials/home/custom.html` (Blowfish theme override).
**Current homepage**: `site-src/layouts/partials/home/test-front.html` (single-column news feed + yesterday brief tabs).

---

## Article + CVE Data Export

- Articles include tags, products, and optional `nist_family`.
- Article export now includes `summary_bullets` for list/search rendering.
- CVEs include `product_title`, severity, and a list of products.
- Entity search is unified at `/entities/` (vendors/products/threats merged into one search surface).
- Product/vendor/threat detail page generation is disabled; links resolve to `/entities/?search=<term>`.

---

## Admin UI Expectations

- Dashboard metrics read from `get_dashboard_metrics()`; ensure `get_setting(conn, key, default)` is always called with a default.
- Jobs page supports filters + pagination.
- Sources page is dense and supports edit/expand; IDs shown in edit form.
- Article suppression exists in per-article menu and strikes in list.

---

## Known Pitfalls

1) **Builder OOM / killed hugo**
   - Hugo can be killed by the OS when memory/CPU spikes.
   - Verify with `build_worker` logs and system `dmesg`.

2) **Permissions flipping on site-src**
   - `fsinit._ensure_dir()` now only chmods on creation.
   - `SV_FIX_SITE_PERMS=0` in compose prevents aggressive chmod/chown.

3) **Stale builds / no changes**
   - If `build_worker` runs but output doesn’t change, confirm a build job exists and that Hugo succeeded.

4) **RSS probe/fetch timeouts (Sophos‑style feeds)**
   - RSS probe and ingest support curl HTTP/2 fetcher with Range prefixing.
   - Per‑source overrides can force `http_fetcher=curl` or `python_then_curl`.

4) **DB host resolution**
   - Builder must run on `svnet` to reach `db`.

---

## Quick Start (Debug)

1) Enqueue a build and watch scheduler:
   ```
   docker compose run --rm worker_fetch sempervigil jobs enqueue build_site
   docker compose logs --tail=50 build_worker
   ```

2) Check output files:
   ```
   ls -la /nfs/sempervigil/site-public/index.html
   head -n 5 /nfs/sempervigil/site-public/index.html
   ```

3) Verify data files (source):
   ```
   ls -la /nfs/sempervigil/site-src/data/articles/today.json
   ls -la /nfs/sempervigil/site-src/data/cves/today.json
   ```

---

## Worker Roles (Allowed Types)

- worker_fetch: ingest, fetch, cve_sync, derive_events, build_daily_brief, web enrich
- worker_llm: summarize_article_llm, cve_enrich_llm, enrich_event_summary_llm

Keep `SV_WORKER_ONLY_TYPES` aligned with actual job types.

---

## Related Docs

- Architecture: `ARCHITECTURE.md`
- Data Model: `DATA_MODEL.md`
- Pipelines: `PIPELINES.md`
- Admin UI: `ADMIN_UI.md`
- CVE Model: `CVE_MODEL.md`
- Event Model: `EVENT_MODEL.md`
