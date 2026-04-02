# SemperVigil — Current Context (2026-02-19)

This doc is the **single source of truth** for the current running state, operational behavior, and known pitfalls.
Use it first when starting a new chat or debugging issues.

---

## Runtime Topology

- **Postgres**: `sempervigil-db` (internal).
- **Admin API/UI**: `sempervigil-admin` on port `SV_ADMIN_PORT` (default 8001).
- **Discovery / Orchestrator**:
  - `orchestrator` (singleton control plane: due-source checks, schedules, launch policy, build admission)
- **Stage runners**:
  - `worker_fetch` runner (claims control launch jobs, executes bounded fetch-stage worker passes)
  - `worker_llm` runner (claims control launch jobs, executes bounded local-LLM-stage worker passes)
  - `worker_openai` runner (claims control launch jobs, executes bounded OpenAI-stage worker passes)
- **Builder**:
  - `builder` (one-shot, profile `build`)
  - `build_worker` runner (claims control launch jobs and executes admitted `build_site`)
- **Web**: nginx serving `/site-public` on `SV_WEB_PORT` (default 8080).

Key volumes (PVC-backed):
- `sempervigil-site-src-csi` mounted at `/site-src` -> Hugo source
- `sempervigil-site-public-csi` mounted at `/site` -> Hugo output
- `sempervigil-data-csi` mounted at `/data` -> runtime data (articles, CVEs, reports)
- `sempervigil-logs-csi` mounted at `/log` -> logs (admin/worker/build)
- `sempervigil-tools-csi` mounted at `/tools` -> shared tooling

The live stack is PVC-backed via `nfs-csi`; there is no runtime dependency on the old NFS share.

---

## Build System Behavior

- **Builds are dirty-state driven**: admin and workers mark the site dirty; the orchestrator admits `build_site`.
- **Single-build at a time**: orchestrator admits at most one pending/running `build_site`.
- **Runner poll interval**: `SV_RUNNER_POLL_SECONDS` (default 5).
- **Orchestrator tick interval**: `SV_ORCH_TICK_SECONDS` (default 30).
- **Debounce**: `SV_BUILD_DEBOUNCE_SECONDS` (default 60), applied at orchestrator admission time.

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

2) **Control queue backlog**
   - If stage work is not moving, inspect control-queue launch jobs first.
   - A growing `launch_*_worker` backlog means the orchestrator is admitting work faster than runners can drain it.

3) **Permissions flipping on site-src**
   - `fsinit._ensure_dir()` now only chmods on creation.
   - `SV_FIX_SITE_PERMS=0` in compose prevents aggressive chmod/chown.

4) **Stale builds / no changes**
   - If `build_worker` runs but output doesn’t change, confirm a build job exists and that Hugo succeeded.
   - Also verify `build_site.state` is being cleared after successful build completion.

5) **RSS probe/fetch timeouts (Sophos‑style feeds)**
   - RSS probe and ingest support curl HTTP/2 fetcher with Range prefixing.
   - Per‑source overrides can force `http_fetcher=curl` or `python_then_curl`.

6) **DB host resolution**
   - Orchestrator, runners, and builder all require direct DB reachability.

---

## Quick Start (Debug)

1) Request a build and watch the orchestrator/build runner:
   ```
   docker compose run --rm worker_fetch sempervigil jobs enqueue build_site
   docker compose logs --tail=50 orchestrator
   docker compose logs --tail=50 build_worker
   ```

2) Check output files:
   ```
   ls -la /site-public/index.html
   head -n 5 /site-public/index.html
   ```

3) Verify data files (source):
   ```
   ls -la /site-src/data/articles/today.json
   ls -la /site-src/data/cves/today.json
   ```

---

## Queue / Runner Roles

- control queue:
  - `launch_fetch_worker`
  - `launch_llm_worker`
  - `launch_openai_worker`
  - `launch_build_worker`
- fetch queue:
  - ingest, fetch, KEV sync, event web enrichment/promotion, rebuild jobs
- llm_local queue:
  - summarization, article/CVE enrichment, event derivation/report
- openai queue:
  - `build_daily_brief`
- build queue:
  - `build_site`

Stage runners should be configured by queue, not by large `SV_WORKER_ONLY_TYPES` lists.

---

## Related Docs

- Architecture: `ARCHITECTURE.md`
- Data Model: `DATA_MODEL.md`
- Pipelines: `PIPELINES.md`
- Admin UI: `ADMIN_UI.md`
- CVE Model: `CVE_MODEL.md`
- Event Model: `EVENT_MODEL.md`
