# SemperVigil

**SemperVigil** (Latin: *“always watchful”*) is a configurable, containerized
news aggregation and intelligence pipeline.

It is designed to ingest content from many sources, normalize and deduplicate it,
correlate related reporting into evolving events, and publish both **daily
coverage** and **long-lived intelligence narratives**.

SemperVigil is **topic-agnostic**:
- cybersecurity today
- AI, geopolitics, or fandom news tomorrow

---

## What Problem Does This Solve?

- Too many sources, too much noise
- Feeds break silently and stop producing data
- The same incident is reported repeatedly across days and sites
- CVEs and incidents evolve, but coverage is fragmented
- Manual effort is required to understand *what actually matters*

SemperVigil addresses this by separating:
- **Articles** (what was reported)
- **Events** (what is actually happening)
- **Vulnerabilities** (what is changing over time)

---

## Core Concepts

- **Articles**  
  Immutable records of individual source publications.  
  Every accepted article is published for transparency and breadth.

- **Events**  
  Living narrative objects that group related articles across time.
  Events evolve, accumulate context, and maintain a timeline.

- **CVEs**  
  First-class entities pulled from authoritative sources (e.g., NVD),
  tracked as timelines with severity changes and metric diffs.

- **Inference with Evidence**  
  Relationships are explicit, inferred, or suggested — always with
  confidence scores and justification.

---

## Architecture Overview

SemperVigil is composed of three primary layers:

1. **Ingest / Processing**
   - Fetches sources (RSS, HTML, APIs)
   - Applies per-source tactics
   - Normalizes and deduplicates content
   - Correlates articles to events and CVEs
   - Tracks source health

2. **Intelligence Layer**
   - Event aggregation
   - CVE tracking and severity upgrades
   - Inference scoring and evidence capture

3. **Publishing**
   - Hugo-based static site
   - Daily article listings
   - Event-centric and CVE-centric pages

For full details, see:
- `docs/ARCHITECTURE.md`

---

## Documentation Map

If you are reading this repo for the first time:

- **Start here**
  - `README.md` (this file)
  - `docs/BASE_CONTEXT.md`

- **System design**
  - `docs/ARCHITECTURE.md`

- **Database & schema**
  - `docs/DATA_MODEL.md`

- **Admin / settings UI**
  - `docs/SETTINGS_UI_MODEL.md`

These documents are the **source of truth** for implementation decisions.

---

## Quickstart

> This gets you from zero to a working local pipeline.

### 1) Copy environment file

```bash
cp .env.example .env
```

Adjust as needed for your environment.

Runtime configuration is stored in the Postgres DB (see System → Runtime Config in the Admin UI).
`config/sources.example.yml` is documentation only. Sources live in the DB.

PostgreSQL is required. Set `SV_DB_URL` in `.env` and start the `db` service.

---

### 2) Initialize the state DB

```bash
docker compose run --rm worker sempervigil db migrate
```

If using Postgres:

```bash
docker compose up -d db
```

Sources are stored in the database, not in static config files.

---

### 3) Start internal services (admin, worker, web)

```bash
docker compose up -d --build db admin worker_fetch worker_llm web
```

Admin health check:

```bash
curl -s http://<host>:8001/health
```

Admin UI:

```bash
open http://<host>:8001/ui
```

Event derivation + purge:
- Use the Events page buttons for **Derive Events** and **Purge Weak Events**.
- Use **Normalize CVE Keys** to backfill missing `event_key` values on CVE clusters.
- Manual events are never purged.
- Purge defaults: keep events with at least 2 linked articles or strong signal kinds.
- For API use:
  - `POST /admin/api/events/derive`
  - `POST /admin/api/events/purge` (supports `dry_run=true`)
Legacy events (from old event_items-only records) are hidden by default. Use `include_legacy=1`
on `/admin/api/events` to debug them; legacy is deprecated and may be dropped.

Verification SQL (Postgres):
```sql
SELECT COUNT(*) FROM events WHERE event_key LIKE 'cve:%' OR kind = 'cve_cluster';
SELECT e.id, e.event_key, COUNT(ea.article_id) AS article_count
FROM events e
LEFT JOIN event_articles ea ON ea.event_id = e.id
WHERE e.event_key LIKE 'cve:%' OR e.kind = 'cve_cluster'
GROUP BY e.id, e.event_key
ORDER BY article_count ASC;
```

Add your first source in the Sources tab (DB-backed).
Analytics and daily brief tools are available at `/ui/analytics`.

Optional token gate:
- Set `SV_ADMIN_TOKEN` in `.env`
- UI access requires login at `/ui/login` (cookie-based)
- Mutating admin endpoints require either:
  - `X-Admin-Token: <token>` header (curl/scripts), or
  - an authenticated browser cookie from `/ui/login`
- `/ui/static` assets remain public; UI pages still require auth when token is set

Reverse proxy notes (Nginx Proxy Manager):
- Forward `X-Forwarded-Proto`, `X-Forwarded-Host`, and `X-Forwarded-For`
- Recommended headers: `X-Frame-Options: SAMEORIGIN`, `X-Content-Type-Options: nosniff`
- Cookies are set `Secure` only when the original scheme is HTTPS

AI Configuration:
- Use `http://<host>:8001/ui/ai` to configure providers, models, prompts, schemas, profiles, and routing
- API keys entered in the UI are encrypted at rest using AES-GCM
- Use the “Test” buttons in the UI to verify providers and profiles
- Generate a master key:
  - `python -c "import os,base64; print(base64.urlsafe_b64encode(os.urandom(32)).decode())"`
- Set environment:
  - `SEMPERIVGIL_MASTER_KEY=<generated>`
  - `SEMPERIVGIL_KEY_ID=v1` (optional)
- If the master key is lost, saved provider keys cannot be decrypted and must be re-entered

LLM summarization (LiteLLM OpenAI-compatible):
- Set `SV_LLM_BASE_URL`, `SV_LLM_API_KEY`, `SV_LLM_MODEL`
- Summaries are generated via the `summarize_article_llm` job
- Full content fetching is controlled by:
  - `SV_FETCH_FULL_CONTENT=1`
  - `SV_STORE_ARTICLE_HTML=0` (default off)

Permissions / first-boot:
- If `${SV_SITE_SRC_DIR}` or `${SV_SITE_PUBLIC_DIR}` are not writable, run:
  - `docker compose run --rm worker_fetch sh /tools/ensure-dirs.sh`

Smoke tests:
```bash
curl -v http://<host>:8001/ui
curl -v http://<host>:8001/ui/
```

Outputs are written to:
- Hugo source (NFS): `${SV_SITE_SRC_DIR}` (content, layouts, themes, static)
- Articles (Markdown): `${SV_SITE_SRC_DIR}/content/posts/`
- JSON index (if enabled): `${SV_SITE_SRC_DIR}/static/sempervigil/index.json`
- Site output: `${SV_SITE_PUBLIC_DIR}` (nginx serves this)

---

### 4) Enqueue ingest jobs

```bash
docker compose run --rm worker_fetch \
  sempervigil jobs enqueue ingest_due_sources
```

The worker service claims and runs queued jobs continuously. To process a single
job for debugging, you can run the worker once:

```bash
docker compose run --rm worker_fetch \
  sempervigil-worker --once
```

---

### 5) Build the site output

The ingest run enqueues a `build_site` job automatically when new articles are accepted.

```bash
docker compose --profile build run --rm --no-deps builder --once
```

If no build job is queued, you can enqueue one manually:

```bash
docker compose run --rm worker_fetch \
  sempervigil jobs enqueue build_site
```

---

## PostgreSQL required

SQLite is no longer supported. The stack requires a Postgres database and `SV_DB_URL`.

---

### Run Tests (Docker)

```bash
docker compose run --rm test
```

---

### Run Tests (Local)

```bash
pip install -e ".[test]"
pytest -q
```

---

### Test a Single Source

To diagnose parsing, filtering, or health issues:

```bash
docker compose run --rm worker \
  sempervigil test-source cisa-alerts
```

This command:
- fetches only the specified source
- shows accept / reject decisions per item
- prints reasons for filtering or skipping
- does not require a full pipeline run

This is the primary troubleshooting tool.

---

### CVE Sync (Deterministic)

```bash
docker compose run --rm worker \
  sempervigil cve sync
```

Set `NVD_API_KEY` in your environment for higher rate limits.

---

### Build the Site (One-Shot)

```bash
docker compose --profile build run --rm builder
```

After build, verify: `${SV_SITE_PUBLIC_DIR}/index.html`

---

### Verify Web Output

```bash
curl -i http://127.0.0.1:8080/ | head
```

---

### NFS Notes

SemperVigil expects a durable NFS layout:

- `${SV_SITE_SRC_DIR}` (Hugo source)
- `${SV_SITE_PUBLIC_DIR}` (Hugo output)
- `${SV_DATA_DIR}` (logs, state)

SemperVigil will create needed directories without attempting `chown` unless running as root.

---

## Internal Services

- admin: FastAPI API for managing sources and enqueueing jobs (binds to `127.0.0.1:8001` by default)
- worker_fetch: polls ingestion + fetch + publish jobs and enqueues `ingest_due_sources`
- worker_llm: polls only LLM summarization jobs (rate-limited via leases)
- builder: one-shot Hugo build container; run on demand (profile `build`)
- web: public static site server (nginx serving `site/public`)

All orchestration is DB-driven; containers do not shell out to Docker.

Flow:
- admin -> jobs table -> worker -> jobs table -> builder -> web

Why DB-backed jobs?
- deterministic orchestration without container-to-container shelling
- durable state for retries, audits, and crash recovery
- clear separation between ingest, build, and publish concerns

---

## Job State Machine (Contributor Notes)

States:
- queued -> running -> succeeded|failed

Recovery:
- running jobs with stale locks are requeued on claim attempts

Auto-pause:
- error streaks or consecutive zero-article runs pause a source and record a health alert


## Configuration Philosophy

- Sources, schedules, and scraping tactics are **DB-driven**
- Secrets are **environment variables only** (never committed)
- File-based configuration is for **bootstrapping**, not long-term state
- Deterministic logic is preferred over LLM calls
- LLMs are optional, assistive, and cost-controlled

This separation allows SemperVigil to:
- pause or adjust sources without redeploying
- evolve scraping tactics safely
- support multiple topical “profiles” from the same codebase

---

## Source Health & Diagnostics

SemperVigil is designed to never fail silently.

For every ingest run, the system records:

- per-source attempt and success timestamps
- HTTP status and parse errors
- items found vs accepted
- duplicate and filter counts
- zero-article streaks

When a source shows repeated failures or produces zero items:

- it is automatically **paused**
- an alert may be generated (email or log)
- scraping is halted to avoid hammering the site

Health and diagnostics data are intended to be viewed via the
**internal admin / settings UI**, not published pages.

---

## Intelligence Over Time

SemperVigil distinguishes between **what was reported** and
**what is actually happening**.

### Daily Coverage (Breadth)

- Every accepted article is published
- Shows the breadth of reporting across sources
- Preserves attribution and original links
- Answers: *“What did people write about today?”*

### Event & CVE Aggregation (Depth)

- Related articles are grouped into events
- Events accumulate context across days or weeks
- CVEs are tracked as evolving timelines
- Severity upgrades and impact changes are highlighted

This allows SemperVigil to answer:
> “What is the current understanding of this issue?”

---

## Deterministic First, LLMs Second

SemperVigil prefers deterministic approaches wherever possible:

- explicit CVE references
- vendor / product matching
- keyword and taxonomy-based classification
- temporal correlation

LLMs may be used **only when deterministic methods are insufficient**, and must:

- return a confidence score
- provide justification / evidence
- clearly mark inferred relationships

LLMs are never used automatically for scraping.

---

## Storage & Scalability

- Postgres is the current system of record
- Postgres is required and provides safe concurrent writers
- Data model is append-heavy and migration-friendly

Expected scale:
- hundreds of thousands of articles
- multi-year CVE and event timelines
- summaries and metadata only (no raw HTML retention)

The schema is designed for PostgreSQL and is managed via migrations,
without rewriting business logic.

---

## Current Status

SemperVigil is under **active development**.

You should expect:
- architectural stability
- evolving schemas with migrations
- incremental feature expansion

You should *not* expect:
- a hosted SaaS
- real-time alerting at this stage
- turnkey production defaults

---

## License

This project is licensed under the  
**GNU General Public License v3.0 (GPL-3.0)**.

You are free to use, modify, and redistribute this software under the GPL.
Any distributed derivative works must also be licensed under GPL-3.0.

---

**SemperVigil is about sustained understanding — not just scraping.**
