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

### 1) Copy example config

```bash
cp config.example.yml config/config.yml
```

Adjust as needed for your environment.

---

### 2) Import example sources into the state DB

```bash
docker compose run --rm worker \
  sempervigil sources import /config/sources.example.yml
```

Sources are stored in the database, not in static config files.

---

### 3) Start internal services (admin, worker, builder)

```bash
docker compose up --build admin worker builder web
```

Outputs are written to:
- Articles (Markdown): `site/content/posts/`
- JSON index (if enabled): `site/static/sempervigil/index.json`
- Site output: `site/public/`

---

### 4) Enqueue ingest jobs

```bash
docker compose run --rm worker \
  sempervigil jobs enqueue ingest_due_sources
```

The worker will claim and run queued jobs.

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

## Internal Services

- admin: FastAPI API for managing sources and enqueueing jobs (binds to `127.0.0.1:8001` by default)
- worker: polls the DB job queue and runs ingestion tasks
- builder: polls the DB job queue and runs `hugo build` for site output
- web: public static site server (nginx serving `site/public`)

All orchestration is DB-driven; containers do not shell out to Docker.


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

- SQLite is the current system of record
- WAL mode is enabled
- Data model is append-heavy and migration-friendly

Expected scale:
- hundreds of thousands of articles
- multi-year CVE and event timelines
- summaries and metadata only (no raw HTML retention)

The schema is designed to migrate cleanly to PostgreSQL if needed,
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
