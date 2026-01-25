# SemperVigil — Base Context

> **SemperVigil** (Latin: *“always watchful”*) is a configurable, containerized
> news aggregation, correlation, and intelligence system.

This document defines **what SemperVigil is, what problems it solves, and how the
system should be reasoned about**.  
Detailed architecture, schemas, and UI models are defined in companion documents
and are referenced rather than duplicated here.

---

## What This Is

SemperVigil addresses the problem of **information overload across many sources**
by producing a **single, coherent intelligence stream** that is:

- reliable (source health visibility)
- explainable (clear evidence and provenance)
- configurable (topic-agnostic)
- extensible (new sources, new tactics, new domains)

The system is **not just a scraper**.  
It is an **analysis pipeline** that progresses from:
> articles → signals → events → evolving narratives

---

## Problems It Solves

- Too many sources, too much noise
- Feeds silently breaking without visibility
- Duplicate reporting across days and sites
- Difficulty tracking *evolving* incidents, CVEs, or campaigns
- Manual effort required to understand “what actually matters today”

---

## High-Level Goals

1. **Configuration-first**
   - Sources, schedules, tactics, and behaviors are data-driven (DB-backed)
   - Minimal hard-coded assumptions

2. **Deterministic by default**
   - Prefer rules, heuristics, and structure
   - LLMs are optional, assistive, and cost-controlled

3. **Explainable intelligence**
   - Every inference includes confidence and evidence
   - Explicit vs inferred relationships are clearly marked

4. **Topic-agnostic**
   - Cybersecurity today
   - Anything tomorrow (AI news, politics, fandoms, etc.)

5. **Safe and polite ingestion**
   - Prefer RSS/Atom/JSON feeds
   - Respect ToS intent
   - Pause broken sources automatically to avoid hammering

---

## Current State

- SemperVigil is an **active refactor and expansion**
- Legacy scripts exist **outside this repo** at `../legacyscripts/`
  - They may be referenced for logic
  - They will **not** be committed
- SQLite is the current system of record
- Docker Compose is the orchestration mechanism

---

## Planned & Active Components

### 1) Ingest / Processing Service

Responsibilities:
- Fetch content from configured sources
- Apply per-source tactics (RSS, HTML index, article HTML, etc.)
- Normalize articles into a canonical schema
- Deduplicate without losing analytical signal
- Tag, classify, and correlate content
- Detect source health issues
- Emit structured outputs for publishing and analysis

Key principles:
- Per-source isolation
- Per-tactic enable/disable
- Explicit run records for diagnostics

See:
- `docs/DATA_MODEL.md`
- `docs/SETTINGS_UI_MODEL.md`

---

### 2) CVE & Vulnerability Intelligence

Responsibilities:
- Poll authoritative vulnerability sources (e.g., NVD)
- Track CVEs as **first-class evolving entities**
- Prefer CVSS v4 when available, fallback to v3.1
- Detect and record:
  - severity upgrades
  - score changes
  - metric changes
- Correlate CVEs with articles and events (explicit or inferred)

Key insight:
> A CVE is not a static record — it is a timeline.

See:
- `docs/DATA_MODEL.md` (CVE, snapshots, changes)

---

### 3) Event & Narrative Layer

Responsibilities:
- Group related articles into **events**
- Maintain a timeline of event summaries
- Support:
  - CVE events
  - incidents/breaches
  - meta-events (campaigns, exploit waves)
- Allow primary vs secondary relationships
- Track confidence and inference level

Events evolve over time and may be:
- updated
- merged
- linked into meta-events

See:
- `docs/DATA_MODEL.md` (events, event_mentions, event_relations)

---

### 4) Site Publisher

Responsibilities:
- Generate publishable outputs:
  - daily article listings
  - event-centric summaries
  - CVE updates
- Hugo-based static site
- Theme-customizable (currently Blowfish-derived)
- No runtime backend dependency required for readers

Publishing philosophy:
- Daily breadth (what was reported)
- Event depth (what actually matters)

---

### 5) Internal Admin / Settings UI (Planned)

Purpose:
- Operational visibility
- Configuration without redeploys
- Safe troubleshooting

Core features:
- Source health dashboard
- Per-source tactic testing
- Inference review queue
- Event & CVE browsers
- Scheduler & alert configuration

This UI is **internal-only**, not public-facing.

See:
- `docs/SETTINGS_UI_MODEL.md`

---

## Configuration Model

Configuration is **DB-driven**, not file-driven.

### Managed via DB:
- Sources
- Scraping tactics
- Schedules
- Inference thresholds
- Retention policies
- Alerts

### Managed via environment variables:
- Secrets (OpenAI API key, SMTP credentials, etc.)

### Files provided for reference only:
- `.env.example`

These exist to bootstrap deployments, not as long-term state.

---

## Diagnostics & Observability

SemperVigil must always be able to answer:
- Did a source run?
- Did it succeed?
- What changed since yesterday?
- Why are we not seeing articles?

Diagnostics include:
- Per-run metrics
- Per-source health
- Zero-article streak detection
- Error streak detection
- Automatic pause & alerting

Health data is stored, trended, and visible in the admin UI.

---

## Canonical Data Model

The **canonical schema is defined in**:
- `docs/DATA_MODEL.md`

Key design principles:
- Articles are immutable facts
- Events are evolving interpretations
- CVEs are timelines, not snapshots
- Inference is explicit and auditable

SQLite is sufficient for:
- multi-year storage
- tens to hundreds of thousands of articles
- structured summaries and metrics

Migration to Postgres is a future option, not a requirement.

---

## Security, Privacy & Ethics

- No secrets committed to the repo
- API keys via environment variables only
- Avoid scraping where feeds or APIs exist
- Respect the spirit of robots.txt and ToS
- Pause on repeated failures to avoid DoS-like behavior

---

## Licensing

- **GPL-3.0**
- Prefer GPL-compatible dependencies
- This project is not intended for proprietary SaaS use

---

## Non-Goals (For Now)

- Real-time alerting
- High-frequency trading-style feeds
- Multi-tenant SaaS
- Heavy ML pipelines

These may come later, but **clarity and correctness come first**.

---

## Companion Documents

- `docs/DATA_MODEL.md` — database schema & relationships
- `docs/SETTINGS_UI_MODEL.md` — admin UI and settings model
- `docs/ARCHITECTURE.md` — system-level design (planned / evolving)

---

**SemperVigil is about sustained understanding, not just ingestion.**
