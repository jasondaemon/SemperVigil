# SemperVigil — Base Context

## What this is
SemperVigil (Latin: “always watchful”) is a configurable, containerized news aggregation and analysis pipeline.

Problem it solves:
- too many sources, too much noise
- need a single consolidated intelligence stream
- need source health visibility and fast troubleshooting when feeds stop producing new items
- future extensibility beyond cyber news (AI, politics, TV shows, etc.)

## High-Level Goals
- Configuration-first: sources and behavior are user-defined.
- Robust ingestion: per-source isolation, retries, and diagnostics.
- Extensible enrichment: optional LLM summarization and tagging via pluggable providers.
- Publishable outputs: generate content for a static site (Hugo) and/or API output.

## Current State
- A set of “legacy scripts” exists OUTSIDE this repo at `../legacyscripts/`.
- These scripts can be referenced for logic but will not be committed.

## Planned Components
1) **Ingest / Processing Service**
   - fetch sources (RSS/Atom/HTML if needed)
   - normalize articles into a canonical schema
   - deduplicate, categorize, and optionally enrich (LLM)
   - emit artifacts for the publisher (e.g., markdown/json)

2) **Site Publisher**
   - Hugo-based static site build
   - static hosting container (or any static host)

3) **Internal-only Admin UI** (optional but likely)
   - source report dashboard (health, last fetch, errors)
   - test a source (fetch+parse preview)
   - view recent ingest runs
   - edit config later (phase 2)

## Configuration Requirements
Config must support:
- feed/source list
- per-source overrides:
  - user agent / headers
  - parser selection or selector hints
  - timeouts and rate limits
  - allow/deny keywords
- output configuration (directories, formats)
- LLM settings:
  - provider(s) (OpenAI, others later)
  - API keys via env vars only
  - model, temperature, max tokens
  - enable/disable summarization

Provide:
- `config.example.yml`
- `.env.example`
- validation with clear messages

## Diagnostics Requirements
The system must expose:
- per-source “health”:
  - last attempt, last success, errors
  - HTTP status, items found, items accepted
- run-level summaries:
  - total sources, total items, deduped, output written
- a “test source” command:
  - run just one source
  - show raw fetch results + parse results + filters applied
  - do not require full pipeline

## Canonical Data Model (initial)
At minimum, a normalized “Article” record should contain:
- id (stable hash)
- title
- url (canonical)
- source_id
- published_at (best-effort)
- fetched_at
- author (optional)
- summary (optional)
- tags/categories (list)
- raw_source (optional: minimal raw fields for debugging)

Persist storage can start simple (SQLite or JSON lines), but must support:
- deduplication
- traceability
- easy export for site generation

## Security & Privacy
- No secrets committed
- Respect robots.txt where applicable (future enhancement)
- Rate limiting and polite user-agent default
- Avoid scraping that violates terms when unnecessary; prefer RSS/Atom first

## Licensing
GPL-3.0. Prefer GPL-compatible dependencies.

## Non-Goals (for now)
- Real-time alerting
- Heavy ML pipelines
- Multi-tenant SaaS
These can be added later, but the core must be stable first.