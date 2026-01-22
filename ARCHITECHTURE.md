# SemperVigil Architecture

> **Canonical architecture specification**
>
> This document is the authoritative reference for SemperVigil’s design.
> All implementation work (human or Codex) must conform to it.

---

## 1. Purpose

SemperVigil is a configurable, source-agnostic news and intelligence aggregation
platform designed to provide:

1. **Breadth** – Daily visibility into what many sources are reporting.
2. **Depth** – Aggregated understanding of events, themes, and vulnerabilities.
3. **Continuity** – Persistent memory of incidents, CVEs, and evolving narratives.
4. **Explainability** – Evidence, confidence, and attribution for all inferences.
5. **Operational safety** – Deterministic scraping, health tracking, and backoff.

---

## 2. Core Objects

### 2.1 Articles (Breadth Layer)

Articles represent **individual source publications**.

**Rules**
- Every accepted article is published as a daily post.
- Articles preserve original URLs and attribution.
- Articles are immutable once ingested.

**Core Fields**
- `article_id`
- `source_id`
- `title`
- `canonical_url`
- `published_at`
- `ingested_at`
- `tags`
- `content_fingerprint` (for grouping, not dedupe deletion)

**Relationships**
- One **primary event**
- Zero or more **secondary events**

Commercial / sponsored content is filtered deterministically.

---

### 2.2 Events (Depth Layer)

Events are **living narrative objects** that aggregate articles over time.

**Event Types**
- `CVE` – CVE-YYYY-NNNN
- `INCIDENT` – breaches, campaigns, outages
- `META` – umbrella groupings (MITRE-like)

**Event Properties**
- Stable `event_key`
- Timeline of updates
- Many-to-many relationship with articles
- Confidence-scored inferences

Events evolve; they are never rewritten in place.

---

## 3. Event Summaries

- Summaries are **event-based**, not article-based.
- Stored as **versioned records**.
- A new version is created when:
  - new articles are linked
  - CVE severity changes
  - analyst or LLM update occurs

The latest version is marked as `current_summary`.

---

## 4. CVEs as First-Class Objects

### 4.1 CVE Ingestion

- CVEs pulled from **NVD API**.
- Default polling: **hourly** (configurable).
- Queries use:
  - published date
  - last modified date

### 4.2 CVSS Handling

- Capture **CVSS v4.0** and **CVSS v3.1** when available.
- Prefer **v4.0** for headline severity.
- Persist **full metric breakdowns**.

### 4.3 Severity Tracking

- CVE records are snapshotted.
- Diffs are computed per update.
- Severity upgrades are highlighted.
- Metric-level diffs explain *why* a change occurred.

---

## 5. Vulnerability Surface Modeling

Each CVE stores:

- Vendor
- Product
- Affected versions (best-effort)
- CWE IDs
- Normalized vulnerability type taxonomy
- Reference URLs

This enables **article-to-CVE matching even without explicit CVE mentions**.

Confidence scores are applied to inferred matches.

---

## 6. Article → Event Correlation

### 6.1 Deterministic Matching (Primary)

- Explicit CVE IDs
- Vendor + product + vuln type
- Known campaign / incident identifiers

### 6.2 Heuristic Matching (Secondary)

- Keyword clusters
- Affected component overlap
- Temporal proximity

### 6.3 LLM-Assisted Matching (Fallback)

- Used only when deterministic methods fail
- Prompted with:
  - article excerpt
  - candidate events
- Output must include:
  - confidence score
  - justification
  - uncertainty flag

---

## 7. Source System

### 7.1 Sources

Sources are stored **only in the database**, not config files.

**Source Properties**
- `source_id`
- `name`
- `base_url`
- enabled / disabled
- scrape frequency (default: hourly)
- allowed scrape tactics
- ToS / robots notes

### 7.2 Per-Source Scraping Tactics

Each source may support multiple tactics:

- RSS / Atom
- JSON feeds
- HTML index parsing
- Sitemap parsing
- Article page scraping

Tactics are:
- individually enabled / disabled
- ordered by preference
- health-scored

---

## 8. Scraping Analysis & Recovery

When a source yields zero articles or parse failures:

1. Record failure statistics
2. Pause scraping automatically
3. Flag source as unhealthy
4. Allow **analysis mode**:
   - inspect raw HTML
   - identify candidate selectors
   - optionally generate an LLM prompt (manual execution)

No automatic LLM calls for scraping.

---

## 9. Configuration & Profiles

SemperVigil supports **build profiles**:

Examples:
- Cybersecurity
- Finance
- Fan News
- General News

Profiles control:
- enabled sources
- CVE ingestion
- tagging vocabularies
- summary behavior

Configured via **settings UI**, not code.

---

## 10. Health, Metrics, and Alerts

### 10.1 Metrics Stored in DB

- articles/day per source
- acceptance ratio
- parse failures
- zero-article days
- latency

### 10.2 Alerts

- Email alerts on:
  - repeated failures
  - zero-article streaks
  - CVE severity upgrades
- Failed sources auto-paused to prevent hammering.

---

## 11. Storage Architecture

### 11.1 SQLite (Current)

SQLite is acceptable because:
- low write contention
- mostly append-only
- WAL mode enabled

Expected scale:
- 100k–300k articles
- multi-year CVE history
- summaries only (no raw HTML)

### 11.2 Future Migration Path

Schema designed to migrate cleanly to:
- PostgreSQL

No SQLite-specific logic in business code.

---

## 12. Publishing Model

### Daily Output
- Chronological article feed
- Links only (no duplicate summaries)

### Aggregated Output
- Event pages
- CVE pages
- Timeline-based summaries

---

## 13. Design Principles

- Deterministic first
- Evidence over inference
- Confidence always visible
- Never DOS a source
- Architecture over refactor

---

## 14. This Document

- This file is **source of truth**
- Codex prompts must reference it
- Changes require intentional updates