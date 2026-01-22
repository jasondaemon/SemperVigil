# SETTINGS_UI_MODEL.md — Settings UI & Admin Data Model

> The settings UI is the operational cockpit for SemperVigil. It manages DB-driven configuration so deployments can be re-themed (cybersecurity, fandom, etc.) without code changes.

## 1) Design Principles
- **DB-driven configuration**: sources and tactics live in DB, not YAML files.
- **Deterministic-first**: toggles enable/disable tactics and inference behaviors without needing LLMs.
- **Explainability**: show evidence and confidence for inferences and relationships.
- **Safety**: failure pause/backoff controls to avoid hammering sources.

---

## 2) Screens & What They Control

## 2.1 Dashboard (Ops Overview)
**Purpose**: system health at a glance.

**Widgets**
- Sources OK / degraded / paused counts
- Articles ingested in last 24h / 7d
- Zero-article streak warnings
- Top sources by volume
- CVE severity upgrades last 24h
- Scheduler queue (next runs)

**Data**
- `source_runs` aggregates
- `sources.pause_until`, `paused_reason`
- `cve_changes` (severity_upgrade)

---

## 2.2 Sources List
**Purpose**: manage sources quickly.

Columns:
- enabled
- source name
- frequency
- last run (ok/error)
- error streak
- pause status
- avg articles/day (7d)

Actions:
- enable/disable source
- run now
- pause/resume
- open “Source Detail”

Backed by:
- `sources`
- `source_runs`

---

## 2.3 Source Detail (Per-source configuration)
**Purpose**: configure how we find articles + how we parse content.

### A) General
- name, base_url
- enabled toggle
- frequency (minutes)
- ToS / robots notes
- “pause on failure” overrides (optional)

Tables:
- `sources`

### B) Tactics
A list of tactics ordered by priority:
- rss/atom
- jsonfeed
- html_index
- sitemap
- article_html (content extraction)

For each tactic:
- enabled toggle
- priority
- config JSON editor (with schema hints)
- last success, last error, error streak
- “Test tactic” button (runs tactic only and shows preview)

Tables:
- `source_tactics`

### C) Filters & Commercial Controls
- include/exclude keyword lists
- URL path filters
- sponsored detection toggles
- publish commercials? (default off)

Storage:
- can start as `source_tactics.config_json` under a `filters` block
- later can normalize into `source_filters` table if needed

### D) Health & Alerts
- thresholds:
  - error streak to pause
  - zero-article days threshold
- notification settings for this source:
  - email recipients override (optional)

Tables:
- `sources` (pause fields)
- `settings` (global defaults)
- optional future: `source_alert_overrides`

---

## 2.4 Inference Review Queue
**Purpose**: handle low-confidence suggestions.

Sections:
- “New Suggestions”
- “Reviewed”
- “Accepted”
- “Rejected”

Each row:
- article title + link
- suggested event key
- suggestion type
- confidence
- evidence viewer (terms/entities/timing)
- actions:
  - accept (creates event_mentions/event_relations)
  - reject (marks rejected)
  - adjust target event

Tables:
- `inference_suggestions`
- on accept:
  - `events` (ensure exists)
  - `event_mentions` / `event_relations`

---

## 2.5 Events Browser
**Purpose**: browse and manage events.

Filters:
- type (CVE / INCIDENT / META)
- status
- updated recently
- confidence threshold (for inferred relations)

Event detail view:
- current summary
- summary timeline
- linked articles
- related events (relations graph)
- merge/split tooling (later)

Tables:
- `events`
- `event_summary_versions`
- `event_mentions`
- `event_relations`

---

## 2.6 CVE Settings & Monitoring
**Purpose**: control NVD polling and see what changed.

Controls:
- NVD enabled
- poll minutes
- lookback hours
- daily report thresholds:
  - include highs/criticals
  - include upgrades
- API key status indicator (key itself never displayed)

Views:
- newest CVEs (high/critical)
- severity upgrades last 24h
- per-CVE change log + diffs

Tables:
- `settings` (nvd.*)
- `cves`, `cve_changes`

---

## 2.7 Profiles (Build Persona)
**Purpose**: configure SemperVigil as “Cybersecurity”, “Stargate SG1 fan news”, etc.

Profile controls:
- active profile key
- taxonomy (tag sets)
- default enabled sources set (or allow per profile)
- summary policy defaults (event-based vs article-based)
- UI branding fields (title, subtitle)

Storage:
- `settings.profile.active_profile`
- optionally: `profiles` and `profile_sources` tables later if needed

---

## 2.8 Retention & Storage
Controls:
- extracted text retention days (default 90)
- raw HTML retention days (default 7)
- cleanup job schedule (daily)

Storage:
- `settings.retention.*`

---

## 2.9 Alerts (Email)
Controls:
- enabled
- recipients
- alert types:
  - source paused
  - zero-article streak
  - repeated errors
  - CVE severity upgrades

Storage:
- `settings.alerts.*`

Implementation note:
- initial implementation can log “would email” events
- later wire SMTP/send service container

---

## 3) Settings Keys (Initial Set)
Recommended `settings.key` values (value stored as JSON):

### Profile
- `profile.active_profile` : `"cybersecurity"`
- `profile.branding` : `{ "site_title": "SemperVigil", "tagline": "...", "theme": "blowfish" }`

### Scheduler
- `scheduler.default_frequency_minutes` : `60`
- `scheduler.max_concurrency` : `3`

### Inference
- `inference.auto_link_threshold` : `0.85`
- `inference.mark_threshold` : `0.60`
- `inference.publish_inferred_links` : `true`
- `inference.require_review_for_inferred` : `false`

### NVD
- `nvd.enabled` : `true`
- `nvd.poll_minutes` : `60`
- `nvd.lookback_hours` : `48`
- `nvd.prefer_cvss_v4` : `true`

### Retention
- `retention.extracted_text_days` : `90`
- `retention.raw_html_days` : `7`

### Alerts
- `alerts.email.enabled` : `false`
- `alerts.email.recipients` : `[]`
- `alerts.pause_on_failure.enabled` : `true`
- `alerts.pause_on_failure.error_streak` : `5`
- `alerts.pause_on_failure.pause_minutes` : `1440`

---

## 4) API/Backend Surface (Future)
Even if the UI is added later, design endpoints around these resources:

- `GET/PUT /api/settings`
- `GET/POST /api/sources`
- `GET/POST /api/sources/{id}/tactics`
- `POST /api/sources/{id}/test` (preview results)
- `GET/POST /api/inference/suggestions`
- `GET /api/events`
- `GET /api/events/{event_key}`
- `GET /api/cves`
- `GET /api/cves/{cve_id}`
- `GET /api/reports/health`
- `GET /api/reports/severity-upgrades`

---

## 5) UX Notes
- Always show **confidence + inference level** (“confirmed”, “inferred”, “suggested”)
- Always show **evidence** for inferred links
- Provide one-click “pause source” and “run now”
- Default safe behavior: pause sources on repeated failure to avoid hammering