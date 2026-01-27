# DATA_MODEL.md — SemperVigil Database Schema

> Postgres is the system of record. Schema is designed to migrate cleanly to Postgres later.
> Prefer additive migrations (new tables/columns), avoid destructive changes.

## Conventions
- Timestamps stored as ISO-8601 text in UTC (`YYYY-MM-DDTHH:MM:SSZ`)
- JSON stored as TEXT containing JSON (validated at app boundary)
- Most tables should have `created_at`, `updated_at` where useful
- Keep write transactions short; rely on Postgres row-level locks

---

## 1) Core Tables

### 1.1 sources
Represents a configured content source (site/feed). Sources live in DB, not config files.

**Columns**
- `id` TEXT PRIMARY KEY                  -- stable source id (slug)
- `name` TEXT NOT NULL
- `enabled` INTEGER NOT NULL DEFAULT 1
- `base_url` TEXT NULL
- `topic_key` TEXT NULL                  -- optional, for per-topic routing
- `default_frequency_minutes` INTEGER NOT NULL DEFAULT 60
- `pause_until` TEXT NULL                -- ISO timestamp; when set, scheduler skips
- `paused_reason` TEXT NULL
- `robots_notes` TEXT NULL               -- manual notes about ToS/robots guidance
- `created_at` TEXT NOT NULL
- `updated_at` TEXT NOT NULL

**Indexes**
- `(enabled, pause_until)`
- `(topic_key)`

---

### 1.2 source_tactics
Per-source scraping tactics and their configuration. Ordered by preference; can be toggled individually.

**Columns**
- `id` BIGSERIAL PRIMARY KEY
- `source_id` TEXT NOT NULL REFERENCES sources(id)
- `tactic_type` TEXT NOT NULL            -- rss|atom|jsonfeed|html_index|sitemap|article_html
- `enabled` INTEGER NOT NULL DEFAULT 1
- `priority` INTEGER NOT NULL DEFAULT 100
- `config_json` TEXT NULL                -- selectors, feed urls, etc.
- `last_success_at` TEXT NULL
- `last_error_at` TEXT NULL
- `error_streak` INTEGER NOT NULL DEFAULT 0
- `created_at` TEXT NOT NULL
- `updated_at` TEXT NOT NULL

**Constraints**
- UNIQUE(`source_id`, `tactic_type`, `priority`)

**Indexes**
- `(source_id, enabled, priority)`
- `(source_id, tactic_type)`

---

### 1.3 source_runs
One run of a source at a point in time; used for health & metrics.

**Columns**
- `id` BIGSERIAL PRIMARY KEY
- `source_id` TEXT NOT NULL REFERENCES sources(id)
- `started_at` TEXT NOT NULL
- `finished_at` TEXT NULL
- `status` TEXT NOT NULL                 -- ok|error|paused|skipped
- `http_status` INTEGER NULL
- `items_found` INTEGER NOT NULL DEFAULT 0
- `items_accepted` INTEGER NOT NULL DEFAULT 0
- `skipped_duplicates` INTEGER NOT NULL DEFAULT 0
- `skipped_filters` INTEGER NOT NULL DEFAULT 0
- `skipped_missing_url` INTEGER NOT NULL DEFAULT 0
- `error` TEXT NULL
- `notes_json` TEXT NULL                 -- per-run debug summary
- `created_at` TEXT NOT NULL

**Indexes**
- `(source_id, started_at DESC)`
- `(status, started_at DESC)`

---

## 2) Articles

### 2.1 articles
Metadata-first record of a single accepted or discovered article.

**Columns**
- `id` BIGSERIAL PRIMARY KEY
- `source_id` TEXT NOT NULL REFERENCES sources(id)
- `stable_id` TEXT NOT NULL              -- hash of normalized_url (or stable canonical)
- `original_url` TEXT NOT NULL
- `normalized_url` TEXT NOT NULL
- `title` TEXT NOT NULL
- `published_at` TEXT NULL               -- from source when possible
- `published_at_source` TEXT NULL        -- published|modified|guessed (optional)
- `ingested_at` TEXT NOT NULL
- `is_commercial` INTEGER NOT NULL DEFAULT 0
- `content_fingerprint` TEXT NULL        -- for grouping, not dedupe deletion
- `extracted_text_path` TEXT NULL        -- /data/blobs/... (short retention)
- `extracted_text_hash` TEXT NULL
- `raw_html_path` TEXT NULL              -- debug only, short retention
- `raw_html_hash` TEXT NULL
- `meta_json` TEXT NULL                  -- optional misc metadata
- `created_at` TEXT NOT NULL
- `updated_at` TEXT NOT NULL

**Constraints**
- UNIQUE(`source_id`, `stable_id`)
- UNIQUE(`normalized_url`) (optional; if cross-source duplicates are desired, omit)

**Indexes**
- `(published_at DESC)`
- `(source_id, published_at DESC)`
- `(is_commercial, published_at DESC)`

---

### 2.2 article_tags
Normalized tags applied to articles (taxonomy controlled by profile).

**Columns**
- `article_id` INTEGER NOT NULL REFERENCES articles(id)
- `tag` TEXT NOT NULL
- `tag_type` TEXT NULL                   -- topic|vendor|product|vuln_type|custom
- PRIMARY KEY(`article_id`, `tag`)

**Indexes**
- `(tag, article_id)`

---

## 3) CVEs

### 3.1 cves
First-class CVE objects, updated from NVD.

**Columns**
- `cve_id` TEXT PRIMARY KEY              -- CVE-YYYY-NNNN
- `published_at` TEXT NULL
- `last_modified_at` TEXT NULL
- `preferred_cvss_version` TEXT NULL     -- "4.0"|"3.1"
- `preferred_base_score` REAL NULL
- `preferred_base_severity` TEXT NULL    -- CRITICAL|HIGH|MEDIUM|LOW|NONE
- `preferred_vector` TEXT NULL
- `cvss_v40_json` TEXT NULL              -- full metric breakdown
- `cvss_v31_json` TEXT NULL
- `cwe_ids_json` TEXT NULL               -- list[str]
- `vuln_tags_json` TEXT NULL             -- list[str] normalized taxonomy
- `affected_products_json` TEXT NULL     -- list[{vendor,product,versions...}]
- `affected_cpes_json` TEXT NULL         -- list[str]
- `reference_domains_json` TEXT NULL     -- list[str]
- `description_text` TEXT NULL           -- short
- `updated_at` TEXT NOT NULL

**Indexes**
- `(preferred_base_severity, preferred_base_score DESC)`
- `(last_modified_at DESC)`
- `(published_at DESC)`

---

### 3.2 cve_snapshots
Append-only snapshots to detect changes. Dedupe via `snapshot_hash`.

**Columns**
- `id` BIGSERIAL PRIMARY KEY
- `cve_id` TEXT NOT NULL REFERENCES cves(cve_id)
- `observed_at` TEXT NOT NULL
- `nvd_last_modified_at` TEXT NULL
- `preferred_cvss_version` TEXT NULL
- `preferred_base_score` REAL NULL
- `preferred_base_severity` TEXT NULL
- `preferred_vector` TEXT NULL
- `cvss_v40_json` TEXT NULL
- `cvss_v31_json` TEXT NULL
- `snapshot_hash` TEXT NOT NULL
- UNIQUE(`cve_id`, `snapshot_hash`)

**Indexes**
- `(cve_id, observed_at DESC)`

---

### 3.3 cve_changes
Derived change log for upgrades and diffs.

**Columns**
- `id` BIGSERIAL PRIMARY KEY
- `cve_id` TEXT NOT NULL REFERENCES cves(cve_id)
- `change_at` TEXT NOT NULL
- `cvss_version` TEXT NULL
- `change_type` TEXT NOT NULL            -- severity_upgrade|score_change|metrics_change|preferred_version_changed
- `from_score` REAL NULL
- `to_score` REAL NULL
- `from_severity` TEXT NULL
- `to_severity` TEXT NULL
- `vector_from` TEXT NULL
- `vector_to` TEXT NULL
- `metrics_changed_json` TEXT NULL       -- list[{metric,from,to}]
- `note` TEXT NULL

**Indexes**
- `(cve_id, change_at DESC)`
- `(change_type, change_at DESC)`

---

## 4) Events

### 4.1 events
Canonical narrative objects.

**Columns**
- `id` BIGSERIAL PRIMARY KEY
- `event_key` TEXT NOT NULL UNIQUE       -- CVE-... | INCIDENT-... | META-...
- `event_type` TEXT NOT NULL             -- CVE|INCIDENT|META
- `title` TEXT NOT NULL
- `status` TEXT NOT NULL DEFAULT 'active' -- active|resolved|archived
- `first_seen_at` TEXT NOT NULL
- `last_updated_at` TEXT NOT NULL
- `current_summary` TEXT NULL
- `update_reason` TEXT NULL
- `cve_id` TEXT NULL REFERENCES cves(cve_id) -- for CVE events
- `meta_json` TEXT NULL
- `created_at` TEXT NOT NULL
- `updated_at` TEXT NOT NULL

**Indexes**
- `(event_type, last_updated_at DESC)`
- `(status, last_updated_at DESC)`

---

### 4.2 event_summary_versions
Timeline entries for event summaries.

**Columns**
- `id` BIGSERIAL PRIMARY KEY
- `event_id` INTEGER NOT NULL REFERENCES events(id)
- `created_at` TEXT NOT NULL
- `created_by` TEXT NOT NULL             -- rule|nvd|manual|llm
- `reason` TEXT NOT NULL                 -- new_articles|severity_upgrade|manual_edit|merge
- `summary` TEXT NOT NULL
- `citations_json` TEXT NULL             -- list[{article_id,url,title,source_id}]
- `meta_json` TEXT NULL

**Indexes**
- `(event_id, created_at DESC)`

---

### 4.3 event_mentions
Links articles to events, with role + inference metadata.

**Columns**
- `id` BIGSERIAL PRIMARY KEY
- `event_id` INTEGER NOT NULL REFERENCES events(id)
- `article_id` INTEGER NOT NULL REFERENCES articles(id)
- `role` TEXT NOT NULL                   -- primary|supporting
- `matched_by` TEXT NOT NULL             -- explicit|rule|nvd|manual|llm
- `confidence` REAL NOT NULL
- `inference_level` TEXT NOT NULL        -- confirmed|inferred|suggested
- `evidence_json` TEXT NULL              -- matched terms/entities/time
- `created_at` TEXT NOT NULL

**Constraints**
- UNIQUE(`event_id`, `article_id`, `role`)

**Indexes**
- `(article_id)`
- `(event_id, role)`

---

### 4.4 event_relations
Directional event-to-event relations (meta-events, exploits, etc.).

**Columns**
- `id` BIGSERIAL PRIMARY KEY
- `parent_event_id` INTEGER NOT NULL REFERENCES events(id)
- `child_event_id` INTEGER NOT NULL REFERENCES events(id)
- `relation_type` TEXT NOT NULL          -- umbrella|related|exploits|caused_by
- `confidence` REAL NOT NULL
- `inference_level` TEXT NOT NULL        -- confirmed|inferred|suggested
- `evidence_json` TEXT NULL
- `created_by` TEXT NOT NULL             -- rule|nvd|manual|llm
- `created_at` TEXT NOT NULL

**Constraints**
- UNIQUE(`parent_event_id`, `child_event_id`, `relation_type`)

**Indexes**
- `(parent_event_id, relation_type)`
- `(child_event_id, relation_type)`

---

### 4.5 inference_suggestions
Records low-confidence candidates for later review in UI.

**Columns**
- `id` BIGSERIAL PRIMARY KEY
- `article_id` INTEGER NOT NULL REFERENCES articles(id)
- `suggested_event_key` TEXT NOT NULL
- `suggestion_type` TEXT NOT NULL        -- cve_link|meta_event|incident_link
- `confidence` REAL NOT NULL
- `evidence_json` TEXT NOT NULL
- `created_at` TEXT NOT NULL
- `status` TEXT NOT NULL DEFAULT 'new'   -- new|reviewed|accepted|rejected

**Indexes**
- `(status, created_at DESC)`
- `(article_id)`

---

## 5) Settings

### 5.1 settings
Key/value settings for profile + scheduler + inference thresholds.

**Columns**
- `key` TEXT PRIMARY KEY
- `value` TEXT NOT NULL                  -- JSON for structured values
- `updated_at` TEXT NOT NULL

**Recommended keys**
- `profile.active_profile` = `"cybersecurity"` (or other)
- `scheduler.default_frequency_minutes` = `60`
- `inference.auto_link_threshold` = `0.85`
- `inference.mark_threshold` = `0.60`
- `nvd.enabled` = `true`
- `nvd.poll_minutes` = `60`
- `nvd.lookback_hours` = `48`
- `retention.extracted_text_days` = `90`
- `retention.raw_html_days` = `7`
- `alerts.email.enabled` = `false`
- `alerts.email.recipients` = `["..."]`
- `alerts.pause_on_failure.enabled` = `true`
- `alerts.pause_on_failure.error_streak` = `5`
- `alerts.pause_on_failure.pause_minutes` = `1440`

---

## 6) Notes on Scale
- Multi-year storage is fine if you store **summaries, metadata, and metrics**, not raw HTML.
- Prefer storing extracted text as blobs with retention; keep only hashes/paths in Postgres.
- If article count grows large (300k+), add additional indexes on common query paths and consider Postgres migration later.
