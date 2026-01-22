# NEXT_STEP_PLAN.md — Phase 1: CVE Linking + Audit Trace (No Events)

## Goal (Phase 1)
For each accepted article:
1) deterministically extract CVE IDs (and a few basic signals)
2) store article↔CVE links with confidence + reasons
3) store a full evidence_json audit trace per article
4) keep daily article publishing unchanged (posts + index.json)
5) add tests (no network calls)

Non-goals:
- no event creation/linking
- no summaries
- no UI
- no schema beyond what Phase 1 needs

---

## A) DB / Schema (Minimal)
Add only what Phase 1 needs.

### Tables required (if not already present)
1) `cves`
- cve_id TEXT PRIMARY KEY
- created_at TEXT NOT NULL
- last_seen_at TEXT NOT NULL

2) `article_cves` (link table)
- article_id TEXT NOT NULL
- cve_id TEXT NOT NULL
- confidence REAL NOT NULL
- confidence_band TEXT NOT NULL  -- "linked"|"linked_inferred"
- reasons_json TEXT NOT NULL     -- JSON array of stable rule ids
- evidence_json TEXT NOT NULL    -- JSON blob (audit trace)
- created_at TEXT NOT NULL
PRIMARY KEY (article_id, cve_id)

3) `articles` (ensure Phase 1 fields exist)
- id, title, url, source_id, published_at, fetched_at, content_text/summary_text
- plus: `evidence_json` optional at article-level OR store only in article_cves
  (recommend storing at link-level for now; optional to duplicate at article level)

### Migration
- Add migration that creates `cves` and `article_cves` if missing.
- Add indexes:
  - idx_article_cves_cve_id
  - idx_article_cves_article_id

---

## B) Deterministic Extraction Rules

### CVE extraction
- Regex: `\bCVE-\d{4}-\d{4,7}\b` (case-insensitive)
- Extract from:
  - title
  - summary/content text
  - url (optional: sometimes CVE appears in url)
- Normalize to uppercase.

### Confidence model (Phase 1)
- explicit CVE mention => confidence=1.0
- confidence_band = "linked"
- reasons: ["rule.cve.explicit"]

(No indirect/inferred matching yet. That’s Phase 2/3.)

### Evidence JSON (audit trace)
For each article (or link), store:

```json
{
  "extracted_signals": {
    "cve_ids": ["CVE-2025-12345"],
    "vendors": [],
    "products": [],
    "incident_keywords": []
  },
  "candidate_cves": [
    {
      "cve_id": "CVE-2025-12345",
      "component_scores": { "explicit": 1.0 },
      "confidence": 1.0
    }
  ],
  "final_decision": {
    "decision": "linked",
    "confidence": 1.0,
    "confidence_band": "linked",
    "rule_ids": ["rule.cve.explicit"]
  },
  "citations": {
    "urls": ["<article_url>"]
  }
}
```
Keep rule IDs stable strings.

---

## C) Worker integration points
In the ingest worker, after article acceptance and before publishing:

1) `signals = extract_signals(article)`
2) `cves = signals.cve_ids`
3) If cves:
   - upsert into `cves` table (update last_seen_at)
   - insert into `article_cves` (one row per CVE)
4) Persist `evidence_json` (link-level or article-level)
5) Continue existing publishing unchanged.

Important:
- Linking should be idempotent (re-runs should not duplicate rows).
- If article already linked to that CVE, skip insert (or replace evidence_json if desired).

---

## D) CLI additions (minimal)
Add command(s):
- `sempervigil cves list --since 7d`
- `sempervigil cves for-article <article_id>`
- `sempervigil cves for-cve CVE-YYYY-NNNN`

(These can be dev-only helpers; admin API later.)

---

## E) Tests (no network)
Add unit tests:
1) `test_extract_cve_regex_basic`
- title contains CVE → extracted normalized

2) `test_extract_cve_multiple`
- body contains 2 CVEs → both extracted

3) `test_article_cve_link_persist_idempotent`
- run persist twice → only 1 row (PK prevents dup)

4) `test_evidence_json_has_required_fields`
- confirm keys exist and rule IDs match

Use in-memory sqlite (`:memory:`) or temp db file.

---

## F) Acceptance Criteria (Phase 1 done when…)
- Running ingest produces the same posts as before
- Articles containing CVEs create rows in `article_cves`
- Evidence JSON is written and readable
- Tests pass locally (`pytest`)
- No new dependencies that require root or shelling out

---

## G) After Phase 1 (Phase 2 preview)
Only after Phase 1 is stable:
- add inferred CVE matching (vendor/product scoring)
- start event matching/creation
- add daily “Critical/High CVE” rollup post

(Do not start this yet.)