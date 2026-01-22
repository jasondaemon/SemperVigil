# LINKING_MODEL.md — Article ↔ Event ↔ CVE Linking (Design-Only)

> Deterministic-first linking rules for SemperVigil.
> No implementation changes implied.

---

## 1) Objectives & Non-Goals

Objectives:
- unify multi-source coverage into event-centric summaries
- preserve daily “breadth” posts for all accepted articles
- support CVE-backed and inferred events
- minimize LLM usage; deterministic rules are primary

Non-goals:
- no LLM requirement
- no real-time alerting
- no UI behavior changes

---

## 2) Entities & Keys (Conceptual)

Article:
- stable_id
- normalized_url
- published_at
- title
- source_id
- tags

Event:
- event_id
- title
- type
- status
- created_at
- updated_at
- canonical_cve_ids (0..n)
- confidence
- markers: primary / secondary / meta-event

Link tables (conceptual):
- event_articles (event_id, article_id, confidence, reasons[])
- event_cves (event_id, cve_id, confidence, reasons[])
- article_cves (article_id, cve_id, confidence, reasons[])

---

## 3) Deterministic Linking Pipeline (Ordered)

A) Pre-extract signals from article title + summary:
- CVE IDs via regex (CVE-YYYY-NNNN)
- vendor/product terms:
  - preferred: from known CVE CPEs when available
  - fallback: curated alias table (vendor/product synonyms)
- named incident/org strings (heuristic, no ML)

B) Candidate CVE matching:
- Direct: explicit CVE mention -> attach confidence=1.0
- Indirect: vendor + product + version + vuln-type keywords -> candidate set with scores

C) Candidate Event matching:
- Exact: event shares CVE id(s) and falls within active window
- Near: event shares vendor/product and incident keyword overlap
- Meta-event: if multiple related events share a common root term, group into meta-event

D) Decision:
- attach to best existing event if score >= threshold
- else create new event
- always record rules fired + scores for auditability

---

## 4) Confidence Scoring (Deterministic)

Score components (0–1), additive:
- +1.0 explicit CVE match
- +0.4 vendor match
- +0.3 product match
- +0.2 version match
- +0.2 vuln-type keyword match (RCE, SSRF, auth bypass, etc.)
- -0.3 ambiguity penalty if competing events exist with similar scores

Thresholds:
- >= 0.85: linked
- 0.60–0.85: linked_inferred
- < 0.60: unlinked (new event or unassigned)

Require storage of:
- confidence
- confidence_band
- reasons[]

---

## 5) Time & Update Semantics

- event active window: 30 days by default; extend on new linked articles
- “repost/update” behavior: event summary updated when new evidence arrives
- daily posts remain article-based and always list accepted articles
- severity upgrades: attach an update note to the event when CVE severity changes

---

## 6) Commercial/Noise Filtering

Deterministic rules to down-rank or tag commercial content:
- press releases
- vendor marketing pages
- affiliate or sponsored content

Do not discard automatically unless explicit allow/deny rules exist; prefer `commercial=true` tag.

---

## 7) Auditability & Troubleshooting

For each article, record:
- extracted signals
- candidate CVEs/events with scores
- final decision and rationale

Worked examples:
- Explicit CVE article: CVE mentioned, confidence=1.0, linked to CVE event
- Incident without CVE: linked via vendor/product + vuln-type keywords, inferred
- Multi-source same event across days: all articles attach to same event within window
- Meta-event grouping: multiple events grouped under a shared campaign root term
