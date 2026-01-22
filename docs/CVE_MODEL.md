# CVE_MODEL.md — CVE Intelligence Model (Draft)

> Design-only reference for CVE ingestion and correlation.
> No implementation changes should occur without explicit approval.

---

## 1) Authority & Sources

Primary authority:
- NVD (National Vulnerability Database)

Optional secondary references (future):
- vendor advisories
- CISA KEV
- GHSA

NVD is the system of record for CVE identifiers and core metadata.

---

## 2) CVSS Preference

Preferred order:
1) CVSS v4.0
2) CVSS v3.1

Store all available metric versions, but surface v4 when present.

---

## 3) Core CVE Fields

Each CVE stores:
- published_at
- last_modified_at
- preferred_cvss_version
- preferred_base_score
- preferred_base_severity
- preferred_vector
- full metric JSON for v4 and v3

---

## 4) Component-Level Diffs

Track changes over time by comparing snapshots:
- base score
- severity
- vector
- metric-level diffs (changed metric values)

Every change should record:
- change_at
- change_type
- from/to values
- diff details (metrics_changed_json)

---

## 5) Severity Change Tracking

Derived change records should identify:
- severity upgrades (HIGH -> CRITICAL)
- score changes
- preferred version changes (v3 -> v4)

These changes drive alerts and summary reporting.

---

## 6) Correlation Confidence

When CVEs are linked to events or articles:
- store confidence (0.0 - 1.0)
- inference_level (confirmed|inferred|suggested)
- matched_by (explicit|rule|nvd|manual|llm)
- evidence summary (terms/entities/links)

Deterministic matching is primary; LLM assistance is optional and cost-controlled.

---

## 7) Planned Storage (No Implementation Yet)

Use the existing schema in `docs/DATA_MODEL.md`:
- cves
- cve_snapshots
- cve_changes
- inference_suggestions (for low-confidence links)

No changes to ingestion or UI until explicitly authorized.
