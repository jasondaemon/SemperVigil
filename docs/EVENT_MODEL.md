# EVENT_MODEL.md — Event & CVE Correlation (Draft)

> This document defines the planned event model for SemperVigil.
> It is intentionally non-implemented and serves as a design target.

---

## 1) Event Types

Events are living narrative objects that group related articles and signals.

Primary types:
- CVE: CVE-backed events anchored to an explicit CVE identifier.
- INCIDENT: breaches, campaigns, outages, or known incidents without a CVE anchor.
- META: umbrella events that group related CVEs or incidents.

---

## 2) Primary vs Secondary Events

- Primary event: the core event an article is primarily about.
- Secondary events: additional related events the article mentions or supports.

Each article can link to:
- one primary event
- zero or more secondary events

---

## 3) CVE-backed vs Inferred Events

Explicit (deterministic):
- explicit CVE IDs mentioned in text
- authoritative vendor references

Inferred (heuristic):
- vendor + product + vulnerability type matches
- time proximity + shared indicators
- weak keyword evidence

Inferred links must carry:
- confidence score
- inference level (confirmed|inferred|suggested)
- evidence summary

---

## 4) Confidence & Inference

Every event link includes:
- confidence (0.0 - 1.0)
- inference_level (confirmed|inferred|suggested)
- matched_by (explicit|rule|nvd|manual|llm)

LLM assistance is optional and must be cost-controlled. Deterministic logic is primary.

---

## 5) Meta-events

Meta-events group related events without replacing them. Examples:
- a campaign covering multiple CVEs
- a vendor-wide incident with multiple advisories

Meta-events link to child events with:
- relation_type (umbrella|related|exploits|caused_by)
- confidence and evidence

---

## 6) Planned Storage (No Implementation Yet)

See `docs/DATA_MODEL.md` for event-related tables:
- events
- event_mentions
- event_relations
- event_summary_versions
- inference_suggestions

No ingestion or UI changes should be made until the event model is implemented.
