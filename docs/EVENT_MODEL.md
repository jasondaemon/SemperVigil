# EVENT_MODEL.md — Event Lifecycle (Design-Only)

> Design-only reference for event lifecycle and aggregation behavior.
> No implementation changes implied.

---

## 1) Purpose & Scope

An Event is a cross-source storyline (incident, vuln campaign, product issue) that aggregates articles and CVEs into a coherent narrative. Events are the unit of aggregation; daily posts remain breadth-first and list all accepted articles.

This document is consistent with:
- `docs/LINKING_MODEL.md`
- `docs/CVE_MODEL.md`

---

## 2) Event Types (Enumeration + Intent)

Initial type set (small and extensible):
- incident: breach, compromise, campaign, outage
- vulnerability: CVE-centered vulnerability storyline
- product_change: vendor security change, patch release, EOL
- threat_activity: APT/campaign without a clear incident anchor
- advisory_roundup: government or vendor advisory collections

Topic builds (cyber/AI/fandom) are deployment profiles, not event types.

---

## 3) Event Identity & Canonicalization

- event_id: stable UUID or hash; never changes once created.
- metadata (title/status/links) can update over time without changing event_id.
- canonical title: deterministic template (Vendor/Product + short descriptor).
- canonical CVE list: 0..n (direct + inferred) with confidence per CVE link.

Primary/Secondary/Meta:
- primary event: main storyline for an article
- secondary events: sub-threads referenced in an article
- meta-event: grouping wrapper across related events

Allowable relationships:
- parent_event_id (meta-event parent)
- child_event_ids (events contained by a meta-event)
- related_event_ids (peer relationships without hierarchy)

---

## 4) Lifecycle States (State Machine)

States:
- proposed
- active
- updating
- dormant
- closed

Transitions:
- proposed -> active: confidence crosses threshold or corroboration rule satisfied
- active <-> updating: new evidence arrives; summary pending refresh
- active -> dormant: no updates for N days
- dormant -> active: new high-confidence link arrives
- dormant -> closed: no updates for M days
- closed -> active: severity upgrade or major new evidence (explicitly logged)

Diagram:
proposed -> active <-> updating -> dormant -> closed
                     ^                |
                     |                v
                     +----------------+

Defaults:
- N = 30 days (dormant window)
- M = 120 days (close window)
These are configurable later via admin settings.

---

## 5) Update Semantics & “Repost” Policy

Event summary updates occur when new evidence arrives. An “event update” post is emitted when:
- new linked article above confidence threshold
- new CVE attached or inference confidence increases
- CVSS severity upgrade or vector change (see `docs/CVE_MODEL.md`)
- vendor patch or mitigation guidance changes

Daily posts remain breadth-first; event pages are storyline-first.

The “update” tag indicates a meaningful change to the event narrative.

---

## 6) Confidence & Evidence Model

Event confidence is derived from linked evidence (articles + CVEs) with explicit reasons.

Evidence buckets:
- direct: explicit CVE mention or official incident confirmation
- corroborated: >= 2 independent sources
- inferred: vendor/product/IOC match without explicit confirmation

Minimum graduation rules (proposed -> active):
- 1 official source, or
- 2 independent sources, or
- explicit CVE with confidence >= threshold

Confidence bands:
- linked: >= 0.85
- linked_inferred: 0.60–0.85
- unlinked: < 0.60

---

## 7) Aggregated Summary Composition (Non-LLM-First)

Deterministic summary skeleton:
- What happened (1–2 lines)
- Affected products/orgs
- Exploit status / IOCs (if present)
- CVEs involved (with scores)
- Timeline (first seen, last updated)
- Sources (linked articles)

LLM usage is optional and must preserve citations and mark inferences.

---

## 8) Filtering/Noise Handling at Event Level

- Commercial content reduces confidence but does not discard evidence automatically.
- Marketing-heavy posts cannot create new events without corroboration.
- Prefer tagging `commercial=true` and down-weighting in scoring.

---

## 9) Worked Examples

1) Multi-day incident across 3 sources:
- Day 1: proposed event created from source A
- Day 2: source B corroborates -> active
- Day 3: source C adds detail -> updating; summary refreshed

2) CVE lifecycle:
- Day 1: CVE introduced -> active vulnerability event
- Day 5: vendor patch released -> update note
- Day 10: severity upgraded -> update note, confidence increases

3) Meta-event grouping:
- “MOVEit exploitation wave” as meta-event
- child events: CVE-2023-XXXX, CVE-2023-YYYY
- meta-event groups child timelines with shared root term

---

## 10) Open Questions / Future Extensions

- Merge/split policy (manual override in admin UI)
- Cross-topic deployments (cyber vs AI vs fandom) and type consistency

---

Design-only. No implementation changes implied. Requires explicit approval before code/schema work.
