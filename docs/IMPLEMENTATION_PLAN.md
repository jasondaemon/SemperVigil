# IMPLEMENTATION_PLAN.md — Event & Linking Implementation Plan (Design-Only)

> Implementation-ready plan derived from:
> - `docs/EVENT_MODEL.md`
> - `docs/LINKING_MODEL.md`
> - `docs/CVE_MODEL.md`
>
> No code, schema, or migration changes implied.

---

## 1) Goals

- Translate the event/linking/CVE designs into concrete worker + storage responsibilities.
- Preserve deterministic-first behavior and auditability.
- Keep daily article publishing unchanged.

Non-goals:
- No UI work.
- No LLM requirements.
- No schema changes yet.

---

## 2) Ownership Boundaries

Worker responsibilities (future):
- Extract deterministic signals from article text/title.
- Score candidate CVEs and events.
- Create/attach event links based on thresholds.
- Emit audit traces for each decision.
- Trigger event summary updates and “update” posts when rules fire.

Storage responsibilities (future):
- Persist event objects and link tables (as defined in `docs/DATA_MODEL.md`).
- Store confidence, confidence_band, reasons[], and evidence summaries.
- Store event summary versions and update metadata.
- Preserve audit trails for troubleshooting.

Admin responsibilities (future):
- Read-only health/trace visibility (no execution).
- Manual overrides for merge/split (later).

---

## 3) Pipeline Integration Points (Worker)

Ordered sequence for a single article batch:
1) Ingest articles (existing pipeline).
2) Signal extraction:
   - CVE regex matches.
   - vendor/product terms (CPE-derived or alias map).
   - incident keywords / named org strings (heuristic).
3) Candidate CVE scoring:
   - direct CVE mention => confidence=1.0.
   - indirect matches => component scoring.
4) Candidate Event matching:
   - exact CVE match + active window.
   - near match via vendor/product + keyword overlap.
   - meta-event grouping rules.
5) Decision:
   - attach to best existing event if threshold met.
   - else create a new event.
6) Persist:
   - event links (article↔event, event↔CVE, article↔CVE).
   - confidence + reasons + evidence.
7) Update semantics:
   - mark event “updating” if new evidence arrives.
   - emit update notes if triggers are met.
8) Publishing:
   - daily posts unchanged.
   - event summaries updated separately.

---

## 4) Concrete Storage Touchpoints

Planned tables (existing in `docs/DATA_MODEL.md`):
- events
- event_mentions
- event_relations
- event_summary_versions
- inference_suggestions
- cves, cve_snapshots, cve_changes

Required stored fields for linking:
- confidence
- confidence_band
- matched_by
- inference_level
- reasons[]
- evidence_json

Auditability requirements:
- store full decision trace per article in `evidence_json`
- keep deterministic rule identifiers in `reasons[]`

---

## 5) Deterministic Scoring Model (Implementation-Ready)

Score components (0–1), additive:
- +1.0 explicit CVE match
- +0.4 vendor match
- +0.3 product match
- +0.2 version match
- +0.2 vuln-type keyword match
- -0.3 ambiguity penalty if competing events close in score

Thresholds:
- >= 0.85: linked
- 0.60–0.85: linked_inferred
- < 0.60: unlinked (create new event or leave unassigned)

Store:
- confidence
- confidence_band
- reasons[]

---

## 6) Event State Updates

State transitions (per `docs/EVENT_MODEL.md`):
- proposed -> active (confidence threshold or corroboration)
- active <-> updating (new evidence)
- active -> dormant (no updates for N days)
- dormant -> active (new high-confidence evidence)
- dormant -> closed (no updates for M days)
- closed -> active (severity upgrade or major evidence)

Defaults (configurable later):
- N = 30 days
- M = 120 days

Triggers for “update” posts:
- new linked article above threshold
- new CVE attached or confidence increase
- CVSS severity upgrade or vector change
- vendor patch/mitigation change

---

## 7) Data Sources for Signals

Deterministic sources only:
- article title + summary text
- CVE metadata from NVD snapshots
- curated vendor/product alias map (future)
- static vuln-type keyword list

LLM usage:
- optional, gated by config, and must preserve citations
- not required for baseline linking

---

## 8) Testing Strategy (Future)

Unit tests:
- CVE regex extraction
- deterministic scoring calculations
- event matching selection rules
- state transition rules
- audit trace completeness

Integration tests:
- article batch with explicit CVE
- incident without CVE that becomes inferred
- meta-event grouping

No live network calls in tests.

---

## 9) Rollout Sequence (Future)

Phase 1:
- implement signal extraction + CVE matching
- persist article↔CVE links

Phase 2:
- implement event matching + event creation
- persist article↔event links

Phase 3:
- implement event summaries + update policy
- meta-event grouping

---

Design-only. No implementation changes implied. Requires explicit approval before code/schema work.
