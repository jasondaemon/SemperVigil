# Analyst Event report contract

Event composition v11 keeps the existing evidence ledger, independent audit,
qualification, publication, and static JSON boundaries. It changes only new
composition requests and their reader-facing rendering. Existing immutable Event
revisions retain their recorded workflow and remain reproducible.

## Reader contract

An Event report starts with a short executive overview. Detail sections then add
evidence-backed information instead of restating that overview:

- attack vector: delivery or initial access;
- attack path: post-access activity and progression;
- timeline: dated milestones in normalized chronological order while preserving the
  source's displayed date and precision;
- impact: operational, data, financial, safety, or downstream consequences;
- response and recovery: investigation, containment, remediation, restoration, and
  current state;
- mitigations: source-supported defensive guidance, never presented as completed
  response;
- attribution: who made the attribution and its qualified basis;
- open questions: material unknowns that remain unresolved.

Only dimensions supported by accepted facts are requested. A supported dimension
must be covered in the first generated draft; absent evidence remains an empty
section. The validator rejects identical and near-identical recycled prose. It does
not impose a word target and cannot authorize invented detail.

Every generated paragraph is either a `sourced_finding` or an
`analyst_assessment`. Sourced findings carry no confidence label. Analyst assessments
must be explicitly framed as assessments, cite all supporting facts, and use high,
moderate, or low confidence. The independent support audit checks that the confidence
and conclusion do not exceed the cited evidence. Unsupported detail may still be
deleted by the existing one-pass remediation; completeness never overrides safety.

## Evolution and compatibility

The public revision footer retains deterministic added, superseded, and disputed
counts. Corrective and additive revisions also render the available changed fact
statements with source citations in a `What changed` section. No daily feed JSON,
article summary, scraper, Event-index schema, URL, publication pointer, or build
contract changes.

## Acceptance gates

- all material prose cites active accepted facts;
- every evidence-backed detail dimension appears in the initial draft;
- sourced findings have null confidence and assessments have an explicit confidence;
- duplicated or near-duplicated narrative is rejected;
- timeline milestones are chronologically ordered after conservative date parsing;
- support audit and publication integrity validation remain mandatory;
- audit remediation may remove unsupported detail but may not add evidence or claims;
- older workflow revisions render and publish under their recorded contracts;
- article ingestion, summaries, daily JSON, Event index, and static-site checks pass.

Roll out behind the existing Event composition enablement and serial hosted-model
queue. Canary one evidence-rich private Event first, inspect section usefulness,
assessment calibration, audit outcomes, latency, and token use, then qualify a public
revision only through the normal promotion and atomic release path.
