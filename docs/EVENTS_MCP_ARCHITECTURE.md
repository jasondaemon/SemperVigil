# Events investigation and MCP architecture

Status: accepted design direction; isolated retrieval services tested offline
and in a targeted disposable-PostgreSQL suite.
Local stdio MCP adapter implemented and tested; workflow integration and
production deployment remain pending. No network endpoint is exposed.
Date: 2026-09-18

## Purpose and boundaries

Support evidence-backed historical correlation and event maintenance without
replacing ingestion, the shared single-job LLM queue, or static publication.
MCP is an internal adapter to application services, not a database agent or a
new source of truth. It adds no public endpoint and no website dependency.

```text
Authorized investigation client -> internal MCP adapter --+
                                                        |
Existing API / admitted worker --------------------------+-> domain services
                                                             |
                                                   scoped database access
                                                   evidence snapshots
                                                             |
                                                  version-bound proposals
                                                             |
                                               separate validation gates
                                                             |
                                                validated event revision
                                                             |
                                           existing API-driven publication
```

Domain services own query behavior, access checks, snapshot identity, proposal
validation, and revision rules. Workers call these services directly; they do not
need MCP round trips. The adapter must not duplicate business logic or bypass
job admission. Disabling it must leave ingestion, jobs, serving, and publication
operational. Application behavior belongs here; environment-specific access and
deployment values belong in k8s-platform; public presentation remains in the
theme repository.

## Initial service/tool contracts

Names below are proposed, not existing endpoints.

Implementation progress: [bounded retrieval foundation](INVESTIGATION_RETRIEVAL.md)
provides article discovery, legacy event metadata, and separately permission-gated
exact stored-text slices as unreferenced shared services. These are not complete
versions of all contracts below. The [optional stdio adapter](INVESTIGATION_MCP.md)
now exposes these three read operations to an authorized local process, with no
HTTP transport. Trusted passage scoping/origins, candidate search, proposals,
remote authentication, and production rollout remain pending.

| Operation | Purpose | Boundary |
| --- | --- | --- |
| `search_articles` | Find historical reporting by text, canonical entity, CVE, and date | Bounded, paginated results; stable ordering; coverage/truncation explicit |
| `get_article_evidence` | Retrieve exact scoped passages and provenance | Source URL, origin, offsets, snapshot version; no arbitrary URL fetch |
| `find_event_candidates` | Retrieve plausible incidents and matching reasons | Candidate retrieval is not a merge decision; weak matches remain candidates |
| `get_event_record` | Read chronology, claims, revisions, and open questions | Distinguish draft and last validated public revision |
| `compare_event_evidence` | Compare evidence identities and structured claims | Deterministic differences; semantic contradiction/entailment remains separately assessed |
| `propose_event_update` | Store a draft against event/evidence versions | Later phase, separate permission; no direct public write or merge |

Use indexed, parameterized database queries first. Measure retrieval recall and
query cost before considering embeddings. Do not confuse retrieval ranking with
the conservative incident-reference matcher: broad candidates can be useful even
when there is insufficient evidence to assign them to an incident.

Each response must identify relevant versions, missing evidence, and result
limits. Empty search results are not proof that an incident never occurred.
Suppressed/nonpublic records require explicit access policy; public export
eligibility must never be inferred from an investigator's ability to read them.

## Event-maintenance workflows

The first [private extractive workflow](EVENTS_PRIVATE_REVIEW.md) is implemented
as local operator tooling over read-only snapshots. It makes no model calls and
persists review artifacts only to a private local directory. It is not exposed
as an MCP tool or production job; its local decisions cannot publish. This gives
reviewers real evidence packets while semantic gates and job integration remain
pending. Browser visual acceptance is still required.

Versioned skills/workflow instructions describe investigation, follow-up updates,
conflicting reports, evidence audits, and reader-facing change summaries. They
must use the same service contracts and identify their version in proposals.
Instructions do not enforce security or publication policy. Local workers need
an explicit bounded workflow runner; skill files alone do not activate behavior.

An investigation retrieves deterministic candidates, assembles a bounded evidence
packet, and either produces a cited proposal or abstains. It must distinguish
repeat incidents at one company, publication versus incident dates, syndication,
and allegation versus independent corroboration. Exact quote validation alone
does not establish entailment or factual truth.

The existing offline passage/matching helpers are foundations, not a trusted
semantic extraction service. Passage scope, canonical aliases, and incident
references need independently established provenance before integration.

## Resource policy

- Read tools perform no implicit inference. Any proposed inference operation
  must enter the existing queue; no synchronous model call inside a tool handler.
- Preserve the current model and one local LLM job at a time. No autonomous
  tool loops, parallel agents, or automatic cloud fallback.
- Configure and test hard bounds for rows, response bytes, query time, tool calls,
  investigation duration, input/output tokens, and retries before pilot admission.
  Numeric budgets follow measured capacity; unset budgets must not mean unlimited.
- Cache against evidence, schema, workflow, and model versions; coalesce updates
  and revisit only changed relevant evidence. No automatic full-history replay.
- Attribute model time and retries to the originating job/event. Pause new pilot
  admissions when fresh-content queue or resource budgets are exceeded.
- Interactive read-only investigation is separate from production inference.
  Do not send private evidence to external AI clients/providers without an
  explicit data-access policy and authorization.

## Security and consistency

- Internal-only authenticated access; separate read and draft-proposal scopes.
  Review the selected transport/auth implementation before deployment.
- Retrieval uses a least-privilege read-only database role. Later draft writes
  use a narrowly scoped service path, not unrestricted database credentials.
- No arbitrary SQL, shell, infrastructure administration, secrets, or unrestricted
  network-fetch tool. Skills cannot extend those permissions.
- Treat scraped text and model output as untrusted data, never instructions.
  Enforce bounded input parsing and server-side authorization on every operation.
- Preserve exact evidence snapshots and immutable proposals. Use transactional
  event/evidence version checks and idempotency on proposal submission; stale
  proposals cannot overwrite newer work. Record actor, workflow, input versions,
  decision reasons, and outcome without logging credentials or unnecessary text.
- The proposal tool cannot publish, approve itself, merge/split incidents, or
  modify the current publication pointer. Those require separate tested domain
  operations and validation policies. Human approval alone is not the eventual
  automation design; unattended acceptance still requires explicit quality gates.
- Failed retrieval/generation/validation retains the last validated public report.
  No changes to daily JSON contracts, history retention, Hugo inputs, build
  commands, or atomic publication are introduced by this adapter.

## Delivery and acceptance

1. **Stage 1/2: domain retrieval and read-only adapter.** Define schemas and access
   policy; test query boundaries, pagination, snapshot consistency, suppression,
   injection-like source text, authorization, and direct/MCP result parity.
   Evaluate historical retrieval with adjudicated multi-document cases. Deploy
   the isolated internal read-only path only after manifest review and regression
   checks; prove disabling it has no effect on production publishing. No new
   inference demand and no write tools in this first release.
2. **Stage 2/3: proposal-only shadow workflow.** Complete trusted scope/reference
   handling, bounded parsing, immutable persistence, and transactional version
   checks. Test stale writes, duplicate retries, and denied publication attempts.
   Observe budgets and quality on 5-10 events through the existing queue, with
   public output disabled. Require the staged plan's seven-day pilot gate.
3. **Stage 4: validated automation.** Enable gradual publication only after the
   separate evidence, relevance, uncertainty, and revision gates pass. Retain
   existing URLs, incremental exports, and last validated pages on failure.

Do not ship unused helpers as proof of an integrated runtime release. Keep
adapter availability, workflow admission, and public publication independently
controlled. Rollback disables the affected path and preserves additive evidence
and drafts; it does not delete or regenerate history.

Track implementation in [upgrade-tracker.html](upgrade-tracker.html) and the
[staged plan](EVENTS_STAGED_IMPLEMENTATION_PLAN.md). This document establishes
design constraints, not a claim of measured performance or deployed security.
