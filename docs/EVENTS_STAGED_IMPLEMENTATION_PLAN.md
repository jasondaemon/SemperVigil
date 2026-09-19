# SemperVigil stabilization and Events implementation plan

Date: 2026-09-18
Status: Staged implementation in progress; consult the tracker for tested and
deployed slices. Shared retrieval and an optional local stdio MCP adapter pass
offline and targeted disposable-PostgreSQL tests. They are not deployed to
production; remote authentication and operator release policy remain pending.
This document alone does not authorize a runtime rollout.

## Objective and constraints

Make SemperVigil worth reading by producing reliable, continuously updated incident
coverage with attributable evidence and meaningful changes. AdSense is a secondary
goal, not the definition of success.

- Preserve one local LLM job at a time across all SemperVigil stages.
- Preserve current static public serving, incremental historical feed exports,
  and the supported API-driven atomic publish process.
- Keep CVEs in the daily feed with NVD links; do not recreate CVE detail pages.
- Introduce Events through small, reversible releases rather than a rewrite.
- Keep application behavior in this repository, presentation in sempervigil-hugo,
  and deployment values in k8s-platform. Persist versioned AI configuration through
  the supported admin/API workflow without committing credentials.
- Historical database migration incidents are outside this plan.
- Treat daily JSON downloads as a downstream API: preserve paths, fields, types,
  IDs, score units, null semantics, and date/count accuracy. Prove compatibility
  and database selection completeness before changing exports.
- Preserve site availability and prior validated outputs during each rollout.
  Record build-time/resource baselines and verify unchanged history remains out
  of Hugo's loaded data path. Never trade correctness for a faster build.

Initial implementation and verification procedures:
[STABILIZATION_VERIFICATION.md](STABILIZATION_VERIFICATION.md).
Track progress locally in [upgrade-tracker.html](upgrade-tracker.html); refresh
after each implementation update. This file records verified status, not live telemetry.

## Inspected baseline

Observed production configuration, not proposed capacity:

| Component | Observation |
| --- | --- |
| AI host | 12 logical CPUs; approximately 31.1 GiB RAM; shared with other AI services |
| GPU | RTX 3060, 12 GiB VRAM; approximately 5.6 GiB used at the sampled instant |
| Model | Qwen 2.5 7B Instruct, 7.6B parameters, Q4_K_M quantization |
| Loaded context | 16,384 tokens; model metadata advertises 32K but serving uses 16K |
| Model placement | Ollama reports 100% GPU; keep-alive is indefinite |
| Ollama controls | One loaded model and one parallel request |
| Ollama resources | No Kubernetes CPU/memory requests or limits configured |
| Local worker | One replica; concurrency 1; 512Mi RAM request and 1Gi limit |
| Worker vs model memory | Worker resources do not bound the separately hosted model |
| Local AI profiles | Article, context, product, threat, CVE, and event derivation routes use the same local model |
| Profile budgets | Local model max_context and profile parameter fields were unset |
| Event reports | No dedicated event-report route; code falls back to article-summary profile |
| Cloud configuration | Older daily-brief routes reference gpt-4o-mini; configured routes do not establish current usage |
| Provider retry settings | Local provider: 1,200-second timeout and retries=2; verify actual retry behavior before tuning |

The 48-hour query returned 90 successful inference records, all within roughly
32 minutes on September 18: mean latency 8.95 seconds, p95 21.63 seconds.
These records did not join to jobs, so stage attribution needs repair or validation.
Completed jobs in the same query window averaged about 5 seconds for summaries,
13 seconds for context, and 5 seconds for product enrichment. Job duration and
inference latency are distinct measurements. This is not a sustained throughput
benchmark and does not establish event-report cost.

At inspection, roughly 450 local jobs were queued, with one summary running.
The oldest queued requests were about half an hour old. Historical failed job
totals must not be interpreted as current failures or blindly retried.

## LLM execution policy for every stage

1. Retain the current model and serialization. Do not add parallel agents, model
   ensembles, automatic cloud fallback, or inference outside the shared queue.
2. Measure effective token limits through the complete application/provider/Ollama
   path. An initial candidate budget is at most 10K total input tokens and 2K
   output tokens, leaving space inside the 16K context. This is an evaluation
   starting point, not an instruction to truncate every document to that length.
3. Chunk long articles at semantic boundaries. Extract relevant passages from
   roundups, preserving passage identifiers and original source text references.
4. Reuse existing validated extraction. Normally permit at most one incremental
   evidence-extraction call per changed article; additional chunks consume an
   explicit budget. Do not add a separate model call for each claim or validator.
5. Generate event prose from a compact evidence ledger, not concatenated article
   histories. Render facts, source lists, diffs, and timelines deterministically.
6. Cache by input fingerprint, prompt/schema version, and model identity. Hash
   relevant evidence, not timestamps that change on every run.
7. Start the event pilot with a configurable ceiling of 10% of measured local
   inference time over a rolling day. This ceiling is provisional; Stage 0 must
   establish that spare capacity exists. Suspend new pilot admissions when the
   agreed fresh-content queue-age budget is exceeded.
8. Schedule current summaries, active-event evidence, and background enrichment
   with weighted fairness and aging. Count execution time, not just job counts.
   Never preempt a running call or let backfills fill the queue without bounds.
9. Coalesce event report requests; initially allow one optional prose refresh per
   event per hour. Urgent, explicitly evidenced changes may request an earlier
   turn through the same scheduler. Deterministic updates need no model call.
10. Allow at most one bounded repair attempt for invalid structured output during
    the pilot. Prevent transport retries, job retries, and repair retries from
    multiplying. Failed generation retains the last validated published revision.

## Stage 0: Establish a reliable baseline

Cross-stage architecture: [Events investigation and MCP](EVENTS_MCP_ARCHITECTURE.md).
Shared domain services are the implementation boundary; an internal MCP adapter
exposes bounded capabilities to authorized clients. It is not a replacement for
workers, a public service, or an inference/publication bypass. Skills describe
procedures; code enforces access, evidence, budgets, and revision rules.

Scope: instrumentation, configuration inventory, and an offline evaluation corpus.

- Collect at least 72 hours of arrival rates, queue ages, completions, retry rates,
  input/output size, inference latency, GPU/RAM use, and model load behavior.
- Distinguish ingest-to-summary latency from queue wait and execution duration.
- Connect inference records to job, stage, prompt version, and model identity.
- Record actual runtime overrides and intended component image versions.
- Inventory shared GPU consumers and determine whether resource contention occurs.
- Create 30-50 fixed evaluation examples: roundups, repeated incidents at one
  organization, aliases, syndicated reports, conflicting dates, corrections,
  unsupported attacker claims, and prompt-like text inside scraped content.
- Curate expected outcomes once; this evaluation work is not daily editorial work.
- First real-source seed: 12 provisional Odido review cases across four documents,
  tracked in [Events evaluation](EVENTS_CURATED_EVALUATION.md). Six misleading
  claims still pass structural checks; semantic rejection is a release gate, not
  implemented behavior. Independent review and actual model evaluation remain.
- Establish publication and fresh-summary latency targets from measured capacity.

Exit: explain queue behavior with measurements; define event capacity and latency
budgets; record reproducible fixtures and current behavior. No new event backlog.

## Stage 1: Stabilization and security foundation

Scope: no additional inference demand. Small independent releases.

- Separate offline tests from disposable-database integration tests; gate releases
  on both. Add publication checks for homepage, search, metrics, a historical day,
  CVE filters, and an event page.
- Monitor output freshness and oldest pending work, not only pod readiness.
- Reconcile current documentation with deployed architecture and retired features.
- Make production admin credentials mandatory; strengthen session expiry and
  state-changing request protection while retaining required health endpoints.
- Map required service connections and introduce tested network restrictions in
  small steps, accounting for VPN sidecars, DNS, database, storage, and inference.
- Validate fetched destinations and redirects; block unintended private-network
  access while explicitly allowing required internal service connections.
- Validate public link schemes and generated content. Introduce CSP in report-only
  mode before considering enforcement; never collect credentials in reports.
- Lock tested dependencies and record immutable release identities.
- Define internal MCP authentication, read/proposal scopes, and client data-access
  policy before exposing any evidence. No arbitrary SQL, shell, or fetch tools.
- Propose Ollama resource reservations and a safety ceiling only after measurement
  of loading and longest supported contexts. Do not fit the model to an arbitrary
  RAM limit or confuse host RAM with VRAM. Account for other GPU consumers.

Exit: representative site checks pass, fresh processing stays within baseline
budgets, and security changes preserve all required service paths.
Rollback: revert each deployment/configuration change independently.

## Stage 2: Evidence and event correctness

Queue integration checkpoint: `event_review_private` now has opt-in admin
admission, bounded/deduplicated requests, worker dispatch, and private artifact
results. Admin job visibility uses registries and observed history rather than
incomplete UI lists. Tested, not deployed; the next runtime rollout is admin and
LLM-worker verification with admission disabled. See [release gates](EVENTS_PRIVATE_QUEUE.md).

Private review milestone: [extractive review workflow](EVENTS_PRIVATE_REVIEW.md)
now connects bounded source snapshots, exact passage suggestions, local HTML
review, immutable decisions/artifacts, and stale-input checks. Three real events
are available from the tracker. This is operator review tooling, not production
job integration or automatic factual approval. No new inference load or public
output changes. Semantic/model evaluation and the shadow worker remain pending.

Scope: additive internal records and deterministic logic; public behavior gated.

- Build shared bounded retrieval services and a read-only MCP adapter for articles,
  exact evidence, event records, and incident candidates. Test result parity,
  authorization, query limits, suppression, and retrieval quality independently
  of LLM output. First adapter deployment adds no writes or inference demand.
- Integrate trusted passage/reference handling and bounded proposal schemas.
  Skills for investigations, updates, conflicts, and audits reuse these contracts;
  immutable proposal writes require idempotency and transactional version checks.

- Add stable claim IDs, source and passage references, incident relevance, date
  precision, observed time, assertion status, and supersession/correction links.
- Record incident date separately from disclosure, article publication, and ingest
  dates. Never invent a milestone date from the incident's starting date.
- Preserve immutable event revisions and validated publication pointers. Enforce
  evidence-version checks so stale generation cannot overwrite a newer result.
- Replace organization-plus-kind matching with incident-aware candidate matching.
  Preserve existing public URLs; support audited merge/split decisions internally.
- Distinguish reporting sources from independent evidence origins. Syndication and
  repeated attacker allegations do not independently confirm an incident.
- Require source-backed publication criteria. A relevant primary disclosure can
  establish a fact; uncertainty must remain explicit when corroboration is weak.
- Separate publication status from lifecycle. Dormant or closed incidents remain
  readable; lifecycle changes must not accidentally unpublish history.
- Add validation that claim citations resolve to supplied evidence. Citation
  existence alone does not prove entailment; ambiguous claims stay unpublished.
- Remove raw-model-text publication fallback and generic-summary fallback for
  event reports behind the new feature path.

Exit: fixtures prevent cross-incident contamination, repeated-incident merges,
unsupported dates, and untraceable published claims. Revision replay is stable.
Rollback: disable the new path; retain additive data and prior publication pointer.

## Stage 3: Bounded LLM pilot in shadow mode

Scope: 5-10 representative events; current evidence first; no bulk history replay.

- Exercise proposal-only maintenance workflows through shared services; MCP clients
  receive no direct publication permission. Bound tool calls and packet sizes,
  record workflow versions, and test stale/duplicate proposals. Model work always
  uses the existing single-job queue, never synchronous inference in read tools.

- Add dedicated event evidence and report profiles using the existing model.
  Version prompts and schemas and verify effective input/output limits.
- Evaluate whether existing article context extraction can emit reusable evidence
  without reducing summary quality. Do not combine stages merely to save calls.
- Use deterministic candidates before model extraction. Broad keyword matching
  alone must not exclude a potentially relevant incident article.
- Extract only event-relevant claims from roundups. Keep unsupported or ambiguous
  claims out of the public report; do not ask the model to fill missing sections.
- Test optional compact prose against a deterministic report. If the 7B model
  cannot meet the evidence standard, publish the deterministic form instead.
- Apply the shared queue, time budget, debounce, cache, and bounded repair policy.
- Disable automatic web-research expansion initially. Later evaluate allowlisted,
  rate-limited research with source provenance and explicit per-event budgets.
- Compare generated revisions with fixtures and inspect pilot reports before
  enabling public output. Log quality failures as failures, not successful reports.

Exit: all mandatory correctness fixtures pass; no known unsupported material claim
or date in reviewed pilot output; at least seven days without a new queue-latency,
GPU-memory, or summary-quality regression. Review high-percentile call durations.
Rollback: stop pilot admissions and retain existing public pages.

## Stage 4: Publish useful living event pages

Scope: validated pilot events, then gradual expansion.

- Render current status, what changed, evidence-linked milestones, impact,
  affected products/CVEs, primary mitigation references, and unresolved questions.
- Promote accepted proposals through separate application validation and revision
  gates, not MCP/skill approval. Adapter failure or shutdown must not affect
  ingestion, existing jobs, public serving, or the last validated report.
- Make confirmed facts, attacker allegations, and estimates visibly distinct.
- Show meaningful update times, data coverage, sources, and correction history.
  Do not label refresh timestamps as new incident developments.
- Use one canonical event URL and indexable HTML. Update structured metadata and
  sitemap modification dates only when relevant content changes.
- Publish event references in daily JSON and cards; keep feed compatibility and
  existing CVE links. Add Events to search without restoring entity-page exports.
- Generate only changed event inputs. Keep historical feed JSON out of Hugo's
  loaded data path. Retain API-driven builds and atomic release activation.
- Validate output before activation. An invalid event export must retain its last
  validated version; monitor and surface the failure to the administrator.
- Audit existing public events before highlighting them. Repair one bounded batch
  at a time; prioritize contaminated pages and active incidents.

Exit: desktop/mobile and regression checks pass; revisions, citations, feed links,
and old event URLs work; repeated evidence does not cause report/build churn.
Rollback: disable new presentation/admission and publish the last validated state
through the supported API. Document any build/publish changes in change control.

## Stage 5: Original value and reader retention

Deliver individually, after Events proves useful:

1. Changes-since-last-visit and event RSS/Atom feeds; no new inference required.
2. Browser-local product/event watchlists before introducing accounts or email.
3. Evidence-backed disclosure, patch, and exploitation intervals, with unknown
   dates and coverage limitations shown. Preserve historical observations first.
4. KEV/EPSS change views, including observation timestamps and missing-data states.
5. Source disagreement and independent-evidence views, derived from claim records.
6. Compact incremental search shards with version-aware caching; avoid fetching
   every full historical day to search the archive.
7. A hands-free "Listen to the news" mode, developed as described below. The
   initial daily-feed player can be delivered independently after Stage 1;
   spoken event updates depend on validated Stage 4 output.

### Audio briefing: one play button, continuous listening

Goal: open the site, select Play before driving, and hear a continuous briefing
without needing to read the screen or operate controls between articles.

First release:

- Use Web Speech API speech synthesis as the initial implementation candidate.
  Read existing published daily JSON; require no extra LLM calls, microphone
  permission, account, or server-side speech generation.
- Default to the selected day's News, in displayed order, with optional CVEs and
  validated event updates. Read the headline, source attribution, and narrative
  summary; use existing bullet text when narrative is unavailable. Skip decorative
  metadata and raw URLs, but preserve material uncertainty and source caveats.
- Provide a prominent Play button, large Pause/Resume and Previous/Next controls,
  Stop, playback speed, and available voice selection. Start only on user action.
- Advance automatically, announce story transitions, and retain a versioned
  article/sentence bookmark locally. Freeze the playlist for the listening session
  so a feed refresh cannot unexpectedly reorder or replace the current briefing.
- Speak bounded sentence chunks to support recovery and approximate resume. Handle
  delayed voice availability, errors, interruptions, and unsupported browsers.
- Do not describe all browser speech as offline or on-device: available voices may
  use remote services. Prefer a local voice when available and disclose behavior.

Driving acceptance gate:

- Test extended playback on physical iOS Safari and Android Chrome devices,
  including screen lock, backgrounding, Bluetooth car/headset audio, navigation
  prompts, incoming calls, connectivity changes, and resume after interruption.
- Evaluate Media Session metadata and hardware controls where supported. Do not
  assume Media Session makes speech synthesis behave like a background audio
  player or guarantees steering-wheel controls.
- Do not label the first release driving-ready unless these checks pass on the
  supported devices. Avoid a design that requires keeping the screen awake.

Second release if browser synthesis cannot meet that gate:

- Evaluate cached speech audio played through a standard HTML audio element,
  with Media Session controls where supported, downloadable briefings, and
  optionally a podcast feed. Reuse validated text; do not add an LLM script pass.
- Cache per text/voice/version fingerprint and assemble playlists from segments
  so unchanged stories need no regeneration. Measure TTS cost and host resources
  separately; never contend with the serialized LLM workload without a budget.
- Publish audio and transcript references atomically through supported workflows.
  Invalidate corrected audio and associate every briefing with its content version.

References:
- https://developer.mozilla.org/en-US/docs/Web/API/SpeechSynthesis
- https://developer.mozilla.org/en-US/docs/Web/API/Media_Session_API

Exit: evaluate return visits, event-update usage, useful referrals, and operational
cost, without substituting page count or text length for quality.

## Stage 6: Monetization readiness

- Publish methodology, automation disclosure, coverage limitations, corrections,
  ownership/contact information, privacy information, and source attribution.
- Avoid thin generated pages, unsupported claims, and promotional text presented
  as reporting. Content rights and ad eligibility require separate consideration.
- Assess AdSense only after the public event feature demonstrates original value.
  Google explicitly lists automatically generated content without manual review
  or curation in its replicated-content examples. Automated checks cannot be
  represented as human editorial review or a guarantee of approval.
- If editorial review is acceptable, define it honestly for ad-bearing content.
  If publishing must remain fully unattended, consider sponsorship or paid change
  alerts/data access without making AdSense approval a release requirement.

Policy reference:
https://support.google.com/publisherpolicies/answer/11190248?hl=en

## Release discipline and stop conditions

- One reviewable stage/change per release, with relevant tests and documentation.
- Keep feature switches separate for extraction, report generation, and publication.
- Render and compare affected manifests; deploy only changed components.
- Run public build verification through the platform API, never direct Hugo.
- Do not automatically reprocess all history when a prompt/model version changes.
  Use explicitly bounded admission and prioritize current affected events.
- Pause new Events work for unsupported claims, fixture regressions, resource
  pressure, repeated invalid output, or sustained fresh-content latency breaches.
- Keep the last validated report and website available while investigating.
- Completion requires source configuration, Git, and deployed version agreement.

The first implementation slice is Stage 0 plus the offline-test separation from
Stage 1. Hardware upgrades, model replacements, and extra inference concurrency
are not prerequisites for this plan.
