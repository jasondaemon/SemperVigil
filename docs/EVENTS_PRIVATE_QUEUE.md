# Private Events jobs and admin visibility

Status: admin `bddeff1`, LLM worker `2de5845`, platform `683abb8`.
**Authenticated operator-triggered private admission and bounded model suggestions
are enabled.** Autonomous admission and public publication are not.
Extractive pilot `job_05152e7dd57246d48c01bb47ead61d93` succeeded: seven documents,
23 passages, authenticated attachment hash verified, event row unchanged.
Model-assisted pilot `job_2be77141919a403592ce6ca21ce8e8a3` targets the same Odido
event (`evt_69844df3a97f`, aliases `["Odido"]`). It is pending behind normal CVE
work; no model quality or latency result is claimed yet. Preserve its low priority
and single-job runner policy rather than bypassing normal work.
The bounded model cohort also includes Vercel
(`job_4cd3c296ba03499380a220b485a0c2de`) and European Commission
(`job_378b8db685ee4c219314058ef0082316`). These are the same three events used in
the prior private visual review, not additional automatic discovery. Do not
duplicate them. The Commission case must test separation of distinct incidents.
Live Event Detail shows the enabled private control; all 35 job types and four
dashboard groups were verified in the preceding release.

The dashboard's initial request now uses `include_backlog=false` to display the
same job-status counters and type catalogue without waiting for content-wide Need
queries. It then requests the unchanged full metrics. Need values are absent
(shown as a dash) until calculated; the default API and Prometheus path retain
their existing full payload. The refresh guard allows one outstanding metrics
request per page. This is display isolation, not a fix for slow backlog SQL.

The chart supports `workerLlm.image` overrides (inheriting shared image defaults)
so this private workflow can be released without replacing unrelated workers.

## Runtime path

An authorized admin POST to `/admin/api/events/{event_id}/private-review` accepts:

```json
{"aliases": ["Organization name"]}
```

The server canonicalizes aliases and admits `event_review_private` to the existing
`llm_local` queue at priority -10 with one attempt. The existing worker dispatches
the tested private review service. Default configuration is extractive without
inference. With the separate model flag enabled, it uses the guarded profile
described below and participates in model admission. Neither mode alters public
event reports, runner concurrency, build commands or publication behavior.

### Optional bounded model assessment (deployed, real-model pilot pending)

`SV_EVENT_REVIEW_MODEL_ENABLED=0` retains the deployed extractive behavior.
With an explicitly enabled worker and `SV_EVENT_REVIEW_PROFILE_ID`, the same
serialized job can make one bounded inference call. No admin-side inference,
automatic admission, extra runner, or public report write is introduced.

The dedicated profile must reuse the currently active CVE enrichment provider
and local model, have no fallback, and use the exact system prompt in
`event_assessment.SYSTEM_PROMPT` with user template `{{input}}`. Its parameters
must contain only `temperature: 0`, `max_tokens: 1024` (allowed 512-1536), and
`max_input_chars: 12000`. Create it through the authenticated AI configuration
API, not by modifying existing stage profiles. Prepared profile
`24a0096b-57f0-5493-a1d4-bb5f41f3d216` uses prompt
`98fb4df1-f96e-5115-840c-6774b97368d9`; neither is routed to an existing stage.
The production assessment flag is on for operator-triggered reviews only.
Changing enablement requires a safely drained worker
restart because model-job classification is initialized at process startup.

Input is capped at 12,000 bytes including system instructions, with at most 12
exact source passages selected round-robin. Output is capped at 12,000 bytes and
must cover every supplied ID exactly once using constrained decision/reason
pairs. Unknown IDs, incomplete output, stale packet versions, and extra fields
fail closed. Coverage omissions remain explicit; an empty packet makes no call.

Suggestions and request fingerprints are saved alongside immutable private
evidence and shown as unverified annotations. Every reading-view choice remains
on hold. Structural validation is not semantic validation, incident approval, or
permission to publish. Real-model latency and multi-incident quality evaluation
are still required before enabling automatic event/report decisions.

### Assessment reuse and router correction (deployed, real pilot pending)

The worker unwraps the actual router result's `parsed` field before
assessment validation and requires `schema_valid: true`. A test exercises the
real router/parsing/envelope path with only provider transport faked; earlier
router mocks missed this contract defect. The pending production pilot has not
run yet. Profiles with a schema are refused to prevent automatic repair calls.
One profile invocation still inherits the existing HTTP transport retry policy;
it is not an independent overall wall-clock deadline.

Validated assessments can be reused for an identical bounded evidence/request
fingerprint and guarded profile/model/provider/prompt configuration fingerprint.
The worker rechecks configuration after inference before caching. Invalid cache
entries fail closed; unversioned callbacks do not cache. Files are private,
bounded, immutable and installed atomically through directory descriptors; symlink
and nonregular entries are refused. Cache hits never change review decisions or
publication eligibility and are exposed as `model_cache_hit` in job metadata.

This identity covers database-visible configuration, not a live attestation of
server model weights. If weights or server-side generation defaults change behind
an unchanged model name/endpoint, revise the dedicated profile through the AI API
to advance its version timestamp before reusing results. Cache coverage is the
bounded evidence packet, not omitted source text. Cache is an optimization for
unverified suggestions, never evidence that a report is safe to publish.

### Suggested reading and timing (deployed, real pilot pending)

Assessed reviews gain a separate expandable Suggested reading section. It is
assembled deterministically from model-included, exact source passages, retaining
source links, character spans, feed-date caveats and coverage counts. Held and
excluded passages remain in the evidence review. This section does not set human
Include choices, confirm incident identity, publish a report, or make another model
call. An empty model selection explicitly produces no incident account.

Private profile invocations record job/provider/model attribution, input/output
character counts and elapsed milliseconds in the existing `llm_runs` table, on
both success and error. No new schema or admin-side inference is involved.
Errors store only the exception type, not source text or provider response bodies.
Cache hits and extractive jobs make no model call and add no inference row.
Success here means generation/envelope handling succeeded; subsequent assessment
validation or factual review can still fail. These records are already available
through the admin LLM-runs API. Older CVE and other job types are not all metered,
so this table must not be described as complete platform inference usage. A hard
process kill may also leave missing telemetry. Automatic budget admission must
account for these gaps before it is enabled.

A **distinct job type** is essential: an older worker could ignore a private-mode
payload on `event_report_llm` and publish a normal report. Old workers do not know
this new type, so a staged request remains queued rather than falling through to
public report generation. Normal `event_report_llm` handling is untouched.

Admission uses a transaction-scoped advisory lock around pending-job checks and
insertion. Equivalent queued/running requests return the same job ID; concurrent
requests were verified with PostgreSQL. The endpoint caps pending private reviews
at ten. Completed requests can be rerun to capture new inputs; unchanged packets
reuse their immutable artifacts. This is operator-triggered admission, not an
automatic evidence-change scheduler or a model proposal/verification loop.

Admin only enqueues; it does not snapshot evidence or render the review. Worker
reads use a separate bounded read-only transaction. Job results contain the event
ID, packet version, relative artifact path, coverage counts, and
`public_eligible: false`, not source bodies or a public download URL.

## Configuration and storage

- `SV_EVENT_REVIEW_ENABLED=0` by default. Both admission and worker execution check
  it. Only exact `0`/`1` values are accepted.
- `SV_EVENT_REVIEW_DIR=/log/event-reviews` by default. It must be an absolute
  subdirectory of `SV_LOG_DIR` (default `/log`). Resolved paths below the site,
  site-source, or data roots are refused. Request bodies cannot choose paths.
- Set environment-specific values in k8s-platform, not application defaults.
- Confirm the selected log volume is private, writable by the worker, persistent,
  and absent from web serving before enabling. Do not expose raw log/artifact paths.
- Model calls require the separate model flag and guarded profile. No automatic
  catch-up, schema migration, or public publication.
- Stopping admission preserves queued jobs/artifacts. A disabled worker returns
  a skipped result if a private review was already queued; requeue explicitly when
  enabled again rather than assuming disabled work will resume automatically.

Event Detail now has a private-review queue control with explicit aliases and
availability feedback. Completed private jobs expose an authenticated attachment
download at `/admin/api/jobs/{job_id}/private-review`. Requests cannot specify
filesystem paths. Reads reject wrong job types/states, symlinked artifact folders
or files, nonregular files, oversized files, malformed paths, and changed HTML
bytes. Private evidence fails closed when the admin token is not configured.
Responses are non-cacheable sandboxed attachments, never inline admin HTML.
Downloads remain available when admission is disabled. They are historical
snapshots, not a claim that current evidence still matches or publication is safe.
Do not link `/log` directly into the website.

## Dashboard corrections

The dashboard previously used incomplete fixed lists, and its JavaScript explicitly
hid `build_daily_brief`. The Jobs filter maintained a different incomplete list.

The updated behavior uses the queue registry, worker registry, and observed job
types. Control and uncategorized jobs appear under **Control / Other Jobs**;
private reviews appear under the existing LLM-worker group because that runner
executes them, not because they invoke a model. All standard terminal states,
including canceled, have visible counts. Canceled counts are explicitly labeled
all-time; existing failed/completed counters retain their configured reset window.
Job names link to the Jobs page with a
type filter. Historical names are escaped before HTML insertion.

The Jobs page requests the complete type catalogue once on initial loading using
`/jobs?include_types=true`, not on every polling refresh. The normal Jobs response
retains its existing shape unless the optional catalogue is requested. Backend
historical types come from `SELECT DISTINCT job_type`; no new full-table source
content or per-job detail retrieval is added to the dashboard.

## Acceptance and release checkpoint

- 371 offline Python tests pass, including worker dispatch, no-publication/no-model
  guards, admission limits, path restrictions, registry/group coverage, and API gates.
- Six disposable PostgreSQL tests pass, including two concurrent submissions
  becoming one job and real private artifact generation with unchanged event data.
- Ten JavaScript unit tests pass, including daily-brief/unknown/control visibility,
  canceled counts, and escaped filtered links. Admin JavaScript syntax check passes.
- User gave initial positive visual feedback on the private review pages. This
  does not validate their facts or the newly changed dashboard in a live browser.

The first three model jobs failed closed with `invalid_assessment_values`:
the model returned slash-joined decisions and prose reasons instead of the exact
two-field codes. They used 32,994 ms in total and produced no review artifacts.
All three event-row `md5(to_jsonb(e)::text)` fingerprints remained unchanged.
The Commission response also confused separate incidents; structural compliance
alone must not unlock publication.

Assessment v2 replaces ambiguous slash shorthand with exact JSON field examples,
clarifies passage-level relevance and rejects using the first source as incident
identity. The validator is unchanged: no coercion of invalid model output, extra
repair call or public permission. Request/workflow versions invalidate old caches.
437 offline tests passed for the runtime correction, including the observed
invalid-output pattern. V2 worker/profile deployed; the bounded new cohort is
listed in `STABILIZATION_VERIFICATION.md`. Do not duplicate its queued jobs.
The offline quality checker and pinned cases are described in
`EVENTS_CURATED_EVALUATION.md`. These tests do not establish model quality.
Evaluate the Commission counterexample and other real incidents before automated
evidence admission or public reporting. Private success is not the final goal.

V2 evaluation completed: two incomplete responses, one private attachment whose
Commission incident-separation checks failed. See the verification notes for IDs,
timing and artifact integrity. V3 preparation keeps one bounded call and exact
validation, makes the requested incident/required IDs explicit, and stops sending
the quote twice inside context. No public eligibility or safety gate is relaxed.
V3 is now deployed with three low-priority evaluation jobs; exact release/job IDs
are in `STABILIZATION_VERIFICATION.md`. No automatic admission or public reporting
is enabled. Evaluate those jobs before expanding the cohort.

V3 jobs now completed: all pass response structure, but two of eight provisional
quality cases still fail. All artifacts remain private; exact job/hash/timing data
are in the verification notes. Do not publish these assessments or continue
unbounded prompt tweaks. Next: `EVENTS_INCIDENT_SCOPING.md`. A single queued v3
reuse check remains; do not duplicate it or invalidate its configuration.

Scope-aware review admission and worker execution are deployed behind a separate
flag/profile, disabled in chart defaults and enabled for the three-job private
pilot. See `EVENTS_INCIDENT_SCOPING.md`. The default v3 path and its hashes are
unchanged. Scoped jobs are queued; semantic acceptance and public automation are
not claimed.

## Coverage display follow-up (local, not deployed)

New private model-job results include a bounded `assessment_summary`: workflow,
optional scope version, assessed/not-assessed candidate counts, and include/hold/
exclude suggestion counts. Its status is always `proposal_only`. No source text
or claim approval is added to job metadata. Extractive results retain their shape.

The Jobs table explains that a completed private draft is not publication approval,
shows coverage and suggestion counts, distinguishes unqualified source anchoring,
and identifies cache reuse. Older results explicitly lack detailed coverage rather
than displaying fabricated zeros. Counts must be nonnegative integers within the
48-passage bound and reconcile before display; unknown text is not interpolated.
The existing authenticated private attachment link is unchanged.

Five new JS tests cover coverage, historical results, invalid/untrusted counts,
ineligible job states and extractive behavior. All 19 JS tests and 529 offline
Python tests pass, with worker assertions for both scoped and unscoped metadata.
No browser acceptance or production UI rollout is claimed yet.
