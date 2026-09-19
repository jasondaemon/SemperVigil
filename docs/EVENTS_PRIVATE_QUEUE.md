# Private Events jobs and admin visibility

Status: admin and LLM worker deployed at `ee91658`, **admission disabled**.
The new API exposes 35 job types and rejects private requests with 503 while
disabled. Live dashboard verification exposed pre-existing slow backlog queries
and overlapping ten-second refreshes. A single-request browser guard is tested;
its admin-only follow-up rollout and final visual verification are pending.

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
the tested extractive review service. This job does not call an LLM or alter
public event reports. It is not in the set of jobs that require model admission.
No runner concurrency, model configuration, build process, or publish behavior
changes are introduced.

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
- No model calls, automatic catch-up, schema migration, or public publication.
- Stopping admission preserves queued jobs/artifacts. A disabled worker returns
  a skipped result if a private review was already queued; requeue explicitly when
  enabled again rather than assuming disabled work will resume automatically.

Artifacts currently require authorized operator retrieval. A dedicated admin
artifact viewer/download route is not implemented; the existing Jobs detail view
shows the bounded result metadata. Do not link `/log` directly into the website.

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

Next elevation: inspect/render the admin and LLM-worker deployment changes, drain
affected runtime work safely, roll out with review admission **disabled**, and
verify the dashboard/catalogue and existing job flow. Only then enable a bounded
private-review request and verify the completed result/artifact. Keep the builder,
web deployment, daily JSON, and unrelated infrastructure unchanged. Do not enable
autonomous model review or publication as part of that rollout.
