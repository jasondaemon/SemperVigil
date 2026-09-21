# Stabilization verification

## September 19: Qwen 3.5 production transport

The 9.7B Qwen 3.5 Q4_K_M weights load completely on the RTX 3060 at 16K context,
using about 8.54 GiB VRAM and leaving about 3.75 GiB free. A generic gateway probe
completed, then read-only tests exercised the seven active local stages against a
current full article and CVE. Summary, context, threat-actor and event contracts
were usable; article product extraction was production-parseable but included
schema-extraneous evidence fields. CVE product extraction failed because the
installed LiteLLM Ollama adapter misplaced `think=false`; this is a transport
failure, not a prompt or model-content failure. The same CVE through Ollama's
native chat API with top-level `think=false` returned valid product JSON in 4.43s.

Added an explicit native Ollama transport with deterministic non-thinking mode,
JSON/schema mapping, profile-schema forwarding, token telemetry and private-review
support. OpenAI and existing LiteLLM behavior are unchanged. Targeted
router/event/private-review tests pass; the full offline suite passes 982 with two skips. The initial deployed candidate run passed six stages;
article-product extraction exposed schema extras before profile-schema forwarding
was added. A second run proved the original active prompt itself explicitly asks
for forbidden confidence fields in both prompt templates. Migrations 037 and 038
align those templates with the existing two-field schema.

The final deployed seven-stage suite passed: summary 12.97s, context 37.32s,
article products 3.11s, article actors 1.44s, event derivation 3.84s, CVE products
1.21s, and CVE actors 0.56s. Eighteen Qwen-backed profiles were switched atomically
to Ollama native Qwen 3.5; zero still reference Qwen 2.5. The final
`qwen3.5:9b-q4_K_M-16k` alias reuses the same weights and restores the intended
16,384-token context. Two normal production jobs succeeded after cutover, and the
worker remains serialized at one replica. Qwen 2.5 is retained only for rollback.

## September 19, 20:23 UTC: passage-bound private article trial

Application `e2c7abf`, platform `c0d9ec8`. Admin and the local LLM worker use the
same immutable image so private admission and execution share workflow v4. Other
workload images, model profile, concurrency, resources, normal prompts, builder,
web and feed behavior are unchanged. Rendered admin/worker manifests match live;
all workloads are Ready with no new restarts; Kubernetes readiness passes.

The full offline suite passes 1,003 tests with one skip. No database integration
suite was run because no disposable database was configured; queue/storage code
was unchanged. The three saved article requests fit the existing byte budget.
Private job `job_eedd71ca19d34ebd85e09fca7c3d3744` completed six serial calls in
65.828 seconds with no retries. Exact passage references validated for all three
contexts, but the semantic gate failed on date role, coverage, allegation typing
and one summary fact reference. Nothing was promoted or retried.

The frozen article title, stored text, summary, context and generation timestamps
are unchanged. Nineteen sampled public HTTP/markup/asset/JSON checks pass; the
feed index contains 5,060 days and today's sampled download contains 17 articles
and 24 CVEs. The shared LLM queue is empty. No Hugo/site build ran. Automatic
evidence admission remains disabled; five shared experiment attempts remain and
are not authorized for prompt iteration.

## September 19, 18:32 UTC: private two-phase audit

LLM worker `d80f98a`, platform `3200453`; admin `eff906d` unchanged. Only worker
image and pinned support profile changed. Orchestrator restored and ready after
draining the old worker. Rendered admin/worker/config/orchestrator matches live.
863 offline tests pass, one Linux-only skip. No DB/JS suite rerun for this slice.
V4 real semantic acceptance fails (12/17 original, 4/5 fresh holdout); no generated
narrative promoted. Three cache replays succeeded with zero inference calls.
Public pointer remains `c44cc56060cba571615cc697fc2d4930edacacdb94bbedf92104d9ce46a90252`.
All 20 publication checks passed at 18:32:26 UTC: 5,060 indexed archive days,
today 17 articles/24 CVEs, Aug 15 seven articles/553 CVEs, earliest sample one CVE.
Metrics generated 18:17; feed index 18:20. These are HTTP/markup/assets/sampled
JSON checks, not browser execution or exhaustive DB completeness proof.
No Hugo/build/feed/schema/resource/concurrency changes. See EVENT_CLAIM_SUPPORT.md
for exact jobs, latency, and unresolved quality failures.

## First implementation slice (2026-09-18)

This slice changes tests, documentation, and an operator-invoked measurement tool.
It does not modify application behavior, public JSON, templates, model settings,
worker concurrency, build commands, or deployment values.

## Offline regression tests

Run from the application repository:

```sh
python3 -m pytest -m offline --strict-markers
```

The first explicitly classified offline modules cover incremental feed exports,
article/CVE serialization, archive paths, atomic file writes, and event qualifiers.
These modules live in tests/offline. Other modules are excluded from default
collection until explicitly reviewed; this is not a passing integration result.
Broader collection also exposed existing syntax/stale-import problems and missing
local admin-test dependencies. These remain separate follow-up work.

Offline tests remove SV_DB_URL and reject psycopg connections, including when a
developer shell has a database URL configured. Use only a disposable database for
the integration suite, which can initialize schema and change application data:

```sh
SV_TEST_DB_URL=postgresql://localhost/sempervigil_test python3 -m pytest --run-db-tests
```

An ambient SV_DB_URL alone never enables the integration suite. Do not supply a
production URL as SV_TEST_DB_URL. No production database is used for test fixtures.

The initial checks establish these behaviors:

- Unchanged archive day files retain their bytes and modification times.
- Changed article/CVE counts or update timestamps regenerate only that day.
- Missing-only backfill restores missing history without regenerating other days.
- Shared historical JSON is not written under Hugo's data or static directories.
- A serialization failure preserves the affected published day and its manifest.
- Article identity, source URL, summary/bullets, and relationship facets survive.
- CVE scores retain their numeric units; missing EPSS and zero EPSS stay distinct.
- CVE links remain NVD links; title parts, KEV, and preferred CVSS are retained.
- Daily counts, descending publication order, CVE deduplication, and JSON encoding
  match the current export behavior.

These tests do not prove database selection completeness, freshness propagation
from every enrichment table, recent/history serializer parity, multi-file snapshot
consistency, or production release activation. Those require subsequent checks.

## Read-only baseline collection

```sh
python3 tools/collect-baseline.py --hours 72
```

The tool uses existing cluster access to run bounded, read-only aggregate queries.
It connects directly with psycopg; it never calls application connect_db/init_db,
which can invoke migrations. It exports queue ages, stage completion timings,
inference timings/linkage, invalid historical timestamp counts, and ten recent
build records. It requires PostgreSQL 16+ for safe timestamp validation. Invalid
timestamps are reported and excluded from duration calculations, never repaired
by this tool. It does not export
credentials, prompts, article contents, or raw job results/errors.

Run on demand and compare observations across at least 72 hours. A single query
for 72 hours is not a 72-hour benchmark if records cover a shorter period. No
scheduler or background collector is installed by this slice.

Before admitting event pilot work, supplement these aggregates with bounded
GPU/RAM observations, shared AI workload activity, stage arrival rates, retries,
build-phase timings, and the public JSON/database comparison below.

## Public JSON and availability release gates

Treat downloaded JSON as an external API. Preserve existing paths, field types,
identifiers, score units, null semantics, date semantics, and counts. Keep news and
CVEs together in daily exports even when the website displays separate tabs.
Additive fields require compatibility checks; incompatible changes require an
explicit versioned contract and downstream transition plan.

For each future production release:

1. Record the current release and baseline build duration/resource measurements.
2. Validate representative recent and historical JSON against expected database
   eligibility, including suppressed items, timezone boundaries, and enrichments.
3. Verify JSON advertised by the feed index exists and parses. During a build,
   sample homepage and feed endpoints to detect transient errors or partial files.
4. Verify unchanged historical exports remain byte-identical and outside Hugo's
   loaded data path. Measure output generation and Hugo work separately.
5. Trigger builds only through the supported platform API. Confirm successful
   activation before treating a build as published; retain rollback capability.
6. Verify homepage, daily navigation, downloads, search, metrics, and Events after
   activation, with a concrete rollback if availability or correctness regresses.

Prioritized investigations discovered during this slice:

- Historical day payload generation currently requests at most 500 CVEs. Compare
  eligible daily counts with exports before deciding whether the limit loses data.
- Inspect all enrichment updates against day-signature invalidation; aggregate
  count/timestamp checks alone are not proof that every relationship change is seen.
- Recent and historical exports contain separate serialization logic. Establish
  parity tests before refactoring or extending either path.
- The build script activates with ln -sfn. Do not assume the symlink replacement
  and externally shared feed updates form one atomic transaction. Inspect and test
  the actual activation and reader behavior before any publish-path change.

No discovered concern in this list has been silently changed or declared fixed.

## Initial measurement

At 2026-09-18 15:14 UTC, the 72-hour query window contained 44 successful build
records with valid timestamps: mean 21.70 seconds and p95 27.47 seconds. These are
whole build-job durations, not isolated Hugo timings. One build was running.

There were 122 successful inference records covering approximately 44 minutes,
with mean 8.88 seconds and p95 21.67 seconds. None joined to a job record, so
inference-stage attribution remains a follow-up. One local LLM job was running.
Historical timestamp validation found 242 malformed job finish timestamps; these
were excluded from time-window calculations. No historical data was modified.

The public homepage responded successfully. The feed index listed 5,059 dates.
Sampled daily exports parsed, matched their declared counts, and used NVD CVE links:

| Day | Articles | CVEs |
| --- | --- | --- |
| 2026-09-18 | 48 | 61 |
| 2017-10-09 | 2 | 0 |
| 1990-05-01 | 0 | 1 |

Old publication dates are not evidence of site operating age. This sample checks
public consistency only; it does not prove completeness against the database or
all 5,059 indexed dates. No extra build was requested for this verification.

## Follow-up: export parity and write interruption

The local HTML tracker is [upgrade-tracker.html](upgrade-tracker.html). Update it
with every implementation slice, distinguishing local validation from deployment.

Representative recent and historical serializer output matches after accounting
for the existing historical article id alias. A new regression reproduces loss of
previously exported records when recent-window results replace a complete day;
this remains a strict expected failure until the completeness correction lands.

Fault injection also reproduced partial public JSON after a recent-day write
failure. A one-line local correction now uses the existing atomic JSON helper,
with identical serialization and payload semantics. This change is tested locally
and not yet deployed. It does not fix the limited-window overwrite.

Read-only inventory on September 18 found 1,031 CVEs dated September 8 and 982
dated September 15 in the database; public downloads contained 10 and 5 CVEs
respectively. Recent selection limits and day rewrites are confirmed code paths;
the full attribution and recovery scope for the existing files remain pending.

## Complete-day correction (local, not deployed)

The previously expected overwrite failure now passes. Recent results identify
days to inspect; the archive serializer reads complete days, honors suppression,
and requests CVEs with `LIMIT NULL` rather than 500. Default UI query limits are
unchanged. The public envelope and existing fields remain; recent articles gain
the archive's existing `id` alias, equal to `article_id`.

Internal manifest schema 3 invalidates affected cached exports. Ordinary refresh
is scoped to selected recent days, so this version bump does not rebuild the whole
archive on every publish. Tests verify unchanged bytes/mtime, unrelated historical
file retention, interrupted-write safety, and all 1,031 CVEs in a large fixture.
Result: **32 passed**, offline only. No Hugo invocation or production writes.

Read-only production selection measurements on September 18:

| Query | Records | Seconds |
| --- | ---: | ---: |
| Feed-day inventory | 5,084 days | 0.403 |
| Uncapped September 8 CVEs | 1,031 unique IDs | 0.143 |
| Uncapped September 15 CVEs | 982 unique IDs | 0.064 |

Measured using a direct PostgreSQL read-only connection with a 10-second statement
timeout, without application initialization/migrations. These measurements do
not include per-record enrichment serialization, file writes, or Hugo.

Release gates remain: reconcile recent local-time bucketing with stored article
brief days and CVE database dates; verify missing-brief-day selection, CVE-only
operation, and enrichment invalidation (including changes outside the recent
window); measure full export cost; then deploy and repair historical omissions
through supported jobs/API. This is not a claim that production data is repaired.

## Date-selection and freshness audit (September 18)

Local corrections now preserve stored article `brief_day` in recent selection,
include CVE database dates even when their displayed local date is the preceding
day, and continue CVE exports when no recent articles exist. Public date semantics
are not migrated: article archive membership still uses stored brief days and CVE
membership still uses database dates. Tests cover a midnight UTC CVE displayed in
New York, an article whose brief day differs from its timestamp date, CVE-only
operation, empty input, and the additional internal query field.

**37 offline tests pass. Production is unchanged.**

Read-only, in-memory production serialization (no files written, no build):

| Day | Articles | CVEs | JSON bytes | Seconds |
| --- | ---: | ---: | ---: | ---: |
| September 8 | 105 | 1,031 | 5,220,818 | 0.550 |
| September 15 | 36 | 982 | 3,009,428 | 0.362 |

This used the deployed serializer with its CVE query uncapped only in the isolated
inspection process, UTC display formatting, and a read-only database connection.
It includes enrichment lookups and JSON encoding, not disk I/O or publishing. It
is not an end-to-end test of the unshipped branch or a sustained performance test.

Freshness is a confirmed release blocker, not just a hypothetical gap. Counts of
records whose enrichment check timestamp exceeds their base `updated_at`:

| Dataset | Check | Records |
| --- | --- | ---: |
| CVEs | EPSS | 50,540 |
| CVEs | Products | 16,785 |
| CVEs | Threat actors | 22,402 |
| CVEs | KEV | 19,817 |
| Articles | Products | 14,691 |
| Articles | Threat actors | 16,007 |
| Articles | Events | 15,135 |

These counts indicate unreliable base-timestamp invalidation, not proven stale
public payloads for every record. Source review confirms the checked-at setters
do not update the base timestamp. A check timestamp alone also does not prove the
rendered content changed. Two of 35,187 articles have missing/empty brief days;
their fallback selection must be reconciled with archive inventory.

Before release, inventory every exported field's dependencies (including linked
products, actors, events, KEV and source labels); test updates and removals outside
the recent window. Do not merely force all historical files to regenerate or
claim that adding checked-at timestamps handles all dependency changes. Compare a
content-based day signature with transactional dirty-day tracking, measure the
chosen mechanism, and preserve unchanged file bytes/mtime. Then repair historical
omissions in a bounded job and validate publication through the API.

## Dependency-aware refresh implemented locally

`feed_inventory.py` now computes per-day content fingerprints in PostgreSQL.
It covers exported base fields, products/vendors/versions, actors and aliases,
event keys, tags, sources/icon availability, and KEV membership/due dates. Counts
and signatures include removals; changes do not depend on base update timestamps.
Missing article brief dates use the existing application-timezone fallback. Empty
brief dates are also normalized in the corresponding article selection query.

The archive now consumes this inventory, includes the rendering timezone and
serializer version in its manifest state, and checks historical dates outside the
recent selection. Normal refresh repairs at most 25 background days, with a soft
five-second budget checked between days. Deferred dates retain their previous
files and manifest state; subsequent refreshes resume. The explicit archive task
still supports full catch-up. Unchanged payload files retain bytes and mtime.

Verification: **55 offline tests passed**. Additional read-only PostgreSQL CTE
simulations verified 12 cases: EPSS, KEV deadline, vendor name, actor alias, source
label, event key, bookkeeping-only base timestamp, and removals of article/CVE
product links, aliases, KEV membership, and event links. Fixtures shadowed tables
within SELECT statements; no production rows/files were changed.

Full database fingerprint scan: 5,084 dates, 35,187 articles, 50,673 CVEs. Initial
run 4.173s; final query including icon availability 2.683s. These two observations
are not a sustained latency distribution. No complete historical JSON dataset was
loaded into Python or Hugo, and no LLM job was invoked.

Release remains pending: verify coordinated writer rollout and shared-file
concurrency, build through the platform API, compare published counts and fields,
measure complete build duration and memory, and observe resumable historical
repair. Production still runs its prior images. The freshness correction is
implemented and tested, not yet a production recovery claim.

## Guarded production rollout completed (September 18)

This section supersedes the earlier not-deployed status. Runtime source revision
`889b2de` is deployed to build, fetch/public-fetch, LLM/OpenAI, and orchestrator
workers. Chart revision `7eb3d14` exposes the background-repair control; production
sets it to **0**. Admin, web, database, Hugo commands/caches, memory limits, and
single-LLM-job policy were not changed.

Release images reused the verified production dependency layers through the
source-only overlay recipe. Live source comparison found no runtime source drift
against the old revision, only packaging metadata/OS metadata. Rendered server-side
diffs contained image replacements and the single environment setting only.
Cross-node filesystem locking passed before rollout. Job admission was paused and
workers drained before replacement, then resumed. New runtime source hashes and
the guarded setting match the repository on all six application roles.

- Offline suite: **58 passed**; two Helm rendering cases confirm limit 0 is carried
  into the runtime ConfigMap. A values-only addition would have been ignored.
- API build `job_a9f9a8490ce9429dba15174198ea357b`: succeeded in **42.725s**;
  Hugo reported **2.130s**. First pass regenerated 38 dates.
- Next normal build `job_58014e4ea3b54c01988873f3d55a14ce`: succeeded in **24.125s**;
  one changed date regenerated, 37 skipped. Compare cautiously to the prior
  44-job mean of 21.70s: these are two new observations, not a new distribution.
- Peak container memory during first validation: **448,327,680 bytes (428 MiB)**,
  below the unchanged 16 GiB limit.
- All **5,059** archive files retained. After first build, **5,021** retained both
  byte hash and mtime. No historical background repair ran.
- Public September 8: 105 articles / **1,031 CVEs**; September 15: 36 articles /
  **982 CVEs**. Article/CVE ID sets exactly matched read-only database selection.
- Historical samples October 9, 2017 and May 1, 1990 remained accessible.
- Homepage, metrics, search, and sampled downloads returned HTTP 200; declared
  counts and NVD-only CVE links passed. **85** cache-busted homepage samples over
  about three minutes had no failures (maximum observed request 0.251s).
- No new kernel OOM events on the build host; nodes reported no memory pressure;
  Kubernetes API and etcd readiness passed. Updated pods had zero restarts.

Operational error during preparation: the temporary rollback snapshot was first
placed under `/data`, which Hugo consumes. A scheduled old-version build failed
on its text metadata file. The prior release remained live. The snapshot was moved
to `/log/release-snapshots/889b2de`, the `/data` staging directory removed, and both
subsequent builds passed. This was an operator staging mistake, not a new-code
failure; do not put rollback artifacts under Hugo data inputs again. No database
backup or recurring backup task was added. Initial Python HTTP monitoring also
encountered a local CA-store error; the reported availability samples use curl's
working certificate validation, not disabled TLS validation.

Retain previous images and the rollback snapshot. The next step is an observation
window, then a small historical batch; full-backlog completeness is not claimed.

## Read-only release gate (2026-09-18)

Added `tools/check-publication.py` and `docs/PUBLICATION_CHECKS.md` without
changing production images, configuration, scheduling, or application code.
The previous `tools/hugo-smoke.sh` is unchanged and was not invoked.

Live checks at 16:33 UTC passed: homepage, search, metrics, Events index, one
linked event, 11 local CSS/JS assets, the 5,059-day index, and four downloads.
September 18 contained 63 articles / 132 CVEs; September 8 contained 105 / 1,031;
October 9, 2017 contained 2 / 0; May 1, 1990 contained 0 / 1. These are sampled
contract checks, not a repeat DB-completeness audit. Feed generation time was
16:33:15 UTC. Another normal production build had succeeded at 16:30:29 UTC.

Offline suite: 84 passed, including 26 new checks for failures, malformed indices,
incorrect units/counts/dates/links, and missing assets. Browser behavior, chart
data freshness, integration tests, and a sustained observation window remain
pending. Historical catch-up stays disabled during the observation window.

## Summary-job attribution (local only, 2026-09-18)

The summary and article-context handlers each passed `job_id=None` to
`insert_llm_run` in both success and exception paths. All four now pass `job.id`.
No schema migration, prompt, provider selection, lease, output, or scheduling
change. Tests execute both handlers with mocked inference/storage and assert
job association, unchanged invocation, failure propagation, and lease release.
Offline suite: **88 passed**. No production deployment in this slice; runtime
remains `889b2de`. No attempt to guess attribution for historical records.

The collector now reports accounting limitations explicitly: only summary/context
handlers and admin probes persist these records; Events/enrichment are not fully
represented. Handler latency can include retries/parse work, primary model IDs
do not necessarily identify the fallback used, and downstream failures can produce
a second error row after success. Do not sum these rows as total inference/GPU
utilization or set an Events capacity budget from them alone.

Next release acceptance: deploy through the documented image/worker rollout,
observe newly completed summary and context jobs, and confirm both successful
and naturally occurring failed records join to `jobs`. Do not induce production
failures solely to test error telemetry. Re-run public release checks and compare
build/queue latency; use offline tests for controlled failure paths. Full
provider-attempt accounting remains a separate, larger instrumentation change.

## Attribution rollout verified (2026-09-18)

Supersedes the local-only release state above. Ingest image `27b9fb3` deployed to
orchestrator, fetch (two replicas), public-fetch, LLM, and OpenAI workers. Builder
remains `889b2de`; admin, web, database, prompts, models, and concurrency unchanged.
Background historical repair remains zero. Source-only overlay reused the
verified `889b2de` dependency image. OCI manifest:
`sha256:f24f99e2fdc0fa8e430919bb8278cd9aaafa913c3b0050d5840259c0f0ecd937`.

- 88 offline tests passed. Rendered server-side diff was ten image substitutions
  across five Deployments (main/init containers), no other spec changes.
- Same image imported on all four schedulable nodes. All six new runtime pods'
  worker source hashes matched the repository. Deployment specs match the rendered
  platform configuration; all workloads ready with new pods at zero restarts.
- New summary records at 16:47:48 and 16:48:43 UTC joined to succeeded jobs
  (11.994s and 6.024s). Context record at 16:48:21 joined to a succeeded job
  (23.963s). No production failure deliberately induced; failure paths tested offline.
- API request found an already queued build, `job_9e4a5d2e9b164ce894c6742fbcaa7fed`;
  it succeeded in 28.309s. Hugo was not invoked manually.
- 21 read-only public checks passed before and after replacement; Kubernetes API
  and etcd readiness passed. These point checks do not claim continuous monitoring.

Rollout issue: an empty-queue snapshot was taken before the old orchestrator had
fully terminated. It admitted another LLM launch during shutdown, so stopping the
old LLM pod interrupted context job `job_c66c63d35410440a87f808d05aedb69d` and left
its launch record marked running. After confirming that pod no longer existed,
the exact abandoned launch/task records were canceled via the admin API, and the
task was rerun with its original payload as `job_677f3b9b03234135b0105c7f94f65d82`.
That retry was queued at verification time; no task was silently discarded and
no direct DB writes were used. New inference continued with one active LLM job.
The rollout documentation now requires a second queue check after orchestrator
termination. Do not describe this as an entirely interruption-free worker rollout.

Rollback: retain the prior image, restore the platform ingest tag to `889b2de`,
render, compare, and apply only the affected Deployments after the corrected drain.
No schema or data rollback is needed for this telemetry-only change.

## Builder-only bounded historical repair (2026-09-18)

Added optional Helm `buildWorker.feedArchiveBackgroundDays` (default null,
inherits shared env; invalid/negative/fractional values fail rendering).
Production sets it to `"1"`; shared `SV_FEED_ARCHIVE_BACKGROUND_DAYS` stays `"0"`.
This activates the existing bounded exporter only on the builder. No app image,
Hugo command, resource, cache, schema, or LLM-concurrency changes. Offline suite:
**96 passed**, including eight override/default/scoping/validation tests.

Server-side dry run confirmed the only builder spec change was the explicit env
override. Paused admission, waited for orchestrator deletion, then rechecked and
waited for build/launch jobs to drain before stopping the builder. Resumed admission
after the new builder was ready. Other workers/web/admin were not restarted.
Live builder spec matches rendered configuration, builder env is 1, sampled
fetch/LLM envs remain 0, and all Deployments are ready.

- API build `job_dcafedc6697143949dca92f1890bc12e`: **35.232s**, succeeded.
  Export refreshed two dates (one foreground, one historical), skipped 40,
  removed zero; deferred 5,055.
- August 15 previously exposed 7 articles and zero CVEs. Repaired output exposes
  7 articles and **553 CVEs**; exact article/CVE ID sets match read-only canonical
  DB selection. Public download hash matches verified archive bytes.
- Before/after inventory retained all **5,059** files; **5,057** retained both
  hashes and mtimes. Only August 15 and September 17 changed. One new historical
  fingerprint was recorded. Evidence snapshots generated under local `/tmp`, not
  production Hugo input paths.
- Next automatic build `job_c4d3c73ba1944fe4b41a4f79dbacbb50`: **33.217s**, succeeded;
  deferred reduced to 5,054. This establishes progress, not a completion ETA.
- First validation peak: **410,861,568 bytes (392 MiB)**; cgroup OOM/max events zero.
- All **21** public checks passed, including repaired and older downloads. No
  continuous outage monitor or full-backlog completeness claim is made.

Recent pre-change build samples were 23-25s; catch-up has measurable added work.
Keep the one-date limit until a larger observation window supports changing it.
The time budget is checked between days, so one expensive date may exceed five
seconds. Background refresh can repair or remove dates according to current DB
state; it does not rewrite every file or push history back into Hugo input.
Rollback: set the builder override to 0, render/compare, drain the build runner,
and apply only its Deployment. Preserve correctly regenerated JSON.

## Desktop interactions and metrics freshness (2026-09-18)

Read-only production follow-up: sampled builds succeeded in 23.587s, 23.865s,
and 31.235s. Consecutive sampled export logs decreased deferred history from
5,050 to 5,047; new enrichment can also dirty previously repaired dates, so this
is not a fixed completion ETA. The interrupted context task rerun
`job_677f3b9b03234135b0105c7f94f65d82` succeeded at 18:25:11 UTC.

In-app desktop browser acceptance:

- Homepage initially selected News and rendered articles; CVE selection replaced
  the feed with CVE cards linking to NVD.
- Settings menu exposed Summary/Bullets and High/Critical controls. Stored
  preferences were not changed; their behavior remains a separate gate.
- Calendar selection of August 15 rendered the repaired CVE feed including
  CVSS/EPSS pills. Switching to News rendered that day's articles. Prev Day moved
  to August 14 and updated content and date.
- Searching `CVE-2026-74767` completed across 90 days, returning one Pandora result
  dated August 15 and linking to NVD, with CVSS 8.7 and EPSS 0.003.
- Metrics screenshot showed populated colored bars and a full-width sources table.
  Its visible export timestamp was September 18 at 20:17:01 UTC.

The operator release checker now independently rejects missing, stale, naive, or
future metrics timestamps. Default tolerance is three hours for hourly metrics;
feed index retains its separate 24-hour default. Seven regression cases cover
timezone offsets, threshold boundaries, missing values, and stale metrics with a
fresh feed. **103 offline tests passed**; live public check passed all 21 checks.
No runtime deployment, scheduling, model, or public JSON changes in this slice.

Observed content-quality finding: some newly ingested CVEs still have severity-only
titles. Browser rendering alone cannot distinguish absent enrichment from serializer
loss; retain this for a targeted data-path investigation, not a speculative UI fix.
Mobile, keyboard-only, stored preference execution, and automated browser coverage
are not claimed complete.

## Isolated Events evidence foundation (2026-09-18)

Inspected current derivation and report handlers: organization-plus-kind keys
remain active and report assembly still consumes article summaries/context and
promoted web sources. No changes to either path in this slice.

Added `event_evidence.py`, a pure module with frozen evidence/citation/claim
records, deterministic snapshot hashes, and sorted validation errors. It checks
trusted incident binding, exact Unicode-code-point citation spans, provenance
presence, source URL schemes, date roles/precision, stale snapshots, unique claim
identities, and correction references. It performs no I/O or inference.

**147 offline tests passed**, including **44 new checks** and 31 named synthetic
claim cases. Cross-incident references, stale evidence, fabricated quotes, invalid
dates, malformed URLs, Unicode spans, identity reuse, and correction targets are
covered. A deliberate unsupported-statement example passes the structural check
to document that this is NOT an entailment or publication gate.

No runtime imports of the new module exist; nothing was deployed. No migration,
public JSON, job admission, prompt, or model changes. See
EVENT_EVIDENCE_FOUNDATION.md for trusted-input requirements, remaining corpus work,
and the next disabled shadow-integration release gate. Do not label the full
Events evaluation corpus, incident matching, immutable persistence, or factual
validation complete based on this contract suite.

## Isolated passage and matching contracts (2026-09-18)

Added exact passage slicing with full-document snapshot hashes, stable passage
identities, source URLs/origins, and document-relative offsets. Added a
conservative matcher requiring canonical entity IDs and trusted namespaced
incident references. Company similarity or publication chronology cannot merge
incidents. Ambiguous references and inconsistent registries abstain.

**172 offline tests passed**, including 25 new checks covering repeat incidents,
roundup scope, follow-up documents, syndication provenance, Unicode, changed
snapshots, invalid spans/URLs, and deterministic candidate ordering. These are
synthetic contract tests, not an adjudicated real reporting/model evaluation.
Trusted semantic scope selection and reference provenance are still required;
the helper cannot validate a caller's incorrect incident assignment.

No runtime imports, deployment, migration, job changes, model calls, or public
JSON changes. Next work is the bounded input boundary and trusted integration
design before a disabled shadow runtime release. Production remains unchanged.

## Bounded investigation retrieval (2026-09-18)

Inspected the existing article/event readers and PostgreSQL schema. The article
reader can follow stored filesystem paths; the event reader loads unbounded
linked collections and legacy prose. Neither is exposed to investigation clients.

Added isolated shared read services with trusted caller scopes, a 4 KiB strict
JSON boundary, parameterized article discovery, 31-day indexed feed-date windows,
keyset pagination, 200-row scan cap plus lookahead, and 64 KiB response cap.
Suppressed/invalid-policy records fail closed. Legacy event reads expose only
bounded active-record metadata, explicitly not a validated published revision.
PostgreSQL factory configures read-only repeatable-read transactions and bounded
connection/statement/lock/idle timeouts without initialization or migrations.

**224 offline tests passed**, including 52 new checks. SQL behavior is exercised
on an in-memory SQLite fixture; PostgreSQL connection options are mocked. Added
`tests/test_investigation_postgres.py` for an explicitly disposable PostgreSQL
database, but did not run it. Production query plans, role permissions, recall,
transport authentication, and exact evidence retrieval remain release gates.

No runtime imports, dependency changes, MCP endpoint, job admission, inference,
database migration, build, or deployment. Existing JSON and site behavior are
untouched. See INVESTIGATION_RETRIEVAL.md for the deliberately limited coverage
and metadata-versus-evidence version distinction.

## Version-pinned source text (2026-09-18)

Inspected `update_article_content`: text writes do not necessarily change the
article's `updated_at`. Added `get_article_evidence` with whole-content and source
metadata hashing rather than timestamp-derived identity. Reads exact database
text only, with a 131,072-code-point document cap and 2,048-code-point slices.
Continuation requires the original version; stale reads return no new text.
Every read rechecks suppression and requires both metadata and evidence scopes.
No paths, summaries, HTML, network fetches, or inferred incident assignments.

**260 offline tests passed**, including 36 new evidence cases. Expanded the
disposable PostgreSQL test to cover Unicode slices, content changes, and
suppression between reads. Could not run it: no local PostgreSQL binaries or
running Docker daemon. No production database substituted; PostgreSQL behavior,
query plans, permissions, and coverage remain explicit release gates.

These are current database text snapshots, not persisted immutable evidence or
validated incident passages. Source IDs are not promoted to independent origin
IDs. No runtime imports, MCP endpoint, deployment, build, or LLM calls. Tracker
and retrieval documentation updated; production behavior remains unchanged.

## Disposable PostgreSQL gate cleared (2026-09-18)

Used an empty, separately bounded Docker database on an existing Docker-capable
host, not the production PostgreSQL cluster. Image: PostgreSQL 18.4 alpine,
digest `sha256:9a8afca54e7861fd90fab5fdf4c42477a6b1cb7d293595148e674e0a3181de15`.
Verified 512 MiB memory/no additional swap, 0.5 CPU, 256 MiB tmpfs, no persistent
mounts, and loopback-only port access through a temporary SSH tunnel. Only
synthetic fixtures were loaded; no production data or credentials were used.

**Three PostgreSQL tests passed in 2.86 seconds** after adding a named-disposable
database guard, enforced statement timeout test, and restricted login test. The
restricted login read through all three services but could not update data or
create tables even without the application's read-only transaction guard.
Exact Unicode slices, suppression, stale-content rejection, and transaction
read-only behavior passed against real PostgreSQL. **260 offline tests passed**
again. This is not a production performance benchmark or full-schema migration
test; query plans, recall/coverage, and authenticated MCP transport remain pending.

Temporary schema/login were dropped by the test; the database container and SSH
tunnel were stopped and absence verified. Container inspection showed no OOM kill.
All SemperVigil deployments had their desired ready replica counts afterward.
No production manifests, application behavior, DB data, builds, or inference jobs
were changed. The downloaded database image remains cached; no test service runs.

## Local MCP adapter and coverage sampling (2026-09-18)

Read-only aggregate inspection: 35,211 articles, 35,209 with stored feed dates,
16,280 with nonempty database text, zero file-only text references. This leaves
18,931 records without stored evidence text; suppression/public eligibility and
extraction quality were not assessed. No backfill or processing change was made.
For August 19-September 18, a single server-side EXPLAIN ANALYZE measured 2.932 ms
for bounded discovery and 1.095 ms for literal title filtering. Both used the
existing feed-date bitmap index path. Latest 100 stored texts had maximum 57,801
code points and no 131,072-character cap exceedance. Not a recall/p95 benchmark.

Added an optional local stdio MCP adapter, pinned to official SDK 2.2.0. It has
no network listener or runtime job imports. Explicit startup enablement and a
dedicated restricted DSN are required; evidence access defaults off. Startup
rejects privileged roles, ownership, and table/column writes. Three read-only
tools reuse the shared services, with sanitized errors/audit, bounded framing,
one concurrent read, and finite per-process admission budgets. No model tools,
proposals, or publication access. Local OS/process authorization is not remote
OAuth or multi-user access control; operator rollout policy remains pending.

In an isolated SDK environment, **286 offline tests passed** and `pip check`
reported no broken requirements. Base environment: **260 passed, one optional
SDK module skipped**. The initial inherited environment had an unrelated existing
Torch dependency conflict; the final environment does not inherit system packages.
No system packages were altered.

**Four disposable PostgreSQL tests passed**, including a real stdio subprocess
and client, role preflight, column-write rejection, and denied SQL-tool access.
Temporary database/container and SSH tunnel were removed and absence verified.
No production role, dependency install, deployment, build, or inference change.
Full-schema integration, curated evidence evaluation, transitive release locking,
and authorized operator deployment remain gates before elevation.

## Historical evidence coverage investigation (2026-09-18)

Read-only production aggregates classified all 18,931 missing stored texts:
18,898 historical age exclusions, 18 retry-exhausted, eight HTTP 404, three legacy
HTTP 401, two legacy redirect errors, and two disabled-test-source rows without
errors. All age exclusions are one source's February 6 ingestion cohort, with
stored feed dates spanning 2007-2025. Current code excludes this marker from
automatic fetching; its original writer predates the available full-project sync.

September inventory: 1,117 articles, 1,090 with full-content flag and nonempty text,
22 with text but no full-content flag, five without text. Recent missing records
are four BleepingComputer September 9 and one Krebs September 8, all retry-exhausted.
No claim of semantic completeness or extraction quality is made.

Decision and bounded recovery gates are recorded in INVESTIGATION_RETRIEVAL.md
and the local HTML tracker. No code, schema, runtime configuration, model calls,
jobs, or production deployment changed. No test suite rerun for this documentation
slice; previous 286 offline/four PostgreSQL results remain historical evidence.
One Kubernetes worker exec returned upstream 502; read-only verification succeeded
through another existing worker. Ready pod status is not proof of exec recovery.

## Real-source Events evaluation seed (2026-09-18)

Read-only inspection found seven articles linked to the active Odido event,
including two multi-topic roundups. Four stored documents supplied seven short
excerpts with exact code-point offsets and full-text SHA-256 provenance anchors.
Only excerpts, public URLs, and evaluation metadata are committed, not full text
or private connection information. Scope/review annotations are provisional
assistant judgments, not model output or independent human adjudication.

Twelve cases cover relevant reporting, unrelated roundup passages, attribution,
units/overlap, date precision/roles, and unjustified source independence. Three
supported cases pass, three unrelated-scope cases fail structurally, and six
misleading claims pass structure but require semantic rejection. The 14 new tests
record these limitations rather than claiming a semantic implementation.

All 300 offline tests passed in the isolated MCP SDK environment. The seed's
14 tests also passed separately. Whitespace/error checks passed.

No runtime code, schemas, jobs, model calls, build process, public JSON, or
production deployment changed. This fixture-only slice does not require a new
PostgreSQL run; the four prior integration results are unchanged. Larger corpus,
independent review, actual single-job model evaluation, and disabled shadow-path
integration remain release gates. See EVENTS_CURATED_EVALUATION.md.

## Private Events review workflow (2026-09-18 local / September 19 UTC)

Implemented a local end-to-end extractive path: read-only repeatable-read source
snapshot, bounded strict packet parser, exact alias-matched passage suggestions,
standalone HTML controls, snapshot-bound decisions, atomic immutable artifacts,
and current-input comparison. It is not imported by existing workers/admin routes,
does not invoke inference or publish, and cannot approve semantic truth.

Real captures: Odido 7 sources/23 suggestions; Vercel 10/40; European Commission
12/41. No missing documents or article-link truncation in these samples. The
four-suggestion-per-source cap still limits coverage. All three packet versions
matched a second production read. Snapshots and full source text are local ignored
data; only implementation/docs/tests are committed. Production code was executed
in memory for the bounded read, not installed or deployed.

348 offline Python tests, five PostgreSQL tests, and eight JavaScript DOM-double
tests passed. New PostgreSQL test used a restricted role and verified capture,
draft/artifact generation, and suppression invalidating prior decisions. The
empty memory/CPU-limited test container and loopback SSH tunnel were removed and
their absence verified. No production schema or roles were changed.

The browser tool denied the local HTML URL. No workaround browser/server was used;
actual visual layout, CSP behavior, keyboard interaction, and downloads are not
claimed verified. Unit tests cover control state/persistence/export/error behavior.
The final read-only public checker passed 20 checks at 02:51 UTC: five pages,
11 linked assets, three day downloads, and feed index. Metrics/feed freshness
passed; August 15 retains 553 CVEs. This is not a full history completeness audit.

Release decision: deliver the private review artifacts now; do not deploy or
replace the public Events report handler yet. Autonomous model evaluation,
semantic checks, and proposal-only queue integration remain later gates.

Python wheel packaging succeeded and includes the new module, HTML template,
and JavaScript, with no private packet data. The first no-isolation packaging
attempt lacked setuptools in the test environment; normal isolated packaging
resolved that tooling prerequisite without modifying production dependencies.

## Private queue integration and admin visibility (2026-09-18 local)

Added distinct `event_review_private` type, registered to llm_local but outside
model-invoking job types. New admin-token-protected admission is disabled by
default, canonicalizes aliases, serializes dedupe/cap checks, and caps pending
requests at ten. Worker dispatch snapshots and renders outside data/site roots;
results contain metadata only, never a publication pointer or source text.

Admin inspection identified three visibility gaps: fixed dashboard lists, explicit
daily-brief hiding in JavaScript, and a separate incomplete Jobs filter list.
Registry/observed discovery, an Other group, canceled counts, and filtered links
replace these. Type catalogue lookup occurs once per Jobs-page load, not each poll.

371 offline Python tests, six disposable PostgreSQL tests, and ten JS unit tests
pass. PostgreSQL proves concurrent admission returns one job and produces a private
artifact without changing the event record. Two existing FastAPI startup-hook
deprecation warnings remain; no lifecycle refactor was included. JS syntax passes.
Temporary isolated database and tunnel removed; no production migrations, jobs,
model calls, deployment values, or application workloads changed.

User confirmed local review pages look good; this is initial visual feedback,
not factual approval or new dashboard browser verification. Next rollout must
verify admin/worker together with private admission disabled before a bounded
request. New LLM proposal/semantic evaluation remains unimplemented and gated.

## Private queue guarded rollout (2026-09-19 UTC, in progress)

Admin and LLM worker now run `ee91658`; source-only image manifest
`sha256:70c18459d5676f2cb4c94c5e49524752e8fa514e403fabb252e4493f66fae171`
was imported on all four schedulable nodes. Rendered diff contained four image
substitutions only. New per-worker chart override leaves shared ingest, builder,
and web images untouched. Orchestrator was temporarily scaled down, termination
confirmed, then the pending LLM launch drained before replacement. Scheduling
resumed on the unchanged image; no task canceled or directly rewritten.

373 offline tests and 11 JavaScript tests pass (six PostgreSQL checks remain from
the prior queue implementation). API catalogue includes 35 types including private
reviews and daily briefs. Disabled private admission returns 503 before DB access.
Dashboard verification exposed pre-existing expensive Need queries and overlapping
ten-second browser polling. Single-flight refresh guard is tested, pending an
admin-only follow-up deployment. No SQL/count semantics changed. Final visual
acceptance and post-release public checks remain pending at this checkpoint.

Browser testing also exposed a stale hard-coded August asset version: the new
API was live but browsers retained old job-filter JavaScript. The follow-up
admin release bumps the script version as well as serializing refresh requests.

The follow-up browser loaded all 35 Jobs filters and successfully filtered private
jobs, but dashboard tables still waited behind legacy content-wide queries.
Added an optional counter-only first request; full metrics remain the default and
load afterward. This preserves counters, backlog eligibility, and external metric
semantics. New tests cover early-return isolation and unknown (not zero) Need.

## Private queue deployment accepted (2026-09-19 UTC)

Supersedes the in-progress notes above. Admin runs `32ad342`; LLM worker runs
`ee91658`. Platform configuration is committed as `e9c6bf2`. Admin image manifest
is `sha256:f695a91df70520a2efa481ef3960c00a8b76681eba8ad9627ce8c0d84571a17f`,
imported on its chart-pinned node. Worker image is present on all four schedulable
nodes. Every follow-up was rendered/diffed and applied only to the admin Deployment.
Final rendered admin/LLM Deployments have no diff against production. Admin source,
storage, JS, template, and worker/private-handler hashes match committed sources.

- 375 offline Python and 12 JS tests pass. Six PostgreSQL queue tests passed in
  the preceding implementation slice; they were not rerun during this deployment.
- Browser shows all 35 job types, including daily briefs and private review, and
  all four dashboard groups. Clicking the private-review row selects that type
  on Jobs and returns an empty list, as expected with admission disabled.
- Counter-only API returned in 0.71 seconds. Need is unknown until the full
  response arrives. Full dashboard rendering also verified, but legacy content-wide
  Need queries are slow; SQL optimization remains separate work. The single-flight
  guard is per browser page, not a cross-client/server cache.
- Authenticated private admission returns 503 `private_review_disabled`. No private
  production job/artifact was admitted, no event was published by this feature,
  and no model proposal or automatic review was enabled.
- Normal CVE enrichment succeeded on the new worker; two sampled model calls
  took 1.975s and 1.897s. Automatic build `job_0328f8c8062e4de58a930e95bbf539c8`
  succeeded at 03:43:55 UTC. No manual Hugo invocation or extra build was needed.
- All application Deployments ready; replacement pods have zero restarts.
  Kubernetes readiness including etcd passed; public checker passed 20 checks.
  These are point checks, not a continuous availability or full-history audit.

One diagnostic request exceeded its 25-second timeout. A later full-metrics probe
was terminated with its old admin pod during the follow-up rollout; it is not an
OOM claim. No direct SQL writes, schema changes, or count-definition changes were
made. Shared ingest/orchestrator remains `27b9fb3`, builder `889b2de`, web unchanged.

Rollback: restore admin tag `6c0d2d9`, remove the worker image override (inherits
`27b9fb3`), render/diff, drain the LLM launch after stopping orchestration, and apply
only the affected Deployments. Restore orchestration after readiness. There are
no schema or public-content changes to undo. Keep private admission disabled.

Application changes and SemperVigil platform values are committed/pushed. The
platform worktree has pre-existing unrelated appliance-proxy/certificate edits;
they were preserved and excluded, so the entire platform repo is not clean.

## Private operator workflow (September 19 UTC, pilot preparation)

Added an explicit Event Detail queue control, admission status API, and completed
job attachment download. Private admission/retrieval require configured admin
authentication; disabling admission does not destroy historical downloads.
Descriptor-based bounded reads reject folder/file symlinks, nonregular/oversized
files, path traversal and byte/hash mismatch. Headers force private no-store
attachment delivery with a sandbox policy. No client-supplied filesystem path.
390 offline Python and 14 JS tests pass. Real production pilot is next; no public
report or model-generation behavior changed. User requested continued overnight
implementation; a bounded eight-run hourly continuation is configured.

## Overnight pilot deployed (September 19 UTC)

Runtime admin/LLM image `0087db6`, chart wiring `205cf91`, platform `1db6492`.
Image manifest `sha256:a8c6547c4a80dfc8493be4d4261b4d0d5da5f0d543c37bc05e3b973afabccc75`
imported on all four schedulable nodes. Render/diff showed four image substitutions
plus two explicit shared environment keys (private admission 1 and private log
subdirectory). Chart originally did not pass arbitrary env keys; explicit wiring
and two rendering tests were added before applying. Total: 392 offline cases and
14 JS cases. Prior six disposable PostgreSQL tests remain the queue integration
baseline; not rerun for this UI/download slice.

Scheduler stopped, fully terminated, and a finishing normal LLM launch drained
before worker replacement. Admin/worker readiness passed; both confirmed enabled,
private writable log root checked, scheduler restored. Web has no log-volume mount.
Builder/web images, concurrency, models, public JSON and build behavior unchanged.
Browser verified the enabled private control and explicit non-publication notice.

Authenticated API admitted only `job_05152e7dd57246d48c01bb47ead61d93` for event
`evt_69844df3a97f` with alias `Odido`. It is low-priority and was still queued behind
113 CVE jobs at the last sample. Do not claim completed worker/artifact validation
yet. Event row hash before and after admission: `92ce5376ad269aa1f073fafdc75f98d4`.
Next overnight run must inspect this job before submitting anything else. No model
call or report publication is performed by the private handler.

Rollback: set private admission to 0 in platform values, render/apply ConfigMap,
restart admin and safely drain/restart LLM worker to adopt it. Preserve artifacts
and last good public output. Image rollback remains possible to admin `32ad342`
and LLM `ee91658`; no schema rollback. Retain low-priority scheduling during pilot.

## Bounded private model assessment (September 19 UTC, local only)

Added an opt-in worker callback through the existing profile router. The default
remains extractive. The dedicated profile must match the active CVE local model
and provider, use the exact evidence-assessment prompt, have no fallback, and
respect explicit input/output budgets. At most twelve exact passages are selected
round-robin within 12,000 input bytes. Responses must cover known IDs exactly
once; invalid/incomplete responses create no review HTML. Model annotations never
select Include, approve facts, dirty a build, or write public reports.

412 offline tests and 14 JavaScript tests pass, including real worker dispatch
with fake model responses, no-publication guards, invalid-output failure, stale
evidence rejection and rendered chart configuration keys. Tests ran in the
existing `.cache/mcp-venv` environment; the system Python lacks FastAPI and its
initial collection failed. No dependency change was needed. The prior six
PostgreSQL integration checks were not rerun for this slice.

At 04:34 UTC the Odido pilot remained queued behind 37 CVEs. Ordinary CVE job
`job_6dce65b8967645cba7020d4f3c01e1ff` entered its model HTTP request at
04:15:52 UTC and had not returned at the next sample. The configured socket
timeout is 1,200 seconds; the router also retries certain URL timeout failures,
so that value is not an overall wall-time guarantee. Ollama remained Ready with
the existing model loaded (16,384 context, 100% GPU placement); sampled GPU memory
was 5,730 MiB of 12,288 MiB and GPU utilization 38%. This is not proof that the
request is healthy or that a restart is warranted. No inference or job was
canceled, reprioritized, or duplicated. Event row fingerprint remains unchanged.

At 04:35 UTC the public checker passed; all application Deployments were Ready
and Kubernetes readiness passed. These are point checks. New assessment code
is not deployed, no dedicated profile exists yet, and model quality/latency are
unmeasured. Platform/theme repos and production were not changed during this
slice. Next: resolve/observe the ordinary request, finish pilot attachment
verification, then release and measure the guarded assessment using the same
single-job worker. Public automated reporting remains an open milestone.

## Inference recovery and assessment preparation (September 19 UTC)

At 04:43 UTC the runner had exceeded its 1,500-second launch window. The prior
CVE job still showed running, while a new request was admitted. Source inspection
confirmed that runner termination does not finalize the child job; a running DB
row therefore does not prove a live subprocess. Ollama had not completed a
generation since 04:15:25. LiteLLM forwarded `options: {}` with JSON mode: the
active CVE profile had no output-token bound. This is a containment defect, not
proof of the exact model-level cause of the stall.

Recovery: stopped orchestration, canceled only the two affected CVE jobs and
current LLM launch through the authenticated admin API, then stopped the LLM
runner. Patched that CVE profile's params from `{}` to `{"max_tokens":1024}` via
the AI API; no prompt, model, provider, fallback or routing change. A server dry
run/diff showed only Ollama's restart annotation changing before its targeted
Recreate restart. No image, resources, context size or parallelism changed.
Both affected CVEs were requeued through the existing rerun API, then the single
LLM worker and orchestrator were restored to one ready replica each. No direct
DB updates or Hugo commands. Public services were not restarted.

By 04:47 UTC normal model calls succeeded in 1.03-2.98 seconds with 16-64 output
tokens. Requeued IDs: `job_2f07c8baa5b6451f9864a2fc34499bf4` and
`job_f24d79cb96f6414097e0d7685cd930d4`; completion still needs checking. Original
private pilot remains pending at low priority. The runner's orphaned-job and
upstream cancellation behavior needs a separate tested correction, not a claim
that this restart solves it permanently. Roll back the profile cap through the
same API with params `{}` only if a demonstrated valid-output regression requires
it; unbounded generation is not the preferred normal configuration.

Prepared an unrouted Events assessment prompt/profile through the AI API and
verified its readback against the exact source prompt and guarded budgets:
profile `24a0096b-57f0-5493-a1d4-bb5f41f3d216`, prompt
`98fb4df1-f96e-5115-840c-6774b97368d9`. Existing routing and model remain unchanged.
No inference ran from admin. Assessment remains disabled until the targeted
application release and real-worker quality/latency pilot. Source-only image
`bddeff1` was built using the retained base without dependency changes.

## Private pilot verified; assessment deployed (September 19 UTC)

Original extractive pilot `job_05152e7dd57246d48c01bb47ead61d93` succeeded with
seven documents, 23 passages, no omitted documents and no link truncation. The
authenticated API returned a 67,698-byte attachment with SHA-256
`3af71c098185a1446f8563df7783ad954c27def3681b898f8f89b5171e780ba6`, matching the
immutable worker filename. Private/no-store, attachment, nosniff and sandbox CSP
headers were present. Event fingerprint remains
`92ce5376ad269aa1f073fafdc75f98d4`. Both recovery reruns also succeeded with one
affected-product item each; no original CVE was dropped from the queue.

Deployed source image `bddeff1` to admin and LLM worker only; platform commit
`f276b4b`. Imported image manifest
`sha256:8e8a5903c22054b01bd42f28434f7ca44dccd290949358cfb678dcfa74846b2c` on all
four schedulable nodes. Render/diff showed four image substitutions and two new
ConfigMap keys only: assessment flag 1 and dedicated profile ID. Orchestration
was stopped and fully terminated; no local LLM job/launch was running when the
worker was drained. Applied ConfigMap before replacement Deployments, waited for
readiness, restored orchestration. Post-apply render diff is empty.

Live admin status reports assessment enabled. A bounded read-only worker preflight
validated the real profile/prompt/model against the guard without invoking the
callback; `event_review_private` is in model admission. Submitted exactly one
model-assisted Odido pilot: `job_2be77141919a403592ce6ca21ce8e8a3`. It remains
queued behind the next ordinary CVE catch-up batch (191 queued at last sample).
Do not duplicate or reprioritize it. Real-model output, runtime and semantic
quality are not yet verified. Default application flags remain disabled; only
the platform operator workflow is enabled. No automatic public reporting.

At 04:56 UTC the public checker passed; all application Deployments ready,
replacement worker zero restarts, Kubernetes readiness passed after recovery.
Builder, web, fetch workers and normal routing unchanged. The HTML tracker and
queue documentation reflect the pending real-model gate. Unrelated platform
appliance/certificate edits were preserved, not committed. Rollback: disable the
assessment flag, retain artifacts, restore admin/worker `0087db6` through the same
targeted rendered/drained rollout. No schema or public-content rollback needed.

## Assessment cache and router contract correction (September 19 UTC, local)

Source inspection during cache work found that `run_profile` returns an envelope
containing `parsed`, not the decisions object directly. The deployed model pilot
is still queued, so no real model assessment has succeeded or failed yet. Fixed
the callback to unwrap the validated envelope; a test now traverses the actual
router with only provider I/O mocked. Refused schema-configured private profiles
to avoid the router's extra schema-repair call. This is why the earlier 412-test
result was not described as real-model verification.

Added private immutable assessment reuse keyed by bounded request/evidence plus
database-visible generation configuration. Rechecks configuration after inference;
cache hits still pass assessment validation and never approve reading selections
or public content. Symlink folders/files, FIFOs, malformed/oversized/stale entries
fail without inference. Atomic cache install does not overwrite earlier results.
The cache does not attest to model weight changes hidden behind unchanged names;
operators must revise the dedicated profile after such changes.

427 offline tests and 14 JavaScript tests pass, including a repeated real worker
dispatch issuing only one mocked inference and retaining identical HTML. No DB
integration or real model quality claim added. New release is not yet deployed;
current model pilot remains `job_2be77141919a403592ce6ca21ce8e8a3` at low priority.

## Cache/router worker release (September 19 UTC)

Deployed `0e826ed` to the LLM worker only, platform commit `3b02a14`; admin stays
`bddeff1`. Render/diff contained only worker and init-container image changes.
Manifest `sha256:f79a003218819f5e63a8e49159ae177ac9baae0756985ef1a8b94b915c08edc4`
imported on all four schedulable nodes. Scheduler fully stopped, no LLM launch or
job running, old worker deleted, replacement Ready, scheduler restored. No
configuration, prompt, model, concurrency, source content or build change during
this rollout. Post-apply worker diff is empty. Live read-only preflight validates
the real profile and produces a 64-character cache identity without inference.

At 05:12 UTC all application Deployments Ready, Kubernetes readiness passed,
public checker passed. Model pilot still queued, 110 ordinary CVE jobs ahead at
last sample; no duplication or priority bypass. Real model quality/latency and
live cache reuse remain unverified. Rollback should disable model assessment
before returning to the previous image because `bddeff1` has the known router
envelope mismatch. The extractive review mode and existing artifacts remain usable.

## Suggested reading and private timing (September 19 UTC, local)

Added an expandable private Suggested reading draft from model-included exact
quotations, with source links/spans, coverage and explicit non-approval/date
caveats. It never selects human Include choices. Held/excluded material remains
auditable below; no included passages means no inferred account. No new model
call or public export path. HTML tests verify escaped source content, retained
citations, non-invention on empty selections, and unchanged extractive-only mode.

Private profile calls now insert one attributed `llm_runs` record on success or
failure. Timing includes router work and configuration recheck, not queue wait.
Only counts and exception type are stored, not source bodies/provider errors.
Generation success is not semantic assessment approval. Cache-hit dispatch is
tested to produce neither a second call nor a second timing row. Existing table
coverage is incomplete for other job types and hard-killed requests; future
automatic admission cannot assume complete inference accounting.

433 offline tests and 14 JavaScript tests pass. No new schema, dependency, model,
concurrency or Hugo behavior. Release pending; original model pilot remains
queued behind ordinary CVEs. Real model quality and live timing rows are not yet
verified. This slice moves private review toward a readable automated draft, not
an approved public Event report.

## Reading/timing worker release (September 19 UTC)

Worker `2de5845`, platform `683abb8`; admin remains `bddeff1`. Source-only image
manifest `sha256:dd3ef97882109bf75e6e1824573560e678605d4d35f095cc3f7f09732e8ca4a8`
imported on all four schedulable nodes. Render/diff showed only the worker and its
init-container image substitutions. Orchestrator fully stopped, active LLM work
drained, old worker deleted, replacement Ready, orchestrator restored. Post-apply
diff empty. Live read-only preflight passed the actual profile guard and confirmed
existing timing columns; no inference or schema change in preflight.

Original model pilot remains queued (24 ordinary CVEs ahead at last sample).
Do not requeue it. Its completed result must be checked for a valid assessment,
private suggested reading, attributed timing and unchanged public eligibility;
factual quality remains a separate gate. The first extractive pilot and recovery
CVE reruns were already verified. No changes to public renderers, JSON contracts,
builder, model, prompts or concurrency in this release.

Rollback is a worker-only return to `0e826ed` if needed: it retains corrected
router handling and cache, but lacks suggested reading/timing. Preserve artifacts.
Automatic evidence refresh and public reporting are still not enabled.

### Three-event model cohort admitted

Added Vercel `job_4cd3c296ba03499380a220b485a0c2de` (event `evt_0ffca0813049`,
alias Vercel) and European Commission `job_378b8db685ee4c219314058ef0082316`
(event `evt_9bfd4aa3616a`, alias European Commission) through the private admin
API, preserving priority -10 and the single worker. The original Odido pilot
remains in place. No automatic discovery or further admission is enabled.

Before admission, event fingerprints were `de70b60cd209cb493f75a529c78524e8`
(Vercel) and `b39cd4a3d7fc432285b4082f6c1c3af0` (Commission). Last-day existing
telemetry had 462 rows totaling 6,027,341 ms; this undercounts unmetered job types.
Three bounded private calls are the explicit pilot, not permission for unlimited
backfill. Evaluate actual aggregate timing before further admissions; pause
expansion if it consumes the provisional 10% budget or delays fresh work.
Commission's mixed-incident sources are a required quality counterexample.

## First model cohort rejected; prompt correction (September 19 UTC)

All three jobs failed with `invalid_assessment_values`. Generation times were
9,785 ms (Odido), 10,680 ms (Vercel), and 12,529 ms (Commission): 32,994 ms total.
The generation-only success records are not assessment approval. Bounded response
inspection found slash-joined decision values and free-text reasons, matching the
ambiguous shorthand in the prompt. No validator bypass, artifact or publication.
Read-only `md5(to_jsonb(e)::text)` hashes for all three events match the pre-pilot
values above. A different `row_to_json` serialization gives different hashes and
must not be mistaken for changed rows.

Commission output additionally merged parts of the staff MDM and cloud/Europa
incidents, despite distinguishing some passages. This is a semantic failure, not
fixed merely by a valid JSON response. Public reporting remains gated.

Local assessment v2 uses explicit JSON decision/reason combinations, excludes
generic company background at passage level, and warns against using the first
source as incident definition. No change to accepted values, model, token cap,
concurrency, router repair behavior, public data or Hugo. 437 offline tests pass;
four new regressions cover prompt examples and rejection of observed bad values.
Deployment and real-model v2 evaluation pending.

### V2 deployed; bounded reevaluation admitted

Worker `5324cab`, platform `0bf871f`. Source-only image manifest
`sha256:da016cb5185691d7f076c26011ec747a0e5284314c8670353b61ddcec7417ec4`
imported on all four schedulable nodes. Only runtime source change from `2de5845`
is the assessment prompt/workflow. Rendered/live diff showed the worker and init
image substitutions plus the dedicated private-profile ID in the ConfigMap.
Orchestrator fully terminated, no active jobs or pending LLM launches, old worker
fully terminated, ConfigMap applied before replacement worker. Read-only actual
profile preflight passed without inference; orchestrator restored Ready. Post-apply
diff empty. Platform source committed/pushed, unrelated platform changes preserved.

Dedicated v2 prompt `b0357022-e57a-5e7b-8c5a-642267740e19`, profile
`98dc8957-780b-585c-b311-64dd4fc2a9f2`; created via admin API, leaving v1 intact.
Same model/provider, temperature 0, 1,024 output tokens, 12,000 input characters,
no fallback or schema-triggered repair. No public renderer, Hugo, build or schema
changes. Rollback requires both prior worker `2de5845` and prior private profile
`24a0096b-57f0-5493-a1d4-bb5f41f3d216`; preserve immutable artifacts.

Reevaluation admitted through the private API at normal private priority -10:

- Odido: `job_2b6480b40694460da676159f09f36d05`
- Vercel: `job_922e4454388d44dbafda5e3668975646`
- Commission: `job_8d3d5fd4f6054105b7f58b28a038b853`

These are the only new calls authorized in this cohort; do not duplicate them.
They remain behind normal CVEs at the latest sample. Public checks passed at
05:47 and 05:49 UTC, including historical August 15 JSON. Kubernetes/etcd readiness
passed before rollout; replacement worker Ready with zero restarts. Ordinary model
calls continue (recent samples roughly 1.2-2.0 seconds, one 5.7-second sample).

Added a local offline evaluation command and eight snapshot-pinned provisional
cases. These make the observed semantic failure measurable without treating model
agreement or valid JSON as public approval. They do not change the running worker.
450 offline tests pass, including malformed/stale inputs, unassessed cases,
unsafe inclusion and all-hold failure. No new PostgreSQL or browser acceptance
claim for this evaluator-only slice.

## V2 real-model evaluation (September 19, 06:13 UTC)

Normal CVE/product and threat-actor queues drained before private work, without
priority bypass. Odido and Vercel failed `incomplete_assessment`; Commission
produced a structurally valid private assessment. Call times: 4,718, 4,827 and
4,833 ms, respectively (14,378 ms total; both cohorts total 47,372 ms).

Commission failed three of four pinned checks: all three unrelated staff/MDM
passages were included; the positive Trivy/AWS passage was retained. This fails
the semantic gate. No automated public report may use this assessment. Its private
download did work: 114,211 bytes, SHA-256
`4eb7c4072f358503124903ddf91e3244e1e9f3600ef655e547c4506e0db75256`.
Suggested reading was present, no human Include option preselected, sandbox,
attachment, private/no-store and nosniff headers retained. All three event-row
fingerprints remain unchanged. No new browser/visual acceptance claim.

Bounded inspection verified the actual model request had the exact v2 prompt,
target metadata and candidate IDs. This was not stale routing/configuration.
The model skipped IDs in two responses and substituted the first incident for
the requested one in the Commission response. No automatic retry/repair occurred.

Local v3 makes the requested incident explicitly the question, repeats its target
and exact required-ID inventory after evidence, and removes duplicated quote text
from context serialization. The same 200-character before/after context is retained
losslessly; source snapshots and spans are unchanged. All three packets fit 12
passages below the unchanged 12,000-byte cap. V3 fixture labels are identical to
v2 (only request identity changes); 452 offline tests pass. No extra model call,
model change, concurrency increase, validator coercion or publication bypass.
V3 is not yet deployed or evaluated. A single Commission v2 reuse check was
admitted as `job_3909e41559c440e28658dfaa12c6b37e`; observe before replacement.

The optional v2 reuse check was canceled through the admin API while still queued
before the v3 rollout, so it made no inference call and does not verify live cache
reuse. Orchestration had admitted another ordinary 200-job threat-actor batch
before this request arrived; the delay was normal priority ordering, not a missing
private job registry or absent runner launches. Read-only source/log inspection
confirmed queue-name-based launches. No orchestrator code/image or priority change.

### V3 targeted release and cohort

Worker source `293ad11`, platform `019f623`; image manifest
`sha256:e99317014b13e30535c6f474e4362369c82a86fda2493b99d3a99bb6ee5378b1`
imported on all four schedulable nodes. V3 prompt
`4e18080e-b38e-546c-a3b2-0658645879d0`, profile
`a2ccda1b-95c1-5085-8de7-22c2d328faa0` created through admin API; older profiles
left intact. Same provider/model, token/input limits, temperature and single-worker
policy. Only assessment source changed between runtime images.

Rendered/diffed worker Deployment and ConfigMap: only worker/init images and
private profile ID changed. Orchestrator fully terminated; no active jobs or queued
LLM launch remained; old worker terminated. Applied ConfigMap then worker,
verified actual profile guard without inference, restored orchestration Ready.
Post-apply diff empty; platform change committed/pushed separately from unrelated
appliance-proxy/certificate work. No Hugo invocation or public-renderer change.

Third bounded cohort admitted at private priority -10:

- Odido: `job_211c1cbb8e694b5d94388fd567c7b84d`
- Vercel: `job_fc27a285bb464e4d9f133ab8da700395`
- Commission: `job_69334e60a73c4f9aa53d7dfd3764098e`

Observe these jobs; do not duplicate or reprioritize them. Run the offline checker
with the v3 fixtures and inspect full suggestions beyond that small subset. A
passing structural job/fixture subset does not authorize public reporting. If
incident confusion persists, prioritize explicit incident evidence/scope handling
over repeated prompt-wording changes. Automatic refresh and public gates remain
unimplemented/disabled. Live unchanged-input cache reuse is still unverified.

Rollback pairs worker `5324cab` with private profile
`98dc8957-780b-585c-b311-64dd4fc2a9f2`, or disables model assessment and preserves
extractive private artifacts. Preserve all existing snapshots and public output.
Post-rollout public checker passed at 06:25 UTC, including historical JSON; all
application Deployments Ready and replacement worker zero restarts. These are
point checks, not continuous availability or full-history completeness proof.

## Event export reuse (September 19, local only)

While the v3 model cohort waits behind ordinary work, inspected the Events public
writer. It previously deleted all generated Markdown before rendering replacements,
including unchanged pages. Its callers are CVE sync and explicit event rebuilds,
not every Hugo build. Local correction pre-renders inputs, reuses identical files,
atomically replaces changed files, then prunes stale Markdown; cleanup failures
surface. Missing/unsafe/colliding slugs fail before writes. All 11 live selected
published event slugs match the validated form. Publication/lifecycle selection,
rendered content, daily/index JSON and Hugo behavior are unchanged.

This change is not deployed. A rich synthetic page's byte hash matches the prior
renderer; unchanged inode/mtime and failure/withdrawal cases are covered. It is not
a whole-directory transaction or shared cross-writer/Hugo lock. See
`EVENT_EXPORT_STABILITY.md` for limitations and release checks. Do not deploy it
incidentally as part of a private-worker prompt fix or claim measured build savings.
471 offline tests pass. No production mutation, build or browser test was performed
for this local writer slice.

## V3 real-model cohort accepted structurally, rejected semantically

All three v3 jobs completed and produced private attachments. Model timings:
Odido 5,117 ms, Vercel 4,812 ms, Commission 4,753 ms (14,682 ms total; all three
cohorts total 62,054 ms). Each assessed 12 passages; omitted candidate counts were
11, 28 and 29 respectively. These omissions are distinct from snapshot omissions
and do not mean evidence does not exist.

Pinned provisional cases: Odido 2/3, Vercel 1/1, Commission 3/4, total 6/8. Unsafe
includes remain: Odido's repeated generic company description and Commission's
staff-phone/MDM intrusion. Vercel's single positive check is not whole-report
approval. Stop prompt-wording iterations; public reporting remains gated. The
next design is explicit source-grounded incident scoping, documented in
`EVENTS_INCIDENT_SCOPING.md`, not model self-approval or weakened expectations.

Authenticated downloads verified status 200, sandboxed private/no-store attachment
headers, and no preselected human Include choices:

- Odido: 74,070 bytes, SHA-256 `9f511c4e225540e423840f9bce34482b532bf8713982af67c16d123ac66b04ce`.
- Vercel: 113,502 bytes, SHA-256 `6e40fd0fe1328756050dfa1306eacafeb99e05ce29c53d99182577f968812c74`.
- Commission: 112,236 bytes, SHA-256 `7713566870933d6efd83fa9ca20d467f4f7b067e7427518ca59bd054d418fbc6`.

All three event-row fingerprints remain unchanged. No browser/visual acceptance
or public revision was claimed. V3 Commission reuse check
`job_99347062943043ee874e5c719e93ed5e` is queued at ordinary private priority.
Do not duplicate or cancel it merely to deploy another prompt; the v3 runtime
should remain stable while scope handling is developed locally. Verify cache hit,
identical artifact and no new `llm_runs` row when it completes.

## Source-scoped private jobs: local implementation, September 19 07:12 UTC

Added exact source/document-version scope proposals, bounded optional authenticated
admission, independent disabled-by-default enablement/profile, worker provenance
validation before inference, scoped cache identity and private comparison-source
display. All human reading decisions remain Hold; no public permission, automatic
admission, schema change, model/concurrency change or build invocation is added.
The offline quality checker now pins the actual scoped request when present.

503 offline tests pass, including 32 additional checks for source/contract changes,
scope/profile gates, stale evidence before inference, serialized one-call dispatch,
cache reuse/cross-scope rejection, escaping and evaluation identity. Existing three
v3 request hashes and complete HTML hashes match the production artifacts recorded
above exactly. No new PostgreSQL, live scoped inference or browser acceptance is
claimed. Earlier six PostgreSQL/fourteen JS checks remain from prior slices.

All live deployments remained Ready during the read-only check. The v3 reuse job
was still queued, with zero inference records, when checked around 07:08 UTC.
No production writes or deployment were performed. The platform's unrelated
appliance-proxy/certificate changes remain untouched. Next release must await the
existing v3 cache check, retain its current default profile, and enable scoped
pilots separately. Keep all eight provisional quality expectations, including
the repeated-company and cross-incident negative cases.

## Scoped pilot deployment, September 19 07:27 UTC

V3 reuse job `job_99347062943043ee874e5c719e93ed5e` succeeded with
`model_cache_hit:true`, identical Commission artifact and zero `llm_runs` rows.
Authenticated download again returned 112,236 bytes, SHA-256
`7713566870933d6efd83fa9ca20d467f4f7b067e7427518ca59bd054d418fbc6`, private/no-store.
Only after that result did the scoped rollout proceed.

Source image `sempervigil-ingest:6141d61` uses the retained `27b9fb3` dependency
base, image config `sha256:598271c3968068f13c2ecb18d88c82fb84905649fd9e201776fae6ab37a4fb55`,
OCI manifest `sha256:3bb11617d1aa51210ac76423a34f55a7408ef78f07d46240e2b994a2cebd2873`.
Imported on docker42/46/47/52. App `1015bea` adds tested chart wiring and pinned
cases without changing image source. 506 offline tests pass.

Render/diff showed only admin/LLM-worker main+init image changes and two new scope
ConfigMap settings. Paused orchestrator admission, waited for pod exit and an empty
running LLM/launch queue, stopped the single LLM worker, then applied only the
ConfigMap and those two Deployments. Both rolled out successfully. Read-only
factory guards passed for both existing v3 and new scoped profiles without any
model invocation. Orchestrator was restored Ready; no concurrency increase.
Post-apply diff is empty, platform `39b6dd2` committed/pushed, and rendered source
values match applied configuration. Unrelated platform edits were preserved.

Public checks before (07:21:08) and after (07:27:24) passed, including sampled
historic JSON, public pages and local assets. Kubernetes/etcd readiness passed.
Existing private attachment remains downloadable after admin replacement. These
are HTTP/contract checks, not a new browser/mobile acceptance claim. Web, builder,
fetch/orchestrator images, Hugo commands, shared archive and stage routing were
not changed. The image contains the local event-writer improvement, but its active
CVE-sync/events-rebuild callers still run the old shared image; no writer rollout
or production build-performance improvement is claimed.

The explicitly selected scoped cohort was submitted through authenticated admin:

- Odido: `job_3e8dbe9ecba7450083d32fa61721c5dc`.
- Vercel: `job_1aeca71d82a04edc97e88f09151a1689`.
- Commission: `job_6f4f2dce65724b9db67206be1d9b3154`.

All were queued behind normal CVE backfill (194 queued at last check), private
priority -10, max attempts 1. No result/quality claim yet. All eight earlier
provisional expectations remain; each request still includes 12 candidates and
fits the original budget. Do not duplicate or reprioritize these jobs. Event-row
fingerprints remained unchanged before rollout. No new public reporting enabled.

Rollback: disable scope admission/model use for scoped requests with
`SV_EVENT_REVIEW_SCOPE_ENABLED=0` and restart only affected processes after draining;
or restore worker `293ad11` and admin `bddeff1`, preserving the v3 profile. Never
leave a running model request orphaned to accelerate rollback. The unused scoped
profile may remain for audit, with no ordinary pipeline routing to it.

## Scoped cache stability follow-up, local only (07:35 UTC)

Inspected the current public report path: `_handle_event_report_llm` builds from
summaries/context, parses model output and calls `update_event_report`, which
overwrites `meta.report` and touches `updated_at`. It does not supply an immutable
validated revision or transactional source-version comparison. No publication
approval may be inferred from this legacy path. Its behavior was not changed.

Prepared the narrower, integrated scoped-cache correction in
`EVENTS_INCIDENT_SCOPING.md`: exclude only report timestamp from cache identity,
retain full source/coverage/configuration identity, and revalidate/rebind cached
decisions to current snapshot IDs. Import an existing valid exact-snapshot scoped
cache without another inference. No request/prompt/schema change. 518 offline tests
pass (12 new); no deployment, new admission, public write, or model call for this
slice. The three live scoped jobs remain queued behind normal backfill (155 jobs
at the 07:34 check), without errors. Keep their runtime stable until evaluated.

Public revision work must keep immutable candidates separate from the last accepted
publication pointer, compare current source/scope versions inside activation's
transaction, and preserve the old pointer on failed/stale assessment. Do not reuse
`update_event_report` as that gate or treat an exact quotation as semantic approval.

## Legacy report lost-update protection (local, September 19)

Implemented the bounded correction described in `EVENT_REPORT_STALE_WRITES.md`:
pass the worker's starting event version and compare the current complete metadata
and timestamp atomically at write time. Failed/stale writes retain the competing
edit/report and do not mark the site dirty. No new inference, schema migration,
public fields or live deployment. This is not an immutable revision/publication
gate and does not establish semantic/source validity.

529 offline tests pass (11 added). Seven disposable PostgreSQL tests pass in
5.50 seconds, including actual two-connection read/write interleaving. Used a fresh
PostgreSQL 18.4 alpine test container on docker52, 512 MiB memory/no extra swap,
0.5 CPU, 256 MiB tmpfs, loopback-only port through a temporary SSH tunnel, no
persistent mounts. Only synthetic data; no production credentials or schema.
Container reported no OOM, was stopped/removed, and tunnel closed after testing.
An initial schema-fixture mismatch was corrected in the test, not production code.
The running scoped pilot remains unchanged; evaluate it before another rollout.

## Admin private-result clarity (local, 07:53 UTC)

Private job results now include actual validated assessment/suggestion coverage;
the Jobs table labels them unapproved drafts, not publishable reports. Historical
rows show coverage unavailable, never invented zero counts. Source-scope proposals
remain explicitly unqualified and reuse is distinguished from a new call. No raw
evidence text or publication capability added. Same job type and authenticated
download endpoint, no new inference/admission or public site changes.

529 offline Python tests pass with new metadata assertions, 19 JS tests pass
(five new), and admin JS syntax checks pass. Prior seven disposable PostgreSQL
tests apply to the unchanged queue/storage paths. No browser/UI deployment was
performed; the real scoped cohort remains on its stable deployed image.

## Scoped pilot evaluated, September 19 08:05 UTC

All three jobs succeeded structurally with one call each: Odido 6,258 ms, Vercel
5,948 ms, Commission 5,911 ms; total 18,117 ms. All four cohorts to date used
80,171 ms across 12 model calls. This excludes ordinary work and is not a complete
platform inference-cost meter. Queue wait behind ordinary backfill is distinct
from these call times; no pilot priority/concurrency changes were made.

Unchanged pinned cases: Odido 2/3, Vercel 0/1, Commission 4/4, total 6/8. Commission
cross-incident exclusions are fixed in this sample, but Odido still includes a
generic company description and Vercel loses relevant reporting. No public output,
approval or automatic admission. All three event-row fingerprints remain unchanged.

Authenticated private/no-store, sandboxed HTML attachments returned 200 and showed
unqualified source anchors with no human Include decisions preselected:

- Odido: 73,955 bytes; SHA-256 `d19c6bc6819e27665820df6797991701dc0bf6224e1e654ff6a1252fd7ec2e94`.
- Vercel: 112,540 bytes; SHA-256 `c61a6f9ec100fb81a66f133abc3f2145702b8fff3501f659c963d1e40ba16f24`.
- Commission: 112,334 bytes; SHA-256 `1bf3d4ad87920695338ab10d308402b620c69922504840f481019270eabcea36`.

Next bounded implementation/test is source-level assessment, not further prompt-only
tuning: same model/profile/one-call jobs, at most four passages from one explicitly
selected source, retaining all eight cases across seven source jobs. See
`EVENTS_INCIDENT_SCOPING.md` for the exact scope and gates. No new calls admitted yet.

## Source-level queue path, September 19 local checkpoint

Added optional explicit source selection through the authenticated private API,
queue payload, assessment, immutable metadata and cache. Same scoped prompt and
one-call profile; all evidence remains visible in the private packet. Unknown
sources fail before inference; source IDs cannot alias cache entries even with
identical text. Timestamp-only reuse rebinds current passage IDs as before.

Added a strict aggregate evaluation mode for pinned source cohorts. It refuses
mixed scopes/generations/snapshots, missing or duplicate sources/cases, and absent
case coverage. Positive expectations still fail all-Hold. These are provisional
quality checks, not public-report approval.

556 offline tests pass (27 new); `git diff --check` passes. Real Odido, Vercel and
Commission v3 request hashes verified unchanged against the stored private packets.
Seven PostgreSQL and 19 JS tests were previously passed, not rerun this checkpoint.
Live admin and LLM Deployments both Ready at the start of this heartbeat. Production
unchanged; no new inference, build, queue admission or rollout. Source-cohort pins
and targeted release verification remain next; do not claim this is deployed.

## Source-level release, September 19 08:37 UTC

Admin and serialized LLM worker now run source `e93b6f0`; fixture/document commit
`0e4a517`, platform values `aadba3a`, all pushed. Dependency base remains `27b9fb3`.
Image manifest `sha256:cd58e77d37c2493b85fed4339cf044c52e7face99f8e8aa2eba7a0733c8a6c81`
was imported to all four eligible nodes. Server-side comparisons verified only
the two Deployments' container/init-container images changed. No ConfigMap,
builder, fetch worker, web, model/profile, concurrency or resource-policy change.

This release also activates the previously local scoped timestamp-cache reuse,
legacy report stale-write guard and private job coverage display. Their prior
seven disposable PostgreSQL checks passed; latest 557 offline and 19 JS tests,
plus JS syntax, pass. No database migration. Unrelated platform edits preserved.

Orchestrator was paused, its pod fully terminated, then the LLM/launch lane was
rechecked empty before the worker was drained. Both targeted rollouts completed;
orchestrator restored and Ready. Post-apply diffs empty. The guarded scoped factory
returned the pinned generation identity without inference before and after rollout.
All seven live packet/request hashes match the pinned local cases exactly.

The seven private jobs were admitted through authenticated admin, unchanged low
priority and single-attempt policy:

- Odido 21505: `job_020cf63278d94e3584f2235415a0ccf9`
- Odido 22331: `job_401bce323195455c9423d309646d67a1`
- Vercel 26190: `job_6b06a8643ef049b5bacdea95d4f6fa5a`
- Commission 21216: `job_85be3ec21fd448f4b5e85563d74c8160`
- Commission 21218: `job_0edebac8fe654745b5773b106e6775c3`
- Commission 23638: `job_639da8d43619476e8b4679b6fc6a74c3`
- Commission 25303: `job_c836509331af459c9facde004f93c96c`

Pending behind ordinary work at this checkpoint; do not duplicate or reprioritize.
Read-only collector `.cache/collect-source-pilot.py` checks stored job/artifact and
cache identities; aggregate evaluator must retain all eight pinned expectations.
All three event fingerprints remain unchanged. Existing Commission private HTML
hash unchanged; authenticated download remains 200, no-store and sandboxed.
New coverage JavaScript served at the template's actual `/ui/static/admin/` path;
no browser acceptance claimed. An initial diagnostic used the wrong static URL
(404) and nonexistent queue sort column; corrected read-only probes succeeded.

Public checker passed before release and at 08:37:20 UTC after release, including
pages/assets and sampled historical JSON. API/etcd readiness passed; all workloads
Ready, new admin/LLM pods zero restarts. This is point verification, not sustained
availability proof. Public reporting remains gated; no new public Events output.

Rollback: restore only admin/LLM tags to `6141d61` in platform values, render/diff,
drain the serialized lane, apply only those Deployments and restore scheduling.
Keep scoped profile/config and private artifacts; older worker rejects new
source-level payloads rather than silently processing them as whole-event jobs.
Avoid rollback with pending source jobs unless they are first handled explicitly.

## Source cohort outcome, September 19 08:50 UTC

Seven calls, 15,021 ms total: Odido 2,435/2,194 ms; Vercel 2,296 ms; Commission
1,811/1,822/2,215/2,248 ms. Six jobs succeeded structurally. Odido passes 3/3 and
Commission 4/4 unchanged cases; Vercel has no valid assessment and the aggregate
gate fails. Across five bounded cohorts: 19 calls / 95,192 ms, excluding ordinary
work and queue wait. This is not complete platform cost accounting.

Vercel raw output has `id:` on p3, rejected as `invalid_assessment_item`; its p1
also proposes exclude/different_incident for the expected positive. No coercion,
retry, model change or public write. New regression test preserves strict key
validation. Worker HTTP logs show no response_format on this request; the current
router only supplies JSON-object mode for configured stages, not schema-constrained
private assessment. Inspect the installed path before implementing format support.

Read-only source inspection found no stored HTML/raw HTML for the four sampled
anchor/candidate articles. Candidate 26190 has 3,856 text characters and anchor
26194 has 6,667, including extraneous footer text. Full-body context cannot simply
be appended to the current 12,000-byte request without budget analysis. Shared
official-link provenance is unavailable in this snapshot; do not fabricate it.

All three production event fingerprints unchanged. Six immutable HTML artifacts
validated against their job hashes and source-specific cache/generation identities:

- Odido 21505: 70,361 bytes, `aedabc75352941f76aad61c4a52fee4f7ccd2eee9696507504506beab29bab9b`.
- Odido 22331: 70,560 bytes, `d2757e26d06c36962ec439d2026c591a9f357136b5c2175ef6fe8a66d1d3fdd5`.
- Commission 21216: 109,337 bytes, `3facfbf425d084c410d829609cdd7bd915c051aab5e7b840054317c0491d0906`.
- Commission 21218: 109,337 bytes, `a15732665f0550fb1ee330ec53c80fbc60757202ee269f0eb714e69540439b26`.
- Commission 23638: 109,433 bytes, `05a9cd332426558bf6cf45c4135b3525638d0188cf26853edd0f498673fa9604`.
- Commission 25303: 110,588 bytes, `4bf6d0fccc02aa4bfed60a6b55bd423f07852da2522dfd80d135df6d057906b6`.

Next is constrained-format/provider and paired-source input analysis, not publishing
the passing subset as a completed automated feature. Source display follow-up
`2b5954a` remains local; runtime remains `e93b6f0`. No deployment this checkpoint.

## Paired diagnostic rollout, September 19 09:15 UTC

Runtime admin/LLM `670558c`, platform `c98e9e4`, both committed/pushed. Imported
manifest `sha256:8b3649a235eef102995a5cafca39ca94e72432d486994042216fcc90eaa304d0`
to all four eligible nodes. Dependency base stays `27b9fb3`; no Hugo, builder,
model, context-size, concurrency or resource-policy changes. Only two Deployment
images and `SV_EVENT_REVIEW_PAIR_ENABLED`/`SV_EVENT_REVIEW_PAIR_PROFILE_ID` changed
in server-side comparisons. Only those objects were applied; post-apply diffs empty.

Dedicated profile `49780c82-fd75-53dd-829b-e8fd8bf7e7bb` created/read back via admin:
same existing scoped prompt/provider/model, temperature 0, output 1,024, input cap
15,000, fallback empty, schema_id null (no router repair loop), is_enabled true.
The first readback mistakenly checked an unsupported `enabled` field; repository
inspection identified `is_enabled`, which was verified without recreating or
changing the profile. Existing scoped profile parameters remain unchanged.

Orchestrator terminated, LLM/launch lane rechecked empty, worker drained before
rollout; both replacements Ready and scheduling restored. Factory preflight made
no model call and verified paired generation
`767cc31100bc1ce61015ea93182201977b356b36224b57db6b1fd6a20c252f06`.
Existing scoped generation is still
`58bedc2903733c7b9f840791cc27583f0b0538167c397742249974fb275aa4e1`.

Live Vercel packet/request matched the pinned local case at 14,753 bytes. Exactly
one paired job admitted through admin: `job_557d20eb1f2449d0979fadfa3c1fe6a9`.
Do not duplicate or reprioritize it. `.cache/collect-paired-pilot.py` collects its
result read-only, validating the exact cache/request/generation and HTML artifact.
Real provider format, tokens, latency and semantic result remain pending.

Public checker passed before and at 09:15:46 UTC after rollout. API/etcd Ready;
all application workloads Ready and new pods zero restarts. Existing authenticated
Commission attachment hash unchanged, still no-store/sandbox. Selected-source
coverage labels are now served with the bumped script URL; no browser acceptance
claimed. Last local gate: 580 offline and 20 JS tests, syntax/diff checks pass;
the prior seven PostgreSQL tests were not rerun (no SQL/schema change).

Rollback: set pair flag 0 and restore admin/LLM `e93b6f0` in platform, render/diff,
drain, apply only ConfigMap and those Deployments, then restore scheduling. Keep
prior private artifacts/profile. Handle any pending paired job explicitly before
rollback; the older worker rejects the new payload instead of dropping its mode.
No new public reports or automatic admissions have been enabled.

## Paired diagnostic result and bounded cohort, September 19 09:30 UTC

Vercel `job_557d20eb1f2449d0979fadfa3c1fe6a9` succeeded in one 4,505 ms call.
The unchanged positive case passes (include/same_incident). Four passages assessed:
three included, one excluded; 36 remain unassessed. Filtered worker HTTP telemetry
confirms strict JSON-schema response format, 3,114 prompt tokens and 79 output
tokens. This establishes the real constrained-format path for this request, not
general model accuracy. All six cohorts so far used 20 calls / 99,697 ms before
the five new jobs below.

Authenticated download: 200, private/no-store, sandbox CSP; 109,734 bytes, SHA256
`79e7391491770fd262c4366d705a17c35a7f08aec2e884a863102d8655f68f7d`.
Cache/request/generation identities revalidated by the read-only collector.
Vercel row fingerprint remains `de70b60cd209cb493f75a529c78524e8`.
No production objects, model settings, priority or public content changed here.

Next bounded cohort uses the same paired method on all seven original sources,
retaining every original label. Six pairs fit; Commission 23638 deterministically
rejects `assessment_pair_over_budget`, with no inference/truncation/fallback. That
case remains **unassessed**, not a model pass even though its allowed labels include
hold. The existing Vercel result is reused without another call. Five additional
jobs were preflighted against live packet/request/generation identities and queued
through admin at unchanged priority:

| Source | Job |
| --- | --- |
| Odido 21505 | `job_0b508e1d4f7342d09d0c738938e474f4` |
| Odido 22331 | `job_b800393904f3431c9e9919af5fcd6ce3` |
| Commission 21216 | `job_0583780f4de34f109e4a0817b3a4b6cc` |
| Commission 21218 | `job_1b012fb29ab844d8868fd02e8ec42db1` |
| Commission 25303 | `job_c1d617c9d91e48958c0cadf4536c9ae3` |

Do not duplicate admissions. Ignored `.cache/paired-cohort-plan.json` records all
exact inputs, pins, cases and the explicit refusal; `.cache/paired-cohort-jobs.jsonl`
records admissions. `tools/check-event-assessment.py --paired-bundle` validates
complete source accounting, same generation/scope, unique results and exact request
pins. It independently recomputes budget refusal and reports it separately; the
overall semantic gate remains false when any case is unassessed. Eight new checks
cover tampering/missing/duplicate/mixed inputs and genuine over-budget behavior.
All 588 offline tests pass; no runtime deployment required for this evaluator.

Next: collect these results once, retain failures and all coverage. The private
pilot is still not automatic public reporting: independent incident qualification,
revision/publication integration and reader-facing verification remain pending.

## Private revision handoff, September 19 09:40 UTC

Local worker integration now saves immutable assessment receipts with input,
generation and full HTML identities. Admin adds authenticated no-store JSON
download and a guarded Jobs link. No SQL, model call, public-output or deployment
change. Old/extractive results remain valid without a receipt. 598 offline and 21
JS tests pass; syntax check passes. Runtime remains `670558c`, platform `c98e9e4`.
See `EVENT_PRIVATE_REVISIONS.md`; public revision pointers and transactional
evidence qualification are explicitly not implemented by this receipt.

Read-only queue sample: five paired diagnostic jobs still queued; 80 normal CVE
threat-actor jobs queued and one running. No reprioritization or retry performed.
`.cache/collect-paired-cohort.py` is a read-only collector validating each completed
cache entry, pinned generation/request, and immutable HTML hash. It does not enqueue.

## Receipt release prepared, September 19 09:50 UTC

Source `78a0739` is committed/pushed. Built its source-only image on the unchanged
dependency base; a network-disabled container imports the receipt, worker and admin
modules and confirms the new route. Image config digest
`sha256:ea86519e70648d3e6923c0b4863f1c2140061475d20b892ac90dea53adfcdb10`;
manifest `sha256:be977ffc8bc3c159543cfbac9e5385de66971f16c10efcbe892285b46cf19429`.
The image is imported on all four eligible nodes, but no Deployment is changed.

Rendered prospective admin and LLM manifests using platform values plus explicit
image overrides. Server-side dry-run comparisons show only each Deployment's main
and permission-init container image moving from `670558c` to `78a0739`. No service,
ingress, environment, model setting, resource policy or builder change. Prospective
files are ignored `.cache/events-receipt-admin.yaml` and
`.cache/events-receipt-worker.yaml`; re-render/recompare at deployment time.
Platform source values deliberately remain aligned with the currently running tag.

Public checks passed at 09:53:16 UTC, including Events, homepage, search, metrics,
sampled historical JSON and current feed index. API and etcd readiness passed.
These are sampled HTTP checks, not semantic acceptance or whole-history proof.
Read-only queue inspection shows ordinary CVE backlog reduced from 80 to 47, with
the five diagnostic jobs still pending. Do not interrupt/reprioritize them.

Deployment is deferred until this cohort is complete. Then preserve `670558c` as
rollback, drain normally, change only the two source-of-truth image overrides,
apply only the affected Deployments, verify a no-inference receipt cache-hit pilot,
and confirm local/remote/platform/runtime agreement. No direct Hugo execution.

## Paired cohort complete and receipt rollout, September 19 10:00 UTC

Five additional jobs succeeded with one call each: Odido 21505 3,112 ms; Odido
22331 4,692 ms; Commission 21216 4,067 ms; 21218 3,482 ms; 25303 4,539 ms.
Total 19,892 ms, plus the reused Vercel diagnostic 4,505 ms. Cumulative diagnostic
inference is 25 calls / 119,589 ms. All seven assessed provisional checks pass:
Odido 3/3, Vercel 1/1, Commission 3/3 assessed. Commission 23638 is still over
budget/unassessed, so the full eight-case gate is **not passed**. No case labels
changed; no missing result was normalized into success; public eligibility false.

Read-only cache/request/generation and HTML integrity verification succeeded:

| Source | Bytes | HTML SHA256 |
| --- | ---: | --- |
| Odido 21505 | 70557 | `2550a33db7f17c632e1806b6b143d343c3643786958dbdbd714e5636ddf04324` |
| Odido 22331 | 70532 | `ff6996a4ea69f518c22f272e1d73abcdbaefaa4f27197abce06bd400977e8a9d` |
| Commission 21216 | 109529 | `8ca8b7bd9087d67b568dba3ddc558544dbfc9ccf4a4f51b81d0bd953f77dc169` |
| Commission 21218 | 109543 | `e9b65ffaac8f069deabef8a4e348436675e182c975471b931095cf5991a44a88` |
| Commission 25303 | 110798 | `508160acaa59abe3c8c84010992b9ee063e869a062dbbb77cf59b1bc0abcb98d` |

All three event-row fingerprints remain exactly unchanged from prior pilots.
The receipt release `78a0739` is now live on admin/LLM only, platform `9681dc1`
committed/pushed. Orchestrator stopped, exited, and LLM/launch queue checked empty
before stopping the old LLM pod. Re-rendered from the corrected platform values;
server-side comparison changed only main/init image fields in those two
Deployments (restoring the deliberately drained replica count). Both rolled out;
scheduling restored and all expected Deployment replicas Ready. Post-apply diffs
empty. Builder, fetch/OpenAI workers, web, models and all configuration flags unchanged.

Public checker passed at 10:03:41 UTC; cluster readiness passed. New JS revision
link is served. Unauthorized receipt download returns 401; an authenticated legacy
job without a receipt returns 404, as designed. No browser acceptance claimed.

Exactly one receipt cache-reuse job admitted after read-only preflight proved the
original Vercel packet, request, generation and validated cache still match:
`job_aefc9fa9885b40e584eaa4e917c9dd20`. Admission is recorded in ignored
`.cache/receipt-pilot-job.jsonl`. No duplicate/reprioritization. Its completion,
zero `llm_runs`, cache-hit result and authenticated receipt bytes remain pending.
Older completed jobs are not rewritten/backfilled. Rollback admin/LLM to `670558c`
with the same drain procedure; preserve harmless private receipts and profiles.

Application repo and source values are committed/pushed. The platform repo's
unrelated appliance/certificate work remains untouched and uncommitted; it is not
claimed globally clean. Full Events automation still requires qualified incident
scope/evidence, transactional public revisions, and reader-facing acceptance.

## Publication-path analysis, September 19 10:10 UTC

Receipt pilot still queued; no duplicate or priority change. Repository inspection
confirms the new publication path cannot simply reuse the legacy report parser and
metadata slot: synthesized timeline dates and old narrative-section blending would
undermine source-qualified output. Event CAS alone also does not cover independent
article-body changes. `EVENT_PUBLICATION_HANDOFF.md` records concrete integration
boundaries, transactional concurrency tests and separate promotion/export gates.
Documentation only this checkpoint; no schema, public content or runtime change.

## Private snapshot repository, September 19 10:20 UTC

Local opt-in worker integration stores canonical packet/receipt together in
`event_private_revisions`, with event FK, bounded text fields, composite identity,
insert-if-absent and exact duplicate comparison. The dedicated transaction does
not touch public metadata or pointers. Disabled by default in code/chart;
no deployment or production schema change. Schema definition is not wired into
startup migrations; explicit controlled migration is still a release prerequisite.

Eight real PostgreSQL tests passed against a disposable PostgreSQL 18.4 container,
including concurrent insert/reuse, no overwrite on mismatch, unchanged event data
and unchanged original recorded time. Existing seven integration checks also pass.
The first invocation omitted the explicit DB-test switch and skipped tests; only
the subsequent `--run-db-tests` run is counted. Container removed and SSH loopback
tunnel stopped after testing. Diagnostics never initialized the production DB.

Additional offline checks cover disabled/no-connection behavior, strict flag values,
worker integration with cache reuse, and rejection of rehashed malformed generation
or removed gate metadata before DB access. The latter receipt guards were added
after the successful PG run; run PG gates again before a persistence rollout.
Current receipt cache-hit job still pending at the initial checkpoint; no duplicate
admission. Actual public revision qualification/promotion remains unimplemented.

Final local offline run: 607 passed. Chart renders the new store flag as `"0"`.
No JavaScript changes this slice; prior 21 JS results remain the current UI gate.

## Current-source locking gate, September 19 10:30 UTC

Local transaction helper rejects incomplete snapshots, unsafe autocommit and a
missing/non-immediate/nonvalidated event-membership FK. Event FOR UPDATE plus
membership/source FOR SHARE NOWAIT protect the snapshot while a future caller
would compare/promote its qualified predecessor. Full data identity ignores only
event updated_at bookkeeping, not source content or coverage. This is not yet a
public promotion caller or approval policy.

Nine real PostgreSQL tests passed: concurrent article updates and membership
insert/deletion attempts time out while the window holds; changed content afterward
fails snapshot comparison; report timestamp-only changes remain acceptable. The
prior persistence and seven baseline PG tests reran successfully. 609 offline
tests passed, including changed off-quote context and early truncation rejection.
Disposable container removed and loopback tunnel stopped. No production schema,
locks, model calls, image changes or public writes from this slice.

Read-only production catalogue confirms both event_articles foreign keys exist.
Receipt verification remains queued; normal CVE threat-actor work completed 166
jobs since 10:00 and had 32 queued at the initial sample, so the observed delay is
low-priority queue wait, not a stuck model. No priority adjustment or duplicate.

## Receipt acceptance and quote-only projection, September 19 10:50 UTC

Receipt pilot `job_aefc9fa9885b40e584eaa4e917c9dd20` succeeded with cache_hit=true
and zero llm_runs. Both authenticated attachments returned 200, private/no-store
and sandbox CSP. HTML remains 109734 bytes, SHA256
`79e7391491770fd262c4366d705a17c35a7f08aec2e884a863102d8655f68f7d`.
Receipt is 2782 bytes, raw SHA256
`02829e1dfc429a191307c7a07db0e88b7662c21edfa9e59487bc2defd79a58af`,
revision identity `62d282c6c399fcb341ba31809fb140fa2abe546ad2160dc24bcdbc959041d3dc`.
Guarded generation remains `767cc31100bc1ce61015ea93182201977b356b36224b57db6b1fd6a20c252f06`.
Vercel event fingerprint remains `de70b60cd209cb493f75a529c78524e8`.
Unauthorized/legacy receipt behavior was previously verified as 401/404.

Authenticated Chrome Jobs filtering now visibly shows this succeeded job, cached
reuse, 4/40 assessed and 36 unassessed passages, 3 include/1 exclude suggestions,
and both private-review and revision download links. No console errors. No other
UI actions or new submissions were taken. Native screen was locked; the available
browser extension session provided the page verification without unlocking it.
Deployment readback shows expected ready replica counts and unchanged release tags.

Local `event_projection.prepare` adds the bounded quote-only preparation contract:
separately trusted qualification identity, exact full snapshot/scope binding,
versioned reviewer, exact spans, no arbitrary prose or inferred incident dates,
unknown origin independence and explicit uncovered source IDs. Default trust is
empty; caller authentication/qualification and publication promotion are not
implemented by this helper. Every result remains not_promoted/public_eligible=false.
This is not the complete public feature and has no runtime caller yet.

21 new checks and all 630 offline tests pass. Initial collection failed because
the new module was placed at repository root; moved to the package before testing.
No SQL/JS changes; prior nine PostgreSQL and 21 JS gates were not rerun. No model
calls, production migration, image change, public export or build this slice.

## Qualified Markdown export boundary, September 19 11:00 UTC

Expected production replicas remain ready. No production changes. Locally added
an optional export branch accepting separately supplied immutable bundles and
promoted pointer identities. Rendering reconstructs and compares exact identities,
uses only source-backed quotations, and bypasses every legacy narrative/detail
field. Existing stable slug and incremental file replacement logic are retained.
Default callers continue producing the same pinned legacy checksum.

Ten new tests cover qualified byte/inode/mtime reuse; unchanged prior files after
invalid pointer, changed quote/scope/event, missing/unmatched pointer and duplicate
event failures; stable URL; absent legacy facts; and source Markdown/HTML/Hugo
shortcode neutralization. All 640 offline tests pass. No SQL or JS changes and no
Hugo invocation; prior PostgreSQL/JS gates were not rerun.

The new branch is not enabled in any runtime caller. Trusted qualification and
publication-pointer storage remain pending. Inspection also confirms
`write_events_index` still exports legacy summary/detail fields, so a matching
qualified JSON index plus joint output preflight are required before activation.
Do not enable Markdown-only qualified publishing or claim end-to-end acceptance.

## Matched qualified index and preflight, September 19 11:10 UTC

Production deployment ready counts remain expected; no runtime changes. Local
Events index preparation now reconstructs the same exact projection and trusted
pointer identity as Markdown. Qualified entries exclude old summary, severity,
dates, CVEs and products; represented source counts and exact quotations are
explicit. The daily article/CVE JSON contract is untouched.

Combined export pre-serializes index data and checks index paths before the
all-page render/replace phase. Rejected pointer, quote, slug, unmatched event or
unserializable data leaves both prior outputs intact. Identical runs preserve
page and index bytes/mtime; legacy index serialization is pinned unchanged.
Symlink output tests pass. This is not atomic multi-file source writing: disk
errors still require no build admission, and coordinated build/export integration
remains outstanding. Production callers still use the unchanged legacy path.

Ten new tests and all 650 offline tests pass. The initial serialization fixture
used a bare object, which the existing JSON fallback stringifies; changed that
fixture to an unsupported dictionary key to exercise an actual JSON failure.
No SQL/JS changes, model calls, deployment or Hugo invocation. Previous nine PG
and 21 JS tests were not rerun. Trusted qualification/pointer storage and
transactional promotion are the next integration prerequisite.

## Qualified pointer transaction, September 19 11:20 UTC

Local publication store defines separate qualification, revision and pointer
tables, with no startup migration or production caller. Promotion uses the tested
current-source transaction window, rejects write-capable qualification principals,
loads an independently stored nonrevoked qualification, reconstructs the exact
projection, checks the expected predecessor, and inserts/verifies the immutable
revision before pointer update in one transaction. A guarded one-way revocation
locks the same event row. A missing/disabled trigger fails closed. No automatic
approval, retry or build; new restricted production credentials remain required.

All ten disposable PostgreSQL tests pass, rerun after adding concurrent promotion
and timestamp-only retry cases. Duplicate concurrent attempts yield one promotion
and reuse/defer, never a second revision; stale predecessors and stale source text
are rejected without moving the pointer. Original stored bytes/timestamps survive
bookkeeping changes. Qualification writes are denied to the promotion role;
revocation blocks on the current-source event lock and cannot be undone/rewritten.
Revoked qualification retries fail. The prior nine integration tests reran too.
All 655 offline tests pass; five new identity checks reject before DB access.

The PostgreSQL 18.4 test container used a named disposable database and loopback
tunnel; both were removed after testing. No production schema/data/role/image
change, model calls or Hugo invocation. All production deployment ready counts
were expected at the opening check. Private review remains the deployed feature.
Trusted qualification admission, revocation-aware export/withdrawal, restricted
role provisioning, coordinated build admission and reader-facing rollout are
still required before the automatic public Events feature can be claimed ready.

## Revocation-aware export snapshot, September 19 11:30 UTC

Local bounded read-only export selection now joins and verifies pointer/revision/
qualification identities under repeatable read, then compares current source data.
Managed identity remains explicit even when revoked or withheld. Revoked
qualifications, hidden/unavailable events and unavailable/suppressed/removed cited
sources produce withdrawal reasons. Other changed evidence is withheld; corrupt
references fail rather than falling back to legacy narrative. No production caller.

Ten real PostgreSQL tests pass after adding valid-read, unsafe-autocommit,
timestamp-only reuse, changed text, suppression, removed membership, hidden event
and revoked qualification read cases. The prior promotion/race/permission and
baseline integration checks reran. All 662 offline tests pass; seven new cases
exercise invalid/oversized/duplicate IDs and empty selection before connection.
Disposable PostgreSQL container and loopback tunnel removed after verification.

Production replica counts were healthy at opening. No production changes, model
calls or build. This reader is not a lease covering later build activation:
withdrawal handling, coordinated build admission and authorization recheck remain
mandatory integration work. Private review is still the deployed feature, not
autonomous public reporting.

## Explicit managed-state export selection, September 19 11:40 UTC

Local authorization wrapper now consumes the entire export snapshot: managed IDs
must partition into active, held or withdrawn states, with matching bundle/pointer
keys. Holds abort before writes; withdrawn events are omitted from page and index
output rather than falling back to old narrative. Unmanaged events preserve the
legacy rendering path. Duplicate event IDs and inconsistent state are refused.

Eight new tests and all 670 offline tests pass, covering qualified output, caller
input immutability, withdrawal cleanup and preserved prior files on holds/missing/
overlapping/invalid state. No SQL/JS changes; prior ten PostgreSQL and 21 JS gates
were not rerun. No production changes, new model calls or Hugo invocation.

Read-only inspection confirms `tools/hugo-build.sh` activates a successful release
without a new Events-specific authorization hook. `BUILD_PIPELINE.md` explicitly
protects that pipeline against unapproved modification. It was not changed.
A reviewed activation/withdrawal strategy remains required; neither an earlier
database snapshot nor helper tests establish authorization at activation time.
All expected production replicas were ready at this checkpoint's opening.
# September 19: guarded Events publication pilot

- Deployed ingest `e091471`, builder `00dffeb`, platform values `7cc2096`.
  Explicit additive schema provisioning and three restricted DB roles precede the
  rollout. Credentials stay in Kubernetes, not git. Manifests were rendered and
  diffed before applying only shared application config and seven application
  Deployments. Web/SearxNG, commands, mounts, limits and LLM concurrency unchanged.
- Stopped orchestrator and waited for termination, then verified no running jobs.
  Rollback source archive: `/log/release-snapshots/events-20260919T130606Z` outside
  Hugo inputs. Imported immutable images on all four schedulable nodes, waited
  for all workers, resumed admission. Render/live values agree for affected fields.
- First API build `job_32279890e4874b38b337bc0f8b170d0e` safely refused activation
  because system Python lacked psycopg. The previous live release remained
  `20260919123939`. Corrected hook interpreter selection in `00dffeb`; real image
  imports and 747 offline tests pass. PostgreSQL 11 and JS 26 tests also pass.
- Pilot event `evt_0ffca0813049`: operator-policy identity `codex-reviewed-pilot`,
  NOT a human approval or autonomous local-model decision. Exact short CSO excerpt
  first, then an additional SecurityWeek excerpt. Unknown dates/independence stay
  unknown; eight other linked sources remain explicitly unrepresented.
- Promotion jobs: `job_0a1d3a03304e48fca36165d1a6bdccf6` and
  `job_9672f74858324b359a375a989fe4844c`, both succeeded.
- API builds: `job_1bdcf1e9116449dfb61a1ff85e211626` 18.81s;
  `job_c6ad0c0b6f5b413fb4b7138645f19f27` 18.26s. Intervening automatic build
  `job_9021e978a4fb4ce084eacfd10aea5da7` 17.33s.
- Public page and `/sempervigil/index/events.json` advanced from
  `6b2828e7bdc9fdd319ebbcef18306dfad9b36a779928ec1a7a4bca68781fb439` to
  `d06c840ec6d2b0ecf75db1901fb157c333082316ccbd123d9659ec1176b01ca2` at stable URLs.
  Desktop browser shows both citations and intact theme/navigation.
- Peak cgroup memory 380,858,368 bytes; all 5,060 feed day files remain. Nodes
  Ready without MemoryPressure; API ready; no new kernel OOM entries on build host.
- **Open finding:** HTTP checks briefly failed around 13:19 UTC after the second
  switch. Nginx logged `/events/` and favicon 404s at 13:19:24, then success by
  13:19:32. Source/output/authorization remained correct. Existing `ln -sfn` is an
  unlink/recreate switch on shared NFS, not atomic rename. Exact cache contribution
  is not proven. Narrow approval was granted to replace the switch and verify it.

## September 19: approved atomic guarded switch deployed

- Application runtime correction `bfa9986`, platform values `4d852c2`, both pushed.
  Only the builder image changed from `00dffeb`; rendered diff contained its main
  and init-container image references only. Scheduling was paused for replacement
  and resumed. All application Deployments are ready. The web was not restarted.
- Enabled activation now creates a temporary sibling symlink and uses
  `os.replace` to atomically replace `current`. Authority locks and the existing
  two-second subprocess timeout remain. Disabled legacy branch, Hugo invocation,
  source content, feed generation, retention, caches and concurrency are unchanged.
- 750 offline tests pass; one Linux reader stress test is skipped on macOS.
  Linux builder-container stress passed 10,000 switches with 17,802 concurrent
  page reads, zero errors. Rapid macOS pathname reads returned EINVAL; local tests
  instead verify same-directory rename, existing live link at replacement,
  failure retention and cleanup. No OS error was silently ignored in that test.
- API build `job_5b4a179d18f24fb890af24000868d4e5` succeeded, reported build
  duration 19.31s (including pre-build work, queued-job execution was about 33.5s),
  release `20260919133406`.
- API build `job_7916f607bfd540c0bf7794910c45ad07` succeeded, reported build
  duration 16.77s (job execution about 24.5s), release `20260919133533`.
- HTTP monitoring across both switches and the subsequent serving window made
  875 successful requests across home, Events list, pilot report, Events JSON and
  a historical daily JSON. Zero HTTP/JSON failures or 404s. This is sampled
  availability evidence, not proof against every NFS/cache failure mode. The
  earlier pre-fix 404's exact cache contribution remains unproven.
- Public pilot HTML fragment still hashes to
  `2d2713f72bed79c6edce927e70d4fa91ab6b9f43797271c8733c0e19574a9cd2`;
  Events JSON hash `7de42fe062c6dcf27bc34c693f4d4d685889e91ed8d64f2eefda2064111b177f`
  matches the activated manifest. Both were HTTP 200, Cloudflare DYNAMIC.
- 5,060 archived JSON files retained; no temporary switch links remain. Builder
  peak 379,756,544 bytes (about 362 MiB), existing 2Gi request/16Gi limit unchanged.
  Nodes Ready/no MemoryPressure; API and etcd readiness pass; PostgreSQL 3/3 healthy.
  Render/live build-worker diff is empty. Previous builder image retained.
- Post-build publication checker at 13:37 UTC passed all 20 page, asset and JSON
  checks. No host kernel OOM records since rollout began at 13:30 UTC.
- Controlled Events publication/update is live. Automatic evidence qualification
  and changed-input admission remain next work; the pilot is not autonomous LLM
  qualification. No new background monitor or schedule was created.
  No zero-interruption claim. The static feed JSON remained available during checks.

## September 19: first automatic scoped update published

- App `7fd1010`, platform `7bcfd87`; only the orchestrator Deployment changed
  image plus explicit enrollment and restricted admission Secret reference.
  Render/live diff is empty. Other ingest images remain `e091471`, builder
  `bfa9986`. No web restart, schema migration, inference configuration, Hugo or
  daily-feed change. Scheduling was drained and resumed for the targeted rollout.
- 777 offline tests pass, one Linux-only stress test skipped on macOS; all 11
  disposable PostgreSQL tests and 26 JavaScript tests pass. Expanded real DB
  test exercises private-review queue admission, exact receipt consumption,
  restricted policy approval, promotion, pending/unchanged reuse and revoked seed
  refusal. Disposable database container and tunnel were removed after tests.
- Enrolled only `evt_0ffca0813049` against reviewed root revision
  `d06c840ec6d2b0ecf75db1901fb157c333082316ccbd123d9659ec1176b01ca2`.
  Production preflight found broad retrieval selected only over-budget new sources.
  Non-entity scope-focus retrieval found a qualifying source with a 13,628-byte
  paired request, below the unchanged 15,000-byte budget. No source truncation or
  context/model-budget increase was used. Two other sources remain over budget.
- Scheduler created private-review job `job_45a1badc4e6c4dc5955cfcc52605dab1` at
  13:56:49 UTC. It succeeded with one real local-model call, 5,154 ms, no cache hit.
  Four assessed passages: two include, one hold, one exclude. These were proposals;
  independent deterministic policy admitted one exact short excerpt from article
  26239, requiring every approved focus term and the per-source word limit.
- Scheduler recorded policy approval
  `3050ef46f7839d8a5b6fa8ddc64f1c89cd012ac05d05df11d6cd3e3e2544d913` and queued
  `job_d199adcd9bc24cbea4ba2111bc17b623` at 13:57:20 UTC. The existing restricted
  promotion worker succeeded with revision
  `c44cc56060cba571615cc697fc2d4930edacacdb94bbedf92104d9ce46a90252`.
  No human approval, direct DB promotion, or operational admission script was used.
- Normal dirty-build admission queued `job_c8dd8d64c45a4d8882af3781a4f356d5`
  with reason `qualified_event_promoted`. It succeeded, reporting 19.86 seconds,
  release `20260919135817`. No manual API build enqueue or direct Hugo invocation
  was needed for this automatic update.
- Public HTML and Events JSON independently returned HTTP 200 / Cloudflare
  DYNAMIC and matched that exact revision. Both contain three source quotations
  (articles 26201, 26217, 26239). All 5,060 daily archive JSON files remain.
- 985 sampled public HTTP/JSON requests during rollout, activation and follow-up
  had zero failures. The publication checker at 13:58 UTC passed all 20 page,
  asset and JSON checks. All Deployments ready, API readiness healthy. Builder
  lifetime peak 490,299,392 bytes (about 468 MiB), comfortably below unchanged
  policy. This sampled evidence is not an absolute availability guarantee.
- Subsequent scheduler passes held sources 26251/26742 as over budget, with empty
  queues and no repeated inference or build. Since rollout there was exactly one
  review, one promotion and one build, all successful. Hold reasons are logged;
  real review/promotion tasks and immutable receipts remain in admin Jobs.
- Limits: automatic additions operate only inside explicitly enrolled, unchanged
  reviewed scopes. General incident discovery/enrollment, revised quoted-document
  qualification, comprehensive coverage and generated narrative are not delivered
  by this slice. See EVENT_AUTOMATION.md for policy and rollback semantics.
# September 19, 2026: private source reuse and compilation, local only

- 31 targeted deconstruction tests pass. Full offline suite: 808 passed, one
  Linux-only skip on macOS. Current cache content is reconstructed against exact
  source/scope/config; invalid or symlinked entries fail without inference.
- Tests cover incremental source addition, changed-source rejection/replacement,
  unchanged-source inference reuse, model-config invalidation, order/timestamp
  stability, explicit omitted coverage and no public approval receipt.
- Live DB inventory was read-only. An initial aggregate incorrectly assumed the
  legacy manual column was boolean; the corrected integer comparison succeeded.
  No candidate cleanup, model call, DB migration, deployment or build performed.
- Semantic synthesis, real-model evaluation, correction adjudication and guarded
  publication remain open. Prior PostgreSQL/JS results were not rerun this slice.

# September 19, 2026: real queued claim support evaluation

- Admin `eff906d`, LLM worker `3148e06`, platform `023f17c`. Render/live scoped
  diff empty. Only admin/LLM images and private support config changed. Drained
  queues, paused/resumed orchestrator admission and waited for old LLM pod deletion
  before requesting inference. No overlapping pilot inference. Image tag was
  quoted after pre-apply rendering caught YAML scientific-notation coercion.
- Explicit `audit_source` uses the existing private-review API/job/viewer with
  current source validation and no fallback extraction. No schema, builder, web,
  feed, model/context/resources or concurrency changes. No direct Hugo invocation.
- Dedicated profiles created through the authenticated AI API: v1
  `3c32c1f5-99fb-5d7e-92a7-708e7c74e9d7` (now disabled), v2
  `51ab710c-fe80-59fb-897e-0653ab078f8b`. Same local model, no fallback, temperature
  zero, 15KB input budget, max output 1536. No automatic audit admission.
- Frozen 17-case expectations committed BEFORE inference in
  `tests/fixtures/event_claim_support_pilot.json`: five supported, twelve rejected.
  V1 jobs `job_15d6ce4138db43b4a90eb2891471e675`,
  `job_45e683e7b9b7479288acc9827ca45879`,
  `job_8ee626dab5684fa5b81a255485406f89` completed with one call each, respectively
  14.562s, 14.946s, 12.374s. One false acceptance, three false rejections. Several
  correct aggregate rejections had wrong reasons, especially null-date failure.
  This did NOT establish semantic validity.
- V2 checks one claim per call, serially, preserving per-claim cache on later
  failure. No asserted date is a deterministic supported/non-assertion dimension.
  Expectations unchanged. Jobs `job_41e282c072db44d59d771aa53d0e7b53` (six calls,
  17.754s), `job_e662e452722e4d30b91b321f37808490` (six, 17.820s), and
  `job_6a2999b7697548dbb25ad8c9585c525a` (five, 14.870s) all succeeded.
  Total 50.444s / 17 calls, about 2.97s each. Five negative cases rejected, seven
  negatives held, all five positives held. ZERO accepted claims is not a passing
  classifier. Publication-quality acceptance remains FAILED. No further prompt
  iterations were run; do not convert uncertainty into approval to raise yield.
- Cache replay jobs `job_91169a6ba9d94ec086899e337f96dc0e`,
  `job_05f8c429869b44fca339103d38990592`,
  `job_5f52908997054d899a23021fd5c8cf54` all succeeded with cache hits and ZERO
  llm_runs. Results remain private, with no approval receipt/public write.
- 856 offline tests pass, one Linux-only skip, existing deprecation warnings.
  Tests cover real worker dispatch with mocked inference, existing artifact
  reader, disabled admission, schema transport restrictions, partial-cache resume,
  and deterministic absent dates. PostgreSQL/JS suites not rerun this slice.
- Publication checks passed all 20 tests at 17:43 and 17:53 UTC. All Deployments
  ready and API readiness healthy; Ollama on GPU, successful calls and no grammar
  crashes in recent logs. All 5,060 archive days retained. Public Vercel revision
  remains `c44cc56060cba571615cc697fc2d4930edacacdb94bbedf92104d9ce46a90252`.
  These are sampled checks, not zero-interruption or DB-completeness guarantees.
- Next: isolate quote entailment from independent factual certainty, then evaluate
  without weakening frozen expectations. Automatic narrative publication remains
  unfinished. Existing quotation-only production automation is unchanged.

# September 19, 2026: local claim support audit

- Added `event_claim_support.py`: six independent support dimensions over exact
  stored claims and full source. Strict bounded schema and complete ID inventory;
  one unsupported dimension rejects, any uncertainty holds. All-supported is
  explicitly only a model suggestion, never public approval.
- At most one callback per source, separate checker identity and immutable cache.
  Unchanged replay makes no inference call, empty extraction abstains locally,
  changed source/claim/prompt/profile invalidates reuse. No truncation, automatic
  repair or publication mutation. Private rendered output escapes model text.
- 32 new audit contract tests pass. Full offline suite: 849 passed, one Linux-only
  skip and existing deprecation warnings. PostgreSQL/JS not rerun; no schema/JS
  changes. These use mocked completions and establish no actual-model accuracy.
- No production/API/job integration, deployment, new profile, direct inference or
  build this slice. Existing runtime remains as recorded in the pilot below.
  Next: explicit queue admission and fixed-error-cohort model evaluation before
  any synthesis/publication use. See EVENT_CLAIM_SUPPORT.md.

# September 19, 2026: deployed private deconstruction pilot

This entry supersedes the local-only status below. Admin `69e76a6`, LLM worker
`c4f8ddf`, platform `5c6044d`. Rendered admin/worker/ConfigMap/orchestrator manifests
match live. Only admin/LLM images and private enablement/profile settings changed;
admission was paused, jobs drained, old pods removed, then admission resumed.
Builder, web, model/context/resources, single-LLM concurrency and feeds unchanged.

- Profile v2: `fc201a04-3f95-5251-b66f-f3afc0e883f2`, created through supported
  AI API with exact source prompt, temperature 0, max tokens 1536, no fallback.
  The earlier v1 profile was disabled through the same API.
- Initial free-output job `job_080a41f7a58c494dacf264a08b41a112` failed closed
  after 13.041s: invalid fields and altered quotations. Constrained job
  `job_073cb367cb0d4bdda790789112e6dcae` failed after 22.224s: date precision
  disagreed with null dates. Neither wrote public data.
- Intermediate job `job_e1cf48d24dce4e1c95cc6e39d7c878a3` encountered HTTP 500:
  Ollama logs show grammar parser failure on a regex escape, then SIGSEGV in its
  inference subprocess. Transport retries repeated the failure; pod restarts
  stayed zero. This is not evidence of OOM. Removed regex/conditional grammar
  and model-generated precision, retaining strict local date/calendar validation.
  The next request reloaded the model successfully without a platform restart.
- `job_1c05a7e1ff9a4b6eafa6b76dd4a7517f`: source 26217, six claims, one call
  28.736s including reload. `job_320730845c8249a1ae6c555ab0aff044`: source 26201,
  six claims, one call 22.224s. `job_8c9b040505744389ad9bcb62b7a3f14d`: source
  26239, five claims, one call 22.191s. All succeeded as PRIVATE unreviewed drafts.
- Replay `job_2b4fc9fc6b924882aea0844928985ab8` succeeded with cache hit and
  zero llm_runs. It retained three-source compilation revision
  `dec4b2a36ddee950ed0df5feaf9444469edd13b3e0bbad1c5231ba86592d9205`.
  Coverage: three included, four pending, two over budget, one unextractable.
  Authenticated artifact downloaded to ignored
  `data/events-review/deconstruction-vercel-pilot.html` for local inspection.
- Semantic review FAILS publication acceptance: unrelated citations for access,
  impact and recovery claims; imprecise ShinyHunters identity/uncertainty; sensitive
  versus unprotected secrets conflated. No overview or dated milestones. Exact
  quote presence alone is demonstrably insufficient. These claims cannot enter
  the public quotation approval path and produced no publication receipt.
- Full offline suite: 817 passed, one Linux-only skip. Final targeted regression
  suite: 48 passed, existing FastAPI deprecation warnings. PostgreSQL/JavaScript
  suites not rerun this slice; no schema or JavaScript changes.
- Public checker passed all 20 tests at 15:03 and 15:22 UTC, including 5,060
  indexed archive days, current/historic JSON, Events, search, metrics and assets.
  Existing Vercel pointer remains `c44cc56060cba571615cc697fc2d4930edacacdb94bbedf92104d9ce46a90252`.
  All Deployments ready; API readiness healthy. Post-correction model runs use
  GPU with unchanged 16K context; final recent logs show successful generation,
  no repeated grammar failures. Sampled checks are not a zero-downtime guarantee.
- No direct Hugo invocation, manual build, migration or candidate deletion. Next
  is semantic support/uncertainty validation before coherent narrative generation.

# September 19, 2026: private deconstruction extraction, local only

- Added opt-in source-level claims to existing private-review admission and LLM
  dispatch, with the existing authenticated artifact reader. Separate flag and
  pinned model profile default disabled; no automatic admission or public writes.
- Targeted review/extraction regressions: 103 passed. Full offline suite: 799
  passed, one Linux-only test skipped on macOS. Existing deprecation warnings.
  `git diff --check` passed. Real worker dispatch is tested with mocked inference.
- No production deployment, profile creation, DB migration, live-model call or
  Hugo/API build performed. PostgreSQL and JavaScript suites not rerun for this
  slice. Source extraction is not qualified narrative or a publication approval.
- Next gate: bounded real-model pilot; then evidence reuse, multi-source synthesis
  and correction handling. See EVENT_DECONSTRUCTION.md.
## Event research reuse and legacy retirement (2026-09-20)

Application `45ced44` added content-addressed Event source versions,
Event/profile-specific relevance receipts, additive migrations 044/045 and the
admin retirement preview/restore workflow. Application `43ad9af` added the two
retirement handlers to the worker safety allowlist after the first queued job was
correctly refused; no Event row changed before that correction. Platform
`b161e58` targets admin `45ced44` and fetch worker `43ad9af` independently.

The rendered diff contained only those main/init image changes. Both deployments
rolled out Ready with zero restarts; rendered/live diff is empty. Migrations 044
and 045 completed. Preview `lerr_06b4d0e03e4744e58f01b396d84bf79a`
contained exactly 1,426 unpublished, non-manual legacy draft candidates and
excluded managed/public/revision/in-flight Events. Job
`job_2b5cb566f762489e828c4f3c97b4ed28` applied all 1,426 with zero
skips in about two seconds. No article, CVE, source link or Event row was deleted;
the fingerprinted restore manifest remains available.

The complete offline suite passed 1,052 tests with two skips; an isolated
PostgreSQL lifecycle passed source-version reuse, relevance receipt reuse,
preview, suppression and exact restore. Homepage, Events index and the WaterPlum
Event returned HTTP 200. Kubernetes readiness passed, all nodes reported no
memory pressure, and no jobs remained running. No Hugo command or site build was
invoked for this database/UI-only release.

Follow-up `d112f9d` moved the retirement-status GET away from the pre-existing
dynamic Event-detail route and extended eligibility to visible unpublished
archived drafts. The admin and fetch worker rolled to that image; platform
`9ec9ff2` differs from live by zero rendered lines. Status readback succeeded.
Preview `lerr_20845a003b294e2ab389c4f5f8877fc0` contained the exact two
remaining active archived drafts; job `job_b05350d0c04b4031b06ec5148b7c8ad2`
suppressed both with zero skips. Confirmed drafts and every published Event remain
excluded, and both retirement runs retain independent restore manifests.

## Event fact-curation recovery (2026-09-20)

- Production inspection separated four held cases into three concrete failure
  modes. Target and Kairos received HTTP 200 responses that consumed the full
  1,200-token completion limit and returned no usable JSON. Nike returned
  `evidence_verdict=hold` while also selecting facts. A Notepad++ roundup returned
  `same_incident` without an incident-establishing fact. Existing validators
  correctly prevented every response from entering an Event ledger.
- Application `f5cc352` adds explicit deterministic anchor flags, raises only this
  curation stage's completion allowance to 2,400 tokens, and converts contradictory
  selections to a conservative non-publishing result. It does not relax source
  independence, evidence support, ledger lineage, narrative audit, promotion or
  activation checks. Migration `pg_event_fact_curation_requeue_052` reactivated
  only the exact corrected hold reasons.
- 1,078 offline tests pass with two skips and five existing warnings. The immutable
  source-only image was built from the deployed dependency image with network
  disabled, imported on docker42/46/47/52, and deployed only to admin,
  orchestrator and the OpenAI worker. Platform commit `726290f`; render/live diff
  is empty. The old orchestrator was stopped before migration activation and the
  hosted-model queue was empty before worker replacement.
- Seven fresh Notepad++ v2 curation jobs succeeded. The unrelated SolarWinds
  source was classified ambiguous with no incident signal; a genuine source was
  enrolled with 13 selected facts. The prior ledger was then withdrawn as stale,
  allowing deterministic reconstruction. Other reactivated cases remain active
  in normal scheduler order and are not yet completion claims.
- Homepage, Events index, WaterPlum detail, feed index and metrics returned HTTP
  200. Updated pods are Ready with zero restarts, all nodes are Ready with
  MemoryPressure false, and normal ingestion/CVE work continued. No Hugo command,
  build-worker, web Deployment, daily JSON contract, model or concurrency change
  occurred.

## Event fact-curation passage normalization (2026-09-20, local)

- The 48,000-byte curation ceiling is an application safety guard, not the hosted
  model's context limit. The v2 request repeated full passage text under every
  fact that cited it. The held Vercel article had 21 facts over eight unique
  passages and measured 48,792 bytes before inference.
- `event-fact-curation-v3` supplies one deterministic shared passage table and
  per-fact `passage_ids`. Exact text, fact statements, incident-anchor flags and
  schema-constrained selection remain present. Conflicting reuse of a passage ID
  fails before inference. Stored evidence and public contracts do not change.
- Read-only measurements across all eight current Vercel sources reduced requests
  by 33.0% to 65.0%; the formerly blocked source is 17,087 bytes and the largest
  normalized request is 17,624 bytes. The 48,000-byte ceiling therefore remains
  unchanged.
- Migration `pg_event_fact_curation_passage_requeue_053` targets only held cases
  whose reason exactly equals `event_fact_curation_input_over_budget`. It does not
  reactivate audit, evidence-quality, incident-identity or editorial holds.
- Seven focused tests and the complete offline suite pass: 1,107 passed, one
  skipped, four existing warnings. No model call, queue admission, migration,
  build, Hugo invocation, public write or production rollout occurred in this
  local slice.

### Compact-request rollout and scheduler finding

- Application `61440ce` was imported on all four schedulable nodes. Namespace-
  correct render/live comparison contained only admin, orchestrator and hosted-
  model worker image substitutions. Admission was empty, the orchestrator was
  fully stopped, the worker and admin were replaced, migration 053 reactivated
  Vercel, and only then was the new orchestrator started.
- The first live v3 request used 4,744 prompt tokens and completed successfully in
  4.16 seconds. It belonged to another normally ordered active Event, confirming
  transport/schema behavior without manually inserting or reprioritizing work.
- Continued observation found that Event repeatedly returned an already-successful
  one-time repair job after its repaired composition failed the second support
  audit. This pre-existing state transition starved later active Events. A local
  correction recognizes the repaired composition from the immutable successful
  repair result and moves the case to its existing audit-held terminal state.
  It does not permit another repair or weaken audit. Twelve focused tests and the
  1,108-test full offline suite pass; orchestrator-only deployment is pending.

## Event audit completion-budget diagnosis (2026-09-21, local)

- The scheduler correction was deployed as application `92db75d` and terminated
  the repeated-repair loop. Cases then advanced in normal priority order without
  manual queue insertion or reprioritization.
- Vercel completed all seven reactivated `event-fact-curation-v3` jobs. The former
  blocker, article 26194 / revision `aer_22d9e...`, used 15,237 request characters,
  completed in 9.30 seconds, and received HTTP 200. No curation input-size hold
  recurred.
- The accepted ledger produced a 42-item composition. Its audit request measured
  25,795 characters, below the unchanged 48,000-byte request guard. The hosted
  model consumed the complete 1,800-token allowance as reasoning, returned zero
  visible characters, and stopped with `finish_reason=length`. The generic parser
  error `input_size` described the empty response, not an oversized request.
- Application `3e150bb` changes only the composition-audit completion allowance
  from 1,800 to 4,800 tokens and the response parser cap from 12 KB to 20 KB. The
  latter covers the legal maximum of the existing strict 42-item response schema.
  Migration 054 reactivates only the exact `composition audit failed: input_size`
  hold. Audit rules, evidence, model, concurrency, composition, publication, Hugo,
  feed JSON and public content remain unchanged.
- Focused tests pass, and the complete documented offline gate passes: 1,109
  passed, one skipped, four existing warnings.
- Platform `06a2837` deploys the image only to admin, orchestrator and the hosted-
  model worker. Both Vercel audit jobs succeeded in 14.49 and 14.08 seconds with
  HTTP 200, `finish_reason=stop`, 7,623 and 7,695 visible characters, and 2,081
  and 2,101 completion tokens. The repaired composition remained unsupported and
  was held by policy, proving the gate remained fail-closed.
- Rendered/live comparison is empty. Updated pods are Ready with zero restarts;
  all nodes are Ready with MemoryPressure false; Kubernetes readiness, homepage,
  Events index, admin route and `/feed/index.json` pass. The feed index retains
  5,062 days. No Hugo or public build was invoked.

## Event semantic fact roles (2026-09-21)

- The prior Event ledger inferred section meaning by scanning fact text for fixed
  words. Production inspection showed that grounded recovery and attack details
  were retained in article evidence but often unavailable to composition because
  they did not contain those cues.
- `event-fact-curation-v4` now returns one compact object per selected fact with
  Event-specific semantic roles. Python validates exact fact membership, known
  roles, recommendation boundaries and timeline date roles; it does not infer
  meaning. `accepted-evidence-event-ledger-v2` stores those roles and
  `curated-sections-v3` authorizes composition from them.
- Existing immutable v1/v4 publications retain their recorded section policy.
  New role decisions make affected older ledgers stale through the existing
  lineage check, causing normal withdrawal and reconstruction rather than
  mutation.
- The complete offline suite passes: 1,112 passed, one skipped, four existing
  warnings. Read-only production history showed 323 prior successful curation
  requests at 2,779-40,534 input characters and 0-2,030 output characters.
- A disposable Kubernetes canary used a real 25-fact retained source but performed
  no database or public write. It completed in 7.75 seconds, used 1,007 completion
  tokens, selected 13 grounded facts, and returned six valid semantic roles. The
  canary Job was deleted.
- Migration `pg_event_fact_semantic_sections_055` installed successfully. The
  first normal v4 job completed in 8.77 seconds with 1,147 completion tokens,
  selected 15 facts and persisted six role types. Its Event remained deferred for
  independent corroboration, confirming that role retention did not bypass the
  two-source gate.
- Application `6abba55` is deployed only to admin, orchestrator and the serial
  hosted worker. Platform `9636c18` contains only those image substitutions.
  The affected pods are Ready with zero restarts; homepage, Events index and feed
  index return HTTP 200. Hugo, build behavior, daily JSON, fetch workers, local
  inference, model choice and concurrency are unchanged.

## Event archive and daily feed integration (2026-09-21)

- Qualified Event exports now retain immutable promotion history with canonical
  public URLs and exact revision publication timestamps. Build preparation and
  activation both verify that projection against the database while holding the
  existing activation lock.
- The public Event archive presents three recently updated reports followed by a
  searchable, type-filtered archive with ten reports per page. Filter and page
  state remain addressable in the URL.
- A daily feed now leads with a separate two-card pager when qualified Event
  revisions were promoted on the selected date. A date with no Event promotion
  hides the section. Existing `/feed/days/<date>.json` files and download links
  are not modified.
- The application offline suite passes with 1,111 tests and two skips. The Hugo
  JavaScript syntax and source diffs pass. API build
  `job_d0ebf3dd0b2d4ea09c224748bd8e825c` completed successfully and atomically
  activated release `20260921211933` in 19.03 seconds; Hugo rendering took
  2.415 seconds.
- Production verification showed two Event cards, independent paging from
  `1–2 of 29` to `3–4 of 29`, a hidden Event section on 2026-09-19, and the
  unchanged `/feed/days/2026-09-19.json` download target.

## CyberNews presentation and feed-contract verification (2026-09-21)

- The production homepage identifies the publication as CyberNews by
  JasonDaemon.net and states that reporting is aggregated, summarized and linked
  to the original publishers. Article cards retain publisher icons, explicit
  source labels and direct external headline links.
- The Event archive and individual reports share type-specific visual markers,
  compact metadata, source-backed language and the existing canonical URLs.
- The complete favicon family and web manifest use the CyberNews mark. SVG, PNG,
  ICO, Apple touch and 192/512 pixel installed-app assets are present.
- Supported API build `job_610957b961ac434cb9982d730dfda73f` succeeded in 21.51
  seconds and atomically activated release `20260921215111`; Hugo rendered 124
  pages in 1.53 seconds. The only build warning is the pre-existing Blowfish Hugo
  compatibility declaration.
- Homepage, Events, favicon assets and manifest return HTTP 200. The live browser
  reports no console warnings or errors.
- The daily feed contract is unchanged. `/feed/days/2026-09-21.json` returns HTTP
  200, 1,054,341 bytes and 171 items, and remains the homepage download target.
