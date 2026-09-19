# Stabilization verification

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
