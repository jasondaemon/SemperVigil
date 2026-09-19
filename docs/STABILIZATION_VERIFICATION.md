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
