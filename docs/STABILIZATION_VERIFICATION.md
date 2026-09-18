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
