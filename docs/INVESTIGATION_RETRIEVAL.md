# Read-only investigation foundation

Status: implemented; offline and targeted disposable-PostgreSQL tests passed.
Local stdio adapter tested; no production integration or deployment.

`src/sempervigil/investigation.py` implements shared services for future workers
and the internal MCP adapter. Nothing in existing jobs, API routes, publishing,
or model execution imports it. The optional [local MCP adapter](INVESTIGATION_MCP.md)
now calls these services; no schema change is required.

## Implemented boundaries

- Authenticated host code supplies `investigation:read`; request JSON cannot set
  scopes or connection parameters. This scope check is not transport authentication.
- Requests are UTF-8 JSON objects limited to 4 KiB. Reject unknown/duplicate keys,
  nonfinite numbers, invalid dates, boolean IDs/limits, and oversized strings.
- `search_articles` requires inclusive start/end stored feed dates, at most 31
  days per request. Optional source and literal case-insensitive title filters;
  title matching is not full-text/semantic search. Search wildcard characters
  are escaped and values parameterized. No inferred dates or filesystem reads.
- Pages default to 20, maximum 50 entries, descending primary-key order. Scan at
  most 200 candidate rows plus one lookahead; a hidden-only page may be empty with
  `has_more=true`. Continue using `next_before_id` and the same filters. Do not
  interpret an empty page as absence of history.
- Byte-limited responses stay under 64 KiB, with explicit stop reason. Suppressed,
  malformed-policy, oversized, and unsafe-link entries are omitted. They are not
  separately counted or disclosed. Null/empty legacy metadata is treated as no
  suppression, matching current behavior; truthy suppression values are excluded.
- Search returns only IDs, source IDs, titles, URLs, feed dates, and a metadata
  fingerprint. No raw policy metadata, source paths, HTML, or generated narrative.
- `get_event_record` returns bounded metadata for active legacy event records,
  not report prose, linked collections, or a validated revision. Missing and
  non-active entries produce the same response. Active does not mean published.
- `get_article_evidence` additionally requires `investigation:evidence:read`.
  Reads only database `content_text`, with no filesystem, HTML, summary, or network
  fallback. Requests select an article ID, start offset, and at most 2,048 Unicode
  code points. Documents over 131,072 code points are rejected before their text
  is transferred from the database, bounding memory for full-document hashing.
  This is not a completeness claim for oversized or file-only content.
- Evidence responses identify source URL, article/source IDs, exact offsets,
  text length, next offset, and a version of all stored text and source metadata.
  Subsequent slices require that version; changes return `stale_snapshot` without
  replacement text. Start at zero to obtain a new version. Suppression is
  rechecked even on version-pinned requests.
- Missing/hidden/unsafe-source records return the same `unavailable` response.
  Visible records with missing or oversized text return `content_missing` or
  `content_too_large`. No summary substitution occurs. Independent origin and
  incident scope remain unassigned: stored-text slices are untrusted evidence
  inputs, not validated incident evidence or instructions.
- PostgreSQL sessions have a 3s connection timeout, 2s statement timeout, 250ms
  lock timeout, 5s idle-transaction timeout, and repeatable-read read-only mode.
  Connection/query failures propagate, never masquerade as an empty result.

These are conservative initial retrieval bounds, not measured production latency
or LLM budgets. A dedicated read-only database role is still required before any
deployment; transaction flags alone do not replace least-privilege credentials.

## Coverage and version limitations

First search scope deliberately uses the existing indexed `brief_day` field and
primary key. Records without a stored feed date are not included. Publication
date, incident date, entity/CVE joins, full-text search, independent source
origins, and relevance ranking are not implemented. Exact stored text is now
available through the separately permission-gated evidence read.
Measure PostgreSQL plans and corpus recall before broadening search.

Each call uses its own transaction snapshot. Keyset pagination prevents offset
shifts, but is not a frozen multi-request archive: updates and suppression changes
can affect later pages. Metadata fingerprints cover returned fields only and
must never be used as claim-evidence versions or publication race guards.

Evidence document versions use the explicit `article_text_v1` schema and hash
actual content rather than `updated_at`: the current content writer does not
necessarily update that timestamp. They bind slices to a current snapshot but
do not persist old text or prevent later changes. Immutable evidence storage and
transactional version checks remain required for proposal/publication integration.

These reads are for authorized internal investigators, not an assertion that
every returned record is public or suitable for an external AI provider. Service
networking, transport authentication, client access policy, audit integration,
and sanitized error translation must precede an MCP endpoint release. Do not wrap
the legacy reader that can load arbitrary stored text paths. Add a separately
reviewed confined-file path only if measured file-only coverage warrants it.

## Verification

```sh
python3 -m pytest -m offline --strict-markers -q
```

Offline tests execute the SELECT logic against an in-memory SQLite fixture and
mock PostgreSQL session configuration. They cover parser/access failures, literal
SQL search, date scoping, hidden records, bounded scanning and responses, cursor
progression, legacy event boundaries, metadata versions, and propagated failures.
They do not prove PostgreSQL planning, production permissions, or retrieval recall.

The separately gated tests use temporary tables plus an isolated schema and
short-lived restricted login in an explicitly disposable database. The suite
requires a database named `sempervigil_test` or `sempervigil_test_*` and a test
administrator able to create/drop roles and schemas. Never grant these setup
permissions to the production retrieval role. The restricted role has SELECT
and schema USAGE only; generated test credentials are not persisted in the repo.

```sh
# Set SV_TEST_DB_URL to the named disposable PostgreSQL database, never production.
python3 -m pytest --run-db-tests tests/test_investigation_postgres.py -q
```

The evidence slice added 36 offline checks; **260 offline tests pass**. Coverage
includes exact Unicode slices, instruction-like source text treated as data,
changed-content/provenance rejection, suppression between pages, permission and
input boundaries, and maximum document/response sizes.

**Four integration tests passed on PostgreSQL 18.4**, covering SELECT execution,
Unicode slicing, changed-content rejection, suppression, read-only transactions,
statement timeout enforcement, and retrieval with a least-privilege login.
The suite now also exercises the local stdio MCP subprocess against the test
database and startup rejection of column-write grants. That login was unable to
update source records or create tables even without the
application's read-only transaction flag. Tests clean up their role and schema.

An empty isolated Docker instance supplied the database because the local machine
had no database runtime. It used a 512 MiB hard memory cap, no additional swap,
0.5 CPU, 256 MiB temporary storage, and loopback-only access through a temporary
SSH tunnel. No production volumes, credentials, or data were used. The container
and tunnel were removed afterward. The official image was
`postgres:18.4-alpine` at digest
`sha256:9a8afca54e7861fd90fab5fdf4c42477a6b1cb7d293595148e674e0a3181de15`.

## Production read-only sampling (2026-09-18)

Aggregate inspection found 35,211 article records: 35,209 have a stored feed date,
16,280 have nonempty database text, and zero have only a stored text-file path.
These are raw inventory counts, not public/suppression eligibility or an audit of
extraction quality. The missing text on 18,931 records limits evidence retrieval;
this slice did not queue a backfill or change source processing.

One bounded EXPLAIN ANALYZE sample for August 19-September 18 executed in 2.932 ms
for a 201-row discovery window and 1.095 ms for literal title matching `breach`
(87 rows). Both used the existing feed-date index through a bitmap scan. These
are single server-side samples, not network latency, p95, or full-history recall.
The latest 100 records with stored text had no document-cap exceedances; maximum
length was 57,801 code points. That does not prove all historical text fits.

Targeted PostgreSQL correctness and initial query-plan sampling now pass. The
local stdio adapter is tested but not deployed. Full-schema integration, curated
retrieval recall, evidence quality, and remote authentication remain separate
gates. Trusted incident scoping remains pending.
Rollback before integration is simply reverting the isolated files. No production
or data rollback is needed.

## Missing-text investigation (2026-09-18)

Read-only follow-up explains the 18,931 missing database texts:

| Recorded outcome | Records |
| --- | ---: |
| `stale_older_than_week` | 18,898 |
| `max_retries_exceeded` | 18 |
| `http_404` | 8 |
| Legacy HTTP 401 | 3 |
| Legacy redirect error | 2 |
| No error, disabled test source | 2 |

All 18,898 age-excluded records belong to Infosecurity Magazine, were ingested
on February 6, 2026, and have stored feed dates from May 16, 2007 through
December 4, 2025. This is a historical cohort, not a current fetch queue.
Current worker and storage selection explicitly treat the age marker as terminal.
The initial full-project sync already included this exclusion; repository history
does not establish which earlier process originally assigned those markers.

For stored feed dates September 1 onward, 1,117 records include 1,090 with nonempty
text and `has_full_content=1`, 22 with text not marked full, and five without text.
The five are retry-exhausted records: four BleepingComputer articles dated
September 9 and one Krebs on Security article dated September 8. These are
inventory/flag measurements, not independent extraction-quality validation.
The prior seven-day job snapshot showed 333 succeeded content-fetch jobs and no
failed jobs requested in that window. A succeeded job can report a permanent
failure or teaser rejection, so job status alone does not establish text coverage.

Decision: do not globally reset terminal errors or backfill this historical
cohort. Proceed with the curated Events evaluation using available source text;
record absent evidence explicitly and abstain where it is required. If a selected
incident needs a missing article, first inspect that specific URL/error through
the existing source test/fetch path, then consider a bounded operator-approved
recovery through existing jobs. Preserve downstream feed history, current retry
policy, single-job LLM capacity, and incremental publication. Missing text does
not authorize treating generated summaries as source evidence.

No production writes, retries, builds, model calls, or deployment occurred.
One worker exec attempt returned a Kubernetes upstream 502; the same bounded
read succeeded through another existing worker. Listed application pods remained
Ready, but that does not prove the affected node's exec path has recovered.
