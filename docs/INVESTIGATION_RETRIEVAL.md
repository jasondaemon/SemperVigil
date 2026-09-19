# Read-only investigation foundation

Status: implemented and tested offline; no runtime integration or deployment.

`src/sempervigil/investigation.py` implements shared services for future workers
and the internal MCP adapter. Nothing in existing jobs, API routes, publishing,
or model execution imports it. No new dependency or schema change is required.

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

The separately gated test uses temporary tables in an explicitly disposable
database and verifies real PostgreSQL read-only enforcement:

```sh
# Set SV_TEST_DB_URL to a disposable PostgreSQL database, never production.
python3 -m pytest --run-db-tests tests/test_investigation_postgres.py -q
```

The evidence slice added 36 offline checks; **260 offline tests pass**. Coverage
includes exact Unicode slices, instruction-like source text treated as data,
changed-content/provenance rejection, suppression between pages, permission and
input boundaries, and maximum document/response sizes.

The PostgreSQL gate now also tests exact evidence and stale versions, but has not
been run. The local Docker daemon and PostgreSQL binaries were unavailable when
checked; no production database was used as a substitute. Next gates: run it in a
disposable environment, measure query plans/coverage, then implement/authenticate
the read-only MCP adapter. Trusted incident scoping remains a later gate.
Rollback before integration is simply reverting the isolated files. No production
or data rollback is needed.
