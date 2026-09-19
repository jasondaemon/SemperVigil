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
- PostgreSQL sessions have a 3s connection timeout, 2s statement timeout, 250ms
  lock timeout, 5s idle-transaction timeout, and repeatable-read read-only mode.
  Connection/query failures propagate, never masquerade as an empty result.

These are conservative initial retrieval bounds, not measured production latency
or LLM budgets. A dedicated read-only database role is still required before any
deployment; transaction flags alone do not replace least-privilege credentials.

## Coverage and version limitations

First search scope deliberately uses the existing indexed `brief_day` field and
primary key. Records without a stored feed date are not included. Publication
date, incident date, entity/CVE joins, full text, evidence text, independent source
origins, and retrieval relevance ranking are not implemented in this slice.
Measure PostgreSQL plans and corpus recall before broadening search.

Each call uses its own transaction snapshot. Keyset pagination prevents offset
shifts, but is not a frozen multi-request archive: updates and suppression changes
can affect later pages. Metadata fingerprints cover returned fields only and
must never be used as claim-evidence versions or publication race guards.

These reads are for authorized internal investigators, not an assertion that
every returned record is public or suitable for an external AI provider. Service
networking, transport authentication, client access policy, audit integration,
and sanitized error translation must precede an MCP endpoint release. Evidence
access will need a separate bounded, provenance-preserving contract; do not wrap
the legacy reader that can load arbitrary stored text paths.

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

That integration test has not been run in this slice. Next gates: run it in the
disposable environment; measure query plans and representative retrieval; add
exact evidence contracts; then implement/authenticate the read-only MCP adapter.
Rollback before integration is simply reverting the isolated files. No production
or data rollback is needed.
