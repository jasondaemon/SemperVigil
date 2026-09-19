# Private Events review release

Status: working local operator workflow, tested against real snapshots. Not a
production worker/API deployment and not autonomous factual validation.

## What is ready

`python -m sempervigil.event_review` implements a complete private review path:

1. Read one active event and up to 12 linked articles in a single read-only,
   repeatable-read transaction. Respect article suppression and report omissions.
2. Pin the exact input packet to a content-derived version. Retain stored source
   text, article identity, URL, and feed date; never substitute generated summaries.
3. Suggest up to four verbatim alias-matched passages per article. Exact offsets
   accompany each passage. Alias matching is candidate retrieval, not a semantic
   relevance decision or trusted incident assignment.
4. Render a standalone private HTML workbench with a reporting timeline, source
   links, full stored context, review checklist, and include/exclude/hold controls.
5. Save browser decisions for that exact snapshot or download them as JSON.
   Import the downloaded file to create an immutable reviewed artifact.
6. Re-read current inputs before carrying decisions forward. Changed source text,
   metadata, aliases, or included links invalidates the prior packet version.

The reading view displays only included quotations. It is not an automatically
written incident narrative. All packets remain `public_eligible: false`, including
after review. No code here publishes, merges events, calls a model, submits a job,
or updates a database. Existing report generation and public outputs are unchanged.

This deterministic extractive mode was selected instead of rushing a new prose
generator into production: six known semantic rejection cases still pass quote
validation. The workbench makes those decisions inspectable without imposing new
inference load. It does not complete the later autonomous Events/pilot gates.

## Review now

The repository's [HTML tracker](upgrade-tracker.html#event-review) links three
generated local packets: Odido (7 sources/23 suggestions), Vercel (10/40), and
European Commission (12/41). All are initially unreviewed. Their 29 source
snapshots have no missing/over-limit inputs, and no article-link truncation.
The four-passage-per-source selection cap still applies; this is not exhaustive
incident coverage. Current-input rechecks matched all three original versions.

Read passages in review mode, inspect full context, and mark useful ones
**Include in reading view**. Use **Reading view** to see only those excerpts.
**Download decisions** preserves choices and notes independently of browser storage.
Notes and selections are not authenticated approvals or factual attestation.

The browser automation tool blocked the local-file URL, so visual layout, actual
browser CSP enforcement, keyboard behavior, and downloads remain user/browser
acceptance checks. No workaround server or alternate browser was started.
Eight JavaScript DOM-double tests verify the decision logic, not visual rendering.

## Operator usage

Use a local authorized shell and a dedicated read-only connection in
`SV_INVESTIGATION_DB_URL`; the tool does not fall back to the application's writer
DSN. The database role should have SELECT on `events`, `event_articles`, and
`articles`, and no write/ownership privileges. Read-only transaction settings are
also enforced. This CLI is local operator tooling, not a remote authentication
boundary; it must not be exposed as an HTTP service or unrestricted MCP tool.

```sh
umask 077
mkdir -p data/events-review
python -m sempervigil.event_review snapshot \
  --event EVENT_ID --alias 'Organization name' > data/events-review/input.json
python -m sempervigil.event_review render data/events-review/input.json
python -m sempervigil.event_review check-current data/events-review/input.json
python -m sempervigil.event_review render data/events-review/input.json \
  --review /path/to/downloaded-decisions.json
```

`render` prints the absolute HTML path. Output defaults to ignored
`data/events-review/<packet-version>/`. Packet and decision files are immutable;
HTML names additionally include the renderer output hash. Installation is atomic
and refuses conflicting existing content. Files are created mode 0600 and packet
directories mode 0700. Keep the trusted output directory local; do not point it
at Hugo inputs, public static roots, shared untrusted paths, or synced public folders.

Generated artifacts include full bounded source text. They are intentionally not
committed. The tracked code, templates, tests, and documentation reproduce them.
The production samples were captured by executing these read-only functions in
memory through an existing authorized worker, then saved locally. No production
files, images, credentials, permissions, or roles were changed.

## Bounds and failures

- At most 12 linked documents, oldest stored article IDs first; truncation shown.
- At most 32,768 code points per source; longer sources omitted, not cut mid-context.
- At most 3 MB per packet and 32 KB per decision input; strict fields/types, duplicate
  key rejection, no non-finite JSON numbers, no unknown decision IDs.
- One to five explicit aliases, 3-80 characters each. No inferred identity/aliases.
- Up to four 35-1,200-character exact sentence-like suggestions per article.
  This intentionally simple segmentation can miss pronouns, sentence fragments,
  and context. Review full text; matching an alias does not prove relevance.
- SQL statement timeout 2 seconds, lock timeout 250 ms, connection timeout 3 seconds.
- Source strings are escaped; no remote scripts, styles, images, or fonts. CSP
  permits only the bundled script hash. Review controls make no network requests.
- No background refresh, automatic retries, or model/tool loops. Unchanged packets
  reuse existing artifacts without overwriting reviews.

`stale_snapshot` or `stale_or_invalid_review`: obtain a new packet and review it;
do not rewrite the version field to force old decisions onto changed evidence.
`artifact_conflict`: preserve the existing file and investigate the unexpected
change; never overwrite it as a repair. Backend errors are sanitized.
Browser storage failure: decisions still work in memory; download before closing.
An unchanged check is valid only at that read, not a transactional publication
approval. No publication operation exists in this workflow.

## Verification and next release

348 offline Python tests, five disposable PostgreSQL tests, and eight JavaScript
unit tests passed. The new restricted-role integration test covers snapshot,
extractive proposal, HTML persistence, and suppression-driven stale-review
rejection. The temporary database and tunnel were removed afterward.

```sh
.cache/mcp-venv/bin/python -m pytest -m offline --strict-markers -q
node --test tests/js/event_review.test.cjs
# Only against a named disposable database:
SV_TEST_DB_URL=... .cache/mcp-venv/bin/python -m pytest --run-db-tests tests/test_investigation_postgres.py -q
```

Review these examples before replacing automated report behavior. Next runtime
release should integrate private proposals with existing job admission and narrow
draft persistence, still separate from public output. Actual bounded model
evaluation, trusted scope/reference establishment, semantic validation, and the
seven-day pilot remain open. No inference concurrency, Hugo behavior, daily JSON,
or public presentation changes are part of this review release.
