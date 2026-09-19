# Scoped automatic Events quotation updates

Status: deployed September 19 with orchestrator `7fd1010`, platform `7bcfd87`.
The first automatic review, policy qualification, promotion and build succeeded;
the live Vercel page and JSON now contain three excerpts. One model call took
5.154 seconds; the normal dirty-build process reported 19.86 seconds. See
STABILIZATION_VERIFICATION.md for IDs and public checks.

Implementation: `event_automation.py`, called by the existing orchestrator.
Default disabled: `orchestrator.eventAutoScopes: "{}"`. An explicit map of at
most three event IDs to previously reviewed revision hashes enrolls bounded
incident scopes. This does not automatically create or enroll arbitrary events.

The scheduler reads complete current evidence, validates the unchanged approved
scope anchor, and preserves only unchanged previously quoted documents. It admits
passages using the approved non-entity scope focus (for example the affected
system), not broad company-name retrieval. Oversized paired requests are held
before queueing, without increasing the model limit or truncating sources. It admits
one paired-source private review at a time into the existing local LLM queue,
only when that queue is idle. Inference remains in the existing serialized worker.
Completed holds/exclusions are reused, not retried until an include appears.
Failed reviews require operator attention rather than an automatic retry loop.

Publication policy `scoped-auto-quotes-v1` requires an exact sentence of at most
25 whitespace-delimited words, every approved scope focus term in that sentence,
and a valid paired local-model include decision for that exact current source
snapshot. Only one excerpt per source URL is admitted, at most one new source per
tick. Generic organization matches, missing/oversized evidence, stale anchors,
revoked authority and mismatched reviews are held. The model is a relevance
filter, not a factual authority. Published material remains attributed reporting;
incident dates and source independence remain unknown. No generated prose or
legacy narrative is added. A same-entity/system match can still be semantically
ambiguous; keep enrollment bounded and evaluate real results before expansion.

The scheduler uses the restricted admission credential to record a versioned
policy decision, its evidence receipt, and the existing `event_promote_reviewed`
job in one source-locked transaction. It cannot advance the public pointer with
that credential. The promotion worker and guarded builder remain unchanged.
No direct Hugo invocation or per-tick build is introduced. Successful promotion
marks the normal build process dirty. Both review and promotion tasks are visible
in the existing admin Jobs dashboard, including their results and receipts.
Scheduler hold/deferred reasons are in `event_automation_tick` logs.

Disable by clearing `orchestrator.eventAutoScopes` and rolling only orchestrator.
This stops new automatic admissions, not already queued work. Inspect/cancel
pending tasks through admin if stopping an in-flight decision is necessary.
Previously published policy qualifications have their own revocation lifecycle;
revoking the enrollment seed stops future admissions but does not retroactively
revoke independently qualified descendants. Use qualification revocation and the
guarded withdrawal build for those. Never bypass activation checks for rollback.

Initial rollout is limited to the already reviewed Vercel pilot. Changed quoted
documents or scope anchors require fresh review/enrollment; automatic event
discovery and broader semantic qualification remain future work. No claim is
made that this produces comprehensive coverage of all linked sources.

Validation commands:

```sh
.cache/mcp-venv/bin/python -m pytest -q tests/offline
# Explicit disposable database only:
.cache/mcp-venv/bin/python -m pytest --run-db-tests tests/test_investigation_postgres.py
```

The PostgreSQL gate covers the real automatic review admission, completed receipt,
restricted approval, promotion, duplicate suppression, unchanged skip and revoked
seed refusal. The model response is stubbed in this test; production inference
and publication must be separately observed before marking rollout verified.
