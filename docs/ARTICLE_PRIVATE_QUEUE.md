# Private stored-article comparison

## Status

Release validation in progress. The earlier access blocker was an operator-context
error: prior successful releases use the designated remote image builder, import
the image into K3s/containerd and apply scoped Helm-rendered Deployments. Local
Docker is not required. The established account/key were recovered from prior
release records and verified. See KUBERNETES_RELEASE.md. Never copy code into live
containers as a workaround. Actual model results are recorded after the canary.

## Operation

`article_review_private` is a distinct operator-triggered job on `llm_local`, with
priority -10 and one attempt. It participates in existing model admission and
appears in the admin LLM dashboard/Jobs registry. No scheduled admission is added.

Both admin and worker default to disabled:

```text
SV_ARTICLE_REVIEW_ENABLED=0
SV_ARTICLE_STRICT_VALIDATION=0
```

After testing and a targeted release, enable only the private flag and submit:

```http
POST /admin/api/articles/private-review
Content-Type: application/json

{"article_ids": [35613, 35614, 35615]}
```

Use the existing admin authentication; never put credentials in this document.
The endpoint rejects extra fields, requires a configured admin token and only
queues work. It makes no inference call. Up to three unique positive IDs are
accepted, each requiring full stored text and existing summary/context.

Admission pins the source, existing outputs and generation configuration. The
same input/configuration returns the existing job even after completion or failure.
The worker checks freshness before each phase and reserves the attempt in the
running job result before calling the provider. Interrupted/retried jobs are not
replayed. Results and per-phase raw output are private job data, not article data.

The active article-context profile selects the existing local model/provider;
private code supplies the versioned prompts/schema and fixed temperature 0,
maximum 2048 output tokens. Active AI profiles are not edited. Configuration
changes invalidate admission. No fallback provider, automatic schema repair,
transport retry or additional worker is used. Context runs before summary; an
invalid/empty context skips its summary. Transport failure stops the batch.

At most six provider attempts for this three-article cohort, counted against the
existing shared 14-attempt experiment budget. The ten-minute elapsed check prevents
starting further phases; it is not cancellation of a running provider request.
The existing provider timeout is retained, not reduced to release the queue while
inference may still be active. Inspect a transport failure before further admission.

The job result includes baseline output, candidate evidence/summary, source and
request fingerprints, raw bounded responses, validation status and latency.
`comparison_ready` means inspectable, not factually correct. Every result remains
`public_eligible: false`. Only job bookkeeping and LLM-run telemetry are written;
no articles, Events, feed JSON, build flags or public revisions are changed.

## Normal job preservation

Earlier local strict-validation changes are now behind
`SV_ARTICLE_STRICT_VALIDATION=1`. Leave it disabled for the canary. The default
preserves deployed summary/context fallback and error behavior, including existing
weaknesses; those are not silently enabled/fixed as part of the comparison release.
Strict-mode tests still run explicitly. Do not enable strict mode until the
generation acceptance and failure-preservation rollout gates have passed.

## Verification and release gates

Full offline suite: 1003 passed, one skipped. PostgreSQL integration and real
provider execution have not been performed for this slice.

Offline tests cover serial six-call bounds, failed-context skip, one-attempt HTTP,
source/configuration changes, replay refusal, reservation failure, admission
deduplication, payload injection rejection, registry visibility and default legacy
behavior. Run:

```sh
.cache/mcp-venv/bin/python -m pytest -m offline --strict-markers -q
```

Before deployment: exercise admission/claim/result against disposable PostgreSQL;
compare rendered manifests with live admin/worker; preserve unrelated platform
edits; drain the existing LLM lane before replacing its worker. No Hugo invocation
or builder/web rollout is required. Then submit the frozen cohort, compare with
`article-quality-comparison.html`, check baseline rows unchanged and report actual
attempts and quality before any rollout to ordinary enrichment.

Rollback: disable private admission, let an active request finish, restore the
previous admin/LLM images using platform values. Preserve private receipts. There
are no schema migrations, new public fields or public content to revert.
