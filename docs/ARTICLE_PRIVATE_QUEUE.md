# Private stored-article comparison

## Status

Deployed September 19, 2026: admin/LLM worker `e2c7abf`, platform `c0d9ec8`.
`article-evidence-v4` segments
stored text deterministically into bounded, numbered passages. The private model
now selects passage IDs and typed facts instead of reproducing quotations. Code
attaches the exact current source text and offsets, canonicalizes passage order,
and rejects unknown/duplicate references or date text outside the selected
passages. Material uncertainty is a passage-bound fact, not an unreferenced note.

This removes the quotation-copying failure mode measured below; it does not prove
the selected passages entail the proposed statements. The workflow remains private,
unreviewed and ineligible for publication. The full offline suite passes 1,003
tests with one skip. The three saved articles produce 5, 6 and 7 passages and
requests of 4,384, 5,039 and 6,270 bytes, below the existing combined request
budget. No database migration, live profile change, public write or site build
was performed.

Real job `job_eedd71ca19d34ebd85e09fca7c3d3744` completed six serial calls in
65.828 seconds. All three context records were structurally valid and exact source
passages were attached by code. Two summaries were structurally valid; WaterPlum
was rejected after using passage ID `p001` as a fact ID. The semantic gate FAILED:

- 35613 still labels the September 17 advisory date as an incident date, extracts
  one broad fact and lets the summary expand that fact into several omitted details.
- 35614 extracts the device count and stolen amount but omits material campaign,
  method and provenance context; its summary is invalid.
- 35615 extracts six statements, but labels attacker claims and disputed history
  as `reported_fact` rather than preserving their evidence status consistently.

The passage design solved quotation copying, not meaning or coverage. Automatic
evidence admission and the new-article canary remain blocked. Do not spend the
five remaining shared experiment attempts on prompt iteration. The safe next path
is deterministic incident candidates plus explicit curation of passage-bound
suggestions; a stronger model or reduced review policy requires a separate measured
decision. Baseline article fields and generation timestamps are unchanged. All
19 sampled public checks pass; rendered admin/worker manifests match production,
all workloads are Ready and the shared LLM queue is empty.

Prior v3 deployment: admin and LLM worker `3d2a981`, platform `583da4c`.
Private admission is enabled; normal strict validation remains OFF. The scoped
Helm render matches live. All other deployment images and model concurrency remain
unchanged. The first real comparison completed; candidate quality FAILED.

Job `job_86bd8979191e493797025e9a71d790fc` used three context calls, 20.092 seconds
of recorded provider latency. Each context failed validation, so zero summary
calls were made. Repeated admission returned the same job with no extra calls.
Three of the shared fourteen attempts are spent; eleven remain, not a new budget.

- 35613: attribution is not an exact substring of the cited passage. Manual
  inspection also finds the advisory date mislabeled as an incident date.
- 35614: copied headline is wrapped in additional quotation marks absent from
  stored source; attribution is borrowed from elsewhere. Only one fact extracted.
- 35615: quotation was altered; attribution lies outside the cited passage. Only
  one fact extracted. An uncertainty was retained, but coverage is insufficient.

The exact stored text, title, summary and context hashes for all three articles
match the pre-run baseline. Public event revision is unchanged. Twenty sampled
HTTP/JSON checks passed before and after rollout, all deployments Ready, Kubernetes
API readiness passed. These are sampled safety checks, not exhaustive guarantees.
See `article-quality-comparison.html`; raw private output is retained in the job
and ignored `data/article-quality-comparison/queued-result.json`.

Next investigate passage-ID selection with source spans attached by code, rather
than model reproduction of quotations. This is a proposed remedy for copy errors,
not proof that selected passages support claims. Keep frozen semantic and coverage
expectations; do not strip quotes/borrow attribution to silently accept these results.
No candidate summary or public rewrite is authorized by this failed comparison.

The earlier access blocker was an operator-context
error: prior successful releases use the designated remote image builder, import
the image into K3s/containerd and apply scoped Helm-rendered Deployments. Local
Docker is not required. The established account/key were recovered from prior
release records and verified. See KUBERNETES_RELEASE.md. Never copy code into live
containers as a workaround.

## Qwen 3.5 structural correction: local verification

The first Qwen 3.5 frozen-cohort job, `job_87fab52cb9224c9596e0250e90d3ead8`,
made three context calls and no summary calls. Ollama received the complete JSON
schema in its native `format` field, but all three responses used a top-level
array, obsolete statement keys, invalid date roles, or null date roles. Strict
validation rejected every response; no article, Event, feed, or build data changed.

A direct production-model probe reproduced that Ollama 0.20.0 does not constrain
this Qwen 3.5 response to the supplied schema. A second probe succeeded only after
the prompt explicitly required the root object, exact field names, enum values,
and null-date pairing. `article-evidence-v5` adds those generation instructions
without accepting, coercing, repairing, or retrying malformed output. The schema
and validators are unchanged. The full offline suite passes: 983 tests, two skips.
This is locally verified and not yet deployed or semantically accepted.

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

Full offline suite: 1004 passed, one skipped. One real disposable PostgreSQL queue
lifecycle test passed with mocked inference. Actual provider results are above.
The test pod and forwarding were removed. No production migration was performed.

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
