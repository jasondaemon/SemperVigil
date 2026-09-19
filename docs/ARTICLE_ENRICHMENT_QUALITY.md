# Shared article enrichment quality

September 19, 2026. Local hardening implemented and tested; NOT deployed.
This corrects the Events architecture reset: reuse existing article enrichment,
not routine re-extraction of each article for each event.

## Inspected production behavior

- Article Summary has no configured schema, no profile parameter overrides and
  no fallback provider. Its prompt requests summary, bullets, entities, CVEs,
  tags and NIST family; the worker previously accepted arbitrary lists or raw text.
- Article Context Pack has a strict object schema with facts, entities, numbers,
  IOCs, CVEs, timeline and uncertainties. The worker ignored `schema_valid` after
  the router's existing repair attempt and could persist invalid output as success.
- Both failure paths replaced existing output/model with null and restamped its
  generation time. That could discard usable previous output on a failed refresh.
- The context prompt allows facts "clearly implied" by the article and describes
  its input as JSON, although the worker sends labeled text. Neither stage binds
  individual assertions to source spans; stored generation timestamps are not
  source-version receipts. The router can truncate long inputs.
- Legacy event reporting already consumes summary/bullets and context facts/
  timeline, but drops context uncertainties and limits facts/timeline to ten.
  Strengthening article output is insufficient if downstream code loses caveats.

Read-only sample: newest 100 articles with BOTH stored outputs, not a random or
whole-database sample. All summaries pass basic shape checks. 39 contexts fail
both the local proposed shape check and a separate check against the current live
schema. Error occurrences include 145 numbers-type violations, six timeline-type
violations, 28 missing root fields, five missing entity fields and four extra-root-
property violations. Multiple errors can belong to one record.

This proves a structural acceptance problem, not a 39% factual error rate. Stored
records do not establish the original router validation status or profile version.
One sampled context has a publication date in its timeline; downstream must not
silently relabel that as an incident date. No factual-accuracy percentage claimed.

## Local correction in this slice

- `article_enrichment.validated_output` requires successful router validation,
  a parsed object, usable summary text and correctly typed supplied summary arrays;
  context requires the current string-array/entity shape. Empty context arrays
  remain allowed. No coercion, raw-text fallback, added model call or public field.
- Both worker handlers validate before storage and downstream enrichment/admission.
  A failed schema cannot become a successful new summary/context record.
- `storage.record_article_enrichment_error` updates only the error column. It
  preserves previous payload, model, generation time and article update timestamp,
  including a concurrent newer value; it never restores a stale captured payload.
  Existing output is not thereby declared factually correct or current. First-time
  failures leave no generated result; normal feed fallback remains unchanged.
- This does not yet bind successful writes to the exact current source version.
  Stale-source protection and downstream eligibility are required before treating
  old enrichment as an Events evidence record. Failed historical context must not
  be silently promoted as trusted merely because it exists.

943 offline tests pass, one Linux-only skip; feed-contract tests included. Tests
cover failed schema/list/raw responses, downstream isolation, existing successful
payload shape, numeric context items, and error-only preservation. No PostgreSQL
integration suite, inference, profile edit, build or deployment in this slice.

Do not deploy rejection alone with the observed failure rate. First prove the
configured generation contract produces acceptable output without retry churn.
No automatic historical rewrite or bulk requeue. Rollback before deployment is
source-only; eventual rollout must retain previous images and profile revisions.

## Next shared contract, within existing work

1. Define versioned summary/context schemas and aligned prompts through the
   existing AI configuration workflow. Require explicit support, attribution and
   uncertainty; separate recommendations, reported actions and speculation. Remove
   permission to infer unstated facts. Input format must match what is actually sent.
2. Add internal source-text hash, full-versus-excerpt/title-only coverage, profile/
   schema version and fact IDs with exact evidence spans. Distinguish disclosure,
   incident and publication dates, including unknown/relative dates. Attach units
   and qualifiers to quantities. Spans prove location, not semantic truth.
3. Retain the existing two article jobs initially. No third routine extraction or
   same-model fact judge. Evaluate a context-first ordering so the existing summary
   call writes from reusable facts plus their source passages; do not reorder live
   jobs before dependency/failure/retry tests pass. Preserve summary/bullets field
   types and the current daily download paths/IDs/null semantics.
4. Events reads eligible article facts AND uncertainties, filters for its incident,
   and composes one report after material changes. Consult original source text
   for a specific missing detail or conflict only, within the existing experiment
   budget; never assume that every article needs another extraction call.
5. Bind writes and reuse to source/config versions, preserve current authorized
   output on failed replacement, and hold stale enrichment for Events. Material
   changes trigger targeted work; timestamp churn does not. Prove source-edit races
   and correction propagation before publication.

## Acceptance and rollout

Use a fixed small cohort drawn from the existing event pilot plus fresh articles:
roundup, allegation, sensitive/unprotected distinction, numeric quantities with
units, recovery versus advice, publication-only dates, irrelevant sidebar content,
oversized input and embedded instructions. Compare both daily summary/bullets and
event report to original text, including required coverage and caveats. Valid JSON
is not the factual gate, and a generic caveat does not excuse an incorrect claim.

First demonstrate reuse from stored outputs with no inference. Then evaluate only
missing/changed enrichment through the existing serial queue. All article calls,
composition, repairs and transport attempts count against the architecture
experiment's 14-attempt ceiling; that ceiling is not an allowance in each stage.
If the combined work cannot fit, reduce the admitted cohort or report the shortfall;
do not silently expand the budget. Freeze expected outcomes before generation.

Stage a bounded profile/worker canary only after offline and disposable-DB tests.
Check structural acceptance rate, factual errors/omissions, queue age, actual
provider attempts and preservation of existing feed output. No bulk rollout with
a failure rate resembling the inspected sample. Existing feed data and build
machinery stay intact; no schema migration or new public content fields assumed.

Reference implementation: `worker._handle_summarize_article_llm`,
`worker._handle_summarize_article_context_llm`, `llm.router._call_with_profile`,
`worker._event_article_context_parts`. Current direction:
[architecture review](EVENTS_ARCHITECTURE_REVIEW.md).
