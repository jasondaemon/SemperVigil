# Real-source Events evaluation seed

## Status

September 18, 2026: a first **assistant-reviewed, provisional** seed covers one
real incident across four stored source documents. It is not independent human
adjudication, a model benchmark, or a production publication gate. No LLM was
called. No production event, link, source text, or generated report was changed.

The active Odido event had seven linked articles, including a Check Point weekly
report and a SecurityWeek news roundup. Read-only inspection confirms those
documents contain both relevant Odido passages and unrelated incident/topic
passages. Article-level association therefore cannot establish passage relevance.
This inspection does not alone prove which generation step caused the previously
observed contaminated public report.

## Reproducible artifacts

- `tests/fixtures/events/odido_review.json`: four public URLs, stored article IDs
  and feed dates, full stored-text SHA-256/length, seven small exact excerpts with
  source-relative Unicode offsets, provisional scopes, and 12 review cases.
- `tests/offline/test_event_curated_seed.py`: fixture integrity and actual
  structural-validator outcomes, including explicit semantic blind spots.

The offsets and hashes were read from production's stored source text, not from
today's web pages. Only short excerpts are committed, not complete articles or
database records. Full-document hashes are provenance anchors: the offline tests
cannot recompute them without the original snapshot. Feed dates are metadata,
not automatically publication, disclosure, or incident dates.

Scope labels are locally reviewed fixture annotations, not automatically inferred
production assignments. `snapshot:` origin identifiers identify inputs only;
different identifiers or publishers do not establish independent corroboration.
Supported cases mean supported reporting statements within the reviewed snapshot,
not independently verified facts. These small fragments deliberately omit wider
context and must not become the complete context for a real model pilot.

## Baseline outcomes

| Case class | Count | Structural result | Review expectation |
| --- | ---: | --- | --- |
| Scoped reporting, customer unit, report-time attribution | 3 | Pass | Supported within snapshot |
| BridgePay, Predator, mixed roundup contamination | 3 | Reject | Reject |
| Record/customer substitution and adding overlapping counts | 2 | Pass | Reject |
| Invented company attribution | 1 | Pass | Reject |
| Detection/weekend or feed date promoted to exact incident date | 2 | Pass | Reject |
| Multiple reporting domains promoted to independent investigations | 1 | Pass | Reject |

**Six semantic rejections pass structural validation.** That is an expected
measurement of the current boundary, not a passing factual-quality score.
Tests keep those gaps visible; they do not implement entailment, chronology,
unit/overlap reasoning, or editorial independence checks. Do not turn an empty
structural error list into publication approval.

## Next acceptance steps

1. Expand beyond this single incident: repeated incidents at one organization,
   aliases, genuine syndication, explicit corrections, and missing evidence.
   Retain unrelated negative examples and independently review expectations.
2. Prepare bounded, full-context incident passages from pinned snapshots for a
   private pilot. Preserve uncertainty and source attribution. Do not send entire
   roundup bodies or treat generated summaries as primary evidence.
3. Capture actual model/profile/prompt versions, inputs and outputs, latency, and
   inference cost under the existing single-job limit. The fixed cases need both
   extraction and unsupported-claim rejection evaluation, not keyword matching.
4. Require all critical contamination/attribution/date cases to reject misleading
   output before public admission. Keep the old validated public output on failure.

The next code slice should implement the already-planned bounded candidate input
boundary, with these evaluation cases retained as release blockers for a later
semantic/shadow integration. No deployment is warranted for this test-only seed.

Run the seed with:

```sh
python3 -m pytest tests/offline/test_event_curated_seed.py -q
```
