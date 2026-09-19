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

## Private assessment regression cohort (September 19 UTC)

Eight additional **assistant-reviewed, provisional** expectations cover Odido,
Vercel and the European Commission. The fixtures `assessment-*-v2.json` contain
only identities, snapshot/request hashes and labels, not complete source bodies.
They pin exact passage IDs, not order-dependent model IDs. Changed inputs fail
evaluation instead of silently applying old labels to different evidence.

Five negative cases must not be included: two generic Odido company descriptions
and three Commission staff/MDM breach passages unrelated to the Trivy/cloud
incident. Hold or exclude are safe for these cases. Three positive cases require
retaining incident reporting, preventing an all-hold response from being called
successful. This is a small regression subset, not whole-packet factual approval,
independent review, exhaustive recall, or a production publication gate. Source
allegations and company statements must retain attribution even when included.

Run against an immutable private packet and assessment from the same job:

```sh
PYTHONPATH=src python3 tools/check-event-assessment.py \
  --packet /private/snapshot/packet.json \
  --assessment /private/snapshot/assessment-HASH.json \
  --cases tests/fixtures/events/assessment-commission-v2.json
```

The command performs no database/network access, inference, or writes. It validates
the complete assessment contract first, reports checked versus assessed/omitted
passage counts, and always returns `public_eligible: false`. Exit 0 means only that
the selected provisional cases pass; 1 means a case failed; 2 means invalid/stale
inputs or unreadable files. Unassessed required cases cannot silently pass.
