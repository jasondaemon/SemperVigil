# Confirmed Event reassessment

Legacy confirmation is an input to review, not proof. The reassessment workflow
rebuilds confirmed Events from retained article text through the current strict
evidence pipeline. It prioritizes Events that are already public but never copies
their legacy summary into evidence or composition prompts.

## Boundaries

- Starting a case freezes the Event identity and exact article/source versions.
- Existing public HTML, JSON, Event rows and article membership remain unchanged
  during extraction and review.
- Evidence extraction is deduplicated by article/source/model/prompt version and
  runs on the existing single local-LLM lane. Invalid or over-budget sources are
  held individually rather than blocking the Event.
- Extracted facts require explicit evidence review. Incident candidates, the
  combined ledger and hosted narrative each retain their existing review gates.
- A ledger requires at least two enrolled sources from distinct publisher
  domains. A legacy title is only a review label; it has no evidentiary standing.
- Promotion rechecks the frozen snapshot, accepted evidence lineage, candidate
  enrollment, ledger, composition, source independence and qualification under
  restricted roles.
- The first successful promotion atomically replaces the legacy database
  projection while retaining the canonical Event ID and URL. Any stale snapshot,
  missing source, changed review state or concurrent publication fails closed.

## Operator sequence

1. Open `Content > Confirmed Event Reassessment` and freeze the cohort.
2. Work public-priority cases first. Queue missing evidence for one Event.
3. Review its passage-bound evidence; hold or reject inaccurate extraction.
4. Project accepted evidence, review the incident candidates, and enroll only
   sources that concern the same incident.
5. Create the strict ledger. Review its combined facts and source boundaries.
6. Compose and review the narrative in the existing Event Ledger and Event
   Narrative screens.
7. Publish through the existing qualified promotion and API-driven atomic build.

The 81 confirmed drafts in the cohort are not automatically published. Cases
that do not meet the source or accuracy gates remain held and can be explicitly
withdrawn later.

## Production status

Migration 046 and the admin/fetch image `10de83b` are live. The first cohort
contains 93 legacy cases: 12 public-priority records and 81 confirmed drafts.
WaterPlum was already managed by the strict ledger pipeline and was therefore
excluded. Starting the cohort did not alter public content.

The first bounded review demonstrates the fail-closed behavior. For Advantest,
one retained source produced accurate, incident-specific evidence and was
accepted. A second revision was held for unattributed publisher speculation and
a third was held because it included unrelated Asahi incident facts. The clean
source was projected and privately enrolled, but no ledger or replacement was
created because fewer than two independent sources passed review. The existing
published Event remains unchanged.
