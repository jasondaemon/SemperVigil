# Event semantic fact roles

## Decision

Event section assignment belongs to Event-scoped fact curation, not to a Python
keyword classifier and not to generic article extraction.

Article evidence remains a reusable, passage-grounded record of what one source
reported. The Event curator sees the Event identity, every extracted statement,
and its exact source passages. In one hosted-model call it independently verifies
the evidence, decides whether the article concerns the same incident, selects all
useful facts, and assigns semantic roles to each selected fact.

The allowed roles are:

- `attack_vector`: delivery or initial access
- `attack_path`: actions and progression after access
- `timeline`: a dated Event milestone
- `impact`
- `response_recovery`
- `mitigation`: source-supported advice only
- `attribution`
- `open_question`
- `context`: useful for the overview but not a specialized section

## Trust boundary

The model is trusted to classify the meaning of text that is already bounded to
the supplied article and exact passages. It is not trusted to invent facts,
rewrite evidence, publish, or bypass review.

Deterministic code enforces:

- every selected fact has one to four known roles;
- assignments refer exactly to selected, known fact IDs;
- recommendations may only be mitigation or context;
- mitigation roles require recommendation evidence;
- timeline roles require an incident or disclosure date;
- every ledger fact retains exact source passages;
- composition may cite a fact only in its stored roles;
- the independent composition support audit and publication qualification remain
  mandatory.

No semantic fact is discarded because its sentence lacks a predefined word. A
selected `context` fact remains available to the overview. Dated facts become
timeline candidates only when curation identified them as actual Event milestones,
which avoids turning publication and administrative dates into a wall of chronology.

## Versioning and compatibility

- Curation: `event-fact-curation-v4`
- Ledger: `accepted-evidence-event-ledger-v2`
- Composition: `event-ledger-composition-v5`
- Section policy: `curated-sections-v3`
- Schema migration: `pg_event_fact_semantic_sections_055`

Existing immutable publications retain their recorded composition policy and
continue to validate under the legacy workflow. New ledgers require curated roles.
When a previously enrolled candidate receives v4 roles, lineage comparison makes
an older keyword-classified ledger stale so the normal reassessment workflow can
withdraw and rebuild it rather than mutating history.

## Operational constraints

The hosted queue remains serial. No Hugo behavior, build command, public feed JSON,
article summary, source content, local model, or publication authority changes.
The role output is compact relative to the existing 48 KB curation input guard and
2,400-token completion allowance.
