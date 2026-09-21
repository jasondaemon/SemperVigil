# Autonomous Event reassessment

The confirmed-Event reassessment coordinator advances retained legacy Events
through the existing evidence-first publication path without operator shepherding.
It is default-disabled through `SV_EVENT_REASSESSMENT_AUTOMATION_ENABLED`.

## Bounded workflow

Each orchestrator pass performs at most one material action across active cases:
cases already waiting on queued or running work are skipped so they cannot starve
other Events, while the first newly initiated or state-changing action still ends
the pass and preserves bounded admission.

1. Refresh a stale case only when the current Event remains eligible. An accepted
   ledger blocks an automatic rebase if its source membership changed.
2. Queue one missing retained article for passage-bound evidence extraction.
   Optional date metadata that is not present in the cited passage is removed;
   the passage-grounded fact remains available for independent curation.
3. Queue one hosted curation job. The independent curation checks the extracted
   statements against their exact passages and selects only facts belonging to
   this Event. Background incidents and generic actor history are excluded.
4. Store Event-scoped incident candidates. The same article evidence may support
   different Events without sharing a fact selection.
5. Require at least two enrolled sources from different publisher domains. When
   coverage is insufficient, use the existing research, fetch, relevance,
   enrichment, and evidence path to seek corroboration.
6. Build and accept a deterministic ledger from the selected facts, then queue one
   hosted narrative composition.
7. Audit every generated narrative item against only its cited fact statements.
   A rejected first composition may receive one constrained corrective rewrite
   using the fixed citations and audit reasons. The replacement must pass a new
   independent audit; a second failure is held. When a legacy derivative sorts
   after the audited draft, repair selection uses the current composer's audited
   original rather than treating the obsolete derivative as authoritative.
8. Admit a passing composition through a policy qualification, restricted
   promotion role, activation-time freshness checks, the normal build API, and
   atomic release switching.

The local Qwen worker remains single-job and performs article evidence extraction.
The existing OpenAI worker remains single-job and serializes curation, composition,
and support audit calls. Queue dedupe keys bind every call to source, prompt,
model, and snapshot versions, so unchanged work is reused rather than repeated.

## Failure behavior

No failure falls back to the legacy narrative. A retained source that cannot
produce reviewable evidence is recorded as a generation-scoped exclusion so the
remaining sources can proceed and research can seek replacement coverage. An
Event is held when the remaining evidence cannot satisfy the publication gates,
the incident identity is ambiguous, lineage is stale, a corrective composition
still fails support review, or publication conflicts. Evidence, exclusions,
decisions, revisions, and jobs remain auditable. Publication authorization
identifies the reviewer as a versioned policy, never as a human.

## Compatibility

The workflow does not modify article summaries, daily JSON, feed generation, Hugo
commands, build caching, or release activation. Legacy unscoped candidate rows are
preserved; new automated candidates are keyed by Event and evidence revision.
