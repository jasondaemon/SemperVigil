# Events publication handoff: remaining integration

Inspected September 19, 2026. This plan does not enable publication or approve
private model suggestions. Current production versions and pilot evidence are in
`STABILIZATION_VERIFICATION.md`; the upgrade tracker remains the status index.

## Existing paths that the new path must not reuse blindly

- `worker._handle_event_report_llm` assembles existing article summaries/context
  and promoted web snippets, then uses `_event_report_profile` and
  `_parse_event_report_output`. It does not consume exact private evidence receipts.
- `_parse_event_report_output` can manufacture timeline entries from report section
  text using a shared incident date. This violates the new explicit-date contract;
  the qualified path must not call this parser or its generic-profile fallback.
- `storage.update_event_report` writes a mutable `meta_json.report` and updates
  `events.updated_at`. Its compare-and-swap prevents concurrent event metadata
  replacement, but is not immutable revision storage or a source-evidence lock.
- `storage.get_event` returns report, narrative and timeline from the same metadata.
  `publish.write_events_markdown` can append old narrative bullets/sections after a
  new report overview. Merely replacing `meta_json.report` can therefore retain
  previously contaminated narrative content. A qualified renderer must be a
  separate branch with no legacy narrative fallback or blending.
- The active export call is `worker._publish_events`, which calls
  `write_events_markdown`.
  It must keep existing stable slugs and incremental byte reuse; it is not an
  instruction to run Hugo or reprocess all feed history.
- `storage.link_event_article` touches the event timestamp, but article content
  updates are independent. Event timestamp comparison alone cannot prove all
  citation text is still current. Membership and full document versions are needed.

## Concrete next slices

1. Receipt cache-hit/download check completed, including browser Jobs controls.
   Zero model calls, unchanged event and HTML. Preserve private artifacts and
   current public pages; a receipt never promotes itself.
2. Implement an immutable revision repository and separate publication pointer as
   additive, bounded records. Store reproducible public projection data in the
   database, not a pointer into disposable private logs. Use a content identity,
   unique event/revision binding, idempotent insertion and explicit predecessor.
   No schema work occurs on a diagnostic read connection. Test migrations only on
   a disposable database before planning a targeted production schema step.
   Local `event_publication_store` now defines these tables and a tested transaction.
   Production migration, restricted-role provisioning, trusted qualification
   admission and revocation-aware export reads remain open; no rollout yet.
   The local bounded revocation-aware reader is now tested; worker withdrawal/hold
   handling and authorization recheck at build activation are still open. Never
   interpret a managed withheld/withdrawn revision as eligible legacy fallback.
3. Persist evidence qualification separately from model proposals. Bind exact
   incident scope, source versions, citation spans, assertion/date roles and
   reviewer/policy identity. Model-selected `include` is relevance, not factual
   entailment or approval. Unknown origin independence stays unknown. Start with
   deterministic attributed quotations; optional prose must not add claims.
   Local `event_projection.prepare` now implements the quote-only data boundary,
   with default-deny trusted qualification identities. It does not evaluate or
   authenticate those qualifications; trusted storage/authorization remains open.
4. Use one transaction for current-input checks and pointer promotion. Lock the
   event/membership and cited source rows in a fixed order, re-read their versions,
   reject stale or suppressed evidence and compare the expected predecessor.
   Verify membership-insert/delete and article-update races on PostgreSQL; an
   isolated hash check or event-row-only CAS is insufficient. Do not nest the
   existing self-committing `update_event_report` inside this transaction.
   Local promotion now does this with the existing snapshot locks; PostgreSQL
   predecessor/duplicate/staleness/revocation cases pass. No runtime caller.
5. Give the qualified export branch only the promoted immutable projection. Never
   merge old narrative sections, infer incident dates from feed dates, or use raw
   model text on parsing failure. Reject invalid revisions before writing files;
   retain prior validated output. Unknown dates remain explicitly unknown.
   Local Markdown branch now reconstructs quote projections and compares their
   identity to a separately supplied promoted pointer. Legacy byte compatibility,
   stable slugs, unchanged-file reuse and no narrative blending pass offline.
   Matching qualified index preparation and `write_events_exports` now prevalidate
   both outputs before page replacements. Legacy index callers remain unchanged.
   This is content preflight, not an atomic multi-file transaction: IO failures
   still need coordinated build admission and the existing atomic publication.
   No live caller supplies qualified maps or uses the combined writer yet, and
   there is no trusted public-pointer store yet.
6. Admit bounded changed-input work through the existing single LLM lane, using
   measured queue age and inference-time budgets, coalescing and cache identities.
   Oversized or ambiguous evidence stays held with visible coverage. No silent
   fallback, wholesale backfill, priority jump or additional inference replica.
7. Enable publication separately for a reviewed pilot only after mandatory safety
   cases pass. Drive the existing API build/atomic activation, verify stable URLs,
   citations, desktop/mobile, daily JSON compatibility and unchanged archive files.
   A disabled qualified path must leave old serving intact; rollback restores the
   previous pointer and republishes through the same API.

## Acceptance that remains open

The paired cohort has seven passing assessed checks and one over-budget,
unassessed source. It is not an eight-case semantic pass or full-article factual
approval. Earlier curated cases also cover false counts, invented attribution,
dates and origin independence; relevance tests alone do not close those gaps.
Broader fixture coverage, independent qualification and sustained pilot observations
remain required before unbounded autonomous admission. Private review usability
and a healthy website are not completion of the automated Events feature.
