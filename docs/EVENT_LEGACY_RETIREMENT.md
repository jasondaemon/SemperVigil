# Legacy event backlog: inspection and retirement boundary

September 19, 2026. Read-only production inspection; no rows changed or deleted.

The user permits retiring nonapplicable old event content, but `candidate=true`
is not a safe deletion criterion. Current inventory:

| Candidate | Lifecycle | Publish state | Count |
| --- | --- | --- | ---: |
| true | candidate | draft | 1,424 |
| false | confirmed | draft | 81 |
| false | confirmed | published | 9 |
| true | archived | draft | 2 |
| true | confirmed | published | 2 |
| true | confirmed | draft | 1 |
| true | candidate | published | 1 |

All these rows currently have visibility `active`. The 1,424 draft candidates
span February 17 through September 19, with no manual flag or published_at value.
Recent titles include plausible incidents; age or a candidate label does not
prove they are junk. There is one new public pointer, for the Vercel pilot.

The legacy derivation worker still creates candidate rows from article content.
It can mark an event confirmed after three distinct sources; this does not prove
those sources concern the same incident or are independent. The new scoped quote
automation does not yet replace candidate discovery. Deleting event rows now
would erase useful linkage and might cause rediscovery rather than migration.

## Retirement policy

1. Retain all original articles, CVEs and source text, even when legacy summaries
   are unusable. Never feed the legacy narrative into new deconstruction claims.
2. Prefer reversible suppression of unreviewed legacy drafts, not deletion.
   Exclude published/previously published, manual, qualified, revision-bearing,
   enrolled and pending-job events. Preserve their canonical URLs and pointers.
3. Snapshot the exact eligible IDs and previous values before applying. Recheck
   eligibility and versions under transaction locks; abort on concurrent changes.
   Retain an audit/restore manifest. A broad delete-all or weak-event purge is not
   an adequate substitute for this bounded operation.
4. Separate legacy candidates from new scope-reviewed drafts in the admin UI.
   Stop or redirect legacy admission as the replacement discovery path becomes
   available; suppression alone does not stop legacy processing cost.
5. Re-evaluate useful source links under the new incident scope, not the old
   merged summary. Published contaminated reports require individually validated
   replacements or explicit withdrawal, not a blanket candidate purge.

Existing `upsert_event` preserves `suppressed` visibility when derivation requests
`active`, but it can overwrite lifecycle/status. Therefore setting only lifecycle
to `archived` is not a durable retirement mechanism. Admin/API changes and a
version-checked maintenance operation remain to be implemented before bulk cleanup.

No new cleanup schedule or destructive maintenance task was started. This audit
is not a claim that all legacy candidates have been reviewed for factual quality.
