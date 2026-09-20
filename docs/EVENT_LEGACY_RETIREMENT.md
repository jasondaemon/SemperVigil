# Legacy event backlog: inspection and retirement boundary

September 20, 2026. The bounded production retirement completed. No rows were
deleted.

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

The version-checked maintenance operation is implemented and applied. The admin
created immutable preview manifest `lerr_06b4d0e03e4744e58f01b396d84bf79a`;
published, managed, manual, revision-bearing, approved and in-flight Events are
excluded. Apply and restore run as bounded fetch-worker jobs. Each row is locked
and fingerprinted before mutation, and restore refuses to overwrite later edits.
Suppression sets both durable visibility and lifecycle while retaining every
article, CVE, source link and Event row.

Fetch job `job_2b5cb566f762489e828c4f3c97b4ed28` suppressed all 1,426
eligible rows with zero skips. The restore manifest remains available. Published
active Events remained 13 and managed public pointers remained 2; articles and
Event-article links were untouched. No cleanup schedule or destructive task was
added. Future runs still require a newly inspected preview. This workflow is not
a claim that all retired candidates were reviewed for factual quality.
