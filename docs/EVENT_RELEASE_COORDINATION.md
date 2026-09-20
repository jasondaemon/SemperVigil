# Builder-owned Events publication

The qualified Events runtime is deployed: ingest `e091471`, orchestrator `7fd1010`,
builder `bfa9986`, platform `7bcfd87`. Two controlled Vercel pilot revisions passed
API publication and public HTML/JSON checks. Bounded automatic qualification for
the enrolled scope subsequently published a third revision; see EVENT_AUTOMATION.md.
General autonomous event discovery/enrollment is not enabled. The approved
atomic switch correction passed two more API builds; see CURRENT_CONTEXT.md and
STABILIZATION_VERIFICATION.md for the HTTP evidence and NFS visibility caveat.

## Release contract

- `SV_EVENT_PUBLICATION_ENABLED=0` preserves the legacy exporter.
- When enabled, workers mark builds dirty instead of writing Events source files.
  The single builder exports the complete managed pointer inventory before its
  existing Hugo invocation. No feed JSON schema, archive regeneration policy,
  inference concurrency, Hugo arguments or resource limits change.
- The existing Events JSON URL remains `/sempervigil/index/events.json`.
- Managed reports are either exact attributed-quotation revisions or explicitly
  accepted, fact-cited composition revisions. The two bundle types are validated
  independently and are never blended. Unmanaged legacy events retain their
  existing path.
- Unchanged Events source bytes are reused. Changed evidence, revoked approvals,
  hidden events or suppressed sources remove the affected managed report until a
  fresh qualified revision is promoted. Unrelated daily news can still publish.
- The builder creates `static/event-publication.json`, workflow
  `event-release-authorization-v2`. It binds the full managed inventory, page
  slugs, exact index bytes and canonical HTML fragments to the candidate release.
- Immediately before activation the guard rereads authority and current evidence,
  locks source rows, and checks the actual rendered page and JSON against the
  approved projection. Mismatches retain the prior live release. Hugo runs outside
  these short transactions. Version-one authority-only manifests cannot activate.

## Deployment

All Events writers must run the same coordinated implementation before setting
the shared publication flag. Drain workers/builds, retain previous image tags and
an Events-source snapshot outside Hugo inputs, render/diff, then roll out only
affected components. Do not change the web server or invoke Hugo directly.

Three distinct database roles are required. Store their URLs in an existing
Kubernetes Secret named by `eventPublication.existingSecret`, never values files:

| Key | Component | Authority |
| --- | --- | --- |
| `SV_EVENT_APPROVAL_DB_URL` | Admin | Insert immutable approvals/qualifications and queue jobs; no pointer/revision writes |
| `SV_EVENT_PROMOTION_DB_URL` | Fetch runner | Read approvals/qualifications, insert revisions, advance pointers; no approval writes |
| `SV_EVENT_ACTIVATION_DB_URL` | Builder | Read publication/evidence and lock pointers/source rows; no qualification writes |

These are application-level connection boundaries, not full pod isolation: existing
components still possess their ordinary application credentials.

Provision the explicit publication and approval schemas transactionally, separately
from normal startup. Enable `SV_EVENT_HUMAN_APPROVAL_ENABLED`,
`SV_EVENT_PUBLICATION_ENABLED` and `SV_EVENT_ACTIVATION_CHECK` only after role tests
and coherent rollout. Helm refuses missing secrets or publication without a guard.
The builder overlay must include the approved `tools/hugo-build.sh` hook as well
as Python code; use `docker/release-overlay/Builder.Dockerfile`.

## Acceptance and rollback

Offline tests cover output corruption, missing/duplicate fragments, symlinks,
withdrawals, unchanged-file reuse and chart credential placement. Disposable
PostgreSQL tests cover source edits during activation, stale evidence, authority
revocation and pointer races. They do not substitute for a real API-driven build.

Before calling this operational, publish a reviewed revision, verify its public
HTML/JSON and daily-feed/archive checks, then advance a second revision and verify
the update. Keep the local LLM at one job. Private model relevance decisions are
not human approvals; automatic qualification remains a separate gate.

That two-revision pilot completed September 19. The qualification identity is
`policy / codex-reviewed-pilot`, explicitly not a human reviewer. One short excerpt
per cited source was inspected and admitted through the restricted approval role;
the existing queued promotion worker handled both revisions. No model-generated
prose, invented dates, or source-independence claims were approved.

Production grants: all three roles SELECT the Events/evidence/publication tables
and have column UPDATE privileges for source-row locking. Admission alone can
INSERT qualifications/approvals and SELECT/INSERT jobs. Promotion alone can INSERT
public revisions/pointers. Promotion and activation can UPDATE pointers for their
locks/CAS. No qualification writes are granted to promotion or activation. There
are no extra credentials in the LLM, OpenAI or public-fetch containers.

On failure, retain the previous live release and stop new admissions. Do not
bypass the guard to publish a failed candidate. Coordinate any rollback of all
writers and their source snapshot; preserve new DB audit records and feed history.
