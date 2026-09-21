# SemperVigil — Current Context (2026-02-19)

> September 19, 2026 release override: the topology notes below are historical.
> Read `STABILIZATION_VERIFICATION.md` and `EVENTS_PRIVATE_QUEUE.md` for current
> deployment tags, inference recovery, private assessment flags and pending pilot
> verification. The HTML upgrade tracker distinguishes deployed work from open
> automatic-publication gates. Do not infer current production settings from the
> February snapshot alone.

## September 20: normalized Event fact-curation requests deployed

`event-fact-curation-v3` transmits every exact evidence passage once and has each
fact reference the shared passage table by immutable passage ID. Stored article
evidence, fact IDs, source text, selection validation, hosted model, completion
allowance, serial queue, publication gates and the 48,000-byte request guard are
unchanged. Conflicting text for one passage ID fails before inference.

Read-only production measurement of the held Vercel cohort found that its failing
21-fact source shrinks from 48,792 bytes to 17,087 bytes. All eight current source
requests fit between 8,870 and 17,624 bytes after normalization, so the safety
limit does not need to increase. Migration 053 reactivated only cases held by the
exact `event_fact_curation_input_over_budget` reason. Application `61440ce` is
deployed to admin, orchestrator and the hosted-model worker. Its first v3 request
completed successfully in 4.16 seconds with 4,744 prompt tokens.

That canary exposed a separate pre-existing scheduler starvation defect: a repaired
composition that failed its second audit rediscovered its already-successful repair
job on every tick. The local correction identifies repaired compositions from the
persisted successful repair result and holds the failed repaired narrative, preserving
the one-repair policy while allowing later Events to proceed. Application `92db75d`
is deployed to the orchestrator, and normal scheduler ordering subsequently advanced
the Vercel case through all seven reactivated curation revisions. The formerly blocked
article completed in 9.30 seconds with a 15,237-character request and HTTP 200.

The resulting 42-item composition audit then exhausted its 1,800-token completion
allowance entirely as hidden reasoning and returned no JSON. The audit request itself
was only 25,795 characters, so this is not an input-size failure. The pending narrow
correction raises only the audit completion allowance to 4,800 tokens and its response
parser cap from 12 KB to 20 KB, sufficient for the schema's legal response size.
Migration 054 reactivates only cases held with the exact resulting reason. Full offline
suite: 1,109 passed, one skipped.

## September 20: resilient fact curation deployed

Application `f5cc352` and platform `726290f` deploy
`event-fact-curation-v2` to admin, orchestrator and the single hosted-model
worker. The contract marks deterministic incident-anchor facts in each request,
requires empty selections for held or nonmatching evidence, and conservatively
discards contradictory selections rather than permitting them to reach an Event
ledger. The completion allowance increased from 1,200 to 2,400 tokens after two
production responses exhausted exactly 1,200 tokens and returned no usable JSON;
input size, source content, model, concurrency and publication gates are unchanged.

Migration 052 reactivated only cases held by the three corrected curation failure
reasons. The first production pass classified an unrelated SolarWinds roundup as
ambiguous with no incident signal and enrolled a genuine Notepad++ source with 13
supported facts. Seven Notepad++ v2 curation jobs succeeded, its stale ledger was
withdrawn, and normal reassessment is rebuilding it from current selections. The
remaining reactivated cases continue through ordinary scheduler ordering; they
have not been manually advanced or declared complete. Full offline suite: 1,078
passed, two skipped. Rendered/live configuration is identical, all updated pods
are ready, all nodes report no memory pressure, and public feed/Event checks pass.

## September 19: Qwen 3.5 production cutover

Qwen 3.5 9B Q4_K_M is downloaded on the existing RTX 3060 host and fits entirely
in 12 GiB VRAM at a 16,384-token context. The first LiteLLM-routed trial exposed
an adapter defect: LiteLLM 1.77.7 places `think=false` inside Ollama generation
options, so Qwen returns hidden reasoning and can leave visible JSON empty. No
production profile was switched until qualification completed. A source-controlled
`ollama_native` transport now
sends top-level `think=false`, retains existing profile prompts and validation,
and forwards every existing profile schema to Ollama's constrained-output field,
including strict private-review schemas. A direct CVE probe returned valid JSON
in 4.43 seconds. All 982 offline tests pass with two existing skips. The deployed
seven-stage gate passed in 0.56 to 37.32 seconds. It exposed a pre-existing prompt
contradiction: article-product extraction requested confidence, evidence, and notes
while its two-field schema forbade them. Migrations 037 and 038 align both prompt
templates without changing extraction meaning or the stored schema.

Production now runs the `qwen3.5:9b-q4_K_M-16k` Ollama alias, which reuses the
downloaded Qwen 3.5 weight layers and sets `num_ctx 16384`. Eighteen Qwen-backed
profiles were changed in one transaction; zero profiles reference Qwen 2.5. The
single local worker is restored. Normal summary and product jobs completed in 5.56
and 1.09 seconds. Qwen 3.5 is the only resident model and uses about 8.54 GiB of
12 GiB VRAM; Qwen 2.5 remains installed only for rollback.

## Current direction: strengthen and reuse article enrichment

Authoritative stabilization sequence: `CONTENT_EVENTS_STABILIZATION.md`. Keep the
current per-article summary publisher and daily JSON unchanged while developing a
separate passage-ID-based evidence sidecar. The order is private evidence quality,
reviewed evidence canary, conservative incident candidates, versioned event ledger,
narrative composition, then guarded publication. No event discovery/publication
expansion precedes the evidence gate. This is a documented direction, not a new
runtime or production change.

Latest deployed slice: admin/LLM `e2c7abf`, platform `c0d9ec8`.
`article-evidence-v4` replaces model-copied quotations with
deterministic passage IDs inside the existing private article queue. Code attaches
exact text/offsets, canonicalizes references and passage-binds uncertainty. The
three saved inputs fit the existing request budget with 5, 6 and 7 passages. Full
offline suite: 1,003 passed, one skipped. Real private job
`job_eedd71ca19d34ebd85e09fca7c3d3744` used six calls / 65.828 seconds. All
contexts passed reference validation, but semantic quality failed: advisory date
misclassification, incomplete coverage, inconsistent allegation typing and one
invalid summary fact reference. Automatic evidence admission remains blocked.
Baseline article fields are unchanged; 19 public checks pass; all workloads Ready;
render/live diff empty; LLM queue empty. Normal summary/context/publication paths
are unchanged. Five of the original shared fourteen experiment attempts remain;
do not spend them on prompt iteration.

Prior v3 deployment: admin/LLM `3d2a981`, platform `583da4c`. Private article queue
enabled; strict normal validation OFF. Render/live diff empty; other images intact.
Real job `job_86bd8979191e493797025e9a71d790fc`: three calls / 20.092s provider time,
all three context candidates invalid, zero summary calls. Factual/coverage quality
also fails manual inspection. No auto repair or rerun; 3/14 shared attempts used.
Baseline article hashes and public event revision unchanged; 20 public checks pass.
1004 offline tests, one skip; disposable PG queue test passes. See
ARTICLE_PRIVATE_QUEUE.md for exact failures and proposed passage-ID next step.
Older local-only/blocker paragraphs below are superseded.

Release-workflow correction: local Docker/guessed SSH were not prerequisites.
Recovered the established remote image-builder account/key from prior successful
release commands; production remains K3s/containerd. See KUBERNETES_RELEASE.md.
During pre-release integration, corrected admin DBConn lifecycle and private
job's attempt-count expectation (claim leaves it zero; result reservation blocks
replay). Validation and scoped release are in progress; older blocker notes below
are superseded.

Latest local work: `article_review_private` implements default-disabled queued
comparison of up to three stored articles (six calls maximum, no retries/repair).
Admin admission and dashboard registry are wired; outputs only go to private jobs.
Normal article strict validation is now gated OFF by default to preserve deployed
behavior during a later canary. See ARTICLE_PRIVATE_QUEUE.md. NOT deployed: Docker
build access is unavailable and SSH authentication failed; user was asked for the
deployment route. No provider calls made. Disposable-DB and manifest/live release
verification remain pending; do not confuse offline tests with a completed canary.

Latest local fix: candidate `article-evidence-v3` records all exact quote occurrences
(maximum 32), with the first only a display anchor. Stored SolarWinds duplicate
fixture now passes without source edits; records stay unreviewed/private. 31 targeted
tests pass. No inference or runtime changes. Next: isolated queued three-article
trial, at most six calls counted against the shared 14-attempt budget. See the
latest section of ARTICLE_ENRICHMENT_QUALITY.md; older unique-quote notes below
describe the prior contract.

Read-only baseline comparison now covers stored articles 35613-35615. Existing
context retains details lost in summaries; output can upgrade allegations or
substitute publication dates. See `article-quality-comparison.html`. No new model
calls or production writes. It also reproduces a v2 unique-quote rejection on
duplicated stored article text; resolve occurrence selection before canary. New
prompt quality remains unmeasured. Snapshot is local ignored data, not Git.

Latest local slice: `article_evidence.py` supplies candidate v2 context/summary
contracts with source spans, attribution/uncertainty/date roles and fact-linked
summary sentences. Pure private preview only; no runtime caller or model calls.
26 targeted tests and 969 total offline tests pass, with one skip; integration
tests were not collected. Real canary requires isolated queued preview admission;
do not use synchronous admin profile tests or live writing article jobs. Do not
deploy this branch wholesale: previous strict-validation changes must remain off
the normal pipeline until generation acceptance is demonstrated. See the safe
canary boundary in ARTICLE_ENRICHMENT_QUALITY.md.

See `ARTICLE_ENRICHMENT_QUALITY.md`. Article context already supplies facts and
timelines; do not add routine per-event extraction. Read-only latest-100 sampling
found 39 context records failing the live schema, while 100 summary records passed
basic shape checks (not factual verification). Workers ignore schema failures and
accept raw fallback; local correction validates before storage and preserves prior
output on failed replacement. 943 offline tests pass, one skip; NOT deployed.
No inference/profile/public-output changes. Do not roll out rejection alone:
first align generation, measure acceptance and bind provenance/source freshness.
Events must reuse caveated shared facts and supplement only identified gaps.

## Earlier direction: architecture reset (review only)

Read `EVENTS_ARCHITECTURE_REVIEW.md` before implementing more Events work. It
supersedes the per-claim judge iteration sequence. Keep publication safeguards;
freeze auditor experiments; next deliver one private evidence-first report with
update/correction/replay and blind-incident acceptance, capped at 14 provider
attempts including one optional structural repair. No runtime was changed or
inference requested for this review. Production quotation automation remains as is.
Do not build new infrastructure or tune another judge before that vertical slice.
R1-R4 in the tracker define the recommended sequence, not completed work.

## Earlier local work: source-context diagnostics

Private audit rendering now loads validated per-phase reasons, shows exact bounded
neighboring source text with offsets, and reports extraction counts by section and
dated-claim count. This is diagnostic display, not additional model evidence or
automatic citation repair. Current inference requests/cache identities unchanged.
881 offline tests pass, one skip. Four real V4 audits rendered locally from their
existing production receipts with zero inference or production writes. Not deployed.

The cached reasons confirm reversed entailment and incorrect security-category
equivalence persist. Adjacent source context exposes wrong-sentence citations and
a legitimate pronoun antecedent. Next extraction experiment should select evidence
before drafting a statement (current schema generates statement before quote),
with explicit passage IDs/offsets and frozen independent tests. This is a proposed
experiment, not an established cause or a reason to relax the support gate.

## Latest deployment checkpoint: September 19, 18:32 UTC

Admin remains `eff906d`; LLM worker `d80f98a`, platform `3200453`.
V4 performs serial quotation/context checks with separately resumable caches.
Saved-packet replay now preserves the original source serialization order;
the production extraction receipt validates after reload without re-extraction.
863 offline tests pass, one Linux-only skip. Render/live deployment diff is empty.
All 20 public checks pass; public revision and build/feed behavior are unchanged.

Quality gate still FAILS. Original 17 cases: 11 correct rejections, one false
acceptance, one correct acceptance, four false rejections. Fresh five-case holdout:
three correct rejections, one correct acceptance, one valid claim held because its
quotation requires a surrounding antecedent. Do not promote these suggestions.
The four jobs used 26 serial calls / 43.009 seconds of model time. Three original
source replays succeeded with zero LLM calls. Stop prompt-only tuning on this
cohort: next evaluate passage/antecedent-bound evidence and extraction coverage
on independent examples before narrative synthesis or automatic authorization.
See EVENT_CLAIM_SUPPORT.md for job IDs and limits.

## Earlier deployment: queued claim support audit

Admin `eff906d`, LLM worker `3148e06`, platform `023f17c`. Explicit `audit_source`
admission uses existing private jobs/artifact viewer and a separate pinned profile.
Builder/web/feed/schema/concurrency unchanged. V1 real evaluation failed: one
false acceptance, three false rejections, and incorrect null-date reasons.
V2 isolates one claim per serialized call and handles absent dates in code, with
per-claim resumable cache. Frozen 17-claim expectations remain unchanged. V2 real
pilot finished: five negative cases rejected, seven negatives held, all five
positives also held. It accepts nothing; do not call this a passing semantic gate.
Total model time 50.444s over 17 calls. Next separate quotation entailment from
factual certainty; do not relabel holds or weaken the frozen expectations.
856 offline tests pass, one Linux-only skip. See EVENT_CLAIM_SUPPORT.md.

## Earlier local implementation: claim support audit

`event_claim_support.py` now supplies a bounded, full-source audit contract for
six separate support dimensions, strict complete claim inventory and an isolated
source/claim/profile-bound cache. One unsupported dimension rejects a proposal;
uncertainty holds it. Model agreement never authorizes publication. Original
claims remain unchanged. No runtime integration, deployment or actual-model
audit yet. Next: explicit existing-queue admission and fixed pilot-error cohort,
then measured model evaluation before synthesis. See EVENT_CLAIM_SUPPORT.md.

## Current release checkpoint: September 19, 15:30 UTC

Private deconstruction is deployed: admin `69e76a6`, LLM worker `c4f8ddf`,
platform `5c6044d`. Orchestrator remains `7fd1010`, other ingest workers
`e091471`, builder `bfa9986`. Single LLM lane, model, resources, build and
feed behavior are unchanged. Production flag is enabled for manual private API
admission only; no automatic narrative publication.

The actual Vercel pilot compiled 17 unreviewed claims from three sources.
Successful model calls took 28.736s (including reload), 22.224s and 22.191s.
Repeating source 26217 reused the cache with zero model calls and the same
three-source compilation revision. Claims have citation/meaning mismatches;
no overview or dated milestones were extracted. Do not promote this draft.
Next: passage-bound claim support evaluation, uncertainty/correction handling,
then coherent narrative synthesis. Do not substitute longer unvalidated prose.

An intermediate schema crashed the installed Ollama grammar parser on a regex
escape, not an established OOM. The corrected contract sends no regex or
conditional grammar and derives date precision locally. Three subsequent model
requests succeeded; the model is back on GPU and all Deployments are ready.
817 offline tests passed with one Linux-only skip; final targeted suite 48 passed.
All 20 publication checks passed at 15:22 UTC. Public event pointer is unchanged.
Legacy candidates were not deleted: some published records carry that flag.
See EVENT_DECONSTRUCTION.md and STABILIZATION_VERIFICATION.md for evidence.

## Earlier checkpoint: September 19, 14:00 UTC

Latest local continuation: private deconstruction now reuses source/config-bound
extractions and compiles all current cached source drafts into one review artifact.
Changed source receipts are rejected; added/withdrawn/retained claim diffs are
available. 808 offline tests pass, one Linux-only skip. No runtime deployment or
model pilot yet. This is compilation, not finished narrative synthesis.
Read-only production inventory found 1,424 draft candidates and three published
rows also marked candidate. No cleanup applied; see EVENT_LEGACY_RETIREMENT.md
for the reversible, publication-preserving retirement boundary.

Later local work: source-level private deconstruction is wired into the existing
private-review API/job/viewer behind a new default-disabled flag and separate
profile. It produces unreviewed cited claims, not publishable narrative. No
production change, real inference or new model profile yet. See
`EVENT_DECONSTRUCTION.md` for boundaries and the next pilot gate. The user has
clarified that quotation-only coverage is not the finished Events feature.

- **Latest override:** orchestrator `7fd1010`, platform `7bcfd87`. Other ingest
  components remain `e091471`; builder remains `bfa9986`. Only the scheduler was
  rolled for scoped automatic Events admission. No build/LLM behavior changed.
- Vercel event `evt_0ffca0813049` is explicitly enrolled. The scheduler queued a
  real paired-source review, admitted one short exact excerpt under policy, and
  the existing promotion worker/dirty-build path published revision
  `c44cc56060cba571615cc697fc2d4930edacacdb94bbedf92104d9ce46a90252`.
  Public HTML and Events JSON both have three cited sources. No manual approval,
  direct DB promotion or manually enqueued build was used for this update.
- Real inference: one call, 5.154s. Automatic build: 19.86s reported duration,
  release `20260919135817`. All 5,060 archive JSON files remain. No repeated
  inference/build after completion; two oversized sources are explicitly held.
- 777 offline / 11 real PostgreSQL / 26 JS tests pass; one Linux-only stress
  test skipped on macOS. The earlier Linux stress validation remains recorded.
- This is bounded production automation for an enrolled, previously reviewed
  scope, not autonomous discovery/enrollment of all events or generated narrative.
  Changed anchor/quoted documents require fresh review. See EVENT_AUTOMATION.md
  and the latest STABILIZATION_VERIFICATION.md entry for exact limits and evidence.

### Earlier checkpoint: September 19, 13:36 UTC

- **Override of the older checkpoint below:** ingest components now run `e091471`,
  builder `bfa9986`, platform values `4d852c2`. Qualified publication, human
  approval and activation flags are enabled with separately restricted credentials.
- Two explicitly agent-reviewed, quotation-only Vercel pilot revisions have been
  published at `/events/evt_0ffca0813049/` through the normal worker and API build
  path. Public Events JSON matches. This was a controlled operator-policy pilot,
  NOT a human UI approval or autonomous local-LLM qualification.
- API builds succeeded in 18.81s and 18.26s; an intervening ordinary build took
  17.33s. All 5,060 daily archive files remain. Builder peak measured 380,858,368
  bytes, with existing 16Gi limit. 747 offline / 11 PostgreSQL / 26 JS tests pass.
- **Approved release-safety correction deployed:** the enabled guard now replaces
  the live link with a same-directory atomic rename inside its authorization
  window. Two API builds succeeded in 19.31s and 16.77s. The separate NFS client's
  contribution to the earlier transient 404 is not established; see dated public
  monitoring results in STABILIZATION_VERIFICATION.md. Do not describe the earlier
  pilot rollout as uninterrupted. The disabled legacy branch remains unchanged.
- Next: bounded automatic evidence
  qualification/refresh admission and reader-facing report improvements. No
  additional decisions or manual testing are needed for the already-live pilot.
- See the September 19 pilot entry in `STABILIZATION_VERIFICATION.md` and
  `EVENT_RELEASE_COORDINATION.md`. The following earlier checkpoint is historical.

Atomic-switch validation: 750 offline tests pass, one Linux-only stress test is
skipped on macOS. A separate Linux builder-container stress run completed 10,000
switches and 17,802 concurrent page reads without errors. macOS rapid pathname
lookup returned EINVAL, so Linux-specific reader behavior is not asserted there.
Failed rename and candidate validation preserve the previous live link. No Hugo,
retention, feed, cache, resource or concurrency behavior changes are included.

### Earlier checkpoint: September 19, 11:40 UTC

- App source and runtime behavior: this repository. Environment values:
  `k8s-platform/apps/sempervigil/values.yaml`. Theme: `sempervigil-hugo`.
- Admin/LLM worker: `78a0739`; platform release: `9681dc1`.
- Shared fetch/OpenAI workers and orchestrator: `27b9fb3`; builder: `889b2de`.
- Private Events review supports ordinary, scoped, source-level and opt-in paired
  diagnostics. See `EVENTS_PRIVATE_QUEUE.md` for current guards/profile IDs.
- No new autonomous Events admission or validated public-report pipeline is enabled.
  Legacy Events behavior is not evidence that the new correctness gates are met.
- Paired cohort completed: all seven assessed checks pass; one over-budget case
  remains unassessed. This is not the full semantic or public-publication gate.
- Private revision receipt release is deployed and verified. Cache-reuse job
  `job_aefc9fa9885b40e584eaa4e917c9dd20` succeeded with zero model calls; authenticated
  attachments and browser Jobs coverage/download controls pass. Do not repeat it.
- Local-only snapshot storage, current-source transaction window and quote-only
  projection preparation and pointer promotion are tested, not deployed.
  The local revocation-aware reader distinguishes managed withheld/withdrawn
  revisions from unmanaged legacy content. A default-disabled human qualification
  screen, atomic approval/job admission and restricted-worker promotion now pass
  a disposable PostgreSQL path through matching page/JSON export. See
  EVENT_HUMAN_APPROVAL.md. Production roles/schema, coordinated export/build
  activation, and autonomous qualification remain unimplemented.
  A narrowly approved, default-disabled activation guard now
  retains authority locks through the existing switch; locally tested only.
  It requires a complete coordinator-produced manifest and separate role before
  enablement. See EVENT_ACTIVATION_GUARD.md. Local Markdown
  and Events JSON preparation use the same pointer-matched quotations with joint
  content preflight and explicit hold/withdrawal selection. No production caller
  enables them. 728 offline / eleven real
  disposable PostgreSQL / 26 JavaScript tests pass;
  legacy bytes and the daily feed contract remain unchanged.
- Public checks passed after the last targeted rollout. Use the dated evidence in
  `STABILIZATION_VERIFICATION.md`, not this checkpoint as a live health monitor.
- Preserve one local LLM job, API-driven publishing, incremental feed history and
  daily JSON compatibility. No direct Hugo calls or production `init_db` diagnostics.

Use this checkpoint and the linked verification notes first. The February topology
and operational notes below are retained as historical context and can be stale.

---

## Runtime Topology

- **Postgres**: `sempervigil-db` (internal).
- **Admin API/UI**: `sempervigil-admin` on port `SV_ADMIN_PORT` (default 8001).
- **Discovery / Orchestrator**:
  - `orchestrator` (singleton control plane: due-source checks, schedules, launch policy, build admission)
- **Stage runners**:
  - `worker_fetch` runner (claims control launch jobs, executes bounded fetch-stage worker passes)
  - `worker_llm` runner (claims control launch jobs, executes bounded local-LLM-stage worker passes)
  - `worker_openai` runner (claims control launch jobs, executes bounded OpenAI-stage worker passes)
- **Builder**:
  - `builder` (one-shot, profile `build`)
  - `build_worker` runner (claims control launch jobs and executes admitted `build_site`)
- **Web**: nginx serving `/site-public` on `SV_WEB_PORT` (default 8080).

Key volumes (PVC-backed):
- `sempervigil-site-src-csi` mounted at `/site-src` -> Hugo source
- `sempervigil-site-public-csi` mounted at `/site` -> Hugo output
- `sempervigil-data-csi` mounted at `/data` -> runtime data (articles, CVEs, reports)
- `sempervigil-logs-csi` mounted at `/log` -> logs (admin/worker/build)
- `sempervigil-tools-csi` mounted at `/tools` -> shared tooling

The live stack is PVC-backed via `nfs-csi`; there is no runtime dependency on the old NFS share.

---

## Build System Behavior

- **Builds are dirty-state driven**: admin and workers mark the site dirty; the orchestrator admits `build_site`.
- **Single-build at a time**: orchestrator admits at most one pending/running `build_site`.
- **Runner poll interval**: `SV_RUNNER_POLL_SECONDS` (default 5).
- **Orchestrator tick interval**: `SV_ORCH_TICK_SECONDS` (default 30).
- **Debounce**: `SV_BUILD_DEBOUNCE_SECONDS` (default 60), applied at orchestrator admission time.

If builds are too frequent or CPU-pegged, verify both values in `.env`.

---

## Publishing Model (DB-backed)

- **Per-article markdown is disabled by default** (`SV_ENABLE_ARTICLE_MARKDOWN=false`).
- Front page uses data files in `site-src/data/`:
  - `data/articles/today.json`
  - `data/articles/recent.json`
  - `data/cves/today.json`
  - `data/cves/recent.json`
- Daily briefs are published at `/daily-briefs/YYYY-MM-DD/` and loaded on the homepage via feed index + day JSON.

**Homepage rendering**: `site-src/layouts/partials/home/custom.html` (Blowfish theme override).
**Current homepage**: `site-src/layouts/partials/home/test-front.html` (single-column news feed + yesterday brief tabs).

---

## Article + CVE Data Export

- Articles include tags, products, and optional `nist_family`.
- Article export now includes `summary_bullets` for list/search rendering.
- CVEs include `product_title`, severity, and a list of products.
- Product/vendor/threat detail page and public entity-search generation is disabled.
- Vendor/product/threat data remains internal enrichment metadata unless explicitly rendered in the daily feed JSON.

---

## Admin UI Expectations

- Dashboard metrics read from `get_dashboard_metrics()`; ensure `get_setting(conn, key, default)` is always called with a default.
- Jobs page supports filters + pagination.
- Sources page is dense and supports edit/expand; IDs shown in edit form.
- Article suppression exists in per-article menu and strikes in list.

---

## Known Pitfalls

1) **Builder OOM / killed hugo**
   - Hugo can be killed by the OS when memory/CPU spikes.
   - Verify with `build_worker` logs and system `dmesg`.

2) **Control queue backlog**
   - If stage work is not moving, inspect control-queue launch jobs first.
   - A growing `launch_*_worker` backlog means the orchestrator is admitting work faster than runners can drain it.

3) **Permissions flipping on site-src**
   - `fsinit._ensure_dir()` now only chmods on creation.
   - `SV_FIX_SITE_PERMS=0` in compose prevents aggressive chmod/chown.

4) **Stale builds / no changes**
   - If `build_worker` runs but output doesn’t change, confirm a build job exists and that Hugo succeeded.
   - Also verify `build_site.state` is being cleared after successful build completion.

5) **RSS probe/fetch timeouts (Sophos‑style feeds)**
   - RSS probe and ingest support curl HTTP/2 fetcher with Range prefixing.
   - Per‑source overrides can force `http_fetcher=curl` or `python_then_curl`.

6) **DB host resolution**
   - Orchestrator, runners, and builder all require direct DB reachability.

---

## Quick Start (Debug)

1) Request a build and watch the orchestrator/build runner:
   ```
   docker compose run --rm worker_fetch sempervigil jobs enqueue build_site
   docker compose logs --tail=50 orchestrator
   docker compose logs --tail=50 build_worker
   ```

2) Check output files:
   ```
   ls -la /site-public/index.html
   head -n 5 /site-public/index.html
   ```

3) Verify data files (source):
   ```
   ls -la /site-src/data/articles/today.json
   ls -la /site-src/data/cves/today.json
   ```

---

## Queue / Runner Roles

- control queue:
  - `launch_fetch_worker`
  - `launch_llm_worker`
  - `launch_openai_worker`
  - `launch_build_worker`
- fetch queue:
  - ingest, fetch, KEV sync, event web enrichment/promotion, rebuild jobs
- llm_local queue:
  - summarization, article/CVE enrichment, event derivation/report
- openai queue:
  - `build_daily_brief`
- build queue:
  - `build_site`

Stage runners should be configured by queue, not by large `SV_WORKER_ONLY_TYPES` lists.

---

## Related Docs

- Architecture: `ARCHITECTURE.md`
- Data Model: `DATA_MODEL.md`
- Pipelines: `PIPELINES.md`
- Admin UI: `ADMIN_UI.md`
- CVE Model: `CVE_MODEL.md`
- Event Model: `EVENT_MODEL.md`
