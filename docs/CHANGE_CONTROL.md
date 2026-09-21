# Change Control Log

## 2026-09-19: Kubernetes private article trial deployed and evaluated

- Admin/LLM image `3d2a981`, platform `583da4c`; explicit default-disabled Helm
  flag wired before apply. Image manifest identical on four eligible nodes:
  `sha256:b4df615f67ec344b86b71010b1b457812503ef3418ffae03d45561b98e2d1098`.
- Compared namespace-qualified dry-run diff: four image references and one
  private enablement key only. Paused admission/drained old LLM pod, applied only
  ConfigMap and admin/LLM Deployments, then restored admission. No public rebuild.
- Job `job_86bd8979191e493797025e9a71d790fc`: 3 calls / 20.092s, all context
  candidates invalid, no summary generation or repair. Failed quality recorded,
  not hidden. Repeat admission returns the same completed job without inference.
- Three article baseline hashes unchanged; public event pointer unchanged.
  Twenty sampled public checks pass before/after; all deployments and API Ready.
  Scoped render/live diff empty; no profile, resources or concurrency changes.
- 1004 offline / one disposable PostgreSQL test passed, one offline skip. Test pod
  and forwarding removed. Previous admin `eff906d` and LLM `d80f98a` retained for
  rollback. Unrelated dirty platform files preserved.

## 2026-09-19: Kubernetes release recovery and queue integration gate

- Recovered and verified the established remote image-builder/K3s import path
  from prior successful release commands. Local Docker was never required; the
  prior access blocker was an operator-context error. Documented the workflow.
- Corrected the admin connection lifetime (DBConn is not a context manager) and
  private guard to match claim_next_job's unchanged zero attempt_count. Persistent
  result reservation, not that unused counter, prevents automatic replay.
- 1004 offline tests pass, one skip. One disposable PostgreSQL lifecycle test
  passes in an isolated Kubernetes pod, exercising admission, deduplication after
  success, actual claim and private result persistence with mocked inference.
- The initial full bootstrap exposed an existing products-table ordering issue;
  no production migration was run or changed. The queue integration test uses
  explicit disposable tables matching the queue fields, not a full migration test.
- Scope for release: admin and local LLM worker images plus private comparison
  flag only. Strict normal validation stays off; no builder/web/public changes.

## 2026-09-19: Private article comparison queue, local only

- Add separate default-disabled admin admission/LLM queue dispatch and dashboard
  registry. Pin source/baseline/generation, deduplicate across final statuses,
  reserve before inference, reject replay, stop on transport errors, no repairs.
- Private results only; no article, event, build or profile writes. Three articles
  imply at most six attempts in the existing serialized lane. Provider timeout
  retained; ten-minute guard only applies before starting another phase.
- Gate previous normal article strict-validation work off by default, preserving
  deployed legacy behavior. Explicit strict-mode tests remain enabled in tests.
- Not deployed; unavailable local Docker and failed deployment SSH authentication
  prevent image release. User asked for supported access route. No model calls.
  Disposable-DB and rendered/live validation remain pending. See ARTICLE_PRIVATE_QUEUE.md.
- Full offline suite: 1003 passed, one skipped; whitespace diff check passes.

## 2026-09-19: Duplicate-safe private article evidence

- Version the undeployed contract to v3; preserve all exact quotation occurrences
  rather than rejecting duplicate stored article passages. Bound matches to 32;
  first occurrence is a display anchor only, not inferred semantic context.
- Revalidate complete occurrence lists on replay. Tests cover duplicated source,
  modified records, overlap and repetition bounds. 31 targeted tests pass.
- Full offline suite: 974 passed, one skipped; integration tests not collected.
- Saved SolarWinds fixture validates both original spans without source changes.
  Fixture is manually specified, not new LLM output. No production/model writes.
- Document bounded queued comparison sequence. Queue integration/generation remain
  pending. Rollback is source-only; no deployed components reference this module.

## 2026-09-19: Stored article baseline comparison, read only

- Compare three saved security articles to existing LLM summaries and context;
  record source-grounded qualification, attribution, date and structure findings.
  Save a local self-contained review and freeze expected candidate outcomes.
- Read-only database transaction, bounded three-record export to ignored local
  data. No model, profile, production data, deployment or application changes.
- Reproduce candidate unique-quote failure on duplicated stored source text; do
  not describe candidate prompts as evaluated. All three full requests fit budget.
- Documentation-only slice; local contract checks exercised against saved data.
  No full regression or integration rerun. Rollback: remove review/docs entry.

## 2026-09-19: Private versioned article evidence contract

- Pure context/summary request and validation functions retain exact provenance,
  assertion/date roles and fact-linked prose. Private preview preserves feed field
  types; no runtime caller, schema migration, inference, profile or public writes.
- 26 targeted tests cover bounds, missing/ambiguous citations, altered evidence,
  unknown references, dates, abstention and the distinction between provenance
  and semantic correctness. Real model canary is pending safe queued admission.
- Do not deploy earlier strict validation into normal article jobs merely to run
  a canary. Current synchronous admin profile tests are not the shared queue.
- Rollback: remove this unreferenced module/tests; production is unchanged.
- Full offline regression suite: 969 passed, one skipped; integration tests were
  not collected. Whitespace diff checks pass.

## 2026-09-19: Shared article enrichment validation, local only

- Inspected live summary/context profiles and newest 100 paired stored outputs.
  39 context records fail the existing schema; all summaries pass basic shape.
  Structural findings do not establish factual accuracy. No model calls or edits
  to production profiles/data/deployments.
- Validate router success and usable object/array fields before either worker
  persists enrichment. Remove raw/list fallbacks. Error-only updates retain prior
  payload/model/generation stamp instead of erasing results on failed refresh.
- 943 offline tests pass, one skip, including feed contracts and invalid-output
  downstream isolation. No DB integration tests or rollout yet. Local rollback:
  revert this slice. Do not deploy rejection alone given the measured failure rate.
- Revise architecture to strengthen existing article enrichment and reuse it for
  Events, not add routine per-event extraction. See ARTICLE_ENRICHMENT_QUALITY.md.

## 2026-09-19: Events architecture review and sequence reset

- Read-only trace of legacy, managed quotation and private deconstruction paths,
  plus live readiness/model/job aggregates. No inference, deployment, profile,
  scheduler or application changes.
- Document evidence-backed keep/simplify/retire decisions and missing narrative,
  correction, lifetime-history and incident-identity contracts. Freeze per-claim
  judge iteration as a development direction, not a live flag change.
- Next proposed gate is one report/update/correction/replay plus blind incident,
  at most 14 provider attempts. No claim of successful report quality or faster
  measured generation. Tracker marks R1-R4 unimplemented and historical entries
  are collapsed to avoid competing status instructions.
- Verification: documentation links/HTML structure and whitespace diff checks.
  No application tests rerun for documentation-only changes. Rollback: revert docs.

## 2026-09-19: Private evidence inspection without new inference

- Add bounded, offset-preserving source context and source-section/date coverage
  to private audit rendering. Load and validate cached phase reasons against
  request/generation/verdict before displaying them. No additional model calls.
- Exact quotes and decisions remain unchanged. Neighboring text is marked as
  display-only, not retroactive support or automatic pronoun resolution.
- 881 offline tests pass, one skip; four real cached audits rendered locally.
  No production deployment, config, model, schema, publication, or build changes.
- Rollback: revert this local rendering/module/test slice. Existing immutable
  artifacts and V4 inference receipts remain valid and unchanged.

## 2026-09-19: Replay determinism and measured comparison-rule correction

- V3 actual results: eleven of twelve bad claims rejected, one falsely accepted;
  two of five supported claims accepted, two rejected and one held. Frozen gold
  remains unchanged. Recorded reasons expose reversed entailment (requiring the
  summary to repeat every citation detail) and an invented category relationship.
- V4 explicitly preserves entailment direction and forbids assumed security-label
  equivalence. No new authority or resource changes. New five-claim source cohort
  frozen before its first audit; actual V4 results pending.
- Fix source request field ordering on sorted artifact reload while retaining the
  exact original snapshot wire order and existing live cache keys. Regression test
  reconstructs the original source receipt from saved JSON without new inference.
- Add offline fixed-cohort evaluator: missing cases, duplicates, holds and false
  decisions cannot be counted as a pass. No expectation changes to improve scores.

## 2026-09-19: Isolate quotation support from source-context audit

- V3 asks one textual comparison at a time. Quotation phase receives no article,
  scope metadata or extraction confidence label that could substitute for citation
  support. Only a supported quotation proceeds to full-source/incident context.
- Both phases return a bounded reason and verdict and have separate immutable
  caches. Context not performed is explicitly `not_assessed`, never inferred as
  a pass. The full source still binds cache identity; no source truncation.
- At most two serial calls per claim, with early rejection and resumable caching.
  No model/context/resources/concurrency change or publication authorization.
  Frozen pilot expectations remain unchanged; real V3 validation pending.

## 2026-09-19: Narrow support audit after failed batch evaluation

- The actual 17-claim batch completed, but rejected three supported claims,
  falsely accepted one actor attribution, and used unsupported-date judgments
  for null dates. Aggregate decisions alone overstated its ability to detect
  citation mismatches. No publication occurred.
- V2 asks one claim per serialized call, with resumable per-claim cache. At most
  eight calls per source instead of one; this explicit private pilot remains
  low-priority/manual. Null dates are deterministic no-assertion cases, not LLM
  judgments. No model, context, resources, concurrency or publication changes.
- Frozen expectations remain unchanged. V2 runtime evaluation pending.

## 2026-09-19: Private support queue integration

- Add explicit `audit_source` digest admission to existing private-review jobs,
  separate disabled flag/profile and pinned local schema transport. Load only an
  existing source-bound extraction; never silently extract or publish.
- Existing Jobs artifact viewer and LLM telemetry are reused. No new worker lane,
  model, concurrency, build, schema, public content or automatic scheduling change.
- 856 offline tests pass, one Linux-only skip. Freeze 17 actual-pilot expectations
  before real inference. Rollout and measured outcomes pending at this commit.

## 2026-09-19: Local private claim-support audit contract

- Added a full-source, bounded audit of incident, entailment, attribution,
  uncertainty, dates and surrounding context. Exact response inventory required;
  unsupported/uncertain outcomes cannot become model-supported proposals.
- Source/claim/checker-bound immutable cache avoids repeating unchanged audits.
  No claim repair, publication receipt, runtime caller, API, schema or deployment
  change. Contract tests use mocked completions; model accuracy remains unmeasured.
- See EVENT_CLAIM_SUPPORT.md for queue integration and real-model acceptance gate.
  Rollback is source-only; working production is unchanged.

## 2026-09-19: Real private pilot completed, publication still gated

- Current admin `69e76a6`, LLM worker `c4f8ddf`, platform `5c6044d`.
  Render/live scoped manifest comparison is empty. No builder, web, source,
  feed, schema, resource or concurrency changes. Orchestrator admission resumed.
- Intermediate date schema caused Ollama grammar parsing to fail on a regex
  escape and its inference subprocess to crash. Removed regex/conditional grammar
  from this private contract; derive and validate precision in application code.
  No AI platform configuration change or manual model restart was needed.
- Three corrected extractions succeeded, compiling 17 private claims. Replay
  reused the cache without inference. Semantic citation failures remain explicit;
  the public report was not replaced. See STABILIZATION_VERIFICATION.md.
- Rollback: disable the separate deconstruction flag, drain and roll only the
  affected admin/LLM components. No data migration or public rollback required.

## 2026-09-19: Private deconstruction pilot and constrained generation

- Rolled only admin and LLM worker to `69e76a6`, preserving the existing model,
  single inference lane, builder and feeds. Added the missing chart ConfigMap
  fields and a dedicated profile through the AI API. Admission was paused and
  jobs drained during rollout; normal orchestrator admission was restored.
- First real private extraction failed closed (`invalid_deconstruction_values`)
  after 13.041s. Captured output used null date-role/precision, an unsupported
  section and altered quotations. No public or report writes occurred.
- Correct the generation contract, not the validator: local private mode gets
  JSON-schema enums and exact source-sentence choices. Count schema bytes within
  the existing 15KB input budget. No concurrency, model or memory increase.
- Subsequent model verification and final deployment state are recorded in
  STABILIZATION_VERIFICATION.md. This does not authorize automatic narrative
  publication or legacy-candidate deletion.

## 2026-09-19: Private source reuse and compilation (local only)

- Source/config-bound extraction cache and multi-source private compilation use
  the existing review job/viewer. No additional model calls for compilation.
- Current evidence is revalidated on cache reads. Material claim change records
  distinguish additions/withdrawals/retention, not factual adjudication.
- Read-only production legacy-candidate inventory completed; no deletes, writes,
  build changes, schema changes or rollouts. See EVENT_LEGACY_RETIREMENT.md.

## 2026-09-19: Private source-level deconstruction (local only)

- Add an opt-in mode to the existing private review API/job. Separate disabled
  flag and pinned profile; retain one local LLM queue and existing job viewer.
- Claims and escaped HTML remain private, with no publication approval receipt.
  No build, feed, activation, public export, schema or production changes.
- Real-model evaluation, multi-source synthesis and incremental reuse remain open.
  See EVENT_DECONSTRUCTION.md. Default-disabled rollback needs no data migration.

## 2026-09-19: Bounded automatic Events admission

- User requested continued implementation until updates elevate through the
  production path. Add default-disabled enrollment of already reviewed scopes,
  paired private review admission and deterministic short-quotation policy.
- No build/activation, Hugo, feed, inference concurrency or source scrape changes.
  Only orchestrator needs a new image and restricted admission credentials.
  Policy decisions enqueue the existing promotion worker; that worker triggers
  the existing dirty-build path. See EVENT_AUTOMATION.md for limits and rollback.

## 2026-09-19: Explicitly approved atomic guarded switch

- User approved the narrow atomic-switch correction after transient public 404s
  during pilot verification. Replace only the enabled guard's `ln -sfn` callback
  with a same-directory temporary symlink and `os.replace` rename. The disabled
  legacy shell branch is unchanged. Retention, Hugo and feed generation unchanged.
- A child process preserves the existing two-second callback timeout and source/
  authority locks remain held. Tests cover concurrent readers without an absent
  live name, failed-rename retention, and invalid candidates. Real API build and
  public checks during switching are required before closing the observed issue.
- Deployed as builder `bfa9986` with platform `4d852c2`. Two API builds succeeded;
  875 sampled public requests across/post switches had no failures. Details and
  NFS caveat are in STABILIZATION_VERIFICATION.md. No broader behavior changed.

## 2026-09-19: Activation interpreter correction during pilot

- API build `job_32279890e4874b38b337bc0f8b170d0e` completed Hugo rendering but
  refused activation: system `python3` lacked `psycopg`. Prior release stayed live.
- The builder now passes its own `sys.executable` to the approved activation hook.
  No dependency installation or Hugo command changes; disabled builds inherit
  their environment exactly as before. Added no-spawn interpreter regression tests.

## 2026-09-19: Builder-owned qualified Events release integration

- Problem: `worker._publish_events` wrote shared Events inputs independently of
  the builder; the new pointer path had no runtime exporter. An authority-only
  manifest could not prove which rendered page/JSON would become live.
- Change: default-disabled builder-only export, output-bound manifest, exact
  HTML/JSON and current-source verification at the approved activation hook.
  Workers request ordinary builds after promotion. No Hugo command, cache,
  resource limit, daily JSON schema or historical rebuild changes.
- Changed evidence withdraws only that managed report, without legacy fallback.
- Added role-specific existing-Secret wiring and render-time configuration checks.
- Verification: offline release/chart tests and real disposable PostgreSQL
  activation/source-lock tests. No production changes at this checkpoint.
- Risk: medium when enabled; requires a coordinated all-writer rollout and an
  API-driven pilot. Disabled path retains existing publication behavior.
- Rollback: retain verified images/live release and Events-only source snapshot;
  stop admissions and coordinate writers. Never bypass a failed candidate guard.

## 2026-09-18: Builder-only historical catch-up

- Added optional, validated chart override `buildWorker.feedArchiveBackgroundDays`;
  production uses 1 while the shared limit remains 0. No application-code changes.
- Only builder Deployment env changed. Paused admission, waited for orchestrator
  termination and build drain, replaced builder, resumed. LLM workers untouched.
- 96 offline tests, server-side diff/spec comparison, 35.232s API build and 33.217s
  following automatic build passed. August 15 recovered 553 CVEs; exact IDs match
  DB. All 5,059 files retained, 5,057 unchanged in first comparison; public checks pass.
- Rollback: builder override 0 and scoped drained rollout; retain repaired JSON.

## 2026-09-18: Summary telemetry attribution rollout

- Deployed ingest `27b9fb3`; retained builder `889b2de` and existing admin/web.
- Runtime scope: four null job IDs replaced with the actual summary/context job ID.
- Verified 88 offline tests, image-only server-side diffs, live source hashes,
  rendered/live spec agreement, linked successful records for both handlers,
  28.309s API-selected build, and 21 public release checks.
- Shutdown race interrupted one old context task; canceled only the abandoned
  records through the admin API and requeued the original payload. Future drains
  must recheck queues after orchestrator termination, not during its grace period.
- See STABILIZATION_VERIFICATION.md for exact evidence and task IDs.
- Rollback: restore platform ingest tag `889b2de`, render, drain, and apply only
  the five affected Deployments. No DB migration or content changes in this release.

## Purpose
This log records all changes that affect build/publish/serve stability, docker-compose mounts, permissions, and Hugo build mechanics.

## Scope (Must Be Logged)
Changes MUST be logged if they touch any of:

- docker-compose.yml volumes/mounts for data/site-src/site-public
- web container user/permissions or nginx document root/ports
- tools/hugo-build.sh (locks, retries, config override, cache/resource dir behavior)
- publish pipeline semantics that affect Hugo reads (atomic write behavior)
- directory creation/permission “fixers” or background chmod/chown behavior
- build scheduler cadence/backoff and any job claiming rules that impact build frequency

## Prohibited Changes Without Explicit Approval

- Reintroducing hardcoded `/nfs` paths in docker-compose.yml
- Using unsupported Hugo flags (e.g., `--resourceDir`)
- Adding rsync/staging/complex rebuild steps unless tied to a reproducible failure + logs

## Required Entry Format (Template)

```
- Date: YYYY-MM-DD HH:MM (local)
- Author: <name or Codex>
- Summary: <1–2 lines>
- Motivation / Problem:
- Files changed:
  - <path>
- Risk / Impact: low | med | high
- Verification steps run:
  - <command>
- Outcome: success | failure
- Rollback plan:
```

## Contributor Rules (Codex)
If you (Codex) propose edits affecting pipeline stability, you must:
1) paste the relevant logs/errors that justify the change, and
2) update CHANGE_CONTROL.md in the same PR/commit.

---

## Entries

- Date: 2026-09-18
- Author: Codex
- Summary: Guarded production rollout of runtime 889b2de and chart 7eb3d14.
- Scope: affected application workers/orchestrator only; retained old image
  dependency layers, resource limits and Hugo commands. Background catch-up 0.
- Verification: 58 offline tests; cross-node lock; runtime source hashes; rendered
  runtime diffs limited to images/one environment setting; API build 42.725s and
  next normal build 24.125s. First peak 428 MiB; 5,059 archive files retained.
  Sampled public IDs exactly match DB. 85 public availability checks passed.
  No new host OOM; API/etcd ready; no updated-pod restarts.
- Incident: temporary rollback snapshot under /data caused one pre-rollout Hugo
  build failure. Moved it to /log before verification; prior release stayed live.
- Outcome: guarded rollout verified, historical background repair still paused.
- Rollback: previous image tags retained in environment repo history; feed snapshot
  retained outside Hugo inputs. Drain writers before restoring images/files; use
  the API for publication. No database rollback or recurring backup introduced.

- Date: 2026-09-18
- Author: Codex
- Summary: Serialize archive writers before the staged production rollout.
- Motivation / Problem: The Hugo lock starts after JSON export; archive writers
  could otherwise race on day temporary files and manifest replacement.
- Files changed: worker.py; offline archive lock test; source-only release image
  recipe. No dependency, Hugo command, cache, or concurrency setting changes.
- Verification: 56 offline tests pass; lock exclusion and release after exceptions
  verified. Cross-pod lock validation and API-driven rollout pending.
- Risk / Impact: medium; writers wait on a shared filesystem lock during archive
  inventory and writes. Public serving never acquires this lock.
- Rollback plan: retain previous images and export snapshot. First rollout disables
  background catch-up; do not enable backlog until public validation passes.

- Date: 2026-09-18
- Author: Codex
- Summary: Prepare dependency-aware feed invalidation and bounded historical repair.
- Motivation / Problem: Base timestamps miss exported enrichment changes. Read-only
  audit counted 50,540 CVEs with newer EPSS check timestamps; linked-data removals
  also cannot be detected reliably by maximum base timestamps.
- Files changed: feed_inventory.py; storage.py; worker.py; offline inventory and
  archive tests; .env.example; build/verification/tracker documentation.
- Risk / Impact: medium. Adds a read-only database fingerprint scan (observed
  2.68-4.17s), not historical payload generation. Changed historical dates are
  repaired in batches of at most 25 during normal refresh with a soft five-second
  background budget. Explicit archive jobs retain full catch-up capability.
- Verification: 55 offline tests; 12 PostgreSQL read-only synthetic mutation
  simulations, including deletions and bookkeeping-only changes. No direct Hugo
  invocation, production write, deployment, or LLM concurrency change.
- Outcome: local implementation only. Coordinated writer rollout, concurrency
  review, API publishing verification and actual build measurements remain.
- Rollback plan: revert this commit before release. At rollout preserve previous
  images and manifest snapshot; pause background repair with
  SV_FEED_ARCHIVE_BACKGROUND_DAYS=0 if necessary. No production rollback needed now.

- Date: 2026-09-18
- Author: Codex
- Summary: Correct daily refresh targeting and CVE-only exports locally.
- Motivation / Problem: Recent exports returned before CVE processing when no
  articles existed; localized CVE timestamps could select the preceding archive
  date. Stored article brief dates were absent from recent query results.
- Files changed: storage.py, worker.py, offline selection/export tests, tracker,
  and stabilization verification documentation.
- Risk / Impact: medium; additive internal brief_day query field, broader correct
  day targeting, and removal of the premature return. No public schema migration,
  Hugo command changes, deployment changes, or LLM concurrency changes.
- Verification: 37 offline checks pass. Read-only full-day serialization of two
  large dates measured 0.550s and 0.362s, excluding disk writes and publishing.
- Outcome: local only. Audit confirms enrichment updates are not reliably covered
  by the base timestamps used for archive invalidation. This blocks rollout until
  dependency-aware freshness is implemented and tested. No historical repair run.
- Rollback plan: revert this isolated commit before rollout. No production state
  was modified.

- Date: 2026-09-18
- Author: Codex
- Summary: Prepare complete-day, manifest-backed recent feed exports.
- Motivation / Problem: Offline reproduction showed limited recent results
  replacing a previously complete day. Read-only production queries found 1,031
  and 982 CVEs on dates whose public downloads held only 10 and 5. The archive
  exporter also imposed a 500-CVE cap.
- Files changed: src/sempervigil/worker.py; src/sempervigil/storage.py;
  tests/offline/test_feed_contract.py; tests/offline/test_recent_archive_parity.py;
  tracker and verification documentation.
- Risk / Impact: medium. Uses the existing full-day serializer and atomic writer;
  internal manifest schema increases to 3. Ordinary refresh is scoped to recent
  days. All existing payload fields remain; the historical article id alias is
  additive for recent exports. Suppressed articles are excluded consistently.
- Verification: 32 offline tests pass; read-only production uncapped selections
  returned all expected unique IDs. Inventory query 0.403s; selections 0.143s and
  0.064s. No build, rollout, production file writes, or direct Hugo invocation.
- Outcome: local implementation only. Date-boundary, enrichment freshness,
  CVE-only behavior, and full export performance remain release gates.
- Rollback plan: revert this isolated source change before release; retain prior
  deployment image and archive snapshot for eventual rollout. No environment
  configuration or application concurrency changed in this slice.

- Date: 2026-09-18
- Author: Codex
- Summary: Prepare atomic replacement of recent daily feed JSON files.
- Motivation / Problem: The running recent exporter writes directly to shared
  public day paths. An offline fault-injection test reproduced a partial public
  file after an interrupted write. Historical archive writes already use the
  existing atomic helper.
- Files changed: src/sempervigil/worker.py; tests/offline/test_recent_archive_parity.py
- Risk / Impact: low; same serialization, payload, path, and export selection;
  one temporary file and replacement per already-written day.
- Verification: offline parity and interrupted-write regression tests; full
  offline suite. No direct Hugo invocation.
- Outcome: local correction prepared; production deployment and API-driven
  publication verification are pending. Limited-window completeness is not fixed
  by this change.
- Rollback plan: revert this single call-site change and use the supported image
  and API deployment workflow. Preserve the previous production image until
  rollout verification passes.

- Date: 2026-07-24 16:25 EDT
- Author: Codex
- Summary: Retired the public `/entities/` search surface and removed stale entity redirect/static generation paths.
- Motivation / Problem: The site does not expose or use public entity browsing, and stale `/entities/` links/exports contradicted the daily-feed-only model while adding persistent site artifacts.
- Files changed:
  - src/sempervigil/worker.py
  - src/sempervigil/builder.py
  - deploy/helm/sempervigil/templates/configmap-nginx.yaml
  - docker/nginx/default.conf
  - tests/test_vendor_product_indexes.py
  - tests/test_builder_legacy_posts_cleanup.py
  - docs/CURRENT_CONTEXT.md
  - docs/PIPELINES.md
  - docs/TARGET-STATE.md
  - docs/VENDOR_PRODUCT_PAGES_PLAN.md
  - docs/CHANGE_CONTROL.md
- Risk / Impact: med
- Verification steps run:
  - python3 -m py_compile src/sempervigil/worker.py src/sempervigil/builder.py
  - python3 -m pytest tests/test_vendor_product_indexes.py tests/test_builder_legacy_posts_cleanup.py
  - pending: production build through SemperVigil build worker/API
- Outcome: pending; pytest collection requires SV_DB_URL in this environment.
- Rollback plan: Revert this entry and the listed app/Hugo cleanup changes, rebuild the previous builder image tag, restore the previous platform image tag if deployed, and redeploy the build worker.

- Date: 2026-07-24 15:59 EDT
- Author: Codex
- Summary: Moved deprecated entity/CVE aggregate exports out of Hugo data loading and added builder cleanup for stale high-cardinality render inputs.
- Motivation / Problem: Production builds were still loading legacy `product_map.json`, `cves.json`, `vendor_map.json`, and related templates from persistent `/site-src`, driving Hugo memory toward the 24Gi safety limit even after the feed archive fix.
- Files changed:
  - src/sempervigil/worker.py
  - src/sempervigil/builder.py
  - tests/test_vendor_product_indexes.py
  - tests/test_builder_legacy_posts_cleanup.py
  - docs/CHANGE_CONTROL.md
- Risk / Impact: med
- Verification steps run:
  - python3 -m py_compile src/sempervigil/worker.py src/sempervigil/builder.py
  - python3 -m pytest tests/test_vendor_product_indexes.py tests/test_builder_legacy_posts_cleanup.py
  - production build through SemperVigil build worker/API: job_38ede24a38fd405f8abd754335591685
- Outcome: success; pytest collection requires SV_DB_URL in this environment, so production build-worker execution was used as the representative runtime validation.
- Rollback plan: Revert this entry and the listed code/test files, rebuild the prior builder image tag, restore the previous platform image tag, and redeploy the build worker.

- Date: 2026-01-31 00:00 (local)
- Author: Codex
- Summary: Stabilized Hugo build/serve pipeline and publishing writes.
- Motivation / Problem: Hugo builds failed due to unsupported flags and non-atomic writes during publish. NFS/Synology mounts required portable defaults and predictable permissions.
- Files changed:
  - tools/hugo-build.sh
  - src/sempervigil/utils.py
  - src/sempervigil/publish.py
  - src/sempervigil/pipelines/daily_brief.py
  - docker-compose.yml
  - docs/BUILD_PIPELINE.md
- Risk / Impact: med
- Verification steps run:
  - docker compose up -d --build
  - docker compose exec build_worker sh -lc 'ls -la /site/index.html'
  - docker compose exec web sh -lc 'ls -la /usr/share/nginx/html/index.html'
  - curl -I http://localhost:${SV_WEB_PORT:-8080}/
- Outcome: success
- Rollback plan: Revert listed files to prior versions and rerun verification steps.
# 2026-09-19: low-cost hosted Event editorial composition

- Summary: Replace the unsuccessful local-model section-selection experiment
  with a one-call, evidence-bound OpenAI editorial composer for private Events.
- Scope: accepted active ledger facts only; `gpt-5.6-luna`; existing serial
  `openai` queue; one attempt; no repair, retry, fallback, public write, feed
  change, build admission, Hugo invocation, or article-summary change.
- Controls: permanent revision/config request identity, strict JSON schema,
  paragraph-level immutable fact references, deterministic timeline labels,
  section authorization, complete dated-fact coverage and explicit human review.
- Verification: 1,034 offline tests passed, one skipped; focused 13-test contract
  passed; live credential/model structured-output probe returned valid JSON.
- Status: local implementation only. Production migration, targeted worker/admin
  rollout and one private WaterPlum canary remain pending.
- Rollback: retain the prior image, disable
  `SV_EVENT_LEDGER_COMPOSITION_ENABLED`, and restore the prior admin/OpenAI worker
  tags. No public-content rollback is required.

# 2026-09-20: normalize Event fact-curation passage transport

- Summary: Replace repeated per-fact passage bodies with one request-level exact
  passage table and immutable per-fact passage IDs.
- Scope: private `event_fact_curate` requests only; no stored evidence, article
  summary, daily JSON, public Event, model, concurrency, build or Hugo change.
- Controls: unchanged 48,000-byte guard; conflicting passage identities fail
  before inference; workflow/request identities advance to v3; migration 053
  reactivates only the exact packaging-related hold reason.
- Verification: Vercel's blocked request measured 48,792 bytes before and 17,087
  bytes after normalization; all eight source requests fit below 17,625 bytes.
  Seven focused tests and the complete offline suite pass: 1,107 passed, one
  skipped, four existing warnings.
- Status: local implementation only; targeted admin, orchestrator and hosted-model
  worker rollout plus a live recovery canary remain pending.
- Rollback: restore the prior three image tags. Migration 053 is state-only and
  need not be reversed; a reactivated case can safely be held again by the prior
  request guard.

# 2026-09-20: terminate failed one-time Event repairs

- Summary: Recognize a held composition created by a successful repair job and
  stop the reassessment case after its failed second audit instead of repeatedly
  returning the already-completed repair job.
- Scope: autonomous reassessment coordinator only; no prompt, model, evidence,
  audit, repair, publication, build, Hugo or public data change.
- Controls: repair lineage is read from the immutable successful job result; the
  existing support-audit hold remains authoritative and no second repair is added.
- Verification: 12 focused tests pass; full offline suite passes 1,108 tests with
  one skip and four existing warnings. Production observation reproduced the
  loop against repaired composition `elc_9fe4...` before implementation.
- Status: local implementation; orchestrator-only rollout pending.
- Rollback: restore the prior orchestrator image. The affected case would resume
  looping but its held composition would remain non-public.

# 2026-09-21: allow complete large Event composition audits

- Summary: Increase only the hosted composition audit's completion allowance from
  1,800 to 4,800 tokens and align its response parser cap with the existing strict
  schema by raising it from 12 KB to 20 KB.
- Scope: private `event_composition_audit` jobs only; no request, evidence, prompt,
  model, concurrency, composition, publication, build, Hugo or feed change.
- Motivation: Vercel's 42-item audit request was 25,795 characters, but the model
  spent all 1,800 completion tokens on reasoning and returned no visible JSON.
- Controls: unchanged 48,000-byte input guard and strict output schema; migration
  054 reactivates only `composition audit failed: input_size` cases.
- Verification: schema-maximum response regression test plus full offline suite:
  1,109 passed, one skipped, four existing warnings.
- Status: deployed as application `3e150bb`, platform `06a2837`. Both original
  and repaired Vercel audits returned complete JSON; the repaired narrative was
  held on substantive support findings. Rendered/live diff is empty and public,
  cluster and node health checks pass.
- Rollback: restore the prior three image tags. Migration 054 is state-only; the
  prior worker will safely hold the case again if its response is empty.

# 2026-09-21: retain Event publication history for public daily views

- Summary: Export immutable qualified Event promotion history and use it for a
  paginated public archive plus a two-card Event-update lead on affected days.
- Scope: Event export/activation verification and public Hugo presentation only;
  no article or CVE daily JSON schema, content generation, qualification rule,
  model, prompt, queue, build command, cache, concurrency or atomic activation
  change.
- Controls: canonical Event URLs and exact history are verified against the
  database under the activation lock. The existing daily JSON remains the sole
  source for the news/CVE feed and download.
- Verification: application offline suite, focused Event release tests and
  JavaScript syntax checks pass. Production API build
  `job_d0ebf3dd0b2d4ea09c224748bd8e825c` succeeded in 19.03 seconds and
  atomically activated release `20260921211933`. Browser checks confirmed the
  Event archive filters, independent daily Event pager and empty-day omission.
- Rollback: restore the prior application builder image and Hugo source commit,
  then invoke the normal build API. Retained database revisions are immutable and
  require no data rollback.

# 2026-09-21: establish the CyberNews public identity

- Summary: Replace the public SemperVigil product branding with CyberNews by
  JasonDaemon.net and introduce a consistent editorial presentation across the
  homepage, Event archive and Event reports.
- Scope: Hugo templates, CSS, client-side presentation, favicon assets and public
  explanatory copy only. Article publisher icons, direct publisher links, Event
  citations and the SemperVigil open-source credit are retained explicitly.
- Controls: no application code, database, prompt, model, queue, source content,
  build command, cache, concurrency, publication policy, daily JSON schema or
  daily JSON generation change. The existing download path remains
  `/feed/days/<date>.json`.
- Verification: JavaScript syntax and source diff checks passed. API build
  `job_610957b961ac434cb9982d730dfda73f` completed in 21.51 seconds and atomically
  activated release `20260921215111`. Homepage and Events return HTTP 200. The
  September 21 daily JSON returns HTTP 200, is 1,054,341 bytes and contains 171
  items. Live HTML contains 73 publisher icons and 75 explicit source labels;
  browser console validation reported no warnings or errors.
- Source of truth: Hugo commits `35158d20` and `afe75547` on
  `codex/remove-cybernews-ads` are pushed to the configured remote.
- Rollback: restore the prior Hugo commit in `/site-src` and invoke the supported
  build API. No database, feed archive or application rollback is required.

# 2026-09-21: redesign the CyberNews daily edition

- Summary: Replace the oversized campaign hero and uniform article stack with a
  compact publication masthead, featured lead report and responsive two-column
  news grid. Remove explanatory interface prose from the daily desk.
- Scope: Hugo homepage markup, CSS and client-side presentation only. Publisher
  icons, publisher names, direct source links, Event paging, date navigation,
  settings and the daily download remain present.
- Promotional policy: omit titles containing webinar, sponsor/sponsored/
  sponsorship, advertorial, registration or virtual-event language, plus known
  `/webinar`, `/spons`, `/sponsored` and `/advertorial` URL paths. Matching is
  title/URL based to avoid dropping legitimate reporting whose summary discusses
  sponsored search abuse.
- Feed contract: the filter affects only the rendered news view. Daily JSON
  content, order, schema, archive path and downstream behavior are unchanged.
- Verification: JavaScript syntax and source diff checks pass. API build
  `job_0a174f8a24f64e02ab2f3283a9f00e53` completed in 20.15 seconds and activated
  release `20260921221022`; live DOM inspection found no promotional matches,
  retained the dated JSON link and reported no browser warnings or errors.
- Availability observation: a browser request made exactly at activation received
  a transient nginx 404. Five immediate command-line probes and the browser reload
  returned 200. This does not invalidate the current release but remains evidence
  that the end-to-end serving path is not yet proven gap-free at switch time.
- Source of truth: Hugo commits `d3628f58`, `203675f5`, `81578586` and `4d628ef4`
  on `codex/remove-cybernews-ads` are pushed to the configured remote.
