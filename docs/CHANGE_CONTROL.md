# Change Control Log

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
