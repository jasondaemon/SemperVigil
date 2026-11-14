# Mainline stabilization, November 14, 2026

## Scope

Hold new Event enrichment feature work. Preserve the existing production Event
flags and publication path while qualifying the current article, daily JSON,
and Hugo pipeline for promotion from `codex/analyst-event-reports` to `main`.
No Event flags or production image tags were changed in this pass.

## Production evidence

- The admin, build, fetch, local-LLM, and hosted-worker Deployments were ready.
  Platform values pin component-specific images; advancing `main` alone does
  not change those production image tags.
- A November 14 article from an enabled Australian source completed acquisition,
  content fetch, and a hosted Luna summary. A later normal Hugo build succeeded.
  The article occurs once in `/feed/days/2026-11-14.json`, which and the root
  page both returned HTTP 200. This is observation of the normal pipeline, not
  a synthetic forced publication or proof that every source is healthy.
- The local LLM worker was missing `SV_REGULATORY_FEED_START_DAY` even though the
  shared ConfigMap and platform values contained it. It therefore routed some
  eligible summaries to Qwen. With no jobs queued or running, only that worker
  was restarted. Its new pod is ready and has both hosted-routing values.
  Historical articles before the forward-only cutoff may still use Qwen by
  design. The chart now hashes those two values into that worker's pod template
  so a future Helm value change restarts the affected worker; this chart change
  is branch-tested, not deployed.

## Local verification

- Offline suite: 1,531 passed, 2 skipped.
- Fresh PostgreSQL 18 smoke tests passed independently for admin content search,
  admin AI, admin danger controls, build jobs, feed facets, vendor/product feed
  output, and fresh bootstrap. Each module used a separate disposable database.
- The fresh-install path now creates `products` before `article_products` and
  reconciles all remaining versioned migrations, including queue columns.
- PostgreSQL search now matches article and CVE text without case sensitivity.
- Helm lint with platform values and a chart render passed. The Hugo application
  build path, content, commands, and concurrency were not modified.

## Open gates

- The full historical database test suite is not a merge gate: tests share state
  and several older cases assume a blank database or obsolete pipeline behavior.
  `tests/test_fetch_pipeline.py` fails even when isolated. This is test debt,
  not evidence that the live article pipeline failed. Do not claim all database
  tests pass.
- Run the new PR verification workflow on the pushed branch. Review its checks,
  compare the 415-commit branch against `main`, and scan for sensitive data
  before a mainline merge. Do not deploy unreviewed chart or application changes
  merely to complete the merge.
- After merge, separately deploy the narrow worker-chart annotation when the
  platform deployment is ready. The manual worker restart already restored the
  current production routing value.

## Reproduce

Run `pytest -q tests/offline`. For a disposable PostgreSQL 18 database, set
`SV_TEST_DB_URL` and run `pytest --run-db-tests -q
tests/test_fresh_postgres_bootstrap.py`; run each database-smoke module against
a fresh database. Run `helm lint deploy/helm/sempervigil -f
../k8s-platform/apps/sempervigil/values.yaml` when the private platform values
repo is available. Never point `SV_TEST_DB_URL` at production.
