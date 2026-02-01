# Change Control Log (Build/Serve Pipeline Stability)

All entries must follow the template and document any pipeline-affecting changes.

## Entry Template
- Date/Time:
- Author:
- Summary:
- Motivation / Problem:
- Files changed:
- Risk / Impact:
- Verification steps run:
- Outcome:
- Rollback plan:

---

- Date/Time: 2026-01-31 21:40
- Author: Codex
- Summary: Added threat actor persistence + enrichment, corrected vendor/product indexing to avoid tag usage, and hardened CVE extraction prompt.
- Motivation / Problem: Vendor/product enrichment was being stored as tags and CVE/detail UI could not show products; add first-class threat actor tracking and fix extraction prompts without touching the build pipeline.
- Files changed:
  - src/sempervigil/migrations_pg.py
  - src/sempervigil/storage.py
  - src/sempervigil/worker.py
  - src/sempervigil/llm/router.py
  - src/sempervigil/admin.py
  - src/sempervigil/static/admin/admin.js
  - site/layouts/partials/home/custom.html
  - tests/test_vendor_product_query_columns.py
  - tests/test_threat_actor_persistence.py
- Risk / Impact: Medium (new tables/jobs and UI wiring; no build/compose changes).
- Verification steps run: Not run in this environment (requires SV_DB_URL and live services).
- Outcome: Pending.
- Rollback plan: Revert migrations and code changes; drop threat actor tables (threat_actors, threat_actor_aliases, article_threat_actors, cve_threat_actors); revert prompt update by restoring previous llm_prompts values; disable new job types by removing stage routing in pipeline_stage_config.

---

- Date/Time: 2026-02-01 10:15
- Author: Codex
- Summary: Consolidated vendor/product and threat actor table creation, added strict LLM prompt/schema definitions, and enforced DB-backed vendor/product rendering.
- Motivation / Problem: Prevent vendor/product data from being represented as tags and ensure stage routing uses validated JSON outputs.
- Files changed:
  - src/sempervigil/migrations_pg.py
  - src/sempervigil/worker.py
  - src/sempervigil/llm/router.py
  - src/sempervigil/admin.py
  - src/sempervigil/static/admin/admin.js
  - tests/test_vendor_product_query_columns.py
- Risk / Impact: Medium (LLM prompt/schema updates and enrich behavior changes).
- Verification steps run: Not run in this environment (requires SV_DB_URL).
- Outcome: Pending.
- Rollback plan: Revert migrations and code changes; restore llm_prompts/llm_schemas for enrich stages; remove stage profile prompt/schema updates; re-run with prior prompt IDs.
