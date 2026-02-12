# Change Control Log

All structural changes (schema/query/pipeline/job types/routing/build) must be recorded here.

## Entry Template
- Date:
- Summary:
- Files touched:
- DB impact (tables/columns):
- Migration notes:
- Manual operator steps:

---

- Date: 2026-02-12
- Summary: Hardened RSS fetch/probe with HTTP/2 curl + Range support and per‑source overrides; fixed tactic execution ordering and self‑heal for missing tactics; expanded article exports with summary bullets and updated site templates to render them; moved logs to `/log` runtime path.
- Files touched:
  - src/sempervigil/http_fetch.py
  - src/sempervigil/admin.py
  - src/sempervigil/ingest.py
  - src/sempervigil/source_overrides.py
  - src/sempervigil/services/sources_service.py
  - src/sempervigil/storage.py
  - src/sempervigil/worker.py
  - site-src/layouts/partials/home/test-front.html
  - site-src/layouts/products/list.html
  - site-src/layouts/vendors/list.html
  - site-src/layouts/threats/list.html
  - site-src/layouts/product/single.html
  - site-src/layouts/vendor/single.html
  - site-src/layouts/threat/single.html
  - site-src/assets/css/custom.css
  - tests/test_http_fetch.py
  - tests/test_ingest_self_heal.py
- DB impact (tables/columns):
  - None (runtime behavior only; no migrations).
- Migration notes:
  - None.
- Manual operator steps:
  - Ensure `/log` is writable by containers.
  - Rebuild data files and Hugo output after deploy.

---

- Date: 2026-02-01
- Summary: Enforced normalized vendor/product + threat actor storage; added strict LLM prompt/schema definitions and Threats admin UI.
- Files touched:
  - src/sempervigil/migrations_pg.py
  - src/sempervigil/storage.py
  - src/sempervigil/worker.py
  - src/sempervigil/llm/router.py
  - src/sempervigil/admin.py
  - src/sempervigil/admin_ui.py
  - src/sempervigil/static/admin/admin.js
  - src/sempervigil/templates/admin/threats.html
  - src/sempervigil/templates/admin/threat_detail.html
  - src/sempervigil/templates/admin/_nav.html
  - tests/test_vendor_product_query_columns.py
- DB impact (tables/columns):
  - Ensures threat actor tables exist and are used.
  - Removes vendor/product tags from article_tags.
  - Adds/updates LLM prompt/schema records for product and threat extraction.
- Migration notes:
  - Apply migrations to update prompts/schemas and cleanup vendor/product tags.
- Manual operator steps:
  - Verify Threats page in Admin.

---

- Date: 2026-02-01
- Summary: Added Threats admin list/detail + backfill actions; hardened enrich parsing/counters; enforced normalized vendor/product writes.
- Files touched:
  - src/sempervigil/worker.py
  - src/sempervigil/storage.py
  - src/sempervigil/admin.py
  - src/sempervigil/admin_ui.py
  - src/sempervigil/static/admin/admin.js
  - src/sempervigil/templates/admin/threats.html
  - src/sempervigil/templates/admin/threat_detail.html
  - src/sempervigil/templates/admin/_nav.html
  - tests/test_no_vendor_product_tags.py
  - tests/test_enrich_parsing.py
  - tests/test_vendor_product_persistence.py
- DB impact (tables/columns):
  - No new tables; uses existing threat/vendor/product tables.
  - Adds list/detail queries for threat actors and missing‑link detection.
- Migration notes:
  - None.
- Manual operator steps:
  - Use Admin → Content → Threats to run backfill buttons.

---

- Date: 2026-02-01
- Summary: Relinked vendor/product pills and word-cloud pages to normalized vendor/product JSON; added vendor/product page templates and JS UI; extended emitted JSON fields for stability.
- Files touched:
  - src/sempervigil/worker.py
  - site/layouts/vendors/list.html
  - site/layouts/products/list.html
  - site/layouts/vendor/single.html
  - site/layouts/product/single.html
  - site/assets/js/vendor_product.js
  - site/assets/css/custom.css
- DB impact (tables/columns):
  - None (uses normalized vendor/product tables only).
- Migration notes:
  - None.
- Manual operator steps:
  - Run rebuild vendor/product indexes and rebuild the site.

---

- Date: 2026-02-01
- Summary: Prevented AI profile updates from crashing when prompt/schema IDs are missing by validating IDs and returning a clear 400 error.
- Files touched:
  - src/sempervigil/services/ai_service.py
- DB impact (tables/columns):
  - None.
- Migration notes:
  - None.
- Manual operator steps:
  - If you see prompt_not_found/schema_not_found, run migrations or select an existing prompt/schema.

---

- Date: 2026-02-01
- Summary: Fixed AI profile editor to restore schema/prompt/provider/model/params/fallback when editing, preventing schema from appearing “lost.”
- Files touched:
  - src/sempervigil/templates/admin/ai.html
  - src/sempervigil/static/admin/admin.js
- DB impact (tables/columns):
  - None.
- Migration notes:
  - None.
- Manual operator steps:
  - Reload Admin → AI Config after update.

---

- Date: 2026-02-01
- Summary: Added dashboard threat identification metrics with queue + limit controls for article/CVE threat actor backfill.
- Files touched:
  - src/sempervigil/storage.py
  - src/sempervigil/admin.py
  - src/sempervigil/static/admin/admin.js
- DB impact (tables/columns):
  - None.
- Migration notes:
  - None.
- Manual operator steps:
  - Use Dashboard → Queue missing to enqueue threat backfill.

---

- Date: 2026-02-01
- Summary: Restored CVE card descriptions by falling back to summary text when description_text isn’t present in recent CVE queries.
- Files touched:
  - src/sempervigil/worker.py
- DB impact (tables/columns):
  - None.
- Migration notes:
  - None.
- Manual operator steps:
  - Rebuild the site to refresh CVE cards.

---

- Date: 2026-02-01
- Summary: Compacted dashboard metric cards and queue controls; queue buttons now inline and labeled “Queue”.
- Files touched:
  - src/sempervigil/static/admin/admin.js
  - src/sempervigil/static/admin/admin.css
- DB impact (tables/columns):
  - None.
- Migration notes:
  - None.
- Manual operator steps:
  - Reload Admin → Dashboard to see updated sizing.

---

- Date: 2026-02-01
- Summary: Enforced normalized vendor/product/threat rules (no “unknown” rows), aligned pill JSON shapes with templates, added unknown pill rendering, and introduced LLM-backed event classification for derive_events_from_articles.
- Files touched:
  - src/sempervigil/worker.py
  - src/sempervigil/storage.py
  - src/sempervigil/llm/router.py
  - src/sempervigil/migrations_pg.py
  - site/layouts/partials/home/custom.html
  - site/assets/js/vendor_product.js
  - site/assets/css/custom.css
  - tests/test_vendor_product_persistence.py
  - tests/test_vendor_product_home_data.py
- DB impact (tables/columns):
  - No new tables (events/events links already exist).
  - Prevents “unknown” vendor/product/threat rows by skipping inserts when missing.
  - Adds LLM prompt/schema for event classification and updates product/threat prompts to forbid unknown items.
- Migration notes:
  - Run migrations to apply prompt/schema updates (`pg_llm_event_classify_012`).
- Manual operator steps:
  - Rebuild the site to refresh homepage pills and CVE cards.
  - Configure AI routing for stage `derive_events_from_articles` if you want LLM event classification.

---

- Date: 2026-02-01
- Summary: Required schemas for enrichment/event stages and documented no-sentinel rules for normalized entities.
- Files touched:
  - src/sempervigil/services/ai_service.py
  - docs/ARCHITECTURE.md
- DB impact (tables/columns):
  - None.
- Migration notes:
  - None.
- Manual operator steps:
  - Ensure AI profiles for product/threat/event stages have schemas attached.

---

- Date: 2026-02-01
- Summary: Prevented suppressed events from being auto-reactivated during event upserts.
- Files touched:
  - src/sempervigil/storage.py
- DB impact (tables/columns):
  - None.
- Migration notes:
  - None.
- Manual operator steps:
  - Re-run purge if you want suppressed events to disappear from the list.

---

- Date: 2026-02-01
- Summary: Routed new LLM enrichment jobs to the LLM worker in docker-compose.
- Files touched:
  - docker-compose.yml
- DB impact (tables/columns):
  - None.
- Migration notes:
  - None.
- Manual operator steps:
  - Restart the llm worker container to pick up SV_WORKER_ONLY_TYPES changes.

---

- Date: 2026-02-01
- Summary: Restored full prompt list visibility in Admin AI Config (removed prompt filter).
- Files touched:
  - src/sempervigil/admin_ui.py
- DB impact (tables/columns):
  - None.
- Migration notes:
  - None.
- Manual operator steps:
  - Refresh Admin → AI Config to see all prompts.

---

- Date: 2026-02-01
- Summary: Added per-article “Publish markdown” action on the Admin article detail page.
- Files touched:
  - src/sempervigil/static/admin/admin.js
- DB impact (tables/columns):
  - None.
- Migration notes:
  - None.
- Manual operator steps:
  - Reload an article detail page to see the new action button.

---

- Date: 2026-02-01
- Summary: Expanded dashboard job queue table to show all job types, queued/failed/completed counts, and a cutoff timestamp for counts since reset.
- Files touched:
  - src/sempervigil/templates/admin/dashboard.html
  - src/sempervigil/static/admin/admin.js
  - src/sempervigil/admin.py
  - src/sempervigil/storage.py
- DB impact (tables/columns):
  - None.
- Migration notes:
  - None.
- Manual operator steps:
  - Reload Admin → Dashboard to see the updated job queue table.

---

- Date: 2026-02-01
- Summary: Log tail filters now include all job types and display events/jobs in two columns.
- Files touched:
  - src/sempervigil/static/admin/admin.js
  - src/sempervigil/static/admin/admin.css
- DB impact (tables/columns):
  - None.
- Migration notes:
  - None.
- Manual operator steps:
  - Reload Admin → Dashboard to see the updated filters.

---

- Date: 2026-02-01
- Summary: Made job queue table compact, split into two columns, and renamed Reset button to “Reset Counters”.
- Files touched:
  - src/sempervigil/templates/admin/dashboard.html
  - src/sempervigil/static/admin/admin.js
  - src/sempervigil/static/admin/admin.css
- DB impact (tables/columns):
  - None.
- Migration notes:
  - None.
- Manual operator steps:
  - Reload Admin → Dashboard to see the compact two-column job table.

---

- Date: 2026-02-01
- Summary: Normalized vendor/product page summaries to render JSON summaries cleanly and added URL slug fallback for single pages.
- Files touched:
  - site/assets/js/vendor_product.js
- DB impact (tables/columns):
  - None.
- Migration notes:
  - None.
- Manual operator steps:
  - Rebuild the site to pick up updated vendor/product page rendering.

---

- Date: 2026-02-01
- Summary: Fixed daily brief enqueue guard to use get_setting default argument to prevent worker crash.
- Files touched:
  - src/sempervigil/worker.py
- DB impact (tables/columns):
  - None.
- Migration notes:
  - None.
- Manual operator steps:
  - Restart the LLM worker (or any worker running the scheduler loop) to clear the crash.

---

- Date: 2026-02-01
- Summary: Made LLM JSON parsing more robust for vendor/product and threat extraction (extracts JSON from mixed output).
- Files touched:
  - src/sempervigil/worker.py
- DB impact (tables/columns):
  - None.
- Migration notes:
  - None.
- Manual operator steps:
  - Restart the LLM worker after updating.

---

- Date: 2026-02-01
- Summary: Improved LLM JSON parsing to extract embedded JSON from mixed output.
- Files touched:
  - src/sempervigil/llm/router.py
- DB impact (tables/columns):
  - None.
- Migration notes:
  - None.
- Manual operator steps:
  - Restart the LLM worker to pick up JSON parsing improvements.

---

- Date: 2026-02-01
- Summary: Downgraded vendor/product “no items” LLM outcomes to info logs instead of warnings.
- Files touched:
  - src/sempervigil/worker.py
- DB impact (tables/columns):
  - None.
- Migration notes:
  - None.
- Manual operator steps:
  - Restart the LLM worker to apply logging change.

---

- Date: 2026-02-01
- Summary: Added product link count column to Admin Products list.
- Files touched:
  - src/sempervigil/storage.py
  - src/sempervigil/templates/admin/products.html
  - src/sempervigil/static/admin/admin.js
- DB impact (tables/columns):
  - None.
- Migration notes:
  - None.
- Manual operator steps:
  - Reload Admin → Products to see the new Links column.

---

- Date: 2026-02-01
- Summary: Fixed Products list to read products.json directly (avoids data/products map collision).
- Files touched:
  - site-src/layouts/products/list.html
- DB impact (tables/columns):
  - None.
- Migration notes:
  - None.
- Manual operator steps:
  - Rebuild the site to update the Products page.

---

- Date: 2026-02-01
- Summary: Lowered vendor word-cloud threshold to total_count > 1.
- Files touched:
  - site/layouts/vendors/list.html
- DB impact (tables/columns):
  - None.
- Migration notes:
  - None.
- Manual operator steps:
  - Rebuild the site to update the Vendors page.

---

- Date: 2026-02-01
- Summary: Added slug fallback for both /product(s)/ and /vendor(s)/ paths on single pages.
- Files touched:
  - site/assets/js/vendor_product.js
- DB impact (tables/columns):
  - None.
- Migration notes:
  - None.
- Manual operator steps:
  - Rebuild the site to update vendor/product pages.

---

- Date: 2026-02-01
- Summary: Fixed products list template to avoid unsupported kindIs and ensure products.json loads.
- Files touched:
  - site/layouts/products/list.html
- DB impact (tables/columns):
  - None.
- Migration notes:
  - None.
- Manual operator steps:
  - Rebuild the site to update the Products page.

---

- Date: 2026-02-01
- Summary: Replaced removed getJSON with resources.Get + transform.Unmarshal for products list data.
- Files touched:
  - site/layouts/products/list.html
- DB impact (tables/columns):
  - None.
- Migration notes:
  - None.
- Manual operator steps:
  - Rebuild the site to clear the Hugo error.

---

- Date: 2026-02-01
- Summary: Added per-source discovery/content overrides (JSONB) with admin UI + test endpoint and extraction modes.
- Files touched:
  - src/sempervigil/migrations_pg.py
  - src/sempervigil/models.py
  - src/sempervigil/storage.py
  - src/sempervigil/services/sources_service.py
  - src/sempervigil/source_overrides.py
  - src/sempervigil/ingest.py
  - src/sempervigil/pipelines/content_fetch.py
  - src/sempervigil/worker.py
  - src/sempervigil/admin.py
  - src/sempervigil/templates/admin/sources.html
  - src/sempervigil/static/admin/admin.js
  - src/sempervigil/static/admin/admin.css
  - docs/ARCHITECTURE.md
  - tests/test_source_overrides.py
- DB impact (tables/columns):
  - sources.overrides (JSONB)
- Migration notes:
  - pg_source_overrides_013 adds sources.overrides JSONB.
- Manual operator steps:
  - Re-open the Sources page and update overrides as needed; rebuild not required.

---

- Date: 2026-02-01
- Summary: Added readability-lxml and trafilatura to base dependencies for content overrides.
- Files touched:
  - pyproject.toml
- DB impact (tables/columns):
  - None.
- Migration notes:
  - None.
- Manual operator steps:
  - Rebuild images to install new Python deps.

---

- Date: 2026-02-01
- Summary: Made pg_source_overrides_013 migration insert idempotent to avoid duplicate-key errors under concurrent startup.
- Files touched:
  - src/sempervigil/migrations_pg.py
- DB impact (tables/columns):
  - None.
- Migration notes:
  - Uses ON CONFLICT DO NOTHING when recording pg_source_overrides_013.
- Manual operator steps:
  - Restart workers to retry migrations.

---

- Date: 2026-02-01
- Summary: Routed source_acquire and threat actor backfill jobs to appropriate workers.
- Files touched:
  - docker-compose.yml
- DB impact (tables/columns):
  - None.
- Migration notes:
  - None.
- Manual operator steps:
  - Restart workers to pick up new SV_WORKER_ONLY_TYPES.

---

- Date: 2026-02-01
- Summary: Removed legacy site/ directory and ignored site-public output.
- Files touched:
  - .gitignore
  - site/ (removed)
- DB impact (tables/columns):
  - None.
- Migration notes:
  - None.
- Manual operator steps:
  - None.

---

- Date: 2026-02-01
- Summary: Added threat actor Hugo pages and published threat index data for the site menu.
- Files touched:
  - src/sempervigil/worker.py
  - site-src/layouts/threats/list.html
  - site-src/layouts/threat/single.html
  - site-src/static/js/vendor_product.js
  - site-src/config/_default/menus.en.toml
- DB impact (tables/columns):
  - None.
- Migration notes:
  - None.
- Manual operator steps:
  - Rebuild the site to render /threats and /threat/<slug> pages.

---

- Date: 2026-02-01
- Summary: Read data/products.json and data/threats.json via readFile + transform.Unmarshal to avoid data dir shadowing.
- Files touched:
  - site-src/layouts/products/list.html
  - site-src/layouts/threats/list.html
- DB impact (tables/columns):
  - None.
- Migration notes:
  - None.
- Manual operator steps:
  - Rebuild the site to apply list page fixes.

---

- Date: 2026-02-01
- Summary: Fixed threat index writer string escaping syntax error.
- Files touched:
  - src/sempervigil/worker.py
- DB impact (tables/columns):
  - None.
- Migration notes:
  - None.
- Manual operator steps:
  - Restart admin/worker services.

---

- Date: 2026-02-01
- Summary: Added Events section index to make /events/ menu link resolve.
- Files touched:
  - site-src/content/events/_index.md
- DB impact (tables/columns):
  - None.
- Migration notes:
  - None.
- Manual operator steps:
  - Rebuild the site to render /events/.

---

- Date: 2026-02-01
- Summary: Lowered Threats word-cloud threshold to include items with >1 links so the page is populated.
- Files touched:
  - site-src/layouts/threats/list.html
- DB impact (tables/columns):
  - None.
- Migration notes:
  - None.
- Manual operator steps:
  - Rebuild the site to refresh /threats/.

---

- Date: 2026-02-01
- Summary: Include all threats (total_count >= 1) in the /threats/ word cloud.
- Files touched:
  - site-src/layouts/threats/list.html
- DB impact (tables/columns):
  - None.
- Migration notes:
  - None.
- Manual operator steps:
  - Rebuild the site to refresh /threats/.

---

- Date: 2026-02-02
- Summary: Write article JSON + enqueue build after LLM summary so summaries appear without new ingest.
- Files touched:
  - src/sempervigil/worker.py
- DB impact (tables/columns):
  - None.
- Migration notes:
  - None.
- Manual operator steps:
  - Restart workers to pick up updated summarization behavior.

---

- Date: 2026-02-02
- Summary: Only rewrite article JSON + enqueue build after summary when the article is in today's feed.
- Files touched:
  - src/sempervigil/worker.py
- DB impact (tables/columns):
  - None.
- Migration notes:
  - None.
- Manual operator steps:
  - Restart workers to pick up updated summarization behavior.

---

- Date: 2026-02-02
- Summary: Sort homepage recent CVEs by published_at (fallback last_modified_at) to match publication date ordering.
- Files touched:
  - src/sempervigil/worker.py
- DB impact (tables/columns):
  - None.
- Migration notes:
  - None.
- Manual operator steps:
  - Rebuild the site to refresh CVE ordering on the homepage.

---

- Date: 2026-02-02
- Summary: Fixed Sources UI modal open/close handling so Add Source works; overrides remain accessible in Edit panel.
- Files touched:
  - src/sempervigil/static/admin/admin.js
- DB impact (tables/columns):
  - None.
- Migration notes:
  - None.
- Manual operator steps:
  - Hard refresh the Sources page to load updated JS.

---

- Date: 2026-02-02
- Summary: Rebuild article JSON + enqueue build when an article is suppressed or deleted so it is removed from the site.
- Files touched:
  - src/sempervigil/admin.py
- DB impact (tables/columns):
  - None.
- Migration notes:
  - None.
- Manual operator steps:
  - Restart admin service to load updated handlers.

---

- Date: 2026-02-02
- Summary: Enhanced Source Test to honor overrides and report discovery/extraction details with RSS/HTML warnings.
- Files touched:
  - src/sempervigil/admin.py
  - src/sempervigil/static/admin/admin.js
- DB impact (tables/columns):
  - None.
- Migration notes:
  - None.
- Manual operator steps:
  - Hard refresh Sources page to load updated JS.

---

- Date: 2026-02-02
- Summary: Preserve article titles on content fetch and backfill missing titles from HTML metadata (JSON-LD/og:title/title).
- Files touched:
  - src/sempervigil/worker.py
- DB impact (tables/columns):
  - None.
- Migration notes:
  - None.
- Manual operator steps:
  - Re-run fetch_article_content for affected articles to backfill titles.

---

- Date: 2026-02-02
- Summary: Add LLM timeout config and stop infinite retries by failing timeout jobs (optional bounded retries).
- Files touched:
  - src/sempervigil/config.py
  - src/sempervigil/llm/router.py
  - src/sempervigil/worker.py
  - .env.example
  - docs/README.md
- DB impact (tables/columns):
  - None.
- Migration notes:
  - None.
- Manual operator steps:
  - Restart worker_llm to load new timeout settings.

---

- Date: 2026-02-02
- Summary: LLM timeout is now provider-configured (admin) rather than env; removed SV_LLM_TIMEOUT_SECONDS usage.
- Files touched:
  - src/sempervigil/llm/router.py
  - src/sempervigil/config.py
  - .env.example
  - docs/README.md
- DB impact (tables/columns):
  - None.
- Migration notes:
  - None.
- Manual operator steps:
  - Set provider timeout_s in Admin > AI Providers.

---

- Date: 2026-02-02
- Summary: Show provider timeout/retries in AI Providers table and populate edit form fields correctly.
- Files touched:
  - src/sempervigil/templates/admin/ai.html
  - src/sempervigil/static/admin/admin.js
- DB impact (tables/columns):
  - None.
- Migration notes:
  - None.
- Manual operator steps:
  - Hard refresh Admin AI page to load updated JS.

---

- Date: 2026-02-02
- Summary: Split log tail service selector into worker_fetch/worker_llm and scope event/job filters to the selected service.
- Files touched:
  - src/sempervigil/admin.py
  - src/sempervigil/templates/admin/dashboard.html
  - src/sempervigil/static/admin/admin.js
- DB impact (tables/columns):
  - None.
- Migration notes:
  - None.
- Manual operator steps:
  - Hard refresh the Dashboard to load updated JS.

---

- Date: 2026-02-02
- Summary: Prevent LLM jobs from being re-claimed mid-run by extending job lock timeout based on provider timeout.
- Files touched:
  - src/sempervigil/worker.py
- DB impact (tables/columns):
  - None.
- Migration notes:
  - None.
- Manual operator steps:
  - Restart worker_llm to apply updated lock timeout logic.

---

- Date: 2026-02-02
- Summary: Count job completions/failures since reset using finished_at so dashboard reflects active processing.
- Files touched:
  - src/sempervigil/storage.py
- DB impact (tables/columns):
  - None.
- Migration notes:
  - None.
- Manual operator steps:
  - Refresh the Dashboard metrics panel.

---

- Date: 2026-02-02
- Summary: Implement topic-first Daily Brief pipeline stages, DB persistence, and Hugo JSON output; remove unused pipeline stages.
- Files touched:
  - src/sempervigil/worker.py
  - src/sempervigil/pipelines/daily_brief.py
  - src/sempervigil/llm/router.py
  - src/sempervigil/llm/__init__.py
  - src/sempervigil/services/ai_service.py
  - src/sempervigil/storage.py
  - src/sempervigil/migrations_pg.py
  - src/sempervigil/admin.py
  - site-src/layouts/daily/single.html
  - docs/ARCHITECTURE.md
- DB impact (tables/columns):
  - Add table: daily_briefs
  - Add schemas for Daily Brief stages (llm_schemas upserts)
- Migration notes:
  - New migration: pg_daily_briefs_014
- Manual operator steps:
  - Run migrations by restarting worker/admin.
  - Configure the four Daily Brief stages in Admin > AI Config.
