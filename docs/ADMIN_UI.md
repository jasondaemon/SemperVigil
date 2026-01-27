# Admin UI

Admin UI is served at `/ui` from the admin container. If `SV_ADMIN_TOKEN` is set,
login via `/ui/login`.

## Pages

- Dashboard: source counts and recent jobs; enqueue ingest/build.
- Sources: CRUD for DB-backed sources; test source; health history.
- Jobs: job queue list, build job status.
- Content: search across articles and CVEs.
- CVEs: browse/search CVEs; settings under CVE Settings.
- Products: browse normalized vendor/product entries and related CVEs/events.
- Events: deterministic event clusters from CVEs/products, with optional Web Enrichment.
  - Event detail includes a Web Enrichment panel to search SearXNG and promote sources to articles.
- AI Config: providers/models/prompts/profiles + test.
- Runtime Config: DB-backed runtime settings.
- Danger Zone: destructive dev-only clears (articles/CVEs/all).
