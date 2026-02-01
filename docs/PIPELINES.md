# Pipelines

> **Current runtime notes:** see [CURRENT_CONTEXT.md](CURRENT_CONTEXT.md) before troubleshooting or starting a new chat.

## Article Ingest

1) Ingest job pulls enabled sources (RSS/Atom).
2) Articles are normalized, deduped, and stored in Postgres.
3) Fetch content (optional) and store readable text.
4) Summarize (optional).
5) Write Hugo markdown under `/site/content/posts`.
6) Enqueue a build job (builder updates `/site`).

## CVE Sync

1) CVE sync job pulls NVD deltas.
2) CVEs are stored with CVSS v3.1/v4 and signals (products, vendors, CPEs).
3) CVE/product links are created.
4) Events are correlated from CVEs/products (deterministic).
5) Publish events markdown + `events.json` for Hugo/static usage.

---

## Vendor/Product Pages Verification

Scriptable check:

```bash
docker compose up -d --build
docker compose exec builder_scheduler sh -lc 'ls -la /site/data/vendors.json /site/data/products.json /site/data/vendor_map.json /site/data/product_map.json'
curl -I http://localhost:${SV_WEB_PORT:-8080}/vendors/
curl -I http://localhost:${SV_WEB_PORT:-8080}/products/
```
