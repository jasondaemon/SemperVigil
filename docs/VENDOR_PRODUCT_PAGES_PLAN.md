# Vendor + Product Pages Plan (Refined, Historical)

> Superseded by unified entity search at `/entities/`.
> Keep this document as implementation history only.

## Purpose
Deliver vendor/product browsing pages with a **popular-only word cloud** and **full-inventory search** without changing pipeline mechanics.

## Key Refinement
- Word cloud shows **only entities with total_count > 2**.
- Search runs over the **full inventory**, including entities with total_count <= 2.

## Data Outputs (Required)
- `data/vendors.json`
- `data/products.json`
- `data/vendor_map.json`
- `data/product_map.json`

Each entity includes:
- `slug`
- `display_name`
- `article_count`
- `cve_count`
- `total_count`

Maps provide drill-down:
- `vendor_map.json`: vendor_slug → { articles, cves, products }
- `product_map.json`: product_slug → { articles, cves, vendors }

## Pages (Original Plan)
- `/vendors/` (directory, word cloud + search)
- `/products/` (directory, word cloud + search)
- `/vendor/<vendor_slug>/` (single vendor, lists)
- `/product/<product_slug>/` (single product, lists)

Current implementation replaced this with:
- `/entities/` (merged vendors/products/threats search + cloud)

## Behavior
- Empty search: show popular word cloud.
- Search query: show filtered results from **full inventory**.
- Clicking a word navigates to the single page (shareable URL).

## Non‑negotiables
- No changes to build pipeline or docker-compose mounts.
- No unsupported Hugo flags.
- No new ingestion logic beyond publishing the indexes/maps.
