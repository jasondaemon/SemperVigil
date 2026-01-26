# TARGET-STATE.md

## Purpose
SemperVigil is a public, technically oriented security news and vulnerability intelligence site. It prioritizes **impact and actions first**, then **evidence and technical depth**, with three primary experiences:

- **Daily News**: curated daily summaries (midnight) + a live front page stream updated hourly
- **Security Events**: evolving dossiers for impactful, multi-source incidents and campaigns
- **Search News**: investigation console across Events, Daily Summaries, Articles, CVEs, Products

The system ingests Articles + CVEs into a DB, correlates and enriches them, then publishes a **Hugo static site** using generated Markdown + JSON indexes.

---

## Information Architecture

### Top-level navigation
- **Daily News**
- **Security Events**
- **Search News**

### Public URL structure (canonical)
- `/` — Live article feed (today + backfill to 20+, day-by-day paging)
- `/daily/` — Daily News index (newest day first)
- `/daily/YYYY-MM-DD/` — Daily News page
- `/events/` — Security Events index (sorted by last_modified desc)
- `/events/<event-id>/` — Event dossier page (updated over time)
- `/cves/` — CVE index (optional; Search-first is fine)
- `/cves/CVE-YYYY-NNNN/` — CVE detail page
- `/products/` — Product index (optional)
- `/products/<vendor>/<product>/` — Product page
- `/search/` — Search UI (client-side for MVP, service-backed later)

---

## Public UX Requirements

### Front page `/` (live feed)
**Goal:** “What’s new right now?” with fast scanning.

Rules:
- Shows **all articles published today** (viewer’s date).
- If fewer than 20 total, automatically backfills previous days until at least 20.
- Sorted newest → oldest within the current view.
- UI shows compact rows:
  - Source badge
  - Clickable title
  - 1–2 sentence summary
  - Human-readable publish time (“3 hours ago”, “Yesterday 5:14 PM”)
- Top-of-page always includes:
  - Link to **yesterday’s Daily News summary**
  - Link(s) to **most recently updated Events**
- Bottom-of-page paging:
  - “Previous day / Next day” navigation (day slices), plus quick jump to dates.

Progressive disclosure:
- Row click expands inline to show:
  - “Why it matters” (short)
  - key tags
  - related CVEs/products if known
  - source links

Update cadence:
- Content updated hourly (or more often), but the front page should remain fast and stable.

### Daily News `/daily/YYYY-MM-DD/`
Generated around midnight (timezone defined in runtime config).

Required structure:
1) **TL;DR** (5–8 bullets max)
2) **Daily Summary** (executive-style but technical, 1–3 paragraphs)
3) **Top Actions** (patch/mitigate/monitor bullets with links)
4) **Stories** (grouped multi-source stories or single-source items)
   - Each story includes:
     - Summary (2–4 sentences)
     - Why it matters (1 paragraph)
     - What to do (bullets)
     - Sources (links)
5) **Daily Index** (tight list)
   - Articles list: source | title(link) | time
   - CVEs list: CVE | severity | affected products (if known)

Required citations:
- Every story cites its sources.
- CVE references point to authoritative sources when possible.

### Security Events `/events/<event-id>/`
Events are evolving dossiers (breach, campaign, crackdown, major exploitation waves, etc).

Required structure:
1) **Impact** (2–4 sentences, plain technical English)
2) **Status** (Open/Contained/Ongoing/Unknown) + last updated timestamp
3) **What to do** (Immediate / Mitigations / Detection)
4) **What we know / don’t know** (confirmed vs open questions)
5) **Timeline** (dated updates; newest on top)
6) **Technical details** (attack chain, IOCs when available; collapsible sections ok)
7) **Related objects**
   - Related CVEs (with version-labeled CVSS, severity)
   - Affected products/vendors
   - Source articles (chronological)

Sorting:
- `/events/` index sorted by `last_modified` desc.

Event inclusion rule:
- Events are for impactful multi-source incidents/campaigns.
- A lone “CVE announced” article does not create an Event by default.
- A CVE wave can become an Event if exploitation/impact signals exist.

---

## Search News `/search/`
**Goal:** investigation console.

Result ordering:
1) Events
2) Daily Summaries
3) Articles
4) CVEs
5) Products (if implemented as first-class)

MVP (client-side search):
- Uses JSON indexes produced at publish time.
- Supports:
  - keyword search
  - filters: content type, date range, severity, vendor/product, tags
  - “exploit signals” filter (future but planned)

Phase 2 (service-backed):
- Add Typesense/Meilisearch (or DB-backed read API) for scalable facets.
- Keep Hugo pages canonical; search service only returns IDs/URLs.

---

## Data Model Targets (DB)

### Articles
Must store:
- id
- source_id
- title
- published_at
- urls (original, normalized)
- tag set (article_tags)
- extracted_text (file path or text)
- summary fields (LLM summary, error, timestamps)
- extracted entities (optional later): orgs, products, CVE IDs, threat actor names

### CVEs
Must store:
- cve_id
- published_at / modified_at
- **description**
- references (domains list)
- affected CPEs list
- affected products list (vendor/product + versions if available)
- CVSS:
  - cvss_v31_json (if present)
  - cvss_v40_json (if present)
  - preferred_cvss_version (v4 if present else v3.1)
  - preferred_score, preferred_severity, preferred_vector

### Vendors/Products (Phase 1+)
- vendors (normalized + display)
- products (vendor_id, normalized + display, product_key)
- cve_products join (cve_id ↔ product_id)
- (later) version ranges

### Events (Stage 2+)
- events (kind, title, summary, severity, status, first_seen_at, last_seen_at, updated_at)
- event_items:
  - CVE links
  - article links
  - product links
- Rebuild capability for tuning correlation rules.

---

## Correlation Logic Targets

### CVE ↔ Product
- Extract from NVD configurations (list/dict shape tolerant).
- Persist vendors/products and join links.

### Article ↔ CVE
- Extract CVE IDs from article content (regex + heuristics).
- Link articles to events that contain those CVEs.

### Event formation
MVP deterministic clustering:
- CVE-centered:
  - merge CVEs into same event if they share product_key and within merge window
- Promotion rules (for “impactful events”):
  - multi-source coverage
  - exploitation signals
  - repeated updates over multiple days
  - breach/outage/law enforcement keywords
- Keep low-value clusters as “low importance” and exclude from `/events/` by default.

---

## Publishing Pipeline Targets (Hugo)

### Generated content (Markdown)
- `/site/content/posts/*.md` — articles (optional to publish all; can publish only “selected” later)
- `/site/content/daily/YYYY-MM-DD.md` — daily summaries
- `/site/content/events/<event-id>.md` — events
- `/site/content/cves/<cve-id>.md` — CVEs (optional, but recommended)
- `/site/content/products/<vendor>-<product>.md` — products (phase 2/3)

### JSON indexes (for Search + front page data)
Under a single namespace directory:
- `/site/static/sempervigil/index/articles.json`
- `/site/static/sempervigil/index/daily.json`
- `/site/static/sempervigil/index/events.json`
- `/site/static/sempervigil/index/cves.json`
- `/site/static/sempervigil/index/products.json`

Index payload design:
- small, fast, searchable
- includes canonical URL for each object
- includes essential fields (title, date, severity, product_keys, tags)
- no huge blobs (truncate text excerpts)

### Build orchestration
- Build is a **job** (`build_site`) with stored:
  - exit_code
  - stdout_tail / stderr_tail
  - duration
- Builder can be ephemeral if desired, but jobs MUST be executed and observable.

---

## Admin/Operator UX Targets

### Admin sections
- Content Browser (Articles, CVEs)
- Events (browse, drilldown, rebuild)
- Products (browse, drilldown)
- Sources / Health / Jobs
- AI Config (providers/models/prompts/schemas)
- Runtime Config (form-driven)
- Danger Zone (clear DB content for dev testing)

### Required admin observability
- Job logs and statuses visible in UI (queue → running → succeeded/failed)
- Build site job shows hugo output tail
- LLM test UI:
  - test provider/model with a prompt
  - show latency and output excerpt
  - show last 10 LLM job results/errors

### Documentation (/docs, GitHub Pages)
- `/docs/index.md` links to:
  - architecture overview
  - admin UI guide
  - pipelines guide
  - “how to run locally”
  - “how to publish”

---

## Definition of Done (Target State)

### Public
- Front page behaves per rules (today + backfill + day paging)
- Daily summaries exist for each day with consistent structure and citations
- Events index contains only meaningful, promoted events (or filterable)
- Search works (MVP client-side) across Events/Daily/Articles/CVEs with facets

### Backend
- CVEs have descriptions, products, CPEs, reference domains, CVSS labeled versions
- Products and vendor normalization works and is searchable
- Events correlate across products/CVEs/articles and can rebuild deterministically
- Site builds on schedule and on-demand with full visibility

### Ops
- Admin UI fully functional with pagination, tag list, and testing tools
- Docs exist in `/docs` and are publishable via GitHub Pages

---

## Roadmap (from current state)

### Phase A: Stabilize pipelines (1–2 iterations)
- Fix CVE parsing + persistence (descriptions/signals)
- Ensure build jobs are actually executed (compose builder runs job loop)
- Ensure analytics uses correct schema and never 500s
- Confirm LLM config + test endpoint + recordkeeping

### Phase B: Publish objects (Daily + Events + CVEs)
- Produce daily summary markdown and publish
- Produce event markdown and publish (already close)
- Produce CVE markdown and publish
- Produce JSON indexes: articles/events/cves/daily/products

### Phase C: Public UX (front page + search)
- Hugo templates consume indexes and render:
  - front page feed rules
  - events index
  - daily index
- Implement Search MVP (Fuse/Lunr) using indexes

### Phase D: Enrichment (products/versions, exploit signals)
- Parse version ranges where possible
- Add exploit-signal classifier
- Add product pages and “all high/critical for product” views

### Development stages.

