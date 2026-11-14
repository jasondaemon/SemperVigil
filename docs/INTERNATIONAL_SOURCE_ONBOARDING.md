# International source onboarding (November 2026)

Official publisher feeds selected for a small forward-looking trial:

| Source ID | Publisher | Feed | Purpose |
| --- | --- | --- | --- |
| `uk_ncsc_news` | UK National Cyber Security Centre | `https://www.ncsc.gov.uk/api/1/services/v1/news-rss-feed.xml` | UK cyber policy, alerts and institutional news |
| `de_bsi_news` | German Federal Office for Information Security (BSI) | `https://www.bsi.bund.de/SiteGlobals/Functions/RSSFeed/RSSNewsfeed/RSSNewsfeed_Presse_Veranstaltungen.xml` | German cyber policy and security developments |
| `ca_opc_news` | Office of the Privacy Commissioner of Canada | `https://www.priv.gc.ca/en/rss/news/` | Canadian privacy enforcement and policy |
| `ca_opc_pipeda` | Office of the Privacy Commissioner of Canada | `https://www.priv.gc.ca/en/rss/pipeda/` | PIPEDA investigations and findings |

The source-specific `discovery.not_before` setting bounds initial ingestion to 2026-11-01 or later. Entries without a reliable publication date are skipped. This avoids backfilling the hundreds of historical items still exposed by the Canadian feeds. Source polling remains normal thereafter; the cutoff is fixed, not a rolling window. Keep source tags empty: publisher location is provenance, not evidence that a regulation applies to that jurisdiction. Article analysis must determine regulatory relevance and applicability from the article itself.

Before enabling, run each disabled source through the normal fetch path in test mode and confirm HTTP success, parsed entries, source URLs and the bounded acceptance count. Monitor content extraction, model backlog, article classification and publication after enabling. Re-disable a source if it repeatedly fails or yields irrelevant items; do not work around blocks by misrepresenting publisher identity.

Deferred: EDPB's advertised feed returned HTTP 503 during verification; the UK ICO says its news and enforcement RSS feeds are unavailable; Australian OAIC and ACSC did not expose a verified RSS endpoint. Revisit these through official endpoints or a separately tested HTML tactic rather than guessing feed URLs.
