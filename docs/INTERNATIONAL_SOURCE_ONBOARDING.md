# International source onboarding (November 2026)

Official publisher feeds selected for a small forward-looking trial:

| Source ID | Publisher | Feed | Purpose |
| --- | --- | --- | --- |
| `uk_ncsc_news` | UK National Cyber Security Centre | `https://www.ncsc.gov.uk/api/1/services/v1/news-rss-feed.xml` | UK cyber policy, alerts and institutional news |
| `de_bsi_news` | German Federal Office for Information Security (BSI) | `https://www.bsi.bund.de/SiteGlobals/Functions/RSSFeed/RSSNewsfeed/RSSNewsfeed_Presse_Veranstaltungen.xml` | German cyber policy and security developments |
| `ca_opc_news` | Office of the Privacy Commissioner of Canada | `https://www.priv.gc.ca/en/rss/news/` | Canadian privacy enforcement and policy |
| `ca_opc_pipeda` | Office of the Privacy Commissioner of Canada | `https://www.priv.gc.ca/en/rss/pipeda/` | PIPEDA investigations and findings |
| `au_itnews_security` | iTnews Security (Australia) | `https://www.itnews.com.au/RSS/rss.ashx?ID=32&type=Category` | Australian security reporting |
| `au_cyberdaily_security` | Cyber Daily Security (Australia) | `https://www.cyberdaily.au/security?format=feed&type=rss` | Australian cyber incidents and policy reporting |

The source-specific `discovery.not_before` setting bounds initial ingestion to 2026-11-01 or later. Entries without a reliable publication date are skipped. This avoids backfilling the hundreds of historical items still exposed by the Canadian feeds. Source polling remains normal thereafter; the cutoff is fixed, not a rolling window. Keep source tags empty: publisher location is provenance, not evidence that a regulation applies to that jurisdiction. Article analysis must determine regulatory relevance and applicability from the article itself.

Before enabling, run each disabled source through the normal fetch path in test mode and confirm HTTP success, parsed entries, source URLs and the bounded acceptance count. Monitor content extraction, model backlog, article classification and publication after enabling. Re-disable a source if it repeatedly fails or yields irrelevant items; do not work around blocks by misrepresenting publisher identity.

Deferred: EDPB's advertised feed returned HTTP 503 during verification; the UK ICO says its news and enforcement RSS feeds are unavailable. Australian government feeds are addressed below. Revisit deferred sources through working official endpoints or a separately tested HTML tactic rather than guessing feed URLs.

November 14 Australian trial: both publisher-declared Security feeds were reachable through the production VPN and yielded extractable article text. `au_itnews_security` uses a URL allowlist for `/news/` to exclude galleries; both feeds use `discovery.not_before: 2026-11-12` and poll every 180 minutes. Test mode accepted two iTnews and five Cyber Daily entries before enablement. Neither source is pre-tagged as regulatory or as applicable to Australia. The official ACSC RSS URLs and Protective Security Policy Framework RSS were discovered, but from both the production fetch path and a non-VPN path they failed after TLS negotiation or timed out without an HTTP response; OAIC's media page returned a browser challenge. They remain deferred rather than adding failing sources.
