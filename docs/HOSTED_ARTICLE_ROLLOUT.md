# Hosted Article Analysis and Regulatory Edition

## Scope

`SV_ARTICLE_INFERENCE_BACKEND=hosted` selects a two-pass production article path
for articles with `brief_day >= SV_REGULATORY_FEED_START_DAY`. Luna creates the
reader narrative, alternate bullets, passage-linked context, primary kind and
an Event seed. Terra independently classifies the primary kind, jurisdiction,
organization and product roles. Both passes receive the complete *retained*
article text through numbered passages; this does not prove the original website
was completely scraped. Refusals, incomplete output, invalid passage IDs, a
changed source version, or a primary-kind disagreement hold the hosted result
and invoke the existing local article path. The local backend remains available
by setting `SV_ARTICLE_INFERENCE_BACKEND=local` and rolling the article worker.

The live summary/context fields keep their existing public shapes. Event
publication and correlation gates are not replaced by a model seed. A positive
evidence-backed Event seed is required before the existing Event-derivation job
is queued; a broad incident label alone is insufficient. Current local
product/actor enrichment and Event derivation remain independent consumers
until their destination contracts are separately validated. This is not a
switch of every inference task in SemperVigil.

## Regulatory data

Only source-version-current, two-pass-agreed regulatory primary stories from
public-feed sources enter `/regulatory/`. The page displays the Luna briefing,
Terra kind and evidenced applicable jurisdictions, publisher identity, date and
the original-source link. It does not present the material as original reporting.
The Hugo builder also writes `data/regulatory.json` for the page.

Daily `/feed/days/YYYY-MM-DD.json` retains the existing `items` and `counts`
contracts. Starting on the configured ISO day, each item has a `sections` array:
ordinary articles carry `news`, CVEs carry `cve`, and a qualifying article
carries both `news` and `regulatory`. That article appears once in `items` and
has a small `regulatory` object with its kind and evidenced jurisdictions.
The existing `kind` discriminator is unchanged for downstream consumers.
Earlier days retain their existing shape.
No all-history rewrite is required. The recent canary is limited to explicitly
chosen article IDs from November 11-14, 2026; the normal pipeline processes new
articles going forward. A changed article updates only its dirty day and the
normal Hugo release path.

The incremental archive uses a separate schema fingerprint for days on or after
the configured start date. This regenerates a recent day once for the additive
field without changing the signature or bytes of earlier days. The production
canary explicitly refreshed November 11-14 only, under the normal archive lock;
November 10 retained its original SHA-256 and three-field JSON shape.

## Budget and rollback

`SV_OPENAI_MONTHLY_BUDGET_USD` is a positive USD cap shared by official OpenAI
calls through the production transport. A request reserves its estimated maximum
cost in PostgreSQL before network submission; provider usage settles the
reservation. Unknown models fail closed while the cap is set. Ambiguous network
failures retain their reservation until reconciled; this favors containment over
automatic overspend. The dashboard reports current-month charged estimate,
reserved estimate, and the last 30 days of hosted published/held analyses with
recent hold reasons. The ledger starts counting when deployed; it does not
retroactively import provider billing. Prices are estimates from the configured
model rate table, not the provider invoice. Keep a separate provider-side alert
below the account's absolute spend ceiling.

To roll back article inference, change the platform value to `local` and roll
only the local article worker. Do not delete hosted analysis rows or republish
historical JSON. If the regulatory page or section membership proves wrong,
leave the original feed intact, disable the menu/page in the Hugo source, and
rebuild only affected recent days after correcting the classification policy.

## Verification

Run the offline suite, Hugo build, Helm lint, rendered/live manifest diff and
server-side dry run before release. Check a ten-article recent cohort for valid
source-version, narrative, supported kind/jurisdiction and expected non-Event
decisions. Then verify worker queue drain, budget ledger, `data/regulatory.json`,
the current and previous daily JSON, root/Regulatory/Search HTTP 200, the
download link, and a normal atomic Hugo build. Older day JSON should remain
byte-identical. Any persistent disagreement or budget exhaustion should leave
the existing news path publishing through local fallback rather than silently
dropping articles.
