# Event correlation and research recovery

## September 30, 2026

Two articles about the DIVD intrusion linked because the victim and incident
window matched and the relevance model selected one unambiguous incident. Both
articles came from BleepingComputer, so this is one publisher, not two. Bitget
coverage was fragmented across three candidate records despite an existing
two-publisher candidate. Apple's zero-day coverage is a held draft: the source
articles describe attacks against unspecified individuals, not an Apple
corporate breach.

The minimal incident anchor is the affected victim/entity, specific unauthorized
action or affected asset, and a valid incident date when the source states one.
Missing or malformed model dates use the article date for matching and event-key
bucketing, but leave the actual incident date unknown. Legacy malformed
dates use first-seen date for bounded candidate lookup. A relevance check must
still find one supported incident match without contradiction; matching an
organization name alone never links an article.

Two independent publisher hostnames confirm a draft. Multiple articles from one
site remain one source. Research articles use their actual domains rather than
the shared `web_enrich` source ID. This is not a publication threshold: evidence
extraction, incident selection, narrative audit and release authorization remain
separate. False classification must be corrected, not pushed through the gate.

An uncorroborated new candidate queues web research. A scheduler pass every five
minutes admits at most one recent candidate (created within 14 days) that has
never had a research job. Search is bounded to six results; the current worker
policy validates at most five per run. Validated sources are fetched, linked and
sent through article summarization. Their linkage re-evaluates confirmation and
enrolls confirmed drafts into the existing evidence-first reassessment pipeline.
Existing explicit `enrich_min_articles: 0` settings remain an opt-out. Research
jobs are deduplicated per Event, not globally.

Existing two-source candidates are not bulk-confirmed by the backfill. Bitget
currently has multiple candidates for apparently the same theft; bulk promotion
could publish duplicates. They require a separate canonical-incident
reconciliation step that merges article links only after a supported same-incident
decision and retires the duplicate without touching published pointers.

Release verification must cover candidate admission, publisher diversity,
confirmed-draft enrollment, evidence jobs, guarded publication, ordinary Hugo
builds and public daily JSON. Held fact-curation and composition audits remain
holds; do not override them to increase event count.
