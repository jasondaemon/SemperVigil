# Event correlation and research recovery

## October 1, 2026: first new-article production result

MetaMask was discovered from normal article acquisition, researched to four
independent publishers, confirmed as a draft, curated, composed and audited by
the autonomous pipeline, then published on the public Events page. The first
build safely refused activation while its Event inventory changed; the next
scheduled build succeeded. This verifies publication of a new event without
manual content work. The curator prompt now explicitly states the existing
semantic-role safety constraints, and a new-anchor case held on the previous
role error retries only after a curator-version change. The validation gate
remains unchanged. The 104 legacy candidates are not migrated by this release;
Bitget duplicates and false anchors require separate reconciliation.

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

Only candidates created under the tested victim-role classification contract
carry an anchor version and enter automatic research or confirmation. The
classifier must distinguish an attacked organization from a vendor publishing
an advisory or a security firm reporting on others' attacks. A scheduler pass
every five minutes admits at most one such recent, unresearched candidate
(created within 14 days). Search is bounded to six results; the current worker
policy validates at most five per run. Validated sources are fetched, linked and
sent through article summarization. Their linkage re-evaluates confirmation and
enrolls confirmed drafts into the existing evidence-first reassessment pipeline.
Existing explicit `enrich_min_articles: 0` settings remain an opt-out. Research
jobs are deduplicated per Event, not globally.

An active draft for the same named victim within a 14-day reporting window
holds automatic confirmation, even after two publisher sites report. This is
a conservative duplicate guard, not a merge decision. Aliases and separate
incidents at the same victim still require a canonical-incident reconciliation
workflow; no old draft receives the new anchor version merely because it has
two sources.

Existing two-source candidates are not bulk-confirmed by the backfill. Bitget
currently has multiple candidates for apparently the same theft; bulk promotion
could publish duplicates. They require a separate canonical-incident
reconciliation step that merges article links only after a supported same-incident
decision and retires the duplicate without touching published pointers.

A read-only classifier probe on stored articles returned no named-victim Event
for the Apple CoreGraphics zero-day, Huntress ClickFix reporting or Citrix
patch advisory, while retaining the Bitget theft and FBI portal claim as
candidate incidents. The FBI claim still needs its allegation status preserved
by the evidence and publication stages; the classifier alone cannot establish
that the claimed compromise occurred.

Later articles now retrieve a bounded set of published Events for the same
named victim even without a shared threat-actor tag. This is only candidate
retrieval; the relevance validator must select exactly one incident without
contradictions before the article is linked as an update. Different incidents
at the same organization remain separate.

Release verification must cover candidate admission, publisher diversity,
confirmed-draft enrollment, evidence jobs, guarded publication, ordinary Hugo
builds and public daily JSON. Held fact-curation and composition audits remain
holds; do not override them to increase event count.
