# Offline editorial checkpoint, October 6, 2026

This branch stages writer/reviewer wording only. It does not change schemas,
providers, models, budgets, correction prompts, evidence, publication gates or
accepted reports. Do not include it in the presentation release or active pilot.
The exact proposed instructions are in `event_report_contract.WRITER` and
`REVIEWER`; the old short-overview requirements are removed.

## Analyst report content contract

The overview orients the reader in connected paragraphs proportional to evidence:
affected parties, supported mechanism and sequence, consequences, response status
and essential uncertainty. An item is a paragraph and provenance unit; separate
items when epistemic type or material attribution changes. Findings retain source
qualifiers; assessments and gaps retain explicit type, confidence, rationale,
premises and limits. Complete cited articles determine support; quoted passages
remain exact, resolvable provenance anchors.

Each deeper section adds supported mechanics, chronology, discriminating evidence,
consequences, response or warranted analysis. Short recap is acceptable when it
anchors added detail. Rephrasing alone is substantive redundancy. Sparse evidence
may justify a short report and omitted sections. There are no paragraph, heading
or word quotas. Generic gaps are not padding. Dates retain source precision;
unknown/relative dates receive no invented calendar anchors. Added report analysis
and newly incorporated older evidence are not later incident developments.

Reviewer issues must name affected item and source IDs and must hold publication.
Stylistic preferences and harmless lexical overlap are separate. Passage locator
warnings do not determine readiness. No semantic heuristic was added to the
structural validator; an independent whole-source review remains necessary.

## Offline acceptance corpus and limits

`tests/fixtures/event_editorial_reports.json` contains clearly synthetic examples
for sparse incidents, documented intrusions, vulnerability disclosures, law
enforcement and evolving multi-source events. Each includes a supported positive
report and a hand-labelled paraphrase-only negative. Tests exercise exact source
spans, admissible connected/typed paragraphs, distinct technical/response detail,
brief recap, uncertainty and allegation preservation, older newly incorporated
evidence, duplicate rejection, issue-to-hold enforcement and generator freshness.

These are deterministic contract and review-response fixtures, not model-output
measurements. They do not prove that a hosted reviewer will detect paraphrase-only
redundancy or that a writer will produce better prose. After rollout review, use
this corpus for a separately authorized bounded canary before enabling a cohort;
record blind analyst labels, reviewer agreement, support/qualification failures,
section novelty, missing material facts and usage. No paid canary ran here.

## Generator identity and rollout separation

`event_source_reports.configuration()` hashes writer and reviewer prompts into
generator identity. `_fresh()` holds a queued run when configuration changes.
Published baselines retain earlier generator identity; a prompt upgrade cannot be
represented as new event evidence. The active pilot permits only evidence-change
successors and rejects generator-upgrade submissions. Shipping this branch into
workers would silently change pilot generation identity even with unchanged
model settings. Keep the running image/configuration frozen.

Presentation is a separate release: application bibliography branch d72daa9 plus
reviewed Hugo presentation commits, builder only. Do not use this generator branch
as its image source. After presentation approval, make a provider-free candidate
build, check immutable report fragments against normal release authorization,
verify sources/index/history and non-event routes, then activate through the
existing guard. Preserve rollback image/theme and public release directory.

For generator rollout, first review baseline policy, queued freshness, cohort
expiry and budgets; pin exact prompts/models/schema/config identity for a new
explicit scope. Resolve stale runs through normal holds, not bypasses. Authorize
any canary/re-generation separately and preserve immutable candidates and prior
publication pointers. Deploy generator workers separately from presentation.
