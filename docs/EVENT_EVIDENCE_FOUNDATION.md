# Events evidence foundation (offline, not enabled)

`src/sempervigil/event_evidence.py` is a pure Python domain/validation module.
Nothing imports it from existing workers, routes, serializers, or publish code.
There is no feature admission, migration, database access, network request, new
LLM job, or public JSON change. Existing Events behavior is unchanged.

## Contracts

- `Evidence`: immutable source snapshot with an evidence ID, incident ID,
  evidence-origin ID, source URL, and exact text.
- `Citation`: evidence ID plus start/end code-point offsets and an exact quote.
  Offsets are not UTF-8 byte offsets or JavaScript UTF-16 offsets.
- `Claim`: immutable claim identity, incident identity, statement, assertion
  status, citations, optional date with explicit role/precision, and optional
  reference to a prior claim being superseded.
- `evidence_version`: deterministic SHA-256 over the entire supplied snapshot,
  including identities and provenance. Mapping insertion order does not matter.
  This is a content identity, not a signature or proof of authenticity.
- `validate_claims`: deterministic sorted error codes. Empty output means only
  that the supplied typed candidate is structurally consistent. It never returns
  a publication approval or a confidence score.

Dates distinguish incident, disclosure, publication, and observation. Unknown
dates remain null; month/year values are not promoted to a fabricated day.
Assertion states are asserted, alleged, disputed, and retracted. No number of
citations automatically promotes a claim to confirmed.

The trusted caller must supply evidence snapshots, incident assignments, and the
prior claim-to-incident map. They must not come from the same untrusted model
candidate being checked. Roundups need separately scoped evidence passages; an
article-wide incident label is insufficient.

## Trusted passage and matching helpers

`event_matching.py` remains isolated from production. `extract_passage` slices
explicitly approved source offsets, preserves the source URL/origin and document
identity, and hashes the entire document snapshot. Passage IDs also bind the
offsets and incident assignment. Citations use passage-relative offsets; the
returned Passage retains document-relative offsets. No text normalization occurs.
These helpers do NOT decide where an incident begins or ends in a roundup:
trusted scope selection still needs an ingestion/evaluation integration.

`match_incident` requires a canonical entity ID and an exact, namespaced,
incident-specific reference shared with exactly one known candidate. Duplicate
registries, conflicting entities, ambiguous references, and missing references
abstain. A new unmatched reference does not automatically create an incident.
Company-name similarity, a shared CVE, publication dates, or event kind are not
matching signals. An alias resolver and reference provenance checks remain
integration prerequisites; never pass a model-invented reference as trusted.
No URL fetching or public URL replacement occurs.

Synthetic multi-document checks cover repeat incidents, follow-ups, roundups,
Unicode offsets, changed snapshots, and syndicated copies retaining the same
origin. Different origin strings alone do not establish editorial independence.

## Evaluation corpus

`tests/offline/test_event_evidence.py` contains 31 named synthetic claim cases plus
additional cross-incident, correction, versioning, URL, identity, Unicode, and
non-entailment checks. The cases are fixed offline contract examples, not copied
reporting and not a claim of calibrated model accuracy.

```sh
python3 -m pytest tests/offline/test_event_evidence.py -q
python3 -m pytest -m offline --strict-markers -q
```

The larger Stage 0 evaluation corpus remains incomplete: curated multi-document
incidents, aliases, syndication, conflicting chronologies, and actual 7B model
outputs still need adjudicated expectations. In particular, source-origin IDs
are carried but their independence is not established by this validator.

A first [real-source seed](EVENTS_CURATED_EVALUATION.md) now supplies 12 provisional
review cases across four Odido documents, with short snapshot-pinned excerpts.
Fourteen offline checks preserve three supported, three structurally rejected,
and six semantically rejected but structurally passing cases. No actual model
evaluation or independent review has occurred; the broader gate remains open.

## Next integration gate

Follow the accepted [MCP/domain-service architecture](EVENTS_MCP_ARCHITECTURE.md).
Retrieval and validation belong to shared application services, with a read-only
internal adapter first. These helpers alone are not an MCP integration; skills
and proposal tools must not bypass trusted evidence or publication gates.

1. Integrate independently established passage scopes, canonical entity aliases,
   and incident-specific references with the tested extraction/matching helpers.
   Curate real multi-document examples, preserving existing public URLs.
2. Add a bounded raw-input parser; typed dataclasses are not a JSON schema or an
   untrusted-input boundary. Limit candidate sizes before constructing objects.
3. Add immutable persistence and evidence-version comparison within a transaction.
   This pure snapshot check alone cannot prevent a concurrent publication race.
4. Require a separate relevance/entailment and uncertainty gate; a genuine quote
   does not prove a statement. The test suite explicitly demonstrates this gap.
5. Wire only a disabled shadow path, validate it in production, and then consider
   bounded pilot admission. Never use an empty error list as public approval.

No deployment is necessary for the current unreferenced module. The next runtime
release must include an actual disabled integration path and its regression gates,
not merely ship unused files. Rollback now is simply reverting the isolated module
and tests; no production or data rollback is involved.
