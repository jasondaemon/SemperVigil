# Private two-pass article evaluation

November 14, 2026. Evaluation only; no article, Event, build, or daily-JSON writes.

## Tested boundary

The read-only probe in `tools/article_unified_hosted_probe.py` supplied every
numbered passage of each retained article to GPT-5.6 Luna. The ten source texts
were 3,676-12,999 characters and all fit the 30 KB input guard; none was
truncated. This is the complete text retained by SemperVigil, not a claim that
the publisher's original page was captured perfectly. A separate, narrowly
framed GPT-5.6 Terra applicability pass used the same source versions and
handled jurisdiction, named organization roles, and products. The Luna pass
supplied reader briefing, cited context facts/timeline/uncertainties, primary
kind, and a tentative Event seed. No model result was promoted to production.

The ten IDs covered a Google GDPR fine (35696), Australian proposed AI rules
(39299), Citrix vulnerability advisory (39337), Maersk breach (39408), Premise
Health insider breach (39301), UK NCSC forecast (39444), ICO reprimand (39457),
NIST implementation guidance (39458), Binance theft (39453), and non-incident
AI analysis (39467). All ten final Luna results passed structural and passage-ID
checks. They contained 12-22 context facts each, versus 13-35 facts in the
existing Qwen context packs; counts alone do not measure factual completeness.
Their primary kinds agreed with the focused Terra output on all ten source
versions. Three incident seeds named the same victims as the focused pass; the
other seven had no Event seed. The ten narratives were read for obvious
source/qualification problems, but every fact has not been independently
adjudicated for entailment.

Five additional final-contract Luna calls repeated the Australian policy,
Binance incident, and NIST guidance inputs. All five passed structural checks
and preserved the primary kind and Event/no-Event decision. Fact selection and
wording varied, as expected; this is semantic stability, not byte identity.

Earlier one-pass attempts that also requested entity roles were not acceptable:
they repeatedly treated a background Australian incident as a primary Event or
assigned private crypto-analysis firms public-authority roles. Repeating that
same prompt reproduced the mistakes. The separate focused Terra classifier
removed those errors on the same pinned sources. Therefore Luna's broad-pass
organization and product roles must not be materialized; the final Luna contract
does not ask for them. The focused pass remains the authority for those fields.

## Measured token cost

| Ten source-pinned calls | Input tokens | Output tokens | Estimated USD |
| --- | ---: | ---: | ---: |
| Luna briefing and evidence | 24,292 | 11,596 | $0.01877 |
| Terra focused applicability | 25,789 | 1,023 | $0.06385 |
| Combined | 50,081 | 12,619 | $0.08263 |

The estimate uses published standard rates of $0.20/$1.20 per million
input/output tokens for [Luna](https://developers.openai.com/api/docs/models/gpt-5.6-luna)
and $2/$12 for [Terra](https://developers.openai.com/api/docs/models/gpt-5.6-terra).
It excludes retries,
cache accounting, and existing Event calls. At this exact case mix and token
usage, 100/150/200 articles per day would cost about $25/$37/$50 per 30-day
month. Regulatory-source expansion may change article length and volume, so
these are scenarios, not forecasts or budget guarantees.

## Release requirements

1. Add a single application-side monthly reservation and actual-cost ledger for
   **all** OpenAI work, including existing Event calls. Establish a hard cap below
   the account allowance and a separate article-enrichment allocation. Reserve
   in-flight worst-case cost before each request; reconcile actual usage and
   fail closed at the cap. If the account key serves other workloads, verify
   account-wide spend separately.
2. Keep the existing Qwen summary and build path authoritative during a shadow
   deployment. A failed, over-budget, stale-source, or oversized hosted request
   must not stop article publication or corrupt the daily JSON. The 30 KB probe
   cap is a test guard, not a license to silently truncate longer articles.
3. Persist a versioned, source-fingerprint-bound result. Validate each section
   independently. Use focused Terra roles, not Luna's discarded role fields.
   Require agreement on incident kind and victim before an Event seed can enter
   the existing correlation workflow; search, corroboration, and promotion gates
   remain separate.
4. Compare shadow outputs with the current site and downstream JSON contract.
   Only then switch article consumers incrementally, with a rollback to the
   unchanged Qwen path. A ten-case sample supports this controlled pilot, not
   unrestricted Event publication.
