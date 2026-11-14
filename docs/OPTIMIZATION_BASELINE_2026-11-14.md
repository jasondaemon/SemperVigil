# Optimization baseline, November 14, 2026

This is a measured first slice, not a claim that every source or inference path is
healthy. Production images remain pinned. Event expansion remains held.

## Before change

- The read-only 24-hour job sample at 22:27 UTC found no queued or running
  jobs. It contained 123 successful `build_site` jobs (mean 53.60 seconds,
  p95 65.67 seconds) and two failures. The immediately preceding successful
  build took 55.93 seconds.
- A representative build refreshed one daily JSON and skipped 35 unchanged
  days. Hugo itself reported about 16 seconds; the full job took about 56
  seconds. The builder ran `ensure-dirs.sh` before every Hugo invocation.
- The live build container inherited `SV_FIX_SITE_PERMS=1`. As UID 501, an
  isolated invocation of that script recursively walked shared trees and took
  24.69 seconds. It reported an ignored permission error on a metrics file.
  The same directory setup with `SV_FIX_SITE_PERMS=0` took 0.01 seconds. The
  Deployment already had a root init container that runs the full ownership
  repair once per pod; the shared paths checked were writable by UID 501/GID 20.
- The feed inventory SQL was the largest visible `pg_stat_statements` temp-I/O
  consumer: 783 calls and 2,757 seconds accumulated since the September 27
  statistics reset. A bounded read-only `EXPLAIN ANALYZE` took 3.69 seconds
  with the current 4 MB `work_mem` and wrote 15,288 temp blocks. Session-local
  32 MB reduced that to 6,570 blocks but took 3.65 seconds. No memory setting
  was changed because elapsed time did not materially improve.
- In the same 24-hour sample, hosted analysis had 20 published records. The
  OpenAI budget ledger showed $0.229247 charged in November against a $90 cap,
  with nothing reserved at collection time. This is ledger cost, not a complete
  quality assessment. Older locally summarized articles remain in the sample.
- The public November 14 JSON had 13 articles and 18 CVEs, with no
  `web_enrich` article. `/` and the day JSON returned HTTP 200.

## Narrow optimization and verification

The application chart now sets `SV_FIX_SITE_PERMS=0` only on the build worker's
main container. Its root init container remains at `1`; `ensure-dirs.sh` still
creates required directories. The rendered/live Deployment diff contained only
this environment override. No image, Hugo command, feed generation, release
switch, resource policy, or application code changed. Only the build-worker
Deployment was applied, and it rolled out with one ready replica.

An ordinary API-requested full build then succeeded in **34.40 seconds** and
activated release `20261114223247`, versus **55.93 seconds** for the preceding
build. The observed saving is 21.53 seconds on this comparison, consistent
with removing the repeated permission walk. A single build is not a p95 claim.
The homepage and November 14 JSON returned HTTP 200 afterward; the JSON counts
remained 13 articles and 18 CVEs with no `web_enrich` item. The active queue
was empty after verification.

## Next measurements

Watch several ordinary builds and compare their median and p95 with the
24-hour baseline. Confirm newly created article data remains writable and
appears in the daily JSON. Capture build-worker peak memory and
acquisition-to-publication latency before altering resource policy or build
cadence. The full-history inventory query is a measured database hotspot, but
its 32 MB test did not justify a memory change; any redesign must preserve
changed-day detection and all existing JSON/link contracts. A five-minute
request bucket would have contained 92 buckets for 125 build requests in the
24-hour sample, but that is only a theoretical coalescing bound and would add
publication latency. Do not enable batching without an explicit latency gate.
