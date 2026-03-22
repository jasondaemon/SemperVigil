# SemperVigil Helm Chart

This chart is the Kubernetes packaging layer for SemperVigil.

Design constraints:
- Docker Compose remains supported.
- PostgreSQL is required for application function.
- The build/serve pipeline remains:
  - ingest -> publish -> hugo build -> web serve
- Storage path contracts remain:
  - /data
  - /site-src
  - /site
- Fetch-worker VPN egress isolation must be preserved.
- Public exposure is limited to the web component by default.
- Component names must remain explicitly SemperVigil-prefixed.

This chart is currently a skeleton and intentionally does not yet render full workloads.
