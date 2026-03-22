# Deployment Support

SemperVigil supports two deployment targets:

1. Docker Compose
- This remains the primary portable local and single-host runtime.
- The compose topology and mount contracts documented in `docs/BUILD_PIPELINE.md` remain canonical.

2. Kubernetes
- Kubernetes support is additive.
- It must preserve the same trust boundaries, path contracts, and orchestration model documented in:
  - `docs/ARCHITECTURE.md`
  - `docs/BUILD_PIPELINE.md`
  - `docs/CURRENT_CONTEXT.md`

## Naming Policy

All Kubernetes resources must be explicitly identifiable as SemperVigil components.

Examples:
- `sempervigil-admin`
- `sempervigil-worker-fetch`
- `sempervigil-worker-llm`
- `sempervigil-worker-openai`
- `sempervigil-builder-scheduler`
- `sempervigil-web`
- `sempervigil-searxng`
- `sempervigil-db`

This is required so workloads remain readable when spread across nodes, namespaces, and dashboards.

## Kubernetes Support Rules

- Kubernetes support must not replace Docker Compose support.
- PostgreSQL is a required runtime dependency for SemperVigil.
- The recommended production Kubernetes topology uses SemperVigil with a separately managed PostgreSQL service.
- A convenience embedded PostgreSQL mode may exist for dev/lab use, but it must not be the production default.
- The build pipeline must remain:
  - `ingest -> publish -> hugo build -> web serve`
- The filesystem contracts must remain:
  - `/data`
  - `/site-src`
  - `/site`
- Fetch-worker VPN egress isolation must be preserved. In Kubernetes, this is implemented as a `sempervigil-worker-fetch` pod with a `sempervigil-vpn` sidecar sharing the same pod network namespace.
- Only the public web component may be internet-facing by default.

## Packaging Direction

Kubernetes support is implemented as a Helm chart under:
- `deploy/helm/sempervigil`

That chart must:
- preserve current env contracts wherever possible
- expose storage, database, ingress, and worker scaling through values
- keep component names SemperVigil-prefixed
- avoid homelab-specific hardcoded paths or hostnames
