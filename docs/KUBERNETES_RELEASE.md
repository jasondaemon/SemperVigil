# Kubernetes source-only release

SemperVigil production is Kubernetes (K3s/containerd), not local Docker or Compose.
Prior releases used a designated remote image-build host with a retained dependency
image. Docker there is an image construction/export tool, not the production
application runtime. Local Docker availability is irrelevant.

1. Read CODEX_RULES, CURRENT_CONTEXT and STABILIZATION_VERIFICATION. Verify live
   image tags and platform values, dirty files, current jobs and public checks.
2. Recover the established build host/account/key from deployment records rather
   than guessing a root login or using a similarly named key from another directory.
   Keep machine-specific credentials and private addresses outside this app repo.
3. For source-only changes, stream `git archive HEAD src docker/release-overlay`
   to the established remote builder. Use `docker/release-overlay/Dockerfile` with
   the retained verified dependency base and exact source revision. Do not use
   this process for dependency changes or copy code into running application pods.
4. Export the immutable image and import it using `k3s ctr images import` on every
   schedulable node. Verify matching manifests. Keep previous tags for rollback.
5. Change only environment-specific tags/flags in k8s-platform values. Render
   the app Helm chart, extract affected resources, use server-side dry run and
   compare with live. Never apply the full release for a scoped change.
6. Pause orchestrator admission, wait until relevant running/queued model work
   and launch leases are empty, then drain the old LLM pod before replacement.
   Apply only approved ConfigMap/Deployment changes, restore the same replica
   counts and admission, and verify readiness. Preserve one local-model job.
7. Validate public HTTP/JSON, API health, scoped behavior and live/render agreement.
   Commit and push application and platform changes; leave unrelated platform
   edits untouched. Record exact tags, tests, canary IDs, rollback and limitations.

No direct Hugo execution belongs in this process. A private article comparison
requires no web or builder rollout, public rebuild, profile edit or DB migration.
Disposable database tests may run in a short-lived Kubernetes pod with no service,
no persistent storage and a localhost-only listener, reached by port-forward.
Delete the test pod and close forwarding after the test.
