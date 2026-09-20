# Kubernetes source-only release

SemperVigil production is Kubernetes (K3s/containerd), not Docker Compose. The
current source-only release path uses the Kubernetes node `docker52` as an image
packaging workstation because it retains the verified dependency layers. Docker
on that node only constructs and exports images; it is not a SemperVigil runtime
component, a Hugo builder, or a separate deployment environment.

1. Read CODEX_RULES, CURRENT_CONTEXT and STABILIZATION_VERIFICATION. Verify live
   image tags and platform values, dirty files, current jobs and public checks.
2. Confirm `docker52` is still the recorded packaging node and inspect the retained
   base image's `org.opencontainers.image.revision` label. Deployment history and
   live K3s state override older host notes. Keep credentials and private addresses
   outside this app repo.
3. For source-only changes, stream `git archive HEAD src docker/release-overlay`
   to the packaging node. Use `docker/release-overlay/Dockerfile` with
   the retained verified dependency base and exact source revision. Do not use
   this process for dependency changes or copy code into running application pods.
4. Export the immutable image and import it using `k3s ctr images import` on every
   schedulable node. Verify matching manifests. Keep previous tags for rollback.
5. Change only environment-specific tags/flags in `k8s-platform` values. Run its
   `apps/sempervigil/render.sh` with `bash`, extract affected resources, use
   server-side dry run, and compare the namespace-scoped render with live. Never
   apply the full release for a scoped change.
6. Check the affected queue before replacing a worker. Pause admission only when
   a rollout could interrupt active work. Apply only the affected resources and
   verify each rollout; do not restart unrelated workers, the Hugo build worker,
   or the public web Deployment.
7. Validate public HTTP/JSON, API health, scoped behavior and live/render agreement.
   Commit and push application and platform changes; leave unrelated platform
   edits untouched. Record exact tags, tests, canary IDs, rollback and limitations.

Do not use the retired pre-Kubernetes `.43` host instructions. No direct Hugo
execution belongs in this process. A private article comparison
requires no web or builder rollout, public rebuild, profile edit or DB migration.
Disposable database tests may run in a short-lived Kubernetes pod with no service,
no persistent storage and a localhost-only listener, reached by port-forward.
Delete the test pod and close forwarding after the test.
