# Registry-based SemperVigil worker releases

Status: source and platform tooling prepared. The node-local worker release path is available without new registry credentials. No registry cutover has been applied.

## Why this exists

The current release builds an image on a packaging host, exports it, and imports it into containerd on every schedulable node. That is a distribution workaround, not an application requirement. A scoped, verified node-local path avoids requiring new credentials merely to make worker releases repeatable. The platform also has a private registry for other applications, but its use for SemperVigil is optional and separate. The first release target is only `sempervigil-worker-llm`, not Hugo, the builder, admin, or other workers.

## Node-local worker release

1. Commit and test the application source. From `k8s-platform`, run `bash apps/sempervigil/prepare-worker-image.sh prepare`. The helper packages the exact clean application HEAD from a verified retained base, assigns `sha-<12-character-commit>-local`, and verifies one image digest on every Ready schedulable node. It reuses an existing built tag rather than rebuilding on retry. Run `verify sha-<commit>-local` independently before cutover.
2. Set only `workerLlm.image.tag` in platform values to that verified tag, leaving `pullPolicy: IfNotPresent` and the applicability-review flag disabled. Run `bash apps/sempervigil/release-component.sh check worker-llm` and inspect the server-side diff. Commit the platform values and release scripts.
3. At an idle `llm_local` job boundary, run `bash apps/sempervigil/release-component.sh apply worker-llm`. Apply repeats the all-node digest verification, renders only that Deployment, and waits for rollout. The previous live runner has no graceful SIGTERM handling, so the idle check matters for this first replacement.
4. Verify exactly one ready LLM worker, queue progress, a normal completed summary job, and unchanged public site and daily JSON. The worker release does not change Hugo, feed generation, or article classification. Retain the previous tag and values for a scoped rollback.

## One-time prerequisites

- Create a SemperVigil-specific package path and separate writer and read-only pull credentials in the existing private registry. Do not copy another application's pull secret or store credentials in either repository.
- Provision a `kubernetes.io/dockerconfigjson` pull Secret named `sempervigil-registry` in the `sempervigil` namespace from the read-only credential. Verify registry TLS and image pulls from the worker's node before replacing the live worker.
- Build the exact committed application revision from the verified retained dependency base. Push an immutable tag to the registry, resolve its digest from the registry, and verify the image's `org.opencontainers.image.revision` label. Do not use `latest` or a mutable branch tag.
- Keep the current node-local image and platform values as the immediate rollback reference. The first cutover still needs an idle job boundary because the current live runner does not have graceful SIGTERM handling.

## Worker-only cutover

1. Set only `workerLlm.image.repository`, `workerLlm.image.digest`, `workerLlm.image.pullPolicy: IfNotPresent`, and `workerLlm.imagePullSecrets: [{name: sempervigil-registry}]` in the platform values. Keep `workerLlm.articleApplicabilityReviewEnabled: false` for the registry proof. Commit both repositories first.
2. Run `bash apps/sempervigil/release-component.sh check worker-llm` from `k8s-platform`. It templates only the worker Deployment, runs a server-side dry run, and shows the live diff. Review every changed field; no other SemperVigil resource is part of the command.
3. Verify no `llm_local` job or launch job is currently running, then run `bash apps/sempervigil/release-component.sh apply worker-llm`. The helper refuses an uncommitted platform values/chart change, a non-digest worker image, or a missing pull Secret. It waits for rollout. The new runner finishes a claimed job after SIGTERM and stops claiming new work; `Recreate` prevents two worker pods from competing for the single model lane.
4. Confirm the new pod's exact image digest, one ready replica, normal summary job completion, queue progress, and unchanged public HTTP/JSON. Only then consider a separately controlled private applicability canary. The initial registry migration does not enable article classification or alter publication.

## Rollback and limits

If the registry pull or rollout fails, restore the previous `workerLlm` image values and apply only the worker Deployment after checking the diff. Do not apply the whole rendered SemperVigil manifest. The old node-local image remains on the nodes. A registry outage does not remove already running pods, but it can prevent a new rollout on a node without the needed cached digest. Preserve the previous digest/tag until the new release is verified.

The release helper does not build or push images and cannot create registry credentials. A later CI step may automate the exact-commit build/push and digest receipt after the worker-first path is proven. The repository's current GHCR workflow follows `main`, which is not the running application revision, so it is not an acceptable shortcut for this migration.
