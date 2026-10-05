# Hosted-model worker deployment

Never replace the hosted-model worker while it owns a running job. A provider
request is synchronous and its durable usage/result record is written only when
the request returns or raises.

For a guarded rollout:

1. Scale the orchestrator to zero so normal automation cannot admit more hosted
   work.
2. Leave the current hosted worker running until the `openai` queue has zero
   queued and zero running jobs.
3. Scale the hosted worker to zero. Recheck the database after termination; it
   must still have zero running `openai` jobs. Queued work may remain safely
   unclaimed while the worker is stopped.
4. Import and apply the new image while both deployments remain stopped. Verify
   the rendered/live image-only diff first.
5. Restore the hosted worker, verify its exact pod image and readiness, then
   restore the orchestrator. Confirm the queue and migration state before any
   canary admission.

If either zero-running check fails, abort the replacement and restore the prior
replica counts. Do not clear job locks, reset result JSON, or delete lineage.
