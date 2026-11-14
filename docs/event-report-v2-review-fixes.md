# Autonomous v2 independent-review fixes

Candidate only; production remains on6065f8c. No hosted model evaluation or migration deployment accompanies these changes.

Publication admission failures return publication_held receipts without changing accepted generation state. This preserves exporter/source-validation behavior when a competing promoter has already published the run. Public validation is unchanged.

Calls reload/recheck authority and phase history after the run and cohort locks. A durable journal is followed by another authority check; the native transport repeats the guard after credential resolution, directly before HTTP. After full freshness queries/hashing return, a final cheap policy/enablement/clock check prevents expiry during those queries from reaching HTTP. Only the instrumented authority boundary proves zero HTTP and clears outstanding transport reservation. The immutable call reservation, failed journal and lifetime admission budget remain consumed; unknown transport retains reservation and cannot replay.

Publication admission and promotion derive policy enforcement from stored autonomous provenance, independently of the caller's automatic flag. A saved autonomous approval without the bound policy proof is rejected. Legitimate non-autonomous editorial publication retains its separately reviewed path. This is a callable authority-boundary defect, not evidence of an unauthenticated HTTP exploit.

Ready reviews allow structurally valid nonblocking locator warnings, while requiring zero substantive issues, valid IDs/schema and independent models.

Migration072 installs scoped autonomous admission integrity triggers. Identity, snapshot/cohort, source/generation/predecessor, lifetime budget, creation time and correction flag freeze at insert; deletion and table truncation containing autonomous runs are rejected. Mutable execution counters/results remain operational. Legacy recovery/corrections retain existing behavior. Admission and checks fail closed if guards are absent. No grants, credentials or security settings change.

Real PostgreSQL regressions cover concurrent promotion/export withdrawal, unpublished publication failures, run/cohort lock waits across expiry and source/predecessor changes, post-journal and post-credential authority changes, admission and promotion rollback, missing/mismatched saved policy proof, benign/malformed warnings, existing-role forbidden edits and no-call deletion, absent guards and truncation. Simulated provider replies and intercepted HTTP only; these do not validate real hosted-model semantics.
