# Verified case-local exploration stops

New exploration policies can opt into a finite case-local stop. Omission keeps the original
whole-study stop behavior and injects no field into old policy/plan output. Confirmation execution
continues to stop the whole study on any failed or invalid selected condition.

```json
{
    "continuation": {
        "schemaVersion": 1,
        "mode": "verified-case-range",
        "maxVerificationMilliseconds": 360000,
        "maxVerificationBytes": 2097152
    }
}
```

These fields belong inside an otherwise complete capacity policy. The example bounds cover the
current non-JVM-observation policy with `sealAndAnalyzeMs=300000`; observation-enabled projects need
the larger independently derived temporary-parser and time reserves. No existing example or frozen
plan is silently upgraded. Plan/verify binds the optional policy, stop rules, budget preview and all
child comparisons. There is no new executor or retry mechanism.

```text
one original foreground task submission
          |
actual terminal identity + complete sealed lifecycle
          |
fixed JS verifier: full parent + paired raw/environment/resource reconstruction
          |
strict allowed symptoms or global stop
          |
exact owned CIDs absent on original observed Docker daemon/socket
          |
source + declared-unit checks
          |
continue next condition, or retain skipped remainder of this case and continue other cases
```

The verifier requires all four original arms, correct answers and sample accounting, distinct raw/run
identities, complete runtime/driver/resource evidence, no global issue and sufficient frozen goodput
and latency samples. An original `HTTP_QUALITY_FAILED` is understood only when its independently rebuilt
run issues consist of `warmup`/`measurement` driver-pressure or dropped-arrival observations. Allowed
measurement drop outcomes are `scheduler-late` and `concurrency-limit`. A generic quality label alone
never authorizes continuation. Cancellation, budget exhaustion, changed encoding, wrong answers,
decoding failures, timeout/transport errors, unknown issues, missing arms or insufficient samples stop
the whole study. The original failed task status, invalid paired evidence and raw problems remain.

An optional predeclared latency SLO may also stop further load. Its actual end-to-end quantile is used;
P99 requires at least 1,000 observations in each arm even when the analysis uses a P50 minimum of 100.
Equality with the SLO is allowed. Error and timeout rate denominators are not invented. This first
continuation increment permits only zero-request-error complete conditions. Neither driver symptoms
nor a protective SLO stop establishes a target-capacity upper bound. In particular, concurrency drops
can reflect the client in-flight cap after target latency rises; their cause is not inferred.

The boundary projection starts with `cleanupProven=false`. Before advancing, the controller checks
the fixed Docker executable's saved identity, then queries the daemon ID/server version and the
physical Unix socket's device/inode. All eight target/driver CIDs come from independently verified,
lock-bound final inspect records. Every ID must be unique and bound to its role's frozen image. Each
fixed `docker inspect --type container <exact CID>` must explicitly report that exact container absent.
A daemon query before and after must match the initial observed daemon/socket. A remaining CID,
unavailable daemon, unrelated not-found error, socket replacement or identity mismatch stops the whole
study. Declared-unit inactivity alone does not establish this cleanup proof. These are trusted local
controller observations, with no cryptographic attestation or whole-host exclusivity claim.
The fixed verifier returns the projection's byte digest through its actual stdout pipe. Python hashes
and parses the same bounded file bytes, rejecting a changed derived receipt. The new policy's existing
controller evidence lock also includes its baseline, boundary and owned-container raw observations;
the legacy lock remains the original four summary members.

A trigger stops all remaining undispatched conditions for that question, conservatively including
both selected modes. Other questions retain their original order. The controller does not prepare or
submit skipped children. `result.json` keeps every original point, task/request ID and failed trigger,
with `caseStopTrigger`, `ownedCleanupProven`, `stoppedCases`, `skipReason` and `triggerPointId`.
`execution-set.json` maps only physical executions. The report continues to show all planned missing
points; consult the controller result and observations for verified skip provenance, since the current
offline report does not project those skip reasons. A successful controller policy run does not make
every measurement valid or grant engineering/performance acceptance.

The per-point controller allowance reserves the old prepare/task tail plus the explicit verification
deadline, ten seconds of cooperative verifier cleanup, thirty seconds shared by all owned-absence
checks, and twenty seconds for the complete before/after declared-unit checks. The global allowance needs 160 seconds for the existing plan check, initial daemon observation
and unchanged final 100-second closing reserve. Computation/owned checks remain inside the original
point and study absolute deadlines. The first cancellation fixes the existing 29-second parent closing
deadline; verifier TERM cleanup and subsequent child cleanup share it. They never renew that budget.

Verification reserves two parent checks and the existing point/JVM replay bound. Work bytes cover
the declared temporary JVM parsing output/raw copy plus two MiB for the short projection and bounded
Docker command receipts. All point verification-byte reservations are included in the study cap even
if a case will later stop. With the non-JVM example above, the old 780,000ms per-point controller
allowance is too small; an explicit 1,160,000ms allowance covers the derived 1,150,000ms minimum.
The planner rejects inadequate reserves without shortening measurement, dropping cases or changing
resources. Normal ETA remains unknown.

Local validation includes a public full-eleven-case parent with four newly executed delayed localhost
HTTP arms producing admission drops and independent fixed-command reconstruction. Docker/JVM/procfs
and collector records in that test are explicit synthetic protocol evidence. Separate bounded real
subprocess fixtures exercise exact Docker CLI error handling over a temporary Unix socket, role-image
binding, remaining/reused IDs, daemon mismatch and original controller identity/skip bookkeeping.
These tests establish local protocol behavior; they do not establish a HugeGraph capacity boundary or
real Docker cleanup acceptance.
