# Finite capacity exploration and confirmation controller

`http capacity prepare` creates one generic component task for a frozen exploration or nonempty confirmation plan. The task
prepares and runs the existing comparison workflow for each point, sequentially. It does not add a
runner, reset protocol, task type or inference algorithm. The default controller stops the entire study
on every failed, ambiguous, cancelled or invalid point. New exploration policies may explicitly enable
[verified case-local stops](http-capacity-continuation.md), with independent complete-condition verification
and exact owned-container absence checks before continuing other questions. A failed task alone is never
a capacity boundary, and a partial attempt cannot resume. Confirmation execution reuses
the same component task and child comparison lifecycle; independent report/scoring remains a separate
verification step.

```text
frozen full capacity plan + saved comparison runtime (unchanged)
        │
capacity prepare: copy/verify plan, bind source/Node/Python, generic task.json
        │
task run (one explicit request identity)
        ├─ create-only attempt dispatch claim
        └─ point in original order
             ├─ source/runtime + declared-unit checks
             ├─ existing comparison prepare → original task identity
             ├─ durable identity + dispatch intent
             ├─ one foreground task run, internal wait
             └─ sealed lifecycle/source/unit checks → next point
```

## Confirmation plans

Pass a frozen [confirmation plan](http-capacity-confirmation.md) directory to the same `--plan`
argument. Exactly one of `capacity-plan.json` or `confirmation-plan.json` must be present. The
execution-runtime schema and existing exploration workflow shape stay unchanged. The current
implementation accepts both strict descriptor shapes; it derives the phase from the locked copied
scientific plan rather than accepting a caller's dispatch recipe.

A confirmation child retains its original `confirm-*` identity, rate/index, `comparison`, A/B modes,
source-point relations and eight arms. Four pairs use the existing comparison task. Preparation checks
the actual child plan still has eight arms, four two-arm blocks and the original modes before any
submission. Each child is submitted once with its original request/task identity. Unknown submission,
quality/lifecycle failure, drift or cancellation stops the full attempt; no automatic retry follows.

The complete eleven-question selection denominator remains in the copied plan. For a two-mode study
it contains 22 rows. `result.json` retains these original `selections`, `selectionRows`, every selected
comparison row (including `not-started`), and `confirmationQualified=false`. Missing candidates do not
produce synthetic child tasks. A plan with zero selected children is a valid planning result with
unknown selections; execution preparation rejects it with `capacity-confirmation-no-eligible-children`
before creating an attempt or running a target command.

Confirmation uses the new plan's `budgets.point`, `.controller` and `.study`; the inherited policy's
exploration four-arm budgets are not reused. The same generic `execution-set` kind maps its
confirmation plan digest and selected point IDs. A confirmation report must verify that plan kind,
all selection rows and the independent eight-arm raw evidence; the exploration report is not reused
as confirmation proof.

The source report is independently replayed during preparation, immediately before the first child
and after the last successful child. All three checks run outside measurement workflows. Let `R` be
the frozen `sourceReview.maxMilliseconds`. These explicit safety reservations are required:

```text
execution-runtime.prepareMilliseconds >= R + 10,000
confirmation controller.totalMilliseconds >= 2 × (R + 10,000) + 100,000
```

The 10-second allowances cover local verification/control work around each replay; the existing
100-second global closing reserve remains. They are safety allowances, not estimated normal runtime.
Each replay keeps its original source deadline and output cap. The runtime's prepare limit still
tops out at 300,000ms. Insufficient preparation or global budget is rejected without reducing the
review, number of children, measurement windows or pairs. Normal task timeout comes from the frozen
confirmation estimate. Failure cleanup remains 20 seconds within the 30-second task TERM grace and
does not attempt another long source replay.

Only the fixed confirmation source-verification command uses a cooperative termination path: one
TERM to its owned CLI group, up to ten seconds for its report handlers to reap nested detached workers,
then KILL/reap. The first cancellation fixes one parent deadline. This replay grace and any subsequent
child cleanup share the original 30-second TERM window, with one second reserved for closing work;
neither a descendant nor a later cleanup call resets it. Other existing control/foreground command
paths keep their original supervision. The local process fixture exercises an actual owner TERM,
detached child and TERM-ignoring detached grandchild and checks that all three PIDs disappear.

New attempt output cannot overlap the original exploration plan, report, execution-set or any raw
execution reference, including parent aliases and missing suffixes. Frozen source references must
remain accessible for both pre/post replay. A changed original source prevents further dispatch or
final success. No case receives a confirmed score from the controller's success flag.

## Inputs and public commands

First deploy and freeze the current implementation, save its actual source/runtime paths in the
project, and create a new [capacity plan](http-capacity-execution.md). The old quick runtime is not
silently rewritten. Preparation rejects a `sourceRoot` that does not identify the actual current
capacity component. It reads `capacity/project/runtime.json` and retains its exact bytes; the execution
runtime accepts no driver, target, JVM, collector, Node or image resource override.

```bash
hgbench schema http-capacity-execution-runtime
hgbench http capacity describe
hgbench http capacity prepare capacity-execution-runtime.json \
  --plan /absolute/capacity-plan --out /absolute/new-attempt
# Inspect the returned frozen taskPlan, then explicitly start it:
hgbench --root /absolute/controller-tasks task run \
  --plan /absolute/new-attempt/task.json --request-id capacity-attempt-001 --detach
hgbench http capacity status /absolute/new-attempt
hgbench http capacity stop /absolute/new-attempt
```

An execution-runtime example, with paths/units selected for the actual host:

```json
{
    "schemaVersion": 1,
    "kind": "http-capacity-execution-runtime",
    "cleanWindow": {
        "systemctlExecutable": "/usr/bin/systemctl",
        "requireClean": true,
        "units": ["retained-convert.service", "retained-reference.service"]
    },
    "budgets": {
        "prepareMilliseconds": 300000,
        "commandMilliseconds": 45000,
        "maxCommandBytes": 2097152,
        "maxWorkBytes": 137438953472,
        "minFreeBytes": 85899345920,
        "cleanupMilliseconds": 20000
    }
}
```

The declared systemd checks reuse the existing seven-field gate: exact Id, `loaded` or `not-found`,
inactive/dead, PID zero, empty control group, successful query and complete unique fields. An absent
unit records `unit-absent`; it does not prove an earlier job succeeded. The observer describes only
these declared units, with `wholeWindowExclusivityProven=false`. Protected or unrelated services are
never stopped by this controller. Existing target/driver watchdogs remain active.

## One submission and durable evidence

Preparation creates no target or HTTP request. It copies the complete plan and freezes the helper
scripts, saved runtime, implementation inventory and actual Node/Python executable identities. During
execution, each point is prepared through public `http comparison prepare` immediately before its use.
The controller validates its comparison digest, generated task digest and task-file SHA, then fsyncs
the prepared receipt, `current.json` and dispatch intent before the sole `task run` call.

The point key is `capacity:<attemptId>:<point.id>`; its task ID uses the existing public JSON-string hash
contract. The normal path launches one foreground CLI with its internal task wait. It does not start a
new CLI every second while measuring. The child comparison retains its existing driver lifecycle and
HTTP request validation/trace behavior.

A lost response leaves `submission-response-unknown`. Cleanup may query, cancel and stop only that
original task/comparison. It never invokes `task run` again. A create-only `dispatch.json` prevents
another run of this attempt. A user may explicitly prepare a new attempt to repeat the scientific plan;
there is no hidden claim in the immutable plan's parent directory and no distributed-lock claim.

`result.json` always retains all original point rows after a claimed run, including `not-started` rows.
It records lifecycle failure separately from the absence of later execution. `execution-set.json` is:

```json
{
    "schemaVersion": 1,
    "kind": "http-capacity-execution-set",
    "planDigest": "<original capacity plan digest>",
    "points": [{ "id": "<plan.points id>", "executionDirectory": "children/<id>/execution" }]
}
```

Only existing physical child execution directories are mapped. Omitted mappings do not remove their
planned points: a report must use the frozen complete plan denominator and independently reverify each
child's raw/quality/environment evidence. Controller summaries and `ok` flags do not establish a score,
headroom, stable warmup, observer-cost qualification, statistical certification or performance acceptance.

## Budgets, cancellation and cleanup

Each point shares an absolute deadline covering its original comparison hard budget and the frozen
per-point controller allowance. Comparison preparation is limited to its existing 330-second public
call bound. Normal control calls are clamped to `commandMilliseconds`; foreground task execution uses
its separate frozen task timeout and is never truncated to the short control-call limit. Each complete
clean-unit check also shares a bounded deadline. The whole task uses the original capacity safety bound,
with its final 100 seconds reserved for abort cleanup and summary/task sealing. Work-directory sampling
and the saved minimum-free-space reserve can stop the whole attempt without shrinking its scope.

The existing generic task permits at most 30 seconds of TERM grace. This controller therefore has
**20 seconds of shared cancellation cleanup**, followed by summary/sealing time. It does not promise
90-second TERM cleanup. Stop/query/cancel errors share that same deadline; one failed command does not
reset it or authorize another submission. Even acknowledged stop commands and a terminal task do not
independently prove every owned container is absent: `cleanupProven=false` remains explicit. Inspect the
original task and child ownership evidence, and use original-identity `stop` for further reconciliation.

Source or runtime drift prevents any new point or changed CLI from executing. If the trusted CLI/runtime
cannot be re-established during cleanup, the controller preserves cleanup as unproven; independent
original child TTLs remain the final containment boundary. It never uses broad process/container kills.
An abrupt kill or exhausted filesystem may prevent a final summary; the durable dispatch and point
intent remain the recovery identities, and the frozen plan still supplies the complete denominator.

The focused tests use explicit local protocol fixtures and real inert public task workers. They cover
lost replies, duplicate claims, source/runtime drift, time/work limits, short versus long command
budgets, exact-identity cancellation, failed cleanup, owner signals and descendant-group termination.
They do not establish a real Docker capacity run or any measured capacity result.
