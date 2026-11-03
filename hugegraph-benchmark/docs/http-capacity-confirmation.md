# Independent capacity confirmation

The confirmation increment implements source replay, deterministic selection, four-pair comparison
planning and independent plan verification. It starts no HTTP requests, Docker containers or tasks.
No caller-supplied `verified`, `candidate` or `confirmed` flag is accepted by the request contract.

```text
original exploration plan + execution set + locked report
             |
     independent raw/environment reconstruction
             |
all 11 questions x execution mode: median of independent arm goodput
             |
candidate + immediate registered lower/upper neighbors, if actually observed safe
             |
new frozen comparison plans, four balanced pairs, fresh JVMs
```

Candidate ranking uses every predeclared independent exploration arm's correct completion rate inside
the measurement window. A single fast arm cannot outweigh the other arms. Equal medians select the
lower offered rate. The original immediate grid neighbors remain visible when missing, unsafe or
outside the grid; selection never jumps across them to insert a more distant point.

Every original question and mode remains in the selection denominator. Missing arms, wrong answers,
original quality issues, unverified environment/lifecycle/resources, observed or unavailable driver
pressure, or an unproven configured SLO prevent a safe nomination. When there is no candidate, the
question/mode remains `unknown`, with reasons, and produces no fabricated task. The current report
preserves SLO error-denominator uncertainty; confirmation cannot replace it with an invented denominator.

Selection is provisional. Unknown driver headroom, warmup stability and observer cost remain unknown;
selecting a point for further measurement does not make it confirmed or engineering accepted. The pure
selection function is an internal arithmetic projection, not a verification API. Its synthetic unit
fixtures are explicitly not runtime evidence.

For two-mode studies, a rate selected and observed safe in both modes uses the existing balanced
forward/embedded comparison. Other selected mode/rate conditions use separate same-mode A/A
comparisons. Each condition requires four balanced independent pairs and a new plan, seeds, request
identity and JVM lifecycle. Modes are never pooled into one goodput median.

The source policy's confirmation warmup/duration, workload, encoding, arrival process, in-flight limit,
timeouts, correctness checks, observer stack, analysis thresholds and optional SLO remain fixed.
`analysis.minimumPairs` becomes four. The A/A005 P99/metric-role policy is a separate experiment and is
not silently adopted by capacity confirmation.

Confirmation budgets are explicit new inputs. With the current example's single-case 30/120-second
confirmation, the existing comparison planner requires 243,000ms per HTTP workflow and 8,252,000ms per
eight-arm comparison including its lifecycle. Eight arms at 256MiB plus the comparison's 64MiB reserve
require 2.0625GiB. The exploration's four-arm 4,200,000ms budget is insufficient. Actual nested planners
must derive and check every bound; no window, pair, neighbor or question is removed to fit a budget.
The JVM parser reservation uses eight arms, including independent reparse cleanup, rather than the
exploration's four. Target/driver/collector TTLs and the new source/runtime inventory require their own
existing prepare verification.

After actual confirmation, the score must be recomputed only from eligible new confirmation arms. An
adjacent point may become the highest observed point. Failed confirmation cannot be replaced with an
exploration value. Without an SLO the result remains highest observed correct throughput, with its
conditions and limits; timeout or driver pressure does not establish a target-capacity upper bound.

The library entry points are `planHttpCapacityConfirmation(requestFile, { out, dryRun? })` and
`verifyHttpCapacityConfirmationPlan(directory)`. The public entry points use the existing controller:

```text
hgbench http capacity confirmation plan request.json --out NEW [--dry-run]
hgbench http capacity confirmation verify NEW
```

The strict `http-capacity-confirmation-request` schema requires an ID, `exploration.plan`,
`exploration.executionSet`, `exploration.report`, `project`, `sourceReview` and new `budgets`.
The exploration report reference names its physical directory containing `report.json` and
`evidence.lock.json`. Relative source/project paths resolve against the request file. The four project
source roles retain the existing HTTP project format. `budgets.point`, `.controller` and `.study` use
the original capacity budget field shapes; all values must be explicit. They reserve every selected
comparison even if a future run stops early.

`sourceReview.maxMilliseconds` and `.maxBytes` must cover the original report's declared replay
budget. The byte cap bounds the report inventory and replay output; original raw traces retain their
own existing analyzer/lock budgets. The consumer streams and verifies the report's exact inventory,
rebuilds the source plan and each available execution with the fixed bounded report component in a new
temporary directory, then requires byte-identical `report.json`. A self-consistently resealed summary
cannot nominate a candidate when the underlying evidence differs. The original and replay tool
inventory digests are both retained. Tool metadata differences are visible; matching numerical output
does not mean matching tool inventories. The temporary replay directory is removed on success and
failure. A source-report timeout, incomplete lock, changed input or replay mismatch rejects planning.

The new plan stores the request, copied project sources, original locked report receipts, full
selection denominator, every new comparison/profile and an exact inventory lock. Verification repeats
source replay, selection, resource checks, child comparison verification, profile/request reconstruction,
whole-study budget calculation and final plan semantics, then rechecks the complete inventory and original
lock bytes to reject a mixed snapshot during the long replay. Original exploration plan/execution/report
paths remain frozen references; they must remain accessible during verification. The plan directory
can move while those references remain valid. The consumer does not duplicate the original execution
raw/work trees or claim standalone portability of the original data.

Output directories are create-only. The output parent is resolved physically before checking input
separation; a symbolic-link alias into original evidence is rejected. The protected paths include every
execution-set directory (including missing references), project inputs and the actual installed
source/dependency locations after resolving symbolic links. Dry-run uses only a temporary
plan and leaves the requested output absent. A failed real freeze preserves its partial output and a
failure receipt without a valid final lock. Every selected point compiles the existing HTTP plan,
freezes the existing comparison and removes its compile-only task template. Nonempty confirmation
plans use the same finite capacity controller and existing prepare/runtime/owned-cleanup lifecycle.
The [confirmation report](http-capacity-confirmation-report.md) independently verifies the new arms
and keeps unavailable selections visible.

Fresh project resources are checked against the source snapshots. The original build JSON must be
byte-identical. CPU, memory, PID/tmpfs limits, JVM actual options, server thread/connection settings,
collector interval, JVM observation event switches and log byte caps remain fixed. Only these explicit
safety increases are accepted, with every changed field's before/after values retained:

- Target `budget.wallSeconds` and driver `wallSeconds`.
- Collector `durationMs`, `maxSamples` and `maxOutputBytes`.
- JVM parser `timeoutMs`, `maxOutputBytes` and `maxWindows`.

The runtime `sourceRoot` can point to the new frozen tool inventory. Fresh host `hostBootId` and
`coRunningJobs` evidence is retained; all other host environment fields must match. Existing prepare
and pre/post checks still have to establish the actual environment, binaries, source inventory and
combined evidence bounds. If the fixed log cap is too small, planning fails and requires a separately
reviewed budget change. No observed setting is silently rewritten. The resource preview's
`unchanged` flag means the planner kept the new explicitly supplied resources; the separate
`resourceChanges` list describes differences from exploration.

Local verification covers pure selection, actual eight-arm comparison/JVM reserve bounds and an
inert complete eleven-case public-plan fixture. Its real source report contains no executed points,
so it freezes eleven unknown selections and zero tasks. The test also covers dry-run, moved plans,
create-only output, symbolic-link input aliases, coherently resealed source summaries and false
acceptance claims. No real capacity exploration or eligible confirmation run has been executed by
these tests. A valid exploration with eligible candidates remains necessary before live four-pair
confirmation and scoring can be exercised.
