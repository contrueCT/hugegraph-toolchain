# HTTP capacity exploration: frozen plans

This first increment implements `plan`, `verify` and `describe`. It creates existing HTTP profiles,
component-task templates and comparison plans. Compilation templates are removed after each comparison
has been frozen; the deliverable publishes profiles and comparisons. It starts no Docker containers,
HTTP clients or controller.
Planning outputs declare `executionReady=false`. The separately prepared
[finite executor](http-capacity-controller.md), [offline report](http-capacity-report.md) and
[independent confirmation planner](http-capacity-confirmation.md) now have local implementations.
Their local validation does not establish deployment readiness or real capacity results.

```bash
hgbench http capacity describe
hgbench schema http-capacity-policy
hgbench http capacity plan --project my-project.json \
  --policy examples/http-capacity-policy.json --out capacity-preview --dry-run
hgbench http capacity plan --project my-project.json \
  --policy examples/http-capacity-policy.json --out capacity-plan-001
hgbench http capacity verify capacity-plan-001
```

Save the existing build, settings, environment and comparison-runtime references once in the
[project format](http-presets.md). Their original bytes are copied into the plan. The target CPU, memory,
JVM, driver and collector resources are retained. Resource changes require a new saved project and plan.
`runtime.sourceRoot` and its actual Node/source identities must match the eventual frozen implementation;
the planner does not rewrite an older runtime or verify unavailable deployment assets.

## Fixed denominator and scope

```text
all 11 questions
  └─ each selected execution mode
       └─ each predeclared strictly increasing open arrival rate
            └─ independent A/A comparison: 2 balanced pairs, 4 fresh arms
                 └─ existing prepare → task → workflow → sealed evidence → report
```

An `aa` project selects its `aaMode`; `forward-embedded` selects both modes. The example has 44 or 88
points, respectively, and 176 or 352 arms. The order is case, mode, then rate. These independent per-mode
A/A observations provide the exploratory curves; their cross-mode order does not establish a balanced
A/B result. The candidate and eligible adjacent observed rates require a **new**, independently frozen
confirmation plan with four pairs. A two-mode confirmation uses the existing balanced forward/embedded
comparison at shared safe tested rates. Distinct per-mode extreme points need their own confirmation.
The original exploration-plan contract retains its confirmation declaration with `implemented=false`;
the separately invoked confirmation planner consumes the exploration evidence and creates a new plan.
Exploration itself creates no confirmation points and reserves no confirmation execution budget.

Correctness, exact answers, pre/post checks and complete traces stay enabled for every point. Each child
keeps the existing P50/goodput two-metric statistical policy, with at least 100 observations. Its inference,
degenerate-goodput warnings and unknown results are preserved. Child P50/goodput inference is descriptive
for that point and is separate from the capacity score. It establishes no study-wide statistical
certification. Reliable P99 still requires at least 1,000 correct observations; fewer observations produce
an unknown P99 claim. At 10 requests/s for 30 seconds, the scheduled maximum is only 300 observations.

The intended score is an eleven-element vector of throughput/latency curves and independently confirmed
highest observed correct throughput. No mixed-business proportions or summed score are invented.
Without an SLO, the observed result is not an unconditional sustainable-capacity limit. An optional SLO
must be frozen before observation. Its passing confirmed point is a lower bound; a higher point can form
a boundary interval only when it is an attributable target SLO failure and driver headroom is established.
The engineering result and optional statistical certification are separate. Absence of observed driver
pressure does not establish a calibrated headroom margin. Warmup stability and observer cost are pending.

## Time, storage and source locks

The example parameters are engineering proposals awaiting calibration: rates 10/20/40/80, gzip,
10-second warmup, 30-second measurement, at most 64 in flight. Confirmation is declared as 30/120 seconds.
These values make no 15–30 minute completion promise.

The planner calls the actual public generation, HTTP planning and comparison planning components for
every point. It sums their returned estimates rather than predicting throughput:

| Quantity                              |                  One point |  Both-mode example, 88 points |
| ------------------------------------- | -------------------------: | ----------------------------: |
| Fixed warmup + measurement            |      4 × (10 + 30) = 160 s |                      14,080 s |
| Fixed settle, listed separately       |                4 × 1 = 4 s |                         352 s |
| Required child lifecycle safety bound | actual comparison estimate | sum of actual child estimates |
| Declared comparison hard budget       |                    4,200 s |                     369,600 s |
| Controller allowance                  |                      780 s |       68,640 s + 300 s global |
| Entire exploration safety bound       |                          — |   438,540 s (about 5.08 days) |
| Normal ETA                            |                    unknown |                       unknown |

The 780-second controller allowance covers the existing prepare call bound, task stop/seal tail and
owner termination margin. It is distinct from the child comparison's own budget. The study cap is seven
days; all points are reserved even if a future controller stops some rates. Startup, checks, drain,
sealing and analysis have no measured normal-time model. Their conservative allowances are not an ETA.

Each arm reserves 256 MiB; four arms plus the comparison's 64 MiB output allowance reserve 1,088 MiB per
point. Eighty-eight points plus a 256 MiB final plan reserve and 64 MiB temporary compilation reserve
require 93.8125 GiB, within the 128 GiB example cap.
The plan's actual physical copies and lock must fit `maxPlanBytes` (at most 256 MiB). Individual inputs
are regular files at most 8 MiB; traversal is bounded by 20,000 entries and sixteen levels. Generation
checks cumulative bytes around each component call and each point. The existing comparison component
has its own 64 MiB output bound; working storage is bounded by `maxPlanBytes + 64 MiB` while the one
compile-only directory is present, and final published storage must fit `maxPlanBytes`. Partial local
generation is retained as `failure.json` when a newly created output fails. Dry-run uses a temporary
directory, removes it and never creates `--out`.

Collector sample/duration, driver/target TTL and known trace/JVM/log/command reserves are checked against
the unchanged saved resources. Their required workflow bound comes from each real child plan. Full
runtime-asset inventory, actual identity and combined evidence reserves remain the existing
`http comparison prepare` responsibility. Passing this planner is not deployment readiness. An
insufficient budget reports a stable `HTTP_CAPACITY_*_BUDGET` error with the missing bound; it never removes
questions or rates, changes timing, increases resources or lowers sample requirements.

`capacity-binding.json` records the original project/policy byte digests, all four source snapshots and
the complete fixed suite identity. It is included in every child comparison as target evidence.
`capacity-plan.json` records each point's case/mode/rate, child relative paths, plan/content digests,
workflow deadline, actual estimate and sampling bounds. Its `planDigest` uses the existing `hash()`
canonical JSON convention over the plan without `planDigest`. `capacity.lock.json` contains the exact
sorted physical-file inventory and `contentDigest=hash(files)`, excluding the lock itself.

Verification checks the physical inventory, original source bytes, normalized settings and every nested
comparison with its existing independent verifier. It reconstructs point IDs, profiles, A/A orders,
thresholds, source attachments and the derived summary. Rewriting an outer lock cannot legitimize a
different runtime snapshot, reduced denominator, load or profile. Source paths remain audit references;
verification uses the copied snapshots and supports moving the complete plan directory. As with other
local unsigned plans, an entirely regenerated consistent plan needs its original digest retained by
the caller as the external identity.

## Stop semantics and current executor scope

The default finite executor conservatively stops the whole study on any failed, ambiguous or invalid point.
New exploration plans may opt into [verified case-local continuation](http-capacity-continuation.md).
The original policy remains unchanged when the new field is absent. Missing points stay visible;
continuation does not claim that every curve has reached a target-capacity boundary.

Cancellation, unknown submission identity, failed ownership cleanup, source/runtime/plan drift or study
budget exhaustion stop the whole attempt and preserve partial evidence. A task's `failed` lifecycle
status alone cannot classify a capacity boundary. Stopping higher rates for one case/mode requires sealed
complete evidence, a valid environment/runtime and explained original quality symptoms. Every unexecuted
point and arm remains in the denominator with its reason; other cases may proceed only under that rule.

Driver pressure leaves target capacity unknown. Wrong answers, decoding/validator faults and unattributed
timeouts or transport errors are invalid boundary evidence. A configured SLO can stop further unsafe
load; there is no invented SLO when `slo=null`. Shared mode comparisons use shared actual safe rates rather
than pressuring a stopped mode to fill a common grid. A dispatched point is never replayed; retry needs
a new frozen attempt. The opt-in controller enforces the finite verified-case rules; its local protocol
tests do not establish live capacity acceptance.
