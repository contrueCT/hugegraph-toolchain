# Single-observation HTTP plans

Request version 3 supports one fresh-process observation for descriptive exploration. It uses the
existing comparison planner, task controller and full target/driver/collector/JVM lifecycle. The
scheduled slot is `O`; there is no fabricated `B`, paired effect, confidence interval or repeated-run
claim. A separate frozen confirmation plan still requires its actual independent observations.

```json
{
    "schemaVersion": 3,
    "kind": "http-comparison-request",
    "id": "example-observation",
    "purpose": "calibration",
    "comparison": "single-observation",
    "modes": { "O": "forward" },
    "blockIds": ["observe-01"],
    "sharedEvidence": {
        "build": { "path": "build.json", "sha256": "<actual SHA-256>" },
        "settings": { "path": "settings.json", "sha256": "<actual SHA-256>" },
        "environment": { "path": "environment.json", "sha256": "<actual SHA-256>" }
    },
    "analysis": {
        "maxMilliseconds": 60000,
        "latency": { "quantile": "p50", "minimumObservations": 100 },
        "goodput": { "minimumObservations": 100 }
    },
    "budgets": { "budgetMode": "total", "...": "all existing explicit lifecycle/storage limits" }
}
```

The fragment shows the new contract; replace the placeholders and supply every ordinary budget
field before planning. `modes` contains exactly `O`, and `blockIds` contains exactly one explicit ID.
The single-observation request excludes `orderSeed`, `metricRoles`, bootstrap parameters and effect
thresholds. All case/load/latency/goodput bindings remain present. The frozen plan uses version 3,
`design: single-observation`, `profiles/O.json`, one block with order `O`, and one actual run ID.
The block and arm receipt fields retain their controller-envelope roles without denoting a pair.

The analysis policy is `http-observation-analysis-policy` with algorithm
`http-single-observation-v1`. Latency uses the explicitly selected P50/P95/P99 and its frozen count.
P99 requires at least `max(1000, minimumObservations)`. Insufficient descriptive samples have
`status: unknown`, `value: null`, and a separate recorded `observedValue`; they do not qualify the
observation. Arrival bounds describe possible counts, without predicting correct throughput.

Version 3 requires `budgets.budgetMode`:

- `total` enforces one whole execution deadline. Planning still displays the conservative sum of
  stage maxima, while allowing it to exceed the total deadline. Fixed warmup, measurement and settle
  time for every scheduled arm, plus declared stop time and a 10-second terminal-evidence reserve,
  must fit. Startup/check/drain/storage costs remain unknown, so a valid plan may still end partially.
- `stage-sum` retains conservative admission based on the sum of stage maxima.

`budgets.requiredTotalMilliseconds` continues to expose that conservative sum.
`budgets.enforcedTotalMilliseconds` is the declared whole deadline, and `stageSumFitsTotal` records
the comparison. The normal ETA remains unknown. The HTTP `workflowMaxMilliseconds` remains its
separate workload bound for collector/lifecycle checks.

Versions 1 and 2 retain their existing paired schemas and reject the new budget field. Version 1
keeps both metrics in inference; version 2 keeps latency in inference and goodput in quality. New
version 3 paired requests explicitly choose `metricRoles` with latency in inference and goodput
either in inference or quality, while preserving all original paired sampling/precision gates.
Frozen plans remain tied to their actual planner implementation; use that implementation to verify
an older snapshot rather than changing its locked provenance.

The offline APIs in `components/http/paired-evidence.mjs` are:

- `normalizeObservationEvidence(options)` accepts only a verified single-observation plan.
- `normalizePairedEvidence(options)` accepts only a verified paired plan.
- `normalizeHttpEvidence(options)` dispatches using the independently verified plan.

Options retain `planDirectory`, `executionDirectory` and optional `includeHttpSummaries`. All paths
perform the same raw-lock, answer/count/timeline, registration, runtime, driver, resource, JVM and
terminal revalidation, including final lock checks. Missing evidence retains the predeclared `O` row.
The single-observation verification has kind `http-observation-evidence-verification` and design
`single-observation`. Its normalized data uses `normalized-http-observations`, and `analysis.json`
uses `http-single-observation-analysis`; neither contains baseline/candidate or paired blocks.

The existing fixed adapter command and `writePairedEvidence()` also dispatch by the verified plan.
They write `observations.json`, `verification.json`, `analysis.json`, a descriptive offline HTML and
an exact output lock, without overwriting a directory. Single output locks have kind
`http-observation-adapter-output-lock`. The command reply retains its ordinary shape, with
`armCount: 1` and `statisticsEligible: false`. Inference and performance acceptance remain false.

Focused protocol coverage includes one newly executed localhost HTTP workflow with synthetic,
explicitly labelled Docker/proc/resource records, missing/corrupt evidence, false paired fields,
outer-resealed fake `B`, sample eligibility, legacy/v3 paired roles, total-deadline admission and
byte-identical offline report reconstruction after moving the inputs. This is tool verification,
without a HugeGraph capacity or engineering-acceptance claim.
