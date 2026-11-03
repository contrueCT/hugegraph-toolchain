# HTTP capacity exploration reports

`hgbench http capacity report <execution-set.json> --plan <capacity-plan-directory> --out <new-directory> --timeout-ms <integer> --max-bytes <integer>` rebuilds an offline report from a frozen exploration plan and sealed comparison evidence. It performs no HTTP requests and starts no containers. `task completed` is never accepted as measurement evidence.

The execution-set has exactly these fields:

```json
{
    "schemaVersion": 1,
    "kind": "http-capacity-execution-set",
    "planDigest": "<frozen capacity plan digest>",
    "points": [{ "id": "<predeclared point id>", "executionDirectory": "children/<point id>/execution" }]
}
```

Paths resolve relative to the execution-set file. Missing mappings, absent files, invalid evidence and failed rebuilds retain their original point and four-arm denominator. Quality, confirmation and acceptance cannot be supplied by this manifest.

## Independent reconstruction

The report first invokes the public capacity-plan verifier, including every frozen child comparison plan. Each mapped point then runs the existing paired evidence bridge in a separate bounded process, rechecking the sealed execution, raw HTTP traces and summaries, environment identities, resource evidence, driver lifecycle and cleanup. The optional `includeHttpSummaries: true` bridge mode exposes summaries from that same raw reconstruction; the default bridge output is unchanged. The report does not read a hand-written quality flag or reuse an old report verdict.

Cross-point checks reject repeated physical execution directories, execution IDs, run IDs or raw-manifest digests. Registered-to-terminal monotonic windows from sealed metadata must follow the frozen point order without overlap on the same host boot. Those windows describe the complete comparison workflow, not individual request phases. Inputs are rechecked after reconstruction. Relative execution references remain stable when moving the input tree; temporary local realpaths are used only for IO and alias checks.

The output contains `report.json`, an interactive offline `report.html`, copied execution-set bytes, individual point verification results, command audit records and `evidence.lock.json`. The lock lists exact payload paths, byte lengths and SHA-256 values; its `contentDigest` is SHA-256 of the UTF-8, two-space-indented JSON files array followed by one LF. `implementation.json` records the physical local `components`, `src`, `dist`, `release`, `benchmarks`, `presets` and `package.json` closure. Installed dependency bytes are explicitly outside this inventory. Rebuilding with a different tool must retain that different tool identity.

## Interpretation

Each curve contains one case and mode, with every frozen arrival rate. Correct throughput is the median of the four independent arms' correct completions inside the measurement window, with minimum and maximum retained. It excludes drain-only completions. Latency is the median of arm quantiles; requests are never pooled across arms or cases. Every arm retains its raw values, counts, issues and evidence gates.

A complete valid exploratory point can nominate a highest observed correct throughput. Its status remains `unconfirmed`; an independent confirmation experiment is still required. No mixed workload score, summed database QPS, driver headroom, warmup stability, observer-cost acceptance, engineering acceptance or statistical certification is inferred.

P99 is unknown when any required arm has fewer than 1000 correct latency observations. The original nearest-rank number remains explicitly descriptive evidence. Existing predeclared minima still apply to other metrics and in-window correct completion counts.

Invalid pressure or dropped-request points may have `diagnosticObservedThroughput` when all four raw correct-count summaries independently verify. These appear as gray hollow points and retain their issues and `qualification: false`. They never enter the highest-valid-point selection. Wrong or missing raw evidence cannot produce that aggregate; individual recorded arm values remain visible.

An optional frozen SLO retains its original latency threshold. The initial policy does not declare error/timeout rate denominators, so those SLO gates and SLO capacity stay unknown. Sent-based and scheduled-based ratios are separately labeled for inspection. This report does not establish an attributable capacity boundary.

## Resource and failure boundaries

Both command limits are mandatory. `--timeout-ms` accepts 1000 through 86400000; `--max-bytes` accepts 1 MiB through 8 GiB and limits newly written evidence, including failed worker output. These limits do not replace the frozen raw-input budgets. The execution-set is at most 1 MiB; worker stdout plus stderr is at most 256 KiB; one point result is at most 32 MiB. Local implementation inventory is limited to 32 MiB and 4096 files. A report may need more than the 1 MiB syntactic minimum to reserve its full denominator and final summaries.

Plan and point workers share the caller's overall deadline. Cancellation or timeout sends TERM to the owned worker group, allows up to three seconds for cleanup, then kills remaining group members and reaps the leader. The public controller adds its own bounded outer cleanup supervision. Expensive final scans check the deadline between bounded files. The final JSON and HTML are encoded and byte-checked before either is written. Output must be new and cannot overlap the input plans, raw executions or local source/dependency trees, including parent symlink aliases. Failed attempts retain bounded failure evidence and never return success.

## Current verification scope

The dedicated pure tests cover complete missing denominators, per-arm aggregation, P99 minima, diagnostic pressure points, SLO unknowns, reused/overlapping identities, input alias protection and cancellation. The localhost projection fixture uses actual HTTP raw records plus explicit synthetic lifecycle/resource evidence and an explicit synthetic parent projection. Its preview is for report inspection only; it is not public parent-plan verification or a HugeGraph capacity score. A real HugeGraph capacity execution-to-report run remains a separate acceptance item.
