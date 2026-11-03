# HG-Bench Core Read v0.1-draft

Status: reproducible draft for semantic review. This package is the first Benchmark deliverable.
It is independent of the hgbench controller and can be regenerated and verified with Node.js 22.
It does not require a database, Python, Java, npm installation, or the controller build.

## 1. Deliverables and release boundary

This package fixes 12 task families, 36 parameter records across numeric/string IDs, two small directed graphs,
expected outputs or admissibility constraints, a reference implementation, a conformance profile and a scorecard schema.
The required conformance run executes every parameter twice (72 logical operations).

`suite.lock.json` locks every normative file, generated dataset, parameter file and expected-result file with SHA-256.
Its suite digest identifies the complete package. Changing an input, validator, profile or rule creates a new digest.
Released results must retain that digest. `generate` deliberately rewrites the draft data and lock; ordinary consumers
use `verify`, which refuses drift. A maliciously rewritten lock is outside checksum integrity guarantees.

The draft's tiny profile is for checking task semantics and adapters. It cannot establish useful database capacity,
production tail latency, a performance ranking, or a regression gate. An independent SUT adapter must still run it.
Reference implementation self-checks are reported as reference checks, not as database results.

## 2. Reproduce the package

From `hugegraph-benchmark/`:

```bash
node benchmarks/core-read-v0.1/reference.mjs verify
node --test tests-normative/*.test.mjs
```

For a reviewed draft change, regenerate:

```bash
node benchmarks/core-read-v0.1/reference.mjs generate
```

The generator contains no random source, current timestamp, external data download or runtime-dependent ordering.
Each dataset has 110 vertices and 282 directed edges, with one edge label `link` and vertex label `node`.
Numeric IDs are safe JSON integers; string IDs use the bijection `n -> "v" + n`.
Each vertex has exactly `group`, integer `rank`, and 256 ASCII `x` characters as `payload`.
The numeric and string datasets have isomorphic topology. Edge identifiers are stable and unique.

The graph contains wide two-hop expansion, convergent two-hop expansion, a directed cycle with a tail and an external entry vertex,
two tied shortest paths, one isolated vertex, and a hub sharing the wide graph's leaves.
No parallel edges or self-loops are present in this release. Such cases require a new dataset and suite digest.

## 3. Fixed tasks

The parameter order in `parameters.json` is normative. Both ID types execute the same task semantics.
All traversals follow outgoing `link` edges. Degree/capacity caps are unlimited unless a future profile specifies them.
`null` means no limit in the language-neutral task; an adapter translates to its backend's supported sentinel.

| ID | Task and parameter buckets | Required normalized result |
| --- | --- | --- |
| CR01 | ID lookup, hit and miss | Full vertex object, or `null` for absence |
| CR02 | Equality filter on `group`, 1/64/0 matching vertices | Unordered distinct ID set |
| CR03 | One-hop neighbors, 8/64 distinct neighbors | Unordered distinct target ID set |
| CR04 | All outgoing neighbors of the hub, page size at most 7 | Sequence of page arrays; union exactly equals all 64 neighbors |
| CR05 | Wide graph, two-hop nearest Kout | All 64 second-layer IDs |
| CR06 | Convergent graph, two-hop nearest Kout | All 16 distinct second-layer IDs despite 128 candidate edges |
| CR07 | Wide graph, two-hop Kout, limit 4 | Any 4 distinct IDs from the 64-member admissible final layer |
| CR08 | Cycle nearest/revisit and isolated-start empty layer | Exact final-layer ID set for each of the three parameters |
| CR09 | Common outgoing neighbors | Exact intersection of the two source neighbor sets |
| CR10 | Directed shortest path, maximum 6 edges | Any shortest legal vertex path; the fixture has two tied paths of length 3 |
| CR11 | Unreachable target with the same maximum depth | `null` |
| CR12 | Materialize every `wide-leaf` vertex | Unordered set of 64 complete vertex objects, including the payload |

CR12 in the tiny profile checks complete materialization with a modest payload. A future large-result performance
profile will increase physical size and must publish its own dataset digest. It must not silently reuse tiny timings
as a large-response performance result.

### 3.1 Exact Kout semantics

Initially frontier is `{source}`. On each of `depth` layers:

1. In nearest mode, add the current frontier to the visited set.
2. Expand every frontier vertex by outgoing `link` edges.
3. Deduplicate targets, always exclude the original source, and exclude visited vertices in nearest mode.
4. Replace the frontier with the resulting set.

Return only the final frontier. A finite result limit applies only to the final output for these fixtures;
intermediate layers are never truncated. For CR07, arbitrary allowed members are valid, but exactly 4 are required.
CR08's revisit parameter intentionally distinguishes nearest mode: from 2004 at depth 4, nearest returns only 2003,
while revisit returns 2000 and 2003. Source exclusion applies in both modes.

An API with different layer semantics is unsupported until a semantics-preserving implementation is supplied.
Sorting, merging, traversing or repairing an answer client-side is not an acceptable normalization shortcut.

### 3.2 Pagination and path details

CR04 does not require stable server iteration order. Each page has at most 7 results; the adapter follows server cursor
or equivalent pagination until exhaustion. No duplicate, omitted or extra vertex may appear across pages. Only the
final page may be empty. Stop after at most 128 pages or the operation deadline; a cursor loop is an error.
Fetching the full unpaginated result and slicing it client-side does not satisfy this case.

CR10/11 use simple directed vertex paths. A valid reachable result includes source and target, uses real graph edges,
has no repeated vertex, and has the independently established minimum edge count. A longer legal path is wrong.
Returning `null` for a reachable target is wrong. The reference oracle computes distance using BFS; reference path
construction enumerates bounded simple paths to supply an example, and tests also check known tied answers manually.

## 4. Adapter obligations

Import the exact graph, retain ID types, create the required equality index, wait for data/index readiness and record
the import manifest, storage settings, version, route, adapter revision and host details. The graph stays read-only.
Metadata added by the database can be removed from the normalized envelope, but every requested property and ID type
must remain. No answer caching, precomputed answer table, hidden index, or fixture-specific branch is permitted.

Normalize point/materialized vertices to `{id, label, properties}`. Set-like results normalize to arrays of IDs.
Pagination normalizes to arrays of pages. A shortest path normalizes to an ID array or `null`. Backend-specific error
codes are translated to separate error/timeout outcomes, never to an empty successful result.

Unsupported cases remain in coverage. Each required case and parameter must be supported for a conformant track.
The first adapter may support a subset, but its card must say incomplete. Route profiles are separate; a native Kout
API and a Gremlin query must share the defined result semantics before their measurements can be compared.

## 5. Fixed conformance execution

Use `tiny-conformance-v1`: one logical request at a time, two passes in the file order, 30-second deadline per complete
logical operation, no retries, all pages fully consumed, full validation on every response. A parameter/pass pair is
unique. Timing, when recorded, runs through full response consumption and declared decoding; oracle validation time
is separate. Conformance does not constrain cache state or interpret its timings as performance results.

The target must remain available between the two passes. A restart or changed graph is recorded as an interrupted
attempt; retain it and start a fresh attempt. Fixed conformance order must not be reused to claim randomized performance
comparisons. The benchmark identity includes the actual profile.

## 6. Performance protocol draft and freeze gate

The eventual performance profiles reuse the task semantics while separately locking larger datasets and stratified
parameter files. Their release requires all of the following concrete fields and evidence:

| Field | Release requirement |
| --- | --- |
| Scale | Exact vertices/edges/property bytes, degree and result-size buckets, generated files and checksums |
| Route and projection | Fixed normalized semantic contract, complete-consumption boundary, protocol and connection settings |
| Environment | Hardware/topology, quotas, JDK, SUT/driver binaries, indexes, effective config and background activity |
| Cache/warmup | Explicit restart/cache procedure, minimum warmup and stability rule, evidence on failure |
| Load | Fixed concurrency or independent arrival schedule, rates, mixture, seeds, inflight/queue bounds and drop rules |
| Measurement | Duration, independent repeats, randomized paired order, deadlines, drain and cohort count accounting |
| Validation | Full or predeclared sampled policy, coverage and measured overhead; no dropped validation records |
| Analysis | Primary metrics, absolute/relative practical budgets, uncertainty method and multiple-comparison family |
| Evidence | A/A noise, known-regression detection, load-generator/collector capacity and a second reproduction |

Until these fields are calibrated on a real adapter and fixed resources, the performance profile remains unreleased.
This is an explicit release gate, not a default that an agent can fill with arbitrary values. A minimal reference harness may support calibration in Phase 1. Full CLI implementation follows the
benchmark release candidate; official performance claims require a frozen performance profile.

## 7. Results and scoring

Use `scorecard.schema.json` and `scoring-policy.json`. Structure validation alone cannot prove coverage: the scorer
must verify all known parameter IDs, the two pass numbers, uniqueness, expected results and the suite digest.
Wrong answers produce correctness-failed; missing/unsupported/timed-out operations remain in the denominator.
All 72 operations must validate for the tiny profile to be conformant. No normalized total performance score exists
for this profile, and performanceVerdict is always `not-evaluated`.

Performance cards, once released, report per-case throughput, latency distributions, errors and resource cost with
environment/profile identity. Capacity claims additionally require predeclared service targets and confirmation at
candidate pass/fail rates. Optional relative indices need a fixed baseline and weights; failure/missing cases cannot
be removed and the remaining weights renormalized. One dimension cannot cancel a regression in a mandatory budget.

## 8. Change process and first-stage completion

Phase 1 targets a publishable, reproducible HG-Bench specification and task suite. This local package is
Phase 1a: a semantic draft with reproducible assets, reference answers, negative validator tests, execution rules
and scorecard contracts. Phase 1 remains unfinished until Phase 1b fixes and validates the performance profile
under Section 6, demonstrates independent reproduction, and prepares a versioned release candidate.
Changing semantics, cases, parameter proportions or validator behavior requires review and a new suite digest.
No public release, third-party certification or real HugeGraph pass is implied by files being present locally.

Phase 2 builds the agent-friendly controller, production executors, analysis and offline reports against the
frozen contracts. Phase 3 runs controlled HTTP/container comparisons using the benchmark and tool.
The HTTP comparison is not a prerequisite for defining the benchmark; a minimal SUT adapter and calibration
harness are sufficient for Phase 1b. Public publication follows the repository review/release process.

## References and design provenance

- [LDBC SNB](https://ldbcouncil.org/benchmarks/snb/interactive/): workloads, validation and reproducible disclosure.
- [FinBench](https://ldbcouncil.org/benchmarks/finbench/): parameter difficulty and graph-specific choke points.
- [MLPerf Inference](https://mlcommons.org/benchmarks/inference-datacenter/): scenario-specific measurement and quality conditions.
- [Issue #3346](https://github.com/apache/hugegraph/issues/3346): end-to-end scope and correct-baseline requirement.

This is a HugeGraph-specific synthetic draft, not an implementation or audited result of those external benchmarks.
