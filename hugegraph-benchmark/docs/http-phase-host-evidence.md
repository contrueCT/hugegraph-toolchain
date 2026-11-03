<!--
Licensed to the Apache Software Foundation (ASF) under one or more contributor
license agreements. See the NOTICE file distributed with this work for additional
information regarding copyright ownership. The ASF licenses this file to You
under the Apache License, Version 2.0 (the "License"); you may not use this file
except in compliance with the License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
-->

# Direct phase / host clock association consumer

`components/http/phase-host-evidence.mjs` exports the synchronous, pure
`associatePhaseHost({ run, profile, collector, external })`. It validates already
collected JSON and returns candidate phase intervals and individual gates. It
does not start a process, read a file, contact a target, attribute resource
counters, or accept a performance result.

The supported clock contract is deliberately fixed:
`linux-node22.14-cpython-monotonic-v1`. Node is the reviewed Linux x64 Node
22.14.0 official executable with libuv 1.49.2, built-in libuv, clean Node argv
and injection environment, and actual Linux process receipts. Python must report
CPython `time.monotonic_ns` using `clock_gettime(CLOCK_MONOTONIC)`. Peer and
Collector versions bind independently: reviewed 3.12.3 and 3.12.15 source
contracts are supported; another version keeps its source gate pending.

## Input and trust boundary

```text
locked run + locked profile ── direct raw validator + S/R raw validator ─┐
verified resource result ──── Collector raw reconstruction ───────────┤
predeclared expectations ──── source / binary / namespace / quality ──┤
independent runtime proof ─── IO verifier's in-memory result ──────────┤
                                                                    v
                                           candidate intervals + per-gate status
```

`run` is the original parsed run, including direct boundary clocks and the full
12-frame S/R exchange. `profile` is the original locked profile. The consumer
calls both raw validators itself; an externally asserted validation flag cannot
replace raw records. Passing the profile prevents removal of both the run S/R
block and its marker from silently downgrading an enabled profile.

`collector` has exactly `{ summary, verification }`. `verification` must be the
in-memory result of independent resource-evidence verification, with its accepted
first/last sample window. The consumer rebuilds the raw clock identity from
`summary` and checks its identity, drift and windows against that prior result.
The caller remains responsible for verifying the sample file, manifest and
original summary bytes. This pure API does not authenticate an arbitrary JSON
object submitted as a resource-verification result.

`external` accepts these optional groups; missing facts keep the corresponding
gate pending, and malformed values fail explicitly:

| Field                        | Predeclared expectation                                                                                                                                              |
| ---------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `runDigest`, `profileDigest` | Canonical `valueDigest` of the parsed objects                                                                                                                        |
| `runtimeDigest`              | SHA-256 of the future experiment's original runtime lock bytes                                                                                                       |
| `probedRuntimeDigest`        | Input runtime-lock digest referenced by the independent probe                                                                                                        |
| `runtimeProofDigest`         | Independent probe raw-lock content digest                                                                                                                            |
| `node`                       | `{ version, uv, executableSha256 }`                                                                                                                                  |
| `peer`                       | `{ imageId, version, implementation, executableSha256 }`                                                                                                             |
| `collector`                  | `{ hostBootId, version, executableSha256, componentSha256, summaryDigest }`                                                                                          |
| `sources`                    | SHA-256 by exact basename: `runner.mjs`, `direct-phase-clock.mjs`, `runner-sanity.mjs`, `clock-correlation.mjs`, `phase-clock-reference.py`, `resource-collector.py` |
| `pythonClockSources`         | Separate `peer` and `collector` values, each `{ version, pytimeSha256, timeModuleSha256 }`                                                                           |
| `quality`                    | `{ maxBoundaryWidthNs, maxRoundTripNs, maxAnchorWidthNs }`, each a decimal nanosecond string                                                                         |
| `runtimeBinding`             | `{ raw, verification }` with the actual in-memory IO verifier result; bare raw remains pending                                                                       |
| `experimentRuntime`          | `{ runtimeRaw, lockRaw }`, original JSON strings from the future experiment's frozen runtime and adjacent lock                                                       |

Canonical object digests are distinct from SHA-256 of original file bytes.
Collector `summaryDigest` is also canonical; its prior `summarySha256` belongs
to the outer byte-level verification. A proof selected using an older runtime
keeps that audit reference in `probedRuntimeDigest`; the future runtime may have
a different digest after adding source locks and the proof reference. Facts are
compared individually, without a digest self-reference requirement.

`runtimeBinding.raw` uses the strict `http-runtime-binding` contract: schema,
Linux origin, probed runtime digest, raw proof digest, immutable driver image,
Python executable/version/implementation, and Node executable/version/libuv.
An extra field, mutable image tag or malformed digest is rejected. Its facts are
compared to the actual run and the predeclared expectations.

`runtimeProofVerifierConnected` is true. Only the actual in-memory object
returned by `verifyRuntimeProvenance` in `runtime-provenance-evidence.mjs` can
close `runtimeProof`. Its private WeakSet distinguishes the returned result from
a handwritten `verification.verified=true`, copied or deserialized JSON, and a
bare normalized binding. Unverified values retain `runtime-source-unverified`
and `independently-verified-runtime-proof-missing`. The consumer checks raw
binding equality against both independently reconstructed copies, then binds the
probe's host Python executable/version/implementation to the actual Collector.

`experimentRuntime` supplies the separate current experiment source root. Its
lock byte SHA must equal `external.runtimeDigest`; the runtime byte SHA/length,
strict lock shape, plan identities, immutable image, Node executable and host
Python must agree. All six observed component source digests must match this
inventory and their external expectations. Missing source inventory stays
pending. The caller must have independently verified comparison execution's
runtimeDigest, run/profile selection, implementation and before/after source
inventories. This is a trusted controller's actual frozen inventory; it is not
cryptographic host attestation. An older probe's inventory cannot establish
these new experiment facts. Missing gates return
`hostAssociation="unverified"` and null `hostIntervals`.

Reviewed upstream source chains are in
`phase2-http-direct-clock-research-v1`（本机记录未随包分发）
and
`phase2-http-cpython-clock-research-v1`（本机记录未随包分发）.
They corroborate upstream semantics. They do not prove reproducible binary
builds or authenticate runtime-provided identity receipts.

## Causal and numerical checks

Direct raw verification binds the session, profile, run, PID/start tick, actual
Node executable and direct component. S/R verification binds both caller Node
anchors, the live peer identity and all 12 raw frames. Tokens generated by
different modules are never required to be equal. Boot, time namespace,
`time_for_children` namespace and parsed offsets must agree with Collector.
Both S/R exchanges must fit in the actual preparation/finalization intervals.
Every actual phase must be enclosed by the completed pre receipt and the post
send, and by the corresponding Node H brackets. Collector's accepted sample
window and before/after captures must enclose both complete S/R round trips.

For phase start `[s0,s1]` and end `[e0,e1]`, the result retains:

```text
outer interval:          [s0, e1]
certain inner interval:  [s1, e0]
duration interval:       [e0 - s1, e1 - s0]
```

These describe the actual phase including drain. Integer nanoseconds remain
decimal strings throughout. All start/end widths, both RTTs and both anchor
widths are compared to the three predeclared limits. There is no default limit,
post hoc threshold selection, anchor averaging or midpoint estimate.

The P/H compatibility gate uses the reviewed Node expression
`double(integer_elapsed_ns) / 1e6`. It inverts both binary64 roundings with exact
rational arithmetic and outward integer endpoints, then intersects all possible
common origins across pre, every phase boundary, and post. This is a compatibility
check only. Its origin interval never supplies a fitted host mapping. The test
suite includes an elapsed value above `2^53` nanoseconds where treating this
expression as a single rounded division would incorrectly exclude the real
origin by one nanosecond.

Gate failures preserve local observations and candidate intervals. Invalid raw
records or contradictory reconstructed records throw. Formal performance
acceptance remains false even if a future trusted adapter lets all clock gates
pass; resource attribution, JVM alignment and performance calibration are
separate acceptance work.

## Verification

`node --test tests-normative/http-phase-host-evidence.test.mjs` exercises synthetic
records, independently resealed mutations, source version separation, deletion
and false-verification attempts, namespace and coverage mismatches, thresholds,
and double rounding. It performs no Docker, HTTP or benchmark execution and
provides no live clock-association evidence.
