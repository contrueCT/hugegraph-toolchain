# Independent capacity confirmation reports

The confirmation report uses the existing bounded capacity evidence consumer and paired evidence
verifier. It sends no requests and starts no Docker workload. Supply the frozen confirmation plan and
the execution set produced by the existing capacity controller:

```text
hgbench http capacity confirmation report execution-set.json \
  --plan confirmation-plan --out NEW \
  --timeout-ms 600000 --max-bytes 268435456
```

The fixed component has the same interface under
`node components/http/capacity-evidence.mjs report-confirmation`, using `--input` for the execution
set. `reportCapacityConfirmation({planDirectory,inputFile,outputDirectory,timeoutMs,maxBytes,signal?})`
is the library entry point. Its reply retains the ordinary capacity report shape; `points` and
`validPoints` count physical comparison conditions, from zero to 66.

```text
frozen confirmation plan
         |
independently replay original exploration raw + report
         |
verify each new comparison's existing sealed evidence
         |
bind new/original executions, raw identities and terminal windows
         |
all 11 questions x mode + every selected candidate/neighbor
```

Each same-mode A/A condition contributes all eight new arms. A forward/embedded comparison contributes
four new arms per actual mode. Throughput is the median of the eligible arms' correct completions
inside the measurement window. Latency descriptions retain each arm's quantiles; P99 requires at
least 1,000 observations per arm. Modes and exploration measurements are never pooled. Any missing,
incorrect, invalid or misassigned arm prevents the complete physical condition from qualifying.
Diagnostic measurements and all missing slots remain visible.

The highest eligible new condition may be a selected immediate neighbor. `confirmedHighest` denotes
descriptive independent confirmation of observed correct throughput. It remains null when new evidence
or independence is unavailable; the original exploration value never substitutes for a failed new
condition. Zero selected conditions still produce every question/mode row. Driver headroom, warmup
stability, SLO capacity, engineering acceptance and statistical certification are not granted by this
report. `engineeringAccepted`, `statisticsCertified` and `performanceAccepted` remain false.

Original identities come only from the current independent source replay and the sealed controller
headers it verified. The consumer rejects reused original execution directories, execution IDs, run
IDs and raw manifests. It checks that new terminal windows follow all original dispatched executions
in the same host boot's monotonic domain; touching endpoints are allowed. Cross-boot comparisons,
missing original proofs or terminal windows keep independence unknown. Current confirmation
duplicates and overlapping/reversed execution windows retain the ordinary capacity checks. This
binding assumes a trusted evidence collector and does not provide cryptographic runtime attestation.

`--timeout-ms` is one absolute computation deadline covering original replay, new-arm verification and
report writing. The work-byte cap reserves the complete source review's declared `maxBytes` plus an
8 MiB verification receipt before report output; insufficient budgets fail without creating output.
Temporary replay output is included in this reservation. The original raw analyzers retain their
own input limits. The confirmation verifier has a seven-second bounded cleanup grace so the inner
source report can reap its detached verifier group within three seconds. The public CLI retains the
report owner for ten seconds during cleanup; its supervision and cleanup can add at most twenty
seconds beyond the requested computation deadline. These intervals do not renew analysis time.

Outputs are create-only `report.json`, offline `report.html`, independently reconstructed point
evidence and an exact `evidence.lock.json`. Canonical path separation also protects original source
execution directories, including currently missing references and symbolic-link aliases. Failed
output has no valid final lock. Source report/plan/header locks are rechecked before sealing; temporary
verification directories are removed on success, cancellation and failure. The original report and
replay tool inventories remain distinct audit identities.

Local checks cover synthetic AA8/AB4 arithmetic and bad-arm/identity/time counterexamples, plus actual
eleven-question source replay with no eligible execution. The real nested cancellation/timeout tests
stop an observed owned verifier group and its Node grandchild, then verify process disappearance and
temporary-directory removal. A public CLI SIGTERM test exercises the additional outer component
owner. These checks establish the offline protocol and cleanup behavior. They do not establish a
successful live capacity confirmation; eligible new runtime measurements are still required.
