# Headroom002 offline engineering assessment

The public reader accepts only the original `http-driver-headroom-002` operation and the exact, separately preregistered [`headroom-engineering-3case-v1`](../presets/http/headroom-engineering-3case-v1.json) criterion. It reads existing files and invokes the fixed independent JVM parser. It performs no HTTP, Docker, SSH or systemd commands and never executes command text from input.

```sh
hgbench http calibration driver-headroom input.json \
  --out /new/report --timeout-ms 600000 --max-bytes 16777216
```

`schema http-driver-headroom-evidence-input` describes the strict public input. The references bind the original plan lock, execution spec, 529-member AA005 async source selection, criterion, assessment binding, prelaunch record and controller-start evidence. Output is create-only `input.json`, `report.json`, and `report.lock.json`. Exit 0 means a report was created; read its `status` and `limitedScopeEngineeringAccepted`.

The assessment always retains 12 physical arms and 36 case/rate/CPU/block cells. A missing, skipped or invalid arm remains unknown. Private operator status, `warmupDerived`, recorded CPU-delta conclusions and handwritten passed flags have no authority.

## Current measured result

The original headroom002 execution, terminal collection and independent assessment are complete. The sealed delivery（本机记录未随包分发） retains all 12 arms and 36 cells: 10 arms executed and two remained unexecuted under the original stop rule. The 629 reader-v2 assessment（本机记录未随包分发） verified the raw, environment, lifecycle, resource, JVM and window evidence of all 10 executed arms. The six cells belonging to unexecuted arms remain unknown, so the full-denominator `verification.valid` remains false.

The engineering result is `status=failed` and `limitedScopeEngineeringAccepted=false`. Across executed measurement phases, 72,000 planned arrivals yielded 71,583 correct responses (71,576 within the completion window and seven during drain) and 417 unsent arrivals. Warmup, P99 consistency and some higher-rate quality/pressure checks failed. At 10rps/IF4, all three cases passed request-quality and CPU/ELU checks in all four arms; warmup and P99 failures at that rate still prevent headroom or steady-state acceptance. This limited observation permits the original observer condition-cost experiment without extending qualification to all eleven cases, 80rps, IF64 or embedded mode.

The first 611 report is preserved separately: six executed arms were falsely rejected because Docker returned the same complete Mounts objects in a different order. Reader-v2 compares full objects by unique Destination, retaining rejection of missing, duplicate or changed mounts. Its independent replay completed in 8.276 seconds without rerunning HTTP. The old report and original measurement source remain unchanged; use the sealed reader-v2 result for the current assessment.

## Preregistration and process identity

```text
criterion SHA + original plan/spec/source
                  |
            assessment binding
                  | SHA
          pre-dispatch launch record
                  |
       systemd start + /proc stat raw
                  |
            original dispatch PID/start/boot
```

The binding has a relative launch filename without a launch hash. The launch record references the binding SHA, and the input separately fixes the launch SHA; this avoids a hash cycle. The launch timestamps must precede the recorded `ExecMainStartTimestampMonotonic`. The five-property original `systemctl --user show` output, PID and `/proc/<PID>/stat` startticks must match the original dispatch claim and host boot. Record the launch before starting the controller and capture controller-start evidence immediately afterward. Missing or late registration cannot pass. These are preserved producer records, with no claim of external attestation.

## Gates and limitations

The reader independently verifies original raw hashes, answers, counts, timeline, source components, actual frozen tasks, exact target/driver identity, pre/post mode, normal target stop, owned-container absence, complete collector lifecycle and JVM same-raw replay. Every declared inactive unit needs its own original seven-property check before and after each arm; adjacent arms cannot share one record. Protected d6 is observed only. A `not-found` unit proves current absence in that record and does not prove historical success.

Whole-phase Node CPU is recomputed from saved user/system microseconds and elapsed time. ELU remains source-bound recorded utilization: raw active/idle counters are unavailable. All original analyzer issues survive; known error/cancel/drop/budget failures fail, unclassified issues are unknown, and absolute CPU/ELU pressure cannot be cleared by quota normalization. Original cgroup endpoint raw counters and actual CPU quota also produce a descriptive workflow CPU delta.

The fixed 0.8 CPU/ELU limits and zero errors/drop/late apply to both phases. Correct whole-measurement P99 needs at least 1000 observations. Eighteen CPU1/2 and eighteen between-block P99 comparisons use the approved symmetric `max(1ms, 10% × min(x,y))` engineering tolerance. Warmup P50/P95 uses the last two complete warmup and first two complete measurement 10-second arrival windows, at least 50 samples per metric/window, and `max(1ms, 10% × warmup reference)`; medians combine the two window quantiles rather than pooling requests.

A passed result is limited to scalar-add, rows-4096 and string-1024k at 10/20/40 arrivals/s, IF4, the declared build and observer stack. Measurement completion windows and drain remain descriptive; there is no new 95% completion gate. Full measurement steady state is `unassessed`, window P99 stays unknown, and both `performanceAccepted` and `inferenceAccepted` always remain false. The result does not establish all-eleven headroom, 80rps, target capacity or statistical equivalence. Optional empirical false-positive certification is separate.

## Bounds and validation evidence

Combined retained plan/execution/assessment input is capped at 16GiB; individual files are bounded and symlinks rejected. `--max-bytes` caps newly written output (1MiB..8GiB). The declared overall timeout (1s..24h) includes the worker's termination and final partial receipt; three seconds for descendant cleanup and 1.2 seconds for finalization are reserved. A budget too short to start safely rejects before creating output. Failure after creation leaves a bounded unknown `partial.json` and no success lock. Canonical input/source aliases are protected from output writes.

Portable normative tests use fixed contract bytes, synthetic process/lifecycle records and arithmetic samples. They prove protocol handling and preserve the full denominator, with no claim of measured HugeGraph headroom. The separate local original-draft check records its historical unknown/unexecuted state. The real headroom002 collection and independent replay are now complete as linked above; their failed engineering result and unexecuted denominator are retained.
