/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { validGraphValue } from "./graph-validation.mjs";
import { validateTraceWriter } from "./trace-writer.mjs";
import { createReadStream } from "node:fs";
import { lstat, readFile, realpath } from "node:fs/promises";
import { join, resolve, sep } from "node:path";
import { pathToFileURL } from "node:url";
import { createInterface } from "node:readline";
import { isDeepStrictEqual } from "node:util";
import { argumentsFor, digest, entry, json, expectedCheckDigest, valueDigest } from "./common.mjs";
import { verifySuiteEvidence } from "./suite-evidence.mjs";
import { verifyProbe } from "./probe.mjs";
import { validateDirectPhaseClockRaw } from "./direct-phase-clock.mjs";

const assert = (value, message) => {
    if (!value) throw new Error(message);
};
const quantiles = (values) => {
    values.sort((a, b) => a - b);
    return {
        count: values.length,
        p50: values[Math.ceil(values.length * 0.5) - 1] ?? null,
        p95: values[Math.ceil(values.length * 0.95) - 1] ?? null,
        p99: values[Math.ceil(values.length * 0.99) - 1] ?? null,
        max: values.at(-1) ?? null,
    };
};
const outcomes = new Set([
    "ok",
    "wrong-answer",
    "invalid-json",
    "http-status",
    "unexpected-encoding",
    "invalid-gzip",
    "decoded-response-too-large",
    "timeout",
    "drain-timeout",
    "response-too-large",
    "transport-error",
    "response-error",
    "response-aborted",
    "cancelled",
    "evidence-failure",
    "runner-stopped",
    "scheduler-late",
    "concurrency-limit",
    "cancelled-before-send",
]);
const droppedOutcomes = new Set(["scheduler-late", "concurrency-limit", "cancelled-before-send"]);

function verifyTimeline(sample, phase, load) {
    const within = (value) => Number.isFinite(value) && value >= 0 && value <= phase.elapsedMs;
    assert(Number.isFinite(phase.elapsedMs) && phase.elapsedMs >= 0, "Invalid actual phase duration");
    assert(typeof sample.late === "boolean", "Invalid lateness evidence");
    if (sample.sentMs === null) {
        // Cancellation records all remaining planned arrivals immediately, including future ones.
        assert(
            within(sample.observedMs) &&
                (sample.outcome === "cancelled-before-send" || sample.observedMs >= sample.scheduledMs) &&
                sample.socketMs === null &&
                sample.endMs === null &&
                sample.late === (sample.outcome === "scheduler-late"),
            "Dropped sample timeline differs from actual phase",
        );
        return;
    }
    assert(
        within(sample.sentMs) &&
            within(sample.endMs) &&
            sample.sentMs >= sample.scheduledMs &&
            sample.endMs >= sample.sentMs &&
            (sample.socketMs === null ||
                (Number.isFinite(sample.socketMs) &&
                    sample.socketMs >= sample.sentMs &&
                    sample.socketMs <= sample.endMs)) &&
            sample.late === (load.model === "open" && sample.sentMs - sample.scheduledMs > load.maxStartDelayMs),
        "Sent sample timeline differs from actual phase",
    );
    if (sample.completeResponse)
        assert(
            Number.isFinite(sample.validationMs) &&
                sample.validationMs >= 0 &&
                sample.endMs + sample.validationMs <= phase.elapsedMs + 1e-6,
            "Response validation extends beyond actual phase",
        );
}

export async function analyze(directory) {
    const root = await realpath(directory),
        manifest = await json(join(root, "raw.lock.json"));
    assert(
        [1, 2, 3].includes(manifest.schemaVersion) && manifest.kind === "http-raw-evidence",
        "Unsupported evidence manifest",
    );
    assert(Array.isArray(manifest.files) && manifest.files.length <= 20000, "Invalid evidence file inventory");
    const partial = manifest.execution?.state === "partial";
    if (manifest.execution !== undefined) {
        assert(
            ["partial", "complete"].includes(manifest.execution.state) &&
                Number.isSafeInteger(manifest.execution.plannedRuns) &&
                manifest.execution.plannedRuns > 0 &&
                (partial ? typeof manifest.execution.reason === "string" : manifest.execution.reason === null),
            "Invalid execution completeness",
        );
        assert(
            Array.isArray(manifest.partialFiles) &&
                manifest.partialFiles.length <= 3 &&
                (partial || manifest.partialFiles.length === 0),
            "Invalid unfinished evidence inventory",
        );
    } else assert(manifest.partialFiles === undefined, "Undeclared unfinished evidence");
    const partialFiles = manifest.partialFiles ?? [];
    const paths = new Set(),
        locked = new Map();
    for (const f of [...manifest.files, ...partialFiles]) {
        assert(
            /^(profile\.json|target-evidence\.json|implementation\.json|executor\.json|(?:preflight|postcheck)\/(?:result\.json|p\d{4}\.(?:ndjson|json|request\.json))|benchmark\/(?:bundle\.lock\.json|catalog\.json|answers\.json|protocol\.json|scoring-policy\.json|calibration-plan\.json|release\.json|spec\.md|reference\.mjs)|target\/\d+\.json|(?:requests|runs)\/r\d{4}\.json|(?:runs\/r\d{4}\.json|(?:preflight|postcheck)\/p\d{4}\.json)\.identity\/(?:ready|finished)(?:-release)?\.json|traces\/r\d{4}\.ndjson)$/.test(
                f.path,
            ),
            "Unexpected evidence path",
        );
        assert(!paths.has(f.path), "Duplicate evidence path");
        paths.add(f.path);
        locked.set(f.path, f);
        const path = join(root, f.path),
            info = await lstat(path);
        assert(
            info.isFile() && (await realpath(path)).startsWith(root + sep),
            "Evidence must be a contained regular file",
        );
        assert(
            Number.isSafeInteger(f.bytes) && f.bytes >= 0 && f.bytes <= 1073741824 && info.size === f.bytes,
            "Evidence file size mismatch",
        );
        assert((await digest(path)) === f.sha256, `Evidence hash mismatch: ${f.path}`);
    }
    for (const required of ["profile.json", "target-evidence.json", "implementation.json"])
        assert(paths.has(required), `Missing locked file: ${required}`);
    const p = await json(join(root, "profile.json"));
    const implementation = await json(join(root, "implementation.json"));
    const nodeRunnerFiles = (Array.isArray(implementation.files) ? implementation.files : [])
        .filter((f) =>
            ["runner.mjs", "trace-writer.mjs", "common.mjs", "direct-phase-clock.mjs", "runner-sanity.mjs"].includes(
                f.file,
            ),
        )
        .sort((a, b) => a.file.localeCompare(b.file));
    const go = p.schemaVersion === 2 && p.executor?.engine === "go-http-v1";
    const engine = go ? await import("./go-engine.mjs") : null;
    assert(p.schemaVersion === 1 || go, "Unsupported HTTP profile version/engine");
    assert(go === (manifest.schemaVersion === 3), "Raw profile/manifest engine differs");
    if (go) {
        assert(manifest.engine === "go-http-v1" && paths.has("executor.json"), "Missing Go registration");
        const registration = await json(join(root, "executor.json"));
        engine.validateGoRegistration(registration, p);
        assert(
            registration.schemaVersion === 1 &&
                registration.kind === "http-go-executor-registration" &&
                implementation.schemaVersion === 2 &&
                implementation.engine === "go-http-v1" &&
                isDeepStrictEqual(implementation.executor, registration),
            "Go implementation registration differs",
        );
    } else assert(!paths.has("executor.json"), "Undeclared Go registration");
    assert(
        (go && p.executor.hostIdentityGate) || ![...paths].some((path) => path.includes(".json.identity/")),
        "Undeclared identity gate files",
    );
    const clock = (run) => (go ? engine.validateGoRaw(run, p) : validateDirectPhaseClockRaw(run, p));
    const target = await json(join(root, "target-evidence.json"));
    assert(
        target.schemaVersion === 1 &&
            target.kind === "operator-supplied-target-evidence" &&
            Array.isArray(target.files) &&
            target.files.length <= 32 &&
            isDeepStrictEqual(
                p.target.evidenceFiles,
                target.files.map((f) => f.path),
            ),
        "Target evidence declaration differs from profile",
    );
    assert(
        isDeepStrictEqual(
            [...paths].filter((path) => path.startsWith("target/")).sort(),
            target.files.map((_, i) => `target/${i}.json`).sort(),
        ),
        "Target attachment inventory differs",
    );
    let targetBytes = 0;
    for (const [i, file] of target.files.entries()) {
        const attachment = locked.get(`target/${i}.json`);
        assert(
            typeof file.path === "string" &&
                file.path.length > 0 &&
                /^[a-f0-9]{64}$/.test(file.sha256) &&
                Number.isSafeInteger(file.bytes) &&
                file.bytes >= 0 &&
                file.bytes <= 8 * 1024 * 1024 &&
                attachment.sha256 === file.sha256 &&
                attachment.bytes === file.bytes,
            "Target attachment identity differs",
        );
        targetBytes += file.bytes;
    }
    assert(targetBytes <= 32 * 1024 * 1024, "Target attachment budget exceeded");
    if (p.graph) {
        assert(go && !p.benchmark && p.cases.every((c) => c.requestPool?.length), "Invalid graph execution binding");
        const referenceIndex = target.files.findIndex((f) => f.sha256 === p.graph.reference.artifactSha256);
        assert(referenceIndex >= 0, "Missing original independent graph reference export");
        if (p.graph.imported) {
            const index = target.files.findIndex((f) => f.sha256 === p.graph.imported.artifactSha256);
            assert(index >= 0, "Missing original graph import snapshot");
            assert(
                isDeepStrictEqual(await json(join(root, `target/${index}.json`), 1048576), p.graph.imported.snapshot),
                "Graph import snapshot differs from frozen target attachment",
            );
        }
        const reference = await json(join(root, `target/${referenceIndex}.json`), 3145728);
        assert(
            reference.kind === "graph-query-reference" &&
                reference.origin === "independent-csv" &&
                isDeepStrictEqual(reference.dataset, p.graph.dataset),
            "Original graph reference identity differs",
        );
    } else
        assert(
            p.cases.every((c) => !c.requestPool),
            "Unbound graph parameter pool",
        );
    assert(
        Number.isSafeInteger(p.maxTraceBytes) && p.maxTraceBytes >= 4096 && p.maxTraceBytes <= 1073741824,
        "Invalid workflow trace budget",
    );
    const actualTraceBytes = [...manifest.files, ...partialFiles]
        .filter((f) => f.path.endsWith(".ndjson"))
        .reduce((sum, f) => sum + f.bytes, 0);
    assert(actualTraceBytes <= p.maxTraceBytes, "Workflow trace budget exceeded");
    if ([2, 3].includes(manifest.schemaVersion))
        assert(manifest.serviceChecks === Boolean(p.serviceChecks), "Service-check declaration differs");
    const serviceChecks = [];
    if (p.serviceChecks) {
        assert([2, 3].includes(manifest.schemaVersion), "Service checks require raw protocol v2/v3");
        for (const phase of ["preflight", "postcheck"]) {
            if (partial) {
                const probe = paths.has(`${phase}/result.json`) ? await json(join(root, phase, "result.json")) : null;
                if (!probe?.passed) {
                    serviceChecks.push({
                        phase,
                        passed: false,
                        verified: false,
                        status: probe ? "incomplete-or-failed" : "not-completed",
                        traceBytes: manifest.files
                            .filter((f) => f.path.startsWith(phase + "/") && f.path.endsWith(".ndjson"))
                            .reduce((sum, f) => sum + f.bytes, 0),
                    });
                    continue;
                }
            }
            assert(paths.has(`${phase}/result.json`), "Missing locked service check");
            const probe = await json(join(root, phase, "result.json"));
            for (const f of probe.files) assert(paths.has(`${phase}/${f.path}`), "Missing locked probe evidence");
            assert(
                isDeepStrictEqual(
                    [...paths].filter((path) => path.startsWith(`${phase}/`)).sort(),
                    [`${phase}/result.json`, ...probe.files.map((f) => `${phase}/${f.path}`)].sort(),
                ),
                "Probe file inventory differs",
            );
            serviceChecks.push(await verifyProbe(root, phase, p, locked));
            for (const c of probe.cases) {
                const observed = await json(join(root, phase, `${c.id}.json`)),
                    sample = JSON.parse(await readFile(join(root, phase, `${c.id}.ndjson`), "utf8"));
                assert(observed.phases[0].durationMs === p.requestTimeoutMs, "Probe duration differs from profile");
                clock(observed);
                verifyTimeline(sample, observed.phases[0], { model: "closed" });
            }
        }
    } else assert(![...paths].some((path) => /^(preflight|postcheck)\//.test(path)), "Undeclared service checks");
    const benchmark = await verifySuiteEvidence(root, p, locked);
    if (benchmark) {
        const lock = await json(join(root, "benchmark/bundle.lock.json"));
        for (const name of ["bundle.lock.json", ...lock.files.map((f) => f.path)])
            assert(paths.has(`benchmark/${name}`), "Missing locked benchmark file");
    } else assert(![...paths].some((path) => path.startsWith("benchmark/")), "Unbound benchmark evidence");
    const expectedRuns = p.cases.length * p.loads.length * p.repeats;
    if (manifest.execution) assert(manifest.execution.plannedRuns === expectedRuns, "Changed planned run denominator");
    assert(
        Array.isArray(manifest.runs) &&
            (partial ? manifest.runs.length <= expectedRuns : manifest.runs.length === expectedRuns) &&
            expectedRuns <= 5120,
        "Incomplete run inventory",
    );
    const nextId = `r${String(manifest.runs.length + 1).padStart(4, "0")}`;
    assert(
        partialFiles.every((f) => new RegExp(`^(requests|runs|traces)/${nextId}\\.(json|ndjson)$`).test(f.path)),
        "Unfinished files do not belong to the next planned run",
    );
    assert(
        manifest.files.filter((f) => /^(requests|runs|traces)\//.test(f.path)).length ===
            manifest.runs.length * (go && p.executor.hostIdentityGate ? 7 : 3),
        "Unexpected run evidence inventory",
    );
    let usedTraceBytes = serviceChecks.find((s) => s.phase === "preflight")?.traceBytes ?? 0;
    const rows = [],
        identities = new Set();
    for (const [index, id] of manifest.runs.entries()) {
        assert(id === `r${String(index + 1).padStart(4, "0")}`, "Invalid run order or identity");
        for (const path of [`requests/${id}.json`, `runs/${id}.json`, `traces/${id}.ndjson`])
            assert(paths.has(path), `Missing locked run evidence: ${path}`);
        const request = await json(join(root, `requests/${id}.json`));
        const run = await json(join(root, `runs/${id}.json`));
        if (go && p.executor.hostIdentityGate) {
            for (const name of engine.goIdentityFiles(`runs/${id}.json`))
                assert(paths.has(name), "Missing locked identity gate evidence");
            await engine.verifyGoIdentityGates(join(root, `runs/${id}.json`), run, p);
        }
        const phaseClock = clock(run);
        if (go) assert(request.schemaVersion === 2, "Go request protocol differs");
        assert(isDeepStrictEqual(request.profile, p), "Run profile differs from locked profile");
        assert(request.remainingTraceBytes === p.maxTraceBytes - usedTraceBytes, "Run remaining trace budget differs");
        const traceBytes = locked.get(`traces/${id}.ndjson`).bytes;
        usedTraceBytes += traceBytes;
        assert(usedTraceBytes <= p.maxTraceBytes, "Workflow trace budget exceeded");
        assert(
            isDeepStrictEqual(
                p.cases.find((c) => c.id === run.caseId),
                request.case,
            ),
            "Case identity differs",
        );
        assert(
            isDeepStrictEqual(
                p.loads.find((l) => l.id === run.loadId),
                request.load,
            ),
            "Load identity differs",
        );
        assert(
            request.id === id &&
                run.id === id &&
                request.repeat === run.repeat &&
                Number.isInteger(run.repeat) &&
                run.repeat >= 0 &&
                run.repeat < p.repeats,
            "Run identity differs",
        );
        const identity = JSON.stringify([run.caseId, run.loadId, run.repeat]);
        assert(!identities.has(identity), "Duplicate case/load/repeat");
        identities.add(identity);
        const names = p.warmupMs ? ["warmup", "measurement"] : ["measurement"];
        assert(
            isDeepStrictEqual(
                run.phases.map((s) => s.name),
                names,
            ),
            "Missing phase or cancelled run",
        );
        const phases = new Map(
            names.map((name) => [
                name,
                {
                    saved: run.phases.find((s) => s.name === name),
                    ids: new Set(),
                    parameters: new Map(),
                    strata: {},
                    sent: 0,
                    completed: 0,
                    failed: 0,
                    dropped: 0,
                    late: 0,
                    counts: {},
                    statusCounts: {},
                    scheduling: { phaseBoundary: 0, maxStartDelay: 0, unresolved: 0 },
                    examples: [],
                    success: [],
                    scheduledLatency: [],
                    responseLatency: [],
                    failureLatency: [],
                    delay: [],
                    validation: [],
                    decode: [],
                    wireSizes: [],
                    decodedSizes: [],
                    encodings: {},
                    decodedBytes: 0,
                    bytes: 0,
                    completedInWindow: 0,
                    successInWindow: 0,
                },
            ]),
        );
        const lines = createInterface({
            input: createReadStream(join(root, `traces/${id}.ndjson`)),
            crlfDelay: Infinity,
        });
        try {
            for await (const line of lines) {
                assert(line.length > 0 && line.length < 65536, "Invalid trace line length");
                const s = JSON.parse(line),
                    phase = phases.get(s.phase);
                const duration = s.phase === "warmup" ? p.warmupMs : p.durationMs;
                if (go) assert(s.schemaVersion === 2 && s.engine === "go-http-v1", "Go trace protocol differs");
                assert(
                    phase && Number.isInteger(s.sequence) && s.sequence >= 0 && s.sequence < p.maxRequestsPerPhase,
                    "Invalid sample identity",
                );
                assert(
                    !phase.ids.has(s.sequence) && (outcomes.has(s.outcome) || (go && s.outcome === "protocol-error")),
                    "Duplicate sample or unknown outcome",
                );
                assert(
                    Number.isFinite(s.scheduledMs) && s.scheduledMs >= 0 && s.scheduledMs < duration,
                    "Scheduled time is outside phase",
                );
                phase.ids.add(s.sequence);
                phase.counts[s.outcome] = (phase.counts[s.outcome] ?? 0) + 1;
                if (s.late) phase.late++;
                if (request.load.model === "open")
                    assert(
                        Math.abs(s.scheduledMs - (s.sequence * 1000) / request.load.ratePerSecond) < 1e-6,
                        "Arrival schedule differs",
                    );
                assert(
                    typeof s.completeResponse === "boolean" && Number.isSafeInteger(s.bytes) && s.bytes >= 0,
                    "Invalid response evidence",
                );
                verifyTimeline(s, phase.saved, request.load);
                if (s.status !== null && Number.isInteger(s.status))
                    phase.statusCounts[s.status] = (phase.statusCounts[s.status] ?? 0) + 1;
                if (s.outcome === "scheduler-late") {
                    const boundary = s.observedMs >= duration;
                    const delayed = s.observedMs - s.scheduledMs > request.load.maxStartDelayMs;
                    if (boundary) phase.scheduling.phaseBoundary++;
                    if (delayed) phase.scheduling.maxStartDelay++;
                    if (!boundary && !delayed) phase.scheduling.unresolved++;
                }
                if (s.outcome !== "ok" && phase.examples.length < 8)
                    phase.examples.push({
                        trace: `traces/${id}.ndjson`,
                        sequence: s.sequence,
                        phase: s.phase,
                        outcome: s.outcome,
                        status: s.status,
                        scheduledMs: s.scheduledMs,
                        observedMs: s.observedMs ?? null,
                    });
                if (s.sentMs === null) {
                    assert(
                        droppedOutcomes.has(s.outcome) && s.endMs === null && !s.completeResponse,
                        "Dropped request has response evidence",
                    );
                    phase.dropped++;
                    continue;
                }
                assert(
                    !droppedOutcomes.has(s.outcome) &&
                        Number.isFinite(s.sentMs) &&
                        s.sentMs >= s.scheduledMs &&
                        Number.isFinite(s.endMs) &&
                        s.endMs >= s.sentMs,
                    "Invalid sent/completed timeline",
                );
                if (request.case.requestPool) {
                    const index = s.sequence % request.case.requestPool.length;
                    assert(s.parameterIndex === index, "Graph parameter schedule differs");
                    phase.parameters.set(index, (phase.parameters.get(index) ?? 0) + 1);
                    const stratum = request.case.requestPool[index].stratum;
                    phase.strata[stratum] = (phase.strata[stratum] ?? 0) + 1;
                } else
                    assert(s.parameterIndex === undefined && s.graphValue === undefined, "Unexpected graph evidence");
                phase.sent++;
                phase.completed++;
                phase.bytes += s.bytes;
                phase.delay.push(s.sentMs - s.scheduledMs);
                if (s.completeResponse) {
                    assert(
                        /^[a-f0-9]{64}$/.test(s.bodySha256) && Number.isFinite(s.validationMs) && s.validationMs >= 0,
                        "Missing complete response validation evidence",
                    );
                    phase.responseLatency.push(s.endMs - s.sentMs);
                    phase.validation.push(s.validationMs);
                    if (s.decodeMs !== null) {
                        assert(
                            Number.isFinite(s.decodeMs) && s.decodeMs >= 0 && s.decodeMs <= s.validationMs,
                            "Invalid decoding duration",
                        );
                        phase.decode.push(s.decodeMs);
                    }
                }
                if (s.endMs <= duration) phase.completedInWindow++;
                if (s.outcome === "ok") {
                    assert(
                        Number.isSafeInteger(s.decodedBytes) &&
                            s.decodedBytes >= 0 &&
                            s.decodedBytes <= p.maxResponseBytes,
                        "Invalid decoded response size",
                    );
                    phase.decodedBytes += s.decodedBytes;
                    assert(
                        s.responseEncoding === "identity" ||
                            (p.responseEncoding === "gzip" && s.responseEncoding === "gzip"),
                        "Invalid successful response encoding",
                    );
                    phase.encodings[s.responseEncoding] = (phase.encodings[s.responseEncoding] ?? 0) + 1;
                    phase.wireSizes.push(s.bytes);
                    phase.decodedSizes.push(s.decodedBytes);
                    assert(
                        s.completeResponse &&
                            s.status ===
                                (request.case.requestPool?.[s.parameterIndex]?.expectedStatus ??
                                    request.case.expectedStatus) &&
                            (request.case.requestPool
                                ? Array.isArray(s.checkDigests) &&
                                  !s.checkDigests.length &&
                                  validGraphValue(s.graphValue, request.case.requestPool[s.parameterIndex].rule)
                                : isDeepStrictEqual(
                                      s.checkDigests,
                                      request.case.checks.map((c) => ({
                                          present: true,
                                          sha256: expectedCheckDigest(c),
                                      })),
                                  )),
                        "Successful sample lacks matching answer digests",
                    );
                    phase.success.push(s.endMs - s.sentMs);
                    phase.scheduledLatency.push(s.endMs - s.scheduledMs);
                    if (s.endMs <= duration) phase.successInWindow++;
                } else {
                    phase.failed++;
                    phase.failureLatency.push(s.endMs - s.sentMs);
                }
            }
        } finally {
            lines.close();
        }
        const issues = [];
        for (const saved of run.phases) {
            const actual = phases.get(saved.name);
            assert(
                saved.durationMs === (saved.name === "warmup" ? p.warmupMs : p.durationMs),
                "Phase duration differs from profile",
            );
            assert(
                Number.isFinite(saved.elapsedMs) &&
                    (saved.elapsedMs >= saved.durationMs || saved.budgetExhausted || saved.cancelled),
                "Phase stopped before its measurement window ended",
            );
            assert(
                actual.ids.size === saved.scheduled &&
                    actual.ids.size === actual.sent + actual.dropped &&
                    actual.completed === actual.sent &&
                    isDeepStrictEqual(actual.counts, saved.counts),
                "Sample count conservation failed",
            );
            for (const key of ["sent", "completed", "failed", "dropped", "late"])
                assert(actual[key] === saved[key], `Run/sample ${key} mismatch`);
            for (let seq = 0; seq < saved.scheduled; seq++) assert(actual.ids.has(seq), "Missing sample sequence");
            if (request.load.model === "open")
                assert(
                    saved.scheduled === Math.ceil((saved.durationMs * request.load.ratePerSecond) / 1000),
                    "Missing planned arrivals",
                );
            if (saved.failed) issues.push(`${saved.name}:request-errors-or-wrong-answers`);
            if (saved.dropped) issues.push(`${saved.name}:dropped-arrivals`);
            if (saved.budgetExhausted) issues.push(`${saved.name}:request-budget-exhausted`);
            if (saved.cancelled) issues.push(`${saved.name}:cancelled`);
            if (Object.keys(actual.encodings).length > 1) issues.push(`${saved.name}:response-encoding-varies`);
            if (go) issues.push(...engine.goResourceIssues(saved, p));
            else if (saved.eventLoopUtilization > 0.8 || saved.averageCpuCores > 0.8)
                issues.push(`${saved.name}:driver-pressure-observed`);
        }
        assert(run.traceBytes === traceBytes, "Run trace byte accounting differs");
        const traceWriter = go ? engine.validateGoWriter(run, traceBytes, p) : validateTraceWriter(run, traceBytes);
        const m = phases.get("measurement"),
            phase = run.phases.find((s) => s.name === "measurement");
        if (!m.success.length) issues.push("measurement:no-correct-results");
        rows.push({
            id,
            caseId: run.caseId,
            loadId: run.loadId,
            repeat: run.repeat,
            model: request.load.model,
            ratePerSecond: request.load.ratePerSecond ?? null,
            concurrency: request.load.concurrency ?? request.load.maxInFlight,
            scheduled: phase.scheduled,
            sent: m.sent,
            completed: m.completed,
            dropped: m.dropped,
            failed: m.failed,
            counts: m.counts,
            ...(request.case.requestPool
                ? {
                      parameters: {
                          selection: "sequence-modulo-pool-v1",
                          poolSize: request.case.requestPool.length,
                          poolSha256: valueDigest(request.case.requestPool),
                          uniqueSent: m.parameters.size,
                          sentCoverage: m.parameters.size / request.case.requestPool.length,
                          visitsMin:
                              m.parameters.size === request.case.requestPool.length
                                  ? Math.min(...m.parameters.values())
                                  : 0,
                          visitsMax: Math.max(0, ...m.parameters.values()),
                          strata: m.strata,
                      },
                  }
                : {}),
            correct: m.success.length,
            late: m.late,
            bytes: m.bytes,
            decodedCorrectBytes: m.decodedBytes,
            responseEncodings: m.encodings,
            wireBodyBytes: quantiles(m.wireSizes),
            decodedBodyBytes: quantiles(m.decodedSizes),
            durationMs: p.durationMs,
            elapsedWithDrainMs: phase.elapsedMs,
            phaseClock,
            offeredPerSecond: phase.scheduled / (p.durationMs / 1000),
            admittedPerSecond: m.sent / (p.durationMs / 1000),
            correctCompletedInWindowPerSecond: m.successInWindow / (p.durationMs / 1000),
            correctCohortPerSecond: m.success.length / (p.durationMs / 1000),
            correctEndToEndMs: quantiles(m.scheduledLatency),
            correctResponseMs: quantiles(m.success),
            allCompleteResponseMs: quantiles(m.responseLatency),
            failedDurationMs: quantiles(m.failureLatency),
            schedulerDelayMs: quantiles(m.delay),
            validationMs: quantiles(m.validation),
            decodeMs: quantiles(m.decode),
            driver: {
                ...(go ? { engine: "go-http-v1", resources: phase.resources } : {}),
                averageCpuCores: phase.averageCpuCores,
                eventLoopUtilization: phase.eventLoopUtilization,
                eventLoopDelayMs: phase.eventLoopDelayMs,
                maxRssKiB: phase.maxRssKiB,
                evidenceWriteMs: run.evidenceWriteMs,
                ...(traceWriter ? { traceWriter } : {}),
            },
            issues,
            diagnostics: {
                runnerIdentity: {
                    engine: run.engine ?? "node-http",
                    node: run.client?.node ?? null,
                    nodeRunnerSourceSha256: !go && nodeRunnerFiles.length === 5 ? valueDigest(nodeRunnerFiles) : null,
                    goVersion: run.client?.goVersion ?? null,
                    binarySha256: run.client?.binarySha256 ?? null,
                    http: run.client?.http ?? null,
                },
                startedAt: run.startedAt ?? null,
                finishedAt: run.finishedAt ?? null,
                phases: [...phases].map(([name, v]) => ({
                    name,
                    statusCounts: v.statusCounts,
                    scheduled: v.ids.size,
                    sent: v.sent,
                    correct: v.success.length,
                    failed: v.failed,
                    dropped: v.dropped,
                    late: v.late,
                    outcomes: v.counts,
                    scheduling: v.scheduling,
                    examples: v.examples,
                    resourceSignals: {
                        averageCpuCores: v.saved.averageCpuCores ?? null,
                        cpuFractionOfDeclaredQuota: v.saved.resources?.cpuFractionOfDeclaredQuota ?? null,
                        throttledUsec: v.saved.resources?.throttledUsec ?? null,
                        maxRssKiB: v.saved.maxRssKiB ?? null,
                        declaredMaxRssBytes: p.executor?.maxRssBytes ?? null,
                    },
                })),
            },
        });
    }
    usedTraceBytes += serviceChecks.find((s) => s.phase === "postcheck")?.traceBytes ?? 0;
    usedTraceBytes += partialFiles.filter((f) => f.path.endsWith(".ndjson")).reduce((sum, f) => sum + f.bytes, 0);
    assert(usedTraceBytes === actualTraceBytes, "Workflow trace byte accounting differs");
    const repeats = [];
    for (const item of p.cases)
        for (const load of p.loads) {
            const group = rows.filter((r) => r.caseId === item.id && r.loadId === load.id);
            const values = group.map((r) => r.correctResponseMs.p99).filter((v) => v !== null);
            repeats.push({
                caseId: item.id,
                loadId: load.id,
                independentRuns: group.length,
                p99RangeMs: values.length ? [Math.min(...values), Math.max(...values)] : null,
                interpretation: "Observed run spread; no confidence interval, A/A acceptance or A/B inference",
            });
        }
    return {
        schemaVersion: go ? 2 : 1,
        ...(go ? { engine: "go-http-v1" } : {}),
        kind: "http-descriptive-analysis",
        rawManifestSha256: await digest(join(root, "raw.lock.json")),
        name: p.name,
        target: p.target,
        profile: p,
        serviceChecks,
        ...(benchmark ? { benchmark } : {}),
        runs: rows,
        repeats,
        ...(manifest.execution
            ? {
                  execution: {
                      ...manifest.execution,
                      verifiedRuns: rows.length,
                      unfinishedFiles: partialFiles,
                      missingRuns: Array.from({ length: p.repeats }, (_, repeat) =>
                          p.cases.flatMap((c) => p.loads.map((l) => ({ caseId: c.id, loadId: l.id, repeat }))),
                      )
                          .flat()
                          .slice(rows.length),
                  },
              }
            : {}),
        verdict: partial ? "partial" : rows.some((r) => r.issues.length) ? "quality-issues" : "descriptive-only",
        performanceAccepted: false,
        gates: {
            evidenceIntegrity: "passed",
            sampleAccounting: partial ? "partial" : "passed",
            correctness: rows.some(
                (r) => r.failed || !r.correct || r.issues.includes("warmup:request-errors-or-wrong-answers"),
            )
                ? "failed"
                : partial
                  ? "unknown"
                  : "passed",
            driverHeadroom: "requires-independent-calibration",
            environment: "requires-measurement-window-evidence",
            repeatability: "requires-A/A-calibration",
            comparability: "not-assessed",
        },
        timing: "Response = request API invocation to complete body, includes socket/network wait. End-to-end = planned arrival to complete body. Validation and trace writes excluded but consume driver CPU. Timeout/failure durations kept separate. Quantiles use nearest rank.",
        limits: [
            "JSON profiles use safe integer number semantics; large numeric graph IDs need a lossless adapter",
            "One local driver, sequential load runs; shared-host resources and task exit are not performance acceptance",
            "Latency distributions describe admitted requests; dropped arrivals remain explicit and have no invented latency",
            "Warmup uses a fixed duration; steady state is not automatically established",
            "No variant switching, paired effect intervals, sustained resource collector or capacity inference yet",
        ],
    };
}

export async function report(directory, output) {
    const result = await analyze(directory);
    const { buildView, writeBundle, singleReport } = await import("./report-view.mjs");
    const view = await buildView(
        [{ id: "run", group: "run", label: result.target.label || result.name, analysis: result }],
        { title: result.name, baseline: "run" },
    );
    await writeBundle(output, view, singleReport(result, view));
    return result;
}
if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href)
    await entry(async () => {
        const args = argumentsFor(["--input", "--output"]);
        const result = await report(args["--input"], args["--output"]);
        process.stdout.write(
            JSON.stringify({ verdict: result.verdict, performanceAccepted: false, runs: result.runs.length }) + "\n",
        );
    }, "HTTP analyzer v1: --input raw-evidence-directory --output NEW-report-directory\nOffline hash/accounting verification; regenerates JSON and interactive HTML. No target calls, no A/B claims.");
