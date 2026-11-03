/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import { constants, createReadStream } from "node:fs";
import { open, realpath, mkdir, writeFile } from "node:fs/promises";
import { createHash } from "node:crypto";
import { basename, dirname, join, resolve } from "node:path";
import { createInterface } from "node:readline";
import { fileURLToPath, pathToFileURL } from "node:url";
import { analyze } from "./analyzer.mjs";
import { argumentsFor, bytesDigest, entry } from "./common.mjs";

const MiB = 1024 * 1024;
const LIMITS = Object.freeze({
    jsonBytes: 8 * MiB,
    inputBytes: 512 * MiB,
    traceBytes: 256 * MiB,
    records: 500000,
    windows: 20000,
    reportBytes: 32 * MiB,
    milliseconds: 300000,
});
const COMPONENT = fileURLToPath(import.meta.url);
const COUNTERS = [
    "scheduledArrivals",
    "observedArrivals",
    "sent",
    "notSent",
    "finished",
    "correct",
    "sentErrors",
    "timeouts",
    "drainTimeouts",
    "completeResponses",
    "incompleteResponses",
    "lateSent",
    "lateNotSent",
];
const METRICS = ["correctResponseMs", "correctEndToEndMs", "allCompleteResponseMs", "failedDurationMs"];
const DROPPED = new Set(["scheduler-late", "concurrency-limit", "cancelled-before-send"]);
function need(value, message) {
    if (!value) throw new Error(`HTTP_COMPLETION_WINDOWS: ${message}`);
}
function policyFor(options) {
    need(
        options && Object.keys(options).sort().join(",") === "minimumLatencyObservations,windowMs",
        "explicit window/sample options required",
    );
    const { windowMs, minimumLatencyObservations } = options;
    need(Number.isSafeInteger(windowMs) && windowMs >= 1 && windowMs <= 300000, "windowMs must be 1..300000");
    need(
        Number.isSafeInteger(minimumLatencyObservations) &&
            minimumLatencyObservations >= 1 &&
            minimumLatencyObservations <= 100000,
        "minimumLatencyObservations must be 1..100000",
    );
    return { windowMs, minimumLatencyObservations, minimumP99Observations: Math.max(1000, minimumLatencyObservations) };
}
function bucket(startMs, endMs, rightInclusive, kind) {
    return {
        kind,
        startMs,
        endMs,
        rightInclusive,
        counts: Object.fromEntries(COUNTERS.map((k) => [k, 0])),
        finishedOutcomes: {},
        notSentOutcomes: {},
        metrics: Object.fromEntries(METRICS.map((k) => [k, []])),
    };
}
function quantiles(values, policy) {
    values.sort((a, b) => a - b);
    const rank = (q) => values[Math.ceil(values.length * q) - 1] ?? null;
    const enough = values.length >= policy.minimumLatencyObservations,
        p99Enough = values.length >= policy.minimumP99Observations;
    return {
        count: values.length,
        sampleEligible: enough,
        p99SampleEligible: p99Enough,
        status: enough ? "descriptive" : "unknown",
        p99Status: p99Enough ? "descriptive" : "unknown",
        p50: enough ? rank(0.5) : null,
        p95: enough ? rank(0.95) : null,
        p99: p99Enough ? rank(0.99) : null,
        observedNearestRank: { p50: rank(0.5), p95: rank(0.95), p99: rank(0.99), max: values.at(-1) ?? null },
    };
}

/** Pure completion cohorts; accepts only complete terminal records, never invents a response for unsent work. */
export function completionWindowsForSamples(samples, { durationMs, elapsedMs, windowMs, minimumLatencyObservations }) {
    const policy = policyFor({ windowMs, minimumLatencyObservations });
    need(
        Number.isSafeInteger(durationMs) &&
            durationMs > 0 &&
            durationMs <= 300000 &&
            Number.isFinite(elapsedMs) &&
            elapsedMs >= 0,
        "invalid measurement duration",
    );
    need(
        samples.length <= LIMITS.records && Math.ceil(durationMs / windowMs) + 1 <= LIMITS.windows,
        "record/window budget exceeded",
    );
    const windows = Array.from({ length: Math.ceil(durationMs / windowMs) }, (_, i) =>
        bucket(i * windowMs, Math.min(durationMs, (i + 1) * windowMs), (i + 1) * windowMs >= durationMs, "measurement"),
    );
    const drain = bucket(durationMs, Math.max(durationMs, elapsedMs), true, "drain"),
        all = [...windows, drain],
        ids = new Set();
    function at(time) {
        need(
            Number.isFinite(time) && time >= 0 && time <= Math.max(durationMs, elapsedMs),
            "event outside observation or planned interval",
        );
        return time > durationMs ? drain : windows[Math.min(Math.floor(time / windowMs), windows.length - 1)];
    }
    for (const sample of samples) {
        need(
            sample.phase === "measurement" &&
                Number.isSafeInteger(sample.sequence) &&
                sample.sequence >= 0 &&
                !ids.has(sample.sequence),
            "duplicate/invalid measurement sequence",
        );
        ids.add(sample.sequence);
        need(
            Number.isFinite(sample.scheduledMs) && sample.scheduledMs >= 0 && sample.scheduledMs < durationMs,
            "invalid scheduledMs",
        );
        at(sample.scheduledMs).counts.scheduledArrivals++;
        if (sample.sentMs === null) {
            need(
                sample.endMs === null &&
                    !sample.completeResponse &&
                    DROPPED.has(sample.outcome) &&
                    sample.observedMs <= elapsedMs,
                "unsent request has terminal response or missing observation",
            );
            const b = at(sample.observedMs);
            b.counts.observedArrivals++;
            b.counts.notSent++;
            if (sample.late) b.counts.lateNotSent++;
            b.notSentOutcomes[sample.outcome] = (b.notSentOutcomes[sample.outcome] ?? 0) + 1;
            continue;
        }
        need(
            Number.isFinite(sample.sentMs) &&
                sample.sentMs >= sample.scheduledMs &&
                Number.isFinite(sample.endMs) &&
                sample.endMs >= sample.sentMs &&
                sample.endMs <= elapsedMs &&
                !DROPPED.has(sample.outcome),
            "unfinished/invalid sent request",
        );
        const sent = at(sample.sentMs),
            finished = at(sample.endMs);
        sent.counts.observedArrivals++;
        sent.counts.sent++;
        if (sample.late) sent.counts.lateSent++;
        const c = finished.counts;
        c.finished++;
        finished.finishedOutcomes[sample.outcome] = (finished.finishedOutcomes[sample.outcome] ?? 0) + 1;
        if (sample.completeResponse) {
            c.completeResponses++;
            finished.metrics.allCompleteResponseMs.push(sample.endMs - sample.sentMs);
        } else c.incompleteResponses++;
        if (sample.outcome === "ok") {
            need(sample.completeResponse, "successful request has no complete response");
            c.correct++;
            finished.metrics.correctResponseMs.push(sample.endMs - sample.sentMs);
            finished.metrics.correctEndToEndMs.push(sample.endMs - sample.scheduledMs);
        } else {
            c.sentErrors++;
            finished.metrics.failedDurationMs.push(sample.endMs - sample.sentMs);
        }
        if (sample.outcome === "timeout") c.timeouts++;
        if (sample.outcome === "drain-timeout") c.drainTimeouts++;
    }
    for (let i = 0; i < ids.size; i++) need(ids.has(i), "missing measurement sequence");
    let backlog = 0;
    for (const b of all) {
        b.backlogStart = backlog;
        backlog += b.counts.sent - b.counts.finished;
        need(
            backlog >= 0 &&
                b.counts.observedArrivals === b.counts.sent + b.counts.notSent &&
                b.counts.finished === b.counts.correct + b.counts.sentErrors &&
                b.counts.finished === b.counts.completeResponses + b.counts.incompleteResponses,
            "window count/backlog conservation failed",
        );
        b.backlogEnd = backlog;
        b.fullyObserved = b.endMs <= elapsedMs;
        b.correctCompletedPerSecond =
            b.fullyObserved && b.endMs > b.startMs ? b.counts.correct / ((b.endMs - b.startMs) / 1000) : null;
        b.metrics = Object.fromEntries(METRICS.map((key) => [key, quantiles(b.metrics[key], policy)]));
    }
    need(backlog === 0, "unfinished requests after drain");
    const totals = Object.fromEntries(COUNTERS.map((key) => [key, all.reduce((n, b) => n + b.counts[key], 0)]));
    need(
        totals.scheduledArrivals === samples.length &&
            totals.observedArrivals === samples.length &&
            totals.sent === totals.finished &&
            totals.scheduledArrivals === totals.sent + totals.notSent,
        "whole-run count conservation failed",
    );
    return {
        durationMs,
        elapsedWithDrainMs: elapsedMs,
        phaseEndedEarly: elapsedMs < durationMs,
        policy,
        totals,
        windows,
        drain,
        correctInMeasurement: windows.reduce((n, b) => n + b.counts.correct, 0),
        correctCompletedInWindowPerSecond: windows.reduce((n, b) => n + b.counts.correct, 0) / (durationMs / 1000),
    };
}

async function bounded(path, cap, member) {
    const fd = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
    try {
        const before = await fd.stat();
        need(
            before.isFile() && before.size <= cap && (!member || before.size === member.bytes),
            "bounded regular input required",
        );
        const raw = Buffer.alloc(before.size + 1);
        let size = 0;
        while (size < raw.length) {
            const part = await fd.read(raw, size, raw.length - size, size);
            if (!part.bytesRead) break;
            size += part.bytesRead;
        }
        const after = await fd.stat();
        need(
            size === before.size &&
                after.size === before.size &&
                after.mtimeMs === before.mtimeMs &&
                after.ctimeMs === before.ctimeMs,
            "input changed while reading",
        );
        const result = raw.subarray(0, size);
        if (member) need(bytesDigest(result) === member.sha256, "locked member changed");
        return result;
    } finally {
        await fd.close();
    }
}
async function measurementSamples(root, path, member, check, counter) {
    need(member && member.bytes <= LIMITS.traceBytes, "missing/oversized trace");
    const fd = await open(join(root, path), constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
    const before = await fd.stat();
    if (!before.isFile() || before.size !== member.bytes) {
        await fd.close();
        need(false, "trace file differs");
    }
    const stream = fd.createReadStream(),
        hash = createHash("sha256"),
        samples = [];
    let size = 0;
    stream.on("data", (chunk) => {
        size += chunk.length;
        hash.update(chunk);
        if (size > member.bytes) stream.destroy(new Error("trace grew"));
    });
    const lines = createInterface({ input: stream, crlfDelay: Infinity });
    try {
        for await (const line of lines) {
            check();
            need(
                line.length > 0 && line.length < 65536 && ++counter.records <= LIMITS.records,
                "trace record budget exceeded",
            );
            const sample = JSON.parse(line);
            if (sample.phase === "measurement") samples.push(sample);
        }
        need(size === member.bytes && hash.digest("hex") === member.sha256, "trace changed during reconstruction");
    } finally {
        lines.close();
        stream.destroy();
        await fd.close().catch(() => {});
    }
    return samples;
}

export async function summarizeCompletionWindows(rawDirectory, options) {
    const policy = policyFor(options),
        started = performance.now(),
        root = await realpath(rawDirectory);
    const check = () => need(performance.now() - started <= LIMITS.milliseconds, "analysis time budget exceeded");
    const original = await bounded(join(root, "raw.lock.json"), LIMITS.jsonBytes),
        manifestSha256 = bytesDigest(original),
        manifest = JSON.parse(original);
    need(
        Array.isArray(manifest.files) &&
            manifest.files.length <= 20000 &&
            Array.isArray(manifest.runs) &&
            manifest.runs.length <= 5120,
        "manifest inventory budget exceeded",
    );
    let inputBytes = original.length,
        traceBytes = 0;
    for (const f of manifest.files) {
        need(Number.isSafeInteger(f.bytes) && f.bytes >= 0, "invalid member size");
        inputBytes += f.bytes;
        if (/\.ndjson$/.test(f.path)) traceBytes += f.bytes;
    }
    need(inputBytes <= LIMITS.inputBytes && traceBytes <= LIMITS.traceBytes, "input byte budget exceeded");
    const verified = await analyze(root);
    check();
    need(verified.rawManifestSha256 === manifestSha256, "raw manifest changed during verification");
    const members = new Map(manifest.files.map((f) => [f.path, f])),
        runs = [],
        counter = { records: 0 };
    let windows = 0;
    for (const row of verified.runs) {
        check();
        const member = members.get(`runs/${row.id}.json`);
        need(member, "missing run metadata");
        const run = JSON.parse(await bounded(join(root, `runs/${row.id}.json`), LIMITS.jsonBytes, member));
        const phase = run.phases.find((p) => p.name === "measurement");
        need(phase, "measurement phase unavailable");
        windows += Math.ceil(phase.durationMs / policy.windowMs) + 1;
        need(windows <= LIMITS.windows, "combined window budget exceeded");
        const samples = await measurementSamples(
            root,
            `traces/${row.id}.ndjson`,
            members.get(`traces/${row.id}.ndjson`),
            check,
            counter,
        );
        const summary = completionWindowsForSamples(samples, {
            durationMs: phase.durationMs,
            elapsedMs: phase.elapsedMs,
            windowMs: policy.windowMs,
            minimumLatencyObservations: policy.minimumLatencyObservations,
        });
        need(
            summary.totals.scheduledArrivals === row.scheduled &&
                summary.totals.sent === row.sent &&
                summary.totals.notSent === row.dropped &&
                summary.totals.finished === row.completed &&
                summary.totals.correct === row.correct &&
                summary.totals.sentErrors === row.failed &&
                summary.totals.lateSent + summary.totals.lateNotSent === row.late &&
                summary.correctCompletedInWindowPerSecond === row.correctCompletedInWindowPerSecond,
            "completion summary differs from original analyzer accounting",
        );
        runs.push({
            id: row.id,
            caseId: row.caseId,
            loadId: row.loadId,
            repeat: row.repeat,
            ...summary,
            qualityIssues: row.issues,
            driverWholePhaseIncludingDrain: row.driver,
        });
    }
    need(
        bytesDigest(await bounded(join(root, "raw.lock.json"), LIMITS.jsonBytes)) === manifestSha256,
        "raw manifest changed after reconstruction",
    );
    const sources = [];
    for (const name of [
        "completion-windows.mjs",
        "analyzer.mjs",
        "common.mjs",
        "probe.mjs",
        "runner.mjs",
        "suite-evidence.mjs",
        "trace-writer.mjs",
        "direct-phase-clock.mjs",
        "runner-sanity.mjs",
        "phase-clock-reference.py",
        "clock-correlation.mjs",
    ]) {
        const raw = await bounded(join(dirname(COMPONENT), name), LIMITS.jsonBytes);
        sources.push({ path: name, bytes: raw.length, sha256: bytesDigest(raw) });
    }
    check();
    return {
        schemaVersion: 1,
        kind: "http-completion-window-diagnostics",
        algorithm: "measurement-completion-cohorts-v1",
        rawManifestSha256: manifestSha256,
        policy,
        limits: LIMITS,
        implementation: { node: process.version, sources },
        verification: {
            evidenceIntegrity: "passed-by-http-analyzer",
            sampleAccounting: "passed-and-completion-windows-conserved",
            correctness: verified.gates.correctness,
            originalVerdict: verified.verdict,
            originalGates: verified.gates,
        },
        runs,
        warmupStable: null,
        engineeringAccepted: false,
        performanceAccepted: false,
        definitions: {
            windows:
                "[start,end); the last measurement window includes end==duration for every event; drain contains strictly later events",
            scheduledArrivals:
                "planned scheduledMs; observedArrivals use sentMs or unsent observedMs; no implied real server queue",
            conservation:
                "observedArrivals=sent+notSent; backlogEnd=backlogStart+sent-finished; sent terminal failures count as finished",
            latency:
                "Completion cohort of measurement requests by endMs. Correct latency excludes failures; failedDurationMs retains terminal failure durations",
            backlog:
                "Recorded admitted requests without terminal outcome at each boundary; no resource or cross-process attribution",
            samples:
                "Per-window counts; P50/P95 need explicit minimumLatencyObservations; P99 needs max(1000,minimum), never pooled across windows",
        },
        limitations: [
            "Descriptive windows do not establish steady state, warmup stability, target saturation, driver headroom or engineering acceptance",
            "Task/HTTP terminal completion includes errors/timeouts; completeResponses is separately counted",
            "Warmup and service probes remain fully verified by analyzer but are excluded from measurement windows",
            "Original quality issues remain; malformed raw or failed/missing service probes reject the report",
            "Immutable evidence required; API time budget is cooperative, standalone CLI additionally has a 300-second watchdog",
        ],
    };
}

export async function reportCompletionWindows(rawDirectory, options, outputDirectory) {
    const root = await realpath(rawDirectory),
        requested = resolve(outputDirectory),
        output = join(await realpath(dirname(requested)), basename(requested));
    need(output !== root && !output.startsWith(root + "/"), "output must be outside raw evidence");
    const result = await summarizeCompletionWindows(root, options),
        raw = Buffer.from(JSON.stringify(result, null, 2) + "\n");
    need(raw.length <= LIMITS.reportBytes, "report byte budget exceeded");
    await mkdir(output, { mode: 0o700 });
    await writeFile(join(output, "report.json"), raw, { flag: "wx", mode: 0o600 });
    await writeFile(
        join(output, "evidence.lock.json"),
        JSON.stringify(
            {
                schemaVersion: 1,
                kind: "http-completion-window-output-lock",
                files: [{ path: "report.json", bytes: raw.length, sha256: bytesDigest(raw) }],
            },
            null,
            2,
        ) + "\n",
        { flag: "wx", mode: 0o600 },
    );
    return result;
}
if (process.argv[1] && pathToFileURL(resolve(process.argv[1])).href === import.meta.url) {
    const watchdog = setTimeout(() => {
        process.stderr.write('{"kind":"http-component-error","message":"completion windows deadline exceeded"}\n');
        process.exit(6);
    }, LIMITS.milliseconds);
    try {
        await entry(async () => {
            const args = argumentsFor(["--raw", "--window-ms", "--minimum-latency-observations", "--out"]);
            for (const name of ["--window-ms", "--minimum-latency-observations"])
                need(/^[1-9][0-9]*$/.test(args[name]), "explicit decimal integer required");
            const result = await reportCompletionWindows(
                args["--raw"],
                {
                    windowMs: Number(args["--window-ms"]),
                    minimumLatencyObservations: Number(args["--minimum-latency-observations"]),
                },
                args["--out"],
            );
            process.stdout.write(
                JSON.stringify({
                    kind: result.kind,
                    rawManifestSha256: result.rawManifestSha256,
                    runs: result.runs.length,
                    warmupStable: null,
                    engineeringAccepted: false,
                    performanceAccepted: false,
                }) + "\n",
            );
        }, "HTTP completion windows: --raw RAW --window-ms INTEGER --minimum-latency-observations INTEGER --out NEW-DIRECTORY\nOffline full analyzer verification and measurement completion cohorts. P99 requires >=1000 per-window observations.\nZero correct responses remain zero; bad raw/probes reject. JSON+lock only, no overwrite or HTTP. Descriptive only; no engineering/performance acceptance.");
    } finally {
        clearTimeout(watchdog);
    }
}
