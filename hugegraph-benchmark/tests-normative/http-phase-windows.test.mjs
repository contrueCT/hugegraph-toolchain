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

import { test } from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { cp, mkdir, mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { analyze } from "../components/http/analyzer.mjs";
import { bytesDigest, expectedCheckDigest } from "../components/http/common.mjs";
import { analyzePhaseWindows, describePhaseWindows, reportPhaseWindows } from "../components/http/phase-windows.mjs";

const exec = promisify(execFile),
    component = resolve("components/http/phase-windows.mjs"),
    policy = { windowMs: 100, minimumRequestsPerWindow: 2, minimumP99Requests: 1000 },
    read = async (path) => JSON.parse(await readFile(path, "utf8")),
    write = async (path, value) => writeFile(path, JSON.stringify(value, null, 2) + "\n");

async function temporary(t) {
    const root = await mkdtemp(join(tmpdir(), "hgbench-http-windows-"));
    t.after(() => rm(root, { recursive: true, force: true }));
    return root;
}

function sample(phase, sequence, scheduledMs, sentMs, endMs, outcome, extra = {}) {
    const complete = sentMs !== null && ["ok", "wrong-answer", "http-status"].includes(outcome);
    return {
        schemaVersion: 1,
        phase,
        sequence,
        scheduledMs,
        sentMs,
        socketMs: sentMs,
        endMs,
        validationMs: complete ? 1 : null,
        decodeMs: null,
        decodedBytes: outcome === "ok" ? 1 : null,
        responseEncoding: complete ? "identity" : null,
        bytes: complete ? 1 : 0,
        status: complete ? (outcome === "http-status" ? 500 : 200) : null,
        bodySha256: complete ? "0".repeat(64) : null,
        checkDigests: outcome === "ok" ? [{ present: true, sha256: expectedCheckDigest({ equals: [2] }) }] : null,
        completeResponse: complete,
        late: outcome === "scheduler-late" || (sentMs !== null && sentMs - scheduledMs > 10),
        outcome,
        ...extra,
    };
}

async function relock(raw) {
    const manifest = await read(join(raw, "raw.lock.json"));
    for (const f of manifest.files) {
        const bytes = await readFile(join(raw, f.path));
        f.bytes = bytes.length;
        f.sha256 = bytesDigest(bytes);
    }
    await write(join(raw, "raw.lock.json"), manifest);
}

/** Synthetic raw-protocol fixture, with real offline verification and no runtime/performance claim. */
async function synthetic(t, options = {}) {
    const root = await temporary(t),
        raw = join(root, "raw");
    for (const path of [raw, join(raw, "runs"), join(raw, "requests"), join(raw, "traces")]) await mkdir(path);
    const profile = {
        schemaVersion: 1,
        kind: "http-benchmark-profile",
        name: "synthetic-window-protocol-no-runtime-proof",
        serviceChecks: false,
        endpoint: "http://127.0.0.1:1/gremlin",
        target: {
            label: "synthetic",
            build: "synthetic",
            mode: "control",
            environment: "synthetic",
            evidenceFiles: [],
        },
        cases: [
            {
                id: "two",
                body: { gremlin: "1+1" },
                expectedStatus: 200,
                checks: [{ pointer: "/result/data", equals: [2] }],
            },
        ],
        loads: [{ id: "arrival", model: "open", ratePerSecond: 20, maxInFlight: 2, maxStartDelayMs: 10 }],
        repeats: 1,
        warmupMs: 100,
        durationMs: 250,
        requestTimeoutMs: 200,
        drainMs: 100,
        maxRequestsPerPhase: 100000,
        maxResponseBytes: 4096,
        maxTraceBytes: 1048576,
        ...options.profile,
    };
    const samples = options.samples ?? [
        sample("warmup", 0, 0, 0, 10, "ok", { validationMs: 2 }),
        sample("warmup", 1, 50, 55, 105, "http-status"),
        sample("measurement", 0, 0, 0, 20, "ok", { validationMs: 5 }),
        sample("measurement", 1, 50, 55, 210, "wrong-answer", { validationMs: 10 }),
        sample("measurement", 2, 100, null, null, "concurrency-limit", { observedMs: 105 }),
        sample("measurement", 3, 150, 165, 265, "ok", { validationMs: 30 }),
        sample("measurement", 4, 200, null, null, "scheduler-late", { observedMs: 255 }),
    ];
    const phases = (profile.warmupMs ? ["warmup", "measurement"] : ["measurement"]).map((name) => {
        const records = samples.filter((s) => s.phase === name),
            durationMs = name === "warmup" ? profile.warmupMs : profile.durationMs,
            sent = records.filter((s) => s.sentMs !== null),
            counts = {};
        for (const s of records) counts[s.outcome] = (counts[s.outcome] ?? 0) + 1;
        return {
            name,
            durationMs,
            elapsedMs:
                Math.max(
                    durationMs,
                    ...records.map((s) => (s.endMs === null ? s.observedMs : s.endMs + (s.validationMs ?? 0))),
                ) + 10,
            scheduled: records.length,
            sent: sent.length,
            completed: sent.length,
            failed: sent.filter((s) => s.outcome !== "ok").length,
            dropped: records.length - sent.length,
            late: records.filter((s) => s.late).length,
            counts,
            budgetExhausted: false,
            cancelled: false,
            averageCpuCores: 0.2,
            eventLoopUtilization: 0.1,
            eventLoopDelayMs: { p99: 1, max: 2 },
            maxRssKiB: 10000,
        };
    });
    const trace = samples.length ? samples.map((s) => JSON.stringify(s)).join("\n") + "\n" : "",
        run = {
            id: "r0001",
            caseId: "two",
            loadId: "arrival",
            repeat: 0,
            phases,
            traceBytes: Buffer.byteLength(trace),
        };
    await write(join(raw, "profile.json"), profile);
    await write(join(raw, "target-evidence.json"), {
        schemaVersion: 1,
        kind: "operator-supplied-target-evidence",
        files: [],
    });
    await write(join(raw, "implementation.json"), {
        schemaVersion: 1,
        fixture: "synthetic raw protocol, no runtime proof",
    });
    await write(join(raw, "requests/r0001.json"), {
        id: "r0001",
        repeat: 0,
        profile,
        case: profile.cases[0],
        load: profile.loads[0],
        remainingTraceBytes: profile.maxTraceBytes,
    });
    await write(join(raw, "runs/r0001.json"), run);
    await writeFile(join(raw, "traces/r0001.ndjson"), trace);
    await write(join(raw, "raw.lock.json"), {
        schemaVersion: 2,
        kind: "http-raw-evidence",
        serviceChecks: false,
        runs: ["r0001"],
        files: [
            "profile.json",
            "target-evidence.json",
            "implementation.json",
            "requests/r0001.json",
            "runs/r0001.json",
            "traces/r0001.ndjson",
        ].map((path) => ({ path })),
    });
    await relock(raw);
    assert.equal((await analyze(raw)).gates.sampleAccounting, "passed");
    return { root, raw, samples };
}

test("HTTP phase windows partition arrival records exactly, preserve empty/final windows and drain completion overlays", async (t) => {
    const f = await synthetic(t),
        result = await analyzePhaseWindows(f.raw, policy),
        [warmup, measurement] = result.runs[0].phases;
    assert.equal(result.performanceAccepted, false);
    assert.equal(result.warmupStable, null);
    assert.equal(result.qualification, "not-assessed");
    assert.deepEqual(
        measurement.windows.map((w) => [w.startMs, w.endMs]),
        [
            [0, 100],
            [100, 200],
            [200, 250],
        ],
    );
    assert.deepEqual(
        measurement.windows.map((w) => w.counts.records),
        [2, 2, 0],
    );
    assert.equal(measurement.drain.arrivals.counts.records, 1);
    assert.equal(measurement.drain.arrivals.counts.notSent, 1);
    assert.deepEqual(measurement.totals, {
        records: 5,
        sent: 3,
        completed: 3,
        correct: 2,
        errors: 1,
        dropped: 2,
        notSent: 2,
        late: 2,
        completeResponses: 3,
    });
    assert.deepEqual(measurement.drain.completionOverlay, { requestTerminals: 1, completeResponseValidations: 1 });
    assert.equal(measurement.windows[0].completion.terminalOutsideArrivalWindow, 1);
    assert.equal(measurement.windows[1].completion.terminalInDrain, 1);
    assert.equal(measurement.windows[1].metrics.correctResponseMs.observedNearestRank.p50, 100);
    assert.equal(measurement.windows[0].metrics.validationMs.p95, 10);
    assert.equal(warmup.windows[0].counts.records, 2);
    assert.equal(warmup.windows[0].completion.terminalOutsideArrivalWindow, 1);
    assert.equal(warmup.drain.completionOverlay.requestTerminals, 1);
    for (const phase of [warmup, measurement]) {
        const all = [...phase.windows, phase.drain.arrivals];
        for (const key of Object.keys(phase.totals))
            assert.equal(
                all.reduce((n, w) => n + w.counts[key], 0),
                phase.totals[key],
            );
        assert.equal(phase.totals.correct + phase.totals.errors + phase.totals.dropped, phase.totals.records);
    }
});

test("HTTP phase windows use sent/observed boundaries, never planned schedule or completion for ownership", async (t) => {
    const samples = [
        sample("measurement", 0, 0, 100, 150, "ok"),
        sample("measurement", 1, 50, null, null, "scheduler-late", { observedMs: 200 }),
        sample("measurement", 2, 100, null, null, "cancelled-before-send", { observedMs: 10 }),
        sample("measurement", 3, 150, 200, 245, "ok", { validationMs: 5 }),
        sample("measurement", 4, 200, 249, 260, "timeout"),
    ];
    const f = await synthetic(t, { profile: { warmupMs: 0 }, samples }),
        phase = (await analyzePhaseWindows(f.raw, policy)).runs[0].phases[0];
    assert.deepEqual(
        phase.windows.map((w) => w.counts.records),
        [1, 1, 3],
    );
    assert.equal(phase.windows[0].counts.notSent, 1);
    assert.equal(phase.windows[1].counts.correct, 1);
    assert.equal(phase.windows[2].completion.terminalInDrain, 1);
    assert.equal(
        phase.windows[2].completion.validationInDrain,
        0,
        "validation ending exactly at duration is not in drain",
    );
    assert.equal(phase.drain.arrivals.counts.records, 0);
    assert.equal(phase.drain.completionOverlay.requestTerminals, 1);
});

test("HTTP phase windows mark low samples unknown and require a separate >=1000 sample P99 threshold per metric", async (t) => {
    const f = await synthetic(t),
        phase = (await analyzePhaseWindows(f.raw, policy)).runs[0].phases[1],
        first = phase.windows[0];
    assert.equal(first.sampleStatus, "descriptive");
    assert.equal(first.metrics.correctResponseMs.sampleStatus, "unknown", "correct-only count has its own minimum");
    assert.equal(first.metrics.correctResponseMs.p50, null);
    assert.equal(first.metrics.correctResponseMs.observedNearestRank.p50, 20);
    assert.equal(first.metrics.allCompleteResponseMs.p95, 155);
    assert.equal(first.metrics.allCompleteResponseMs.p99, null);
    assert.equal(first.metrics.allCompleteResponseMs.p99Status, "unknown");
    assert.equal(phase.windows[2].metrics.correctResponseMs.observedNearestRank.p99, null);
    const enoughSamples = Array.from({ length: 1000 }, (_, i) =>
            sample("measurement", i, i / 10, i / 10, i / 10 + (i + 1) / 10000, "ok", { validationMs: 0 }),
        ),
        positive = await synthetic(t, {
            profile: {
                warmupMs: 0,
                durationMs: 100,
                loads: [{ id: "arrival", model: "open", ratePerSecond: 10000, maxInFlight: 2, maxStartDelayMs: 10 }],
            },
            samples: enoughSamples,
        }),
        quantile = (await analyzePhaseWindows(positive.raw, { ...policy, minimumRequestsPerWindow: 1000 })).runs[0]
            .phases[0].windows[0].metrics.correctResponseMs;
    assert.equal(quantile.sampleStatus, "descriptive");
    assert.equal(quantile.p99Status, "descriptive");
    assert(Math.abs(quantile.p99 - 0.099) < 1e-9);
});

test("HTTP phase windows reuse full raw hash/count/timeline validation, including completely resealed corruption", async (t) => {
    const f = await synthetic(t);
    for (const mode of ["hash", "count", "timeline", "duplicate", "remaining-budget", "target-lock"]) {
        const raw = join(f.root, mode);
        await cp(f.raw, raw, { recursive: true });
        if (mode === "hash") await writeFile(join(raw, "implementation.json"), "{}\n");
        else if (mode === "count") {
            const run = await read(join(raw, "runs/r0001.json"));
            run.phases[1].sent++;
            await write(join(raw, "runs/r0001.json"), run);
        } else if (mode === "remaining-budget") {
            const request = await read(join(raw, "requests/r0001.json"));
            request.remainingTraceBytes--;
            await write(join(raw, "requests/r0001.json"), request);
        } else if (mode === "target-lock") {
            const target = await read(join(raw, "target-evidence.json"));
            target.files.push({ path: "missing.json", bytes: 1, sha256: "0".repeat(64) });
            await write(join(raw, "target-evidence.json"), target);
        } else {
            const records = structuredClone(f.samples);
            if (mode === "timeline") records[2].endMs = -1;
            else records[3].sequence = records[2].sequence;
            const text = records.map((s) => JSON.stringify(s)).join("\n") + "\n";
            await writeFile(join(raw, "traces/r0001.ndjson"), text);
            const run = await read(join(raw, "runs/r0001.json"));
            run.traceBytes = Buffer.byteLength(text);
            await write(join(raw, "runs/r0001.json"), run);
        }
        if (mode !== "hash") await relock(raw);
        await assert.rejects(
            analyzePhaseWindows(raw, policy),
            /Evidence file size|hash mismatch|sent mismatch|timeline|Duplicate sample|remaining trace budget|Target evidence/,
        );
    }
});

test("HTTP phase windows have explicit bounded options and reject excessive windows before creating output", async (t) => {
    const f = await synthetic(t);
    for (const invalid of [
        undefined,
        {},
        { ...policy, windowMs: 0 },
        { ...policy, minimumP99Requests: 999 },
        { ...policy, minimumRequestsPerWindow: 0 },
        { ...policy, minimumRequestsPerWindow: 1.2 },
        { ...policy, extra: true },
    ])
        await assert.rejects(analyzePhaseWindows(f.raw, invalid), /HTTP_PHASE_WINDOWS/);
    const huge = await synthetic(t, {
        profile: { warmupMs: 0, durationMs: 300000, loads: [{ id: "arrival", model: "closed", concurrency: 1 }] },
        samples: [],
    });
    const output = join(huge.root, "never-created");
    await assert.rejects(reportPhaseWindows(huge.raw, output, { ...policy, windowMs: 1 }), /window budget/);
    await assert.rejects(stat(output), { code: "ENOENT" });
    assert.equal(describePhaseWindows().limits.minimumP99Requests[0], 1000);
});

test("HTTP phase window JSON/HTML rebuild byte-for-byte offline after relocation and never overwrite output", async (t) => {
    const f = await synthetic(t),
        first = join(f.root, "first"),
        second = join(f.root, "second"),
        moved = join(f.root, "moved");
    await reportPhaseWindows(f.raw, first, policy);
    await cp(f.raw, moved, { recursive: true });
    await rm(f.raw, { recursive: true });
    await reportPhaseWindows(moved, second, policy);
    for (const file of ["report.json", "report.html"])
        assert.deepEqual(await readFile(join(first, file)), await readFile(join(second, file)));
    const original = await readFile(join(first, "report.json"));
    await assert.rejects(reportPhaseWindows(moved, first, policy), { code: "EEXIST" });
    assert.deepEqual(await readFile(join(first, "report.json")), original);
    const html = await readFile(join(first, "report.html"), "utf8");
    assert(html.includes("default-src 'none'"));
    assert(!html.includes('src="http'));
    assert(html.includes("completionOverlay"));
});

test("HTTP phase window fixed CLI describes, validates flags, writes actual reports and returns structured failures", async (t) => {
    const f = await synthetic(t),
        flags = [
            "--input",
            f.raw,
            "--output",
            join(f.root, "cli"),
            "--window-ms",
            "100",
            "--minimum-requests-per-window",
            "2",
            "--minimum-p99-requests",
            "1000",
        ];
    assert.match((await exec(process.execPath, [component, "--help"])).stdout, /minimum-p99-requests/);
    assert.equal(JSON.parse((await exec(process.execPath, [component, "describe"])).stdout).network, false);
    assert.equal(
        JSON.parse((await exec(process.execPath, [component, "report", ...flags])).stdout).performanceAccepted,
        false,
    );
    for (const args of [
        ["unknown"],
        ["describe", "--extra"],
        ["report"],
        ["report", ...flags, "--extra", "1"],
        ["report", ...flags, "--window-ms", "100"],
        ["report", ...flags.slice(0, -1), "1e3"],
    ]) {
        await assert.rejects(exec(process.execPath, [component, ...args]), (error) => {
            assert.equal(error.code, 6);
            assert.equal(JSON.parse(error.stderr).kind, "http-component-error");
            return true;
        });
    }
});

test("HTTP phase windows public CLI rebuilds actual local HTTP raw offline after relocation and rejects bad inputs/output reuse", async (t) => {
    let requests = 0;
    const root = await temporary(t),
        server = http.createServer((request, response) => {
            requests++;
            request.resume();
            setTimeout(() => {
                response.setHeader("Content-Type", "application/json");
                response.end('{"result":{"data":[2]}}');
            }, 15);
        });
    await new Promise((done) => server.listen(0, "127.0.0.1", done));
    t.after(async () => {
        server.closeAllConnections();
        await new Promise((done) => server.close(done));
    });
    const { planHttp, httpProfileSchema } = await import("../dist/http.js"),
        attachment = join(root, "environment.json");
    await write(attachment, { kind: "local-http-functional-fixture", performanceAccepted: false });
    const profile = httpProfileSchema.parse({
        schemaVersion: 1,
        kind: "http-benchmark-profile",
        name: "actual-local-http-functional-only",
        endpoint: `http://127.0.0.1:${server.address().port}/gremlin`,
        serviceChecks: false,
        target: {
            label: "local",
            build: "test",
            mode: "control",
            environment: "functional",
            evidenceFiles: [attachment],
        },
        cases: [{ id: "two", body: { gremlin: "1+1" }, checks: [{ pointer: "/result/data", equals: [2] }] }],
        loads: [{ id: "arrival", model: "open", ratePerSecond: 20, maxInFlight: 2, maxStartDelayMs: 1000 }],
        repeats: 1,
        warmupMs: 100,
        durationMs: 200,
        requestTimeoutMs: 100,
        drainMs: 100,
        maxRequestsPerPhase: 100,
        maxResponseBytes: 4096,
        maxTraceBytes: 1048576,
    });
    const source = join(root, "profile.json"),
        plan = join(root, "plan");
    await write(source, profile);
    await planHttp(source, plan);
    await exec(process.execPath, [resolve("components/http/workflow.mjs"), "--plan", plan], { timeout: 15000 });
    const raw = join(plan, "output"),
        result = await analyzePhaseWindows(raw, policy),
        actual = await read(join(raw, "runs/r0001.json"));
    assert.equal(actual.phaseClock.verified, false);
    for (const [i, phase] of result.runs[0].phases.entries()) {
        assert.equal(phase.totals.records, actual.phases[i].scheduled);
        assert.equal(phase.totals.sent, actual.phases[i].sent);
        assert.equal(phase.totals.errors, 0);
        assert.equal(phase.totals.correct, actual.phases[i].sent);
    }
    assert.equal(result.performanceAccepted, false);
    const cli = resolve("dist/cli.js"),
        first = join(root, "public-first"),
        second = join(root, "public-second"),
        moved = join(root, "moved-raw"),
        before = requests;
    const argsFor = (input, output) => [
        cli,
        "http",
        "calibration",
        "windows",
        input,
        "--out",
        output,
        "--window-ms",
        "100",
        "--minimum-requests-per-window",
        "2",
        "--minimum-p99-requests",
        "1000",
    ];
    const reply = JSON.parse((await exec(process.execPath, argsFor(raw, first))).stdout);
    assert.equal(reply.schemaVersion, 1);
    assert.equal(reply.ok, true);
    assert.equal(reply.command, "http.calibration.windows");
    assert.equal(reply.data.kind, "http-phase-window-diagnostics");
    assert.equal(reply.data.performanceAccepted, false);
    assert.equal(reply.data.warmupStable, null);
    assert.deepEqual(reply.nextActions, [
        { kind: "read", path: join(first, "report.json"), mediaType: "application/json" },
        { kind: "read", path: join(first, "report.html"), mediaType: "text/html" },
    ]);
    await cp(raw, moved, { recursive: true });
    await rm(raw, { recursive: true });
    const movedReply = JSON.parse((await exec(process.execPath, argsFor(moved, second))).stdout);
    assert.equal(movedReply.data.rawManifestSha256, reply.data.rawManifestSha256);
    for (const file of ["report.json", "report.html"])
        assert.deepEqual(await readFile(join(first, file)), await readFile(join(second, file)));
    const original = await readFile(join(first, "report.json"));
    const failure = (code, expectedCode) => (error) => {
        assert.equal(error.code, code);
        const envelope = JSON.parse(error.stdout);
        assert.equal(envelope.ok, false);
        assert.equal(envelope.error.code, expectedCode);
        return true;
    };
    await assert.rejects(exec(process.execPath, argsFor(moved, first)), failure(6, "HTTP_CALIBRATION_FAILED"));
    assert.deepEqual(await readFile(join(first, "report.json")), original);
    for (const [flag, invalid] of [
        ["--window-ms", "1e2"],
        ["--window-ms", "0"],
        ["--window-ms", "300001"],
        ["--minimum-requests-per-window", "1.5"],
        ["--minimum-requests-per-window", "100001"],
        ["--minimum-p99-requests", "999"],
        ["--minimum-p99-requests", "100001"],
    ]) {
        const bad = argsFor(moved, join(root, "never-created"));
        bad[bad.indexOf(flag) + 1] = invalid;
        await assert.rejects(exec(process.execPath, bad), failure(2, "CLI_USAGE"));
    }
    await assert.rejects(stat(join(root, "never-created")), { code: "ENOENT" });
    const missing = argsFor(moved, join(root, "never-created")).slice(0, -2);
    await assert.rejects(exec(process.execPath, missing), failure(2, "CLI_USAGE"));
    const damaged = join(root, "damaged-raw"),
        damagedOutput = join(root, "damaged-output");
    await cp(moved, damaged, { recursive: true });
    await writeFile(join(damaged, "implementation.json"), "{}\n");
    await assert.rejects(
        exec(process.execPath, argsFor(damaged, damagedOutput)),
        failure(6, "HTTP_CALIBRATION_FAILED"),
    );
    await assert.rejects(stat(damagedOutput), { code: "ENOENT" });
    assert.equal(requests, before, "public window reconstruction sends no HTTP requests");
    const help = (await exec(process.execPath, [cli, "http", "calibration", "windows", "--help"])).stdout;
    assert.match(help, /offline/i);
    assert.match(help, /unknown/);
    assert.match(help, /damaged raw is rejected/);
    assert.match(help, /no automatic warmup\/steady state/);
});
