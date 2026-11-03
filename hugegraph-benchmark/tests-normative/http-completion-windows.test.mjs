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
import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, mkdir, readFile, writeFile, rm, cp, stat, open, symlink } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import http from "node:http";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { bytesDigest, expectedCheckDigest } from "../components/http/common.mjs";
import {
    completionWindowsForSamples,
    summarizeCompletionWindows,
    reportCompletionWindows,
} from "../components/http/completion-windows.mjs";

const exec = promisify(execFile),
    component = resolve("components/http/completion-windows.mjs");
const policy = { windowMs: 100, minimumLatencyObservations: 1 };
const read = async (p) => JSON.parse(await readFile(p)),
    put = (p, v) => writeFile(p, JSON.stringify(v, null, 2) + "\n");
async function temporary(t) {
    const root = await mkdtemp(join(tmpdir(), "hgbench-completion-windows-"));
    t.after(() => rm(root, { recursive: true, force: true }));
    return root;
}
function sample(sequence, scheduledMs, sentMs, endMs, outcome = "ok", observedMs) {
    const completeResponse = sentMs !== null && ["ok", "wrong-answer", "http-status"].includes(outcome);
    return {
        schemaVersion: 1,
        phase: "measurement",
        sequence,
        scheduledMs,
        sentMs,
        endMs,
        observedMs,
        socketMs: sentMs,
        late: outcome === "scheduler-late",
        outcome,
        completeResponse,
        status: completeResponse ? (outcome === "http-status" ? 503 : 200) : null,
        bytes: completeResponse ? 1 : 0,
        decodedBytes: outcome === "ok" ? 1 : null,
        responseEncoding: completeResponse ? "identity" : null,
        validationMs: completeResponse ? 0 : null,
        decodeMs: null,
        bodySha256: completeResponse ? "0".repeat(64) : null,
        checkDigests: outcome === "ok" ? [{ present: true, sha256: expectedCheckDigest({ equals: [2] }) }] : null,
    };
}
function core(samples, patch = {}) {
    return completionWindowsForSamples(samples, { durationMs: 250, elapsedMs: 310, ...policy, ...patch });
}

test("completion boundaries include zero, internal edges, duration exactly, drain and conserved in-flight requests", () => {
    const data = [
        sample(0, 0, 0, 0),
        sample(1, 20, 20, 100),
        sample(2, 40, 40, 200, "timeout"),
        sample(3, 60, 60, 250),
        sample(4, 80, 80, 300, "wrong-answer"),
        sample(5, 100, null, null, "concurrency-limit", 100),
        sample(6, 120, 120, 130, "http-status"),
        sample(7, 140, null, null, "scheduler-late", 250),
        sample(8, 160, 160, 251),
    ];
    const result = core(data);
    assert.deepEqual(
        result.windows.map((w) => [w.startMs, w.endMs, w.rightInclusive]),
        [
            [0, 100, false],
            [100, 200, false],
            [200, 250, true],
        ],
    );
    assert.deepEqual(
        result.windows.map((w) => w.counts.finished),
        [1, 2, 2],
    );
    assert.deepEqual(
        result.windows.map((w) => [w.backlogStart, w.backlogEnd]),
        [
            [0, 4],
            [4, 4],
            [4, 2],
        ],
    );
    assert.equal(result.drain.backlogStart, 2);
    assert.equal(result.drain.backlogEnd, 0);
    assert.equal(result.drain.counts.finished, 2);
    assert.equal(result.drain.counts.correct, 1);
    assert.equal(result.correctInMeasurement, 3);
    assert.equal(result.correctCompletedInWindowPerSecond, 12);
    assert.equal(result.totals.finished, 7);
    assert.equal(result.totals.notSent, 2);
    assert.equal(result.totals.timeouts, 1);
    assert.equal(result.totals.sentErrors, 3);
    assert.equal(result.windows[1].metrics.correctResponseMs.p50, 80);
    assert.equal(result.windows[2].metrics.correctEndToEndMs.p50, 190);
    for (const b of [...result.windows, result.drain]) {
        assert.equal(b.counts.observedArrivals, b.counts.sent + b.counts.notSent);
        assert.equal(b.backlogEnd, b.backlogStart + b.counts.sent - b.counts.finished);
        assert.equal(b.metrics.correctResponseMs.p99, null);
    }
});
test("equal send/finish at duration stays conserved; zero success, timeout and future cancellation are explicit", () => {
    const result = core([
        sample(0, 0, 250, 250, "http-status"),
        sample(1, 200, null, null, "cancelled-before-send", 10),
    ]);
    assert.equal(result.windows.at(-1).counts.sent, 1);
    assert.equal(result.windows.at(-1).counts.finished, 1);
    assert.equal(result.windows[0].counts.observedArrivals, 1);
    assert.equal(result.windows[2].counts.scheduledArrivals, 1);
    assert.equal(result.correctCompletedInWindowPerSecond, 0);
    assert.equal(result.windows[2].metrics.correctResponseMs.count, 0);
    assert.equal(result.windows[2].metrics.correctResponseMs.status, "unknown");
    const early = core([sample(0, 0, null, null, "cancelled-before-send", 0)], { elapsedMs: 20 });
    assert.equal(early.phaseEndedEarly, true);
    assert.equal(early.windows.at(-1).correctCompletedPerSecond, null);
});
test("completion quantiles use each completion cohort count and never pool P99 samples", () => {
    const records = Array.from({ length: 1000 }, (_, i) => sample(i, i / 10, i / 10, i / 10 + 1));
    const split = core(records, { durationMs: 200, elapsedMs: 210, windowMs: 100, minimumLatencyObservations: 2 });
    assert.equal(split.totals.correct, 1000);
    assert(split.windows.every((w) => w.metrics.correctResponseMs.p99SampleEligible === false));
    const pooled = core(records, { durationMs: 200, elapsedMs: 210, windowMs: 200, minimumLatencyObservations: 2 });
    assert.equal(pooled.windows[0].metrics.correctResponseMs.p99SampleEligible, true);
    const strict = core(records, { durationMs: 200, elapsedMs: 210, windowMs: 200, minimumLatencyObservations: 1001 });
    assert.equal(strict.windows[0].metrics.correctResponseMs.p50, null);
    assert.equal(strict.windows[0].metrics.correctResponseMs.p99, null);
});
test("pure core rejects duplicate, missing and unfinished terminal records and invalid bounds", () => {
    const a = sample(0, 0, 0, 100);
    for (const rows of [
        [a, a],
        [{ ...a, sequence: 1 }],
        [{ ...a, endMs: null }],
        [{ ...a, endMs: 311 }],
        [{ ...a, sentMs: null, endMs: 100 }],
        [{ ...a, scheduledMs: 250 }],
    ])
        assert.throws(() => core(rows), /HTTP_COMPLETION_WINDOWS/);
    for (const patch of [
        { windowMs: 0 },
        { minimumLatencyObservations: 0 },
        { minimumLatencyObservations: 1.5 },
        { durationMs: 300000, windowMs: 1 },
    ])
        assert.throws(() => core([a], patch), /HTTP_COMPLETION_WINDOWS/);
});

async function relock(raw) {
    const lock = await read(join(raw, "raw.lock.json"));
    for (const f of lock.files) {
        const bytes = await readFile(join(raw, f.path));
        f.bytes = bytes.length;
        f.sha256 = bytesDigest(bytes);
    }
    await put(join(raw, "raw.lock.json"), lock);
}
/** Synthetic protocol fixture; no runtime or performance proof. The unchanged analyzer must accept it first. */
async function fixture(t, records) {
    const root = await temporary(t),
        raw = join(root, "raw");
    for (const name of [raw, join(raw, "requests"), join(raw, "runs"), join(raw, "traces")]) await mkdir(name);
    const profile = {
        schemaVersion: 1,
        kind: "http-benchmark-profile",
        name: "synthetic-completion-window-protocol",
        serviceChecks: false,
        endpoint: "http://127.0.0.1:1/gremlin",
        target: {
            label: "synthetic",
            build: "fixture",
            mode: "control",
            environment: "no runtime proof",
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
        loads: [{ id: "open", model: "open", ratePerSecond: 20, maxInFlight: 2, maxStartDelayMs: 10 }],
        repeats: 1,
        warmupMs: 0,
        durationMs: 250,
        requestTimeoutMs: 200,
        drainMs: 100,
        maxRequestsPerPhase: 100,
        maxResponseBytes: 4096,
        maxTraceBytes: 1048576,
    };
    records ??= [
        sample(0, 0, 0, 100),
        sample(1, 50, 50, 250),
        sample(2, 100, 100, 251, "timeout"),
        sample(3, 150, null, null, "concurrency-limit", 155),
        sample(4, 200, 200, 220, "wrong-answer"),
    ];
    const sent = records.filter((s) => s.sentMs !== null),
        counts = {};
    for (const s of records) counts[s.outcome] = (counts[s.outcome] ?? 0) + 1;
    const phases = [
        {
            name: "measurement",
            durationMs: 250,
            elapsedMs: 310,
            scheduled: records.length,
            sent: sent.length,
            completed: sent.length,
            dropped: records.length - sent.length,
            failed: sent.filter((s) => s.outcome !== "ok").length,
            late: records.filter((s) => s.late).length,
            counts,
            budgetExhausted: false,
            cancelled: false,
            averageCpuCores: 0.2,
            eventLoopUtilization: 0.1,
            eventLoopDelayMs: { p99: 1, max: 2 },
            maxRssKiB: 10000,
        },
    ];
    const trace = records.map((s) => JSON.stringify(s)).join("\n") + "\n";
    const values = {
        "profile.json": profile,
        "target-evidence.json": { schemaVersion: 1, kind: "operator-supplied-target-evidence", files: [] },
        "implementation.json": { fixture: "synthetic/no-runtime-proof" },
        "requests/r0001.json": {
            id: "r0001",
            repeat: 0,
            profile,
            case: profile.cases[0],
            load: profile.loads[0],
            remainingTraceBytes: profile.maxTraceBytes,
        },
        "runs/r0001.json": {
            id: "r0001",
            caseId: "two",
            loadId: "open",
            repeat: 0,
            phases,
            traceBytes: Buffer.byteLength(trace),
        },
    };
    for (const [name, value] of Object.entries(values)) await put(join(raw, name), value);
    await writeFile(join(raw, "traces/r0001.ndjson"), trace);
    await put(join(raw, "raw.lock.json"), {
        schemaVersion: 2,
        kind: "http-raw-evidence",
        serviceChecks: false,
        runs: ["r0001"],
        files: [...Object.keys(values), "traces/r0001.ndjson"].map((path) => ({ path })),
    });
    await relock(raw);
    return { root, raw };
}
test("full analyzer verification preserves quality errors while completion goodput exactly includes end==duration", async (t) => {
    const f = await fixture(t),
        result = await summarizeCompletionWindows(f.raw, policy),
        run = result.runs[0];
    assert.equal(run.correctInMeasurement, 2);
    assert.equal(run.correctCompletedInWindowPerSecond, 8);
    assert.equal(run.totals.correct, 2);
    assert.equal(run.totals.timeouts, 1);
    assert.equal(run.totals.notSent, 1);
    assert(run.qualityIssues.includes("measurement:dropped-arrivals"));
    assert(run.qualityIssues.includes("measurement:request-errors-or-wrong-answers"));
    assert.equal(result.verification.correctness, "failed");
    assert.equal(result.verification.evidenceIntegrity, "passed-by-http-analyzer");
    assert.deepEqual(
        result.implementation.sources.map((source) => source.path),
        [
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
        ],
    );
    for (const source of result.implementation.sources) {
        const raw = await readFile(resolve("components/http", source.path));
        assert.equal(source.bytes, raw.length);
        assert.equal(source.sha256, bytesDigest(raw));
    }
    assert.equal(result.warmupStable, null);
    assert.equal(result.engineeringAccepted, false);
    assert.equal(result.performanceAccepted, false);
});
test("complete zero-correct raw displays zero rather than dropping the point or fabricating latency", async (t) => {
    const records = Array.from({ length: 5 }, (_, i) => sample(i, i * 50, i * 50, i * 50 + 30, "timeout"));
    const f = await fixture(t, records),
        result = await summarizeCompletionWindows(f.raw, policy);
    assert.equal(result.runs[0].correctCompletedInWindowPerSecond, 0);
    assert.equal(result.runs[0].totals.timeouts, 5);
    assert(
        result.runs[0].windows.every(
            (w) => w.metrics.correctResponseMs.count === 0 && w.metrics.correctResponseMs.p99 === null,
        ),
    );
});
test("damaged raw, resealed duplicate/unfinished traces and missing probes reject without producing output", async (t) => {
    for (const mode of ["digest", "duplicate", "unfinished", "probe"]) {
        const f = await fixture(t),
            output = join(f.root, "no-report");
        if (mode === "probe") {
            const p = await read(join(f.raw, "profile.json"));
            p.serviceChecks = true;
            await put(join(f.raw, "profile.json"), p);
            await relock(f.raw);
        } else {
            const trace = join(f.raw, "traces/r0001.ndjson"),
                records = (await readFile(trace, "utf8")).trim().split("\n").map(JSON.parse);
            if (mode === "duplicate") records.push(records[0]);
            else records[0].endMs = null;
            await writeFile(trace, records.map((x) => JSON.stringify(x)).join("\n") + "\n");
            if (mode !== "digest") await relock(f.raw);
        }
        await assert.rejects(reportCompletionWindows(f.raw, policy, output));
        await assert.rejects(stat(output), { code: "ENOENT" });
    }
});
test("manifest input budget is enforced before the analyzer or output reservation", async (t) => {
    const f = await fixture(t),
        fd = await open(join(f.raw, "raw.lock.json"), "w");
    await fd.truncate(8 * 1024 * 1024 + 1);
    await fd.close();
    const output = join(f.root, "no-report");
    await assert.rejects(reportCompletionWindows(f.raw, policy, output), /bounded regular/);
    await assert.rejects(stat(output), { code: "ENOENT" });
});

test("report rejects output inside raw even through a symlinked parent", async (t) => {
    const f = await fixture(t),
        link = join(f.root, "raw-alias");
    await symlink(f.raw, link);
    await assert.rejects(reportCompletionWindows(f.raw, policy, join(link, "new-report")), /outside raw evidence/);
    await assert.rejects(stat(join(f.raw, "new-report")), { code: "ENOENT" });
});
test("JSON and output lock relocate byte-for-byte and CLI requires explicit integer options without overwriting", async (t) => {
    const f = await fixture(t),
        first = join(f.root, "first"),
        second = join(f.root, "second"),
        moved = join(f.root, "moved");
    await reportCompletionWindows(f.raw, policy, first);
    await cp(f.raw, moved, { recursive: true });
    await rm(f.raw, { recursive: true });
    const args = ["--raw", moved, "--window-ms", "100", "--minimum-latency-observations", "1", "--out", second];
    const reply = JSON.parse((await exec(process.execPath, [component, ...args], { timeout: 15000 })).stdout);
    assert.equal(reply.kind, "http-completion-window-diagnostics");
    for (const name of ["report.json", "evidence.lock.json"])
        assert.deepEqual(await readFile(join(first, name)), await readFile(join(second, name)));
    const lock = await read(join(second, "evidence.lock.json")),
        raw = await readFile(join(second, "report.json"));
    assert.deepEqual(lock.files, [{ path: "report.json", bytes: raw.length, sha256: bytesDigest(raw) }]);
    await assert.rejects(exec(process.execPath, [component, ...args]));
    assert.deepEqual(await readFile(join(second, "report.json")), raw);
    for (const flags of [args.slice(0, 4), [...args, "--window-ms", "1"], args.map((x) => (x === "100" ? "1e2" : x))])
        await assert.rejects(exec(process.execPath, [component, ...flags]));
    assert.match((await exec(process.execPath, [component, "--help"])).stdout, /minimum-latency-observations/);
});
test("actual local HTTP raw reuses its original complete-response and goodput accounting offline", async (t) => {
    const root = await temporary(t),
        server = http.createServer((request, response) => {
            request.resume();
            setTimeout(() => response.end('{"result":{"data":[2]}}'), 10);
        });
    await new Promise((done) => server.listen(0, "127.0.0.1", done));
    t.after(async () => {
        server.closeAllConnections();
        await new Promise((done) => server.close(done));
    });
    const { planHttp, httpProfileSchema } = await import("../dist/http.js");
    const p = httpProfileSchema.parse({
        schemaVersion: 1,
        kind: "http-benchmark-profile",
        name: "local-completion-protocol-only",
        endpoint: `http://127.0.0.1:${server.address().port}/gremlin`,
        serviceChecks: false,
        target: { label: "local", build: "test", mode: "control", environment: "tool validation" },
        cases: [{ id: "two", body: { gremlin: "1+1" }, checks: [{ pointer: "/result/data", equals: [2] }] }],
        loads: [{ id: "open", model: "open", ratePerSecond: 20, maxInFlight: 2, maxStartDelayMs: 1000 }],
        repeats: 1,
        warmupMs: 0,
        durationMs: 200,
        requestTimeoutMs: 100,
        drainMs: 100,
        maxRequestsPerPhase: 100,
        maxResponseBytes: 4096,
        maxTraceBytes: 1048576,
    });
    const profile = join(root, "profile.json"),
        plan = join(root, "plan");
    await put(profile, p);
    await planHttp(profile, plan);
    await exec(process.execPath, [resolve("components/http/workflow.mjs"), "--plan", plan], { timeout: 15000 });
    const result = await summarizeCompletionWindows(join(plan, "output"), policy);
    assert.equal(result.runs[0].totals.sent, 4);
    assert.equal(result.runs[0].totals.correct, 4);
    assert.equal(result.runs[0].totals.sentErrors, 0);
    assert.equal(result.verification.correctness, "passed");
});
