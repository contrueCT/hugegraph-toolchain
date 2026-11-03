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
import { gzipSync } from "node:zlib";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { cp, mkdir, mkdtemp, open, readFile, rm, stat, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { bytesDigest, expectedCheckDigest } from "../components/http/common.mjs";
import { analyzeDriverSelfCost, reportDriverSelfCost } from "../components/http/driver-self-cost.mjs";

const exec = promisify(execFile),
    read = async (path) => JSON.parse(await readFile(path, "utf8")),
    put = async (path, value) => writeFile(path, JSON.stringify(value, null, 2) + "\n");

async function temp(t) {
    const root = await mkdtemp(join(tmpdir(), "hgbench-driver-cost-"));
    t.after(() => rm(root, { recursive: true, force: true }));
    return root;
}

async function relock(raw) {
    const lock = await read(join(raw, "raw.lock.json"));
    for (const f of lock.files) {
        const content = await readFile(join(raw, f.path));
        f.bytes = content.length;
        f.sha256 = bytesDigest(content);
    }
    await put(join(raw, "raw.lock.json"), lock);
}

/** Synthetic raw protocol; wall-cost values are exact fixture numbers, not runtime measurements. */
async function fixture(t) {
    const root = await temp(t),
        raw = join(root, "raw");
    for (const path of [raw, join(raw, "requests"), join(raw, "runs"), join(raw, "traces")]) await mkdir(path);
    const profile = {
        schemaVersion: 1,
        kind: "http-benchmark-profile",
        name: "synthetic-driver-cost",
        serviceChecks: false,
        endpoint: "http://127.0.0.1:1/gremlin",
        responseEncoding: "gzip",
        target: {
            label: "synthetic",
            build: "fixture",
            mode: "control",
            environment: "protocol-only",
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
        durationMs: 200,
        requestTimeoutMs: 100,
        drainMs: 100,
        maxRequestsPerPhase: 100,
        maxResponseBytes: 4096,
        maxTraceBytes: 1048576,
    };
    const sample = (phase, sequence, sentMs, endMs, outcome, validationMs = null, decodeMs = null) => ({
        phase,
        sequence,
        scheduledMs: sequence * 50,
        sentMs,
        endMs,
        socketMs: sentMs,
        observedMs: sentMs === null ? 55 : undefined,
        outcome,
        late: false,
        completeResponse: validationMs !== null,
        validationMs,
        decodeMs,
        bytes: validationMs !== null ? 1 : 0,
        decodedBytes: outcome === "ok" ? 1 : null,
        status: validationMs !== null ? 200 : null,
        responseEncoding: validationMs !== null ? "gzip" : null,
        bodySha256: validationMs !== null ? "0".repeat(64) : null,
        checkDigests: outcome === "ok" ? [{ present: true, sha256: expectedCheckDigest({ equals: [2] }) }] : null,
    });
    const records = [
        sample("warmup", 0, 0, 10, "ok", 2, 0.5),
        sample("warmup", 1, null, null, "concurrency-limit"),
        sample("measurement", 0, 0, 10, "wrong-answer", 3, 1),
        sample("measurement", 1, 50, 130, "timeout"),
        sample("measurement", 2, 100, 110, "ok", 0, 0),
        { ...sample("measurement", 3, null, null, "cancelled-before-send"), observedMs: 100 },
    ];
    const phases = ["warmup", "measurement"].map((name) => {
        const rows = records.filter((s) => s.phase === name),
            sent = rows.filter((s) => s.sentMs !== null),
            counts = {};
        for (const s of rows) counts[s.outcome] = (counts[s.outcome] ?? 0) + 1;
        return {
            name,
            durationMs: name === "warmup" ? 100 : 200,
            elapsedMs: name === "warmup" ? 110 : 210,
            scheduled: rows.length,
            sent: sent.length,
            completed: sent.length,
            dropped: rows.length - sent.length,
            failed: sent.filter((s) => s.outcome !== "ok").length,
            late: 0,
            counts,
            budgetExhausted: false,
            cancelled: false,
            averageCpuCores: 0,
            eventLoopUtilization: 0,
            eventLoopDelayMs: { p99: 0, max: 0 },
            maxRssKiB: 0,
        };
    });
    const trace = records.map((s) => JSON.stringify(s)).join("\n") + "\n";
    await put(join(raw, "profile.json"), profile);
    await put(join(raw, "target-evidence.json"), {
        schemaVersion: 1,
        kind: "operator-supplied-target-evidence",
        files: [],
    });
    await put(join(raw, "implementation.json"), { fixture: "synthetic; no runtime proof" });
    await put(join(raw, "requests/r0001.json"), {
        id: "r0001",
        repeat: 0,
        profile,
        case: profile.cases[0],
        load: profile.loads[0],
        remainingTraceBytes: profile.maxTraceBytes,
    });
    await put(join(raw, "runs/r0001.json"), {
        id: "r0001",
        caseId: "two",
        loadId: "arrival",
        repeat: 0,
        phases,
        traceBytes: Buffer.byteLength(trace),
        evidenceWriteMs: 12,
    });
    await writeFile(join(raw, "traces/r0001.ndjson"), trace);
    await put(join(raw, "raw.lock.json"), {
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
    return { root, raw, records };
}

test("driver self-cost rejects oversized/non-regular/symlink manifest before digest or analyzer reads, without output", async (t) => {
    const root = await temp(t),
        oversized = join(root, "oversized.json"),
        handle = await open(oversized, "wx");
    await handle.truncate(8 * 1024 * 1024 + 1); // sparse fixture; no large payload is read or allocated on disk
    await handle.close();
    for (const mode of ["oversized", "directory", "symlink"]) {
        const raw = join(root, mode),
            output = join(root, mode + "-report.json");
        await mkdir(raw);
        if (mode === "directory") await mkdir(join(raw, "raw.lock.json"));
        else if (mode === "symlink") await symlink(oversized, join(raw, "raw.lock.json"));
        else await cp(oversized, join(raw, "raw.lock.json"));
        await assert.rejects(reportDriverSelfCost(raw, output), (error) => {
            assert(error.code === "ELOOP" || /manifest must be regular and <=8MiB/.test(error.message));
            return true;
        });
        await assert.rejects(stat(output), { code: "ENOENT" });
    }
});

test("driver self-cost preserves complete-error work, null responses, decode subset, and all-record trace denominator", async (t) => {
    const f = await fixture(t),
        result = await analyzeDriverSelfCost(f.raw),
        run = result.runs[0],
        [warmup, measurement] = run.phases;
    assert.equal(result.performanceAccepted, false);
    assert.equal(result.overheadAccepted, false);
    assert.equal(result.quality.verdict, "quality-issues");
    assert(run.qualityIssues.includes("measurement:request-errors-or-wrong-answers"));
    assert.equal(warmup.validation.recordedTotalMs, 2);
    assert.equal(warmup.validation.meanRecordedMs, 2);
    assert.equal(measurement.validation.recordedTotalMs, 3);
    assert.equal(measurement.validation.recordedCompleteResponses, 2);
    assert.equal(measurement.validation.meanRecordedMs, 1.5);
    assert.equal(measurement.decode.recordedTotalMs, 1);
    assert.equal(measurement.decode.meanRecordedMs, 0.5);
    assert.deepEqual(measurement.validation.unavailableOrNotRecorded, {
        notSent: 1,
        incompleteResponses: 1,
        completeResponses: 0,
    });
    assert.equal(measurement.counts.completeResponseErrors, 1);
    assert.equal(measurement.counts.incompleteResponseErrors, 1);
    assert.equal(run.traceProduction.recordedRequestsIncludingWarmupAndMeasurement, 6);
    assert.equal(
        run.traceProduction.meanRecordedMsPerTraceRecord,
        2,
        "not divided by just 4 measurement records or 4 sent records",
    );
    assert.equal(run.traceBytes, (await stat(join(f.raw, "traces/r0001.ndjson"))).size);
    assert(!Object.hasOwn(measurement, "evidenceWriteMs"));
    assert(result.limits.some((s) => s.includes("subset of validation")));
    assert(result.limits.some((s) => s.includes("no validation/decode replay")));
});

test("driver self-cost absent legacy trace timing and unrecorded decode remain unavailable, invalid timing rejects", async (t) => {
    const f = await fixture(t),
        runPath = join(f.raw, "runs/r0001.json"),
        run = await read(runPath);
    delete run.evidenceWriteMs;
    await put(runPath, run);
    const records = structuredClone(f.records);
    for (const s of records) s.decodeMs = null;
    const trace = records.map((s) => JSON.stringify(s)).join("\n") + "\n";
    await writeFile(join(f.raw, "traces/r0001.ndjson"), trace);
    run.traceBytes = Buffer.byteLength(trace);
    await put(runPath, run);
    await relock(f.raw);
    const result = await analyzeDriverSelfCost(f.raw);
    assert.equal(result.runs[0].traceProduction.status, "unavailable");
    assert.equal(result.runs[0].traceProduction.meanRecordedMsPerTraceRecord, null);
    assert.equal(result.runs[0].traceProduction.reason, "legacy-missing-evidenceWriteMs");
    for (const p of result.runs[0].phases) {
        assert.equal(p.decode.recordedTotalMs, null);
        assert.equal(p.decode.meanRecordedMs, null);
        assert.equal(p.decode.unavailableOrNotRecorded.completeResponses, p.counts.completeResponses);
    }
    for (const invalid of [-1, null, "12"]) {
        run.evidenceWriteMs = invalid;
        await put(runPath, run);
        await relock(f.raw);
        await assert.rejects(analyzeDriverSelfCost(f.raw), /invalid evidenceWriteMs/);
    }
});

test("driver self-cost all-incomplete phase is explicit unavailable and zero recorded duration is preserved", async (t) => {
    const f = await fixture(t),
        records = structuredClone(f.records);
    for (const s of records.filter((s) => s.sentMs !== null))
        Object.assign(s, {
            outcome: "timeout",
            completeResponse: false,
            validationMs: null,
            decodeMs: null,
            bodySha256: null,
            checkDigests: null,
        });
    const runPath = join(f.raw, "runs/r0001.json"),
        run = await read(runPath),
        text = records.map((s) => JSON.stringify(s)).join("\n") + "\n";
    for (const p of run.phases) {
        p.counts = {};
        for (const s of records.filter((s) => s.phase === p.name)) p.counts[s.outcome] = (p.counts[s.outcome] ?? 0) + 1;
        p.failed = p.sent;
    }
    run.evidenceWriteMs = 0;
    run.traceBytes = Buffer.byteLength(text);
    await put(runPath, run);
    await writeFile(join(f.raw, "traces/r0001.ndjson"), text);
    await relock(f.raw);
    const result = await analyzeDriverSelfCost(f.raw);
    for (const p of result.runs[0].phases) {
        assert.equal(p.validation.status, "unavailable");
        assert.equal(p.validation.recordedTotalMs, null);
        assert.equal(p.validation.meanRecordedMs, null);
        assert.equal(p.counts.completeResponses, 0);
        assert.equal(p.counts.incompleteResponses, p.counts.sent);
    }
    assert.equal(result.runs[0].traceProduction.meanRecordedMsPerTraceRecord, 0);
});

test("driver self-cost rejects broken lock and resealed count/timeline corruption before output", async (t) => {
    const f = await fixture(t);
    for (const mode of ["hash", "count", "validation", "decode"]) {
        const raw = join(f.root, mode),
            output = join(f.root, mode + ".json");
        await cp(f.raw, raw, { recursive: true });
        if (mode === "hash") await writeFile(join(raw, "implementation.json"), "{}\n");
        else {
            const runPath = join(raw, "runs/r0001.json"),
                run = await read(runPath);
            if (mode === "count") run.phases[1].scheduled++;
            else {
                const records = structuredClone(f.records);
                if (mode === "validation") records[0].validationMs = -1;
                else records[0].decodeMs = records[0].validationMs + 1;
                const text = records.map((s) => JSON.stringify(s)).join("\n") + "\n";
                await writeFile(join(raw, "traces/r0001.ndjson"), text);
                run.traceBytes = Buffer.byteLength(text);
            }
            await put(runPath, run);
            await relock(raw);
        }
        await assert.rejects(
            reportDriverSelfCost(raw, output),
            /size mismatch|hash mismatch|conservation|validation extends|decoding duration/,
        );
        await assert.rejects(stat(output), { code: "ENOENT" });
    }
});

test("driver self-cost fixed CLI writes deterministic relocatable JSON, never overwrites and documents offline limits", async (t) => {
    const f = await fixture(t),
        component = resolve("components/http/driver-self-cost.mjs"),
        first = join(f.root, "first.json"),
        second = join(f.root, "second.json"),
        moved = join(f.root, "moved");
    assert.match((await exec(process.execPath, [component, "--help"])).stdout, /No stored bodies\/replay/);
    await exec(process.execPath, [component, "--input", f.raw, "--output", first]);
    await cp(f.raw, moved, { recursive: true });
    await rm(f.raw, { recursive: true });
    await exec(process.execPath, [component, "--input", moved, "--output", second]);
    const original = await readFile(first);
    assert.deepEqual(original, await readFile(second));
    await assert.rejects(exec(process.execPath, [component, "--input", moved, "--output", first]), (error) => {
        assert.equal(error.code, 6);
        assert.equal(JSON.parse(error.stderr).kind, "http-component-error");
        return true;
    });
    assert.deepEqual(original, await readFile(first));
});

test("driver self-cost public CLI returns strict paths, survives raw relocation and rejects overwrite or corruption", async (t) => {
    const { root, raw } = await fixture(t),
        output = join(root, "public-cost.json"),
        args = [resolve("dist/cli.js"), "http", "calibration", "driver-cost"],
        call = await exec(process.execPath, [...args, raw, "--out", output], { timeout: 15000 }),
        reply = JSON.parse(call.stdout);
    assert.equal(reply.ok, true);
    assert.equal(reply.command, "http.calibration.driver-cost");
    assert.equal(reply.data.reportJson, output);
    assert.equal(reply.data.overheadAccepted, false);
    assert.equal(reply.data.performanceAccepted, false);
    assert.deepEqual(reply.nextActions, [{ kind: "read", path: output, mediaType: "application/json" }]);
    const original = await readFile(output);
    await assert.rejects(exec(process.execPath, [...args, raw, "--out", output]), (error) => {
        assert.equal(error.code, 6);
        assert.equal(JSON.parse(error.stdout).ok, false);
        return true;
    });
    assert.deepEqual(await readFile(output), original);
    const moved = join(root, "moved-raw"),
        second = join(root, "moved-cost.json");
    await cp(raw, moved, { recursive: true });
    await rm(raw, { recursive: true });
    await exec(process.execPath, [...args, moved, "--out", second]);
    assert.deepEqual(await readFile(second), original);
    await writeFile(join(moved, "traces/r0001.ndjson"), "broken evidence\n");
    const bad = join(root, "bad-cost.json");
    await assert.rejects(exec(process.execPath, [...args, moved, "--out", bad]), (error) => {
        assert.equal(error.code, 6);
        assert.equal(JSON.parse(error.stdout).error.code, "HTTP_CALIBRATION_FAILED");
        return true;
    });
    await assert.rejects(stat(bad), { code: "ENOENT" });
    const help = await exec(process.execPath, [...args, "--help"]);
    assert.match(help.stdout, /no HTTP, Docker or replay/);
    assert.match(help.stdout, /warmup\+measurement/);
});

test("driver self-cost independently sums actual gzip HTTP raw, including wrong answers and missing responses", async (t) => {
    const root = await temp(t);
    let requests = 0;
    const server = http.createServer((request, response) => {
        request.resume();
        requests++;
        if (requests % 4 === 0) return;
        const data = requests % 3 === 0 ? [3] : [2];
        response.setHeader("Content-Encoding", "gzip");
        response.end(gzipSync(JSON.stringify({ result: { data } })));
    });
    await new Promise((done) => server.listen(0, "127.0.0.1", done));
    t.after(async () => {
        server.closeAllConnections();
        await new Promise((done) => server.close(done));
    });
    const { planHttp, httpProfileSchema } = await import("../dist/http.js");
    const profile = httpProfileSchema.parse({
        schemaVersion: 1,
        kind: "http-benchmark-profile",
        name: "actual-http-driver-cost",
        endpoint: `http://127.0.0.1:${server.address().port}/gremlin`,
        responseEncoding: "gzip",
        serviceChecks: false,
        target: {
            label: "local",
            build: "fixture",
            mode: "control",
            environment: "functional only",
            evidenceFiles: [],
        },
        cases: [{ id: "two", body: { gremlin: "1+1" }, checks: [{ pointer: "/result/data", equals: [2] }] }],
        loads: [{ id: "arrival", model: "open", ratePerSecond: 20, maxInFlight: 2, maxStartDelayMs: 1000 }],
        repeats: 1,
        warmupMs: 100,
        durationMs: 200,
        requestTimeoutMs: 80,
        drainMs: 100,
        maxRequestsPerPhase: 100,
        maxResponseBytes: 4096,
        maxTraceBytes: 1048576,
    });
    const input = join(root, "profile.json"),
        plan = join(root, "plan");
    await put(input, profile);
    await planHttp(input, plan);
    await exec(process.execPath, [resolve("components/http/workflow.mjs"), "--plan", plan], { timeout: 15000 });
    const raw = join(plan, "output"),
        result = await analyzeDriverSelfCost(raw),
        run = result.runs[0],
        source = await read(join(raw, "runs/r0001.json")),
        records = (await readFile(join(raw, "traces/r0001.ndjson"), "utf8")).trim().split("\n").map(JSON.parse),
        before = requests;
    assert(records.some((s) => s.outcome === "wrong-answer"));
    assert(records.some((s) => !s.completeResponse && s.sentMs !== null));
    for (const phase of run.phases) {
        const complete = records.filter((s) => s.phase === phase.name && s.completeResponse),
            decode = complete.filter((s) => s.decodeMs !== null);
        assert.equal(
            phase.validation.recordedTotalMs,
            complete.length ? complete.reduce((n, s) => n + s.validationMs, 0) : null,
        );
        assert.equal(phase.decode.recordedTotalMs, decode.length ? decode.reduce((n, s) => n + s.decodeMs, 0) : null);
        assert.equal(phase.counts.completeResponses, complete.length);
    }
    assert.equal(run.traceProduction.meanRecordedMsPerTraceRecord, null);
    assert.equal(run.traceProduction.reason, "async-writer-costs-reported-separately");
    assert.deepEqual(run.traceProduction.asynchronousWriter, source.traceWriter);
    assert.equal(source.traceWriter.records, records.length);
    assert.equal(source.traceWriter.complete, true);
    assert(!Object.hasOwn(source, "evidenceWriteMs"));
    assert.equal(run.traceProduction.recordedRequestsIncludingWarmupAndMeasurement, records.length);
    assert.equal(result.quality.verdict, "quality-issues");
    await reportDriverSelfCost(raw, join(root, "actual-cost.json"));
    assert.equal(requests, before, "consumer makes no target calls or pseudo-replay");
});
