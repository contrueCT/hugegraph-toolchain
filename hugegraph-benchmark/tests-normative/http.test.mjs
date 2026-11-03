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
import { mkdtemp, readFile, writeFile, rm, appendFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { run } from "../components/http/runner.mjs";
import { TraceWriter } from "../components/http/trace-writer.mjs";
import { analyze, report } from "../components/http/analyzer.mjs";
import { digest } from "../components/http/common.mjs";
import { planHttp, inspectHttpProfile, httpProfileSchema } from "../dist/http.js";
import { submitTask, waitTask } from "../dist/tasks.js";

async function fixture(t, handler) {
    const root = await mkdtemp(join(tmpdir(), "hgbench-http-"));
    const server = http.createServer(handler);
    await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
    t.after(async () => {
        server.closeAllConnections();
        await new Promise((resolve) => server.close(resolve));
        await rm(root, { recursive: true, force: true });
    });
    return { root, endpoint: `http://127.0.0.1:${server.address().port}/gremlin` };
}
function profile(endpoint, patch = {}) {
    return httpProfileSchema.parse({
        schemaVersion: 1,
        kind: "http-benchmark-profile",
        name: "transport-test",
        endpoint,
        target: { label: "controlled server", build: "test", mode: "control", environment: "tool validation" },
        cases: [{ id: "one", body: { gremlin: "1+1" }, checks: [{ pointer: "/result/data", equals: [2] }] }],
        loads: [{ id: "closed-2", model: "closed", concurrency: 2 }],
        repeats: 1,
        warmupMs: 0,
        durationMs: 140,
        requestTimeoutMs: 100,
        drainMs: 120,
        maxRequestsPerPhase: 10000,
        maxResponseBytes: 4096,
        maxTraceBytes: 1048576,
        ...patch,
    });
}
async function execute(f, p, name = "trace") {
    const path = join(f.root, name + ".ndjson");
    const result = await run({ profile: p, case: p.cases[0], load: p.loads[0], id: "r0001", repeat: 0 }, path);
    const records = (await readFile(path, "utf8")).trim().split("\n").filter(Boolean).map(JSON.parse);
    return { result, records, phase: result.phases.find((p) => p.name === "measurement") };
}

test("HTTP cancellation during final asynchronous trace close cannot return an uncancelled success", async (t) => {
    const f = await fixture(t, (req, res) => {
        req.resume();
        res.end('{"result":{"data":[2]}}');
    });
    const p = profile(f.endpoint, { durationMs: 100 });
    const original = TraceWriter.prototype.close;
    let closing, release;
    const started = new Promise((r) => {
        closing = r;
    });
    const gate = new Promise((r) => {
        release = r;
    });
    TraceWriter.prototype.close = async function () {
        closing();
        await gate;
        return original.call(this);
    };
    try {
        const pending = execute(f, p);
        await started;
        process.emit("SIGTERM");
        release();
        await assert.rejects(pending, /cancelled during trace finalization/);
    } finally {
        release();
        TraceWriter.prototype.close = original;
    }
});

test("HTTP runner measures the complete delayed body and reuses sockets; validation is separate", async (t) => {
    let requests = 0;
    const f = await fixture(t, (req, res) => {
        requests++;
        req.resume();
        res.writeHead(200, { "Content-Type": "application/json" });
        res.write('{"result":');
        setTimeout(() => res.end('{"data":[2]}}'), 30);
    });
    const p = profile(f.endpoint, { loads: [{ id: "c1", model: "closed", concurrency: 1 }], durationMs: 220 });
    const { records, phase } = await execute(f, p);
    assert.equal(phase.sent, requests);
    assert.equal(phase.failed, 0);
    assert(phase.sent >= 2);
    for (const r of records) {
        assert.equal(r.outcome, "ok");
        assert(r.endMs - r.sentMs >= 25);
        assert(r.validationMs >= 0);
        assert(r.checkDigests[0].present);
    }
    assert(records.slice(1).some((r) => r.reusedSocket));
});

test("HTTP open arrivals remain conserved when concurrency is saturated, including drain expiry", async (t) => {
    const f = await fixture(t, (req, res) => {
        req.resume();
        setTimeout(() => res.end('{"result":{"data":[2]}}'), 500);
    });
    const p = profile(f.endpoint, {
        loads: [{ id: "open", model: "open", ratePerSecond: 100, maxInFlight: 1, maxStartDelayMs: 80 }],
        requestTimeoutMs: 1000,
        drainMs: 30,
        durationMs: 100,
    });
    const { records, phase } = await execute(f, p);
    assert.equal(phase.scheduled, 10);
    assert.equal(phase.sent + phase.dropped, 10);
    assert.equal(phase.sent, phase.completed);
    assert.equal(phase.sent, 1);
    assert(phase.dropped >= 8);
    assert.equal(phase.counts["drain-timeout"], 1);
    assert.equal(new Set(records.map((r) => r.sequence)).size, 10);
    assert(records.filter((r) => r.sentMs === null).every((r) => r.endMs === null));
});

test("HTTP faults are explicit: no redirect/retry, timeout, wrong answer, malformed/oversized/encoded body", async (t) => {
    let mode = "wrong",
        requests = 0,
        redirectHits = 0;
    const f = await fixture(t, (req, res) => {
        req.resume();
        requests++;
        if (req.url === "/redirect-target") redirectHits++;
        if (mode === "wrong") res.end('{"result":{"data":[3]}}');
        if (mode === "invalid") res.end("{");
        if (mode === "redirect") {
            res.writeHead(307, { Location: "/redirect-target" });
            res.end("{}");
        }
        if (mode === "timeout") setTimeout(() => res.end("{}"), 300);
        if (mode === "oversize") res.end("x".repeat(6000));
        if (mode === "encoding") {
            res.setHeader("Content-Encoding", "gzip");
            res.end('{"result":{"data":[2]}}');
        }
    });
    for (const [name, outcome] of [
        ["wrong", "wrong-answer"],
        ["invalid", "invalid-json"],
        ["redirect", "http-status"],
        ["timeout", "timeout"],
        ["oversize", "response-too-large"],
        ["encoding", "unexpected-encoding"],
    ]) {
        mode = name;
        requests = 0;
        const p = profile(f.endpoint, { durationMs: 100, requestTimeoutMs: 30, maxRequestsPerPhase: 2 });
        const result = await execute(f, p, name);
        assert.equal(result.phase.sent, requests);
        assert.equal(result.phase.counts[outcome], 2, JSON.stringify(result.phase));
        assert.equal(result.phase.failed, 2);
    }
    assert.equal(redirectHits, 0);
});

test("HTTP absolute deadlines include partial bodies; aborted responses cannot pass correctness", async (t) => {
    const f = await fixture(t, (req, res) => {
        req.resume();
        res.writeHead(200, { "Content-Type": "application/json" });
        res.write('{"result":');
        setTimeout(() => res.destroy(), 15);
    });
    const { records, phase } = await execute(f, profile(f.endpoint, { maxRequestsPerPhase: 2 }));
    assert.equal(phase.failed, 2);
    assert(records.every((r) => !r.completeResponse && r.outcome.startsWith("response-")));
});

test("HTTP trace budget failure leaves partial evidence without a successful result", async (t) => {
    const f = await fixture(t, (req, res) => {
        req.resume();
        res.end('{"result":{"data":[2]}}');
    });
    const p = profile(f.endpoint);
    await assert.rejects(
        run(
            { profile: p, case: p.cases[0], load: p.loads[0], id: "r0001", repeat: 0, remainingTraceBytes: 100 },
            join(f.root, "bounded.ndjson"),
        ),
        /Trace byte budget/,
    );
    assert.equal((await readFile(join(f.root, "bounded.ndjson"))).length, 0);
});

test("HTTP profile validation rejects duplicated identities, unsafe endpoint credentials and impossible budgets", async (t) => {
    const f = await fixture(t, (req, res) => res.end());
    const p = profile(f.endpoint),
        file = join(f.root, "profile.json");
    for (const [patch, code] of [
        [{ cases: [p.cases[0], p.cases[0]] }, "HTTP_DUPLICATE_ID"],
        [{ endpoint: "http://user:secret@127.0.0.1/gremlin" }, "HTTP_ENDPOINT_INVALID"],
        [
            {
                loads: [{ id: "open", model: "open", ratePerSecond: 1000, maxInFlight: 1, maxStartDelayMs: 10 }],
                maxRequestsPerPhase: 10,
            },
            "HTTP_REQUEST_BUDGET",
        ],
        [
            {
                cases: Array.from({ length: 16 }, (_, i) => ({ ...p.cases[0], id: `c${i}` })),
                loads: [
                    { id: "a", model: "closed", concurrency: 1 },
                    { id: "b", model: "closed", concurrency: 1 },
                ],
                repeats: 20,
            },
            "HTTP_METADATA_BUDGET",
        ],
    ]) {
        await writeFile(file, JSON.stringify({ ...p, ...patch }));
        await assert.rejects(inspectHttpProfile(file), (e) => e.code === code);
    }
});

test("HTTP CLI/task workflow locks real traffic, regenerates identical reports and rejects corrupt or lost samples", async (t) => {
    const f = await fixture(t, (req, res) => {
        req.resume();
        res.end('{"result":{"data":[2]}}');
    });
    const file = join(f.root, "profile.json"),
        dir = join(f.root, "attempt");
    const p = profile(f.endpoint, {
        name: "safe-name",
        loads: [{ id: "arrival", model: "open", ratePerSecond: 40, maxInFlight: 2, maxStartDelayMs: 100 }],
        warmupMs: 100,
        durationMs: 150,
        repeats: 2,
    });
    // Untrusted target descriptions must render as text, never executable markup.
    p.target.label = "</script><script>bad()</script>";
    await writeFile(file, JSON.stringify(p));
    const cli = promisify(execFile);
    const planned = JSON.parse(
        (await cli(process.execPath, [resolve("dist/cli.js"), "http", "plan", file, "--out", dir])).stdout,
    ).data;
    const frozen = JSON.parse(await readFile(planned.taskPlan, "utf8"));
    const submission = await submitTask(join(f.root, "tasks"), frozen, "http-once");
    const result = await waitTask(join(f.root, "tasks"), submission.taskId, 20000);
    assert.equal(result.state, "completed", JSON.stringify(result));
    const again = await submitTask(join(f.root, "tasks"), frozen, "http-once");
    assert(again.reused);
    const output = join(dir, "output"),
        report1 = JSON.parse(await readFile(join(output, "analysis/report.json"), "utf8"));
    assert.equal(report1.runs.length, 2);
    assert.equal(report1.verdict, "descriptive-only");
    assert.equal(report1.performanceAccepted, false);
    assert.equal(report1.gates.correctness, "passed");
    assert(report1.runs.every((r) => r.scheduled === 6 && r.correct === 6));
    await cli(process.execPath, [resolve("dist/cli.js"), "http", "report", output, "--out", join(f.root, "rebuilt")]);
    assert.equal(
        await readFile(join(output, "analysis/report.json"), "utf8"),
        await readFile(join(f.root, "rebuilt/report.json"), "utf8"),
    );
    const html = await readFile(join(f.root, "rebuilt/report.html"), "utf8");
    assert(!html.includes("</script><script>bad()"));
    assert(html.includes("Content-Security-Policy"));
    await assert.rejects(planHttp(file, dir), (e) => e.code === "EEXIST");
    const trace = join(output, "traces/r0001.ndjson"),
        original = await readFile(trace, "utf8");
    await appendFile(trace, " ");
    await assert.rejects(analyze(output), /size mismatch/);
    await writeFile(trace, original.split("\n").slice(1).join("\n"));
    const lockFile = join(output, "raw.lock.json"),
        lock = JSON.parse(await readFile(lockFile, "utf8"));
    const entry = lock.files.find((f) => f.path === "traces/r0001.ndjson");
    entry.bytes = Buffer.byteLength(await readFile(trace, "utf8"));
    entry.sha256 = await digest(trace);
    await writeFile(lockFile, JSON.stringify(lock));
    await assert.rejects(analyze(output), /count conservation/);
});

test("HTTP gzip is explicitly negotiated, decoded after timing, and bounded against decompression expansion", async (t) => {
    let payload = JSON.stringify({ result: { data: [2] } });
    const f = await fixture(t, (req, res) => {
        req.resume();
        assert.equal(req.headers["accept-encoding"], "gzip");
        res.setHeader("Content-Encoding", "gzip");
        res.end(gzipSync(payload));
    });
    const p = profile(f.endpoint, { responseEncoding: "gzip", maxRequestsPerPhase: 2 });
    const ok = await execute(f, p, "gzip-ok");
    assert.equal(ok.phase.failed, 0);
    assert(
        ok.records.every(
            (r) => r.decodeMs >= 0 && r.validationMs >= r.decodeMs && r.decodedBytes === Buffer.byteLength(payload),
        ),
    );
    payload = "x".repeat(10000);
    const large = await execute(f, p, "gzip-large");
    assert.equal(large.phase.counts["decoded-response-too-large"], 2);
});

test("HTTP preflight fails closed before measurement and preserves the one-request diagnostic", async (t) => {
    let requests = 0;
    const f = await fixture(t, (req, res) => {
        requests++;
        req.resume();
        res.end('{"result":{"data":[999]}}');
    });
    const file = join(f.root, "profile.json"),
        attempt = join(f.root, "attempt");
    const p = profile(f.endpoint, { serviceChecks: true });
    await writeFile(file, JSON.stringify(p));
    const plan = await planHttp(file, attempt),
        frozen = JSON.parse(await readFile(plan.taskPlan, "utf8"));
    const task = await submitTask(join(f.root, "tasks"), frozen, "preflight-fail");
    const terminal = await waitTask(join(f.root, "tasks"), task.taskId, 15000);
    assert.equal(terminal.state, "failed");
    assert.equal(requests, 1);
    const result = JSON.parse(await readFile(join(attempt, "output/preflight/result.json"), "utf8"));
    assert.equal(result.passed, false);
    const trace = JSON.parse((await readFile(join(attempt, "output/preflight/p0001.ndjson"), "utf8")).trim());
    assert.equal(trace.outcome, "wrong-answer");
    await assert.rejects(readFile(join(attempt, "output/raw.lock.json")), /ENOENT/);
    await assert.rejects(readFile(join(attempt, "output/runs/r0001.json")), /ENOENT/);
});

test("HTTP postcheck rejects changed answers after correct measurement and preserves partial evidence", async (t) => {
    let requests = 0;
    const f = await fixture(t, (req, res) => {
        requests++;
        req.resume();
        res.end(JSON.stringify({ result: { data: [requests <= 3 ? 2 : 3] } }));
    });
    const file = join(f.root, "profile.json"),
        attempt = join(f.root, "attempt");
    const p = profile(f.endpoint, {
        serviceChecks: true,
        durationMs: 1000,
        requestTimeoutMs: 1000,
        loads: [{ id: "two", model: "open", ratePerSecond: 2, maxInFlight: 1, maxStartDelayMs: 1000 }],
    });
    await writeFile(file, JSON.stringify(p));
    const plan = await planHttp(file, attempt),
        frozen = JSON.parse(await readFile(plan.taskPlan, "utf8"));
    const task = await submitTask(join(f.root, "tasks"), frozen, "postcheck-fail");
    const terminal = await waitTask(join(f.root, "tasks"), task.taskId, 15000);
    assert.equal(terminal.state, "failed");
    assert.equal(requests, 4);
    const measured = JSON.parse(await readFile(join(attempt, "output/runs/r0001.json"), "utf8"));
    assert.equal(measured.phases[0].counts.ok, 2);
    const result = JSON.parse(await readFile(join(attempt, "output/postcheck/result.json"), "utf8"));
    assert.equal(result.passed, false);
    await assert.rejects(readFile(join(attempt, "output/raw.lock.json")), /ENOENT/);
    await assert.rejects(readFile(join(attempt, "output/completed.json")), /ENOENT/);
});
