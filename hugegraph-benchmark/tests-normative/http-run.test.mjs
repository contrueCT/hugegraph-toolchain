/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import http from "node:http";
import { mkdtemp, rm, readFile, writeFile, stat, appendFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { registerHttpGo } from "../dist/http-go.js";
import { runHttpProfile } from "../dist/http-run.js";
import { cancelTask, waitTask } from "../dist/tasks.js";
const exec = promisify(execFile);
async function fixture(t) {
    const directory = await mkdtemp(join(tmpdir(), "hgbench-http-run-"));
    t.after(() => rm(directory, { recursive: true, force: true }));
    let requests = 0;
    const server = http.createServer((req, res) => {
        requests++;
        req.resume();
        res.end(JSON.stringify({ result: { data: [42] } }));
    });
    await new Promise((r) => server.listen(0, "127.0.0.1", r));
    t.after(
        () =>
            new Promise((r) => {
                server.closeAllConnections();
                server.close(r);
            }),
    );
    const profile = {
        schemaVersion: 1,
        kind: "http-benchmark-profile",
        name: "one-command",
        endpoint: `http://127.0.0.1:${server.address().port}/gremlin`,
        target: {
            label: "Protocol fixture",
            build: "development",
            mode: "fixture",
            environment: "No graph performance result",
        },
        cases: [{ id: "one", body: { gremlin: "17+25" }, checks: [{ pointer: "/result/data", equals: [42] }] }],
        loads: [{ id: "open-10", model: "open", ratePerSecond: 10, maxInFlight: 2, maxStartDelayMs: 1000 }],
        repeats: 1,
        warmupMs: 0,
        durationMs: 200,
        requestTimeoutMs: 1000,
        drainMs: 1000,
        maxRequestsPerPhase: 1000,
        maxResponseBytes: 4096,
        maxTraceBytes: 1048576,
    };
    const file = join(directory, "profile.json");
    await writeFile(file, JSON.stringify(profile));
    return { directory, file, profile, requests: () => requests };
}
test("one-command CLI performs a real HTTP run and exposes the existing report; repeated output cannot redispatch", async (t) => {
    const f = await fixture(t),
        out = join(f.directory, "run"),
        root = join(f.directory, "state");
    const args = [resolve("dist/cli.js"), "--root", root, "http", "run", f.file, "--out", out, "--request-id", "one"];
    const r = JSON.parse((await exec(process.execPath, args, { timeout: 30000 })).stdout);
    assert.equal(r.ok, true);
    assert.equal(r.data.task.state, "completed");
    assert.equal(r.data.summary.reportAvailable, true);
    assert.equal(r.data.summary.runs[0].correct, 2);
    assert.equal(r.data.summary.performanceAccepted, false);
    assert.equal(f.requests(), 2);
    const saved = JSON.parse(await readFile(join(out, "run.json"))),
        report = JSON.parse(await readFile(saved.reportJson));
    assert.equal(report.runs[0].correct, 2);
    assert.equal(report.performanceAccepted, false);
    assert.match(await readFile(saved.reportHtml, "utf8"), /html/i);
    assert.equal(saved.taskId, r.data.taskId);
    assert.equal(saved.rawDirectory, join(out, "evidence"));
    for (const removed of ["intent.json", "plan", "evidence/task.json", "evidence/task.source.json", "evidence/output"])
        await assert.rejects(stat(join(out, removed)), { code: "ENOENT" });
    await assert.rejects(
        exec(process.execPath, args, { timeout: 10000 }),
        (e) => JSON.parse(e.stdout).error.code === "HTTP_RUN_EXISTS",
    );
    assert.equal(f.requests(), 2);
});
test("one-command dry-run makes no files or HTTP calls", async (t) => {
    const f = await fixture(t),
        out = join(f.directory, "dry"),
        root = join(f.directory, "state");
    const preview = await runHttpProfile(f.file, { out, root, dryRun: true });
    assert.equal(preview.dispatched, false);
    assert.equal(preview.fixedWindowMs, 200);
    assert.equal(f.requests(), 0);
    await assert.rejects(stat(out), { code: "ENOENT" });
    await assert.rejects(stat(root), { code: "ENOENT" });
});
test("one-command detached run uses the original task cancellation and preserves its run identity", async (t) => {
    const f = await fixture(t),
        out = join(f.directory, "cancel"),
        root = join(f.directory, "state");
    f.profile.durationMs = 30000;
    f.profile.cases.push({ ...f.profile.cases[0], id: "second" });
    await writeFile(f.file, JSON.stringify(f.profile));
    const run = await runHttpProfile(f.file, { out, root, requestId: "cancel" });
    assert(run.taskId);
    const end = Date.now() + 5000;
    try {
        while (!f.requests() && Date.now() < end) await new Promise((r) => setTimeout(r, 10));
        assert(f.requests() > 0, "cancellation exercises an active real HTTP workload");
    } finally {
        await cancelTask(root, run.taskId);
    }
    let state = await waitTask(root, run.taskId, 1000);
    while (!state.terminal) state = await waitTask(root, run.taskId, 1000);
    assert.equal(state.state, "cancelled");
    assert.equal(JSON.parse(await readFile(join(out, "run.json"))).taskId, run.taskId);
    const report = JSON.parse(await readFile(run.reportJson));
    assert.equal(report.verdict, "partial");
    assert.equal(report.execution.plannedRuns, 2);
    assert.equal(report.execution.missingRuns.length, 1);
    assert.equal(report.gates.sampleAccounting, "partial");
    assert.equal(report.performanceAccepted, false);
});
test("one-command entry preserves explicit Go host observation requirements before writes or requests", async (t) => {
    const f = await fixture(t),
        out = join(f.directory, "held");
    Object.assign(f.profile, {
        schemaVersion: 2,
        executor: {
            engine: "go-http-v1",
            hostIdentityGate: true,
            binary: {
                path: "/registered/go",
                sha256: "a".repeat(64),
                sourceSha256: "b".repeat(64),
                goVersion: "go1.27.3",
                goos: "linux",
                goarch: "amd64",
            },
            gomaxprocs: 4,
            cpuQuotaCores: 4,
            maxRssBytes: 3221225472,
            maxPendingBytes: 1048576,
        },
    });
    await writeFile(f.file, JSON.stringify(f.profile));
    await assert.rejects(runHttpProfile(f.file, { out, root: join(f.directory, "state") }), /host identity handshakes/);
    assert.equal(f.requests(), 0);
    await assert.rejects(stat(out), { code: "ENOENT" });
});

test(
    "one-command entry uses the real registered Go executor and preserves raw v3 reporting",
    { skip: !process.env.HGBENCH_GO_BINARY },
    async (t) => {
        const f = await fixture(t),
            go = join(f.directory, "go.json"),
            out = join(f.directory, "go-run");
        await registerHttpGo(f.file, process.env.HGBENCH_GO_BINARY, go, {
            gomaxprocs: 4,
            cpuQuotaCores: 4,
            maxRssBytes: 3221225472,
        });
        const reply = JSON.parse(
            (
                await exec(
                    process.execPath,
                    [resolve("dist/cli.js"), "--root", join(f.directory, "state"), "http", "run", go, "--out", out],
                    { timeout: 30000 },
                )
            ).stdout,
        );
        assert.equal(reply.ok, true);
        assert.equal(reply.data.task.state, "completed");
        assert.equal(reply.data.engine, "go-http-v1");
        const run = JSON.parse(await readFile(join(out, "run.json"))),
            report = JSON.parse(await readFile(run.reportJson));
        assert.equal(report.engine, "go-http-v1");
        assert.equal(report.runs[0].correct, 2);
        assert.equal(f.requests(), 2);
        assert.equal(JSON.parse(await readFile(join(run.rawDirectory, "raw.lock.json"))).schemaVersion, 3);
        assert.equal(report.performanceAccepted, false);
    },
);

test("whole execution deadline retains a partial report and the complete planned denominator", async (t) => {
    const f = await fixture(t);
    f.profile.durationMs = 30000;
    f.profile.cases.push({ ...f.profile.cases[0], id: "second" });
    await writeFile(f.file, JSON.stringify(f.profile));
    const root = join(f.directory, "state");
    const started = Date.now();
    const run = await runHttpProfile(f.file, {
        out: join(f.directory, "budget"),
        root,
        budgetMs: 2000,
        requestId: "budget",
    });
    let state = await waitTask(root, run.taskId, 1000);
    while (!state.terminal) state = await waitTask(root, run.taskId, 1000);
    assert.equal(state.state, "failed");
    assert.equal(state.error.code, "TASK_TIMEOUT");
    assert(Date.now() - started < 6000, "bounded task cleanup exceeded expected local allowance");
    const report = JSON.parse(await readFile(run.reportJson));
    assert.equal(report.verdict, "partial");
    assert.equal(report.execution.plannedRuns, 2);
    // On a slower host the deadline may expire before the first runner starts.
    assert(report.execution.missingRuns.length >= 1 && report.execution.missingRuns.length <= 2);
    assert.equal(report.runs.length + report.execution.missingRuns.length, 2);
});

test("stage-sum rejection and corrupt raw evidence cannot become a successful report", async (t) => {
    const f = await fixture(t),
        root = join(f.directory, "state"),
        out = join(f.directory, "bad-budget");
    await assert.rejects(
        runHttpProfile(f.file, { out, root, budgetMs: 1000, budgetMode: "stage-sum" }),
        (e) => e.code === "HTTP_RUN_BUDGET",
    );
    await assert.rejects(stat(out), { code: "ENOENT" });
    assert.equal(f.requests(), 0);
    const run = await runHttpProfile(f.file, { out: join(f.directory, "normal"), root });
    let state = await waitTask(root, run.taskId, 1000);
    while (!state.terminal) state = await waitTask(root, run.taskId, 1000);
    assert.equal(state.state, "completed");
    await appendFile(join(run.rawDirectory, "traces/r0001.ndjson"), "{}\n");
    await assert.rejects(
        exec(process.execPath, [
            resolve("dist/cli.js"),
            "http",
            "report",
            run.rawDirectory,
            "--out",
            join(f.directory, "corrupt-report"),
        ]),
    );
});

test("failed preflight produces an honest partial report without measurements", async (t) => {
    const f = await fixture(t),
        root = join(f.directory, "state");
    f.profile.serviceChecks = true;
    f.profile.cases[0].checks[0].equals = [99];
    await writeFile(f.file, JSON.stringify(f.profile));
    const run = await runHttpProfile(f.file, { out: join(f.directory, "wrong"), root });
    let state = await waitTask(root, run.taskId, 1000);
    while (!state.terminal) state = await waitTask(root, run.taskId, 1000);
    assert.equal(state.state, "failed");
    const report = JSON.parse(await readFile(run.reportJson));
    assert.equal(report.verdict, "partial");
    assert.equal(report.runs.length, 0);
    assert.equal(report.execution.missingRuns.length, 1);
    assert.notEqual(report.gates.correctness, "passed");
    assert.equal(f.requests(), 1);
});

test(
    "Go execution session keeps native identity enforcement before HTTP",
    { skip: !process.env.HGBENCH_GO_BINARY },
    async (t) => {
        const f = await fixture(t),
            registered = join(f.directory, "go.json");
        await registerHttpGo(f.file, process.env.HGBENCH_GO_BINARY, registered, {
            gomaxprocs: 4,
            cpuQuotaCores: 4,
            maxRssBytes: 3221225472,
        });
        const p = JSON.parse(await readFile(registered));
        const { createGoExecution } = await import("../components/http/go-engine.mjs");
        const execute = await createGoExecution(p);
        const bad = structuredClone(p);
        bad.executor.binary.sha256 = "f".repeat(64);
        const request = join(f.directory, "bad.request.json");
        await writeFile(
            request,
            JSON.stringify({
                schemaVersion: 2,
                id: "r0001",
                profile: bad,
                case: bad.cases[0],
                load: bad.loads[0],
                repeat: 0,
                remainingTraceBytes: bad.maxTraceBytes,
            }),
        );
        await assert.rejects(
            execute([
                "--request",
                request,
                "--trace",
                join(f.directory, "bad.ndjson"),
                "--output",
                join(f.directory, "bad.result.json"),
            ]),
        );
        assert.equal(f.requests(), 0);
    },
);

for (const warmupMs of [0, 30000])
    test(
        `real Go cancellation preserves ${warmupMs ? "unfinished warmup" : "measurement"} and a partial report`,
        { skip: !process.env.HGBENCH_GO_BINARY },
        async (t) => {
            const f = await fixture(t),
                root = join(f.directory, "state"),
                registered = join(f.directory, "go.json");
            f.profile.durationMs = 30000;
            f.profile.warmupMs = warmupMs;
            f.profile.cases.push({ ...f.profile.cases[0], id: "second" });
            await writeFile(f.file, JSON.stringify(f.profile));
            await registerHttpGo(f.file, process.env.HGBENCH_GO_BINARY, registered, {
                gomaxprocs: 4,
                cpuQuotaCores: 4,
                maxRssBytes: 3221225472,
            });
            const run = await runHttpProfile(registered, { out: join(f.directory, "cancel"), root });
            const deadline = Date.now() + 5000;
            try {
                while (!f.requests() && Date.now() < deadline) await new Promise((r) => setTimeout(r, 10));
                assert(f.requests() > 0);
            } finally {
                await cancelTask(root, run.taskId);
            }
            let state = await waitTask(root, run.taskId, 1000);
            while (!state.terminal) state = await waitTask(root, run.taskId, 1000);
            assert.equal(state.state, "cancelled");
            const report = JSON.parse(await readFile(run.reportJson));
            assert.equal(report.verdict, "partial");
            assert.equal(report.execution.plannedRuns, 2);
            assert.equal(report.runs.length, warmupMs ? 0 : 1);
            assert.equal(report.execution.missingRuns.length, warmupMs ? 2 : 1);
            assert.equal(report.execution.unfinishedFiles.length, warmupMs ? 3 : 0);
            assert.equal(report.performanceAccepted, false);
        },
    );

test(
    "Go failure wrapper retains root error and unfinished identity without counting its nested measurement",
    { skip: !process.env.HGBENCH_GO_BINARY, timeout: 30000 },
    async (t) => {
        const f = await fixture(t),
            root = join(f.directory, "state"),
            out = join(f.directory, "writer-failure");
        Object.assign(f.profile, {
            maxTraceBytes: 4096,
            durationMs: 500,
            repeats: 2,
            loads: [{ id: "closed-one", model: "closed", concurrency: 1 }],
        });
        await writeFile(f.file, JSON.stringify(f.profile));
        const go = join(f.directory, "go.json");
        await registerHttpGo(f.file, process.env.HGBENCH_GO_BINARY, go, {
            gomaxprocs: 2,
            cpuQuotaCores: 2,
            maxRssBytes: 1073741824,
        });
        const run = await runHttpProfile(go, { out, root });
        let task = await waitTask(root, run.taskId, 1000);
        while (!task.terminal) task = await waitTask(root, run.taskId, 1000);
        assert.equal(task.state, "failed");
        const raw = JSON.parse(await readFile(join(run.rawDirectory, "runs/r0001.json")));
        assert.equal(raw.kind, "http-go-run-failure");
        assert.match(raw.error, /trace persistence: trace byte budget exhausted/);
        assert.equal(raw.result.phases[0].name, "measurement");
        const lock = JSON.parse(await readFile(join(run.rawDirectory, "raw.lock.json")));
        assert.deepEqual(lock.runs, []);
        assert.match(lock.execution.reason, /trace persistence: trace byte budget exhausted/);
        assert.doesNotMatch(lock.execution.reason, /TypeError/);
        assert.deepEqual(lock.execution.unfinished, {
            id: "r0001",
            caseId: "one",
            loadId: "closed-one",
            repeat: 0,
            result: "runs/r0001.json",
        });
        assert.equal(lock.partialFiles.length, 3);
        const report = JSON.parse(await readFile(run.reportJson));
        assert.equal(report.verdict, "partial");
        assert.equal(report.runs.length, 0);
        assert.equal(report.execution.plannedRuns, 2);
        assert.equal(report.execution.missingRuns.length, 2);
        assert.equal(report.performanceAccepted, false);
    },
);
