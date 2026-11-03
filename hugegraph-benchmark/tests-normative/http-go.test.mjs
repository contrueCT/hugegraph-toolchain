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
import { execFile, spawn } from "node:child_process";
import { promisify } from "node:util";
import { mkdtemp, readFile, writeFile, rm, cp, mkdir, readdir, link } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve, basename } from "node:path";
import { httpProfileSchema, httpGoProfileSchema } from "../dist/http-contracts.js";
import { planHttp, inspectAnyHttpProfile } from "../dist/http.js";
import { inspectHttpGoBinary, registerHttpGo } from "../dist/http-go.js";
import { submitTask, waitTask } from "../dist/tasks.js";
import { inspect, expectedData } from "../benchmarks/http-serving-v0.1/reference.mjs";
import { generateHttpProfile } from "../dist/http-suite.js";
import { analyze, report } from "../components/http/analyzer.mjs";
import { validateGoRaw, validateGoWriter } from "../components/http/go-engine.mjs";
import { digest, valueDigest } from "../components/http/common.mjs";
const binary = process.env.HGBENCH_GO_BINARY;
const read = async (p) => JSON.parse(await readFile(p, "utf8"));
const execute = promisify(execFile);
const limits = { gomaxprocs: 2, cpuQuotaCores: 2, maxRssBytes: 1073741824 };
function base(endpoint) {
    return httpProfileSchema.parse({
        schemaVersion: 1,
        kind: "http-benchmark-profile",
        name: "go-test",
        endpoint,
        target: {
            label: "protocol fixture",
            build: "test",
            mode: "fixture",
            environment: "Development validation only",
        },
        cases: [{ id: "one", body: { gremlin: "1+1" }, checks: [{ pointer: "/result/data", equals: [2] }] }],
        loads: [{ id: "open", model: "open", ratePerSecond: 10, maxInFlight: 2, maxStartDelayMs: 1000 }],
        repeats: 1,
        warmupMs: 0,
        durationMs: 100,
        requestTimeoutMs: 1000,
        drainMs: 1000,
        maxRequestsPerPhase: 100,
        maxResponseBytes: 2097152,
        maxTraceBytes: 1048576,
    });
}
async function temporary(t) {
    const p = await mkdtemp(join(tmpdir(), "hgbench-go-"));
    t.after(async () => {
        if (process.env.HGBENCH_GO_KEEP_ROOT) {
            const kept = resolve(process.env.HGBENCH_GO_KEEP_ROOT);
            await mkdir(kept, { recursive: true });
            await cp(p, join(kept, basename(p)), { recursive: true });
            t.diagnostic("Retained development evidence: " + join(kept, basename(p)));
        }
        await rm(p, { recursive: true, force: true });
    });
    return p;
}
async function listen(t, handler) {
    const s = http.createServer(handler);
    await new Promise((r) => s.listen(0, "127.0.0.1", r));
    t.after(async () => {
        s.closeAllConnections();
        await new Promise((r) => s.close(r));
    });
    return `http://127.0.0.1:${s.address().port}/gremlin`;
}

test("Go profile v2 is strict and cannot inherit the Node clock contract", () => {
    const p = {
        ...base("http://127.0.0.1:1/gremlin"),
        schemaVersion: 2,
        executor: {
            engine: "go-http-v1",
            binary: {
                path: "/registered",
                sha256: "a".repeat(64),
                sourceSha256: "b".repeat(64),
                goVersion: "go1.27.3",
                goos: "linux",
                goarch: "amd64",
            },
            ...limits,
            maxPendingBytes: 1048576,
        },
    };
    assert.equal(httpGoProfileSchema.safeParse(p).success, true);
    assert.equal(httpProfileSchema.safeParse(p).success, false);
    assert.equal(httpGoProfileSchema.safeParse({ ...p, clockReference: {} }).success, false);
    for (const executor of [
        { ...p.executor, gomaxprocs: 0 },
        { ...p.executor, engine: "node" },
        { ...p.executor, binary: { ...p.executor.binary, goVersion: "go1.25.5" } },
        { ...p.executor, maxPendingBytes: 16777216 },
    ])
        assert.equal(httpGoProfileSchema.safeParse({ ...p, executor }).success, false);
});

test(
    "Go public CLI runs all 11 questions with gzip, service checks and byte-identical offline rebuild",
    { skip: !binary, timeout: 60000 },
    async (t) => {
        const root = await temporary(t),
            suite = await inspect();
        let calls = 0;
        const endpoint = await listen(t, async (req, res) => {
            let body = "";
            for await (const b of req) body += b;
            const question = suite.questions.find((q) => JSON.stringify(q.body) === body);
            assert(question);
            assert.equal(req.method, "POST");
            assert.equal(req.headers["accept-encoding"], "gzip");
            calls++;
            res.setHeader("content-encoding", "gzip");
            res.end(gzipSync(JSON.stringify({ status: { code: 200 }, result: { data: expectedData(question) } })));
        });
        const settings = await read(resolve("examples/http-suite-functional.json"));
        Object.assign(settings, {
            endpoint,
            responseEncoding: "gzip",
            durationMs: 100,
            loads: [{ id: "one", model: "open", ratePerSecond: 10, maxInFlight: 1, maxStartDelayMs: 1000 }],
        });
        await writeFile(join(root, "settings.json"), JSON.stringify(settings));
        await generateHttpProfile(join(root, "settings.json"), join(root, "node.json"), false);
        const registered = await registerHttpGo(join(root, "node.json"), binary, join(root, "go.json"), limits);
        assert.equal(registered.profile.benchmark.coverage, "full");
        await inspectAnyHttpProfile(join(root, "go.json"));
        const planned = JSON.parse(
            (
                await execute(process.execPath, [
                    resolve("dist/cli.js"),
                    "http",
                    "plan",
                    join(root, "go.json"),
                    "--out",
                    join(root, "attempt"),
                ])
            ).stdout,
        ).data;
        const frozen = await read(planned.taskPlan);
        assert(frozen.files.some((f) => f.path === registered.profile.executor.binary.path));
        const submitted = await submitTask(join(root, "tasks"), frozen, "go-full-once"),
            state = await waitTask(join(root, "tasks"), submitted.taskId, 40000);
        if (state.state !== "completed") {
            for (const f of ["stderr.log", "stdout.log"])
                try {
                    console.error(await readFile(join(state.evidenceDir, f), "utf8"));
                } catch {}
        }
        assert.equal(state.state, "completed", JSON.stringify(state));
        assert.equal(calls, 33);
        const output = join(planned.directory, "output"),
            r = await analyze(output);
        assert.equal(r.schemaVersion, 2);
        assert.equal(r.engine, "go-http-v1");
        assert.equal(r.runs.length, 11);
        assert.equal(r.gates.correctness, "passed");
        assert.equal(r.performanceAccepted, false);
        assert(
            r.runs.every(
                (r) =>
                    r.correct === 1 &&
                    r.failed === 0 &&
                    r.dropped === 0 &&
                    r.driver.eventLoopUtilization === null &&
                    r.driver.engine === "go-http-v1",
            ),
        );
        assert(r.runs.every((r) => r.phaseClock.externalCorrelationAccepted === false));
        await report(output, join(root, "rebuilt"));
        for (const file of ["report.json", "report.html"])
            assert.equal(
                await readFile(join(output, "analysis", file), "utf8"),
                await readFile(join(root, "rebuilt", file), "utf8"),
            );
        const moved = join(root, "moved");
        await cp(output, moved, { recursive: true });
        const portable = await analyze(moved);
        assert.deepEqual(portable, r);
        const raw = await read(join(output, "runs/r0001.json")),
            p = await read(join(output, "profile.json"));
        const forged = structuredClone(raw);
        forged.phases[0].eventLoopUtilization = 0;
        assert.throws(() => validateGoRaw(forged, p), /resource engine/);
        const forgedClock = structuredClone(raw);
        forgedClock.phaseClock.externalCorrelationAccepted = true;
        assert.throws(() => validateGoRaw(forgedClock, p), /clock identity/);
        const forgedWriter = structuredClone(raw);
        forgedWriter.traceWriter.persistedBytes--;
        assert.throws(() => validateGoWriter(forgedWriter, raw.traceBytes, p), /accounting/);
        const manifest = await read(join(output, "raw.lock.json"));
        manifest.schemaVersion = 2;
        await writeFile(join(output, "raw.lock.json"), JSON.stringify(manifest));
        await assert.rejects(analyze(output), /engine differs/);
        assert.equal((await submitTask(join(root, "tasks"), frozen, "go-full-once")).reused, true);
    },
);

test("Go frozen plans reject unbound source or binary identity before target effects", { skip: !binary }, async (t) => {
    const root = await temporary(t),
        file = join(root, "go.json"),
        identity = await inspectHttpGoBinary(binary),
        p = {
            ...base("http://127.0.0.1:1/gremlin"),
            schemaVersion: 2,
            executor: { engine: "go-http-v1", binary: identity.binary, ...limits, maxPendingBytes: 1048576 },
        };
    p.executor.binary.sourceSha256 = "0".repeat(64);
    await writeFile(file, JSON.stringify(p));
    await assert.rejects(planHttp(file, join(root, "bad-source")), (e) => e.code === "HTTP_GO_SOURCE_IDENTITY");
    p.executor.binary.sourceSha256 = identity.identity.sourceSha256;
    p.executor.binary.sha256 = "0".repeat(64);
    await writeFile(file, JSON.stringify(p));
    await assert.rejects(planHttp(file, join(root, "bad-binary")), /SHA differs/);
});

test("Go component --help/version and CLI engine schemas are agent discoverable", { skip: !binary }, async () => {
    const help = (await execute(binary, ["--help"])).stdout;
    assert.match(help, /SIGINT\/SIGTERM/);
    assert.match(help, /never downloads/);
    const inspected = JSON.parse(
        (await execute(process.execPath, [resolve("dist/cli.js"), "http", "engine", "inspect", binary])).stdout,
    );
    assert.equal(inspected.data.identity.engine, "go-http-v1");
    const schema = JSON.parse(
        (await execute(process.execPath, [resolve("dist/cli.js"), "schema", "http-go-profile"])).stdout,
    );
    assert(schema.ok);
    const command = (await execute(process.execPath, [resolve("dist/cli.js"), "http", "engine", "register", "--help"]))
        .stdout;
    assert.match(command, /new\s+calibration/);
});

test(
    "Go process SIGTERM retains the complete arrival schedule and exits nonzero",
    { skip: !binary, timeout: 10000 },
    async (t) => {
        const root = await temporary(t);
        let received;
        const first = new Promise((r) => {
            received = r;
        });
        const endpoint = await listen(t, (req, res) => {
            req.resume();
            received();
        });
        const info = await inspectHttpGoBinary(binary),
            p = {
                ...base(endpoint),
                schemaVersion: 2,
                durationMs: 1000,
                requestTimeoutMs: 5000,
                loads: [{ id: "hundred", model: "open", ratePerSecond: 100, maxInFlight: 1, maxStartDelayMs: 1000 }],
                executor: { engine: "go-http-v1", binary: info.binary, ...limits, maxPendingBytes: 1048576 },
            };
        const request = {
            schemaVersion: 2,
            id: "r0001",
            profile: p,
            case: p.cases[0],
            load: p.loads[0],
            repeat: 0,
            remainingTraceBytes: p.maxTraceBytes,
        };
        const req = join(root, "request.json"),
            trace = join(root, "trace.ndjson"),
            output = join(root, "result.json");
        await writeFile(req, JSON.stringify(request));
        const child = spawn(binary, ["run", "--request", req, "--trace", trace, "--output", output], {
            stdio: ["ignore", "pipe", "pipe"],
            env: { PATH: "/usr/bin:/bin", GODEBUG: "" },
        });
        let stderr = "";
        child.stderr.on("data", (b) => (stderr += b));
        const closed = new Promise((resolve, reject) => {
            child.once("error", reject);
            child.once("close", (code, signal) => resolve({ code, signal }));
        });
        await first;
        child.kill("SIGTERM");
        const end = await closed;
        assert.notEqual(end.code, 0, stderr);
        const result = await read(output),
            rows = (await readFile(trace, "utf8")).trim().split("\n").map(JSON.parse);
        assert.equal(result.kind, "http-run");
        assert.equal(result.phases[0].cancelled, true);
        assert.equal(result.phases[0].scheduled, 100);
        assert.equal(rows.length, 100);
        assert.equal(new Set(rows.map((r) => r.sequence)).size, 100);
        assert.equal(result.phases[0].sent + result.phases[0].dropped, 100);
        assert.equal(result.traceWriter.complete, true);
        assert(rows.some((r) => r.outcome === "cancelled-before-send"));
        validateGoRaw(result, p);
        validateGoWriter(result, Buffer.byteLength(await readFile(trace)), p);
    },
);

test("Go clock-probe SIGTERM releases idle stdin promptly", { skip: !binary, timeout: 5000 }, async (t) => {
    const child = spawn(binary, ["clock-probe"], {
        stdio: ["pipe", "pipe", "pipe"],
        env: { PATH: "/usr/bin:/bin", GODEBUG: "" },
    });
    t.after(() => {
        child.kill("SIGKILL");
        child.stdin.destroy();
    });
    let stdout = "",
        stderr = "";
    let ready;
    const started = new Promise((resolve) => {
        ready = resolve;
    });
    child.stdout.on("data", (b) => {
        stdout += b;
        if (stdout.includes("\n")) ready();
    });
    child.stderr.on("data", (b) => {
        stderr += b;
    });
    const closed = new Promise((resolve, reject) => {
        child.once("error", reject);
        child.once("close", (code, signal) => resolve({ code, signal }));
    });
    try {
        await started;
        assert.equal(JSON.parse(stdout.trim()).kind, "http-go-clock-ready");
        const before = Date.now();
        child.kill("SIGTERM");
        const end = await closed;
        assert.notEqual(end.code, 0, stderr);
        assert(Date.now() - before < 2000, "blocked stdin delayed cancellation");
    } finally {
        child.kill("SIGKILL");
        child.stdin.destroy();
    }
});

test(
    "Go task workflow freezes both identity gates for runs and service checks",
    { skip: !binary, timeout: 30000 },
    async (t) => {
        const root = await temporary(t);
        let calls = 0;
        const endpoint = await listen(t, (req, res) => {
            req.resume();
            calls++;
            res.setHeader("Content-Type", "application/json");
            res.end('{"result":{"data":[2]}}');
        });
        const node = { ...base(endpoint), serviceChecks: true };
        await writeFile(join(root, "node.json"), JSON.stringify(node));
        const registered = await registerHttpGo(join(root, "node.json"), binary, join(root, "go-base.json"), limits);
        const p = { ...registered.profile, executor: { ...registered.profile.executor, hostIdentityGate: true } };
        await writeFile(join(root, "go.json"), JSON.stringify(p));
        const planned = await planHttp(join(root, "go.json"), join(root, "attempt")),
            frozen = await read(planned.taskPlan);
        assert(frozen.plan.timeoutMs >= 120000, "gate cleanup reserve was not included in task budget");
        let done = false;
        const released = new Set();
        const host = (async () => {
            while (!done) {
                for (const folder of ["preflight", "runs", "postcheck"]) {
                    const parent = join(planned.outputDirectory, folder);
                    let names = [];
                    try {
                        names = await readdir(parent);
                    } catch (e) {
                        if (e.code !== "ENOENT") throw e;
                    }
                    for (const name of names.filter((n) => n.endsWith(".json.identity")))
                        for (const stage of ["ready", "finished"]) {
                            const dir = join(parent, name),
                                ack = join(dir, stage + "-release.json");
                            if (released.has(ack)) continue;
                            let raw;
                            try {
                                raw = await readFile(join(dir, stage + ".json"));
                            } catch (e) {
                                if (e.code === "ENOENT") continue;
                                throw e;
                            }
                            const m = JSON.parse(raw),
                                release = {
                                    schemaVersion: 1,
                                    kind: "http-go-live-process-release",
                                    id: m.id,
                                    stage,
                                    nonce: m.nonce,
                                    markerSha256: await digest(join(dir, stage + ".json")),
                                };
                            const tmp = ack + ".tmp";
                            await writeFile(tmp, JSON.stringify(release), { flag: "wx" });
                            await link(tmp, ack);
                            await rm(tmp);
                            released.add(ack);
                        }
                }
                await new Promise((r) => setTimeout(r, 10));
            }
        })();
        try {
            const submitted = await submitTask(join(root, "tasks"), frozen, "go-gates-once");
            const state = await waitTask(join(root, "tasks"), submitted.taskId, 20000);
            if (state.state !== "completed")
                for (const file of ["stderr.log", "stdout.log"])
                    try {
                        console.error(await readFile(join(state.evidenceDir, file), "utf8"));
                    } catch {}
            assert.equal(state.state, "completed", JSON.stringify(state));
        } finally {
            done = true;
            await host;
        }
        assert.equal(calls, 3);
        assert.equal(released.size, 6);
        const manifest = await read(join(planned.outputDirectory, "raw.lock.json"));
        assert.equal(manifest.files.filter((f) => f.path.includes(".json.identity/")).length, 12);
        const result = await analyze(planned.outputDirectory);
        assert.equal(result.gates.correctness, "passed");
        assert.equal(result.performanceAccepted, false);
        const missing = structuredClone(manifest);
        missing.files = missing.files.filter((f) => f.path !== "runs/r0001.json.identity/ready-release.json");
        await writeFile(join(planned.outputDirectory, "raw.lock.json"), JSON.stringify(missing));
        await assert.rejects(analyze(planned.outputDirectory), /inventory|locked identity/);
    },
);
