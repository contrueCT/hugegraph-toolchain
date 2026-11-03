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
import { mkdtemp, cp, readFile, writeFile, rm, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { planHttp, httpProfileSchema } from "../dist/http.js";
import { analyze } from "../components/http/analyzer.mjs";
import { digest } from "../components/http/common.mjs";

const read = async (path) => JSON.parse(await readFile(path, "utf8"));
const write = async (path, value) => writeFile(path, JSON.stringify(value) + "\n");

async function fixture(t, serviceChecks = true) {
    const root = await mkdtemp(join(tmpdir(), "hgbench-http-evidence-"));
    const server = http.createServer((request, response) => {
        request.resume();
        response.setHeader("Content-Type", "application/json");
        response.end('{"result":{"data":[2]}}');
    });
    await new Promise((done) => server.listen(0, "127.0.0.1", done));
    t.after(async () => {
        server.closeAllConnections();
        await new Promise((done) => server.close(done));
        await rm(root, { recursive: true, force: true });
    });
    const attachments = [join(root, "build.json"), join(root, "environment.json")];
    await write(attachments[0], { build: "local-controlled-fixture" });
    await write(attachments[1], { environment: "local-http-fixture" });
    const profile = httpProfileSchema.parse({
        schemaVersion: 1,
        kind: "http-benchmark-profile",
        name: "evidence-consistency-test",
        serviceChecks,
        endpoint: `http://127.0.0.1:${server.address().port}/gremlin`,
        target: {
            label: "controlled HTTP fixture",
            build: "test",
            mode: "control",
            environment: "tool verification only",
            evidenceFiles: attachments,
        },
        cases: [{ id: "two", body: { gremlin: "1+1" }, checks: [{ pointer: "/result/data", equals: [2] }] }],
        loads: [{ id: "arrival", model: "open", ratePerSecond: 30, maxInFlight: 2, maxStartDelayMs: 1000 }],
        repeats: 2,
        warmupMs: 100,
        durationMs: 200,
        requestTimeoutMs: 100,
        drainMs: 100,
        maxRequestsPerPhase: 100,
        maxResponseBytes: 4096,
        maxTraceBytes: 1048576,
    });
    const file = join(root, "profile.json"),
        attempt = join(root, "attempt");
    await write(file, profile);
    await planHttp(file, attempt);
    await promisify(execFile)(process.execPath, [resolve("components/http/workflow.mjs"), "--plan", attempt], {
        timeout: 15000,
    });
    const output = join(attempt, "output");
    assert.equal((await analyze(output)).gates.evidenceIntegrity, "passed");
    return { root, output };
}

async function copied(f, name) {
    const path = join(f.root, name);
    await cp(f.output, path, { recursive: true });
    return path;
}

async function relock(root) {
    const path = join(root, "raw.lock.json"),
        lock = await read(path);
    for (const f of lock.files) {
        f.bytes = (await stat(join(root, f.path))).size;
        f.sha256 = await digest(join(root, f.path));
    }
    await write(path, lock);
}

async function modifyProfile(root, patch) {
    const profile = await read(join(root, "profile.json"));
    patch(profile);
    await write(join(root, "profile.json"), profile);
    const lock = await read(join(root, "raw.lock.json"));
    for (const id of lock.runs) {
        const path = join(root, `requests/${id}.json`),
            request = await read(path);
        request.profile = profile;
        await write(path, request);
    }
}

async function editSample(root, relativeTrace, patch, updatePhase = null) {
    const path = join(root, relativeTrace),
        samples = (await readFile(path, "utf8")).trimEnd().split("\n").map(JSON.parse),
        sample = samples.find((s) => s.phase !== "warmup");
    patch(sample);
    const raw = samples.map((s) => JSON.stringify(s)).join("\n") + "\n";
    await writeFile(path, raw);
    const runPath = relativeTrace.startsWith("traces/")
        ? relativeTrace.replace("traces/", "runs/").replace(".ndjson", ".json")
        : relativeTrace.replace(".ndjson", ".json");
    const run = await read(join(root, runPath));
    run.traceBytes = Buffer.byteLength(raw);
    // This helper coherently re-seals synthetic sample mutations; writer-cost observations stay captured.
    if (Object.hasOwn(run, "traceWriter")) {
        run.traceWriter.enqueuedBytes = run.traceBytes;
        run.traceWriter.persistedBytes = run.traceBytes;
    }
    if (updatePhase)
        updatePhase(
            run.phases.find((p) => p.name === sample.phase),
            sample,
        );
    await write(join(root, runPath), run);
    // Re-seal byte accounting as well, so timeline rejection cannot rely on stale hashes or lengths.
    if (relativeTrace.startsWith("traces/")) {
        const lock = await read(join(root, "raw.lock.json")),
            profile = await read(join(root, "profile.json"));
        let used = profile.serviceChecks ? (await read(join(root, "preflight/result.json"))).traceBytes : 0;
        for (const id of lock.runs) {
            const requestPath = join(root, `requests/${id}.json`),
                request = await read(requestPath);
            request.remainingTraceBytes = profile.maxTraceBytes - used;
            await write(requestPath, request);
            used += (await stat(join(root, `traces/${id}.ndjson`))).size;
        }
    } else {
        const phase = relativeTrace.split("/")[0],
            resultPath = join(root, phase, "result.json"),
            result = await read(resultPath);
        for (const f of result.files) {
            f.bytes = (await stat(join(root, phase, f.path))).size;
            f.sha256 = await digest(join(root, phase, f.path));
        }
        result.traceBytes = result.files.filter((f) => f.path.endsWith(".ndjson")).reduce((n, f) => n + f.bytes, 0);
        await write(resultPath, result);
    }
    await relock(root);
}

test("HTTP offline verification binds every target attachment to its declared index, digest and size", async (t) => {
    const f = await fixture(t);
    for (const mode of ["missing", "extra", "index", "digest", "size", "source-path"]) {
        await t.test(mode, async () => {
            const root = await copied(f, mode),
                lock = await read(join(root, "raw.lock.json")),
                target = await read(join(root, "target-evidence.json"));
            if (mode === "missing") {
                await rm(join(root, "target/0.json"));
                lock.files = lock.files.filter((f) => f.path !== "target/0.json");
            } else if (mode === "extra") {
                await cp(join(root, "target/0.json"), join(root, "target/2.json"));
                lock.files.push({ path: "target/2.json" });
            } else if (mode === "index") {
                const original = await readFile(join(root, "target/0.json"));
                await cp(join(root, "target/1.json"), join(root, "target/0.json"));
                await writeFile(join(root, "target/1.json"), original);
            } else if (mode === "digest") target.files[0].sha256 = "0".repeat(64);
            else if (mode === "size") target.files[0].bytes++;
            else target.files[0].path = "/different/source.json";
            await write(join(root, "target-evidence.json"), target);
            await write(join(root, "raw.lock.json"), lock);
            await relock(root);
            await assert.rejects(analyze(root), /Target (attachment|evidence)/);
        });
    }
});

test("HTTP offline verification reconciles run/probe traces and the whole-workflow budget", async (t) => {
    const f = await fixture(t);
    for (const mode of [
        "run-bytes",
        "remaining",
        "preflight-remaining",
        "workflow-cap",
        "probe-workflow-cap",
        "probe-bytes",
        "probe-total",
    ]) {
        await t.test(mode, async () => {
            const root = await copied(f, mode);
            if (mode === "workflow-cap") await modifyProfile(root, (p) => (p.maxTraceBytes = 4096));
            else if (mode === "probe-workflow-cap") {
                const lock = await read(join(root, "raw.lock.json"));
                const measurementTraceBytes = lock.files
                    .filter((f) => f.path.startsWith("traces/"))
                    .reduce((n, f) => n + f.bytes, 0);
                assert(measurementTraceBytes >= 4096);
                // Measured-run traces fit; only including pre/postcheck exceeds the workflow budget.
                await modifyProfile(root, (p) => (p.maxTraceBytes = measurementTraceBytes));
            } else {
                const relative =
                    mode === "run-bytes"
                        ? "runs/r0001.json"
                        : mode === "remaining" || mode === "preflight-remaining"
                          ? "requests/r0001.json"
                          : mode === "probe-bytes"
                            ? "postcheck/p0001.json"
                            : "postcheck/result.json";
                const path = join(root, relative),
                    value = await read(path);
                if (mode === "remaining") value.remainingTraceBytes--;
                else if (mode === "preflight-remaining") value.remainingTraceBytes = value.profile.maxTraceBytes;
                else value.traceBytes++;
                await write(path, value);
                if (mode === "probe-bytes") {
                    const resultPath = join(root, "postcheck/result.json"),
                        result = await read(resultPath),
                        file = result.files.find((f) => f.path === "p0001.json");
                    file.bytes = (await stat(path)).size;
                    file.sha256 = await digest(path);
                    await write(resultPath, result);
                }
            }
            await relock(root);
            await assert.rejects(analyze(root), /trace (byte|budget)|remaining trace|Probe answer\/accounting/);
        });
    }
});

test("HTTP offline verification bounds sample times by the actual phase, including service probes", async (t) => {
    const f = await fixture(t);
    for (const mode of ["end", "socket", "validation", "late", "probe-end", "probe-duration"]) {
        await t.test(mode, async () => {
            const root = await copied(f, mode),
                trace = mode.startsWith("probe-") ? "postcheck/p0001.ndjson" : "traces/r0001.ndjson";
            await editSample(
                root,
                trace,
                (s) => {
                    if (mode === "end" || mode === "probe-end") s.endMs = 1000000;
                    else if (mode === "socket") s.socketMs = s.endMs + 1;
                    else if (mode === "validation") s.validationMs = 1000000;
                    else if (mode === "late") s.late = !s.late;
                },
                mode === "probe-duration" ? (phase) => phase.durationMs++ : null,
            );
            await assert.rejects(analyze(root), /timeline|validation extends|Probe duration/);
        });
    }
    await t.test("completion after nominal deadline remains valid within actual elapsed time", async () => {
        const root = await copied(f, "delayed-event-loop");
        const run = await read(join(root, "runs/r0001.json")),
            phase = run.phases.find((p) => p.name === "measurement");
        assert(phase.elapsedMs > phase.durationMs);
        await editSample(root, "traces/r0001.ndjson", (s) => {
            // Keep the actual captured phase/clock envelope; move only the synthetic sample
            // into its observed drain interval, with no extra post-response validation work.
            s.endMs = (phase.durationMs + phase.elapsedMs) / 2;
            s.validationMs = 0;
            s.decodeMs = 0;
        });
        assert.equal((await analyze(root)).gates.sampleAccounting, "passed");
    });
});

test("HTTP raw v1 without service-check fields remains independently analyzable", async (t) => {
    const f = await fixture(t, false),
        root = await copied(f, "legacy-v1");
    await modifyProfile(root, (p) => delete p.serviceChecks);
    const lockPath = join(root, "raw.lock.json"),
        lock = await read(lockPath);
    lock.schemaVersion = 1;
    delete lock.serviceChecks;
    await write(lockPath, lock);
    await relock(root);
    const result = await analyze(root);
    assert.equal(result.runs.length, 2);
    assert.equal(result.gates.evidenceIntegrity, "passed");
    assert.equal(result.gates.sampleAccounting, "passed");
    assert.deepEqual(result.serviceChecks, []);
});
