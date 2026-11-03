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
import http from "node:http";
import { mkdtemp, readFile, writeFile, rm, copyFile } from "node:fs/promises";
import { execFile, spawn } from "node:child_process";
import { promisify } from "node:util";
import { pathToFileURL } from "node:url";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { run } from "../components/http/runner.mjs";
import { httpProfileSchema, planHttp } from "../dist/http.js";
import { submitTask, waitTask } from "../dist/tasks.js";
import { analyze, report } from "../components/http/analyzer.mjs";
import { validateDirectPhaseClockRaw, captureDirectPhaseBoundary } from "../components/http/direct-phase-clock.mjs";
import { valueDigest, bytesDigest } from "../components/http/common.mjs";
import { verifyRunnerSanityRaw } from "../components/http/runner-sanity.mjs";

const python = (
    await promisify(execFile)(process.env.HGBENCH_PYTHON ?? "/usr/bin/python3", [
        "-I",
        "-S",
        "-B",
        "-c",
        "import os,sys; print(os.path.realpath(sys.executable))",
    ])
).stdout.trim();
const clockReference = {
    python,
    spec: {
        schemaVersion: 1,
        kind: "http-runner-sanity-spec",
        sourceOrigin: "fixture",
        startupMs: 3000,
        exchangeMs: 1000,
        finishMs: 1000,
        wallMs: 15000,
        maxFrameBytes: 8192,
        maxRawBytes: 65536,
        maxStderrBytes: 2048,
    },
};

async function fixture(t, handler) {
    const root = await mkdtemp(join(tmpdir(), "direct-phase-clock-")),
        server = http.createServer(handler);
    await new Promise((ok) => server.listen(0, "127.0.0.1", ok));
    t.after(async () => {
        server.closeAllConnections();
        await new Promise((ok) => server.close(ok));
        await rm(root, { recursive: true, force: true });
    });
    return { root, endpoint: `http://127.0.0.1:${server.address().port}/gremlin` };
}
function profile(endpoint, extra = {}) {
    return httpProfileSchema.parse({
        schemaVersion: 1,
        kind: "http-benchmark-profile",
        name: "phase-clock-test",
        endpoint,
        target: { label: "local fixture", build: "test", mode: "control", environment: "clock functional" },
        cases: [{ id: "constant", body: { gremlin: "1+1" }, checks: [{ pointer: "/result/data", equals: [2] }] }],
        loads: [{ id: "closed", model: "closed", concurrency: 1 }],
        repeats: 1,
        warmupMs: 40,
        durationMs: 100,
        requestTimeoutMs: 500,
        drainMs: 200,
        maxRequestsPerPhase: 100,
        maxResponseBytes: 4096,
        maxTraceBytes: 1048576,
        ...extra,
    });
}
async function execute(f, p, extra = {}) {
    const trace = join(f.root, "trace.ndjson"),
        result = await run({ profile: p, case: p.cases[0], load: p.loads[0], id: "r0001", repeat: 0, ...extra }, trace);
    return { result, trace: (await readFile(trace, "utf8")).trimEnd().split("\n").map(JSON.parse) };
}
function account(result) {
    const { evidenceBytes, ...session } = result.phaseClock;
    result.phaseClock.evidenceBytes = Buffer.byteLength(
        JSON.stringify({ session, phases: result.phases.map((p) => p.clock) }),
    );
}

test("runner records both real phase boundaries, preserves elapsed and leaves host association unverified", async (t) => {
    const f = await fixture(t, (req, res) => {
        req.resume();
        setTimeout(() => res.end('{"result":{"data":[2]}}'), 15);
    });
    const { result, trace } = await execute(f, profile(f.endpoint));
    const checked = validateDirectPhaseClockRaw(result);
    assert.equal(checked.rawValidated, true);
    assert.equal(checked.verified, false);
    assert.equal(checked.hostAssociation, "unverified");
    assert.equal(result.performanceAccepted, false);
    assert.equal(result.phaseClock.verified, false);
    assert.ok(result.phaseClock.evidenceBytes < 65536);
    assert.equal(result.phaseClock.identityBefore.pid, process.pid);
    assert.equal(result.phaseClock.runtime.node, process.version);
    assert.match(result.phaseClock.runtime.files.executable.sha256, /^[a-f0-9]{64}$/);
    assert.equal(result.phaseClock.runtime.files.executable.reportedPath, process.execPath);
    assert.deepEqual(
        result.phases.map((p) => p.name),
        ["warmup", "measurement"],
    );
    for (const phase of result.phases) {
        assert.equal(phase.clock.status, "raw-unverified");
        assert.equal(phase.clock.verified, false);
        assert.equal(phase.elapsedMs, phase.clock.end.performanceMs - phase.clock.start.performanceMs);
        assert.ok(BigInt(result.phaseClock.preparedIntervalNs[1]) <= BigInt(phase.clock.start.hrtimeBeforeNs));
        assert.ok(BigInt(phase.clock.end.hrtimeAfterNs) <= BigInt(result.phaseClock.finishedIntervalNs[0]));
        for (const row of trace.filter((r) => r.phase === phase.name)) {
            assert.equal(row.outcome, "ok");
            assert.ok(row.endMs <= phase.elapsedMs);
            assert.equal(row.clock, undefined); // No per-request clock payload or clock hashing.
        }
    }
    assert.throws(() => captureDirectPhaseBoundary({}), /DIRECT_CLOCK_CONTEXT/);
    const legacy = structuredClone(result);
    delete legacy.phaseClock;
    for (const phase of legacy.phases) delete phase.clock;
    assert.equal(validateDirectPhaseClockRaw(legacy).reason, "legacy-no-phase-clock");
    for (const [name, patch, expected] of [
        ["elapsed", (r) => r.phases[1].elapsedMs++, /ELAPSED_MISMATCH/],
        ["reverse-boundary", (r) => (r.phases[0].clock.start.hrtimeAfterNs = "0"), /BOUNDARY_ORDER/],
        ["unsafe-number", (r) => (r.phases[0].clock.start.hrtimeBeforeNs = 9007199254740993), /INTEGER_PRECISION/],
        ["wrong-phase", (r) => (r.phases[0].clock.phase = "measurement"), /PHASE_BINDING/],
        ["wrong-run", (r) => (r.phaseClock.runIdentity.id = "r0002"), /RUN_BINDING/],
        ["restarted", (r) => r.phaseClock.identityAfter.pid++, /IDENTITY_DRIFT|PROC_STAT|PROC_STATUS/],
        ["asserted-verified", (r) => (r.phaseClock.verified = true), /SESSION_KIND/],
        ["phase-verified", (r) => (r.phases[0].clock.verified = true), /RAW_KIND/],
        ["gate-deleted", (r) => r.phaseClock.pendingGates.pop(), /PENDING_GATES/],
        ["budget", (r) => r.phaseClock.maxEvidenceBytes++, /EVIDENCE_BUDGET/],
        ["missing-phase", (r) => delete r.phases[0].clock, /PHASE_BINDING/],
    ])
        await t.test(name, () => {
            const changed = structuredClone(result);
            patch(changed);
            account(changed);
            assert.throws(() => validateDirectPhaseClockRaw(changed), expected);
        });
    const missingSession = structuredClone(result);
    delete missingSession.phaseClock;
    assert.throws(() => validateDirectPhaseClockRaw(missingSession), /DIRECT_CLOCK_MISSING_SESSION/);
});

test("phase actual end includes a real delayed response beyond nominal duration", async (t) => {
    const f = await fixture(t, (req, res) => {
        req.resume();
        setTimeout(() => res.end('{"result":{"data":[2]}}'), 160);
    });
    const { result } = await execute(
        f,
        profile(f.endpoint, {
            warmupMs: 0,
            durationMs: 100,
            loads: [{ id: "open", model: "open", ratePerSecond: 10, maxInFlight: 1, maxStartDelayMs: 50 }],
        }),
    );
    const phase = result.phases[0];
    assert.equal(phase.sent, 1);
    assert.equal(phase.completed, 1);
    assert.equal(phase.failed, 0);
    assert.ok(phase.elapsedMs >= 145);
    assert.equal(phase.durationMs, 100);
    assert.equal(phase.clock.end.performanceMs - phase.clock.start.performanceMs, phase.elapsedMs);
    assert.equal(validateDirectPhaseClockRaw(result).verified, false);
});

test("runtime identity drift during HTTP work fails the run and keeps its bounded raw trace", async (t) => {
    const old = process.env.NODE_OPTIONS;
    const f = await fixture(t, (req, res) => {
        req.resume();
        if (old) delete process.env.NODE_OPTIONS;
        else process.env.NODE_OPTIONS = "--trace-warnings";
        res.end('{"result":{"data":[2]}}');
    });
    const p = profile(f.endpoint, { warmupMs: 0, maxRequestsPerPhase: 1, clockReference }),
        path = join(f.root, "drift.ndjson");
    try {
        await assert.rejects(
            run({ profile: p, case: p.cases[0], load: p.loads[0], id: "r0001", repeat: 0 }, path),
            /DIRECT_CLOCK_RUNTIME_DRIFT/,
        );
    } finally {
        if (old === undefined) delete process.env.NODE_OPTIONS;
        else process.env.NODE_OPTIONS = old;
    }
    const rows = (await readFile(path, "utf8")).trimEnd().split("\n").map(JSON.parse);
    assert.equal(rows.length, 1);
    assert.equal(rows[0].outcome, "ok");
    const partial = JSON.parse(await readFile(`${path}.clock-reference.partial.json`, "utf8"));
    assert.equal(partial.status, "partial");
    assert.equal(partial.failure, "DIRECT_CLOCK_RUNTIME_DRIFT");
    assert.equal(partial.clockReference.terminal.closed, true);
    assert.equal(partial.clockReference.terminal.stdoutEof, true);
    assert.equal(partial.clockReference.terminal.stderrEof, true);
    assert.throws(() => process.kill(partial.clockReference.peerPid, 0), { code: "ESRCH" });
});

test("functional probes retain their own phase binding and unverified raw clock", async (t) => {
    const f = await fixture(t, (req, res) => {
        req.resume();
        res.end('{"result":{"data":[2]}}');
    });
    const { result } = await execute(f, profile(f.endpoint), { probePhase: "preflight" });
    assert.equal(result.kind, "http-functional-probe");
    assert.equal(result.phaseClock.runIdentity.probePhase, "preflight");
    assert.equal(result.phases[0].clock.phase, "preflight");
    assert.equal(validateDirectPhaseClockRaw(result).verified, false);
});

test("runtime file mutation fails outside the measured window without weakening the source lock", async (t) => {
    const root = await mkdtemp(join(tmpdir(), "direct-phase-drift-"));
    t.after(() => rm(root, { recursive: true, force: true }));
    const component = join(root, "direct-phase-clock.mjs");
    for (const name of [
        "direct-phase-clock.mjs",
        "common.mjs",
        "runner-sanity.mjs",
        "phase-clock-reference.py",
        "clock-correlation.mjs",
    ])
        await copyFile(new URL(`../components/http/${name}`, import.meta.url), join(root, name));
    // A separate process and private copy preserve the workspace and the process-wide runtime cache.
    const result = await promisify(execFile)(
        process.execPath,
        [
            "--input-type=module",
            "-e",
            `import assert from 'node:assert/strict';
             import { appendFile } from 'node:fs/promises';
             const source = process.argv[1];
             const c = await import(source);
             const handle = await c.prepareDirectPhaseClock({id:'drift'});
             const start = c.captureDirectPhaseBoundary(handle);
             const end = c.captureDirectPhaseBoundary(handle);
             const phase = {clock:c.recordDirectPhaseClock(handle, 'measurement', start, end,
                 end.performanceMs-start.performanceMs)};
             await appendFile(new URL(source), '\\n// changed after measurement\\n');
             await assert.rejects(c.finishDirectPhaseClock(handle,[phase]),
                 {code:'DIRECT_CLOCK_RUNTIME_DRIFT'});
             assert.throws(()=>c.captureDirectPhaseBoundary(handle), {code:'DIRECT_CLOCK_CONTEXT'});
             process.stdout.write(JSON.stringify({rejected:'DIRECT_CLOCK_RUNTIME_DRIFT'}));`,
            pathToFileURL(component).href,
        ],
        { timeout: 10000, maxBuffer: 16384 },
    );
    assert.deepEqual(JSON.parse(result.stdout), { rejected: "DIRECT_CLOCK_RUNTIME_DRIFT" });
});

function changeReferenceAnchor(e, index, patch) {
    const frame = e.frames[index],
        value = JSON.parse(frame.raw);
    patch(value.anchor);
    frame.raw = JSON.stringify(value) + "\n";
    frame.bytes = Buffer.byteLength(frame.raw);
    const digest = (raw) => bytesDigest(Buffer.from(raw));
    frame.sha256 = digest(frame.raw);
    const receipt = e.frames[index + 1],
        receiptValue = JSON.parse(receipt.raw);
    receiptValue.responseRaw = frame.raw;
    receiptValue.responseSha256 = frame.sha256;
    receipt.raw = JSON.stringify(receiptValue) + "\n";
    receipt.bytes = Buffer.byteLength(receipt.raw);
    receipt.sha256 = digest(receipt.raw);
    e.wireBytes = e.frames.reduce((n, f) => n + f.bytes, 0);
}

test("optional S/R is captured in the actual runner around every phase and remains raw/unverified", async (t) => {
    const f = await fixture(t, (req, res) => {
        req.resume();
        res.end('{"result":{"data":[2]}}');
    });
    const p = profile(f.endpoint, { clockReference });
    const { result, trace } = await execute(f, p);
    const raw = result.clockReference,
        session = result.phaseClock;
    assert.equal(raw.callerPid, process.pid);
    assert.deepEqual(session.clockReference, {
        required: true,
        profileDigest: valueDigest(p),
        specDigest: valueDigest(clockReference.spec),
        maxEvidenceBytes: 4 * clockReference.spec.maxRawBytes + 65536,
    });
    assert.equal(raw.binding.sessionId, session.sessionId);
    const pre = JSON.parse(raw.frames[4].raw).anchor,
        post = JSON.parse(raw.frames[8].raw).anchor;
    assert.notEqual(pre.identityBefore.processToken, session.identityBefore.processToken);
    for (const phase of result.phases) {
        assert.ok(BigInt(pre.hrtimeAfterNs) <= BigInt(phase.clock.start.hrtimeBeforeNs));
        assert.ok(BigInt(phase.clock.end.hrtimeAfterNs) <= BigInt(post.hrtimeBeforeNs));
    }
    assert.equal(verifyRunnerSanityRaw(raw).rawSanity, "passed");
    const verdict = validateDirectPhaseClockRaw(result, p);
    assert.equal(verdict.clockReference.rawSanity, "passed");
    assert.equal(verdict.verified, false);
    assert.equal(verdict.clockReference.verified, false);
    assert.equal(verdict.hostAssociation, "unverified");
    assert.equal(result.performanceAccepted, false);
    assert.equal(raw.terminal.closed, true);
    assert.equal(raw.terminal.stdoutEof, true);
    assert.equal(raw.terminal.stderrEof, true);
    assert.throws(() => process.kill(raw.peerPid, 0), { code: "ESRCH" });
    assert.ok(
        Buffer.byteLength(JSON.stringify(result, null, 2) + "\n") <= 4 * clockReference.spec.maxRawBytes + 131072,
    );
    assert.ok(trace.every((r) => r.clockReference === undefined && r.clock === undefined));
    for (const field of ["clockReference", "phaseClock"]) {
        const changed = structuredClone(result);
        delete changed[field];
        assert.throws(() => validateDirectPhaseClockRaw(changed, p), /DIRECT_CLOCK_REFERENCE_REQUIRED/);
    }
    const stripped = structuredClone(result);
    delete stripped.clockReference;
    delete stripped.phaseClock.clockReference;
    account(stripped);
    assert.throws(() => validateDirectPhaseClockRaw(stripped, p), /DIRECT_CLOCK_REFERENCE_REQUIRED/);
    assert.throws(
        () => validateDirectPhaseClockRaw(result, { ...p, clockReference: undefined }),
        /DIRECT_CLOCK_REFERENCE_UNEXPECTED/,
    );
    assert.throws(
        () => validateDirectPhaseClockRaw(result, { ...p, name: "changed-profile" }),
        /DIRECT_CLOCK_REFERENCE_PROFILE/,
    );
    await t.test("fully resealed reference anchors outside the actual preparation are rejected", () => {
        const changed = structuredClone(result),
            boundary = result.phases[0].clock.start;
        changeReferenceAnchor(changed.clockReference, 4, (a) => {
            a.hrtimeBeforeNs = boundary.hrtimeBeforeNs;
            a.hrtimeAfterNs = boundary.hrtimeAfterNs;
            a.performanceMs = boundary.performanceMs;
        });
        assert.equal(verifyRunnerSanityRaw(changed.clockReference).rawSanity, "passed");
        assert.throws(() => validateDirectPhaseClockRaw(changed, p), /DIRECT_CLOCK_REFERENCE_ENCLOSURE/);
    });
    await t.test("reference Node executable must match the actual direct-clock Node", () => {
        const changed = structuredClone(result);
        for (const index of [4, 8])
            changeReferenceAnchor(changed.clockReference, index, (a) => {
                for (const key of ["identityBefore", "identityAfter"]) a[key].runtime.executableSha256 = "c".repeat(64);
            });
        assert.equal(verifyRunnerSanityRaw(changed.clockReference).rawSanity, "passed");
        assert.throws(() => validateDirectPhaseClockRaw(changed, p), /DIRECT_CLOCK_REFERENCE_RUNTIME/);
    });
});

test("clock reference startup failure retains a bounded sidecar and sends no HTTP", async (t) => {
    let requests = 0;
    const f = await fixture(t, (req, res) => {
        requests++;
        req.resume();
        res.end("{}");
    });
    const p = profile(f.endpoint, { clockReference: { ...clockReference, python: join(f.root, "missing-python") } });
    const trace = join(f.root, "failed.ndjson");
    await assert.rejects(run({ profile: p, case: p.cases[0], load: p.loads[0], id: "r1", repeat: 0 }, trace), {
        code: "ENOENT",
    });
    const raw = await readFile(`${trace}.clock-reference.partial.json`, "utf8"),
        evidence = JSON.parse(raw);
    assert.equal(requests, 0);
    assert.equal(evidence.status, "partial");
    assert.equal(evidence.failure, "ENOENT");
    assert.equal(evidence.clockReference, null);
    assert.equal(evidence.performanceAccepted, false);
    assert.ok(Buffer.byteLength(raw) <= 4 * clockReference.spec.maxRawBytes + 131072);
});

test("enabled functional probes retain the same profile/session and causal enclosure", async (t) => {
    const f = await fixture(t, (req, res) => {
        req.resume();
        res.end('{"result":{"data":[2]}}');
    });
    const p = profile(f.endpoint, { clockReference });
    const { result } = await execute(f, p, { probePhase: "preflight" });
    assert.equal(result.kind, "http-functional-probe");
    assert.equal(result.clockReference.binding.runIdentity.probePhase, "preflight");
    assert.equal(result.clockReference.binding.profileDigest, valueDigest(p));
    assert.equal(result.phases.length, 1);
    assert.equal(result.phases[0].name, "preflight");
    assert.equal(validateDirectPhaseClockRaw(result, p).clockReference.rawSanity, "passed");
});

test("real runner SIGTERM awaits its peer cleanup and writes partial evidence instead of a run", async (t) => {
    let child,
        sent = false;
    const f = await fixture(t, (req, res) => {
        req.resume();
        if (!sent) {
            sent = true;
            child.kill("SIGTERM");
        }
        res.end('{"result":{"data":[2]}}');
    });
    const p = profile(f.endpoint, { clockReference, warmupMs: 0, durationMs: 5000 }),
        request = join(f.root, "request.json"),
        trace = join(f.root, "cancel.ndjson"),
        output = join(f.root, "run.json");
    await writeFile(request, JSON.stringify({ profile: p, case: p.cases[0], load: p.loads[0], id: "r1", repeat: 0 }));
    child = spawn(
        process.execPath,
        [
            new URL("../components/http/runner.mjs", import.meta.url).pathname,
            "--request",
            request,
            "--trace",
            trace,
            "--output",
            output,
        ],
        { stdio: ["ignore", "ignore", "pipe"] },
    );
    let stderr = "";
    child.stderr.on("data", (b) => {
        stderr = (stderr + b).slice(0, 8192);
    });
    const timer = setTimeout(() => child.kill("SIGKILL"), 10000);
    t.after(() => {
        clearTimeout(timer);
        child.kill("SIGKILL");
    });
    const terminal = await new Promise((resolve, reject) => {
        child.once("error", reject);
        child.once("close", (code, signal) => resolve({ code, signal }));
    });
    clearTimeout(timer);
    assert.equal(sent, true, stderr);
    assert.deepEqual(terminal, { code: 6, signal: null }, stderr);
    await assert.rejects(readFile(output), { code: "ENOENT" });
    const evidence = JSON.parse(await readFile(`${trace}.clock-reference.partial.json`, "utf8"));
    assert.equal(evidence.status, "partial");
    assert.equal(evidence.clockReference.status, "partial");
    assert.equal(evidence.clockReference.failure, "SR_CALLER_CANCELLED");
    assert.equal(evidence.clockReference.terminal.closed, true);
    assert.equal(evidence.clockReference.terminal.stdoutEof, true);
    assert.equal(evidence.clockReference.terminal.stderrEof, true);
    assert.throws(() => process.kill(evidence.clockReference.peerPid, 0), { code: "ESRCH" });
});

test("enabled CLI/task workflow locks three actual sessions and independently rebuilds identical reports", async (t) => {
    const f = await fixture(t, (req, res) => {
        req.resume();
        res.end('{"result":{"data":[2]}}');
    });
    const p = profile(f.endpoint, { clockReference, serviceChecks: true, maxRequestsPerPhase: 2 });
    const file = join(f.root, "profile.json"),
        attempt = join(f.root, "attempt"),
        taskRoot = join(f.root, "tasks");
    await writeFile(file, JSON.stringify(p));
    const planned = await planHttp(file, attempt),
        frozen = JSON.parse(await readFile(planned.taskPlan, "utf8"));
    const task = await submitTask(taskRoot, frozen, "clock-reference-once");
    const terminal = await waitTask(taskRoot, task.taskId, 30000);
    assert.equal(terminal.state, "completed", JSON.stringify(terminal));
    const output = join(attempt, "output");
    for (const path of ["runs/r0001.json", "preflight/p0001.json", "postcheck/p0001.json"]) {
        const run = JSON.parse(await readFile(join(output, path), "utf8"));
        assert.equal(validateDirectPhaseClockRaw(run, p).clockReference.rawSanity, "passed");
        assert.equal(run.performanceAccepted, false);
        assert.throws(() => process.kill(run.clockReference.peerPid, 0), { code: "ESRCH" });
    }
    const first = await readFile(join(output, "analysis/report.json"), "utf8");
    const independent = await analyze(output);
    assert.equal(independent.performanceAccepted, false);
    assert.equal(independent.gates.correctness, "passed");
    const rebuilt = join(f.root, "rebuilt");
    await report(output, rebuilt);
    assert.equal(await readFile(join(rebuilt, "report.json"), "utf8"), first);
});
