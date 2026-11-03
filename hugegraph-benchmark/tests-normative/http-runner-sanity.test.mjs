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
import { mkdtemp, writeFile, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createHash } from "node:crypto";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { getEventListeners } from "node:events";
import {
    startRunnerSanity,
    captureRunnerSanity,
    finishRunnerSanity,
    stopRunnerSanity,
    verifyRunnerSanityRaw,
    validateRunnerSanitySpec,
} from "../components/http/runner-sanity.mjs";

const sha = (raw) => createHash("sha256").update(raw).digest("hex");
const python = process.env.HGBENCH_PYTHON ?? "/usr/bin/python3";
const fixturePython = (
    await promisify(execFile)(python, ["-I", "-S", "-c", "import os,sys; print(os.path.realpath(sys.executable))"])
).stdout.trim();
const spec = {
    schemaVersion: 1,
    kind: "http-runner-sanity-spec",
    sourceOrigin: "fixture",
    startupMs: 2000,
    exchangeMs: 1000,
    finishMs: 500,
    wallMs: 10000,
    maxFrameBytes: 8192,
    maxRawBytes: 65536,
    maxStderrBytes: 2048,
};
const binding = {
    sessionId: "a".repeat(32),
    runIdentity: { id: "r1", caseId: "c1", loadId: "l1", repeat: 0, probePhase: null },
    profileDigest: "b".repeat(64),
};

function changeFrame(e, index, patch) {
    const value = JSON.parse(e.frames[index].raw);
    patch(value);
    const raw = JSON.stringify(value) + "\n";
    Object.assign(e.frames[index], { raw, bytes: Buffer.byteLength(raw), sha256: sha(raw) });
    if ([3, 4, 7, 8].includes(index)) {
        const receipt = index < 6 ? 5 : 9,
            challenge = index % 4 === 3;
        changeFrame(e, receipt, (v) => {
            v[challenge ? "challengeRaw" : "responseRaw"] = raw;
            v[challenge ? "challengeSha256" : "responseSha256"] = sha(raw);
        });
    }
    e.wireBytes = e.frames.reduce((n, f) => n + f.bytes, 0);
}

test("actual caller Node encloses real local HTTP work with two fixed Python causal roundtrips", async (t) => {
    const server = http.createServer((req, res) => {
        req.resume();
        setTimeout(() => res.end("ok"), 30);
    });
    await new Promise((ok) => server.listen(0, "127.0.0.1", ok));
    t.after(async () => {
        server.closeAllConnections();
        await new Promise((ok) => server.close(ok));
    });
    let handle,
        completed = false;
    t.after(async () => {
        if (handle && !completed) await stopRunnerSanity(handle).catch(() => {});
    });
    const controller = new AbortController();
    handle = await startRunnerSanity({ python, spec, binding, signal: controller.signal });
    const pre = await captureRunnerSanity(handle, "pre"),
        before = process.hrtime.bigint();
    assert.equal(await (await fetch(`http://127.0.0.1:${server.address().port}`)).text(), "ok");
    const after = process.hrtime.bigint(),
        post = await captureRunnerSanity(handle, "post");
    const evidence = await finishRunnerSanity(handle);
    completed = true;
    assert.equal(getEventListeners(controller.signal, "abort").length, 0);
    const result = verifyRunnerSanityRaw(evidence);
    assert.equal(result.rawSanity, "passed");
    assert.equal(result.verified, false);
    assert.equal(result.hostAssociation, "unverified");
    assert.equal(result.performanceAccepted, false);
    assert.equal(result.sourceOrigin, "fixture");
    assert.equal(evidence.callerPid, process.pid);
    assert.deepEqual(evidence.terminal, {
        closed: true,
        stdoutEof: true,
        stderrEof: true,
        exitCode: 0,
        signal: null,
        termSent: false,
        killSent: false,
        forced: false,
    });
    assert.ok(evidence.wireBytes < spec.maxRawBytes);
    const a = JSON.parse(pre.responseRaw).anchor,
        b = JSON.parse(post.responseRaw).anchor;
    assert.equal(a.identityBefore.pid, process.pid);
    assert.equal(b.identityBefore.pid, process.pid);
    assert.ok(BigInt(a.hrtimeAfterNs) <= before && before <= after && after <= BigInt(b.hrtimeBeforeNs));
    assert.equal(result.samples[0].observedContainment, null);
    assert.deepEqual(result.pendingGates, [
        "external-runtime-source-lock",
        "host-collector-namespace-binding",
        "predeclared-quality-thresholds",
    ]);
    await t.test("fresh read-only CLI rebuilds the same raw verdict and explains its limits", async () => {
        const directory = await mkdtemp(join(tmpdir(), "runner-sanity-verify-"));
        try {
            const file = join(directory, "raw.json");
            await writeFile(file, JSON.stringify(evidence));
            const cli = new URL("../components/http/runner-sanity.mjs", import.meta.url).pathname;
            const observed = await promisify(execFile)(process.execPath, [cli, "verify", "--evidence", file]);
            assert.deepEqual(JSON.parse(observed.stdout), { ok: true, data: result });
            const help = await promisify(execFile)(process.execPath, [cli, "--help"]);
            assert.match(help.stdout, /never launches a peer/);
            assert.match(help.stdout, /verified=false/);
        } finally {
            await rm(directory, { recursive: true, force: true });
        }
    });
    await assert.rejects(captureRunnerSanity(handle, "pre"), /SR_CONTEXT/);
    for (const [name, patch, expected] of [
        [
            "asserted verification",
            (e) => {
                e.verified = true;
            },
            /SR_RAW_KIND/,
        ],
        ["removed pending gate", (e) => e.pendingGates.pop(), /SR_PENDING_GATES/],
        [
            "false terminal EOF",
            (e) => {
                e.terminal.stdoutEof = false;
            },
            /SR_RAW_TERMINAL/,
        ],
        [
            "unknown source SHA",
            (e) => {
                e.clientSources[0].sha256 = "invalid";
            },
            /SR_SOURCE_IDENTITY/,
        ],
        [
            "changed bytes",
            (e) => {
                e.frames[3].raw += " ";
            },
            /SR_FRAME_DIGEST/,
        ],
        ["missing receipt", (e) => e.frames.splice(5, 1), /SR_RAW_FRAMES/],
        [
            "false reference parent",
            (e) => {
                e.callerPid++;
            },
            /SR_REFERENCE_PARENT/,
        ],
        [
            "wrong nonce",
            (e) =>
                changeFrame(e, 4, (v) => {
                    v.nonce = "c".repeat(32);
                }),
            /SR_EXCHANGE_BINDING/,
        ],
        [
            "unsafe ns number",
            (e) =>
                changeFrame(e, 4, (v) => {
                    v.anchor.hrtimeBeforeNs = 9007199254740993;
                }),
            /SR_INTEGER_PRECISION/,
        ],
        [
            "changed Node process",
            (e) =>
                changeFrame(e, 8, (v) => {
                    v.anchor.identityBefore.pid++;
                }),
            /SR_NODE_IDENTITY_DRIFT/,
        ],
        [
            "reverse S R",
            (e) =>
                changeFrame(e, 5, (v) => {
                    v.sentMonotonicNs = v.receivedMonotonicNs + "0";
                }),
            /SR_EXCHANGE_TIME_ORDER/,
        ],
        [
            "replaced reference file",
            (e) =>
                changeFrame(e, 11, (v) => {
                    v.files[0].metadata.inode = "1";
                }),
            /SR_REFERENCE_FILE_DRIFT/,
        ],
    ])
        await t.test(name, () => {
            const changed = structuredClone(evidence);
            patch(changed);
            assert.throws(() => verifyRunnerSanityRaw(changed), expected);
        });
    await t.test("duplicate raw JSON key is rejected after recomputing its outer digest", () => {
        const changed = structuredClone(evidence),
            f = changed.frames[0];
        f.raw = f.raw.replace("{", '{"kind":"init",');
        f.bytes = Buffer.byteLength(f.raw);
        f.sha256 = sha(f.raw);
        changed.wireBytes = changed.frames.reduce((n, f) => n + f.bytes, 0);
        assert.throws(() => verifyRunnerSanityRaw(changed), /SR_DUPLICATE_JSON_KEY/);
    });
    await t.test("deleting mandatory Node runtime and process token fails even after complete raw resealing", () => {
        const changed = structuredClone(evidence);
        for (const index of [4, 8])
            changeFrame(changed, index, (v) => {
                for (const key of ["identityBefore", "identityAfter"]) {
                    delete v.anchor[key].runtime;
                    delete v.anchor[key].processToken;
                }
            });
        assert.throws(() => verifyRunnerSanityRaw(changed), /SR_NODE_IDENTITY_FIELDS/);
    });
    await t.test("self-recorded Node anchor implementation must match its captured component source", () => {
        const changed = structuredClone(evidence);
        for (const index of [4, 8])
            changeFrame(changed, index, (v) => {
                for (const key of ["identityBefore", "identityAfter"])
                    v.anchor[key].runtime.componentSha256 = "c".repeat(64);
            });
        assert.throws(() => verifyRunnerSanityRaw(changed), /SR_ANCHOR_SOURCE_BINDING/);
    });
    await t.test("deleting mandatory reference clock fields fails with both identities and frames resealed", () => {
        const changed = structuredClone(evidence);
        for (const index of [1, 11])
            changeFrame(changed, index, (v) => {
                delete v.identity.clock;
            });
        assert.throws(() => verifyRunnerSanityRaw(changed), /SR_REFERENCE_IDENTITY_FIELDS/);
    });
    await t.test("Python peer source must match the independently captured Node source record", () => {
        const changed = structuredClone(evidence);
        changeFrame(changed, 1, (v) => {
            v.files[0].sha256 = "c".repeat(64);
        });
        assert.throws(() => verifyRunnerSanityRaw(changed), /SR_REFERENCE_SOURCE_BINDING/);
    });
});

test("same session refuses an out-of-order caller and preserves bounded partial terminal evidence", async () => {
    const h = await startRunnerSanity({ python, spec, binding });
    await assert.rejects(captureRunnerSanity(h, "post"), (error) => {
        assert.equal(error.code, "SR_STAGE");
        assert.equal(error.evidence.status, "partial");
        assert.equal(error.evidence.verified, false);
        assert.equal(error.evidence.terminal.closed, true);
        return true;
    });
});

test("caller cancellation reaps its Python child and never produces a completed clock", async () => {
    const controller = new AbortController();
    const h = await startRunnerSanity({ python, spec, binding, signal: controller.signal });
    await captureRunnerSanity(h, "pre");
    controller.abort();
    const evidence = await stopRunnerSanity(h);
    assert.equal(evidence.status, "partial");
    assert.equal(evidence.failure, "SR_CALLER_CANCELLED");
    assert.equal(evidence.terminal.closed, true);
    assert.equal(evidence.terminal.stdoutEof, true);
    assert.equal(evidence.terminal.stderrEof, true);
    assert.throws(() => process.kill(evidence.peerPid, 0), { code: "ESRCH" });
    assert.throws(() => verifyRunnerSanityRaw(evidence), /SR_RAW_KIND/);
    assert.equal(getEventListeners(controller.signal, "abort").length, 0);
    assert.deepEqual(await stopRunnerSanity(h), evidence); // finally may repeat cleanup, never resend signals.
});

test("a pre-aborted request is rejected before source reads or Python dispatch", async () => {
    const controller = new AbortController();
    controller.abort();
    await assert.rejects(
        startRunnerSanity({ python, spec, binding, signal: controller.signal }),
        (error) => error.code === "SR_CALLER_CANCELLED" && error.evidence === undefined,
    );
});

test("abort during startup reaps a real TERM-ignoring fault peer and removes its listener", async (t) => {
    const root = await mkdtemp(join(tmpdir(), "runner-sanity-abort-startup-"));
    t.after(() => rm(root, { recursive: true, force: true }));
    const executable = join(root, "fault-peer"),
        marker = join(root, "pid");
    await writeFile(
        executable,
        `#!${fixturePython}\nimport os,signal,time
signal.signal(signal.SIGTERM,signal.SIG_IGN)
with open(${JSON.stringify(marker)},'w') as f:f.write(str(os.getpid()))
time.sleep(10)\n`,
        { mode: 0o700 },
    );
    const controller = new AbortController();
    const pending = startRunnerSanity({
        python: executable,
        spec: { ...spec, startupMs: 3000 },
        binding,
        signal: controller.signal,
    });
    pending.catch(() => {}); // Keep a fast startup failure handled while observing the marker.
    // Observe actual child readiness, avoiding an elapsed-time guess about process startup.
    const deadline = performance.now() + 2500;
    let pid;
    try {
        while (pid === undefined && performance.now() < deadline) {
            try {
                const value = await readFile(marker, "utf8");
                if (/^[1-9][0-9]*$/.test(value)) pid = Number(value);
            } catch (error) {
                if (error.code !== "ENOENT") throw error;
            }
            if (pid === undefined) await new Promise((ok) => setTimeout(ok, 20));
        }
        assert.ok(Number.isSafeInteger(pid) && pid > 0);
    } finally {
        controller.abort();
    }
    await assert.rejects(pending, (error) => {
        assert.equal(error.code, "SR_CALLER_CANCELLED");
        assert.equal(error.evidence.status, "partial");
        assert.equal(error.evidence.peerPid, pid);
        assert.equal(error.evidence.terminal.closed, true);
        assert.equal(error.evidence.terminal.signal, "SIGKILL");
        assert.equal(error.evidence.terminal.forced, true);
        assert.equal(error.evidence.terminal.stdoutEof, true);
        assert.equal(error.evidence.terminal.stderrEof, true);
        return true;
    });
    assert.equal(getEventListeners(controller.signal, "abort").length, 0);
    assert.throws(() => process.kill(pid, 0), { code: "ESRCH" });
});

test("invalid budgets reject before any child is started", () => {
    for (const patch of [
        { startupMs: 0 },
        { wallMs: 1000 },
        { maxFrameBytes: 8192, maxRawBytes: 16384 },
        { unknown: true },
    ])
        assert.throws(() => validateRunnerSanitySpec({ ...spec, ...patch }), /SR_SPEC_/);
});

test("startup EOF, drip feed and stderr flood are bounded failures with actual child cleanup", async (t) => {
    const root = await mkdtemp(join(tmpdir(), "runner-sanity-fault-"));
    t.after(() => rm(root, { recursive: true, force: true }));
    for (const [name, body, expected] of [
        ["eof", "import sys; sys.exit(7)", /SR_PEER_EOF/],
        ["drip", "import os,time\nfor i in range(100):\n os.write(1,b' ');time.sleep(.02)", /SR_DEADLINE/],
        ["stderr", "import os,time;os.write(2,b'x'*8192);time.sleep(10)", /SR_STDERR_BUDGET/],
    ])
        await t.test(name, async () => {
            const executable = join(root, name);
            await writeFile(executable, `#!${fixturePython}\n${body}\n`, { mode: 0o700 });
            const started = performance.now();
            await assert.rejects(
                startRunnerSanity({
                    python: executable,
                    spec: { ...spec, startupMs: name === "drip" ? 1000 : 2000 },
                    binding,
                }),
                (error) => {
                    assert.match(error.code, expected);
                    assert.equal(error.evidence.status, "partial");
                    assert.equal(error.evidence.terminal.closed, true);
                    assert.equal(error.evidence.terminal.stdoutEof, true);
                    assert.equal(error.evidence.terminal.stderrEof, true);
                    assert.ok(error.evidence.stderrRetainedBytes <= spec.maxStderrBytes);
                    assert.throws(() => process.kill(error.evidence.peerPid, 0), { code: "ESRCH" });
                    return true;
                },
            );
            assert.ok(performance.now() - started < 3500);
        });
});

test("an idle caller still gets a wall watchdog and KILL cleanup for a TERM-ignoring fault peer", async (t) => {
    const root = await mkdtemp(join(tmpdir(), "runner-sanity-wall-"));
    t.after(() => rm(root, { recursive: true, force: true }));
    const executable = join(root, "fault-peer");
    await writeFile(
        executable,
        `#!${fixturePython}\nimport os,sys,json,time,signal
signal.signal(signal.SIGTERM,signal.SIG_IGN)
binding=json.loads(sys.stdin.readline())['binding']
print(json.dumps(dict(kind='ready',binding=binding,identity=dict(pid=os.getpid(),parentPid=os.getppid()),files=[])),flush=True)
time.sleep(10)
`,
        { mode: 0o700 },
    );
    // Deliberate malformed/inert reference; this test only exercises the independent process watchdog.
    // Parallel test files can delay cold Python readiness beyond one second.
    // Keep startup separate from the idle-wall assertion, matching the nearby
    // startup-abort fixture's allowance; production budgets remain unchanged.
    const watchdogSpec = { ...spec, startupMs: 3000, exchangeMs: 100, finishMs: 100, wallMs: 4000 };
    const h = await startRunnerSanity({
        python: executable,
        spec: watchdogSpec,
        binding,
    });
    await new Promise((ok) => setTimeout(ok, watchdogSpec.wallMs + 300));
    const evidence = await stopRunnerSanity(h);
    assert.equal(evidence.failure, "SR_WALL_DEADLINE");
    assert.equal(evidence.status, "partial");
    assert.equal(evidence.terminal.closed, true);
    assert.equal(evidence.terminal.forced, true);
    assert.equal(evidence.terminal.signal, "SIGKILL");
    assert.throws(() => process.kill(evidence.peerPid, 0), { code: "ESRCH" });
});
