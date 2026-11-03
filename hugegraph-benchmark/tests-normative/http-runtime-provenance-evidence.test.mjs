/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, mkdir, readFile, writeFile, cp, rm, chmod, symlink, realpath, open } from "node:fs/promises";
import { execFile, spawn } from "node:child_process";
import { getEventListeners } from "node:events";
import { promisify } from "node:util";
import { join, resolve } from "node:path";
import { tmpdir } from "node:os";
import { pathToFileURL } from "node:url";
import { bytesDigest, valueDigest } from "../components/http/common.mjs";
import { verifyRuntimeProvenance, isVerifiedRuntimeProof } from "../components/http/runtime-provenance-evidence.mjs";

// Negative local protocol fixtures only. No Docker/runtime proof is generated or branded by these tests.
const invoke = promisify(execFile),
    adapter = resolve("components/http/runtime-provenance-evidence.mjs");
const python = await realpath(
    (
        await invoke(process.env.HGBENCH_PYTHON || "python3", ["-I", "-B", "-c", "import sys;print(sys.executable)"])
    ).stdout.trim(),
);
const sources = [
    "runtime-provenance.py",
    "runtime-provenance-worker.py",
    "runtime-provenance-node.mjs",
    "comparison-workflow.py",
];
const encoded = (value) => Buffer.from(JSON.stringify(value, null, 2) + "\n");
const read = async (path) => JSON.parse(await readFile(path, "utf8"));

async function fixture(t, behavior = "error") {
    const root = await mkdtemp(join(tmpdir(), "hgbench-runtime-adapter-negative-")),
        proof = join(root, "proof"),
        control = join(proof, "control"),
        fake = join(root, "negative-only-python"),
        marker = join(root, "negative-worker.json");
    t.after(() => rm(root, { recursive: true, force: true }));
    await mkdir(control, { recursive: true });
    const rows = [];
    const expectedSourceHashes = {};
    for (const name of sources) {
        const raw = await readFile(resolve("components/http", name));
        await writeFile(join(control, name), raw);
        rows.push({ path: `control/${name}`, bytes: raw.length, sha256: bytesDigest(raw) });
        expectedSourceHashes[name] = bytesDigest(raw);
    }
    const payload = encoded({ kind: "local-negative-protocol-fixture", noRuntimeAttestation: true });
    await writeFile(join(proof, "fixture.json"), payload);
    rows.push({ path: "fixture.json", bytes: payload.length, sha256: bytesDigest(payload) });
    rows.sort((a, b) => (a.path < b.path ? -1 : a.path > b.path ? 1 : 0));
    const args = { proofDirectory: proof, python: fake, expectedProofDigest: valueDigest(rows), expectedSourceHashes };
    await writeFile(
        join(proof, "raw.lock.json"),
        encoded({
            schemaVersion: 1,
            kind: "http-runtime-provenance-raw-lock",
            files: rows,
            contentDigest: args.expectedProofDigest,
        }),
    );
    const childReady = join(root, "grandchild-ready");
    const childCode =
        "import os,signal,time;signal.signal(signal.SIGTERM,signal.SIG_IGN);" +
        `open(${JSON.stringify(childReady)},'w').write(str(os.getpid()));time.sleep(60)`;
    const body =
        behavior === "hang"
            ? "signal.signal(signal.SIGTERM,signal.SIG_IGN)\n" +
              `child=subprocess.Popen([sys.executable,'-B','-c',${JSON.stringify(childCode)}])\n` +
              `deadline=time.monotonic()+4\nwhile not os.path.exists(${JSON.stringify(childReady)}):\n` +
              " if time.monotonic()>deadline: sys.exit(3)\n time.sleep(0.01)\n" +
              `open(${JSON.stringify(marker)},'w').write(json.dumps({'pid':os.getpid(),'child':child.pid,'group':os.getpgrp()}))\n` +
              "time.sleep(60)\n"
            : `open(${JSON.stringify(marker)},'w').write(json.dumps({'pid':os.getpid(),'group':os.getpgrp()}))\n` +
              (behavior === "overflow"
                  ? "os.write(1,b'x'*(2*1024*1024));time.sleep(60)\n"
                  : behavior === "protocol"
                    ? "print('not a JSON runtime proof')\n"
                    : "sys.exit(2)\n");
    assert(!/[\r\n ]/.test(python), "fixture shebang requires an absolute executable without whitespace");
    await writeFile(
        fake,
        `#!${python}\n# LOCAL NEGATIVE PROTOCOL FIXTURE; never emits a valid proof\n` +
            "import json,os,signal,subprocess,sys,time\n" +
            body,
    );
    await chmod(fake, 0o700);
    return { root, proof, marker, args, rows };
}
async function waitFor(path, timeout = 4000) {
    const until = performance.now() + timeout;
    while (performance.now() < until) {
        try {
            return await read(path);
        } catch (error) {
            if (error.code !== "ENOENT") throw error;
        }
        await new Promise((ok) => setTimeout(ok, 20));
    }
    throw new Error("negative fixture readiness deadline");
}
function killGroup(group) {
    try {
        process.kill(-group, "SIGKILL");
    } catch (error) {
        if (error.code !== "ESRCH") throw error;
    }
}
async function executing(pid) {
    if (process.platform === "linux") {
        try {
            const raw = await readFile(`/proc/${pid}/stat`, "utf8"),
                state = raw.slice(raw.lastIndexOf(") ") + 2).split(/\s+/)[0];
            return !["Z", "X", "x"].includes(state);
        } catch (error) {
            if (error.code === "ENOENT") return false;
            throw error;
        }
    }
    try {
        process.kill(pid, 0);
        return true;
    } catch (error) {
        if (error.code === "ESRCH") return false;
        throw error;
    }
}
async function assertStopped(pids) {
    const until = performance.now() + 2000;
    while (performance.now() < until) {
        if (!(await Promise.all(pids.map(executing))).some(Boolean)) return;
        await new Promise((ok) => setTimeout(ok, 20));
    }
    assert.fail("negative fixture verifier group is still executing");
}
function assertReaped(pid) {
    assert.throws(
        () => process.kill(pid, 0),
        (error) => error.code === "ESRCH",
        "the direct verifier child must be reaped before replay settles",
    );
}
async function reseal(f) {
    const rows = [];
    for (const old of f.rows) {
        const raw = await readFile(join(f.proof, old.path));
        rows.push({ path: old.path, bytes: raw.length, sha256: bytesDigest(raw) });
    }
    f.args.expectedProofDigest = valueDigest(rows);
    await writeFile(
        join(f.proof, "raw.lock.json"),
        encoded({
            schemaVersion: 1,
            kind: "http-runtime-provenance-raw-lock",
            files: rows,
            contentDigest: f.args.expectedProofDigest,
        }),
    );
}

test("runtime proof identity cannot be minted by caller JSON or pre-aborted invocation", async () => {
    assert.equal(isVerifiedRuntimeProof({ kind: "http-runtime-provenance-verification", valid: true }), false);
    const controller = new AbortController();
    controller.abort();
    await assert.rejects(
        verifyRuntimeProvenance({
            proofDirectory: "/does-not-exist",
            python,
            expectedProofDigest: "a".repeat(64),
            signal: controller.signal,
        }),
        /RUNTIME_PROOF_CANCELLED/,
    );
});

test("runtime adapter rejects changed raw locks, missing/extra content and resealed implementation changes", async (t) => {
    for (const [name, mutate, code] of [
        [
            "lock-kind",
            async (f) => {
                const p = join(f.proof, "raw.lock.json"),
                    v = await read(p);
                v.kind = "forged";
                await writeFile(p, encoded(v));
            },
            /RUNTIME_PROOF_LOCK/,
        ],
        [
            "lock-digest",
            async (f) => {
                f.args.expectedProofDigest = "a".repeat(64);
            },
            /RUNTIME_PROOF_LOCK/,
        ],
        [
            "extra",
            async (f) => {
                await writeFile(join(f.proof, "extra.json"), "{}\n");
            },
            /RUNTIME_PROOF_CONTENT/,
        ],
        [
            "missing",
            async (f) => {
                await rm(join(f.proof, "fixture.json"));
            },
            /RUNTIME_PROOF_CONTENT/,
        ],
        [
            "raw-changed",
            async (f) => {
                await writeFile(join(f.proof, "fixture.json"), "changed\n");
            },
            /RUNTIME_PROOF_CONTENT/,
        ],
        [
            "implementation",
            async (f) => {
                const p = join(f.proof, "control/runtime-provenance-worker.py");
                await writeFile(p, (await readFile(p)) + "\n# local negative mutation\n");
                await reseal(f);
            },
            /RUNTIME_PROOF_IMPLEMENTATION/,
        ],
        [
            "source-expectation",
            async (f) => {
                f.args.expectedSourceHashes["runtime-provenance.py"] = "a".repeat(64);
            },
            /RUNTIME_PROOF_IMPLEMENTATION/,
        ],
        [
            "source-shape",
            async (f) => {
                delete f.args.expectedSourceHashes["runtime-provenance.py"];
            },
            /RUNTIME_PROOF_SOURCE_EXPECTATIONS/,
        ],
        [
            "payload-symlink",
            async (f) => {
                await symlink(join(f.proof, "fixture.json"), join(f.proof, "link"));
            },
            /RUNTIME_PROOF_SYMLINK/,
        ],
        [
            "lock-symlink",
            async (f) => {
                await cp(join(f.proof, "raw.lock.json"), join(f.root, "real-lock.json"));
                await rm(join(f.proof, "raw.lock.json"));
                await symlink(join(f.root, "real-lock.json"), join(f.proof, "raw.lock.json"));
            },
            /ELOOP|RUNTIME_PROOF_FILE/,
        ],
    ])
        await t.test(name, async (t) => {
            const f = await fixture(t);
            await mutate(f);
            await assert.rejects(verifyRuntimeProvenance(f.args), code);
            await assert.rejects(readFile(f.marker), /ENOENT/);
        });
});

test("FIFO payload and raw lock are rejected in a bounded isolated owner without invoking verifier", async (t) => {
    for (const name of ["extra.fifo", "raw.lock.json"])
        await t.test(name, async (t) => {
            const f = await fixture(t);
            if (name === "raw.lock.json") await rm(join(f.proof, name));
            await invoke("mkfifo", [join(f.proof, name)]);
            const config = join(f.root, "config.json"),
                wrapper = join(f.root, "fifo-owner.mjs");
            await writeFile(config, encoded(f.args));
            await writeFile(
                wrapper,
                `import {readFile} from 'node:fs/promises';\n` +
                    `import {verifyRuntimeProvenance} from ${JSON.stringify(pathToFileURL(adapter).href)};\n` +
                    `try{await verifyRuntimeProvenance(JSON.parse(await readFile(process.argv[2],'utf8')));process.exitCode=7}` +
                    `catch(error){process.stdout.write(JSON.stringify({code:error.code})+'\\n')}\n`,
            );
            const out = await invoke(process.execPath, [wrapper, config], {
                timeout: 2500,
                killSignal: "SIGKILL",
                maxBuffer: 65536,
            });
            assert.match(JSON.parse(out.stdout).code, /^RUNTIME_PROOF_FILE(?:_TYPE)?$/);
            await assert.rejects(readFile(f.marker), /ENOENT/);
        });
});

test("oversized raw lock and sparse payload fail byte budgets before verifier invocation", async (t) => {
    for (const [name, bytes, code] of [
        ["raw.lock.json", 1024 * 1024 + 1, /RUNTIME_PROOF_FILE/],
        ["huge-payload", 16 * 1024 * 1024 + 1, /RUNTIME_PROOF_BYTES/],
    ])
        await t.test(name, async (t) => {
            const f = await fixture(t),
                handle = await open(join(f.proof, name), "w");
            try {
                await handle.truncate(bytes);
            } finally {
                await handle.close();
            }
            await assert.rejects(verifyRuntimeProvenance(f.args), code);
            await assert.rejects(readFile(f.marker), /ENOENT/);
        });
});

test("negative verifier exit, malformed JSON and bounded output never create verified runtime identity", async (t) => {
    for (const [behavior, code] of [
        ["error", /RUNTIME_PROOF_VERIFIER_FAILED/],
        ["protocol", /RUNTIME_PROOF_PROTOCOL/],
        ["overflow", /RUNTIME_PROOF_OUTPUT/],
    ])
        await t.test(behavior, async (t) => {
            const f = await fixture(t, behavior);
            await assert.rejects(verifyRuntimeProvenance(f.args), code);
            const child = await read(f.marker);
            await assertStopped([child.pid]);
        });
});

test(
    "AbortSignal reaps TERM-ignoring detached verifier and stops inherited-pipe grandchild",
    { timeout: 14000 },
    async (t) => {
        const f = await fixture(t, "hang"),
            controller = new AbortController(),
            before = [
                process.rawListeners("SIGTERM"),
                process.rawListeners("SIGINT"),
                getEventListeners(controller.signal, "abort"),
            ];
        let child,
            complete = false;
        t.after(() => {
            if (!complete && child) killGroup(child.group);
        });
        const pending = verifyRuntimeProvenance({ ...f.args, signal: controller.signal });
        const rejection = assert.rejects(pending, /RUNTIME_PROOF_CANCELLED/);
        child = await waitFor(f.marker);
        assert.equal(child.pid, child.group);
        controller.abort();
        await rejection;
        assertReaped(child.pid);
        // Grandchildren may be briefly adopted zombies under a container PID 1; they must stop executing.
        await assertStopped([child.pid, child.child]);
        complete = true;
        assert.deepEqual(
            [
                process.rawListeners("SIGTERM"),
                process.rawListeners("SIGINT"),
                getEventListeners(controller.signal, "abort"),
            ],
            before,
        );
    },
);

test(
    "owner Node SIGTERM cleans detached verifier group before finishing negative replay",
    { timeout: 14000 },
    async (t) => {
        const f = await fixture(t, "hang"),
            config = join(f.root, "config.json"),
            wrapper = join(f.root, "signal-owner.mjs");
        await writeFile(config, encoded(f.args));
        await writeFile(
            wrapper,
            `import {readFile} from 'node:fs/promises';\n` +
                `import {verifyRuntimeProvenance,isVerifiedRuntimeProof} from ${JSON.stringify(pathToFileURL(adapter).href)};\n` +
                `try{const result=await verifyRuntimeProvenance(JSON.parse(await readFile(process.argv[2],'utf8')));` +
                `process.stdout.write(JSON.stringify({unexpectedBrand:isVerifiedRuntimeProof(result)})+'\\n');process.exitCode=7}` +
                `catch(error){process.stdout.write(JSON.stringify({code:error.code})+'\\n')}\n`,
        );
        const owner = spawn(process.execPath, [wrapper, config], { stdio: ["ignore", "pipe", "pipe"] }),
            stdout = [],
            stderr = [];
        owner.stdout.on("data", (v) => stdout.push(v));
        owner.stderr.on("data", (v) => stderr.push(v));
        const closed = new Promise((ok, fail) => {
            owner.once("error", fail);
            owner.once("close", (code, signal) => ok({ code, signal }));
        });
        let child,
            complete = false;
        t.after(() => {
            if (!complete) {
                owner.kill("SIGKILL");
                if (child) killGroup(child.group);
            }
        });
        child = await waitFor(f.marker);
        assert.notEqual(child.pid, owner.pid);
        assert.equal(child.pid, child.group);
        owner.kill("SIGTERM");
        const result = await closed;
        assert.equal(result.code, 0, Buffer.concat(stderr).toString());
        assert.equal(JSON.parse(Buffer.concat(stdout).toString()).code, "RUNTIME_PROOF_SIGTERM");
        assertReaped(child.pid);
        await assertStopped([child.pid, child.child]);
        complete = true;
    },
);
