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
import { mkdtemp, readFile, writeFile, readdir, rm, access, realpath, rename, symlink, mkdir } from "node:fs/promises";
import { execFile, spawn } from "node:child_process";
import { promisify } from "node:util";
import { join, resolve } from "node:path";
import { tmpdir } from "node:os";
import { bytesDigest, valueDigest } from "../components/http/common.mjs";
import { isVerifiedRuntimeProof, verifyRuntimeProvenance } from "../components/http/runtime-provenance-evidence.mjs";
import {
    bundleRuntimeProvenance,
    verifyRuntimeProvenanceBundle,
} from "../components/http/runtime-provenance-bundle.mjs";

// Protocol-only synthetic observations. Real local Python parses these records;
// these tests never start Docker, execute the pretend binaries, or qualify a runtime.
const exec = promisify(execFile),
    MiB = 1024 * 1024,
    cli = resolve("components/http/runtime-provenance-bundle.mjs");
const python = await realpath(
    (
        await exec(process.env.HGBENCH_PYTHON ?? "python3", ["-I", "-B", "-c", "import sys;print(sys.executable)"])
    ).stdout.trim(),
);
const json = async (file) => JSON.parse(await readFile(file, "utf8"));
const canonical = (value) => {
    const sorted = (v) =>
        Array.isArray(v)
            ? v.map(sorted)
            : v && typeof v === "object"
              ? Object.fromEntries(
                    Object.keys(v)
                        .sort()
                        .map((key) => [key, sorted(v[key])]),
                )
              : v;
    return Buffer.from(JSON.stringify(sorted(value)) + "\n");
};
async function fixture(t) {
    const root = await mkdtemp(join(tmpdir(), "hgbench-runtime-bundle-fixture-")),
        proof = join(root, "proof");
    t.after(() => rm(root, { recursive: true, force: true }));
    await exec(
        python,
        [
            "-I",
            "-B",
            "-c",
            [
                "import sys,shutil,json",
                `sys.path.insert(0,${JSON.stringify(resolve("tests-normative"))})`,
                "from test_runtime_provenance import Provenance,MOD",
                "t=Provenance();t.setUp()",
                "try:",
                " p=t.complete();v=MOD.verify(p)",
                " assert v['valid']",
                " MOD.save(p/'runtime-binding.json',v['runtimeBinding'])",
                " MOD.save(p/'external-expectations.json',v['externalExpectations'])",
                ` shutil.copytree(p,${JSON.stringify(proof)})`,
                "finally: t.doCleanups()",
            ].join("\n"),
        ],
        { timeout: 15000, maxBuffer: MiB },
    );
    const expectedProofDigest = (await json(join(proof, "raw.lock.json"))).contentDigest;
    const outputFile = join(root, "runtime-proof.json"),
        args = { proofDirectory: proof, python, expectedProofDigest, outputFile, maxBundleBytes: 8 * MiB };
    return { root, proof, args, outputFile, expectedProofDigest };
}
async function pack(t) {
    const f = await fixture(t),
        receipt = await bundleRuntimeProvenance(f.args),
        raw = await readFile(f.outputFile);
    return {
        ...f,
        receipt,
        raw,
        bundle: JSON.parse(raw),
        verifyArgs: {
            file: f.outputFile,
            python,
            expectedFileSha256: bytesDigest(raw),
            expectedProofDigest: f.expectedProofDigest,
        },
    };
}
async function mutateBundle(f, change, options = {}) {
    const bundle = structuredClone(f.bundle);
    change(bundle);
    const raw = options.raw ?? canonical(bundle),
        file = join(f.root, "mutation.json");
    await writeFile(file, raw);
    return { ...f.verifyArgs, file, expectedFileSha256: bytesDigest(raw) };
}
async function reseal(proof) {
    const paths = [];
    async function walk(relative = "") {
        for (const name of (await readdir(join(proof, relative))).sort()) {
            const path = relative ? relative + "/" + name : name;
            if (
                ["raw.lock.json", "runtime-binding.json", "external-expectations.json"].includes(path) ||
                path === "recovery"
            )
                continue;
            const candidate = join(proof, path);
            try {
                await readdir(candidate);
                await walk(path);
            } catch (error) {
                if (error.code !== "ENOTDIR") throw error;
                const raw = await readFile(candidate);
                paths.push({ path, bytes: raw.length, sha256: bytesDigest(raw) });
            }
        }
    }
    await walk();
    paths.sort((a, b) => (a.path < b.path ? -1 : a.path > b.path ? 1 : 0));
    const digest = valueDigest(paths);
    await writeFile(
        join(proof, "raw.lock.json"),
        JSON.stringify({
            schemaVersion: 1,
            kind: "http-runtime-provenance-raw-lock",
            files: paths,
            contentDigest: digest,
        }) + "\n",
    );
    await rm(join(proof, "runtime-binding.json"), { force: true });
    await rm(join(proof, "external-expectations.json"), { force: true });
    return digest;
}
async function waitFor(file) {
    const deadline = performance.now() + 5000;
    while (performance.now() < deadline) {
        try {
            return await json(file);
        } catch (error) {
            if (error.code !== "ENOENT" && !(error instanceof SyntaxError)) throw error;
        }
        await new Promise((accept) => setTimeout(accept, 20));
    }
    throw new Error("synthetic parser readiness deadline");
}
async function blockingPython(root) {
    const fake = join(root, "synthetic-blocking-parser"),
        marker = join(root, "parser-marker.json");
    assert(!/[\r\n ]/.test(python));
    await writeFile(
        fake,
        `#!${python}\n# SYNTHETIC cancellation fixture; never returns proof data\nimport json,os,sys,time\n` +
            `open(${JSON.stringify(marker)},'w').write(json.dumps({'proof':sys.argv[-1],'pid':os.getpid()}))\ntime.sleep(60)\n`,
        { mode: 0o700 },
    );
    return { fake, marker };
}

test("portable raw bundle replays the real verifier after original removal and preserves its private identity", async (t) => {
    const f = await fixture(t);
    await mkdir(join(f.proof, "recovery"));
    await writeFile(join(f.proof, "recovery", "audit.json"), '{"fixture":true}\n');
    const original = await verifyRuntimeProvenance(f.args);
    const receipt = await bundleRuntimeProvenance(f.args),
        raw = await readFile(f.outputFile),
        content = JSON.parse(raw);
    assert.equal(receipt.sha256, bytesDigest(raw));
    assert.equal(receipt.bytes, raw.length);
    assert.equal(receipt.performanceAccepted, false);
    assert.ok(raw.equals(canonical(content)));
    const lock = await json(join(f.proof, "raw.lock.json"));
    assert.deepEqual(
        content.files.map(({ path }) => path).sort(),
        [...lock.files.map(({ path }) => path), "raw.lock.json"].sort(),
    );
    assert.ok(
        content.files.every(
            ({ path }) =>
                !path.startsWith("recovery/") &&
                !path.endsWith("runtime-binding.json") &&
                !path.endsWith("external-expectations.json"),
        ),
    );
    const moved = join(f.root, "moved-proof.json");
    await rename(f.outputFile, moved);
    await rm(f.proof, { recursive: true });
    const before = (await readdir(tmpdir())).filter((name) => name.startsWith("hgbench-runtime-bundle-"));
    const rebuilt = await verifyRuntimeProvenanceBundle({
        file: moved,
        python,
        expectedFileSha256: receipt.sha256,
        expectedProofDigest: f.expectedProofDigest,
    });
    assert.deepEqual(rebuilt, original);
    assert.equal(isVerifiedRuntimeProof(rebuilt), true);
    assert.equal(isVerifiedRuntimeProof(JSON.parse(JSON.stringify(rebuilt))), false);
    assert.equal(Object.isFrozen(rebuilt.runtimeBinding.driver), true);
    assert.equal(rebuilt.performanceAccepted, false);
    assert.deepEqual(
        (await readdir(tmpdir())).filter((name) => name.startsWith("hgbench-runtime-bundle-")),
        before,
    );
});

test("bundle creation refuses overwrite, absent explicit budgets, content drift, and proof-overlapping output", async (t) => {
    const f = await pack(t);
    await assert.rejects(bundleRuntimeProvenance(f.args), (error) => error.code === "EEXIST");
    assert.ok((await readFile(f.outputFile)).equals(f.raw));
    for (const maxBundleBytes of [undefined, 0, MiB - 1, 8 * MiB + 1, Infinity, 1.5])
        await assert.rejects(bundleRuntimeProvenance({ ...f.args, maxBundleBytes }), /MAX_BYTES/);
    await assert.rejects(
        bundleRuntimeProvenance({ ...f.args, outputFile: join(f.proof, "bundle.json") }),
        /OVERLAPS_PROOF/,
    );
    const link = join(f.root, "proof-alias");
    await symlink(f.proof, link);
    await assert.rejects(
        bundleRuntimeProvenance({ ...f.args, outputFile: join(link, "bundle.json") }),
        /OVERLAPS_PROOF/,
    );
    await assert.rejects(access(join(f.proof, "bundle.json")));
    const member = join(f.proof, "roles", "driver-python.json");
    await writeFile(member, "{}\n");
    await assert.rejects(
        bundleRuntimeProvenance({ ...f.args, outputFile: join(f.root, "drift.json") }),
        /RUNTIME_PROOF_CONTENT/,
    );
    await assert.rejects(access(join(f.root, "drift.json")));
});

test("strict canonical JSON, base64, member limits, paths and complete raw lock are checked before execution", async (t) => {
    const f = await pack(t);
    const cases = [
        [
            (b) => {
                b.extra = true;
            },
            /FIELDS/,
        ],
        [
            (b) => {
                b.files[0].extra = true;
            },
            /MEMBER_FIELDS/,
        ],
        [
            (b) => {
                b.files[0].base64 = b.files[0].base64.slice(0, -1) + "!";
            },
            /BASE64/,
        ],
        [
            (b) => {
                b.files[0].sha256 = "0".repeat(64);
            },
            /MEMBER_CONTENT/,
        ],
        [
            (b) => {
                b.files.shift();
            },
            /INCOMPLETE_RAW/,
        ],
        [
            (b) => {
                b.files = b.files.filter((r) => r.path !== "raw.lock.json");
            },
            /RAW_LOCK_MISSING/,
        ],
        [
            (b) => {
                b.files.splice(1, 0, { ...b.files[0] });
            },
            /MEMBER_ORDER/,
        ],
        [
            (b) => {
                b.files[0].bytes = 16 * MiB + 1;
            },
            /MEMBER_IDENTITY/,
        ],
        [
            (b) => {
                b.files[0].bytes = 9 * MiB;
                b.files[1].bytes = 9 * MiB;
            },
            /RAW_BUDGET/,
        ],
        [
            (b) => {
                b.files = Array.from({ length: 1001 }, () => b.files[0]);
            },
            /FILES/,
        ],
    ];
    for (const path of [
        "/tmp/escape",
        "../escape",
        "control/../escape",
        "control/./escape",
        "control//escape",
        "control\\escape",
        "control/中文",
        "a/b/c/d/e/f/g/h/i",
        "recovery/a.json",
        "runtime-binding.json",
        "external-expectations.json",
    ])
        cases.push([
            (b) => {
                b.files[0].path = path;
            },
            /PATH|EXCLUDED_MEMBER/,
        ]);
    for (const [mutation, pattern] of cases) {
        const args = await mutateBundle(f, mutation);
        await assert.rejects(
            verifyRuntimeProvenanceBundle({ ...args, python: join(f.root, "must-not-execute") }),
            pattern,
        );
    }
    for (const raw of [
        Buffer.from(JSON.stringify(f.bundle, null, 2) + "\n"),
        Buffer.from('{"schemaVersion":1,"schemaVersion":1}'),
    ]) {
        const args = await mutateBundle(f, () => {}, { raw });
        await assert.rejects(
            verifyRuntimeProvenanceBundle({ ...args, python: join(f.root, "must-not-execute") }),
            /NONCANONICAL|FIELDS/,
        );
    }
    await assert.rejects(
        verifyRuntimeProvenanceBundle({ ...f.verifyArgs, expectedFileSha256: "0".repeat(64) }),
        /FILE_DIGEST/,
    );
    await assert.rejects(
        verifyRuntimeProvenanceBundle({ ...f.verifyArgs, expectedProofDigest: "0".repeat(64) }),
        /IDENTITY/,
    );
    const huge = join(f.root, "oversized.json");
    await writeFile(huge, Buffer.alloc(8 * MiB + 1));
    await assert.rejects(verifyRuntimeProvenanceBundle({ ...f.verifyArgs, file: huge }), /REGULAR_FILE/);
    const link = join(f.root, "bundle-link.json");
    await symlink(f.outputFile, link);
    await assert.rejects(
        verifyRuntimeProvenanceBundle({ ...f.verifyArgs, file: link }),
        (error) => error.code === "ELOOP",
    );
});

test("independent parser failure and cancellation remove the exclusive expanded temporary directory", async (t) => {
    const f = await pack(t),
        { fake, marker } = await blockingPython(f.root);
    const controller = new AbortController();
    const pending = verifyRuntimeProvenanceBundle({ ...f.verifyArgs, python: fake, signal: controller.signal });
    const observed = await waitFor(marker);
    assert.match(observed.proof, /hgbench-runtime-bundle-/);
    assert.notEqual(observed.proof, f.proof);
    controller.abort();
    await assert.rejects(pending, /RUNTIME_PROOF_CANCELLED/);
    await assert.rejects(access(observed.proof));
    assert.throws(
        () => process.kill(observed.pid, 0),
        (error) => error.code === "ESRCH",
    );
    await assert.rejects(verifyRuntimeProvenanceBundle({ ...f.verifyArgs, signal: AbortSignal.abort() }), /CANCELLED/);
    await writeFile(fake, `#!${python}\nimport sys\nprint('invalid verifier protocol')\n`, { mode: 0o700 });
    const before = (await readdir(tmpdir())).filter((name) => name.startsWith("hgbench-runtime-bundle-"));
    await assert.rejects(verifyRuntimeProvenanceBundle({ ...f.verifyArgs, python: fake }), /RUNTIME_PROOF_PROTOCOL/);
    assert.deepEqual(
        (await readdir(tmpdir())).filter((name) => name.startsWith("hgbench-runtime-bundle-")),
        before,
    );
});

test("fully resealed foreign source stays non-executable and canonical base64 rejects alternate padding bits", async (t) => {
    const f = await pack(t),
        before = (await readdir(tmpdir())).filter((name) => name.startsWith("hgbench-runtime-bundle-"));
    const bundle = structuredClone(f.bundle),
        member = bundle.files.find((row) => row.path === "control/runtime-provenance.py");
    const marker = join(f.root, "archive-source-must-never-run");
    const poison = Buffer.from(`from pathlib import Path\nPath(${JSON.stringify(marker)}).write_text('bad')\n`);
    Object.assign(member, { bytes: poison.length, sha256: bytesDigest(poison), base64: poison.toString("base64") });
    const lockMember = bundle.files.find((row) => row.path === "raw.lock.json"),
        lock = JSON.parse(Buffer.from(lockMember.base64, "base64"));
    lock.files = bundle.files
        .filter((row) => row.path !== "raw.lock.json")
        .map(({ path, bytes, sha256 }) => ({ path, bytes, sha256 }));
    lock.contentDigest = valueDigest(lock.files);
    bundle.proofDigest = lock.contentDigest;
    const lockRaw = Buffer.from(JSON.stringify(lock) + "\n");
    Object.assign(lockMember, {
        bytes: lockRaw.length,
        sha256: bytesDigest(lockRaw),
        base64: lockRaw.toString("base64"),
    });
    const file = join(f.root, "resealed-foreign-source.json"),
        raw = canonical(bundle);
    await writeFile(file, raw);
    await assert.rejects(
        verifyRuntimeProvenanceBundle({
            file,
            python: join(f.root, "must-not-execute"),
            expectedFileSha256: bytesDigest(raw),
            expectedProofDigest: bundle.proofDigest,
        }),
        /RUNTIME_PROOF_IMPLEMENTATION/,
    );
    await assert.rejects(access(marker));
    assert.deepEqual(
        (await readdir(tmpdir())).filter((name) => name.startsWith("hgbench-runtime-bundle-")),
        before,
    );
    const encoded = structuredClone(f.bundle),
        padded = encoded.files.find((row) => row.bytes % 3 !== 0),
        alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    assert.ok(padded);
    const index = padded.base64.length - (padded.bytes % 3 === 1 ? 3 : 2),
        chars = [...padded.base64];
    chars[index] = alphabet[alphabet.indexOf(chars[index]) + 1];
    const noncanonical = chars.join("");
    assert.ok(Buffer.from(noncanonical, "base64").equals(Buffer.from(padded.base64, "base64")));
    padded.base64 = noncanonical;
    const args = await mutateBundle(f, (b) => {
        b.files = encoded.files;
    });
    await assert.rejects(verifyRuntimeProvenanceBundle(args), /MEMBER_CONTENT/);
});

test("failed or oversized original proofs remain unchanged and cannot be repackaged as accepted evidence", async (t) => {
    const f = await fixture(t),
        resultPath = join(f.proof, "result.json"),
        result = await json(resultPath);
    result.padding = "x".repeat(MiB);
    await writeFile(resultPath, JSON.stringify(result) + "\n");
    let expectedProofDigest = await reseal(f.proof);
    assert.equal((await verifyRuntimeProvenance({ ...f.args, expectedProofDigest })).valid, true);
    await assert.rejects(
        bundleRuntimeProvenance({ ...f.args, expectedProofDigest, maxBundleBytes: MiB }),
        /OUTPUT_BUDGET/,
    );
    await assert.rejects(access(f.outputFile));
    delete result.padding;
    result.status = "failed";
    result.failure = "synthetic-cleanup-failure";
    result.cleanup.containerRemoved = false;
    await writeFile(resultPath, JSON.stringify(result) + "\n");
    expectedProofDigest = await reseal(f.proof);
    const original = await readFile(resultPath);
    await assert.rejects(bundleRuntimeProvenance({ ...f.args, expectedProofDigest }), /RUNTIME_PROOF_NOT_VALID/);
    assert.ok((await readFile(resultPath)).equals(original));
    await assert.rejects(access(f.outputFile));
});

test("fixed CLI exposes effects, rejects invalid options and cleans expansion on real SIGTERM", async (t) => {
    const f = await pack(t);
    assert.match((await exec(process.execPath, [cli, "--help"])).stdout, /Copied proof scripts are never executed/);
    await assert.rejects(exec(process.execPath, [cli, "verify", "--arbitrary", "command"]), (error) => {
        assert.equal(error.code, 6);
        assert.equal(JSON.parse(error.stdout).error.code, "RUNTIME_BUNDLE_ARGUMENTS");
        return true;
    });
    const { fake, marker } = await blockingPython(f.root);
    const child = spawn(
        process.execPath,
        [
            cli,
            "verify",
            "--file",
            f.outputFile,
            "--python",
            fake,
            "--file-sha256",
            f.verifyArgs.expectedFileSha256,
            "--proof-digest",
            f.expectedProofDigest,
        ],
        { stdio: ["ignore", "pipe", "pipe"] },
    );
    t.after(() => {
        try {
            child.kill("SIGKILL");
        } catch {}
    });
    let stdout = "",
        stderr = "";
    child.stdout.on("data", (chunk) => {
        stdout += chunk;
    });
    child.stderr.on("data", (chunk) => {
        stderr += chunk;
    });
    const closed = new Promise((accept, reject) => {
        child.once("error", reject);
        child.once("close", (code, signal) => accept({ code, signal }));
    });
    const observed = await waitFor(marker);
    child.kill("SIGTERM");
    child.kill("SIGTERM");
    assert.deepEqual(await closed, { code: 143, signal: null });
    assert.equal(JSON.parse(stdout).ok, false);
    assert.equal(stderr, "");
    await assert.rejects(access(observed.proof));
    assert.throws(
        () => process.kill(observed.pid, 0),
        (error) => error.code === "ESRCH",
    );
});
