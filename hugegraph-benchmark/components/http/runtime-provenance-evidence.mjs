/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
import { spawn } from "node:child_process";
import { open, lstat, readdir, realpath } from "node:fs/promises";
import { constants } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { bytesDigest, valueDigest } from "./common.mjs";
import { isDeepStrictEqual } from "node:util";

const HERE = dirname(fileURLToPath(import.meta.url));
const SHA = /^[a-f0-9]{64}$/;
const SOURCES = [
    "runtime-provenance.py",
    "runtime-provenance-worker.py",
    "runtime-provenance-node.mjs",
    "comparison-workflow.py",
];
const MAX_BYTES = 16 * 1024 * 1024;
const verified = new WeakSet();

export class RuntimeProvenanceEvidenceError extends Error {
    constructor(code) {
        super(code);
        this.code = code;
    }
}
function need(value, code) {
    if (!value) throw new RuntimeProvenanceEvidenceError(code);
}
function freeze(value) {
    if (value && typeof value === "object") {
        for (const child of Object.values(value)) freeze(child);
        Object.freeze(value);
    }
    return value;
}
export function isVerifiedRuntimeProof(value) {
    return value !== null && typeof value === "object" && verified.has(value);
}
async function regularBytes(path, cap) {
    const handle = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
    try {
        const before = await handle.stat({ bigint: true });
        need(before.isFile() && before.size <= BigInt(cap), "RUNTIME_PROOF_FILE");
        const chunks = [];
        let count = 0;
        while (count <= cap) {
            const buffer = Buffer.allocUnsafe(Math.min(1024 * 1024, cap + 1 - count));
            const { bytesRead } = await handle.read(buffer, 0, buffer.length, null);
            if (!bytesRead) break;
            chunks.push(buffer.subarray(0, bytesRead));
            count += bytesRead;
        }
        need(count <= cap, "RUNTIME_PROOF_BYTES");
        const after = await handle.stat({ bigint: true });
        need(
            BigInt(count) === before.size &&
                ["dev", "ino", "size", "mtimeNs", "ctimeNs"].every((field) => before[field] === after[field]),
            "RUNTIME_PROOF_FILE_CHANGED",
        );
        return Buffer.concat(chunks);
    } finally {
        await handle.close();
    }
}
async function inventory(root) {
    const rows = [];
    let total = 0,
        entries = 0;
    async function walk(relative = "", depth = 0) {
        need(depth <= 8, "RUNTIME_PROOF_DEPTH");
        for (const name of (await readdir(join(root, relative))).sort()) {
            need(++entries <= 1000 && /^[A-Za-z0-9._-]+$/.test(name), "RUNTIME_PROOF_MEMBER");
            const path = relative ? `${relative}/${name}` : name;
            const info = await lstat(join(root, path));
            need(!info.isSymbolicLink(), "RUNTIME_PROOF_SYMLINK");
            if (info.isDirectory()) await walk(path, depth + 1);
            else {
                need(info.isFile(), "RUNTIME_PROOF_FILE_TYPE");
                total += info.size;
                need(total <= MAX_BYTES, "RUNTIME_PROOF_BYTES");
                if (
                    ["raw.lock.json", "runtime-binding.json", "external-expectations.json"].includes(path) ||
                    path.startsWith("recovery/")
                )
                    continue;
                const raw = await regularBytes(join(root, path), MAX_BYTES);
                rows.push({ path, bytes: raw.length, sha256: bytesDigest(raw) });
            }
        }
    }
    await walk();
    return rows.sort((a, b) => (a.path < b.path ? -1 : a.path > b.path ? 1 : 0));
}
async function invoke(python, root, signal) {
    return new Promise((accept, reject) => {
        const grouped = process.platform !== "win32";
        const child = spawn(python, ["-B", join(HERE, "runtime-provenance.py"), "verify", "--proof", root], {
            detached: grouped,
            stdio: ["ignore", "pipe", "pipe"],
        });
        const stdout = [],
            stderr = [];
        let size = 0,
            failure,
            force;
        function kill(name) {
            if (!child.pid) return;
            try {
                if (grouped) process.kill(-child.pid, name);
                else child.kill(name);
            } catch (error) {
                if (error.code !== "ESRCH") failure ??= new RuntimeProvenanceEvidenceError("RUNTIME_PROOF_CLEANUP");
            }
        }
        function stop(code) {
            if (failure) return;
            failure = new RuntimeProvenanceEvidenceError(code);
            kill("SIGTERM");
            force = setTimeout(() => kill("SIGKILL"), 5000);
        }
        const abort = () => stop("RUNTIME_PROOF_CANCELLED");
        const interrupt = () => stop("RUNTIME_PROOF_SIGINT");
        const terminate = () => stop("RUNTIME_PROOF_SIGTERM");
        const timer = setTimeout(() => stop("RUNTIME_PROOF_TIMEOUT"), 30000);
        process.on("SIGINT", interrupt);
        process.on("SIGTERM", terminate);
        signal?.addEventListener("abort", abort, { once: true });
        if (signal?.aborted) abort();
        for (const [stream, values] of [
            [child.stdout, stdout],
            [child.stderr, stderr],
        ])
            stream.on("data", (chunk) => {
                const remaining = Math.max(0, 1024 * 1024 - size);
                if (remaining) values.push(chunk.subarray(0, remaining));
                size += chunk.length;
                if (size > 1024 * 1024) stop("RUNTIME_PROOF_OUTPUT");
            });
        child.once("error", () => {
            failure ??= new RuntimeProvenanceEvidenceError("RUNTIME_PROOF_PYTHON_UNAVAILABLE");
        });
        child.once("close", (code) => {
            clearTimeout(timer);
            clearTimeout(force);
            signal?.removeEventListener("abort", abort);
            process.removeListener("SIGINT", interrupt);
            process.removeListener("SIGTERM", terminate);
            kill("SIGKILL");
            if (failure) return reject(failure);
            if (code !== 0) return reject(new RuntimeProvenanceEvidenceError("RUNTIME_PROOF_VERIFIER_FAILED"));
            let reply;
            try {
                reply = JSON.parse(Buffer.concat(stdout).toString("utf8"));
            } catch {
                return reject(new RuntimeProvenanceEvidenceError("RUNTIME_PROOF_PROTOCOL"));
            }
            if (reply?.ok !== true || !reply.data || typeof reply.data !== "object" || Array.isArray(reply.data))
                return reject(new RuntimeProvenanceEvidenceError("RUNTIME_PROOF_PROTOCOL"));
            accept(reply.data);
        });
    });
}

/** Independently replay a sealed proof once, outside all measured phases.
 * The private identity marker prevents accidentally accepting a caller's
 * serialized `verification:true`; it is not cryptographic host attestation.
 */
export async function verifyRuntimeProvenance({
    proofDirectory,
    python,
    expectedProofDigest,
    expectedSourceHashes,
    signal,
}) {
    need(typeof python === "string" && python.startsWith("/") && !/[\x00-\x1f]/.test(python), "RUNTIME_PROOF_PYTHON");
    need(SHA.test(expectedProofDigest), "RUNTIME_PROOF_EXPECTED_DIGEST");
    need(!signal?.aborted, "RUNTIME_PROOF_CANCELLED");
    if (expectedSourceHashes !== undefined)
        need(
            expectedSourceHashes &&
                typeof expectedSourceHashes === "object" &&
                isDeepStrictEqual(Object.keys(expectedSourceHashes).sort(), [...SOURCES].sort()) &&
                Object.values(expectedSourceHashes).every((v) => typeof v === "string" && SHA.test(v)),
            "RUNTIME_PROOF_SOURCE_EXPECTATIONS",
        );
    const root = await realpath(proofDirectory);
    need((await lstat(root)).isDirectory(), "RUNTIME_PROOF_DIRECTORY");
    const lockRaw = await regularBytes(join(root, "raw.lock.json"), 1024 * 1024);
    const lock = JSON.parse(lockRaw);
    need(
        lock.schemaVersion === 1 &&
            lock.kind === "http-runtime-provenance-raw-lock" &&
            Array.isArray(lock.files) &&
            lock.contentDigest === expectedProofDigest &&
            valueDigest(lock.files) === expectedProofDigest,
        "RUNTIME_PROOF_LOCK",
    );
    const actual = await inventory(root);
    need(isDeepStrictEqual(actual, lock.files), "RUNTIME_PROOF_CONTENT");
    for (const name of SOURCES) {
        const localSha = bytesDigest(await regularBytes(join(HERE, name), 1024 * 1024));
        const original = actual.find((file) => file.path === `control/${name}`);
        need(
            original?.sha256 === localSha &&
                (expectedSourceHashes === undefined || expectedSourceHashes[name] === localSha),
            "RUNTIME_PROOF_IMPLEMENTATION",
        );
    }
    const result = await invoke(python, root, signal);
    need(
        result.kind === "http-runtime-provenance-verification" &&
            result.valid === true &&
            result.expectationsAvailable === true &&
            result.proofDigest === expectedProofDigest &&
            result.futureExperimentsOnly === true &&
            result.performanceAccepted === false &&
            result.clockAlignment === "unverified" &&
            result.runtimeBinding?.proofDigest === expectedProofDigest &&
            result.externalExpectations?.proofDigest === expectedProofDigest,
        "RUNTIME_PROOF_NOT_VALID",
    );
    need(
        bytesDigest(await regularBytes(join(root, "raw.lock.json"), 1024 * 1024)) === bytesDigest(lockRaw) &&
            isDeepStrictEqual(await inventory(root), actual),
        "RUNTIME_PROOF_CHANGED_DURING_REPLAY",
    );
    const value = freeze({ ...result, rawLockSha256: bytesDigest(lockRaw) });
    verified.add(value);
    return value;
}
