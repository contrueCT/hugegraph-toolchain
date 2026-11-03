/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
import { constants } from "node:fs";
import { open, lstat, realpath, mkdtemp, mkdir, rm } from "node:fs/promises";
import { basename, dirname, join, resolve } from "node:path";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import { isDeepStrictEqual } from "node:util";
import { bytesDigest, valueDigest } from "./common.mjs";
import { verifyRuntimeProvenance } from "./runtime-provenance-evidence.mjs";

const MiB = 1024 * 1024,
    MAX_BUNDLE = 8 * MiB,
    MAX_RAW = 16 * MiB,
    SHA = /^[a-f0-9]{64}$/,
    KIND = "http-runtime-provenance-bundle";

export class RuntimeProvenanceBundleError extends Error {
    constructor(code) {
        super(code);
        this.code = code;
    }
}
function need(value, code) {
    if (!value) throw new RuntimeProvenanceBundleError(code);
}
function exact(value, fields, code) {
    need(
        value !== null &&
            typeof value === "object" &&
            !Array.isArray(value) &&
            isDeepStrictEqual(Object.keys(value).sort(), [...fields].sort()),
        code,
    );
}
function check(signal) {
    need(!signal?.aborted, "RUNTIME_BUNDLE_CANCELLED");
}
function canonical(value) {
    const sorted = (v) =>
        Array.isArray(v)
            ? v.map(sorted)
            : v !== null && typeof v === "object"
              ? Object.fromEntries(
                    Object.keys(v)
                        .sort()
                        .map((key) => [key, sorted(v[key])]),
                )
              : v;
    return Buffer.from(JSON.stringify(sorted(value)) + "\n");
}
function parse(raw) {
    try {
        return JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(raw));
    } catch {
        throw new RuntimeProvenanceBundleError("RUNTIME_BUNDLE_JSON");
    }
}
function safePath(path) {
    need(typeof path === "string" && path.length > 0 && path.length <= 1024, "RUNTIME_BUNDLE_PATH");
    const parts = path.split("/");
    need(
        parts.length <= 8 && parts.every((part) => /^[A-Za-z0-9._-]+$/.test(part) && part !== "." && part !== ".."),
        "RUNTIME_BUNDLE_PATH",
    );
    need(
        !["runtime-binding.json", "external-expectations.json"].includes(path) && !path.startsWith("recovery/"),
        "RUNTIME_BUNDLE_EXCLUDED_MEMBER",
    );
    return parts;
}
function members(rows, encoded) {
    need(Array.isArray(rows) && rows.length > 0 && rows.length <= 1000, "RUNTIME_BUNDLE_FILES");
    const entries = new Set(),
        files = new Set();
    let total = 0,
        previous;
    for (const row of rows) {
        exact(
            row,
            encoded ? ["path", "bytes", "sha256", "base64"] : ["path", "bytes", "sha256"],
            "RUNTIME_BUNDLE_MEMBER_FIELDS",
        );
        const parts = safePath(row.path);
        need(previous === undefined || previous < row.path, "RUNTIME_BUNDLE_MEMBER_ORDER");
        previous = row.path;
        need(!files.has(row.path), "RUNTIME_BUNDLE_DUPLICATE_PATH");
        files.add(row.path);
        need(
            Number.isSafeInteger(row.bytes) &&
                row.bytes >= 0 &&
                row.bytes <= MAX_RAW &&
                typeof row.sha256 === "string" &&
                SHA.test(row.sha256),
            "RUNTIME_BUNDLE_MEMBER_IDENTITY",
        );
        if (encoded) need(typeof row.base64 === "string", "RUNTIME_BUNDLE_BASE64");
        for (let i = 1; i <= parts.length; i++) entries.add(parts.slice(0, i).join("/"));
        total += row.bytes;
        need(entries.size <= 1000 && total <= MAX_RAW, "RUNTIME_BUNDLE_RAW_BUDGET");
    }
    for (const row of rows) {
        const parts = row.path.split("/");
        need(
            parts.slice(0, -1).every((_, i) => !files.has(parts.slice(0, i + 1).join("/"))),
            "RUNTIME_BUNDLE_PATH_COLLISION",
        );
    }
    return total;
}
function lockContent(raw, expectedProofDigest) {
    const lock = parse(raw);
    exact(lock, ["schemaVersion", "kind", "files", "contentDigest"], "RUNTIME_BUNDLE_RAW_LOCK_FIELDS");
    members(lock.files, false);
    need(
        lock.schemaVersion === 1 &&
            lock.kind === "http-runtime-provenance-raw-lock" &&
            lock.contentDigest === expectedProofDigest &&
            valueDigest(lock.files) === expectedProofDigest,
        "RUNTIME_BUNDLE_RAW_LOCK",
    );
    need(!lock.files.some((row) => row.path === "raw.lock.json"), "RUNTIME_BUNDLE_SELF_LOCK");
    return lock;
}
async function regularBytes(path, cap, signal) {
    check(signal);
    const handle = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
    try {
        const before = await handle.stat({ bigint: true });
        need(before.isFile() && before.size <= BigInt(cap), "RUNTIME_BUNDLE_REGULAR_FILE");
        const chunks = [];
        let count = 0;
        while (count <= cap) {
            check(signal);
            const buffer = Buffer.allocUnsafe(Math.min(MiB, cap + 1 - count));
            const { bytesRead } = await handle.read(buffer, 0, buffer.length, null);
            if (!bytesRead) break;
            chunks.push(buffer.subarray(0, bytesRead));
            count += bytesRead;
        }
        need(count <= cap, "RUNTIME_BUNDLE_FILE_BUDGET");
        const after = await handle.stat({ bigint: true });
        need(
            BigInt(count) === before.size &&
                ["dev", "ino", "size", "mtimeNs", "ctimeNs"].every((key) => before[key] === after[key]),
            "RUNTIME_BUNDLE_FILE_CHANGED",
        );
        check(signal);
        return Buffer.concat(chunks);
    } finally {
        await handle.close();
    }
}
async function withinRoot(root, path) {
    const parts = safePath(path);
    let current = root;
    for (const part of parts.slice(0, -1)) {
        current = join(current, part);
        const info = await lstat(current);
        need(info.isDirectory() && !info.isSymbolicLink(), "RUNTIME_BUNDLE_DIRECTORY");
    }
    return join(root, path);
}
function validateBundle(raw, expectedProofDigest) {
    const bundle = parse(raw);
    exact(bundle, ["schemaVersion", "kind", "proofDigest", "files"], "RUNTIME_BUNDLE_FIELDS");
    need(
        bundle.schemaVersion === 1 && bundle.kind === KIND && bundle.proofDigest === expectedProofDigest,
        "RUNTIME_BUNDLE_IDENTITY",
    );
    const rawBytes = members(bundle.files, true);
    need(canonical(bundle).equals(raw), "RUNTIME_BUNDLE_NONCANONICAL_JSON");
    const decoded = bundle.files.map((row) => {
        need(
            typeof row.base64 === "string" &&
                row.base64.length === 4 * Math.ceil(row.bytes / 3) &&
                /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(row.base64),
            "RUNTIME_BUNDLE_BASE64",
        );
        const bytes = Buffer.from(row.base64, "base64");
        need(
            bytes.length === row.bytes && bytes.toString("base64") === row.base64 && bytesDigest(bytes) === row.sha256,
            "RUNTIME_BUNDLE_MEMBER_CONTENT",
        );
        return { ...row, raw: bytes };
    });
    const own = decoded.find((row) => row.path === "raw.lock.json");
    need(own !== undefined && own.bytes <= MiB, "RUNTIME_BUNDLE_RAW_LOCK_MISSING");
    const lock = lockContent(own.raw, expectedProofDigest);
    need(
        isDeepStrictEqual(
            lock.files,
            decoded
                .filter((row) => row.path !== "raw.lock.json")
                .map(({ path, bytes, sha256 }) => ({ path, bytes, sha256 })),
        ),
        "RUNTIME_BUNDLE_INCOMPLETE_RAW",
    );
    return { bundle, decoded, rawBytes };
}

/** One portable raw proof. No derived JSON, recovery data or collection replay. */
export async function bundleRuntimeProvenance({
    proofDirectory,
    python,
    expectedProofDigest,
    outputFile,
    maxBundleBytes,
    signal,
}) {
    need(
        Number.isSafeInteger(maxBundleBytes) && maxBundleBytes >= MiB && maxBundleBytes <= MAX_BUNDLE,
        "RUNTIME_BUNDLE_MAX_BYTES",
    );
    need(typeof expectedProofDigest === "string" && SHA.test(expectedProofDigest), "RUNTIME_BUNDLE_EXPECTED_PROOF");
    need(typeof outputFile === "string" && outputFile.length > 0, "RUNTIME_BUNDLE_OUTPUT");
    check(signal);
    const result = await verifyRuntimeProvenance({ proofDirectory, python, expectedProofDigest, signal });
    const root = await realpath(proofDirectory),
        lockRaw = await regularBytes(join(root, "raw.lock.json"), MiB, signal);
    need(bytesDigest(lockRaw) === result.rawLockSha256, "RUNTIME_BUNDLE_LOCK_CHANGED");
    const lock = lockContent(lockRaw, expectedProofDigest),
        files = [];
    const own = { path: "raw.lock.json", bytes: lockRaw.length, sha256: bytesDigest(lockRaw) };
    const projected = [...lock.files, own].map((row) => ({ ...row, base64: "" }));
    projected.sort((a, b) => (a.path < b.path ? -1 : a.path > b.path ? 1 : 0));
    members(projected, true);
    need(
        canonical({ schemaVersion: 1, kind: KIND, proofDigest: expectedProofDigest, files: projected }).length +
            projected.reduce((sum, row) => sum + 4 * Math.ceil(row.bytes / 3), 0) <=
            maxBundleBytes,
        "RUNTIME_BUNDLE_OUTPUT_BUDGET",
    );
    for (const row of lock.files) {
        const raw = await regularBytes(await withinRoot(root, row.path), MAX_RAW, signal);
        need(raw.length === row.bytes && bytesDigest(raw) === row.sha256, "RUNTIME_BUNDLE_RAW_CHANGED");
        files.push({ ...row, base64: raw.toString("base64") });
    }
    files.push({ ...own, base64: lockRaw.toString("base64") });
    files.sort((a, b) => (a.path < b.path ? -1 : a.path > b.path ? 1 : 0));
    const raw = canonical({ schemaVersion: 1, kind: KIND, proofDigest: expectedProofDigest, files });
    need(raw.length <= maxBundleBytes, "RUNTIME_BUNDLE_OUTPUT_BUDGET");
    const { rawBytes } = validateBundle(raw, expectedProofDigest);
    const requested = resolve(outputFile),
        path = join(await realpath(dirname(requested)), basename(requested));
    need(path !== root && !path.startsWith(root + "/"), "RUNTIME_BUNDLE_OUTPUT_OVERLAPS_PROOF");
    check(signal);
    const handle = await open(
        path,
        constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW,
        0o600,
    );
    let complete = false;
    try {
        await handle.writeFile(raw);
        complete = true;
    } finally {
        await handle.close();
        if (!complete) await rm(path, { force: true });
    }
    return {
        schemaVersion: 1,
        kind: "http-runtime-provenance-bundle-receipt",
        file: path,
        bytes: raw.length,
        sha256: bytesDigest(raw),
        proofDigest: expectedProofDigest,
        files: files.length,
        rawBytes,
        performanceAccepted: false,
    };
}

/** Expand only validated raw bytes; execute the local known verifier and return its original branded result. */
export async function verifyRuntimeProvenanceBundle({ file, python, expectedFileSha256, expectedProofDigest, signal }) {
    need(typeof expectedFileSha256 === "string" && SHA.test(expectedFileSha256), "RUNTIME_BUNDLE_EXPECTED_FILE");
    need(typeof expectedProofDigest === "string" && SHA.test(expectedProofDigest), "RUNTIME_BUNDLE_EXPECTED_PROOF");
    check(signal);
    const raw = await regularBytes(file, MAX_BUNDLE, signal);
    need(bytesDigest(raw) === expectedFileSha256, "RUNTIME_BUNDLE_FILE_DIGEST");
    const { decoded } = validateBundle(raw, expectedProofDigest);
    check(signal);
    const root = await mkdtemp(join(tmpdir(), "hgbench-runtime-bundle-"));
    try {
        for (const row of decoded) {
            check(signal);
            const path = join(root, row.path);
            await mkdir(dirname(path), { recursive: true, mode: 0o700 });
            const handle = await open(
                path,
                constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW,
                0o600,
            );
            try {
                await handle.writeFile(row.raw);
            } finally {
                await handle.close();
            }
        }
        const result = await verifyRuntimeProvenance({ proofDirectory: root, python, expectedProofDigest, signal });
        check(signal);
        return result;
    } finally {
        await rm(root, { recursive: true, force: true });
    }
}

const HELP = `Usage:
  node runtime-provenance-bundle.mjs bundle --proof DIR --python ABS --proof-digest SHA256 --out FILE --max-bundle-bytes N
  node runtime-provenance-bundle.mjs verify --file FILE --python ABS --file-sha256 SHA256 --proof-digest SHA256

bundle verifies the existing original proof and creates one new canonical JSON (explicit 1..8 MiB budget).
verify is offline: checks original file/content locks, expands to a new private temporary directory,
runs only the local known verifier, then removes that directory on success, failure or cancellation.
No Docker probe or target is started. Copied proof scripts are never executed.
Both commands emit one JSON envelope; failures exit 6, SIGINT 130 and SIGTERM 143.
The proof remains an expectation source for future experiments; performance acceptance remains false.
`;
async function main() {
    if (process.argv.includes("--help")) {
        process.stdout.write(HELP);
        return;
    }
    const [action, ...argv] = process.argv.slice(2),
        args = {};
    const names =
        action === "bundle"
            ? ["--proof", "--python", "--proof-digest", "--out", "--max-bundle-bytes"]
            : action === "verify"
              ? ["--file", "--python", "--file-sha256", "--proof-digest"]
              : [];
    const controller = new AbortController();
    let cancelled = 0;
    const interrupt = () => {
            cancelled ||= 130;
            controller.abort();
        },
        terminate = () => {
            cancelled ||= 143;
            controller.abort();
        };
    process.on("SIGINT", interrupt);
    process.on("SIGTERM", terminate);
    try {
        need(names.length && argv.length === 2 * names.length, "RUNTIME_BUNDLE_ARGUMENTS");
        for (let i = 0; i < argv.length; i += 2) {
            need(names.includes(argv[i]) && !Object.hasOwn(args, argv[i]) && argv[i + 1], "RUNTIME_BUNDLE_ARGUMENTS");
            args[argv[i]] = argv[i + 1];
        }
        const data =
            action === "bundle"
                ? await bundleRuntimeProvenance({
                      proofDirectory: args["--proof"],
                      python: args["--python"],
                      expectedProofDigest: args["--proof-digest"],
                      outputFile: args["--out"],
                      maxBundleBytes: Number(args["--max-bundle-bytes"]),
                      signal: controller.signal,
                  })
                : await verifyRuntimeProvenanceBundle({
                      file: args["--file"],
                      python: args["--python"],
                      expectedFileSha256: args["--file-sha256"],
                      expectedProofDigest: args["--proof-digest"],
                      signal: controller.signal,
                  });
        check(controller.signal);
        process.stdout.write(JSON.stringify({ ok: true, data }) + "\n");
    } catch (error) {
        process.stdout.write(
            JSON.stringify({
                ok: false,
                error: { code: error.code ?? "RUNTIME_BUNDLE_FAILED", performanceAccepted: false },
            }) + "\n",
        );
        process.exitCode = cancelled || 6;
    } finally {
        process.removeListener("SIGINT", interrupt);
        process.removeListener("SIGTERM", terminate);
    }
}
if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) await main();
