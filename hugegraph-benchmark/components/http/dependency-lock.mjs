/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import { constants } from "node:fs";
import { open, lstat, mkdir, writeFile } from "node:fs/promises";
import { createHash } from "node:crypto";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { isDeepStrictEqual } from "node:util";

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "../..");
const ASSET = "release/dependency-lock.json";
const LIMIT = 8 * 1024 * 1024;
const hash = (raw) => createHash("sha256").update(raw).digest("hex");
function need(value, message) {
    if (!value) throw Object.assign(new Error(message), { code: "RELEASE_DEPENDENCY_LOCK" });
}
async function bytes(path) {
    let handle;
    try {
        handle = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
        const before = await handle.stat();
        need(before.isFile() && before.size <= LIMIT, "Dependency-lock input must be a regular file <= 8 MiB");
        const raw = Buffer.alloc(before.size + 1);
        let length = 0;
        while (length < raw.length) {
            const result = await handle.read(raw, length, raw.length - length, length);
            if (!result.bytesRead) break;
            length += result.bytesRead;
        }
        const after = await handle.stat();
        need(
            length === before.size && before.size === after.size && before.mtimeMs === after.mtimeMs,
            "Dependency-lock input changed while reading",
        );
        return raw.subarray(0, length);
    } catch (error) {
        need(
            false,
            error.code === "RELEASE_DEPENDENCY_LOCK" ? error.message : "Dependency-lock asset is missing or unreadable",
        );
    } finally {
        await handle?.close();
    }
}
function parse(raw) {
    try {
        return JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(raw));
    } catch {
        need(false, "Invalid dependency-lock JSON");
    }
}
function declarations(pkg, lock) {
    need(
        lock?.lockfileVersion === 3 && lock.name === pkg.name && lock.version === pkg.version,
        "Dependency-lock package identity differs",
    );
    const root = lock.packages?.[""];
    need(root && typeof root === "object", "Dependency-lock root package is missing");
    for (const key of ["name", "version", "license", "bin", "engines", "dependencies", "devDependencies"])
        need(isDeepStrictEqual(root[key], pkg[key]), `Dependency-lock declaration differs: ${key}`);
}

// Runtime reads only the package's fixed release asset, never a consumer's npm lock.
export async function verifyDependencyLock(root = ROOT, { source = false } = {}) {
    const pkg = parse(await bytes(join(root, "package.json")));
    const metadata = pkg.hgbenchRelease?.dependencyLock;
    need(
        metadata &&
            isDeepStrictEqual(Object.keys(metadata).sort(), ["bytes", "path", "sha256"]) &&
            metadata.path === ASSET &&
            Number.isSafeInteger(metadata.bytes) &&
            metadata.bytes > 0 &&
            metadata.bytes <= LIMIT &&
            /^[a-f0-9]{64}$/.test(metadata.sha256),
        "Missing or invalid release dependency-lock binding",
    );
    const directory = await lstat(join(root, "release")).catch(() => null);
    need(directory?.isDirectory() && !directory.isSymbolicLink(), "Release directory must be a regular directory");
    const raw = await bytes(join(root, ASSET));
    need(raw.length === metadata.bytes && hash(raw) === metadata.sha256, "Release dependency-lock digest differs");
    declarations(pkg, parse(raw));
    if (source)
        need(
            raw.equals(await bytes(join(root, "package-lock.json"))),
            "Release dependency lock is stale; run npm run build to sync the original lock bytes",
        );
    return { path: ASSET, bytes: raw.length, sha256: hash(raw) };
}

export async function syncDependencyLock(root = ROOT) {
    const raw = await bytes(join(root, "package-lock.json"));
    const pkg = parse(await bytes(join(root, "package.json")));
    declarations(pkg, parse(raw));
    await mkdir(join(root, "release"), { recursive: true });
    const directory = await lstat(join(root, "release"));
    need(directory.isDirectory() && !directory.isSymbolicLink(), "Release directory must be a regular directory");
    const asset = await open(
        join(root, ASSET),
        constants.O_WRONLY | constants.O_CREAT | constants.O_TRUNC | constants.O_NOFOLLOW,
        0o644,
    );
    try {
        await asset.writeFile(raw);
    } finally {
        await asset.close();
    }
    pkg.hgbenchRelease = { dependencyLock: { path: ASSET, bytes: raw.length, sha256: hash(raw) } };
    await writeFile(join(root, "package.json"), JSON.stringify(pkg, null, 4) + "\n");
    return verifyDependencyLock(root, { source: true });
}

if (process.argv[1] && pathToFileURL(resolve(process.argv[1])).href === import.meta.url) {
    const command = process.argv[2];
    if (["--help", "-h"].includes(command)) {
        process.stdout.write(
            "Release dependency lock: sync | verify\nBuild syncs package-lock.json bytes to release/dependency-lock.json; prepack verifies both.\nRuntime consumers verify only the fixed release asset and its package.json binding.\n",
        );
    } else {
        try {
            const quiet = command === "verify" && process.argv[3] === "--quiet";
            need(
                (process.argv.length === 3 || (quiet && process.argv.length === 4)) &&
                    ["sync", "verify"].includes(command),
                "Expected sync or verify [--quiet]",
            );
            const result =
                command === "sync" ? await syncDependencyLock() : await verifyDependencyLock(ROOT, { source: true });
            if (!quiet) process.stdout.write(JSON.stringify(result) + "\n");
        } catch (error) {
            process.stderr.write(
                JSON.stringify({ code: error.code ?? "RELEASE_DEPENDENCY_LOCK", message: error.message }) + "\n",
            );
            process.exitCode = 1;
        }
    }
}
