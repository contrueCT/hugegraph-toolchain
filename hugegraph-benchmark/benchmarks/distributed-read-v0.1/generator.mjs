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

import { createHash } from "node:crypto";
import { open, mkdir, readFile } from "node:fs/promises";
import { join, dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

export const directory = dirname(fileURLToPath(import.meta.url));
export const suiteId = "hgbench-distributed-read-v0.1-draft";
export const scales = Object.freeze({ dev: 8192, s1: 1048576, s2: 16777216, s3: 134217728 });
export const cases = Array.from({ length: 12 }, (_, i) => `DR${String(i + 1).padStart(2, "0")}`);
export const seed = 3346;
const json = (x) => JSON.stringify(x) + "\n";
const sha = (bytes) => createHash("sha256").update(bytes).digest("hex");

export function size(scale) {
    if (!Object.hasOwn(scales, scale)) throw new Error(`Unknown scale: ${scale}`);
    return scales[scale];
}

function mix(n) {
    n = Math.imul(n ^ seed, 0x45d9f3b);
    n = Math.imul(n ^ (n >>> 16), 0x45d9f3b);
    return (n ^ (n >>> 16)) >>> 0;
}

export function vertex(id, n) {
    if (!Number.isSafeInteger(id) || id < 1 || id > n) return null;
    // Per-vertex deterministic payload avoids making the entire property column one compressible constant.
    const payload = Array.from({ length: 4 }, (_, block) =>
        createHash("sha256").update(`hgbench:${seed}:${id}:${block}`).digest("hex"),
    ).join("");
    return { id, label: "node", properties: { group: `bucket-${(id - 1) % 4096}`, rank: id, payload } };
}

export function neighbors(id, n) {
    if (!Number.isSafeInteger(id) || id < 1 || id > n) throw new Error("Invalid source ID");
    const index = id - 1;
    const degree = index % 1024 === 0 ? 1024 : 8;
    const targets = new Set([((index + 1) % n) + 1]); // A directed ring connects the entire graph.
    const hub = (mix(index) % (n / 1024)) * 1024 + 1;
    if (hub !== id) targets.add(hub); // Skewed incoming traffic toward the sparse hub set.
    for (let j = 0; targets.size < degree; j++) {
        const target = (((mix(index + 1) + Math.imul(j, 2654435761)) >>> 0) % n) + 1;
        if (target !== id) targets.add(target);
    }
    return [...targets];
}

export function facts(scale) {
    const n = size(scale);
    return {
        vertices: n,
        edges: n * 8 + (n / 1024) * 1016,
        hubs: n / 1024,
        ordinaryOutDegree: 8,
        hubOutDegree: 1024,
        logicalPayloadBytes: n * 256,
        stronglyConnected: true,
        physicalPartitionsAssumed: false,
    };
}

// An odd affine permutation visits every ID in a power-of-two domain before repeating.
function permute(i, domain, salt) {
    return ((Math.imul(i, 1664525) + seed + salt) >>> 0) % domain;
}

export function parameterAt(caseId, ordinal, scale) {
    const n = size(scale);
    if (!cases.includes(caseId) || !Number.isSafeInteger(ordinal) || ordinal < 0 || ordinal >= 2 ** 32)
        throw new Error("Unknown case or invalid ordinal");
    const id = permute(ordinal, n, 17) + 1;
    const hub = permute(ordinal, n / 1024, 31) * 1024 + 1;
    // Enumerate 1023 ordinary vertices per 1024-ID block without mapping several ordinals to one ID.
    let ordinaryOrdinal = ordinal % (n - n / 1024);
    do {
        ordinaryOrdinal = permute(ordinaryOrdinal, n, 53);
    } while (ordinaryOrdinal >= n - n / 1024);
    const ordinary = Math.floor(ordinaryOrdinal / 1023) * 1024 + (ordinaryOrdinal % 1023) + 2;
    const group = `bucket-${permute(ordinal, 4096, 71)}`;
    const table = {
        DR01: ["point", { id }, "exact-value"],
        DR02: ["point", { id: n + id }, "exact-value"],
        DR03: ["filter", { group }, "exact-set"],
        DR04: ["neighbors", { source: ordinary }, "exact-set"],
        DR05: ["neighbors", { source: hub }, "exact-set"],
        DR06: ["kout", { source: ordinary, depth: 2, nearest: true, limit: null }, "exact-set"],
        DR07: ["kout", { source: hub, depth: 2, nearest: true, limit: null }, "exact-set"],
        DR08: ["kout", { source: hub, depth: 2, nearest: true, limit: 64 }, "bounded-set"],
        DR09: ["paged-neighbors", { source: hub, pageSize: 128 }, "paged-set"],
        DR10: ["common", { sources: [ordinary, ordinary === n ? 1 : ordinary + 1] }, "exact-set"],
        DR11: ["shortest", { source: ordinary, target: ((ordinary + 1) % n) + 1, maxDepth: 4 }, "shortest-path"],
        DR12: ["materialize", { group }, "vertex-set"],
    };
    const [operation, args, policy] = table[caseId];
    return { parameterId: `${scale}-${caseId}-${ordinal}`, caseId, ordinal, datasetId: scale, operation, args, policy };
}

export function expected(parameter, scale) {
    const n = size(scale);
    const { operation, args } = parameter;
    if (operation === "point") return vertex(args.id, n);
    if (operation === "neighbors" || operation === "paged-neighbors") return neighbors(args.source, n);
    if (operation === "filter" || operation === "materialize") {
        const bucket = Number(args.group.slice(7));
        const ids = Array.from({ length: n / 4096 }, (_, i) => bucket + 1 + i * 4096);
        return operation === "filter" ? ids : ids.map((id) => vertex(id, n));
    }
    if (operation === "common") {
        const other = new Set(neighbors(args.sources[1], n));
        return neighbors(args.sources[0], n).filter((id) => other.has(id));
    }
    if (operation === "kout") {
        let frontier = [args.source];
        const visited = new Set();
        for (let i = 0; i < args.depth; i++) {
            if (args.nearest) frontier.forEach((id) => visited.add(id));
            frontier = [...new Set(frontier.flatMap((id) => neighbors(id, n)))].filter(
                (id) => id !== args.source && !visited.has(id),
            );
        }
        return args.limit === null ? frontier : { allowed: frontier, count: Math.min(args.limit, frontier.length) };
    }
    if (operation === "shortest") {
        // Every generated target has a two-edge ring path. Determine whether a shorter direct edge exists.
        return { distance: neighbors(args.source, n).includes(args.target) ? 1 : 2 };
    }
    throw new Error("Unsupported task");
}

export async function contractDigest() {
    const files = [
        "spec.md",
        "performance-profile.json",
        "environment.schema.json",
        "generator.mjs",
        "../../tools/verify-distributed.py",
        "../core-read-v0.1/spec.md",
        "../core-read-v0.1/reference.mjs",
    ];
    const hashes = {};
    for (const file of files) hashes[file] = sha(await readFile(join(directory, file)));
    return { files: hashes, digest: sha(JSON.stringify(hashes)) };
}

// Flush graph rows around 1 MiB. A single validation record can be larger, proportional to its answer.
async function writeLines(path, records) {
    const file = await open(path, "wx");
    const hash = createHash("sha256");
    let buffer = "";
    let bytes = 0;
    let count = 0;
    async function flush() {
        const chunk = Buffer.from(buffer);
        hash.update(chunk);
        bytes += chunk.length;
        await file.writeFile(chunk);
        buffer = "";
    }
    try {
        for (const record of records) {
            buffer += json(record);
            count++;
            if (buffer.length >= 1024 * 1024) await flush();
        }
        if (buffer) await flush();
    } finally {
        await file.close();
    }
    return { records: count, bytes, sha256: hash.digest("hex") };
}

export async function generateTo(scale, output) {
    const n = size(scale);
    await mkdir(output); // Refuse existing directories, including partial previous generations.
    const files = {};
    files["vertices.ndjson"] = await writeLines(
        join(output, "vertices.ndjson"),
        (function* () {
            for (let id = 1; id <= n; id++) yield vertex(id, n);
        })(),
    );
    files["edges.ndjson"] = await writeLines(
        join(output, "edges.ndjson"),
        (function* () {
            for (let source = 1; source <= n; source++) {
                let i = 0;
                for (const target of neighbors(source, n))
                    yield { id: `${source}:${i++}`, source, target, label: "link" };
            }
        })(),
    );
    files["validation.ndjson"] = await writeLines(
        join(output, "validation.ndjson"),
        (function* () {
            for (const caseId of cases)
                for (let i = 0; i < 16; i++) {
                    const parameter = parameterAt(caseId, i, scale);
                    yield { parameter, expected: expected(parameter, scale) };
                }
        })(),
    );
    const manifest = {
        schemaVersion: 1,
        suiteId,
        scale,
        seed,
        facts: facts(scale),
        contract: await contractDigest(),
        files,
    };
    const file = await open(join(output, "manifest.json"), "wx");
    try {
        await file.writeFile(JSON.stringify(manifest, null, 2) + "\n");
    } finally {
        await file.close();
    }
    return manifest;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
    try {
        const [command, scale, output] = process.argv.slice(2);
        if (command === "describe" && scale && !output)
            console.log(JSON.stringify({ suiteId, scale, ...facts(scale), contract: await contractDigest() }, null, 2));
        else if (command === "generate" && scale && output && process.argv.length === 5)
            console.log(JSON.stringify(await generateTo(scale, output), null, 2));
        else if (command === "--help" || command === "-h")
            console.log(
                "Usage: node generator.mjs describe <dev|s1|s2|s3>\n       node generator.mjs generate <scale> <new-output-directory>\nNo network access; output must not exist. Larger scales consume substantial disk. describe writes nothing.",
            );
        else throw new Error("Use --help. Unknown command or missing arguments.");
    } catch (error) {
        console.error(JSON.stringify({ ok: false, error: String(error) }));
        process.exitCode = 1;
    }
}
