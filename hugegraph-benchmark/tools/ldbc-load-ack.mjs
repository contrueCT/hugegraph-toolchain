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

// Process acknowledgement permits deferring content verification, never dataset acceptance or replay.
import { lstat, readFile } from "node:fs/promises";
import { join } from "node:path";
import { createHash } from "node:crypto";
import { hash } from "../dist/io.js";
import { validateSf100Request, validateSf100Receipt } from "../dist/sf100-protocol.js";

export async function acknowledgeLoad(job, attempt) {
    const frozen = validateSf100Request(job.request);
    if (frozen.stage !== "load") throw Error("A Loader request is required");
    if (
        !Number.isSafeInteger(job.config?.maxLogBytes) ||
        job.config.maxLogBytes < 1 ||
        job.config.maxLogBytes > 64 * 1024 ** 2
    )
        throw Error("Invalid Loader log budget");
    const evidence = [];
    async function read(name, limit = 65536) {
        const path = join(attempt, name);
        const before = await lstat(path);
        if (!before.isFile() || before.isSymbolicLink() || before.size > limit)
            throw Error("Unbounded Loader evidence");
        const raw = await readFile(path);
        const after = await lstat(path);
        if (
            before.ino !== after.ino ||
            before.size !== after.size ||
            before.mtimeMs !== after.mtimeMs ||
            before.ctimeMs !== after.ctimeMs ||
            raw.length !== before.size
        )
            throw Error("Loader evidence changed");
        const fact = { path: name, bytes: String(raw.length), sha256: createHash("sha256").update(raw).digest("hex") };
        evidence.push(fact);
        return { raw, fact };
    }
    async function json(name) {
        return JSON.parse((await read(name)).raw);
    }
    const process = await json("process-result.json");
    const request = await json("request.json");
    const receipt = validateSf100Receipt(job.request, await json("receipt.json"));
    if (
        process.executed !== true ||
        process.processExitCode !== 0 ||
        process.loadVerified !== false ||
        process.reason !== "RECONCILIATION_REQUIRED" ||
        process.nextAction !== "reconcile" ||
        hash(request) !== hash(job.request) ||
        receipt.effect !== "unknown" ||
        receipt.error?.code !== "RECONCILIATION_REQUIRED"
    )
        throw Error("Loader acknowledgement or request differs");
    if (
        receipt.artifacts.length !== 2 ||
        new Set(receipt.artifacts.map((x) => x.path)).size !== 2 ||
        receipt.artifacts.some((x) => !["stdout.log", "stderr.log"].includes(x.path))
    )
        throw Error("Unexpected Loader logs");
    let stdout;
    let logBytes = 0;
    for (const wanted of receipt.artifacts) {
        const { raw, fact } = await read(wanted.path, 64 * 1024 ** 2);
        logBytes += raw.length;
        if (logBytes > job.config.maxLogBytes) throw Error("Combined Loader log budget exceeded");
        if (fact.bytes !== wanted.bytes || fact.sha256 !== wanted.sha256) throw Error("Loader log digest differs");
        if (wanted.path === "stdout.log") stdout = raw.toString("utf8");
    }
    if (!stdout?.includes("All the insert tasks finished")) throw Error("Synchronous insert completion is unproven");
    const records = BigInt(job.request.shard.records);
    const vertex = job.mapping.vertices.length === 1 && job.mapping.edges.length === 0;
    const edge = job.mapping.vertices.length === 0 && job.mapping.edges.length === 1;
    if (!vertex && !edge) throw Error("Ambiguous Loader category");
    const category = vertex ? "vertex" : "edge";
    for (const kind of ["vertex", "edge"]) {
        for (const phase of ["parse", "insert"]) {
            for (const outcome of ["success", "failure"]) {
                const pattern = new RegExp(`${kind} ${phase} ${outcome}\\s*:\\s*([0-9]+)`, "g");
                const values = [...stdout.matchAll(pattern)].map((x) => BigInt(x[1]));
                const wanted = kind === category && outcome === "success" ? records : 0n;
                if (!values.length || values.some((value) => value !== wanted))
                    throw Error(`Loader ${kind}/${phase}/${outcome} count differs`);
            }
        }
    }
    return {
        schemaVersion: 1,
        kind: "sf100-write-acknowledgement",
        requestDigest: hash(job.request),
        target: job.request.target,
        shardId: job.request.shard.id,
        records: String(records),
        effect: "acknowledged-writes",
        contentVerified: false,
        datasetVerified: false,
        writeReplayed: false,
        nextAction: "full-content-verification-before-dataset-acceptance",
        evidence,
    };
}
