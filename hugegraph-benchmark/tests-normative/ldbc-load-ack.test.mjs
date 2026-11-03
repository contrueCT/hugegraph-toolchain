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

import assert from "node:assert/strict";
import { test } from "node:test";
import { mkdtemp, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createHash } from "node:crypto";
import { hash } from "../dist/io.js";
import { acknowledgeLoad } from "../tools/ldbc-load-ack.mjs";

async function fixture(fn, options = {}) {
    const root = await mkdtemp(join(tmpdir(), "hgbench-load-ack-"));
    try {
        const category = options.category ?? "vertex";
        const records = "9007199254740993";
        const request = {
            protocolVersion: 1,
            kind: "sf100-request",
            suite: "hgbench-snb-interactive-v1-sf100",
            requestId: "load-fixture",
            attemptId: "initial",
            stage: "load",
            repetition: null,
            identity: Object.fromEntries(
                ["planDigest", "datasetDigest", "mappingDigest", "componentDigest"].map((key) => [key, "a".repeat(64)]),
            ),
            target: { targetDigest: "b".repeat(64), epoch: "unit-initial" },
            shard: { id: category + "-0", records },
            inputs: [{ name: "shard", path: "shard.json", bytes: "1", sha256: "c".repeat(64) }],
            outputBudgetBytes: "65536",
        };
        const job = {
            request,
            config: { maxLogBytes: 65536 },
            mapping: {
                vertices: category === "vertex" ? [{}] : [],
                edges: category === "edge" ? [{}] : [],
            },
        };
        const lines = [];
        for (const kind of ["vertex", "edge"])
            for (const phase of ["parse", "insert"])
                for (const result of ["success", "failure"]) {
                    const value = kind === category && result === "success" ? records : "0";
                    lines.push(`${kind} ${phase} ${result} : ${value}`);
                }
        lines.push("All the insert tasks finished");
        let stdout = lines.join("\n");
        if (options.stdout) stdout = options.stdout(stdout);
        const artifacts = [];
        for (const [name, text] of [
            ["stdout", stdout],
            ["stderr", ""],
        ]) {
            const path = name + ".log";
            await writeFile(join(root, path), text);
            artifacts.push({
                name,
                path,
                bytes: String(Buffer.byteLength(text)),
                sha256: createHash("sha256").update(text).digest("hex"),
            });
        }
        const result = {
            executed: true,
            processExitCode: options.exit ?? 0,
            loadVerified: false,
            reason: "RECONCILIATION_REQUIRED",
            nextAction: "reconcile",
        };
        const receipt = {
            protocolVersion: 1,
            kind: "sf100-receipt",
            requestId: request.requestId,
            attemptId: request.attemptId,
            requestDigest: hash(request),
            status: "failed",
            effect: "unknown",
            records: null,
            error: { code: "RECONCILIATION_REQUIRED", message: "Content pending" },
            artifacts,
        };
        for (const [name, value] of [
            ["process-result.json", result],
            ["receipt.json", receipt],
            ["request.json", request],
        ])
            await writeFile(join(root, name), JSON.stringify(value));
        await fn(job, root);
    } finally {
        await rm(root, { recursive: true, force: true });
    }
}

test("vertex and edge acknowledgements retain exact counts and never qualify content", async () => {
    for (const category of ["vertex", "edge"])
        await fixture(
            async (job, root) => {
                const result = await acknowledgeLoad(job, root);
                assert.equal(result.records, "9007199254740993");
                assert.equal(result.contentVerified, false);
                assert.equal(result.datasetVerified, false);
                assert.equal(result.evidence.length, 5);
                assert.equal(result.writeReplayed, false);
            },
            { category },
        );
});

test("zero exit without drained futures or complete counters remains unacknowledged", async () => {
    for (const stdout of [
        (text) => text.replace("All the insert tasks finished", "pending"),
        (text) => text.replace("vertex insert success : 9007199254740993", "vertex insert success : 9007199254740992"),
        (text) => text.replace("vertex insert failure : 0", "vertex insert failure : 1"),
        (text) => text.replace("edge parse success : 0", "edge parse success : 1"),
    ]) {
        await fixture((job, root) => assert.rejects(acknowledgeLoad(job, root)), { stdout });
    }
});

test("a failed process is rejected despite good counters", async () => {
    await fixture((job, root) => assert.rejects(acknowledgeLoad(job, root)), { exit: 1 });
});

test("log mutation, conflicting request and ambiguous categories are rejected", async () => {
    await fixture(async (job, root) => {
        await writeFile(join(root, "stdout.log"), "changed");
        await assert.rejects(acknowledgeLoad(job, root), /digest/);
    });
    await fixture(async (job, root) => {
        await writeFile(join(root, "request.json"), JSON.stringify({ ...job.request, requestId: "other" }));
        await assert.rejects(acknowledgeLoad(job, root), /request differs/);
    });
    await fixture(async (job, root) => {
        job.mapping.vertices = [{}, {}];
        await assert.rejects(acknowledgeLoad(job, root), /Ambiguous/);
    });
});

test("missing summary counters and combined log overflow are rejected", async () => {
    await fixture((job, root) => assert.rejects(acknowledgeLoad(job, root)), {
        stdout: (text) => text.replace(/vertex parse success[^\n]+\n/, ""),
    });
    await fixture((job, root) => {
        job.config.maxLogBytes = 1;
        return assert.rejects(acknowledgeLoad(job, root), /budget/);
    });
});
