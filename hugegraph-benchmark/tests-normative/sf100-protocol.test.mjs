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
import { mkdtemp, readFile, writeFile, rm, symlink, mkdir, readdir } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import { hash } from "../dist/io.js";
import {
    validateSf100Request,
    validateSf100Receipt,
    verifyProtocolArtifacts,
    assessSf100Recovery,
} from "../dist/sf100-protocol.js";
import { checkpointSf100 } from "../dist/sf100-checkpoints.js";

const digest = (value) => createHash("sha256").update(value).digest("hex");
async function fixture(fn, stage = "load") {
    const directory = await mkdtemp(join(tmpdir(), "hgbench-protocol-"));
    const request = {
        protocolVersion: 1,
        kind: "sf100-request",
        suite: "hgbench-snb-interactive-v1-sf100",
        requestId: "r1",
        attemptId: "a1",
        identity: {
            planDigest: "1".repeat(64),
            datasetDigest: "2".repeat(64),
            mappingDigest: "3".repeat(64),
            componentDigest: "4".repeat(64),
        },
        stage,
        target: stage === "convert" ? null : { targetDigest: "5".repeat(64), epoch: "empty-graph-1" },
        shard: { id: "person-00001", records: "3" },
        repetition: null,
        inputs: [
            {
                name: stage === "convert" ? "source" : "shard",
                path: "input.jsonl",
                sha256: "6".repeat(64),
                bytes: "21",
            },
        ],
        outputBudgetBytes: "4096",
    };
    const receipt = {
        protocolVersion: 1,
        kind: "sf100-receipt",
        requestId: "r1",
        attemptId: "a1",
        requestDigest: hash(request),
        status: "cancelled",
        effect: "unknown",
        records: null,
        artifacts: [],
        error: { code: "CANCELLED", message: "Writer outcome unknown" },
    };
    const requestFile = join(directory, "request.json"),
        receiptFile = join(directory, "receipt.json"),
        observationFile = join(directory, "observation.json");
    const put = async (name, body) => {
        await writeFile(join(directory, name), body);
        return { path: name, sha256: digest(body), bytes: String(Buffer.byteLength(body)) };
    };
    const save = async () => {
        await writeFile(requestFile, JSON.stringify(request));
        await writeFile(receiptFile, JSON.stringify(receipt));
    };
    const observation = async (disposition, records) => {
        const evidence = await put("db-evidence.txt", "quiesced mock observation");
        const value = {
            protocolVersion: 1,
            kind: "sf100-load-observation",
            requestDigest: hash(request),
            target: structuredClone(request.target),
            shardId: request.shard.id,
            disposition,
            records,
            scope: "entire-shard-identities-and-content",
            writerQuiesced: true,
            evidence: [evidence],
        };
        await writeFile(observationFile, JSON.stringify(value));
        return value;
    };
    try {
        await save();
        await fn({ directory, request, receipt, requestFile, receiptFile, observationFile, put, save, observation });
    } finally {
        await rm(directory, { recursive: true, force: true });
    }
}

test("unknown and cancelled loads require reconciliation, even when receipt claims no effect", async () => {
    await fixture(async ({ requestFile, receiptFile, receipt, directory, save }) => {
        for (const status of ["failed", "cancelled"])
            for (const effect of ["none", "unknown", "committed"]) {
                receipt.status = status;
                receipt.effect = effect;
                await save();
                assert.equal((await assessSf100Recovery(requestFile, receiptFile, directory)).action, "reconcile");
            }
        assert.equal((await assessSf100Recovery(requestFile)).action, "reconcile");
    });
});

test("whole-shard observations distinguish skip, retry, restore and unresolved outcomes", async () => {
    await fixture(async (c) => {
        for (const [disposition, records, action] of [
            ["committed", "3", "skip"],
            ["absent", "0", "retry-new-attempt"],
            ["partial", "1", "restore-and-reload"],
            ["unknown", null, "reconcile"],
        ]) {
            await c.observation(disposition, records);
            assert.equal(
                (await assessSf100Recovery(c.requestFile, c.receiptFile, c.directory, c.observationFile)).action,
                action,
            );
        }
        await c.observation("committed", "3");
        assert.equal(
            (await assessSf100Recovery(c.requestFile, c.receiptFile, undefined, c.observationFile)).action,
            "reconcile",
        );
    });
});

test("completed loader receipt needs a matching proof, not only a successful status or row count", async () => {
    await fixture(async (c) => {
        const proof = await c.observation("committed", "3");
        c.receipt.status = "completed";
        c.receipt.effect = "committed";
        c.receipt.records = "3";
        c.receipt.error = null;
        c.receipt.artifacts = [{ name: "commit-proof", ...(await c.put("proof.json", JSON.stringify(proof))) }];
        await c.save();
        assert.equal((await assessSf100Recovery(c.requestFile, c.receiptFile, c.directory)).action, "skip");
        proof.target.epoch = "another-restore";
        c.receipt.artifacts = [{ name: "commit-proof", ...(await c.put("proof.json", JSON.stringify(proof))) }];
        await c.save();
        await assert.rejects(assessSf100Recovery(c.requestFile, c.receiptFile, c.directory), /epoch or shard/);
    });
});

test("reconciliation rejects partial proof, active writer and stale attempt identity", async () => {
    for (const mutate of [
        (o) => {
            o.records = "2";
        },
        (o) => {
            o.writerQuiesced = false;
        },
        (o) => {
            o.requestDigest = "0".repeat(64);
        },
        (o) => {
            o.scope = "row-count-only";
        },
    ]) {
        await fixture(async (c) => {
            const proof = await c.observation("committed", "3");
            mutate(proof);
            await writeFile(c.observationFile, JSON.stringify(proof));
            await assert.rejects(assessSf100Recovery(c.requestFile, c.receiptFile, c.directory, c.observationFile));
        });
    }
});

test("immutable converter output is reused only after byte verification", async () => {
    await fixture(async (c) => {
        c.receipt.status = "completed";
        c.receipt.effect = "committed";
        c.receipt.records = "3";
        c.receipt.error = null;
        c.receipt.artifacts = [{ name: "shard", ...(await c.put("output.jsonl", "a\nb\nc\n")) }];
        await c.save();
        assert.equal((await assessSf100Recovery(c.requestFile, c.receiptFile, c.directory)).action, "reuse-shard");
        assert.equal((await assessSf100Recovery(c.requestFile, c.receiptFile)).action, "retry-new-attempt");
        await writeFile(join(c.directory, "output.jsonl"), "x\nb\nc\n");
        await assert.rejects(assessSf100Recovery(c.requestFile, c.receiptFile, c.directory), /checksum mismatch/);
    }, "convert");
});

test("all driver recovery paths restart from restored state", async () => {
    await fixture(async (c) => {
        for (const stage of ["validate", "restore", "measure"]) {
            c.request.stage = stage;
            c.request.shard = null;
            c.request.repetition = stage === "validate" ? null : 0;
            const names = {
                validate: ["reference", "driver-config"],
                restore: ["restore-config"],
                measure: ["driver-config", "restoration"],
            }[stage];
            c.request.inputs = names.map((name) => ({ name, path: name + ".json", bytes: "0", sha256: digest("") }));
            c.receipt.requestDigest = hash(c.request);
            await c.save();
            assert.equal(
                (await assessSf100Recovery(c.requestFile, c.receiptFile, c.directory)).action,
                "restore-new-attempt",
            );
        }
    });
});

test("protocol rejects cross-attempt replies, changed input identity, traversal and contradictory status", async () => {
    await fixture(async (c) => {
        const original = structuredClone(c.receipt);
        for (const mutation of [
            (r) => {
                r.requestId = "r2";
            },
            (r) => {
                r.attemptId = "a2";
            },
            (r) => {
                r.requestDigest = "0".repeat(64);
            },
            (r) => {
                r.error = null;
            },
            (r) => {
                r.effect = "none";
                r.records = "1";
            },
        ]) {
            const receipt = structuredClone(original);
            mutation(receipt);
            assert.throws(() => validateSf100Receipt(c.request, receipt));
        }
        c.request.identity.mappingDigest = "0".repeat(64);
        assert.throws(() => validateSf100Receipt(c.request, original), /another immutable/);
        for (const path of ["../escape", "/absolute", "a/../b", "a//b", "a\\b"]) {
            c.request.inputs[0].path = path;
            assert.throws(() => validateSf100Request(c.request));
        }
    });
});

test("artifact checks reject symlinks and keep byte budgets exact above 2^53", async () => {
    await fixture(async (c) => {
        const artifact = await c.put("file", "a");
        await symlink(join(c.directory, "file"), join(c.directory, "link"));
        await assert.rejects(verifyProtocolArtifacts(c.directory, [{ ...artifact, path: "link" }]), /symbolic links/);
        await mkdir(join(c.directory, "folder"));
        await symlink(join(c.directory, "folder"), join(c.directory, "folder-link"));
        await assert.rejects(
            verifyProtocolArtifacts(c.directory, [{ ...artifact, path: "folder-link/file" }]),
            /symbolic links/,
        );
        c.request.outputBudgetBytes = "9007199254740992";
        c.receipt.requestDigest = hash(c.request);
        c.receipt.artifacts = [{ name: "log", path: "log", sha256: digest(""), bytes: "9007199254740993" }];
        assert.throws(() => validateSf100Receipt(c.request, c.receipt), /byte budget/);
    });
});

test("checkpoint records intent before receipt, is concurrent-idempotent, and never replaces an attempt", async () => {
    await fixture(async (c) => {
        const store = join(c.directory, "store");
        await assert.rejects(
            checkpointSf100(store, c.requestFile, c.receiptFile, c.directory),
            (error) => error.code === "CHECKPOINT_MISSING",
        );
        const results = await Promise.all(Array.from({ length: 8 }, () => checkpointSf100(store, c.requestFile)));
        assert.equal(results.filter((r) => !r.reused).length, 1);
        const sealed = await checkpointSf100(store, c.requestFile, c.receiptFile, c.directory);
        assert.equal(sealed.componentStatus, "cancelled");
        assert.equal((await checkpointSf100(store, c.requestFile, c.receiptFile, c.directory)).reused, true);
        const files = await readdir(join(store, "sf100-checkpoints/r1"));
        assert.deepEqual(files.sort(), ["receipt.json", "request.json"]);
        c.receipt.status = "failed";
        await c.save();
        await assert.rejects(
            checkpointSf100(store, c.requestFile, c.receiptFile, c.directory),
            (e) => e.code === "CHECKPOINT_CONFLICT",
        );
        c.request.attemptId = "a2";
        await c.save();
        await assert.rejects(checkpointSf100(store, c.requestFile), (e) => e.code === "CHECKPOINT_CONFLICT");
        assert.equal(JSON.parse(await readFile(results[0].requestPath, "utf8")).attemptId, "a1");
    });
});

test("CLI exports protocol schemas and treats recovery as advice without writing or executing", async () => {
    await fixture(async (c) => {
        const cli = (args) => {
            const p = spawnSync(process.execPath, [resolve(import.meta.dirname, "../dist/cli.js"), ...args], {
                cwd: c.directory,
                encoding: "utf8",
            });
            assert.equal(p.stderr, "");
            return { status: p.status, data: JSON.parse(p.stdout) };
        };
        const before = await readdir(c.directory);
        assert.equal(
            cli(["benchmark", "exchange", c.requestFile, "--receipt", c.receiptFile]).data.data.executed,
            false,
        );
        assert.equal(cli(["benchmark", "recover", c.requestFile]).data.data.action, "reconcile");
        assert.deepEqual(await readdir(c.directory), before);
        assert.ok(cli(["schema", "sf100-request"]).data.data.properties.stage);
        assert.ok(cli(["schema", "sf100-receipt"]).data.data.properties.effect);
        assert.equal(cli(["benchmark", "exchange", c.requestFile, "--artifact-root", c.directory]).status, 2);
    });
});

test("a completed receipt contradicted by a later observation never authorizes blind retry", async () => {
    await fixture(async (c) => {
        const proof = await c.observation("committed", "3");
        c.receipt.status = "completed";
        c.receipt.effect = "committed";
        c.receipt.records = "3";
        c.receipt.error = null;
        c.receipt.artifacts = [{ name: "commit-proof", ...(await c.put("proof.json", JSON.stringify(proof))) }];
        await c.save();
        await c.observation("absent", "0");
        assert.equal(
            (await assessSf100Recovery(c.requestFile, c.receiptFile, c.directory, c.observationFile)).action,
            "restore-and-reload",
        );
    });
});

test("an unknown observation can disclose missing quiescence without enabling replay", async () => {
    await fixture(async (c) => {
        const proof = await c.observation("unknown", null);
        proof.writerQuiesced = false;
        await writeFile(c.observationFile, JSON.stringify(proof));
        assert.equal(
            (await assessSf100Recovery(c.requestFile, c.receiptFile, c.directory, c.observationFile)).action,
            "reconcile",
        );
    });
});
