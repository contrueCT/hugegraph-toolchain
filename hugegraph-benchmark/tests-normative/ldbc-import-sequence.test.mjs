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
import { hash } from "../dist/io.js";
import { importSequential } from "../tools/ldbc-import-sequence.mjs";

function fixture(changes = {}) {
    const calls = [];
    const jobs = ["vertices", "vertices", "edges", "edges"].map((kind, i) => ({
        mapping: { vertices: kind === "vertices" ? [{}] : [], edges: kind === "edges" ? [{}] : [] },
        request: { shard: { id: String(i), records: "2" }, target: { epoch: "initial" } },
    }));
    const proof = (scope) => ({
        scope,
        outcome: "matched",
        contentMatches: true,
        countsMatch: true,
        schemaMatches: true,
        datasetVerified: scope === "full",
    });
    const options = {
        jobs,
        prefixCount: 0,
        counts: { vertices: "4", edges: "4" },
        load: async (job, i) => {
            calls.push("load" + i);
            return {
                kind: "sf100-write-acknowledgement",
                effect: "acknowledged-writes",
                contentVerified: false,
                datasetVerified: false,
                writeReplayed: false,
                requestDigest: hash(job.request),
                target: job.request.target,
                shardId: job.request.shard.id,
                records: job.request.shard.records,
            };
        },
        verifyVertices: async () => {
            calls.push("vertices");
            return proof("vertices");
        },
        verifyFull: async (vertices) => {
            calls.push(vertices ? "full-with-vertex-proof" : "full-alone");
            return proof("full");
        },
        ...changes,
    };
    return { calls, options };
}

test("default content gate precedes edge writes and final full proof", async () => {
    const { calls, options } = fixture();
    const result = await importSequential(options);
    assert.deepEqual(calls, ["load0", "load1", "vertices", "load2", "load3", "full-with-vertex-proof"]);
    assert.equal(result.datasetVerified, true);
    assert.equal(result.performanceAccepted, false);
    assert.equal(result.completedShards, 4);
    assert.equal(result.vertexAdmission, "content");
});

test("explicit acknowledgement policy still requires a full final content proof", async () => {
    const { calls, options } = fixture({ vertexAdmission: "acknowledged-writes" });
    await importSequential(options);
    assert.deepEqual(calls, ["load0", "load1", "load2", "load3", "full-alone"]);
});

test("classified stopped prefix is never replayed", async () => {
    const { calls, options } = fixture({ prefixCount: 3 });
    await importSequential(options);
    assert.deepEqual(calls, ["vertices", "load3", "full-with-vertex-proof"]);
});

test("failed vertex content blocks every edge write", async () => {
    const { calls, options } = fixture({ verifyVertices: async () => ({ scope: "vertices", outcome: "mismatch" }) });
    await assert.rejects(importSequential(options), /content proof/);
    assert.deepEqual(calls, ["load0", "load1"]);
});

test("unknown write acknowledgement stops without replay or later writes", async () => {
    const { calls, options } = fixture();
    const load = options.load;
    options.load = async (job, i) => (i === 1 ? { effect: "unknown" } : load(job, i));
    await assert.rejects(importSequential(options), /acknowledgement/);
    assert.deepEqual(calls, ["load0"]);
});

test("failed full scan cannot return a verified dataset", async () => {
    const { options } = fixture({ verifyFull: async () => ({ scope: "full", outcome: "unknown" }) });
    await assert.rejects(importSequential(options), /content proof/);
});

test("cancellation during event handling prevents the next database operation", async () => {
    let stopped = false;
    const { calls, options } = fixture({
        stopped: () => stopped,
        event: async (state) => {
            if (state.stage === "load") stopped = true;
        },
    });
    await assert.rejects(importSequential(options), /Cancelled/);
    assert.deepEqual(calls, []);
});

test("invalid prefix, category order, duplicate identity and totals fail before writes", async () => {
    for (const mutation of [
        (options) => {
            options.prefixCount = 5;
        },
        (options) => {
            options.vertexAdmission = "automatic";
        },
        (options) => {
            [options.jobs[1], options.jobs[2]] = [options.jobs[2], options.jobs[1]];
        },
        (options) => {
            options.jobs[1].request.shard.id = "0";
        },
        (options) => {
            options.counts.vertices = "3";
        },
    ]) {
        const { calls, options } = fixture();
        mutation(options);
        await assert.rejects(importSequential(options));
        assert.deepEqual(calls, []);
    }
});

test("record coverage stops at the selected boundary without claiming full content", async () => {
    const { calls, options } = fixture({ minimumEdgeCoverage: 0.5, finalVerification: "write-acknowledgements" });
    const result = await importSequential(options);
    assert.deepEqual(calls, ["load0", "load1", "vertices", "load2"]);
    assert.equal(result.coverage.counts.edges.fraction, 0.5);
    assert.equal(result.coverage.accepted, true);
    assert.equal(result.coverage.fullImport, false);
    assert.equal(result.datasetVerified, false);
    assert.equal(result.fullProof, null);
});

test("100 percent remains selectable without forcing a content scan", async () => {
    const { calls, options } = fixture({ finalVerification: "write-acknowledgements" });
    const result = await importSequential(options);
    assert.equal(result.coverage.fullImport, true);
    assert.deepEqual(calls, ["load0", "load1", "vertices", "load2", "load3"]);
});

test("95 percent counts records rather than equally weighted shards", async () => {
    const { options } = fixture({ minimumEdgeCoverage: 0.95, finalVerification: "write-acknowledgements" });
    options.jobs[2].request.shard.records = "95";
    options.jobs[3].request.shard.records = "5";
    options.counts.edges = "100";
    const result = await importSequential(options);
    assert.equal(result.completedShards, 3);
    assert.equal(result.coverage.counts.edges.acknowledged, "95");
});

test("query-required edge labels complete even when aggregate threshold was reached", async () => {
    const { options } = fixture({
        minimumEdgeCoverage: 0.5,
        finalVerification: "write-acknowledgements",
        requiredEdgeLabels: ["knows"],
    });
    options.jobs[2].mapping.edges[0].label = "other";
    options.jobs[3].mapping.edges[0].label = "knows";
    const result = await importSequential(options);
    assert.equal(result.completedShards, 4);
});

test("invalid thresholds and incompatible full-proof settings fail before writing", async () => {
    for (const fraction of [0, -1, 1.01, NaN, Infinity]) {
        const { calls, options } = fixture({
            minimumEdgeCoverage: fraction,
            finalVerification: "write-acknowledgements",
        });
        await assert.rejects(importSequential(options));
        assert.deepEqual(calls, []);
    }
    const { options } = fixture({ minimumEdgeCoverage: 0.95 });
    await assert.rejects(importSequential(options), /Full content/);
});
