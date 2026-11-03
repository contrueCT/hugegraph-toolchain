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

import { hash } from "../dist/io.js";
import { importCoverage } from "./ldbc-import-coverage.mjs";

function need(value, message) {
    if (!value) throw Error(message);
}

function matched(proof, scope) {
    need(
        proof?.outcome === "matched" &&
            proof.scope === scope &&
            proof.contentMatches === true &&
            proof.schemaMatches === true &&
            proof.countsMatch === true,
        "Full content proof did not match",
    );
    if (scope === "full") need(proof.datasetVerified === true, "Full dataset proof is missing");
    else need(proof.datasetVerified === false, "Vertex proof must not qualify a full dataset");
}

// Lifecycle only. Caller must bind immutable jobs, classified stopped-prefix evidence and target leases.
export async function importSequential({
    jobs,
    prefixCount,
    counts,
    vertexAdmission = "content",
    minimumEdgeCoverage = 1,
    requiredEdgeLabels = [],
    finalVerification = "full-content",
    load,
    verifyVertices,
    verifyFull,
    stopped = () => false,
    event = async () => {},
}) {
    need(["content", "acknowledged-writes"].includes(vertexAdmission), "Unknown vertex-admission policy");
    need(Number.isSafeInteger(prefixCount) && prefixCount >= 0 && prefixCount <= jobs.length, "Invalid stopped prefix");
    const categories = jobs.map((job) => {
        const vertex = job.mapping.vertices.length === 1 && job.mapping.edges.length === 0;
        const edge = job.mapping.vertices.length === 0 && job.mapping.edges.length === 1;
        need(vertex || edge, "Ambiguous Loader category");
        return vertex ? "vertices" : "edges";
    });
    need(["full-content", "write-acknowledgements"].includes(finalVerification), "Unknown final verification policy");
    need(finalVerification !== "full-content" || minimumEdgeCoverage === 1, "Full content requires all source records");
    importCoverage(jobs, prefixCount, minimumEdgeCoverage, requiredEdgeLabels);
    const firstEdge = categories.indexOf("edges");
    need(firstEdge > 0 && categories.slice(firstEdge).every((kind) => kind === "edges"), "Vertices must precede edges");
    need(new Set(jobs.map((job) => job.request.shard.id)).size === jobs.length, "Duplicate shard identity");
    const totals = { vertices: 0n, edges: 0n };
    for (const [index, job] of jobs.entries()) totals[categories[index]] += BigInt(job.request.shard.records);
    need(
        totals.vertices === BigInt(counts.vertices) && totals.edges === BigInt(counts.edges),
        "Full job totals differ",
    );
    const loaded = { vertices: 0n, edges: 0n };
    for (let i = 0; i < prefixCount; i++) loaded[categories[i]] += BigInt(jobs[i].request.shard.records);
    let acknowledged = prefixCount;
    const guard = () => need(!stopped(), "Cancelled/deadline reached; do not retry writes");
    const state = async (stage, extra = {}) =>
        event({
            stage,
            acknowledgedShards: acknowledged,
            acknowledged: { vertices: String(loaded.vertices), edges: String(loaded.edges) },
            vertexAdmission,
            datasetVerified: false,
            ...extra,
        });
    async function write(index) {
        guard();
        const job = jobs[index];
        await state("load", { shardIndex: index, shardId: job.request.shard.id, category: categories[index] });
        guard();
        const ack = await load(job, index);
        guard();
        need(
            ack?.kind === "sf100-write-acknowledgement" &&
                ack.effect === "acknowledged-writes" &&
                ack.contentVerified === false &&
                ack.datasetVerified === false &&
                ack.writeReplayed === false &&
                ack.requestDigest === hash(job.request) &&
                ack.shardId === job.request.shard.id &&
                hash(ack.target) === hash(job.request.target) &&
                ack.records === job.request.shard.records,
            "Loader acknowledgement is missing or belongs to another job",
        );
        loaded[categories[index]] += BigInt(ack.records);
        acknowledged++;
        await state("acknowledged", { shardIndex: index });
    }
    for (let i = prefixCount; i < firstEdge; i++) await write(i);
    guard();
    need(loaded.vertices === totals.vertices, "All vertex writes must be acknowledged before edges");
    let vertexProof = null;
    if (vertexAdmission === "content") {
        await state("vertex-content-scan", { allWritersStopped: true });
        guard();
        vertexProof = await verifyVertices();
        guard();
        matched(vertexProof, "vertices");
        await state("vertex-content-matched", { allWritersStopped: true });
    } else {
        await state("vertex-content-deferred-to-final", { allWritersStopped: true });
    }
    for (let i = Math.max(prefixCount, firstEdge); i < jobs.length; i++) {
        if (importCoverage(jobs, acknowledged, minimumEdgeCoverage, requiredEdgeLabels).accepted) break;
        await write(i);
    }
    const coverage = importCoverage(jobs, acknowledged, minimumEdgeCoverage, requiredEdgeLabels);
    guard();
    need(coverage.accepted, "Import coverage threshold was not reached");
    if (finalVerification === "write-acknowledgements") {
        await state("import-accepted", { allWritersStopped: true, coverage });
        return {
            datasetVerified: false,
            performanceAccepted: false,
            importAccepted: true,
            vertexAdmission,
            completedShards: acknowledged,
            counts: { vertices: String(loaded.vertices), edges: String(loaded.edges) },
            vertexProof,
            fullProof: null,
            coverage,
        };
    }
    guard();
    need(loaded.edges === totals.edges && acknowledged === jobs.length, "Not all writes were acknowledged");
    await state("full-content-scan", { allWritersStopped: true });
    guard();
    const fullProof = await verifyFull(vertexProof);
    guard();
    matched(fullProof, "full");
    return {
        datasetVerified: true,
        performanceAccepted: false,
        vertexAdmission,
        completedShards: acknowledged,
        counts: { vertices: String(loaded.vertices), edges: String(loaded.edges) },
        vertexProof,
        fullProof,
    };
}

// Process acknowledgement permits deferring content verification, never dataset acceptance or replay.
