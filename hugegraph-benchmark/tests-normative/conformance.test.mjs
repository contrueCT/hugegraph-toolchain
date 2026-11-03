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

import { test } from "node:test";
import assert from "node:assert/strict";
import { createServer } from "node:http";
import { once } from "node:events";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { spawn } from "node:child_process";
import { generate, verify, canonicalAnswer } from "../benchmarks/core-read-v0.1/reference.mjs";
import { HugeGraphRest, requestFor, checkTrace, verifySnapshot } from "../adapters/hugegraph-rest.mjs";
import { assess, seal, verifyBundle } from "../tools/conformance.mjs";

const assets = generate();
const graphFor = (p) => assets.datasets.find((g) => g.id === p.datasetId);
const expectedFor = (p) => assets.expected.find((e) => e.parameterId === p.parameterId).expected;
function traces(p) {
    const answer = canonicalAnswer(graphFor(p), p, expectedFor(p));
    const row = (body, cursor, status = 200) => ({ method: "GET", ...requestFor(p, cursor), status, body });
    if (p.operation === "paged-neighbors")
        return answer.map((ids, i) =>
            row(
                {
                    edges: ids.map((id) => ({ outV: p.args.source, inV: id, label: "link" })),
                    page: i === answer.length - 1 ? null : `cursor-${i + 1}`,
                },
                i ? `cursor-${i}` : "",
            ),
        );
    if (p.operation === "point")
        return [
            answer
                ? row(answer)
                : row(
                      {
                          exception: "class org.apache.hugegraph.exception.NotFoundException",
                          message: "Vertex does not exist",
                      },
                      undefined,
                      404,
                  ),
        ];
    if (p.operation === "filter") return [row({ vertices: graphFor(p).vertices.filter((v) => answer.includes(v.id)) })];
    const key = p.operation === "common" ? "same_neighbors" : p.operation === "shortest" ? "path" : "vertices";
    return [row({ [key]: answer ?? [] })];
}
function snapshot(graph) {
    return {
        vertices: { vertices: graph.vertices },
        edges: { edges: graph.edges.map((e) => ({ outV: e.source, inV: e.target, label: e.label })) },
        indexes: {
            indexlabels: [{ name: "node_by_group", base_value: "node", index_type: "SECONDARY", fields: ["group"] }],
        },
    };
}
async function submission() {
    const identity = await verify();
    const context = {
        ...identity,
        profile: "tiny-conformance-v1",
        route: "graph-rest",
        adapterRevision: "a".repeat(64),
        sut: { name: "transport-fixture", revision: "test", backend: "fake", environmentRef: "test-only" },
    };
    const snapshots = Object.fromEntries(assets.datasets.map((g) => [g.id, snapshot(g)]));
    const operations = [1, 2].flatMap((pass) =>
        assets.parameters.map((p) => {
            const trace = traces(p);
            return {
                pass,
                parameterId: p.parameterId,
                durationMs: 1,
                status: "response",
                trace,
                actual: checkTrace(p, trace),
            };
        }),
    );
    return [context, { before: snapshots, after: snapshots }, operations];
}

test("all 72 evidence-backed answers produce conformance; missing, reordered, duplicated and forged answers do not", async () => {
    const [context, snapshots, ops] = await submission();
    assert.equal((await assess(context, snapshots, ops)).outcome, "conformant");
    assert.equal((await assess(context, snapshots, ops.slice(0, -1))).outcome, "incomplete");
    assert.equal((await assess(context, { ...snapshots, after: null }, ops)).outcome, "incomplete");
    await assert.rejects(assess(context, snapshots, [...ops, ops[0]]), /duplicate/);
    await assert.rejects(assess(context, snapshots, [ops[1], ops[0], ...ops.slice(2)]), /reordered/);
    await assert.rejects(assess({ ...context, suiteDigest: "0".repeat(64) }, snapshots, ops), /identity/);
    const wrong = structuredClone(ops);
    wrong[0].actual = structuredClone(wrong[0].actual);
    wrong[0].actual.properties.payload = "incorrect";
    await assert.rejects(assess(context, snapshots, wrong), /differs from HTTP/);
    wrong[0].trace[0].body.properties.payload = "incorrect";
    assert.equal((await assess(context, snapshots, wrong)).outcome, "correctness-failed");
    const timeout = structuredClone(ops);
    timeout[0].durationMs = 30001;
    assert.equal((await assess(context, snapshots, timeout)).outcome, "incomplete");
});

test("offline verification detects altered evidence rather than trusting a saved scorecard", async () => {
    const root = await mkdtemp(join(tmpdir(), "hgbench-score-test-"));
    try {
        await seal(root, ...(await submission()));
        assert.equal((await verifyBundle(root)).outcome, "conformant");
        await writeFile(join(root, "scorecard.json"), '{"outcome":"wrong"}');
        assert.equal((await verifyBundle(root)).outcome, "conformant");
        await writeFile(join(root, "operations.json"), "[]");
        await assert.rejects(verifyBundle(root), /checksum/);
    } finally {
        await rm(root, { recursive: true, force: true });
    }
});

test("request and normalization checks preserve string IDs, full projection and pagination exhaustion", () => {
    const p = assets.parameters.find((p) => p.operation === "paged-neighbors");
    const good = traces(p);
    assert.throws(() => checkTrace(p, good.slice(0, -1)), /Incomplete/);
    const loop = structuredClone(good);
    loop[0].body.page = "";
    assert.throws(() => checkTrace(p, loop), /semantics|excessive/);
    const missing = structuredClone(good);
    delete missing.at(-1).body.page;
    assert.throws(() => checkTrace(p, missing), /completion marker/);
    const stringPoint = assets.parameters.find((p) => p.parameterId === "CR01-string-hit");
    assert.match(requestFor(stringPoint).path, /%22v100%22/);
    const absent = assets.parameters.find((p) => p.parameterId === "CR01-numeric-miss");
    const unavailable = traces(absent);
    unavailable[0].body.message = "Graph does not exist";
    assert.throws(() => checkTrace(absent, unavailable), /absence/);
    const badSnapshot = snapshot(assets.datasets[0]);
    badSnapshot.edges.edges = badSnapshot.edges.edges.slice(1);
    assert.throws(() => verifySnapshot(assets.datasets[0], badSnapshot), /edges/);
});

test("real HTTP transport consumes complete pages, bounds responses, times out and refuses nonempty import", async () => {
    const parameter = assets.parameters.find((p) => p.operation === "paged-neighbors");
    const pages = traces(parameter);
    let mode = "normal";
    let mutations = 0;
    const server = createServer((request, response) => {
        const url = new URL(request.url, "http://localhost");
        if (request.method !== "GET") mutations++;
        if (mode === "timeout") return;
        const page = pages.find((p) => p.query.page === url.searchParams.get("page"));
        const body = mode === "occupied" ? { vertices: [assets.datasets[0].vertices[0]] } : page?.body;
        response.writeHead(200, { "Content-Type": "application/json" });
        response.end(JSON.stringify(body));
    });
    server.listen(0, "127.0.0.1");
    await once(server, "listening");
    const url = `http://127.0.0.1:${server.address().port}/graphs/test`;
    try {
        const normal = await new HugeGraphRest(url).execute(parameter);
        assert.equal(normal.status, "response");
        assert.equal(normal.actual.flat().length, 64);
        assert.equal(normal.trace.length, 10);
        assert.equal((await new HugeGraphRest(url, { maxBytes: 10 }).execute(parameter)).status, "error");
        mode = "timeout";
        assert.equal((await new HugeGraphRest(url, { timeoutMs: 20 }).execute(parameter)).status, "timeout");
        mode = "occupied";
        await assert.rejects(new HugeGraphRest(url).importTiny(assets.datasets[0]), /empty dedicated/);
        assert.equal(mutations, 0);
    } finally {
        server.closeAllConnections();
        await new Promise((resolve) => server.close(resolve));
    }
});

test("standalone run and offline verify complete end-to-end against an explicitly labeled HTTP fixture", async () => {
    const root = await mkdtemp(join(tmpdir(), "hgbench-http-e2e-"));
    const key = (query) =>
        JSON.stringify(
            Object.entries(query)
                .map(([k, v]) => [k, String(v)])
                .sort(),
        );
    let writes = 0;
    const server = createServer((request, response) => {
        if (request.method !== "GET") writes++;
        const url = new URL(request.url, "http://localhost");
        const [, datasetId, ...parts] = url.pathname.split("/").slice(1);
        const path = parts.join("/");
        const graph = assets.datasets.find((g) => g.id === datasetId);
        let body;
        let status = 200;
        if (graph) {
            const snap = snapshot(graph);
            if (path === "schema/indexlabels") body = snap.indexes;
            else if (url.searchParams.get("limit") === "10000")
                body = path === "graph/vertices" ? snap.vertices : snap.edges;
            else {
                const match = assets.parameters
                    .filter((p) => p.datasetId === datasetId)
                    .flatMap(traces)
                    .find((t) => t.path === path && key(t.query) === key(Object.fromEntries(url.searchParams)));
                body = match?.body;
                status = match?.status ?? 500;
            }
        }
        response.writeHead(body ? status : 500, { "Content-Type": "application/json" });
        response.end(JSON.stringify(body ?? { error: "Unexpected fixture request" }));
    });
    server.listen(0, "127.0.0.1");
    await once(server, "listening");
    const script = fileURLToPath(new URL("../tools/conformance.mjs", import.meta.url));
    const run = (args) =>
        new Promise((resolve, reject) => {
            const child = spawn(process.execPath, [script, ...args], { stdio: ["ignore", "pipe", "pipe"] });
            let stdout = "";
            let stderr = "";
            child.stdout.on("data", (x) => (stdout += x));
            child.stderr.on("data", (x) => (stderr += x));
            child.on("error", reject);
            child.on("close", (code) => resolve({ code, stdout, stderr }));
        });
    try {
        const config = {
            graphs: Object.fromEntries(
                assets.datasets.map((g) => [g.id, `http://127.0.0.1:${server.address().port}/graphs/${g.id}`]),
            ),
            sut: {
                name: "HTTP fixture, not HugeGraph",
                revision: "test",
                backend: "fake",
                environmentRef: "test-only",
            },
        };
        await writeFile(join(root, "config.json"), JSON.stringify(config));
        const executed = await run(["run", join(root, "config.json"), join(root, "evidence")]);
        assert.equal(executed.code, 0, executed.stdout + executed.stderr);
        assert.equal(JSON.parse(executed.stdout).outcome, "conformant");
        const verified = await run(["verify", join(root, "evidence")]);
        assert.equal(verified.code, 0, verified.stderr);
        assert.equal(JSON.parse(verified.stdout).operations.length, 72);
        assert.equal(writes, 0);
    } finally {
        server.closeAllConnections();
        await new Promise((resolve) => server.close(resolve));
        await rm(root, { recursive: true, force: true });
    }
});
