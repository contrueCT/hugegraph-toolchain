/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, readFile, writeFile, access, readdir } from "node:fs/promises";
import { join, resolve } from "node:path";
import { tmpdir } from "node:os";
import http from "node:http";
import { createHash } from "node:crypto";
import { prepareGraphProfile, verifyGraphProfile, validateGraphImport, graphCases } from "../dist/graph.js";
import { generateHttpProfile } from "../dist/http-suite.js";
import { registerHttpGo } from "../dist/http-go.js";
import { runHttpThroughput } from "../dist/http-throughput.js";
import { graphThroughputPresets, throughputFixedMs } from "../dist/throughput.js";
import { validGraphValue, graphResponse } from "../components/http/graph-validation.mjs";
import { reportThroughput } from "../components/http/throughput-report.mjs";

const a = "person:9007199254740993",
    b = "person:9007199254740994";
function reference() {
    const pools = {};
    pools.point = [
        { id: "p0", stratum: "hit", source: a, rule: { kind: "vertex", id: a, label: "person", absent: false } },
        {
            id: "p1",
            stratum: "miss",
            source: "missing",
            rule: { kind: "vertex", id: "missing", label: "person", absent: true },
        },
    ];
    for (const id of ["neighbors", "gremlin-neighbors", "common"])
        pools[id] = [
            {
                id: "p0",
                stratum: "nonempty",
                source: a,
                ...(id === "common" ? { target: b } : {}),
                rule: { kind: "id-set", pointer: "/vertices", allowed: [b, "c", "d"], count: 3 },
            },
            {
                id: "p1",
                stratum: "empty",
                source: b,
                ...(id === "common" ? { target: a } : {}),
                rule: { kind: "id-set", pointer: "/vertices", allowed: [], count: 0 },
            },
        ];
    pools.shortest = [
        {
            id: "p0",
            stratum: "distance-2",
            source: a,
            target: b,
            rule: {
                kind: "shortest-path",
                pointer: "/path",
                source: a,
                target: b,
                distance: 2,
                edges: [
                    [a, "c"],
                    ["c", b],
                    [a, "d"],
                    ["d", b],
                ],
            },
        },
        {
            id: "p1",
            stratum: "unreachable",
            source: b,
            target: a,
            rule: { kind: "shortest-path", pointer: "/path", source: b, target: a, distance: -1, edges: [] },
        },
    ];
    return {
        schemaVersion: 1,
        kind: "graph-query-reference",
        dataset: {
            id: "PROTOCOL-FIXTURE-ONLY-no-loaded-graph",
            scaleFactor: 100,
            manifestSha256: "a".repeat(64),
            vertices: 282637871,
            edges: 1775513741,
        },
        origin: "independent-csv",
        provenance: { scope: "Protocol fixture; declared metadata only, no SF100 import or graph result" },
        pools,
    };
}
test("graph defaults retain five cases and SF100; independent set/path validator rejects semantic errors", () => {
    assert.equal(throughputFixedMs(5, graphThroughputPresets.quick), 625000);
    assert.equal(throughputFixedMs(5, graphThroughputPresets.standard), 3150000);
    const r = reference();
    const set = r.pools.neighbors[0].rule;
    assert.ok(validGraphValue(["d", b, "c"], set));
    for (const wrong of [["d", "d", b], ["d", b], ["d", b, "wrong"], [Number(a.slice(7)), "c", "d"], null])
        assert.equal(validGraphValue(wrong, set), false);
    const path = r.pools.shortest[0].rule;
    assert.ok(validGraphValue([a, "d", b], path));
    assert.equal(validGraphValue([a, b], path), false);
    assert.equal(validGraphValue([b, "d", a], path), false);
    assert.deepEqual(graphResponse({ ...r.pools.shortest[1], method: "GET" }), { path: [] });
});
test(
    "graph pooled GET/POST -> Go -> shared quick/standard -> verified raw/coverage -> SVG, with cached calibration",
    { timeout: 720000 },
    async (t) => {
        const binary = process.env.HGBENCH_GO_BINARY;
        if (!binary) return t.skip("Set HGBENCH_GO_BINARY to the current registered native Go runner");
        const dir = process.env.HGBENCH_GRAPH_EVIDENCE
            ? resolve(process.env.HGBENCH_GRAPH_EVIDENCE)
            : await mkdtemp(join(tmpdir(), "hgbench-graph-"));
        let calls = 0,
            bad = false;
        const routes = new Map();
        const server = http.createServer((req, res) => {
            const chunks = [];
            req.on("data", (b) => chunks.push(b));
            req.on("end", () => {
                const bodyKey =
                    req.method === "POST" ? JSON.stringify(JSON.parse(Buffer.concat(chunks).toString())) : "";
                const row = routes.get(req.url + bodyKey);
                calls++;
                if (
                    !row ||
                    req.method !== row.method ||
                    (row.method === "POST" &&
                        JSON.stringify(JSON.parse(Buffer.concat(chunks).toString())) !== JSON.stringify(row.body))
                ) {
                    res.writeHead(400);
                    res.end("{}");
                    return;
                }
                let body = graphResponse(row);
                if (row.rule.kind === "id-set") {
                    const key =
                        row.rule.pointer === "/same_neighbors"
                            ? "same_neighbors"
                            : row.rule.pointer === "/vertices"
                              ? "vertices"
                              : null;
                    if (key) body[key].reverse();
                }
                if (row.rule.kind === "shortest-path" && row.rule.distance === 2) body = { path: [a, "d", b] };
                if (bad && row.rule.kind === "vertex") body = { id: "wrong", label: "person" };
                setTimeout(() => {
                    res.writeHead(row.expectedStatus, { "Content-Type": "application/json" });
                    res.end(JSON.stringify(body));
                }, 5);
            });
        });
        await new Promise((r) => server.listen(Number(process.env.HGBENCH_GRAPH_PORT ?? 0), "127.0.0.1", r));
        t.after(
            () =>
                new Promise((r) => {
                    server.closeAllConnections();
                    server.close(r);
                }),
        );
        const settings = JSON.parse(await readFile("examples/http-suite-functional.json"));
        settings.endpoint = `http://127.0.0.1:${server.address().port}/gremlin`;
        Object.assign(settings, { requestTimeoutMs: 1000, drainMs: 1000, maxRequestsPerPhase: 1000000 });
        await writeFile(join(dir, "settings.json"), JSON.stringify(settings));
        await generateHttpProfile(join(dir, "settings.json"), join(dir, "http.json"), false);
        await registerHttpGo(join(dir, "http.json"), binary, join(dir, "go.json"), {
            gomaxprocs: 2,
            cpuQuotaCores: 2,
            maxRssBytes: 1073741824,
        });
        const ref = reference();
        await writeFile(join(dir, "reference.json"), JSON.stringify(ref));
        const importedEdges = Math.ceil(ref.dataset.edges * 0.95);
        const importSnapshot = join(dir, "snapshot.json");
        await writeFile(
            importSnapshot,
            JSON.stringify({
                schemaVersion: 1,
                kind: "graph-import-snapshot",
                sourceManifestSha256: ref.dataset.manifestSha256,
                acknowledgedPrefixSha256: "c".repeat(64),
                vertices: ref.dataset.vertices,
                edges: importedEdges,
                expectedVertices: ref.dataset.vertices,
                expectedEdges: ref.dataset.edges,
                writersStopped: true,
                basis: "loader-acknowledgements",
                vertexContentVerified: true,
                edgeContentVerified: false,
                labels: [
                    {
                        category: "vertices",
                        label: "person",
                        expected: String(ref.dataset.vertices),
                        acknowledged: String(ref.dataset.vertices),
                        missing: "0",
                    },
                    {
                        category: "edges",
                        label: "person_knows_person",
                        expected: "10",
                        acknowledged: "10",
                        missing: "0",
                    },
                    {
                        category: "edges",
                        label: "other",
                        expected: String(ref.dataset.edges - 10),
                        acknowledged: String(importedEdges - 10),
                        missing: String(ref.dataset.edges - importedEdges),
                    },
                ],
            }),
        );
        const profile = join(dir, "graph.json");
        await prepareGraphProfile(join(dir, "reference.json"), join(dir, "go.json"), {
            out: profile,
            importSnapshot,
            minEdgeCoverage: 0.95,
            graphUrl: `http://127.0.0.1:${server.address().port}/graphspaces/DEFAULT/graphs/sf100`,
            traversalSource: "__g_sf100",
        });
        const p = JSON.parse(await readFile(profile));
        verifyGraphProfile(p);
        assert.deepEqual(
            p.cases.map((c) => c.id),
            graphCases,
        );
        for (const c of p.cases)
            for (const r of c.requestPool) {
                const u = new URL(r.url);
                routes.set(u.pathname + u.search + (r.method === "POST" ? JSON.stringify(r.body) : ""), r);
            }
        const drift = structuredClone(p);
        drift.cases[0].requestPool[0].rule.id = "changed";
        assert.throws(() => verifyGraphProfile(drift));
        const custom = join(dir, "windows.json");
        await writeFile(
            custom,
            JSON.stringify({
                concurrency: [1, 2],
                exploreWarmupMs: 0,
                exploreDurationMs: 100,
                confirmWarmupMs: 0,
                confirmDurationMs: 100,
                confirmRepeats: 2,
            }),
        );
        const options = {
            out: join(dir, "standard"),
            root: join(dir, "tasks"),
            preset: "standard",
            track: "graph",
            settings: custom,
            cache: process.env.HGBENCH_GRAPH_CACHE ?? join(dir, "cache"),
            budgetMs: 420000,
        };
        const dry = await runHttpThroughput(profile, { ...options, dryRun: true });
        assert.equal(dry.track, "graph");
        assert.deepEqual(dry.cases, graphCases);
        assert.equal(calls, 0);
        await assert.rejects(access(options.out));
        const result = await runHttpThroughput(profile, options);
        assert.equal(result.status, "completed", JSON.stringify(result.failure));
        assert.equal(result.incomplete.length, 0);
        assert.equal(result.calibration.records.length, 2);
        assert.equal(result.stages.filter((s) => s.stage !== "calibration").length, 10);
        const report = JSON.parse(await readFile(result.reportJson));
        for (const c of report.cases) {
            assert.equal(c.rows.length, 6);
            for (const row of c.rows) {
                assert.equal(row.failed, 0);
                assert.equal(row.parameters.poolSize, 2);
                assert.equal(row.parameters.sentCoverage, 1);
            }
        }
        assert.equal(report.performanceAccepted, false);
        assert.match(await readFile(join(options.out, "report/report.html"), "utf8"), /Imported edges: 95/);
        assert.equal(
            (await readdir(join(options.out, "report/charts"))).some((n) => n.endsWith("-concurrency.svg")),
            true,
        );
        const quick = await runHttpThroughput(profile, {
            ...options,
            preset: "quick",
            out: join(dir, "quick"),
            // Bounded Linux containers need room for process/evidence overhead between the fixed windows.
            budgetMs: 240000,
        });
        assert.equal(quick.status, "completed", JSON.stringify(quick.failure));
        assert.equal(quick.calibration.reused, true);
        const rebuilt = await reportThroughput(options.out, join(dir, "rebuilt"));
        assert.deepEqual(JSON.parse(await readFile(join(dir, "rebuilt/report.json"))).cases, report.cases);
        // Actual Go/HTTP windows with synthetic observation inputs: diagnostics cannot change metrics or gates.
        const origin = Date.parse(report.cases[0].rows[0].diagnostics.startedAt);
        const last = Math.max(...report.cases.flatMap((c) => c.rows.map((r) => Date.parse(r.diagnostics.finishedAt))));
        const hosts = Array.from(
            { length: Math.ceil((last - origin) / 1000) + 4 },
            (_, i) => JSON.stringify({ atUnix: (origin - 1000 + i * 1000) / 1000, swapTotalBytes: 0 }) + "\n",
        ).join("");
        const gc = `[${new Date(origin + 50).toISOString()}][0.05s][info][gc] GC(1) Pause Full (synthetic protocol observation) 32M->16M(64M) 10.000ms\n`;
        const sha = (b) => createHash("sha256").update(b).digest("hex");
        await writeFile(join(dir, "host.ndjson"), hosts);
        await writeFile(join(dir, "gc.log"), gc);
        const manifest = {
            schemaVersion: 1,
            kind: "throughput-target-diagnostics",
            planSha256: report.planSha256,
            maxSampleGapMs: 1500,
            hostSamples: {
                path: "host.ndjson",
                bytes: Buffer.byteLength(hosts),
                sha256: sha(hosts),
                format: "sf100-host-guard-v1",
            },
            gcLogs: [{ role: "server", path: "gc.log", bytes: Buffer.byteLength(gc), sha256: sha(gc) }],
        };
        await writeFile(join(dir, "observations.json"), JSON.stringify(manifest));
        const callsBeforeReport = calls;
        const attached = await reportThroughput(options.out, join(dir, "attached"), {
            targetDiagnostics: join(dir, "observations.json"),
        });
        const attachedReport = JSON.parse(await readFile(attached.reportJson));
        assert.deepEqual(attachedReport.cases, report.cases);
        assert.equal(attachedReport.performanceAccepted, false);
        assert.equal(attachedReport.targetDiagnostics.rows.length, 30);
        assert(attachedReport.targetDiagnostics.rows.every((r) => r.swap.status === "sampled-disabled"));
        assert(attachedReport.targetDiagnostics.rows.some((r) => r.gc.loggedPauses === 1));
        assert.match(await readFile(attached.reportHtml, "utf8"), /不改变性能资格/);
        const relocated = await reportThroughput(options.out, join(dir, "relocated"), {
            targetDiagnostics: join(dir, "attached/target-diagnostics/manifest.json"),
        });
        assert.deepEqual(JSON.parse(await readFile(relocated.reportJson)), attachedReport);
        assert.equal(calls, callsBeforeReport);
        const callsBeforeWrongAnswer = calls;
        bad = true;
        const failed = await runHttpThroughput(profile, {
            ...options,
            preset: "quick",
            out: join(dir, "wrong-answer"),
            budgetMs: 30000,
        });
        assert.equal(failed.status, "partial");
        assert.ok(failed.incomplete.length);
        assert.equal(failed.failure?.code, "THROUGHPUT_CHILD_PARTIAL");
        assert.ok(calls > callsBeforeWrongAnswer);
        const badProbe = join(dir, "wrong-answer/explore-point/evidence/preflight");
        assert.equal(JSON.parse(await readFile(join(badProbe, "result.json"))).passed, false);
        const rejected = JSON.parse(await readFile(join(badProbe, "p0001.json"))).phases[0];
        assert.deepEqual(rejected.counts, { "wrong-answer": 1 });
        assert.equal(rejected.sent, 1);
        assert.equal(rejected.budgetExhausted, false);
        assert.equal(rejected.cancelled, false);
        await writeFile(
            join(dir, "acceptance.json"),
            JSON.stringify(
                {
                    scope: "Protocol fixture, no graph imported or measured",
                    standard: report.cases.reduce((n, c) => n + c.rows.length, 0),
                    quick: quick.status,
                    cacheReused: quick.calibration.reused,
                    wrongAnswerRejected: failed.status === "partial",
                    performanceAccepted: false,
                },
                null,
                2,
            ),
        );
    },
);

test("graph import admission honors 95/100 choices and preserves reference-required topology", () => {
    const source = { manifestSha256: "a".repeat(64), vertices: 100, edges: 100 };
    const snapshot = {
        sourceManifestSha256: source.manifestSha256,
        expectedVertices: 100,
        expectedEdges: 100,
        vertices: 100,
        edges: 95,
        labels: [
            { category: "vertices", label: "person", expected: "100", acknowledged: "100", missing: "0" },
            { category: "edges", label: "person_knows_person", expected: "10", acknowledged: "10", missing: "0" },
            { category: "edges", label: "other", expected: "90", acknowledged: "85", missing: "5" },
        ],
    };
    assert.doesNotThrow(() => validateGraphImport(snapshot, source, 0.95));
    assert.throws(() => validateGraphImport(snapshot, source, 1), /coverage/);
    const missingKnows = structuredClone(snapshot);
    missingKnows.labels[1].acknowledged = "9";
    missingKnows.labels[1].missing = "1";
    missingKnows.labels[2].acknowledged = "86";
    missingKnows.labels[2].missing = "4";
    assert.throws(() => validateGraphImport(missingKnows, source, 0.95), /person_knows_person/);
    assert.throws(
        () => validateGraphImport({ ...snapshot, sourceManifestSha256: "b".repeat(64) }, source, 0.95),
        /identity/,
    );
    assert.throws(() => validateGraphImport({ ...snapshot, edges: 96 }, source, 0.95), /totals/);
});
