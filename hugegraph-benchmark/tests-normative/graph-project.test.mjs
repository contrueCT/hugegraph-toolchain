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
import { join } from "node:path";
import { readFile, lstat, readdir } from "node:fs/promises";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { fixture } from "./graph-project.fixture.mjs";
import { importGraphDataset, runGraphProject, graphDatasets, graphStatus } from "../dist/graph-project.js";
import { graphProjectSchema } from "../dist/graph-project-contracts.js";
import { prepareGraphProfile } from "../dist/graph.js";
import { generateHttpProfile } from "../dist/http-suite.js";
import { registerHttpGo } from "../dist/http-go.js";
const read = async (path) => JSON.parse(await readFile(path, "utf8"));
const run = promisify(execFile);
test("default SF1, optional SF10/SF100, public help and preparation preview", async () => {
    const definitions = await graphDatasets();
    assert.equal(definitions.defaultScale, 1);
    assert.deepEqual(
        definitions.datasets.map((d) => d.scaleFactor),
        [1, 10, 100],
    );
    for (const args of [
        ["graph", "dataset", "prepare", "--help"],
        ["graph", "import", "--help"],
        ["graph", "benchmark", "--help"],
    ]) {
        const result = await run(process.execPath, ["dist/cli.js", ...args]);
        assert.match(result.stdout, /--dry-run/);
    }
    const dry = await run(process.execPath, [
        "dist/cli.js",
        "graph",
        "dataset",
        "prepare",
        "--out",
        "/tmp/hgbench-do-not-create-sf1",
        "--dry-run",
    ]);
    assert.equal(JSON.parse(dry.stdout).data.scaleFactor, 1);
});
test("lightweight import checks source/ack counts, prioritizes knows, persists coverage and resumes without replay", async (t) => {
    const c = await fixture(t),
        out = join(c.dir, "import");
    const options = {
        out,
        loaderHome: c.home,
        graphUrl: c.graphUrl,
        traversalSource: c.traversalSource,
        minimumEdgeCoverage: 0.5,
    };
    const dry = await importGraphDataset(c.data, { ...options, dryRun: true });
    assert.equal(dry.fullContentScan, false);
    assert.equal(c.state.checks, 0);
    await assert.rejects(lstat(out));
    const imported = await importGraphDataset(c.data, options);
    assert.equal(imported.status, "completed");
    assert.deepEqual(
        c.state.loads.map((l) => l.label),
        ["person", "person_knows_person", "background"],
    );
    const snapshot = await read(imported.snapshot);
    assert.equal(snapshot.vertices, 2);
    assert.equal(snapshot.edges, 2);
    assert.equal(snapshot.edgeContentVerified, false);
    assert.equal(c.state.schemas, 1);
    assert.equal(c.state.checks, 1);
    await importGraphDataset(c.data, { ...options, resume: true });
    assert.equal(c.state.loads.length, 3);
    assert.equal(c.state.schemas, 1);
    const status = await graphStatus(out);
    assert.equal(status.states["state.json"].stage, "completed");
    await assert.rejects(importGraphDataset(c.data, { ...options, resume: true, minimumEdgeCoverage: 1 }), {
        code: "GRAPH_IMPORT_CHANGED",
    });
});
test("bad source totals and cancelled import stop before target requests", async (t) => {
    const c = await fixture(t);
    const options = {
        out: join(c.dir, "import"),
        loaderHome: c.home,
        graphUrl: c.graphUrl,
        traversalSource: c.traversalSource,
    };
    await assert.rejects(importGraphDataset(c.data, { ...options, signal: AbortSignal.abort() }));
    assert.equal(c.state.checks, 0);
    assert.equal(c.state.loads.length, 0);
    const meta = await read(join(c.data, "dataset.json"));
    const reference = await read(join(c.data, "reference.json"));
    meta.edges = 4;
    reference.dataset.edges = 4;
    meta.artifacts["reference.json"] = (await c.write(join(c.data, "reference.json"), reference)).sha256;
    await c.write(join(c.data, "dataset.json"), meta);
    await assert.rejects(importGraphDataset(c.data, { ...options, resume: true }), { code: "GRAPH_DATASET" });
    assert.equal(c.state.checks, 0);
});
test("an ambiguous Loader write stops the chain and resume never resubmits it", async (t) => {
    const c = await fixture(t),
        options = {
            out: join(c.dir, "failed"),
            loaderHome: c.home,
            graphUrl: c.graphUrl,
            traversalSource: c.traversalSource,
        };
    c.state.failLoad = true;
    await assert.rejects(importGraphDataset(c.data, options), { code: "GRAPH_IMPORT_UNKNOWN" });
    assert.equal(c.state.loads.length, 1);
    c.state.failLoad = false;
    await assert.rejects(importGraphDataset(c.data, { ...options, resume: true }), { code: "GRAPH_IMPORT_UNKNOWN" });
    assert.equal(c.state.loads.length, 1);
    const state = await read(join(options.out, "state.json"));
    assert.equal(state.completedShards, 0);
});
test("unknown schema result is not automatically replayed", async (t) => {
    const c = await fixture(t),
        options = {
            out: join(c.dir, "schema-failed"),
            loaderHome: c.home,
            graphUrl: c.graphUrl,
            traversalSource: c.traversalSource,
        };
    c.state.rejectSchema = true;
    await assert.rejects(importGraphDataset(c.data, options), { code: "GRAPH_TARGET" });
    await assert.rejects(importGraphDataset(c.data, { ...options, resume: true }), { code: "GRAPH_SCHEMA_UNKNOWN" });
    assert.equal(c.state.schemas, 1);
    assert.equal(c.state.loads.length, 0);
});
test("SF10 import uses its own dataset and Loader binding", async (t) => {
    const c = await fixture(t, 10);
    const result = await importGraphDataset(c.data, {
        out: join(c.dir, "import"),
        loaderHome: c.home,
        graphUrl: c.graphUrl,
        traversalSource: c.traversalSource,
        minimumEdgeCoverage: 1,
    });
    assert.equal(result.scaleFactor, 10);
    assert.equal(result.coverage.fullImport, true);
});
test(
    "public project -> import -> Go quick/standard -> per-case SVG and idempotent completed resume",
    { timeout: 360000 },
    async (t) => {
        const binary = process.env.HGBENCH_GO_BINARY;
        if (!binary) return t.skip("Set HGBENCH_GO_BINARY to a current native runner");
        const c = await fixture(t),
            profile = join(c.dir, "go.json"),
            project = join(c.dir, "project.json");
        const httpSettings = await read("examples/http-suite-functional.json");
        Object.assign(httpSettings, {
            endpoint: c.origin + "/gremlin",
            requestTimeoutMs: 1000,
            drainMs: 1000,
            maxRequestsPerPhase: 1000000,
        });
        await c.write(join(c.dir, "http-settings.json"), httpSettings);
        await generateHttpProfile(join(c.dir, "http-settings.json"), join(c.dir, "http.json"), false);
        await registerHttpGo(join(c.dir, "http.json"), binary, profile, {
            gomaxprocs: 2,
            cpuQuotaCores: 2,
            maxRssBytes: 1073741824,
        });
        await c.write(join(c.dir, "windows.json"), {
            concurrency: [1],
            exploreWarmupMs: 0,
            exploreDurationMs: 100,
            confirmWarmupMs: 0,
            confirmDurationMs: 100,
            confirmRepeats: 1,
        });
        const configuration = {
            schemaVersion: 1,
            kind: "graph-benchmark-project",
            dataset: c.data,
            profile,
            settings: "windows.json",
            graphUrl: c.graphUrl,
            traversalSource: c.traversalSource,
            loaderHome: c.home,
        };
        assert.equal(graphProjectSchema.parse(configuration).scaleFactor, 1);
        await c.write(project, configuration);
        const options = { out: join(c.dir, "project-run"), root: join(c.dir, "tasks"), budgetMs: 300000 };
        const dry = await runGraphProject(project, { ...options, dryRun: true });
        assert.equal(dry.scaleFactor, 1);
        assert.equal(c.state.loads.length, 0);
        const result = await runGraphProject(project, options);
        assert.equal(result.status, "completed");
        assert.equal(result.reports.length, 2);
        for (const report of result.reports) {
            assert.equal(report.status, "completed", JSON.stringify(report.failure));
            const data = await read(report.reportJson);
            assert.equal(data.track, "graph");
            assert.equal(data.cases.length, 5);
            assert.match(await readFile(report.reportHtml, "utf8"), /图查询 SF1/);
            assert.ok((await readdir(join(report.directory, "report/charts"))).some((n) => n.endsWith(".svg")));
        }
        const writes = c.state.loads.length,
            queries = c.state.queries;
        await runGraphProject(project, { ...options, resume: true });
        assert.equal(c.state.loads.length, writes);
        assert.equal(c.state.queries, queries);
    },
);
