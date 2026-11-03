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
import http from "node:http";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { mkdtemp, readFile, writeFile, rm, cp, readdir, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { buildView, barSvg, METRICS, writeBundle, renderBundle } from "../components/http/report-view.mjs";
import { createReport } from "../components/http/reports.mjs";
import { planHttp, httpProfileSchema } from "../dist/http.js";
const exec = promisify(execFile),
    read = async (p) => JSON.parse(await readFile(p, "utf8"));
const root = resolve(".");
const row = (id, value, count = 1000) => ({
    id,
    caseId: "two",
    loadId: "open",
    issues: [],
    scheduled: 1000,
    sent: 1000,
    correct: count,
    failed: 1000 - count,
    dropped: 0,
    late: 0,
    correctCompletedInWindowPerSecond: value,
    correctResponseMs: { count, p50: value, p95: value * 2, p99: value * 4 },
    validationMs: { count, p50: 0 },
    wireBodyBytes: { p50: 10 },
    decodedBodyBytes: { p50: 20 },
});
const input = (id, group, values, patch = {}) => ({
    id,
    group,
    analysis: {
        name: "test",
        rawManifestSha256: id,
        target: {},
        gates: {},
        profile: {
            cases: [{ id: "two", body: { gremlin: "1+1" }, checks: [{ pointer: "/result/data", equals: [2] }] }],
            loads: [{ id: "open", model: "open", ratePerSecond: 100 }],
            durationMs: 10000,
            warmupMs: 0,
            responseEncoding: "identity",
            repeats: values.length,
            ...patch,
        },
        runs: values.map((v, i) => row(`r${i}`, v)),
    },
});

test("multi-run aggregation retains observations, uses medians and gaps instead of pooled percentiles", async () => {
    const a = input("a", "base", [1, 9]),
        b = input("b", "new", [10]);
    b.analysis.runs[0].correctResponseMs.count = 999;
    const v = await buildView([a, b]);
    const groups = v.cells[0].groups;
    assert.equal(groups[0].metrics.p50.median, 5);
    assert.deepEqual(groups[0].metrics.p50.values, [1, 9]);
    assert.equal(groups[1].metrics.p99.median, null);
    const p50 = METRICS.find((x) => x.id === "p50"),
        p99 = METRICS.find((x) => x.id === "p99");
    assert.match(barSvg(v, v.cells[0], p50, "base"), /\+100%/);
    assert.match(barSvg(v, v.cells[0], p99), /N\/A/);
    assert.equal(v.performanceAccepted, false);
    assert.equal(v.inferenceAccepted, false);
    a.analysis.profile.repeats = 3;
    const partial = await buildView([a, b]);
    assert.equal(partial.cells[0].groups[0].missing, 1);
    assert(!barSvg(partial, partial.cells[0], p50, "base").includes("+100%"));
});

test("same names with different request/rate/encoding settings split, with full union and no fake deltas", async () => {
    for (const patch of [
        { responseEncoding: "gzip" },
        { loads: [{ id: "open", model: "open", ratePerSecond: 40 }] },
        { cases: [{ id: "two", body: { gremlin: "2+2" }, checks: [] }] },
    ]) {
        const v = await buildView([input("a", "base", [1]), input("b", "new", [2], patch)]);
        assert.equal(v.cells.length, 2);
        assert(v.cells.every((c) => c.alignment === "different-settings"));
        assert(v.cells.every((c) => c.groups.some((g) => g.runs.length === 0 && g.presentInProfile === false)));
        assert(!barSvg(v, v.cells[0], METRICS[1], "base").includes("+100%"));
    }
});

test("annotations never exclude rows; HTML/SVG/CSV labels are escaped and every chart is independently exported", async (t) => {
    const dir = await mkdtemp(join(tmpdir(), "hgbench-report-view-"));
    t.after(() => rm(dir, { recursive: true, force: true }));
    const a = input("a", "base", [1]),
        b = input("b", "new", [2]);
    a.label = '<script>alert("x")</script>';
    b.label = "=formula";
    const v = await buildView([a, b], {
        title: "<img onerror='x'>",
        events: [{ inputId: "a", message: "<script>bad</script>", source: "test" }],
    });
    assert.equal(v.cells[0].groups[0].runs.length, 1);
    assert.equal(v.cells[0].groups[0].runs[0].events.length, 1);
    const out = join(dir, "parents/report");
    const result = await writeBundle(out, v);
    const html = await readFile(join(out, "report.html"), "utf8");
    assert(!html.includes("<script>bad"));
    assert(html.includes("&lt;script&gt;bad&lt;/script&gt;"));
    assert(html.includes("XMLSerializer"));
    const files = [...html.matchAll(/href="(charts\/[^"]+\.svg)"/g)].map((m) => m[1]);
    assert.equal(files.length, result.charts);
    assert.equal(new Set(files).size, files.length);
    for (const f of files) {
        const raw = await readFile(join(out, f), "utf8");
        assert(raw.startsWith('<svg xmlns="http://www.w3.org/2000/svg"'));
        assert(!raw.includes("<script>"));
        assert(!raw.includes("NaN"));
    }
    await assert.rejects(writeBundle(out, v), (e) => e.code === "REPORT_OUTPUT_EXISTS");
    await writeFile(join(dir, "file"), "x");
    await assert.rejects(writeBundle(join(dir, "file/no"), v), (e) => e.code === "REPORT_OUTPUT_PATH");
    const enormous = { ...v, cells: Array(513).fill(v.cells[0]) };
    assert.throws(
        () => renderBundle(enormous),
        (e) => e.code === "REPORT_TOO_LARGE",
    );
});

test("comparison CLI verifies raw, reports diagnostics, resolves manifests and fails safely", async (t) => {
    const dir = await mkdtemp(join(tmpdir(), "hgbench-report-command-"));
    const server = http.createServer((req, res) => {
        req.resume();
        res.setHeader("Content-Type", "application/json");
        res.end('{"result":{"data":[2]}}');
    });
    await new Promise((done) => server.listen(0, "127.0.0.1", done));
    t.after(async () => {
        server.closeAllConnections();
        await new Promise((done) => server.close(done));
        await rm(dir, { recursive: true, force: true });
    });
    const profile = httpProfileSchema.parse({
        schemaVersion: 1,
        kind: "http-benchmark-profile",
        name: "report-fixture",
        endpoint: `http://127.0.0.1:${server.address().port}/gremlin`,
        target: { label: "fixture", build: "a", mode: "control", environment: "local tests" },
        serviceChecks: false,
        cases: [{ id: "two", body: { gremlin: "1+1" }, checks: [{ pointer: "/result/data", equals: [2] }] }],
        loads: [{ id: "open", model: "open", ratePerSecond: 20, maxInFlight: 2, maxStartDelayMs: 1000 }],
        repeats: 1,
        warmupMs: 100,
        durationMs: 200,
        requestTimeoutMs: 100,
        drainMs: 100,
        maxRequestsPerPhase: 20,
        maxResponseBytes: 4096,
        maxTraceBytes: 1048576,
    });
    for (const name of ["a", "b"]) {
        await writeFile(
            join(dir, `${name}.json`),
            JSON.stringify({ ...profile, target: { ...profile.target, build: name } }),
        );
        await planHttp(join(dir, `${name}.json`), join(dir, name));
        await exec(process.execPath, [join(root, "components/http/workflow.mjs"), "--plan", join(dir, name)], {
            timeout: 15000,
        });
    }
    const manifest = {
        groups: [
            { id: "base", runs: [{ id: "a", path: "a" }] },
            { id: "new", runs: [{ id: "b", path: "b" }] },
        ],
    };
    const mp = join(dir, "compare.json");
    await writeFile(mp, JSON.stringify(manifest));
    const dry = await createReport({ manifest: mp, dryRun: true });
    assert.equal(dry.cells.length, 1);
    assert.equal(dry.cells[0].groups[0].verified, 1);
    assert(!(await readdir(dir)).includes("report"));
    const out = join(dir, "reports/multi");
    const result = await createReport({ manifest: mp, out, windows: true, windowMs: 100 });
    assert(result.charts > 0);
    const data = await read(join(out, "report.json"));
    assert.equal(data.cells[0].groups[0].runs[0].diagnostics.phases[1].statusCounts[200], 4);
    assert(data.cells[0].groups[0].runs[0].windows.every((p) => p.windows.every((w) => w.p50 === null)));
    await assert.rejects(
        createReport({ input: [`a=${dir}/a`, `b=${dir}/a`], dryRun: true }),
        (e) => e.code === "COMPARE_DUPLICATE_RAW",
    );
    await cp(join(dir, "a/output"), join(dir, "bad"), { recursive: true });
    await writeFile(join(dir, "bad/traces/r0001.ndjson"), "corrupted\n");
    await assert.rejects(
        createReport({ input: [`a=${dir}/bad`, `b=${dir}/b`], dryRun: true }),
        (e) => e.code === "REPORT_EVIDENCE" && e.details.inputId === "input-1",
    );
    const single = join(dir, "reports/single");
    await exec(process.execPath, [join(root, "dist/cli.js"), "http", "report", join(dir, "a"), "--out", single]);
    const duplicate = await exec(process.execPath, [
        join(root, "dist/cli.js"),
        "http",
        "report",
        join(dir, "a"),
        "--out",
        single,
    ]).catch((e) => e);
    assert.equal(JSON.parse(duplicate.stdout).error.code, "REPORT_OUTPUT_EXISTS");
    const before = (await stat(join(dir, "a/output/raw.lock.json"))).mtimeMs;
    await exec(process.execPath, [join(root, "dist/cli.js"), "http", "compare", "--manifest", mp, "--dry-run"]);
    assert.equal((await stat(join(dir, "a/output/raw.lock.json"))).mtimeMs, before);
    const version = JSON.parse((await exec(process.execPath, [join(root, "dist/cli.js"), "version"])).stdout).data;
    assert.match(version.buildId, /^sha256:[0-9a-f]{64}$/);
    assert.equal(version.reportVersion, 2);
    assert.match(
        (await exec(process.execPath, [join(root, "dist/cli.js"), "http", "compare", "--help"])).stdout,
        /no\s+new tests/,
    );
});
