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
import { mkdtemp, writeFile, readFile, stat, appendFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import http from "node:http";
import { runHttpThroughput } from "../dist/http-throughput.js";
import { generateHttpProfile } from "../dist/http-suite.js";
import { registerHttpGo } from "../dist/http-go.js";
import {
    throughputPresets,
    throughputSettingsSchema,
    selectThroughputPoints,
    httpPointHealth,
} from "../dist/throughput.js";
import { inspect, expectedData } from "../benchmarks/http-serving-v0.1/reference.mjs";
import { reportThroughput } from "../components/http/throughput-report.mjs";

test("shared selector preserves bounds and distinguishes plateau from a growing curve or invalid points", () => {
    const s = throughputPresets.standard;
    assert.throws(() => throughputSettingsSchema.parse({ ...s, concurrency: [4, 1] }));
    assert.deepEqual(selectThroughputPoints([], s).confirmation, []);
    assert.equal(
        selectThroughputPoints(
            [
                { concurrency: 1, goodput: 10, eligible: true },
                { concurrency: 2, goodput: 20, eligible: true },
            ],
            s,
        ).plateauObserved,
        false,
    );
    const r = selectThroughputPoints(
        [
            { concurrency: 1, goodput: 10, eligible: true },
            { concurrency: 2, goodput: 100, eligible: true },
            { concurrency: 4, goodput: 103, eligible: true },
            { concurrency: 8, goodput: 1000, eligible: false },
        ],
        s,
    );
    assert.equal(r.candidate, 2);
    assert.deepEqual(r.confirmation, [2, 4]);
    assert.equal(r.plateauObserved, true);
    assert.equal(
        httpPointHealth({ correct: 9, failed: 0, dropped: 0, issues: ["measurement:request-budget-exhausted"] })
            .eligible,
        false,
    );
    assert.equal(
        httpPointHealth({ correct: 9, failed: 0, dropped: 0, issues: ["measurement:go-resource-isolation-unverified"] })
            .unknown.length,
        1,
    );
});

test(
    "real HTTP standard and quick share reverified light calibration, confirm new windows, preserve partial and reject damaged evidence",
    { timeout: 480000 },
    async (t) => {
        const dir = await mkdtemp(join(tmpdir(), "hgbench-throughput-"));
        let passed = false;
        t.after(() =>
            passed ? rm(dir, { recursive: true, force: true }) : console.error("Kept failed evidence:", dir),
        );
        const suite = await inspect();
        const bodies = new Map(
            suite.questions.map((q) => [
                JSON.stringify(q.body),
                Buffer.from(JSON.stringify({ status: { code: 200 }, result: { data: expectedData(q) } })),
            ]),
        );
        let requests = 0;
        const server = http.createServer((req, res) => {
            const chunks = [];
            req.on("data", (b) => chunks.push(b));
            req.on("end", () => {
                requests++;
                const body = bodies.get(Buffer.concat(chunks).toString());
                setTimeout(() => {
                    if (!body) {
                        res.statusCode = 400;
                        res.end();
                    } else {
                        res.setHeader("content-type", "application/json");
                        res.end(body);
                    }
                }, 5);
            });
        });
        await new Promise((r) => server.listen(0, "127.0.0.1", r));
        t.after(
            () =>
                new Promise((r) => {
                    server.closeAllConnections();
                    server.close(r);
                }),
        );
        const settings = JSON.parse(await readFile("examples/http-suite-functional.json"));
        settings.endpoint = `http://127.0.0.1:${server.address().port}/gremlin`;
        const settingsFile = join(dir, "suite.json"),
            profile = join(dir, "profile.json");
        await writeFile(settingsFile, JSON.stringify(settings));
        await generateHttpProfile(settingsFile, profile, false);
        let input = profile;
        if (process.env.HGBENCH_GO_BINARY) {
            input = join(dir, "go.json");
            await registerHttpGo(profile, process.env.HGBENCH_GO_BINARY, input, {
                gomaxprocs: 2,
                cpuQuotaCores: 2,
                maxRssBytes: 1073741824,
            });
        }
        const override = join(dir, "windows.json");
        await writeFile(
            override,
            JSON.stringify({
                concurrency: [1, 2],
                exploreWarmupMs: 0,
                exploreDurationMs: 100,
                confirmWarmupMs: 0,
                confirmDurationMs: 100,
                confirmRepeats: 2,
            }),
        );
        const root = join(dir, "tasks"),
            cache = process.env.HGBENCH_LIGHT_CACHE ?? join(dir, "cache");
        const opts = {
            out: join(dir, "standard"),
            root,
            cache,
            preset: "standard",
            settings: override,
            budgetMs: 420000,
        };
        const dry = await runHttpThroughput(input, { ...opts, dryRun: true });
        assert.equal(dry.dispatched, false);
        assert.equal(dry.cases.length, 11);
        assert.equal(requests, 0);
        await assert.rejects(stat(opts.out), { code: "ENOENT" });
        if (!process.env.HGBENCH_LIGHT_CACHE) await assert.rejects(stat(cache), { code: "ENOENT" });
        const standard = await runHttpThroughput(input, opts);
        assert.equal(standard.status, "completed", JSON.stringify(standard.failure));
        assert(["passed", "limited"].includes(standard.calibration.status));
        assert.equal(standard.calibration.records.length, 2);
        assert.equal(standard.stages.filter((s) => s.stage === "exploration").length, 11);
        assert.equal(standard.stages.filter((s) => s.stage === "confirmation").length, 11);
        const report = JSON.parse(await readFile(standard.reportJson));
        assert.equal(report.cases.length, 11);
        assert.equal(report.performanceAccepted, false);
        assert(report.cases.every((c) => c.confirmed.length > 0));
        const html = await readFile(standard.reportHtml, "utf8");
        assert.match(html, /Export SVG/);
        assert.match(html, /2-window confirmation observation/);
        assert.match(html, /repeats share the same service JVM/);
        assert.doesNotMatch(html, /independently confirmed observation/);
        await assert.rejects(runHttpThroughput(input, opts), (e) => e.code === "THROUGHPUT_EXISTS");
        const quick = await runHttpThroughput(input, {
            ...opts,
            preset: "quick",
            out: join(dir, "quick"),
            budgetMs: 120000,
        });
        assert.equal(quick.status, "completed", JSON.stringify(quick.failure));
        assert.equal(quick.calibration.reused, true);
        assert.equal(quick.cases.length, 3);
        assert.equal(
            quick.stages.some((s) => s.stage === "calibration"),
            false,
        );
        const before = requests;
        const limited = await runHttpThroughput(input, { ...opts, out: join(dir, "deadline"), budgetMs: 1000 });
        assert.equal(limited.status, "partial");
        assert(limited.incomplete.length);
        assert.equal(requests, before);
        await reportThroughput(opts.out, join(dir, "rebuilt"));
        assert.deepEqual(JSON.parse(await readFile(join(dir, "rebuilt/report.json"))), report);
        const raw = standard.stages.find((s) => s.stage === "exploration").rawDirectory;
        await appendFile(join(raw, "traces/r0001.ndjson"), "{}\n");
        const corrupt = await reportThroughput(opts.out, join(dir, "corrupt"));
        assert.equal(corrupt.status, "partial");
        assert(JSON.parse(await readFile(corrupt.reportJson)).evidenceIssues.some((e) => /hash|size/i.test(e.reason)));
        passed = true;
    },
);

test("HTTP standard default budget is 90 minutes and explicit/custom/quick/calibration budgets retain their scopes", async (t) => {
    const dir = await mkdtemp(join(tmpdir(), "hgbench-budget-"));
    t.after(() => rm(dir, { recursive: true, force: true }));
    const settings = JSON.parse(await readFile("examples/http-suite-functional.json"));
    const suiteFile = join(dir, "suite.json"),
        profile = join(dir, "profile.json"),
        override = join(dir, "override.json");
    await writeFile(suiteFile, JSON.stringify(settings));
    await writeFile(override, "{}");
    await generateHttpProfile(suiteFile, profile, false);
    const opts = { root: join(dir, "tasks"), out: join(dir, "dry"), preset: "standard", dryRun: true };
    const standard = await runHttpThroughput(profile, opts);
    assert.equal(standard.fixedWindowMs, 3300000);
    assert.equal(standard.defaultBudgetMs, 5400000);
    assert.equal(standard.budgetMs, 5400000);
    assert.equal((await runHttpThroughput(profile, { ...opts, budgetMs: 10000 })).budgetMs, 10000);
    const quick = await runHttpThroughput(profile, { ...opts, preset: "quick" });
    assert.equal(quick.budgetMs, quick.fixedWindowMs + 180000 + 3 * 30000);
    const custom = await runHttpThroughput(profile, { ...opts, settings: override });
    assert.equal(custom.budgetMs, custom.fixedWindowMs + 180000 + 11 * 30000);
    const calibration = await runHttpThroughput(profile, { ...opts, calibrationOnly: true });
    assert.equal(calibration.budgetMs, 80000 + 180000 + 11 * 30000);
    await assert.rejects(stat(opts.out), { code: "ENOENT" });
    await assert.rejects(stat(opts.root), { code: "ENOENT" });
});
