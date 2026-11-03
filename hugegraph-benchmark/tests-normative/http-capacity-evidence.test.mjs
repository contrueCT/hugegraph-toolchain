/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, mkdir, writeFile, readdir, symlink, rm, cp, readFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { tmpdir } from "node:os";
import {
    validateCapacityReportLimits,
    validateCapacityExecutionSet,
    capacityArm,
    summarizeCapacity,
    markCapacityAssociations,
    assertCapacityOutputSeparate,
    reportCapacity,
    runCapacityWorker,
} from "../components/http/capacity-evidence.mjs";
import { capacityHtml } from "../components/http/capacity-report.mjs";
const sha = "a".repeat(64),
    MiB = 1024 ** 2;
function fixturePlan(modes = ["forward"], rates = [20]) {
    const caseIds = Array.from({ length: 11 }, (_, i) => `case-${i}`),
        points = [];
    for (const caseId of caseIds)
        for (const mode of modes)
            for (const ratePerSecond of rates) {
                const index = points.length;
                points.push({
                    id: `p${index}`,
                    index,
                    caseId,
                    mode,
                    ratePerSecond,
                    expectedArms: 4,
                    comparisonPlanDigest: sha,
                });
            }
    return {
        schemaVersion: 1,
        kind: "http-capacity-plan",
        phase: "exploration",
        planDigest: sha,
        policySha256: sha,
        suite: { caseIds },
        modes,
        expectedArms: points.length * 4,
        points,
        policy: {
            analysis: { latency: { minimumObservations: 100 }, goodput: { minimumObservations: 100 } },
            tailPolicy: { p99MinimumObservations: 1000 },
            slo: null,
        },
    };
}
function row(point, index, value = 20, count = 100) {
    return {
        runId: `r${index}`,
        arm: index % 2 ? "B" : "A",
        blockId: `b${index >> 1}`,
        expectedMode: point.mode,
        status: "ok",
        issues: [],
        rawVerified: true,
        runtimeArtifactsConsistent: true,
        driverVerified: true,
        resourcesVerified: true,
        rawEvidenceSha256: sha,
        httpSummary: {
            rawManifestSha256: sha,
            gates: { sampleAccounting: "passed", correctness: "passed" },
            verdict: "passed",
            runs: [
                {
                    caseId: point.caseId,
                    model: "open",
                    repeat: 0,
                    ratePerSecond: point.ratePerSecond,
                    issues: [],
                    correctCompletedInWindowPerSecond: value,
                    durationMs: 10000,
                    elapsedWithDrainMs: 10001,
                    scheduled: 200,
                    sent: Math.round(value * 10),
                    completed: Math.round(value * 10),
                    correct: Math.round(value * 10),
                    failed: 0,
                    dropped: 0,
                    late: 0,
                    counts: { correct: Math.round(value * 10) },
                    correctEndToEndMs: { count, p50: 1, p95: 2, p99: 3 },
                    offeredPerSecond: 20,
                    admittedPerSecond: 20,
                    driver: { fixture: true },
                },
            ],
        },
    };
}
function record(point, values = [16, 18, 18, 20]) {
    return {
        id: point.id,
        status: "reverified",
        issues: [],
        executionDirectory: `children/${point.id}/execution`,
        result: { verification: { valid: true, rows: values.map((v, i) => row(point, i, v)) } },
    };
}

test("capacity report strict resource and execution mapping limits", () => {
    assert.deepEqual(validateCapacityReportLimits(1000, MiB), { timeoutMs: 1000, maxBytes: MiB });
    for (const value of [0, 999, 86400001, NaN, 1.5])
        assert.throws(() => validateCapacityReportLimits(value, MiB), /TIME_LIMIT/);
    for (const value of [0, MiB - 1, 8 * 1024 ** 3 + 1, NaN])
        assert.throws(() => validateCapacityReportLimits(1000, value), /BYTE_LIMIT/);
    const p = fixturePlan(),
        manifest = {
            schemaVersion: 1,
            kind: "http-capacity-execution-set",
            planDigest: sha,
            points: [{ id: "p0", executionDirectory: "children/p0/execution" }],
        };
    assert.deepEqual(validateCapacityExecutionSet(manifest, p), manifest);
    for (const bad of [
        { ...manifest, quality: true },
        { ...manifest, planDigest: "b".repeat(64) },
        { ...manifest, points: [...manifest.points, ...manifest.points] },
        { ...manifest, points: [{ id: "not-planned", executionDirectory: "x" }] },
        { ...manifest, points: [{ ...manifest.points[0], confirmed: true }] },
    ])
        assert.throws(() => validateCapacityExecutionSet(bad, p));
});
test("capacity complete frozen denominator survives missing raw and both modes", () => {
    const p = fixturePlan(["forward", "embedded"], [10, 20]),
        s = summarizeCapacity(p, []);
    assert.equal(s.plannedPoints, 44);
    assert.equal(s.expectedArms, 176);
    assert.equal(s.curves.length, 22);
    assert.equal(s.validPoints, 0);
    assert(s.points.every((point) => point.arms.length === 4 && !point.measurementValid));
    assert(
        s.curves.every((curve) => curve.highestObserved.status === "unavailable" && curve.confirmedHighest === null),
    );
    assert.equal(s.performanceAccepted, false);
    assert.equal(s.evidenceValid, false);
});
test("capacity descriptive arm median and quantiles do not pool or establish confirmation", () => {
    const p = fixturePlan(),
        r = record(p.points[0]),
        s = summarizeCapacity(p, [r]),
        v = s.points[0];
    assert.equal(v.measurementValid, true);
    assert.equal(v.throughput.median, 18);
    assert.equal(v.throughput.minimum, 16);
    assert.equal(v.throughput.maximum, 20);
    assert.equal(v.latency.p50.value, 1);
    assert.equal(v.latency.p99.value, null);
    assert.equal(v.latency.p99.status, "unknown");
    assert(
        v.arms.every(
            (arm) => arm.latency.p99.observedNearestRank === undefined && arm.latency.observedNearestRank.p99 === 3,
        ),
    );
    assert.equal(s.curves[0].highestObserved.status, "unconfirmed");
    assert.equal(s.curves[0].confirmedHighest, null);
    assert.equal(v.capacityQualified, false);
    const allLarge = structuredClone(r);
    allLarge.result.verification.rows.forEach((v) => (v.httpSummary.runs[0].correctEndToEndMs.count = 1000));
    assert.equal(summarizeCapacity(p, [allLarge]).points[0].latency.p99.value, 3);
});
test("capacity raw values and overload diagnostics survive invalid quality without becoming candidates", () => {
    const p = fixturePlan(),
        r = record(p.points[0]);
    r.result.verification.valid = false;
    r.result.verification.rows[0].status = "invalid";
    r.result.verification.rows[0].issues.push("measurement:driver-pressure:dropped");
    const s = summarizeCapacity(p, [r]),
        point = s.points[0];
    assert.equal(point.measurementValid, false);
    assert.equal(point.throughput.median, null);
    assert.equal(point.arms[0].correctCompletedInWindowPerSecond, 16);
    assert.equal(point.diagnosticObservedThroughput.median, 18);
    assert.equal(point.diagnosticObservedThroughput.qualification, false);
    assert.equal(s.curves[0].highestObserved.status, "unavailable");
    const bad = structuredClone(r);
    bad.result.verification.rows[0].httpSummary.gates.correctness = "failed";
    assert.equal(summarizeCapacity(p, [bad]).points[0].diagnosticObservedThroughput.median, null);
    r.result.verification.rows.pop();
    assert.equal(summarizeCapacity(p, [r]).points[0].diagnosticObservedThroughput.median, null);
});
test("capacity rejects incomplete source, wrong workload, mode and in-window count without erasing values", () => {
    const p = fixturePlan(),
        base = row(p.points[0], 0);
    for (const key of ["rawVerified", "runtimeArtifactsConsistent", "driverVerified", "resourcesVerified"]) {
        const r = structuredClone(base);
        r[key] = false;
        assert.equal(capacityArm(p.points[0], r, p.policy).measurementValid, false);
    }
    for (const mutate of [
        (r) => (r.httpSummary.runs[0].caseId = "wrong"),
        (r) => (r.httpSummary.runs[0].ratePerSecond = 10),
        (r) => (r.expectedMode = "embedded"),
        (r) => r.httpSummary.runs.push(r.httpSummary.runs[0]),
        (r) => (r.httpSummary.runs[0].correctCompletedInWindowPerSecond = 9.9),
    ]) {
        const r = structuredClone(base);
        mutate(r);
        assert.equal(capacityArm(p.points[0], r, p.policy).measurementValid, false);
    }
    const r = structuredClone(base);
    r.httpSummary.runs[0].correctEndToEndMs.count = 99;
    assert.equal(capacityArm(p.points[0], r, p.policy).latency.p95.value, null);
});
test("capacity SLO retains frozen threshold and unknown rate denominator", () => {
    const p = fixturePlan();
    p.policy.slo = { endToEndQuantile: "p50", maxLatencyMs: 0.5, maxErrorRate: 0.01, maxTimeoutRate: 0.01 };
    const arm = capacityArm(p.points[0], row(p.points[0], 0), p.policy);
    assert.equal(arm.slo.latency, "above-threshold");
    assert.equal(arm.slo.errorRate, "unknown");
    assert.equal(arm.slo.timeoutRate, "unknown");
    assert.equal(arm.slo.capacity, "unknown");
    assert.deepEqual(arm.slo.policy, p.policy.slo);
    assert.equal(summarizeCapacity(p, [record(p.points[0])]).curves[0].sloCapacity.status, "unknown");
});
test("capacity highest observed chooses complete arm median and lower offered rate on ties", () => {
    const p = fixturePlan(["forward"], [10, 20]),
        a = record(p.points[0], [9, 10, 10, 11]),
        b = record(p.points[1], [8, 10, 10, 12]);
    // Both points satisfy the same predeclared sample requirement using longer windows.
    for (const r of [a, b]) for (const arm of r.result.verification.rows) arm.httpSummary.runs[0].durationMs = 20000;
    const s = summarizeCapacity(p, [a, b]);
    assert.equal(s.curves[0].highestObserved.pointId, a.id);
    assert.equal(s.curves[0].highestObserved.correctRequestsPerSecond, 10);
});
test("capacity associations reject reused identities and same-boot overlap or reversed order", () => {
    const mk = (start, end, boot = "boot") => ({ issues: [], window: { start, end, boot } });
    let rows = [mk("1", "5"), mk("4", "8")];
    markCapacityAssociations(rows);
    assert(rows.every((r) => r.issues.includes("CAPACITY_EXECUTION_WINDOWS_OVERLAP")));
    rows = [mk("8", "9"), mk("1", "5")];
    markCapacityAssociations(rows);
    assert(rows.every((r) => r.issues.includes("CAPACITY_EXECUTION_ORDER_DIFFER")));
    rows = [mk("1", "5"), mk("5", "8"), mk("1", "2", "other")];
    markCapacityAssociations(rows);
    assert(rows.every((r) => !r.issues.length));
    rows = [
        { issues: [], actualDirectory: "/a", executionId: "same", runIds: ["r"] },
        { issues: [], actualDirectory: "/a", executionId: "same", runIds: ["r"] },
    ];
    markCapacityAssociations(rows);
    assert(
        rows.every((r) => r.issues.includes("CAPACITY_EXECUTION_REUSED") && r.issues.includes("CAPACITY_RUN_REUSED")),
    );
});
test("capacity report rejects realpath parent alias and all input overlap before any output mutation", async (t) => {
    const root = await mkdtemp(join(tmpdir(), "hgbench-capacity-overlap-"));
    t.after(() => rm(root, { recursive: true, force: true }));
    const plan = join(root, "plan"),
        input = join(root, "execution-set.json"),
        p = fixturePlan();
    await mkdir(plan);
    await writeFile(join(plan, "capacity-plan.json"), JSON.stringify(p));
    await writeFile(
        input,
        JSON.stringify({ schemaVersion: 1, kind: "http-capacity-execution-set", planDigest: sha, points: [] }),
    );
    await symlink(plan, join(root, "alias"));
    await assert.rejects(
        reportCapacity({
            planDirectory: plan,
            inputFile: input,
            outputDirectory: join(root, "alias", "new"),
            timeoutMs: 1000,
            maxBytes: MiB,
        }),
        /OUTPUT_OVERLAP/,
    );
    assert.deepEqual(await readdir(plan), ["capacity-plan.json"]);
    assert.throws(() => assertCapacityOutputSeparate("/a", ["/a/b"]), /OUTPUT_OVERLAP/);
    assert.throws(() => assertCapacityOutputSeparate("/a/b", ["/a"]), /OUTPUT_OVERLAP/);
    assertCapacityOutputSeparate("/a/b", ["/a/bb"]);
});
test("capacity whole-plan worker obeys cancellation and reaps its process", async () => {
    const controller = new AbortController();
    controller.abort();
    const started = Date.now();
    const result = await runCapacityWorker(
        ["__verify-plan", "--plan", "/not-an-existing-capacity-plan", "--out", "/never-created"],
        1000,
        controller.signal,
    );
    assert.equal(result.failure, "CAPACITY_REPORT_CANCELLED");
    assert(Date.now() - started < 5000);
});
test("capacity HTML escapes evidence and displays diagnostic hollow points without false capacity claims", () => {
    const p = fixturePlan(),
        r = record(p.points[0]);
    r.result.verification.rows[0].issues.push("<script>alert(1)</script>");
    const summary = summarizeCapacity(p, [r]),
        html = capacityHtml(summary);
    assert(!html.includes("<script>alert(1)</script>"));
    assert(html.includes("&lt;script&gt;alert(1)&lt;/script&gt;"));
    assert.match(html, /fill="white"/);
    assert.match(html, /诊断观察/);
    assert.match(html, /report.json/);
    assert.match(html, /text-anchor="end"/);
});

test("capacity preserves child global failures and original arm reasons in the summary", () => {
    const plan = fixturePlan(),
        source = record(plan.points[0]);
    source.result.verification.valid = false;
    source.result.verification.globalIssues = ["EXECUTION_FAILED"];
    source.result.verification.rows[0].status = "invalid";
    source.result.verification.rows[0].issues.push("SOURCE_IDENTITY_DIFFER");
    const summary = summarizeCapacity(plan, [source]);
    assert(summary.points[0].issues.includes("EXECUTION_FAILED"));
    assert(summary.points[0].issues.includes("CAPACITY_ARM_MEASUREMENT_INVALID"));
    assert(summary.points[0].arms[0].issues.includes("SOURCE_IDENTITY_DIFFER"));
    assert.equal(summary.curves[0].highestObserved.status, "unavailable");
});

test("capacity plan worker deadline remains distinguishable from invalid plan and cancellation", async () => {
    const started = Date.now();
    const result = await runCapacityWorker(
        ["__verify-plan", "--plan", "/not-an-existing-capacity-plan", "--out", "/never-created"],
        1,
        new AbortController().signal,
    );
    assert.equal(result.failure, "CAPACITY_REPORT_WORKER_TIMEOUT");
    assert(Date.now() - started < 5000);
});

async function isolatedReportModule(t) {
    const root = await mkdtemp(join(tmpdir(), "hgbench-capacity-guard-"));
    t.after(() => rm(root, { recursive: true, force: true }));
    const packageRoot = join(root, "package"),
        dependencies = join(root, "dependencies");
    await mkdir(packageRoot);
    await mkdir(dependencies);
    for (const directory of ["components", "dist"])
        await cp(resolve(directory), join(packageRoot, directory), { recursive: true });
    for (const directory of ["src", "release", "benchmarks", "presets"]) await mkdir(join(packageRoot, directory));
    await cp(resolve("package.json"), join(packageRoot, "package.json"));
    for (const name of ["zod", "commander"]) await symlink(resolve("node_modules", name), join(dependencies, name));
    await symlink(dependencies, join(packageRoot, "node_modules"));
    const entry = join(packageRoot, "components/http/capacity-evidence.mjs");
    // A private-function test re-export changes no production logic or public command.
    // It exists only in this disposable physical copy and is not runtime evidence.
    await writeFile(entry, (await readFile(entry, "utf8")) + "\nexport { implementation as fixtureInventory };\n");
    return { root, packageRoot, dependencies, module: await import(pathToFileURL(entry).href) };
}

test(
    "capacity canonical protected dependencies reject external target through a root symlink",
    { timeout: 30000 },
    async (t) => {
        const fixture = await isolatedReportModule(t),
            planRoot = join(fixture.root, "plan"),
            input = join(fixture.root, "execution-set.json");
        await mkdir(planRoot);
        const plan = fixturePlan();
        await writeFile(join(planRoot, "capacity-plan.json"), JSON.stringify(plan));
        await writeFile(
            input,
            JSON.stringify({ schemaVersion: 1, kind: "http-capacity-execution-set", planDigest: sha, points: [] }),
        );
        await assert.rejects(
            fixture.module.reportCapacity({
                planDirectory: planRoot,
                inputFile: input,
                outputDirectory: join(fixture.dependencies, "new-report"),
                timeoutMs: 1000,
                maxBytes: MiB,
            }),
            /CAPACITY_REPORT_OUTPUT_OVERLAP/,
        );
        assert.deepEqual((await readdir(fixture.dependencies)).sort(), ["commander", "zod"]);
    },
);

test(
    "capacity implementation enforces 4096 total files including package JSON at each insertion",
    { timeout: 30000 },
    async (t) => {
        const fixture = await isolatedReportModule(t),
            baseline = await fixture.module.fixtureInventory();
        assert(baseline.files.length < 4096);
        const additional = 4096 - baseline.files.length;
        for (let index = 0; index < additional; index += 32)
            await Promise.all(
                Array.from({ length: Math.min(32, additional - index) }, (_, offset) =>
                    writeFile(
                        join(fixture.packageRoot, "presets", `empty-${String(index + offset).padStart(5, "0")}`),
                        "",
                    ),
                ),
            );
        const exact = await fixture.module.fixtureInventory();
        assert.equal(exact.files.length, 4096);
        assert(exact.files.some((entry) => entry.path === "package.json"));
        await writeFile(join(fixture.packageRoot, "presets", "one-file-over-limit"), "");
        await assert.rejects(fixture.module.fixtureInventory(), /CAPACITY_REPORT_SOURCE_BUDGET/);
    },
);

test("capacity report arm details occupy full width with collapsed JSON and no empty-issue suffix", () => {
    const plan = fixturePlan(),
        summary = summarizeCapacity(plan, [record(plan.points[0])]),
        html = capacityHtml(summary);
    assert.match(html, /<tr class="arm-detail-row"><td colspan="6"><details/);
    assert.match(html, /<details><summary>计数、driver 与证据 JSON<\/summary><pre>/);
    assert(!html.includes("完整有效观察无"));
    assert(!html.includes("ok无"));
    assert.equal((html.match(/尚无可绘制数值/g) ?? []).length, 10);
    assert.equal((html.match(/个臂的原始观察/g) ?? []).length, 11);
});

test("single observation curves require the versioned observation proof and never imply repeated confirmation", () => {
    const plan = fixturePlan();
    plan.schemaVersion = 2;
    plan.policy.schemaVersion = 2;
    plan.policy.exploration = { mode: "single-run", runs: 1 };
    plan.expectedArms = plan.points.length;
    plan.points.forEach((point) => (point.expectedArms = 1));
    const observation = record(plan.points[0], [20]);
    Object.assign(observation.result.verification, {
        kind: "http-observation-evidence-verification",
        design: "single-observation",
    });
    observation.result.verification.rows[0].arm = "O";
    const summary = summarizeCapacity(plan, [observation]);
    assert.equal(summary.algorithm, "capacity-curves-v2-single-run");
    assert.equal(summary.expectedArms, 11);
    assert.equal(summary.points[0].measurementValid, true);
    assert.equal(summary.points[0].throughput.median, 20);
    assert.match(summary.points[0].throughput.aggregation, /single-observation/);
    assert.equal(summary.points[0].latency.p99.status, "unknown");
    assert.equal(summary.curves[0].highestObserved.status, "unconfirmed");
    assert.equal(summary.performanceAccepted, false);
    assert.match(capacityHtml(summary), /单次观察正确RPS/);
    const wrong = structuredClone(observation);
    delete wrong.result.verification.design;
    assert.equal(summarizeCapacity(plan, [wrong]).points[0].measurementValid, false);
    const fakeB = structuredClone(observation);
    fakeB.result.verification.rows.push({ ...fakeB.result.verification.rows[0], arm: "B", runId: "fake-b" });
    assert.equal(summarizeCapacity(plan, [fakeB]).points[0].measurementValid, false);
});
