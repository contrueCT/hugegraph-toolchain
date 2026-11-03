/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
import test from "node:test";
import assert from "node:assert/strict";
import { readFile, mkdtemp, mkdir, writeFile, rm, readdir, lstat, symlink, access, rename } from "node:fs/promises";
import { join, resolve, dirname } from "node:path";
import { tmpdir } from "node:os";
import { createHash } from "node:crypto";
import {
    selectCapacityConfirmation,
    capacityConfirmationComparisonRequest,
    capacityConfirmationResourceChanges,
    planHttpCapacityConfirmation,
    verifyHttpCapacityConfirmationPlan,
} from "../dist/http-capacity-confirmation.js";
import { httpCapacityConfirmationRequestSchema } from "../dist/http-capacity-confirmation-contracts.js";
import { previewCapacityResources } from "../dist/http-capacity.js";
import { missingExploration } from "./http-capacity-confirmation.fixture.mjs";
import { hash } from "../dist/io.js";
import { buildComparisonPlan } from "../components/http/comparison-plan.mjs";

const MiB = 1024 ** 2;
const middle = (values) => {
    const sorted = [...values].sort((a, b) => a - b);
    return (sorted[1] + sorted[2]) / 2;
};

// Synthetic arithmetic/selection protocol fixtures. They are not independently
// verified reports and never enter the public planner's evidence-verification path.
function fixture(modes = ["forward"], rates = [10, 20, 40, 80]) {
    const cases = Array.from({ length: 11 }, (_, i) => "synthetic-case-" + i);
    const plan = {
        kind: "http-capacity-plan",
        phase: "exploration",
        planDigest: "a".repeat(64),
        modes,
        suite: { caseIds: cases },
        policy: { rates, slo: null },
        points: cases.flatMap((caseId) =>
            modes.flatMap((mode) =>
                rates.map((ratePerSecond, rateIndex) => ({
                    id: `${caseId}-${mode}-${rateIndex}`,
                    caseId,
                    mode,
                    rateIndex,
                    ratePerSecond,
                    expectedArms: 4,
                    comparisonPlanDigest: "b".repeat(64),
                })),
            ),
        ),
    };
    const report = {
        schemaVersion: 1,
        kind: "http-capacity-report",
        algorithm: "capacity-curves-v1",
        phase: "exploration",
        planDigest: plan.planDigest,
        points: plan.points.map((point) => ({
            ...point,
            measurementValid: true,
            issues: [],
            throughput: { median: point.ratePerSecond },
            arms: ["b1-A", "b1-B", "b2-A", "b2-B"].map((arm) => ({
                runId: point.id + ":" + arm,
                status: "ok",
                measurementValid: true,
                evidence: { raw: true, environment: true, driverLifecycle: true, resources: true },
                correctCompletedInWindowPerSecond: point.ratePerSecond,
                driverPressureObserved: false,
                slo: { status: "not-requested" },
                issues: [],
            })),
        })),
    };
    const row = (rate, mode = modes[0], caseId = cases[0]) =>
        report.points.find((p) => p.caseId === caseId && p.mode === mode && p.ratePerSecond === rate);
    const values = (rate, numbers, mode, caseId) => {
        const point = row(rate, mode, caseId);
        point.arms.forEach((arm, i) => (arm.correctCompletedInWindowPerSecond = numbers[i]));
        point.throughput.median = middle(numbers);
    };
    return { plan, report, cases, row, values };
}
const select = (f) => selectCapacityConfirmation(f.plan, f.report, "confirm-local");

test("confirmation ranks all independent arm medians and keeps every question", () => {
    const f = fixture(["forward"], [10, 20, 40]);
    f.values(10, [9, 9, 9, 100]);
    f.values(20, [15, 15, 15, 15]);
    f.values(40, [14, 14, 14, 14]);
    const result = select(f),
        row = result.selections[0];
    assert.equal(row.candidatePointId, f.row(20).id);
    assert.equal(row.candidateCorrectRequestsPerSecond, 15);
    assert.equal(row.neighborhood.filter((p) => p.status === "selected").length, 3);
    assert.equal(result.selections.length, 11);
    assert.equal(result.performanceAccepted, false);
    assert(result.comparisons.every((point) => point.expectedArms === 8 && point.comparison === "aa"));
    assert.deepEqual(select(f), result);
});

test("ties select the lower offered rate and only immediate registered neighbors", () => {
    const f = fixture();
    for (const rate of f.plan.policy.rates) f.values(rate, [10, 10, 10, 10]);
    const result = select(f),
        row = result.selections[0];
    assert.equal(row.candidatePointId, f.row(10).id);
    assert.equal(row.neighborhood[0].status, "outside-grid");
    assert.deepEqual(
        row.neighborhood.filter((p) => p.status === "selected").map((p) => p.ratePerSecond),
        [10, 20],
    );
    assert(!result.comparisons.filter((p) => p.caseId === f.cases[0]).some((p) => p.ratePerSecond === 40));
});

test("unsafe or unexecuted adjacent points stay explicit rather than being skipped over", () => {
    const f = fixture();
    f.values(40, [40, 40, 40, 40]);
    f.row(20).arms[0].driverPressureObserved = true;
    f.row(80).measurementValid = false;
    f.row(80).issues = ["CAPACITY_POINT_NOT_STARTED"];
    f.row(80).arms.forEach((arm) => {
        arm.measurementValid = false;
        arm.status = "missing";
    });
    const row = select(f).selections[0];
    assert.equal(row.candidatePointId, f.row(40).id);
    assert.equal(row.neighborhood[0].status, "unsafe-or-unavailable");
    assert.equal(row.neighborhood[2].status, "unsafe-or-unavailable");
    assert.deepEqual(
        row.neighborhood.filter((p) => p.status === "selected").map((p) => p.ratePerSecond),
        [40],
    );
    assert(row.rankedSafePointIds.includes(f.row(10).id));
});

test("missing, incorrect and unproven arms cannot be nominated; unknown cases remain in the denominator", () => {
    for (const mutate of [
        (row) => row.arms.pop(),
        (row) => (row.arms[0].evidence.environment = false),
        (row) => row.arms[0].issues.push("HTTP_CORRECTNESS_FAILED"),
        (row) => (row.arms[0].driverPressureObserved = null),
        (row) => (row.arms[0].runId = row.arms[1].runId),
        (row) => (row.arms[0].correctCompletedInWindowPerSecond = null),
    ]) {
        const f = fixture();
        f.report.points.filter((p) => p.caseId === f.cases[0]).forEach(mutate);
        const result = select(f);
        assert.equal(result.selections.length, 11);
        assert.equal(result.selections[0].status, "unknown");
        assert.equal(result.selections[0].candidatePointId, null);
        assert(!result.comparisons.some((point) => point.caseId === f.cases[0]));
    }
});

test("shared selected safe rates use balanced cross-mode conditions and other extrema stay separate", () => {
    const f = fixture(["forward", "embedded"]);
    for (const rate of f.plan.policy.rates) {
        f.values(
            rate,
            [rate === 20 ? 100 : rate, rate === 20 ? 100 : rate, rate === 20 ? 100 : rate, rate === 20 ? 100 : rate],
            "forward",
        );
        f.values(
            rate,
            [rate === 40 ? 100 : rate, rate === 40 ? 100 : rate, rate === 40 ? 100 : rate, rate === 40 ? 100 : rate],
            "embedded",
        );
    }
    const result = select(f),
        first = result.comparisons.filter((p) => p.caseId === f.cases[0]);
    assert.equal(result.selections.length, 22);
    assert.deepEqual(
        first.map((p) => [p.ratePerSecond, p.comparison]),
        [
            [10, "aa"],
            [20, "forward-embedded"],
            [40, "forward-embedded"],
            [80, "aa"],
        ],
    );
    assert.deepEqual(first[0].modes, { A: "forward", B: "forward" });
    assert.deepEqual(first[1].modes, { A: "forward", B: "embedded" });
    assert.deepEqual(first[3].modes, { A: "embedded", B: "embedded" });
    assert(first.every((point) => point.expectedArms === 8));
});

test("frozen SLO unknown is never converted into a passing candidate", () => {
    const f = fixture();
    f.plan.policy.slo = { endToEndQuantile: "p99", maxLatencyMs: 10, maxErrorRate: 0, maxTimeoutRate: 0 };
    f.report.points.forEach((row) => row.arms.forEach((arm) => (arm.slo.status = "unknown")));
    const unknown = select(f);
    assert(unknown.selections.every((row) => row.status === "unknown"));
    assert.equal(unknown.comparisons.length, 0);
    f.report.points.forEach((row) => row.arms.forEach((arm) => (arm.slo.status = "passed")));
    const provisional = select(f);
    assert(provisional.selections.every((row) => row.status === "provisional-candidate" && row.confirmed === false));
    assert.equal(provisional.engineeringAccepted, false);
});

test("report and full-grid denominator tampering are rejected by the pure selection boundary", () => {
    for (const mutate of [
        (f) => f.report.points.pop(),
        (f) => (f.report.points[1] = structuredClone(f.report.points[0])),
        (f) => (f.report.planDigest = "c".repeat(64)),
        (f) => (f.report.points[0].ratePerSecond = 11),
        (f) => (f.report.points[0].comparisonPlanDigest = "d".repeat(64)),
        (f) => f.plan.suite.caseIds.pop(),
        (f) => (f.plan.points[1].rateIndex = 0),
        (f) => (f.report.points[0].throughput.median = 10000),
    ]) {
        const f = fixture();
        mutate(f);
        assert.throws(() => select(f), { name: "Error" });
    }
});

test("confirmation requests demand explicit budgets and reject caller verification claims", () => {
    const request = {
        schemaVersion: 1,
        kind: "http-capacity-confirmation-request",
        id: "confirmation-local",
        exploration: { plan: "original-plan", executionSet: "execution-set.json", report: "original-report" },
        project: "new-project.json",
        sourceReview: { maxMilliseconds: 60000, maxBytes: MiB },
        budgets: {
            point: {
                prepareMs: 120000,
                startMs: 180000,
                modeProbeMs: 45000,
                settleMs: 1000,
                stopMs: 60000,
                sealAndAnalyzeMs: 300000,
                maxEvidenceBytesPerArm: 256 * MiB,
                maxTotalEvidenceBytes: 8 * 256 * MiB + 64 * MiB,
                maxTotalMilliseconds: 8400000,
            },
            controller: { perPointMilliseconds: 780000, totalMilliseconds: 300000 },
            study: { maxTotalMilliseconds: 7 * 86400000, maxEvidenceBytes: 128 * 1024 ** 3, maxPlanBytes: 256 * MiB },
        },
    };
    assert.equal(httpCapacityConfirmationRequestSchema.safeParse(request).success, true);
    for (const mutate of [
        (value) => (value.confirmed = true),
        (value) => (value.exploration.verified = true),
        (value) => (value.candidates = ["manual"]),
        (value) => delete value.sourceReview,
        (value) => delete value.budgets,
        (value) => (value.budgets.point.maxTotalMilliseconds = -1),
    ]) {
        const changed = structuredClone(request);
        mutate(changed);
        assert.equal(httpCapacityConfirmationRequestSchema.safeParse(changed).success, false);
    }
});

test("JVM reparse reservation uses the actual eight confirmation arms", () => {
    const loaded = {
        target: {
            budget: { wallSeconds: 1200, maxLogBytes: 8 * MiB },
            jvm: {},
            jvmObservation: { maxRawBytes: 8 * MiB, parser: { maxOutputBytes: 32 * MiB, timeoutMs: 20000 } },
        },
        runtime: {
            sourceRoot: "/synthetic",
            driver: { wallSeconds: 400, maxLogBytes: MiB },
            collector: { durationMs: 300000, intervalMs: 1000, maxSamples: 301, maxOutputBytes: MiB },
            supervisor: { maxCommandBytes: MiB },
        },
    };
    const policy = {
        http: { maxTraceBytes: MiB },
        pointBudgets: {
            startMs: 10000,
            modeProbeMs: 10000,
            settleMs: 0,
            sealAndAnalyzeMs: 300000,
            maxEvidenceBytesPerArm: 128 * MiB,
        },
        analysis: { maxMilliseconds: 60000 },
    };
    const binding = { planningToolSourceRoot: "/synthetic" };
    assert(previewCapacityResources(loaded, policy, binding, [{ expectedArms: 4, workflowMaxMilliseconds: 243000 }]));
    assert.throws(
        () => previewCapacityResources(loaded, policy, binding, [{ expectedArms: 8, workflowMaxMilliseconds: 243000 }]),
        { code: "HTTP_CAPACITY_JVM_BUDGET" },
    );
    loaded.target.jvmObservation.parser.timeoutMs = 10000;
    assert(previewCapacityResources(loaded, policy, binding, [{ expectedArms: 8, workflowMaxMilliseconds: 243000 }]));
    assert.throws(() => previewCapacityResources(loaded, policy, binding, []), {
        code: "HTTP_CAPACITY_RESOURCE_POINTS",
    });
});

test("confirmation compiles the existing comparison with actual eight-arm budgets and unchanged policy", async () => {
    const policy = JSON.parse(await readFile(new URL("../examples/http-capacity-policy.json", import.meta.url)));
    const suite = JSON.parse(await readFile(new URL("../examples/http-suite-functional.json", import.meta.url)));
    const catalog = JSON.parse(
        await readFile(new URL("../benchmarks/http-serving-v0.1/catalog.json", import.meta.url)),
    );
    const f = fixture(["forward"], [10]),
        condition = select(f).comparisons[0];
    condition.caseId = catalog.cases[0].id;
    const { caseIds, purpose, ...settings } = suite;
    const profile = {
        ...settings,
        kind: "http-benchmark-profile",
        warmupMs: policy.confirmation.warmupMs,
        durationMs: policy.confirmation.durationMs,
        requestTimeoutMs: policy.http.requestTimeoutMs,
        drainMs: policy.http.drainMs,
        maxRequestsPerPhase: policy.http.maxRequestsPerPhase,
        maxTraceBytes: policy.http.maxTraceBytes,
        cases: [catalog.cases[0]],
        loads: [
            {
                id: "open-0",
                model: "open",
                ratePerSecond: 10,
                maxInFlight: policy.http.maxInFlight,
                maxStartDelayMs: policy.http.maxStartDelayMs,
            },
        ],
        benchmark: {
            suiteId: "hgbench-http-serving-v0.1",
            version: "0.1.0-draft.1",
            bundleSha256: "e".repeat(64),
            caseIds: [condition.caseId],
            coverage: "subset",
            purpose: "calibration",
        },
    };
    const shared = Object.fromEntries(
        ["build", "settings", "environment"].map((role) => [role, { path: role + ".json", sha256: "f".repeat(64) }]),
    );
    const budgets = {
        ...policy.pointBudgets,
        maxTotalMilliseconds: 8400000,
        maxTotalEvidenceBytes: 8 * policy.pointBudgets.maxEvidenceBytesPerArm + 64 * MiB,
    };
    const request = capacityConfirmationComparisonRequest(policy, condition, budgets, shared, { schemaVersion: 1 });
    assert.equal(request.schemaVersion, 1);
    assert(!Object.hasOwn(request, "metricRoles"));
    assert.equal(request.analysis.minimumPairs, 4);
    assert.equal(request.analysis.latency.quantile, "p50");
    assert.deepEqual(request.analysis.latency, policy.analysis.latency);
    assert.equal(request.analysis.familyAlpha, policy.analysis.familyAlpha);
    const actual = buildComparisonPlan(request, profile, [], { files: [] }, []).plan;
    assert.equal(actual.blocks.length, 4);
    assert.deepEqual(actual.blocks.map((block) => block.order).sort(), ["AB", "AB", "BA", "BA"]);
    assert.equal(actual.budgets.armCount, 8);
    assert.equal(actual.budgets.workflowMaxMilliseconds, 243000);
    assert.equal(actual.budgets.requiredTotalMilliseconds, 8252000);
    assert.equal(actual.budgets.requiredTotalEvidenceBytes, 8 * 256 * MiB + 64 * MiB);
    assert.equal(actual.estimate.normalEtaMilliseconds, null);
    assert.throws(
        () => buildComparisonPlan({ ...request, budgets: policy.pointBudgets }, profile, [], { files: [] }, []),
        { code: "COMPARISON_TIME_BUDGET" },
    );
    assert.throws(
        () =>
            buildComparisonPlan(
                {
                    ...request,
                    budgets: { ...budgets, maxTotalEvidenceBytes: policy.pointBudgets.maxTotalEvidenceBytes },
                },
                profile,
                [],
                { files: [] },
                [],
            ),
        { code: "COMPARISON_EVIDENCE_BUDGET" },
    );
    assert.throws(
        () =>
            capacityConfirmationComparisonRequest(
                policy,
                condition,
                { ...budgets, settleMs: policy.pointBudgets.settleMs + 1 },
                shared,
                { schemaVersion: 1 },
            ),
        { code: "HTTP_CONFIRMATION_CONDITION_DRIFT" },
    );
    assert.throws(
        () =>
            capacityConfirmationComparisonRequest(policy, condition, budgets, shared, {
                schemaVersion: 1,
                metricRoles: { latency: "inference", goodput: "quality" },
            }),
        { code: "HTTP_CONFIRMATION_COMPARISON_VERSION" },
    );
});

test("fresh confirmation resources permit only explicit safety increases and fresh host state", () => {
    const original = {
        settings: {
            budget: { wallSeconds: 1000, cpus: 2, memoryBytes: 4096, pids: 128, tmpfsBytes: 1024, maxLogBytes: 1024 },
            jvm: { activeProcessorCount: 2 },
            server: { workerThreads: 2 },
            jvmObservation: {
                events: ["gc", "jit"],
                parser: { timeoutMs: 10000, maxOutputBytes: 4096, maxWindows: 10 },
            },
        },
        environment: { hostBootId: "old", coRunningJobs: [], cpu: { model: "same" }, kernel: { release: "same" } },
        runtime: {
            sourceRoot: "/old/source",
            driver: { wallSeconds: 900, cpus: 1, memoryBytes: 2048, pids: 64 },
            collector: { durationMs: 60000, maxSamples: 61, maxOutputBytes: 4096, intervalMs: 1000 },
        },
    };
    const next = structuredClone(original);
    next.settings.budget.wallSeconds = 1100;
    next.settings.jvmObservation.parser.timeoutMs = 20000;
    next.runtime.sourceRoot = "/new/frozen/source";
    next.runtime.driver.wallSeconds = 1000;
    next.runtime.collector.durationMs = 120000;
    next.runtime.collector.maxSamples = 121;
    next.runtime.collector.maxOutputBytes = 8192;
    next.environment.hostBootId = "new";
    next.environment.coRunningJobs = ["declared-other-job"];
    const result = capacityConfirmationResourceChanges(original, next);
    assert.equal(result.length, 9);
    assert(
        result.some(
            (entry) => entry.field === "environment.hostBootId" && entry.before === "old" && entry.after === "new",
        ),
    );
    assert.deepEqual(original.runtime.sourceRoot, "/old/source");
    for (const mutate of [
        (x) => x.settings.budget.cpus++,
        (x) => x.settings.budget.memoryBytes++,
        (x) => x.settings.budget.pids++,
        (x) => x.settings.budget.tmpfsBytes++,
        (x) => x.settings.budget.maxLogBytes++,
        (x) => x.settings.jvm.activeProcessorCount++,
        (x) => x.settings.server.workerThreads++,
        (x) => x.settings.jvmObservation.events.pop(),
        (x) => x.runtime.driver.cpus++,
        (x) => x.runtime.collector.intervalMs++,
        (x) => (x.environment.kernel.release = "other"),
        (x) => x.settings.jvmObservation.parser.timeoutMs--,
    ]) {
        const changed = structuredClone(original);
        mutate(changed);
        assert.throws(() => capacityConfirmationResourceChanges(original, changed), {
            code: "HTTP_CONFIRMATION_RESOURCE_DRIFT",
        });
    }
});

const encoded = (value) => Buffer.from(JSON.stringify(value, null, 2) + "\n");
const diskJson = async (file) => JSON.parse(await readFile(file, "utf8"));
const diskSave = (file, value) => writeFile(file, encoded(value));
const byteSha = (raw) => createHash("sha256").update(raw).digest("hex");
async function relockFixture(directory, filename, report = false) {
    const files = [];
    async function walk(prefix = "") {
        for (const name of (await readdir(join(directory, prefix))).sort()) {
            const path = prefix ? prefix + "/" + name : name;
            if (path === filename) continue;
            if ((await lstat(join(directory, path))).isDirectory()) await walk(path);
            else {
                const raw = await readFile(join(directory, path));
                files.push({ path, bytes: raw.length, sha256: byteSha(raw) });
            }
        }
    }
    await walk();
    files.sort((a, b) => (a.path < b.path ? -1 : a.path > b.path ? 1 : 0));
    const lock = await diskJson(join(directory, filename));
    lock.files = files;
    lock.contentDigest = report ? byteSha(encoded(files)) : hash(files);
    await diskSave(join(directory, filename), lock);
}

test(
    "real locked missing exploration replays into eleven unknown selections and rejects coherent reseals",
    { timeout: 120000 },
    async (t) => {
        const f = await missingExploration(t),
            out = join(f.root, "confirmation");
        const frozen = await planHttpCapacityConfirmation(f.requestFile, { out });
        assert.equal(frozen.selection.selections.length, 11);
        assert(frozen.selection.selections.every((r) => r.status === "unknown" && r.confirmed === false));
        assert.equal(frozen.points.length, 0);
        assert.equal(frozen.resources, null);
        assert.equal(frozen.expectedArms, 0);
        assert.equal((await verifyHttpCapacityConfirmationPlan(out)).valid, true);
        const extra = join(out, "late-extra.bin");
        const mutation = new Promise((resolveMutation, rejectMutation) =>
            setTimeout(() => {
                writeFile(extra, "owned concurrent fixture mutation").then(resolveMutation, rejectMutation);
            }, 100),
        );
        await assert.rejects(verifyHttpCapacityConfirmationPlan(out), { code: "HTTP_CONFIRMATION_PLAN_CHANGED" });
        await mutation;
        await rm(extra);
        const executionAlias = join(f.root, "execution-alias");
        await symlink(join(f.root, "partial-execution"), executionAlias);
        await assert.rejects(
            planHttpCapacityConfirmation(f.requestFile, { out: join(executionAlias, "would-mutate-execution") }),
            { code: "HTTP_CONFIRMATION_OUTPUT_OVERLAP" },
        );
        assert.deepEqual(await readdir(join(f.root, "partial-execution")), []);
        const previewOut = join(f.root, "preview-no-write"),
            preview = await planHttpCapacityConfirmation(f.requestFile, { out: previewOut, dryRun: true });
        assert.equal(preview.written, false);
        await assert.rejects(access(previewOut), { code: "ENOENT" });
        await assert.rejects(planHttpCapacityConfirmation(f.requestFile, { out }), {
            code: "HTTP_CONFIRMATION_OUTPUT_EXISTS",
        });
        const moved = join(f.root, "moved-confirmation");
        await rename(out, moved);
        assert.equal((await verifyHttpCapacityConfirmationPlan(moved)).valid, true);
        const plan = await diskJson(join(moved, "confirmation-plan.json"));
        plan.engineeringAccepted = true;
        await diskSave(join(moved, "confirmation-plan.json"), plan);
        await relockFixture(moved, "confirmation.lock.json");
        await assert.rejects(verifyHttpCapacityConfirmationPlan(moved), { code: "HTTP_CONFIRMATION_PLAN_SEMANTICS" });
        const alias = join(f.root, "report-alias");
        await symlink(join(f.root, "report"), alias);
        const before = await readdir(join(f.root, "report"));
        await assert.rejects(planHttpCapacityConfirmation(f.requestFile, { out: join(alias, "new-output") }), {
            code: "HTTP_CONFIRMATION_OUTPUT_OVERLAP",
        });
        assert.deepEqual(await readdir(join(f.root, "report")), before);
        const source = await diskJson(join(f.root, "report/report.json"));
        source.points[0].measurementValid = true;
        source.points[0].throughput.median = 999;
        await diskSave(join(f.root, "report/report.json"), source);
        await relockFixture(join(f.root, "report"), "evidence.lock.json", true);
        const forgedOut = join(f.root, "forged-not-created");
        await assert.rejects(planHttpCapacityConfirmation(f.requestFile, { out: forgedOut }), {
            code: "HTTP_CONFIRMATION_REPORT_REBUILD_MISMATCH",
        });
        await assert.rejects(access(forgedOut), { code: "ENOENT" });
    },
);

test("single-observation candidates retain independent eight-arm confirmation and reject legacy relabeling", async () => {
    const f = fixture();
    f.plan.schemaVersion = 2;
    f.plan.policy.schemaVersion = 2;
    f.plan.policy.exploration = { mode: "single-run", runs: 1 };
    f.plan.points.forEach((point) => (point.expectedArms = 1));
    f.report.algorithm = "capacity-curves-v2-single-run";
    f.report.points.forEach((point) => {
        point.expectedArms = 1;
        point.arms = [point.arms[0]];
    });
    const selected = select(f);
    assert.equal(selected.selections.length, 11);
    assert(selected.comparisons.length > 0);
    assert(selected.comparisons.every((condition) => condition.expectedArms === 8));
    const policy = JSON.parse(await readFile("examples/http-capacity-policy-v2.json", "utf8"));
    const shared = Object.fromEntries(
        ["build", "settings", "environment"].map((role) => [role, { path: `${role}.json`, sha256: "c".repeat(64) }]),
    );
    const request = capacityConfirmationComparisonRequest(
        policy,
        selected.comparisons[0],
        policy.pointBudgets,
        shared,
        { schemaVersion: 3, comparison: "single-observation" },
        "total",
    );
    assert.equal(request.schemaVersion, 3);
    assert.equal(request.comparison, "aa");
    assert.equal(request.blockIds.length, 4);
    assert.equal(request.budgets.budgetMode, "total");
    assert.deepEqual(request.metricRoles, { latency: "inference", goodput: "inference" });
    f.report.algorithm = "capacity-curves-v1";
    assert.throws(
        () => select(f),
        (e) => e.code === "HTTP_CONFIRMATION_SOURCE_IDENTITY",
    );
});
