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
import { mkdtemp, readFile, writeFile, mkdir, rm, cp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import {
    buildComparisonPlan,
    validateRequest,
    freezeComparison,
    verifyComparison,
} from "../components/http/comparison-plan.mjs";
import { metricValues, metricRoleEvidence } from "../components/http/paired-evidence.mjs";
import { analyze, valueHash } from "../components/paired/core.mjs";
import { html } from "../components/http/paired-report.mjs";
import { bytesDigest } from "../components/http/common.mjs";

const read = async (path) => JSON.parse(await readFile(path, "utf8"));
const put = (path, value) => writeFile(path, JSON.stringify(value, null, 2) + "\n");
const run = promisify(execFile);
const catalog = await read(resolve("benchmarks/http-serving-v0.1/catalog.json"));
const answers = await read(resolve("benchmarks/http-serving-v0.1/answers.json"));
const base = await read(resolve("examples/http-suite-functional.json"));
const MiB = 1024 * 1024;

function fixture(version = 2, caseCount = 11) {
    const { caseIds, purpose, ...settings } = structuredClone(base);
    const selected = caseIds.slice(0, caseCount);
    const profile = {
        ...settings,
        kind: "http-benchmark-profile",
        serviceChecks: true,
        target: {
            label: "protocol-fixture",
            build: "synthetic",
            mode: "forward",
            environment: "not-runtime-proof",
            evidenceFiles: [],
        },
        warmupMs: 0,
        durationMs: 15000,
        requestTimeoutMs: 100,
        drainMs: 100,
        maxRequestsPerPhase: 100000,
        maxTraceBytes: MiB,
        loads: [{ id: "open-10", model: "open", ratePerSecond: 10, maxInFlight: 4, maxStartDelayMs: 1000 }],
        cases: selected.map((id) => ({
            id,
            body: catalog.cases.find((item) => item.id === id).body,
            expectedStatus: 200,
            checks: [
                { pointer: "/status/code", equals: 200 },
                {
                    pointer: "/result/data",
                    canonicalSha256: answers.cases.find((item) => item.id === id).canonicalSha256,
                },
            ],
        })),
        benchmark: {
            suiteId: catalog.suiteId,
            version: catalog.version,
            bundleSha256: "a".repeat(64),
            caseIds: selected,
            coverage: caseCount === 11 ? "full" : "subset",
            purpose,
        },
    };
    const request = {
        schemaVersion: version,
        kind: "http-comparison-request",
        id: "metric-roles-fixture",
        purpose,
        comparison: "aa",
        modes: { A: "forward", B: "forward" },
        blockIds: ["b1", "b2", "b3", "b4"],
        orderSeed: "roles-order",
        sharedEvidence: Object.fromEntries(
            ["build", "settings", "environment"].map((role) => [
                role,
                { path: `${role}.json`, sha256: "a".repeat(64) },
            ]),
        ),
        analysis: {
            familyAlpha: 0.05,
            minimumPairs: 4,
            bootstrapReplicates: 40000,
            minimumTailDraws: 20,
            maxWork: 1000000000,
            maxMilliseconds: 60000,
            resampleSeed: "roles-resample",
            latency: { relativeBudget: 0.05, absoluteBudget: 1, minimumObservations: 1000, quantile: "p50" },
            goodput: { relativeBudget: 0.05, absoluteBudget: 1, minimumObservations: 100 },
        },
        budgets: {
            prepareMs: 100,
            startMs: 1000,
            modeProbeMs: 30000,
            settleMs: 0,
            stopMs: 1000,
            sealAndAnalyzeMs: 1000,
            maxEvidenceBytesPerArm: 32 * MiB,
            maxTotalEvidenceBytes: 512 * MiB,
            maxTotalMilliseconds: 7200000,
        },
        ...(version === 2 ? { metricRoles: { latency: "inference", goodput: "quality" } } : {}),
    };
    const build = () => buildComparisonPlan(request, profile, [], { files: [] }, []);
    return { request, profile, build };
}

async function temporary(t) {
    const root = await mkdtemp(join(tmpdir(), "hgbench-metric-roles-"));
    t.after(() => rm(root, { recursive: true, force: true }));
    return root;
}

function valuesFor(plan, position = 0, candidate = false) {
    return Object.fromEntries(
        plan.metricBindings.map((binding) => [
            binding.id,
            {
                value:
                    binding.kind === "goodput"
                        ? 10
                        : 10 + position + (candidate ? [0.1, -0.2, 0.3, -0.4][position] : 0),
                observations: 2000,
            },
        ]),
    );
}

function normalized(plan, failQuality = false) {
    return {
        schemaVersion: 1,
        kind: "normalized-paired-observations",
        policy: plan.analysisPolicy,
        blocks: plan.blocks.map((block, position) => ({
            id: block.id,
            observedOrder: block.order,
            ...Object.fromEntries(
                [
                    ["baseline", "A"],
                    ["candidate", "B"],
                ].map(([label, arm]) => {
                    const values = valuesFor(plan, position, arm === "B");
                    if (failQuality && position === 3 && arm === "B")
                        delete values[plan.metricBindings.find((binding) => binding.kind === "goodput").id];
                    const result = metricRoleEvidence(plan, values);
                    return [
                        label,
                        {
                            runId: `${block.id}-${arm}`,
                            evidenceSha256: valueHash({ position, arm }),
                            variantDigest: plan.variants[arm].variantDigest,
                            conditionsDigest: block.conditionsDigest,
                            status: result.quality && result.quality.status !== "passed" ? "invalid" : "ok",
                            values: result.inferenceValues,
                        },
                    ];
                }),
            ),
        })),
    };
}

test("metric roles require explicit v2 roles and reject legacy or arbitrary role overrides", () => {
    const f = fixture();
    assert.equal(validateRequest(f.request), f.request);
    for (const roles of [
        undefined,
        null,
        {},
        { latency: "inference", goodput: "inference" },
        { latency: "quality", goodput: "inference" },
        { latency: "inference", goodput: "quality", passed: true },
    ])
        assert.throws(() => validateRequest({ ...f.request, metricRoles: roles }));
    assert.throws(() => validateRequest({ ...fixture(1).request, metricRoles: f.request.metricRoles }));
    assert.throws(() => validateRequest({ ...f.request, schemaVersion: 3 }));
    for (const minimumObservations of [0, -1, 1.5, Number.MAX_SAFE_INTEGER + 1])
        assert.throws(() =>
            validateRequest({
                ...f.request,
                analysis: { ...f.request.analysis, goodput: { ...f.request.analysis.goodput, minimumObservations } },
            }),
        );
});

test("metric roles preserve all eleven question bindings and quality sampling bounds", () => {
    const { plan } = fixture().build();
    assert.equal(plan.schemaVersion, 2);
    assert.deepEqual(plan.metricRoles, { latency: "inference", goodput: "quality" });
    assert.equal(plan.metricBindings.length, 22);
    assert.equal(plan.samplingBounds.length, 22);
    assert.equal(plan.analysisPolicy.metrics.length, 11);
    assert.equal(new Set(plan.metricBindings.map((binding) => binding.caseId)).size, 11);
    for (const binding of plan.metricBindings) {
        assert.equal(binding.role, binding.kind === "latency" ? "inference" : "quality");
        assert.equal(binding.minimumObservations, binding.kind === "latency" ? 1000 : 100);
        const bound = plan.samplingBounds.find((item) => item.metricId === binding.id);
        assert.equal(bound.minimumRequiredObservations, binding.minimumObservations);
        assert.equal(bound.minimumPotentiallyReachable, binding.kind === "goodput");
        assert.equal(
            plan.analysisPolicy.metrics.some((metric) => metric.id === binding.id),
            binding.role === "inference",
        );
    }
    assert.equal(plan.budgets.analysisWork, 4 * 40000 * 11);
});

test("legacy v1 retains both primary metrics without v2 role fields", () => {
    const { plan } = fixture(1).build();
    assert.equal(plan.schemaVersion, 1);
    assert(!Object.hasOwn(plan, "metricRoles"));
    assert.equal(plan.metricBindings.length, 22);
    assert.equal(plan.analysisPolicy.metrics.length, 22);
    assert(
        plan.metricBindings.every(
            (binding) => !Object.hasOwn(binding, "role") && !Object.hasOwn(binding, "minimumObservations"),
        ),
    );
    const values = valuesFor(plan);
    assert.deepEqual(metricRoleEvidence(plan, values), { inferenceValues: values });
});

test("quality inventories retain missing and insufficient observations without dropping a question", () => {
    const { plan } = fixture().build(),
        values = valuesFor(plan);
    const quality = plan.metricBindings.filter((binding) => binding.role === "quality");
    delete values[quality[0].id];
    values[quality[1].id].observations = 99;
    const result = metricRoleEvidence(plan, values);
    assert.equal(result.quality.status, "failed");
    assert.equal(result.quality.metrics.length, 11);
    assert.equal(result.quality.metrics[0].status, "unknown");
    assert.equal(result.quality.metrics[0].observations, null);
    assert.equal(result.quality.metrics[1].status, "failed");
    assert.equal(result.quality.metrics[1].observations, 99);
    assert.equal(result.quality.metrics.filter((metric) => metric.status === "passed").length, 9);
    assert.equal(Object.keys(result.inferenceValues).length, 11);
    assert.equal(metricRoleEvidence(plan, {}).quality.metrics.length, 11);
    assert(metricRoleEvidence(plan, {}).quality.metrics.every((metric) => metric.status === "unknown"));
    for (const mutate of [
        (p) => (p.metricRoles.goodput = "inference"),
        (p) => p.metricBindings.pop(),
        (p) => (p.metricBindings[1].role = "inference"),
        (p) => p.analysisPolicy.metrics.pop(),
    ]) {
        const changed = structuredClone(plan);
        mutate(changed);
        assert.throws(() => metricRoleEvidence(changed, values));
    }
    assert.throws(
        () => metricRoleEvidence(plan, { ...values, unexpected: { value: 1, observations: 1 } }),
        /HTTP_METRIC_VALUE_UNDECLARED/,
    );
});

test("quality raw accounting independently recounts goodput before applying its observation minimum", async (t) => {
    const root = await temporary(t),
        f = fixture(2, 1);
    f.request.analysis.latency.minimumObservations = 4;
    f.request.analysis.goodput.minimumObservations = 5;
    const { plan } = f.build(),
        caseId = f.profile.cases[0].id,
        loadId = f.profile.loads[0].id;
    await mkdir(join(root, "runs"));
    await mkdir(join(root, "traces"));
    await put(join(root, "runs/r1.json"), { phases: [{ elapsedMs: 1000 }] });
    await writeFile(
        join(root, "traces/r1.ndjson"),
        [
            ...[10, 20, 30, 40].map((endMs) => ({ phase: "measurement", outcome: "ok", endMs })),
            { phase: "warmup", outcome: "ok", endMs: 1 },
            { phase: "measurement", outcome: "ok", endMs: 1001 },
            { phase: "measurement", outcome: "http-error", endMs: 50 },
        ]
            .map((value) => JSON.stringify(value) + "\n")
            .join(""),
    );
    const raw = {
        runs: [
            {
                id: "r1",
                caseId,
                loadId,
                repeat: 0,
                durationMs: 1000,
                correctEndToEndMs: { p50: 6, count: 4 },
                correctCompletedInWindowPerSecond: 4,
            },
        ],
    };
    const observed = await metricValues(raw, root, plan.metricBindings);
    assert.equal(observed.minimumWorkflowMs, 1000);
    const quality = metricRoleEvidence(plan, observed.values).quality;
    assert.equal(quality.metrics[0].value, 4);
    assert.equal(quality.metrics[0].observations, 4);
    assert.equal(quality.metrics[0].minimumObservations, 5);
    assert.equal(quality.status, "failed");
    const changed = structuredClone(raw);
    changed.runs[0].correctCompletedInWindowPerSecond = 5;
    await assert.rejects(metricValues(changed, root, plan.metricBindings), /HTTP_GOODPUT_ACCOUNTING/);
    delete changed.runs[0].correctCompletedInWindowPerSecond;
    await assert.rejects(metricValues(changed, root, plan.metricBindings), /HTTP_GOODPUT_ACCOUNTING/);
});

test("constant goodput stays visible as quality without producing a v2 degenerate inference cell", () => {
    const v2 = fixture(2, 1).build().plan,
        v1 = fixture(1, 1).build().plan;
    const actual = analyze(normalized(v2)),
        legacy = analyze(normalized(v1));
    assert.equal(actual.cells.length, 1);
    assert.equal(actual.familyStatements, 2);
    assert(actual.cells.every((cell) => cell.issues.length === 0 && cell.warnings.length === 0));
    assert.equal(legacy.cells.length, 2);
    assert(
        legacy.cells
            .find((cell) => cell.metric.unit === "requests/s")
            .warnings.some((warning) => warning.includes("degenerate")),
    );
    const damaged = analyze(normalized(v2, true));
    assert(damaged.cells.every((cell) => cell.issues.some((issue) => issue.includes("invalid:"))));
    assert.equal(damaged.cells.length, 1);
});

test("render keeps every planned quality row, failures, escaping and the v1 boundary", () => {
    const plan = fixture().build().plan,
        analysis = analyze(normalized(plan));
    const rows = plan.blocks.flatMap((block) =>
        block.arms.map((arm) => ({
            ...arm,
            blockId: block.id,
            status: "ok",
            issues: [],
            values: valuesFor(plan),
            metricQuality: metricRoleEvidence(plan, valuesFor(plan)).quality,
        })),
    );
    const missing = rows.at(-1).metricQuality.metrics.at(-1);
    missing.caseId = '<img src=x onerror="alert(1)">';
    missing.status = "unknown";
    missing.value = null;
    missing.observations = null;
    missing.issues = ["HTTP_QUALITY_METRIC_MISSING"];
    rows.at(-1).status = "invalid";
    const input = {
        analysis,
        reportContext: {
            comparisonSchemaVersion: 2,
            metricRoles: plan.metricRoles,
            metricBindings: plan.metricBindings,
        },
        verification: {
            rows,
            valid: false,
            statisticsEligible: false,
            globalIssues: [],
            gates: { metricQuality: { plannedMetrics: 88, unknownMetrics: 1 } },
        },
    };
    const output = html(input);
    assert(output.includes("Goodput 质量与描述"));
    assert(output.includes("HTTP_QUALITY_METRIC_MISSING"));
    assert(output.includes("88"));
    assert(output.includes("&lt;img src=x onerror=&quot;alert(1)&quot;&gt;"));
    assert(!output.includes("<img src=x"));
    assert.equal(
        output
            .split("<h2>Goodput 质量与描述</h2>")[1]
            .split("</section>")[0]
            .match(/<code>correctCompletedInWindowPerSecond<\/code>/g).length,
        88,
    );
    const legacy = structuredClone(input);
    delete legacy.reportContext.comparisonSchemaVersion;
    delete legacy.reportContext.metricRoles;
    const old = html(legacy);
    assert(!old.includes("Goodput 质量与描述"));
    assert(old.includes("正值表示退化；延迟和吞吐按各自方向统一符号。统计单位"));
});

test("freeze v2 locks roles and rejects re-sealed derived-role tampering through public CLI", async (t) => {
    const root = await temporary(t),
        f = fixture();
    const httpPlanDirectory = join(root, "http-plan");
    await mkdir(httpPlanDirectory);
    await cp(resolve("benchmarks/http-serving-v0.1"), join(httpPlanDirectory, "benchmark"), { recursive: true });
    f.profile.benchmark.bundleSha256 = bytesDigest(
        await readFile(join(httpPlanDirectory, "benchmark/bundle.lock.json")),
    );
    await put(join(httpPlanDirectory, "profile.json"), f.profile);
    await put(join(httpPlanDirectory, "target-evidence.json"), {
        schemaVersion: 1,
        kind: "operator-supplied-target-evidence",
        files: [],
        attestation: "Protocol fixture only",
    });
    for (const role of ["build", "settings", "environment"]) {
        const path = join(root, `${role}.json`);
        await put(path, { kind: `fixture-${role}`, realTargetVerified: false });
        f.request.sharedEvidence[role].sha256 = bytesDigest(await readFile(path));
    }
    const inputFile = join(root, "request.json");
    await put(inputFile, f.request);
    const outputDirectory = join(root, "comparison");
    const reply = JSON.parse(
        (
            await run(
                process.execPath,
                [
                    resolve("dist/cli.js"),
                    "http",
                    "comparison",
                    "plan",
                    inputFile,
                    "--http-plan",
                    httpPlanDirectory,
                    "--out",
                    outputDirectory,
                ],
                { timeout: 30000 },
            )
        ).stdout,
    );
    assert.equal(reply.ok, true);
    assert.equal((await verifyComparison(outputDirectory)).planDigest, reply.data.planDigest);
    const second = await freezeComparison({ inputFile, httpPlanDirectory, outputDirectory: join(root, "second") });
    assert.equal(second.planDigest, reply.data.planDigest);
    const plan = await read(join(outputDirectory, "plan.json"));
    assert.equal(plan.metricBindings.length, 22);
    assert.equal(plan.analysisPolicy.metrics.length, 11);
    plan.metricBindings.find((binding) => binding.role === "quality").role = "inference";
    await put(join(outputDirectory, "plan.json"), plan);
    const lock = await read(join(outputDirectory, "plan.lock.json")),
        raw = await readFile(join(outputDirectory, "plan.json"));
    Object.assign(
        lock.files.find((file) => file.path === "plan.json"),
        { bytes: raw.length, sha256: bytesDigest(raw) },
    );
    lock.contentDigest = valueHash(lock.files);
    await put(join(outputDirectory, "plan.lock.json"), lock);
    await assert.rejects(verifyComparison(outputDirectory), { code: "COMPARISON_PLAN_CHANGED" });
});
