/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
import test from "node:test";
import assert from "node:assert/strict";
import { mkdir, cp, readFile, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { createHash } from "node:crypto";
import { freezeComparison, verifyComparison, validateRequest } from "../components/http/comparison-plan.mjs";
import {
    normalizeHttpEvidence,
    normalizeObservationEvidence,
    normalizePairedEvidence,
    writePairedEvidence,
    metricRoleEvidence,
} from "../components/http/paired-evidence.mjs";
import { localFixtureContext, emitPairedFixture, seal } from "./http-paired-evidence.fixture.mjs";
import { valueHash } from "../components/paired/core.mjs";

const MiB = 1024 ** 2;
const hash = (raw) => createHash("sha256").update(raw).digest("hex");
const encode = (value) => Buffer.from(JSON.stringify(value, null, 2) + "\n");
const read = async (path) => JSON.parse(await readFile(path));
const put = (path, value) => writeFile(path, encode(value));

// Actual localhost HTTP; all Docker/JVM/proc/controller observations remain synthetic protocol fixtures.
test(
    "single observation freezes one actual O and keeps independent lifecycle/quality verification without pairs",
    { timeout: 90000 },
    async (t) => {
        const context = await localFixtureContext(t),
            { root, settings, shared } = context;
        const http = join(root, "http-plan");
        await mkdir(http);
        await cp(resolve("benchmarks/http-serving-v0.1"), join(http, "benchmark"), { recursive: true });
        const catalog = await read(join(http, "benchmark/catalog.json")),
            answers = await read(join(http, "benchmark/answers.json"));
        const { caseIds, purpose, ...base } = await read(resolve("examples/http-suite-functional.json"));
        const profile = {
            ...base,
            kind: "http-benchmark-profile",
            serviceChecks: true,
            target: {
                label: "localhost single protocol fixture",
                build: "fixture",
                mode: "forward",
                environment: "synthetic lifecycle, actual HTTP",
                evidenceFiles: [],
            },
            cases: [
                {
                    id: "scalar-add",
                    body: catalog.cases.find((row) => row.id === "scalar-add").body,
                    expectedStatus: 200,
                    checks: [
                        { pointer: "/status/code", equals: 200 },
                        {
                            pointer: "/result/data",
                            canonicalSha256: answers.cases.find((row) => row.id === "scalar-add").canonicalSha256,
                        },
                    ],
                },
            ],
            loads: [{ id: "open-20", model: "open", ratePerSecond: 20, maxInFlight: 4, maxStartDelayMs: 500 }],
            repeats: 1,
            warmupMs: 0,
            durationMs: 200,
            requestTimeoutMs: 500,
            drainMs: 500,
            maxRequestsPerPhase: 100,
            maxTraceBytes: MiB,
            benchmark: {
                suiteId: catalog.suiteId,
                version: catalog.version,
                bundleSha256: hash(await readFile(join(http, "benchmark/bundle.lock.json"))),
                caseIds: ["scalar-add"],
                coverage: "subset",
                purpose,
            },
        };
        await put(join(http, "profile.json"), profile);
        await put(join(http, "target-evidence.json"), {
            schemaVersion: 1,
            kind: "operator-supplied-target-evidence",
            files: [],
        });
        const sharedEvidence = {};
        for (const role of ["build", "settings", "environment"]) {
            const path = join(root, role + ".json");
            await put(path, shared[role]);
            sharedEvidence[role] = { path, sha256: hash(await readFile(path)) };
        }
        const request = {
            schemaVersion: 3,
            kind: "http-comparison-request",
            id: "single-protocol",
            purpose,
            comparison: "single-observation",
            modes: { O: "forward" },
            blockIds: ["observe-01"],
            sharedEvidence,
            analysis: {
                maxMilliseconds: 60000,
                latency: { quantile: "p50", minimumObservations: 1 },
                goodput: { minimumObservations: 1 },
            },
            budgets: {
                prepareMs: 100,
                startMs: 1000,
                modeProbeMs: 30000,
                settleMs: 0,
                stopMs: 1000,
                sealAndAnalyzeMs: 1000,
                maxEvidenceBytesPerArm: 8 * MiB,
                maxTotalEvidenceBytes: 128 * MiB,
                maxTotalMilliseconds: 60000,
                budgetMode: "total",
            },
        };
        const inputFile = join(root, "request.json"),
            planDirectory = join(root, "comparison");
        await put(inputFile, request);
        const first = await freezeComparison({ inputFile, httpPlanDirectory: http, outputDirectory: planDirectory });
        const plan = await read(join(planDirectory, "plan.json"));

        await t.test("strict request and exact single denominator reject invented paired fields", async () => {
            assert.equal(first.armCount, 1);
            assert.deepEqual(Object.keys(plan.variants), ["O"]);
            assert.equal(plan.blocks.length, 1);
            assert.equal(plan.blocks[0].order, "O");
            assert.deepEqual(
                plan.blocks[0].arms.map((row) => row.arm),
                ["O"],
            );
            assert.equal(plan.analysisPolicy.kind, "http-observation-analysis-policy");
            assert.equal(Object.hasOwn(plan.analysisPolicy, "minimumPairs"), false);
            assert.equal(Object.hasOwn(plan.analysisPolicy, "baselineDigest"), false);
            assert(plan.metricBindings.every((binding) => binding.role === "descriptive"));
            for (const patch of [
                { orderSeed: "invented" },
                { metricRoles: { latency: "inference", goodput: "quality" } },
                { modes: { A: "forward", B: "forward" } },
                { blockIds: ["observe-01", "observe-02"] },
                { analysis: { ...request.analysis, minimumPairs: 1 } },
                { budgets: { ...request.budgets, budgetMode: undefined } },
            ])
                assert.throws(() => validateRequest({ ...request, ...patch }));
            assert.equal((await verifyComparison(planDirectory)).planDigest, first.planDigest);
        });
        await t.test("total is explicit and stage-sum/known baseline still fail closed", async () => {
            assert.equal(plan.budgets.stageSumFitsTotal, false);
            assert.equal(plan.budgets.enforcedTotalMilliseconds, 60000);
            assert.equal(plan.estimate.safetyUpperBoundMilliseconds, 60000);
            for (const [name, budgets] of [
                ["stage-sum", { ...request.budgets, budgetMode: "stage-sum" }],
                ["too-short", { ...request.budgets, maxTotalMilliseconds: 11001 }],
            ]) {
                const path = join(root, name + ".json");
                await put(path, { ...request, budgets });
                await assert.rejects(
                    freezeComparison({ inputFile: path, httpPlanDirectory: http, outputDirectory: join(root, name) }),
                    /deadline|total|Lifecycle/i,
                );
            }
            const legacy = { ...request, schemaVersion: 1 };
            assert.throws(() => validateRequest(legacy));
        });

        await t.test(
            "v3 paired roles are explicit while old requests retain their paired family and stage-sum admission",
            async () => {
                const paired = {
                    ...request,
                    comparison: "aa",
                    modes: { A: "forward", B: "forward" },
                    blockIds: ["b01", "b02"],
                    orderSeed: "explicit-paired-order",
                    metricRoles: { latency: "inference", goodput: "inference" },
                    analysis: {
                        familyAlpha: 0.05,
                        minimumPairs: 2,
                        bootstrapReplicates: 10000,
                        minimumTailDraws: 20,
                        maxWork: 1000000000,
                        maxMilliseconds: 60000,
                        resampleSeed: "explicit-paired-resample",
                        latency: { quantile: "p50", relativeBudget: 0.05, absoluteBudget: 1, minimumObservations: 1 },
                        goodput: { relativeBudget: 0.05, absoluteBudget: 1, minimumObservations: 1 },
                    },
                    budgets: { ...request.budgets, maxTotalMilliseconds: 600000 },
                };
                const path = join(root, "paired-request.json"),
                    out = join(root, "paired-v3");
                await put(path, paired);
                await freezeComparison({ inputFile: path, httpPlanDirectory: http, outputDirectory: out });
                const p = await read(join(out, "plan.json"));
                assert.equal(p.budgets.armCount, 4);
                assert.equal(p.analysisPolicy.kind, "paired-analysis-policy");
                assert.equal(p.analysisPolicy.metrics.length, 2);
                assert.equal(
                    metricRoleEvidence(
                        p,
                        Object.fromEntries(
                            p.metricBindings.map((binding) => [binding.id, { value: 1, observations: 4 }]),
                        ),
                    ).quality.status,
                    "passed",
                );
                const { metricRoles, ...legacy } = paired;
                legacy.schemaVersion = 1;
                legacy.budgets = { ...paired.budgets };
                delete legacy.budgets.budgetMode;
                await put(path, legacy);
                const old = join(root, "paired-v1");
                await freezeComparison({ inputFile: path, httpPlanDirectory: http, outputDirectory: old });
                const oldPlan = await read(join(old, "plan.json"));
                assert.deepEqual(oldPlan.analysisPolicy, p.analysisPolicy);
                assert.equal(Object.hasOwn(oldPlan.budgets, "budgetMode"), false);
                assert.equal(Object.hasOwn(oldPlan.budgets, "enforcedTotalMilliseconds"), false);
                assert.equal((await verifyComparison(old)).armCount, 4);
                assert.throws(() =>
                    validateRequest({ ...legacy, budgets: { ...legacy.budgets, budgetMode: "total" } }),
                );
                const quality = {
                    ...paired,
                    schemaVersion: 2,
                    metricRoles: { latency: "inference", goodput: "quality" },
                    budgets: { ...legacy.budgets },
                };
                await put(path, quality);
                const v2 = join(root, "paired-v2");
                await freezeComparison({ inputFile: path, httpPlanDirectory: http, outputDirectory: v2 });
                assert.equal((await read(join(v2, "plan.json"))).analysisPolicy.metrics.length, 1);
                assert.throws(() => validateRequest({ ...quality, metricRoles }));
                assert.throws(() => validateRequest({ ...paired, metricRoles: undefined }));
            },
        );

        await t.test("P99 below 1000 remains unknown without a lowered hidden threshold", async () => {
            const path = join(root, "p99-request.json"),
                out = join(root, "p99-plan");
            await put(path, {
                ...request,
                analysis: { ...request.analysis, latency: { quantile: "p99", minimumObservations: 1 } },
            });
            await freezeComparison({ inputFile: path, httpPlanDirectory: http, outputDirectory: out });
            const p = await read(join(out, "plan.json"));
            const latency = p.metricBindings.find((binding) => binding.kind === "latency");
            assert.equal(latency.minimumObservations, 1000);
            assert.equal(p.samplingBounds.find((row) => row.metricId === latency.id).minimumRequiredObservations, 1000);
            const result = metricRoleEvidence(
                p,
                Object.fromEntries(p.metricBindings.map((binding) => [binding.id, { value: 1, observations: 4 }])),
            );
            const metric = result.quality.metrics.find((metric) => metric.metricId === latency.id);
            assert.equal(metric.status, "unknown");
            assert.equal(metric.value, null);
            assert.equal(metric.observedValue, 1);
            assert.equal(result.quality.status, "unknown");
        });

        const emitted = await emitPairedFixture({ root, planDirectory, settings });
        await t.test("one new HTTP workflow independently rebuilds O with no paired statistic", async () => {
            const result = await normalizeObservationEvidence({
                planDirectory,
                executionDirectory: emitted.executionDirectory,
                includeHttpSummaries: true,
            });
            assert.equal(result.verification.valid, true, JSON.stringify(result.verification));
            assert.equal(result.verification.kind, "http-observation-evidence-verification");
            assert.equal(result.verification.design, "single-observation");
            assert.equal(result.verification.rows.length, 1);
            assert.equal(result.verification.rows[0].arm, "O");
            assert.equal(result.verification.rows[0].httpSummary.runs.length, 1);
            assert.equal(result.normalized.kind, "normalized-http-observations");
            assert.equal(Object.hasOwn(result.normalized, "blocks"), false);
            assert.equal(result.analysis.kind, "http-single-observation-analysis");
            assert.equal(result.analysis.inferenceAccepted, false);
            assert.equal(result.verification.statisticsEligible, false);
            await assert.rejects(
                normalizePairedEvidence({ planDirectory, executionDirectory: emitted.executionDirectory }),
                { code: "HTTP_EVIDENCE_DESIGN" },
            );
            assert.deepEqual(
                await normalizeHttpEvidence({
                    planDirectory,
                    executionDirectory: emitted.executionDirectory,
                    includeHttpSummaries: true,
                }),
                result,
            );
        });
        await t.test("missing O stays missing and bad raw is not rescued by a complete terminal", async () => {
            for (const mode of ["missing", "bad-raw"]) {
                const directory = join(root, mode);
                await cp(emitted.executionDirectory, directory, { recursive: true });
                if (mode === "missing") {
                    const execution = await read(join(directory, "execution.json"));
                    execution.receipts = [];
                    await put(join(directory, "execution.json"), execution);
                } else await writeFile(join(directory, "arms", emitted.first.runId, "raw/traces/r0001.ndjson"), "{}\n");
                await seal(directory, plan.planDigest);
                const result = await normalizeObservationEvidence({ planDirectory, executionDirectory: directory });
                assert.equal(result.verification.valid, false);
                assert.equal(result.verification.rows.length, 1);
                assert.equal(result.verification.rows[0].rawVerified, undefined);
                assert.equal(result.verification.statisticsEligible, false);
            }
        });
        await t.test(
            "public adapter writes descriptive outputs and relocation preserves mathematical bytes",
            async () => {
                const out = join(root, "report");
                const command = await promisify(execFile)(
                    process.execPath,
                    [
                        resolve("components/http/paired-evidence.mjs"),
                        "--plan",
                        planDirectory,
                        "--execution",
                        emitted.executionDirectory,
                        "--out",
                        out,
                    ],
                    { timeout: 30000 },
                );
                const reply = JSON.parse(command.stdout);
                assert.equal(reply.data.armCount, 1);
                assert.equal(reply.data.statisticsEligible, false);
                const moved = join(root, "moved");
                await mkdir(moved);
                await cp(planDirectory, join(moved, "comparison"), { recursive: true });
                await cp(emitted.executionDirectory, join(moved, "execution"), { recursive: true });
                await writePairedEvidence({
                    planDirectory: join(moved, "comparison"),
                    executionDirectory: join(moved, "execution"),
                    outputDirectory: join(moved, "report"),
                });
                for (const name of ["observations.json", "verification.json", "analysis.json", "report.html"])
                    assert.deepEqual(await readFile(join(out, name)), await readFile(join(moved, "report", name)));
                await assert.rejects(
                    writePairedEvidence({
                        planDirectory,
                        executionDirectory: emitted.executionDirectory,
                        outputDirectory: out,
                    }),
                    /EEXIST/,
                );
            },
        );
        await t.test("extra fake B profile remains forbidden even after a coherent outer re-seal", async () => {
            const fake = join(root, "fake-paired");
            await cp(planDirectory, fake, { recursive: true });
            const raw = await readFile(join(fake, "profiles/O.json"));
            await writeFile(join(fake, "profiles/B.json"), raw);
            const lock = await read(join(fake, "plan.lock.json"));
            lock.files.push({ path: "profiles/B.json", bytes: raw.length, sha256: hash(raw) });
            lock.files.sort((a, b) => a.path.localeCompare(b.path));
            lock.contentDigest = valueHash(lock.files);
            await put(join(fake, "plan.lock.json"), lock);
            await assert.rejects(verifyComparison(fake), { code: "COMPARISON_LOCK" });
        });
    },
);
