/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
import test from "node:test";
import assert from "node:assert/strict";
import { readFile, writeFile } from "node:fs/promises";
import { join, resolve, dirname } from "node:path";
import { randomBytes } from "node:crypto";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { projectCapacityBoundary } from "../components/http/capacity-boundary.mjs";
import { httpCapacityPolicySchema } from "../dist/http-capacity-contracts.js";
import { planHttpCapacity, verifyHttpCapacityPlan, capacityInventory } from "../dist/http-capacity.js";
import { hash } from "../dist/io.js";
import { missingExploration } from "./http-capacity-confirmation.fixture.mjs";
import { localFixtureContext, emitPairedFixture } from "./http-paired-evidence.fixture.mjs";

// Synthetic projection inputs only. Actual IO always uses the fixed independent verifier.
function fixture() {
    const point = {
        id: "point",
        caseId: "scalar-add",
        mode: "forward",
        ratePerSecond: 200,
        expectedArms: 4,
        comparisonPlanDigest: "a".repeat(64),
    };
    const policy = {
        continuation: { schemaVersion: 1, mode: "verified-case-range" },
        analysis: { latency: { minimumObservations: 100 }, goodput: { minimumObservations: 100 } },
        slo: null,
    };
    const verification = {
        planDigest: point.comparisonPlanDigest,
        executionLockSha256: "b".repeat(64),
        globalIssues: [],
        valid: true,
        rows: Array.from({ length: 4 }, (_, index) => ({
            runId: "new-" + index,
            rawEvidenceSha256: String(index + 1).repeat(64),
            rawVerified: true,
            runtimeArtifactsConsistent: true,
            driverVerified: true,
            resourcesVerified: true,
            expectedMode: "forward",
            status: "ok",
            issues: [],
            httpSummary: {
                gates: { sampleAccounting: "passed", correctness: "passed" },
                runs: [
                    {
                        caseId: "scalar-add",
                        model: "open",
                        repeat: 0,
                        ratePerSecond: 200,
                        durationMs: 1000,
                        failed: 0,
                        correct: 200,
                        counts: { ok: 200 },
                        correctCompletedInWindowPerSecond: 200,
                        correctEndToEndMs: { count: 200, p50: 1, p95: 2, p99: 3 },
                        issues: [],
                    },
                ],
            },
        })),
    };
    return { point, policy, verification };
}
const project = (f) => projectCapacityBoundary(f);
const pressure = (f, index = 0) => {
    const row = f.verification.rows[index];
    row.status = "invalid";
    row.issues = ["HTTP_QUALITY_FAILED"];
    row.httpSummary.runs[0].issues = ["measurement:driver-pressure-observed"];
    f.verification.valid = false;
};
test("complete mixed valid and driver-quality-invalid arms allow only a case stop with original validity retained", () => {
    const f = fixture();
    assert.equal(project(f).action, "continue");
    pressure(f);
    const result = project(f);
    assert.equal(result.action, "stop-case");
    assert.equal(result.rawQualityValid, false);
    assert.equal(result.cleanupProven, false);
    assert.equal(result.targetCapacityUpperBound, "unknown");
    assert.equal(result.performanceAccepted, false);
});
test("scheduler/concurrency drops are driver range observations, never a target upper bound", () => {
    const f = fixture();
    pressure(f);
    const run = f.verification.rows[0].httpSummary.runs[0];
    Object.assign(run, {
        correct: 100,
        counts: { ok: 100, "scheduler-late": 45, "concurrency-limit": 55 },
        correctCompletedInWindowPerSecond: 100,
        correctEndToEndMs: { count: 100, p50: 1, p95: 2, p99: 3 },
        issues: ["measurement:dropped-arrivals"],
    });
    assert.equal(project(f).action, "stop-case");
});
test("partial arms, missing evidence, low samples, duplicate identities and unknown issues stop the study", () => {
    for (const mutate of [
        (f) => f.verification.rows.pop(),
        (f) => {
            f.verification.rows[0].rawVerified = false;
        },
        (f) => {
            f.verification.rows[0].resourcesVerified = false;
        },
        (f) => {
            f.verification.rows[0].runId = f.verification.rows[1].runId;
        },
        (f) => {
            f.verification.rows[0].rawEvidenceSha256 = f.verification.rows[1].rawEvidenceSha256;
        },
        (f) => f.verification.globalIssues.push("EXECUTION_TERMINAL_FAILED"),
        (f) => {
            f.verification.valid = true;
        },
        (f) => {
            f.verification.rows[0].httpSummary.runs[0].correctEndToEndMs.count = 99;
        },
        (f) => f.verification.rows[0].httpSummary.runs[0].issues.push("measurement:unknown"),
        (f) => {
            f.verification.rows[0].metricQuality = { status: "unknown" };
        },
        (f) => {
            delete f.policy.continuation;
        },
    ]) {
        const f = fixture();
        pressure(f);
        mutate(f);
        assert.equal(project(f).action, "stop-study");
    }
    const f = fixture();
    f.verification.valid = false;
    assert.equal(project(f).action, "stop-study");
});
test("generic HTTP_QUALITY_FAILED cannot hide wrong answers, decoding, timeout or transport failures", () => {
    for (const outcome of [
        "wrong-answer",
        "invalid-json",
        "invalid-gzip",
        "timeout",
        "transport-error",
        "cancelled-before-send",
    ]) {
        const f = fixture();
        pressure(f);
        f.verification.rows[0].httpSummary.runs[0].counts[outcome] = 1;
        assert.equal(project(f).action, "stop-study", outcome);
    }
    const f = fixture();
    pressure(f);
    f.verification.rows[0].httpSummary.gates.correctness = "failed";
    assert.equal(project(f).action, "stop-study");
});
test("predeclared SLO uses its actual quantile and P99 needs 1000 per arm despite P50 analysis minimum", () => {
    const f = fixture();
    f.policy.slo = { endToEndQuantile: "p99", maxLatencyMs: 2, maxErrorRate: 0, maxTimeoutRate: 0 };
    assert.equal(project(f).action, "stop-study");
    for (const row of f.verification.rows) {
        const run = row.httpSummary.runs[0];
        Object.assign(run, { correct: 1000, counts: { ok: 1000 }, correctCompletedInWindowPerSecond: 1000 });
        run.correctEndToEndMs.count = 1000;
    }
    assert.equal(project(f).action, "stop-case");
    f.policy.slo.maxLatencyMs = 3;
    assert.equal(project(f).action, "continue");
});
test("optional new policy is strict and omission has no injected fields", async () => {
    const policy = JSON.parse(await readFile(new URL("../examples/http-capacity-policy.json", import.meta.url)));
    assert.deepEqual(httpCapacityPolicySchema.parse(policy), policy);
    assert.equal(Object.hasOwn(httpCapacityPolicySchema.parse(policy), "continuation"), false);
    const continuation = {
        schemaVersion: 1,
        mode: "verified-case-range",
        maxVerificationMilliseconds: 360000,
        maxVerificationBytes: 2 * 1024 ** 2,
    };
    assert(httpCapacityPolicySchema.safeParse({ ...policy, continuation }).success);
    for (const changed of [
        { ...continuation, schemaVersion: 2 },
        { ...continuation, maxVerificationBytes: 1 },
        { ...continuation, retry: true },
    ])
        assert.equal(httpCapacityPolicySchema.safeParse({ ...policy, continuation: changed }).success, false);
});
test(
    "public parent planner freezes new continuation and its work/time bounds while legacy still verifies",
    { timeout: 90000 },
    async (t) => {
        const f = await missingExploration(t),
            path = join(f.root, "policy.json"),
            policy = JSON.parse(await readFile(path));
        const legacy = await verifyHttpCapacityPlan(join(f.root, "exploration"));
        assert.equal(Object.hasOwn(legacy.plan.policy, "continuation"), false);
        policy.continuation = {
            schemaVersion: 1,
            mode: "verified-case-range",
            maxVerificationMilliseconds: 360000,
            maxVerificationBytes: 2 * 1024 ** 2,
        };
        await writeFile(path, JSON.stringify(policy));
        await assert.rejects(
            planHttpCapacity(join(f.root, "project.json"), path, { out: join(f.root, "short-controller") }),
            /allowance|cleanup/i,
        );
        policy.controllerBudgets.perPointMilliseconds = 1140000;
        await writeFile(path, JSON.stringify(policy));
        await assert.rejects(
            planHttpCapacity(join(f.root, "project.json"), path, { out: join(f.root, "short-clean-checks") }),
            /allowance|cleanup/i,
        );
        policy.controllerBudgets.perPointMilliseconds = 1160000;
        await writeFile(path, JSON.stringify(policy));
        const out = join(f.root, "new-policy"),
            built = await planHttpCapacity(join(f.root, "project.json"), path, { out });
        const checked = await verifyHttpCapacityPlan(out);
        assert.equal(checked.planDigest, built.planDigest);
        assert.equal(checked.plan.stopRules.implementation, "verified-case-range-v1");
        assert.equal(checked.plan.stopRules.localStopScope, "case-all-selected-modes");
        assert.equal(checked.plan.stopRules.confirmation, "whole-study-stop");
        assert.equal(checked.plan.estimate.continuation.perPointOwnedCleanupCheckMilliseconds, 30000);
        assert.equal(checked.plan.estimate.continuation.perPointDeclaredUnitCheckMilliseconds, 20000);
        assert.equal(
            checked.plan.budgets.reservedEvidenceBytes - legacy.plan.budgets.reservedEvidenceBytes,
            11 * 2 * 1024 ** 2,
        );
        assert.equal((await verifyHttpCapacityPlan(join(f.root, "exploration"))).planDigest, legacy.planDigest);
        const planFile = join(out, "capacity-plan.json"),
            changed = JSON.parse(await readFile(planFile));
        changed.stopRules.implementation = "legacy-stop-study";
        const { planDigest, ...payload } = changed;
        changed.planDigest = hash(payload);
        await writeFile(planFile, JSON.stringify(changed, null, 2) + "\n");
        const files = (await capacityInventory(out, 256 * 1024 ** 2)).filter(
            (file) => file.path !== "capacity.lock.json",
        );
        await writeFile(
            join(out, "capacity.lock.json"),
            JSON.stringify(
                { schemaVersion: 1, kind: "http-capacity-plan-lock", files, contentDigest: hash(files) },
                null,
                2,
            ) + "\n",
        );
        await assert.rejects(verifyHttpCapacityPlan(out), { code: "HTTP_CAPACITY_PLAN_SEMANTICS" });
    },
);

test(
    "fixed boundary command independently replays fresh localhost dropped arrivals under a public full parent",
    { timeout: 90000 },
    async (t) => {
        const f = await missingExploration(t),
            context = await localFixtureContext(t),
            read = async (path) => JSON.parse(await readFile(path)),
            put = (path, value) => writeFile(path, JSON.stringify(value, null, 2) + "\n");
        // Real delayed localhost responses generate admission drops. Runtime/ownership remains an explicit synthetic protocol fixture.
        context.server.removeAllListeners("request");
        context.server.on("request", (request, response) => {
            request.resume();
            setTimeout(() => {
                response.setHeader("Content-Type", "application/json");
                response.end('{"status":{"code":200},"result":{"data":[42]}}');
            }, 5);
        });
        const settings = {
                ...context.settings,
                mode: "forward",
                purpose: "calibration",
                budget: { ...context.settings.budget, wallSeconds: 2467 },
            },
            runtime = await read(join(f.root, "runtime.json")),
            policy = await read(join(f.root, "policy.json"));
        for (const [name, value] of Object.entries({
            settings,
            environment: context.shared.environment,
            build: context.shared.build,
        }))
            await put(join(f.root, name + ".json"), value);
        Object.assign(policy, {
            rates: [500],
            exploration: { warmupMs: 0, durationMs: 1500, pairs: 2 },
            http: { ...policy.http, responseEncoding: "identity", maxInFlight: 1, requestTimeoutMs: 500, drainMs: 500 },
            continuation: {
                schemaVersion: 1,
                mode: "verified-case-range",
                maxVerificationMilliseconds: 360000,
                maxVerificationBytes: 2 * 1024 ** 2,
            },
            controllerBudgets: { ...policy.controllerBudgets, perPointMilliseconds: 1160000 },
        });
        await put(join(f.root, "policy.json"), policy);
        const parent = join(f.root, "fresh-boundary-parent"),
            built = await planHttpCapacity(join(f.root, "project.json"), join(f.root, "policy.json"), { out: parent }),
            point = built.points.find((row) => row.caseId === "scalar-add"),
            execution = join(f.root, "fresh-boundary-execution");
        await emitPairedFixture({
            root: f.root,
            planDirectory: join(parent, point.comparisonDirectory),
            settings,
            runtimeSettings: {
                sourceRoot: resolve("."),
                nodeHome: dirname(dirname(process.execPath)),
                driver: runtime.driver,
                collector: runtime.collector,
            },
            executionDirectory: execution,
            executionId: randomBytes(16).toString("hex"),
            timeBaseNs: 0,
        });
        const output = join(f.root, "boundary-receipt.json");
        await promisify(execFile)(
            process.execPath,
            [
                resolve("components/http/capacity-evidence.mjs"),
                "__boundary",
                "--plan",
                parent,
                "--point",
                point.id,
                "--execution",
                execution,
                "--out",
                output,
                "--max-bytes",
                String(2 * 1024 ** 2),
            ],
            { timeout: 30000, maxBuffer: 1024 ** 2 },
        );
        const result = await read(output);
        assert.equal(result.action, "stop-case", JSON.stringify(result));
        assert.equal(result.rawQualityValid, false);
        assert.equal(result.ownedContainers.length, 8);
        assert.equal(new Set(result.ownedContainers.map((row) => row.id)).size, 8);
        assert.equal(result.capacityPlanDigest, built.planDigest);
        assert.equal(result.cleanupProven, false);
        assert.equal(result.targetCapacityUpperBound, "unknown");
    },
);
