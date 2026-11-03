/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdir, mkdtemp, rm, readFile, writeFile, cp, access } from "node:fs/promises";
import { join, resolve } from "node:path";
import { tmpdir } from "node:os";
import { performance } from "node:perf_hooks";
import { bytesDigest } from "../components/http/common.mjs";
import { freezeCalibration } from "../components/http/calibration-plan.mjs";
import { reportCurrentAaContribution } from "../components/http/engineering-aa-current.mjs";
const MiB = 1024 ** 2;
const put = (p, v) => writeFile(p, JSON.stringify(v, null, 2) + "\n");
const read = async (p) => JSON.parse(await readFile(p, "utf8"));
const absent = async (p) => {
    await assert.rejects(access(p), { code: "ENOENT" });
};
async function fixture(t) {
    const root = await mkdtemp(join(tmpdir(), "hgbench-current-aa-"));
    t.after(() => rm(root, { recursive: true, force: true }));
    const httpPlanDirectory = join(root, "http");
    await mkdir(httpPlanDirectory);
    await cp(resolve("benchmarks/http-serving-v0.1"), join(httpPlanDirectory, "benchmark"), { recursive: true });
    const catalog = await read(join(httpPlanDirectory, "benchmark/catalog.json")),
        answers = await read(join(httpPlanDirectory, "benchmark/answers.json")),
        settings = await read(resolve("examples/http-suite-functional.json")),
        { caseIds, purpose, ...base } = settings;
    const profile = {
        ...base,
        kind: "http-benchmark-profile",
        serviceChecks: true,
        target: {
            label: "current AA protocol fixture",
            build: "fixture",
            mode: "forward",
            environment: "inert fixture; no deployed target",
            evidenceFiles: [],
        },
        warmupMs: 0,
        durationMs: 100,
        requestTimeoutMs: 100,
        drainMs: 100,
        loads: [{ id: "open-10", model: "open", ratePerSecond: 10, maxInFlight: 4, maxStartDelayMs: 100 }],
        cases: [
            {
                id: "scalar-add",
                body: catalog.cases.find((c) => c.id === "scalar-add").body,
                expectedStatus: 200,
                checks: [
                    { pointer: "/status/code", equals: 200 },
                    {
                        pointer: "/result/data",
                        canonicalSha256: answers.cases.find((c) => c.id === "scalar-add").canonicalSha256,
                    },
                ],
            },
        ],
        benchmark: {
            suiteId: catalog.suiteId,
            version: catalog.version,
            bundleSha256: bytesDigest(await readFile(join(httpPlanDirectory, "benchmark/bundle.lock.json"))),
            caseIds: ["scalar-add"],
            coverage: "subset",
            purpose: "calibration",
        },
    };
    await put(join(httpPlanDirectory, "profile.json"), profile);
    await put(join(httpPlanDirectory, "target-evidence.json"), {
        schemaVersion: 1,
        kind: "operator-supplied-target-evidence",
        files: [],
        attestation: "Protocol fixture only",
    });
    const sharedEvidence = {};
    for (const role of ["build", "settings", "environment"]) {
        const path = join(root, role + ".json");
        await put(path, { schemaVersion: 1, kind: "fixture-" + role, realTargetVerified: false });
        sharedEvidence[role] = { path: role + ".json", sha256: bytesDigest(await readFile(path)) };
    }
    const comparison = {
        schemaVersion: 2,
        kind: "http-comparison-request",
        id: "current-aa-fixture",
        purpose: "calibration",
        comparison: "aa",
        modes: { A: "forward", B: "forward" },
        blockIds: ["b1", "b2", "b3", "b4"],
        orderSeed: "fixture",
        metricRoles: { latency: "inference", goodput: "quality" },
        sharedEvidence,
        analysis: {
            familyAlpha: 0.05,
            minimumPairs: 4,
            bootstrapReplicates: 20000,
            minimumTailDraws: 20,
            maxWork: 1000000000,
            maxMilliseconds: 1000,
            resampleSeed: "fixture",
            latency: { relativeBudget: 0.05, absoluteBudget: 1, minimumObservations: 1000 },
            goodput: { relativeBudget: 0.05, absoluteBudget: 1, minimumObservations: 1000 },
        },
        budgets: {
            prepareMs: 100,
            startMs: 1000,
            modeProbeMs: 30000,
            settleMs: 0,
            stopMs: 1000,
            sealAndAnalyzeMs: 1000,
            maxEvidenceBytesPerArm: 8 * MiB,
            maxTotalEvidenceBytes: 512 * MiB,
            maxTotalMilliseconds: 7200000,
        },
    };
    const request = {
        schemaVersion: 2,
        kind: "http-calibration-request",
        id: "current-aa-cohort",
        purpose: "calibration",
        scenario: "repeatability-aa",
        comparison,
        experiments: [
            { id: "e1", orderSeed: "o1", resampleSeed: "r1" },
            { id: "e2", orderSeed: "o2", resampleSeed: "r2" },
        ],
        assessment: { confidence: 0.95, maximumFamilyFalsePositiveRate: 0.05, minimumExperiments: 20 },
        acceptancePolicy: { id: "engineering-v1", empiricalFalsePositiveCertification: "optional" },
        budgets: { maxPlanBytes: 64 * MiB, maxReportBytes: 128 * MiB, maxReportMilliseconds: 300000 },
    };
    const inputFile = join(root, "request.json"),
        planDirectory = join(root, "plan");
    await put(inputFile, request);
    const planned = await freezeCalibration({ inputFile, httpPlanDirectory, outputDirectory: planDirectory });
    const executionSetFile = join(root, "execution-set.json");
    await put(executionSetFile, {
        schemaVersion: 1,
        kind: "http-calibration-execution-set",
        planDigest: planned.planDigest,
        experiments: [],
    });
    const reference = {
            planDirectory,
            planLockSha256: bytesDigest(await readFile(join(planDirectory, "plan.lock.json"))),
            executionSetFile,
            executionSetSha256: bytesDigest(await readFile(executionSetFile)),
        },
        outputDirectory = join(root, "review");
    const run = (extra = {}) =>
        reportCurrentAaContribution({
            reference,
            outputDirectory,
            deadline: performance.now() + request.budgets.maxReportMilliseconds + 20000,
            maxBytes: request.budgets.maxReportBytes + MiB,
            ...extra,
        });
    return { root, reference, outputDirectory, request, run };
}
test("current public calibration plan/report retains both missing experiments, original optional FPR and real scoped parameters", async (t) => {
    const f = await fixture(t),
        result = await f.run();
    assert.equal(result.id, "repeatabilityAa");
    assert.equal(result.rawVerified, false);
    assert.equal(result.status, "unknown");
    assert.deepEqual(result.gate.experimentIds, ["e1", "e2"]);
    assert.equal(result.empiricalCertification.required, false);
    assert.equal(result.empiricalCertification.accepted, false);
    assert.equal(result.empiricalCertification.unknownExperiments, 2);
    assert.deepEqual(result.scope.caseIds, ["scalar-add"]);
    assert.deepEqual(result.scope.ratesPerSecond, [10]);
    assert.equal(result.scope.physicalArms, 16);
    assert.equal(result.conditions.profiles.length, 4);
    assert.equal(result.performanceAccepted, false);
    const summary = await read(join(f.outputDirectory, "report/summary.json"));
    assert.deepEqual(
        summary.experiments.map((e) => e.status),
        ["missing", "missing"],
    );
    const before = await readFile(join(f.outputDirectory, "contribution.lock.json"));
    await assert.rejects(f.run(), { code: "EEXIST" });
    assert.deepEqual(await readFile(join(f.outputDirectory, "contribution.lock.json")), before);
});
test("reference identity, time reserve and output reserve reject before creating any output", async (t) => {
    const f = await fixture(t);
    await assert.rejects(f.run({ reference: { ...f.reference, planLockSha256: "0".repeat(64) } }), {
        code: "CURRENT_AA_REFERENCE_DIGEST",
    });
    await absent(f.outputDirectory);
    await assert.rejects(f.run({ deadline: performance.now() + f.request.budgets.maxReportMilliseconds }), {
        code: "CURRENT_AA_REPORT_TIME_RESERVE",
    });
    await absent(f.outputDirectory);
    await assert.rejects(f.run({ maxBytes: f.request.budgets.maxReportBytes }), {
        code: "CURRENT_AA_REPORT_BYTE_RESERVE",
    });
    await absent(f.outputDirectory);
});
test("a pre-cancelled current report cannot create output", async (t) => {
    const f = await fixture(t);
    await assert.rejects(f.run({ signal: AbortSignal.abort() }), { code: "CURRENT_AA_CANCELLED" });
    await absent(f.outputDirectory);
});
test("actual fixed report process is cancelled and reaped with a retained partial command and no contribution lock", async (t) => {
    const f = await fixture(t),
        abort = new AbortController();
    const originalAdd = abort.signal.addEventListener.bind(abort.signal);
    // The fixed worker installs this listener immediately after spawn. Triggering
    // here tests actual process cancellation without a CPU-dependent startup sleep.
    Object.defineProperty(abort.signal, "addEventListener", {
        value: (...args) => {
            originalAdd(...args);
            queueMicrotask(() => abort.abort());
        },
    });
    await assert.rejects(f.run({ signal: abort.signal }), { code: "CURRENT_AA_CANCELLED" });
    const command = await read(join(f.outputDirectory, "command.json"));
    assert.equal(command.failure, "CURRENT_AA_CANCELLED");
    assert(Number.isSafeInteger(command.pid));
    assert.throws(() => process.kill(command.pid, 0), { code: "ESRCH" });
    await absent(join(f.outputDirectory, "contribution.lock.json"));
});
