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
import { mkdtemp, readFile, writeFile, mkdir, rm, cp, symlink } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import {
    freezeCalibration,
    verifyCalibration,
    validateCalibrationRequest,
    experimentRequest,
    describe,
} from "../components/http/calibration-plan.mjs";
import {
    assessExperiment,
    assessCohort,
    assessAcceptance,
    binomialInterval,
    reportCalibration,
} from "../components/http/calibration-evidence.mjs";
import { bytesDigest } from "../components/http/common.mjs";
import { valueHash } from "../components/paired/core.mjs";
const run = promisify(execFile);
const read = async (path) => JSON.parse(await readFile(path, "utf8"));
const put = (path, value) => writeFile(path, JSON.stringify(value, null, 2) + "\n");
const MiB = 1024 * 1024;
async function fixture(t) {
    const root = await mkdtemp(join(tmpdir(), "hgbench-comparison-plan-"));
    t.after(() => rm(root, { recursive: true, force: true }));
    const httpPlanDirectory = join(root, "http-plan");
    await mkdir(httpPlanDirectory);
    await cp(resolve("benchmarks/http-serving-v0.1"), join(httpPlanDirectory, "benchmark"), { recursive: true });
    const catalog = await read(join(httpPlanDirectory, "benchmark/catalog.json"));
    const answers = await read(join(httpPlanDirectory, "benchmark/answers.json"));
    const settings = await read(resolve("examples/http-suite-functional.json"));
    const { caseIds: ignored, purpose, ...base } = settings;
    const selected = ["scalar-add", "string-0001k"];
    const profile = {
        ...base,
        kind: "http-benchmark-profile",
        serviceChecks: true,
        target: {
            label: "fixture",
            build: "fixture-build",
            mode: "forward",
            environment: "fixture",
            evidenceFiles: [],
        },
        warmupMs: 0,
        durationMs: 100,
        requestTimeoutMs: 100,
        drainMs: 100,
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
        loads: [
            { id: "open-2", model: "open", ratePerSecond: 2, maxInFlight: 1, maxStartDelayMs: 100 },
            { id: "closed-1", model: "closed", concurrency: 1 },
        ],
        benchmark: {
            suiteId: catalog.suiteId,
            version: catalog.version,
            bundleSha256: bytesDigest(await readFile(join(httpPlanDirectory, "benchmark/bundle.lock.json"))),
            caseIds: selected,
            coverage: "subset",
            purpose: "calibration",
        },
    };
    await put(join(httpPlanDirectory, "profile.json"), profile);
    await put(join(httpPlanDirectory, "target-evidence.json"), {
        schemaVersion: 1,
        kind: "operator-supplied-target-evidence",
        files: [],
        attestation: "Fixture only",
    });
    const sharedEvidence = {};
    for (const role of ["build", "environment", "settings"]) {
        const path = join(root, `${role}.json`);
        await put(path, { schemaVersion: 1, kind: `fixture-${role}`, realTargetVerified: false });
        sharedEvidence[role] = { path: `${role}.json`, sha256: bytesDigest(await readFile(path)) };
    }
    const request = {
        schemaVersion: 1,
        kind: "http-comparison-request",
        id: "fixture-pairs-v1",
        purpose: "calibration",
        comparison: "aa",
        modes: { A: "forward", B: "forward" },
        blockIds: ["b01", "b02", "b03", "b04"],
        orderSeed: "fixture-order-v1",
        sharedEvidence,
        analysis: {
            familyAlpha: 0.05,
            minimumPairs: 4,
            bootstrapReplicates: 20000,
            minimumTailDraws: 20,
            maxWork: 1000000000,
            maxMilliseconds: 60000,
            resampleSeed: "fixture-resample-v1",
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
            maxEvidenceBytesPerArm: 8 * 1024 * 1024,
            maxTotalEvidenceBytes: 512 * 1024 * 1024,
            maxTotalMilliseconds: 7200000,
        },
    };
    const inputFile = join(root, "request.json");
    await put(inputFile, request);
    return {
        root,
        profile,
        request,
        inputFile,
        httpPlanDirectory,
        outputDirectory: join(root, "comparison"),
        save: () => put(inputFile, request),
        saveProfile: () => put(join(httpPlanDirectory, "profile.json"), profile),
    };
}

function request(template) {
    return {
        schemaVersion: 1,
        kind: "http-calibration-request",
        id: "aa-cohort",
        purpose: "calibration",
        scenario: "repeatability-aa",
        comparison: template,
        experiments: [
            { id: "experiment-1", orderSeed: "order-1", resampleSeed: "resample-1" },
            { id: "experiment-2", orderSeed: "order-2", resampleSeed: "resample-2" },
        ],
        assessment: { confidence: 0.95, maximumFamilyFalsePositiveRate: 0.05, minimumExperiments: 20 },
        budgets: { maxPlanBytes: 64 * MiB, maxReportBytes: 128 * MiB, maxReportMilliseconds: 400000 },
    };
}
async function calibration(t) {
    const f = await fixture(t),
        input = request(f.request),
        inputFile = join(f.root, "calibration-request.json");
    await put(inputFile, input);
    return { ...f, input, inputFile, outputDirectory: join(f.root, "calibration") };
}
function qualifiedCell(relation = "within-at-least-one-budget") {
    const cell = {
        metric: { id: "latency", relativeBudget: 0.05, absoluteBudget: 1 },
        issues: [],
        warnings: [],
        point: { relativeMedian: 0, absoluteMedian: 0 },
        bootstrapIntervals: { relative: { low: -0.01, high: 0.01 }, absolute: { low: -0.1, high: 0.1 } },
        budgetRelation: relation,
    };
    if (relation === "crosses-budget")
        cell.bootstrapIntervals = { relative: { low: -0.1, high: 0.1 }, absolute: { low: -2, high: 2 } };
    if (relation === "above-both-budgets")
        cell.bootstrapIntervals = { relative: { low: 0.1, high: 0.2 }, absolute: { low: 2, high: 3 } };
    if (relation === "below-both-budgets")
        cell.bootstrapIntervals = { relative: { low: -0.2, high: -0.1 }, absolute: { low: -3, high: -2 } };
    return cell;
}
const verification = { valid: true, statisticsEligible: true };

test("v2 freezes a strict acceptance policy; v1 rejects additional policy fields", async (t) => {
    const f = await calibration(t);
    for (const policy of [
        { id: "engineering-v1", empiricalFalsePositiveCertification: "optional", passed: true },
        { id: "engineering-v1", empiricalFalsePositiveCertification: "optional", minimumExperiments: 1 },
        { id: "engineering-v1", empiricalFalsePositiveCertification: "disabled" },
        { id: "engineering-v2", empiricalFalsePositiveCertification: "optional" },
        { id: "engineering-v1" },
    ])
        assert.throws(() => validateCalibrationRequest({ ...f.input, schemaVersion: 2, acceptancePolicy: policy }));
    assert.throws(() =>
        validateCalibrationRequest({
            ...f.input,
            acceptancePolicy: { id: "engineering-v1", empiricalFalsePositiveCertification: "optional" },
        }),
    );
    f.input.schemaVersion = 2;
    f.input.acceptancePolicy = { id: "engineering-v1", empiricalFalsePositiveCertification: "optional" };
    await put(f.inputFile, f.input);
    const compiled = await freezeCalibration(f);
    const planPath = join(f.outputDirectory, "plan.json"),
        plan = await read(planPath);
    assert.equal(plan.schemaVersion, 2);
    assert.deepEqual(plan.acceptancePolicy, f.input.acceptancePolicy);
    assert.deepEqual(plan.engineeringRequirements, { repeatabilityAaMinimumExperiments: 2 });
    assert.equal((await verifyCalibration(f.outputDirectory)).planDigest, compiled.planDigest);
    plan.acceptancePolicy.empiricalFalsePositiveCertification = "required";
    await put(planPath, plan);
    await assert.rejects(verifyCalibration(f.outputDirectory), /CONTENT_CHANGED/);
    // Re-sealing the ordinary content lock cannot substitute a policy that differs from the original request.
    const lockPath = join(f.outputDirectory, "plan.lock.json"),
        lock = await read(lockPath);
    const row = lock.files.find((x) => x.path === "plan.json"),
        raw = await readFile(planPath);
    Object.assign(row, { bytes: raw.length, sha256: bytesDigest(raw) });
    lock.contentDigest = valueHash(lock.files);
    await put(lockPath, lock);
    await assert.rejects(verifyCalibration(f.outputDirectory), /DERIVED_PLAN_CHANGED/);
    f.input.acceptancePolicy.empiricalFalsePositiveCertification = "required";
    await put(f.inputFile, f.input);
    const requiredDirectory = join(f.root, "required-plan");
    const required = await freezeCalibration({ ...f, outputDirectory: requiredDirectory });
    assert.notEqual(required.planDigest, compiled.planDigest);
    assert.equal((await verifyCalibration(requiredDirectory)).planDigest, required.planDigest);
    const requiredPlan = await read(join(requiredDirectory, "plan.json"));
    requiredPlan.engineeringRequirements.repeatabilityAaMinimumExperiments = 1;
    await put(join(requiredDirectory, "plan.json"), requiredPlan);
    const requiredLock = await read(join(requiredDirectory, "plan.lock.json")),
        requiredRaw = await readFile(join(requiredDirectory, "plan.json"));
    Object.assign(
        requiredLock.files.find((x) => x.path === "plan.json"),
        { bytes: requiredRaw.length, sha256: bytesDigest(requiredRaw) },
    );
    requiredLock.contentDigest = valueHash(requiredLock.files);
    await put(join(requiredDirectory, "plan.lock.json"), requiredLock);
    await assert.rejects(verifyCalibration(requiredDirectory), /DERIVED_PLAN_CHANGED/);
});

test("v2 AA engineering contribution is independent of optional empirical certification and retains unconnected gates", () => {
    const plan = {
        schemaVersion: 2,
        experiments: [{ id: "e1" }, { id: "e2" }],
        engineeringRequirements: { repeatabilityAaMinimumExperiments: 2 },
        acceptancePolicy: { id: "engineering-v1", empiricalFalsePositiveCertification: "optional" },
        familyFalsePositiveDefinition: "predeclared-practical-change",
    };
    const experiments = ["e1", "e2"].map((id) => ({
        id,
        ...assessExperiment(verification, { cells: [qualifiedCell()] }),
    }));
    const cohort = assessCohort(experiments, {
        confidence: 0.95,
        maximumFamilyFalsePositiveRate: 0.05,
        minimumExperiments: 2,
    });
    const result = assessAcceptance(plan, experiments, cohort);
    assert.equal(result.engineering.gates[0].status, "passed");
    assert.equal(result.engineering.gates[0].minimumExperiments, 2);
    assert.equal(result.engineering.gates[0].plannedExperiments, 2);
    assert.equal(result.empiricalCertification.status, "uncertain");
    assert.equal(result.empiricalCertification.required, false);
    assert.equal(result.empiricalCertification.accepted, false);
    assert.equal(result.engineering.status, "unknown");
    assert.equal(result.engineering.accepted, false);
    assert.equal(result.policyAccepted, false);
    assert.equal(result.engineering.gates.filter((g) => g.status === "unknown").length, 6);
    assert(!result.policyBlockingReasons.includes("empirical-false-positive-certification"));
    const required = assessAcceptance(
        { ...plan, acceptancePolicy: { ...plan.acceptancePolicy, empiricalFalsePositiveCertification: "required" } },
        experiments,
        cohort,
    );
    assert.deepEqual(required.engineering, result.engineering);
    assert.equal(required.empiricalCertification.required, true);
    assert(required.policyBlockingReasons.includes("empirical-false-positive-certification"));
    assert.deepEqual(assessAcceptance({ schemaVersion: 1 }, experiments, cohort), {});
});

test("v2 meaningful A/A change fails while uncertain, degenerate and missing experiments remain unknown", () => {
    const plan = {
        schemaVersion: 2,
        experiments: [{ id: "e1" }],
        engineeringRequirements: { repeatabilityAaMinimumExperiments: 2 },
        acceptancePolicy: { id: "engineering-v1", empiricalFalsePositiveCertification: "optional" },
    };
    const assessment = { confidence: 0.95, maximumFamilyFalsePositiveRate: 0.05, minimumExperiments: 1 };
    for (const [cell, expected] of [
        [qualifiedCell("above-both-budgets"), "failed"],
        [qualifiedCell("below-both-budgets"), "failed"],
        [qualifiedCell("crosses-budget"), "unknown"],
        [{ ...qualifiedCell(), warnings: ["degenerate"] }, "unknown"],
    ]) {
        const experiments = [{ id: "e1", ...assessExperiment(verification, { cells: [cell] }) }];
        const value = assessAcceptance(plan, experiments, assessCohort(experiments, assessment));
        assert.equal(value.engineering.gates[0].status, expected);
        assert.equal(value.engineering.accepted, false);
    }
    const missing = [{ id: "e1", ...assessExperiment(null, null, ["EXECUTION_MISSING"]) }];
    assert.equal(
        assessAcceptance(plan, missing, assessCohort(missing, assessment)).engineering.gates[0].status,
        "unknown",
    );
    const equivalent = { id: "e1", ...assessExperiment(verification, { cells: [qualifiedCell()] }) };
    assert.equal(
        assessAcceptance(plan, [equivalent, equivalent], assessCohort([equivalent, equivalent], assessment)).engineering
            .gates[0].status,
        "unknown",
    );
    const unexpected = {
        id: "unexpected",
        ...assessExperiment(verification, { cells: [qualifiedCell("above-both-budgets")] }),
    };
    assert.equal(
        assessAcceptance(plan, [unexpected], assessCohort([unexpected], assessment)).engineering.gates[0].status,
        "unknown",
    );
    const single = assessAcceptance(plan, [equivalent], assessCohort([equivalent], assessment));
    assert.equal(single.engineering.gates[0].status, "unknown");
    assert.equal(single.engineering.gates[0].plannedExperiments, 1);
    assert.equal(single.engineering.gates[0].minimumExperiments, 2);
    const two = [equivalent, { ...equivalent, id: "e2" }];
    const engineeringRepeat = assessAcceptance(
        { ...plan, experiments: [{ id: "e1" }, { id: "e2" }] },
        two,
        assessCohort(two, { ...assessment, minimumExperiments: 100 }),
    );
    assert.equal(engineeringRepeat.engineering.gates[0].status, "passed");
    assert.equal(engineeringRepeat.empiricalCertification.status, "insufficient-evidence");
    assert.throws(
        () =>
            assessAcceptance(
                { ...plan, engineeringRequirements: { repeatabilityAaMinimumExperiments: 1 } },
                [equivalent],
                assessCohort([equivalent], assessment),
            ),
        /CALIBRATION_ENGINEERING_REQUIREMENTS/,
    );
});

test("v2 empirical certification can meet its bound without accepting unconnected engineering gates", () => {
    const experiments = Array.from({ length: 72 }, (_, i) => ({
        id: `e${i}`,
        ...assessExperiment(verification, { cells: [qualifiedCell()] }),
    }));
    const plan = {
        schemaVersion: 2,
        experiments: experiments.map(({ id }) => ({ id })),
        engineeringRequirements: { repeatabilityAaMinimumExperiments: 2 },
        acceptancePolicy: { id: "engineering-v1", empiricalFalsePositiveCertification: "required" },
    };
    const result = assessAcceptance(
        plan,
        experiments,
        assessCohort(experiments, { confidence: 0.95, maximumFamilyFalsePositiveRate: 0.05, minimumExperiments: 72 }),
    );
    assert.equal(result.empiricalCertification.accepted, true);
    assert.equal(result.engineering.accepted, false);
    assert.equal(result.policyAccepted, false);
    assert(!result.policyBlockingReasons.includes("empirical-false-positive-certification"));
});

test("AA calibration creates independent strict child plans and verifies from frozen snapshots", async (t) => {
    const f = await calibration(t),
        result = await freezeCalibration(f);
    assert.equal(result.experiments.length, 2);
    assert.equal(result.qualificationAccepted, false);
    for (const value of [result, await read(join(f.outputDirectory, "plan.json"))])
        for (const key of [
            "acceptancePolicy",
            "engineeringRequirements",
            "engineering",
            "empiricalCertification",
            "policyAccepted",
            "policyBlockingReasons",
        ])
            assert(!Object.hasOwn(value, key), `v1 plan must not add ${key}`);
    assert.notEqual(result.experiments[0].planDigest, result.experiments[1].planDigest);
    const verified = await verifyCalibration(f.outputDirectory);
    assert.equal(verified.planDigest, result.planDigest);
    for (const child of result.experiments) {
        const plan = await read(join(f.outputDirectory, child.directory, "plan.json"));
        assert.equal(plan.comparison, "aa");
        assert.equal(plan.variants.A.mode, plan.variants.B.mode);
        assert.equal(plan.variants.A.variantDigest, plan.variants.B.variantDigest);
    }
    await assert.rejects(freezeCalibration(f), /EEXIST/);
});
test("strict scenario rejects formal AB, mode drift, overrides, IDs and insufficient supervision budgets", async (t) => {
    const f = await calibration(t);
    for (const mutate of [
        (x) => (x.purpose = "functional"),
        (x) => (x.scenario = "collector-cost"),
        (x) => (x.comparison.comparison = "forward-embedded"),
        (x) => (x.comparison.modes.B = "embedded"),
        (x) => (x.experiments[0].extra = "override"),
        (x) => (x.experiments[1].id = x.experiments[0].id),
        (x) => (x.id = null),
        (x) => (x.experiments[0].id = "../escape"),
        (x) => (x.budgets.maxReportMilliseconds = 1000),
        (x) => (x.budgets.maxReportBytes = MiB),
        (x) => (x.assessment.confidence = 1),
    ]) {
        const altered = structuredClone(f.input);
        mutate(altered);
        assert.throws(() => validateCalibrationRequest(altered));
    }
});
test("only declared experiment identifiers and seeds may differ in compiled comparison requests", async (t) => {
    const f = await calibration(t),
        a = experimentRequest(f.input, f.input.experiments[0]),
        b = experimentRequest(f.input, f.input.experiments[1]);
    b.id = a.id;
    b.orderSeed = a.orderSeed;
    b.analysis.resampleSeed = a.analysis.resampleSeed;
    assert.deepEqual(a, b);
    assert.equal(a.sharedEvidence.build.path, "inputs/build.json");
});
test("report reserve independently includes statistical and JVM replay work even if a child seal is too small", async (t) => {
    for (const jvm of [false, true]) {
        const f = await calibration(t);
        f.input.budgets.maxReportMilliseconds = jvm ? 400000 : 240000;
        if (jvm) {
            const path = join(f.root, "settings.json"),
                settings = await read(path);
            settings.jvmObservation = { parser: { timeoutMs: 30000 } };
            await put(path, settings);
            f.input.comparison.sharedEvidence.settings.sha256 = bytesDigest(await readFile(path));
        }
        await put(f.inputFile, f.input);
        await assert.rejects(freezeCalibration(f), /CALIBRATION_REPORT_REPLAY_TIME_RESERVE/);
    }
});
test("updated hashes do not permit semantic AA plan or child settings forgery", async (t) => {
    const f = await calibration(t);
    await freezeCalibration(f);
    const plan = await read(join(f.outputDirectory, "plan.json"));
    plan.mode = "embedded";
    await put(join(f.outputDirectory, "plan.json"), plan);
    const lock = await read(join(f.outputDirectory, "plan.lock.json"));
    const raw = await readFile(join(f.outputDirectory, "plan.json"));
    Object.assign(
        lock.files.find((x) => x.path === "plan.json"),
        { bytes: raw.length, sha256: bytesDigest(raw) },
    );
    lock.contentDigest = valueHash(lock.files);
    await put(join(f.outputDirectory, "plan.lock.json"), lock);
    await assert.rejects(verifyCalibration(f.outputDirectory), /DERIVED_PLAN_CHANGED/);
});
test("additional files and symlink leaves reject calibration content verification", async (t) => {
    const f = await calibration(t);
    await freezeCalibration(f);
    const path = join(f.outputDirectory, "extra");
    await writeFile(path, "extra");
    await assert.rejects(verifyCalibration(f.outputDirectory), /CONTENT_CHANGED/);
    await rm(path);
    await symlink("request.json", path);
    await assert.rejects(verifyCalibration(f.outputDirectory), /SYMLINK/);
});
test("Clopper Pearson binomial bounds match exact endpoint formulas and known central values", () => {
    for (const n of [1, 2, 20, 100, 10000]) {
        const zero = binomialInterval(0, n, 0.95),
            all = binomialInterval(n, n, 0.95);
        assert.equal(zero.low, 0);
        assert.equal(all.high, 1);
        assert.ok(Math.abs(zero.high - (1 - Math.pow(0.025, 1 / n))) < 2e-11);
        assert.ok(Math.abs(all.low - Math.pow(0.025, 1 / n)) < 2e-11);
    }
    const central = binomialInterval(5, 10, 0.95);
    assert.ok(Math.abs(central.low - 0.1870860284) < 1e-9);
    assert.ok(Math.abs(central.high - 0.8129139716) < 1e-9);
    assert.throws(() => binomialInterval(2, 1, 0.95));
    assert.throws(() => binomialInterval(0, 0, 0.95));
});
test("equivalent and uncertain experiments are not practical-change family false positives", () => {
    const equivalent = assessExperiment(verification, { cells: [qualifiedCell()] });
    assert.equal(equivalent.state, "equivalent-within-declared-budgets");
    assert.equal(equivalent.familyFalsePositive, false);
    const uncertain = assessExperiment(verification, { cells: [qualifiedCell("crosses-budget")] });
    assert.equal(uncertain.state, "uncertain");
    assert.equal(uncertain.familyFalsePositive, false);
    const small = qualifiedCell();
    small.bootstrapIntervals.relative = { low: 0.001, high: 0.002 };
    const smallShift = assessExperiment(verification, { cells: [small] });
    assert.equal(smallShift.familyExcludesZero, true);
    assert.equal(smallShift.familyFalsePositive, false);
});
test("any qualified practical improvement or regression counts once at experiment family level", () => {
    for (const relation of ["above-both-budgets", "below-both-budgets"]) {
        const result = assessExperiment(verification, { cells: [qualifiedCell(), qualifiedCell(relation)] });
        assert.equal(result.familyFalsePositive, true);
        assert.equal(result.state, "false-positive-meaningful-change");
    }
});
test("bad raw, low counts, degenerate intervals and reuse remain unknown rather than negative trials", () => {
    const bad = qualifiedCell();
    bad.warnings.push("degenerate-paired-effects");
    for (const result of [
        assessExperiment({ valid: false, statisticsEligible: true }, { cells: [qualifiedCell()] }),
        assessExperiment({ valid: true, statisticsEligible: false }, { cells: [qualifiedCell()] }),
        assessExperiment(verification, { cells: [bad] }),
        assessExperiment(verification, { cells: [] }),
        assessExperiment(verification, { cells: [qualifiedCell()] }, ["REUSED_RUNID"]),
    ]) {
        assert.equal(result.state, "unknown");
        assert.equal(result.familyFalsePositive, null);
    }
});
test("missing experiments stay in planned denominator with conservative count envelope", () => {
    const result = assessCohort(
        [{ familyFalsePositive: false }, { familyFalsePositive: true }, { familyFalsePositive: null }],
        { confidence: 0.95, maximumFamilyFalsePositiveRate: 0.05, minimumExperiments: 2 },
    );
    assert.equal(result.plannedExperiments, 3);
    assert.equal(result.classifiableExperiments, 2);
    assert.equal(result.unknownExperiments, 1);
    assert.deepEqual(result.plannedFalsePositiveCountRange, [1, 2]);
    assert.equal(result.rateAssessment, "insufficient-evidence");
    assert.ok(result.plannedCohortIntervalEnvelope.low <= result.classifiableCohortInterval.low);
    assert.ok(result.plannedCohortIntervalEnvelope.high >= result.classifiableCohortInterval.high);
});
test("missing execution manifest creates locked incomplete report and never qualifies noise calibration", async (t) => {
    const f = await calibration(t),
        compiled = await freezeCalibration(f),
        inputFile = join(f.root, "execution-set.json");
    await put(inputFile, {
        schemaVersion: 1,
        kind: "http-calibration-execution-set",
        planDigest: compiled.planDigest,
        experiments: [],
    });
    const out = join(f.root, "report"),
        result = await reportCalibration({ planDirectory: f.outputDirectory, inputFile, outputDirectory: out });
    assert.equal(result.evidenceValid, false);
    assert.equal(result.classifiableExperiments, 0);
    const report = await read(join(out, "summary.json"));
    assert.equal(report.experiments.length, 2);
    assert.equal(report.cohort.unknownExperiments, 2);
    assert.equal(report.qualificationAccepted, false);
    assert.equal(report.performanceAccepted, false);
    for (const key of [
        "acceptancePolicy",
        "engineeringRequirements",
        "engineering",
        "empiricalCertification",
        "policyAccepted",
        "policyBlockingReasons",
    ])
        assert(!Object.hasOwn(report, key), `v1 report must not add ${key}`);
    assert.ok(report.experiments.every((x) => x.state === "unknown" && x.familyFalsePositive === null));
    assert.equal((await read(join(out, "evidence.lock.json"))).kind, "http-calibration-report-lock");
});
test("v2 missing executions preserve both policy axes and all planned unknowns through public CLI", async (t) => {
    const f = await calibration(t);
    f.input.schemaVersion = 2;
    f.input.acceptancePolicy = { id: "engineering-v1", empiricalFalsePositiveCertification: "optional" };
    await put(f.inputFile, f.input);
    const cli = resolve("dist/cli.js"),
        planned = join(f.root, "v2-public-plan");
    const frozen = JSON.parse(
        (
            await run(
                process.execPath,
                [cli, "http", "calibration", "plan", f.inputFile, "--http-plan", f.httpPlanDirectory, "--out", planned],
                { timeout: 30000 },
            )
        ).stdout,
    );
    assert.equal(frozen.ok, true);
    const inputFile = join(f.root, "execution-set-v2.json");
    await put(inputFile, {
        schemaVersion: 1,
        kind: "http-calibration-execution-set",
        planDigest: frozen.data.planDigest,
        experiments: [],
    });
    const out = join(f.root, "v2-public-report");
    const reply = JSON.parse(
        (
            await run(
                process.execPath,
                [cli, "http", "calibration", "report", inputFile, "--plan", planned, "--out", out],
                { timeout: 30000 },
            )
        ).stdout,
    );
    assert.equal(reply.ok, true);
    assert.equal(reply.data.engineering.accepted, false);
    assert.equal(reply.data.empiricalCertification.required, false);
    const summary = await read(join(out, "summary.json"));
    assert.equal(summary.schemaVersion, 2);
    assert.deepEqual(summary.acceptancePolicy, f.input.acceptancePolicy);
    assert.equal(summary.cohort.plannedExperiments, 2);
    assert.equal(summary.cohort.unknownExperiments, 2);
    assert.equal(summary.engineering.gates[0].status, "unknown");
    assert.equal(summary.empiricalCertification.status, "insufficient-evidence");
    assert.equal(summary.policyAccepted, false);
    assert.equal(summary.qualificationAccepted, false);
    assert.equal(summary.performanceAccepted, false);
});
test("execution set rejects unknown or duplicated IDs before starting child analyzers", async (t) => {
    const f = await calibration(t),
        compiled = await freezeCalibration(f),
        inputFile = join(f.root, "bad-set.json");
    for (const experiments of [
        [{ id: "unknown", executionDirectory: "/missing" }],
        [
            { id: "experiment-1", executionDirectory: "/missing" },
            { id: "experiment-1", executionDirectory: "/other" },
        ],
    ]) {
        await put(inputFile, {
            schemaVersion: 1,
            kind: "http-calibration-execution-set",
            planDigest: compiled.planDigest,
            experiments,
        });
        await assert.rejects(
            reportCalibration({ planDirectory: f.outputDirectory, inputFile, outputDirectory: join(f.root, "never") }),
            /CALIBRATION_EXECUTION_ID/,
        );
    }
});
async function fakeMetadata(f, compiled, options = {}) {
    const entries = [];
    for (let i = 0; i < compiled.experiments.length; i++) {
        const child = compiled.experiments[i],
            path = join(f.root, "execution-" + i);
        await mkdir(path);
        const executionId = (options.sameExecutionId ? "a" : String(i + 1)).repeat(32);
        await put(join(path, "execution.json"), {
            schemaVersion: 1,
            kind: "http-comparison-execution",
            planDigest: child.planDigest,
            executionId,
            hostBootId: "fixture-boot",
            registeredMonotonicNs: options.overlap ? "100" : String(i * 100 + 100),
            receipts: [{ runId: options.sameRun ? "same-run" : "run-" + i, path: "unused" }],
        });
        await put(join(path, "terminal.json"), {
            schemaVersion: 1,
            kind: "http-comparison-execution-terminal",
            planDigest: child.planDigest,
            executionId,
            finishedMonotonicNs: options.overlap ? "150" : String(i * 100 + 150),
        });
        await put(join(path, "execution.lock.json"), { kind: "synthetic-offline-metadata", i });
        entries.push({ id: child.id, executionDirectory: path });
    }
    return entries;
}
test("duplicate execution or physical run and temporal overlap invalidate all affected trials before bridge dispatch", async (t) => {
    for (const option of [{ sameExecutionId: true }, { sameRun: true }, { overlap: true }]) {
        const f = await calibration(t),
            compiled = await freezeCalibration(f),
            entries = await fakeMetadata(f, compiled, option);
        const inputFile = join(f.root, "execution-set.json"),
            out = join(f.root, "report");
        await put(inputFile, {
            schemaVersion: 1,
            kind: "http-calibration-execution-set",
            planDigest: compiled.planDigest,
            experiments: entries,
        });
        await reportCalibration({ planDirectory: f.outputDirectory, inputFile, outputDirectory: out });
        const report = await read(join(out, "summary.json"));
        assert.equal(report.cohort.unknownExperiments, 2);
        assert.ok(report.experiments.every((x) => x.issues.some((issue) => /REUSED_|ORDER_OR_BOOT/.test(issue))));
        await assert.rejects(readFile(join(out, "experiment-1-command.json")), /ENOENT/);
    }
});
test("realpath aliases to the same experiment are explicitly rejected without dropping a planned trial", async (t) => {
    const f = await calibration(t),
        compiled = await freezeCalibration(f),
        entries = await fakeMetadata(f, compiled);
    const alias = join(f.root, "alias");
    await symlink(entries[0].executionDirectory, alias);
    entries[1].executionDirectory = alias;
    const inputFile = join(f.root, "execution-set.json"),
        out = join(f.root, "report");
    await put(inputFile, {
        schemaVersion: 1,
        kind: "http-calibration-execution-set",
        planDigest: compiled.planDigest,
        experiments: entries,
    });
    await reportCalibration({ planDirectory: f.outputDirectory, inputFile, outputDirectory: out });
    const report = await read(join(out, "summary.json"));
    assert.ok(report.experiments.every((x) => x.issues.includes("REUSED_DIRECTORY")));
    assert.equal(report.cohort.plannedExperiments, 2);
    assert.equal(report.cohort.classifiableExperiments, 0);
});
test("moving complete input trees preserves summary and result lock bytes, while replay audit is separate", async (t) => {
    for (const variant of ["missing", "overlap", "bridge-invalid-raw"]) {
        const f = await calibration(t),
            compiled = await freezeCalibration(f);
        const entries =
            variant === "missing" ? [] : await fakeMetadata(f, compiled, { overlap: variant === "overlap" });
        for (const row of entries) row.executionDirectory = row.executionDirectory.split("/").at(-1);
        const inputFile = join(f.root, "execution-set.json");
        await put(inputFile, {
            schemaVersion: 1,
            kind: "http-calibration-execution-set",
            planDigest: compiled.planDigest,
            experiments: entries,
        });
        const first = join(f.root, "first-report");
        await reportCalibration({ planDirectory: f.outputDirectory, inputFile, outputDirectory: first });
        const moved = await mkdtemp(join(tmpdir(), "hgbench-calibration-relocated-"));
        t.after(() => rm(moved, { recursive: true, force: true }));
        await cp(f.root, join(moved, "inputs"), { recursive: true });
        const second = join(moved, "second-report");
        await reportCalibration({
            planDirectory: join(moved, "inputs/calibration"),
            inputFile: join(moved, "inputs/execution-set.json"),
            outputDirectory: second,
        });
        for (const file of ["summary.json", "plan-reference.json", "input.json", "evidence.lock.json"])
            assert.deepEqual(
                await readFile(join(first, file)),
                await readFile(join(second, file)),
                variant + ":" + file,
            );
        if (variant === "bridge-invalid-raw") {
            const audit = await read(join(first, "rebuild.lock.json"));
            assert.equal(audit.files.length, 2);
            assert.equal(audit.resultContentDigest, (await read(join(first, "evidence.lock.json"))).contentDigest);
        }
    }
});
test("standalone help and capability are agent readable, strict unknown command exits six", async () => {
    const component = resolve("components/http/calibration-plan.mjs");
    assert.match((await run(process.execPath, [component, "--help"])).stdout, /repeatability-aa/);
    const reply = JSON.parse((await run(process.execPath, [component, "describe"])).stdout);
    assert.equal(reply.kind, "http-calibration-planner-capability");
    assert.equal(describe().performanceAccepted, false);
    await assert.rejects(run(process.execPath, [component, "wrong"]), (error) => error.code === 6);
});
test("public CLI delegates real freeze verify and incomplete evidence report with absolute recovery paths", async (t) => {
    const f = await calibration(t),
        cli = resolve("dist/cli.js");
    const invoke = async (args) =>
        JSON.parse(
            (await run(process.execPath, [cli, "http", "calibration", ...args], { timeout: 30000, maxBuffer: 2 * MiB }))
                .stdout,
        );
    const description = await invoke(["describe"]);
    assert.equal(description.data.kind, "http-calibration-planner-capability");
    assert.match((await run(process.execPath, [cli, "http", "calibration", "--help"])).stdout, /repeatability-aa/);
    const frozen = await invoke(["plan", f.inputFile, "--http-plan", f.httpPlanDirectory, "--out", f.outputDirectory]);
    assert.equal(frozen.command, "http.calibration.plan");
    assert.equal(frozen.data.executed, false);
    assert.ok(frozen.nextActions.some((x) => x.argv.at(-1) === resolve(f.outputDirectory)));
    assert.equal((await invoke(["verify", f.outputDirectory])).data.planDigest, frozen.data.planDigest);
    const input = join(f.root, "missing-execution-set.json");
    await put(input, {
        schemaVersion: 1,
        kind: "http-calibration-execution-set",
        planDigest: frozen.data.planDigest,
        experiments: [],
    });
    const out = join(f.root, "cli-report");
    const report = await invoke(["report", input, "--plan", f.outputDirectory, "--out", out]);
    assert.equal(report.data.evidenceValid, false);
    assert.equal(report.data.qualificationAccepted, false);
    assert.equal(report.data.experiments, 2);
    assert.equal(report.data.classifiableExperiments, 0);
    await assert.rejects(
        invoke(["plan", f.inputFile, "--http-plan", f.httpPlanDirectory, "--out", f.outputDirectory]),
        (error) => {
            assert.equal(error.code, 6);
            const reply = JSON.parse(error.stdout);
            assert.equal(reply.ok, false);
            assert.equal(reply.error.code, "HTTP_CALIBRATION_FAILED");
            assert.ok(reply.error.details);
            return true;
        },
    );
});
