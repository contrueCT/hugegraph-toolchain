/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
// Pure projection fixtures below are protocol tests, never calibration evidence.
import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, mkdir, readFile, writeFile, rm, readdir, symlink, lstat } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { createHash } from "node:crypto";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { fileURLToPath } from "node:url";
import { setTimeout as pause } from "node:timers/promises";
import {
    ENGINEERING_CASES,
    ENGINEERING_GATES,
    validateEngineeringInput,
    projectReaderContribution,
    projectEngineeringAcceptance,
    buildCalibrationIndex,
    reportEngineeringAcceptance,
    readEngineeringConditions,
    renderEngineeringAcceptanceHtml,
} from "../components/http/engineering-acceptance.mjs";
import { knownFixture } from "./http-known-regression.fixture.mjs";
import { valueHash } from "../components/paired/core.mjs";
import {
    HEADROOM002,
    headroomGrid,
    assessHeadroomCells,
    headroomTolerance,
} from "../components/http/headroom002-contract.mjs";

const MiB = 1024 ** 2,
    sha = (v) => createHash("sha256").update(v).digest("hex");
const ids = ["aa005", "repeatabilityAa", "knownRegression", "driverHeadroom", "observerCost"];
const input = () => ({
    schemaVersion: 1,
    kind: "http-engineering-acceptance-input",
    evidence: Object.fromEntries(ids.map((id) => [id, null])),
    comparison: null,
});
const missing = () =>
    ids.map((id) => ({
        id,
        rawVerified: false,
        status: "unknown",
        verificationMethod: "not-verified",
        scope: { kind: "missing", caseIds: [], modes: [], ratesPerSecond: [] },
        gates: {},
        evidenceDigests: {},
        conditions: null,
        limits: [],
    }));
const get = async (p) => JSON.parse(await readFile(p, "utf8"));
const put = (p, v) => writeFile(p, JSON.stringify(v, null, 2) + "\n");
const exists = (p) =>
    lstat(p).then(
        () => true,
        (e) => {
            if (e.code === "ENOENT") return false;
            throw e;
        },
    );
async function temporary(t) {
    const root = await mkdtemp(join(tmpdir(), "hgbench-engineering-"));
    t.after(() => rm(root, { recursive: true, force: true }));
    return root;
}
function arms(n) {
    return Array.from({ length: n }, (_, i) => ({
        runId: "fixture-" + i,
        valid: true,
        rawManifestSha256: sha("raw" + i),
        observations: 1200,
    }));
}
function known() {
    return {
        kind: "http-known-regression-evidence",
        status: "passed",
        plannedArms: 16,
        plannedPairs: 8,
        verifiedArms: 16,
        arms: arms(16),
        verification: { valid: true, inputClosureStable: true, issues: [] },
        detection: {
            status: "detected",
            metric: { id: "end-to-end-p99", relativeBudget: 0.05, absoluteBudget: 5, minimumObservations: 1000 },
            intervals: { relative: { low: 0.1, high: 0.3 }, absolute: { low: 8, high: 12 } },
        },
        analysis: { cells: [{ observedPairs: 8, issues: [], warnings: [] }] },
        references: { planLockSha256: sha("plan"), inputSha256: sha("wrapper") },
        limits: [],
    };
}
function headroom() {
    const cells = [];
    for (const block of ["b01", "b02"])
        for (const rate of [10, 20, 40])
            for (const cpu of [1, 2])
                for (const caseId of ["scalar-add", "rows-4096", "string-1024k"])
                    cells.push({
                        cellId: `${block}-${rate}-${cpu}`,
                        caseId,
                        evidenceValid: true,
                        observations: 1200,
                        qualityStatus: "passed",
                        pressureStatus: "passed",
                        transitions: [
                            { metric: "correctResponseMs.p50", status: "passed" },
                            { metric: "correctResponseMs.p95", status: "passed" },
                        ],
                    });
    return {
        kind: "http-driver-headroom-evidence",
        status: "passed",
        plannedPhysicalArms: 12,
        plannedCaseCells: 36,
        verifiedPhysicalArms: 12,
        limitedScopeEngineeringAccepted: true,
        arms: arms(12),
        verification: { valid: true, inputClosureStable: true, issues: [] },
        cells,
        p99Comparisons: Array.from({ length: 36 }, () => ({ status: "passed" })),
        limits: [],
    };
}
function observer() {
    return {
        kind: "http-observer-cost-evidence",
        status: "passed",
        plannedArms: 16,
        plannedCaseRuns: 48,
        evidenceVerified: true,
        scopedCostMeasured: true,
        arms: arms(16),
        verification: { valid: true, inputClosureStable: true, issues: [] },
        groups: ["J", "C"].map((id) => ({
            id,
            plannedPairs: 4,
            plannedMetrics: 6,
            usable: true,
            analysis: {
                cells: Array.from({ length: 6 }, () => ({
                    observedPairs: 4,
                    issues: [],
                    warnings: [],
                    bootstrapIntervals: { absolute: { low: 10, high: 20 }, relative: { low: 0.5, high: 1 } },
                })),
            },
        })),
        limitations: [],
    };
}

// Internal-reader protocol fixture only: no HTTP, host provenance or HG qualification.
function partialHeadroom(withFailures = true) {
    const arms = headroomGrid().map((grid, i) => {
        const runId = "hhr-0123456789abcdef-" + grid.cellId;
        if (i >= 10) return { ...grid, runId, valid: false, issues: ["ENOENT"] };
        const cells = HEADROOM002.cases.map((caseId, n) => ({
            caseId,
            runId: "r" + String(n + 1).padStart(4, "0"),
            physicalArmRunId: runId,
            observations: 1200,
            p99: 10,
            qualityStatus: "passed",
            pressureStatus: "passed",
            originalIssues: [],
            wholePhaseDriver: ["warmup", "measurement"].map((phase) => ({
                phase,
                averageCpuCores: 0.2,
                eventLoopUtilization: 0.1,
            })),
            transitions: ["correctResponseMs.p50", "correctResponseMs.p95"].map((metric) => ({
                metric,
                ...headroomTolerance(10, 10, false),
            })),
        }));
        return { ...grid, runId, valid: true, issues: [], rawManifestSha256: sha("partial-raw-" + i), cells };
    });
    if (withFailures) {
        arms[0].cells[0].transitions[0] = { metric: "correctResponseMs.p50", ...headroomTolerance(10, 20, false) };
        arms[0].cells[1].transitions[1] = { metric: "correctResponseMs.p95", ...headroomTolerance(10, 20, false) };
        arms[4].cells[1].qualityStatus = "failed";
        arms[4].cells[1].originalIssues = ["measurement:dropped-arrivals", "measurement:driver-pressure-observed"];
        arms[4].cells[1].pressureStatus = "failed";
        arms[4].cells[1].wholePhaseDriver[1].averageCpuCores = 0.9;
        arms[1].cells[2].p99 = 20;
    }
    return {
        schemaVersion: 1,
        kind: "http-driver-headroom-evidence",
        operationVersion: HEADROOM002.operationVersion,
        ...assessHeadroomCells(arms),
        verifiedPhysicalArms: 10,
        arms,
        verification: { valid: false, inputClosureStable: true, issues: [] },
        registration: { verified: true },
        references: {
            sourceSelectionSha256: HEADROOM002.sourceSelectionSha256,
            assessmentCriteriaSha256: HEADROOM002.criteriaSha256,
            ...Object.fromEntries(
                [
                    "planLockSha256",
                    "executionSpecSha256",
                    "assessmentBindingSha256",
                    "preDispatchLaunchRecordSha256",
                    "controllerStartEvidenceSha256",
                ].map((key) => [key, sha(key)]),
            ),
        },
        limits: [],
    };
}
function resummarizePartial(report) {
    Object.assign(report, assessHeadroomCells(report.arms, report.verification.issues));
    return report;
}

test("strict five evidence fields and exclusive A/A inputs reject operator verdicts", () => {
    assert.equal(validateEngineeringInput(input()).schemaVersion, 1);
    for (const mutate of [
        (v) => delete v.evidence.repeatabilityAa,
        (v) => (v.evidence.passed = true),
        (v) => (v.evidence.knownRegression = { inputFile: "x", sha256: "a".repeat(64), passed: true }),
        (v) => (v.script = "node anything"),
        (v) => {
            v.evidence.aa005 = { evidenceRoot: "old" };
            v.evidence.repeatabilityAa = {
                planDirectory: "p",
                planLockSha256: "a".repeat(64),
                executionSetFile: "s",
                executionSetSha256: "b".repeat(64),
            };
        },
    ]) {
        const v = input();
        mutate(v);
        assert.throws(() => validateEngineeringInput(v));
    }
});
test("known regression requires the original complete detection result, not a passed label", () => {
    assert.equal(projectReaderContribution("knownRegression", known()).status, "passed");
    for (const mutate of [
        (r) => (r.detection.intervals = null),
        (r) => (r.detection.intervals.absolute.low = 4),
        (r) => (r.arms[0].observations = 999),
        (r) => (r.analysis.cells[0].observedPairs = 7),
        (r) => (r.verification.inputClosureStable = false),
        (r) => r.arms.pop(),
        (r) => (r.kind = "unrelated"),
    ]) {
        const r = known();
        mutate(r);
        assert.notEqual(projectReaderContribution("knownRegression", r).status, "passed");
    }
});
test("headroom retains high-rate failures and warmup checks remain limited to their three cases", () => {
    const r = headroom(),
        c = projectReaderContribution("driverHeadroom", r);
    assert.equal(c.status, "passed");
    assert.equal(c.gates["warmup-stability"].status, "passed");
    r.status = "failed";
    r.limitedScopeEngineeringAccepted = false;
    r.cells[35].pressureStatus = "failed";
    assert.equal(projectReaderContribution("driverHeadroom", r).status, "failed");
    const all = missing();
    all[3] = c;
    const result = projectEngineeringAcceptance(all, { status: "passed", mismatches: [], unknownConditions: [] });
    assert.equal(result.coverage.cases.length, 11);
    assert.equal(result.engineering.accepted, false);
    assert.equal(
        result.coverage.cases
            .find((x) => x.id === "structured-one")
            .gateCoverage.find((x) => x.id === "warmup-stability").status,
        "unknown",
    );
    const bad = headroom();
    bad.cells[0].transitions[1].metric = "correctResponseMs.p50";
    assert.equal(projectReaderContribution("driverHeadroom", bad).gates["warmup-stability"].status, "unknown");
});
test("partial headroom preserves matched negatives, all12/36 and case-specific warmup unknowns", () => {
    const r = partialHeadroom();
    // A top-level verdict cannot substitute for the matched observations below.
    r.status = "passed";
    const contribution = projectReaderContribution("driverHeadroom", r);
    assert.equal(contribution.rawVerified, false);
    assert.equal(contribution.status, "failed");
    assert.equal(contribution.gates["driver-headroom"].status, "failed");
    assert.equal(contribution.gates["warmup-stability"].status, "failed");
    assert.equal(contribution.gates["independent-clean-measurement-windows"], undefined);
    assert.deepEqual(
        [
            contribution.observedEvidence.plannedPhysicalArms,
            contribution.observedEvidence.verifiedPhysicalArms,
            contribution.observedEvidence.plannedCaseCells,
            contribution.observedEvidence.verifiedCaseCells,
        ],
        [12, 10, 36, 30],
    );
    assert.equal(contribution.observedEvidence.missingArms.length, 2);
    assert.equal(contribution.observedEvidence.missingCells.length, 6);
    assert.equal(contribution.observedEvidence.passedGatesGranted, false);
    assert.deepEqual(
        new Set(contribution.observedFailures.map((f) => f.criterion)),
        new Set(["http-quality", "driver-pressure", "warmup-transition", "p99-consistency"]),
    );
    for (const failure of contribution.observedFailures)
        for (const source of failure.sources) {
            const arm = r.arms.find((a) => a.runId === source.physicalArmRunId);
            assert.equal(arm.valid, true);
            assert.equal(source.rawManifestSha256, arm.rawManifestSha256);
            assert(arm.cells.some((c) => c.runId === source.runId && c.caseId === failure.caseId));
        }
    const contributions = missing();
    contributions[3] = contribution;
    const result = projectEngineeringAcceptance(contributions);
    assert.equal(result.engineering.status, "failed");
    assert.equal(result.engineering.accepted, false);
    assert.equal(result.coverage.cases.length, 11);
    assert.equal(result.coverage.fullElevenStatus, "unknown");
    const coverage = (caseId, gateId) =>
        result.coverage.cases.find((c) => c.id === caseId).gateCoverage.find((g) => g.id === gateId).status;
    for (const caseId of HEADROOM002.cases) assert.equal(coverage(caseId, "driver-headroom"), "failed");
    assert.equal(coverage("scalar-add", "warmup-stability"), "failed");
    assert.equal(coverage("rows-4096", "warmup-stability"), "failed");
    assert.equal(coverage("string-1024k", "warmup-stability"), "unknown");
    assert(result.coverage.cases.filter((c) => !HEADROOM002.cases.includes(c.id)).every((c) => c.status === "unknown"));
    assert(result.engineering.gates.every((g) => g.status !== "passed"));
    const index = buildCalibrationIndex(contributions);
    assert.deepEqual(index.contributions[3].observedFailures, contribution.observedFailures);
    const html = renderEngineeringAcceptanceHtml({ ...result, contributions, limits: [] });
    assert.match(html, /Verified physical arms: 10\/12; verified case cells: 30\/36/);
    assert.match(html, /Partial evidence grants no passed gate/);
    assert.match(html, /warmup-transition/);
    assert.match(html, /p99-consistency/);
    assert.match(html, new RegExp(r.arms[4].rawManifestSha256));
    assert.equal(projectReaderContribution("driverHeadroom", headroom()).observedFailures, undefined);
});
test("partial headroom failure labels without supported observations remain unknown", () => {
    const mutations = [
        (r) => (r.status = "failed"),
        (r) => (r.arms[0].cells[0].qualityStatus = "failed"),
        (r) => (r.arms[0].cells[0].pressureStatus = "failed"),
        (r) => (r.arms[0].cells[0].transitions[0].status = "failed"),
        (r) => {
            r.arms[0].cells[0].p99 = 100;
            r.arms[0].cells[0].observations = 999;
        },
    ];
    for (const mutate of mutations) {
        const r = partialHeadroom(false);
        mutate(r);
        resummarizePartial(r);
        r.status = "failed";
        const c = projectReaderContribution("driverHeadroom", r);
        assert.equal(c.rawVerified, false);
        assert.equal(c.status, "unknown");
        assert.deepEqual(c.observedFailures, []);
        assert(Object.values(c.gates).every((g) => g.status === "unknown"));
    }
});
test("partial headroom a rows-only negative does not mark other cases failed", () => {
    const r = partialHeadroom(false),
        cell = r.arms[0].cells[1];
    cell.qualityStatus = "failed";
    cell.originalIssues = ["warmup:dropped-arrivals"];
    resummarizePartial(r);
    const c = projectReaderContribution("driverHeadroom", r),
        result = projectEngineeringAcceptance([c]);
    assert.equal(result.engineering.gates.find((g) => g.id === "driver-headroom").status, "failed");
    const failed = result.coverage.cases.filter((c) => c.status === "failed");
    assert.deepEqual(
        failed.map((c) => c.id),
        ["rows-4096"],
    );
    assert(
        result.coverage.cases.every(
            (c) => c.gateCoverage.find((g) => g.id === "warmup-stability").status === "unknown",
        ),
    );
});
test("partial headroom rejects unstable, unbound, duplicate and mismatched failure references", () => {
    const mutations = [
        (r) => (r.verification.inputClosureStable = false),
        (r) => r.verification.issues.push("HEADROOM_CONTROLLER_COMMAND_WINDOW"),
        (r) => (r.registration.verified = false),
        (r) => (r.references.assessmentCriteriaSha256 = sha("wrong-criterion")),
        (r) => delete r.references.planLockSha256,
        (r) => (r.verifiedPhysicalArms = 11),
        (r) => r.arms.pop(),
        (r) => (r.arms[1].runId = r.arms[0].runId),
        (r) => (r.arms[1].cellId = r.arms[0].cellId),
        (r) => (r.arms[1].rawManifestSha256 = r.arms[0].rawManifestSha256),
        (r) => delete r.arms[0].rawManifestSha256,
        (r) => {
            r.arms[0].cells[0].physicalArmRunId = r.arms[1].runId;
            resummarizePartial(r);
        },
        (r) => {
            r.arms[0].cells[1].caseId = r.arms[0].cells[0].caseId;
            resummarizePartial(r);
        },
        (r) => {
            r.arms[0].cells[1].runId = r.arms[0].cells[0].runId;
            resummarizePartial(r);
        },
        (r) => (r.cells[0].observations = 999),
        (r) => (r.p99Comparisons[0].caseId = "structured-one"),
        (r) => (r.p99Comparisons[0].difference = 999),
    ];
    for (const mutate of mutations) {
        const r = partialHeadroom();
        mutate(r);
        const c = projectReaderContribution("driverHeadroom", r);
        assert.equal(c.rawVerified, false);
        assert.equal(c.status, "unknown");
        assert.equal(c.observedEvidence, undefined);
        assert.equal(c.observedFailures, undefined);
        assert.deepEqual(c.gates, {});
    }
});
test("observer nonzero measured cost can pass its scoped gate; partial pairs cannot", () => {
    const r = observer();
    assert.equal(projectReaderContribution("observerCost", r).status, "passed");
    r.groups[1].analysis.cells[5].observedPairs = 3;
    assert.equal(projectReaderContribution("observerCost", r).status, "unknown");
    r.groups[1].analysis.cells[5].observedPairs = 4;
    r.groups[1].analysis.cells[5].warnings = ["too-few-samples"];
    assert.equal(projectReaderContribution("observerCost", r).status, "unknown");
});
test("index deterministically preserves all contributions and excludes wrapper-only paths", () => {
    const a = known(),
        b = known();
    a.references.inputSha256 = sha("a wrapper");
    b.references.inputSha256 = sha("another wrapper");
    const x = missing(),
        y = missing();
    x[2] = projectReaderContribution("knownRegression", a);
    y[2] = projectReaderContribution("knownRegression", b);
    assert.deepEqual(buildCalibrationIndex(x), buildCalibrationIndex(y));
    assert.equal(buildCalibrationIndex(x).contributions.length, 5);
    assert.throws(() => buildCalibrationIndex(x.slice(1)), { code: "ENGINEERING_CONTRIBUTION_ORDER" });
});
test("current A/A embedded coverage and HTML use verified modes without assuming forward", () => {
    const contributions = missing();
    assert.deepEqual(projectEngineeringAcceptance(contributions).coverage.modes, []);
    contributions[1] = {
        ...contributions[1],
        rawVerified: true,
        scope: { kind: "current-aa", caseIds: ["scalar-add"], modes: ["embedded"], ratesPerSecond: [20] },
        gates: { "repeatability-aa": { status: "unknown", reason: "Fixture remains uncertain" } },
    };
    const report = { ...projectEngineeringAcceptance(contributions), limits: ["Synthetic projection only"] };
    assert.deepEqual(report.coverage.modes, ["embedded"]);
    assert.equal(report.coverage.embeddedMode, "unknown");
    assert.equal(report.engineering.accepted, false);
    const html = renderEngineeringAcceptanceHtml(report);
    assert.match(html, /modes embedded; rates 20/);
    assert.doesNotMatch(html, /forward rates/);
});
test("all-null IO keeps seven gates and eleven cases unknown and seals exact output", async (t) => {
    const root = await temporary(t),
        file = join(root, "input.json");
    await put(file, input());
    const options = { inputFile: file, outputDirectory: join(root, "report"), timeoutMs: 5000, maxBytes: MiB };
    const reply = await reportEngineeringAcceptance(options),
        report = await get(reply.reportJson),
        lock = await get(reply.reportLock);
    assert.equal(reply.engineeringAccepted, false);
    assert.equal(reply.qualificationAccepted, false);
    assert.equal(reply.performanceAccepted, false);
    assert.deepEqual(
        report.engineering.gates.map((g) => g.id),
        ENGINEERING_GATES,
    );
    assert(report.engineering.gates.every((g) => g.status === "unknown"));
    assert.deepEqual(
        report.coverage.cases.map((c) => c.id),
        ENGINEERING_CASES,
    );
    assert(report.coverage.cases.every((c) => c.status === "unknown"));
    assert.equal(reply.calibrationIndex, join(reply.directory, "calibration-evidence-index.json"));
    assert.deepEqual(
        (await readdir(options.outputDirectory)).sort(),
        [...lock.files.map((f) => f.path), "report.lock.json"].sort(),
    );
    for (const member of lock.files) {
        const raw = await readFile(join(options.outputDirectory, member.path));
        assert.equal(raw.length, member.bytes);
        assert.equal(sha(raw), member.sha256);
    }
    await assert.rejects(reportEngineeringAcceptance(options), { code: "EEXIST" });
    const next = await reportEngineeringAcceptance({ ...options, outputDirectory: join(root, "moved-report") });
    assert.deepEqual(await readFile(next.calibrationIndex), await readFile(reply.calibrationIndex));
});
test("input and output budget failures occur without modifying source or creating outputs", async (t) => {
    const root = await temporary(t),
        file = join(root, "input.json"),
        out = join(root, "report");
    await writeFile(file, Buffer.alloc(MiB + 1, 32));
    await assert.rejects(
        reportEngineeringAcceptance({ inputFile: file, outputDirectory: out, timeoutMs: 5000, maxBytes: MiB }),
        { code: "ENGINEERING_INPUT_BYTES" },
    );
    assert.equal(await exists(out), false);
    await put(file, input());
    for (const options of [
        { timeoutMs: 999, maxBytes: MiB },
        { timeoutMs: 1000, maxBytes: MiB - 1 },
    ])
        await assert.rejects(reportEngineeringAcceptance({ inputFile: file, outputDirectory: out, ...options }), {
            code: "ENGINEERING_OPTIONS",
        });
    assert.equal(await exists(out), false);
});
test("an output parent alias cannot create reports inside protected input", async (t) => {
    const root = await temporary(t),
        original = join(root, "original"),
        alias = join(root, "alias");
    await mkdir(original);
    await symlink(original, alias);
    const file = join(original, "input.json");
    await put(file, input());
    await assert.rejects(
        reportEngineeringAcceptance({
            inputFile: file,
            outputDirectory: join(alias, "input.json", "child"),
            timeoutMs: 5000,
            maxBytes: MiB,
        }),
    );
    assert.deepEqual(await readdir(original), ["input.json"]);
    const f = await knownFixture(t);
    const v = input();
    v.evidence.knownRegression = { inputFile: f.inputFile, sha256: sha(await readFile(f.inputFile)) };
    const ref = join(root, "reference.json");
    await put(ref, v);
    await symlink(f.plan, join(root, "plan-alias"));
    const before = await readdir(f.plan);
    await assert.rejects(
        reportEngineeringAcceptance({
            inputFile: ref,
            outputDirectory: join(root, "plan-alias", "pollution"),
            timeoutMs: 5000,
            maxBytes: MiB,
        }),
        { code: "ENGINEERING_OUTPUT_OVERLAP" },
    );
    assert.deepEqual(await readdir(f.plan), before);
});
test("real offline reader preserves every unexecuted arm and cannot promote operator result flags", async (t) => {
    const f = await knownFixture(t);
    await put(join(f.execution, "result.json"), { status: "passed", cleanupComplete: true });
    const v = input();
    v.evidence.knownRegression = { inputFile: f.inputFile, sha256: sha(await readFile(f.inputFile)) };
    const file = join(f.root, "acceptance.json");
    await put(file, v);
    const reply = await reportEngineeringAcceptance({
        inputFile: file,
        outputDirectory: join(f.root, "acceptance"),
        timeoutMs: 10000,
        maxBytes: 8 * MiB,
    });
    const report = await get(reply.reportJson),
        reader = await get(join(reply.directory, "rebuild-knownRegression/report.json"));
    assert.equal(reader.arms.length, 16);
    assert.equal(reader.verifiedArms, 0);
    assert.equal(report.contributions[2].rawVerified, false);
    assert.equal(report.engineering.accepted, false);
});
test("shared cancellation of an active offline reader leaves no successful outer lock", async (t) => {
    const f = await knownFixture(t);
    const v = input();
    v.evidence.knownRegression = { inputFile: f.inputFile, sha256: sha(await readFile(f.inputFile)) };
    const file = join(f.root, "acceptance.json"),
        out = join(f.root, "acceptance");
    await put(file, v);
    const controller = new AbortController();
    const result = reportEngineeringAcceptance({
        inputFile: file,
        outputDirectory: out,
        timeoutMs: 10000,
        maxBytes: 8 * MiB,
        signal: controller.signal,
    });
    const rejection = assert.rejects(result);
    for (let i = 0; i < 200 && !(await exists(join(out, "rebuild-knownRegression"))); i++) await pause(5);
    controller.abort();
    await rejection;
    assert.equal(await exists(join(out, "report.lock.json")), false);
    assert.equal(await exists(join(out, "partial.json")), true);
});

test("condition snapshots must match the previously verified plan lock members", async (t) => {
    const f = await knownFixture(t);
    const conditions = await readEngineeringConditions(f.inputFile, "knownRegression", f.input.reference);
    assert.equal(Object.hasOwn(conditions, "settingsVariants"), false);
    assert(conditions.settings?.value);
    assert.equal(conditions.profiles.length, 16);
    assert.equal(conditions.observationArms.length, 16);
    assert(conditions.observationArms.every((a) => !a.collector && !a.jvmObservation && a.driverCpus === 1));
    await writeFile(
        join(f.plan, "settings.json"),
        JSON.stringify({ cpu: "different settings after reader completed" }),
    );
    await assert.rejects(readEngineeringConditions(f.inputFile, "knownRegression", f.input.reference), {
        code: "ENGINEERING_CONDITION_CHANGED",
    });
});

test("the aggregate cannot create even its wrapper inside the original measurement source", async (t) => {
    const f = await knownFixture(t, { localSource: true }),
        v = input();
    v.evidence.knownRegression = { inputFile: f.inputFile, sha256: sha(await readFile(f.inputFile)) };
    const file = join(f.root, "acceptance.json");
    await put(file, v);
    const sourceRoot = join(f.root, "measured-source");
    await assert.rejects(
        reportEngineeringAcceptance({
            inputFile: file,
            outputDirectory: join(sourceRoot, "new-report"),
            timeoutMs: 5000,
            maxBytes: MiB,
        }),
        { code: "ENGINEERING_OUTPUT_OVERLAP" },
    );
    assert.deepEqual(await readdir(sourceRoot), []);
});

test("current A/A must reserve its unchanged report budget inside the aggregate cap", async (t) => {
    const root = await temporary(t),
        plan = join(root, "plan");
    await mkdir(plan);
    const header = {
        schemaVersion: 2,
        kind: "http-calibration-plan",
        scenario: "repeatability-aa",
        planDigest: "a".repeat(64),
        experiments: [{ id: "e1", directory: "experiments/e1" }],
        budgets: { maxReportMilliseconds: 1000, maxReportBytes: 66 * MiB },
    };
    await put(join(plan, "plan.json"), header);
    const raw = await readFile(join(plan, "plan.json"));
    const files = [{ path: "plan.json", bytes: raw.length, sha256: sha(raw) }];
    await put(join(plan, "plan.lock.json"), {
        kind: "http-calibration-plan-lock",
        planDigest: header.planDigest,
        files,
        contentDigest: valueHash(files),
    });
    const set = join(root, "execution-set.json");
    await put(set, {
        schemaVersion: 1,
        kind: "http-calibration-execution-set",
        planDigest: header.planDigest,
        experiments: [],
    });
    const v = input();
    v.evidence.repeatabilityAa = {
        planDirectory: plan,
        planLockSha256: sha(await readFile(join(plan, "plan.lock.json"))),
        executionSetFile: set,
        executionSetSha256: sha(await readFile(set)),
    };
    const file = join(root, "input.json");
    await put(file, v);
    const reply = await reportEngineeringAcceptance({
        inputFile: file,
        outputDirectory: join(root, "report"),
        timeoutMs: 20000,
        maxBytes: 2 * MiB,
    });
    const report = await get(reply.reportJson);
    assert.equal(report.engineering.accepted, false);
    assert.equal(report.contributions[1].rawVerified, false);
    assert.deepEqual(report.issues, [{ id: "repeatabilityAa", code: "CURRENT_AA_REPORT_BYTE_RESERVE" }]);
    assert.equal(await exists(join(reply.directory, "rebuild-repeatabilityAa")), false);
    const lock = await get(reply.reportLock);
    const total = lock.files.reduce((sum, f) => sum + f.bytes, 0) + (await readFile(reply.reportLock)).length;
    assert(total <= 2 * MiB);
});

test("current A/A public component rejects execution-tree output before any wrapper writes", async (t) => {
    const root = await temporary(t),
        plan = join(root, "plan"),
        execution = join(root, "execution");
    await mkdir(plan);
    await mkdir(execution);
    await put(join(execution, "original.json"), { original: true });
    const original = await readFile(join(execution, "original.json")),
        set = join(root, "execution-set.json");
    await put(set, {
        schemaVersion: 1,
        kind: "http-calibration-execution-set",
        planDigest: "a".repeat(64),
        experiments: [{ id: "e1", executionDirectory: "execution" }],
    });
    const value = input();
    value.evidence.repeatabilityAa = {
        planDirectory: plan,
        planLockSha256: "a".repeat(64),
        executionSetFile: set,
        executionSetSha256: sha(await readFile(set)),
    };
    const file = join(root, "input.json");
    await put(file, value);
    const alias = join(root, "execution-alias");
    await symlink(execution, alias);
    for (const parent of [execution, alias]) {
        await assert.rejects(
            promisify(execFile)(process.execPath, [
                fileURLToPath(new URL("../components/http/engineering-acceptance.mjs", import.meta.url)),
                "--input",
                file,
                "--output",
                join(parent, "new-report"),
                "--timeout-ms",
                "10000",
                "--max-bytes",
                String(2 * MiB),
            ]),
            (e) => e.code === 6 && JSON.parse(e.stderr).error.code === "ENGINEERING_OUTPUT_OVERLAP",
        );
        assert.deepEqual(await readdir(execution), ["original.json"]);
        assert.deepEqual(await readFile(join(execution, "original.json")), original);
    }
});
