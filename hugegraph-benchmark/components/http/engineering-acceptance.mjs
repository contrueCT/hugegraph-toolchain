/* Licensed to the Apache Software Foundation (ASF) under one or more
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
import { constants } from "node:fs";
import { open, realpath, readdir, lstat, mkdir, writeFile, rm } from "node:fs/promises";
import { createHash } from "node:crypto";
import { dirname, basename, resolve, join, relative, sep } from "node:path";
import { pathToFileURL, fileURLToPath } from "node:url";
import { performance } from "node:perf_hooks";
import { isDeepStrictEqual } from "node:util";
import { reportKnownRegression } from "./known-regression-evidence.mjs";
import { reportHeadroom } from "./headroom-evidence.mjs";
import { reportObserverCost } from "./observer-cost-evidence.mjs";
import { HEADROOM002, headroomGrid, assessHeadroomCells, headroomTolerance } from "./headroom002-contract.mjs";
import { observer003Schedule } from "./observer003-contract.mjs";

const MiB = 1024 ** 2;
const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "../..");
const SHA = /^[a-f0-9]{64}$/;
const IDS = ["aa005", "repeatabilityAa", "knownRegression", "driverHeadroom", "observerCost"];
export const ENGINEERING_CASES = Object.freeze([
    "scalar-add",
    "structured-one",
    "rows-0016",
    "rows-0256",
    "rows-4096",
    "string-0001k",
    "string-0016k",
    "string-0064k",
    "string-0256k",
    "string-1024k",
    "string-unicode",
]);
export const ENGINEERING_GATES = Object.freeze([
    "repeatability-aa",
    "independent-clean-measurement-windows",
    "driver-headroom",
    "observer-and-validator-cost",
    "known-regression-detection",
    "warmup-stability",
    "formal-parameter-freeze",
]);
const REPRESENTATIVE_CASES = ["scalar-add", "rows-4096", "string-1024k"];
const FIXED_LIMITS = [
    "Historical A/A005 uses its immutable prior replay chain; current A/A evidence uses a new independent raw rebuild. Neither path changes the original policy.",
    "Three-case driver/warmup and observer evidence does not establish coverage of the other eight cases or 80rps.",
    "Forward evidence does not establish embedded-mode driver headroom or target capacity.",
    "Declared job checks do not prove whole-host exclusivity; d6, background services and VM interference remain disclosed.",
    "JVM/collector conditional costs are not additive; validation and trace remain enabled and their counterfactual cost is unknown.",
    "No fine phase-to-host/JVM clock attribution, statistical certification or performance qualification is granted.",
];
const hash = (raw) => createHash("sha256").update(raw).digest("hex");
const sortValue = (v) =>
    Array.isArray(v)
        ? v.map(sortValue)
        : v && typeof v === "object"
          ? Object.fromEntries(
                Object.keys(v)
                    .sort()
                    .map((k) => [k, sortValue(v[k])]),
            )
          : v;
const encoded = (v) => Buffer.from(JSON.stringify(sortValue(v), null, 2) + "\n");
const valueHash = (v) => hash(JSON.stringify(sortValue(v)));
const need = (ok, code) => {
    if (!ok) {
        const e = new Error(code);
        e.code = code;
        throw e;
    }
};
const exact = (v, keys) =>
    need(
        v &&
            typeof v === "object" &&
            !Array.isArray(v) &&
            Object.keys(v).length === keys.length &&
            keys.every((k) => Object.hasOwn(v, k)),
        "ENGINEERING_INPUT_FIELDS",
    );
const pathValue = (v) =>
    need(typeof v === "string" && v.length > 0 && v.length <= 4096 && !/[\0\r\n]/.test(v), "ENGINEERING_INPUT_PATH");
const limitStrings = (v) =>
    Array.isArray(v) ? v.map(String) : v && typeof v === "object" ? [JSON.stringify(sortValue(v))] : [];
const state = (v) => (["passed", "failed", "unknown"].includes(v) ? v : "unknown");
const beneath = (a, b) => a === b || a.startsWith(b + sep);

export function validateEngineeringInput(value) {
    exact(value, ["schemaVersion", "kind", "evidence", "comparison"]);
    need(value.schemaVersion === 1 && value.kind === "http-engineering-acceptance-input", "ENGINEERING_INPUT_VERSION");
    exact(value.evidence, IDS);
    for (const id of IDS) {
        const row = value.evidence[id];
        if (row === null) continue;
        if (id === "repeatabilityAa") {
            exact(row, ["planDirectory", "planLockSha256", "executionSetFile", "executionSetSha256"]);
            pathValue(row.planDirectory);
            pathValue(row.executionSetFile);
            need(SHA.test(row.planLockSha256) && SHA.test(row.executionSetSha256), "ENGINEERING_INPUT_SHA");
        } else {
            exact(row, id === "aa005" ? ["evidenceRoot"] : ["inputFile", "sha256"]);
            pathValue(row[id === "aa005" ? "evidenceRoot" : "inputFile"]);
            if (id !== "aa005") need(SHA.test(row.sha256), "ENGINEERING_INPUT_SHA");
        }
    }
    need(value.evidence.aa005 === null || value.evidence.repeatabilityAa === null, "ENGINEERING_AA_EXCLUSIVE");
    if (value.comparison !== null) {
        exact(value.comparison, ["planDirectory", "planLockSha256", "calibrationIndexFile", "calibrationIndexSha256"]);
        pathValue(value.comparison.planDirectory);
        pathValue(value.comparison.calibrationIndexFile);
        need(
            SHA.test(value.comparison.planLockSha256) && SHA.test(value.comparison.calibrationIndexSha256),
            "ENGINEERING_INPUT_SHA",
        );
    }
    return value;
}

async function bounded(path, maximum = MiB) {
    const fd = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
    try {
        const before = await fd.stat();
        need(before.isFile() && before.size <= maximum, "ENGINEERING_INPUT_BYTES");
        const raw = Buffer.alloc(before.size + 1);
        let size = 0;
        while (size < raw.length) {
            const r = await fd.read(raw, size, raw.length - size);
            if (!r.bytesRead) break;
            size += r.bytesRead;
        }
        const after = await fd.stat();
        need(
            size === before.size &&
                before.dev === after.dev &&
                before.ino === after.ino &&
                before.size === after.size &&
                before.mtimeMs === after.mtimeMs &&
                before.ctimeMs === after.ctimeMs,
            "ENGINEERING_INPUT_CHANGED",
        );
        return raw.subarray(0, size);
    } finally {
        await fd.close();
    }
}
async function canonical(path) {
    const suffix = [];
    let current = resolve(path);
    for (;;) {
        try {
            return join(await realpath(current), ...suffix.reverse());
        } catch (e) {
            if (e.code !== "ENOENT") throw e;
        }
        suffix.push(basename(current));
        const parent = dirname(current);
        need(parent !== current, "ENGINEERING_INPUT_PATH");
        current = parent;
    }
}
const json = async (path, maximum) =>
    JSON.parse(new TextDecoder("utf8", { fatal: true }).decode(await bounded(path, maximum)));

async function inventory(root, maximum, deadline = Infinity) {
    const files = [];
    let total = 0;
    async function walk(dir, depth) {
        need(performance.now() < deadline, "ENGINEERING_DEADLINE");
        need(depth <= 20, "ENGINEERING_OUTPUT_DEPTH");
        for (const item of await readdir(dir, { withFileTypes: true })) {
            const path = join(dir, item.name),
                info = await lstat(path);
            need(!info.isSymbolicLink(), "ENGINEERING_OUTPUT_SYMLINK");
            if (info.isDirectory()) await walk(path, depth + 1);
            else {
                need(info.isFile() && files.length < 20000 && total + info.size <= maximum, "ENGINEERING_OUTPUT_BYTES");
                const fd = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
                let size = 0;
                const digest = createHash("sha256"),
                    buffer = Buffer.alloc(65536);
                try {
                    const before = await fd.stat();
                    need(before.isFile() && total + before.size <= maximum, "ENGINEERING_OUTPUT_BYTES");
                    for (;;) {
                        need(performance.now() < deadline, "ENGINEERING_DEADLINE");
                        const part = await fd.read(buffer, 0, buffer.length, null);
                        if (!part.bytesRead) break;
                        need(total + size + part.bytesRead <= maximum, "ENGINEERING_OUTPUT_BYTES");
                        size += part.bytesRead;
                        digest.update(buffer.subarray(0, part.bytesRead));
                    }
                    const after = await fd.stat();
                    need(
                        size === before.size &&
                            before.dev === after.dev &&
                            before.ino === after.ino &&
                            before.size === after.size &&
                            before.mtimeMs === after.mtimeMs &&
                            before.ctimeMs === after.ctimeMs,
                        "ENGINEERING_OUTPUT_CHANGED",
                    );
                } finally {
                    await fd.close();
                }
                total += size;
                files.push({
                    path: relative(root, path).split(sep).join("/"),
                    bytes: size,
                    sha256: digest.digest("hex"),
                });
            }
        }
    }
    await walk(root, 0);
    return { files: files.sort((a, b) => a.path.localeCompare(b.path)), bytes: total };
}

function fixedScope(id) {
    if (id === "repeatabilityAa")
        return {
            kind: "current-aa-unavailable",
            caseIds: [],
            modes: [],
            ratesPerSecond: [],
            maxInFlight: null,
            warmupMs: null,
            durationMs: null,
        };
    return {
        kind:
            id === "knownRegression"
                ? "fixed-positive-method-control"
                : id === "aa005"
                  ? "full-eleven-aa"
                  : "three-representative-cases",
        caseIds:
            id === "aa005"
                ? [...ENGINEERING_CASES]
                : id === "knownRegression"
                  ? ["arithmetic-control"]
                  : [...REPRESENTATIVE_CASES],
        modes: ["forward"],
        ratesPerSecond: id === "driverHeadroom" ? [10, 20, 40] : id === "knownRegression" ? [20] : [10],
        maxInFlight: 4,
        warmupMs: id === "knownRegression" ? 10000 : 30000,
        durationMs: id === "knownRegression" ? 60000 : 120000,
    };
}
const gate = (status, reason) => ({ status: state(status), reason });
function missingContribution(id, reason = "evidence-not-provided") {
    return {
        id,
        rawVerified: false,
        verificationMethod: "not-verified",
        status: "unknown",
        scope: fixedScope(id),
        gates: {},
        evidenceDigests: {},
        conditions: null,
        limits: [reason],
    };
}

// This consumes only the fresh internal reader result. Partial evidence can retain a
// demonstrated negative; it can never establish a passed gate or complete coverage.
function partialHeadroomNegatives(report) {
    const grid = headroomGrid();
    if (
        report?.kind !== "http-driver-headroom-evidence" ||
        report.operationVersion !== HEADROOM002.operationVersion ||
        report.plannedPhysicalArms !== 12 ||
        report.plannedCaseCells !== 36 ||
        report.verification?.valid !== false ||
        report.verification.inputClosureStable !== true ||
        !Array.isArray(report.verification.issues) ||
        report.verification.issues.length ||
        report.registration?.verified !== true ||
        report.references?.sourceSelectionSha256 !== HEADROOM002.sourceSelectionSha256 ||
        report.references.assessmentCriteriaSha256 !== HEADROOM002.criteriaSha256 ||
        ![
            "planLockSha256",
            "executionSpecSha256",
            "assessmentBindingSha256",
            "preDispatchLaunchRecordSha256",
            "controllerStartEvidenceSha256",
        ].every((key) => SHA.test(report.references[key])) ||
        !Array.isArray(report.arms) ||
        report.arms.length !== 12 ||
        !report.arms.every((a) => a && typeof a === "object") ||
        new Set(report.arms.map((a) => a.runId)).size !== 12
    )
        return null;
    const validArms = report.arms.filter((a) => a.valid === true);
    if (
        !Number.isSafeInteger(report.verifiedPhysicalArms) ||
        report.verifiedPhysicalArms !== validArms.length ||
        validArms.length === 12 ||
        new Set(validArms.map((a) => a.rawManifestSha256)).size !== validArms.length ||
        !report.arms.every((arm, i) => {
            const expected = grid[i];
            if (
                !["cellId", "blockId", "ratePerSecond", "driverCpus"].every((key) => arm[key] === expected[key]) ||
                typeof arm.runId !== "string" ||
                !new RegExp("^hhr-[a-f0-9]{16}-" + expected.cellId + "$").test(arm.runId) ||
                !Array.isArray(arm.issues)
            )
                return false;
            if (arm.valid !== true)
                return arm.valid === false && arm.issues.length > 0 && (!arm.cells || arm.cells.length === 0);
            return (
                arm.issues.length === 0 &&
                SHA.test(arm.rawManifestSha256) &&
                Array.isArray(arm.cells) &&
                arm.cells.length === 3 &&
                arm.cells.every(
                    (cell, n) =>
                        cell &&
                        cell.caseId === HEADROOM002.cases[n] &&
                        cell.physicalArmRunId === arm.runId &&
                        cell.runId === "r" + String(n + 1).padStart(4, "0") &&
                        Number.isSafeInteger(cell.observations) &&
                        cell.observations >= 0,
                )
            );
        })
    )
        return null;
    const summary = assessHeadroomCells(report.arms, report.verification.issues);
    if (
        !isDeepStrictEqual(report.cells, summary.cells) ||
        !isDeepStrictEqual(report.p99Comparisons, summary.p99Comparisons)
    )
        return null;
    const source = (cell) => {
        const arm = validArms.find((a) => a.runId === cell.physicalArmRunId);
        return {
            physicalArmRunId: arm.runId,
            cellId: arm.cellId,
            caseId: cell.caseId,
            runId: cell.runId,
            rawManifestSha256: arm.rawManifestSha256,
        };
    };
    const failures = [];
    for (const cell of summary.cells.filter((c) => c.evidenceValid)) {
        const base = { caseId: cell.caseId, sources: [source(cell)], gateIds: ["driver-headroom"] };
        const symptoms = (Array.isArray(cell.originalIssues) ? cell.originalIssues : []).filter(
            (issue) =>
                typeof issue === "string" &&
                /^(warmup|measurement):(request-errors-or-wrong-answers|dropped-arrivals|request-budget-exhausted|cancelled|no-correct-results)$/.test(
                    issue,
                ),
        );
        if (cell.qualityStatus === "failed" && symptoms.length)
            failures.push({ ...base, criterion: "http-quality", symptoms });
        if (cell.pressureStatus === "failed")
            for (const phase of Array.isArray(cell.wholePhaseDriver) ? cell.wholePhaseDriver : [])
                for (const metric of ["averageCpuCores", "eventLoopUtilization"])
                    if (
                        phase &&
                        ["warmup", "measurement"].includes(phase.phase) &&
                        Number.isFinite(phase[metric]) &&
                        phase[metric] > 0.8 &&
                        (metric !== "eventLoopUtilization" || phase[metric] <= 1)
                    )
                        failures.push({
                            ...base,
                            criterion: "driver-pressure",
                            phase: phase.phase,
                            metric,
                            observed: phase[metric],
                            maximum: 0.8,
                        });
        if (
            Array.isArray(cell.transitions) &&
            cell.transitions.length === 2 &&
            cell.transitions.every((t, i) => t && t.metric === ["correctResponseMs.p50", "correctResponseMs.p95"][i])
        )
            for (const transition of cell.transitions) {
                const rebuilt = {
                    metric: transition.metric,
                    ...headroomTolerance(transition.reference, transition.candidate, false),
                };
                if (rebuilt.status === "failed" && isDeepStrictEqual(transition, rebuilt))
                    failures.push({
                        ...base,
                        gateIds: ["driver-headroom", "warmup-stability"],
                        criterion: "warmup-transition",
                        ...rebuilt,
                    });
            }
    }
    for (const comparison of summary.p99Comparisons.filter((c) => c.status === "failed")) {
        const cells = summary.cells.filter(
            (c) =>
                c.evidenceValid &&
                c.caseId === comparison.caseId &&
                c.ratePerSecond === comparison.ratePerSecond &&
                (comparison.kind === "cpu-sensitivity"
                    ? c.blockId === comparison.blockId
                    : c.driverCpus === comparison.driverCpus),
        );
        if (cells.length === 2 && cells.every((c) => c.observations >= 1000))
            failures.push({
                criterion: "p99-consistency",
                metric: "correctEndToEndMs.p99",
                gateIds: ["driver-headroom"],
                ...comparison,
                sources: cells.map(source),
            });
    }
    return {
        observedEvidence: {
            kind: "verified-partial-headroom-negatives",
            plannedPhysicalArms: 12,
            verifiedPhysicalArms: validArms.length,
            plannedCaseCells: 36,
            verifiedCaseCells: summary.cells.filter((c) => c.evidenceValid).length,
            missingArms: report.arms
                .filter((a) => a.valid !== true)
                .map((a) => ({ runId: a.runId, cellId: a.cellId, issues: a.issues })),
            missingCells: summary.cells
                .filter((c) => !c.evidenceValid)
                .map((c) => ({ cellId: c.cellId, caseId: c.caseId })),
            passedGatesGranted: false,
        },
        observedFailures: failures,
    };
}

// Pure projection only. Public IO never accepts these reports or gate objects from an operator.
export function projectReaderContribution(id, report, conditions = null) {
    need(["knownRegression", "driverHeadroom", "observerCost"].includes(id), "ENGINEERING_READER_ID");
    const count = id === "driverHeadroom" ? 12 : 16;
    const expectedKind = {
        knownRegression: "http-known-regression-evidence",
        driverHeadroom: "http-driver-headroom-evidence",
        observerCost: "http-observer-cost-evidence",
    }[id];
    const denominator =
        id === "driverHeadroom"
            ? report?.plannedPhysicalArms === 12 &&
              report?.verifiedPhysicalArms === 12 &&
              report?.plannedCaseCells === 36
            : report?.plannedArms === 16 &&
              (id === "knownRegression"
                  ? report.plannedPairs === 8 && report.verifiedArms === 16
                  : report.plannedCaseRuns === 48);
    const valid =
        report?.kind === expectedKind &&
        denominator &&
        report?.verification?.valid === true &&
        report.verification.inputClosureStable === true &&
        Array.isArray(report.verification.issues) &&
        !report.verification.issues.length &&
        Array.isArray(report.arms) &&
        report.arms.length === count &&
        report.arms.every((a) => a.valid === true && SHA.test(a.rawManifestSha256)) &&
        new Set(report.arms.map((a) => a.runId)).size === count;
    const result = {
        id,
        rawVerified: valid,
        verificationMethod: "new-independent-raw-reader",
        status: "unknown",
        scope: fixedScope(id),
        gates: {},
        evidenceDigests: {},
        conditions,
        limits: limitStrings(report?.limits ?? report?.limitations),
    };
    const refs = report?.references ?? {};
    for (const [key, value] of Object.entries(refs))
        if (key !== "inputSha256" && SHA.test(value)) result.evidenceDigests[key] = value;
    result.evidenceDigests.rawManifestSha256s = (report?.arms ?? [])
        .map((a) => a.rawManifestSha256)
        .filter((v) => SHA.test(v));
    if (!valid) {
        result.limits.push(
            "Independent raw/lifecycle verification or the complete original arm denominator is unavailable.",
        );
        const negatives = id === "driverHeadroom" ? partialHeadroomNegatives(report) : null;
        if (negatives) {
            Object.assign(result, negatives);
            for (const id of ["driver-headroom", "warmup-stability"])
                result.gates[id] = gate(
                    negatives.observedFailures.some((f) => f.gateIds.includes(id)) ? "failed" : "unknown",
                    "Matched independently verified headroom arm/cell negatives retained; incomplete original denominator grants no passed gate.",
                );
            result.status = negatives.observedFailures.length ? "failed" : "unknown";
        }
        return result;
    }
    result.gates["independent-clean-measurement-windows"] = gate(
        "passed",
        "Original exact declared-job commands, modes, lifecycle and nonoverlapping arms verified; scoped producer evidence only.",
    );
    if (id === "knownRegression") {
        const metric = report.detection?.metric,
            analysis = report.analysis?.cells;
        const complete =
            report.kind === "http-known-regression-evidence" &&
            report.plannedArms === 16 &&
            report.plannedPairs === 8 &&
            report.verifiedArms === 16 &&
            metric?.id === "end-to-end-p99" &&
            metric.relativeBudget === 0.05 &&
            metric.absoluteBudget === 5 &&
            metric.minimumObservations === 1000 &&
            report.arms.every((a) => Number.isSafeInteger(a.observations) && a.observations >= 1000) &&
            analysis?.length === 1 &&
            analysis[0].observedPairs === 8 &&
            !analysis[0].issues?.length &&
            !analysis[0].warnings?.length;
        const intervals = report.detection?.intervals;
        const detected =
            complete &&
            report.status === "passed" &&
            report.detection.status === "detected" &&
            intervals?.relative?.low > 0.05 &&
            intervals?.absolute?.low > 5;
        result.status = !complete ? "unknown" : detected ? "passed" : report.status === "failed" ? "failed" : "unknown";
        result.gates["known-regression-detection"] = gate(
            result.status,
            "Only the preregistered forward arithmetic sleep10 E2E-P99 control at 5% AND5ms; no added all-case or1ms requirement.",
        );
    } else if (id === "driverHeadroom") {
        const complete =
            report.kind === "http-driver-headroom-evidence" &&
            report.plannedPhysicalArms === 12 &&
            report.plannedCaseCells === 36 &&
            report.verifiedPhysicalArms === 12 &&
            report.cells?.length === 36 &&
            new Set(report.cells.map((c) => c.cellId + ":" + c.caseId)).size === 36 &&
            report.cells.every(
                (c) => c.evidenceValid === true && c.observations >= 1000 && REPRESENTATIVE_CASES.includes(c.caseId),
            );
        const existingChecks =
            complete &&
            report.cells.every(
                (c) =>
                    c.qualityStatus === "passed" &&
                    c.pressureStatus === "passed" &&
                    c.transitions?.length === 2 &&
                    c.transitions.every((t) => t.status === "passed"),
            ) &&
            report.p99Comparisons?.length === 36 &&
            report.p99Comparisons.every((p) => p.status === "passed");
        result.status =
            report.status === "failed"
                ? "failed"
                : complete &&
                    existingChecks &&
                    report.status === "passed" &&
                    report.limitedScopeEngineeringAccepted === true
                  ? "passed"
                  : "unknown";
        result.gates["driver-headroom"] = gate(
            result.status,
            "Original complete headroom002 criterion bundle, including all preregistered rates; no failed high-rate arm removed.",
        );
        const transitions = complete
            ? report.cells.flatMap((c) =>
                  c.evidenceValid &&
                  c.transitions?.length === 2 &&
                  new Set(c.transitions.map((t) => t.metric)).size === 2 &&
                  c.transitions.every((t) => ["correctResponseMs.p50", "correctResponseMs.p95"].includes(t.metric))
                      ? c.transitions.map((t) => state(t.status))
                      : ["unknown", "unknown"],
              )
            : [];
        const warm = transitions.includes("failed")
            ? "failed"
            : transitions.length === 72 && transitions.every((v) => v === "passed")
              ? "passed"
              : "unknown";
        result.gates["warmup-stability"] = gate(
            warm,
            "Existing last-two-warmup/first-two-measurement response-P50/P95 transition checks only; full measurement steady state is unassessed.",
        );
    } else {
        const complete =
            report.kind === "http-observer-cost-evidence" &&
            report.plannedArms === 16 &&
            report.plannedCaseRuns === 48 &&
            report.evidenceVerified === true &&
            report.groups?.length === 2 &&
            new Set(report.groups.map((g) => g.id)).size === 2 &&
            report.groups.every(
                (g) =>
                    ["J", "C"].includes(g.id) &&
                    g.usable === true &&
                    g.plannedPairs === 4 &&
                    g.plannedMetrics === 6 &&
                    g.analysis?.cells?.length === 6 &&
                    g.analysis.cells.every(
                        (c) =>
                            c.observedPairs === 4 && c.bootstrapIntervals && !c.issues?.length && !c.warnings?.length,
                    ),
            );
        result.status = complete && report.scopedCostMeasured === true ? "passed" : "unknown";
        result.gates["observer-and-validator-cost"] = gate(
            result.status,
            "Three-case conditional costs effectively measured and disclosed; nonzero cost does not fail this gate, and JVM/collector effects are not additive.",
        );
    }
    return result;
}

export function buildCalibrationIndex(contributions) {
    need(
        contributions.length === IDS.length && IDS.every((id, n) => contributions[n].id === id),
        "ENGINEERING_CONTRIBUTION_ORDER",
    );
    return {
        schemaVersion: 1,
        kind: "http-calibration-evidence-index",
        policyId: "engineering-v1",
        fullElevenCaseIds: [...ENGINEERING_CASES],
        contributions: structuredClone(contributions),
        limits: [...FIXED_LIMITS],
    };
}

export function projectEngineeringAcceptance(contributions, comparison = null) {
    const gates = ENGINEERING_GATES.map((id) => {
        const sources = contributions.filter((c) => Object.hasOwn(c.gates, id));
        let status = sources.some((c) => c.gates[id].status === "failed")
            ? "failed"
            : sources.length && sources.every((c) => c.rawVerified && c.gates[id].status === "passed")
              ? "passed"
              : "unknown";
        if (id === "formal-parameter-freeze") status = state(comparison?.status);
        return {
            id,
            status,
            evidenceSources: sources.map((c) => c.id),
            scope: sources.map((c) => c.scope),
            reason:
                id === "formal-parameter-freeze"
                    ? "Exact comparison/index freeze is distinct from calibrated scope coverage."
                    : sources.length
                      ? sources.map((c) => c.gates[id].reason ?? "Existing reader contribution").join(" ")
                      : "Verified contribution unavailable.",
            evidenceDigests: sources.map((c) => c.evidenceDigests),
            limits: [...new Set(sources.flatMap((c) => c.limits))],
        };
    });
    const cases = ENGINEERING_CASES.map((id) => {
        const gateCoverage = gates.map((g) => {
            if (g.id === "formal-parameter-freeze")
                return { id: g.id, status: g.status, coverage: "exact-plan-freeze-only" };
            const source = contributions.filter(
                (c) =>
                    Object.hasOwn(c.gates, g.id) &&
                    (c.scope.caseIds.includes(id) || g.id === "known-regression-detection"),
            );
            const states = source.map((c) =>
                c.rawVerified
                    ? state(c.gates[g.id].status)
                    : c.gates[g.id].status === "failed" &&
                        c.observedFailures?.some((f) => f.caseId === id && f.gateIds.includes(g.id))
                      ? "failed"
                      : "unknown",
            );
            return {
                id: g.id,
                status: states.includes("failed")
                    ? "failed"
                    : states.length && states.every((s) => s === "passed")
                      ? "passed"
                      : "unknown",
                coverage:
                    g.id === "known-regression-detection"
                        ? "fixed-method-control; no per-case target claim"
                        : "only listed original modes/rates/resources",
            };
        });
        return {
            id,
            status: gateCoverage.some((g) => g.status === "failed")
                ? "failed"
                : gateCoverage.every((g) => g.status === "passed")
                  ? "passed"
                  : "unknown",
            gateCoverage,
            limits: REPRESENTATIVE_CASES.includes(id)
                ? []
                : ["No three-representative-case headroom/warmup or observer result establishes this case."],
        };
    });
    const mismatch =
        !comparison ||
        comparison.status !== "passed" ||
        comparison.mismatches?.length ||
        comparison.unknownConditions?.length;
    const accepted =
        gates.every((g) => g.status === "passed") && cases.every((c) => c.status === "passed") && !mismatch;
    return {
        engineering: {
            policyId: "engineering-v1",
            status: gates.some((g) => g.status === "failed") ? "failed" : accepted ? "passed" : "unknown",
            accepted,
            gates,
        },
        coverage: {
            cases,
            fullElevenStatus: cases.every((c) => c.status === "passed") && !mismatch ? "passed" : "unknown",
            modes: [
                ...new Set(contributions.filter((c) => c.rawVerified === true).flatMap((c) => c.scope?.modes ?? [])),
            ].sort(),
            higherRates: "unknown",
            embeddedMode: "unknown",
        },
    };
}

export async function readEngineeringConditions(inputFile, id, references) {
    const input = await json(inputFile),
        plan = await realpath(resolve(dirname(inputFile), input.planDirectory));
    const lockPath = join(plan, "plan.lock.json"),
        lockRaw = await bounded(lockPath, 16 * MiB),
        lock = JSON.parse(lockRaw);
    need(hash(lockRaw) === references.planLockSha256 && Array.isArray(lock.files), "ENGINEERING_CONDITIONS_LOCK");
    const locked = new Map(lock.files.map((f) => [f.path, f]));
    need(locked.size === lock.files.length, "ENGINEERING_CONDITIONS_LOCK");
    async function snapshot(name, optional = false) {
        const entry = locked.get(name);
        if (optional && !entry) return null;
        need(
            entry &&
                Number.isSafeInteger(entry.bytes) &&
                entry.bytes >= 0 &&
                entry.bytes <= 16 * MiB &&
                SHA.test(entry.sha256),
            "ENGINEERING_CONDITION_MEMBER",
        );
        const raw = await bounded(join(plan, name), 16 * MiB);
        need(raw.length === entry.bytes && hash(raw) === entry.sha256, "ENGINEERING_CONDITION_CHANGED");
        return { value: JSON.parse(raw), sha256: hash(raw) };
    }
    const sequence = (await snapshot("sequence.json")).value,
        profiles = [],
        observationArms = [],
        observer = id === "observerCost";
    let settingsVariants;
    if (observer) {
        settingsVariants = await Promise.all(
            ["off", "on"].map(async (switchName) => {
                const path = "settings-jvm-" + switchName + ".json";
                return { id: "jvm-" + switchName, path, ...(await snapshot(path)) };
            }),
        );
        const [off, on] = settingsVariants.map((v) => v.value);
        need(
            off?.kind === "hugegraph-http-environment-settings" &&
                on?.kind === off.kind &&
                !Object.hasOwn(off, "jvmObservation") &&
                on.jvmObservation !== null &&
                typeof on.jvmObservation === "object" &&
                !Array.isArray(on.jvmObservation) &&
                isDeepStrictEqual(off, Object.fromEntries(Object.entries(on).filter(([k]) => k !== "jvmObservation"))),
            "ENGINEERING_OBSERVER_SETTINGS_VARIANTS",
        );
        const schedule = observer003Schedule(sequence.blockOrders);
        need(
            Array.isArray(sequence.physicalArms) &&
                sequence.physicalArms.length === 16 &&
                /^[a-f0-9]{16}$/.test(sequence.attemptToken) &&
                new Set(sequence.physicalArms.map((a) => a?.runId)).size === 16 &&
                sequence.physicalArms.every(
                    (a, i) =>
                        a &&
                        Object.entries(schedule[i]).every(([k, v]) => a[k] === v) &&
                        a.runId === "hoc3-" + sequence.attemptToken + "-" + a.cellId.toLowerCase(),
                ),
            "ENGINEERING_OBSERVER_ARM_SETTINGS_IDENTITY",
        );
    }
    for (const arm of sequence.physicalArms) {
        const directory = "arms/" + (id === "knownRegression" ? arm.blockId + "-" + arm.arm : arm.cellId);
        const profile = await snapshot(directory + "/profile.json");
        profiles.push({ id: arm.runId, ...profile });
        const observation = {
            id: arm.runId,
            driverCpus: id === "driverHeadroom" ? arm.driverCpus : 1,
            collector: id === "knownRegression" ? false : id === "driverHeadroom" ? true : arm.collector,
            jvmObservation: id === "knownRegression" ? false : id === "driverHeadroom" ? true : arm.jvmObservation,
        };
        if (observer) {
            const variant = settingsVariants[arm.jvmObservation ? 1 : 0],
                actual = await snapshot(directory + "/settings.json");
            need(isDeepStrictEqual(actual.value, variant.value), "ENGINEERING_OBSERVER_ARM_SETTINGS_VARIANT");
            Object.assign(observation, {
                group: arm.group,
                blockId: arm.blockId,
                arm: arm.arm,
                cellId: arm.cellId,
                settingsVariantId: variant.id,
                settingsSha256: variant.sha256,
                armSettingsSha256: actual.sha256,
            });
        }
        observationArms.push(observation);
    }
    const result = {
        build: await snapshot("build.json"),
        settings: observer ? null : await snapshot("settings.json"),
        ...(observer ? { settingsVariants } : {}),
        environment: await snapshot("environment.json"),
        runtime: await snapshot("runtime-selection.json", true),
        sourceSelection: await snapshot("source-selection.lock.json"),
        profiles,
        observationArms,
    };
    need(hash(await bounded(lockPath, 16 * MiB)) === hash(lockRaw), "ENGINEERING_CONDITIONS_LOCK_CHANGED");
    return result;
}

export function renderEngineeringAcceptanceHtml(report) {
    const esc = (v) =>
        String(v).replace(
            /[&<>"']/g,
            (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c],
        );
    const status = (value) => `<span class="${state(value)}">${esc(value ?? "unknown")}</span>`;
    const list = (items, empty = "No limits recorded.") =>
        items?.length ? `<ul>${items.map((v) => `<li>${esc(v)}</li>`).join("")}</ul>` : `<p>${esc(empty)}</p>`;
    const table = (headings, rows) =>
        `<div class="table-wrap"><table><thead><tr>${headings.map((h) => `<th scope="col">${esc(h)}</th>`).join("")}</tr></thead><tbody>${rows.join("")}</tbody></table></div>`;
    const jsonText = (value) => `<pre>${esc(JSON.stringify(value, null, 2))}</pre>`;
    const evidenceValue = (value) => {
        if (!value || typeof value !== "object") return jsonText(value);
        return `${Object.hasOwn(value, "value") ? jsonText(value.value) : "<p>Value omitted in the source report; digest only.</p>"}${value.sha256 ? `<small>SHA-256: <code>${esc(value.sha256)}</code></small>` : ""}`;
    };
    const calibratedValues = (values) => {
        const unique = new Map();
        for (const value of values ?? []) {
            const key = JSON.stringify(value),
                entry = unique.get(key) ?? { value, count: 0 };
            entry.count++;
            unique.set(key, entry);
        }
        return unique.size
            ? [...unique.values()]
                  .map(
                      ({ value, count }) =>
                          `<div>${evidenceValue(value)}${count > 1 ? `<p><small>Recorded ${count} times.</small></p>` : ""}</div>`,
                  )
                  .join("")
            : "<p>No calibrated value recorded.</p>";
    };
    const differences = (rows, empty) =>
        rows?.length
            ? table(
                  ["Contribution", "Field", "Formal plan", "Calibrated evidence"],
                  rows.map(
                      (r) =>
                          `<tr><td>${esc(r.contributionId ?? "unknown")}</td><td><code>${esc(r.field ?? "unknown")}</code></td><td>${evidenceValue(r.formal)}</td><td>${calibratedValues(r.calibrated)}</td></tr>`,
                  ),
              )
            : `<p>${esc(empty)}</p>`;
    const unknowns = (rows, empty) =>
        rows?.length
            ? table(
                  ["Contribution", "Field", "Reason"],
                  rows.map(
                      (r) =>
                          `<tr><td>${esc(typeof r === "string" ? "unknown" : (r.contributionId ?? "unknown"))}</td><td><code>${esc(typeof r === "string" ? "unknown" : (r.field ?? "unknown"))}</code></td><td>${esc(typeof r === "string" ? r : (r.reason ?? "No reason recorded."))}</td></tr>`,
                  ),
              )
            : `<p>${esc(empty)}</p>`;
    const comparison = report.comparison;
    const comparisonHtml = comparison
        ? `<p>Content-lock status: ${status(comparison.status)}. ${esc(comparison.gate?.reason ?? "No lock reason recorded.")}</p>
${comparison.conditions ? `<details><summary>Declared formal conditions</summary>${jsonText(comparison.conditions)}</details>` : ""}
<h3>Actual mismatches (${comparison.mismatches?.length ?? 0})</h3>${differences(comparison.mismatches, "No actual mismatches recorded. This does not resolve unknown conditions.")}
<h3>Unknown conditions (${comparison.unknownConditions?.length ?? 0})</h3>${unknowns(comparison.unknownConditions, "No unknown conditions recorded.")}
<h3>Known-method control disclosures (${comparison.methodControlDisclosures?.length ?? 0})</h3>
<p>The fixed positive control retains its declared method scope. Its non-blocking disclosures grant no per-case, embedded-mode, headroom or observer-cost coverage.</p>
${comparison.methodControlDisclosures?.length ? comparison.methodControlDisclosures.map((d) => `<details><summary>${esc(d.contributionId)} · ${esc(d.kind)} · blocking: ${esc(d.blocking)}</summary><p>Original gate ${esc(d.gateId)}: ${status(d.originalGateStatus)}</p><p>${esc(d.interpretation ?? "No interpretation recorded.")}</p><h4>Original control scope</h4>${jsonText(d.originalScope)}<h4>Disclosed differences (${d.differences?.length ?? 0})</h4>${differences(d.differences, "No method-control differences recorded.")}<h4>Disclosed unknown conditions (${d.unknownConditions?.length ?? 0})</h4>${unknowns(d.unknownConditions, "No method-control unknown conditions recorded.")}</details>`).join("") : "<p>No method-control disclosure recorded.</p>"}
${list(comparison.limits)}`
        : "<p>No formal comparison was supplied. Its parameter freeze and calibrated-condition match remain unknown.</p>";
    const caseRows = report.coverage.cases.map((c) => {
        const rows = (c.gateCoverage ?? []).map((g) => {
            const originalGate = report.engineering.gates.find((original) => original.id === g.id);
            return `<tr><td><code>${esc(g.id)}</code></td><td>${status(g.status)}</td><td>${esc(g.coverage ?? "No coverage recorded.")}</td><td>${list(g.limits ?? originalGate?.limits)}</td></tr>`;
        });
        return `<tr><td><code>${esc(c.id)}</code></td><td>${status(c.status)}</td><td><details class="case-details"><summary>${rows.length} recorded gate results and limits</summary>${rows.length ? table(["Gate", "Status", "Coverage", "Gate limits"], rows) : "<p>No per-gate coverage recorded.</p>"}<h4>Case limits</h4>${list(c.limits)}</details></td></tr>`;
    });
    const partialNegatives = (report.contributions ?? []).filter((c) => c.observedEvidence);
    const negativeHtml = partialNegatives.length
        ? partialNegatives
              .map(
                  (c) =>
                      `<h3>${esc(c.id)}</h3><p>Verified physical arms: ${esc(c.observedEvidence.verifiedPhysicalArms)}/${esc(c.observedEvidence.plannedPhysicalArms)}; verified case cells: ${esc(c.observedEvidence.verifiedCaseCells)}/${esc(c.observedEvidence.plannedCaseCells)}. Full raw verification: false. Partial evidence grants no passed gate; missing cells remain unknown.</p><details><summary>Missing original arms and cells</summary>${jsonText({ arms: c.observedEvidence.missingArms, cells: c.observedEvidence.missingCells })}</details>${
                          c.observedFailures.length
                              ? table(
                                    [
                                        "Case",
                                        "Failed criterion and gates",
                                        "Observed evidence",
                                        "Matched original raw/run sources",
                                    ],
                                    c.observedFailures.map((f) => {
                                        const { caseId, criterion, gateIds, sources, ...values } = f;
                                        return `<tr><td><code>${esc(caseId)}</code></td><td>${status("failed")} ${esc(criterion)}<br>${esc(gateIds.join(", "))}</td><td>${jsonText(values)}</td><td>${jsonText(sources)}</td></tr>`;
                                    }),
                                )
                              : "<p>No matched observed failure. Incomplete evidence remains unknown.</p>"
                      }`,
              )
              .join("")
        : "<p>No verified partial headroom negatives recorded.</p>";
    return `<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>HG-Bench engineering evidence</title>
<style>body{font:16px/1.5 system-ui;max-width:1200px;margin:32px auto;padding:0 20px;color:#17212d}h2{margin-top:36px}table{border-collapse:collapse;width:100%}th,td{padding:10px;text-align:left;vertical-align:top;border-bottom:1px solid #cbd5e1}th{background:#f1f5f9}code,pre{overflow-wrap:anywhere;white-space:pre-wrap}pre{font-size:13px;background:#f8fafc;padding:10px}li{margin:8px 0}details{border:1px solid #cbd5e1;border-radius:6px;padding:10px;margin:12px 0}summary{cursor:pointer;font-weight:600}.case-details{margin:0}.table-wrap{overflow-x:auto}td ul{padding-left:20px;margin-top:0}small{color:#475569}.unknown{color:#8a5800}.failed{color:#a32020}.passed{color:#146132}</style>
<h1>Engineering evidence: <span class="${esc(report.engineering.status)}">${esc(report.engineering.status)}</span></h1><p>Performance and qualification accepted: false. Scoped calibration evidence is distinct from full11 coverage.</p>
<p><a href="report.json">Report JSON</a> · <a href="calibration-evidence-index.json">Calibration index</a> · <a href="report.lock.json">Output lock</a></p>
<table><thead><tr><th>Original gate</th><th>Status</th><th>Scope and reason</th></tr></thead><tbody>${report.engineering.gates.map((g) => `<tr><td>${esc(g.id)}</td><td class="${esc(g.status)}">${esc(g.status)}</td><td>${esc(g.reason)}<br><small>${esc(g.scope.map((s) => s.kind + ": " + s.caseIds.join(", ") + "; modes " + (s.modes?.join(",") || "unknown") + "; rates " + s.ratesPerSecond.join(",")).join(" | "))}</small></td></tr>`).join("")}</tbody></table>
<h2>Frozen comparison and calibrated coverage</h2><p>A passed formal-parameter-freeze gate establishes the exact comparison and calibration-index content lock. It does not establish calibrated modes, workloads, loads, resources or observer settings. Actual mismatches and unknown conditions below retain their original meaning.</p>${comparisonHtml}
<h2>All eleven cases</h2>${table(["Case", "Status", "Recorded gate coverage and limits"], caseRows)}
<h2>Observed failures in partial headroom evidence</h2>${negativeHtml}
<h2>Limits</h2><ul>${report.limits.map((s) => `<li>${esc(s)}</li>`).join("")}</ul><p>No saved Docker, HTTP, SSH or task commands were executed by this report.</p></html>\n`;
}

export async function reportEngineeringAcceptance({ inputFile, outputDirectory, timeoutMs, maxBytes, signal }) {
    need(
        Number.isSafeInteger(timeoutMs) &&
            timeoutMs >= 1000 &&
            timeoutMs <= 86400000 &&
            Number.isSafeInteger(maxBytes) &&
            maxBytes >= MiB &&
            maxBytes <= 8 * 1024 ** 3,
        "ENGINEERING_OPTIONS",
    );
    const begun = performance.now(),
        deadline = begun + timeoutMs,
        workDeadline = deadline - Math.min(5000, timeoutMs / 3);
    const cancellation = new AbortController();
    const interrupt = () => cancellation.abort("ENGINEERING_CANCELLED");
    const timer = setTimeout(
        () => cancellation.abort("ENGINEERING_DEADLINE"),
        Math.max(1, workDeadline - performance.now()),
    );
    signal?.addEventListener("abort", interrupt, { once: true });
    process.on("SIGTERM", interrupt);
    process.on("SIGINT", interrupt);
    let output,
        owned = false;
    const check = () => {
        need(
            !signal?.aborted && !cancellation.signal.aborted,
            String(cancellation.signal.reason ?? "ENGINEERING_CANCELLED"),
        );
        need(performance.now() < workDeadline, "ENGINEERING_DEADLINE");
    };
    try {
        check();
        inputFile = resolve(inputFile);
        const sourceRaw = await bounded(inputFile),
            input = validateEngineeringInput(JSON.parse(sourceRaw)),
            snapshot = structuredClone(input),
            protectedPaths = [inputFile];
        output = join(await realpath(dirname(resolve(outputDirectory))), basename(resolve(outputDirectory)));
        const sources = {};
        for (const id of IDS) {
            const row = input.evidence[id];
            if (row === null) continue;
            if (id === "repeatabilityAa") {
                snapshot.evidence[id].planDirectory = await realpath(resolve(dirname(inputFile), row.planDirectory));
                snapshot.evidence[id].executionSetFile = resolve(dirname(inputFile), row.executionSetFile);
                protectedPaths.push(snapshot.evidence[id].planDirectory, snapshot.evidence[id].executionSetFile);
                const setRaw = await bounded(snapshot.evidence[id].executionSetFile, 8 * MiB);
                need(hash(setRaw) === row.executionSetSha256, "ENGINEERING_EVIDENCE_INPUT_SHA");
                const set = JSON.parse(setRaw);
                need(
                    set.schemaVersion === 1 &&
                        set.kind === "http-calibration-execution-set" &&
                        Array.isArray(set.experiments) &&
                        set.experiments.length <= 10000,
                    "ENGINEERING_AA_EXECUTION_SET",
                );
                for (const experiment of set.experiments) {
                    pathValue(experiment.executionDirectory);
                    need(experiment.executionDirectory.length <= 2048, "ENGINEERING_INPUT_PATH");
                    protectedPaths.push(
                        await canonical(
                            resolve(dirname(snapshot.evidence[id].executionSetFile), experiment.executionDirectory),
                        ),
                    );
                }
                continue;
            }
            if (id === "aa005") {
                const path = await realpath(resolve(dirname(inputFile), row.evidenceRoot));
                snapshot.evidence[id].evidenceRoot = path;
                protectedPaths.push(path);
                continue;
            }
            const path = resolve(dirname(inputFile), row.inputFile),
                raw = await bounded(path);
            need(hash(raw) === row.sha256, "ENGINEERING_EVIDENCE_INPUT_SHA");
            const typed = JSON.parse(raw);
            need(
                typeof typed.planDirectory === "string" && typeof typed.executionDirectory === "string",
                "ENGINEERING_READER_INPUT",
            );
            const plan = await realpath(resolve(dirname(path), typed.planDirectory)),
                execution = await realpath(resolve(dirname(path), typed.executionDirectory));
            protectedPaths.push(plan, execution, path);
            const specRaw = await bounded(join(execution, "execution-spec.json"), MiB),
                spec = JSON.parse(specRaw);
            need(hash(specRaw) === typed.reference?.executionSpecSha256, "ENGINEERING_EVIDENCE_SPEC_SHA");
            pathValue(spec.sourceRoot);
            need(spec.sourceRoot.startsWith("/"), "ENGINEERING_INPUT_PATH");
            protectedPaths.push(await canonical(spec.sourceRoot));
            if (id === "driverHeadroom") {
                const runtime = await json(join(plan, "runtime-selection.json")),
                    settings = await json(join(plan, "settings.json"));
                for (const selected of [runtime.nodeHome, settings.distribution, settings.javaHome]) {
                    pathValue(selected);
                    need(selected.startsWith("/"), "ENGINEERING_INPUT_PATH");
                    protectedPaths.push(await canonical(selected));
                }
                for (const name of [
                    "assessmentBindingFile",
                    "preDispatchLaunchRecordFile",
                    "controllerStartEvidenceFile",
                ]) {
                    pathValue(typed[name]);
                    protectedPaths.push(await canonical(resolve(dirname(path), typed[name])));
                }
            }
            sources[id] = { path, raw };
            snapshot.evidence[id].inputFile = path;
        }
        if (input.comparison !== null) {
            snapshot.comparison.planDirectory = await realpath(
                resolve(dirname(inputFile), input.comparison.planDirectory),
            );
            snapshot.comparison.calibrationIndexFile = resolve(
                dirname(inputFile),
                input.comparison.calibrationIndexFile,
            );
            protectedPaths.push(snapshot.comparison.planDirectory, snapshot.comparison.calibrationIndexFile);
        }
        for (const name of [
            "components",
            "src",
            "dist",
            "release",
            "benchmarks",
            "presets",
            "adapters",
            "tools",
            "examples",
            "docs",
            "tests-normative",
            "node_modules",
            "package.json",
            "package-lock.json",
        ]) {
            try {
                protectedPaths.push(await realpath(join(ROOT, name)));
            } catch (e) {
                if (e.code !== "ENOENT") throw e;
            }
        }
        need(!protectedPaths.some((p) => beneath(output, p) || beneath(p, output)), "ENGINEERING_OUTPUT_OVERLAP");
        await mkdir(output);
        owned = true;
        async function save(name, value, text = false) {
            const raw = text ? Buffer.from(value) : encoded(value),
                used = await inventory(output, maxBytes, deadline);
            need(used.bytes + raw.length <= maxBytes, "ENGINEERING_OUTPUT_BYTES");
            await writeFile(join(output, name), raw, { flag: "wx", mode: 0o600 });
        }
        await save("input.json", snapshot);
        const contributions = IDS.map((id) => missingContribution(id));
        const rebuilds = [],
            issues = [];
        const readerFns = {
            knownRegression: reportKnownRegression,
            driverHeadroom: reportHeadroom,
            observerCost: reportObserverCost,
        };
        for (let n = 0; n < IDS.length; n++) {
            const id = IDS[n],
                row = snapshot.evidence[id];
            if (row === null) continue;
            try {
                check();
                if (id === "aa005") {
                    const { verifyAa005Contribution } = await import("./engineering-aa005.mjs");
                    const result = await verifyAa005Contribution({
                        evidenceRoot: row.evidenceRoot,
                        signal: cancellation.signal,
                        deadline: workDeadline,
                        maxBytes: 2 * 1024 ** 3,
                    });
                    need(
                        result.id === id && result.rawVerified === true && result.gates?.["repeatability-aa"],
                        "ENGINEERING_AA_PROTOCOL",
                    );
                    contributions[n] = {
                        id,
                        rawVerified: true,
                        verificationMethod: "previous-independent-replay-chain/no-new-raw-analysis",
                        status: state(result.status),
                        scope: result.scope,
                        gates: result.gates,
                        evidenceDigests: result.evidenceDigests ?? {},
                        conditions: result.conditions ?? null,
                        limits: limitStrings(result.limits),
                    };
                    rebuilds.push({
                        id,
                        method: contributions[n].verificationMethod,
                        empiricalCertification: result.empiricalCertification ?? null,
                    });
                } else if (id === "repeatabilityAa") {
                    const { reportCurrentAaContribution } = await import("./engineering-aa-current.mjs");
                    const remaining = maxBytes - (await inventory(output, maxBytes, deadline)).bytes;
                    const result = await reportCurrentAaContribution({
                        reference: row,
                        outputDirectory: join(output, "rebuild-" + id),
                        signal: cancellation.signal,
                        deadline: workDeadline,
                        maxBytes: remaining - 256 * 1024,
                    });
                    need(
                        result.id === id &&
                            typeof result.rawVerified === "boolean" &&
                            result.scope &&
                            result.gates?.["repeatability-aa"],
                        "ENGINEERING_AA_PROTOCOL",
                    );
                    contributions[n] = {
                        id,
                        rawVerified: result.rawVerified,
                        status: state(result.status),
                        verificationMethod: result.verificationMethod,
                        scope: result.scope,
                        gates: result.gates,
                        evidenceDigests: result.evidenceDigests ?? {},
                        conditions: result.conditions ?? null,
                        limits: limitStrings(result.limits),
                    };
                    rebuilds.push({
                        id,
                        method: result.verificationMethod,
                        empiricalCertification: result.empiricalCertification ?? null,
                    });
                    await inventory(output, maxBytes, deadline);
                } else {
                    const used = await inventory(output, maxBytes, deadline),
                        remaining = maxBytes - used.bytes;
                    // Allow bounded temporary and final reader output inside the one aggregate cap.
                    const childBytes = Math.floor((remaining - 256 * 1024) / 2);
                    need(childBytes >= MiB && workDeadline - performance.now() >= 1500, "ENGINEERING_REBUILD_RESERVE");
                    const child = join(output, "rebuild-" + id);
                    await readerFns[id]({
                        inputFile: sources[id].path,
                        outputDirectory: child,
                        timeoutMs: Math.floor(workDeadline - performance.now()),
                        maxBytes: childBytes,
                        signal: cancellation.signal,
                    });
                    check();
                    const report = await json(join(child, "report.json"), 16 * MiB),
                        conditions =
                            report.verification?.valid === true
                                ? await readEngineeringConditions(sources[id].path, id, report.references)
                                : null;
                    need(hash(await bounded(sources[id].path)) === row.sha256, "ENGINEERING_EVIDENCE_INPUT_CHANGED");
                    contributions[n] = projectReaderContribution(id, report, conditions);
                    rebuilds.push({
                        id,
                        directory: "rebuild-" + id,
                        reportSha256: hash(await bounded(join(child, "report.json"), 16 * MiB)),
                        reportLockSha256: hash(await bounded(join(child, "report.lock.json"), MiB)),
                    });
                    await inventory(output, maxBytes, deadline);
                }
            } catch (e) {
                contributions[n] = missingContribution(id, e.code ?? "ENGINEERING_REBUILD_FAILED");
                issues.push({ id, code: e.code ?? "ENGINEERING_REBUILD_FAILED" });
            }
        }
        const index = buildCalibrationIndex(contributions),
            indexRaw = encoded(index);
        await save("calibration-evidence-index.json", index);
        let comparison = null;
        if (snapshot.comparison !== null) {
            try {
                check();
                const { verifyEngineeringComparison } = await import("./engineering-comparison.mjs");
                comparison = await verifyEngineeringComparison({
                    comparison: snapshot.comparison,
                    index,
                    contributions,
                    signal: cancellation.signal,
                    deadline: workDeadline,
                    maxBytes: maxBytes - (await inventory(output, maxBytes, deadline)).bytes,
                });
            } catch (e) {
                comparison = {
                    status: "unknown",
                    mismatches: [],
                    unknownConditions: [e.code ?? "ENGINEERING_COMPARISON_FAILED"],
                    limits: [],
                };
                issues.push({ id: "comparison", code: e.code ?? "ENGINEERING_COMPARISON_FAILED" });
            }
        }
        need(hash(await bounded(inputFile)) === hash(sourceRaw), "ENGINEERING_INPUT_CHANGED");
        for (const [id, source] of Object.entries(sources))
            need(
                hash(await bounded(source.path)) === snapshot.evidence[id].sha256,
                "ENGINEERING_EVIDENCE_INPUT_CHANGED",
            );
        const projection = projectEngineeringAcceptance(contributions, comparison);
        const implementation = [];
        for (const name of [
            "engineering-acceptance.mjs",
            "engineering-aa005.mjs",
            "engineering-aa-current.mjs",
            "engineering-comparison.mjs",
            "known-regression-evidence.mjs",
            "headroom-evidence.mjs",
            "headroom002-contract.mjs",
            "observer-cost-evidence.mjs",
            "observer003-contract.mjs",
        ]) {
            try {
                const raw = await bounded(join(ROOT, "components/http", name), MiB);
                implementation.push({ path: "components/http/" + name, bytes: raw.length, sha256: hash(raw) });
            } catch (e) {
                if (e.code !== "ENOENT") throw e;
            }
        }
        const report = {
            schemaVersion: 1,
            kind: "http-engineering-acceptance-report",
            ...projection,
            contributions,
            comparison,
            rebuilds,
            implementation,
            sourceInputSha256: hash(sourceRaw),
            calibrationIndexSha256: hash(indexRaw),
            issues,
            partial: issues.length > 0 || cancellation.signal.aborted,
            cancelled: signal?.aborted === true || cancellation.signal.reason === "ENGINEERING_CANCELLED",
            empiricalCertification: rebuilds.find((r) => ["aa005", "repeatabilityAa"].includes(r.id))
                ?.empiricalCertification ?? { status: "unknown", accepted: false },
            limits: [...FIXED_LIMITS],
            budgets: {
                timeoutMs,
                maxBytes,
                elapsedMs: performance.now() - begun,
                inputCaps:
                    "AA2GiB; independent readers retain their existing input caps; maxBytes covers new output and temporary files only",
            },
            qualificationAccepted: false,
            performanceAccepted: false,
        };
        if (report.partial) {
            report.engineering.accepted = false;
            if (report.engineering.status === "passed") report.engineering.status = "unknown";
        }
        await save("report.json", report);
        await save("report.html", renderEngineeringAcceptanceHtml(report), true);
        need(performance.now() < deadline, "ENGINEERING_DEADLINE");
        const files = await inventory(output, maxBytes, deadline);
        await save("report.lock.json", {
            schemaVersion: 1,
            kind: "http-engineering-acceptance-report-lock",
            files: files.files,
            contentDigest: valueHash(files.files),
            calibrationIndexSha256: hash(indexRaw),
            qualificationAccepted: false,
            performanceAccepted: false,
        });
        if (cancellation.signal.aborted) need(false, String(cancellation.signal.reason));
        return {
            kind: report.kind,
            directory: output,
            reportJson: join(output, "report.json"),
            reportHtml: join(output, "report.html"),
            reportLock: join(output, "report.lock.json"),
            calibrationIndex: join(output, "calibration-evidence-index.json"),
            engineeringAccepted: report.engineering.accepted,
            performanceAccepted: false,
            qualificationAccepted: false,
        };
    } catch (e) {
        if (owned) {
            await rm(join(output, "report.lock.json"), { force: true }).catch(() => {});
            const raw = encoded({
                schemaVersion: 1,
                kind: "http-engineering-acceptance-partial",
                status: "unknown",
                error: e.code ?? "ENGINEERING_ERROR",
                engineeringAccepted: false,
                qualificationAccepted: false,
                performanceAccepted: false,
            });
            try {
                if ((await inventory(output, maxBytes, deadline)).bytes + raw.length <= maxBytes)
                    await writeFile(join(output, "partial.json"), raw, { flag: "wx", mode: 0o600 });
            } catch {}
        }
        throw e;
    } finally {
        clearTimeout(timer);
        signal?.removeEventListener("abort", interrupt);
        process.removeListener("SIGTERM", interrupt);
        process.removeListener("SIGINT", interrupt);
    }
}

async function main() {
    const args = process.argv.slice(2);
    if (args.length === 1 && args[0] === "--help") {
        process.stdout.write(
            "Offline engineering sidecar: --input INPUT --output NEW --timeout-ms INTEGER --max-bytes INTEGER\nRebuilds known/headroom/observer raw with existing readers; AA005 uses its immutable prior replay chain, or repeatabilityAa rebuilds current-tool A/A raw; the two inputs are exclusive. Missing/partial/scope mismatch stays unknown. One overall deadline and aggregate new-output/temp cap; no HTTP/Docker/SSH or stored commands. Full11 and performance qualification remain separate.\n",
        );
        return;
    }
    const values = {};
    for (let i = 0; i < args.length; i += 2) {
        need(
            ["--input", "--output", "--timeout-ms", "--max-bytes"].includes(args[i]) && args[i + 1] && !values[args[i]],
            "ENGINEERING_ARGUMENTS",
        );
        values[args[i]] = args[i + 1];
    }
    need(
        Object.keys(values).length === 4 &&
            /^[0-9]+$/.test(values["--timeout-ms"]) &&
            /^[0-9]+$/.test(values["--max-bytes"]),
        "ENGINEERING_ARGUMENTS",
    );
    const data = await reportEngineeringAcceptance({
        inputFile: values["--input"],
        outputDirectory: values["--output"],
        timeoutMs: Number(values["--timeout-ms"]),
        maxBytes: Number(values["--max-bytes"]),
    });
    process.stdout.write(JSON.stringify({ ok: true, data }) + "\n");
}
if (process.argv[1] && pathToFileURL(resolve(process.argv[1])).href === import.meta.url)
    main().catch((e) => {
        process.stderr.write(
            JSON.stringify({ ok: false, error: { code: e.code ?? "ENGINEERING_ERROR", message: e.message } }) + "\n",
        );
        process.exitCode = 6;
    });
