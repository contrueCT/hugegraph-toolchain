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

import { constants } from "node:fs";
import { open, mkdir, writeFile, lstat, readdir } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { isDeepStrictEqual } from "node:util";
import { balancedOrders, valueHash, validatePolicy, ALGORITHM } from "../paired/core.mjs";
import { bytesDigest } from "./common.mjs";
import { verifySuiteEvidence } from "./suite-evidence.mjs";
import { verifyDependencyLock } from "./dependency-lock.mjs";

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "../..");
const MiB = 1024 * 1024;
const OUTPUT_LIMIT = 64 * MiB;
const SHA = /^[a-f0-9]{64}$/;
const ID = /^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/;
const ROLES = ["build", "environment", "settings"];
const SUITE_FILES = [
    "catalog.json",
    "answers.json",
    "protocol.json",
    "scoring-policy.json",
    "calibration-plan.json",
    "release.json",
    "spec.md",
    "reference.mjs",
];
const SOURCE_FILES = [
    "components/http/comparison-plan.mjs",
    "components/http/dependency-lock.mjs",
    "components/http/common.mjs",
    "components/http/suite-evidence.mjs",
    "components/paired/core.mjs",
    "src/http-contracts.ts",
    "dist/http-contracts.js",
    "package.json",
    "release/dependency-lock.json",
];
const PHASES = ["prepareMs", "startMs", "modeProbeMs", "settleMs", "stopMs", "sealAndAnalyzeMs"];

export class ComparisonPlanError extends Error {
    constructor(code, message) {
        super(message);
        this.code = code;
    }
}
function need(condition, code, message) {
    if (!condition) throw new ComparisonPlanError(code, message);
}
function keys(value, expected, label) {
    need(
        value &&
            typeof value === "object" &&
            !Array.isArray(value) &&
            isDeepStrictEqual(Object.keys(value).sort(), [...expected].sort()),
        "COMPARISON_SCHEMA",
        `${label} fields differ from the versioned contract`,
    );
}
function integer(value, low, high, label) {
    need(Number.isSafeInteger(value) && value >= low && value <= high, "COMPARISON_BUDGET", `Invalid ${label}`);
}
function seed(value) {
    need(
        typeof value === "string" && value.length > 0 && value.length <= 128,
        "COMPARISON_SCHEMA",
        "Seeds must contain 1..128 characters",
    );
}
function encode(value) {
    return Buffer.from(JSON.stringify(value, null, 2) + "\n");
}
function parse(raw, label) {
    try {
        return JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(raw));
    } catch {
        throw new ComparisonPlanError("COMPARISON_JSON", `Invalid UTF-8 JSON in ${label}`);
    }
}
async function boundedFile(path, max = 8 * MiB) {
    let handle;
    try {
        // A FIFO must not block before fstat can reject it as non-regular input.
        handle = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
        const before = await handle.stat();
        need(before.isFile() && before.size <= max, "COMPARISON_FILE", "Input must be a bounded regular file");
        const buffer = Buffer.alloc(before.size + 1);
        let length = 0;
        while (length < buffer.length) {
            const { bytesRead } = await handle.read(buffer, length, buffer.length - length, length);
            if (!bytesRead) break;
            length += bytesRead;
        }
        const after = await handle.stat();
        need(
            length === before.size && before.size === after.size && before.mtimeMs === after.mtimeMs,
            "COMPARISON_FILE_CHANGED",
            "Input changed while it was read",
        );
        return buffer.subarray(0, length);
    } catch (error) {
        if (error instanceof ComparisonPlanError) throw error;
        throw new ComparisonPlanError("COMPARISON_FILE", "Input is missing, inaccessible, or a symlink");
    } finally {
        await handle?.close();
    }
}
function fileEntry(path, raw) {
    return { path, bytes: raw.length, sha256: bytesDigest(raw) };
}

export function validateRequest(request) {
    const v2 = request?.schemaVersion === 2;
    const v3 = request?.schemaVersion === 3;
    const single = v3 && request.comparison === "single-observation";
    keys(
        request,
        [
            "schemaVersion",
            "kind",
            "id",
            "purpose",
            "comparison",
            "modes",
            "blockIds",
            ...(!single ? ["orderSeed"] : []),
            "sharedEvidence",
            "analysis",
            "budgets",
            ...(v2 || (v3 && !single) ? ["metricRoles"] : []),
        ],
        "request",
    );
    need(
        (request.schemaVersion === 1 || v2 || v3) &&
            request.kind === "http-comparison-request" &&
            typeof request.id === "string" &&
            ID.test(request.id),
        "COMPARISON_SCHEMA",
        "Unsupported comparison request identity",
    );
    if (v2 || (v3 && !single)) validateMetricRoles(request.metricRoles, { allowGoodputInference: v3 });
    need(
        ["functional", "calibration"].includes(request.purpose),
        "COMPARISON_CALIBRATION",
        "Only functional/calibration plans are supported; formal acceptance is unavailable",
    );
    keys(request.modes, single ? ["O"] : ["A", "B"], "modes");
    need(
        Object.values(request.modes).every((mode) => ["forward", "embedded"].includes(mode)),
        "COMPARISON_MODE",
        "Modes must be forward or embedded",
    );
    need(
        single ||
            (request.comparison === "aa" && request.modes.A === request.modes.B) ||
            (request.comparison === "forward-embedded" &&
                request.modes.A === "forward" &&
                request.modes.B === "embedded"),
        "COMPARISON_MODE",
        "A/A requires equal modes; forward-embedded requires A=forward and B=embedded",
    );
    need(Array.isArray(request.blockIds), "COMPARISON_SCHEMA", "blockIds must be explicit");
    integer(request.blockIds.length, single ? 1 : 2, single ? 1 : 1000, "block count");
    need(
        (single || request.blockIds.length % 2 === 0) &&
            request.blockIds.every((id) => typeof id === "string" && ID.test(id)) &&
            new Set(request.blockIds).size === request.blockIds.length,
        "COMPARISON_BLOCKS",
        "Use unique explicit block IDs and an even count for exact AB/BA balance",
    );
    if (!single) seed(request.orderSeed);
    keys(request.sharedEvidence, ROLES, "sharedEvidence");
    for (const role of ROLES) {
        const file = request.sharedEvidence[role];
        keys(file, ["path", "sha256"], `sharedEvidence.${role}`);
        need(
            typeof file.path === "string" && file.path.length > 0 && file.path.length <= 4096 && SHA.test(file.sha256),
            "COMPARISON_IDENTITY",
            "Each shared identity needs a file and expected SHA-256",
        );
    }
    keys(
        request.analysis,
        single
            ? ["maxMilliseconds", "latency", "goodput"]
            : [
                  "familyAlpha",
                  "minimumPairs",
                  "bootstrapReplicates",
                  "minimumTailDraws",
                  "maxWork",
                  "maxMilliseconds",
                  "resampleSeed",
                  "latency",
                  "goodput",
              ],
        "analysis",
    );
    keys(
        request.analysis.latency,
        [
            ...(!single ? ["relativeBudget", "absoluteBudget"] : []),
            "minimumObservations",
            ...(Object.hasOwn(request.analysis.latency ?? {}, "quantile") ? ["quantile"] : []),
        ],
        "latency",
    );
    need(
        (!single && !Object.hasOwn(request.analysis.latency, "quantile")) ||
            ["p50", "p95", "p99"].includes(request.analysis.latency.quantile),
        "COMPARISON_SCHEMA",
        "analysis.latency.quantile must be p50, p95 or p99; omission preserves p99",
    );
    keys(
        request.analysis.goodput,
        single ? ["minimumObservations"] : ["relativeBudget", "absoluteBudget", "minimumObservations"],
        "goodput",
    );
    if (single) integer(request.analysis.maxMilliseconds, 1, 300000, "analysis time budget");
    if (v2 || v3)
        for (const kind of ["latency", "goodput"]) {
            const threshold = request.analysis[kind];
            need(
                (single ||
                    (Number.isFinite(threshold.relativeBudget) &&
                        threshold.relativeBudget >= 0 &&
                        Number.isFinite(threshold.absoluteBudget) &&
                        threshold.absoluteBudget >= 0)) &&
                    Number.isSafeInteger(threshold.minimumObservations) &&
                    threshold.minimumObservations >= 1,
                "COMPARISON_POLICY",
                "Invalid frozen metric thresholds",
            );
        }
    keys(
        request.budgets,
        [
            ...PHASES,
            "maxEvidenceBytesPerArm",
            "maxTotalEvidenceBytes",
            "maxTotalMilliseconds",
            ...(v3 ? ["budgetMode"] : []),
        ],
        "budgets",
    );
    for (const name of PHASES) integer(request.budgets[name], name === "settleMs" ? 0 : 1, 300000, name);
    integer(request.budgets.maxEvidenceBytesPerArm, 4096, 1024 * MiB, "per-arm evidence budget");
    integer(request.budgets.maxTotalEvidenceBytes, 4096, 1024 * 1024 * MiB, "total evidence budget");
    integer(request.budgets.maxTotalMilliseconds, 1, 7 * 24 * 3600000, "total time budget");
    if (v3) {
        need(
            ["total", "stage-sum"].includes(request.budgets.budgetMode),
            "COMPARISON_BUDGET_MODE",
            "Version 3 requires an explicit budgetMode",
        );
        need(
            request.budgets.maxTotalMilliseconds > request.budgets.stopMs + 10000,
            "COMPARISON_TIME_BUDGET",
            "Total deadline must leave declared stop and terminal-evidence reserve",
        );
    }
    return request;
}

export function validateMetricRoles(roles, { allowGoodputInference = false } = {}) {
    keys(roles, ["latency", "goodput"], "metricRoles");
    need(
        roles.latency === "inference" &&
            (roles.goodput === "quality" || (allowGoodputInference && roles.goodput === "inference")),
        "COMPARISON_METRIC_ROLES",
        "Version 2 requires latency=inference and goodput=quality",
    );
    return roles;
}

async function validateProfile(raw, root) {
    let schema;
    try {
        ({ httpAnyProfileSchema: schema } = await import("../../dist/http-contracts.js"));
    } catch {
        throw new ComparisonPlanError("COMPARISON_CONTRACT_UNAVAILABLE", "Run npm run build before freezing a plan");
    }
    const result = schema.safeParse(parse(raw, "profile"));
    need(result.success, "COMPARISON_PROFILE", "HTTP profile does not satisfy the shared controller contract");
    const profile = result.data;
    if (profile.schemaVersion === 2)
        need(
            profile.executor.hostIdentityGate === true,
            "COMPARISON_GO_IDENTITY_GATE",
            "Go comparisons require the live host identity gate and runtime v2",
        );
    let endpoint;
    try {
        endpoint = new URL(profile.endpoint);
    } catch {
        /* Rejected below. */
    }
    need(
        endpoint &&
            ["http:", "https:"].includes(endpoint.protocol) &&
            !endpoint.username &&
            !endpoint.password &&
            !endpoint.search &&
            !endpoint.hash &&
            endpoint.pathname === "/gremlin",
        "COMPARISON_ENDPOINT",
        "Use a /gremlin HTTP(S) endpoint without credentials, query or fragment",
    );
    need(
        profile.repeats === 1 && profile.serviceChecks === true && profile.benchmark,
        "COMPARISON_PROFILE",
        "Each fresh-process arm needs one complete suite run with preflight/postcheck",
    );
    for (const items of [profile.cases, profile.loads])
        need(
            new Set(items.map((item) => item.id)).size === items.length,
            "COMPARISON_PROFILE",
            "Duplicate case or load ID",
        );
    for (const load of profile.loads)
        if (load.model === "open")
            need(
                Math.ceil((Math.max(profile.warmupMs, profile.durationMs) * load.ratePerSecond) / 1000) <=
                    profile.maxRequestsPerPhase,
                "COMPARISON_REQUEST_BUDGET",
                "Open schedule exceeds the request budget",
            );
    if (profile.clockReference) {
        const spec = profile.clockReference.spec;
        const runMs =
            (profile.warmupMs ? profile.warmupMs + profile.drainMs : 0) + profile.durationMs + profile.drainMs;
        const probeMs = profile.requestTimeoutMs + profile.drainMs;
        need(
            spec.wallMs >= spec.startupMs + 2 * spec.exchangeMs + spec.finishMs + Math.max(runMs, probeMs) + 1000,
            "COMPARISON_CLOCK_REFERENCE_TIME_BUDGET",
            "Reference wall budget must cover exchanges, the largest run/probe including drain, and cleanup",
        );
    }
    need(
        encode(profile).length * profile.cases.length * profile.loads.length +
            (profile.clockReference ? 4 * profile.clockReference.spec.maxRawBytes + 131072 : 65536) *
                (profile.cases.length * profile.loads.length + 2 * profile.cases.length) <=
            32 * MiB,
        "COMPARISON_METADATA_BUDGET",
        "Per-arm request and bounded phase clock metadata exceed 32 MiB",
    );
    let suite;
    try {
        suite = await verifySuiteEvidence(root, profile);
    } catch {
        throw new ComparisonPlanError("COMPARISON_SUITE", "Frozen suite content, questions or answers differ");
    }
    const largest = Math.max(...suite.answers.map((answer) => answer.resultDataCanonicalBytes));
    need(profile.maxResponseBytes >= largest + 4096, "COMPARISON_RESPONSE_BUDGET", "Response cap is below suite needs");
    return profile;
}

function metricFamily(profile, request) {
    const bindings = [];
    const single = request.schemaVersion === 3 && request.comparison === "single-observation";
    const quantile = request.analysis.latency.quantile ?? "p99";
    for (const item of profile.cases)
        for (const load of profile.loads) {
            for (const type of ["latency", "goodput"]) {
                const valuePath =
                    type === "goodput"
                        ? "correctCompletedInWindowPerSecond"
                        : load.model === "open"
                          ? `correctEndToEndMs.${quantile}`
                          : `correctResponseMs.${quantile}`;
                const selector = { caseId: item.id, loadId: load.id, repeat: 0, valuePath };
                const id = `metric-${valueHash(selector).slice(0, 32)}`;
                bindings.push({
                    id,
                    ...selector,
                    kind: type,
                    observations:
                        type === "latency"
                            ? valuePath.replace(/p(?:50|95|99)$/, "count")
                            : "derive-correct-completions-in-measurement-window-from-verified-trace",
                    ...(request.schemaVersion >= 2
                        ? {
                              role: single ? "descriptive" : request.metricRoles[type],
                              minimumObservations:
                                  single && type === "latency" && quantile === "p99"
                                      ? Math.max(1000, request.analysis[type].minimumObservations)
                                      : request.analysis[type].minimumObservations,
                          }
                        : {}),
                });
            }
        }
    need(
        bindings.length <= 128,
        "COMPARISON_FAMILY_BUDGET",
        "Complete case x load x primary-metric family exceeds 128; freeze smaller explicit load strata",
    );
    const metrics = bindings
        .filter((binding) => single || request.schemaVersion === 1 || binding.role === "inference")
        .map((binding) => {
            const threshold = request.analysis[binding.kind];
            return {
                id: binding.id,
                unit: binding.kind === "latency" ? "ms" : "requests/s",
                better: binding.kind === "latency" ? "lower" : "higher",
                ...(!single
                    ? { relativeBudget: threshold.relativeBudget, absoluteBudget: threshold.absoluteBudget }
                    : {}),
                minimumObservations: single ? binding.minimumObservations : threshold.minimumObservations,
            };
        });
    return { bindings, metrics };
}

function planningEstimate(request, profile, armCount, requiredTotalMilliseconds) {
    const cases = profile.cases.length,
        groups = cases * profile.loads.length,
        totalRuns = groups * armCount,
        settleTotal = request.budgets.settleMs * armCount,
        fixedPerArm = groups * (profile.warmupMs + profile.durationMs),
        probeAllowance = armCount * cases * (profile.requestTimeoutMs + profile.drainMs + 1000),
        finalAnalysisBudget = Math.max(request.analysis.maxMilliseconds, request.budgets.sealAndAnalyzeMs);
    const unknown = (safetyAllowanceMilliseconds) => ({ normalMilliseconds: null, safetyAllowanceMilliseconds });
    return {
        schemaVersion: 1,
        confidence: "low",
        counts: {
            cases,
            loadPoints: profile.loads.length,
            caseLoadGroups: groups,
            blocks: request.blockIds.length,
            arms: armCount,
            httpRuns: totalRuns,
            preflightRequests: cases * armCount,
            postcheckRequests: cases * armCount,
            ...(request.schemaVersion === 3 && request.comparison === "single-observation"
                ? { observations: 1, pairs: 0 }
                : {}),
        },
        fixedBaselineMilliseconds: {
            warmupPerRun: profile.warmupMs,
            measurementPerRun: profile.durationMs,
            perArm: fixedPerArm,
            total: fixedPerArm * armCount,
            settleTotal,
            includingSettleTotal: fixedPerArm * armCount + settleTotal,
        },
        normalEtaMilliseconds: null,
        safetyUpperBoundMilliseconds:
            request.budgets.budgetMode === "total" ? request.budgets.maxTotalMilliseconds : requiredTotalMilliseconds,
        ...(request.schemaVersion === 3
            ? {
                  stageSumMaximumMilliseconds: requiredTotalMilliseconds,
                  budgetMode: request.budgets.budgetMode,
                  completionGuaranteed: false,
              }
            : {}),
        overheads: {
            prepare: unknown(armCount * request.budgets.prepareMs),
            startup: unknown(armCount * request.budgets.startMs),
            modeChecks: unknown(armCount * 2 * request.budgets.modeProbeMs),
            preflight: unknown(probeAllowance),
            postcheck: unknown(probeAllowance),
            drain: unknown(totalRuns * 2 * profile.drainMs),
            analysis: unknown(armCount * request.budgets.sealAndAnalyzeMs + finalAnalysisBudget),
            cleanup: unknown(armCount * request.budgets.stopMs),
            ...(profile.schemaVersion === 2
                ? { liveProcessIdentity: unknown(armCount * (groups + 2 * cases) * 20000) }
                : {}),
            clockReference: profile.clockReference
                ? unknown(
                      armCount *
                          (groups + 2 * cases) *
                          (profile.clockReference.spec.wallMs + profile.clockReference.spec.finishMs + 100),
                  )
                : { normalMilliseconds: 0, safetyAllowanceMilliseconds: 0 },
        },
        basis: [
            "Sequential arms and case/load groups with profile.repeats=1; baseline is configured warmup and measurement time for a completed run, plus settle reported separately",
            "Normal startup, checks, response/drain, process scheduling, evidence sealing and analysis times have no measured model; normal ETA remains unknown",
            "Overhead allowances are conservative execution reserves, not measured costs or a normal ETA; the full safety bound also includes workflow scheduling/startup reserves",
            "Sampling bounds describe possible counts only; no target throughput, driver headroom, stable warmup or performance acceptance is inferred",
        ],
    };
}

// The profile loop is identical for both arms. Fresh JVM lifecycle/probe/collector phases belong to the controller.
export function buildComparisonPlan(request, profile, inputFiles, implementation, targetFiles) {
    validateRequest(request);
    const single = request.schemaVersion === 3 && request.comparison === "single-observation";
    const arms = single ? ["O"] : ["A", "B"];
    need(profile.benchmark.purpose === request.purpose, "COMPARISON_PURPOSE", "Profile and comparison purpose differ");
    need(
        targetFiles.length <= 32,
        "COMPARISON_ATTACHMENT_LIMIT",
        "Comparison profiles include three shared attachments; original HTTP plan permits at most 29",
    );
    const normalized = structuredClone(request);
    for (const role of ROLES) normalized.sharedEvidence[role].path = `inputs/${role}.json`;
    const sourceProfile = structuredClone(profile);
    sourceProfile.target.evidenceFiles = targetFiles.map((file) => file.path);
    sourceProfile.target.mode = "__declared-comparison-mode__";
    const invariants = {
        profile: sourceProfile,
        sharedEvidence: normalized.sharedEvidence,
        suite: profile.benchmark,
        resetProtocol: "new-process-per-arm;dedicated-empty-target;no-os-cache-dropping",
        sourceFiles: implementation.files,
    };
    const conditionsBaseDigest = valueHash(invariants);
    const variants = Object.fromEntries(
        arms.map((arm) => [
            arm,
            {
                mode: request.modes[arm],
                variantDigest: valueHash({
                    build: request.sharedEvidence.build.sha256,
                    settings: request.sharedEvidence.settings.sha256,
                    mode: request.modes[arm],
                }),
            },
        ]),
    );
    const { bindings, metrics } = metricFamily(profile, request);
    const samplingBounds = bindings.map((binding) => {
        const load = profile.loads.find((load) => load.id === binding.loadId);
        const maximumPossibleObservations =
            load.model === "open"
                ? Math.min(profile.maxRequestsPerPhase, Math.ceil((profile.durationMs * load.ratePerSecond) / 1000))
                : profile.maxRequestsPerPhase;
        return {
            metricId: binding.id,
            loadModel: load.model,
            boundKind: load.model === "open" ? "planned-arrival-upper-bound" : "request-cap-only",
            maximumPossibleObservations,
            estimatedObservations: null,
            estimatedCorrectRequestsPerSecond: null,
            minimumRequiredObservations:
                binding.minimumObservations ?? request.analysis[binding.kind].minimumObservations,
            minimumPotentiallyReachable:
                (binding.minimumObservations ?? request.analysis[binding.kind].minimumObservations) >
                maximumPossibleObservations
                    ? false
                    : load.model === "open"
                      ? true
                      : null,
            interpretation:
                load.model === "open"
                    ? "planned-arrival upper bound only; correct observations, throughput and driver headroom remain unknown"
                    : "request cap only; without measured closed-loop throughput, sample reachability and actual correct observations remain unknown",
        };
    });
    const { latency, goodput, ...analysis } = request.analysis;
    const contexts = Object.fromEntries(
        request.blockIds.map((id) => [id, valueHash({ conditionsBaseDigest, blockId: id })]),
    );
    const policy = single
        ? {
              schemaVersion: 1,
              kind: "http-observation-analysis-policy",
              id: request.id,
              algorithm: "http-single-observation-v1",
              maxMilliseconds: request.analysis.maxMilliseconds,
              metrics,
              contexts,
          }
        : {
              schemaVersion: 1,
              kind: "paired-analysis-policy",
              id: request.id,
              algorithm: ALGORITHM,
              ...analysis,
              orderSeed: request.orderSeed,
              blockIds: request.blockIds,
              metrics,
              baselineDigest: variants.A.variantDigest,
              candidateDigest: variants.B.variantDigest,
              contexts,
          };
    if (!single)
        try {
            validatePolicy(policy);
        } catch {
            throw new ComparisonPlanError("COMPARISON_POLICY", "Invalid paired analysis parameters or thresholds");
        }
    if (!single)
        need(
            policy.minimumPairs <= request.blockIds.length,
            "COMPARISON_POLICY",
            "minimumPairs exceeds planned blocks",
        );
    need(
        single || (policy.bootstrapReplicates * policy.familyAlpha) / (4 * metrics.length) >= policy.minimumTailDraws,
        "COMPARISON_PRECISION",
        "Bootstrap budget cannot resolve the complete predeclared family tails",
    );
    const analysisWork = single ? 0 : request.blockIds.length * policy.bootstrapReplicates * metrics.length;
    if (!single)
        need(analysisWork <= policy.maxWork, "COMPARISON_ANALYSIS_BUDGET", "Frozen statistical work exceeds maxWork");
    const groups = profile.cases.length * profile.loads.length;
    const goSessions = profile.schemaVersion === 2 ? groups + 2 * profile.cases.length : 0;
    const workflowBaseMs =
        goSessions * 20000 +
        groups * (profile.warmupMs + profile.durationMs + 2 * profile.drainMs + 1000) +
        2 * profile.cases.length * (profile.requestTimeoutMs + profile.drainMs + 1000) +
        (profile.clockReference
            ? (groups + 2 * profile.cases.length) *
              (profile.clockReference.spec.wallMs + profile.clockReference.spec.finishMs + 100)
            : 0);
    need(workflowBaseMs <= 3600000, "COMPARISON_TIME_BUDGET", "One HTTP workflow exceeds its one-hour bound");
    const workflowMaxMs = Math.ceil(workflowBaseMs + 60000);
    const armMaxMilliseconds =
        PHASES.reduce((sum, key) => sum + request.budgets[key], 0) + request.budgets.modeProbeMs + workflowMaxMs;
    const armCount = arms.length * request.blockIds.length;
    if (request.schemaVersion === 3)
        need(
            request.budgets.maxTotalMilliseconds >=
                armCount * (groups * (profile.warmupMs + profile.durationMs) + request.budgets.settleMs) +
                    request.budgets.stopMs +
                    10000,
            "COMPARISON_TIME_BUDGET",
            "Total deadline does not cover fixed warmup/measurement/settle and terminal reserve",
        );
    const requiredTotalMilliseconds =
        armCount * armMaxMilliseconds + Math.max(request.analysis.maxMilliseconds, request.budgets.sealAndAnalyzeMs);
    const estimate = planningEstimate(request, profile, armCount, requiredTotalMilliseconds);
    need(
        request.budgets.budgetMode === "total" || requiredTotalMilliseconds <= request.budgets.maxTotalMilliseconds,
        "COMPARISON_TIME_BUDGET",
        "Lifecycle, both mode probes, workflow and analysis exceed total deadline",
    );
    const immutableAllowance =
        goSessions * (6 * 65536 + 8 * 4096) +
        (goSessions ? 2 * MiB : 0) +
        encode(sourceProfile).length * (groups + 2) +
        inputFiles.reduce((sum, file) => sum + file.bytes, 0) +
        65536 * (2 * groups + 2 * profile.cases.length + 4) +
        (profile.clockReference
            ? (groups + 2 * profile.cases.length) * (4 * profile.clockReference.spec.maxRawBytes + 65536)
            : 0);
    need(
        profile.maxTraceBytes + immutableAllowance <= request.budgets.maxEvidenceBytesPerArm,
        "COMPARISON_EVIDENCE_BUDGET",
        "Per-arm evidence cap does not cover trace plus frozen metadata allowance",
    );
    const requiredTotalEvidenceBytes = armCount * request.budgets.maxEvidenceBytesPerArm + OUTPUT_LIMIT;
    need(
        requiredTotalEvidenceBytes <= request.budgets.maxTotalEvidenceBytes,
        "COMPARISON_EVIDENCE_BUDGET",
        "All arm evidence caps plus plan/analysis reserve exceed total evidence budget",
    );
    const orders = single ? { [request.blockIds[0]]: "O" } : balancedOrders(request.blockIds, request.orderSeed);
    const identityDigest = valueHash({ request: normalized, invariants, inputFiles, policy });
    const profiles = Object.fromEntries(
        arms.map((arm) => [
            arm,
            {
                ...structuredClone(profile),
                target: {
                    ...structuredClone(profile.target),
                    evidenceFiles: targetFiles.map((file) => `../${file.path}`),
                    mode: request.modes[arm],
                },
            },
        ]),
    );
    for (const arm of arms) {
        variants[arm].profileFile = `profiles/${arm}.json`;
        variants[arm].profileSha256 = bytesDigest(encode(profiles[arm]));
    }
    let position = 0;
    const blocks = request.blockIds.map((id) => ({
        id,
        order: orders[id],
        conditionsDigest: policy.contexts[id],
        arms: [...orders[id]].map((arm) => ({
            arm,
            position: position++,
            runId: `arm-${valueHash({ identityDigest, blockId: id, arm }).slice(0, 40)}`,
            variantDigest: variants[arm].variantDigest,
            profileFile: variants[arm].profileFile,
            profileSha256: variants[arm].profileSha256,
            expectedMode: variants[arm].mode,
            maxMilliseconds: armMaxMilliseconds,
            maxEvidenceBytes: request.budgets.maxEvidenceBytesPerArm,
        })),
    }));
    const payload = {
        schemaVersion: request.schemaVersion,
        kind: "http-comparison-plan",
        id: request.id,
        purpose: request.purpose,
        comparison: request.comparison,
        ...(request.schemaVersion >= 2 && !single ? { metricRoles: request.metricRoles } : {}),
        ...(single ? { design: "single-observation" } : {}),
        status: "awaiting-calibration-and-execution",
        identityDigest,
        sourceFiles: inputFiles,
        implementation,
        suite: profile.benchmark,
        conditionsBaseDigest,
        sharedEvidence: normalized.sharedEvidence,
        declaredVariable: single ? null : "gremlinserver.rest_execution_mode",
        variants,
        analysisPolicy: policy,
        metricBindings: bindings,
        samplingBounds,
        estimate,
        blocks,
        budgets: {
            ...request.budgets,
            workflowMaxMilliseconds: workflowMaxMs,
            armCount,
            armMaxMilliseconds,
            requiredTotalMilliseconds,
            requiredTotalEvidenceBytes,
            analysisWork,
            metadataAllowanceBytesPerArm: immutableAllowance,
            ...(request.schemaVersion === 3
                ? {
                      enforcedTotalMilliseconds: request.budgets.maxTotalMilliseconds,
                      stageSumFitsTotal: requiredTotalMilliseconds <= request.budgets.maxTotalMilliseconds,
                      terminalEvidenceReserveMilliseconds: 10000,
                  }
                : {}),
        },
        executionContract: {
            order: "strict-block-order-and-planned-arm-order; no concurrent arms",
            lifecycle: [
                "prepare",
                "start-fresh-process",
                "readiness-and-mode-probe",
                "settle",
                "http-workflow",
                "drain-and-mode-probe",
                "stop-owned-process",
                "seal-evidence",
            ],
            reset: invariants.resetProtocol,
            profileSequence: "same-case-and-load-order-in-every-arm; profile.repeats=1",
            retry: "none; retain failed/missing arms; new attempt requires a new experiment id and frozen plan",
            preRegistration:
                "controller records immutable planDigest before first dispatch; file timestamp is insufficient",
            actualIdentity: "verify build/settings/environment and mode per arm; metadata alone is not runtime proof",
            cleanWindow: "co-running-conversion-or-import-invalidates-performance-window",
            budgets:
                "outer controller enforces wall time, storage and process/container ownership; estimates are not enforcement",
        },
        gates: {
            prospectiveExecutionVerified: false,
            runtimeIdentityVerified: false,
            calibrationAccepted: false,
            automaticGateEnabled: false,
        },
        parametersStatus: "provisional-for-functional-or-calibration-work; formal-profile-calibration-pending",
        executed: false,
        inferenceAccepted: false,
        performanceAccepted: false,
    };
    return { request: normalized, profiles, plan: { ...payload, planDigest: valueHash(payload) } };
}

async function implementationIdentity() {
    await verifyDependencyLock(ROOT);
    const files = [];
    for (const path of SOURCE_FILES) files.push(fileEntry(path, await boundedFile(join(ROOT, path), 8 * MiB)));
    return {
        kind: "http-comparison-planner-implementation",
        files,
        dependencyNote:
            "package lock identifies dependencies; target runtime and installed-package integrity require controller verification",
    };
}
function forbidModeSettings(value) {
    if (!value || typeof value !== "object") return;
    for (const [key, child] of Object.entries(value)) {
        need(
            !/(^|\.)rest_execution_mode$/.test(key),
            "COMPARISON_SETTINGS",
            "Shared settings must omit rest_execution_mode; it is injected only from the declared arm",
        );
        forbidModeSettings(child);
    }
}

export async function freezeComparison({ inputFile, httpPlanDirectory, outputDirectory }) {
    const input = resolve(inputFile),
        root = resolve(httpPlanDirectory),
        output = resolve(outputDirectory);
    const request = validateRequest(parse(await boundedFile(input, 256 * 1024), "request"));
    const profileRaw = await boundedFile(join(root, "profile.json"), 4 * MiB);
    const profile = await validateProfile(profileRaw, root);
    const files = new Map([["inputs/profile.source.json", profileRaw]]);
    for (const role of ROLES) {
        const entry = request.sharedEvidence[role];
        const raw = await boundedFile(resolve(dirname(input), entry.path));
        need(bytesDigest(raw) === entry.sha256, "COMPARISON_IDENTITY", "Shared identity file digest differs");
        const value = parse(raw, role);
        need(
            value && typeof value === "object" && !Array.isArray(value),
            "COMPARISON_IDENTITY",
            "Shared identity evidence must be a JSON object",
        );
        if (role === "settings") forbidModeSettings(value);
        files.set(`inputs/${role}.json`, raw);
    }
    const suiteLockRaw = await boundedFile(join(root, "benchmark/bundle.lock.json"), MiB);
    const suiteLock = parse(suiteLockRaw, "suite lock");
    need(
        bytesDigest(suiteLockRaw) === profile.benchmark.bundleSha256 &&
            Array.isArray(suiteLock.files) &&
            isDeepStrictEqual(suiteLock.files.map((entry) => entry.path).sort(), [...SUITE_FILES].sort()),
        "COMPARISON_SUITE",
        "Suite changed before snapshot or has an unexpected file inventory",
    );
    files.set("benchmark/bundle.lock.json", suiteLockRaw);
    for (const name of SUITE_FILES) {
        const raw = await boundedFile(join(root, "benchmark", name), 2 * MiB);
        const expected = suiteLock.files.find((entry) => entry.path === name);
        need(
            raw.length === expected.bytes && bytesDigest(raw) === expected.sha256,
            "COMPARISON_SUITE",
            "Suite changed while it was copied",
        );
        files.set(`benchmark/${name}`, raw);
    }
    const targetManifestRaw = await boundedFile(join(root, "target-evidence.json"), MiB);
    const targetManifest = parse(targetManifestRaw, "target evidence");
    files.set("inputs/target-evidence.source.json", targetManifestRaw);
    need(
        targetManifest.schemaVersion === 1 &&
            targetManifest.kind === "operator-supplied-target-evidence" &&
            Array.isArray(targetManifest.files) &&
            targetManifest.files.length <= 32,
        "COMPARISON_TARGET_EVIDENCE",
        "Unsupported HTTP target evidence manifest",
    );
    need(
        isDeepStrictEqual(
            targetManifest.files.map((file) => file.path),
            profile.target.evidenceFiles,
        ),
        "COMPARISON_TARGET_EVIDENCE",
        "Profile and target-evidence inventory differ",
    );
    const targetFiles = [];
    for (const [index, entry] of targetManifest.files.entries()) {
        const raw = await boundedFile(resolve(root, entry.path));
        need(
            raw.length === entry.bytes && bytesDigest(raw) === entry.sha256,
            "COMPARISON_TARGET_EVIDENCE",
            "Target evidence file changed",
        );
        const path = `target/${String(index).padStart(3, "0")}.json`;
        files.set(path, raw);
        targetFiles.push(fileEntry(path, raw));
    }
    need(
        targetFiles.reduce((sum, file) => sum + file.bytes, 0) <= 32 * MiB,
        "COMPARISON_TARGET_EVIDENCE",
        "Combined target evidence exceeds 32 MiB",
    );
    for (const role of ROLES) targetFiles.push(fileEntry(`inputs/${role}.json`, files.get(`inputs/${role}.json`)));
    const sourceFiles = [...files]
        .map(([path, raw]) => fileEntry(path, raw))
        .sort((a, b) => (a.path < b.path ? -1 : a.path > b.path ? 1 : 0));
    const implementation = await implementationIdentity();
    const built = buildComparisonPlan(request, profile, sourceFiles, implementation, targetFiles);
    for (const arm of Object.keys(built.profiles)) await validateProfile(encode(built.profiles[arm]), root);
    files.set("request.json", encode(built.request));
    files.set("target-files.json", encode(targetFiles));
    files.set("implementation.json", encode(implementation));
    files.set("plan.json", encode(built.plan));
    for (const arm of Object.keys(built.profiles)) files.set(`profiles/${arm}.json`, encode(built.profiles[arm]));
    need(
        [...files.values()].reduce((sum, raw) => sum + raw.length, 0) <= OUTPUT_LIMIT,
        "COMPARISON_OUTPUT_BUDGET",
        "Frozen plan exceeds 64 MiB",
    );
    const entries = [...files]
        .map(([path, raw]) => fileEntry(path, raw))
        .sort((a, b) => (a.path < b.path ? -1 : a.path > b.path ? 1 : 0));
    const lock = {
        schemaVersion: 1,
        kind: "http-comparison-content-lock",
        planDigest: built.plan.planDigest,
        contentDigest: valueHash(entries),
        files: entries,
    };
    // Reserve once. A failed write leaves evidence; never overwrite or resume an existing directory.
    try {
        await mkdir(output, { mode: 0o700 });
    } catch {
        throw new ComparisonPlanError(
            "COMPARISON_OUTPUT_EXISTS",
            "Choose a new output directory with an existing parent",
        );
    }
    for (const [path, raw] of files) {
        await mkdir(dirname(join(output, path)), { recursive: true, mode: 0o700 });
        await writeFile(join(output, path), raw, { flag: "wx", mode: 0o600 });
    }
    await writeFile(join(output, "plan.lock.json"), encode(lock), { flag: "wx", mode: 0o600 });
    return {
        directory: output,
        planDigest: built.plan.planDigest,
        contentDigest: lock.contentDigest,
        armCount: built.plan.budgets.armCount,
        estimate: built.plan.estimate,
        samplingBounds: built.plan.samplingBounds,
        executed: false,
        performanceAccepted: false,
        nextAction: "Verify this plan, then bind its digest to a new controller execution before dispatch",
    };
}

async function actualFiles(root, relative = "", depth = 0) {
    need(depth <= 4, "COMPARISON_LOCK", "Unexpected nested plan content");
    const files = [];
    for (const name of await readdir(join(root, relative))) {
        const path = relative ? `${relative}/${name}` : name;
        const info = await lstat(join(root, path));
        need(!info.isSymbolicLink(), "COMPARISON_LOCK", "Symlinks are forbidden in frozen plans");
        if (info.isDirectory()) files.push(...(await actualFiles(root, path, depth + 1)));
        else {
            need(info.isFile(), "COMPARISON_LOCK", "Frozen plans contain only regular files");
            files.push(path);
        }
        need(files.length <= 128, "COMPARISON_LOCK", "Unexpected plan inventory size");
    }
    return files;
}

export async function verifyComparison(directory) {
    const root = resolve(directory);
    const lock = parse(await boundedFile(join(root, "plan.lock.json"), MiB), "plan lock");
    need(
        lock.schemaVersion === 1 &&
            lock.kind === "http-comparison-content-lock" &&
            Array.isArray(lock.files) &&
            lock.files.length <= 128 &&
            lock.contentDigest === valueHash(lock.files),
        "COMPARISON_LOCK",
        "Invalid plan content lock",
    );
    need(
        lock.files.every(
            (file) =>
                typeof file.path === "string" &&
                /^[A-Za-z0-9][A-Za-z0-9._/-]*$/.test(file.path) &&
                !file.path.split("/").includes("..") &&
                !file.path.split("/").includes(".") &&
                SHA.test(file.sha256) &&
                Number.isSafeInteger(file.bytes) &&
                file.bytes >= 0 &&
                file.bytes <= OUTPUT_LIMIT,
        ) && new Set(lock.files.map((file) => file.path)).size === lock.files.length,
        "COMPARISON_LOCK",
        "Invalid or duplicate locked path",
    );
    need(
        isDeepStrictEqual(
            (await actualFiles(root)).sort(),
            [...lock.files.map((file) => file.path), "plan.lock.json"].sort(),
        ),
        "COMPARISON_LOCK",
        "Missing or unlisted plan content",
    );
    need(
        lock.files.reduce((sum, file) => sum + file.bytes, 0) <= OUTPUT_LIMIT,
        "COMPARISON_OUTPUT_BUDGET",
        "Plan inventory exceeds 64 MiB",
    );
    const raw = new Map();
    for (const file of lock.files) {
        const bytes = await boundedFile(join(root, file.path), OUTPUT_LIMIT);
        need(
            bytes.length === file.bytes && bytesDigest(bytes) === file.sha256,
            "COMPARISON_LOCK",
            "Locked content changed",
        );
        raw.set(file.path, bytes);
    }
    const request = validateRequest(parse(raw.get("request.json"), "request"));
    const implementation = parse(raw.get("implementation.json"), "implementation");
    need(
        isDeepStrictEqual(implementation, await implementationIdentity()),
        "COMPARISON_IMPLEMENTATION",
        "Current planner/contracts differ; use the frozen implementation or create a new plan",
    );
    const profile = await validateProfile(raw.get("inputs/profile.source.json"), root);
    for (const role of ROLES) {
        need(
            request.sharedEvidence[role].path === `inputs/${role}.json` &&
                request.sharedEvidence[role].sha256 === bytesDigest(raw.get(`inputs/${role}.json`)),
            "COMPARISON_IDENTITY",
            "Shared evidence identity differs",
        );
        if (role === "settings") forbidModeSettings(parse(raw.get(`inputs/${role}.json`), role));
    }
    const targetFiles = parse(raw.get("target-files.json"), "target files");
    const originalTarget = parse(raw.get("inputs/target-evidence.source.json"), "source target evidence");
    need(
        originalTarget?.schemaVersion === 1 &&
            originalTarget.kind === "operator-supplied-target-evidence" &&
            Array.isArray(originalTarget.files) &&
            isDeepStrictEqual(
                originalTarget.files.map((file) => file.path),
                profile.target.evidenceFiles,
            ),
        "COMPARISON_TARGET_EVIDENCE",
        "Original target evidence inventory differs from the source profile",
    );
    const expectedTargetPaths = [
        ...profile.target.evidenceFiles.map((_, index) => `target/${String(index).padStart(3, "0")}.json`),
        ...ROLES.map((role) => `inputs/${role}.json`),
    ];
    const sourceFiles = lock.files.filter(
        (file) =>
            file.path.startsWith("inputs/") || file.path.startsWith("benchmark/") || file.path.startsWith("target/"),
    );
    need(
        Array.isArray(targetFiles) &&
            isDeepStrictEqual(
                targetFiles.map((file) => file.path),
                expectedTargetPaths,
            ) &&
            targetFiles.every(
                (file) =>
                    raw.has(file.path) &&
                    file.sha256 === bytesDigest(raw.get(file.path)) &&
                    file.bytes === raw.get(file.path).length,
            ),
        "COMPARISON_TARGET_EVIDENCE",
        "Target input references differ",
    );
    need(
        originalTarget.files.every(
            (file, index) => file.bytes === targetFiles[index].bytes && file.sha256 === targetFiles[index].sha256,
        ),
        "COMPARISON_TARGET_EVIDENCE",
        "Original target evidence digests differ from the frozen attachments",
    );
    need(
        isDeepStrictEqual(
            sourceFiles.map((file) => file.path).sort(),
            [
                "inputs/profile.source.json",
                "inputs/target-evidence.source.json",
                ...ROLES.map((role) => `inputs/${role}.json`),
                ...SUITE_FILES.map((name) => `benchmark/${name}`),
                "benchmark/bundle.lock.json",
                ...expectedTargetPaths.filter((path) => path.startsWith("target/")),
            ].sort(),
        ),
        "COMPARISON_LOCK",
        "Unexpected frozen source inventory",
    );
    const expected = buildComparisonPlan(request, profile, sourceFiles, implementation, targetFiles);
    for (const arm of Object.keys(expected.profiles)) await validateProfile(encode(expected.profiles[arm]), root);
    const plan = parse(raw.get("plan.json"), "plan");
    need(
        isDeepStrictEqual(plan, expected.plan) && lock.planDigest === plan.planDigest,
        "COMPARISON_PLAN_CHANGED",
        "Schedule, conditions, identities, policy or budget differs from frozen inputs",
    );
    if (request.schemaVersion === 3) samePlanInventory();
    function samePlanInventory() {
        need(
            isDeepStrictEqual(
                lock.files.map((file) => file.path).sort(),
                [
                    ...sourceFiles.map((file) => file.path),
                    "request.json",
                    "target-files.json",
                    "implementation.json",
                    "plan.json",
                    ...Object.keys(expected.profiles).map((arm) => `profiles/${arm}.json`),
                ].sort(),
            ),
            "COMPARISON_LOCK",
            "Version 3 plan contains undeclared profiles or members",
        );
    }
    for (const arm of Object.keys(expected.profiles))
        need(
            raw.get(`profiles/${arm}.json`).equals(encode(expected.profiles[arm])),
            "COMPARISON_PROFILE_CHANGED",
            "Arm profiles differ from the declared single-variable experiment",
        );
    return {
        verified: true,
        planDigest: plan.planDigest,
        contentDigest: lock.contentDigest,
        armCount: plan.budgets.armCount,
        estimate: plan.estimate,
        samplingBounds: plan.samplingBounds,
        executed: false,
        performanceAccepted: false,
    };
}

export function describe() {
    return {
        schemaVersion: 1,
        kind: "http-comparison-planner-capability",
        protocol: "balanced-http-comparison-v1",
        requestSchemaVersions: [1, 2, 3],
        version3: {
            singleObservation: {
                comparison: "single-observation",
                modes: ["O"],
                blockCount: 1,
                analysisFields: ["maxMilliseconds", "latency", "goodput"],
                inferenceSupported: false,
            },
            pairedMetricRoles: { latency: ["inference"], goodput: ["inference", "quality"] },
            budgetMode: { required: true, allowed: ["total", "stage-sum"], totalMayBePartial: true },
            legacyOmission: "Versions 1/2 retain strict paired stage-sum semantics and reject new budget fields",
        },
        version2MetricRoles: { latency: "inference", goodput: "quality" },
        metricRolesContract:
            "Version 2 freezes roles before execution; all bindings and quality accounting remain; only inference enters the paired family",
        operations: ["freeze", "verify"],
        effects: ["read-local-inputs", "create-new-plan-directory"],
        executesHttp: false,
        executesDocker: false,
        formalAcceptanceSupported: false,
        requiredInput: {
            kind: "http-comparison-request",
            fields: [
                "id",
                "purpose",
                "comparison",
                "modes",
                "blockIds",
                "orderSeed",
                "sharedEvidence",
                "analysis",
                "budgets",
            ],
            sharedEvidenceRoles: ROLES,
            sharedEvidenceEntryFields: ["path", "sha256"],
            analysisFields: [
                "familyAlpha",
                "minimumPairs",
                "bootstrapReplicates",
                "minimumTailDraws",
                "maxWork",
                "maxMilliseconds",
                "resampleSeed",
                "latency",
                "goodput",
            ],
            effectThresholdFields: ["relativeBudget", "absoluteBudget", "minimumObservations"],
            latencyQuantile: { optional: true, allowed: ["p50", "p95", "p99"], default: "p99" },
            budgetFields: [...PHASES, "maxEvidenceBytesPerArm", "maxTotalEvidenceBytes", "maxTotalMilliseconds"],
        },
        comparisonTypes: ["aa", "forward-embedded", "single-observation"],
        purpose: ["functional", "calibration"],
        blockCount: { min: 2, max: 1000, even: true },
        maxPrimaryMetrics: 128,
        planningPreview: {
            estimate: "fixed configured time and low-confidence overhead disclosure; unknown normal ETA is null",
            samplingBounds: "open arrival upper bounds or closed request caps; no achieved-throughput prediction",
            safetyBound: "estimate.safetyUpperBoundMilliseconds equals budgets.requiredTotalMilliseconds; not ETA",
        },
        limits: { outputBytes: OUTPUT_LIMIT, requestBytes: 256 * 1024, profileBytes: 4 * MiB },
        exitCodes: { 0: "plan-created-or-verified", 6: "structured-error" },
        documentation: "docs/http-comparison-execution.md",
        performanceAccepted: false,
    };
}
const HELP = `HTTP comparison/observation planner v1/v2/v3 — plan only; no HTTP, Docker or measurements.

Freeze: node components/http/comparison-plan.mjs --input REQUEST.json --http-plan FROZEN_HTTP_PLAN --out NEW_DIRECTORY
Verify: node components/http/comparison-plan.mjs --verify PLAN_DIRECTORY
Describe: node components/http/comparison-plan.mjs --describe

Use an existing versioned HTTP plan with repeats=1 and serviceChecks=true. Provide shared build/environment/settings
JSON evidence with expected SHA-256, explicit even blockIds and orderSeed, all phase/storage budgets and provisional
paired-analysis thresholds. A/A uses equal modes; forward-embedded fixes A=forward, B=embedded. Each arm is a fresh JVM.
Output contains a locked schedule, unique arm identities, profiles and the complete primary metric family.
analysis.latency.quantile optionally selects p50/p95/p99 (default p99); observation thresholds are never lowered.
Version 2 requires metricRoles={latency:inference,goodput:quality}; goodput remains fully verified and descriptive.
Version 1 retains both latency and goodput in the complete paired family. Roles cannot be changed after freezing.
Version 3 single-observation uses only modes.O and one block ID, no order seed, no paired policy or fabricated B.
Its analysis has maxMilliseconds, latency {quantile,minimumObservations}, goodput {minimumObservations}; all results are descriptive.
Version 3 paired designs explicitly freeze latency=inference and goodput=inference|quality.
Version 3 budgets.budgetMode is required: total enforces one whole deadline; stage-sum retains conservative stage-sum admission.
Total must cover all fixed warmup/measurement/settle and stop+10s terminal reserve; other costs may leave partial evidence.
estimate separates fixed warmup/measurement time, unknown normal ETA and conservative execution bounds.
samplingBounds never predicts achieved throughput; closed-loop sample reachability is unknown without measurement.
No execution occurs; parameters and performance acceptance remain pending calibration. Existing output is never reused.
Run npm run build first. Full input example and controller requirements: docs/http-comparison-execution.md
Exit 0 means plan creation/integrity validation only. Exit 6 emits a structured error on stderr.`;

async function main(argv) {
    if (argv.length === 1 && argv[0] === "--help") {
        process.stdout.write(HELP + "\n");
        return;
    }
    if (argv.length === 1 && argv[0] === "--describe") {
        process.stdout.write(JSON.stringify(describe()) + "\n");
        return;
    }
    try {
        let result;
        if (argv.length === 2 && argv[0] === "--verify") result = await verifyComparison(argv[1]);
        else {
            need(argv.length === 6, "COMPARISON_ARGUMENTS", "Run --help for supported arguments");
            const args = {};
            for (let i = 0; i < argv.length; i += 2) {
                need(
                    ["--input", "--http-plan", "--out"].includes(argv[i]) && !Object.hasOwn(args, argv[i]),
                    "COMPARISON_ARGUMENTS",
                    "Invalid or repeated planner option",
                );
                args[argv[i]] = argv[i + 1];
            }
            result = await freezeComparison({
                inputFile: args["--input"],
                httpPlanDirectory: args["--http-plan"],
                outputDirectory: args["--out"],
            });
        }
        process.stdout.write(JSON.stringify({ ok: true, data: result }) + "\n");
    } catch (error) {
        process.stderr.write(
            JSON.stringify({
                ok: false,
                error: {
                    code: error instanceof ComparisonPlanError ? error.code : "COMPARISON_IO_OR_CONTRACT_ERROR",
                    message:
                        error instanceof ComparisonPlanError ? error.message : "Plan cannot be read or written safely",
                    retryable: false,
                    nextAction: "Inspect inputs and partial evidence; use a new output directory for a new attempt",
                },
            }) + "\n",
        );
        process.exitCode = 6;
    }
}
if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href)
    await main(process.argv.slice(2));
