/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
import { constants } from "node:fs";
import { open, mkdir, mkdtemp, rm, writeFile, opendir, lstat, realpath } from "node:fs/promises";
import { basename, dirname, join, resolve } from "node:path";
import { tmpdir } from "node:os";
import { createHash } from "node:crypto";
import { isDeepStrictEqual } from "node:util";
import { z } from "zod";
import { BenchError, hash } from "./io.js";
import { packageRoot } from "./plans.js";
import { projectInputs } from "./http-presets.js";
import { httpProjectSchema } from "./http-preset-contracts.js";
import { httpEnvironmentSettingsSchema } from "./http-environment-contracts.js";
import { httpCapacityPolicySchema, isSingleRunCapacity, type HttpCapacityPolicy } from "./http-capacity-contracts.js";
import { inspectHttpSuite, generateHttpProfile, type HttpSuite } from "./http-suite.js";
import { planHttp } from "./http.js";
import { freezeHttpComparison, httpComparisonPlan } from "./http-comparison.js";
import { validateProjectExecutor, projectHttpProfile } from "./http-project-executor.js";

const MiB = 1024 ** 2;
// Existing comparison planner output bound; compile-only templates are removed after each point.
const temporaryPointBytes = 64 * MiB;
const roles = ["build", "settings", "environment", "runtime"] as const;
const jsonBytes = (value: unknown) => Buffer.from(JSON.stringify(value, null, 2) + "\n");
const sha = (raw: Buffer) => createHash("sha256").update(raw).digest("hex");
type Loaded = Awaited<ReturnType<typeof projectInputs>>;
type FileEntry = { path: string; bytes: number; sha256: string };
type PointIdentity = {
    id: string;
    index: number;
    caseId: string;
    mode: "forward" | "embedded";
    rateIndex: number;
    ratePerSecond: number;
    comparisonDirectory: string;
    profileFile: string;
    expectedArms: number;
};
export type HttpCapacityPoint = PointIdentity & {
    comparisonPlanDigest: string;
    comparisonContentDigest: string;
    estimate: Record<string, unknown>;
    samplingBounds: unknown;
    workflowMaxMilliseconds: number;
};
const sourceSchema = z.strictObject({
    path: z.string().min(1).max(4096),
    bytes: z
        .number()
        .int()
        .nonnegative()
        .max(8 * MiB),
    sha256: z.string().regex(/^[a-f0-9]{64}$/),
});
const snapshotsSchema = z.strictObject({
    build: sourceSchema,
    settings: sourceSchema,
    environment: sourceSchema,
    runtime: sourceSchema,
});
const bindingSchema = z.strictObject({
    schemaVersion: z.literal(1),
    kind: z.literal("http-capacity-binding"),
    projectId: z.string(),
    projectSha256: z.string().regex(/^[a-f0-9]{64}$/),
    policySha256: z.string().regex(/^[a-f0-9]{64}$/),
    sourceSnapshots: snapshotsSchema,
    planningToolSourceRoot: z.string().min(1),
    planningDirectory: z.string().min(1),
    suite: z.strictObject({
        suiteId: z.literal("hgbench-http-serving-v0.1"),
        version: z.literal("0.1.0-draft.1"),
        bundleSha256: z.string().regex(/^[a-f0-9]{64}$/),
        caseIds: z.array(z.string()).length(11),
    }),
});
type Binding = z.infer<typeof bindingSchema>;

function need(condition: unknown, code: string, message: string, details?: unknown): asserts condition {
    if (!condition) throw new BenchError(code, message, 3, false, details);
}
async function bytes(path: string, cap = 8 * MiB): Promise<Buffer> {
    const handle = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
    try {
        const before = await handle.stat();
        need(before.isFile() && before.size <= cap, "HTTP_CAPACITY_INPUT_TYPE", "Use a bounded regular file", {
            path,
            cap,
        });
        const raw = Buffer.alloc(before.size + 1);
        let used = 0;
        while (used < raw.length) {
            const part = await handle.read(raw, used, raw.length - used, used);
            if (!part.bytesRead) break;
            used += part.bytesRead;
        }
        const after = await handle.stat();
        need(
            used === before.size &&
                before.size === after.size &&
                before.mtimeMs === after.mtimeMs &&
                before.ctimeMs === after.ctimeMs,
            "HTTP_CAPACITY_INPUT_CHANGED",
            "Input changed while it was read",
            { path },
        );
        return raw.subarray(0, used);
    } finally {
        await handle.close();
    }
}
function parse<T>(raw: Buffer, schema: z.ZodType<T>, path: string): T {
    try {
        return schema.parse(JSON.parse(new TextDecoder("utf8", { fatal: true }).decode(raw)));
    } catch (error) {
        throw new BenchError("HTTP_CAPACITY_INPUT_INVALID", "Capacity JSON does not satisfy its contract", 2, false, {
            path,
            issues: error instanceof z.ZodError ? error.issues : String(error),
        });
    }
}
const json = async (path: string) => parse(await bytes(path), z.record(z.string(), z.any()), path);
async function save(path: string, value: unknown) {
    await mkdir(dirname(path), { recursive: true, mode: 0o700 });
    await writeFile(path, jsonBytes(value), { flag: "wx", mode: 0o600 });
}
async function inventory(root: string, cap: number): Promise<FileEntry[]> {
    const files: FileEntry[] = [];
    let entries = 0,
        total = 0;
    async function walk(prefix: string, depth: number): Promise<void> {
        need(depth <= 16, "HTTP_CAPACITY_PLAN_BUDGET", "Capacity plan exceeds sixteen directory levels");
        for await (const entry of await opendir(join(root, prefix))) {
            need(++entries <= 20000, "HTTP_CAPACITY_PLAN_BUDGET", "Capacity plan exceeds 20000 entries");
            const path = prefix ? prefix + "/" + entry.name : entry.name;
            const stat = await lstat(join(root, path));
            if (stat.isDirectory() && !stat.isSymbolicLink()) await walk(path, depth + 1);
            else {
                need(
                    stat.isFile() && !stat.isSymbolicLink(),
                    "HTTP_CAPACITY_PLAN_CHANGED",
                    "Plan contains a nonregular file",
                    { path },
                );
                total += stat.size;
                need(
                    total <= cap && stat.size <= 8 * MiB,
                    "HTTP_CAPACITY_PLAN_BUDGET",
                    "Capacity plan exceeds its byte cap",
                    { cap, total, path },
                );
                const raw = await bytes(join(root, path));
                files.push({ path, bytes: raw.length, sha256: sha(raw) });
            }
        }
    }
    await walk("", 0);
    return files.sort((a, b) => (a.path < b.path ? -1 : a.path > b.path ? 1 : 0));
}
function validatePolicy(policy: HttpCapacityPolicy) {
    need(
        policy.rates.every((rate, i) => i === 0 || rate > policy.rates[i - 1]!),
        "HTTP_CAPACITY_RATES_INVALID",
        "Rates must be strictly increasing; no points are silently removed",
    );
    const requiredRequests = Math.ceil(
        (Math.max(policy.exploration.warmupMs, policy.exploration.durationMs) * policy.rates.at(-1)!) / 1000,
    );
    need(
        requiredRequests <= policy.http.maxRequestsPerPhase,
        "HTTP_CAPACITY_REQUEST_BUDGET",
        "Request cap does not cover the exploration arrival schedule",
        {
            requiredRequestsPerPhase: requiredRequests,
            declared: policy.http.maxRequestsPerPhase,
            suggestion:
                "Freeze a new policy with an explicit larger cap or a different declared study; no parameter was changed",
        },
    );
    need(
        policy.pointBudgets.sealAndAnalyzeMs >= policy.analysis.maxMilliseconds,
        "HTTP_CAPACITY_POINT_BUDGET",
        "Sealing budget must cover the existing statistical analyzer",
    );
    // Existing prepare has a 330s subprocess bound; task wrappers reserve stop/seal plus a 10s margin.
    const requiredController =
        330000 +
        policy.pointBudgets.stopMs +
        policy.pointBudgets.sealAndAnalyzeMs +
        10000 +
        30000 +
        (policy.continuation ? policy.continuation.maxVerificationMilliseconds + 10000 + 30000 + 20000 : 0);
    need(
        policy.controllerBudgets.perPointMilliseconds >= requiredController,
        "HTTP_CAPACITY_CONTROLLER_BUDGET",
        "Per-point controller allowance must cover prepare and task/owner cleanup",
        {
            requiredMilliseconds: requiredController,
            declaredMilliseconds: policy.controllerBudgets.perPointMilliseconds,
        },
    );
    need(
        !policy.continuation || policy.controllerBudgets.totalMilliseconds >= 160000,
        "HTTP_CAPACITY_CONTINUATION_BUDGET",
        "Global allowance must cover plan/daemon checks and the unchanged 100-second closing reserve",
    );
}
function identities(loaded: Pick<Loaded, "project">, policy: HttpCapacityPolicy, binding: Binding): PointIdentity[] {
    const modes: Array<"forward" | "embedded"> =
        loaded.project.comparison === "aa" ? [loaded.project.aaMode] : ["forward", "embedded"];
    return binding.suite.caseIds
        .flatMap((caseId) =>
            modes.flatMap((mode) =>
                policy.rates.map((ratePerSecond, rateIndex) => {
                    const id =
                        "point-" +
                        hash({
                            projectSha256: binding.projectSha256,
                            policySha256: binding.policySha256,
                            caseId,
                            mode,
                            ratePerSecond,
                        }).slice(0, 32);
                    const prefix = "points/" + id;
                    return {
                        id,
                        index: 0,
                        caseId,
                        mode,
                        rateIndex,
                        ratePerSecond,
                        comparisonDirectory: prefix + "/comparison",
                        profileFile: prefix + "/profile.json",
                        expectedArms: isSingleRunCapacity(policy) ? 1 : 4,
                    };
                }),
            ),
        )
        .map((point, index) => ({ ...point, index }));
}
function suiteSettings(
    loaded: Pick<Loaded, "project" | "sources">,
    policy: HttpCapacityPolicy,
    binding: Binding,
    point: PointIdentity,
) {
    const { maxInFlight, maxStartDelayMs, ...http } = policy.http;
    return {
        schemaVersion: 1,
        kind: "http-suite-settings",
        name: point.id,
        caseIds: [point.caseId],
        ...http,
        purpose: "calibration",
        repeats: 1,
        warmupMs: policy.exploration.warmupMs,
        durationMs: policy.exploration.durationMs,
        loads: [
            {
                id: "open-" + point.rateIndex,
                model: "open",
                ratePerSecond: point.ratePerSecond,
                maxInFlight,
                maxStartDelayMs,
            },
        ],
        endpoint: "http://127.0.0.1:18080/gremlin",
        authorizationEnv: null,
        target: {
            label: loaded.project.label,
            build: String(loaded.sources.build.value.sourceCommit ?? binding.sourceSnapshots.build.sha256),
            mode: point.mode,
            environment: loaded.project.description,
            evidenceFiles: [join(binding.planningDirectory, "capacity-binding.json")],
        },
    };
}
function pointRequest(policy: HttpCapacityPolicy, point: PointIdentity, sharedEvidence: unknown) {
    if (policy.schemaVersion === 2)
        return {
            schemaVersion: 3,
            kind: "http-comparison-request",
            id: point.id,
            purpose: "calibration",
            comparison: "single-observation",
            modes: { O: point.mode },
            blockIds: ["observe-01"],
            sharedEvidence,
            analysis: {
                maxMilliseconds: policy.analysis.maxMilliseconds,
                latency: {
                    quantile: policy.analysis.latency.quantile,
                    minimumObservations: policy.analysis.latency.minimumObservations,
                },
                goodput: { minimumObservations: policy.analysis.goodput.minimumObservations },
            },
            budgets: { ...policy.pointBudgets, budgetMode: policy.budgetMode },
        };
    return {
        schemaVersion: 1,
        kind: "http-comparison-request",
        id: point.id,
        purpose: "calibration",
        comparison: "aa",
        modes: { A: point.mode, B: point.mode },
        blockIds: ["explore-01", "explore-02"],
        orderSeed: point.id + ":order",
        sharedEvidence,
        analysis: { ...policy.analysis, resampleSeed: point.id + ":resample" },
        budgets: policy.pointBudgets,
    };
}
function resourcePreview(
    loaded: Pick<Loaded, "target" | "runtime">,
    policy: Pick<HttpCapacityPolicy, "http" | "pointBudgets" | "continuation"> & {
        analysis: { maxMilliseconds: number };
    },
    binding: Pick<Binding, "planningToolSourceRoot">,
    points: Array<Pick<HttpCapacityPoint, "workflowMaxMilliseconds" | "expectedArms">>,
) {
    need(
        points.length > 0 &&
            points.every((p) => Number.isInteger(p.expectedArms) && p.expectedArms >= 1 && p.expectedArms <= 40),
        "HTTP_CAPACITY_RESOURCE_POINTS",
        "Resource validation requires explicit nonempty physical arm counts",
    );
    const armCount = Math.max(...points.map((p) => p.expectedArms));
    const runtime = parse(
        jsonBytes(loaded.runtime),
        z
            .object({
                sourceRoot: z.string(),
                driver: z
                    .object({ wallSeconds: z.number().int().positive(), maxLogBytes: z.number().int().positive() })
                    .passthrough(),
                collector: z
                    .object({
                        durationMs: z.number().int().positive(),
                        intervalMs: z.number().int().positive(),
                        maxSamples: z.number().int().positive(),
                        maxOutputBytes: z.number().int().positive(),
                    })
                    .passthrough(),
                supervisor: z.object({ maxCommandBytes: z.number().int().positive() }).passthrough(),
            })
            .passthrough(),
        "project runtime resource bounds",
    );
    const expectedWorkflow = Math.max(...points.map((p) => p.workflowMaxMilliseconds));
    const collectorMinimum = expectedWorkflow + 3 * runtime.collector.intervalMs;
    const driverMinimum = runtime.collector.durationMs + 30000;
    const targetMinimum =
        policy.pointBudgets.startMs +
        2 * policy.pointBudgets.modeProbeMs +
        policy.pointBudgets.settleMs +
        runtime.collector.durationMs +
        30000;
    const observation = loaded.target.jvmObservation;
    const jvmBytes = observation ? observation.maxRawBytes + observation.parser.maxOutputBytes + 2 * MiB : 0;
    if (policy.continuation) {
        const verificationMinimum =
            60000 +
            Math.max(
                60000,
                policy.pointBudgets.sealAndAnalyzeMs,
                policy.analysis.maxMilliseconds + 60000 + (observation ? 4 * (observation.parser.timeoutMs + 5000) : 0),
            );
        need(
            policy.continuation.maxVerificationMilliseconds >= verificationMinimum &&
                policy.continuation.maxVerificationBytes >= jvmBytes + 2 * MiB,
            "HTTP_CAPACITY_CONTINUATION_BUDGET",
            "Continuation must cover complete parent/point revalidation and temporary JVM/output evidence",
            { requiredVerificationMilliseconds: verificationMinimum, requiredVerificationBytes: jvmBytes + 2 * MiB },
        );
    }
    const knownArmBytes =
        policy.http.maxTraceBytes +
        runtime.collector.maxOutputBytes +
        8 * MiB +
        jvmBytes +
        4 * runtime.driver.maxLogBytes +
        20 * runtime.supervisor.maxCommandBytes;
    need(
        runtime.collector.durationMs >= collectorMinimum &&
            runtime.collector.maxSamples >=
                Math.ceil(runtime.collector.durationMs / runtime.collector.intervalMs) + 1 &&
            runtime.driver.wallSeconds * 1000 >= driverMinimum &&
            loaded.target.budget.wallSeconds * 1000 >= targetMinimum,
        "HTTP_CAPACITY_RUNTIME_BUDGET",
        "Saved resources do not cover a point; freeze a new project runtime explicitly",
        {
            collectorMinimumMs: collectorMinimum,
            driverMinimumMs: driverMinimum,
            targetMinimumMs: targetMinimum,
        },
    );
    need(
        knownArmBytes < policy.pointBudgets.maxEvidenceBytesPerArm,
        "HTTP_CAPACITY_EVIDENCE_BUDGET",
        "Known trace/collector/JVM/command reserves exhaust the per-arm cap",
        {
            knownArmBytes,
            declaredArmBytes: policy.pointBudgets.maxEvidenceBytesPerArm,
            additionalRuntimeInventoryAndAttachments: "not yet measured; existing prepare remains required",
        },
    );
    if (observation)
        need(
            observation.maxRawBytes === loaded.target.budget.maxLogBytes &&
                observation.parser.maxOutputBytes >= observation.maxRawBytes + 2 * MiB &&
                armCount * (observation.parser.timeoutMs + 5000) + policy.analysis.maxMilliseconds + 60000 <=
                    policy.pointBudgets.sealAndAnalyzeMs,
            "HTTP_CAPACITY_JVM_BUDGET",
            "JVM raw/parser bounds or independent reparse do not fit the existing seal budget",
        );
    return {
        target: loaded.target.budget,
        jvm: loaded.target.jvm,
        jvmObservation: observation ?? null,
        driver: loaded.runtime.driver,
        collector: loaded.runtime.collector,
        supervisor: loaded.runtime.supervisor,
        necessaryBounds: {
            workflowMaxMilliseconds: expectedWorkflow,
            collectorMinimumMs: collectorMinimum,
            driverMinimumMs: driverMinimum,
            targetMinimumMs: targetMinimum,
            knownArmEvidenceBytes: knownArmBytes,
        },
        unchanged: true,
        runtimeCompatibility: {
            declaredSourceRoot: runtime.sourceRoot,
            planningToolSourceRoot: binding.planningToolSourceRoot,
            sourceRootMatchesCurrentTool: resolve(runtime.sourceRoot) === resolve(binding.planningToolSourceRoot),
            actualAssetsAndCombinedEvidence: "pending existing comparison prepare; resources were not changed",
        },
    };
}
function derivedPlan(
    loaded: Pick<Loaded, "project" | "target" | "runtime">,
    policy: HttpCapacityPolicy,
    binding: Binding,
    points: HttpCapacityPoint[],
) {
    const sum = (fn: (point: HttpCapacityPoint) => number) => points.reduce((n, p) => n + fn(p), 0);
    const fixed = (key: string) =>
        sum((p) => Number((p.estimate.fixedBaselineMilliseconds as Record<string, number>)[key]));
    const requiredComparisons = sum((p) => Number(p.estimate.safetyUpperBoundMilliseconds));
    const declaredComparisons = points.length * policy.pointBudgets.maxTotalMilliseconds;
    const controller =
        points.length * policy.controllerBudgets.perPointMilliseconds + policy.controllerBudgets.totalMilliseconds;
    const safety = declaredComparisons + controller;
    const single = policy.schemaVersion === 2;
    const totalBudget = single && policy.budgetMode === "total";
    const cleanupReserveMilliseconds = 100000;
    // Fixed workload cannot be silently shortened. Overhead remains bounded at
    // runtime by the shared deadline; stage caps are not an expected duration.
    const minimumBudgetMilliseconds = fixed("includingSettleTotal") + cleanupReserveMilliseconds + 60000;
    const evidence =
        points.length * policy.pointBudgets.maxTotalEvidenceBytes +
        policy.studyBudgets.maxPlanBytes +
        temporaryPointBytes +
        (policy.continuation ? points.length * policy.continuation.maxVerificationBytes : 0);
    need(
        (totalBudget ? minimumBudgetMilliseconds : safety) <= policy.studyBudgets.maxTotalMilliseconds,
        "HTTP_CAPACITY_STUDY_TIME_BUDGET",
        "The declared workload and required reserves exceed the selected study deadline",
        {
            requiredStudyMilliseconds: totalBudget ? minimumBudgetMilliseconds : safety,
            declaredStudyMilliseconds: policy.studyBudgets.maxTotalMilliseconds,
            points: points.length,
            changedParameters: [],
            normalEtaMilliseconds: null,
        },
    );
    need(
        evidence <= policy.studyBudgets.maxEvidenceBytes,
        "HTTP_CAPACITY_STUDY_EVIDENCE_BUDGET",
        "All point evidence caps plus the plan reserve exceed the study byte cap",
        {
            requiredEvidenceBytes: evidence,
            declaredEvidenceBytes: policy.studyBudgets.maxEvidenceBytes,
            points: points.length,
        },
    );
    const payload = {
        schemaVersion: single ? 2 : 1,
        kind: "http-capacity-plan",
        id: loaded.project.id + "-" + policy.id,
        phase: "exploration",
        ...(single
            ? {
                  budgetMode: policy.budgetMode,
                  totalDeadlineMilliseconds: policy.studyBudgets.maxTotalMilliseconds,
                  cleanupReserveMilliseconds,
                  minimumBudgetMilliseconds: totalBudget ? minimumBudgetMilliseconds : safety,
              }
            : {}),
        scope: policy.scope,
        policy,
        policySha256: binding.policySha256,
        projectSha256: binding.projectSha256,
        bindingSha256: sha(jsonBytes(binding)),
        sourceSnapshots: binding.sourceSnapshots,
        suite: binding.suite,
        modes: loaded.project.comparison === "aa" ? [loaded.project.aaMode] : ["forward", "embedded"],
        points,
        pointCount: points.length,
        expectedArms: sum((p) => p.expectedArms),
        confirmation: {
            ...policy.confirmation,
            implemented: false,
            plannedPoints: [],
            budgetIncluded: false,
            rule: "Freeze a separate confirmation plan before new observations; candidate and eligible adjacent tested rates, four balanced independent pairs per point",
            modeComparison:
                loaded.project.comparison === "aa"
                    ? "A/A in the selected mode"
                    : "New forward-embedded balanced comparison at shared safe observed rates; confirm distinct per-mode extrema separately",
        },
        stopRules: {
            ...(policy.continuation
                ? {
                      implementation: "verified-case-range-v1",
                      legacyWhenAbsent: "whole-study-stop",
                      localStopScope: "case-all-selected-modes",
                      confirmation: "whole-study-stop",
                      verificationBudget: policy.continuation,
                      ownedCleanup:
                          "exact recorded CIDs absent on the same observed Docker daemon and socket before advancing",
                      targetCapacityUpperBound: "unknown",
                  }
                : {}),
            order: "case-then-mode-then-strictly-increasing-rate; sequential points and arms",
            globalStop: [
                "cancellation",
                "unknown-submission-identity",
                "cleanup-or-ownership-failure",
                "source-runtime-or-plan-lock-drift",
                "whole-study-budget-exhaustion",
            ],
            ...(!policy.continuation ? { localStopScope: "case-and-mode" } : {}),
            localStopPrerequisites:
                "Complete sealed evidence, valid environment/runtime and explained original quality symptoms; task failed alone is insufficient",
            localStop: [
                "driver-pressure: higher rates unexecuted; target capacity unknown",
                "predeclared SLO failure only if supplied; no invented SLO",
            ],
            invalidBoundary: ["wrong-answer", "decode-or-validator-error", "unattributed-timeout-or-transport-error"],
            unexecuted:
                "Retain every planned point and arm in the denominator with a reason; compare modes only on shared actual points",
            retry: "Never resubmit a dispatched point; a new attempt requires a new frozen plan",
        },
        estimate: {
            schemaVersion: 1,
            confidence: "low",
            normalEtaMilliseconds: null,
            counts: {
                cases: 11,
                modes: loaded.project.comparison === "aa" ? 1 : 2,
                rates: policy.rates.length,
                points: points.length,
                pairs: single ? 0 : 2 * points.length,
                arms: sum((p) => p.expectedArms),
                httpRuns: sum((p) => p.expectedArms),
                preflightRequests: sum((p) => p.expectedArms),
                postcheckRequests: sum((p) => p.expectedArms),
                ...(single ? { targetStarts: sum((p) => p.expectedArms), independentObservations: points.length } : {}),
            },
            fixedBaselineMilliseconds: {
                total: fixed("total"),
                settleTotal: fixed("settleTotal"),
                includingSettleTotal: fixed("includingSettleTotal"),
            },
            requiredComparisonSafetyMilliseconds: requiredComparisons,
            declaredComparisonSafetyMilliseconds: declaredComparisons,
            controllerAllowanceMilliseconds: controller,
            ...(policy.continuation
                ? {
                      continuation: {
                          mode: policy.continuation.mode,
                          perPointVerificationMaximumMilliseconds: policy.continuation.maxVerificationMilliseconds,
                          perPointVerificationCleanupMilliseconds: 10000,
                          perPointOwnedCleanupCheckMilliseconds: 30000,
                          perPointDeclaredUnitCheckMilliseconds: 20000,
                          perPointWorkByteReservation: policy.continuation.maxVerificationBytes,
                          reservedForAllPoints: true,
                      },
                  }
                : {}),
            safetyUpperBoundMilliseconds: totalBudget ? policy.studyBudgets.maxTotalMilliseconds : safety,
            ...(single
                ? {
                      stageSumMaximumMilliseconds: safety,
                      budgetMode: policy.budgetMode,
                      totalDeadlineMilliseconds: policy.studyBudgets.maxTotalMilliseconds,
                      minimumBudgetMilliseconds: totalBudget ? minimumBudgetMilliseconds : safety,
                      cleanupReserveMilliseconds,
                      overhead: "Startup, verification, analysis and cleanup are variable; stage maxima are not an ETA",
                      historicalReference: { status: "unavailable-for-this-configuration", confidence: "low" },
                  }
                : {}),
            basis: [
                "Actual child comparison estimates are summed; no fixed timeout is presented as a normal ETA",
                "Confirmation is a separately frozen future plan and is not included in the exploration budget",
                "All planned points remain reserved even if later rates will be stopped; no automatic scope reduction",
            ],
        },
        budgets: {
            point: policy.pointBudgets,
            controller: policy.controllerBudgets,
            study: policy.studyBudgets,
            reservedEvidenceBytes: evidence,
            temporaryPointBytes,
            maxWorkingPlanBytes: policy.studyBudgets.maxPlanBytes + temporaryPointBytes,
        },
        resources: resourcePreview(loaded, policy, binding, points),
        engineeringGates: {
            driverHeadroom: "pending-independent-calibration",
            warmupStability: "unknown",
            observerCost: "pending-independent-calibration",
            correctCompleteEvidence: "pending-execution",
            p99: "minimum-1000-correct-observations-or-unknown",
        },
        scoreContract: {
            perCaseVector: true,
            mixedWorkload: false,
            withoutSlo:
                "curve and independently confirmed highest observed correct throughput; no unconditional capacity claim",
            withSlo:
                "Passing confirmed point is a lower bound; an attributable target SLO-failing higher point with driver headroom can bound an interval",
            statisticalScope: single
                ? "One observation per point; descriptive latency/throughput only, no paired inference or confirmation"
                : "Existing child P50/goodput analysis is descriptive per point; no study-wide certification",
            exploratoryModeComparison: single
                ? "Separate per-mode observations select candidates; independent confirmation retains balanced comparisons"
                : "Independent per-mode A/A observations are not a balanced cross-mode A/B comparison",
            engineeringAcceptanceIndependentOfOptionalStatisticsCertification: true,
        },
        executionReady: false,
        executed: false,
        engineeringAccepted: false,
        statisticsCertified: false,
        performanceAccepted: false,
    };
    return { ...payload, planDigest: hash(payload) };
}
export type HttpCapacityPlan = ReturnType<typeof derivedPlan>;

async function expand(root: string, loaded: Loaded, policy: HttpCapacityPolicy, policyRaw: Buffer, suite: HttpSuite) {
    const { mode: unused, ...settings } = loaded.target;
    const normalizedSettings = { ...settings, purpose: "calibration" };
    await save(join(root, "project/settings.json"), normalizedSettings);
    await writeFile(join(root, "project/project.json"), loaded.projectRaw, { flag: "wx", mode: 0o600 });
    await writeFile(join(root, "policy.json"), policyRaw, { flag: "wx", mode: 0o600 });
    for (const role of roles)
        await writeFile(
            join(root, "project", role + (role === "settings" ? ".original" : "") + ".json"),
            loaded.sources[role].raw,
            { flag: "wx", mode: 0o600 },
        );
    const binding: Binding = {
        schemaVersion: 1,
        kind: "http-capacity-binding",
        projectId: loaded.project.id,
        projectSha256: sha(loaded.projectRaw),
        policySha256: sha(policyRaw),
        sourceSnapshots: Object.fromEntries(
            roles.map((role) => [
                role,
                {
                    path: loaded.sources[role].path,
                    bytes: loaded.sources[role].raw.length,
                    sha256: loaded.sources[role].sha256,
                },
            ]),
        ) as Binding["sourceSnapshots"],
        planningToolSourceRoot: packageRoot,
        planningDirectory: root,
        suite: {
            suiteId: suite.suiteId,
            version: suite.version,
            bundleSha256: suite.bundleSha256,
            caseIds: suite.cases.map((c) => c.id),
        },
    };
    need(
        binding.suite.caseIds.length === 11,
        "HTTP_CAPACITY_SUITE",
        "Capacity v1 requires the complete eleven-question suite",
    );
    await save(join(root, "capacity-binding.json"), binding);
    const sharedEvidence = Object.fromEntries(
        await Promise.all(
            ["build", "settings", "environment"].map(async (role) => {
                const path = join(root, "project", role + ".json");
                return [role, { path, sha256: sha(await bytes(path)) }];
            }),
        ),
    );
    const points: HttpCapacityPoint[] = [];
    for (const point of identities(loaded, policy, binding)) {
        const folder = join(root, "points", point.id);
        const settingsFile = join(folder, "profile.settings.json");
        const compileDirectory = join(folder, "compile-http-plan");
        await save(settingsFile, suiteSettings(loaded, policy, binding, point));
        const generated = await generateHttpProfile(settingsFile, join(root, point.profileFile), true);
        await save(join(root, point.profileFile), projectHttpProfile(loaded.project, generated.profile));
        await planHttp(join(root, point.profileFile), compileDirectory);
        await inventory(root, policy.studyBudgets.maxPlanBytes + temporaryPointBytes);
        const requestFile = join(folder, "comparison-request.json");
        await save(requestFile, pointRequest(policy, point, sharedEvidence));
        const child = await freezeHttpComparison(requestFile, compileDirectory, join(root, point.comparisonDirectory));
        const childPlan = await json(join(root, point.comparisonDirectory, "plan.json"));
        await inventory(root, policy.studyBudgets.maxPlanBytes + temporaryPointBytes);
        await rm(compileDirectory, { recursive: true });
        points.push({
            ...point,
            comparisonPlanDigest: String(child.planDigest),
            comparisonContentDigest: String(child.contentDigest),
            estimate: child.estimate as Record<string, unknown>,
            samplingBounds: child.samplingBounds,
            workflowMaxMilliseconds: childPlan.budgets.workflowMaxMilliseconds,
        });
        // Bound actual copies while generating, not only after the full study has been materialized.
        await inventory(root, policy.studyBudgets.maxPlanBytes);
    }
    const plan = derivedPlan(loaded, policy, binding, points);
    await save(join(root, "capacity-plan.json"), plan);
    const files = await inventory(root, policy.studyBudgets.maxPlanBytes);
    const lock = { schemaVersion: 1, kind: "http-capacity-plan-lock", contentDigest: hash(files), files };
    need(
        files.reduce((n, f) => n + f.bytes, 0) + jsonBytes(lock).length <= policy.studyBudgets.maxPlanBytes,
        "HTTP_CAPACITY_PLAN_BUDGET",
        "Actual child copies and lock exceed maxPlanBytes",
    );
    await save(join(root, "capacity.lock.json"), lock);
    return { plan, contentDigest: lock.contentDigest };
}

export async function planHttpCapacity(
    projectFile: string,
    policyFile: string,
    options: { out: string; dryRun?: boolean },
) {
    need(
        typeof options.out === "string" &&
            options.out.length > 0 &&
            (options.dryRun === undefined || typeof options.dryRun === "boolean"),
        "HTTP_CAPACITY_OPTIONS",
        "Specify a new --out directory",
    );
    const loaded = await projectInputs(projectFile),
        policyRaw = await bytes(resolve(policyFile));
    const policy = parse(policyRaw, httpCapacityPolicySchema, policyFile);
    validatePolicy(policy);
    const suite = await inspectHttpSuite();
    const requested = resolve(options.out);
    const directory = options.dryRun ? requested : join(await realpath(dirname(requested)), basename(requested));
    const root = options.dryRun ? await mkdtemp(join(tmpdir(), "hgbench-capacity-preview-")) : directory;
    if (!options.dryRun) {
        try {
            await mkdir(root, { mode: 0o700 });
        } catch {
            throw new BenchError(
                "HTTP_CAPACITY_OUTPUT_EXISTS",
                "Choose a new output directory with an existing parent",
                4,
                false,
                { directory },
            );
        }
    }
    try {
        const built = await expand(root, loaded, policy, policyRaw, suite);
        return {
            ...built.plan,
            root: directory,
            directory,
            contentDigest: options.dryRun ? null : built.contentDigest,
            written: !options.dryRun,
            executed: false,
            nextActions: options.dryRun
                ? []
                : [{ kind: "read", argv: ["hgbench", "http", "capacity", "verify", directory] }],
        };
    } catch (error) {
        if (!options.dryRun)
            await save(join(root, "failure.json"), {
                partialPreserved: true,
                executed: false,
                error:
                    error instanceof BenchError
                        ? { code: error.code, message: error.message, details: error.details }
                        : String(error),
            });
        throw error;
    } finally {
        if (options.dryRun) await rm(root, { recursive: true, force: true });
    }
}

export async function verifyHttpCapacityPlan(directory: string) {
    const rootInfo = await lstat(resolve(directory));
    need(
        rootInfo.isDirectory() && !rootInfo.isSymbolicLink(),
        "HTTP_CAPACITY_PLAN_CHANGED",
        "Use a physical plan directory",
    );
    const root = await realpath(resolve(directory));
    const lock = parse(
        await bytes(join(root, "capacity.lock.json"), 8 * MiB),
        z.strictObject({
            schemaVersion: z.literal(1),
            kind: z.literal("http-capacity-plan-lock"),
            contentDigest: z.string().regex(/^[a-f0-9]{64}$/),
            files: z.array(sourceSchema).max(20000),
        }),
        "capacity lock",
    );
    const actual = (await inventory(root, 256 * MiB)).filter((f) => f.path !== "capacity.lock.json");
    need(
        isDeepStrictEqual(actual, lock.files) && hash(actual) === lock.contentDigest,
        "HTTP_CAPACITY_PLAN_CHANGED",
        "Physical files differ from the capacity lock",
    );
    const policyRaw = await bytes(join(root, "policy.json")),
        policy = parse(policyRaw, httpCapacityPolicySchema, "policy snapshot");
    validatePolicy(policy);
    const bindingRaw = await bytes(join(root, "capacity-binding.json")),
        binding = parse(bindingRaw, bindingSchema, "capacity binding");
    const projectRaw = await bytes(join(root, "project/project.json")),
        project = parse(projectRaw, httpProjectSchema, "project snapshot");
    need(
        binding.projectSha256 === sha(projectRaw) &&
            binding.policySha256 === sha(policyRaw) &&
            binding.projectId === project.id,
        "HTTP_CAPACITY_SOURCE_BINDING",
        "Project/policy snapshots differ from child-bound sources",
    );
    const sources = {} as Loaded["sources"];
    for (const role of roles) {
        const raw = await bytes(join(root, "project", role + (role === "settings" ? ".original" : "") + ".json"));
        need(
            raw.length === binding.sourceSnapshots[role].bytes && sha(raw) === binding.sourceSnapshots[role].sha256,
            "HTTP_CAPACITY_SOURCE_BINDING",
            "Copied source differs from its original source digest",
            { role },
        );
        sources[role] = {
            raw,
            path: binding.sourceSnapshots[role].path,
            sha256: sha(raw),
            value: parse(raw, z.record(z.string(), z.any()), role),
        };
    }
    const target = httpEnvironmentSettingsSchema.parse({
        ...sources.settings.value,
        mode: sources.settings.value.mode ?? "forward",
    });
    const { mode: unused, ...settings } = target;
    const normalized = jsonBytes({ ...settings, purpose: "calibration" });
    need(
        normalized.equals(await bytes(join(root, "project/settings.json"))),
        "HTTP_CAPACITY_SOURCE_BINDING",
        "Normalized target resources changed",
    );
    const suite = await inspectHttpSuite();
    need(
        isDeepStrictEqual(binding.suite, {
            suiteId: suite.suiteId,
            version: suite.version,
            bundleSha256: suite.bundleSha256,
            caseIds: suite.cases.map((c) => c.id),
        }),
        "HTTP_CAPACITY_SUITE",
        "All eleven cases and the fixed suite are required",
    );
    const loaded = { project, sources, target, runtime: sources.runtime.value };
    validateProjectExecutor(project, loaded.runtime);
    const stored = await json(join(root, "capacity-plan.json"));
    const expectedPoints = identities(loaded, policy, binding);
    need(
        Array.isArray(stored.points) && stored.points.length === expectedPoints.length,
        "HTTP_CAPACITY_POINT_INVALID",
        "The complete point denominator changed",
    );
    const points: HttpCapacityPoint[] = [];
    for (const point of expectedPoints) {
        const path = join(root, point.comparisonDirectory);
        const child = await httpComparisonPlan(["--verify", path]);
        const request = await json(join(path, "request.json"));
        const expectedShared = Object.fromEntries(
            ["build", "settings", "environment"].map((role) => [
                role,
                {
                    path: "inputs/" + role + ".json",
                    sha256: sha(role === "settings" ? normalized : sources[role as "build" | "environment"].raw),
                },
            ]),
        );
        const originalShared = Object.fromEntries(
            Object.entries(expectedShared).map(([role, entry]) => [
                role,
                {
                    ...entry,
                    path: join(binding.planningDirectory, "project", role + ".json"),
                },
            ]),
        );
        need(
            isDeepStrictEqual(
                await json(join(root, "points", point.id, "comparison-request.json")),
                pointRequest(policy, point, originalShared),
            ),
            "HTTP_CAPACITY_POINT_INVALID",
            "Original comparison request differs from the frozen point",
            { id: point.id },
        );
        need(
            isDeepStrictEqual(request, pointRequest(policy, point, expectedShared)),
            "HTTP_CAPACITY_POINT_INVALID",
            "Child observation design, thresholds, modes or budgets differ from policy",
            { id: point.id },
        );
        for (const role of ["build", "settings", "environment"] as const)
            need(
                (await bytes(join(path, "inputs/" + role + ".json"))).equals(
                    role === "settings" ? normalized : sources[role].raw,
                ),
                "HTTP_CAPACITY_SOURCE_BINDING",
                "Child source snapshot differs",
                { id: point.id, role },
            );
        need(
            (await bytes(join(path, "target/000.json"))).equals(bindingRaw),
            "HTTP_CAPACITY_SOURCE_BINDING",
            "Child target attachment does not bind the study sources",
            { id: point.id },
        );
        const expectedSettings = suiteSettings(loaded, policy, binding, point);
        need(
            isDeepStrictEqual(await json(join(root, "points", point.id, "profile.settings.json")), expectedSettings),
            "HTTP_CAPACITY_POINT_INVALID",
            "Point suite settings changed",
            { id: point.id },
        );
        const { caseIds, purpose, ...base } = expectedSettings;
        const expectedProfile = projectHttpProfile(project, {
            ...base,
            kind: "http-benchmark-profile",
            serviceChecks: true,
            cases: [suite.cases.find((c) => c.id === point.caseId)],
            benchmark: {
                suiteId: suite.suiteId,
                version: suite.version,
                bundleSha256: suite.bundleSha256,
                caseIds,
                coverage: "subset",
                purpose,
            },
        });
        const sourceProfile = await json(join(path, "inputs/profile.source.json"));
        need(
            isDeepStrictEqual(sourceProfile, expectedProfile) &&
                isDeepStrictEqual(await json(join(root, point.profileFile)), expectedProfile),
            "HTTP_CAPACITY_POINT_INVALID",
            "Child case, load, timing, correctness or trace profile changed",
            { id: point.id },
        );
        const childPlan = await json(join(path, "plan.json"));
        const actualPoint: HttpCapacityPoint = {
            ...point,
            comparisonPlanDigest: String(child.planDigest),
            comparisonContentDigest: String(child.contentDigest),
            estimate: child.estimate as Record<string, unknown>,
            samplingBounds: child.samplingBounds,
            workflowMaxMilliseconds: childPlan.budgets.workflowMaxMilliseconds,
        };
        need(
            isDeepStrictEqual(stored.points[point.index], actualPoint),
            "HTTP_CAPACITY_POINT_INVALID",
            "Point list differs from independently verified child",
            { id: point.id },
        );
        points.push(actualPoint);
    }
    const plan = derivedPlan(loaded, policy, binding, points);
    need(
        isDeepStrictEqual(plan, stored),
        "HTTP_CAPACITY_PLAN_SEMANTICS",
        "Capacity summary, budgets, source bindings or claims differ from derived children",
    );
    const size = actual.reduce((n, f) => n + f.bytes, 0) + (await bytes(join(root, "capacity.lock.json"))).length;
    need(
        size <= policy.studyBudgets.maxPlanBytes,
        "HTTP_CAPACITY_PLAN_BUDGET",
        "Actual plan exceeds the declared physical plan cap",
    );
    return {
        root,
        directory: root,
        planDigest: plan.planDigest,
        contentDigest: lock.contentDigest,
        plan,
        points: plan.points,
        estimate: plan.estimate,
        valid: true,
        executionReady: false,
        executed: false,
        performanceAccepted: false,
    };
}

export function describeHttpCapacity() {
    return {
        schemaVersion: 1,
        kind: "http-capacity-planner-capability",
        planImplemented: true,
        executionReady: false,
        commands: ["http capacity plan --project P --policy POLICY --out NEW [--dry-run]", "http capacity verify NEW"],
        scope: "All eleven fixed-suite cases; v2 observes each case/mode/rate once; v1 preserves two-pair A/A exploration",
        recommendedPolicy: "examples/http-capacity-policy-v2.json",
        budgetModes: { default: "total", configurable: ["total", "stage-sum"], legacy: "unchanged frozen policy" },
        modes: "aa projects select aaMode; forward-embedded projects plan both modes separately",
        score: "Per-case throughput/latency curves and independently confirmed highest observed correct throughput; optional frozen SLO",
        confirmation:
            "Separate four-pair confirmation plan, execution and report; not budgeted by the exploration plan",
        tooling: {
            explorationController: true,
            explorationReport: true,
            confirmationPlanner: true,
            confirmationController: true,
            confirmationReport: true,
            actualCapacityAcceptance: false,
        },
        pending: [
            "eligible real exploration and confirmation acceptance",
            "driver headroom",
            "warmup stability",
            "observer cost",
        ],
        statisticsCertified: false,
        performanceAccepted: false,
    };
}

// Shared by exploration and independently frozen confirmation planners; both retain the same bounded I/O.
export {
    bytes as readCapacityBytes,
    json as readCapacityJson,
    jsonBytes as encodeCapacityJson,
    sha as capacitySha256,
    inventory as capacityInventory,
    resourcePreview as previewCapacityResources,
};
