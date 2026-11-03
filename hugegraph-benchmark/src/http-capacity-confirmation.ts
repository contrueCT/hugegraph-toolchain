/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
import { isDeepStrictEqual } from "node:util";
import { constants } from "node:fs";
import { open, mkdir, mkdtemp, rm, writeFile, realpath, lstat, opendir } from "node:fs/promises";
import { basename, dirname, join, resolve, sep } from "node:path";
import { tmpdir } from "node:os";
import { createHash } from "node:crypto";
import { pathToFileURL } from "node:url";
import { z } from "zod";
import { BenchError, hash } from "./io.js";
import {
    readCapacityBytes as bytes,
    readCapacityJson as json,
    encodeCapacityJson as encode,
    capacitySha256 as sha,
    capacityInventory as inventory,
    previewCapacityResources,
    type HttpCapacityPlan,
} from "./http-capacity.js";
import { packageRoot } from "./plans.js";
import { projectInputs } from "./http-presets.js";
import { httpProjectSchema } from "./http-preset-contracts.js";
import { httpEnvironmentSettingsSchema } from "./http-environment-contracts.js";
import { inspectHttpSuite, generateHttpProfile } from "./http-suite.js";
import { planHttp } from "./http.js";
import { freezeHttpComparison, httpComparisonPlan } from "./http-comparison.js";
import type { HttpCapacityPolicy } from "./http-capacity-contracts.js";
import {
    httpCapacityConfirmationRequestSchema,
    type HttpCapacityConfirmationRequest,
} from "./http-capacity-confirmation-contracts.js";

type Mode = "forward" | "embedded";
type ExplorationPoint = Pick<
    HttpCapacityPlan["points"][number],
    "id" | "caseId" | "mode" | "rateIndex" | "ratePerSecond" | "expectedArms" | "comparisonPlanDigest"
>;
export type CapacityConfirmationSelectionSource = {
    schemaVersion?: number;
    kind: "http-capacity-plan";
    phase: "exploration";
    planDigest: string;
    modes: Mode[];
    suite: { caseIds: string[] };
    policy: { schemaVersion?: number; exploration?: { mode?: string; runs?: number }; rates: number[]; slo: unknown };
    points: ExplorationPoint[];
};
type Arm = {
    runId: string | null;
    measurementValid: boolean;
    status: string;
    evidence: { raw: boolean; environment: boolean; driverLifecycle: boolean; resources: boolean };
    correctCompletedInWindowPerSecond: number | null;
    driverPressureObserved: boolean | null;
    issues: string[];
    slo: { status: string };
};
export type CapacityConfirmationSelectionReport = {
    schemaVersion: 1;
    kind: "http-capacity-report";
    algorithm: "capacity-curves-v1" | "capacity-curves-v2-single-run";
    phase: "exploration";
    planDigest: string;
    points: Array<{
        id: string;
        caseId: string;
        mode: Mode;
        ratePerSecond: number;
        comparisonPlanDigest: string;
        measurementValid: boolean;
        expectedArms: number;
        arms: Arm[];
        throughput: { median: number | null };
        issues: string[];
    }>;
};
type Relation = "lower" | "candidate" | "upper";
export type CapacityConfirmationCondition = {
    id: string;
    index: number;
    caseId: string;
    rateIndex: number;
    ratePerSecond: number;
    comparison: "aa" | "forward-embedded";
    modes: { A: Mode; B: Mode };
    expectedArms: 8;
    sourcePoints: Array<{ mode: Mode; pointId: string; relation: Relation }>;
};

/** Compile the existing comparison contract; no new execution lifecycle. */
export function capacityConfirmationComparisonRequest(
    policy: HttpCapacityPolicy,
    condition: CapacityConfirmationCondition,
    budgets: HttpCapacityConfirmationRequest["budgets"]["point"],
    sharedEvidence: Record<"build" | "settings" | "environment", { path: string; sha256: string }>,
    sourceComparison: {
        schemaVersion: 1 | 2 | 3;
        comparison?: string;
        metricRoles?: { latency: "inference"; goodput: "quality" };
    },
    budgetMode?: "total" | "stage-sum",
) {
    need(policy.confirmation.pairs === 4 && policy.confirmation.requiresNewPlan === true, "HTTP_CONFIRMATION_POLICY");
    need(budgets.settleMs === policy.pointBudgets.settleMs, "HTTP_CONFIRMATION_CONDITION_DRIFT", {
        field: "settleMs",
        expected: policy.pointBudgets.settleMs,
        actual: budgets.settleMs,
    });
    need(
        sourceComparison.schemaVersion === 1 ||
            (sourceComparison.schemaVersion === 2 &&
                isDeepStrictEqual(sourceComparison.metricRoles, { latency: "inference", goodput: "quality" })) ||
            (sourceComparison.schemaVersion === 3 &&
                policy.schemaVersion === 2 &&
                sourceComparison.comparison === "single-observation" &&
                sourceComparison.metricRoles === undefined),
        "HTTP_CONFIRMATION_COMPARISON_VERSION",
    );
    need(
        sourceComparison.schemaVersion !== 1 || sourceComparison.metricRoles === undefined,
        "HTTP_CONFIRMATION_COMPARISON_VERSION",
    );
    const v3 = budgetMode !== undefined || sourceComparison.schemaVersion === 3;
    return {
        schemaVersion: v3 ? 3 : sourceComparison.schemaVersion,
        kind: "http-comparison-request",
        id: condition.id,
        purpose: "calibration",
        comparison: condition.comparison,
        modes: condition.modes,
        blockIds: ["confirm-01", "confirm-02", "confirm-03", "confirm-04"],
        orderSeed: condition.id + ":order",
        sharedEvidence,
        analysis: { ...policy.analysis, minimumPairs: 4, resampleSeed: condition.id + ":resample" },
        budgets: v3 ? { ...budgets, budgetMode: budgetMode ?? "stage-sum" } : budgets,
        ...(v3
            ? { metricRoles: sourceComparison.metricRoles ?? { latency: "inference", goodput: "inference" } }
            : sourceComparison.schemaVersion === 2
              ? { metricRoles: sourceComparison.metricRoles }
              : {}),
    };
}

function need(condition: unknown, code: string, details?: unknown): asserts condition {
    if (!condition)
        throw new BenchError(code, "Capacity confirmation source does not satisfy its contract", 3, false, details);
}
function median(values: number[]) {
    const ordered = [...values].sort((a, b) => a - b),
        middle = ordered.length >> 1;
    return ordered.length % 2 ? ordered[middle]! : (ordered[middle - 1]! + ordered[middle]!) / 2;
}

/**
 * Pure deterministic selection, not an evidence-verification API. Production callers
 * must obtain report rows by independently rebuilding the original locked exploration.
 */
export function selectCapacityConfirmation(
    plan: CapacityConfirmationSelectionSource,
    report: CapacityConfirmationSelectionReport,
    confirmationId: string,
) {
    need(/^[A-Za-z0-9][A-Za-z0-9_-]{0,39}$/.test(confirmationId), "HTTP_CONFIRMATION_ID");
    const single = plan.policy.schemaVersion === 2;
    if (single)
        need(
            plan.schemaVersion === 2 &&
                plan.policy.exploration?.mode === "single-run" &&
                plan.policy.exploration.runs === 1,
            "HTTP_CONFIRMATION_OBSERVATION_DESIGN",
        );
    need(
        plan.kind === "http-capacity-plan" &&
            plan.phase === "exploration" &&
            report.schemaVersion === 1 &&
            report.kind === "http-capacity-report" &&
            report.algorithm === (single ? "capacity-curves-v2-single-run" : "capacity-curves-v1") &&
            report.phase === "exploration" &&
            report.planDigest === plan.planDigest,
        "HTTP_CONFIRMATION_SOURCE_IDENTITY",
    );
    need(
        plan.suite.caseIds.length === 11 &&
            new Set(plan.suite.caseIds).size === 11 &&
            plan.modes.length >= 1 &&
            plan.modes.length <= 2 &&
            new Set(plan.modes).size === plan.modes.length &&
            plan.modes.every((mode) => mode === "forward" || mode === "embedded") &&
            plan.policy.rates.length >= 1 &&
            plan.policy.rates.length <= 16 &&
            plan.policy.rates.every(
                (rate, i) => Number.isFinite(rate) && rate > 0 && (i === 0 || rate > plan.policy.rates[i - 1]!),
            ) &&
            plan.points.length === 11 * plan.modes.length * plan.policy.rates.length &&
            new Set(plan.points.map((point) => point.id)).size === plan.points.length &&
            report.points.length === plan.points.length &&
            new Set(report.points.map((point) => point.id)).size === plan.points.length,
        "HTTP_CONFIRMATION_SOURCE_DENOMINATOR",
    );
    const evaluated = plan.points.map((point) => {
        const row = report.points.find((candidate) => candidate.id === point.id);
        need(
            row &&
                row.caseId === point.caseId &&
                row.mode === point.mode &&
                row.ratePerSecond === point.ratePerSecond &&
                row.expectedArms === point.expectedArms &&
                row.comparisonPlanDigest === point.comparisonPlanDigest &&
                point.expectedArms === (single ? 1 : 4) &&
                Number.isSafeInteger(point.rateIndex) &&
                plan.policy.rates[point.rateIndex] === point.ratePerSecond,
            "HTTP_CONFIRMATION_POINT_IDENTITY",
            { id: point.id },
        );
        const reasons: string[] = [];
        if (!row.measurementValid) reasons.push("measurement-not-valid");
        if (row.issues.length) reasons.push("original-point-quality-issues");
        if (row.arms.length !== point.expectedArms) reasons.push("arm-denominator-incomplete");
        if (
            new Set(row.arms.map((arm) => arm.runId)).size !== row.arms.length ||
            row.arms.some((arm) => typeof arm.runId !== "string" || !arm.runId.length)
        )
            reasons.push("independent-arm-identity-unavailable");
        const values: number[] = [];
        for (const arm of row.arms) {
            if (
                !arm.measurementValid ||
                arm.status !== "ok" ||
                ![
                    arm.evidence.raw,
                    arm.evidence.environment,
                    arm.evidence.driverLifecycle,
                    arm.evidence.resources,
                ].every((value) => value === true)
            )
                reasons.push("arm-measurement-or-environment-not-valid");
            if (arm.issues.length) reasons.push("original-arm-quality-issues");
            if (arm.driverPressureObserved !== false) reasons.push("driver-pressure-or-observation-unavailable");
            if (plan.policy.slo !== null && arm.slo.status !== "passed") reasons.push("frozen-slo-not-proven-passed");
            const value = arm.correctCompletedInWindowPerSecond;
            if (typeof value !== "number" || !Number.isFinite(value) || value < 0)
                reasons.push("correct-throughput-unavailable");
            else values.push(value);
        }
        const safe = reasons.length === 0,
            correctRequestsPerSecond = safe ? median(values) : null;
        if (safe)
            need(row.throughput.median === correctRequestsPerSecond, "HTTP_CONFIRMATION_REPORT_ACCOUNTING", {
                id: point.id,
            });
        return { point, safe, reasons: [...new Set(reasons)], correctRequestsPerSecond };
    });
    const selections = plan.suite.caseIds.flatMap((caseId) =>
        plan.modes.map((mode) => {
            const rows = evaluated.filter((row) => row.point.caseId === caseId && row.point.mode === mode);
            need(
                rows.length === plan.policy.rates.length &&
                    new Set(rows.map((row) => row.point.rateIndex)).size === rows.length,
                "HTTP_CONFIRMATION_SOURCE_DENOMINATOR",
                { caseId, mode },
            );
            const ranked = rows
                .filter((row) => row.safe)
                .sort(
                    (a, b) =>
                        b.correctRequestsPerSecond! - a.correctRequestsPerSecond! ||
                        a.point.ratePerSecond - b.point.ratePerSecond,
                );
            const candidate = ranked[0];
            const neighborhood = ([-1, 0, 1] as const).map((offset) => {
                const relation: Relation = offset === -1 ? "lower" : offset === 0 ? "candidate" : "upper";
                const index = candidate ? candidate.point.rateIndex + offset : null;
                const row = index === null ? undefined : rows.find((row) => row.point.rateIndex === index);
                return {
                    relation,
                    rateIndex: index,
                    ratePerSecond: row?.point.ratePerSecond ?? null,
                    pointId: row?.point.id ?? null,
                    status: !candidate
                        ? "unknown"
                        : !row
                          ? "outside-grid"
                          : row.safe
                            ? "selected"
                            : "unsafe-or-unavailable",
                    reasons: !candidate
                        ? ["no-complete-safe-observed-candidate"]
                        : (row?.reasons ?? ["no-adjacent-registered-point"]),
                };
            });
            return {
                caseId,
                mode,
                status: candidate ? "provisional-candidate" : "unknown",
                candidatePointId: candidate?.point.id ?? null,
                candidateCorrectRequestsPerSecond: candidate?.correctRequestsPerSecond ?? null,
                rankedSafePointIds: ranked.map((row) => row.point.id),
                neighborhood,
                excludedPoints: rows
                    .filter((row) => !row.safe)
                    .map((row) => ({ pointId: row.point.id, reasons: row.reasons })),
                qualifications: { driverHeadroom: "unknown", warmupStable: null, observerCost: "unknown" },
                confirmed: false,
                engineeringAccepted: false,
            };
        }),
    );
    const comparisons: CapacityConfirmationCondition[] = [];
    for (const caseId of plan.suite.caseIds)
        for (const [rateIndex, ratePerSecond] of plan.policy.rates.entries()) {
            const selected = selections
                .filter((selection) => selection.caseId === caseId)
                .flatMap((selection) =>
                    selection.neighborhood
                        .filter((point) => point.status === "selected" && point.rateIndex === rateIndex)
                        .map((point) => ({ mode: selection.mode, pointId: point.pointId!, relation: point.relation })),
                );
            if (!selected.length) continue;
            const shared = selected.length === 2;
            const modes: CapacityConfirmationCondition["modes"] = shared
                ? { A: "forward", B: "embedded" }
                : { A: selected[0]!.mode, B: selected[0]!.mode };
            const comparison = shared ? "forward-embedded" : "aa";
            comparisons.push({
                id:
                    "confirm-" +
                    hash({
                        confirmationId,
                        explorationPlanDigest: plan.planDigest,
                        caseId,
                        ratePerSecond,
                        comparison,
                        modes,
                    }).slice(0, 32),
                index: comparisons.length,
                caseId,
                rateIndex,
                ratePerSecond,
                comparison,
                modes,
                expectedArms: 8,
                sourcePoints: selected.sort((a, b) => a.mode.localeCompare(b.mode)),
            });
        }
    need(
        selections.length === 11 * plan.modes.length &&
            comparisons.every(
                (comparison) => comparison.sourcePoints.length === (comparison.comparison === "aa" ? 1 : 2),
            ),
        "HTTP_CONFIRMATION_SELECTION_DENOMINATOR",
    );
    return {
        algorithm: "capacity-confirmation-selection-v1",
        sourcePlanDigest: plan.planDigest,
        tieBreak: "lower-offered-rate",
        adjacency: "immediate-predeclared-grid-neighbors; never-skip-an-unsafe-or-unexecuted-point",
        aggregation: "median-of-all-independent-exploration-arm-correct-completion-rates",
        selections,
        comparisons,
        engineeringAccepted: false,
        performanceAccepted: false,
    };
}

const MiB = 1024 ** 2;
const temporaryPointBytes = 64 * MiB;
const roles = ["build", "settings", "environment", "runtime"] as const;
type Loaded = Awaited<ReturnType<typeof projectInputs>>;
type Entry = { path: string; bytes: number; sha256: string };
const entrySchema = z.strictObject({
    path: z.string().min(1).max(4096),
    bytes: z.number().int().nonnegative(),
    sha256: z.string().regex(/^[a-f0-9]{64}$/),
});
const bindingSchema = z.strictObject({
    schemaVersion: z.literal(1),
    kind: z.literal("http-capacity-confirmation-binding"),
    requestSha256: z.string(),
    requestDirectory: z.string(),
    planningDirectory: z.string(),
    planningToolSourceRoot: z.string(),
    projectSha256: z.string(),
    sourceSnapshots: z.strictObject({
        build: entrySchema,
        settings: entrySchema,
        environment: entrySchema,
        runtime: entrySchema,
    }),
    exploration: z.strictObject({
        planDirectory: z.string(),
        executionSet: z.string(),
        reportDirectory: z.string(),
        planDigest: z.string(),
        capacityLockSha256: z.string(),
        executionSetSha256: z.string(),
        reportLockSha256: z.string(),
        reportJsonSha256: z.string(),
        originalImplementationSha256: z.string(),
        reverifiedImplementationSha256: z.string(),
    }),
});
type Binding = z.infer<typeof bindingSchema>;
function parse<T>(raw: Buffer, schema: z.ZodType<T>): T {
    try {
        return schema.parse(JSON.parse(new TextDecoder("utf8", { fatal: true }).decode(raw)));
    } catch (error) {
        throw new BenchError(
            "HTTP_CONFIRMATION_INPUT_INVALID",
            "Confirmation input does not satisfy its contract",
            2,
            false,
            error instanceof z.ZodError ? error.issues : String(error),
        );
    }
}
async function save(path: string, value: unknown) {
    await mkdir(dirname(path), { recursive: true, mode: 0o700 });
    await writeFile(path, encode(value), { flag: "wx", mode: 0o600 });
}
async function physical(path: string) {
    const requested = resolve(path),
        info = await lstat(requested);
    need(!info.isSymbolicLink(), "HTTP_CONFIRMATION_INPUT_SYMLINK", { path });
    return realpath(requested);
}
async function canonicalReference(path: string): Promise<string> {
    let cursor = resolve(path);
    const suffix: string[] = [];
    while (true) {
        try {
            return join(await realpath(cursor), ...suffix.reverse());
        } catch (error) {
            if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
            const parent = dirname(cursor);
            need(parent !== cursor, "HTTP_CONFIRMATION_INPUT_PATH", { path });
            suffix.push(basename(cursor));
            cursor = parent;
        }
    }
}
const overlaps = (a: string, b: string) => a === b || a.startsWith(b + sep) || b.startsWith(a + sep);
function separate(output: string, inputs: string[]) {
    need(!inputs.some((input) => overlaps(output, input)), "HTTP_CONFIRMATION_OUTPUT_OVERLAP", { output });
}

/** Pure resource/configuration comparison. Changes are explicit safety-cap increases only. */
export function capacityConfirmationResourceChanges(
    original: { settings: Record<string, any>; environment: Record<string, any>; runtime: Record<string, any> },
    next: { settings: Record<string, any>; environment: Record<string, any>; runtime: Record<string, any> },
) {
    const a = structuredClone(original),
        b = structuredClone(next);
    const changes: Array<{ field: string; before: unknown; after: unknown }> = [];
    const increase = (section: keyof typeof a, keys: string[]) => {
        let left: any = a[section],
            right: any = b[section];
        for (const key of keys.slice(0, -1)) {
            left = left?.[key];
            right = right?.[key];
        }
        const key = keys.at(-1)!;
        if (left?.[key] === undefined && right?.[key] === undefined) return;
        need(
            left &&
                right &&
                Number.isSafeInteger(left[key]) &&
                Number.isSafeInteger(right[key]) &&
                right[key] >= left[key],
            "HTTP_CONFIRMATION_RESOURCE_DRIFT",
            { field: section + "." + keys.join(".") },
        );
        if (right[key] !== left[key])
            changes.push({ field: section + "." + keys.join("."), before: left[key], after: right[key] });
        right[key] = left[key];
    };
    increase("settings", ["budget", "wallSeconds"]);
    for (const key of ["timeoutMs", "maxOutputBytes", "maxWindows"])
        increase("settings", ["jvmObservation", "parser", key]);
    increase("runtime", ["driver", "wallSeconds"]);
    for (const key of ["durationMs", "maxSamples", "maxOutputBytes"]) increase("runtime", ["collector", key]);
    for (const key of ["hostBootId", "coRunningJobs"]) {
        if (!isDeepStrictEqual(a.environment[key], b.environment[key]))
            changes.push({ field: "environment." + key, before: a.environment[key], after: b.environment[key] });
        delete a.environment[key];
        delete b.environment[key];
    }
    if (a.runtime.sourceRoot !== b.runtime.sourceRoot)
        changes.push({ field: "runtime.sourceRoot", before: a.runtime.sourceRoot, after: b.runtime.sourceRoot });
    delete a.runtime.sourceRoot;
    delete b.runtime.sourceRoot;
    need(isDeepStrictEqual(a, b), "HTTP_CONFIRMATION_RESOURCE_DRIFT", {
        allowed: "only declared safety-cap increases, runtime sourceRoot, fresh boot/coRunningJobs evidence",
    });
    return changes;
}

// The report's exact output inventory is independently streamed before and after replay.
// Large point JSON is never accumulated into a single all-report buffer.
async function reportInventory(root: string, cap: number, check: () => void): Promise<Entry[]> {
    const files: Entry[] = [];
    let total = 0,
        entries = 0;
    async function walk(prefix = "", depth = 0): Promise<void> {
        need(depth <= 16, "HTTP_CONFIRMATION_REPORT_BUDGET");
        for await (const item of await opendir(join(root, prefix))) {
            check();
            need(++entries <= 20000, "HTTP_CONFIRMATION_REPORT_BUDGET");
            const path = prefix ? prefix + "/" + item.name : item.name;
            need(
                /^[A-Za-z0-9_./-]+$/.test(path) && !path.split("/").some((p) => p === ".." || p === "."),
                "HTTP_CONFIRMATION_REPORT_PATH",
            );
            const info = await lstat(join(root, path));
            if (info.isDirectory() && !info.isSymbolicLink()) {
                await walk(path, depth + 1);
                continue;
            }
            need(info.isFile() && !info.isSymbolicLink(), "HTTP_CONFIRMATION_REPORT_TYPE");
            total += info.size;
            need(total <= cap && info.size <= 32 * MiB, "HTTP_CONFIRMATION_REPORT_BUDGET");
            const fd = await open(join(root, path), constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
            try {
                const before = await fd.stat(),
                    digest = createHash("sha256"),
                    chunk = Buffer.alloc(65536);
                let used = 0;
                need(before.isFile() && before.size === info.size, "HTTP_CONFIRMATION_SOURCE_CHANGED");
                while (true) {
                    check();
                    const result = await fd.read(chunk, 0, chunk.length, used);
                    if (!result.bytesRead) break;
                    used += result.bytesRead;
                    need(used <= before.size, "HTTP_CONFIRMATION_SOURCE_CHANGED");
                    digest.update(chunk.subarray(0, result.bytesRead));
                }
                const after = await fd.stat();
                need(
                    used === before.size &&
                        before.size === after.size &&
                        before.mtimeMs === after.mtimeMs &&
                        before.ctimeMs === after.ctimeMs,
                    "HTTP_CONFIRMATION_SOURCE_CHANGED",
                );
                files.push({ path, bytes: used, sha256: digest.digest("hex") });
            } finally {
                await fd.close();
            }
        }
    }
    await walk();
    return files.sort((a, b) => (a.path < b.path ? -1 : a.path > b.path ? 1 : 0));
}
async function reverifySource(
    request: HttpCapacityConfirmationRequest,
    base: string,
    includeSourceIndependence = false,
) {
    const started = performance.now(),
        check = () =>
            need(
                performance.now() - started < request.sourceReview.maxMilliseconds,
                "HTTP_CONFIRMATION_SOURCE_DEADLINE",
            );
    const planDirectory = await physical(resolve(base, request.exploration.plan)),
        executionSet = await physical(resolve(base, request.exploration.executionSet)),
        reportDirectory = await physical(resolve(base, request.exploration.report));
    check();
    const reportLockRaw = await bytes(join(reportDirectory, "evidence.lock.json")),
        lock = parse(
            reportLockRaw,
            z.strictObject({
                schemaVersion: z.literal(1),
                kind: z.literal("http-capacity-report-lock"),
                planDigest: z.string(),
                files: z.array(entrySchema).max(20000),
                contentDigest: z.string(),
            }),
        );
    const all = await reportInventory(reportDirectory, request.sourceReview.maxBytes, check),
        actual = all.filter((f) => f.path !== "evidence.lock.json");
    need(
        isDeepStrictEqual(actual, lock.files) &&
            sha(encode(actual)) === lock.contentDigest &&
            all.find((f) => f.path === "evidence.lock.json")?.sha256 === sha(reportLockRaw),
        "HTTP_CONFIRMATION_REPORT_LOCK",
    );
    const reportRaw = await bytes(join(reportDirectory, "report.json")),
        report = JSON.parse(reportRaw.toString("utf8"));
    const reportBudget = report.reportBudget;
    need(
        Number.isSafeInteger(reportBudget?.timeoutMs) &&
            reportBudget.timeoutMs >= 1000 &&
            reportBudget.timeoutMs <= request.sourceReview.maxMilliseconds &&
            Number.isSafeInteger(reportBudget?.maxBytes) &&
            reportBudget.maxBytes >= MiB &&
            reportBudget.maxBytes <= request.sourceReview.maxBytes,
        "HTTP_CONFIRMATION_SOURCE_REVIEW_BUDGET",
    );
    const planRaw = await bytes(join(planDirectory, "capacity-plan.json")),
        plan = JSON.parse(planRaw.toString("utf8")) as HttpCapacityPlan,
        capacityLockRaw = await bytes(join(planDirectory, "capacity.lock.json")),
        executionRaw = await bytes(executionSet, MiB);
    need(
        lock.planDigest === plan.planDigest && report.planDigest === plan.planDigest,
        "HTTP_CONFIRMATION_SOURCE_IDENTITY",
    );
    const temp = await mkdtemp(join(tmpdir(), "hgbench-confirmation-source-")),
        cancellation = new AbortController();
    const timer = setTimeout(
        () => cancellation.abort(),
        Math.max(1, request.sourceReview.maxMilliseconds - (performance.now() - started)),
    );
    try {
        const component = await import(pathToFileURL(join(packageRoot, "components/http/capacity-evidence.mjs")).href);
        await component.reportCapacity({
            planDirectory,
            inputFile: executionSet,
            outputDirectory: join(temp, "report"),
            timeoutMs: reportBudget.timeoutMs,
            maxBytes: reportBudget.maxBytes,
            signal: cancellation.signal,
        });
        check();
        need(
            (await bytes(join(temp, "report/report.json"))).equals(reportRaw),
            "HTTP_CONFIRMATION_REPORT_REBUILD_MISMATCH",
        );
        need(
            isDeepStrictEqual(await reportInventory(reportDirectory, request.sourceReview.maxBytes, check), all) &&
                (await bytes(join(planDirectory, "capacity-plan.json"))).equals(planRaw) &&
                (await bytes(join(planDirectory, "capacity.lock.json"))).equals(capacityLockRaw) &&
                (await bytes(executionSet, MiB)).equals(executionRaw),
            "HTTP_CONFIRMATION_SOURCE_CHANGED",
        );
        const executionInput = JSON.parse(executionRaw.toString("utf8"));
        const executionDirectories = await Promise.all(
            executionInput.points.map((point: { executionDirectory: string }) =>
                canonicalReference(resolve(dirname(executionSet), point.executionDirectory)),
            ),
        );
        const sourceIndependence = includeSourceIndependence
            ? await component.collectCapacitySourceIndependence({
                  plan,
                  inputFile: executionSet,
                  reportDirectory: join(temp, "report"),
                  maxBytes: request.sourceReview.maxBytes,
                  check,
              })
            : undefined;
        check();
        return {
            plan,
            report: report as CapacityConfirmationSelectionReport,
            reportRaw,
            reportLockRaw,
            executionDirectories: executionDirectories as string[],
            ...(includeSourceIndependence ? { sourceIndependence } : {}),
            reference: {
                planDirectory,
                executionSet,
                reportDirectory,
                planDigest: plan.planDigest,
                capacityLockSha256: sha(capacityLockRaw),
                executionSetSha256: sha(executionRaw),
                reportLockSha256: sha(reportLockRaw),
                reportJsonSha256: sha(reportRaw),
                originalImplementationSha256: sha(await bytes(join(reportDirectory, "implementation.json"))),
                reverifiedImplementationSha256: sha(await bytes(join(temp, "report/implementation.json"))),
            },
        };
    } finally {
        clearTimeout(timer);
        await rm(temp, { recursive: true, force: true });
    }
}
type Source = Awaited<ReturnType<typeof reverifySource>>;
function normalizedSettings(loaded: Pick<Loaded, "target">) {
    const { mode: ignored, ...settings } = loaded.target;
    return { ...settings, purpose: "calibration" };
}
async function resourceChanges(source: Source, loaded: Loaded) {
    need(
        (await bytes(join(source.reference.planDirectory, "project/build.json"))).equals(loaded.sources.build.raw),
        "HTTP_CONFIRMATION_BUILD_DRIFT",
    );
    return capacityConfirmationResourceChanges(
        {
            settings: await json(join(source.reference.planDirectory, "project/settings.json")),
            environment: await json(join(source.reference.planDirectory, "project/environment.json")),
            runtime: await json(join(source.reference.planDirectory, "project/runtime.json")),
        },
        {
            settings: normalizedSettings(loaded),
            environment: loaded.sources.environment.value,
            runtime: loaded.runtime,
        },
    );
}
function settingsFor(source: Source, loaded: Loaded, binding: Binding, condition: CapacityConfirmationCondition) {
    const policy = source.plan.policy,
        { maxInFlight, maxStartDelayMs, ...http } = policy.http;
    return {
        schemaVersion: 1,
        kind: "http-suite-settings",
        name: condition.id,
        caseIds: [condition.caseId],
        ...http,
        purpose: "calibration",
        repeats: 1,
        warmupMs: policy.confirmation.warmupMs,
        durationMs: policy.confirmation.durationMs,
        loads: [
            {
                id: "open-" + condition.rateIndex,
                model: "open",
                ratePerSecond: condition.ratePerSecond,
                maxInFlight,
                maxStartDelayMs,
            },
        ],
        endpoint: "http://127.0.0.1:18080/gremlin",
        authorizationEnv: null,
        target: {
            label: loaded.project.label,
            build: String(loaded.sources.build.value.sourceCommit ?? binding.sourceSnapshots.build.sha256),
            mode: condition.modes.A,
            environment: loaded.project.description,
            evidenceFiles: [join(binding.planningDirectory, "confirmation-binding.json")],
        },
    };
}
async function originalComparison(source: Source, condition: CapacityConfirmationCondition) {
    const sourcePoint = source.plan.points.find((p) => p.id === condition.sourcePoints[0]!.pointId)!;
    return (await json(join(source.reference.planDirectory, sourcePoint.comparisonDirectory, "request.json"))) as {
        schemaVersion: 1 | 2 | 3;
        comparison?: string;
        metricRoles?: { latency: "inference"; goodput: "quality" };
    };
}
type Child = CapacityConfirmationCondition & {
    comparisonDirectory: string;
    profileFile: string;
    comparisonPlanDigest: string;
    comparisonContentDigest: string;
    estimate: Record<string, any>;
    samplingBounds: unknown;
    workflowMaxMilliseconds: number;
};
function derived(
    source: Source,
    request: HttpCapacityConfirmationRequest,
    binding: Binding,
    loaded: Loaded,
    selection: ReturnType<typeof selectCapacityConfirmation>,
    changes: Awaited<ReturnType<typeof resourceChanges>>,
    points: Child[],
) {
    const declaredComparisons = points.length * request.budgets.point.maxTotalMilliseconds,
        controller =
            points.length * request.budgets.controller.perPointMilliseconds +
            request.budgets.controller.totalMilliseconds,
        safety = declaredComparisons + controller,
        evidence =
            points.length * request.budgets.point.maxTotalEvidenceBytes +
            request.budgets.study.maxPlanBytes +
            temporaryPointBytes;
    const v2 = request.schemaVersion === 2;
    const total = v2 && request.budgetMode === "total";
    const cleanupReserveMilliseconds = 100000;
    const fixed = points.reduce((n, p) => n + Number(p.estimate.fixedBaselineMilliseconds.includingSettleTotal), 0);
    const minimumBudgetMilliseconds = fixed + cleanupReserveMilliseconds + 60000;
    need(
        (total ? minimumBudgetMilliseconds : safety) <= request.budgets.study.maxTotalMilliseconds,
        "HTTP_CONFIRMATION_STUDY_TIME_BUDGET",
        {
            required: total ? minimumBudgetMilliseconds : safety,
            changedParameters: [],
        },
    );
    need(evidence <= request.budgets.study.maxEvidenceBytes, "HTTP_CONFIRMATION_STUDY_EVIDENCE_BUDGET", {
        evidence,
        changedParameters: [],
    });
    const resources = points.length
        ? previewCapacityResources(
              loaded,
              {
                  http: source.plan.policy.http,
                  pointBudgets: request.budgets.point,
                  analysis: source.plan.policy.analysis,
              },
              binding,
              points,
          )
        : null;
    const payload = {
        schemaVersion: v2 ? 2 : 1,
        kind: "http-capacity-confirmation-plan",
        ...(v2
            ? {
                  budgetMode: request.budgetMode,
                  totalDeadlineMilliseconds: request.budgets.study.maxTotalMilliseconds,
                  cleanupReserveMilliseconds,
                  minimumBudgetMilliseconds: total ? minimumBudgetMilliseconds : safety,
              }
            : {}),
        phase: "confirmation",
        id: request.id,
        scope: "all-eleven-single-case",
        exploration: binding.exploration,
        requestSha256: binding.requestSha256,
        projectSha256: binding.projectSha256,
        bindingSha256: sha(encode(binding)),
        suite: source.plan.suite,
        modes: source.plan.modes,
        policy: source.plan.policy,
        selection,
        points,
        pointCount: points.length,
        expectedArms: points.length * 8,
        sourceSnapshots: binding.sourceSnapshots,
        resourceChanges: changes,
        resources,
        estimate: {
            schemaVersion: 1,
            confidence: "low",
            normalEtaMilliseconds: null,
            counts: {
                cases: 11,
                modes: source.plan.modes.length,
                selectionRows: selection.selections.length,
                comparisons: points.length,
                pairs: points.length * 4,
                arms: points.length * 8,
            },
            fixedBaselineMilliseconds: {
                total: points.reduce((n, p) => n + Number(p.estimate.fixedBaselineMilliseconds.total), 0),
            },
            requiredComparisonSafetyMilliseconds: points.reduce(
                (n, p) => n + Number(p.estimate.safetyUpperBoundMilliseconds),
                0,
            ),
            declaredComparisonSafetyMilliseconds: declaredComparisons,
            controllerAllowanceMilliseconds: controller,
            safetyUpperBoundMilliseconds: total ? request.budgets.study.maxTotalMilliseconds : safety,
            ...(v2
                ? {
                      stageSumMaximumMilliseconds: safety,
                      budgetMode: request.budgetMode,
                      totalDeadlineMilliseconds: request.budgets.study.maxTotalMilliseconds,
                      minimumBudgetMilliseconds: total ? minimumBudgetMilliseconds : safety,
                      cleanupReserveMilliseconds,
                      overhead: "Startup, verification, analysis and cleanup are variable; stage maxima are not an ETA",
                      historicalReference: { status: "unavailable-for-this-configuration", confidence: "low" },
                  }
                : {}),
            sourceReviewMaximumMilliseconds: request.sourceReview.maxMilliseconds,
            basis: [
                "Actual four-pair child comparisons; no unknown startup overhead is an ETA",
                "Every selected comparison remains reserved; no parameter is silently reduced",
            ],
        },
        budgets: { ...request.budgets, reservedEvidenceBytes: evidence, temporaryPointBytes },
        scoreContract: {
            aggregation: "median-of-all-independent-confirmation-arm-correct-completion-rates; no exploration fallback",
            candidate: "a safe adjacent confirmation point may exceed the exploratory candidate",
            withoutSlo: "independently confirmed highest observed correct throughput, no unconditional capacity claim",
            withSlo: "SLO status must be independently proven under the inherited policy; unknown remains unknown",
        },
        executionReady: false,
        executed: false,
        engineeringAccepted: false,
        statisticsCertified: false,
        performanceAccepted: false,
    };
    return { ...payload, planDigest: hash(payload) };
}
function validateBudgets(
    source: Source,
    request: HttpCapacityConfirmationRequest,
    conditions: CapacityConfirmationCondition[],
) {
    const budget = request.budgets.point,
        policy = source.plan.policy;
    need(budget.settleMs === policy.pointBudgets.settleMs, "HTTP_CONFIRMATION_CONDITION_DRIFT");
    need(
        budget.sealAndAnalyzeMs >= policy.analysis.maxMilliseconds &&
            request.budgets.controller.perPointMilliseconds >= 330000 + budget.stopMs + budget.sealAndAnalyzeMs + 40000,
        "HTTP_CONFIRMATION_CONTROLLER_BUDGET",
    );
    const maximumRate = Math.max(0, ...conditions.map((p) => p.ratePerSecond)),
        requiredRequests = Math.ceil(
            (Math.max(policy.confirmation.warmupMs, policy.confirmation.durationMs) * maximumRate) / 1000,
        );
    need(requiredRequests <= policy.http.maxRequestsPerPhase, "HTTP_CONFIRMATION_REQUEST_BUDGET", { requiredRequests });
    // These are reservations, never a claimed normal ETA. Early check avoids constructing a partial oversized study.
    need(
        (request.schemaVersion === 2 && request.budgetMode === "total"
            ? conditions.length *
                  8 *
                  (policy.confirmation.warmupMs + policy.confirmation.durationMs + budget.settleMs) +
              160000
            : conditions.length * (budget.maxTotalMilliseconds + request.budgets.controller.perPointMilliseconds) +
              request.budgets.controller.totalMilliseconds) <= request.budgets.study.maxTotalMilliseconds,
        "HTTP_CONFIRMATION_STUDY_TIME_BUDGET",
    );
    need(
        conditions.length * budget.maxTotalEvidenceBytes + request.budgets.study.maxPlanBytes + temporaryPointBytes <=
            request.budgets.study.maxEvidenceBytes,
        "HTTP_CONFIRMATION_STUDY_EVIDENCE_BUDGET",
    );
}
async function expand(
    root: string,
    requestRaw: Buffer,
    request: HttpCapacityConfirmationRequest,
    base: string,
    source: Source,
    loaded: Loaded,
    selection: ReturnType<typeof selectCapacityConfirmation>,
) {
    validateBudgets(source, request, selection.comparisons);
    const changes = await resourceChanges(source, loaded);
    const binding: Binding = {
        schemaVersion: 1,
        kind: "http-capacity-confirmation-binding",
        requestSha256: sha(requestRaw),
        requestDirectory: base,
        planningDirectory: root,
        planningToolSourceRoot: packageRoot,
        projectSha256: sha(loaded.projectRaw),
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
        exploration: source.reference,
    };
    await save(join(root, "confirmation-binding.json"), binding);
    await writeFile(join(root, "request.json"), requestRaw, { flag: "wx", mode: 0o600 });
    await save(join(root, "project/settings.json"), normalizedSettings(loaded));
    await writeFile(join(root, "project/project.json"), loaded.projectRaw, { flag: "wx", mode: 0o600 });
    for (const role of roles)
        await writeFile(
            join(root, "project", role + (role === "settings" ? ".original" : "") + ".json"),
            loaded.sources[role].raw,
            { flag: "wx", mode: 0o600 },
        );
    await writeFile(join(root, "source-report.json"), source.reportRaw, { flag: "wx", mode: 0o600 });
    await writeFile(join(root, "source-report.lock.json"), source.reportLockRaw, { flag: "wx", mode: 0o600 });
    const shared = Object.fromEntries(
        await Promise.all(
            (["build", "settings", "environment"] as const).map(async (role) => {
                const path = join(root, "project", role + ".json");
                return [role, { path, sha256: sha(await bytes(path)) }];
            }),
        ),
    ) as Record<"build" | "settings" | "environment", { path: string; sha256: string }>;
    const points: Child[] = [];
    for (const condition of selection.comparisons) {
        const folder = join(root, "points", condition.id),
            profileFile = "points/" + condition.id + "/profile.json",
            comparisonDirectory = "points/" + condition.id + "/comparison",
            compile = join(folder, "compile-http-plan");
        await save(join(folder, "profile.settings.json"), settingsFor(source, loaded, binding, condition));
        await generateHttpProfile(join(folder, "profile.settings.json"), join(root, profileFile), false);
        await planHttp(join(root, profileFile), compile);
        await inventory(root, request.budgets.study.maxPlanBytes + temporaryPointBytes);
        await save(
            join(folder, "comparison-request.json"),
            capacityConfirmationComparisonRequest(
                source.plan.policy,
                condition,
                request.budgets.point,
                shared,
                await originalComparison(source, condition),
                request.schemaVersion === 2 ? request.budgetMode : undefined,
            ),
        );
        const child = await freezeHttpComparison(
            join(folder, "comparison-request.json"),
            compile,
            join(root, comparisonDirectory),
        );
        const childPlan = await json(join(root, comparisonDirectory, "plan.json"));
        await inventory(root, request.budgets.study.maxPlanBytes + temporaryPointBytes);
        await rm(compile, { recursive: true });
        points.push({
            ...condition,
            comparisonDirectory,
            profileFile,
            comparisonPlanDigest: String(child.planDigest),
            comparisonContentDigest: String(child.contentDigest),
            estimate: child.estimate as Record<string, any>,
            samplingBounds: child.samplingBounds,
            workflowMaxMilliseconds: childPlan.budgets.workflowMaxMilliseconds,
        });
        await inventory(root, request.budgets.study.maxPlanBytes);
    }
    const plan = derived(source, request, binding, loaded, selection, changes, points);
    await save(join(root, "confirmation-plan.json"), plan);
    const files = await inventory(root, request.budgets.study.maxPlanBytes),
        lock = { schemaVersion: 1, kind: "http-capacity-confirmation-lock", contentDigest: hash(files), files };
    need(
        files.reduce((n, f) => n + f.bytes, 0) + encode(lock).length <= request.budgets.study.maxPlanBytes,
        "HTTP_CONFIRMATION_PLAN_BUDGET",
    );
    await save(join(root, "confirmation.lock.json"), lock);
    return { plan, contentDigest: lock.contentDigest };
}

export async function planHttpCapacityConfirmation(requestFile: string, options: { out: string; dryRun?: boolean }) {
    need(
        typeof options.out === "string" &&
            options.out.length > 0 &&
            (options.dryRun === undefined || typeof options.dryRun === "boolean"),
        "HTTP_CONFIRMATION_OPTIONS",
    );
    const file = await physical(requestFile),
        raw = await bytes(file),
        request = parse(raw, httpCapacityConfirmationRequestSchema),
        base = dirname(file);
    const source = await reverifySource(request, base),
        loaded = await projectInputs(resolve(base, request.project)),
        selection = selectCapacityConfirmation(
            source.plan as CapacityConfirmationSelectionSource,
            source.report,
            request.id,
        );
    validateBudgets(source, request, selection.comparisons);
    await resourceChanges(source, loaded);
    const requested = resolve(options.out),
        directory = join(await realpath(dirname(requested)), basename(requested));
    separate(directory, [
        source.reference.planDirectory,
        source.reference.reportDirectory,
        source.reference.executionSet,
        ...source.executionDirectories,
        file,
        await canonicalReference(loaded.projectPath),
        ...(await Promise.all(roles.map((role) => canonicalReference(loaded.sources[role].path)))),
        ...(await Promise.all(
            ["components", "src", "dist", "release", "benchmarks", "presets", "node_modules"].map((p) =>
                canonicalReference(join(packageRoot, p)),
            ),
        )),
    ]);
    const root = options.dryRun ? await mkdtemp(join(tmpdir(), "hgbench-confirmation-preview-")) : directory;
    if (!options.dryRun) {
        try {
            await mkdir(root, { mode: 0o700 });
        } catch {
            throw new BenchError(
                "HTTP_CONFIRMATION_OUTPUT_EXISTS",
                "Use a new output directory with an existing parent",
                4,
            );
        }
    }
    try {
        const result = await expand(root, raw, request, base, source, loaded, selection);
        return {
            ...result.plan,
            directory,
            contentDigest: options.dryRun ? null : result.contentDigest,
            written: !options.dryRun,
            nextActions: options.dryRun
                ? []
                : [{ kind: "read", argv: ["hgbench", "http", "capacity", "confirmation", "verify", directory] }],
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

export async function verifyHttpCapacityConfirmationPlan(
    directory: string,
    options?: { includeSourceIndependence: true },
) {
    need(
        options === undefined || (Object.keys(options).length === 1 && options.includeSourceIndependence === true),
        "HTTP_CONFIRMATION_VERIFY_OPTIONS",
    );
    const root = await physical(directory),
        info = await lstat(root);
    need(info.isDirectory(), "HTTP_CONFIRMATION_PLAN_TYPE");
    const lockRaw = await bytes(join(root, "confirmation.lock.json"));
    const lock = parse(
        lockRaw,
        z.strictObject({
            schemaVersion: z.literal(1),
            kind: z.literal("http-capacity-confirmation-lock"),
            contentDigest: z.string(),
            files: z.array(entrySchema).max(20000),
        }),
    );
    const actual = (await inventory(root, 256 * MiB)).filter((f) => f.path !== "confirmation.lock.json");
    need(
        isDeepStrictEqual(actual, lock.files) && hash(actual) === lock.contentDigest,
        "HTTP_CONFIRMATION_PLAN_CHANGED",
    );
    const raw = await bytes(join(root, "request.json")),
        request = parse(raw, httpCapacityConfirmationRequestSchema),
        bindingRaw = await bytes(join(root, "confirmation-binding.json")),
        binding = parse(bindingRaw, bindingSchema),
        source = await reverifySource(request, binding.requestDirectory, options?.includeSourceIndependence === true);
    need(
        sha(raw) === binding.requestSha256 &&
            isDeepStrictEqual(source.reference, binding.exploration) &&
            (await bytes(join(root, "source-report.json"))).equals(source.reportRaw) &&
            (await bytes(join(root, "source-report.lock.json"))).equals(source.reportLockRaw),
        "HTTP_CONFIRMATION_SOURCE_BINDING",
    );
    const projectRaw = await bytes(join(root, "project/project.json")),
        project = parse(projectRaw, httpProjectSchema);
    need(sha(projectRaw) === binding.projectSha256, "HTTP_CONFIRMATION_SOURCE_BINDING");
    const sources = {} as Loaded["sources"];
    for (const role of roles) {
        const raw = await bytes(join(root, "project", role + (role === "settings" ? ".original" : "") + ".json"));
        need(
            raw.length === binding.sourceSnapshots[role].bytes && sha(raw) === binding.sourceSnapshots[role].sha256,
            "HTTP_CONFIRMATION_SOURCE_BINDING",
        );
        sources[role] = {
            raw,
            path: binding.sourceSnapshots[role].path,
            sha256: sha(raw),
            value: JSON.parse(raw.toString("utf8")),
        };
    }
    const loaded = {
        project,
        projectRaw,
        projectPath: join(root, "project/project.json"),
        sources,
        target: httpEnvironmentSettingsSchema.parse({
            ...sources.settings.value,
            mode: sources.settings.value.mode ?? "forward",
        }),
        runtime: sources.runtime.value,
    };
    need(
        encode(normalizedSettings(loaded)).equals(await bytes(join(root, "project/settings.json"))),
        "HTTP_CONFIRMATION_SOURCE_BINDING",
    );
    const changes = await resourceChanges(source, loaded),
        selection = selectCapacityConfirmation(
            source.plan as CapacityConfirmationSelectionSource,
            source.report,
            request.id,
        );
    validateBudgets(source, request, selection.comparisons);
    const suite = await inspectHttpSuite(),
        points: Child[] = [];
    need(
        isDeepStrictEqual(source.plan.suite, {
            suiteId: suite.suiteId,
            version: suite.version,
            bundleSha256: suite.bundleSha256,
            caseIds: suite.cases.map((c) => c.id),
        }),
        "HTTP_CONFIRMATION_SUITE",
    );
    const shared = Object.fromEntries(
        (["build", "settings", "environment"] as const).map((role) => [
            role,
            {
                path: "inputs/" + role + ".json",
                sha256: sha(role === "settings" ? encode(normalizedSettings(loaded)) : sources[role].raw),
            },
        ]),
    ) as Record<"build" | "settings" | "environment", { path: string; sha256: string }>;
    const originalShared = Object.fromEntries(
        Object.entries(shared).map(([role, entry]) => [
            role,
            { ...entry, path: join(binding.planningDirectory, "project", role + ".json") },
        ]),
    ) as typeof shared;
    for (const condition of selection.comparisons) {
        const prefix = "points/" + condition.id,
            comparisonDirectory = prefix + "/comparison",
            profileFile = prefix + "/profile.json",
            childRoot = join(root, comparisonDirectory),
            child = await httpComparisonPlan(["--verify", childRoot]),
            sourceComparison = await originalComparison(source, condition),
            expectedSettings = settingsFor(source, loaded, binding, condition);
        need(
            isDeepStrictEqual(
                await json(join(root, prefix, "comparison-request.json")),
                capacityConfirmationComparisonRequest(
                    source.plan.policy,
                    condition,
                    request.budgets.point,
                    originalShared,
                    sourceComparison,
                    request.schemaVersion === 2 ? request.budgetMode : undefined,
                ),
            ) &&
                isDeepStrictEqual(
                    await json(join(childRoot, "request.json")),
                    capacityConfirmationComparisonRequest(
                        source.plan.policy,
                        condition,
                        request.budgets.point,
                        shared,
                        sourceComparison,
                        request.schemaVersion === 2 ? request.budgetMode : undefined,
                    ),
                ),
            "HTTP_CONFIRMATION_CHILD_SEMANTICS",
        );
        for (const role of ["build", "settings", "environment"] as const)
            need(
                (await bytes(join(childRoot, "inputs", role + ".json"))).equals(
                    role === "settings" ? encode(normalizedSettings(loaded)) : sources[role].raw,
                ),
                "HTTP_CONFIRMATION_SOURCE_BINDING",
            );
        need(
            (await bytes(join(childRoot, "target/000.json"))).equals(bindingRaw) &&
                isDeepStrictEqual(await json(join(root, prefix, "profile.settings.json")), expectedSettings),
            "HTTP_CONFIRMATION_SOURCE_BINDING",
        );
        const { caseIds, purpose, ...base } = expectedSettings;
        const expectedProfile = {
            ...base,
            kind: "http-benchmark-profile",
            serviceChecks: true,
            cases: [suite.cases.find((c) => c.id === condition.caseId)],
            benchmark: {
                suiteId: suite.suiteId,
                version: suite.version,
                bundleSha256: suite.bundleSha256,
                caseIds,
                coverage: "subset",
                purpose,
            },
        };
        need(
            isDeepStrictEqual(await json(join(root, profileFile)), expectedProfile) &&
                isDeepStrictEqual(await json(join(childRoot, "inputs/profile.source.json")), expectedProfile),
            "HTTP_CONFIRMATION_PROFILE_DRIFT",
        );
        const childPlan = await json(join(childRoot, "plan.json"));
        points.push({
            ...condition,
            comparisonDirectory,
            profileFile,
            comparisonPlanDigest: String(child.planDigest),
            comparisonContentDigest: String(child.contentDigest),
            estimate: child.estimate as Record<string, any>,
            samplingBounds: child.samplingBounds,
            workflowMaxMilliseconds: childPlan.budgets.workflowMaxMilliseconds,
        });
    }
    const plan = derived(source, request, binding, loaded, selection, changes, points);
    need(isDeepStrictEqual(await json(join(root, "confirmation-plan.json")), plan), "HTTP_CONFIRMATION_PLAN_SEMANTICS");
    need(
        actual.reduce((n, f) => n + f.bytes, 0) + (await bytes(join(root, "confirmation.lock.json"))).length <=
            request.budgets.study.maxPlanBytes,
        "HTTP_CONFIRMATION_PLAN_BUDGET",
    );
    need(
        isDeepStrictEqual(
            (await inventory(root, 256 * MiB)).filter((f) => f.path !== "confirmation.lock.json"),
            actual,
        ) && (await bytes(join(root, "confirmation.lock.json"))).equals(lockRaw),
        "HTTP_CONFIRMATION_PLAN_CHANGED",
    );
    return {
        directory: root,
        planDigest: plan.planDigest,
        contentDigest: lock.contentDigest,
        plan,
        points,
        valid: true,
        executionReady: false,
        executed: false,
        performanceAccepted: false,
        ...(options?.includeSourceIndependence ? { sourceIndependence: source.sourceIndependence } : {}),
    };
}
