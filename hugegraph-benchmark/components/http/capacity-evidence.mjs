/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
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
import { open, mkdir, writeFile, realpath, lstat, readdir, mkdtemp, rm } from "node:fs/promises";
import { basename, dirname, join, resolve, sep } from "node:path";
import { tmpdir } from "node:os";
import { fileURLToPath, pathToFileURL } from "node:url";
import { createHash } from "node:crypto";
import { spawn } from "node:child_process";
import { isDeepStrictEqual } from "node:util";
import { performance } from "node:perf_hooks";
import { verifyHttpCapacityPlan } from "../../dist/http-capacity.js";
import { verifyHttpCapacityConfirmationPlan } from "../../dist/http-capacity-confirmation.js";
import { normalizeHttpEvidence } from "./paired-evidence.mjs";
import { capacityHtml } from "./capacity-report.mjs";
import { markConfirmationIndependence, summarizeCapacityConfirmation } from "./capacity-confirmation-report.mjs";
import { projectCapacityBoundary } from "./capacity-boundary.mjs";

const MiB = 1024 ** 2,
    ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "../..");
const SELF = fileURLToPath(import.meta.url),
    SHA = /^[a-f0-9]{64}$/;
export const CAPACITY_REPORT_LIMITS = Object.freeze({
    minimumMilliseconds: 1000,
    maximumMilliseconds: 86400000,
    minimumOutputBytes: MiB,
    maximumOutputBytes: 8 * 1024 ** 3,
    manifestBytes: MiB,
    pointOutputBytes: 32 * MiB,
    commandOutputBytes: 256 * 1024,
    points: 352,
    cleanupMilliseconds: 3000,
});
const encode = (v) => Buffer.from(JSON.stringify(v, null, 2) + "\n");
const hash = (v) => createHash("sha256").update(v).digest("hex");
const need = (ok, code) => {
    if (!ok) throw Object.assign(new Error(code), { code });
};
const exact = (v, keys) =>
    need(
        v && typeof v === "object" && !Array.isArray(v) && isDeepStrictEqual(Object.keys(v).sort(), [...keys].sort()),
        "CAPACITY_REPORT_FIELDS",
    );
const median = (v) => {
    const a = [...v].sort((a, b) => a - b);
    return a.length ? (a.length % 2 ? a[a.length >> 1] : (a[a.length / 2 - 1] + a[a.length / 2]) / 2) : null;
};
const finite = (v) => typeof v === "number" && Number.isFinite(v) && v >= 0;

export function validateCapacityReportLimits(timeoutMs, maxBytes) {
    need(
        Number.isSafeInteger(timeoutMs) &&
            timeoutMs >= CAPACITY_REPORT_LIMITS.minimumMilliseconds &&
            timeoutMs <= CAPACITY_REPORT_LIMITS.maximumMilliseconds,
        "CAPACITY_REPORT_TIME_LIMIT",
    );
    need(
        Number.isSafeInteger(maxBytes) &&
            maxBytes >= CAPACITY_REPORT_LIMITS.minimumOutputBytes &&
            maxBytes <= CAPACITY_REPORT_LIMITS.maximumOutputBytes,
        "CAPACITY_REPORT_BYTE_LIMIT",
    );
    return { timeoutMs, maxBytes };
}
async function bounded(path, cap = 8 * MiB) {
    const fd = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
    try {
        const a = await fd.stat();
        need(a.isFile() && a.size <= cap, "CAPACITY_REPORT_INPUT_FILE");
        const raw = Buffer.alloc(a.size + 1);
        let n = 0;
        while (n < raw.length) {
            const r = await fd.read(raw, n, raw.length - n, n);
            if (!r.bytesRead) break;
            n += r.bytesRead;
        }
        const b = await fd.stat();
        need(
            n === a.size && b.size === a.size && b.mtimeMs === a.mtimeMs && b.ctimeMs === a.ctimeMs,
            "CAPACITY_REPORT_INPUT_CHANGED",
        );
        return raw.subarray(0, n);
    } finally {
        await fd.close();
    }
}
const json = async (path, cap) => JSON.parse(new TextDecoder("utf8", { fatal: true }).decode(await bounded(path, cap)));
const save = async (path, value) => writeFile(path, encode(value), { flag: "wx", mode: 0o600 });

export function validateCapacityExecutionSet(value, plan) {
    exact(value, ["schemaVersion", "kind", "planDigest", "points"]);
    need(
        value.schemaVersion === 1 &&
            value.kind === "http-capacity-execution-set" &&
            value.planDigest === plan.planDigest &&
            Array.isArray(value.points) &&
            value.points.length <= plan.points.length,
        "CAPACITY_EXECUTION_SET_IDENTITY",
    );
    const seen = new Set();
    for (const point of value.points) {
        exact(point, ["id", "executionDirectory"]);
        need(
            plan.points.some((p) => p.id === point.id) &&
                !seen.has(point.id) &&
                typeof point.executionDirectory === "string" &&
                point.executionDirectory.length > 0 &&
                point.executionDirectory.length <= 4096 &&
                !point.executionDirectory.includes("\0"),
            "CAPACITY_EXECUTION_SET_POINT",
        );
        seen.add(point.id);
    }
    return value;
}

function latencyMetric(raw, minimum, p99Minimum) {
    const count = Number.isSafeInteger(raw?.count) && raw.count >= 0 ? raw.count : null;
    return {
        count,
        observedNearestRank: { p50: raw?.p50 ?? null, p95: raw?.p95 ?? null, p99: raw?.p99 ?? null },
        ...Object.fromEntries(
            ["p50", "p95", "p99"].map((q) => {
                const required = q === "p99" ? p99Minimum : minimum;
                const eligible = count !== null && count >= required && finite(raw?.[q]);
                return [
                    q,
                    {
                        status: eligible ? "descriptive" : "unknown",
                        value: eligible ? raw[q] : null,
                        minimumObservations: required,
                    },
                ];
            }),
        ),
    };
}

export function capacityArm(point, row, policy) {
    const raw = row?.httpSummary,
        runs = raw?.runs ?? [],
        selected = runs.filter(
            (r) =>
                r.caseId === point.caseId &&
                r.model === "open" &&
                r.repeat === 0 &&
                r.ratePerSecond === point.ratePerSecond,
        );
    const run = selected.length === 1 && runs.length === 1 ? selected[0] : null;
    const evidence = {
        raw: row?.rawVerified === true,
        environment: row?.runtimeArtifactsConsistent === true,
        driverLifecycle: row?.driverVerified === true,
        resources: row?.resourcesVerified === true,
    };
    const issues = [...(row?.issues ?? [])];
    if (!run) issues.push("CAPACITY_HTTP_RUN_MISSING_OR_AMBIGUOUS");
    const correctInWindow = run ? Math.round((run.correctCompletedInWindowPerSecond * run.durationMs) / 1000) : null;
    if (run) {
        issues.push(...run.issues);
        if (correctInWindow < policy.analysis.goodput.minimumObservations)
            issues.push("CAPACITY_GOODPUT_OBSERVATIONS_INSUFFICIENT");
        if (row.expectedMode !== point.mode) issues.push("CAPACITY_POINT_MODE_DIFFER");
    }
    const rawCountVerified = Boolean(
        run &&
        evidence.raw &&
        raw?.gates?.sampleAccounting === "passed" &&
        raw?.gates?.correctness === "passed" &&
        row.expectedMode === point.mode &&
        finite(run.correctCompletedInWindowPerSecond),
    );
    const valid =
        row?.status === "ok" &&
        Object.values(evidence).every(Boolean) &&
        issues.length === 0 &&
        raw?.gates?.correctness === "passed" &&
        run &&
        finite(run.correctCompletedInWindowPerSecond);
    const counts = run
        ? {
              scheduled: run.scheduled,
              sent: run.sent,
              completed: run.completed,
              correct: run.correct,
              failed: run.failed,
              dropped: run.dropped,
              late: run.late,
              outcomes: run.counts,
              correctCompletedInWindow: correctInWindow,
              correctCompletedAfterWindow: run.correct - correctInWindow,
          }
        : null;
    const ratio = (a, b) => (Number.isFinite(a) && Number.isFinite(b) && b > 0 ? a / b : null);
    const timeouts = run ? (run.counts?.timeout ?? 0) + (run.counts?.["drain-timeout"] ?? 0) : null;
    const latency = latencyMetric(
        run?.correctEndToEndMs,
        policy.analysis.latency.minimumObservations,
        policy.tailPolicy.p99MinimumObservations,
    );
    const slo =
        policy.slo === null
            ? { status: "not-requested" }
            : {
                  status: "unknown",
                  latency:
                      latency[policy.slo.endToEndQuantile].value === null
                          ? "unknown"
                          : latency[policy.slo.endToEndQuantile].value <= policy.slo.maxLatencyMs
                            ? "within-threshold"
                            : "above-threshold",
                  errorRate: "unknown",
                  timeoutRate: "unknown",
                  reason: "rate-denominator-not-declared-in-frozen-policy",
                  capacity: "unknown",
                  policy: policy.slo,
              };
    return {
        runId: row?.runId ?? null,
        arm: row?.arm ?? null,
        blockId: row?.blockId ?? null,
        status: row?.status ?? "missing",
        measurementValid: Boolean(valid),
        rawCountVerified,
        evidence,
        rawManifestSha256: raw?.rawManifestSha256 ?? null,
        correctCompletedInWindowPerSecond: run?.correctCompletedInWindowPerSecond ?? null,
        latency,
        counts,
        durationMs: run?.durationMs ?? null,
        elapsedWithDrainMs: run?.elapsedWithDrainMs ?? null,
        rates: run
            ? {
                  offered: run.offeredPerSecond,
                  admitted: run.admittedPerSecond,
                  sentErrorFraction: ratio(run.failed, run.sent),
                  scheduledErrorOrDropFraction: ratio(run.failed + run.dropped, run.scheduled),
                  sentTimeoutFraction: ratio(timeouts, run.sent),
                  scheduledTimeoutFraction: ratio(timeouts, run.scheduled),
              }
            : null,
        driver: run?.driver ?? null,
        driverPressureObserved: run ? run.issues.some((s) => s.includes("driver-pressure")) : null,
        headroom: "unknown",
        warmupStable: null,
        observerCost: "unknown",
        slo,
        issues: [...new Set(issues)],
        capacityQualified: false,
    };
}

/** Internal pure projection: public report obtains these inputs only from the independently reverified point worker. */
export function summarizeCapacity(plan, records) {
    need(
        plan.kind === "http-capacity-plan" &&
            plan.phase === "exploration" &&
            plan.points.length >= 11 &&
            plan.points.length <= CAPACITY_REPORT_LIMITS.points,
        "CAPACITY_REPORT_PLAN_SCOPE",
    );
    const single = plan.policy.schemaVersion === 2;
    if (single)
        need(
            plan.schemaVersion === 2 &&
                plan.policy.exploration.mode === "single-run" &&
                plan.policy.exploration.runs === 1,
            "CAPACITY_REPORT_OBSERVATION_DESIGN",
        );
    const points = plan.points.map((point) => {
        const record = records.find((r) => r.id === point.id),
            verification = record?.result?.verification;
        const rows = verification?.rows ?? [];
        const arms = Array.from({ length: point.expectedArms }, (_, i) => capacityArm(point, rows[i], plan.policy));
        const issues = [
            ...(record?.issues ?? (record ? [] : ["CAPACITY_POINT_NOT_STARTED"])),
            ...(verification?.globalIssues ?? []),
        ];
        if (point.expectedArms !== (single ? 1 : 4)) issues.push("CAPACITY_OBSERVATION_DESIGN_DIFFER");
        if (
            single &&
            rows.length &&
            (verification?.kind !== "http-observation-evidence-verification" ||
                verification?.design !== "single-observation" ||
                rows.some((row) => row.arm !== "O"))
        )
            issues.push("CAPACITY_OBSERVATION_DESIGN_DIFFER");
        if (rows.length !== point.expectedArms) issues.push("CAPACITY_ARM_DENOMINATOR_INCOMPLETE");
        if (rows.length && arms.some((arm) => !arm.measurementValid)) issues.push("CAPACITY_ARM_MEASUREMENT_INVALID");
        const measurementValid =
            verification?.valid === true &&
            rows.length === point.expectedArms &&
            arms.every((a) => a.measurementValid) &&
            issues.length === 0;
        const values = arms.map((a) => a.correctCompletedInWindowPerSecond),
            throughput = measurementValid
                ? {
                      median: median(values),
                      minimum: Math.min(...values),
                      maximum: Math.max(...values),
                      aggregation: single
                          ? "single-observation-correct-completion-rate; unconfirmed"
                          : "median-of-all-predeclared-arm-correct-completion-rates; no pooling",
                  }
                : {
                      median: null,
                      minimum: null,
                      maximum: null,
                      aggregation: "unavailable-incomplete-or-invalid-point",
                  };
        const diagnosticObservedThroughput =
            rows.length === point.expectedArms && arms.every((a) => a.rawCountVerified)
                ? {
                      status: "diagnostic-only",
                      qualification: false,
                      median: median(values),
                      minimum: Math.min(...values),
                      maximum: Math.max(...values),
                      aggregation: single
                          ? "single-observation-correct-completion-rate; unconfirmed"
                          : "median-of-all-predeclared-arm-correct-completion-rates; no pooling",
                      issues: [...new Set([...issues, ...arms.flatMap((a) => a.issues)])],
                  }
                : {
                      status: "unavailable",
                      qualification: false,
                      median: null,
                      minimum: null,
                      maximum: null,
                      reason: "incomplete-or-unverified-raw-correct-counts",
                  };
        const latency = Object.fromEntries(
            ["p50", "p95", "p99"].map((q) => {
                const enough = measurementValid && arms.every((a) => a.latency[q].value !== null);
                return [
                    q,
                    {
                        status: enough ? "descriptive" : "unknown",
                        value: enough ? median(arms.map((a) => a.latency[q].value)) : null,
                        aggregation: single
                            ? "single-observation-request-quantile"
                            : "median-of-arm-quantiles; not-a-pooled-request-quantile",
                    },
                ];
            }),
        );
        return {
            id: point.id,
            index: point.index,
            caseId: point.caseId,
            mode: point.mode,
            ratePerSecond: point.ratePerSecond,
            comparisonPlanDigest: point.comparisonPlanDigest,
            executionDirectory: record?.executionDirectory ?? null,
            status: record?.status ?? "not-started",
            measurementValid,
            capacityQualified: false,
            issues: [...new Set(issues)],
            expectedArms: point.expectedArms,
            arms,
            throughput,
            diagnosticObservedThroughput,
            latency,
            slo: plan.policy.slo === null ? "not-requested" : "unknown",
            headroom: "unknown",
            warmupStable: null,
            observerCost: "unknown",
            confirmation: "not-implemented",
        };
    });
    const groups = [];
    for (const caseId of plan.suite.caseIds)
        for (const mode of plan.modes) {
            const rows = points.filter((p) => p.caseId === caseId && p.mode === mode);
            const candidates = rows
                .filter((p) => p.measurementValid)
                .sort((a, b) => b.throughput.median - a.throughput.median || a.ratePerSecond - b.ratePerSecond);
            const highest = candidates[0];
            groups.push({
                caseId,
                mode,
                pointIds: rows.map((p) => p.id),
                highestObserved: highest
                    ? {
                          status: "unconfirmed",
                          pointId: highest.id,
                          ratePerSecond: highest.ratePerSecond,
                          correctRequestsPerSecond: highest.throughput.median,
                          aggregation: highest.throughput.aggregation,
                      }
                    : {
                          status: "unavailable",
                          pointId: null,
                          correctRequestsPerSecond: null,
                          reason: "no-complete-valid-point",
                      },
                confirmedHighest: null,
                capacityQualified: false,
                sloCapacity: plan.policy.slo === null ? { status: "not-requested" } : { status: "unknown" },
            });
        }
    return {
        schemaVersion: 1,
        kind: "http-capacity-report",
        algorithm: single ? "capacity-curves-v2-single-run" : "capacity-curves-v1",
        phase: plan.phase,
        planDigest: plan.planDigest,
        policySha256: plan.policySha256,
        suite: plan.suite,
        modes: plan.modes,
        plannedPoints: plan.points.length,
        expectedArms: plan.expectedArms,
        validPoints: points.filter((p) => p.measurementValid).length,
        evidenceValid: points.every((p) => p.measurementValid),
        points,
        curves: groups,
        engineeringAccepted: false,
        statisticsCertified: false,
        performanceAccepted: false,
        limits: [
            "Exploration alone is unconfirmed; task success is not evidence validity",
            "No pooled request quantiles, mixed workload score or summed database QPS",
            "No independent driver headroom, stable warmup or observer-cost acceptance",
            "SLO rate denominators are undeclared; no SLO capacity or attributable upper boundary is established",
            single
                ? "One observation per point; no paired inference, confirmation or study-wide certification"
                : "Per-point paired statistics remain descriptive; no study-wide false-positive certification",
        ],
    };
}

async function implementation(check = () => {}) {
    const files = [];
    let total = 0;
    async function walk(folder, depth = 0) {
        need(depth <= 16, "CAPACITY_REPORT_SOURCE_BUDGET");
        for (const name of (await readdir(join(ROOT, folder))).sort()) {
            check();
            const path = join(folder, name),
                info = await lstat(join(ROOT, path));
            need(!info.isSymbolicLink(), "CAPACITY_REPORT_SOURCE_TYPE");
            if (info.isDirectory()) await walk(path, depth + 1);
            else {
                need(info.isFile(), "CAPACITY_REPORT_SOURCE_TYPE");
                need(files.length < 4096, "CAPACITY_REPORT_SOURCE_BUDGET");
                const raw = await bounded(join(ROOT, path));
                total += raw.length;
                need(total <= 32 * MiB, "CAPACITY_REPORT_SOURCE_BUDGET");
                files.push({ path, bytes: raw.length, sha256: hash(raw) });
            }
        }
    }
    for (const path of ["components", "src", "dist", "release", "benchmarks", "presets"]) await walk(path);
    need(files.length < 4096, "CAPACITY_REPORT_SOURCE_BUDGET");
    const raw = await bounded(join(ROOT, "package.json"));
    total += raw.length;
    need(total <= 32 * MiB, "CAPACITY_REPORT_SOURCE_BUDGET");
    files.push({ path: "package.json", bytes: raw.length, sha256: hash(raw) });
    files.sort((a, b) => (a.path < b.path ? -1 : a.path > b.path ? 1 : 0));
    return {
        scope: "physical-local-components-src-dist-release-benchmarks-presets-and-package; installed-dependency-bytes-not-attested",
        files,
        contentDigest: hash(encode(files)),
    };
}

export async function runCapacityWorker(argv, milliseconds, signal) {
    // Confirmation verification owns an inner source-report worker with a 3s group cleanup.
    // Its owner must remain alive long enough to reap that independently detached group.
    const cleanupMilliseconds =
        argv[0] === "__verify-confirmation-plan" ? 7000 : CAPACITY_REPORT_LIMITS.cleanupMilliseconds;
    return await new Promise((resolveResult) => {
        const child = spawn(process.execPath, [SELF, ...argv], {
            detached: true,
            stdio: ["ignore", "pipe", "pipe"],
            env: {
                PATH: process.env.PATH || "/usr/bin:/bin",
                ...(process.env.HGBENCH_PYTHON ? { HGBENCH_PYTHON: process.env.HGBENCH_PYTHON } : {}),
            },
        });
        let size = 0,
            failure = null,
            killer,
            stopAt = null;
        const stdout = [],
            stderr = [];
        const group = (s) => {
            try {
                process.kill(-child.pid, s);
            } catch (e) {
                if (e.code !== "ESRCH") failure = "CAPACITY_REPORT_GROUP_SIGNAL";
            }
        };
        const groupAlive = () => {
            try {
                process.kill(-child.pid, 0);
                return true;
            } catch (e) {
                return e.code !== "ESRCH";
            }
        };
        const stop = (reason) => {
            failure ||= reason;
            if (stopAt === null) {
                stopAt = performance.now();
                group("SIGTERM");
                killer = setTimeout(() => group("SIGKILL"), cleanupMilliseconds);
            }
        };
        const abort = () => stop("CAPACITY_REPORT_CANCELLED");
        signal.addEventListener("abort", abort, { once: true });
        const timer = setTimeout(() => stop("CAPACITY_REPORT_WORKER_TIMEOUT"), milliseconds);
        for (const [stream, chunks] of [
            [child.stdout, stdout],
            [child.stderr, stderr],
        ])
            stream.on("data", (chunk) => {
                size += chunk.length;
                if (size > CAPACITY_REPORT_LIMITS.commandOutputBytes) stop("CAPACITY_REPORT_COMMAND_OUTPUT");
                else chunks.push(chunk);
            });
        child.once("error", () => {
            failure = "CAPACITY_REPORT_SPAWN";
        });
        child.once("exit", () => {
            // Descendants may keep inherited pipes open after the leader exits.
            // Terminate that owned group now rather than waiting for the whole worker deadline.
            if (child.pid && groupAlive()) stop("CAPACITY_REPORT_RESIDUAL_PROCESS_GROUP");
        });
        child.once("close", async (code, terminationSignal) => {
            clearTimeout(timer);
            signal.removeEventListener("abort", abort);
            // Reap the leader first. A closed leader must not cancel cleanup of its still-live group.
            if (child.pid && groupAlive()) {
                stop("CAPACITY_REPORT_RESIDUAL_PROCESS_GROUP");
                const grace = Math.max(0, cleanupMilliseconds - (performance.now() - stopAt));
                if (grace > 0) await new Promise((done) => setTimeout(done, grace));
                group("SIGKILL");
            }
            clearTimeout(killer);
            resolveResult({
                code,
                signal: terminationSignal,
                failure,
                stdout: Buffer.concat(stdout).toString("utf8"),
                stderr: Buffer.concat(stderr).toString("utf8"),
            });
        });
        if (signal.aborted) abort();
    });
}

async function canonical(path) {
    const suffix = [];
    let current = resolve(path);
    while (true) {
        try {
            return join(await realpath(current), ...suffix.reverse());
        } catch (error) {
            if (error.code !== "ENOENT") throw error;
            suffix.push(basename(current));
            const parent = dirname(current);
            need(parent !== current, "CAPACITY_REPORT_CANONICAL_PATH");
            current = parent;
        }
    }
}
export function assertCapacityOutputSeparate(output, inputs) {
    const inside = (a, b) => a === b || a.startsWith(b + sep);
    need(
        inputs.every((input) => !inside(output, input) && !inside(input, output)),
        "CAPACITY_REPORT_OUTPUT_OVERLAP",
    );
}
export function markCapacityAssociations(records) {
    const fields = ["actualDirectory", "executionId"];
    for (const field of fields) {
        const seen = new Map();
        for (const record of records) {
            if (!record[field]) continue;
            if (seen.has(record[field])) {
                record.issues.push("CAPACITY_EXECUTION_REUSED");
                seen.get(record[field]).issues.push("CAPACITY_EXECUTION_REUSED");
            } else seen.set(record[field], record);
        }
    }
    const ids = new Map();
    for (const record of records)
        for (const id of record.runIds ?? []) {
            if (ids.has(id)) {
                record.issues.push("CAPACITY_RUN_REUSED");
                ids.get(id).issues.push("CAPACITY_RUN_REUSED");
            } else ids.set(id, record);
        }
    for (let i = 0; i < records.length; i++)
        for (let j = i + 1; j < records.length; j++) {
            const a = records[i],
                b = records[j];
            if (!a.window || !b.window || a.window.boot !== b.window.boot) continue;
            if (BigInt(a.window.end) > BigInt(b.window.start)) {
                const issue =
                    BigInt(b.window.end) > BigInt(a.window.start)
                        ? "CAPACITY_EXECUTION_WINDOWS_OVERLAP"
                        : "CAPACITY_EXECUTION_ORDER_DIFFER";
                a.issues.push(issue);
                b.issues.push(issue);
            }
        }
    return records;
}
function compactResult(result) {
    return {
        verification: {
            valid: result.verification.valid,
            ...(result.verification.design === "single-observation"
                ? {
                      kind: result.verification.kind,
                      design: result.verification.design,
                  }
                : {}),
            globalIssues: result.verification.globalIssues ?? [],
            rows: result.verification.rows.map((row) => ({
                ...Object.fromEntries(
                    [
                        "runId",
                        "arm",
                        "blockId",
                        "expectedMode",
                        "status",
                        "issues",
                        "rawVerified",
                        "rawEvidenceSha256",
                        "runtimeArtifactsConsistent",
                        "driverVerified",
                        "resourcesVerified",
                    ].map((key) => [key, row[key]]),
                ),
                ...(row.httpSummary
                    ? {
                          httpSummary: {
                              ...row.httpSummary,
                              runs: row.httpSummary.runs.map(({ phaseClock, ...run }) => run),
                          },
                      }
                    : {}),
            })),
        },
    };
}

/** Called only on this replay's freshly sealed report, while its source-review owner/deadline is active. */
export async function collectCapacitySourceIndependence({ plan, inputFile, reportDirectory, maxBytes, check }) {
    check();
    const inputRaw = await bounded(inputFile, MiB),
        input = validateCapacityExecutionSet(JSON.parse(inputRaw), plan),
        reportLockRaw = await bounded(join(reportDirectory, "evidence.lock.json")),
        reportLock = JSON.parse(reportLockRaw),
        reportRaw = await bounded(join(reportDirectory, "report.json")),
        summary = JSON.parse(reportRaw);
    need(
        reportLock.kind === "http-capacity-report-lock" &&
            reportLock.planDigest === plan.planDigest &&
            hash(encode(reportLock.files)) === reportLock.contentDigest &&
            summary.planDigest === plan.planDigest &&
            summary.executionSetSha256 === hash(inputRaw),
        "CONFIRMATION_SOURCE_REPLAY_IDENTITY",
    );
    const executions = [],
        unavailable = [],
        guards = [];
    let readBytes = reportLockRaw.length + reportRaw.length + inputRaw.length;
    for (const item of input.points) {
        check();
        try {
            const point = plan.points.find((p) => p.id === item.id),
                relative = `points/${point.id}.json`,
                locked = reportLock.files.find((file) => file.path === relative);
            need(locked && locked.bytes <= 32 * MiB, "CONFIRMATION_SOURCE_POINT_PROOF_UNAVAILABLE");
            const pointRaw = await bounded(join(reportDirectory, relative), Math.min(32 * MiB, maxBytes));
            readBytes += pointRaw.length;
            need(
                pointRaw.length === locked.bytes && hash(pointRaw) === locked.sha256,
                "CONFIRMATION_SOURCE_REPLAY_CHANGED",
            );
            const proof = JSON.parse(pointRaw).verification;
            need(
                proof.planDigest === point.comparisonPlanDigest &&
                    SHA.test(proof.executionLockSha256) &&
                    Array.isArray(proof.rows) &&
                    proof.rows.length === point.expectedArms &&
                    proof.rows.every(
                        (row) =>
                            row.rawVerified === true &&
                            SHA.test(row.rawEvidenceSha256) &&
                            row.runtimeArtifactsConsistent === true &&
                            row.driverVerified === true &&
                            row.resourcesVerified === true,
                    ),
                "CONFIRMATION_SOURCE_POINT_PROOF_UNAVAILABLE",
            );
            const actualDirectory = await realpath(resolve(dirname(inputFile), item.executionDirectory)),
                lockRaw = await bounded(join(actualDirectory, "execution.lock.json")),
                lock = JSON.parse(lockRaw);
            need(
                hash(lockRaw) === proof.executionLockSha256 && Array.isArray(lock.files),
                "CONFIRMATION_SOURCE_EXECUTION_CHANGED",
            );
            const raw = await bounded(join(actualDirectory, "execution.json")),
                terminalRaw = await bounded(join(actualDirectory, "terminal.json"));
            readBytes += lockRaw.length + raw.length + terminalRaw.length;
            for (const [path, data] of [
                ["execution.json", raw],
                ["terminal.json", terminalRaw],
            ]) {
                const member = lock.files.find((file) => file.path === path);
                need(
                    member?.bytes === data.length && member.sha256 === hash(data),
                    "CONFIRMATION_SOURCE_EXECUTION_CHANGED",
                );
            }
            const execution = JSON.parse(raw),
                terminal = JSON.parse(terminalRaw);
            need(
                execution.planDigest === point.comparisonPlanDigest &&
                    /^[a-f0-9]{32}$/.test(execution.executionId) &&
                    terminal.executionId === execution.executionId &&
                    terminal.planDigest === point.comparisonPlanDigest &&
                    typeof execution.hostBootId === "string" &&
                    execution.hostBootId.length > 0 &&
                    /^(0|[1-9][0-9]{0,18})$/.test(execution.registeredMonotonicNs) &&
                    /^(0|[1-9][0-9]{0,18})$/.test(terminal.finishedMonotonicNs) &&
                    BigInt(terminal.finishedMonotonicNs) >= BigInt(execution.registeredMonotonicNs) &&
                    Array.isArray(execution.receipts) &&
                    execution.receipts.length <= 4 &&
                    execution.receipts.every((r) => typeof r.runId === "string" && r.runId.length <= 128),
                "CONFIRMATION_SOURCE_TERMINAL_UNPROVEN",
            );
            executions.push({
                pointId: point.id,
                actualDirectory,
                executionId: execution.executionId,
                executionLockSha256: hash(lockRaw),
                runIds: execution.receipts.map((r) => r.runId),
                rawManifestSha256s: proof.rows.map((r) => r.rawEvidenceSha256).filter((value) => SHA.test(value)),
                window: {
                    boot: execution.hostBootId,
                    start: execution.registeredMonotonicNs,
                    end: terminal.finishedMonotonicNs,
                },
            });
            for (const [path, data] of [
                ["execution.lock.json", lockRaw],
                ["execution.json", raw],
                ["terminal.json", terminalRaw],
            ])
                guards.push({ path: join(actualDirectory, path), bytes: data.length, sha256: hash(data) });
        } catch (error) {
            unavailable.push({ pointId: item.id, reason: error.code || "CONFIRMATION_SOURCE_PROOF_UNAVAILABLE" });
        }
        need(readBytes <= maxBytes + 8 * MiB, "CONFIRMATION_SOURCE_RECEIPT_BUDGET");
    }
    check();
    const inputDirectories = await Promise.all(
        input.points.map((item) => canonical(resolve(dirname(inputFile), item.executionDirectory))),
    );
    const result = {
        schemaVersion: 1,
        kind: "http-capacity-source-independence",
        planDigest: plan.planDigest,
        executionSetSha256: hash(inputRaw),
        executions,
        unavailable,
        guards,
        inputDirectories,
        scope: "fresh-independent-exploration-replay-and-locked-controller-headers; no-cryptographic-attestation",
    };
    need(encode(result).length <= MiB, "CONFIRMATION_SOURCE_RECEIPT_BUDGET");
    return result;
}

export async function reportCapacityConfirmation(options) {
    return reportCapacityForPhase(options, true);
}

export async function verifyCapacityBoundary({ planDirectory, pointId, executionDirectory }) {
    const verified = await verifyHttpCapacityPlan(planDirectory),
        plan = verified.plan,
        point = plan.points.find((row) => row.id === pointId);
    need(
        plan.phase === "exploration" && point && plan.policy.continuation?.mode === "verified-case-range",
        "BOUNDARY_PREREGISTRATION",
    );
    const root = await realpath(executionDirectory),
        lockRaw = await bounded(join(root, "execution.lock.json")),
        lock = JSON.parse(lockRaw),
        childRoot = join(await realpath(planDirectory), point.comparisonDirectory),
        result = await normalizeHttpEvidence({
            planDirectory: childRoot,
            executionDirectory: root,
            includeHttpSummaries: true,
        });
    need(hash(lockRaw) === result.verification.executionLockSha256, "BOUNDARY_EXECUTION_CHANGED");
    const decision = projectCapacityBoundary({ point, policy: plan.policy, verification: result.verification });
    const ownedContainers = [];
    if (decision.action !== "stop-study") {
        for (const row of result.verification.rows)
            for (const [role, path] of [
                ["driver", `arms/${row.runId}/controller/driver-final.json`],
                ["target", `arms/${row.runId}/environment/container-final.json`],
            ]) {
                const raw = await bounded(join(root, path)),
                    member = lock.files.find((file) => file.path === path),
                    info = JSON.parse(raw);
                need(
                    member?.bytes === raw.length &&
                        member.sha256 === hash(raw) &&
                        SHA.test(info.Id) &&
                        info.State?.Running === false &&
                        info.State.Status === "exited" &&
                        info.State.ExitCode === 0 &&
                        info.State.OOMKilled === false &&
                        !ownedContainers.some((prior) => prior.id === info.Id),
                    "BOUNDARY_OWNED_IDENTITY",
                );
                ownedContainers.push({ runId: row.runId, role, id: info.Id, imageId: info.Image });
            }
    }
    need(hash(await bounded(join(root, "execution.lock.json"))) === hash(lockRaw), "BOUNDARY_EXECUTION_CHANGED");
    const after = await verifyHttpCapacityPlan(planDirectory);
    need(
        after.contentDigest === verified.contentDigest && after.planDigest === verified.planDigest,
        "BOUNDARY_PLAN_CHANGED",
    );
    return {
        ...decision,
        capacityPlanDigest: plan.planDigest,
        capacityContentDigest: verified.contentDigest,
        ownedContainers,
    };
}
export async function reportCapacity({
    planDirectory,
    inputFile,
    outputDirectory,
    timeoutMs,
    maxBytes,
    signal: externalSignal,
}) {
    return reportCapacityForPhase(
        { planDirectory, inputFile, outputDirectory, timeoutMs, maxBytes, signal: externalSignal },
        false,
    );
}
async function reportCapacityForPhase(
    { planDirectory, inputFile, outputDirectory, timeoutMs, maxBytes, signal: externalSignal },
    confirmation,
) {
    validateCapacityReportLimits(timeoutMs, maxBytes);
    const started = performance.now(),
        cancellation = new AbortController(),
        interrupt = () => cancellation.abort(),
        signal = cancellation.signal;
    process.on("SIGTERM", interrupt);
    process.on("SIGINT", interrupt);
    externalSignal?.addEventListener("abort", interrupt, { once: true });
    if (externalSignal?.aborted) interrupt();
    const remaining = () => Math.floor(timeoutMs - (performance.now() - started));
    const check = () => {
        need(!signal.aborted, timedOut ? "CAPACITY_REPORT_DEADLINE" : "CAPACITY_REPORT_CANCELLED");
        need(remaining() > 0, "CAPACITY_REPORT_DEADLINE");
    };
    let timedOut = false;
    const timer = setTimeout(() => {
        timedOut = true;
        interrupt();
    }, timeoutMs);
    let output,
        temp,
        used = 0,
        sourceReviewReservedBytes = 0,
        created = false,
        sealed = false;
    const write = async (path, raw) => {
        check();
        need(used + raw.length + sourceReviewReservedBytes <= maxBytes, "CAPACITY_REPORT_BYTE_BUDGET");
        await writeFile(join(output, path), raw, { flag: "wx", mode: 0o600 });
        used += raw.length;
    };
    const put = async (path, value) => write(path, encode(value));
    try {
        check();
        const planRoot = await realpath(resolve(planDirectory)),
            inputPath = await realpath(resolve(inputFile));
        const planFile = confirmation ? "confirmation-plan.json" : "capacity-plan.json",
            lockFile = confirmation ? "confirmation.lock.json" : "capacity.lock.json";
        const originalPlan = await bounded(join(planRoot, planFile)),
            stub = JSON.parse(originalPlan),
            inputRaw = await bounded(inputPath, MiB);
        need(
            stub?.kind === (confirmation ? "http-capacity-confirmation-plan" : "http-capacity-plan") &&
                (!confirmation || stub.phase === "confirmation") &&
                Array.isArray(stub.points) &&
                stub.points.length <= (confirmation ? 66 : 352),
            "CAPACITY_REPORT_PLAN_SCOPE",
        );
        const input = validateCapacityExecutionSet(JSON.parse(inputRaw), stub);
        output = await canonical(outputDirectory);
        const executionPaths = await Promise.all(
            input.points.map((point) => canonical(resolve(dirname(inputPath), point.executionDirectory))),
        );
        const protectedNames = ["components", "src", "dist", "release", "benchmarks", "presets", "node_modules"];
        const protectedPaths = await Promise.all(protectedNames.map((name) => canonical(join(ROOT, name))));
        assertCapacityOutputSeparate(output, [planRoot, inputPath, ...executionPaths, ...protectedPaths]);
        if (confirmation) {
            const request = await json(join(planRoot, "request.json"));
            need(
                Number.isSafeInteger(request.sourceReview?.maxBytes) &&
                    request.sourceReview.maxBytes >= MiB &&
                    request.sourceReview.maxBytes <= 8 * 1024 ** 3,
                "CONFIRMATION_SOURCE_REVIEW_BUDGET",
            );
            // Include the complete nested temporary replay and the bounded verification receipt in the work-byte cap.
            sourceReviewReservedBytes = request.sourceReview.maxBytes + 8 * MiB;
            need(sourceReviewReservedBytes + MiB < maxBytes, "CONFIRMATION_REPORT_SOURCE_BYTE_RESERVE");
        }
        temp = await mkdtemp(join(tmpdir(), "hgbench-capacity-verify-"));
        const verificationCommand = await runCapacityWorker(
            [
                confirmation ? "__verify-confirmation-plan" : "__verify-plan",
                "--plan",
                planRoot,
                "--out",
                join(temp, "verified.json"),
            ],
            Math.max(1, remaining()),
            signal,
        );
        need(
            !verificationCommand.failure && verificationCommand.code === 0,
            timedOut
                ? "CAPACITY_REPORT_DEADLINE"
                : (verificationCommand.failure ?? "CAPACITY_REPORT_PLAN_VERIFICATION"),
        );
        check();
        const verified = await json(join(temp, "verified.json")),
            plan = verified.plan;
        need(isDeepStrictEqual(plan, JSON.parse(originalPlan)), "CAPACITY_REPORT_PLAN_CHANGED");
        validateCapacityExecutionSet(input, plan);
        const sources = await implementation(check),
            capacityLock = hash(await bounded(join(planRoot, lockFile)));
        const sourceIndependence = confirmation ? verified.sourceIndependence : null;
        if (confirmation)
            need(
                sourceIndependence?.kind === "http-capacity-source-independence" &&
                    sourceIndependence.planDigest === plan.exploration.planDigest &&
                    sourceIndependence.executionSetSha256 === plan.exploration.executionSetSha256,
                "CONFIRMATION_SOURCE_RECEIPT",
            );
        const originalProtectedPaths = confirmation
            ? await Promise.all(
                  [
                      plan.exploration.planDirectory,
                      plan.exploration.reportDirectory,
                      plan.exploration.executionSet,
                      ...sourceIndependence.inputDirectories,
                  ].map(canonical),
              )
            : [];
        const records = plan.points.map((p) => ({
            id: p.id,
            status: "not-started",
            executionDirectory: input.points.find((r) => r.id === p.id)?.executionDirectory ?? null,
            issues: input.points.some((r) => r.id === p.id) ? [] : ["CAPACITY_POINT_NOT_STARTED"],
        }));
        for (const record of records.filter((r) => r.executionDirectory !== null)) {
            check();
            try {
                record.actualDirectory = await realpath(resolve(dirname(inputPath), record.executionDirectory));
                const raw = await bounded(join(record.actualDirectory, "execution.json")),
                    execution = JSON.parse(raw),
                    terminalRaw = await bounded(join(record.actualDirectory, "terminal.json")),
                    terminal = JSON.parse(terminalRaw);
                record.executionSha256 = hash(raw);
                record.terminalSha256 = hash(terminalRaw);
                record.executionLockSha256 = hash(await bounded(join(record.actualDirectory, "execution.lock.json")));
                need(
                    /^[a-f0-9]{32}$/.test(execution.executionId) &&
                        Array.isArray(execution.receipts) &&
                        execution.receipts.length <= (confirmation ? 8 : 4) &&
                        typeof execution.hostBootId === "string" &&
                        /^(0|[1-9][0-9]{0,18})$/.test(execution.registeredMonotonicNs) &&
                        /^(0|[1-9][0-9]{0,18})$/.test(terminal.finishedMonotonicNs) &&
                        BigInt(terminal.finishedMonotonicNs) >= BigInt(execution.registeredMonotonicNs),
                    "CAPACITY_EXECUTION_IDENTITY",
                );
                record.executionId = execution.executionId;
                record.runIds = execution.receipts.map((r) => r.runId);
                need(
                    record.runIds.every((id) => typeof id === "string" && id.length <= 128),
                    "CAPACITY_EXECUTION_IDENTITY",
                );
                record.window = {
                    boot: execution.hostBootId,
                    start: execution.registeredMonotonicNs,
                    end: terminal.finishedMonotonicNs,
                };
            } catch (error) {
                record.issues.push(error.code || "CAPACITY_EXECUTION_UNAVAILABLE");
                record.status = "invalid";
            }
        }
        markCapacityAssociations(records);
        const reserve = MiB + plan.expectedArms * 8192;
        const initial = [inputRaw, encode(sources), encode(verificationCommand)].reduce((n, b) => n + b.length, 0);
        need(initial + reserve + sourceReviewReservedBytes < maxBytes, "CAPACITY_REPORT_BYTE_RESERVE");
        // All path and original-input checks precede the first output mutation.
        need((await canonical(outputDirectory)) === output, "CAPACITY_REPORT_OUTPUT_CHANGED");
        const refreshedPaths = await Promise.all(
            input.points.map((point) => canonical(resolve(dirname(inputPath), point.executionDirectory))),
        );
        need(isDeepStrictEqual(refreshedPaths, executionPaths), "CAPACITY_REPORT_INPUT_PATH_CHANGED");
        const refreshedProtectedPaths = await Promise.all(protectedNames.map((name) => canonical(join(ROOT, name))));
        need(isDeepStrictEqual(refreshedProtectedPaths, protectedPaths), "CAPACITY_REPORT_PROTECTED_PATH_CHANGED");
        assertCapacityOutputSeparate(output, [
            planRoot,
            inputPath,
            ...refreshedPaths,
            ...refreshedProtectedPaths,
            ...originalProtectedPaths,
        ]);
        await mkdir(output, { mode: 0o700 });
        created = true;
        await mkdir(join(output, "points"));
        await mkdir(join(output, "audit"));
        await write("execution-set.json", inputRaw);
        await put("implementation.json", sources);
        await put("audit/plan-verification.json", verificationCommand);
        let exhausted = null;
        for (const record of records) {
            if (record.executionDirectory === null) continue;
            if (record.issues.length) {
                record.status = "invalid";
                continue;
            }
            if (signal.aborted || remaining() < 5000) {
                exhausted = "CAPACITY_REPORT_DEADLINE";
                break;
            }
            const auditReserve = 6 * CAPACITY_REPORT_LIMITS.commandOutputBytes + 8192;
            if (used + reserve + auditReserve + sourceReviewReservedBytes + 65536 >= maxBytes) {
                exhausted = "CAPACITY_REPORT_BYTE_BUDGET";
                break;
            }
            const point = plan.points.find((p) => p.id === record.id),
                childRoot = join(planRoot, point.comparisonDirectory),
                child = await json(join(childRoot, "plan.json")),
                settings = await json(join(childRoot, "inputs/settings.json"));
            check();
            const parser = settings.jvmObservation?.parser?.timeoutMs;
            if (parser !== undefined)
                need(Number.isSafeInteger(parser) && parser >= 100 && parser <= 300000, "CAPACITY_JVM_REPLAY_BUDGET");
            const perPoint = Math.max(
                60000,
                child.budgets.sealAndAnalyzeMs,
                child.analysisPolicy.maxMilliseconds +
                    60000 +
                    (parser === undefined ? 0 : child.budgets.armCount * (parser + 5000)),
            );
            const deadline = Math.min(perPoint, Math.max(1, remaining() - 5000)),
                path = `points/${point.id}.json`,
                cap = Math.min(32 * MiB, maxBytes - used - reserve - auditReserve - sourceReviewReservedBytes);
            const command = await runCapacityWorker(
                [
                    "__point",
                    "--plan",
                    childRoot,
                    "--execution",
                    record.actualDirectory,
                    "--out",
                    join(output, path),
                    "--max-bytes",
                    String(cap),
                ],
                deadline,
                signal,
            );
            let pointRaw = null;
            try {
                pointRaw = await bounded(join(output, path), cap);
                used += pointRaw.length;
            } catch (error) {
                if (error.code !== "ENOENT") throw error;
            }
            need(used <= maxBytes, "CAPACITY_REPORT_BYTE_BUDGET");
            await put(`audit/${point.id}.json`, {
                argv: [
                    "__point",
                    "--plan",
                    point.comparisonDirectory,
                    "--execution",
                    record.executionDirectory,
                    "--out",
                    path,
                    "--max-bytes",
                    String(cap),
                ],
                deadlineMilliseconds: deadline,
                ...command,
            });
            if (command.failure || command.code !== 0) {
                record.status = "invalid";
                record.issues.push(command.failure || "CAPACITY_POINT_REVERIFICATION_FAILED");
                continue;
            }
            try {
                need(pointRaw, "CAPACITY_POINT_OUTPUT_MISSING");
                const result = JSON.parse(pointRaw);
                need(
                    result.verification.planDigest === point.comparisonPlanDigest &&
                        result.verification.executionLockSha256 === record.executionLockSha256 &&
                        hash(await bounded(join(record.actualDirectory, "execution.json"))) ===
                            record.executionSha256 &&
                        hash(await bounded(join(record.actualDirectory, "terminal.json"))) === record.terminalSha256,
                    "CAPACITY_EXECUTION_CHANGED_DURING_REBUILD",
                );
                record.result = compactResult(result);
                record.status = "reverified";
            } catch (error) {
                record.status = "invalid";
                record.issues.push(error.code || "CAPACITY_POINT_OUTPUT_INVALID");
            }
        }
        const rawIds = new Map();
        for (const record of records)
            for (const row of record.result?.verification.rows ?? []) {
                const sha = row.rawEvidenceSha256;
                if (!sha) continue;
                if (rawIds.has(sha)) {
                    record.issues.push("CAPACITY_RAW_REUSED");
                    rawIds.get(sha).issues.push("CAPACITY_RAW_REUSED");
                } else rawIds.set(sha, record);
            }
        if (exhausted)
            for (const record of records)
                if (record.status === "not-started" && record.executionDirectory !== null)
                    record.issues.push(exhausted);
        if (confirmation) markConfirmationIndependence(records, sourceIndependence);
        const summary = {
            ...(confirmation
                ? summarizeCapacityConfirmation(plan, records, sourceIndependence, capacityArm)
                : summarizeCapacity(plan, records)),
            capacityContentDigest: verified.contentDigest,
            executionSetSha256: hash(inputRaw),
            reportBudget: { timeoutMs, maxBytes, exhausted },
            ...(confirmation
                ? {
                      workBudget: {
                          sourceReviewReservedBytes,
                          verificationReceiptMaximumBytes: 8 * MiB,
                          outputPayloadMaximumBytes: maxBytes - sourceReviewReservedBytes,
                          executionDeadlineMilliseconds: timeoutMs,
                          verificationCleanupMilliseconds: 7000,
                      },
                  }
                : {}),
            cancelled: signal.aborted,
        };
        check();
        need(
            hash(await bounded(inputPath, MiB)) === hash(inputRaw) &&
                hash(await bounded(join(planRoot, lockFile))) === capacityLock,
            "CAPACITY_REPORT_INPUT_CHANGED",
        );
        if (confirmation) {
            for (const guard of sourceIndependence.guards) {
                check();
                const raw = await bounded(guard.path);
                need(
                    raw.length === guard.bytes && hash(raw) === guard.sha256,
                    "CONFIRMATION_SOURCE_CHANGED_DURING_REBUILD",
                );
            }
            for (const [path, expected] of [
                [plan.exploration.executionSet, plan.exploration.executionSetSha256],
                [join(plan.exploration.reportDirectory, "report.json"), plan.exploration.reportJsonSha256],
                [join(plan.exploration.reportDirectory, "evidence.lock.json"), plan.exploration.reportLockSha256],
                [join(plan.exploration.planDirectory, "capacity.lock.json"), plan.exploration.capacityLockSha256],
            ]) {
                check();
                need(hash(await bounded(path)) === expected, "CONFIRMATION_SOURCE_CHANGED_DURING_REBUILD");
            }
        }
        need(isDeepStrictEqual(await implementation(check), sources), "CAPACITY_REPORT_IMPLEMENTATION_CHANGED");
        check();
        const summaryRaw = encode(summary),
            htmlRaw = Buffer.from(capacityHtml(summary));
        need(
            used + summaryRaw.length + htmlRaw.length + sourceReviewReservedBytes <= maxBytes,
            "CAPACITY_REPORT_BYTE_BUDGET",
        );
        await write("report.json", summaryRaw);
        await write("report.html", htmlRaw);
        const files = [];
        async function inventory(folder = "") {
            for (const name of (await readdir(join(output, folder))).sort()) {
                check();
                const path = folder ? `${folder}/${name}` : name,
                    info = await lstat(join(output, path));
                need(!info.isSymbolicLink(), "CAPACITY_REPORT_OUTPUT_TYPE");
                if (info.isDirectory()) await inventory(path);
                else {
                    const raw = await bounded(join(output, path), maxBytes);
                    files.push({ path, bytes: raw.length, sha256: hash(raw) });
                }
            }
        }
        await inventory();
        files.sort((a, b) => (a.path < b.path ? -1 : a.path > b.path ? 1 : 0));
        const lock = {
            schemaVersion: 1,
            kind: "http-capacity-report-lock",
            planDigest: plan.planDigest,
            files,
            contentDigest: hash(encode(files)),
        };
        await put("evidence.lock.json", lock);
        check();
        sealed = true;
        return {
            directory: output,
            planDigest: plan.planDigest,
            points: summary.plannedPoints,
            validPoints: summary.validPoints,
            engineeringAccepted: false,
            statisticsCertified: false,
            performanceAccepted: false,
        };
    } catch (error) {
        if (created && !sealed)
            await lstat(output)
                .then(async (info) => {
                    if (info.isDirectory()) {
                        const raw = encode({ code: error.code || "CAPACITY_REPORT_FAILED", message: error.message });
                        if (used + raw.length + sourceReviewReservedBytes <= maxBytes)
                            await writeFile(join(output, "failure.json"), raw, { flag: "wx", mode: 0o600 });
                    }
                })
                .catch(() => {});
        throw error;
    } finally {
        clearTimeout(timer);
        process.removeListener("SIGTERM", interrupt);
        process.removeListener("SIGINT", interrupt);
        externalSignal?.removeEventListener("abort", interrupt);
        if (temp) await rm(temp, { recursive: true, force: true });
    }
}

export function describeCapacityEvidence() {
    return {
        schemaVersion: 1,
        kind: "http-capacity-report-capability",
        operation: "report --plan CAPACITY --input EXECUTION_SET --out NEW --timeout-ms N --max-bytes N",
        limits: CAPACITY_REPORT_LIMITS,
        effects: ["offline-input-reads", "bounded-independent-point-subprocesses", "new-report-files"],
        confirmationImplemented: true,
        confirmationOperation:
            "report-confirmation --plan CONFIRMATION --input EXECUTION_SET --out NEW --timeout-ms N --max-bytes N",
        sloCapacityImplemented: false,
        performanceAccepted: false,
    };
}

async function main(args) {
    if (args.length === 1 && args[0] === "describe") {
        process.stdout.write(JSON.stringify(describeCapacityEvidence()) + "\n");
        return;
    }
    if (args.length === 1 && args[0] === "--help") {
        process.stdout.write(
            describeCapacityEvidence().operation +
                "\n" +
                describeCapacityEvidence().confirmationOperation +
                "\nOffline; task success is not validity. Highest observations are unconfirmed; SLO rate denominator and capacity remain unknown.\n",
        );
        return;
    }
    const action = args.shift(),
        options = {};
    const keys =
        action === "__boundary"
            ? ["--plan", "--point", "--execution", "--out", "--max-bytes"]
            : ["__verify-plan", "__verify-confirmation-plan"].includes(action)
              ? ["--plan", "--out"]
              : action === "__point"
                ? ["--plan", "--execution", "--out", "--max-bytes"]
                : ["--plan", "--input", "--out", "--timeout-ms", "--max-bytes"];
    need(
        [
            "report",
            "report-confirmation",
            "__point",
            "__boundary",
            "__verify-plan",
            "__verify-confirmation-plan",
        ].includes(action) && args.length === keys.length * 2,
        "CAPACITY_REPORT_COMMAND",
    );
    for (let i = 0; i < args.length; i += 2) {
        need(keys.includes(args[i]) && !Object.hasOwn(options, args[i]) && args[i + 1], "CAPACITY_REPORT_OPTIONS");
        options[args[i]] = args[i + 1];
    }
    if (action === "__boundary") {
        const cap = Number(options["--max-bytes"]);
        need(Number.isSafeInteger(cap) && cap >= MiB && cap <= 256 * MiB, "BOUNDARY_OUTPUT_BUDGET");
        const decision = await verifyCapacityBoundary({
            planDirectory: options["--plan"],
            pointId: options["--point"],
            executionDirectory: options["--execution"],
        });
        const raw = encode(decision);
        need(raw.length <= MiB && raw.length <= cap, "BOUNDARY_OUTPUT_BUDGET");
        await writeFile(options["--out"], raw, { flag: "wx", mode: 0o600 });
        process.stdout.write(JSON.stringify({ ok: true, receiptSha256: hash(raw) }) + "\n");
        return;
    }
    if (["__verify-plan", "__verify-confirmation-plan"].includes(action)) {
        const verified =
                action === "__verify-confirmation-plan"
                    ? await verifyHttpCapacityConfirmationPlan(options["--plan"], { includeSourceIndependence: true })
                    : await verifyHttpCapacityPlan(options["--plan"]),
            raw = encode({
                plan: verified.plan,
                contentDigest: verified.contentDigest,
                ...(action === "__verify-confirmation-plan" ? { sourceIndependence: verified.sourceIndependence } : {}),
            });
        need(raw.length <= 8 * MiB, "CAPACITY_REPORT_PLAN_OUTPUT");
        await writeFile(options["--out"], raw, { flag: "wx", mode: 0o600 });
        process.stdout.write(JSON.stringify({ ok: true }) + "\n");
        return;
    }
    if (action === "__point") {
        const cap = Number(options["--max-bytes"]);
        need(Number.isSafeInteger(cap) && cap > 0 && cap <= 32 * MiB, "CAPACITY_REPORT_POINT_LIMIT");
        const result = await normalizeHttpEvidence({
            planDirectory: options["--plan"],
            executionDirectory: options["--execution"],
            includeHttpSummaries: true,
        });
        const raw = encode({
            verification: result.verification,
            analysis: result.analysis,
            reportContext: result.reportContext,
        });
        need(raw.length <= cap, "CAPACITY_REPORT_POINT_OUTPUT");
        await writeFile(options["--out"], raw, { flag: "wx", mode: 0o600 });
        process.stdout.write(JSON.stringify({ ok: true }) + "\n");
        return;
    }
    const result = await (action === "report-confirmation" ? reportCapacityConfirmation : reportCapacity)({
        planDirectory: options["--plan"],
        inputFile: options["--input"],
        outputDirectory: options["--out"],
        timeoutMs: Number(options["--timeout-ms"]),
        maxBytes: Number(options["--max-bytes"]),
    });
    process.stdout.write(JSON.stringify({ ok: true, data: result }) + "\n");
}
if (process.argv[1] && pathToFileURL(resolve(process.argv[1])).href === import.meta.url)
    // The confirmation verifier reuses this module through a dynamic import during source replay.
    // Finish module initialization before awaiting that verifier; top-level await would self-deadlock.
    main(process.argv.slice(2)).catch((error) => {
        process.stderr.write(
            JSON.stringify({
                ok: false,
                error: { code: error.code || "CAPACITY_REPORT_FAILED", message: error.message, retryable: false },
            }) + "\n",
        );
        process.exitCode = 6;
    });
