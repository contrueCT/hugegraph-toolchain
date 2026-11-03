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

import { spawn } from "node:child_process";
import { basename, dirname, join, resolve } from "node:path";
import { realpath } from "node:fs/promises";
import { isDeepStrictEqual } from "node:util";
import { z } from "zod";
import { BenchError, readJson, writeJson } from "./io.js";
import { packageRoot } from "./plans.js";
import { freezeTask } from "./tasks.js";
import { httpKnownRegressionReportOptionsSchema } from "./http-known-regression-contracts.js";
import { httpHeadroomReportOptionsSchema } from "./http-headroom-contracts.js";
import { httpObserverCostReportOptionsSchema } from "./http-observer-cost-contracts.js";
import { httpEngineeringAcceptanceOptionsSchema } from "./http-engineering-acceptance-contracts.js";

const OUTPUT_BYTES = 16 * 1024 * 1024;
const CLEANUP_MS = 5000;

export async function reportHttpEngineeringAcceptance(
    input: string,
    output: string,
    options: { timeoutMs: number; maxBytes: number },
) {
    const checked = httpEngineeringAcceptanceOptionsSchema.safeParse(options);
    if (!checked.success)
        throw new BenchError(
            "HTTP_ENGINEERING_ACCEPTANCE_OPTIONS",
            "Specify an explicit shared report deadline and output byte cap",
            2,
            false,
            checked.error.issues,
        );
    const directory = join(await realpath(dirname(resolve(output))), basename(resolve(output)));
    const result = await invoke(
        "engineering-acceptance.mjs",
        [
            "--input",
            resolve(input),
            "--output",
            directory,
            "--timeout-ms",
            String(checked.data.timeoutMs),
            "--max-bytes",
            String(checked.data.maxBytes),
        ],
        checked.data.timeoutMs + CLEANUP_MS,
    );
    const reply = z
        .object({
            kind: z.literal("http-engineering-acceptance-report"),
            directory: z.literal(directory),
            reportJson: z.literal(join(directory, "report.json")),
            reportHtml: z.literal(join(directory, "report.html")),
            reportLock: z.literal(join(directory, "report.lock.json")),
            calibrationIndex: z.literal(join(directory, "calibration-evidence-index.json")),
            engineeringAccepted: z.boolean(),
            performanceAccepted: z.literal(false),
            qualificationAccepted: z.literal(false),
        })
        .passthrough()
        .safeParse(result);
    if (!reply.success)
        throw new BenchError("HTTP_CALIBRATION_PROTOCOL", "Unexpected engineering acceptance response", 6);
    return reply.data;
}
const phaseWindowReplySchema = z.strictObject({
    kind: z.literal("http-phase-window-diagnostics"),
    rawManifestSha256: z.string().regex(/^[a-f0-9]{64}$/),
    runs: z.number().int().min(1).max(5120),
    performanceAccepted: z.literal(false),
    warmupStable: z.null(),
});
const driverCostReplySchema = z.strictObject({
    kind: z.literal("http-driver-self-cost"),
    rawManifestSha256: z.string().regex(/^[a-f0-9]{64}$/),
    runs: z.number().int().min(1).max(5120),
    performanceAccepted: z.literal(false),
    overheadAccepted: z.literal(false),
});
const completionWindowReplySchema = z.strictObject({
    kind: z.literal("http-completion-window-diagnostics"),
    rawManifestSha256: z.string().regex(/^[a-f0-9]{64}$/),
    runs: z.number().int().min(1).max(5120),
    warmupStable: z.null(),
    engineeringAccepted: z.literal(false),
    performanceAccepted: z.literal(false),
});
const phaseWindowOptionsSchema = z.strictObject({
    windowMs: z.number().int().min(1).max(300000),
    minimumRequestsPerWindow: z.number().int().min(1).max(100000),
    minimumP99Requests: z.number().int().min(1000).max(100000),
});

// The CLI supervises one replaceable component. Planning and inference live in
// that component; no target requests or statistical calculations occur here.
async function invoke(
    component: string,
    args: string[],
    timeoutMs = 330000,
    capability = false,
    cleanupMs = CLEANUP_MS,
): Promise<Record<string, unknown>> {
    const reply = await new Promise<Record<string, unknown>>((accept, reject) => {
        const grouped = process.platform !== "win32";
        const python = component.endsWith(".py");
        const child = spawn(
            python ? (process.env.HGBENCH_PYTHON ?? "python3") : process.execPath,
            [...(python ? ["-B"] : []), join(packageRoot, "components/http", component), ...args],
            {
                detached: grouped,
                stdio: ["ignore", "pipe", "pipe"],
            },
        );
        let failure: BenchError | undefined;
        let totalBytes = 0;
        let force: ReturnType<typeof setTimeout> | undefined;
        const stdout: Buffer[] = [];
        const stderr: Buffer[] = [];
        function signal(name: NodeJS.Signals) {
            if (!child.pid) return;
            try {
                if (grouped) process.kill(-child.pid, name);
                else child.kill(name);
            } catch (error) {
                if ((error as NodeJS.ErrnoException).code !== "ESRCH") {
                    failure ??= new BenchError("HTTP_CALIBRATION_CLEANUP", "Cannot stop the component group", 6);
                }
            }
        }
        function stop(error: BenchError) {
            if (failure) return;
            failure = error;
            signal("SIGTERM");
            force = setTimeout(() => signal("SIGKILL"), cleanupMs);
        }
        const interrupt = () =>
            stop(new BenchError("HTTP_CALIBRATION_CANCELLED", "Calibration command interrupted", 130));
        const terminate = () =>
            stop(new BenchError("HTTP_CALIBRATION_CANCELLED", "Calibration command terminated", 143));
        process.on("SIGINT", interrupt);
        process.on("SIGTERM", terminate);
        const deadline = setTimeout(
            () => stop(new BenchError("HTTP_CALIBRATION_TIMEOUT", "Calibration component exceeded its deadline", 6)),
            timeoutMs,
        );
        for (const [stream, output] of [
            [child.stdout, stdout],
            [child.stderr, stderr],
        ] as const) {
            stream.on("data", (chunk: Buffer) => {
                const available = Math.max(0, OUTPUT_BYTES - totalBytes);
                if (available) output.push(chunk.subarray(0, available));
                totalBytes += chunk.length;
                if (totalBytes > OUTPUT_BYTES)
                    stop(new BenchError("HTTP_CALIBRATION_OUTPUT", "Calibration component exceeded output budget", 6));
            });
        }
        child.once("error", (error: NodeJS.ErrnoException) => {
            failure ??= new BenchError(
                "HTTP_CALIBRATION_UNAVAILABLE",
                "Cannot start the local calibration component",
                error.code === "ENOENT" ? 3 : 6,
            );
        });
        child.once("close", (code) => {
            clearTimeout(deadline);
            clearTimeout(force);
            process.removeListener("SIGINT", interrupt);
            process.removeListener("SIGTERM", terminate);
            if (failure) return reject(failure);
            const encoded = Buffer.concat(stdout).toString("utf8");
            const diagnostic = Buffer.concat(stderr).toString("utf8");
            if (code !== 0) {
                let details: unknown;
                try {
                    const failed = JSON.parse(encoded.trim() || diagnostic);
                    details = failed?.error ?? failed;
                } catch {
                    details = { diagnostic: diagnostic.slice(0, 2048) };
                }
                return reject(
                    new BenchError(
                        "HTTP_CALIBRATION_FAILED",
                        "Calibration component failed; inspect its diagnostic and preserved output",
                        code === 2 || code === 3 ? code : 6,
                        false,
                        details,
                    ),
                );
            }
            let parsed: Record<string, unknown>;
            try {
                parsed = JSON.parse(encoded);
            } catch {
                return reject(new BenchError("HTTP_CALIBRATION_PROTOCOL", "Component returned invalid JSON", 6));
            }
            if (!parsed || typeof parsed !== "object" || Array.isArray(parsed))
                return reject(new BenchError("HTTP_CALIBRATION_PROTOCOL", "Component returned invalid envelope", 6));
            accept(parsed);
        });
    });
    if (capability && reply.kind === "http-calibration-planner-capability") return reply;
    // These fixed components use a short bare reply. Do not accept
    // arbitrary bare JSON from other components or normalize failed responses.
    if (!capability && component === "phase-windows.mjs") {
        const result = phaseWindowReplySchema.safeParse(reply);
        if (!result.success)
            throw new BenchError("HTTP_CALIBRATION_PROTOCOL", "Unexpected phase-window component response", 6);
        return result.data;
    }
    if (!capability && component === "driver-self-cost.mjs") {
        const result = driverCostReplySchema.safeParse(reply);
        if (!result.success)
            throw new BenchError("HTTP_CALIBRATION_PROTOCOL", "Unexpected driver-cost component response", 6);
        return result.data;
    }
    if (!capability && component === "completion-windows.mjs") {
        const result = completionWindowReplySchema.safeParse(reply);
        if (!result.success)
            throw new BenchError("HTTP_CALIBRATION_PROTOCOL", "Unexpected completion-window component response", 6);
        return result.data;
    }
    if (!capability && reply.ok === true && reply.data && typeof reply.data === "object")
        return reply.data as Record<string, unknown>;
    throw new BenchError("HTTP_CALIBRATION_PROTOCOL", "Unexpected calibration component response", 6);
}

const workflowBudgetSchema = z.object({
    kind: z.literal("http-calibration-runtime"),
    budgets: z.object({
        prepareMilliseconds: z
            .number()
            .int()
            .min(1000)
            .max(7 * 86400000),
        maxRunMilliseconds: z
            .number()
            .int()
            .min(1000)
            .max(7 * 86400000),
        cleanupMilliseconds: z.number().int().min(5000).max(20000),
    }),
});

export async function prepareHttpCalibration(runtime: string, calibration: string, directory: string) {
    const input = await readJson(resolve(runtime), workflowBudgetSchema);
    const timeoutMs = input.budgets.maxRunMilliseconds + input.budgets.cleanupMilliseconds + 10000;
    if (timeoutMs > 7 * 86400000)
        throw new BenchError("HTTP_CALIBRATION_TASK_BUDGET", "Cohort and cleanup exceed seven-day task limit", 3);
    const attempt = join(await realpath(dirname(resolve(directory))), basename(directory));
    const result = await invoke(
        "calibration-workflow.py",
        ["prepare", "--runtime", resolve(runtime), "--plan", resolve(calibration), "--out", attempt],
        input.budgets.prepareMilliseconds + 10000,
    );
    const frozen = await readJson(
        join(attempt, "workflow.json"),
        z.object({
            runtime: workflowBudgetSchema.extend({
                comparisonRuntime: z.object({
                    hostPython: z.string().min(1),
                    schemaVersion: z.union([z.literal(1), z.literal(2)]).default(1),
                }),
            }),
        }),
        16 * 1024 * 1024,
    );
    if (
        result.attempt !== attempt ||
        result.executed !== false ||
        typeof result.planSha256 !== "string" ||
        !/^[a-f0-9]{64}$/.test(result.planSha256) ||
        !isDeepStrictEqual(frozen.runtime.budgets, input.budgets)
    )
        throw new BenchError("HTTP_CALIBRATION_PROTOCOL", "Prepared cohort identity or budgets changed", 6);
    const source = join(attempt, "task.source.json");
    await writeJson(source, {
        schemaVersion: 1,
        kind: "component-task",
        name: "http-calibration-cohort",
        command: [
            frozen.runtime.comparisonRuntime.hostPython,
            "-B",
            join(attempt, "calibration-workflow.py"),
            "run",
            "--attempt",
            attempt,
        ],
        cwd: attempt,
        trackedFiles: [
            "calibration-workflow.py",
            "comparison-workflow.py",
            "workflow.json",
            "workflow.lock.json",
            "calibration/plan.json",
            "calibration/plan.lock.json",
            ...(frozen.runtime.comparisonRuntime.schemaVersion === 2
                ? ["go-comparison-runtime.py", "go-process-observer.py", "go-clock-probe.py"]
                : []),
        ].map((name) => join(attempt, name)),
        effect: "database",
        timeoutMs,
        terminateGraceMs: 30000,
        maxLogBytes: 1048576,
    });
    const task = await freezeTask(source),
        taskPlan = join(attempt, "task.json");
    await writeJson(taskPlan, task);
    return {
        ...result,
        planSha256: result.planSha256 as string,
        taskPlan,
        taskPlanDigest: task.digest,
        replayAllowed: false,
    };
}

export function httpCalibrationWorkflow(action: "status" | "stop", attempt: string) {
    return invoke(
        "calibration-workflow.py",
        [action, "--attempt", resolve(attempt)],
        action === "stop" ? 45000 : 330000,
    );
}

export async function describeHttpCalibration() {
    const [planner, workflow] = await Promise.all([
        invoke("calibration-plan.mjs", ["describe"], 30000, true),
        invoke("calibration-workflow.py", ["describe"], 30000),
    ]);
    return { ...planner, workflow };
}

export function freezeHttpCalibration(input: string, httpPlan: string, output: string) {
    return invoke("calibration-plan.mjs", [
        "plan",
        "--input",
        resolve(input),
        "--http-plan",
        resolve(httpPlan),
        "--out",
        resolve(output),
    ]);
}

export function verifyHttpCalibration(plan: string) {
    return invoke("calibration-plan.mjs", ["verify", "--plan", resolve(plan)]);
}

export async function reportHttpCalibration(input: string, plan: string, output: string) {
    // This read sets the outer supervision deadline only. The component verifies
    // the complete immutable plan before reading executions or running analysis.
    const budget = await readJson(
        join(resolve(plan), "plan.json"),
        z.object({
            kind: z.literal("http-calibration-plan"),
            budgets: z.object({ maxReportMilliseconds: z.number().int().min(1000).max(86400000) }),
        }),
    );
    return invoke(
        "calibration-evidence.mjs",
        ["report", "--plan", resolve(plan), "--input", resolve(input), "--out", resolve(output)],
        budget.budgets.maxReportMilliseconds + 10000,
    );
}

export async function reportHttpPhaseWindows(
    input: string,
    output: string,
    options: z.infer<typeof phaseWindowOptionsSchema>,
) {
    const checked = phaseWindowOptionsSchema.safeParse(options);
    if (!checked.success)
        throw new BenchError(
            "HTTP_CALIBRATION_WINDOWS_OPTIONS",
            "Window and sample thresholds must be explicit integers in the documented ranges",
            2,
            false,
            checked.error.issues,
        );
    const policy = checked.data;
    const result = await invoke("phase-windows.mjs", [
        "report",
        "--input",
        resolve(input),
        "--output",
        resolve(output),
        "--window-ms",
        String(policy.windowMs),
        "--minimum-requests-per-window",
        String(policy.minimumRequestsPerWindow),
        "--minimum-p99-requests",
        String(policy.minimumP99Requests),
    ]);
    return {
        ...result,
        reportJson: join(resolve(output), "report.json"),
        reportHtml: join(resolve(output), "report.html"),
    };
}

export async function reportHttpDriverSelfCost(input: string, output: string) {
    const reportJson = resolve(output);
    const result = await invoke("driver-self-cost.mjs", ["--input", resolve(input), "--output", reportJson]);
    return { ...result, reportJson };
}

export async function reportHttpKnownRegression(
    input: string,
    output: string,
    options: { timeoutMs: number; maxBytes: number },
) {
    const checked = httpKnownRegressionReportOptionsSchema.safeParse(options);
    if (!checked.success)
        throw new BenchError(
            "HTTP_KNOWN_REGRESSION_OPTIONS",
            "Specify an explicit bounded deadline and byte cap",
            2,
            false,
            checked.error.issues,
        );
    const result = await invoke(
        "known-regression-evidence.mjs",
        [
            "--input",
            resolve(input),
            "--output",
            resolve(output),
            "--timeout-ms",
            String(checked.data.timeoutMs),
            "--max-bytes",
            String(checked.data.maxBytes),
        ],
        checked.data.timeoutMs + CLEANUP_MS,
    );
    const reply = z
        .object({
            kind: z.literal("http-known-regression-evidence"),
            directory: z.literal(resolve(output)),
            reportJson: z.literal(join(resolve(output), "report.json")),
            reportLock: z.literal(join(resolve(output), "report.lock.json")),
            gateId: z.literal("known-regression-detection"),
            status: z.enum(["passed", "failed", "unknown"]),
            inferenceAccepted: z.literal(false),
            performanceAccepted: z.literal(false),
        })
        .passthrough()
        .safeParse(result);
    if (!reply.success)
        throw new BenchError("HTTP_CALIBRATION_PROTOCOL", "Unexpected known-regression evidence response", 6);
    return reply.data;
}

export async function reportHttpDriverHeadroom(
    input: string,
    output: string,
    options: { timeoutMs: number; maxBytes: number },
) {
    const checked = httpHeadroomReportOptionsSchema.safeParse(options);
    if (!checked.success)
        throw new BenchError(
            "HTTP_HEADROOM_OPTIONS",
            "Specify an explicit component deadline and output byte cap",
            2,
            false,
            checked.error.issues,
        );
    const result = await invoke(
        "headroom-evidence.mjs",
        [
            "report",
            "--input",
            resolve(input),
            "--out",
            resolve(output),
            "--timeout-ms",
            String(checked.data.timeoutMs),
            "--max-bytes",
            String(checked.data.maxBytes),
        ],
        checked.data.timeoutMs + CLEANUP_MS,
    );
    const reply = z
        .object({
            kind: z.literal("http-driver-headroom-evidence"),
            directory: z.literal(resolve(output)),
            reportJson: z.literal(join(resolve(output), "report.json")),
            reportLock: z.literal(join(resolve(output), "report.lock.json")),
            status: z.enum(["passed", "failed", "unknown"]),
            limitedScopeEngineeringAccepted: z.boolean(),
            inferenceAccepted: z.literal(false),
            performanceAccepted: z.literal(false),
        })
        .passthrough()
        .safeParse(result);
    if (!reply.success || (reply.data.limitedScopeEngineeringAccepted && reply.data.status !== "passed"))
        throw new BenchError("HTTP_CALIBRATION_PROTOCOL", "Unexpected driver-headroom evidence response", 6);
    return reply.data;
}

export async function reportHttpObserverCost(
    input: string,
    output: string,
    options: { timeoutMs: number; maxBytes: number },
) {
    const checked = httpObserverCostReportOptionsSchema.safeParse(options);
    if (!checked.success)
        throw new BenchError(
            "HTTP_OBSERVER_COST_OPTIONS",
            "Specify an explicit component deadline and output byte cap",
            2,
            false,
            checked.error.issues,
        );
    const result = await invoke(
        "observer-cost-evidence.mjs",
        [
            "report",
            "--input",
            resolve(input),
            "--out",
            resolve(output),
            "--timeout-ms",
            String(checked.data.timeoutMs),
            "--max-bytes",
            String(checked.data.maxBytes),
        ],
        checked.data.timeoutMs + CLEANUP_MS,
    );
    const reply = z
        .object({
            kind: z.literal("http-observer-cost-evidence"),
            directory: z.literal(resolve(output)),
            reportJson: z.literal(join(resolve(output), "report.json")),
            reportLock: z.literal(join(resolve(output), "report.lock.json")),
            status: z.enum(["passed", "failed", "unknown"]),
            scopedCostMeasured: z.boolean(),
            evidenceVerified: z.boolean(),
            inferenceAccepted: z.literal(false),
            performanceAccepted: z.literal(false),
        })
        .passthrough()
        .safeParse(result);
    if (
        !reply.success ||
        (reply.data.scopedCostMeasured && (reply.data.status !== "passed" || !reply.data.evidenceVerified))
    )
        throw new BenchError("HTTP_CALIBRATION_PROTOCOL", "Unexpected observer-cost evidence response", 6);
    return reply.data;
}

export async function reportHttpCompletionWindows(
    input: string,
    output: string,
    options: { windowMs: number; minimumLatencyObservations: number },
) {
    const checked = z
        .strictObject({
            windowMs: z.number().int().min(1).max(300000),
            minimumLatencyObservations: z.number().int().min(1).max(100000),
        })
        .safeParse(options);
    if (!checked.success)
        throw new BenchError(
            "HTTP_CAPACITY_WINDOWS_OPTIONS",
            "Specify a positive window size and latency sample minimum; P99 still requires at least 1000 observations",
            2,
            false,
            checked.error.issues,
        );
    const result = await invoke("completion-windows.mjs", [
        "--raw",
        resolve(input),
        "--window-ms",
        String(checked.data.windowMs),
        "--minimum-latency-observations",
        String(checked.data.minimumLatencyObservations),
        "--out",
        resolve(output),
    ]);
    return { ...result, reportJson: join(resolve(output), "report.json") };
}

export async function reportHttpCapacity(
    input: string,
    plan: string,
    output: string,
    options: { timeoutMs: number; maxBytes: number },
) {
    return reportCapacityPhase("report", input, plan, output, options);
}

export async function reportHttpCapacityConfirmation(
    input: string,
    plan: string,
    output: string,
    options: { timeoutMs: number; maxBytes: number },
) {
    return reportCapacityPhase("report-confirmation", input, plan, output, options);
}

async function reportCapacityPhase(
    action: "report" | "report-confirmation",
    input: string,
    plan: string,
    output: string,
    options: { timeoutMs: number; maxBytes: number },
) {
    const checked = z
        .strictObject({
            timeoutMs: z.number().int().min(1000).max(86400000),
            maxBytes: z.number().int().min(1048576).max(8589934592),
        })
        .safeParse(options);
    if (!checked.success)
        throw new BenchError(
            "HTTP_CAPACITY_REPORT_OPTIONS",
            "Specify an explicit report deadline (1000..86400000 ms) and output cap (1 MiB..8 GiB)",
            2,
            false,
            checked.error.issues,
        );
    const directory = join(await realpath(dirname(resolve(output))), basename(resolve(output)));
    const result = await invoke(
        "capacity-evidence.mjs",
        [
            action,
            "--plan",
            resolve(plan),
            "--input",
            resolve(input),
            "--out",
            directory,
            "--timeout-ms",
            String(checked.data.timeoutMs),
            "--max-bytes",
            String(checked.data.maxBytes),
        ],
        checked.data.timeoutMs + 10000,
        false,
        action === "report-confirmation" ? 10000 : CLEANUP_MS,
    );
    const reply = z
        .strictObject({
            directory: z.string(),
            planDigest: z.string().regex(/^[a-f0-9]{64}$/),
            points: z
                .number()
                .int()
                .min(action === "report" ? 11 : 0)
                .max(action === "report" ? 352 : 66),
            validPoints: z
                .number()
                .int()
                .nonnegative()
                .max(action === "report" ? 352 : 66),
            engineeringAccepted: z.literal(false),
            statisticsCertified: z.literal(false),
            performanceAccepted: z.literal(false),
        })
        .safeParse(result);
    if (
        !reply.success ||
        (reply.success && (reply.data.directory !== directory || reply.data.validPoints > reply.data.points))
    )
        throw new BenchError("HTTP_CAPACITY_REPORT_PROTOCOL", "Unexpected capacity report response", 6);
    return {
        ...reply.data,
        reportJson: join(directory, "report.json"),
        reportHtml: join(directory, "report.html"),
    };
}
