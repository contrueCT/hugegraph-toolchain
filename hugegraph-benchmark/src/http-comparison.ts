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

import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { basename, dirname, join, resolve } from "node:path";
import { realpath } from "node:fs/promises";
import { z } from "zod";
import { BenchError, readJson, writeJson } from "./io.js";
import { packageRoot } from "./plans.js";
import { freezeTask } from "./tasks.js";

async function invoke(
    program: string,
    component: string,
    args: string[],
    capability = false,
    timeout = 330000,
): Promise<Record<string, unknown>> {
    let stdout: string;
    try {
        stdout = (
            await promisify(execFile)(program, [component, ...args], {
                timeout,
                maxBuffer: 16 * 1024 * 1024,
            })
        ).stdout;
    } catch (error) {
        const failure = error as { stdout?: string; stderr?: string; message?: string; code?: string | number };
        let details: unknown;
        try {
            details = JSON.parse(failure.stdout?.trim() || failure.stderr || "");
        } catch {
            details = { diagnostic: (failure.stderr ?? failure.message ?? "").slice(0, 2048) };
        }
        throw new BenchError(
            "HTTP_COMPARISON_FAILED",
            "Comparison component failed; inspect its diagnostic and the same attempt",
            failure.code === "ENOENT" ? 3 : 6,
            false,
            details,
        );
    }
    let reply: Record<string, unknown>;
    try {
        reply = JSON.parse(stdout);
    } catch {
        throw new BenchError("HTTP_COMPARISON_PROTOCOL", "Comparison component returned invalid JSON", 6);
    }
    if (capability && reply.kind === "http-comparison-planner-capability") return reply;
    if (!capability && reply.ok === true && reply.data && typeof reply.data === "object")
        return reply.data as Record<string, unknown>;
    throw new BenchError("HTTP_COMPARISON_PROTOCOL", "Unexpected comparison protocol response", 6);
}

export async function httpComparisonPlan(args: string[], capability = false) {
    return invoke(process.execPath, join(packageRoot, "components/http/comparison-plan.mjs"), args, capability);
}

export async function freezeHttpComparison(request: string, httpPlan: string, output: string) {
    return httpComparisonPlan([
        "--input",
        resolve(request),
        "--http-plan",
        resolve(httpPlan),
        "--out",
        resolve(output),
    ]);
}

export async function reportHttpComparison(execution: string, plan: string, output: string) {
    return invoke(process.execPath, join(packageRoot, "components/http/paired-evidence.mjs"), [
        "--execution",
        resolve(execution),
        "--plan",
        resolve(plan),
        "--out",
        resolve(output),
    ]);
}

export async function httpComparisonWorkflow(action: "describe" | "status" | "stop", attempt?: string) {
    return invoke(
        process.env.HGBENCH_PYTHON ?? "python3",
        join(packageRoot, "components/http/comparison-workflow.py"),
        [action, ...(attempt ? ["--attempt", resolve(attempt)] : []), "--json"],
    );
}

export async function prepareHttpComparison(runtime: string, comparison: string, directory: string) {
    const budgetSchema = z
        .object({
            budgets: z
                .object({
                    maxTotalMilliseconds: z
                        .number()
                        .int()
                        .positive()
                        .max(7 * 24 * 3600000),
                    budgetMode: z.enum(["total", "stage-sum"]).optional(),
                    stopMs: z.number().int().positive().max(300000),
                    sealAndAnalyzeMs: z.number().int().positive().max(300000),
                })
                .passthrough(),
        })
        .passthrough();
    const input = await readJson(join(resolve(comparison), "plan.json"), budgetSchema, 16 * 1024 * 1024);
    const taskTimeout = (plan: z.infer<typeof budgetSchema>) =>
        plan.budgets.budgetMode === "total"
            ? plan.budgets.maxTotalMilliseconds
            : plan.budgets.maxTotalMilliseconds + plan.budgets.stopMs + plan.budgets.sealAndAnalyzeMs + 10000;
    if (taskTimeout(input) > 7 * 24 * 3600000)
        throw new BenchError(
            "HTTP_COMPARISON_TASK_BUDGET",
            "Comparison plus cleanup exceeds the seven-day task limit; use a smaller explicit plan budget",
            3,
        );
    const attempt = join(await realpath(dirname(resolve(directory))), basename(directory));
    const result = await invoke(
        process.env.HGBENCH_PYTHON ?? "python3",
        join(packageRoot, "components/http/comparison-workflow.py"),
        ["freeze", "--runtime", resolve(runtime), "--plan", resolve(comparison), "--out", attempt, "--json"],
    );
    if (result.attempt !== attempt || result.executed !== false)
        throw new BenchError("HTTP_COMPARISON_PROTOCOL", "Comparison attempt identity differs", 6);
    const frozenRuntime = await readJson(
        join(attempt, "runtime.json"),
        z
            .object({
                schemaVersion: z.union([z.literal(1), z.literal(2)]),
                settings: z.object({ hostPython: z.string().min(1) }).passthrough(),
                identities: z
                    .object({
                        goBinary: z
                            .object({ path: z.string().min(1) })
                            .passthrough()
                            .optional(),
                    })
                    .passthrough(),
            })
            .passthrough(),
        64 * 1024 * 1024,
    );
    const plan = await readJson(join(attempt, "comparison/plan.json"), budgetSchema, 16 * 1024 * 1024);
    if (JSON.stringify(plan.budgets) !== JSON.stringify(input.budgets))
        throw new BenchError("HTTP_COMPARISON_PROTOCOL", "Comparison budget changed during preparation", 6);
    const source = join(attempt, "task.source.json");
    await writeJson(source, {
        schemaVersion: 1,
        kind: "component-task",
        name: "http-comparison",
        command: [
            frozenRuntime.settings.hostPython,
            join(attempt, "comparison-workflow.py"),
            "run",
            "--attempt",
            attempt,
        ],
        cwd: attempt,
        trackedFiles: [
            "comparison-workflow.py",
            "runtime.json",
            "runtime.lock.json",
            "comparison/plan.json",
            "comparison/plan.lock.json",
            ...(frozenRuntime.schemaVersion === 2
                ? [
                      "go-comparison-runtime.py",
                      "go-process-observer.py",
                      "go-clock-probe.py",
                      ...["producer.py", "proof.json", "raw.lock.json", "stderr.txt"].map((n) => "go-clock-proof/" + n),
                  ]
                : []),
        ]
            .map((p) => join(attempt, p))
            .concat(frozenRuntime.schemaVersion === 2 ? [frozenRuntime.identities.goBinary!.path] : []),
        effect: "database",
        timeoutMs: taskTimeout(plan),
        terminateGraceMs: 30000,
        maxLogBytes: 1048576,
    });
    const task = await freezeTask(source);
    const taskPlan = join(attempt, "task.json");
    await writeJson(taskPlan, task);
    return { ...result, taskPlan, taskPlanDigest: task.digest, replayAllowed: false };
}
