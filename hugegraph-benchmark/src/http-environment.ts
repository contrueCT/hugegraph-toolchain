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
import { httpEnvironmentSettingsSchema } from "./http-environment-contracts.js";
export { httpEnvironmentSettingsSchema } from "./http-environment-contracts.js";

const component = join(packageRoot, "components/http/hugegraph-environment.py");
async function invoke(command: string, args: string[], timeout: number): Promise<Record<string, unknown>> {
    let stdout: string;
    try {
        stdout = (
            await promisify(execFile)(process.env.HGBENCH_PYTHON ?? "python3", [component, command, ...args], {
                timeout,
                maxBuffer: 2097152,
            })
        ).stdout;
    } catch (error) {
        const e = error as { stdout?: string; stderr?: string; message?: string; code?: number | string };
        let details: unknown;
        try {
            details = JSON.parse(e.stdout ?? "");
        } catch {
            details = { diagnostic: e.stderr?.slice(0, 2048) ?? e.message };
        }
        throw new BenchError(
            "HTTP_ENVIRONMENT_FAILED",
            "Environment component failed; inspect its diagnostic and same attempt",
            e.code === "ENOENT" ? 3 : 6,
            false,
            details,
        );
    }
    const reply = JSON.parse(stdout);
    if (reply.schemaVersion !== 1 || reply.ok !== true || reply.command !== command || typeof reply.data !== "object")
        throw new BenchError("HTTP_ENVIRONMENT_PROTOCOL", "Invalid environment component response", 6);
    return reply.data;
}

export async function planHttpEnvironment(settingsFile: string, directory: string) {
    await readJson(settingsFile, httpEnvironmentSettingsSchema, 65536);
    const attempt = join(await realpath(dirname(resolve(directory))), basename(directory));
    const result = await invoke("plan", ["--settings", resolve(settingsFile), "--out", attempt], 300000);
    if (result.attempt !== attempt || result.executed !== false)
        throw new BenchError("HTTP_ENVIRONMENT_PROTOCOL", "Environment attempt identity differs", 6);
    const planned = await readJson(
        join(attempt, "plan.json"),
        z
            .object({
                settings: httpEnvironmentSettingsSchema,
                controlFiles: z
                    .array(
                        z.strictObject({
                            path: z
                                .string()
                                .regex(
                                    /^(?:(?:conf\/)?(?:graphs\/)?[a-zA-Z0-9_.-]+|jvm-observation\/(?:spec|plan|plan\.lock)\.json)$/,
                                ),
                            sha256: z.string().regex(/^[a-f0-9]{64}$/),
                            bytes: z.number().int().nonnegative(),
                        }),
                    )
                    .min(1)
                    .max(32),
            })
            .passthrough(),
        8388608,
    );
    const source = join(attempt, "task.source.json"),
        budget = planned.settings.budget;
    await writeJson(source, {
        schemaVersion: 1,
        kind: "component-task",
        name: "hugegraph-http-" + planned.settings.mode,
        command: [
            process.env.HGBENCH_PYTHON ?? "python3",
            join(attempt, "hugegraph-environment.py"),
            "serve",
            "--attempt",
            attempt,
        ],
        cwd: attempt,
        trackedFiles: [
            join(attempt, "plan.json"),
            join(attempt, "plan.lock.json"),
            ...planned.controlFiles.map((f) => join(attempt, f.path)),
        ],
        effect: "database",
        timeoutMs: (budget.wallSeconds + budget.startupSeconds + budget.shutdownSeconds + 300) * 1000,
        terminateGraceMs: Math.min(30000, (budget.shutdownSeconds + 5) * 1000),
        maxLogBytes: 1048576,
    });
    const frozen = await freezeTask(source);
    const taskPlan = join(attempt, "task.json");
    await writeJson(taskPlan, frozen);
    return {
        ...result,
        taskPlan,
        taskPlanDigest: frozen.digest,
        actualModeVerified: false,
        performanceAccepted: false,
        budgetSemantics:
            "Container TTL is independent of task/SSH. Polling disk protection may overshoot one interval.",
    };
}
export async function httpEnvironmentAction(action: "status" | "stop", attempt: string) {
    return invoke(action, ["--attempt", resolve(attempt)], action === "stop" ? 90000 : 30000);
}
