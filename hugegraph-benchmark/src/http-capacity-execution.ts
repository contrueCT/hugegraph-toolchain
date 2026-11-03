/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
import { spawn } from "node:child_process";
import { basename, dirname, join, resolve } from "node:path";
import { realpath } from "node:fs/promises";
import { isDeepStrictEqual } from "node:util";
import { z } from "zod";
import { BenchError, readJson, writeJson } from "./io.js";
import { packageRoot } from "./plans.js";
import { freezeTask } from "./tasks.js";
import { httpCapacityExecutionRuntimeSchema } from "./http-capacity-execution-contracts.js";
export { httpCapacityExecutionRuntimeSchema } from "./http-capacity-execution-contracts.js";

const positive = z.number().int().positive();
const absolute = z
    .string()
    .startsWith("/")
    .max(2048)
    .regex(/^[^\r\n\0,]+$/);

async function invoke(args: string[], timeoutMs: number, signal?: AbortSignal): Promise<Record<string, unknown>> {
    if (signal?.aborted) throw new BenchError("HTTP_CAPACITY_CANCELLED", "Capacity command was cancelled", 130);
    return new Promise((accept, reject) => {
        const grouped = process.platform !== "win32";
        const child = spawn(
            process.env.HGBENCH_PYTHON ?? "python3",
            ["-B", join(packageRoot, "components/http/capacity-workflow.py"), ...args],
            {
                detached: grouped,
                stdio: ["ignore", "pipe", "pipe"],
            },
        );
        let failure: BenchError | undefined,
            used = 0,
            force: ReturnType<typeof setTimeout> | undefined;
        const stdout: Buffer[] = [],
            stderr: Buffer[] = [];
        function kill(name: NodeJS.Signals) {
            if (!child.pid) return;
            try {
                if (grouped) process.kill(-child.pid, name);
                else child.kill(name);
            } catch (error) {
                if ((error as NodeJS.ErrnoException).code !== "ESRCH")
                    failure ??= new BenchError("HTTP_CAPACITY_CLEANUP", "Unable to stop the component group", 6);
            }
        }
        function stop(error: BenchError) {
            if (failure) return;
            failure = error;
            kill("SIGTERM");
            force = setTimeout(() => kill("SIGKILL"), 30000);
        }
        const interrupt = () => stop(new BenchError("HTTP_CAPACITY_CANCELLED", "Capacity command interrupted", 130));
        const terminate = () => stop(new BenchError("HTTP_CAPACITY_CANCELLED", "Capacity command terminated", 143));
        process.on("SIGINT", interrupt);
        process.on("SIGTERM", terminate);
        signal?.addEventListener("abort", interrupt, { once: true });
        const deadline = setTimeout(
            () => stop(new BenchError("HTTP_CAPACITY_TIMEOUT", "Capacity component exceeded its deadline", 6)),
            timeoutMs,
        );
        for (const [stream, target] of [
            [child.stdout, stdout],
            [child.stderr, stderr],
        ] as const)
            stream.on("data", (chunk: Buffer) => {
                const left = Math.max(0, 16 * 1024 ** 2 - used);
                if (left) target.push(chunk.subarray(0, left));
                used += chunk.length;
                if (used > 16 * 1024 ** 2)
                    stop(new BenchError("HTTP_CAPACITY_OUTPUT", "Capacity component exceeded its output cap", 6));
            });
        child.once("error", () => {
            failure ??= new BenchError("HTTP_CAPACITY_UNAVAILABLE", "Cannot start the capacity component", 3);
        });
        child.once("close", (code) => {
            clearTimeout(deadline);
            clearTimeout(force);
            process.removeListener("SIGINT", interrupt);
            process.removeListener("SIGTERM", terminate);
            signal?.removeEventListener("abort", interrupt);
            if (failure) return reject(failure);
            let reply: Record<string, unknown>;
            try {
                reply = JSON.parse(Buffer.concat(stdout).toString("utf8"));
            } catch {
                return reject(
                    new BenchError("HTTP_CAPACITY_PROTOCOL", "Invalid capacity component JSON", 6, false, {
                        diagnostic: Buffer.concat(stderr).toString("utf8").slice(0, 2048),
                    }),
                );
            }
            if (
                code !== 0 ||
                reply.ok !== true ||
                !reply.data ||
                typeof reply.data !== "object" ||
                Array.isArray(reply.data)
            )
                return reject(
                    new BenchError(
                        "HTTP_CAPACITY_EXECUTION_FAILED",
                        "Capacity component failed; inspect preserved evidence and the original identity",
                        6,
                        false,
                        reply.error ?? reply.data,
                    ),
                );
            accept(reply.data as Record<string, unknown>);
        });
    });
}

export async function prepareHttpCapacityExecution(
    runtimeFile: string,
    capacityDirectory: string,
    out: string,
    options: { signal?: AbortSignal } = {},
) {
    const input = await readJson(resolve(runtimeFile), httpCapacityExecutionRuntimeSchema, 65536);
    const directory = join(await realpath(dirname(resolve(out))), basename(out));
    const reply = await invoke(
        ["prepare", "--runtime", resolve(runtimeFile), "--plan", resolve(capacityDirectory), "--out", directory],
        input.budgets.prepareMilliseconds + 30000,
        options.signal,
    );
    const workflow = await readJson(
        join(directory, "workflow.json"),
        z
            .object({
                runtime: z.object({
                    budgets: httpCapacityExecutionRuntimeSchema.shape.budgets,
                    comparisonRuntime: z.object({
                        hostPython: absolute,
                        schemaVersion: z.union([z.literal(1), z.literal(2)]).default(1),
                    }),
                }),
                maxRunMilliseconds: positive.min(30000).max(7 * 86400000),
            })
            .passthrough(),
        16 * 1024 ** 2,
    );
    if (
        reply.attempt !== directory ||
        reply.executed !== false ||
        typeof reply.planSha256 !== "string" ||
        !/^[a-f0-9]{64}$/.test(reply.planSha256) ||
        !isDeepStrictEqual(workflow.runtime.budgets, input.budgets) ||
        reply.maxRunMilliseconds !== workflow.maxRunMilliseconds
    )
        throw new BenchError("HTTP_CAPACITY_PROTOCOL", "Prepared capacity identity or budgets changed", 6);
    const phase = reply.phase,
        planFiles =
            phase === "exploration"
                ? ["capacity-plan.json", "capacity.lock.json"]
                : phase === "confirmation"
                  ? ["confirmation-plan.json", "confirmation.lock.json"]
                  : null;
    if (!planFiles || !isDeepStrictEqual(reply.planFiles, planFiles))
        throw new BenchError("HTTP_CAPACITY_PROTOCOL", "Prepared capacity phase or plan inventory changed", 6);
    const scientificPlan = await readJson(
        join(directory, "capacity", planFiles[0]!),
        z.object({ phase: z.enum(["exploration", "confirmation"]), planDigest: z.string() }).passthrough(),
        16 * 1024 ** 2,
    );
    if (scientificPlan.phase !== phase)
        throw new BenchError("HTTP_CAPACITY_PROTOCOL", "Prepared capacity phase does not match its frozen plan", 6);
    const source = join(directory, "task.source.json");
    await writeJson(source, {
        schemaVersion: 1,
        kind: "component-task",
        name: "http-capacity-" + phase,
        command: [
            workflow.runtime.comparisonRuntime.hostPython,
            "-B",
            join(directory, "capacity-workflow.py"),
            "run",
            "--attempt",
            directory,
        ],
        cwd: directory,
        trackedFiles: [
            "capacity-workflow.py",
            "calibration-workflow.py",
            "comparison-workflow.py",
            "comparison-runtime.json",
            "execution-runtime.json",
            "workflow.json",
            "workflow.lock.json",
            ...planFiles.map((name) => "capacity/" + name),
            ...(workflow.runtime.comparisonRuntime.schemaVersion === 2
                ? ["go-comparison-runtime.py", "go-process-observer.py", "go-clock-probe.py"]
                : []),
        ].map((name) => join(directory, name)),
        effect: "database",
        timeoutMs: workflow.maxRunMilliseconds,
        terminateGraceMs: 30000,
        maxLogBytes: 1048576,
    });
    const task = await freezeTask(source),
        taskPlan = join(directory, "task.json");
    await writeJson(taskPlan, task);
    return {
        ...reply,
        taskPlan,
        taskPlanDigest: task.digest,
        replayAllowed: false,
        executed: false,
        performanceAccepted: false,
    };
}

export function httpCapacityExecution(
    action: "describe" | "status" | "stop",
    attempt?: string,
    options: { signal?: AbortSignal } = {},
) {
    if (action !== "describe" && !attempt)
        throw new BenchError("HTTP_CAPACITY_ATTEMPT_REQUIRED", "Use the original prepared attempt directory");
    return invoke(
        [action, ...(attempt ? ["--attempt", resolve(attempt)] : [])],
        action === "stop" ? 30000 : 330000,
        options.signal,
    );
}
