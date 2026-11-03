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

import { appendFile, mkdir, writeFile } from "node:fs/promises";
import { arch, cpus, platform, release, totalmem } from "node:os";
import { join } from "node:path";
import { z } from "zod";
import {
    analysisRequestSchema,
    analysisResultSchema,
    executionRequestSchema,
    executionResultSchema,
    resolvedSchema,
    stateSchema,
} from "./contracts.js";
import type { ExecutionResult, ExecutionRequest, State } from "./contracts.js";
import { describe, invoke } from "./components.js";
import { BenchError, errorInfo, experimentDir, fileHash, readJson, writeJson } from "./io.js";
import { verifyPlan } from "./plans.js";
import { isCancelled, seal } from "./store.js";

function random(seed: number) {
    return () => {
        seed |= 0;
        seed = (seed + 0x6d2b79f5) | 0;
        let value = Math.imul(seed ^ (seed >>> 15), 1 | seed);
        value = (value + Math.imul(value ^ (value >>> 7), 61 | value)) ^ value;
        return ((value ^ (value >>> 14)) >>> 0) / 4294967296;
    };
}

export function validateResult(result: ExecutionResult, request: ExecutionRequest): void {
    for (const key of ["experimentId", "runId", "caseId", "variantId", "block"] as const) {
        if (result[key] !== request[key]) throw new BenchError("RESULT_IDENTITY_MISMATCH", `Result ${key} mismatch`, 6);
    }
    if (
        result.validation.checked !== result.samples.length ||
        result.validation.failed !== result.samples.filter((sample) => !sample.correct).length
    ) {
        throw new BenchError("RESULT_COUNTS_MISMATCH", "Validation counts do not match sample records", 6);
    }
}

export async function work(root: string, id: string): Promise<void> {
    const dir = experimentDir(root, id);
    try {
        await mkdir(join(dir, ".worker-lock"));
    } catch (error) {
        if ((error as NodeJS.ErrnoException).code === "EEXIST") return;
        throw error;
    }
    let state = await readJson(join(dir, "state.json"), stateSchema);
    let writes = Promise.resolve();
    const persist = () => {
        state.updatedAt = new Date().toISOString();
        const value = { ...state };
        writes = writes.then(() => writeJson(join(dir, "state.json"), value));
        return writes;
    };
    const event = async (type: string, data: unknown) => {
        await appendFile(
            join(dir, "events.ndjson"),
            `${JSON.stringify({
                schemaVersion: 1,
                experimentId: id,
                at: new Date().toISOString(),
                type,
                data,
            })}\n`,
            { mode: 0o600 },
        );
    };
    let heartbeatFailure: unknown;
    const heartbeat = setInterval(() => {
        void persist().catch((error) => {
            heartbeatFailure = error;
        });
    }, 1000);
    const cancelled = async () => {
        if (heartbeatFailure) throw heartbeatFailure;
        return isCancelled(dir);
    };
    const assertActive = async () => {
        if (await cancelled()) throw new BenchError("CANCELLED", "Cancellation requested", 6);
    };
    const transition = async (phase: string) => {
        state.phase = phase;
        await persist();
        await event("phase", { phase });
    };
    let outcome: State["state"] = "completed";
    try {
        state.state = "running";
        state.workerPid = process.pid;
        await transition("preflight");
        await assertActive();
        const identity = await readJson(
            join(dir, "controller.json"),
            z.array(z.object({ path: z.string(), sha256: z.string() })),
        );
        for (const file of identity) {
            if ((await fileHash(file.path)) !== file.sha256) {
                throw new BenchError("CONTROLLER_CHANGED", "Controller changed after submission", 5);
            }
        }
        const plan = await readJson(join(dir, "plan.json"), resolvedSchema);
        await verifyPlan(plan);
        await writeJson(join(dir, "runtime.json"), {
            toolVersion: plan.toolVersion,
            node: process.version,
            platform: platform(),
            release: release(),
            arch: arch(),
            cpu: cpus()[0]?.model,
            logicalCpus: cpus().length,
            totalMemoryBytes: totalmem(),
            scope: plan.scope,
            isolation: "local-shared-development-host",
            startedAt: new Date().toISOString(),
        });
        const capabilities = [];
        for (const component of [plan.executor, plan.analyzer]) {
            await assertActive();
            capabilities.push(
                await describe(component, Math.min(5000, plan.plan.componentTimeoutMs), join(dir, "capabilities")),
            );
        }
        await writeJson(join(dir, "capabilities.json"), capabilities);
        const rng = random(plan.plan.seed);
        const schedule: ExecutionRequest[] = [];
        for (let block = 0; block < plan.plan.blocks; block++) {
            for (const item of plan.plan.cases) {
                const variants = rng() < 0.5 ? plan.plan.variants : [...plan.plan.variants].reverse();
                for (const variant of variants) {
                    schedule.push(
                        executionRequestSchema.parse({
                            protocolVersion: 1,
                            kind: "execution",
                            scope: plan.scope,
                            experimentId: id,
                            runId: `r${String(schedule.length + 1).padStart(4, "0")}`,
                            caseId: item.id,
                            variantId: variant.id,
                            block,
                            seed: plan.plan.seed + block,
                            caseConfig: item.config,
                            variantConfig: variant.config,
                        }),
                    );
                }
            }
        }
        await writeJson(join(dir, "schedule.json"), schedule);
        const results: ExecutionResult[] = [];
        let resultBytes = 0;
        await transition("execute");
        for (const request of schedule) {
            await assertActive();
            await verifyPlan(plan);
            const stage = join(dir, "runs", request.runId);
            const input = join(stage, "request.json");
            const output = join(stage, "result.json");
            await writeJson(input, request);
            await event("run-started", { runId: request.runId, caseId: request.caseId, variantId: request.variantId });
            await invoke(plan.executor, ["--request", input, "--output", output], {
                timeoutMs: plan.plan.componentTimeoutMs,
                logPrefix: join(stage, "component"),
                cancelled,
            });
            const result = await readJson(output, executionResultSchema);
            validateResult(result, request);
            resultBytes += Buffer.byteLength(JSON.stringify(result));
            if (resultBytes > 32 * 1024 * 1024)
                throw new BenchError("EVIDENCE_BUDGET", "Results exceeded 32 MiB MVP budget", 6);
            results.push(result);
            state.completedRuns++;
            await persist();
            await event("run-completed", { runId: request.runId, validation: result.validation });
        }
        await assertActive();
        await transition("analyze");
        await verifyPlan(plan);
        const input = join(dir, "analysis", "request.json");
        const output = join(dir, "analysis", "result.json");
        await writeJson(
            input,
            analysisRequestSchema.parse({
                protocolVersion: 1,
                kind: "analysis",
                scope: plan.scope,
                experimentId: id,
                planDigest: plan.digest,
                plan: plan.plan,
                results,
            }),
        );
        await invoke(plan.analyzer, ["--request", input, "--output", output], {
            timeoutMs: plan.plan.componentTimeoutMs,
            logPrefix: join(dir, "analysis", "component"),
            cancelled,
        });
        await assertActive();
        const analysis = await readJson(output, analysisResultSchema);
        if (analysis.experimentId !== id || analysis.planDigest !== plan.digest) {
            throw new BenchError("ANALYSIS_IDENTITY_MISMATCH", "Analyzer returned a result for different inputs", 6);
        }
        const failed = results.some((result) => result.validation.failed > 0);
        if ((analysis.verdict === "correctness-failed") !== failed) {
            throw new BenchError(
                "ANALYSIS_VALIDATION_MISMATCH",
                "Analyzer disagrees with executor validation counts",
                6,
            );
        }
        state.verdict = analysis.verdict;
        await writeFile(join(dir, "report.html"), analysis.reportHtml, { mode: 0o600 });
        const { reportHtml, ...scorecard } = analysis;
        await writeJson(join(dir, "scorecard.json"), scorecard);
    } catch (error) {
        const info = errorInfo(error);
        outcome = info.code === "CANCELLED" ? "cancelled" : "failed";
        state.error = { code: info.code, message: info.message };
        await event("error", state.error);
    } finally {
        clearInterval(heartbeat);
        await writes;
        await transition("seal");
        await writeJson(join(dir, "outcome.json"), {
            state: outcome,
            verdict: state.verdict,
            error: state.error,
            completedRuns: state.completedRuns,
            totalRuns: state.totalRuns,
            finishedAt: new Date().toISOString(),
        });
        await event("finished", { state: outcome });
        try {
            await seal(dir, id);
        } catch (error) {
            outcome = "failed";
            state.error = { code: "SEAL_FAILED", message: String(error) };
        }
        state.state = outcome;
        state.phase = "finished";
        await persist();
    }
}
