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

import { mkdir, stat } from "node:fs/promises";
import { randomUUID } from "node:crypto";
import { dirname, join, resolve } from "node:path";
import { inspectHttpInput, httpDeadline, prepareHttpExecution } from "./http.js";
import { goBaseProfile } from "./http-go.js";
import { BenchError, fileHash, readJson, hash } from "./io.js";
import { z } from "zod";
import { submitTask, taskRequestId, durableJson } from "./tasks.js";

// Convenience controller only. Measurement, validation, report and cancellation stay in existing components.
export async function runHttpProfile(
    file: string,
    options: {
        out: string;
        root: string;
        requestId?: string;
        dryRun?: boolean;
        budgetMs?: number;
        budgetMode?: "total" | "stage-sum";
        executionDeadlineMs?: number;
    },
) {
    if (options.requestId !== undefined) taskRequestId(options.requestId);
    const profileFile = resolve(file),
        info = await stat(profileFile);
    if (!info.isFile() || info.size > 4 * 1024 * 1024)
        throw new BenchError("HTTP_RUN_PROFILE_FILE", "Use a regular profile file no larger than 4 MiB");
    const before = await fileHash(profileFile),
        input = await inspectHttpInput(profileFile),
        profile = input.profile;
    if (profile.schemaVersion === 2 && profile.executor.hostIdentityGate)
        throw new BenchError(
            "HTTP_RUN_HOST_OBSERVER_REQUIRED",
            "This profile enables host identity handshakes; use its controlled comparison task with the host observer. No settings were changed.",
        );
    const base = profile.schemaVersion === 2 ? goBaseProfile(profile) : profile;
    const derivedMs = Math.ceil(httpDeadline(base) + 60000);
    const budgetMs = options.budgetMs ?? derivedMs + 30000;
    if (!Number.isSafeInteger(budgetMs) || budgetMs < 1000 || budgetMs > 7 * 86400000)
        throw new BenchError("HTTP_RUN_BUDGET", "budget-ms must be an integer from 1000 to 604800000");
    if (options.budgetMode === "stage-sum" && budgetMs < derivedMs + 30000)
        throw new BenchError(
            "HTTP_RUN_BUDGET",
            "stage-sum budget must cover the derived execution and 30s cleanup bound",
        );
    const cleanupReserveMs = Math.min(30000, Math.max(100, Math.floor(budgetMs / 4)));
    const directory = resolve(options.out),
        root = resolve(options.root);
    const preview = {
        directory,
        root,
        profileFile,
        engine: profile.schemaVersion === 2 ? "go-http-v1" : "node-http-v1",
        units: profile.cases.length * profile.loads.length * profile.repeats,
        fixedWindowMs:
            profile.cases.length * profile.loads.length * profile.repeats * (profile.warmupMs + profile.durationMs),
        maxExecutionMs: budgetMs,
        budgetMode: options.budgetMode ?? "total",
        cleanupReserveMs,
        scope: "Descriptive HTTP run against the configured endpoint; deployment provenance and performance qualification are separate.",
        budgetSemantics:
            "One execution deadline covers probes, warmup, measurement, drain, report and reserved termination. Offline input preparation and task submission have separate bounds. No windows or questions are shortened.",
        performanceAccepted: false,
    };
    if (options.dryRun) return { ...preview, written: false, dispatched: false };
    await mkdir(dirname(directory), { recursive: true, mode: 0o700 });
    try {
        await mkdir(directory, { mode: 0o700 });
    } catch (error) {
        if ((error as NodeJS.ErrnoException).code === "EEXIST")
            throw new BenchError(
                "HTTP_RUN_EXISTS",
                "Run directory already exists. Inspect run.json or intent.json and query its original task; choose a new directory for a new run.",
                4,
            );
        throw error;
    }
    const requestId = options.requestId ?? "http-" + randomUUID();
    const paths = {
        rawDirectory: join(directory, "evidence"),
        reportJson: join(directory, "evidence/analysis/report.json"),
        reportHtml: join(directory, "evidence/analysis/report.html"),
    };
    const pointer = {
        schemaVersion: 1,
        kind: "http-run",
        ...preview,
        ...paths,
        written: true,
        requestId,
        taskId: taskRequestId(requestId),
        profileSha256: before,
    };
    await durableJson(join(directory, "run.json"), { ...pointer, submissionAcknowledged: false });
    const planned = await prepareHttpExecution(input, profileFile, paths.rawDirectory, {
        inPlace: true,
        timeoutMs: budgetMs - cleanupReserveMs,
        terminateGraceMs: cleanupReserveMs,
    });
    if ((await fileHash(profileFile)) !== before)
        throw new BenchError(
            "HTTP_RUN_PROFILE_CHANGED",
            "Profile changed during preparation; no task was submitted. Keep this attempt and use a new directory.",
            5,
        );
    if (options.executionDeadlineMs !== undefined) {
        const timeoutMs = Math.floor(options.executionDeadlineMs - performance.now() - cleanupReserveMs);
        if (timeoutMs < 1)
            throw new BenchError(
                "HTTP_RUN_DEADLINE",
                "Whole-run deadline expired during preparation; no task submitted",
            );
        const { digest: _, ...content } = planned.frozen;
        content.plan = { ...content.plan, timeoutMs };
        planned.frozen = { ...content, digest: hash(content) };
        planned.planDigest = planned.frozen.digest;
    }
    const task = await submitTask(root, planned.frozen, requestId);
    const run = {
        ...pointer,
        submissionAcknowledged: true,
        taskId: task.taskId,
        planDigest: planned.planDigest,
        taskPlan: join(root, "tasks", task.taskId, "plan.json"),
    };
    await durableJson(join(directory, "run.json"), run);
    return { ...run, task };
}

export async function httpRunSummary(file: string) {
    try {
        const report = await readJson(
            file,
            z.object({
                verdict: z.string(),
                execution: z.unknown().optional(),
                gates: z.record(z.string(), z.unknown()),
                runs: z.array(
                    z.object({
                        caseId: z.string(),
                        loadId: z.string(),
                        repeat: z.number(),
                        scheduled: z.number(),
                        correct: z.number(),
                        failed: z.number(),
                        dropped: z.number(),
                        late: z.number(),
                        correctCompletedInWindowPerSecond: z.number(),
                        issues: z.array(z.string()),
                    }),
                ),
            }),
            8 * 1024 * 1024,
        );
        return {
            reportAvailable: true,
            verdict: report.verdict,
            ...(report.execution ? { execution: report.execution } : {}),
            gates: report.gates,
            runs: report.runs,
            performanceAccepted: false,
        };
    } catch (error) {
        return {
            reportAvailable: false,
            reason: error instanceof BenchError ? error.code : "REPORT_UNAVAILABLE",
            performanceAccepted: false,
        };
    }
}
