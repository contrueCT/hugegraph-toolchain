/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
import { copyFile, mkdir, realpath, stat, writeFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { BenchError, fileHash, writeJson } from "./io.js";
import { packageRoot } from "./plans.js";
import { freezeTaskPlan } from "./tasks.js";
import { httpSuiteDirectory, verifyHttpSuiteProfile } from "./http-suite.js";
import type { HttpProfile, HttpGoProfile } from "./http-contracts.js";

export type HttpInput = {
    profile: HttpProfile | HttpGoProfile;
    suite: Awaited<ReturnType<typeof verifyHttpSuiteProfile>>;
};
export type HttpExecutionOptions = { inPlace?: boolean; timeoutMs?: number; terminateGraceMs?: number };

// Node and Go share one asset snapshot, one task description and one result layout.
export async function freezeHttpExecution(
    input: HttpInput,
    file: string,
    directory: string,
    defaultTimeoutMs: number,
    options: HttpExecutionOptions = {},
    go?: { registration: unknown; trackedFiles: string[] },
) {
    const { profile, suite } = input;
    const component = join(packageRoot, "components/http");
    const programs = [
        "workflow.mjs",
        "runner.mjs",
        "trace-writer.mjs",
        "direct-phase-clock.mjs",
        "runner-sanity.mjs",
        "phase-clock-reference.py",
        "clock-correlation.mjs",
        "analyzer.mjs",
        "graph-validation.mjs",
        "report-view.mjs",
        "build-info.mjs",
        "common.mjs",
        "suite-evidence.mjs",
        "probe.mjs",
        ...(go ? ["go-engine.mjs"] : []),
    ].map((name) => join(component, name));
    const attachments = await Promise.all(
        profile.target.evidenceFiles.map(async (name) => {
            const path = await realpath(resolve(dirname(resolve(file)), name));
            const info = await stat(path);
            if (!info.isFile() || info.size > 8 * 1024 * 1024)
                throw new BenchError("HTTP_TARGET_EVIDENCE", "Target evidence must be a regular file <=8 MiB");
            return { path, sha256: await fileHash(path), bytes: info.size };
        }),
    );
    if (profile.graph && !attachments.some((f) => f.sha256 === profile.graph!.reference.artifactSha256))
        throw new BenchError(
            "GRAPH_REFERENCE_CHANGED",
            "Original independent reference export is missing or changed; no task was dispatched",
        );
    if (attachments.reduce((sum, f) => sum + f.bytes, 0) > 32 * 1024 * 1024)
        throw new BenchError("HTTP_TARGET_EVIDENCE", "Combined target evidence exceeds 32 MiB");
    const dir = resolve(directory);
    await mkdir(dirname(dir), { recursive: true, mode: 0o700 });
    await mkdir(dir, { mode: 0o700 });
    const suiteFiles: string[] = [];
    if (suite) {
        await mkdir(join(dir, "benchmark"), { mode: 0o700 });
        for (const name of suite.files) {
            const path = join(dir, "benchmark", name);
            await copyFile(join(httpSuiteDirectory, name), path);
            suiteFiles.push(path);
        }
    }
    await writeJson(join(dir, "profile.json"), {
        ...profile,
        target: { ...profile.target, evidenceFiles: attachments.map((f) => f.path) },
    });
    await writeJson(join(dir, "target-evidence.json"), {
        schemaVersion: 1,
        kind: "operator-supplied-target-evidence",
        files: attachments,
        attestation: "File identity only; deployment and measurement-window verification remain required",
    });
    if (go) await writeJson(join(dir, "executor.json"), go.registration);
    const task = {
        schemaVersion: 1,
        kind: "component-task",
        name: profile.name,
        command: [
            process.execPath,
            join(component, "workflow.mjs"),
            "--plan",
            dir,
            ...(options.inPlace ? ["--layout", "in-place"] : []),
        ],
        cwd: packageRoot,
        trackedFiles: [
            ...programs,
            ...suiteFiles,
            join(dir, "profile.json"),
            join(dir, "target-evidence.json"),
            ...attachments.map((f) => f.path),
            ...(go ? [join(dir, "executor.json"), ...go.trackedFiles] : []),
        ],
        effect: "database",
        timeoutMs: options.timeoutMs ?? defaultTimeoutMs,
        terminateGraceMs: options.terminateGraceMs ?? 5000,
        maxLogBytes: 1048576,
    };
    const frozen = await freezeTaskPlan(task);
    if (!options.inPlace) {
        // Preserve the advanced, explicitly reviewable plan API.
        await writeJson(join(dir, "task.source.json"), task);
        await writeJson(join(dir, "task.json"), frozen);
        await writeFile(
            join(dir, "README.txt"),
            "One execution per plan. Use hgbench task run/status/cancel. Never remove output/ to replay.\n",
            { mode: 0o600 },
        );
    }
    return {
        directory: dir,
        taskPlan: join(dir, "task.json"),
        planDigest: frozen.digest,
        frozen,
        outputDirectory: options.inPlace ? dir : join(dir, "output"),
        ...(go ? { engine: "go-http-v1" } : {}),
        executed: false,
        performanceAccepted: false,
    };
}
