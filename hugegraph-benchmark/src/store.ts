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

import { randomUUID } from "node:crypto";
import { closeSync, openSync } from "node:fs";
import { access, mkdir, readFile, readdir, stat, writeFile } from "node:fs/promises";
import { spawn } from "node:child_process";
import { join, resolve, relative } from "node:path";
import { z } from "zod";
import type { Resolved, State } from "./contracts.js";
import { resolvedSchema, stateSchema, terminal } from "./contracts.js";
import { BenchError, delay, experimentDir, fileHash, hash, readJson, writeJson } from "./io.js";
import { controllerIdentity, packageRoot, verifyPlan } from "./plans.js";

export function requestExperiment(requestId: string): string {
    if (!/^[\x21-\x7e]{1,128}$/.test(requestId)) {
        throw new BenchError("INVALID_REQUEST_ID", "request-id must be 1–128 visible ASCII characters");
    }
    return `exp-${hash(requestId).slice(0, 32)}`;
}

export async function snapshot(root: string, id: string) {
    const dir = experimentDir(root, id);
    const state = await readJson(join(dir, "state.json"), stateSchema);
    let workerAlive = false;
    let pid = state.workerPid;
    try {
        const owner = await readJson(join(dir, "worker.json"), z.object({ pid: z.number().int().positive() }));
        pid = owner.pid;
    } catch {
        /* Submission may still be recording its worker. */
    }
    if (pid) {
        try {
            process.kill(pid, 0);
            workerAlive = true;
        } catch (error) {
            workerAlive = (error as NodeJS.ErrnoException).code === "EPERM";
        }
    }
    const age = Date.now() - Date.parse(state.updatedAt);
    const interrupted = !terminal(state) && !workerAlive && age > 3000;
    const stalled = !terminal(state) && workerAlive && age > 15000;
    return {
        ...state,
        terminal: terminal(state) || interrupted,
        interrupted,
        stalled,
        workerAlive,
        evidenceDir: dir,
        pollAfterMs: terminal(state) || interrupted ? 0 : 1000,
    };
}

export async function submit(root: string, plan: Resolved, requestId?: string, parentExperimentId?: string) {
    await verifyPlan(plan);
    const id = requestId ? requestExperiment(requestId) : `exp-${randomUUID()}`;
    const dir = experimentDir(root, id);
    await mkdir(join(resolve(root), "experiments"), { recursive: true, mode: 0o700 });
    try {
        await mkdir(dir, { mode: 0o700 });
    } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
        for (let attempt = 0; attempt < 20; attempt++) {
            try {
                const prior = await readJson(
                    join(dir, "submission.json"),
                    z.object({
                        planDigest: z.string(),
                        parentExperimentId: z.string().optional(),
                    }),
                );
                if (prior.planDigest !== plan.digest || prior.parentExperimentId !== parentExperimentId) {
                    throw new BenchError("REQUEST_ID_CONFLICT", "request-id already belongs to different inputs", 5);
                }
                return { ...(await snapshot(root, id)), reused: true };
            } catch (readError) {
                if (readError instanceof BenchError && readError.code === "REQUEST_ID_CONFLICT") throw readError;
                await delay(100);
            }
        }
        throw new BenchError(
            "SUBMISSION_IN_PROGRESS",
            `Submission reserved as ${id}; query status before retrying`,
            4,
            true,
        );
    }
    const now = new Date().toISOString();
    const state: State = {
        schemaVersion: 1,
        experimentId: id,
        planDigest: plan.digest,
        state: "queued",
        phase: "queued",
        createdAt: now,
        updatedAt: now,
        completedRuns: 0,
        totalRuns: plan.plan.blocks * plan.plan.cases.length * plan.plan.variants.length,
        ...(parentExperimentId ? { parentExperimentId } : {}),
    };
    await writeJson(join(dir, "plan.json"), plan);
    await writeJson(join(dir, "controller.json"), await controllerIdentity());
    await writeJson(join(dir, "state.json"), state);
    await writeJson(join(dir, "submission.json"), { planDigest: plan.digest, parentExperimentId });
    const log = openSync(join(dir, "controller.log"), "a", 0o600);
    try {
        const worker = spawn(process.execPath, [join(packageRoot, "dist", "cli.js"), "__worker", resolve(root), id], {
            detached: true,
            stdio: ["ignore", log, log],
            cwd: packageRoot,
        });
        await new Promise<void>((done, fail) => {
            worker.once("spawn", done);
            worker.once("error", fail);
        });
        await writeJson(join(dir, "worker.json"), { pid: worker.pid });
        worker.unref();
    } catch (error) {
        await writeJson(join(dir, "state.json"), {
            ...state,
            state: "failed",
            phase: "submission",
            error: { code: "WORKER_START_FAILED", message: String(error) },
            updatedAt: new Date().toISOString(),
        });
        throw new BenchError("WORKER_START_FAILED", `Worker failed to start; inspect ${id}`, 6);
    } finally {
        closeSync(log);
    }
    return { ...(await snapshot(root, id)), reused: false };
}

export async function wait(root: string, id: string, timeoutMs: number) {
    const deadline = Date.now() + timeoutMs;
    while (true) {
        const state = await snapshot(root, id);
        if (state.terminal || Date.now() >= deadline) return state;
        await delay(Math.min(100, Math.max(1, deadline - Date.now())));
    }
}

export async function cancel(root: string, id: string) {
    const state = await snapshot(root, id);
    if (!state.terminal) await writeFile(join(experimentDir(root, id), "cancel"), "requested\n", { mode: 0o600 });
    return { ...state, cancellationRequested: !state.terminal };
}

export async function isCancelled(dir: string): Promise<boolean> {
    try {
        await access(join(dir, "cancel"));
        return true;
    } catch (error) {
        if ((error as NodeJS.ErrnoException).code === "ENOENT") return false;
        throw error;
    }
}

export async function resume(root: string, id: string, requestId?: string) {
    const state = await snapshot(root, id);
    if (!state.terminal || state.state === "completed" || (state.interrupted && state.workerAlive)) {
        throw new BenchError("RESUME_CONFLICT", "Resume requires a failed, cancelled, or interrupted experiment", 5);
    }
    const plan = await readJson(join(experimentDir(root, id), "plan.json"), resolvedSchema);
    return submit(root, plan, requestId, id);
}

const evidenceSchema = z.strictObject({
    schemaVersion: z.literal(1),
    experimentId: z.string(),
    files: z.array(z.strictObject({ path: z.string(), sha256: z.string(), bytes: z.number() })),
});

export async function seal(dir: string, experimentId: string): Promise<void> {
    const files: { path: string; sha256: string; bytes: number }[] = [];
    async function walk(folder: string) {
        for (const item of await readdir(folder, { withFileTypes: true })) {
            if (
                item.name.startsWith(".") ||
                ["evidence.json", "state.json", "worker.json", "controller.log", "cancel"].includes(item.name)
            )
                continue;
            const path = join(folder, item.name);
            if (item.isDirectory()) await walk(path);
            else if (item.isFile())
                files.push({ path: relative(dir, path), sha256: await fileHash(path), bytes: (await stat(path)).size });
        }
    }
    await walk(dir);
    files.sort((a, b) => a.path.localeCompare(b.path));
    await writeJson(join(dir, "evidence.json"), { schemaVersion: 1, experimentId, files });
}

export async function evidence(root: string, id: string) {
    const dir = experimentDir(root, id);
    const manifest = await readJson(join(dir, "evidence.json"), evidenceSchema);
    for (const file of manifest.files) {
        const full = resolve(dir, file.path);
        if (!full.startsWith(`${dir}/`) || (await fileHash(full)) !== file.sha256) {
            throw new BenchError("EVIDENCE_CHANGED", `Evidence integrity check failed: ${file.path}`, 6);
        }
    }
    return manifest;
}
