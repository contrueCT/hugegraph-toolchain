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

import { fork } from "node:child_process";
import { mkdir, open, stat } from "node:fs/promises";
import { hostname } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { z } from "zod";
import { BenchError, errorInfo, fileHash, readJson } from "./io.js";
import { frozenTaskSchema, taskStateSchema } from "./task-contracts.js";
import type { ProcessResult } from "./task-contracts.js";
import { durableJson, processStart, taskDirectory, verifyTask } from "./tasks.js";

export async function taskWork(root: string, id: string, token: string) {
    const dir = taskDirectory(root, id);
    const launch = await readJson(join(dir, "launch.json"), z.object({ token: z.string() }));
    if (token !== launch.token) throw new BenchError("TASK_OWNER_CONFLICT", "Worker does not own this submission", 5);
    try {
        await mkdir(join(dir, ".worker-lock"));
    } catch (error) {
        if ((error as NodeJS.ErrnoException).code === "EEXIST") return;
        throw error;
    }
    const start = await processStart(process.pid);
    if (!start) throw new BenchError("TASK_OWNER_UNAVAILABLE", "Cannot record worker process identity", 3);
    await durableJson(join(dir, "owner.json"), { pid: process.pid, start, token, host: hostname() });
    let state = await readJson(join(dir, "state.json"), taskStateSchema);
    let writes = Promise.resolve();
    const persist = () => {
        state.updatedAt = new Date().toISOString();
        const snapshot = { ...state };
        writes = writes.then(() => durableJson(join(dir, "state.json"), snapshot));
        return writes;
    };
    let sequence = 0;
    const event = async (type: string, data: unknown) => {
        const file = await open(join(dir, "events.ndjson"), "a", 0o600);
        try {
            await file.writeFile(
                JSON.stringify({
                    schemaVersion: 1,
                    taskId: id,
                    sequence: ++sequence,
                    at: new Date().toISOString(),
                    type,
                    data,
                }) + "\n",
            );
            await file.sync();
        } finally {
            await file.close();
        }
    };
    let signalRequested = false;
    const onSignal = () => {
        signalRequested = true;
    };
    process.on("SIGTERM", onSignal);
    process.on("SIGINT", onSignal);
    const cancelled = async () => {
        if (signalRequested) return true;
        try {
            await stat(join(dir, "cancel.json"));
            return true;
        } catch (error) {
            if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
            return false;
        }
    };
    let heartbeatFailure: unknown;
    const heartbeat = setInterval(
        () =>
            void persist().catch((error) => {
                heartbeatFailure = error;
            }),
        1000,
    );
    try {
        state.state = "running";
        state.phase = "preflight";
        await persist();
        await event("started", { host: hostname(), pid: process.pid });
        const identity = await readJson(
            join(dir, "controller.json"),
            z.array(z.object({ path: z.string(), sha256: z.string() })),
        );
        for (const file of identity)
            if ((await fileHash(file.path)) !== file.sha256)
                throw new BenchError("CONTROLLER_CHANGED", "Controller changed after submission", 5);
        const frozen = await readJson(join(dir, "plan.json"), frozenTaskSchema);
        await verifyTask(frozen);
        if (await cancelled()) throw new BenchError("CANCELLED", "Cancelled before dispatch", 6);
        // Persist intent before dispatch. A crash here conservatively leaves effect unknown.
        state.dispatched = true;
        state.effect = frozen.plan.effect === "read-only" ? "none" : "unknown";
        state.phase = "execute";
        await persist();
        await event("dispatch-intent", { planDigest: frozen.digest, effect: frozen.plan.effect });
        const guardian = fork(fileURLToPath(new URL("./task-process.js", import.meta.url)), [], {
            stdio: ["ignore", "inherit", "inherit", "ipc"],
        });
        let result: ProcessResult | undefined;
        guardian.on("message", (message) => {
            if ((message as { type?: string }).type === "result")
                result = (message as { result: ProcessResult }).result;
        });
        const closed = new Promise<void>((done, fail) => {
            guardian.once("error", fail);
            guardian.once("exit", () => done());
        });
        guardian.send({ dir, task: frozen });
        let checking = false;
        const poll = setInterval(async () => {
            if (checking || !guardian.connected) return;
            checking = true;
            try {
                if (heartbeatFailure || (await cancelled())) guardian.send({ type: "cancel" }, () => undefined);
            } catch {
                if (guardian.connected) guardian.send({ type: "cancel" }, () => undefined);
            } finally {
                checking = false;
            }
        }, 100);
        try {
            await closed;
        } finally {
            clearInterval(poll);
        }
        if (!result)
            throw new BenchError(
                "GUARDIAN_LOST",
                "Guardian ended without a result; inspect component and target before any retry",
                6,
            );
        state.processExitCode = result.exitCode;
        if (!result.dispatched) {
            state.dispatched = false;
            state.effect = "none";
        }
        if (heartbeatFailure) throw heartbeatFailure;
        if (result.error) throw new BenchError(result.error.code, result.error.message, 6);
        state.state = "completed";
    } catch (error) {
        const info = errorInfo(error);
        state.state = info.code === "CANCELLED" ? "cancelled" : "failed";
        state.error = { code: info.code, message: info.message };
    } finally {
        clearInterval(heartbeat);
        await writes;
        state.phase = "seal";
        // Keep the on-disk state non-terminal until logs and lifecycle evidence are sealed.
        const finalState = state.state;
        state.state = "running";
        await persist();
        await durableJson(join(dir, "outcome.json"), {
            state: finalState,
            processExitCode: state.processExitCode,
            error: state.error,
            effect: state.effect,
            benchmarkValid: false,
            finishedAt: new Date().toISOString(),
        });
        await event("finished", { state: finalState, error: state.error });
        const files = [];
        for (const name of [
            "plan.json",
            "controller.json",
            "submission.json",
            "events.ndjson",
            "stdout.log",
            "stderr.log",
            "process-result.json",
            "outcome.json",
        ]) {
            const path = join(dir, name);
            try {
                files.push({ path: name, bytes: (await stat(path)).size, sha256: await fileHash(path) });
            } catch (error) {
                if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
            }
        }
        await durableJson(join(dir, "evidence.json"), { schemaVersion: 1, taskId: id, files });
        state.state = finalState;
        state.phase = "finished";
        await persist();
        process.removeListener("SIGTERM", onSignal);
        process.removeListener("SIGINT", onSignal);
    }
}
