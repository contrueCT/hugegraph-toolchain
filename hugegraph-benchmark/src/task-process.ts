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
import { createWriteStream } from "node:fs";
import { open } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { finished } from "node:stream/promises";
import { frozenTaskSchema } from "./task-contracts.js";
import type { FrozenTask, ProcessResult } from "./task-contracts.js";
import { durableJson } from "./tasks.js";

// Separate guardian owns the component group. IPC loss means the worker vanished.
// Components must keep children in this process group; daemonizing is unsupported.
async function guard(dir: string, task: FrozenTask) {
    const result: ProcessResult = {
        dispatched: false,
        exitCode: null,
        signal: null,
        error: null,
        stdoutBytes: 0,
        stderrBytes: 0,
    };
    const child = spawn(task.plan.command[0]!, task.plan.command.slice(1), {
        cwd: task.plan.cwd,
        detached: true,
        stdio: ["ignore", "pipe", "pipe"],
        env: { ...process.env, HGBENCH_TASK_DIR: dir },
    });
    let escalation: NodeJS.Timeout | undefined;
    let closed = false;
    const kill = (signal: NodeJS.Signals) => {
        if (!child.pid) return;
        try {
            process.kill(-child.pid, signal);
        } catch (error) {
            if ((error as NodeJS.ErrnoException).code !== "ESRCH")
                result.error = { code: "TASK_TERMINATION_FAILED", message: String(error) };
        }
    };
    const stop = (code: string, message: string) => {
        if (closed || result.error) return;
        result.error = { code, message };
        kill("SIGTERM");
        escalation = setTimeout(() => kill("SIGKILL"), task.plan.terminateGraceMs);
    };
    const streams = (["stdout", "stderr"] as const).map((name) => {
        const out = createWriteStream(join(dir, `${name}.log`), { flags: "wx", mode: 0o600 });
        const completion = finished(out).catch((error) => {
            stop("EVIDENCE_WRITE_FAILED", String(error));
            result.error = { code: "EVIDENCE_WRITE_FAILED", message: String(error) };
        });
        const source = child[name];
        out.on("error", (error) => {
            stop("EVIDENCE_WRITE_FAILED", String(error));
            // A failed destination never emits drain; consume until group termination closes the pipe.
            source.resume();
        });
        source.on("data", (chunk: Buffer) => {
            const key = name === "stdout" ? "stdoutBytes" : "stderrBytes";
            const before = result[key];
            result[key] += chunk.length;
            const remaining = Math.max(0, task.plan.maxLogBytes - before);
            if (remaining && !out.destroyed && !out.write(chunk.subarray(0, remaining))) {
                source.pause();
                out.once("drain", () => source.resume());
            }
            if (result[key] > task.plan.maxLogBytes)
                stop("TASK_LOG_LIMIT", `${name} exceeded ${task.plan.maxLogBytes} bytes`);
        });
        return { out, completion };
    });
    const timeout = setTimeout(
        () => stop("TASK_TIMEOUT", `Component exceeded ${task.plan.timeoutMs} ms`),
        task.plan.timeoutMs,
    );
    const disconnected = () => stop("WORKER_DISCONNECTED", "Worker IPC closed; stopping owned component group");
    process.once("disconnect", disconnected);
    if (!process.connected) disconnected();
    process.on("message", (message) => {
        if ((message as { type?: string })?.type === "cancel") stop("CANCELLED", "Cancellation requested");
    });
    process.once("SIGTERM", () => stop("CANCELLED", "Guardian termination requested"));
    process.once("SIGINT", () => stop("CANCELLED", "Guardian interrupt requested"));
    child.once("spawn", () => {
        result.dispatched = true;
    });
    child.once("error", (error) => {
        result.error = { code: "COMPONENT_START_FAILED", message: error.message };
    });
    // Exit (not close) fires even when a descendant inherited and holds stdout open.
    child.once("exit", () => kill("SIGKILL"));
    const cleanupOnExit = () => kill("SIGKILL");
    process.once("exit", cleanupOnExit);
    await new Promise<void>((done) =>
        child.once("close", (code, signal) => {
            result.exitCode = code;
            result.signal = signal;
            done();
        }),
    );
    closed = true;
    clearTimeout(timeout);
    if (escalation) clearTimeout(escalation);
    kill("SIGKILL");
    process.removeListener("exit", cleanupOnExit);
    for (const { out } of streams) out.end();
    await Promise.all(streams.map((s) => s.completion));
    if (result.error?.code !== "EVIDENCE_WRITE_FAILED") {
        for (const name of ["stdout", "stderr"]) {
            const file = await open(join(dir, `${name}.log`), "r");
            try {
                await file.sync();
            } finally {
                await file.close();
            }
        }
    }
    if (!result.error && result.exitCode !== 0)
        result.error = {
            code: "COMPONENT_FAILED",
            message: `Component exited ${result.exitCode} (${result.signal ?? "no signal"})`,
        };
    await durableJson(join(dir, "process-result.json"), result);
    process.removeListener("disconnect", disconnected);
    if (process.connected) {
        await new Promise<void>((resolve) => process.send!({ type: "result", result }, () => resolve()));
        process.disconnect!();
    }
}
if (process.argv[1] === fileURLToPath(import.meta.url) && process.send) {
    const startup = setTimeout(() => process.exit(6), 10000);
    process.once("message", (message) => {
        clearTimeout(startup);
        const { dir, task } = message as { dir: string; task: unknown };
        void guard(dir, frozenTaskSchema.parse(task)).catch((error) => {
            process.stderr.write(`${String(error)}\n`);
            process.exitCode = 6;
            if (process.connected) process.disconnect!();
        });
    });
}
