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
import { mkdir } from "node:fs/promises";
import { dirname, join } from "node:path";
import type { Component } from "./contracts.js";
import { capabilitySchema } from "./contracts.js";
import { BenchError } from "./io.js";

interface Options {
    timeoutMs: number;
    logPrefix?: string;
    cancelled?: () => Promise<boolean>;
}

// The controller never uses a shell. Each component owns a POSIX process group.
export async function invoke(component: Component, args: string[], options: Options): Promise<string> {
    if (options.logPrefix) await mkdir(dirname(options.logPrefix), { recursive: true, mode: 0o700 });
    return new Promise((resolve, reject) => {
        const child = spawn(component.command[0]!, [...component.command.slice(1), ...args], {
            cwd: dirname(component.manifestPath),
            stdio: ["ignore", "pipe", "pipe"],
            detached: true,
        });
        const out = options.logPrefix
            ? createWriteStream(`${options.logPrefix}.stdout.log`, { mode: 0o600 })
            : undefined;
        const err = options.logPrefix
            ? createWriteStream(`${options.logPrefix}.stderr.log`, { mode: 0o600 })
            : undefined;
        let captured = "";
        let outBytes = 0;
        let errBytes = 0;
        let failure: BenchError | undefined;
        let escalation: NodeJS.Timeout | undefined;
        let done = false;
        const kill = (signal: NodeJS.Signals) => {
            if (!child.pid) return;
            try {
                process.kill(-child.pid, signal);
            } catch (error) {
                if ((error as NodeJS.ErrnoException).code !== "ESRCH") child.kill(signal);
            }
        };
        const stop = (error: BenchError) => {
            if (done || failure) return;
            failure = error;
            kill("SIGTERM");
            escalation = setTimeout(() => kill("SIGKILL"), 250);
        };
        out?.on("error", () => stop(new BenchError("EVIDENCE_WRITE_FAILED", "Cannot persist component stdout", 6)));
        err?.on("error", () => stop(new BenchError("EVIDENCE_WRITE_FAILED", "Cannot persist component stderr", 6)));
        child.stdout.on("data", (chunk: Buffer) => {
            outBytes += chunk.length;
            if (outBytes <= 1024 * 1024) {
                captured += chunk.toString("utf8");
                out?.write(chunk);
            } else stop(new BenchError("COMPONENT_OUTPUT_LIMIT", "Component stdout exceeded 1 MiB", 6));
        });
        child.stderr.on("data", (chunk: Buffer) => {
            errBytes += chunk.length;
            if (errBytes <= 1024 * 1024) err?.write(chunk);
            else stop(new BenchError("COMPONENT_OUTPUT_LIMIT", "Component stderr exceeded 1 MiB", 6));
        });
        const timeout = setTimeout(
            () => stop(new BenchError("COMPONENT_TIMEOUT", `Component exceeded ${options.timeoutMs}ms`, 6)),
            options.timeoutMs,
        );
        let checking = false;
        const poll = setInterval(async () => {
            if (!options.cancelled || checking || done) return;
            checking = true;
            try {
                if (await options.cancelled()) stop(new BenchError("CANCELLED", "Cancellation requested", 6));
            } catch {
                stop(new BenchError("STATE_UNREADABLE", "Cannot check cancellation state", 6));
            } finally {
                checking = false;
            }
        }, 100);
        child.on("error", (error) => {
            failure = new BenchError("COMPONENT_START_FAILED", error.message, 3);
        });
        child.on("close", async (code, signal) => {
            done = true;
            clearTimeout(timeout);
            clearInterval(poll);
            if (escalation) clearTimeout(escalation);
            // Clean up any descendants even if the component leader exited first.
            kill("SIGKILL");
            await Promise.all(
                [out, err].filter(Boolean).map(
                    (stream) =>
                        new Promise<void>((finish) => {
                            if (stream!.destroyed) finish();
                            else stream!.end(finish);
                        }),
                ),
            );
            if (failure) reject(failure);
            else if (code !== 0)
                reject(new BenchError("COMPONENT_FAILED", `Component exited ${code} (${signal ?? "no signal"})`, 6));
            else resolve(captured);
        });
    });
}

export async function describe(component: Component, timeoutMs = 5000, logDir?: string): Promise<unknown> {
    const text = await invoke(component, ["--describe"], {
        timeoutMs,
        logPrefix: logDir ? join(logDir, component.manifest.kind) : undefined,
    });
    try {
        const result = capabilitySchema.parse(JSON.parse(text));
        if (
            result.kind !== component.manifest.kind ||
            result.id !== component.manifest.id ||
            result.version !== component.manifest.version
        )
            throw new Error("Manifest/capability identity mismatch");
        return result;
    } catch (error) {
        throw new BenchError("INVALID_CAPABILITIES", String(error), 3);
    }
}
