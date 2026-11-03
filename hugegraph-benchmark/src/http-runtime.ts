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
import { join, resolve } from "node:path";
import { BenchError, readJson } from "./io.js";
import { packageRoot } from "./plans.js";
import { httpRuntimeProvenanceSpecSchema } from "./http-runtime-contracts.js";

// One fixed replaceable component owns collection, validation and precise Docker
// cleanup. The TypeScript entry point only supervises bounded JSON and signals.
async function invoke(args: string[], timeoutMs: number, cleanupMs: number) {
    return new Promise<Record<string, unknown>>((accept, reject) => {
        const grouped = process.platform !== "win32";
        const child = spawn(
            process.env.HGBENCH_PYTHON ?? "python3",
            ["-B", join(packageRoot, "components/http/runtime-provenance.py"), ...args],
            { detached: grouped, stdio: ["ignore", "pipe", "pipe"] },
        );
        const stdout: Buffer[] = [];
        const stderr: Buffer[] = [];
        let capturedBytes = 0;
        let failure: BenchError | undefined;
        let force: NodeJS.Timeout | undefined;
        function signal(name: NodeJS.Signals) {
            if (!child.pid) return;
            try {
                if (grouped) process.kill(-child.pid, name);
                else child.kill(name);
            } catch (error) {
                if ((error as NodeJS.ErrnoException).code !== "ESRCH")
                    failure ??= new BenchError("HTTP_RUNTIME_CLEANUP", "Cannot signal runtime component group", 6);
            }
        }
        function stop(error: BenchError) {
            if (failure) return;
            failure = error;
            signal("SIGTERM");
            force = setTimeout(() => signal("SIGKILL"), cleanupMs + 3000);
        }
        const interrupt = () => stop(new BenchError("HTTP_RUNTIME_CANCELLED", "Runtime probe interrupted", 130));
        const terminate = () => stop(new BenchError("HTTP_RUNTIME_CANCELLED", "Runtime probe terminated", 143));
        process.on("SIGINT", interrupt);
        process.on("SIGTERM", terminate);
        const deadline = setTimeout(
            () => stop(new BenchError("HTTP_RUNTIME_TIMEOUT", "Runtime component exceeded its deadline", 6)),
            timeoutMs,
        );
        for (const [stream, chunks] of [
            [child.stdout, stdout],
            [child.stderr, stderr],
        ] as const)
            stream.on("data", (chunk: Buffer) => {
                const remaining = Math.max(0, 1024 * 1024 - capturedBytes);
                if (remaining) chunks.push(chunk.subarray(0, remaining));
                capturedBytes += chunk.length;
                if (capturedBytes > 1024 * 1024)
                    stop(new BenchError("HTTP_RUNTIME_OUTPUT", "Runtime component exceeded its JSON output budget", 6));
            });
        child.once("error", (error: NodeJS.ErrnoException) => {
            failure ??= new BenchError(
                "HTTP_RUNTIME_UNAVAILABLE",
                "Cannot start the runtime provenance component",
                error.code === "ENOENT" ? 3 : 6,
            );
        });
        child.once("close", (code) => {
            clearTimeout(deadline);
            clearTimeout(force);
            process.removeListener("SIGINT", interrupt);
            process.removeListener("SIGTERM", terminate);
            // A component leader exiting early must not leave pipe-owning
            // descendants. This group belongs solely to this invocation.
            signal("SIGKILL");
            if (failure) return reject(failure);
            let reply: { ok?: boolean; data?: unknown; error?: unknown };
            try {
                reply = JSON.parse(Buffer.concat(stdout).toString("utf8"));
            } catch {
                return reject(
                    new BenchError("HTTP_RUNTIME_PROTOCOL", "Runtime component did not return bounded JSON", 6),
                );
            }
            if (!reply || typeof reply !== "object" || Array.isArray(reply))
                return reject(
                    new BenchError("HTTP_RUNTIME_PROTOCOL", "Runtime component returned invalid envelope", 6),
                );
            if (code !== 0 || reply.ok !== true)
                return reject(
                    new BenchError(
                        "HTTP_RUNTIME_FAILED",
                        "Runtime provenance failed; inspect preserved proof and cleanup state",
                        code === 2 || code === 3 ? code : 6,
                        false,
                        reply.error ?? { diagnostic: Buffer.concat(stderr).toString("utf8").slice(0, 2048) },
                    ),
                );
            if (!reply.data || typeof reply.data !== "object" || Array.isArray(reply.data))
                return reject(new BenchError("HTTP_RUNTIME_PROTOCOL", "Runtime component returned invalid data", 6));
            accept(reply.data as Record<string, unknown>);
        });
    });
}

export async function probeHttpRuntime(specFile: string, runtimeFile: string, outputDirectory: string) {
    const spec = await readJson(specFile, httpRuntimeProvenanceSpecSchema, 65536);
    return invoke(
        ["probe", "--spec", resolve(specFile), "--runtime", resolve(runtimeFile), "--out", resolve(outputDirectory)],
        spec.budgets.wallMs + spec.budgets.cleanupMs + 5000,
        spec.budgets.cleanupMs,
    );
}

export async function httpRuntimeAction(action: "describe" | "verify" | "stop", proof?: string) {
    return invoke([action, ...(proof === undefined ? [] : ["--proof", resolve(proof)])], 30000, 10000);
}
