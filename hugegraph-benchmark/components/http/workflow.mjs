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
import { copyFile, mkdir, stat, readdir } from "node:fs/promises";
import { arch, cpus, hostname, platform, release, totalmem } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { argumentsFor, digest, entry, json, put } from "./common.mjs";
import { verifySuiteEvidence } from "./suite-evidence.mjs";
import { probe } from "./probe.mjs";
import { report } from "./analyzer.mjs";

const componentRoot = dirname(fileURLToPath(import.meta.url));
export async function runWorkflow(plan, { inPlace = false } = {}) {
    let cancelled = false,
        failure = null;
    const onSignal = () => {
        cancelled = true;
    };
    if (inPlace) {
        process.on("SIGTERM", onSignal);
        process.on("SIGINT", onSignal);
    }
    try {
        const output = inPlace ? plan : join(plan, "output");
        const profile = await json(join(plan, "profile.json"));
        const target = await json(join(plan, "target-evidence.json"));
        const go = profile.schemaVersion === 2 && profile.executor?.engine === "go-http-v1";
        const engine = go ? await import("./go-engine.mjs") : null;
        const executor = go ? await json(join(plan, "executor.json")) : null;
        if (go) engine.validateGoRegistration(executor, profile);
        let executeGo = null;
        if (go && !cancelled) {
            try {
                executeGo = await engine.createGoExecution(profile);
            } catch (error) {
                if (!cancelled) throw error;
            }
        }
        if (inPlace) await put(join(output, "execution-started.json"), { startedAt: new Date().toISOString() });
        else await mkdir(output, { mode: 0o700 });
        const files = new Set(),
            runs = [];
        const track = (file) => {
            files.add(file);
            return join(output, file);
        };
        for (const file of ["profile.json", "target-evidence.json", ...(go ? ["executor.json"] : [])]) {
            if (!inPlace) await copyFile(join(plan, file), track(file));
            else track(file);
        }
        if (profile.benchmark) {
            const lock = await json(join(plan, "benchmark/bundle.lock.json"));
            await verifySuiteEvidence(plan, profile);
            if (!inPlace) await mkdir(join(output, "benchmark"));
            for (const name of ["bundle.lock.json", ...lock.files.map((f) => f.path)]) {
                if (!inPlace) await copyFile(join(plan, "benchmark", name), track(`benchmark/${name}`));
                else track(`benchmark/${name}`);
            }
        }
        for (const folder of ["requests", "runs", "traces", "target"]) await mkdir(join(output, folder));
        for (const [i, file] of target.files.entries()) {
            const dest = track(`target/${i}.json`);
            await copyFile(file.path, dest);
            if ((await digest(dest)) !== file.sha256) throw new Error("Target evidence changed during copy");
        }
        await put(track("implementation.json"), {
            schemaVersion: go ? 2 : 1,
            ...(go ? { engine: "go-http-v1", executor } : {}),
            node: process.version,
            nodeExecutableSha256: await digest(process.execPath),
            hostname: hostname(),
            platform: platform(),
            release: release(),
            arch: arch(),
            cpus: cpus().length,
            cpuModel: cpus()[0]?.model,
            hostMemoryBytes: totalmem(),
            files: await Promise.all(
                [
                    "workflow.mjs",
                    "runner.mjs",
                    "trace-writer.mjs",
                    "direct-phase-clock.mjs",
                    "runner-sanity.mjs",
                    "phase-clock-reference.py",
                    "clock-correlation.mjs",
                    "analyzer.mjs",
                    "report-view.mjs",
                    "build-info.mjs",
                    "common.mjs",
                    "suite-evidence.mjs",
                    "probe.mjs",
                    ...(go ? ["go-engine.mjs"] : []),
                ].map(async (file) => ({ file, sha256: await digest(join(componentRoot, file)) })),
            ),
            timing: go
                ? "Go time.Time monotonic elapsed with raw Linux CLOCK_MONOTONIC brackets; host association requires new independent verification"
                : "Node performance.now per phase; raw hrtime brackets preserve actual boundaries including drain; host association requires separate verification",
            resourceIsolation: "External task/container/cgroup controls must be recorded separately",
            workflow: {
                layout: inPlace ? "in-place" : "planned-output",
                probes: "in-process",
                analysis: "in-process",
                goIdentity: go ? "once-per-workflow plus native pre-request identity in every Go process" : null,
            },
        });
        let traceBytes = 0,
            unfinished = null;
        async function child(args) {
            if (go) return executeGo(args);
            await new Promise((done, reject) => {
                const p = spawn(process.execPath, [join(componentRoot, "runner.mjs"), ...args], {
                    stdio: ["ignore", "inherit", "inherit"],
                });
                p.on("error", reject);
                p.on("close", (code, signal) =>
                    code === 0 ? done() : reject(new Error(`runner exited ${code} (${signal})`)),
                );
            });
        }
        async function serviceCheck(phase) {
            if (!profile.serviceChecks || cancelled) return;
            const result = await probe(
                profile,
                phase,
                join(output, phase),
                profile.maxTraceBytes - traceBytes,
                executeGo,
            );
            track(`${phase}/result.json`);
            for (const file of result.files) track(`${phase}/${file.path}`);
            traceBytes += result.traceBytes;
            if (traceBytes > profile.maxTraceBytes) throw new Error("Probe exhausted workflow trace budget");
            if (!result.passed) throw new Error("HTTP service probe failed; inspect partial probe evidence");
        }
        try {
            await serviceCheck("preflight");
            for (let repeat = 0; repeat < profile.repeats; repeat++)
                for (const item of profile.cases)
                    for (const load of profile.loads) {
                        if (cancelled) throw new Error("HTTP workflow cancelled");
                        const id = `r${String(runs.length + 1).padStart(4, "0")}`;
                        if (traceBytes >= profile.maxTraceBytes) throw new Error("Workflow trace budget exhausted");
                        const request = track(`requests/${id}.json`),
                            trace = track(`traces/${id}.ndjson`),
                            result = track(`runs/${id}.json`);
                        unfinished = { id, caseId: item.id, loadId: load.id, repeat, result: `runs/${id}.json` };
                        await put(request, {
                            schemaVersion: go ? 2 : 1,
                            id,
                            profile,
                            case: item,
                            load,
                            repeat,
                            remainingTraceBytes: profile.maxTraceBytes - traceBytes,
                        });
                        let childError;
                        try {
                            await child(["--request", request, "--trace", trace, "--output", result]);
                        } catch (error) {
                            childError = error;
                        }
                        let observed;
                        try {
                            observed = await json(result);
                        } catch (error) {
                            throw childError ?? error;
                        }
                        // A failure receipt preserves partial raw data under result. It must
                        // never enter measurement accounting, even if that nested phase started.
                        if (go && observed?.kind === "http-go-run-failure")
                            throw new Error(
                                `Go run ${id} failed: ${String(observed.error ?? "missing failure reason").slice(0, 700)}`,
                                { cause: childError },
                            );
                        if (!Array.isArray(observed?.phases))
                            throw childError ?? new Error(`Run ${id} has no phase records`);
                        if (go && profile.executor.hostIdentityGate) {
                            await engine.verifyGoIdentityGates(result, observed, profile);
                            for (const path of engine.goIdentityFiles(`runs/${id}.json`)) track(path);
                        }
                        // A cancelled warmup has no measurement; retain it as unfinished data,
                        // without manufacturing a measurement row or changing the denominator.
                        if (!observed.phases.some((phase) => phase.name === "measurement"))
                            throw childError ?? new Error("Measurement was not started");
                        traceBytes += observed.traceBytes;
                        if (traceBytes > profile.maxTraceBytes) throw new Error("Workflow trace budget exceeded");
                        runs.push(id);
                        unfinished = null;
                        process.stdout.write(
                            JSON.stringify({ event: "http-run-completed", id, case: item.id, load: load.id, repeat }) +
                                "\n",
                        );
                        if (childError) throw childError;
                        if (observed.phases.some((phase) => phase.cancelled)) throw new Error("HTTP run was cancelled");
                    }
            await serviceCheck("postcheck");
            if (cancelled) throw new Error("HTTP workflow cancelled");
        } catch (error) {
            if (!inPlace) throw error;
            failure = String(error).slice(0, 1000);
        }
        // Preserve failed/cancelled probe artifacts too; only the analyzer decides
        // which complete records can be verified and displayed as observations.
        if (failure)
            for (const phase of ["preflight", "postcheck"]) {
                try {
                    for (const name of await readdir(join(output, phase)))
                        if (/^(result\.json|p\d{4}\.(json|ndjson|request\.json))$/.test(name))
                            track(`${phase}/${name}`);
                } catch (error) {
                    if (error.code !== "ENOENT") throw error;
                }
            }
        const inventory = [];
        for (const path of files) {
            try {
                inventory.push({
                    path,
                    sha256: await digest(join(output, path)),
                    bytes: (await stat(join(output, path))).size,
                });
            } catch (error) {
                if (error.code !== "ENOENT") throw error;
            }
        }
        const completed = new Set(runs);
        const partialFiles = failure
            ? inventory.filter((f) => {
                  const match = /^(requests|runs|traces)\/(r\d{4})\./.exec(f.path);
                  return match && !completed.has(match[2]);
              })
            : [];
        const partialNames = new Set(partialFiles.map((f) => f.path));
        await put(join(output, "raw.lock.json"), {
            schemaVersion: go ? 3 : 2,
            ...(go ? { engine: "go-http-v1" } : {}),
            kind: "http-raw-evidence",
            serviceChecks: Boolean(profile.serviceChecks),
            runs,
            files: inventory.filter((f) => !partialNames.has(f.path)),
            ...(inPlace
                ? {
                      execution: {
                          state: failure ? "partial" : "complete",
                          reason: failure,
                          ...(unfinished ? { unfinished } : {}),
                          plannedRuns: profile.cases.length * profile.loads.length * profile.repeats,
                      },
                      partialFiles,
                  }
                : {}),
        });
        await report(output, join(output, "analysis"));
        await put(join(output, failure ? "partial.json" : "completed.json"), {
            schemaVersion: 1,
            kind: failure ? "http-workflow-partial" : "http-workflow-completed",
            runs: runs.length,
            rawManifestSha256: await digest(join(output, "raw.lock.json")),
            reportSha256: await digest(join(output, "analysis/report.json")),
            reason: failure,
            performanceAccepted: false,
        });
        if (failure) throw new Error(failure);
    } finally {
        if (inPlace) {
            process.removeListener("SIGTERM", onSignal);
            process.removeListener("SIGINT", onSignal);
        }
    }
}
if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href)
    await entry(async () => {
        const flat = process.argv.includes("--layout");
        const args = argumentsFor(flat ? ["--plan", "--layout"] : ["--plan"]);
        if (flat && args["--layout"] !== "in-place") throw new Error("Unknown HTTP layout");
        await runWorkflow(resolve(args["--plan"]), { inPlace: flat });
    }, "HTTP workflow: --plan NEW-plan [--layout in-place]\nOne execution, shared probes/analysis, independent measurement processes; task owns cancellation.");
