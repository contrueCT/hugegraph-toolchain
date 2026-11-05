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

import { mkdir, readFile, writeFile, statfs, stat } from "node:fs/promises";
import { cpus, hostname, platform, arch, release, totalmem } from "node:os";
import { join, resolve, dirname } from "node:path";
import { spawn, execFile } from "node:child_process";
import { once } from "node:events";
import { promisify } from "node:util";
import { z } from "zod";
import { inspectHttpInput, validateHttpProfile } from "./http.js";
import { httpAnyProfileSchema, type HttpGoProfile, type HttpProfile } from "./http-contracts.js";
import { goBaseProfile } from "./http-go.js";
import { inspectHttpSuite } from "./http-suite.js";
import { runHttpProfile } from "./http-run.js";
import { cancelTask, waitTask, durableJson } from "./tasks.js";
import { BenchError, fileHash, hash, readJson } from "./io.js";
import { packageRoot } from "./plans.js";
import {
    throughputPresets,
    graphThroughputPresets,
    throughputSettingsSchema,
    throughputFixedMs,
    runThroughputSchedule,
    httpPointHealth,
} from "./throughput.js";

const settingsSchema = z.strictObject(throughputSettingsSchema.shape).partial();
const quickCases = ["scalar-add", "rows-4096", "string-1024k"];
type Profile = HttpProfile | HttpGoProfile;
// The analyzer remains the only reader of raw evidence.
async function component(action: string, path: string, out: string, timeout: number) {
    if (timeout < 1) throw new BenchError("THROUGHPUT_DEADLINE", "Whole-run deadline reached");
    const { stdout } = await promisify(execFile)(
        process.execPath,
        [join(packageRoot, "components/http/throughput-reader.mjs"), action, path, out],
        { timeout: Math.max(1, Math.floor(timeout)), maxBuffer: 8 * 1024 * 1024, killSignal: "SIGKILL" },
    );
    return JSON.parse(stdout);
}
async function optionalText(path: string) {
    try {
        return (await readFile(path, "utf8")).trim();
    } catch {
        return null;
    }
}
async function driverIdentity(p: Profile) {
    return {
        policy: "light-http-driver-v1",
        node: process.version,
        hostname: hostname(),
        platform: platform(),
        arch: arch(),
        release: release(),
        cpus: cpus().map((c) => c.model),
        totalmem: totalmem(),
        boot: await optionalText("/proc/sys/kernel/random/boot_id"),
        cpuFeatures: hash(
            (await optionalText("/proc/cpuinfo"))
                ?.split("\n")
                .filter((s) => /^(flags|Features|model name|microcode)\s*:/.test(s)) ?? [],
        ),
        quota: await optionalText("/sys/fs/cgroup/cpu.max"),
        memoryMax: await optionalText("/sys/fs/cgroup/memory.max"),
        executor:
            p.schemaVersion === 2
                ? { ...p.executor, binary: { ...p.executor.binary, path: undefined } }
                : { engine: "node", sha256: await fileHash(process.execPath) },
        implementation: await Promise.all(
            ["runner.mjs", "analyzer.mjs", "go-engine.mjs", "light-provider.mjs", "generator-provider.mjs"].map(
                async (f) => [f, await fileHash(join(packageRoot, "components/http", f))],
            ),
        ),
        requestTimeoutMs: p.requestTimeoutMs,
        drainMs: p.drainMs,
        maxResponseBytes: p.maxResponseBytes,
        authorization: Boolean(p.authorizationEnv),
        encoding: p.responseEncoding,
        validation: "full-canonical-answer",
        connection: "http1-keepalive-default",
    };
}
function eligible(row: any) {
    return httpPointHealth(row).eligible;
}
export async function runHttpThroughput(
    file: string,
    options: {
        out: string;
        root: string;
        preset: string;
        settings?: string;
        cache?: string;
        dryRun?: boolean;
        budgetMs?: number;
        signal?: AbortSignal;
        calibrationOnly?: boolean;
        budgetMode?: "total" | "stage-sum";
        refreshCalibration?: boolean;
        track?: "http" | "graph";
    },
) {
    const started = performance.now();
    const graph = options.track === "graph";
    const presets = graph ? graphThroughputPresets : throughputPresets;
    if (!Object.hasOwn(presets, options.preset)) throw new BenchError("THROUGHPUT_PRESET", "Use quick or standard");
    const inputFile = resolve(file),
        input = await inspectHttpInput(inputFile);
    let base = input.profile;
    if (base.schemaVersion === 2 && base.executor.hostIdentityGate)
        throw new BenchError(
            "THROUGHPUT_HOST_OBSERVER",
            "Use the controlled comparison workflow for profiles requiring host handshakes; no setting was disabled",
        );
    if (
        graph &&
        (!base.graph || base.schemaVersion !== 2 || base.cases.length !== 5 || base.cases.some((c) => !c.requestPool))
    )
        throw new BenchError(
            "GRAPH_SUITE",
            "Use graph prepare with the five independent reference pools and Go executor",
        );
    if (!graph && (base.graph || base.cases.some((c) => c.requestPool)))
        throw new BenchError("THROUGHPUT_TRACK", "Use graph run for a graph profile");
    if (graph) {
        const { verifyGraphProfile } = await import("./graph.js");
        verifyGraphProfile(base);
    }
    if (!graph && !base.benchmark)
        throw new BenchError(
            "THROUGHPUT_SUITE",
            "Use a generated HTTP Serving suite profile with independently verified answers",
        );
    if (base.schemaVersion === 2)
        base = {
            ...base,
            executor: {
                ...base.executor,
                binary: { ...base.executor.binary, path: resolve(dirname(inputFile), base.executor.binary.path) },
            },
        };
    base = {
        ...base,
        target: { ...base.target, evidenceFiles: base.target.evidenceFiles.map((f) => resolve(dirname(inputFile), f)) },
    };
    if (!options.dryRun && base.schemaVersion === 2) {
        const engine = await import(join(packageRoot, "components/http/go-engine.mjs"));
        await engine.inspectGoBinary(base.executor.binary);
    }
    const suite = graph ? { ...base.graph!, cases: base.cases } : await inspectHttpSuite();
    const ids = !graph && options.preset === "quick" ? quickCases : suite.cases.map((c) => c.id);
    const cases = ids.map((id) => suite.cases.find((c) => c.id === id)!);
    const settings = throughputSettingsSchema.parse({
        ...presets[options.preset as keyof typeof presets],
        ...(options.settings ? await readJson(resolve(options.settings), settingsSchema, 65536) : {}),
    });
    const directory = resolve(options.out),
        root = resolve(options.root);
    const cache = resolve(options.cache ?? join(root, "calibration"));
    const fixedWindowMs = options.calibrationOnly ? 80000 : throughputFixedMs(cases.length, settings);
    const defaultBudgetMs =
        !graph && options.preset === "standard" && !options.settings && !options.calibrationOnly
            ? 5400000
            : fixedWindowMs + 180000 + cases.length * 30000;
    const budgetMs = options.budgetMs ?? defaultBudgetMs;
    if (!Number.isSafeInteger(budgetMs) || budgetMs < 1000 || budgetMs > 7 * 86400000)
        throw new BenchError("THROUGHPUT_BUDGET", "budget-ms must be an integer from 1000 to 604800000");
    const conservativeBudgetMs =
        fixedWindowMs +
        80000 +
        (2 + (options.calibrationOnly ? 0 : cases.length * 2)) * (60000 + 4 * base.drainMs + 2 * base.requestTimeoutMs);
    if (options.budgetMode === "stage-sum" && budgetMs < conservativeBudgetMs)
        throw new BenchError(
            "THROUGHPUT_STAGE_BUDGET",
            `stage-sum requires at least ${conservativeBudgetMs} ms; total can preserve partial results with a smaller cap`,
        );
    const identity = {
        ...(await driverIdentity(base)),
        ...(graph
            ? {
                  graphProtocol: "graph-pool-v1",
                  validator: await fileHash(join(packageRoot, "components/http/graph-validation.mjs")),
                  provider: await fileHash(join(packageRoot, "components/http/graph-provider.mjs")),
                  pools: hash(base.cases),
              }
            : {}),
    };
    const graphValidation = graph ? await import(join(packageRoot, "components/http/graph-validation.mjs")) : null;
    const representatives: [string, number][] = graph
        ? [
              ["point", 1000],
              [
                  base.cases
                      .filter((c) => c.id !== "point")
                      .reduce(
                          (largest, c) => {
                              const size = (x: typeof c) =>
                                  x.requestPool!.reduce(
                                      (max, r) =>
                                          Math.max(
                                              max,
                                              Buffer.byteLength(JSON.stringify(graphValidation!.graphResponse(r))),
                                          ),
                                      0,
                                  );
                              return size(c) > size(largest) ? c : largest;
                          },
                          base.cases.find((c) => c.id !== "point")!,
                      ).id,
                  40,
              ],
          ]
        : [
              ["scalar-add", 1000],
              ["rows-4096", 40],
          ];
    const key = hash({
        identity,
        suite: suite.bundleSha256,
        requestCap: settings.maxRequestsPerPhase,
        traceCap: settings.maxTraceBytes,
    });
    const preview = {
        schemaVersion: 1,
        kind: graph ? "graph-throughput-run" : "http-throughput-run",
        version: "0.2.0",
        track: graph ? "graph" : "http",
        preset: options.preset,
        calibrationOnly: Boolean(options.calibrationOnly),
        customSettings: Boolean(options.settings),
        suite: { suiteId: suite.suiteId, version: suite.version, bundleSha256: suite.bundleSha256 },
        ...(graph
            ? {
                  graph: base.graph,
                  parameterPools: base.cases.map((c) => ({
                      caseId: c.id,
                      size: c.requestPool!.length,
                      sha256: hash(c.requestPool),
                  })),
              }
            : {}),
        cases: ids,
        settings,
        fixedWindowMs,
        defaultBudgetMs,
        firstCalibrationWindowMs: 80000,
        budgetMs,
        budgetMode: options.budgetMode ?? "total",
        conservativeBudgetMs,
        cleanupReserveMs: 5000,
        cacheKey: key,
        cache,
        encoding: base.responseEncoding,
        requestCapPerPhase: settings.maxRequestsPerPhase,
        traceCapPerChild: settings.maxTraceBytes,
        maxTotalTraceBytes: settings.maxTotalTraceBytes,
        scope: graph
            ? `End-to-end graph queries over HTTP; declared SF${input.profile.graph!.dataset.scaleFactor} source and recorded target import coverage, deterministic frozen pools, independent semantic references. Observed throughput, no automatic capacity certification.`
            : "Closed-loop HTTP service throughput; existing endpoint, same JVM unless externally changed. No graph queries or automatic capacity certification.",
        refreshCalibration: Boolean(options.refreshCalibration),
        performanceAccepted: false,
    };
    if (options.dryRun) return { ...preview, written: false, dispatched: false };
    await mkdir(dirname(directory), { recursive: true, mode: 0o700 });
    try {
        await mkdir(directory, { mode: 0o700 });
    } catch (e) {
        if ((e as NodeJS.ErrnoException).code === "EEXIST")
            throw new BenchError(
                "THROUGHPUT_EXISTS",
                "Existing attempt is never replayed; inspect state.json and its original active task",
            );
        throw e;
    }
    const deadline = started + budgetMs - 5000;
    let activeTask: string | null = null,
        stopped = false;
    const stages: any[] = [],
        selections: any[] = [];
    const state: any = {
        ...preview,
        directory,
        inputSha256: await fileHash(inputFile),
        status: "running",
        stages,
        selections,
        calibration: null,
        activeTask: null,
        failure: null,
        usedTraceBytes: 0,
        incomplete: ids.map((caseId) => ({ caseId, stage: "exploration-and-confirmation" })),
    };
    await durableJson(join(directory, "state.json"), state);
    await writeFile(join(directory, "plan.json"), JSON.stringify({ ...preview, profile: base }, null, 2) + "\n", {
        flag: "wx",
        mode: 0o600,
    });
    state.planSha256 = await fileHash(join(directory, "plan.json"));
    await durableJson(join(directory, "state.json"), state);
    let provider: ReturnType<typeof spawn> | null = null;
    async function stopProvider() {
        if (!provider || provider.exitCode !== null || provider.signalCode !== null) return;
        const child = provider,
            exited = once(child, "exit");
        const killer = setTimeout(() => child.kill("SIGKILL"), 3000);
        child.kill("SIGTERM");
        try {
            await exited;
        } finally {
            clearTimeout(killer);
            provider = null;
        }
    }
    const onAbort = () => {
        stopped = true;
        if (activeTask) void cancelTask(root, activeTask);
    };
    options.signal?.addEventListener("abort", onAbort);
    if (options.signal?.aborted) onAbort();
    const remaining = () => Math.floor(deadline - performance.now());
    const save = async () => {
        state.activeTask = activeTask;
        await durableJson(join(directory, "state.json"), state);
    };
    async function execute(
        label: string,
        selected: Profile["cases"],
        loads: Profile["loads"],
        warmupMs: number,
        durationMs: number,
        repeats: number,
        endpoint = base.endpoint,
    ) {
        if (stopped || remaining() < 1000)
            throw new BenchError(
                stopped ? "THROUGHPUT_CANCELLED" : "THROUGHPUT_DEADLINE",
                "Stopped with partial results; no measurement windows were shortened",
            );
        const disk = await statfs(directory);
        if (Number(disk.bavail) * Number(disk.bsize) < settings.minFreeBytes)
            throw new BenchError("THROUGHPUT_DISK", "Free disk is below minFreeBytes; preserving completed evidence");
        const traceBudget = Math.min(settings.maxTraceBytes, settings.maxTotalTraceBytes - state.usedTraceBytes);
        if (traceBudget < 4096)
            throw new BenchError(
                "THROUGHPUT_TRACE_BUDGET",
                "Whole-run trace budget exhausted; completed evidence retained",
            );
        const p = httpAnyProfileSchema.parse({
            ...base,
            name: label,
            endpoint,
            serviceChecks: true,
            cases:
                graph && endpoint !== base.endpoint
                    ? selected.map((c) => ({
                          ...c,
                          requestPool: c.requestPool!.map((r, i) => ({ ...r, url: endpoint + "/" + c.id + "/" + i })),
                      }))
                    : selected,
            loads,
            benchmark: graph
                ? undefined
                : {
                      ...base.benchmark!,
                      caseIds: selected.map((c) => c.id),
                      coverage: selected.length === suite.cases.length ? "full" : "subset",
                      purpose: "functional",
                  },
            warmupMs,
            durationMs,
            repeats,
            maxRequestsPerPhase: settings.maxRequestsPerPhase,
            maxTraceBytes: traceBudget,
        });
        validateHttpProfile(p.schemaVersion === 2 ? goBaseProfile(p) : p);
        const profileFile = join(directory, label + ".json");
        await writeFile(profileFile, JSON.stringify(p, null, 2) + "\n", { flag: "wx", mode: 0o600 });
        const entry: any = {
            label,
            profileFile,
            caseIds: selected.map((c) => c.id),
            loads,
            stage: label.startsWith("cal-")
                ? "calibration"
                : label.startsWith("explore-")
                  ? "exploration"
                  : "confirmation",
            status: "preparing",
        };
        stages.push(entry);
        await save();
        const run = await runHttpProfile(profileFile, {
            out: join(directory, label),
            root,
            budgetMs: Math.max(1000, remaining()),
            executionDeadlineMs: deadline,
        });
        if (!("taskId" in run)) throw new Error("Missing child task identity");
        activeTask = run.taskId;
        entry.taskId = activeTask;
        entry.rawDirectory = run.rawDirectory;
        entry.status = "running";
        await save();
        if (stopped) await cancelTask(root, activeTask);
        let task = await waitTask(root, activeTask, 1000);
        while (!task.terminal) {
            if (stopped || remaining() <= 0) await cancelTask(root, activeTask);
            task = await waitTask(root, activeTask, 1000);
        }
        activeTask = null;
        entry.status = task.state;
        await save();
        const result = await component("analyze", run.rawDirectory, "", remaining()); // independently verify, even if a child failed
        const lock = JSON.parse(await readFile(join(run.rawDirectory, "raw.lock.json"), "utf8"));
        entry.traceBytes = [...lock.files, ...(lock.partialFiles ?? [])]
            .filter((f) => f.path.endsWith(".ndjson"))
            .reduce((sum, f) => sum + f.bytes, 0);
        state.usedTraceBytes += entry.traceBytes;
        entry.rows = result.runs;
        entry.verdict = result.verdict;
        entry.execution = result.execution;
        await save();
        if (task.state !== "completed" || result.verdict === "partial")
            throw new BenchError(
                "THROUGHPUT_CHILD_PARTIAL",
                `${label}: ${result.execution?.reason ?? "Child did not complete; its full planned denominator and raw evidence are retained"}`.slice(
                    0,
                    1000,
                ),
            );
        return result;
    }
    try {
        await mkdir(cache, { recursive: true, mode: 0o700 });
        const cachedFile = join(cache, key + ".json");
        let reused = false;
        try {
            if (options.refreshCalibration) throw new Error("explicit-refresh-requested");
            const cached = await readJson(cachedFile, z.record(z.string(), z.any()), 1024 * 1024);
            if (
                cached.key !== key ||
                hash(cached.identity) !== hash(identity) ||
                !["passed", "limited"].includes(cached.status)
            )
                throw new Error("cache identity/status");
            if (!Array.isArray(cached.records) || cached.records.length !== 2 || !cached.providerRoot)
                throw new Error("cache coverage missing");
            for (const [index, c] of cached.records.entries()) {
                if (c.caseId !== representatives[index]![0] || c.rate !== representatives[index]![1])
                    throw new Error("cache coverage changed");
                const report = await component("analyze", c.rawDirectory, "", remaining());
                if (
                    report.profile.warmupMs !== 10000 ||
                    report.profile.durationMs !== 30000 ||
                    report.profile.loads[0]?.ratePerSecond !== c.rate ||
                    report.profile.cases.length !== 1 ||
                    report.profile.cases[0].id !== c.caseId ||
                    report.runs.length !== 1 ||
                    hash(report.runs) !== c.rowsSha256 ||
                    Boolean(c.passed) !== report.runs.every(eligible) ||
                    (graph &&
                        (c.maxDecodedBytes !== report.runs[0]?.decodedBodyBytes?.max ||
                            hash(report.profile.graph) !== hash(base.graph)))
                )
                    throw new Error("cache raw verification");
            }
            const providerRaw = await readFile(join(cached.providerRoot, "provider/summary.json"));
            const sampleRaw = await readFile(join(cached.providerRoot, "provider/samples.ndjson"));
            if (
                hash(providerRaw.toString()) !== cached.providerSummarySha256 ||
                hash(sampleRaw.toString()) !== cached.providerSamplesSha256
            )
                throw new Error("cache provider evidence changed");
            state.calibration = { ...cached, reused: true };
            reused = true;
        } catch (e) {
            if ((e as NodeJS.ErrnoException).code !== "ENOENT") state.cacheMissReason = String(e);
        }
        if (!reused) {
            if (stopped || remaining() < 1000)
                throw new BenchError("THROUGHPUT_CANCELLED", "Stopped before provider startup");
            const providerRoot = join(directory, "calibration-provider");
            if (graph)
                await writeFile(
                    join(directory, "provider-input.json"),
                    JSON.stringify({ cases: representatives.map(([id]) => base.cases.find((c) => c.id === id)) }),
                    { flag: "wx", mode: 0o600 },
                );
            provider = spawn(
                process.execPath,
                [
                    join(
                        packageRoot,
                        graph ? "components/http/graph-provider.mjs" : "components/http/light-provider.mjs",
                    ),
                    providerRoot,
                    String(Math.min(86400000, Math.max(1000, remaining()))),
                    ...(graph ? [join(directory, "provider-input.json")] : []),
                ],
                { stdio: ["pipe", "pipe", "pipe"] },
            );
            let stdout = "",
                stderr = "";
            provider.stdout!.on("data", (b) => {
                stdout += b.toString();
                if (stdout.length > 4096) provider?.kill();
            });
            provider.stderr!.on("data", (b) => {
                stderr = (stderr + b.toString()).slice(-4096);
            });
            const endpoint = await new Promise<string>((yes, no) => {
                const timer = setTimeout(
                    () => {
                        provider?.kill();
                        no(new Error("Calibration provider startup deadline"));
                    },
                    Math.min(10000, Math.max(1, remaining())),
                );
                provider!.once("error", (e) => {
                    clearTimeout(timer);
                    no(e);
                });
                provider!.once("exit", () => {
                    clearTimeout(timer);
                    no(new Error("Calibration provider exited: " + stderr));
                });
                provider!.stdout!.on("data", () => {
                    if (stdout.includes("\n")) {
                        clearTimeout(timer);
                        try {
                            yes(JSON.parse(stdout.split("\n")[0]!).endpoint);
                        } catch (e) {
                            no(e);
                        }
                    }
                });
            });
            const records = [];
            for (const [caseId, rate] of representatives) {
                const r = await execute(
                    "cal-" + caseId,
                    [suite.cases.find((c) => c.id === caseId)!],
                    [{ id: "probe", model: "open", ratePerSecond: rate, maxInFlight: 64, maxStartDelayMs: 50 }],
                    10000,
                    30000,
                    1,
                    endpoint,
                );
                records.push({
                    caseId,
                    rate,
                    rawDirectory: stages.at(-1).rawDirectory,
                    rowsSha256: hash(r.runs),
                    passed: r.runs.every(eligible),
                    maxDecodedBytes: r.runs[0]?.decodedBodyBytes?.max ?? null,
                });
            }
            await stopProvider();
            const providerSummary = JSON.parse(await readFile(join(providerRoot, "provider/summary.json"), "utf8"));
            const samples = (await readFile(join(providerRoot, "provider/samples.ndjson"), "utf8"))
                .trim()
                .split("\n")
                .map((s) => JSON.parse(s));
            const providerHealthy =
                providerSummary.status === "completed" &&
                Object.values(providerSummary.errors).every((v) => v === 0) &&
                samples
                    .filter((s) => s.index > 0 && s.elapsedMs >= 500)
                    .every((s) => s.averageCpuCores < 0.8 && s.eventLoopUtilization < 0.8);
            const record = {
                key,
                identity,
                records,
                providerSummary,
                providerHealthy,
                providerRoot,
                providerSummarySha256: hash((await readFile(join(providerRoot, "provider/summary.json"))).toString()),
                providerSamplesSha256: hash((await readFile(join(providerRoot, "provider/samples.ndjson"))).toString()),
                status: providerHealthy && records.every((r) => r.passed) ? "passed" : "limited",
                reused: false,
                scope: graph
                    ? "Loopback frozen graph parameter selection, semantic projection and trace checks at stated rates/shapes; no storage/network or all-case certification"
                    : "Loopback fixed-response driver checks at stated rates/shapes; no target-network, graph-validator or all-workload certification",
                performanceAccepted: false,
            };
            state.calibration = record;
            await durableJson(cachedFile, record);
            await save();
        }
        if (!options.calibrationOnly)
            await runThroughputSchedule(cases, settings, {
                explore: async (c) => {
                    const exploration = await execute(
                        "explore-" + c.id,
                        [c],
                        settings.concurrency.map((concurrency) => ({
                            id: "c" + concurrency,
                            model: "closed",
                            concurrency,
                        })),
                        settings.exploreWarmupMs,
                        settings.exploreDurationMs,
                        1,
                    );
                    return exploration.runs.map((r: any) => ({
                        concurrency: r.concurrency,
                        goodput: r.correctCompletedInWindowPerSecond,
                        eligible: eligible(r),
                    }));
                },
                selected: async (c, selection) => {
                    selections.push({ caseId: c.id, ...selection });
                    await save();
                },
                confirm: async (c, concurrency) => {
                    await execute(
                        "confirm-" + c.id,
                        [c],
                        concurrency.map((n) => ({ id: "c" + n, model: "closed", concurrency: n })),
                        settings.confirmWarmupMs,
                        settings.confirmDurationMs,
                        settings.confirmRepeats,
                    );
                },
                completed: async (c, confirmed) => {
                    state.incomplete = state.incomplete.filter((v: any) => v.caseId !== c.id);
                    if (!confirmed) state.incomplete.push({ caseId: c.id, stage: "no-eligible-candidate" });
                    await save();
                },
            });
        if (options.calibrationOnly) state.incomplete = [];
        state.status = state.incomplete.length ? "partial" : "completed";
    } catch (e) {
        state.status = "partial";
        state.failure = { code: e instanceof BenchError ? e.code : "THROUGHPUT_FAILURE", message: String(e) };
    } finally {
        await stopProvider();
        if (activeTask) {
            await cancelTask(root, activeTask);
            let t = await waitTask(root, activeTask, 1000);
            while (!t.terminal) t = await waitTask(root, activeTask, 1000);
        }
        options.signal?.removeEventListener("abort", onAbort);
        activeTask = null;
        state.elapsedMs = performance.now() - started;
        await save();
    }
    try {
        await component("report", directory, join(directory, "report"), started + budgetMs - performance.now());
    } catch (e) {
        state.reportFailure = String(e);
        state.status = "partial";
        await save();
    }
    return {
        ...state,
        reportJson: join(directory, "report/report.json"),
        reportHtml: join(directory, "report/report.html"),
    };
}
