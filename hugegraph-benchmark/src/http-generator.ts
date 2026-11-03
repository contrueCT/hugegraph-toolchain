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

import { mkdir, readdir, lstat, realpath, writeFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { z } from "zod";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { BenchError, readJson, writeJson, fileHash } from "./io.js";
import { packageRoot } from "./plans.js";
import { freezeTask } from "./tasks.js";
import { inspectHttpSuite } from "./http-suite.js";
import { httpGeneratorRequestSchema } from "./http-generator-contracts.js";
export { httpGeneratorRequestSchema } from "./http-generator-contracts.js";
export function describeHttpGenerator() {
    return {
        schemaVersion: 1,
        kind: "http-generator-calibration-contract",
        request: z.toJSONSchema(httpGeneratorRequestSchema),
        commands: [
            "http generator plan REQUEST --out NEW [--dry-run]",
            "http generator verify PLAN",
            "task run --plan PLAN/task.json --request-id UNIQUE --detach",
            "task status TASK_ID",
            "task cancel TASK_ID",
            "http generator report PLAN --out NEW",
        ],
        scope: "Actual HTTP generator calibration. Fixed correct responses, all 11 questions, identity/gzip. No HugeGraph or server capacity claims.",
        fixed: {
            armsCpu: [4, 6, 6, 4],
            driverMemoryBytes: 3221225472,
            gomaxprocs: 4,
            providerCpu: 1,
            providerMemoryBytes: 536870912,
            rate: 80,
            maxInFlight: 64,
            unitsPerArm: 22,
        },
        budget: "Default total hard deadline includes setup, all arms and cleanup. per-arm explicitly changes the deadline scope. No automatic reduction.",
        engineeringTiming: { warmupMs: 30000, durationMs: 60000, requestTimeoutMs: 5000, drainMs: 5000 },
        performanceAccepted: false,
    };
}
async function inventory(root: string, prefixes: string[]) {
    const result: Array<{ path: string; bytes: number; sha256: string }> = [];
    async function visit(relative: string) {
        const path = join(root, relative),
            info = await lstat(path);
        if (info.isDirectory()) {
            for (const name of (await readdir(path)).sort())
                if (name !== "__pycache__" && !name.endsWith(".pyc")) await visit(relative + "/" + name);
        } else if (info.isFile() && !info.isSymbolicLink()) {
            if (info.size > 8388608) throw new BenchError("GENERATOR_SOURCE_BOUND", "Source member exceeds 8 MiB");
            result.push({ path: relative, bytes: info.size, sha256: await fileHash(path) });
        } else throw new BenchError("GENERATOR_SOURCE_TYPE", "Frozen source must contain regular files only");
    }
    for (const prefix of prefixes) await visit(prefix);
    return result.sort((a, b) => a.path.localeCompare(b.path, "en"));
}
export async function planHttpGenerator(requestFile: string, directory: string, dryRun = false) {
    const request = await readJson(requestFile, httpGeneratorRequestSchema, 65536);
    if (
        request.binary.goos !== "linux" ||
        !request.binary.path.startsWith("/") ||
        ["/source", "/node", "/evidence", "/study", "/proc", "/sys", "/dev", "/tmp"].some(
            (p) => request.binary.path === p || request.binary.path.startsWith(p + "/"),
        )
    )
        throw new BenchError(
            "GENERATOR_BINARY",
            "Use an absolute registered Linux binary outside container mount roots",
        );
    if (
        request.purpose === "engineering-calibration" &&
        (request.timing.warmupMs !== 30000 ||
            request.timing.durationMs !== 60000 ||
            request.timing.requestTimeoutMs !== 5000 ||
            request.timing.drainMs !== 5000 ||
            request.budget.cleanupReserveMs !== 100000 ||
            request.budget.minFreeBytes < 85899345920)
    )
        throw new BenchError(
            "GENERATOR_ENGINEERING_POLICY",
            "Engineering calibration requires frozen 30s/60s windows, request/drain 5s, cleanup 100s and disk floor 80GiB",
        );
    if (request.budget.wallMs <= request.budget.cleanupReserveMs)
        throw new BenchError("GENERATOR_BUDGET", "Deadline must exceed cleanup reserve");
    const fixedWindowMs = 88 * (request.timing.warmupMs + request.timing.durationMs);
    const preview = {
        directory: resolve(directory),
        environmentKind: "http-generator-calibration",
        request,
        fixedWindowMs,
        arms: 4,
        units: 88,
        plannedMeasurementArrivals: 88 * Math.ceil((80 * request.timing.durationMs) / 1000),
        executed: false,
        performanceAccepted: false,
    };
    if (dryRun) return { ...preview, written: false };
    const dir = join(await realpath(dirname(resolve(directory))), directory.split("/").filter(Boolean).at(-1)!);
    if (dir !== resolve(directory)) throw new BenchError("GENERATOR_OUTPUT", "Use a canonical new output path");
    const runtimeFiles = await Promise.all(
        (
            [
                ["node", join(request.runtime.nodeRoot, "bin/node")],
                ["hostPython", request.runtime.hostPython],
                ["docker", request.runtime.docker],
            ] as const
        ).map(async ([role, file]) => {
            const path = await realpath(file),
                info = await lstat(path);
            if (!info.isFile() || info.size > 268435456)
                throw new BenchError("GENERATOR_RUNTIME_FILE", "Runtime executable must be a regular file <=256MiB");
            return { role, path, bytes: info.size, sha256: await fileHash(path) };
        }),
    );
    const suite = await inspectHttpSuite();
    const assets = await import(join(packageRoot, "components/http/generator-assets.mjs"));
    await mkdir(dir, { mode: 0o700 });
    await assets.prepareGeneratorAssets(join(dir, "assets"));
    const sourceInventory = await inventory(packageRoot, [
        "components/http",
        "components/http-go",
        "dist",
        "benchmarks/http-serving-v0.1",
        "release",
        "package.json",
        "node_modules/commander",
        "node_modules/zod",
    ]);
    const plan = {
        schemaVersion: 1,
        kind: "http-generator-calibration-plan",
        environmentKind: "http-generator-calibration",
        request,
        sourceRoot: packageRoot,
        runtimeFiles,
        user: { uid: process.getuid?.() ?? -1, gid: process.getgid?.() ?? -1 },
        sourceInventory,
        assetsInventory: await inventory(dir, ["assets"]),
        suite: { suiteId: suite.suiteId, version: suite.version, bundleSha256: suite.bundleSha256 },
        arms: [4, 6, 6, 4].map((cpus, i) => ({
            id: `a${i + 1}`,
            block: Math.floor(i / 2) + 1,
            cpus,
            memoryBytes: 3221225472,
            gomaxprocs: 4,
        })),
        cases: suite.cases,
        encodings: ["identity", "gzip"],
        ratePerSecond: 80,
        maxInFlight: 64,
        provider: { cpus: 1, memoryBytes: 536870912 },
        fixedWindowMs,
        plannedMeasurementArrivals: preview.plannedMeasurementArrivals,
        performanceAccepted: false,
    };
    await writeJson(join(dir, "plan.json"), plan);
    await writeJson(join(dir, "plan.lock.json"), {
        schemaVersion: 1,
        planSha256: await fileHash(join(dir, "plan.json")),
    });
    const source = join(dir, "task.source.json");
    await writeJson(source, {
        schemaVersion: 1,
        kind: "component-task",
        name: request.name,
        command: [
            request.runtime.hostPython,
            "-B",
            join(packageRoot, "components/http/generator-workflow.py"),
            "--plan",
            dir,
        ],
        cwd: dir,
        trackedFiles: [
            join(dir, "plan.json"),
            join(dir, "plan.lock.json"),
            join(packageRoot, "components/http/generator-workflow.py"),
        ],
        effect: "filesystem",
        timeoutMs: request.budget.wallMs * (request.budget.mode === "per-arm" ? 4 : 1),
        terminateGraceMs: 30000,
        maxLogBytes: 1048576,
    });
    const frozen = await freezeTask(source);
    await writeJson(join(dir, "task.json"), frozen);
    await writeFile(
        join(dir, "README.txt"),
        "Fixed provider calibration, no HugeGraph. One execution per plan; cancellation/deadline preserve unfinished cells. No replacements.\n",
        { flag: "wx", mode: 0o600 },
    );
    return { ...preview, written: true, taskPlan: join(dir, "task.json"), taskPlanDigest: frozen.digest };
}
export async function generatorComponent(action: "verify" | "report", directory: string, out?: string) {
    try {
        const args = [
            join(packageRoot, "components/http/generator-evidence.mjs"),
            action,
            "--plan",
            resolve(directory),
            ...(out ? ["--out", resolve(out)] : []),
        ];
        return JSON.parse(
            (await promisify(execFile)(process.execPath, args, { timeout: 300000, maxBuffer: 2097152 })).stdout,
        );
    } catch (error) {
        const e = error as { stderr?: string; message?: string };
        throw new BenchError("GENERATOR_EVIDENCE", "Generator evidence verification failed", 6, false, {
            diagnostic: e.stderr?.slice(0, 2048) ?? e.message,
        });
    }
}
