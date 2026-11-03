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

import { realpath, stat, writeFile, readdir } from "node:fs/promises";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { dirname, join, resolve } from "node:path";
import { httpProfileSchema, httpGoProfileSchema, type HttpGoProfile } from "./http-contracts.js";
import { BenchError, fileHash, hash } from "./io.js";
import { packageRoot } from "./plans.js";
import { verifyHttpSuiteProfile } from "./http-suite.js";
import { httpDeadline, inspectHttpProfile } from "./http.js";
import { freezeHttpExecution, type HttpExecutionOptions, type HttpInput } from "./http-plan.js";

export function goBaseProfile(profile: HttpGoProfile) {
    const { executor: _, ...base } = profile;
    return httpProfileSchema.parse({ ...base, schemaVersion: 1 });
}
export function validateGoHttpBudget(profile: HttpGoProfile) {
    const runs = profile.cases.length * profile.loads.length * profile.repeats;
    const sessions = runs + (profile.serviceChecks ? 2 * profile.cases.length : 0);
    const gateMs = profile.executor.hostIdentityGate ? 20000 * sessions : 0;
    if (httpDeadline(goBaseProfile(profile)) + gateMs > 3600000)
        throw new BenchError(
            "HTTP_GO_TIME_BUDGET",
            "Go workflow plus live identity gates exceeds one hour; split the profile",
        );
    if (
        profile.graph &&
        profile.cases.some((c) => Buffer.byteLength(JSON.stringify({ profile, case: c })) > 4 * 1024 * 1024 - 4096)
    )
        throw new BenchError(
            "GRAPH_REQUEST_METADATA",
            "Frozen Go request exceeds 4 MiB; reduce the declared pool before a new experiment",
        );
    const metadata =
        Buffer.byteLength(JSON.stringify(profile)) * runs +
        sessions * (65536 + (profile.executor.hostIdentityGate ? 2 * 65536 + 2 * 4096 : 0));
    if (metadata > 32 * 1024 * 1024)
        throw new BenchError(
            "HTTP_GO_METADATA_BUDGET",
            "Go requests and bounded identity metadata exceed 32 MiB; split the profile",
        );
    return gateMs;
}
async function goSources(root: string): Promise<string[]> {
    const files: string[] = [];
    for (const item of await readdir(root, { withFileTypes: true })) {
        const path = join(root, item.name);
        if (item.isDirectory()) files.push(...(await goSources(path)));
        else if (item.isFile() && (item.name.endsWith(".go") || item.name === "go.mod")) files.push(path);
    }
    return files.sort();
}
export async function planGoHttp(
    profile: HttpGoProfile,
    file: string,
    directory: string,
    options: HttpExecutionOptions = {},
    knownSuite?: HttpInput["suite"],
) {
    const gateMs = validateGoHttpBudget(profile);
    const base = goBaseProfile(profile),
        component = join(packageRoot, "components/http");
    const sources = await goSources(join(packageRoot, "components/http-go"));
    if (sources.length > 64) throw new BenchError("HTTP_GO_SOURCE_BUDGET", "Too many Go source members");
    const inventory = await Promise.all(
        sources.map(async (path) => ({
            path: path
                .slice(join(packageRoot, "components/http-go").length + 1)
                .split("\\")
                .join("/"),
            bytes: (await stat(path)).size,
            sha256: await fileHash(path),
        })),
    );
    if (hash(inventory) !== profile.executor.binary.sourceSha256)
        throw new BenchError("HTTP_GO_SOURCE_IDENTITY", "Registered binary source digest differs from this package");
    const binaryPath = await realpath(resolve(dirname(resolve(file)), profile.executor.binary.path));
    const engine = await import(join(component, "go-engine.mjs"));
    const binary = await engine.inspectGoBinary({ ...profile.executor.binary, path: binaryPath });
    const suite = knownSuite === undefined ? await verifyHttpSuiteProfile(base) : knownSuite;
    return freezeHttpExecution(
        {
            profile: {
                ...profile,
                executor: { ...profile.executor, binary: { ...profile.executor.binary, path: binaryPath } },
            },
            suite,
        },
        file,
        directory,
        Math.ceil(httpDeadline(base) + 60000 + gateMs),
        options,
        {
            registration: {
                schemaVersion: 1,
                kind: "http-go-executor-registration",
                identity: binary.identity,
                sourceFiles: inventory,
                toolchainLockSha256: await fileHash(join(packageRoot, "release/http-go-toolchain.json")),
            },
            trackedFiles: [...sources, binaryPath, join(packageRoot, "release/http-go-toolchain.json")],
        },
    );
}

export async function inspectHttpGoBinary(file: string) {
    const path = await realpath(resolve(file)),
        info = await stat(path);
    if (!info.isFile() || info.size > 32 * 1024 * 1024)
        throw new BenchError("HTTP_GO_BINARY", "Binary must be a regular executable <=32 MiB");
    const result = await promisify(execFile)(path, ["version"], { timeout: 10000, maxBuffer: 65536 });
    const identity = JSON.parse(result.stdout),
        spec = {
            path,
            sha256: await fileHash(path),
            sourceSha256: identity.sourceSha256,
            goVersion: "go1.27.3",
            goos: identity.goos,
            goarch: identity.goarch,
        };
    await (await import(join(packageRoot, "components/http/go-engine.mjs"))).inspectGoBinary(spec);
    return {
        binary: spec,
        identity,
        executed: false,
        scope: "Component identity only; no target request or calibration",
    };
}
export async function registerHttpGo(
    file: string,
    binaryFile: string,
    output: string,
    limits: { gomaxprocs: number; cpuQuotaCores: number; maxRssBytes: number; hostIdentityGate?: true },
) {
    const profile = await inspectHttpProfile(file),
        binary = await inspectHttpGoBinary(binaryFile);
    const { clockReference: _, ...base } = profile;
    const upgraded = httpGoProfileSchema.parse({
        ...base,
        schemaVersion: 2,
        executor: { engine: "go-http-v1", binary: binary.binary, ...limits, maxPendingBytes: 1048576 },
    });
    await writeFile(resolve(output), JSON.stringify(upgraded, null, 2) + "\n", { flag: "wx", mode: 0o600 });
    return { file: resolve(output), profile: upgraded, executed: false, performanceAccepted: false };
}
