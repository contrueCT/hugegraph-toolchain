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

import { httpProfileSchema, httpAnyProfileSchema } from "./http-contracts.js";
import type { HttpProfile } from "./http-contracts.js";
export { httpProfileSchema } from "./http-contracts.js";
import { BenchError, readJson } from "./io.js";
import { verifyHttpSuiteProfile } from "./http-suite.js";
import { freezeHttpExecution, type HttpInput, type HttpExecutionOptions } from "./http-plan.js";

export async function inspectHttpProfile(file: string): Promise<HttpProfile> {
    const p = await readJson(file, httpProfileSchema, 4 * 1024 * 1024);
    if (p.graph || p.cases.some((c) => c.requestPool))
        throw new BenchError("GRAPH_EXECUTOR", "Graph queries require Go profile v2");
    validateHttpProfile(p);
    await verifyHttpSuiteProfile(p);
    return p;
}

export async function inspectHttpInput(file: string): Promise<HttpInput> {
    const profile = await readJson(file, httpAnyProfileSchema, 4 * 1024 * 1024);
    if (profile.schemaVersion === 1) {
        if (profile.graph || profile.cases.some((c) => c.requestPool))
            throw new BenchError("GRAPH_EXECUTOR", "Graph queries require Go profile v2");
        validateHttpProfile(profile);
        return { profile, suite: await verifyHttpSuiteProfile(profile) };
    }
    const { goBaseProfile, validateGoHttpBudget } = await import("./http-go.js");
    const base = goBaseProfile(profile);
    validateHttpProfile(base);
    validateGoHttpBudget(profile);
    return { profile, suite: await verifyHttpSuiteProfile(base) };
}
export async function inspectAnyHttpProfile(file: string) {
    return (await inspectHttpInput(file)).profile;
}

export function validateHttpProfile(p: HttpProfile): void {
    const url = new URL(p.endpoint);
    if (!["http:", "https:"].includes(url.protocol) || url.username || url.password || url.search || url.hash)
        throw new BenchError("HTTP_ENDPOINT_INVALID", "Use an HTTP(S) endpoint without credentials, query or fragment");
    const pooled = p.cases.filter((c) => c.requestPool);
    if (pooled.length && !p.graph)
        throw new BenchError("GRAPH_EXECUTOR", "Graph request pools require the Go executor and graph binding");
    for (const c of pooled) {
        if (new Set(c.requestPool!.map((r) => r.id)).size !== c.requestPool!.length)
            throw new BenchError("GRAPH_DUPLICATE_PARAMETER", "Parameter IDs must be unique per case");
        for (const r of c.requestPool!) {
            const request = new URL(r.url);
            if (request.origin !== url.origin || request.username || request.password || request.hash)
                throw new BenchError("GRAPH_REQUEST_ORIGIN", "Frozen graph requests must stay on the target origin");
        }
    }
    for (const items of [p.cases, p.loads])
        if (new Set(items.map((x) => x.id)).size !== items.length)
            throw new BenchError("HTTP_DUPLICATE_ID", "Case and load IDs must be unique within their collection");
    for (const load of p.loads)
        if (
            load.model === "open" &&
            Math.ceil((Math.max(p.warmupMs, p.durationMs) * load.ratePerSecond) / 1000) > p.maxRequestsPerPhase
        )
            throw new BenchError("HTTP_REQUEST_BUDGET", "Open arrival schedule exceeds maxRequestsPerPhase");
    if (p.clockReference) {
        const spec = p.clockReference.spec;
        const runMs = (p.warmupMs ? p.warmupMs + p.drainMs : 0) + p.durationMs + p.drainMs;
        const probeMs = p.serviceChecks ? p.requestTimeoutMs + p.drainMs : 0;
        if (spec.wallMs < spec.startupMs + 2 * spec.exchangeMs + spec.finishMs + Math.max(runMs, probeMs) + 1000)
            throw new BenchError(
                "HTTP_CLOCK_REFERENCE_TIME_BUDGET",
                "Clock reference wall budget must cover exchanges, the largest run/probe including drain, and cleanup",
            );
    }
    const deadline = httpDeadline(p);
    if (deadline > 3600000)
        throw new BenchError("HTTP_TIME_BUDGET", "This profile exceeds the one-hour workflow bound; split the profile");
    const runs = p.cases.length * p.loads.length * p.repeats;
    const clockSessions = runs + (p.serviceChecks ? 2 * p.cases.length : 0);
    const clockBytes = p.clockReference ? 4 * p.clockReference.spec.maxRawBytes + 131072 : 65536;
    if (Buffer.byteLength(JSON.stringify(p)) * runs + clockSessions * clockBytes > 32 * 1024 * 1024)
        throw new BenchError(
            "HTTP_METADATA_BUDGET",
            "Request and bounded phase clock metadata exceed 32 MiB; split the profile",
        );
}

export function httpDeadline(p: HttpProfile): number {
    const runs = p.cases.length * p.loads.length * p.repeats;
    const probes = p.serviceChecks ? 2 * p.cases.length : 0;
    const referenceMs = p.clockReference ? p.clockReference.spec.wallMs + p.clockReference.spec.finishMs + 100 : 0;
    return (
        runs * (p.warmupMs + p.durationMs + 2 * p.drainMs + 1000) +
        probes * (p.requestTimeoutMs + p.drainMs + 1000) +
        (runs + probes) * referenceMs
    );
}

export async function prepareHttpExecution(
    input: HttpInput,
    file: string,
    directory: string,
    options: HttpExecutionOptions = {},
) {
    if (input.profile.schemaVersion === 2)
        return (await import("./http-go.js")).planGoHttp(input.profile, file, directory, options, input.suite);
    return freezeHttpExecution(input, file, directory, Math.ceil(httpDeadline(input.profile) + 60000), options);
}
export async function planHttp(file: string, directory: string) {
    const result = await prepareHttpExecution(await inspectHttpInput(file), file, directory);
    const { frozen: _, ...publicResult } = result;
    return publicResult;
}
