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

import http from "node:http";
import { pathToFileURL } from "node:url";
import { resolve } from "node:path";
import https from "node:https";
import { gunzipSync } from "node:zlib";
import { openSync, writeSync, closeSync, fsyncSync } from "node:fs";
import { performance, monitorEventLoopDelay } from "node:perf_hooks";
import { isDeepStrictEqual } from "node:util";
import { openTraceWriter } from "./trace-writer.mjs";
import { setTimeout as sleep, setImmediate as yieldLoop } from "node:timers/promises";
import { argumentsFor, bytesDigest, entry, json, put, valueDigest, expectedCheckDigest } from "./common.mjs";
import {
    prepareDirectPhaseClock,
    captureDirectPhaseBoundary,
    recordDirectPhaseClock,
    finishDirectPhaseClock,
    getDirectPhaseClockReferenceRaw,
    stopDirectPhaseClock,
    validateDirectPhaseClockRaw,
} from "./direct-phase-clock.mjs";

function referenceFailure(tracePath, input, error, raw) {
    const declared = input.profile.clockReference.spec?.maxRawBytes,
        cap =
            4 * (Number.isSafeInteger(declared) && declared >= 16384 && declared <= 262144 ? declared : 262144) +
            131072;
    const bytes = Buffer.from(
        JSON.stringify({
            schemaVersion: 1,
            kind: "http-clock-reference-failure",
            status: "partial",
            id: input.id,
            caseId: input.case.id,
            loadId: input.load.id,
            repeat: input.repeat,
            failure: String(error.code || "HTTP_RUN_FAILED").slice(0, 128),
            clockReference: raw ?? null,
            performanceAccepted: false,
        }) + "\n",
    );
    if (bytes.length > cap) throw new Error("HTTP clock reference partial evidence budget exhausted");
    const fd = openSync(`${tracePath}.clock-reference.partial.json`, "wx", 0o600);
    try {
        let offset = 0;
        while (offset < bytes.length) offset += writeSync(fd, bytes, offset, bytes.length - offset);
        fsyncSync(fd);
    } finally {
        closeSync(fd);
    }
}

// One complete response is measured before hashing, parsing or answer checks.
// No retries, redirects, proxy env or HTTP pipelining. Decompression follows timing.
export async function run(input, tracePath) {
    const { profile: p, case: item, load } = input;
    if (input.probePhase !== undefined && !["preflight", "postcheck"].includes(input.probePhase))
        throw new Error("Invalid probe phase");
    if (input.probePhase && (load.model !== "closed" || load.concurrency !== 1))
        throw new Error("Functional probes require one sequential request");
    let clockContext;
    const maxInFlight = load.model === "closed" ? load.concurrency : load.maxInFlight;
    const url = new URL(p.endpoint);
    const transport = url.protocol === "https:" ? https : http;
    const agent = new transport.Agent({
        keepAlive: true,
        maxSockets: maxInFlight,
        maxFreeSockets: maxInFlight,
        scheduling: "fifo",
    });
    const body = Buffer.from(JSON.stringify(item.body));
    const headers = {
        "Content-Type": "application/json",
        "Content-Length": body.length,
        Accept: "application/json",
        "Accept-Encoding": p.responseEncoding,
    };
    if (p.authorizationEnv) {
        const auth = process.env[p.authorizationEnv];
        if (!auth || /[\r\n]/.test(auth)) throw new Error("Missing or invalid authorization environment value");
        headers.Authorization = auth;
    }
    const active = new Set();
    const clockAbort = new AbortController();
    let cancelled = false,
        fatal = null,
        primaryFailure = null;
    const writer = await openTraceWriter(
        tracePath,
        Math.min(p.maxTraceBytes, input.remainingTraceBytes ?? p.maxTraceBytes),
        (error) => {
            fatal = error;
            cancelled = true;
            for (const cancel of active) cancel("evidence-failure");
        },
    );
    const onSignal = () => {
        cancelled = true;
        clockAbort.abort();
        for (const cancel of active) cancel("cancelled");
    };
    process.on("SIGTERM", onSignal);
    process.on("SIGINT", onSignal);
    const emit = (record) => {
        if (fatal) return;
        try {
            writer.append(record);
        } catch (error) {
            fatal = error;
            cancelled = true;
            for (const cancel of active) cancel("evidence-failure");
        }
    };
    function check(data) {
        return item.checks.map((rule) => {
            let value = data;
            if (rule.pointer)
                for (const encoded of rule.pointer.slice(1).split("/")) {
                    const key = encoded.replace(/~1/g, "/").replace(/~0/g, "~");
                    if (value === null || typeof value !== "object" || !Object.hasOwn(value, key))
                        return { present: false, sha256: null };
                    value = value[key];
                }
            return { present: true, sha256: valueDigest(value) };
        });
    }
    const expectedDigests = item.checks.map((rule) => ({ present: true, sha256: expectedCheckDigest(rule) }));
    async function phase(name, durationMs) {
        const startClock = captureDirectPhaseBoundary(clockContext),
            start = startClock.performanceMs,
            deadline = start + durationMs;
        const cpuStart = process.cpuUsage(),
            eluStart = performance.eventLoopUtilization();
        const delay = monitorEventLoopDelay({ resolution: 10 });
        delay.enable();
        let scheduled = 0,
            sent = 0,
            completed = 0,
            dropped = 0,
            failed = 0,
            late = 0,
            budgetExhausted = false;
        const counts = {};
        const finishRecord = (record) => {
            counts[record.outcome] = (counts[record.outcome] ?? 0) + 1;
            if (record.sentMs !== null) {
                completed++;
                if (record.outcome !== "ok") failed++;
            } else dropped++;
            if (record.late) late++;
            emit(record);
        };
        const base = (sequence, scheduledMs) => ({
            schemaVersion: 1,
            phase: name,
            sequence,
            scheduledMs,
            sentMs: null,
            socketMs: null,
            endMs: null,
            validationMs: null,
            decodeMs: null,
            decodedBytes: null,
            responseEncoding: null,
            bytes: 0,
            status: null,
            bodySha256: null,
            checkDigests: null,
            completeResponse: false,
            reusedSocket: null,
            late: false,
        });
        const drop = (sequence, at, outcome) =>
            finishRecord({
                ...base(sequence, at),
                observedMs: performance.now() - start,
                late: outcome === "scheduler-late",
                outcome,
            });
        const request = (sequence, at) =>
            new Promise((resolve) => {
                const record = base(sequence, at);
                record.sentMs = performance.now() - start;
                record.late = load.model === "open" && record.sentMs - at > load.maxStartDelayMs;
                sent++;
                let req,
                    timer,
                    done = false,
                    chunks = [];
                const finish = (outcome, complete = false) => {
                    if (done) return;
                    done = true;
                    clearTimeout(timer);
                    active.delete(abort);
                    record.endMs = performance.now() - start;
                    record.completeResponse = complete;
                    record.reusedSocket = req?.reusedSocket ?? false;
                    if (complete) {
                        const checkStart = performance.now();
                        let bytes = Buffer.concat(chunks);
                        record.bodySha256 = bytesDigest(bytes);
                        if (outcome === "ok" && record.responseEncoding === "gzip") {
                            const decodeStart = performance.now();
                            try {
                                bytes = gunzipSync(bytes, { maxOutputLength: p.maxResponseBytes });
                            } catch (error) {
                                outcome =
                                    error.code === "ERR_BUFFER_TOO_LARGE"
                                        ? "decoded-response-too-large"
                                        : "invalid-gzip";
                            }
                            record.decodeMs = performance.now() - decodeStart;
                        }
                        if (outcome === "ok") {
                            record.decodedBytes = bytes.length;
                            try {
                                record.checkDigests = check(
                                    JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes)),
                                );
                                if (!isDeepStrictEqual(record.checkDigests, expectedDigests)) outcome = "wrong-answer";
                            } catch {
                                outcome = "invalid-json";
                            }
                        }
                        record.validationMs = performance.now() - checkStart;
                    }
                    chunks = [];
                    finishRecord({ ...record, outcome });
                    resolve();
                };
                const abort = (outcome) => {
                    finish(outcome);
                    req?.destroy();
                };
                active.add(abort);
                timer = setTimeout(() => abort("timeout"), p.requestTimeoutMs);
                try {
                    req = transport.request(
                        url,
                        { method: "POST", agent, headers, rejectUnauthorized: true },
                        (response) => {
                            record.status = response.statusCode;
                            record.responseEncoding = response.headers["content-encoding"]?.toLowerCase() ?? "identity";
                            response.on("data", (chunk) => {
                                if (done) return;
                                record.bytes += chunk.length;
                                if (record.bytes > p.maxResponseBytes) {
                                    abort("response-too-large");
                                    response.destroy();
                                } else chunks.push(chunk);
                            });
                            response.on("end", () =>
                                finish(
                                    record.status !== item.expectedStatus
                                        ? "http-status"
                                        : record.responseEncoding !== "identity" &&
                                            !(p.responseEncoding === "gzip" && record.responseEncoding === "gzip")
                                          ? "unexpected-encoding"
                                          : "ok",
                                    true,
                                ),
                            );
                            response.on("error", () => finish("response-error"));
                            response.on("aborted", () => finish("response-aborted"));
                        },
                    );
                    req.on("socket", (socket) => {
                        record.socketMs = performance.now() - start;
                        socket.setNoDelay(true);
                    });
                    req.on("error", () => finish("transport-error"));
                    req.end(body);
                } catch {
                    abort("transport-error");
                }
            });
        const drainTimer = setTimeout(() => {
            for (const cancel of active) cancel("drain-timeout");
        }, durationMs + p.drainMs);
        try {
            if (input.probePhase) {
                scheduled = 1;
                await request(0, 0);
            } else if (load.model === "closed") {
                await Promise.all(
                    Array.from({ length: load.concurrency }, async () => {
                        while (!cancelled && performance.now() < deadline) {
                            if (scheduled >= p.maxRequestsPerPhase) {
                                budgetExhausted = true;
                                break;
                            }
                            const sequence = scheduled++;
                            await request(sequence, performance.now() - start);
                        }
                    }),
                );
            } else {
                const total = Math.ceil((durationMs * load.ratePerSecond) / 1000);
                if (total > p.maxRequestsPerPhase) throw new Error("Arrival schedule exceeds request budget");
                const pending = new Set();
                for (let sequence = 0; sequence < total; sequence++) {
                    const at = (sequence * 1000) / load.ratePerSecond;
                    // Wake against the absolute clock; do not accumulate timer rounding drift.
                    while (!cancelled && performance.now() < start + at)
                        await sleep(Math.max(1, Math.min(50, start + at - performance.now())));
                    scheduled++;
                    const now = performance.now();
                    if (cancelled) drop(sequence, at, "cancelled-before-send");
                    else if (now - start - at > load.maxStartDelayMs || now >= deadline)
                        drop(sequence, at, "scheduler-late");
                    else if (active.size >= load.maxInFlight) drop(sequence, at, "concurrency-limit");
                    else {
                        const promise = request(sequence, at);
                        pending.add(promise);
                        promise.finally(() => pending.delete(promise));
                    }
                    if (sequence % 100 === 99) await yieldLoop();
                }
                await Promise.all(pending);
                // Keep the observation window fixed even when the final response is early.
                while (!cancelled && performance.now() < deadline)
                    await sleep(Math.max(1, Math.min(50, deadline - performance.now())));
            }
        } finally {
            clearTimeout(drainTimer);
            delay.disable();
        }
        const endClock = captureDirectPhaseBoundary(clockContext),
            elapsedMs = endClock.performanceMs - start,
            cpu = process.cpuUsage(cpuStart);
        return {
            name,
            durationMs,
            elapsedMs,
            scheduled,
            sent,
            completed,
            dropped,
            failed,
            late,
            counts,
            budgetExhausted,
            cancelled,
            cpu,
            averageCpuCores: (cpu.user + cpu.system) / (elapsedMs * 1000),
            eventLoopUtilization: performance.eventLoopUtilization(eluStart).utilization,
            eventLoopDelayMs: { p99: delay.percentile(99) / 1e6, max: delay.max / 1e6 },
            maxRssKiB: process.resourceUsage().maxRSS,
            clock: recordDirectPhaseClock(clockContext, name, startClock, endClock, elapsedMs),
        };
    }
    const startedAt = new Date().toISOString(),
        phases = [];
    try {
        clockContext = await prepareDirectPhaseClock(
            {
                id: input.id,
                caseId: item.id,
                loadId: load.id,
                repeat: input.repeat,
                probePhase: input.probePhase,
                clockReference: p.clockReference,
                ...(p.clockReference ? { profileDigest: valueDigest(p) } : {}),
            },
            { signal: clockAbort.signal },
        );
        if (input.probePhase) phases.push(await phase(input.probePhase, p.requestTimeoutMs));
        else {
            if (p.warmupMs) phases.push(await phase("warmup", p.warmupMs));
            if (!cancelled) phases.push(await phase("measurement", p.durationMs));
        }
        if (fatal) throw fatal;
        const traceWriter = await writer.finish();
        const phaseClock = await finishDirectPhaseClock(clockContext, phases);
        if (cancelled && !phases.some((phase) => phase.cancelled))
            throw new Error("HTTP runner cancelled during clock finalization");
        const result = {
            schemaVersion: 1,
            kind: input.probePhase ? "http-functional-probe" : "http-run",
            id: input.id,
            caseId: item.id,
            loadId: load.id,
            repeat: input.repeat,
            startedAt,
            finishedAt: new Date().toISOString(),
            phases,
            traceBytes: traceWriter.persistedBytes,
            traceWriter,
            phaseClock,
            ...(p.clockReference ? { clockReference: getDirectPhaseClockReferenceRaw(clockContext) } : {}),
            client: {
                node: process.version,
                http: "1.1",
                keepAlive: true,
                maxSockets: maxInFlight,
                retries: 0,
                redirects: 0,
                compression: p.responseEncoding,
                proxy: "none",
                tlsVerification: "default-enabled",
                authorization: p.authorizationEnv ? "environment-supplied" : "none",
            },
            performanceAccepted: false,
        };
        validateDirectPhaseClockRaw(result, p);
        if (
            p.clockReference &&
            Buffer.byteLength(JSON.stringify(result, null, 2) + "\n") > 4 * p.clockReference.spec.maxRawBytes + 131072
        )
            throw new Error("HTTP run clock reference metadata budget exhausted");
        return result;
    } catch (error) {
        primaryFailure = error;
        let partial =
            error.clockReference ?? (error.evidence?.kind === "http-runner-sanity-raw" ? error.evidence : undefined);
        if (clockContext) partial ??= await stopDirectPhaseClock(clockContext, error.code || "HTTP_RUN_FAILED");
        if (p.clockReference) {
            error.clockReference = partial;
            try {
                referenceFailure(tracePath, input, error, partial);
            } catch (writeError) {
                writeError.cause = error;
                writeError.clockReference = partial;
                throw writeError;
            }
        }
        throw error;
    } finally {
        try {
            if (clockContext) await stopDirectPhaseClock(clockContext, "HTTP_RUN_FINALLY");
        } finally {
            for (const cancel of active) cancel("runner-stopped");
            agent.destroy();
            try {
                try {
                    await writer.close();
                } catch (error) {
                    if (!primaryFailure) throw error;
                }
                if (!primaryFailure && cancelled && !phases.some((phase) => phase.cancelled))
                    throw new Error("HTTP runner cancelled during trace finalization");
            } finally {
                process.off("SIGTERM", onSignal);
                process.off("SIGINT", onSignal);
            }
        }
    }
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href)
    await entry(async () => {
        const args = argumentsFor(["--request", "--trace", "--output"]);
        const result = await run(await json(args["--request"]), args["--trace"]);
        await put(args["--output"], result);
    }, "HTTP runner v1: --request request.json --trace samples.ndjson --output run.json\nPOST HTTP/1.1, no retries/redirects. Complete-response timing; answer validation follows timing.");
