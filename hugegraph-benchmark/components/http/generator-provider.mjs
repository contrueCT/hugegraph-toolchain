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
import { mkdir, readFile, open, writeFile } from "node:fs/promises";
import { readFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { performance, monitorEventLoopDelay } from "node:perf_hooks";
import { verifyGeneratorAssets } from "./generator-assets.mjs";
import { argumentsFor, entry, digest, put } from "./common.mjs";
const need = (v, m) => {
    if (!v) throw new Error("GENERATOR_PROVIDER: " + m);
};
function cgroup() {
    try {
        const cpuStat = readFileSync("/sys/fs/cgroup/cpu.stat", "utf8");
        return {
            cpuStat,
            cpuMax: readFileSync("/sys/fs/cgroup/cpu.max", "utf8").trim(),
            memoryCurrent: Number(readFileSync("/sys/fs/cgroup/memory.current", "utf8")),
            memoryMax: readFileSync("/sys/fs/cgroup/memory.max", "utf8").trim(),
            memoryEvents: readFileSync("/sys/fs/cgroup/memory.events", "utf8"),
        };
    } catch {
        return null;
    }
}
export async function startGeneratorProvider({ assets, output, port = 18080, wallMs = 28800000, maxBytes = 67108864 }) {
    need(
        Number.isSafeInteger(port) &&
            port >= 0 &&
            port <= 65535 &&
            Number.isSafeInteger(wallMs) &&
            wallMs >= 1000 &&
            wallMs <= 86400000 &&
            maxBytes >= 65536 &&
            maxBytes <= 67108864,
        "bounds",
    );
    const prepared = await verifyGeneratorAssets(assets);
    await mkdir(output, { mode: 0o700 });
    const samples = await open(join(output, "samples.ndjson"), "wx", 0o600);
    const byRequest = new Map(prepared.assets.map((a) => [a.requestUtf8, a]));
    const counts = Object.fromEntries(
        prepared.assets.flatMap((a) =>
            ["identity", "gzip"].map((e) => [a.id + "/" + e, { accepted: 0, finished: 0, aborted: 0, bytes: 0 }]),
        ),
    );
    const errors = { rejected: 0, clientErrors: 0, responseErrors: 0 },
        sockets = new Set();
    const errorKinds = new Map();
    let overflowErrorEvents = 0;
    function recordError(source, error) {
        const code =
            typeof error?.code === "string" && /^[A-Z][A-Z0-9_]{0,63}$/.test(error.code) ? error.code : "UNCLASSIFIED";
        const key = source + "/" + code,
            now = process.hrtime.bigint().toString();
        const previous = errorKinds.get(key);
        if (previous) {
            previous.count++;
            previous.lastMonotonicNs = now;
        } else if (errorKinds.size < 16) {
            errorKinds.set(key, { source, code, count: 1, firstMonotonicNs: now, lastMonotonicNs: now });
        } else overflowErrorEvents++;
    }
    let active = 0,
        index = 0,
        bytesWritten = 0,
        stopped = false,
        failure = null;
    let previousCpu = process.cpuUsage(),
        previousTime = process.hrtime.bigint();
    let previousElu = performance.eventLoopUtilization();
    const delay = monitorEventLoopDelay({ resolution: 10 });
    delay.enable();
    async function sample(final = false) {
        const now = process.hrtime.bigint(),
            cpu = process.cpuUsage(),
            elu = performance.eventLoopUtilization();
        const diff = performance.eventLoopUtilization(elu, previousElu);
        const row = {
            schemaVersion: 1,
            kind: "http-generator-provider-sample",
            index: index++,
            final,
            monotonicNs: now.toString(),
            elapsedMs: Number(now - previousTime) / 1e6,
            cpuUserMicros: cpu.user,
            cpuSystemMicros: cpu.system,
            averageCpuCores:
                ((cpu.user + cpu.system - previousCpu.user - previousCpu.system) * 1000) / Number(now - previousTime),
            rssBytes: process.memoryUsage().rss,
            eventLoopUtilization: diff.utilization,
            eventLoopDelayMaxMs: delay.max / 1e6,
            cgroup: cgroup(),
            active,
            counts,
            errors,
        };
        previousCpu = cpu;
        previousTime = now;
        previousElu = elu;
        delay.reset();
        const raw = JSON.stringify(row) + "\n";
        need(bytesWritten + Buffer.byteLength(raw) <= maxBytes - 4096, "sample evidence cap");
        await samples.write(raw);
        bytesWritten += Buffer.byteLength(raw);
    }
    const server = http.createServer(
        { maxHeaderSize: 8192, requestTimeout: 10000, headersTimeout: 10000, keepAliveTimeout: 5000 },
        (req, res) => {
            let length = 0,
                chunks = [],
                rejected = false;
            const reject = () => {
                if (!rejected) {
                    rejected = true;
                    errors.rejected++;
                    res.writeHead(400, { "content-length": "0", connection: "close" });
                    res.end();
                }
            };
            if (
                req.method !== "POST" ||
                req.url !== "/gremlin" ||
                req.httpVersion !== "1.1" ||
                !["identity", "gzip"].includes(req.headers["accept-encoding"]) ||
                req.headers["content-encoding"] ||
                req.headers["transfer-encoding"] ||
                !/^[0-9]+$/.test(req.headers["content-length"] ?? "") ||
                Number(req.headers["content-length"]) > 8192
            )
                reject();
            req.on("error", (error) => {
                errors.clientErrors++;
                recordError("request", error);
            });
            req.on("data", (chunk) => {
                length += chunk.length;
                if (length > 8192) reject();
                if (!rejected) chunks.push(chunk);
            });
            req.on("end", () => {
                if (rejected) return;
                const raw = Buffer.concat(chunks),
                    item = byRequest.get(raw.toString("utf8"));
                if (!item || !Buffer.from(item.requestUtf8).equals(raw)) return reject();
                const encoding = req.headers["accept-encoding"],
                    body = item.responses[encoding];
                const counter = counts[item.id + "/" + encoding];
                counter.accepted++;
                active++;
                let finished = false;
                res.on("error", (error) => {
                    errors.responseErrors++;
                    recordError("response", error);
                });
                res.on("finish", () => {
                    finished = true;
                    counter.finished++;
                    counter.bytes += body.length;
                });
                res.on("close", () => {
                    active--;
                    if (!finished) counter.aborted++;
                });
                res.writeHead(200, {
                    "content-type": "application/json",
                    "content-length": String(body.length),
                    ...(encoding === "gzip" ? { "content-encoding": "gzip" } : {}),
                });
                res.end(body);
            });
        },
    );
    server.on("connection", (socket) => {
        sockets.add(socket);
        socket.on("close", () => sockets.delete(socket));
    });
    server.on("clientError", (error, socket) => {
        errors.clientErrors++;
        recordError("server-client", error);
        socket.destroy();
    });
    await new Promise((yes, no) => {
        server.once("error", no);
        server.listen(port, "127.0.0.1", yes);
    });
    await put(join(output, "ready.json"), {
        schemaVersion: 1,
        kind: "http-generator-provider-ready",
        environmentKind: "http-generator-calibration",
        pid: process.pid,
        node: process.version,
        executableSha256: await digest(process.execPath),
        sourceSha256: await digest(new URL(import.meta.url)),
        assetsManifestSha256: prepared.manifestSha256,
        port: server.address().port,
        monotonicNs: process.hrtime.bigint().toString(),
        performanceAccepted: false,
    });
    await sample();
    let pending = Promise.resolve();
    const timer = setInterval(() => {
        pending = pending
            .then(() => sample())
            .catch((error) => {
                failure ??= String(error);
                queueMicrotask(() => {
                    void stop("sample-failure");
                });
            });
    }, 1000);
    const deadline = setTimeout(() => {
        void stop("deadline");
    }, wallMs);
    async function stop(reason = "requested") {
        if (stopped) return;
        stopped = true;
        clearInterval(timer);
        clearTimeout(deadline);
        server.close();
        for (const socket of sockets) socket.destroy();
        delay.disable();
        await pending;
        try {
            await sample(true);
        } catch (error) {
            failure ??= String(error);
        }
        await samples.close();
        const summary = {
            schemaVersion: 1,
            kind: "http-generator-provider-summary",
            reason,
            status: failure ? "partial" : "completed",
            failure,
            samples: index,
            bytesWritten,
            active,
            counts,
            errors,
            errorDiagnostics: {
                countUnit: "error-handler-events-not-requests",
                maximumKinds: 16,
                kinds: [...errorKinds.values()],
                overflowEvents: overflowErrorEvents,
            },
            performanceAccepted: false,
        };
        await put(join(output, "summary.json"), summary);
        return summary;
    }
    return { endpoint: `http://127.0.0.1:${server.address().port}/gremlin`, stop };
}
if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href)
    await entry(async () => {
        const args = argumentsFor(["--assets", "--output", "--wall-ms"]);
        const provider = await startGeneratorProvider({
            assets: args["--assets"],
            output: args["--output"],
            wallMs: Number(args["--wall-ms"]),
        });
        for (const signal of ["SIGTERM", "SIGINT"])
            process.once(signal, () => {
                void provider.stop(signal);
            });
    }, "Fixed correct HTTP provider: --assets VERIFIED-assets --output NEW-directory --wall-ms LIMIT\n" + "HTTP generator calibration only. No HugeGraph, database benchmark, or graph capacity conclusion.");
