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

import { createServer } from "node:http";
import { mkdir, readFile, writeFile, appendFile } from "node:fs/promises";
import { join } from "node:path";
import { gzipSync } from "node:zlib";
import { graphResponse } from "./graph-validation.mjs";
import { valueDigest } from "./common.mjs";
const [directory, wall, input] = process.argv.slice(2);
const root = join(directory, "provider");
await mkdir(root, { recursive: true, mode: 0o700 });
const raw = await readFile(input);
if (raw.length > 4194304 || !Number.isSafeInteger(Number(wall)) || Number(wall) < 1 || Number(wall) > 86400000)
    throw new Error("Provider input/budget bound");
const routes = new Map();
for (const c of JSON.parse(raw).cases)
    for (const [i, r] of c.requestPool.entries()) {
        const body = Buffer.from(JSON.stringify(graphResponse(r)));
        routes.set("/graph/" + c.id + "/" + i, { request: r, identity: body, gzip: gzipSync(body) });
    }
const errors = { clientErrors: 0, requestErrors: 0, responseErrors: 0 };
let requests = 0,
    stopped = false,
    index = 0,
    previous = performance.now(),
    usage = process.cpuUsage(),
    previousElu = performance.eventLoopUtilization();
let writes = Promise.resolve();
const server = createServer(async (req, res) => {
    try {
        const route = routes.get(req.url);
        if (!route || route.request.method !== req.method) throw new Error("Unregistered request");
        if (req.method === "POST") {
            const chunks = [];
            let bytes = 0;
            for await (const b of req) {
                bytes += b.length;
                if (bytes > 4194304) throw new Error("Request size");
                chunks.push(b);
            }
            if (valueDigest(JSON.parse(Buffer.concat(chunks))) !== valueDigest(route.request.body))
                throw new Error("Request body differs");
        }
        requests++;
        const encoding = req.headers["accept-encoding"] === "gzip" ? "gzip" : "identity";
        res.on("error", () => errors.responseErrors++);
        res.writeHead(route.request.expectedStatus, {
            "Content-Type": "application/json",
            "Content-Encoding": encoding,
            "Content-Length": route[encoding].length,
        });
        res.end(route[encoding]);
    } catch {
        errors.requestErrors++;
        res.writeHead(400);
        res.end("{}");
    }
});
server.on("clientError", (_, socket) => {
    errors.clientErrors++;
    socket.destroy();
});
await new Promise((yes) => server.listen(0, "127.0.0.1", yes));
async function sample() {
    const now = performance.now(),
        cpu = process.cpuUsage(),
        elu = performance.eventLoopUtilization();
    const elapsedMs = now - previous;
    const data = {
        index: index++,
        elapsedMs,
        averageCpuCores: (cpu.user + cpu.system - usage.user - usage.system) / (elapsedMs * 1000),
        eventLoopUtilization:
            (elu.active - previousElu.active) /
            Math.max(1, elu.active + elu.idle - previousElu.active - previousElu.idle),
        requests,
        rssBytes: process.memoryUsage().rss,
    };
    previous = now;
    usage = cpu;
    previousElu = elu;
    await appendFile(join(root, "samples.ndjson"), JSON.stringify(data) + "\n");
}
const samples = setInterval(() => {
    writes = writes.then(sample);
}, 1000);
async function stop(reason) {
    if (stopped) return;
    stopped = true;
    clearInterval(samples);
    clearTimeout(timer);
    server.close();
    server.closeAllConnections();
    await writes;
    await sample();
    await writeFile(
        join(root, "summary.json"),
        JSON.stringify(
            {
                status: reason === "deadline" ? "expired" : "completed",
                reason,
                requests,
                errors,
                routes: routes.size,
                scope: "Reference-derived fixed graph projections; no database service",
            },
            null,
            2,
        ) + "\n",
        { flag: "wx" },
    );
    process.stdin.destroy();
}
const timer = setTimeout(() => void stop("deadline"), Number(wall));
for (const s of ["SIGINT", "SIGTERM"]) process.once(s, () => void stop(s));
process.stdin.resume();
process.stdin.once("end", () => void stop("controller-eof"));
process.stdout.write(JSON.stringify({ endpoint: "http://127.0.0.1:" + server.address().port + "/graph" }) + "\n");
