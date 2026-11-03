/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import net from "node:net";
import { mkdtemp, rm, readFile, writeFile, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { gunzipSync } from "node:zlib";
import { prepareGeneratorAssets, verifyGeneratorAssets } from "../components/http/generator-assets.mjs";
import { startGeneratorProvider } from "../components/http/generator-provider.mjs";
import { verifyGeneratorWireTrace } from "../components/http/generator-evidence.mjs";
import { bytesDigest } from "../components/http/common.mjs";
import { describeHttpGenerator, planHttpGenerator, httpGeneratorRequestSchema } from "../dist/http-generator.js";
async function temporary(t) {
    const root = await mkdtemp(join(tmpdir(), "hgbench-generator-"));
    t.after(() => rm(root, { recursive: true, force: true }));
    return root;
}
function request(endpoint, body, encoding = "identity", extra = {}) {
    return new Promise((resolve, reject) => {
        const raw = Buffer.from(body);
        const req = http.request(
            endpoint,
            {
                method: "POST",
                headers: {
                    "content-length": raw.length,
                    "content-type": "application/json",
                    "accept-encoding": encoding,
                    ...extra,
                },
            },
            (res) => {
                const chunks = [];
                res.on("data", (c) => chunks.push(c));
                res.on("end", () =>
                    resolve({ status: res.statusCode, headers: res.headers, raw: Buffer.concat(chunks) }),
                );
                res.on("error", reject);
            },
        );
        req.on("error", reject);
        req.end(raw);
    });
}
test("generator assets bind all eleven exact independent answers and fail tampered payloads", async (t) => {
    const root = await temporary(t),
        assets = join(root, "assets");
    const result = await prepareGeneratorAssets(assets);
    assert.equal(result.assets.length, 11);
    assert.equal(result.manifest.entries.length, 11);
    const file = result.manifest.entries.at(-1).variants.gzip.path;
    await writeFile(join(assets, file), Buffer.from("invalid gzip"));
    await assert.rejects(verifyGeneratorAssets(assets), /asset type\/size|asset digest/);
});
test("fixed provider sends all eleven frozen identity/gzip bodies and rejects modified or unsupported requests", async (t) => {
    const root = await temporary(t),
        assets = join(root, "assets"),
        output = join(root, "provider");
    const prepared = await prepareGeneratorAssets(assets),
        provider = await startGeneratorProvider({ assets, output, port: 0, wallMs: 10000 });
    t.after(() => provider.stop());
    for (const item of prepared.assets)
        for (const encoding of ["identity", "gzip"]) {
            const response = await request(provider.endpoint, item.requestUtf8, encoding);
            assert.equal(response.status, 200);
            assert.equal(response.raw.length, Number(response.headers["content-length"]));
            assert.equal(bytesDigest(response.raw), item.variants[encoding].sha256);
            assert.deepEqual(encoding === "gzip" ? gunzipSync(response.raw) : response.raw, item.responses.identity);
        }
    for (const [body, encoding] of [
        [prepared.assets[0].requestUtf8 + " ", "identity"],
        ["{}", "identity"],
        [prepared.assets[0].requestUtf8, "br"],
    ])
        assert.equal((await request(provider.endpoint, body, encoding)).status, 400);
    const result = await provider.stop("requested");
    assert.equal(result.status, "completed");
    assert.equal(result.errors.rejected, 3);
    for (const value of Object.values(result.counts)) {
        assert.equal(value.accepted, 1);
        assert.equal(value.finished, 1);
        assert.equal(value.aborted, 0);
    }
    assert.equal(
        (await readFile(join(output, "samples.ndjson"), "utf8")).trim().split("\n").map(JSON.parse).at(-1).final,
        true,
    );
});
test("provider bounded lifetime closes listener and preserves terminal evidence", async (t) => {
    const root = await temporary(t),
        assets = join(root, "assets"),
        output = join(root, "provider");
    await prepareGeneratorAssets(assets);
    const provider = await startGeneratorProvider({ assets, output, port: 0, wallMs: 1000 });
    t.after(() => provider.stop());
    await new Promise((r) => setTimeout(r, 1150));
    assert.equal(JSON.parse(await readFile(join(output, "summary.json"))).reason, "deadline");
    await assert.rejects(request(provider.endpoint, "{}"));
});
test("generator planning rejects engineering policy reduction and dry-run performs no writes", async (t) => {
    const root = await temporary(t),
        path = join(root, "request.json"),
        out = join(root, "plan");
    const request = {
        schemaVersion: 1,
        kind: "http-generator-calibration-request",
        name: "verify",
        purpose: "engineering-calibration",
        binary: {
            path: "/registered/go",
            sha256: "a".repeat(64),
            sourceSha256: "b".repeat(64),
            goVersion: "go1.27.3",
            goos: "linux",
            goarch: "amd64",
        },
        runtime: {
            image: "sha256:" + "c".repeat(64),
            docker: "/usr/bin/docker",
            hostPython: "/usr/bin/python3",
            containerPython: "/usr/local/bin/python3",
            nodeRoot: "/node-root",
            clockProof: "/proof",
        },
        timing: { warmupMs: 30000, durationMs: 60000, requestTimeoutMs: 5000, drainMs: 5000 },
        budget: { wallMs: 28800000, cleanupReserveMs: 100000, maxEvidenceBytes: 4294967296, minFreeBytes: 85899345920 },
    };
    assert.equal(httpGeneratorRequestSchema.parse(request).budget.mode, "total");
    await writeFile(path, JSON.stringify(request));
    const result = await planHttpGenerator(path, out, true);
    assert.equal(result.fixedWindowMs, 7920000);
    assert.equal(result.units, 88);
    assert.equal(result.plannedMeasurementArrivals, 422400);
    assert.equal(result.written, false);
    await assert.rejects(stat(out), { code: "ENOENT" });
    assert(describeHttpGenerator().scope.includes("No HugeGraph"));
    request.timing.durationMs = 100;
    await writeFile(path, JSON.stringify(request));
    await assert.rejects(planHttpGenerator(path, out, true), /Engineering calibration/);
});

test("independent fixed-wire reader rejects an envelope or encoding change even with the same answer", async (t) => {
    const root = await temporary(t),
        path = join(root, "trace.ndjson"),
        raw = Buffer.from('{"status":{"code":200},"result":{"data":[42]}}');
    const variant = { bytes: raw.length, sha256: bytesDigest(raw) };
    const sample = {
        completeResponse: true,
        bytes: variant.bytes,
        bodySha256: variant.sha256,
        responseEncoding: "identity",
    };
    await writeFile(path, JSON.stringify(sample) + "\n");
    assert.equal((await verifyGeneratorWireTrace(path, variant, "identity")).completeResponses, 1);
    for (const corrupt of [
        { ...sample, bodySha256: "0".repeat(64) },
        { ...sample, responseEncoding: "gzip" },
        { ...sample, bytes: sample.bytes + 1 },
    ]) {
        await writeFile(path, JSON.stringify(corrupt) + "\n");
        await assert.rejects(verifyGeneratorWireTrace(path, variant, "identity"), /fixed wire bytes\/encoding differ/);
    }
});

test("provider preserves bounded socket error codes without request bytes or credentials", async (t) => {
    const root = await temporary(t),
        assets = join(root, "assets"),
        output = join(root, "provider");
    await prepareGeneratorAssets(assets);
    const provider = await startGeneratorProvider({ assets, output, port: 0, wallMs: 10000 });
    t.after(() => provider.stop());
    for (let i = 0; i < 2; i++)
        await new Promise((resolve, reject) => {
            const socket = net.connect(Number(new URL(provider.endpoint).port), "127.0.0.1", () => {
                socket.end("PRIVATE / HTTP/1.1\r\nAuthorization: Bearer DO-NOT-RETAIN\r\n\r\n");
            });
            socket.once("error", reject);
            socket.once("close", resolve);
        });
    const summary = await provider.stop();
    assert.equal(summary.errors.clientErrors, 2);
    assert.equal(summary.errorDiagnostics.countUnit, "error-handler-events-not-requests");
    assert.equal(summary.errorDiagnostics.maximumKinds, 16);
    assert.equal(summary.errorDiagnostics.overflowEvents, 0);
    const [error] = summary.errorDiagnostics.kinds;
    assert.equal(summary.errorDiagnostics.kinds.length, 1);
    assert.equal(error.source, "server-client");
    assert.equal(error.code, "HPE_INVALID_METHOD");
    assert.equal(error.count, 2);
    assert(BigInt(error.lastMonotonicNs) >= BigInt(error.firstMonotonicNs));
    assert.doesNotMatch(
        await readFile(join(output, "summary.json"), "utf8"),
        /PRIVATE|DO-NOT-RETAIN|Authorization|rawPacket/,
    );
});
