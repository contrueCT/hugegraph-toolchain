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

import { mkdir, readFile, writeFile, lstat, readdir } from "node:fs/promises";
import { join } from "node:path";
import { gzipSync, gunzipSync } from "node:zlib";
import { isDeepStrictEqual } from "node:util";
import { bytesDigest, digest, json, put, valueDigest } from "./common.mjs";
import { inspect, expectedData } from "../../benchmarks/http-serving-v0.1/reference.mjs";
const need = (v, message) => {
    if (!v) throw new Error("GENERATOR_ASSETS: " + message);
};
export async function prepareGeneratorAssets(out) {
    const suite = await inspect();
    await mkdir(out, { mode: 0o700 });
    const entries = [];
    for (const q of suite.questions) {
        const request = Buffer.from(JSON.stringify(q.body));
        const identity = Buffer.from(JSON.stringify({ status: { code: 200 }, result: { data: expectedData(q) } }));
        const gzip = gzipSync(identity, { level: 6 });
        const variants = {};
        for (const [encoding, bytes] of Object.entries({ identity, gzip })) {
            const path = `${q.id}.${encoding}.bin`;
            await writeFile(join(out, path), bytes, { flag: "wx", mode: 0o600 });
            variants[encoding] = { path, bytes: bytes.length, sha256: bytesDigest(bytes) };
        }
        entries.push({
            id: q.id,
            requestUtf8: request.toString(),
            requestSha256: bytesDigest(request),
            answerSha256: q.answer.canonicalSha256,
            variants,
        });
    }
    await put(join(out, "assets.json"), {
        schemaVersion: 1,
        kind: "http-generator-calibration-assets",
        suite: { suiteId: suite.suiteId, version: suite.version, bundleSha256: suite.bundleSha256 },
        entries,
        performanceAccepted: false,
    });
    return verifyGeneratorAssets(out);
}
// Offline: compare bytes to this installed version's independent reference, never execute evidence code.
export async function verifyGeneratorAssets(root) {
    const manifest = await json(join(root, "assets.json"), 131072),
        suite = await inspect();
    need(
        manifest.schemaVersion === 1 &&
            manifest.kind === "http-generator-calibration-assets" &&
            manifest.performanceAccepted === false &&
            isDeepStrictEqual(manifest.suite, {
                suiteId: suite.suiteId,
                version: suite.version,
                bundleSha256: suite.bundleSha256,
            }),
        "identity",
    );
    need(Array.isArray(manifest.entries) && manifest.entries.length === 11, "full eleven required");
    const assets = [];
    for (const [i, entry] of manifest.entries.entries()) {
        const q = suite.questions[i],
            request = Buffer.from(JSON.stringify(q.body));
        need(
            entry.id === q.id &&
                entry.requestUtf8 === request.toString() &&
                entry.requestSha256 === bytesDigest(request) &&
                entry.answerSha256 === q.answer.canonicalSha256,
            "request/answer binding",
        );
        const identity = Buffer.from(JSON.stringify({ status: { code: 200 }, result: { data: expectedData(q) } }));
        const responses = {};
        need(isDeepStrictEqual(Object.keys(entry.variants).sort(), ["gzip", "identity"]), "encodings");
        for (const encoding of ["identity", "gzip"]) {
            const file = entry.variants[encoding];
            need(file.path === `${q.id}.${encoding}.bin`, "asset path");
            const info = await lstat(join(root, file.path));
            need(
                info.isFile() && !info.isSymbolicLink() && info.size === file.bytes && info.size <= 2097152,
                "asset type/size",
            );
            const raw = await readFile(join(root, file.path));
            need(bytesDigest(raw) === file.sha256, "asset digest");
            const decoded = encoding === "gzip" ? gunzipSync(raw, { maxOutputLength: 2097152 }) : raw;
            need(
                decoded.equals(identity) && valueDigest(JSON.parse(decoded).result.data) === entry.answerSha256,
                "independent full answer",
            );
            responses[encoding] = raw;
        }
        assets.push({ ...entry, responses });
    }
    need(
        isDeepStrictEqual(
            (await readdir(root)).sort(),
            ["assets.json", ...manifest.entries.flatMap((e) => Object.values(e.variants).map((v) => v.path))].sort(),
        ),
        "asset inventory",
    );
    return { manifest, assets, manifestSha256: await digest(join(root, "assets.json")) };
}
