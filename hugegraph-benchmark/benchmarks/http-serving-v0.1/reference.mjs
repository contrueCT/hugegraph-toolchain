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

import { createHash } from "node:crypto";
import { lstat, readFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { isDeepStrictEqual } from "node:util";

export const directory = dirname(fileURLToPath(import.meta.url));
export const bundleFiles = [
    "catalog.json",
    "answers.json",
    "protocol.json",
    "scoring-policy.json",
    "calibration-plan.json",
    "release.json",
    "spec.md",
    "reference.mjs",
];
const sha = (value) => createHash("sha256").update(value).digest("hex");
const read = async (root, file) => JSON.parse(await readFile(join(root, file), "utf8"));
export function canonical(value) {
    if (Array.isArray(value)) return value.map(canonical);
    if (value !== null && typeof value === "object")
        return Object.fromEntries(
            Object.keys(value)
                .sort()
                .map((key) => [key, canonical(value[key])]),
        );
    return value;
}
// Deliberately independent of the submitted Groovy program: no script evaluation or DB calls.
export function expectedData(item) {
    const b = item.body.bindings;
    switch (item.oracle) {
        case "sum":
            return [b.x + b.y];
        case "map":
            return [{ id: b.x, name: "hgbench", values: [1, 2, 3], active: true }];
        case "rows":
            if (!Number.isInteger(b.n) || b.n < 1 || b.n > 4096) throw new Error("Invalid row bound");
            return Array.from({ length: b.n }, (_, i) => ({
                id: i,
                name: "hgbench",
                value: i * 3,
                active: i % 2 === 0,
            }));
        case "string":
        case "unicode":
            if (!Number.isInteger(b.n) || b.n < 1 || b.n > 1048576) throw new Error("Invalid string bound");
            return [(item.oracle === "string" ? "x" : b.text).repeat(b.n)];
        default:
            throw new Error("Unknown oracle");
    }
}
export function answerFor(item) {
    const data = expectedData(item),
        bytes = Buffer.from(JSON.stringify(canonical(data)));
    return {
        id: item.id,
        canonicalSha256: sha(bytes),
        resultDataCanonicalBytes: bytes.length,
        resultRows: data.length,
        payloadUtf8Bytes: item.shape === "single-string" ? Buffer.byteLength(data[0]) : null,
    };
}
export async function inspect(root = directory) {
    const lock = await read(root, "bundle.lock.json");
    if (
        lock.schemaVersion !== 1 ||
        lock.kind !== "http-serving-bundle" ||
        !isDeepStrictEqual(lock.files.map((f) => f.path).sort(), [...bundleFiles].sort())
    )
        throw new Error("HTTP suite file inventory differs");
    for (const file of lock.files) {
        const path = join(root, file.path),
            info = await lstat(path);
        if (
            !info.isFile() ||
            info.size > 2097152 ||
            info.size !== file.bytes ||
            sha(await readFile(path)) !== file.sha256
        )
            throw new Error(`HTTP suite file mismatch: ${file.path}`);
    }
    const catalog = await read(root, "catalog.json"),
        answers = await read(root, "answers.json");
    if (
        catalog.suiteId !== "hgbench-http-serving-v0.1" ||
        catalog.version !== lock.version ||
        catalog.cases.length !== 11 ||
        new Set(catalog.cases.map((c) => c.id)).size !== 11 ||
        !isDeepStrictEqual(answers.cases, catalog.cases.map(answerFor))
    )
        throw new Error("HTTP suite answer/identity mismatch");
    const release = await read(root, "release.json");
    return {
        suiteId: catalog.suiteId,
        version: catalog.version,
        bundleSha256: sha(await readFile(join(root, "bundle.lock.json"))),
        files: ["bundle.lock.json", ...bundleFiles],
        questions: catalog.cases.map((item, i) => ({ ...item, answer: answers.cases[i] })),
        cases: catalog.cases.map((item, i) => ({
            id: item.id,
            body: item.body,
            expectedStatus: 200,
            checks: [
                { pointer: "/status/code", equals: 200 },
                { pointer: "/result/data", canonicalSha256: answers.cases[i].canonicalSha256 },
            ],
        })),
        protocol: await read(root, "protocol.json"),
        scoring: await read(root, "scoring-policy.json"),
        calibration: await read(root, "calibration-plan.json"),
        release,
        performanceAccepted: false,
        executed: false,
    };
}
if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
    try {
        const [command, id, ...extra] = process.argv.slice(2);
        if (command === "--help" && !id) {
            process.stdout.write(
                "HTTP Serving reference: verify | questions [case-id] | materialize\nOffline only; verifies bundle and independent answers. No network, no files written.\n",
            );
        } else {
            if (
                !["verify", "questions", "materialize"].includes(command) ||
                extra.length ||
                (id && command !== "questions")
            )
                throw new Error("Use --help for commands");
            const result = await inspect();
            let data = result;
            if (command === "questions") {
                data = id ? result.questions.find((q) => q.id === id) : result.questions;
                if (!data) throw new Error(`Unknown HTTP case: ${id}`);
            }
            if (command === "verify")
                data = {
                    ...result,
                    cases: undefined,
                    questions: result.questions.map((q) => ({ id: q.id, shape: q.shape, answer: q.answer })),
                };
            process.stdout.write(JSON.stringify({ schemaVersion: 1, ok: true, command, data }) + "\n");
        }
    } catch (error) {
        process.stderr.write(JSON.stringify({ ok: false, message: String(error) }) + "\n");
        process.exitCode = 2;
    }
}
