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
import { createReadStream } from "node:fs";
import { readFile, writeFile, stat } from "node:fs/promises";

export async function json(path, max = 8 * 1024 * 1024) {
    if ((await stat(path)).size > max) throw new Error(`Input exceeds ${max} bytes: ${path}`);
    return JSON.parse(await readFile(path, "utf8"));
}
export async function put(path, value) {
    await writeFile(path, JSON.stringify(value, null, 2) + "\n", { flag: "wx", mode: 0o600 });
}
export async function digest(path) {
    const hash = createHash("sha256");
    for await (const chunk of createReadStream(path)) hash.update(chunk);
    return hash.digest("hex");
}
export const bytesDigest = (bytes) => createHash("sha256").update(bytes).digest("hex");
export function argumentsFor(names) {
    const args = process.argv.slice(2);
    const out = {};
    for (let i = 0; i < args.length; i += 2) {
        if (!names.includes(args[i]) || !args[i + 1] || out[args[i]]) throw new Error("Invalid component arguments");
        out[args[i]] = args[i + 1];
    }
    for (const name of names) if (!out[name]) throw new Error(`Missing ${name}`);
    return out;
}
export async function entry(main, help) {
    if (process.argv.includes("--help")) {
        process.stdout.write(help + "\n");
        return;
    }
    try {
        await main();
    } catch (error) {
        process.stderr.write(JSON.stringify({ kind: "http-component-error", message: String(error) }) + "\n");
        process.exitCode = 6;
    }
}
export function valueDigest(value) {
    const canonical = (v) =>
        Array.isArray(v)
            ? v.map(canonical)
            : v !== null && typeof v === "object"
              ? Object.fromEntries(
                    Object.keys(v)
                        .sort()
                        .map((key) => [key, canonical(v[key])]),
                )
              : v;
    return bytesDigest(Buffer.from(JSON.stringify(canonical(value))));
}

export function expectedCheckDigest(rule) {
    if (Object.hasOwn(rule, "canonicalSha256")) {
        if (Object.hasOwn(rule, "equals") || !/^[a-f0-9]{64}$/.test(rule.canonicalSha256))
            throw new Error("Ambiguous or invalid answer digest");
        return rule.canonicalSha256;
    }
    if (!Object.hasOwn(rule, "equals")) throw new Error("Missing answer expectation");
    return valueDigest(rule.equals);
}
