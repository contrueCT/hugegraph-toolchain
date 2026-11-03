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

import { createHash, randomUUID } from "node:crypto";
import { createReadStream } from "node:fs";
import { mkdir, readFile, rename, writeFile, stat } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { z } from "zod";

export class BenchError extends Error {
    constructor(
        public code: string,
        message: string,
        public exitCode = 2,
        public retryable = false,
        public details?: unknown,
    ) {
        super(message);
    }
}

export function hash(value: unknown): string {
    const normalize = (item: unknown): unknown => {
        if (Array.isArray(item)) return item.map(normalize);
        if (item !== null && typeof item === "object") {
            return Object.fromEntries(
                Object.entries(item)
                    .sort(([a], [b]) => a.localeCompare(b))
                    .map(([k, v]) => [k, normalize(v)]),
            );
        }
        return item;
    };
    return createHash("sha256")
        .update(JSON.stringify(normalize(value)))
        .digest("hex");
}

export async function fileHash(path: string): Promise<string> {
    const digest = createHash("sha256");
    for await (const chunk of createReadStream(path)) digest.update(chunk);
    return digest.digest("hex");
}

export async function readJson<T>(path: string, schema: z.ZodType<T>, maxBytes = 8 * 1024 * 1024): Promise<T> {
    try {
        if ((await stat(path)).size > maxBytes)
            throw new BenchError("INPUT_TOO_LARGE", `File exceeds ${maxBytes} bytes`);
        return schema.parse(JSON.parse(await readFile(path, "utf8")));
    } catch (error) {
        if (error instanceof BenchError) throw error;
        if (error instanceof z.ZodError) {
            throw new BenchError("SCHEMA_INVALID", `Schema validation failed: ${path}`, 2, false, error.issues);
        }
        throw new BenchError("INPUT_UNREADABLE", `Cannot read JSON: ${path}: ${String(error)}`);
    }
}

export async function writeJson(path: string, value: unknown): Promise<void> {
    await mkdir(dirname(path), { recursive: true, mode: 0o700 });
    const temp = `${path}.${randomUUID()}.tmp`;
    await writeFile(temp, `${JSON.stringify(value, null, 2)}\n`, { mode: 0o600 });
    await rename(temp, path);
}

export function experimentDir(root: string, id: string): string {
    if (!/^exp-[a-f0-9-]{16,64}$/.test(id)) throw new BenchError("INVALID_EXPERIMENT_ID", "Invalid experiment ID");
    return join(resolve(root), "experiments", id);
}

export function errorInfo(error: unknown): BenchError {
    return error instanceof BenchError ? error : new BenchError("INTERNAL_ERROR", String(error), 6);
}

export const delay = (ms: number) => new Promise<void>((done) => setTimeout(done, ms));
