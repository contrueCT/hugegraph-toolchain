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

import { access, readFile, realpath } from "node:fs/promises";
import { constants } from "node:fs";
import { delimiter, dirname, isAbsolute, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { componentSchema, manifestSchema, planSchema, resolvedSchema, TOOL_VERSION } from "./contracts.js";
import type { Component, Resolved } from "./contracts.js";
import { BenchError, fileHash, hash, readJson } from "./io.js";

export const packageRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
export const builtins: Record<string, string> = {
    "builtin:fixture": join(packageRoot, "components", "fixture", "component.json"),
    "builtin:descriptive": join(packageRoot, "components", "descriptive", "component.json"),
};

export async function executable(value: string, base: string): Promise<string> {
    if (value === "$NODE") return realpath(process.execPath);
    const candidates =
        isAbsolute(value) || value.includes("/")
            ? [resolve(base, value)]
            : (process.env.PATH ?? "")
                  .split(delimiter)
                  .filter(Boolean)
                  .map((dir) => join(dir, value));
    for (const candidate of candidates) {
        try {
            await access(candidate, constants.X_OK);
            return await realpath(candidate);
        } catch {
            /* Try the next PATH entry. */
        }
    }
    throw new BenchError("COMPONENT_UNAVAILABLE", `Executable not found: ${value}`, 3);
}

export async function resolveComponent(ref: string, base: string): Promise<Component> {
    const manifestPath = await realpath(builtins[ref] ?? resolve(base, ref));
    const manifest = await readJson(manifestPath, manifestSchema);
    const dir = dirname(manifestPath);
    const program = await executable(manifest.command[0]!, dir);
    const tracked = await Promise.all(manifest.trackedFiles.map((path) => realpath(resolve(dir, path))));
    const command = [
        program,
        ...manifest.command.slice(1).map((arg) => (arg.startsWith("./") ? resolve(dir, arg) : arg)),
    ];
    const paths = [...new Set([manifestPath, program, ...tracked])].sort();
    const files = await Promise.all(paths.map(async (path) => ({ path, sha256: await fileHash(path) })));
    return componentSchema.parse({ manifest, manifestPath, command, files });
}

export async function freezePlan(path: string): Promise<Resolved> {
    const plan = await readJson(path, planSchema);
    for (const [field, items] of [
        ["cases", plan.cases],
        ["variants", plan.variants],
    ] as const) {
        if (new Set(items.map((item) => item.id)).size !== items.length) {
            throw new BenchError("DUPLICATE_ID", `${field} must have unique IDs`);
        }
    }
    const base = dirname(resolve(path));
    const executor = await resolveComponent(plan.executor, base);
    const analyzer = await resolveComponent(plan.analyzer, base);
    if (executor.manifest.kind !== "executor" || analyzer.manifest.kind !== "analyzer") {
        throw new BenchError("COMPONENT_KIND_MISMATCH", "Executor/analyzer manifest kind does not match its role");
    }
    const content = {
        schemaVersion: 1 as const,
        toolVersion: TOOL_VERSION,
        scope: "controller-fixture" as const,
        plan,
        executor,
        analyzer,
    };
    return resolvedSchema.parse({ ...content, digest: hash(content) });
}

export async function verifyPlan(plan: Resolved): Promise<void> {
    const { digest, ...content } = plan;
    if (hash(content) !== digest) throw new BenchError("PLAN_DIGEST_MISMATCH", "Frozen plan was modified", 5);
    for (const component of [plan.executor, plan.analyzer]) {
        for (const file of component.files) {
            let current: string;
            try {
                current = await fileHash(file.path);
            } catch {
                throw new BenchError("COMPONENT_UNAVAILABLE", `Tracked file is unavailable: ${file.path}`, 3);
            }
            if (current !== file.sha256) {
                throw new BenchError("COMPONENT_CHANGED", `Tracked file changed: ${file.path}; compile a new plan`, 5);
            }
        }
    }
}

export async function loadPlan(path: string): Promise<Resolved> {
    const plan = await readJson(path, resolvedSchema);
    await verifyPlan(plan);
    return plan;
}

export async function controllerIdentity(): Promise<{ path: string; sha256: string }[]> {
    const modules = [
        "cli",
        "contracts",
        "io",
        "plans",
        "components",
        "store",
        "worker",
        "sf100",
        "sf100-protocol",
        "sf100-checkpoints",
        "sf100-loader",
        "benchmark-spec",
        "task-contracts",
        "task-list",
        "tasks",
        "task-worker",
        "task-process",
    ];
    return Promise.all(
        modules.map(async (name) => {
            const path = join(packageRoot, "dist", `${name}.js`);
            return { path, sha256: await fileHash(path) };
        }),
    );
}
