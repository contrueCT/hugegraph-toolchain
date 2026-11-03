/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
import { readdir, realpath } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { spawn } from "node:child_process";

// These files own the same fixed localhost protocol port. Keep new users of that fixture in this group.
const serialFiles = [
    "http-paired-evidence.test.mjs",
    "http-capacity-positive.test.mjs",
    "http-capacity-boundary.test.mjs",
    "http-single-observation.test.mjs",
];

async function inventory() {
    const root = await realpath(fileURLToPath(new URL("../", import.meta.url)));
    const entries = await readdir(join(root, "tests-normative"), { withFileTypes: true });
    const tests = entries.filter((entry) => entry.name.endsWith(".test.mjs"));
    if (tests.some((entry) => !entry.isFile())) throw new Error("Normative test files must be regular files");
    const files = tests.map((entry) => entry.name).sort();
    const serial = new Set(serialFiles);
    if (serial.size !== serialFiles.length || serialFiles.some((name) => !files.includes(name)))
        throw new Error("Fixed-port test inventory is duplicated or incomplete");
    const parallel = files.filter((name) => !serial.has(name));
    const all = [...parallel, ...serialFiles];
    if (new Set(all).size !== files.length || all.length !== files.length || files.length === 0)
        throw new Error("Normative test groups must cover every file exactly once");
    return {
        root,
        counts: { total: files.length, parallel: parallel.length, serial: serialFiles.length },
        groups: [
            { id: "remaining-tests", concurrency: "node-default", files: parallel },
            { id: "fixed-port-http-protocol", concurrency: 1, files: serialFiles },
        ],
    };
}

async function run(grouping) {
    let child = null;
    let interruption = null;
    let firstFailure = 0;
    const handlers = new Map();
    for (const signal of ["SIGINT", "SIGTERM"]) {
        const handler = () => {
            interruption ??= signal;
            child?.kill(interruption);
        };
        handlers.set(signal, handler);
        process.on(signal, handler);
    }
    try {
        for (const group of grouping.groups) {
            if (interruption) return { signal: interruption };
            if (group.files.length === 0) continue;
            process.stdout.write(`\n# HG-Bench normative group: ${group.id} (${group.files.length} files)\n`);
            const args = [
                "--test",
                ...(group.concurrency === 1 ? ["--test-concurrency=1"] : []),
                ...group.files.map((name) => join("tests-normative", name)),
            ];
            const result = await new Promise((accept, reject) => {
                child = spawn(process.execPath, args, { cwd: grouping.root, stdio: "inherit", detached: false });
                child.once("error", reject);
                child.once("close", (code, signal) => {
                    child = null;
                    accept({ code, signal });
                });
                if (interruption) child.kill(interruption);
            });
            if (interruption) return { signal: interruption };
            if (result.signal) return result;
            if (result.code !== 0) firstFailure ||= result.code ?? 1;
        }
        return { code: firstFailure };
    } finally {
        for (const [signal, handler] of handlers) process.removeListener(signal, handler);
    }
}

try {
    const args = process.argv.slice(2);
    if (args.length && (args.length !== 1 || args[0] !== "--list"))
        throw new Error("Usage: node tools/test-normative.mjs [--list]");
    const grouping = await inventory();
    if (args[0] === "--list") process.stdout.write(JSON.stringify(grouping, null, 2) + "\n");
    else {
        const result = await run(grouping);
        if (result.signal) process.kill(process.pid, result.signal);
        else process.exitCode = result.code ?? 1;
    }
} catch (error) {
    process.stderr.write(`Normative test runner: ${error.message}\n`);
    process.exitCode = 1;
}
