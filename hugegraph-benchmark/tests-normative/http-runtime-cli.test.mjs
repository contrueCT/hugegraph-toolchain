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
import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, writeFile, rm, access } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { httpRuntimeProvenanceSpecSchema } from "../dist/http-runtime-contracts.js";
const exec = promisify(execFile),
    cli = resolve("dist/cli.js"),
    MiB = 1024 * 1024;
const spec = () => ({
    schemaVersion: 1,
    kind: "http-runtime-provenance-spec",
    sourceOrigin: "linux-procfs",
    budgets: {
        wallMs: 60000,
        commandMs: 5000,
        handshakeMs: 10000,
        cleanupMs: 5000,
        maxRawBytes: 256 * 1024,
        maxEvidenceBytes: 8 * MiB,
        maxExecutableBytes: 256 * MiB,
        maxHashBytes: 768 * MiB,
    },
    container: { cpus: 0.25, memoryBytes: 128 * MiB, pids: 32, tmpfsBytes: 8 * MiB },
});
const invoke = (args, options = {}) =>
    exec(process.execPath, [cli, "http", "runtime", ...args], { timeout: 10000, maxBuffer: 2 * MiB, ...options });

test("runtime CLI help and schema expose effects, original identity and explicit budgets without execution", async () => {
    for (const action of [undefined, "probe", "verify", "stop"])
        assert.match((await invoke([...(action ? [action] : []), "--help"])).stdout, /runtime|probe|proof/);
    const schema = JSON.parse((await exec(process.execPath, [cli, "schema", "http-runtime-provenance-spec"])).stdout);
    assert.equal(schema.ok, true);
    assert.equal(schema.data.additionalProperties, false);
    assert.deepEqual(httpRuntimeProvenanceSpecSchema.parse(spec()), spec());
    for (const changed of [
        { ...spec(), command: ["arbitrary"] },
        { ...spec(), sourceOrigin: "fixture" },
        { ...spec(), budgets: { ...spec().budgets, wallMs: 5000 } },
        { ...spec(), budgets: { ...spec().budgets, cleanupMs: 10001 } },
        { ...spec(), container: { ...spec().container, pids: 128 } },
    ])
        assert.equal(httpRuntimeProvenanceSpecSchema.safeParse(changed).success, false);
});

test("invalid runtime spec refuses before invoking the component or creating an output", async (t) => {
    const root = await mkdtemp(join(tmpdir(), "hgbench-runtime-cli-"));
    t.after(() => rm(root, { recursive: true, force: true }));
    const input = join(root, "invalid.json"),
        output = join(root, "proof");
    await writeFile(input, JSON.stringify({ ...spec(), command: ["not-allowed"] }));
    await assert.rejects(
        invoke(["probe", input, "--runtime", join(root, "missing-runtime"), "--out", output], {
            env: { ...process.env, HGBENCH_PYTHON: join(root, "missing-component-runtime") },
        }),
        (error) => {
            assert.equal(error.code, 2);
            assert.equal(JSON.parse(error.stdout).error.code, "SCHEMA_INVALID");
            return true;
        },
    );
    await assert.rejects(access(output));
});

test("runtime controller rejects malformed component envelopes and preserves declared component failures", async (t) => {
    const root = await mkdtemp(join(tmpdir(), "hgbench-runtime-protocol-"));
    t.after(() => rm(root, { recursive: true, force: true }));
    for (const [id, raw, exitCode, expected] of [
        ["null", "null", 0, "HTTP_RUNTIME_PROTOCOL"],
        ["array", "[]", 0, "HTTP_RUNTIME_PROTOCOL"],
        ["data-array", '{"ok":true,"data":[]}', 0, "HTTP_RUNTIME_PROTOCOL"],
        ["component-failure", '{"ok":false,"error":{"code":"PROBE_IDENTITY_CHANGED"}}', 6, "HTTP_RUNTIME_FAILED"],
    ]) {
        const fake = join(root, id);
        await writeFile(
            fake,
            `#!${process.execPath}\nprocess.stdout.write(${JSON.stringify(raw)}); process.exitCode=${exitCode};\n`,
            { mode: 0o700 },
        );
        await assert.rejects(
            invoke(["verify", join(root, "proof")], { env: { ...process.env, HGBENCH_PYTHON: fake } }),
            (error) => {
                const reply = JSON.parse(error.stdout);
                assert.equal(reply.error.code, expected);
                assert.equal(error.code, 6);
                if (id === "component-failure") assert.equal(reply.error.details.code, "PROBE_IDENTITY_CHANGED");
                return true;
            },
        );
    }
});
