/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
import test from "node:test";
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { mkdtemp, access, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { resolve, join } from "node:path";
import { httpKnownRegressionInputSchema } from "../dist/http-known-regression-contracts.js";
import { reportHttpKnownRegression } from "../dist/http-calibration.js";

const exec = promisify(execFile);
const cli = (args) => exec(process.execPath, [resolve("dist/cli.js"), ...args], { timeout: 15000 });
const input = () => ({
    schemaVersion: 1,
    kind: "http-known-regression-evidence-input",
    operationVersion: "aa005-async-end-to-end-p99-v1",
    planDirectory: "plan",
    executionDirectory: "execution",
    reference: {
        planLockSha256: "a".repeat(64),
        executionSpecSha256: "b".repeat(64),
        sourceSelectionSha256: "2b878425d6b5f9968777a14c3bd1622a6fc3349b7d5f88c139c773556dfde89a",
    },
});

test("known-regression input binds original references and refuses operator verdicts", () => {
    assert.deepEqual(httpKnownRegressionInputSchema.parse(input()), input());
    for (const edit of [
        (v) => (v.passed = true),
        (v) => (v.reference.status = "passed"),
        (v) => (v.operationVersion = "custom-injection"),
        (v) => (v.reference.executionSpecSha256 = "not-a-digest"),
        (v) => (v.reference.sourceSelectionSha256 = "c".repeat(64)),
        (v) => (v.planDirectory = "plan\ncommand"),
    ]) {
        const value = input();
        edit(value);
        assert.equal(httpKnownRegressionInputSchema.safeParse(value).success, false);
    }
});

test("known-regression public help and schema explain offline scope and unknowns", async () => {
    const help = (await cli(["http", "calibration", "known-regression", "--help"])).stdout;
    assert.match(help, /16 arms \/ 8 pairs/);
    assert.match(help, /no HTTP, Docker/);
    assert.match(help, /Exit 0 means report creation/);
    const schema = JSON.parse((await cli(["schema", "http-known-regression-evidence-input"])).stdout);
    assert.equal(schema.ok, true);
    assert.match(JSON.stringify(schema), /sourceSelectionSha256/);
});

test("invalid known-regression budgets reject before component dispatch or output", async (t) => {
    const root = await mkdtemp(join(tmpdir(), "hgbench-known-cli-"));
    t.after(() => rm(root, { recursive: true, force: true }));
    const out = join(root, "never-created");
    await assert.rejects(
        reportHttpKnownRegression(join(root, "missing.json"), out, { timeoutMs: 0, maxBytes: 1048576 }),
        (error) => error.code === "HTTP_KNOWN_REGRESSION_OPTIONS",
    );
    await assert.rejects(
        cli([
            "http",
            "calibration",
            "known-regression",
            "missing.json",
            "--out",
            out,
            "--timeout-ms",
            "1000",
            "--max-bytes",
            "0",
        ]),
        (error) => JSON.parse(error.stdout).error.code === "CLI_USAGE",
    );
    await assert.rejects(access(out), { code: "ENOENT" });
});
