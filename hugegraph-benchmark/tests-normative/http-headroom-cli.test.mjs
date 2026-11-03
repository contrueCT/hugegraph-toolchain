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
import { httpHeadroomInputSchema } from "../dist/http-headroom-contracts.js";
import { reportHttpDriverHeadroom } from "../dist/http-calibration.js";

const exec = promisify(execFile);
const cli = (args) => exec(process.execPath, [resolve("dist/cli.js"), ...args], { timeout: 15000 });
const input = () => ({
    schemaVersion: 1,
    kind: "http-driver-headroom-evidence-input",
    operationVersion: "http-driver-headroom-002",
    planDirectory: "plan",
    executionDirectory: "execution",
    assessmentBindingFile: "assessment-binding.json",
    preDispatchLaunchRecordFile: "launch.json",
    controllerStartEvidenceFile: "controller-start.json",
    reference: {
        planLockSha256: "a".repeat(64),
        executionSpecSha256: "b".repeat(64),
        assessmentCriteriaSha256: "71fe2c244e0308d451e2759c06a457d576cf9afb73c526b00dfb978d4b524a0a",
        assessmentBindingSha256: "d".repeat(64),
        preDispatchLaunchRecordSha256: "e".repeat(64),
        controllerStartEvidenceSha256: "f".repeat(64),
        sourceSelectionSha256: "2b878425d6b5f9968777a14c3bd1622a6fc3349b7d5f88c139c773556dfde89a",
    },
});

test("driver-headroom input binds original references and refuses operator verdicts", () => {
    assert.deepEqual(httpHeadroomInputSchema.parse(input()), input());
    for (const edit of [
        (v) => (v.passed = true),
        (v) => (v.reference.status = "passed"),
        (v) => (v.operationVersion = "custom-injection"),
        (v) => (v.reference.executionSpecSha256 = "not-a-digest"),
        (v) => (v.reference.sourceSelectionSha256 = "c".repeat(64)),
        (v) => (v.planDirectory = "plan\ncommand"),
        (v) => (v.reference.assessmentCriteriaSha256 = "d".repeat(64)),
        (v) => (v.preDispatchLaunchRecordFile = "launch\r.json"),
    ]) {
        const value = input();
        edit(value);
        assert.equal(httpHeadroomInputSchema.safeParse(value).success, false);
    }
});

test("driver-headroom public help and schema explain offline scope and unknowns", async () => {
    const help = (await cli(["http", "calibration", "driver-headroom", "--help"])).stdout;
    assert.match(help, /36 cells \/ 12 JVM arms/);
    assert.match(help, /no HTTP, Docker/);
    assert.match(help, /recorded before dispatch/);
    assert.match(help, /16 GiB cap/);
    assert.match(help, /Exit 0 means report creation/);
    const schema = JSON.parse((await cli(["schema", "http-driver-headroom-evidence-input"])).stdout);
    assert.equal(schema.ok, true);
    assert.match(JSON.stringify(schema), /sourceSelectionSha256/);
});

test("invalid driver-headroom budgets reject before component dispatch or output", async (t) => {
    const root = await mkdtemp(join(tmpdir(), "hgbench-headroom-cli-"));
    t.after(() => rm(root, { recursive: true, force: true }));
    const out = join(root, "never-created");
    await assert.rejects(
        reportHttpDriverHeadroom(join(root, "missing.json"), out, { timeoutMs: 0, maxBytes: 1048576 }),
        (error) => error.code === "HTTP_HEADROOM_OPTIONS",
    );
    await assert.rejects(
        cli([
            "http",
            "calibration",
            "driver-headroom",
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
