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
import { mkdtemp, readFile, writeFile, access, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { resolve, join } from "node:path";
import { httpEngineeringAcceptanceInputSchema } from "../dist/http-engineering-acceptance-contracts.js";
import { reportHttpEngineeringAcceptance } from "../dist/http-calibration.js";

const exec = promisify(execFile);
const cli = (args) => exec(process.execPath, [resolve("dist/cli.js"), ...args], { timeout: 20000 });
const empty = () => ({
    schemaVersion: 1,
    kind: "http-engineering-acceptance-input",
    evidence: { aa005: null, repeatabilityAa: null, knownRegression: null, driverHeadroom: null, observerCost: null },
    comparison: null,
});

test("acceptance input requires explicit evidence absence and rejects operator judgments or commands", () => {
    assert.deepEqual(httpEngineeringAcceptanceInputSchema.parse(empty()), empty());
    for (const edit of [
        (v) => (v.passed = true),
        (v) => (v.evidence.knownRegression = { inputFile: "known.json", sha256: "a".repeat(64), passed: true }),
        (v) => (v.evidence.knownRegression = { inputFile: "known.json", sha256: "bad" }),
        (v) => (v.evidence.aa005 = { evidenceRoot: "original", script: "custom.mjs" }),
        (v) => (v.evidence.aa005 = { evidenceRoot: "original\ncommand" }),
        (v) => {
            v.evidence.aa005 = { evidenceRoot: "original" };
            v.evidence.repeatabilityAa = {
                planDirectory: "plan",
                planLockSha256: "a".repeat(64),
                executionSetFile: "set.json",
                executionSetSha256: "b".repeat(64),
            };
        },
        (v) => delete v.evidence.observerCost,
        (v) => (v.comparison = { planDirectory: "p", planLockSha256: "a".repeat(64) }),
    ]) {
        const value = empty();
        edit(value);
        assert.equal(httpEngineeringAcceptanceInputSchema.safeParse(value).success, false);
    }
});

test("acceptance public help and schema disclose offline effects, partial coverage and shared budgets", async () => {
    const help = (await cli(["http", "calibration", "acceptance", "--help"])).stdout;
    assert.match(help, /no HTTP, Docker or SSH/);
    assert.match(help, /All seven engineering gates and eleven cases/);
    assert.match(help, /Shared deadline/);
    assert.match(help, /Exit 0 means report creation/);
    const schema = JSON.parse((await cli(["schema", "http-engineering-acceptance-input"])).stdout);
    assert.equal(schema.ok, true);
    assert.match(JSON.stringify(schema), /calibrationIndexSha256/);
});

test("acceptance invalid budgets reject before component dispatch or filesystem output", async (t) => {
    const root = await mkdtemp(join(tmpdir(), "hgbench-acceptance-budget-"));
    t.after(() => rm(root, { recursive: true, force: true }));
    const out = join(root, "not-created");
    await assert.rejects(
        reportHttpEngineeringAcceptance(join(root, "missing.json"), out, { timeoutMs: 0, maxBytes: 1048576 }),
        (error) => error.code === "HTTP_ENGINEERING_ACCEPTANCE_OPTIONS",
    );
    await assert.rejects(
        cli([
            "http",
            "calibration",
            "acceptance",
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

test("empty acceptance public report preserves all seven gates and eleven cases as unknown", async (t) => {
    const root = await mkdtemp(join(tmpdir(), "hgbench-acceptance-empty-"));
    t.after(() => rm(root, { recursive: true, force: true }));
    const input = join(root, "input.json"),
        out = join(root, "report");
    await writeFile(input, JSON.stringify(empty()));
    const reply = JSON.parse(
        (
            await cli([
                "http",
                "calibration",
                "acceptance",
                input,
                "--out",
                out,
                "--timeout-ms",
                "10000",
                "--max-bytes",
                "4194304",
            ])
        ).stdout,
    );
    assert.equal(reply.ok, true);
    const report = JSON.parse(await readFile(join(out, "report.json"), "utf8"));
    assert.equal(report.engineering.accepted, false);
    assert.equal(report.engineering.status, "unknown");
    assert.equal(report.engineering.gates.length, 7);
    assert.ok(report.engineering.gates.every((gate) => gate.status === "unknown"));
    assert.equal(report.coverage.cases.length, 11);
    assert.ok(report.coverage.cases.every((row) => row.status === "unknown"));
    assert.equal(report.performanceAccepted, false);
    assert.equal(report.qualificationAccepted, false);
    await access(join(out, "report.html"));
    await access(join(out, "report.lock.json"));
    await access(join(out, "calibration-evidence-index.json"));
    await assert.rejects(
        cli([
            "http",
            "calibration",
            "acceptance",
            input,
            "--out",
            out,
            "--timeout-ms",
            "10000",
            "--max-bytes",
            "4194304",
        ]),
    );
});
