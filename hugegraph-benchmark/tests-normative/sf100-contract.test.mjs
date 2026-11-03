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

import assert from "node:assert/strict";
import { test } from "node:test";
import { mkdtemp, readFile, writeFile, rm, readdir } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { inspectSf100Plan, operationIds } from "../dist/sf100.js";

const root = resolve(import.meta.dirname, "..");
const template = join(root, "benchmarks/ldbc-snb-interactive-v1/sf100-plan.json");
async function fixture(fn) {
    const directory = await mkdtemp(join(tmpdir(), "hgbench-contract-"));
    try {
        const plan = JSON.parse(await readFile(template, "utf8"));
        const catalog = JSON.parse(
            await readFile(join(root, "benchmarks/ldbc-snb-interactive-v1/candidates/sf100.json"), "utf8"),
        );
        plan.catalog = "catalog.json";
        const file = join(directory, "plan.json");
        const save = async () => {
            await writeFile(file, JSON.stringify(plan));
            await writeFile(join(directory, "catalog.json"), JSON.stringify(catalog));
        };
        await save();
        await fn({ plan, catalog, file, directory, save });
    } finally {
        await rm(directory, { recursive: true, force: true });
    }
}
function cli(args, cwd = root) {
    const result = spawnSync(process.execPath, [join(root, "dist/cli.js"), ...args], { cwd, encoding: "utf8" });
    assert.equal(result.stderr, "");
    return { code: result.status, body: JSON.parse(result.stdout) };
}

test("SF100 draft compiles offline with explicit blockers and stable identity", async () => {
    await fixture(async ({ file, directory, catalog, save }) => {
        const before = await readdir(directory);
        const result = await inspectSf100Plan(file);
        assert.equal(result.contractValid, true);
        assert.equal(result.executionReady, false);
        assert.equal(result.executed, false);
        assert.equal(result.resourceEstimate.totalBytes, null);
        assert.equal(result.identity.plan.workload.operations.length, 29);
        assert.ok(!result.blockers.some((b) => b.code === "ASSETS_UNFROZEN"));
        assert.ok(result.blockers.some((b) => b.code === "RUNTIME_NOT_IMPLEMENTED"));
        assert.equal(result.digest, (await inspectSf100Plan(file)).digest);
        assert.deepEqual(await readdir(directory), before);
        const stages = new Set();
        for (const stage of result.stages) {
            assert.ok(stage.dependsOn.every((id) => stages.has(id)));
            stages.add(stage.id);
        }
        for (const asset of catalog.assets) {
            asset.sha256 = null;
            asset.bytes = null;
        }
        await save();
        const unfrozen = await inspectSf100Plan(file);
        assert.ok(unfrozen.blockers.some((b) => b.code === "ASSETS_UNFROZEN"));
        assert.equal(unfrozen.executionReady, false);
    });
});

test("SF100 rejects duplicate operations, smaller-scale assets and changed upstream identities", async () => {
    for (const mutate of [
        ({ plan }) => {
            plan.workload.operations[0] = "IS1";
        },
        ({ plan }) => {
            plan.workload.operations[0] = "CUSTOM";
        },
        ({ catalog }) => {
            catalog.assets[0].scale = "0.1";
        },
        ({ catalog }) => {
            catalog.assets[2].updatePartitions = 2;
        },
        ({ catalog }) => {
            catalog.assets[1].url += "-different";
        },
        ({ catalog }) => {
            catalog.driver.commit = "a".repeat(40);
        },
        ({ catalog }) => {
            catalog.assets[0].sha256 = "a".repeat(64);
        },
        ({ catalog }) => {
            catalog.assets[0].bytes += 1;
        },
        ({ plan }) => {
            plan.execution.restore.method = "verified-snapshot";
        },
        ({ plan }) => {
            plan.validation.onFailure = "ignore";
        },
        ({ plan }) => {
            plan.unrecognized = true;
        },
    ]) {
        await fixture(async (context) => {
            mutate(context);
            await context.save();
            await assert.rejects(inspectSf100Plan(context.file), (error) =>
                ["SCHEMA_INVALID", "SF100_CONTRACT_INVALID"].includes(error.code),
            );
        });
    }
});

test("SF100 validates topology and byte budgets without Number precision loss", async () => {
    await fixture(async ({ plan, file, save }) => {
        plan.budgets = {
            downloadPerAssetBytes: "100",
            expandedBytes: "100",
            workspaceBytes: "9007199254740993",
            reserveBytes: "9007199254740992",
            estimate: { path: "future-estimate.json", sha256: "a".repeat(64) },
        };
        await save();
        assert.equal((await inspectSf100Plan(file)).executionReady, false);
        plan.budgets.reserveBytes = "9007199254740993";
        await save();
        await assert.rejects(inspectSf100Plan(file), /reserved free space/);
        plan.budgets = null;
        plan.target = {
            backend: "hstore",
            graph: "sf100",
            isolation: "dedicated-benchmark-graph",
            replicas: 2,
            partitions: 32,
            nodes: ["server", "pd", "driver", "store", "store"].map((role, i) => ({
                id: `n${i}`,
                role,
                host: `host${i}`,
                cpuCores: 4,
                memoryBytes: "1000",
                diskBudgetBytes: "10000",
                version: "test",
                configSha256: "a".repeat(64),
            })),
        };
        await save();
        assert.equal((await inspectSf100Plan(file)).topology.deployment, "multi-host");
        plan.target.nodes[4].host = plan.target.nodes[3].host;
        await save();
        await assert.rejects(inspectSf100Plan(file), /two Store hosts/);
        plan.target.nodes[4].host = "host4";
        plan.target.replicas = 3;
        await save();
        await assert.rejects(inspectSf100Plan(file), /Replica count/);
    });
});

test("single-host multi-Store calibration is explicit and cannot claim cross-host evidence", async () => {
    await fixture(async ({ plan, file, save }) => {
        plan.target = {
            backend: "hstore",
            graph: "sf100",
            isolation: "dedicated-benchmark-graph",
            deployment: "single-host-multi-store",
            replicas: 1,
            partitions: 12,
            nodes: ["server", "pd", "driver", "store", "store", "store"].map((role, i) => ({
                id: `n${i}`,
                role,
                host: "physical-host-1",
                cpuCores: 2,
                memoryBytes: "1000",
                diskBudgetBytes: "10000",
                version: "test",
                configSha256: "a".repeat(64),
            })),
        };
        await save();
        const report = await inspectSf100Plan(file);
        assert.equal(report.topology.declaredHosts, 1);
        assert.equal(report.topology.declaredStoreHosts, 1);
        assert.equal(report.topology.driverSharesStoreHost, true);
        assert.equal(report.topology.crossHostBehaviorVerified, false);
        assert.equal(report.topology.comparisonGroup, "single-host-multi-store");
        assert.equal(report.executionReady, false);
        plan.target.nodes[3].host = "container-alias";
        await save();
        await assert.rejects(inspectSf100Plan(file), /same host/);
        plan.target.nodes[3].host = "physical-host-1";
        plan.target.nodes.splice(4);
        await save();
        await assert.rejects(inspectSf100Plan(file), /two Store nodes/);
    });
});

test("declared component capabilities are identity checked and never imply runtime readiness", async () => {
    await fixture(async ({ plan, file, directory, save }) => {
        const manifest = {
            protocolVersion: 1,
            kind: "sf100-component",
            role: "driver",
            id: "test-only",
            version: "0",
            command: ["this-program-must-never-execute"],
            trackedFiles: [{ path: "absent.jar", sha256: "a".repeat(64) }],
            capabilities: {
                scale: "100",
                serializer: "CsvBasic",
                operations: operationIds,
                checkpoint: "restart-from-restored-state",
            },
        };
        const body = JSON.stringify(manifest);
        await writeFile(join(directory, "driver.json"), body);
        plan.components.driver = { path: "driver.json", sha256: createHash("sha256").update(body).digest("hex") };
        await save();
        const report = await inspectSf100Plan(file);
        assert.equal(report.executionReady, false);
        assert.ok(report.blockers.some((b) => b.code === "OPERATIONS_UNVALIDATED"));
        await writeFile(join(directory, "driver.json"), body + " ");
        await assert.rejects(inspectSf100Plan(file), /digest mismatch/);
    });
});

test("CLI distinguishes valid drafts, strict readiness failure, schema failure and fixture execution", async () => {
    await fixture(async ({ file, directory, plan, save }) => {
        const validated = cli(["benchmark", "validate", file], directory);
        assert.equal(validated.code, 0);
        assert.equal(validated.body.data.executionReady, false);
        const strict = cli(["benchmark", "plan", file, "--require-ready"], directory);
        assert.equal(strict.code, 3);
        assert.equal(strict.body.ok, false);
        assert.equal(strict.body.error.code, "SF100_NOT_READY");
        const run = cli(["run", "--plan", file, "--dry-run"], directory);
        assert.equal(run.code, 2);
        assert.equal(run.body.error.code, "SCHEMA_INVALID");
        plan.execution.measurementSeconds = 0;
        await save();
        assert.equal(cli(["benchmark", "validate", file], directory).code, 2);
        assert.deepEqual((await readdir(directory)).sort(), ["catalog.json", "plan.json"]);
    });
    assert.ok(cli(["benchmark", "list"]).body.data.some((s) => s.scale === "100"));
    assert.ok(cli(["schema", "sf100-plan"]).body.data.properties.workload);
});

test("CLI exposes the independent specification checker and all question contracts without execution", () => {
    const spec = cli(["benchmark", "spec"]);
    assert.equal(spec.code, 0);
    assert.equal(spec.body.data.operations, 29);
    assert.equal(spec.body.data.releaseReady, false);
    assert.equal(cli(["benchmark", "spec", "--require-release"]).code, 3);
    assert.equal(cli(["benchmark", "questions"]).body.data.length, 29);
    assert.equal(cli(["benchmark", "questions", "--id", "IC14"]).body.data.resultShape, "rows");
    assert.equal(cli(["benchmark", "questions", "--id", "IC99"]).code, 2);
    assert.equal(cli(["schema", "ldbc-answer-scorecard"]).body.data.properties.counts.required.length, 29);
});
