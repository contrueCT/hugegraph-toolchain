/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
// Offline contract fixture only. It contains no executed HugeGraph/JVM/container proof.
import { mkdtemp, mkdir, writeFile, readFile, readdir, cp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, dirname } from "node:path";
import { balancedOrders, valueHash } from "../components/paired/core.mjs";
import {
    OBSERVER003 as O,
    observerSha as hash,
    observerTaskId,
    observer003Policy,
    observer003Schedule,
    observer003Scope,
} from "../components/http/observer003-contract.mjs";
export const put = async (p, v) => {
    await mkdir(dirname(p), { recursive: true });
    await writeFile(p, JSON.stringify(v, null, 2) + "\n");
};
export const get = async (p) => JSON.parse(await readFile(p, "utf8"));
export async function observerFixture(t) {
    const root = await mkdtemp(join(tmpdir(), "hgbench-observer003-"));
    t.after(() => rm(root, { recursive: true, force: true }));
    const plan = join(root, "plan"),
        execution = join(root, "execution"),
        origin = "/original/observer003/plan",
        sourceRoot = "/original/aa005/source",
        nodeHome = "/original/node";
    await mkdir(plan);
    await mkdir(execution);
    const runtime = {
        sourceRoot,
        nodeHome,
        hostPython: "/usr/bin/python3.12",
        dockerExecutable: "/usr/bin/docker",
        driver: {
            image: "sha256:" + "1".repeat(64),
            python: "/usr/local/bin/python3",
            cpus: 1,
            memoryBytes: 805306368,
            pids: 128,
            tmpfsBytes: 67108864,
            wallSeconds: 960,
            shutdownSeconds: 5,
            maxLogBytes: 1048576,
        },
        collector: { intervalMs: 1000, durationMs: 800000, maxSamples: 801, maxOutputBytes: 33554432 },
    };
    const on = {
            mode: "forward",
            purpose: "calibration",
            image: runtime.driver.image,
            budget: { cpus: 2, memoryBytes: 2147483648, wallSeconds: 1500 },
            jvmObservation: { parser: { timeoutMs: 10000 } },
        },
        off = structuredClone(on);
    delete off.jvmObservation;
    await put(join(plan, "runtime-selection.json"), runtime);
    await put(join(plan, "settings-jvm-on.json"), on);
    await put(join(plan, "settings-jvm-off.json"), off);
    await put(join(plan, "build.json"), { sourceCommit: O.commit, distribution: "/original/build", files: [] });
    await put(join(plan, "environment.json"), { kind: "offline-observer-fixture" });
    await put(join(plan, "source-verification.json"), { membersVerified: 529 });
    await put(join(plan, "scope.json"), observer003Scope());
    await cp(
        new URL("./fixtures/known002-source-selection.lock.json", import.meta.url),
        join(plan, "source-selection.lock.json"),
    );
    const shared = {
            buildSha256: hash(await readFile(join(plan, "build.json"))),
            mode: "forward",
            sourceLockSha256: O.sourceSha256,
            sameTargetAndDriverResources: true,
            cases: O.cases,
        },
        policies = Object.fromEntries(["J", "C"].map((g) => [g, observer003Policy(g, shared)]));
    for (const g of ["J", "C"]) await put(join(plan, "groups", g, "policy.json"), policies[g]);
    const orders = Object.fromEntries(
        ["J", "C"].map((g) => [g, balancedOrders(O.pairs, O.id + ":" + g + ":order-v1")]),
    );
    const sequence = {
        schemaVersion: 1,
        kind: "http-observer-cost-003-operation-recipe",
        id: O.id,
        sourceRoot,
        attemptToken: "a".repeat(16),
        mode: "draft",
        executed: false,
        dispatchImplemented: false,
        groupOrder: ["J", "C"],
        blockOrders: orders,
        physicalArms: [],
    };
    for (const item of observer003Schedule(orders)) {
        const runId = "hoc3-" + sequence.attemptToken + "-" + item.cellId.toLowerCase(),
            arm = {
                ...item,
                runId,
                directory: origin + "/arms/" + item.cellId,
                httpRequestId: runId + "-http",
                targetRequestId: runId + "-target",
                collectorRequestId: item.collector ? runId + "-collector" : null,
            };
        sequence.physicalArms.push(arm);
        const profile = {
            schemaVersion: 1,
            kind: "http-benchmark-profile",
            name: "offline-contract-fixture",
            benchmark: {
                suiteId: "hgbench-http-serving-v0.1",
                version: "0.1.0-draft.1",
                bundleSha256: O.bundleSha256,
                coverage: "subset",
                caseIds: O.cases,
                purpose: "calibration",
            },
            serviceChecks: true,
            responseEncoding: "gzip",
            authorizationEnv: null,
            endpoint: "http://127.0.0.1:18080/gremlin",
            target: { build: O.commit, mode: "forward", evidenceFiles: [] },
            warmupMs: 30000,
            durationMs: 120000,
            requestTimeoutMs: 5000,
            drainMs: 5000,
            repeats: 1,
            maxTraceBytes: 268435456,
            maxRequestsPerPhase: 2000,
            maxResponseBytes: 2097152,
            cases: O.cases.map((id) => ({ id })),
            loads: [{ id: "open-10", model: "open", ratePerSecond: 10, maxInFlight: 4, maxStartDelayMs: 1000 }],
        };
        await put(join(plan, "arms", item.cellId, "profile.json"), profile);
        await put(join(plan, "arms", item.cellId, "settings.json"), item.jvmObservation ? on : off);
    }
    const budgets = {
        httpTaskTimeoutMs: 609000,
        workflowDeadlineMs: 549000,
        operationMaximumMs: 30000000,
        operationCleanupMs: 90000,
        maxWorkBytes: 34359738368,
    };
    await put(join(plan, "budgets.json"), budgets);
    await put(join(plan, "sequence.json"), sequence);
    const spec = {
        schemaVersion: 1,
        kind: "http-observer-cost-execution-spec",
        planRoot: origin,
        sourceRoot,
        node: nodeHome + "/bin/node",
        nodeExecutable: { bytes: 123, sha256: "2".repeat(64) },
        runtime,
        owner: "1000:1000",
        executorSha256: O.executorSha256,
        generatorSha256: O.generatorSha256,
        runnable: false,
        physicalArms: 16,
        budgets,
        expectedTasks: sequence.physicalArms.map((a) => ({
            runId: a.runId,
            http: observerTaskId(a.httpRequestId),
            target: observerTaskId(a.targetRequestId),
            collector: a.collector ? observerTaskId(a.collectorRequestId) : null,
        })),
    };
    const input = {
        schemaVersion: 1,
        kind: "http-observer-cost-evidence-input",
        operationVersion: O.id,
        planDirectory: "plan",
        executionDirectory: "execution",
        reference: { sourceSelectionSha256: O.sourceSha256 },
    };
    async function reseal() {
        const files = [];
        async function walk(p = "") {
            for (const e of await readdir(join(plan, p), { withFileTypes: true })) {
                const name = p ? p + "/" + e.name : e.name;
                if (name === "plan.lock.json") continue;
                if (e.isDirectory()) await walk(name);
                else {
                    const b = await readFile(join(plan, name));
                    files.push({ path: name, bytes: b.length, sha256: hash(b) });
                }
            }
        }
        await walk();
        files.sort((a, b) => a.path.localeCompare(b.path));
        await put(join(plan, "plan.lock.json"), {
            schemaVersion: 1,
            kind: "http-observer-cost-plan-lock",
            generator: { sha256: O.generatorSha256 },
            fixedPlanningHelper: { sha256: O.helperSha256 },
            files,
            contentDigest: valueHash(files),
        });
        input.reference.planLockSha256 = hash(await readFile(join(plan, "plan.lock.json")));
        spec.planLockSha256 = input.reference.planLockSha256;
        await put(join(execution, "execution-spec.json"), spec);
        input.reference.executionSpecSha256 = hash(await readFile(join(execution, "execution-spec.json")));
        await put(join(execution, "execution.lock.json"), {
            schemaVersion: 1,
            kind: "http-observer-cost-execution-lock",
            specSha256: input.reference.executionSpecSha256,
        });
        await put(join(root, "input.json"), input);
    }
    await reseal();
    return { root, plan, execution, spec, sequence, policies, input, inputFile: join(root, "input.json"), reseal };
}
export function observerArms(policies) {
    return observer003Schedule(
        Object.fromEntries(["J", "C"].map((g) => [g, balancedOrders(O.pairs, policies[g].orderSeed)])),
    ).map((a, index) => ({
        ...a,
        runId: "synthetic-" + index,
        valid: true,
        rawManifestSha256: hash("raw" + index),
        values: Object.fromEntries(
            policies[a.group].metrics.map((m, i) => [
                m.id,
                { value: a.arm === "A" ? 10 + i : 20 + i + O.pairs.indexOf(a.blockId), observations: 1200 },
            ]),
        ),
    }));
}
