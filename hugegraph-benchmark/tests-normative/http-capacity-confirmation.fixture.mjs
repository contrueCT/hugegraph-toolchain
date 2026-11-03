/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
import { mkdtemp, mkdir, writeFile, readFile, rm } from "node:fs/promises";
import { join, resolve, dirname } from "node:path";
import { tmpdir } from "node:os";
import { planHttpCapacity } from "../dist/http-capacity.js";
import { reportCapacity } from "../components/http/capacity-evidence.mjs";
const MiB = 1024 ** 2;
const diskSave = (file, value) => writeFile(file, JSON.stringify(value, null, 2) + "\n");
// Inert, self-contained public planning fixture. No Docker/HTTP process is started.
// Its real independently rebuilt report proves all eleven cases are unavailable.
export async function missingExploration(t) {
    const root = await mkdtemp(join(tmpdir(), "hgbench-confirmation-"));
    t.after(() => rm(root, { recursive: true, force: true }));
    const image = "sha256:" + "a".repeat(64);
    const settings = {
        schemaVersion: 1,
        kind: "hugegraph-http-environment-settings",
        purpose: "calibration",
        mode: "forward",
        distribution: "/fixture/distribution",
        javaHome: "/fixture/jdk",
        image,
        python: "/usr/local/bin/python3",
        dockerSocket: "/fixture/docker.sock",
        budget: {
            cpus: 2,
            memoryBytes: 2147483648,
            pids: 256,
            wallSeconds: 2467,
            startupSeconds: 180,
            shutdownSeconds: 10,
            maxAllocatedBytes: 1073741824,
            maxLogicalBytes: 2147483648,
            maxLogBytes: 8388608,
            minFreeBytes: 0,
            tmpfsBytes: 67108864,
            pollSeconds: 1,
        },
        jvm: {
            heapMinBytes: 536870912,
            heapMaxBytes: 1073741824,
            maxDirectMemoryBytes: 268435456,
            activeProcessorCount: 2,
        },
        server: {
            gremlinPool: 2,
            bossThreads: 1,
            workerThreads: 2,
            maxWorkQueueSize: 1024,
            httpWorkerThreads: 4,
            maxRoute: 16,
            timeoutSeconds: 60,
            idleTimeoutSeconds: 60,
            connectionMaxRequests: 10000,
            maxContentLengthBytes: 2097152,
            resultIterationBatchSize: 64,
        },
    };
    const runtime = {
        schemaVersion: 1,
        kind: "http-comparison-runtime",
        sourceRoot: resolve("."),
        nodeHome: dirname(dirname(process.execPath)),
        hostPython: "/fixture/python3",
        dockerExecutable: "/fixture/docker",
        driver: {
            cpus: 1,
            memoryBytes: 805306368,
            pids: 128,
            image,
            python: "/usr/local/bin/python3",
            wallSeconds: 2196,
            shutdownSeconds: 5,
            maxLogBytes: 1048576,
            tmpfsBytes: 67108864,
        },
        collector: { durationMs: 2076000, intervalMs: 1000, maxSamples: 2077, maxOutputBytes: 33554432 },
        supervisor: {
            commandMs: 45000,
            pollMs: 1000,
            maxCommandBytes: 2097152,
            minFreeBytes: 0,
            maxWorkBytes: 8589934592,
        },
    };
    const environment = {
        schemaVersion: 1,
        kind: "http-comparison-host-environment",
        hostBootId: "11111111-2222-3333-4444-555555555555",
        kernel: { release: "fixture", architecture: "fixture" },
        cpu: { model: "fixture", logicalProcessors: 4 },
        memoryBytes: 8589934592,
        docker: { serverVersion: "fixture", imageId: image },
        node: { version: process.version },
        coRunningJobs: [],
    };
    const build = {
        schemaVersion: 1,
        sourceCommit: "b".repeat(40),
        distribution: "/fixture/distribution",
        testsRun: false,
        files: [{ path: "lib/fixture.jar", bytes: 1, sha256: "c".repeat(64) }],
    };
    for (const [name, value] of Object.entries({ settings, runtime, environment, build }))
        await diskSave(join(root, name + ".json"), value);
    const project = {
        schemaVersion: 1,
        kind: "http-benchmark-project",
        id: "confirmation-local",
        label: "inert confirmation fixture",
        description: "No runtime observations or performance claim",
        comparison: "aa",
        aaMode: "forward",
        sources: Object.fromEntries(
            ["build", "settings", "environment", "runtime"].map((role) => [role, role + ".json"]),
        ),
    };
    const policy = JSON.parse(await readFile(new URL("../examples/http-capacity-policy.json", import.meta.url)));
    policy.rates = [10];
    await diskSave(join(root, "project.json"), project);
    await diskSave(join(root, "policy.json"), policy);
    const plan = await planHttpCapacity(join(root, "project.json"), join(root, "policy.json"), {
        out: join(root, "exploration"),
    });
    await diskSave(join(root, "execution-set.json"), {
        schemaVersion: 1,
        kind: "http-capacity-execution-set",
        planDigest: plan.planDigest,
        points: [
            { id: plan.points[0].id, executionDirectory: "partial-execution" },
            { id: plan.points[1].id, executionDirectory: "missing-parent/nonexistent-execution" },
        ],
    });
    await mkdir(join(root, "partial-execution"));
    await reportCapacity({
        planDirectory: join(root, "exploration"),
        inputFile: join(root, "execution-set.json"),
        outputDirectory: join(root, "report"),
        timeoutMs: 60000,
        maxBytes: 16 * MiB,
    });
    const request = {
        schemaVersion: 1,
        kind: "http-capacity-confirmation-request",
        id: "confirm-missing-local",
        exploration: { plan: "exploration", executionSet: "execution-set.json", report: "report" },
        project: "project.json",
        sourceReview: { maxMilliseconds: 120000, maxBytes: 16 * MiB },
        budgets: {
            point: {
                ...policy.pointBudgets,
                maxTotalMilliseconds: 8400000,
                maxTotalEvidenceBytes: 8 * 256 * MiB + 64 * MiB,
            },
            controller: policy.controllerBudgets,
            study: policy.studyBudgets,
        },
    };
    const requestFile = join(root, "confirmation-request.json");
    await diskSave(requestFile, request);
    return { root, requestFile, request };
}
