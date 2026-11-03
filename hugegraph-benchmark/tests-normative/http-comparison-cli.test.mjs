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
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { mkdtemp, mkdir, writeFile, readFile, rm, realpath, copyFile, access } from "node:fs/promises";
import { constants } from "node:fs";
import { join, resolve } from "node:path";
import { tmpdir } from "node:os";
import { createHash } from "node:crypto";
import { verifyTask } from "../dist/tasks.js";
import { valueHash } from "../components/paired/core.mjs";

const execute = promisify(execFile);
const root = resolve(".");
const cliPath = join(root, "dist/cli.js");
const sha = (raw) => createHash("sha256").update(raw).digest("hex");
const read = async (path) => JSON.parse(await readFile(path, "utf8"));
const put = async (path, value) => writeFile(path, JSON.stringify(value, null, 2) + "\n");
async function cli(args, options = {}) {
    return execute(process.execPath, [cliPath, ...args], { maxBuffer: 16 * 1024 * 1024, ...options });
}
async function jsonCli(args, options) {
    return JSON.parse((await cli(args, options)).stdout);
}
async function missing(path) {
    await assert.rejects(access(path), { code: "ENOENT" });
}

// Real public CLI generation, planning and Python freezing. JDK/distribution/Docker are inert
// identity fixtures which fail if executed; this test makes no target/runtime-performance claim.
async function fixture(t, maxTotalMilliseconds = 3000000) {
    const directory = await realpath(await mkdtemp(join(tmpdir(), "hgbench-comparison-cli-")));
    t.after(() => rm(directory, { recursive: true, force: true }));
    const nodeHome = join(directory, "node"),
        jdk = join(directory, "jdk"),
        distribution = join(directory, "distribution");
    await mkdir(join(nodeHome, "bin"), { recursive: true });
    const nodeBinary = await realpath(process.execPath);
    // Independent inode: a hard link changes the real executable's ctime on create/remove,
    // which correctly invalidates an already-running clock session in parallel tests.
    await copyFile(nodeBinary, join(nodeHome, "bin/node"), constants.COPYFILE_FICLONE);
    for (const [base, relative, content] of [
        [jdk, "bin/java", "#!/bin/sh\nexit 99\n"],
        [jdk, "bin/jcmd", "#!/bin/sh\nexit 99\n"],
        [jdk, "release", 'JAVA_VERSION="17.0.0"\n'],
        [distribution, "lib/hugegraph-fixture.jar", "INERT OFFLINE FIXTURE\n"],
        [directory, "bin/docker", "#!/bin/sh\nexit 99\n"],
    ]) {
        const path = join(base, relative);
        await mkdir(join(path, ".."), { recursive: true });
        await writeFile(path, content, { mode: 0o700 });
    }
    const python = (
        await execute(process.env.HGBENCH_PYTHON ?? "python3", [
            "-c",
            "import os,sys; print(os.path.realpath(sys.executable))",
        ])
    ).stdout.trim();
    const image = "sha256:" + "1".repeat(64);
    const shared = {
        schemaVersion: 1,
        kind: "hugegraph-http-environment-settings",
        purpose: "functional",
        distribution,
        javaHome: jdk,
        image,
        python: "/usr/bin/python3",
        dockerSocket: "/var/run/docker.sock",
        budget: {
            cpus: 1,
            memoryBytes: 1073741824,
            pids: 128,
            wallSeconds: 3600,
            startupSeconds: 60,
            shutdownSeconds: 5,
            maxAllocatedBytes: 67108864,
            maxLogicalBytes: 134217728,
            maxLogBytes: 1048576,
            minFreeBytes: 0,
            tmpfsBytes: 33554432,
            pollSeconds: 1,
        },
        jvm: {
            heapMinBytes: 16777216,
            heapMaxBytes: 268435456,
            maxDirectMemoryBytes: 67108864,
            activeProcessorCount: 1,
        },
        server: {
            gremlinPool: 2,
            bossThreads: 1,
            workerThreads: 2,
            maxWorkQueueSize: 64,
            httpWorkerThreads: 4,
            maxRoute: 4,
            timeoutSeconds: 30,
            idleTimeoutSeconds: 60,
            connectionMaxRequests: 10000,
            maxContentLengthBytes: 2097152,
            resultIterationBatchSize: 64,
        },
    };
    const bytes = await readFile(join(distribution, "lib/hugegraph-fixture.jar"));
    const sharedValues = {
        settings: shared,
        build: {
            sourceCommit: "a".repeat(40),
            distribution,
            testsRun: false,
            files: [{ path: "lib/hugegraph-fixture.jar", bytes: bytes.length, sha256: sha(bytes) }],
        },
        environment: {
            schemaVersion: 1,
            kind: "http-comparison-host-environment",
            hostBootId: "00000000-0000-0000-0000-000000000001",
            kernel: { release: "fixture", architecture: "fixture" },
            cpu: { model: "fixture", logicalProcessors: 1 },
            memoryBytes: 1073741824,
            docker: { serverVersion: "fixture", imageId: image },
            node: { version: process.version },
            coRunningJobs: ["offline-fixture-only"],
        },
    };
    const sharedEvidence = {};
    for (const [role, value] of Object.entries(sharedValues)) {
        const path = join(directory, `${role}.json`);
        await put(path, value);
        sharedEvidence[role] = { path, sha256: sha(await readFile(path)) };
    }
    const suite = await read(join(root, "examples/http-suite-functional.json"));
    Object.assign(suite, {
        caseIds: ["scalar-add"],
        durationMs: 100,
        requestTimeoutMs: 1000,
        drainMs: 1000,
        responseEncoding: "gzip",
    });
    suite.target = {
        label: "offline-fixture",
        build: "a".repeat(40),
        mode: "forward",
        environment: "Offline fixture; no Docker/JDK executed",
        evidenceFiles: [],
    };
    await put(join(directory, "suite.json"), suite);
    await jsonCli(["http", "generate", join(directory, "suite.json"), "--out", join(directory, "profile.json")]);
    await jsonCli(["http", "plan", join(directory, "profile.json"), "--out", join(directory, "http-plan")]);
    const request = {
        schemaVersion: 1,
        kind: "http-comparison-request",
        id: "cli-functional-fixture",
        purpose: "functional",
        comparison: "forward-embedded",
        modes: { A: "forward", B: "embedded" },
        blockIds: ["b01", "b02"],
        orderSeed: "cli-order",
        sharedEvidence,
        analysis: {
            familyAlpha: 0.05,
            minimumPairs: 2,
            bootstrapReplicates: 4000,
            minimumTailDraws: 20,
            maxWork: 10000000,
            maxMilliseconds: 10000,
            resampleSeed: "cli-resample",
            latency: { relativeBudget: 0.05, absoluteBudget: 1, minimumObservations: 1000 },
            goodput: { relativeBudget: 0.05, absoluteBudget: 1, minimumObservations: 1000 },
        },
        budgets: {
            prepareMs: 300000,
            startMs: 60000,
            modeProbeMs: 30000,
            settleMs: 0,
            stopMs: 30000,
            sealAndAnalyzeMs: 60000,
            maxEvidenceBytesPerArm: 128 * 1024 * 1024,
            maxTotalEvidenceBytes: 1024 * 1024 * 1024,
            maxTotalMilliseconds,
        },
    };
    await put(join(directory, "request.json"), request);
    const comparison = join(directory, "comparison");
    await jsonCli([
        "http",
        "comparison",
        "plan",
        join(directory, "request.json"),
        "--http-plan",
        join(directory, "http-plan"),
        "--out",
        comparison,
    ]);
    const runtime = {
        schemaVersion: 1,
        kind: "http-comparison-runtime",
        sourceRoot: root,
        nodeHome,
        hostPython: python,
        dockerExecutable: join(directory, "bin/docker"),
        driver: {
            image,
            python: "/usr/bin/python3",
            cpus: 1,
            memoryBytes: 536870912,
            pids: 128,
            tmpfsBytes: 67108864,
            wallSeconds: 600,
            shutdownSeconds: 5,
            maxLogBytes: 1048576,
        },
        collector: { intervalMs: 1000, durationMs: 300000, maxSamples: 301, maxOutputBytes: 1048576 },
        supervisor: {
            pollMs: 1000,
            commandMs: 45000,
            maxCommandBytes: 1048576,
            minFreeBytes: 0,
            maxWorkBytes: 1024 * 1024 * 1024,
        },
    };
    const runtimeFile = join(directory, "runtime-settings.json");
    await put(runtimeFile, runtime);
    return { directory, request, comparison, runtime, runtimeFile, python };
}

test("public prepare performs an actual offline freeze and produces a verifiable bounded database task", async (t) => {
    const f = await fixture(t),
        attempt = join(f.directory, "attempt"),
        taskRoot = join(f.directory, "tasks");
    const reply = await jsonCli([
        "--root",
        taskRoot,
        "http",
        "comparison",
        "prepare",
        f.runtimeFile,
        "--plan",
        f.comparison,
        "--out",
        attempt,
    ]);
    assert.equal(reply.command, "http.comparison.prepare");
    assert.equal(reply.data.executed, false);
    assert.equal(reply.data.replayAllowed, false);
    assert.equal(reply.data.performanceAccepted, false);
    assert.equal(reply.data.attempt, attempt);
    const task = await read(reply.data.taskPlan);
    await verifyTask(task);
    assert.equal(task.digest, reply.data.taskPlanDigest);
    assert.equal(task.plan.cwd, attempt);
    assert.equal(task.plan.effect, "database");
    assert.deepEqual(task.plan.command, [
        f.python,
        join(attempt, "comparison-workflow.py"),
        "run",
        "--attempt",
        attempt,
    ]);
    assert.equal(
        task.plan.timeoutMs,
        f.request.budgets.maxTotalMilliseconds + f.request.budgets.stopMs + f.request.budgets.sealAndAnalyzeMs + 10000,
    );
    assert.equal(task.plan.terminateGraceMs, 30000);
    assert.equal(task.plan.maxLogBytes, 1048576);
    for (const file of [
        "comparison-workflow.py",
        "runtime.json",
        "runtime.lock.json",
        "comparison/plan.json",
        "comparison/plan.lock.json",
    ])
        assert(task.files.some((item) => item.path === join(attempt, file)));
    assert.equal(sha(await readFile(join(attempt, "runtime.lock.json"))), reply.data.runtimeDigest);
    assert.deepEqual(reply.nextActions[0].argv, [
        "hgbench",
        "--root",
        taskRoot,
        "task",
        "run",
        "--plan",
        reply.data.taskPlan,
        "--detach",
    ]);
    await missing(join(attempt, "dispatch.json"));
    await missing(taskRoot);
    const status = await jsonCli(["http", "comparison", "status", attempt]);
    assert.equal(status.data.state, "planned");
    const stopped = await jsonCli(["http", "comparison", "stop", attempt]);
    assert.equal(stopped.data.stopRequested, true);
    assert.deepEqual(stopped.data.errors, []);
    await missing(join(attempt, "dispatch.json"));
    await assert.rejects(
        cli(["http", "comparison", "prepare", f.runtimeFile, "--plan", f.comparison, "--out", attempt]),
        (error) => {
            const body = JSON.parse(error.stdout);
            return (
                body.error.code === "HTTP_COMPARISON_FAILED" && body.error.details.error.code === "new-attempt-required"
            );
        },
    );
    await t.test(
        "sealed missing arms remain visible and return valid=false without performance acceptance",
        async () => {
            const execution = join(f.directory, "missing-arms-execution");
            await mkdir(execution);
            const plan = await read(join(attempt, "comparison/plan.json"));
            const lock = await read(join(attempt, "comparison/plan.lock.json"));
            const executionId = "e".repeat(32);
            await put(join(execution, "execution.json"), {
                schemaVersion: 1,
                kind: "http-comparison-execution",
                executionId,
                planDigest: plan.planDigest,
                planContentDigest: lock.contentDigest,
                hostBootId: "00000000-0000-0000-0000-000000000001",
                registeredMonotonicNs: "1",
                runtimeDigest: reply.data.runtimeDigest,
                receipts: plan.blocks.flatMap((block) =>
                    block.arms.map((arm) => ({
                        runId: arm.runId,
                        path: `arms/${arm.runId}/receipt.json`,
                    })),
                ),
            });
            await put(join(execution, "terminal.json"), {
                schemaVersion: 1,
                kind: "http-comparison-execution-terminal",
                executionId,
                planDigest: plan.planDigest,
                runtimeDigest: reply.data.runtimeDigest,
                status: "failed",
                failure: "fixture-no-dispatch",
                runtimeReverified: false,
                finishedMonotonicNs: "2",
                performanceAccepted: false,
            });
            const files = [];
            for (const path of ["execution.json", "terminal.json"]) {
                const raw = await readFile(join(execution, path));
                files.push({ path, bytes: raw.length, sha256: sha(raw) });
            }
            await put(join(execution, "execution.lock.json"), {
                schemaVersion: 1,
                kind: "http-comparison-execution-lock",
                planDigest: plan.planDigest,
                files,
                contentDigest: valueHash(files),
            });
            const output = join(f.directory, "missing-arms-report");
            const report = await jsonCli([
                "http",
                "comparison",
                "report",
                execution,
                "--plan",
                join(attempt, "comparison"),
                "--out",
                output,
            ]);
            assert.equal(report.data.valid, false);
            assert.equal(report.data.statisticsEligible, false);
            assert.equal(report.data.armCount, 4);
            assert.equal(report.data.performanceAccepted, false);
            const verification = await read(join(output, "verification.json"));
            assert.equal(verification.rows.length, 4);
            assert(verification.rows.every((row) => row.status === "invalid"));
            await missing(join(attempt, "dispatch.json"));
        },
    );
    const source = await readFile(join(attempt, "runtime.json"));
    await writeFile(join(attempt, "runtime.json"), Buffer.concat([source, Buffer.from(" ")]));
    await assert.rejects(verifyTask(task), (error) => error.code === "COMPONENT_CHANGED");
});

test("prepare rejects a legal comparison whose cleanup would exceed task duration before asset freezing", async (t) => {
    const f = await fixture(t, 7 * 24 * 3600000),
        attempt = join(f.directory, "unsupported-task-budget");
    await assert.rejects(
        cli(["http", "comparison", "prepare", f.runtimeFile, "--plan", f.comparison, "--out", attempt], {
            env: { ...process.env, HGBENCH_PYTHON: join(f.directory, "must-not-be-invoked") },
        }),
        (error) => {
            assert.equal(error.code, 3);
            assert.equal(JSON.parse(error.stdout).error.code, "HTTP_COMPARISON_TASK_BUDGET");
            return true;
        },
    );
    await missing(attempt);
});

test("public comparison help describes offline preparation, same-attempt recovery and evidence validity", async () => {
    const help = (await cli(["http", "comparison", "--help"])).stdout;
    for (const word of ["prepare", "status", "stop", "report", "same task ID", "never replay"])
        assert(help.includes(word));
    const report = (await cli(["http", "comparison", "report", "--help"])).stdout;
    assert.match(report, /valid=false/);
    assert.match(report, /statisticsEligible/);
    const planner = await jsonCli(["http", "comparison", "describe"]);
    assert.equal(planner.data.kind, "http-comparison-planner-capability");
    const execution = await jsonCli(["http", "comparison", "describe", "--execution"]);
    assert.equal(execution.data.kind, "http-comparison-workflow-capability");
    assert(execution.data.runtimeRequiredFields.includes("driver"));
    assert(execution.data.runtimeRequiredFields.includes("collector"));
    assert.equal(execution.data.performanceAccepted, false);
});

test("public comparison preserves component JSON failures from stdout and rejects missing report evidence", async (t) => {
    const f = await fixture(t);
    f.runtime.driver.image = "latest";
    await put(f.runtimeFile, f.runtime);
    const attempt = join(f.directory, "bad-runtime");
    await assert.rejects(
        cli(["http", "comparison", "prepare", f.runtimeFile, "--plan", f.comparison, "--out", attempt]),
        (error) => {
            assert.equal(error.code, 6);
            const body = JSON.parse(error.stdout);
            assert.equal(body.error.code, "HTTP_COMPARISON_FAILED");
            assert.equal(body.error.retryable, false);
            assert.equal(body.error.details.error.code, "driver-image");
            return true;
        },
    );
    await missing(attempt);
    await assert.rejects(
        cli([
            "http",
            "comparison",
            "report",
            join(f.directory, "missing-execution"),
            "--plan",
            f.comparison,
            "--out",
            join(f.directory, "report"),
        ]),
        (error) => {
            const body = JSON.parse(error.stdout);
            assert.equal(body.error.code, "HTTP_COMPARISON_FAILED");
            assert.equal(body.error.details.ok, false);
            assert.equal(body.error.details.error.retryable, false);
            return true;
        },
    );
});
