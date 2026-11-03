/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, writeFile, readFile, rm, access, mkdir, symlink } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve, dirname } from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { createHash } from "node:crypto";
import { valueHash } from "../components/paired/core.mjs";

const invoke = promisify(execFile),
    cliPath = resolve("dist/cli.js");
const json = async (path) => JSON.parse(await readFile(path, "utf8"));
const encoded = (value) => Buffer.from(JSON.stringify(value, null, 2) + "\n");
const save = (path, value) => writeFile(path, encoded(value));
const sha = (raw) => createHash("sha256").update(raw).digest("hex");
async function cli(args) {
    return JSON.parse(
        (await invoke(process.execPath, [cliPath, ...args], { maxBuffer: 8 * 1024 * 1024, timeout: 30000 })).stdout,
    );
}
async function failed(args, code) {
    await assert.rejects(cli(args), (error) => {
        const reply = JSON.parse(error.stdout);
        assert.equal(reply.ok, false);
        assert.equal(reply.error.code, code);
        return true;
    });
}
async function missing(path) {
    await assert.rejects(access(path), { code: "ENOENT" });
}

// Inert identities/resources for actual public planning and failed-evidence reconstruction only.
// Nothing starts Docker, a target, an HTTP client or a workload.
async function fixture(t) {
    const root = await mkdtemp(join(tmpdir(), "hgbench-presets-"));
    t.after(() => rm(root, { recursive: true, force: true }));
    const image = "sha256:" + "a".repeat(64),
        boot = "11111111-2222-3333-4444-555555555555";
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
    const environment = {
        schemaVersion: 1,
        kind: "http-comparison-host-environment",
        hostBootId: boot,
        kernel: { release: "fixture", architecture: "fixture" },
        cpu: { model: "fixture", logicalProcessors: 4 },
        memoryBytes: 8589934592,
        docker: { serverVersion: "fixture", imageId: image },
        node: { version: process.version },
        coRunningJobs: [],
    };
    const runtime = {
        schemaVersion: 1,
        kind: "http-comparison-runtime",
        sourceRoot: resolve("."),
        nodeHome: dirname(dirname(process.execPath)),
        hostPython: "/usr/bin/python3",
        dockerExecutable: "/usr/bin/docker",
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
    const build = {
        schemaVersion: 1,
        sourceCommit: "b".repeat(40),
        distribution: "/fixture/distribution",
        testsRun: false,
        files: [{ path: "lib/fixture.jar", bytes: 1, sha256: "c".repeat(64) }],
    };
    for (const [name, value] of Object.entries({ settings, environment, runtime, build }))
        await save(join(root, name + ".json"), value);
    const project = {
        schemaVersion: 1,
        kind: "http-benchmark-project",
        id: "local-fixture",
        label: "inert planning fixture",
        description: "No real target or measurement",
        comparison: "forward-embedded",
        aaMode: "forward",
        sources: {
            build: "build.json",
            settings: "settings.json",
            environment: "environment.json",
            runtime: "runtime.json",
        },
    };
    const projectPath = join(root, "project.json");
    await save(projectPath, project);
    return { root, projectPath, settings, environment, runtime, boot };
}
async function plan(f) {
    const output = join(f.root, "plan");
    const reply = await cli(["http", "preset", "plan", "--project", f.projectPath, "--out", output]);
    return { output, data: reply.data };
}

test(
    "Go project explicitly binds the actual executor across quick planning and offline verification",
    {
        skip: !process.env.HGBENCH_GO_BINARY,
    },
    async (t) => {
        const f = await fixture(t),
            { inspectHttpGoBinary } = await import("../dist/http-go.js"),
            { binary } = await inspectHttpGoBinary(process.env.HGBENCH_GO_BINARY),
            project = await json(f.projectPath);
        project.schemaVersion = 2;
        project.executor = {
            engine: "go-http-v1",
            binary,
            gomaxprocs: 1,
            maxPendingBytes: 1048576,
            cpuQuotaCores: f.runtime.driver.cpus,
            maxRssBytes: f.runtime.driver.memoryBytes,
            hostIdentityGate: true,
        };
        f.runtime.schemaVersion = 2;
        f.runtime.executor = { engine: "go-http-v1", binary, clockProof: "/fixture/clock-proof" };
        await save(f.projectPath, project);
        await save(join(f.root, "runtime.json"), f.runtime);
        const p = await plan(f),
            source = await json(join(p.output, "profile.json"));
        assert.equal(source.schemaVersion, 2);
        assert.deepEqual(source.executor, project.executor);
        assert.equal(Object.hasOwn(source, "clockReference"), false);
        assert.equal((await cli(["http", "preset", "verify", p.output])).data.verified, true);
        assert.equal(p.data.performanceAccepted, false);
        project.executor.cpuQuotaCores = 2;
        await save(f.projectPath, project);
        await failed(
            ["http", "preset", "plan", "--project", f.projectPath, "--out", join(f.root, "bad")],
            "HTTP_PROJECT_EXECUTOR_MISMATCH",
        );
        await missing(join(f.root, "bad"));
    },
);
async function failedExecution(f, planRoot) {
    const root = join(f.root, "failed-execution");
    await mkdir(root);
    const plan = await json(join(planRoot, "comparison/plan.json")),
        lock = await json(join(planRoot, "comparison/plan.lock.json"));
    const registration = {
        schemaVersion: 1,
        kind: "http-comparison-execution",
        executionId: "1".repeat(32),
        planDigest: plan.planDigest,
        planContentDigest: lock.contentDigest,
        hostBootId: f.boot,
        runtimeDigest: "d".repeat(64),
        registeredMonotonicNs: "1000000",
        receipts: [],
    };
    const terminal = {
        schemaVersion: 1,
        kind: "http-comparison-execution-terminal",
        executionId: registration.executionId,
        planDigest: plan.planDigest,
        runtimeDigest: registration.runtimeDigest,
        finishedMonotonicNs: "2000000",
        status: "failed",
        failure: "local protocol fixture with all arms missing",
        runtimeReverified: false,
    };
    const files = [];
    for (const [name, value] of [
        ["execution.json", registration],
        ["terminal.json", terminal],
    ]) {
        const raw = encoded(value);
        await writeFile(join(root, name), raw);
        files.push({ path: name, bytes: raw.length, sha256: sha(raw) });
    }
    await save(join(root, "execution.lock.json"), {
        schemaVersion: 1,
        kind: "http-comparison-execution-lock",
        planDigest: plan.planDigest,
        files,
        contentDigest: valueHash(files),
    });
    return root;
}

test("public catalog and schemas distinguish quick implementation from standard/full design", async () => {
    const listed = (await cli(["http", "preset", "list"])).data;
    assert.equal(listed.defaultPreset, "quick");
    assert.deepEqual(
        listed.presets.map((p) => [p.id, p.planImplemented, p.realExecutionValidated]),
        [
            ["quick", true, false],
            ["standard", false, false],
            ["full", false, false],
        ],
    );
    const quick = (await cli(["http", "preset", "show"])).data;
    assert.deepEqual(quick.workload.caseIds, ["scalar-add", "rows-4096", "string-1024k"]);
    assert.equal(quick.analysis.latency.quantile, "p50");
    assert.equal(quick.scope, "screener/descriptive");
    assert.equal((await cli(["schema", "http-project"])).ok, true);
    const help = await invoke(process.execPath, [cliPath, "http", "preset", "plan", "--help"]);
    assert.match(help.stdout, /budget-ms/);
    assert.match(help.stdout, /dry-run/);
    const packageFile = await json("package.json");
    assert.ok(packageFile.files.includes("presets"));
});

test("public dry-run previews 216s baseline, independent safety bound and honest 150-arrival upper bounds", async (t) => {
    const f = await fixture(t),
        output = join(f.root, "preview");
    const data = (await cli(["http", "preset", "plan", "--project", f.projectPath, "--out", output, "--dry-run"])).data;
    await missing(output);
    assert.equal(data.written, false);
    assert.equal(data.planDigest, null);
    assert.equal(data.estimate.fixedBaselineMilliseconds.total, 216000);
    assert.equal(data.estimate.normalEtaMilliseconds, null);
    assert.equal(data.estimate.confidence, "low");
    assert.equal(data.estimate.counts.arms, 4);
    assert.equal(data.estimate.counts.cases, 3);
    assert.ok(data.estimate.safetyUpperBoundMilliseconds > data.engineeringTargetMilliseconds.high);
    assert.equal(data.resources.driver.cpus, 1);
    assert.equal(data.resources.target.cpus, 2);
    assert.equal(data.statisticsAccepted, false);
    assert.equal(data.executionReady, false);
    assert.equal(data.samplingBounds.length, 6);
    for (const bound of data.samplingBounds) {
        assert.equal(bound.maximumPossibleObservations, 150);
        assert.equal(bound.estimatedObservations, null);
        assert.equal(bound.minimumRequiredObservations, 100);
    }
    assert.equal(data.expanded.analysis.latency.quantile, "p50");
});

test("public frozen plan verifies existing comparison binding and ignores later edits to once-saved original sources", async (t) => {
    const f = await fixture(t),
        p = await plan(f);
    const verified = (await cli(["http", "preset", "verify", p.output])).data;
    assert.equal(verified.verified, true);
    assert.equal(verified.performanceAccepted, false);
    assert.match(p.data.nextActions[1].argv.join(" "), /http comparison prepare/);
    const comparison = await json(join(p.output, "comparison/plan.json"));
    assert.equal(comparison.blocks.length, 2);
    assert.equal(comparison.metricBindings.length, 6);
    assert.ok(
        comparison.metricBindings
            .filter((x) => x.kind === "latency")
            .every((x) => x.valuePath.endsWith(".p50") && x.observations.endsWith(".count")),
    );
    assert.equal((await json(join(p.output, "project/settings.original.json"))).purpose, "calibration");
    assert.equal((await json(join(p.output, "project/settings.json"))).purpose, "functional");
    await save(join(f.root, "runtime.json"), { ...f.runtime, driver: { ...f.runtime.driver, cpus: 3 } });
    assert.equal((await cli(["http", "preset", "verify", p.output])).data.verified, true);
    await failed(
        ["http", "preset", "plan", "--project", f.projectPath, "--out", p.output],
        "HTTP_PRESET_OUTPUT_EXISTS",
    );
});

test("stable project/preset/budget errors do not shrink workload or create an output", async (t) => {
    const f = await fixture(t),
        output = join(f.root, "rejected");
    const base = ["http", "preset", "plan", "--project", f.projectPath, "--out", output];
    await failed([...base, "--budget-ms", "1"], "HTTP_PRESET_BUDGET_INSUFFICIENT");
    await missing(output);
    await failed([...base, "--budget-ms", "0"], "HTTP_PRESET_OPTIONS_INVALID");
    await missing(output);
    for (const id of ["standard", "full"]) await failed([...base, "--preset", id], "HTTP_PRESET_NOT_IMPLEMENTED");
    await failed([...base, "--preset", "fast"], "HTTP_PRESET_UNKNOWN");
    await rm(join(f.root, "build.json"));
    await failed(base, "HTTP_PRESET_PROJECT_FILE");
    await missing(output);
});

test("resealing outer metadata cannot relabel different workload parameters as the versioned quick preset", async (t) => {
    const f = await fixture(t),
        p = await plan(f),
        path = join(p.output, "preset-binding.json");
    const binding = await json(path);
    binding.definition.workload.durationMs++;
    await save(path, binding);
    const lockPath = join(p.output, "preset.lock.json"),
        lock = await json(lockPath),
        raw = await readFile(path);
    const row = lock.files.find((entry) => entry.path === "preset-binding.json");
    row.bytes = raw.length;
    row.sha256 = sha(raw);
    await save(lockPath, lock);
    await failed(["http", "preset", "verify", p.output], "HTTP_PRESET_BINDING_INVALID");
});

test("resealed plan headers cannot promote scope, forecasts or acceptance", async (t) => {
    const f = await fixture(t),
        p = await plan(f),
        path = join(p.output, "preset-plan.json");
    const original = await json(path),
        lockPath = join(p.output, "preset.lock.json");
    for (const mutate of [
        (value) => {
            value.preset.id = "full";
        },
        (value) => {
            value.performanceAccepted = true;
        },
        (value) => {
            value.estimate.normalEtaMilliseconds = 300000;
        },
        (value) => {
            value.samplingBounds[0].maximumPossibleObservations = 1000;
        },
    ]) {
        const value = structuredClone(original);
        mutate(value);
        await save(path, value);
        const lock = await json(lockPath),
            raw = await readFile(path),
            row = lock.files.find((entry) => entry.path === "preset-plan.json");
        row.bytes = raw.length;
        row.sha256 = sha(raw);
        await save(lockPath, lock);
        await failed(["http", "preset", "verify", p.output], "HTTP_PRESET_BINDING_INVALID");
    }
});

test("resealing outer lock cannot change the runtime resources passed to prepare", async (t) => {
    const f = await fixture(t),
        p = await plan(f),
        path = join(p.output, "project/runtime.json");
    const runtime = await json(path);
    runtime.driver.cpus = 3;
    await save(path, runtime);
    const lockPath = join(p.output, "preset.lock.json"),
        lock = await json(lockPath),
        raw = await readFile(path);
    const row = lock.files.find((entry) => entry.path === "project/runtime.json");
    row.bytes = raw.length;
    row.sha256 = sha(raw);
    await save(lockPath, lock);
    await failed(["http", "preset", "verify", p.output], "HTTP_PRESET_BINDING_INVALID");
});

test("public verification bounds external directory depth, bytes, members and file types", async (t) => {
    const f = await fixture(t),
        p = await plan(f),
        extra = join(p.output, "extra");
    await writeFile(extra, "extra");
    await failed(["http", "preset", "verify", p.output], "HTTP_PRESET_PLAN_CHANGED");
    await rm(extra);
    await symlink(join(f.root, "project.json"), extra);
    await failed(["http", "preset", "verify", p.output], "HTTP_PRESET_LOCK");
    await rm(extra);
    await writeFile(extra, Buffer.alloc(8 * 1024 * 1024 + 1));
    await failed(["http", "preset", "verify", p.output], "HTTP_PRESET_PLAN_BUDGET");
    await rm(extra);
    await mkdir(join(extra, ...Array.from({ length: 13 }, (_, i) => "d" + i)), { recursive: true });
    await failed(["http", "preset", "verify", p.output], "HTTP_PRESET_PLAN_BUDGET");
    await rm(extra, { recursive: true });
    assert.equal((await cli(["http", "preset", "verify", p.output])).data.verified, true);
});

test("public report reuses failed raw evidence, keeps all four arms/six metrics and puts screener scope upfront", async (t) => {
    const f = await fixture(t),
        p = await plan(f),
        execution = await failedExecution(f, p.output),
        output = join(f.root, "report");
    const reply = await cli(["http", "preset", "report", execution, "--plan", p.output, "--out", output]);
    assert.equal(reply.data.valid, false);
    assert.equal(reply.data.scope, "screener/descriptive");
    const header = await json(join(output, "preset-report.json"));
    assert.equal(header.statisticsAccepted, false);
    assert.equal(header.reliableP99Claimed, false);
    assert.equal(header.smallDifferenceClaimed, false);
    const v = await json(join(output, "comparison/verification.json")),
        a = await json(join(output, "comparison/analysis.json"));
    assert.equal(v.rows.length, 4);
    assert.ok(v.rows.every((row) => row.issues.includes("ARM_MISSING")));
    assert.equal(a.cells.length, 6);
    assert.ok(a.cells.every((cell) => cell.issues.length > 0));
    const html = await readFile(join(output, "report.html"), "utf8");
    assert.match(html, /screener/);
    assert.match(html, /P99/);
    const invalidOutput = join(f.root, "invalid-report");
    await writeFile(join(execution, "execution.json"), "{}\n");
    await failed(
        ["http", "preset", "report", execution, "--plan", p.output, "--out", invalidOutput],
        "HTTP_COMPARISON_FAILED",
    );
    assert.equal((await json(join(invalidOutput, "failure.json"))).partialPreserved, true);
});

test("new total preset freezes one deadline below stage sum, while explicit stage-sum retains its minimum", async (t) => {
    const f = await fixture(t);
    const total = join(f.root, "total");
    const preview = (
        await cli([
            "http",
            "preset",
            "plan",
            "--project",
            f.projectPath,
            "--out",
            total,
            "--budget-ms",
            "300000",
            "--dry-run",
        ])
    ).data;
    assert.equal(preview.budgetMode, "total");
    assert.equal(preview.selectedBudgetMs, 300000);
    assert.equal(preview.estimate.safetyUpperBoundMilliseconds, 300000);
    assert.ok(preview.estimate.stageSumMaximumMilliseconds > 300000);
    assert.equal(preview.estimate.fixedBaselineMilliseconds.total, 216000);
    assert.equal(preview.expanded.lifecycleBudgets.maxTotalMilliseconds, 300000);
    assert.equal(preview.expanded.lifecycleBudgets.budgetMode, "total");
    await missing(total);
    const built = (
        await cli(["http", "preset", "plan", "--project", f.projectPath, "--out", total, "--budget-ms", "300000"])
    ).data;
    assert.equal(built.budgetMode, "total");
    const request = await json(join(total, "comparison/request.json"));
    assert.equal(request.schemaVersion, 3);
    assert.deepEqual(request.metricRoles, { latency: "inference", goodput: "inference" });
    assert.equal(request.budgets.budgetMode, "total");
    assert.equal((await json(join(total, "preset-plan.json"))).schemaVersion, 2);
    assert.equal((await cli(["http", "preset", "verify", total])).data.verified, true);

    const frozenHeader = join(total, "preset-plan.json");
    const changed = await json(frozenHeader);
    changed.budgetSemantics = "Timeouts are accepted as complete measurements";
    await save(frozenHeader, changed);
    const outerLock = await json(join(total, "preset.lock.json"));
    const row = outerLock.files.find((entry) => entry.path === "preset-plan.json");
    const bytes = await readFile(frozenHeader);
    row.bytes = bytes.length;
    row.sha256 = sha(bytes);
    await save(join(total, "preset.lock.json"), outerLock);
    await failed(["http", "preset", "verify", total], "HTTP_PRESET_BINDING_INVALID");
    const rejected = join(f.root, "stage-too-small");
    await failed(
        [
            "http",
            "preset",
            "plan",
            "--project",
            f.projectPath,
            "--out",
            rejected,
            "--budget-ms",
            "300000",
            "--budget-mode",
            "stage-sum",
        ],
        "HTTP_PRESET_BUDGET_INSUFFICIENT",
    );
    await missing(rejected);
    const enough = (
        await cli([
            "http",
            "preset",
            "plan",
            "--project",
            f.projectPath,
            "--out",
            rejected,
            "--budget-mode",
            "stage-sum",
            "--dry-run",
        ])
    ).data;
    assert.equal(enough.budgetMode, "stage-sum");
    assert.ok(enough.estimate.safetyUpperBoundMilliseconds > 300000);
    await missing(rejected);
});
