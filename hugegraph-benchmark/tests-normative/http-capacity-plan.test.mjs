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
import { mkdtemp, writeFile, readFile, rm, access, readdir, lstat, rename, symlink } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve, dirname } from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { createHash } from "node:crypto";
import { hash } from "../dist/io.js";
import { httpCapacityPolicySchema } from "../dist/http-capacity-contracts.js";

const invoke = promisify(execFile),
    cli = resolve("dist/cli.js");
const json = async (path) => JSON.parse(await readFile(path, "utf8"));
const encoded = (value) => Buffer.from(JSON.stringify(value, null, 2) + "\n");
const save = (path, value) => writeFile(path, encoded(value));
const sha = (raw) => createHash("sha256").update(raw).digest("hex");
async function command(args) {
    return JSON.parse(
        (await invoke(process.execPath, [cli, ...args], { timeout: 180000, maxBuffer: 16 * 1024 ** 2 })).stdout,
    );
}
async function rejected(args, expected) {
    await assert.rejects(command(args), (error) => {
        const result = JSON.parse(error.stdout);
        assert.equal(result.ok, false);
        assert.equal(result.error.code, expected);
        return true;
    });
}
async function absent(path) {
    await assert.rejects(access(path), { code: "ENOENT" });
}

// Inert public-planning inputs. No target, Docker, remote connection, HTTP request or workload is run.
async function fixture(t) {
    const root = await mkdtemp(join(tmpdir(), "hgbench-capacity-"));
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
        id: "capacity-local",
        label: "inert capacity fixture",
        description: "Planning only; no performance claim",
        comparison: "aa",
        aaMode: "forward",
        sources: {
            build: "build.json",
            settings: "settings.json",
            environment: "environment.json",
            runtime: "runtime.json",
        },
    };
    const projectFile = join(root, "project.json"),
        policyFile = join(root, "policy.json");
    const policy = await json(resolve("examples/http-capacity-policy.json"));
    await save(projectFile, project);
    await save(policyFile, policy);
    return { root, projectFile, policyFile, project, policy, runtime };
}
const planArgs = (f, out, dryRun = false) => [
    "http",
    "capacity",
    "plan",
    "--project",
    f.projectFile,
    "--policy",
    f.policyFile,
    "--out",
    out,
    ...(dryRun ? ["--dry-run"] : []),
];

test(
    "Go capacity keeps all eleven questions/four rates and freezes one explicit executor at every point",
    {
        skip: !process.env.HGBENCH_GO_BINARY,
    },
    async (t) => {
        const f = await fixture(t),
            { inspectHttpGoBinary } = await import("../dist/http-go.js"),
            { binary } = await inspectHttpGoBinary(process.env.HGBENCH_GO_BINARY);
        f.project.schemaVersion = 2;
        f.project.executor = {
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
        f.policy = await json(resolve("examples/http-capacity-policy-v2.json"));
        await save(f.projectFile, f.project);
        await save(f.policyFile, f.policy);
        await save(join(f.root, "runtime.json"), f.runtime);
        const output = join(f.root, "go-plan"),
            data = (await command(planArgs(f, output))).data;
        assert.equal(data.points.length, 44);
        for (const point of data.points) {
            const profile = await json(join(output, point.profileFile));
            assert.equal(profile.schemaVersion, 2);
            assert.deepEqual(profile.executor, f.project.executor);
            assert.equal(Object.hasOwn(profile, "clockReference"), false);
            assert.equal(point.expectedArms, 1);
        }
        assert.equal((await command(["http", "capacity", "verify", output])).data.valid, true);
        assert.equal(data.performanceAccepted, false);
    },
);
async function relock(root) {
    const files = [];
    async function walk(prefix = "") {
        for (const name of await readdir(join(root, prefix))) {
            const path = prefix ? prefix + "/" + name : name;
            if (path === "capacity.lock.json") continue;
            if ((await lstat(join(root, path))).isDirectory()) await walk(path);
            else {
                const raw = await readFile(join(root, path));
                files.push({ path, bytes: raw.length, sha256: sha(raw) });
            }
        }
    }
    await walk();
    files.sort((a, b) => (a.path < b.path ? -1 : a.path > b.path ? 1 : 0));
    await save(join(root, "capacity.lock.json"), {
        schemaVersion: 1,
        kind: "http-capacity-plan-lock",
        contentDigest: hash(files),
        files,
    });
}
async function mutation(root, path, change, expected) {
    const file = join(root, path),
        original = await readFile(file);
    try {
        const value = JSON.parse(original);
        change(value);
        await save(file, value);
        await relock(root);
        await rejected(["http", "capacity", "verify", root], expected);
    } finally {
        await writeFile(file, original);
        await relock(root);
    }
}

test("capacity policy is strict, public discovery is honest, and unsafe inputs do not create an output", async (t) => {
    const f = await fixture(t),
        out = join(f.root, "never-created");
    const capability = (await command(["http", "capacity", "describe"])).data;
    assert.equal(capability.planImplemented, true);
    assert.equal(capability.executionReady, false);
    assert.equal((await command(["schema", "http-capacity-policy"])).ok, true);
    for (const edit of [(p) => (p.rates = [20, 10]), (p) => (p.rates = [10, 10])]) {
        const policy = structuredClone(f.policy);
        edit(policy);
        await save(f.policyFile, policy);
        await rejected(planArgs(f, out), "HTTP_CAPACITY_RATES_INVALID");
        await absent(out);
    }
    for (const edit of [
        (p) => (p.caseIds = ["scalar-add"]),
        (p) => (p.confirmation.pairs = 2),
        (p) => (p.tailPolicy.p99MinimumObservations = 100),
        (p) => (p.analysis.latency.quantile = "p99"),
        (p) => (p.analysis.goodput.minimumObservations = 99),
        (p) => (p.http.extra = true),
    ]) {
        const policy = structuredClone(f.policy);
        edit(policy);
        assert.equal(httpCapacityPolicySchema.safeParse(policy).success, false);
    }
    const requestCap = structuredClone(f.policy);
    requestCap.http.maxRequestsPerPhase = 1;
    await save(f.policyFile, requestCap);
    await rejected(planArgs(f, out), "HTTP_CAPACITY_REQUEST_BUDGET");
    await absent(out);
    const controller = structuredClone(f.policy);
    controller.controllerBudgets.perPointMilliseconds = 1000;
    await save(f.policyFile, controller);
    await rejected(planArgs(f, out), "HTTP_CAPACITY_CONTROLLER_BUDGET");
    await absent(out);
});

test("all-eleven public A/A plan freezes 44 real child plans and rejects outer re-sealing", async (t) => {
    const f = await fixture(t),
        out = join(f.root, "plan"),
        reply = (await command(planArgs(f, out))).data;
    assert.equal(reply.pointCount, 44);
    assert.equal(reply.expectedArms, 176);
    assert.equal(reply.estimate.fixedBaselineMilliseconds.total, 7040000);
    assert.equal(reply.estimate.fixedBaselineMilliseconds.settleTotal, 176000);
    assert.equal(reply.estimate.normalEtaMilliseconds, null);
    assert.equal(reply.estimate.safetyUpperBoundMilliseconds, 219420000);
    assert.equal(reply.confirmation.implemented, false);
    assert.equal(reply.executionReady, false);
    assert.equal(new Set(reply.points.map((p) => p.caseId)).size, 11);
    assert.deepEqual(reply.modes, ["forward"]);
    assert.deepEqual(
        reply.points.slice(0, 4).map((p) => p.ratePerSecond),
        [10, 20, 40, 80],
    );
    assert.ok(reply.points.every((p) => p.workflowMaxMilliseconds === 133000));
    const valid = (await command(["http", "capacity", "verify", out])).data;
    assert.equal(valid.valid, true);
    assert.equal(valid.planDigest, reply.planDigest);
    assert.equal(valid.contentDigest, reply.contentDigest);
    assert.equal(valid.plan.engineeringGates.driverHeadroom, "pending-independent-calibration");
    const first = reply.points[0],
        child = await json(join(out, first.comparisonDirectory, "plan.json"));
    assert.equal(child.budgets.requiredTotalMilliseconds, 3836000);
    assert.equal(child.analysisPolicy.metrics.length, 2);
    assert.equal(child.metricBindings[0].valuePath, "correctEndToEndMs.p50");
    assert.deepEqual(child.blocks.map((b) => b.order).sort(), ["AB", "BA"]);
    assert.equal(child.samplingBounds[0].maximumPossibleObservations, 300);
    assert.equal((await json(join(out, first.comparisonDirectory, "profiles/A.json"))).serviceChecks, true);
    await t.test("original runtime CPU bytes cannot be replaced by an outer lock", () =>
        mutation(out, "project/runtime.json", (value) => (value.driver.cpus = 2), "HTTP_CAPACITY_SOURCE_BINDING"),
    );
    await t.test("normalized target resources remain tied to the original settings", () =>
        mutation(out, "project/settings.json", (value) => (value.budget.cpus = 3), "HTTP_CAPACITY_SOURCE_BINDING"),
    );
    await t.test("changing the point rate is rejected after rebuilding the plan digest and outer lock", () =>
        mutation(
            out,
            "capacity-plan.json",
            (value) => {
                value.points[0].ratePerSecond = 11;
                const { planDigest, ...payload } = value;
                value.planDigest = hash(payload);
            },
            "HTTP_CAPACITY_POINT_INVALID",
        ),
    );
    await t.test("missing points cannot reduce the predeclared denominator", () =>
        mutation(out, "capacity-plan.json", (value) => value.points.pop(), "HTTP_CAPACITY_POINT_INVALID"),
    );
    await t.test("new policy bytes require new child source bindings", () =>
        mutation(out, "policy.json", (value) => (value.rates[0] = 9), "HTTP_CAPACITY_SOURCE_BINDING"),
    );
    await t.test("profile settings cannot change correctness, timing or load behind a new outer lock", () =>
        mutation(
            out,
            "points/" + first.id + "/profile.settings.json",
            (value) => (value.durationMs = 100),
            "HTTP_CAPACITY_POINT_INVALID",
        ),
    );
    await t.test("child lock failure is consumed rather than trusted from the outer manifest", () =>
        mutation(
            out,
            first.comparisonDirectory + "/request.json",
            (value) => (value.modes.B = "embedded"),
            "HTTP_COMPARISON_FAILED",
        ),
    );
    await t.test("nonregular extra members are rejected without following symlinks", async () => {
        const link = join(out, "unlocked-symlink");
        await symlink(f.policyFile, link);
        try {
            await rejected(["http", "capacity", "verify", out], "HTTP_CAPACITY_PLAN_CHANGED");
        } finally {
            await rm(link);
        }
    });
    await t.test("complete snapshots can be moved without opening the original project references", async () => {
        const moved = join(f.root, "moved-plan");
        await rename(out, moved);
        for (const role of ["build", "settings", "environment", "runtime"]) await rm(join(f.root, role + ".json"));
        const result = (await command(["http", "capacity", "verify", moved])).data;
        assert.equal(result.planDigest, reply.planDigest);
        assert.equal(result.contentDigest, reply.contentDigest);
    });
});

test("two-mode dry-run covers all points with optional SLO, unknown ETA and no dispatch or output", async (t) => {
    const f = await fixture(t),
        out = join(f.root, "preview");
    f.project.comparison = "forward-embedded";
    await save(f.projectFile, f.project);
    // A single predeclared rate keeps this public-planner fixture bounded while covering all case/mode pairs.
    f.policy.rates = [10];
    f.policy.slo = { endToEndQuantile: "p99", maxLatencyMs: 100, maxErrorRate: 0, maxTimeoutRate: 0 };
    await save(f.policyFile, f.policy);
    const result = (await command(planArgs(f, out, true))).data;
    assert.equal(result.pointCount, 22);
    assert.equal(result.expectedArms, 88);
    assert.deepEqual(result.modes, ["forward", "embedded"]);
    assert.equal(result.estimate.fixedBaselineMilliseconds.total, 3520000);
    assert.equal(result.estimate.normalEtaMilliseconds, null);
    assert.equal(result.policy.tailPolicy.p99MinimumObservations, 1000);
    assert.equal(result.written, false);
    assert.equal(result.executed, false);
    assert.deepEqual(result.nextActions, []);
    await absent(out);
});

test("insufficient study and physical plan budgets preserve scope and fail explicitly", async (t) => {
    const f = await fixture(t);
    f.policy.rates = [10];
    for (const [name, edit, code] of [
        ["time", (p) => (p.studyBudgets.maxTotalMilliseconds = 1000), "HTTP_CAPACITY_STUDY_TIME_BUDGET"],
        ["evidence", (p) => (p.studyBudgets.maxEvidenceBytes = 1000), "HTTP_CAPACITY_STUDY_EVIDENCE_BUDGET"],
        ["physical", (p) => (p.studyBudgets.maxPlanBytes = 4096), "HTTP_CAPACITY_PLAN_BUDGET"],
    ]) {
        const policy = structuredClone(f.policy);
        edit(policy);
        await save(f.policyFile, policy);
        const out = join(f.root, name);
        await rejected(planArgs(f, out), code);
        const failure = await json(join(out, "failure.json"));
        assert.equal(failure.partialPreserved, true);
        assert.equal(failure.executed, false);
        assert.equal(failure.error.code, code);
    }
});

test("a coherent independently valid replacement child still cannot change the parent rate policy", async (t) => {
    const f = await fixture(t);
    f.policy.rates = [10];
    await save(f.policyFile, f.policy);
    const out = join(f.root, "plan");
    const original = (await command(planArgs(f, out))).data;
    const point = original.points[0],
        child = join(out, point.comparisonDirectory);
    const replacementProfile = join(f.root, "replacement-profile.json");
    const profile = await json(join(out, point.profileFile));
    profile.loads[0].ratePerSecond = 11;
    await save(replacementProfile, profile);
    const compile = join(f.root, "replacement-compile");
    await command(["http", "plan", replacementProfile, "--out", compile]);
    await rm(child, { recursive: true });
    const replacement = (
        await command([
            "http",
            "comparison",
            "plan",
            join(out, "points", point.id, "comparison-request.json"),
            "--http-plan",
            compile,
            "--out",
            child,
        ])
    ).data;
    assert.equal((await command(["http", "comparison", "verify", child])).ok, true);
    const parent = await json(join(out, "capacity-plan.json"));
    parent.points[0].comparisonPlanDigest = replacement.planDigest;
    parent.points[0].comparisonContentDigest = replacement.contentDigest;
    parent.points[0].estimate = replacement.estimate;
    parent.points[0].samplingBounds = replacement.samplingBounds;
    const { planDigest, ...payload } = parent;
    parent.planDigest = hash(payload);
    await save(join(out, "capacity-plan.json"), parent);
    await relock(out);
    await rejected(["http", "capacity", "verify", out], "HTTP_CAPACITY_POINT_INVALID");
});

test("saved TTL and observer reserves fail without changing the saved resources", async (t) => {
    const f = await fixture(t);
    f.policy.rates = [10];
    for (const [name, change, code] of [
        ["collector", (p, r) => (r.collector.durationMs = 1000), "HTTP_CAPACITY_RUNTIME_BUDGET"],
        ["evidence", (p) => (p.pointBudgets.maxEvidenceBytesPerArm = 64 * 1024 ** 2), "HTTP_CAPACITY_EVIDENCE_BUDGET"],
    ]) {
        const policy = structuredClone(f.policy),
            runtime = structuredClone(f.runtime);
        change(policy, runtime);
        await save(f.policyFile, policy);
        await save(join(f.root, "runtime.json"), runtime);
        const out = join(f.root, name);
        await rejected(planArgs(f, out), code);
        assert.deepEqual(await json(join(out, "project/runtime.json")), runtime);
        assert.equal((await json(join(out, "failure.json"))).executed, false);
    }
});

test("v2 public exploration freezes one observation per point with a configurable shared deadline", async (t) => {
    const f = await fixture(t);
    const policy = await json(resolve("examples/http-capacity-policy-v2.json"));
    const withoutMode = structuredClone(policy);
    delete withoutMode.budgetMode;
    assert.equal(httpCapacityPolicySchema.parse(withoutMode).budgetMode, "total");
    assert.equal(httpCapacityPolicySchema.parse({ ...policy, budgetMode: "stage-sum" }).budgetMode, "stage-sum");
    for (const exploration of [
        { ...policy.exploration, runs: 2 },
        { warmupMs: 10000, durationMs: 30000, pairs: 2 },
    ])
        assert.equal(httpCapacityPolicySchema.safeParse({ ...policy, exploration }).success, false);
    await save(f.policyFile, policy);
    const out = join(f.root, "single-plan");
    const plan = (await command(planArgs(f, out))).data;
    assert.equal(plan.schemaVersion, 2);
    assert.equal(plan.pointCount, 44);
    assert.equal(plan.expectedArms, 44);
    assert.equal(plan.estimate.counts.pairs, 0);
    assert.equal(plan.estimate.counts.targetStarts, 44);
    assert.equal(plan.estimate.fixedBaselineMilliseconds.total, 1760000);
    assert.equal(plan.budgetMode, "total");
    assert.equal(plan.totalDeadlineMilliseconds, policy.studyBudgets.maxTotalMilliseconds);
    assert(plan.totalDeadlineMilliseconds < plan.estimate.stageSumMaximumMilliseconds);
    assert.equal(plan.confirmation.pairs, 4);
    const child = await json(join(out, plan.points[0].comparisonDirectory, "plan.json"));
    assert.equal(child.schemaVersion, 3);
    assert.equal(child.comparison, "single-observation");
    assert.deepEqual(Object.keys(child.variants), ["O"]);
    assert.equal(child.blocks.length, 1);
    assert.equal(child.blocks[0].arms.length, 1);
    assert.equal(child.blocks[0].arms[0].arm, "O");
    assert.equal(child.budgets.armCount, 1);
    assert.equal(child.budgets.budgetMode, "total");
    assert.equal(child.analysisPolicy.kind, "http-observation-analysis-policy");
    assert.equal((await command(["http", "capacity", "verify", out])).data.valid, true);
    await mutation(out, "capacity-plan.json", (value) => (value.expectedArms = 176), "HTTP_CAPACITY_PLAN_SEMANTICS");
});
