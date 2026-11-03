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
import { mkdtemp, readFile, writeFile, mkdir, rm, cp, symlink, access } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import http from "node:http";
import { freezeComparison, verifyComparison, validateRequest, describe } from "../components/http/comparison-plan.mjs";
import { balancedOrders, valueHash, validatePolicy } from "../components/paired/core.mjs";
import { bytesDigest } from "../components/http/common.mjs";

const run = promisify(execFile);
const read = async (path) => JSON.parse(await readFile(path, "utf8"));
const put = (path, value) => writeFile(path, JSON.stringify(value, null, 2) + "\n");
const component = resolve("components/http/comparison-plan.mjs");

async function fixture(t) {
    const root = await mkdtemp(join(tmpdir(), "hgbench-comparison-plan-"));
    t.after(() => rm(root, { recursive: true, force: true }));
    const httpPlanDirectory = join(root, "http-plan");
    await mkdir(httpPlanDirectory);
    await cp(resolve("benchmarks/http-serving-v0.1"), join(httpPlanDirectory, "benchmark"), { recursive: true });
    const catalog = await read(join(httpPlanDirectory, "benchmark/catalog.json"));
    const answers = await read(join(httpPlanDirectory, "benchmark/answers.json"));
    const settings = await read(resolve("examples/http-suite-functional.json"));
    const { caseIds: ignored, purpose, ...base } = settings;
    const selected = ["scalar-add", "string-0001k"];
    const profile = {
        ...base,
        kind: "http-benchmark-profile",
        serviceChecks: true,
        target: {
            label: "fixture",
            build: "fixture-build",
            mode: "forward",
            environment: "fixture",
            evidenceFiles: [],
        },
        warmupMs: 0,
        durationMs: 100,
        requestTimeoutMs: 100,
        drainMs: 100,
        cases: selected.map((id) => ({
            id,
            body: catalog.cases.find((item) => item.id === id).body,
            expectedStatus: 200,
            checks: [
                { pointer: "/status/code", equals: 200 },
                {
                    pointer: "/result/data",
                    canonicalSha256: answers.cases.find((item) => item.id === id).canonicalSha256,
                },
            ],
        })),
        loads: [
            { id: "open-2", model: "open", ratePerSecond: 2, maxInFlight: 1, maxStartDelayMs: 100 },
            { id: "closed-1", model: "closed", concurrency: 1 },
        ],
        benchmark: {
            suiteId: catalog.suiteId,
            version: catalog.version,
            bundleSha256: bytesDigest(await readFile(join(httpPlanDirectory, "benchmark/bundle.lock.json"))),
            caseIds: selected,
            coverage: "subset",
            purpose,
        },
    };
    await put(join(httpPlanDirectory, "profile.json"), profile);
    await put(join(httpPlanDirectory, "target-evidence.json"), {
        schemaVersion: 1,
        kind: "operator-supplied-target-evidence",
        files: [],
        attestation: "Fixture only",
    });
    const sharedEvidence = {};
    for (const role of ["build", "environment", "settings"]) {
        const path = join(root, `${role}.json`);
        await put(path, { schemaVersion: 1, kind: `fixture-${role}`, realTargetVerified: false });
        sharedEvidence[role] = { path: `${role}.json`, sha256: bytesDigest(await readFile(path)) };
    }
    const request = {
        schemaVersion: 1,
        kind: "http-comparison-request",
        id: "fixture-pairs-v1",
        purpose: "functional",
        comparison: "forward-embedded",
        modes: { A: "forward", B: "embedded" },
        blockIds: ["b01", "b02", "b03", "b04"],
        orderSeed: "fixture-order-v1",
        sharedEvidence,
        analysis: {
            familyAlpha: 0.05,
            minimumPairs: 4,
            bootstrapReplicates: 20000,
            minimumTailDraws: 20,
            maxWork: 1000000000,
            maxMilliseconds: 60000,
            resampleSeed: "fixture-resample-v1",
            latency: { relativeBudget: 0.05, absoluteBudget: 1, minimumObservations: 1000 },
            goodput: { relativeBudget: 0.05, absoluteBudget: 1, minimumObservations: 1000 },
        },
        budgets: {
            prepareMs: 100,
            startMs: 1000,
            modeProbeMs: 30000,
            settleMs: 0,
            stopMs: 1000,
            sealAndAnalyzeMs: 1000,
            maxEvidenceBytesPerArm: 8 * 1024 * 1024,
            maxTotalEvidenceBytes: 512 * 1024 * 1024,
            maxTotalMilliseconds: 7200000,
        },
    };
    const inputFile = join(root, "request.json");
    await put(inputFile, request);
    return {
        root,
        profile,
        request,
        inputFile,
        httpPlanDirectory,
        outputDirectory: join(root, "comparison"),
        save: () => put(inputFile, request),
        saveProfile: () => put(join(httpPlanDirectory, "profile.json"), profile),
    };
}
async function relock(root, file, value) {
    await put(join(root, file), value);
    const lock = await read(join(root, "plan.lock.json"));
    const entry = lock.files.find((entry) => entry.path === file);
    const bytes = await readFile(join(root, file));
    Object.assign(entry, { bytes: bytes.length, sha256: bytesDigest(bytes) });
    lock.contentDigest = valueHash(lock.files);
    await put(join(root, "plan.lock.json"), lock);
}

test("comparison plan freezes deterministic balanced independent arms and complete metric family", async (t) => {
    const f = await fixture(t);
    const first = await freezeComparison(f);
    const second = await freezeComparison({ ...f, outputDirectory: join(f.root, "second") });
    assert.equal(first.planDigest, second.planDigest);
    assert.equal(first.contentDigest, second.contentDigest);
    assert.equal(first.armCount, 8);
    assert.equal(first.executed, false);
    const plan = await read(join(f.outputDirectory, "plan.json"));
    assert.deepEqual(
        Object.fromEntries(plan.blocks.map((block) => [block.id, block.order])),
        balancedOrders(f.request.blockIds, f.request.orderSeed),
    );
    assert.equal(plan.blocks.filter((block) => block.order === "AB").length, 2);
    assert.equal(new Set(plan.blocks.flatMap((block) => block.arms.map((arm) => arm.runId))).size, 8);
    assert.deepEqual(
        plan.blocks.flatMap((block) => block.arms.map((arm) => arm.position)),
        [0, 1, 2, 3, 4, 5, 6, 7],
    );
    assert.equal(plan.metricBindings.length, 8);
    assert.equal(plan.analysisPolicy.metrics.length, 8);
    assert.equal(validatePolicy(plan.analysisPolicy), plan.analysisPolicy);
    assert(plan.metricBindings.some((metric) => metric.valuePath === "correctEndToEndMs.p99"));
    assert(plan.metricBindings.some((metric) => metric.valuePath === "correctResponseMs.p99"));
    assert.equal(plan.performanceAccepted, false);
    assert.equal(plan.inferenceAccepted, false);
    assert.equal(plan.gates.prospectiveExecutionVerified, false);
    assert.equal((await verifyComparison(f.outputDirectory)).verified, true);
});

test("optional latency quantile preserves P99 defaults and binds every declared case/load without leaking policy fields", async (t) => {
    const f = await fixture(t);
    await freezeComparison(f);
    const original = await read(join(f.outputDirectory, "plan.json"));
    assert(!Object.hasOwn((await read(join(f.outputDirectory, "request.json"))).analysis.latency, "quantile"));
    for (const quantile of ["p50", "p95", "p99"]) {
        f.request.analysis.latency.quantile = quantile;
        await f.save();
        const outputDirectory = join(f.root, quantile);
        const result = await freezeComparison({ ...f, outputDirectory });
        const plan = await read(join(outputDirectory, "plan.json"));
        const latency = plan.metricBindings.filter((metric) => metric.kind === "latency");
        assert.equal(plan.metricBindings.length, 8);
        assert.equal(latency.length, 4);
        assert(latency.some((metric) => metric.valuePath === `correctEndToEndMs.${quantile}`));
        assert(latency.some((metric) => metric.valuePath === `correctResponseMs.${quantile}`));
        for (const binding of latency) {
            assert.equal(binding.observations, binding.valuePath.replace(/p(?:50|95|99)$/, "count"));
            const policy = plan.analysisPolicy.metrics.find((metric) => metric.id === binding.id);
            assert.equal(policy.minimumObservations, 1000);
            assert.deepEqual(Object.keys(policy).sort(), [
                "absoluteBudget",
                "better",
                "id",
                "minimumObservations",
                "relativeBudget",
                "unit",
            ]);
        }
        assert.equal(validatePolicy(plan.analysisPolicy), plan.analysisPolicy);
        assert.deepEqual(result.estimate, plan.estimate);
        assert.deepEqual(result.samplingBounds, plan.samplingBounds);
        const verified = await verifyComparison(outputDirectory);
        assert.deepEqual(verified.estimate, plan.estimate);
        if (quantile === "p99") {
            assert.deepEqual(plan.metricBindings, original.metricBindings);
            assert.deepEqual(plan.analysisPolicy, original.analysisPolicy);
        } else {
            assert.notEqual(latency[0].id, original.metricBindings.find((metric) => metric.kind === "latency").id);
        }
    }
    for (const quantile of ["p90", "P99", null, 99]) {
        const request = structuredClone(f.request);
        request.analysis.latency.quantile = quantile;
        assert.throws(() => validateRequest(request), { code: "COMPARISON_SCHEMA" });
    }
    const request = structuredClone(f.request);
    request.analysis.goodput.quantile = "p50";
    assert.throws(() => validateRequest(request), { code: "COMPARISON_SCHEMA" });
});

test("preview separates fixed configured time from safety reserves and never predicts closed-loop throughput", async (t) => {
    const f = await fixture(t);
    f.profile.warmupMs = 500;
    f.profile.durationMs = 1500;
    f.request.budgets.settleMs = 250;
    f.request.analysis.latency.minimumObservations = 2;
    f.request.analysis.goodput.minimumObservations = 2;
    await f.saveProfile();
    await f.save();
    const result = await freezeComparison(f);
    const plan = await read(join(f.outputDirectory, "plan.json"));
    assert.deepEqual(result.estimate.counts, {
        cases: 2,
        loadPoints: 2,
        caseLoadGroups: 4,
        blocks: 4,
        arms: 8,
        httpRuns: 32,
        preflightRequests: 16,
        postcheckRequests: 16,
    });
    assert.deepEqual(result.estimate.fixedBaselineMilliseconds, {
        warmupPerRun: 500,
        measurementPerRun: 1500,
        perArm: 8000,
        total: 64000,
        settleTotal: 2000,
        includingSettleTotal: 66000,
    });
    assert.equal(result.estimate.confidence, "low");
    assert.equal(result.estimate.normalEtaMilliseconds, null);
    assert.equal(result.estimate.safetyUpperBoundMilliseconds, plan.budgets.requiredTotalMilliseconds);
    assert(result.estimate.safetyUpperBoundMilliseconds > 66000);
    for (const stage of ["prepare", "startup", "modeChecks", "preflight", "postcheck", "drain", "analysis", "cleanup"])
        assert.equal(result.estimate.overheads[stage].normalMilliseconds, null);
    assert.equal(result.estimate.overheads.clockReference.normalMilliseconds, 0);
    for (const bound of result.samplingBounds) {
        assert.equal(bound.estimatedObservations, null);
        assert.equal(bound.estimatedCorrectRequestsPerSecond, null);
        assert.equal(bound.minimumRequiredObservations, 2);
        if (bound.loadModel === "open") {
            assert.equal(bound.maximumPossibleObservations, 3);
            assert.equal(bound.boundKind, "planned-arrival-upper-bound");
            assert.equal(bound.minimumPotentiallyReachable, true);
        } else {
            assert.equal(bound.maximumPossibleObservations, f.profile.maxRequestsPerPhase);
            assert.equal(bound.boundKind, "request-cap-only");
            assert.equal(bound.minimumPotentiallyReachable, null);
        }
    }
    assert.equal(plan.performanceAccepted, false);
});

test("quantile source, observation count and planning previews remain semantically locked even after resealing", async (t) => {
    const f = await fixture(t);
    f.request.analysis.latency.quantile = "p50";
    await f.save();
    await freezeComparison(f);
    const original = await read(join(f.outputDirectory, "plan.json"));
    for (const mutate of [
        (plan) => {
            plan.metricBindings.find((metric) => metric.kind === "latency").valuePath = "correctResponseMs.p99";
        },
        (plan) => {
            plan.metricBindings.find((metric) => metric.kind === "latency").observations = "correctResponseMs.count";
        },
        (plan) => {
            plan.estimate.normalEtaMilliseconds = plan.budgets.requiredTotalMilliseconds;
        },
        (plan) => {
            plan.samplingBounds[0].estimatedObservations = plan.samplingBounds[0].maximumPossibleObservations;
        },
    ]) {
        const plan = structuredClone(original);
        mutate(plan);
        delete plan.planDigest;
        plan.planDigest = valueHash(plan);
        await relock(f.outputDirectory, "plan.json", plan);
        const lock = await read(join(f.outputDirectory, "plan.lock.json"));
        lock.planDigest = plan.planDigest;
        await put(join(f.outputDirectory, "plan.lock.json"), lock);
        await assert.rejects(verifyComparison(f.outputDirectory), { code: "COMPARISON_PLAN_CHANGED" });
    }
});

test("public planner returns a P50 sample/time preview and verifies it without executing a target", async (t) => {
    const f = await fixture(t);
    f.request.analysis.latency.quantile = "p50";
    await f.save();
    const cli = resolve("dist/cli.js");
    const planned = JSON.parse(
        (
            await run(process.execPath, [
                cli,
                "http",
                "comparison",
                "plan",
                f.inputFile,
                "--http-plan",
                f.httpPlanDirectory,
                "--out",
                f.outputDirectory,
            ])
        ).stdout,
    );
    assert.equal(planned.ok, true);
    assert.equal(planned.data.estimate.normalEtaMilliseconds, null);
    assert.equal(planned.data.estimate.counts.cases, 2);
    assert.equal(planned.data.samplingBounds.length, 8);
    assert.equal(planned.data.executed, false);
    const verified = JSON.parse(
        (await run(process.execPath, [cli, "http", "comparison", "verify", f.outputDirectory])).stdout,
    );
    assert.equal(verified.data.verified, true);
    assert.deepEqual(verified.data.estimate, planned.data.estimate);
    assert.equal(verified.data.performanceAccepted, false);
});

test("A/A has equal variant identity but unique run identities and independent process starts", async (t) => {
    const f = await fixture(t);
    f.request.comparison = "aa";
    f.request.modes.B = "forward";
    await f.save();
    await freezeComparison(f);
    const plan = await read(join(f.outputDirectory, "plan.json"));
    assert.equal(plan.variants.A.variantDigest, plan.variants.B.variantDigest);
    assert.equal(plan.variants.A.profileSha256, plan.variants.B.profileSha256);
    assert.equal(new Set(plan.blocks.flatMap((block) => block.arms.map((arm) => arm.runId))).size, 8);
    assert.match(plan.executionContract.reset, /new-process-per-arm/);
    assert.equal((await verifyComparison(f.outputDirectory)).verified, true);
});

test("all 11 questions preserve all primary metrics and disclose unreachable functional sample minima", async (t) => {
    const f = await fixture(t);
    const catalog = await read(join(f.httpPlanDirectory, "benchmark/catalog.json"));
    const answers = await read(join(f.httpPlanDirectory, "benchmark/answers.json"));
    f.profile.benchmark.caseIds = catalog.cases.map((item) => item.id);
    f.profile.benchmark.coverage = "full";
    f.profile.cases = catalog.cases.map((item) => ({
        id: item.id,
        body: item.body,
        expectedStatus: 200,
        checks: [
            { pointer: "/status/code", equals: 200 },
            {
                pointer: "/result/data",
                canonicalSha256: answers.cases.find((answer) => answer.id === item.id).canonicalSha256,
            },
        ],
    }));
    f.request.analysis.bootstrapReplicates = 100000;
    await f.saveProfile();
    await f.save();
    await freezeComparison(f);
    const plan = await read(join(f.outputDirectory, "plan.json"));
    assert.equal(plan.metricBindings.length, 44);
    assert.equal(plan.suite.coverage, "full");
    assert.equal(plan.samplingBounds.length, 44);
    assert(plan.samplingBounds.every((bound) => bound.minimumPotentiallyReachable === false));
    assert.equal((await verifyComparison(f.outputDirectory)).verified, true);
    f.profile.loads = Array.from({ length: 6 }, (_, index) => ({
        id: `closed-${index}`,
        model: "closed",
        concurrency: 1,
    }));
    await f.saveProfile();
    await assert.rejects(freezeComparison({ ...f, outputDirectory: join(f.root, "oversized-family") }), {
        code: "COMPARISON_FAMILY_BUDGET",
    });
});

test("arm profiles differ only in target.mode and retain shared endpoint, workload and transport", async (t) => {
    const f = await fixture(t);
    await freezeComparison(f);
    const a = await read(join(f.outputDirectory, "profiles/A.json"));
    const b = await read(join(f.outputDirectory, "profiles/B.json"));
    assert.equal(a.target.mode, "forward");
    assert.equal(b.target.mode, "embedded");
    b.target.mode = "forward";
    assert.deepEqual(a, b);
    assert.equal(a.target.evidenceFiles.length, 3);
});

test("new seed or experiment ID changes run identities without changing common conditions", async (t) => {
    const f = await fixture(t);
    await freezeComparison(f);
    const before = await read(join(f.outputDirectory, "plan.json"));
    f.request.orderSeed = "another-order";
    f.request.id = "fixture-pairs-v2";
    await f.save();
    const out = join(f.root, "next");
    await freezeComparison({ ...f, outputDirectory: out });
    const after = await read(join(out, "plan.json"));
    assert.notEqual(before.planDigest, after.planDigest);
    assert.notEqual(before.blocks[0].arms[0].runId, after.blocks[0].arms[0].runId);
    assert.equal(before.conditionsBaseDigest, after.conditionsBaseDigest);
});

test("frozen plan is relocatable and does not reopen original external evidence", async (t) => {
    const f = await fixture(t);
    await freezeComparison(f);
    await rm(f.httpPlanDirectory, { recursive: true });
    for (const role of ["build", "environment", "settings"]) await rm(join(f.root, `${role}.json`));
    const relocated = join(f.root, "relocated");
    await cp(f.outputDirectory, relocated, { recursive: true });
    assert.equal((await verifyComparison(relocated)).verified, true);
});

test("existing target attachments are copied and their profile references resolve inside the snapshot", async (t) => {
    const f = await fixture(t);
    const attachment = join(f.root, "observed-target.json");
    await put(attachment, { kind: "fixture-target-observation", accepted: false });
    const raw = await readFile(attachment);
    f.profile.target.evidenceFiles = [attachment];
    await f.saveProfile();
    await put(join(f.httpPlanDirectory, "target-evidence.json"), {
        schemaVersion: 1,
        kind: "operator-supplied-target-evidence",
        files: [{ path: attachment, bytes: raw.length, sha256: bytesDigest(raw) }],
        attestation: "Fixture attachment only",
    });
    await freezeComparison(f);
    await rm(attachment);
    const profile = await read(join(f.outputDirectory, "profiles/A.json"));
    assert.equal(profile.target.evidenceFiles[0], "../target/000.json");
    assert.deepEqual(await readFile(resolve(f.outputDirectory, "profiles", profile.target.evidenceFiles[0])), raw);
    assert.equal((await verifyComparison(f.outputDirectory)).verified, true);
});

test("formal purpose, odd/duplicate blocks, hidden variant fields and mismatched modes are refused", async (t) => {
    const f = await fixture(t);
    for (const mutate of [
        (r) => {
            r.purpose = "formal";
        },
        (r) => {
            r.blockIds.pop();
        },
        (r) => {
            r.blockIds[1] = r.blockIds[0];
        },
        (r) => {
            r.modes.B = "forward";
        },
        (r) => {
            r.modes.endpoint = "http://other";
        },
        (r) => {
            r.id = null;
        },
    ]) {
        const changed = structuredClone(f.request);
        mutate(changed);
        assert.throws(() => validateRequest(changed));
    }
});

test("time, evidence, precision and analysis resource budgets reject before output reservation", async (t) => {
    const f = await fixture(t);
    for (const [field, value, code] of [
        ["maxTotalMilliseconds", 1, "COMPARISON_TIME_BUDGET"],
        ["maxEvidenceBytesPerArm", 4096, "COMPARISON_EVIDENCE_BUDGET"],
        ["maxTotalEvidenceBytes", 4096, "COMPARISON_EVIDENCE_BUDGET"],
    ]) {
        const original = f.request.budgets[field];
        f.request.budgets[field] = value;
        await f.save();
        await assert.rejects(freezeComparison(f), { code });
        await assert.rejects(access(f.outputDirectory));
        f.request.budgets[field] = original;
    }
    f.request.analysis.bootstrapReplicates = 1000;
    await f.save();
    await assert.rejects(freezeComparison(f), { code: "COMPARISON_PRECISION" });
    f.request.analysis.bootstrapReplicates = 20000;
    f.request.analysis.maxWork = 1;
    await f.save();
    await assert.rejects(freezeComparison(f), { code: "COMPARISON_ANALYSIS_BUDGET" });
});

test("profile repeats, missing checks, missing suite and wrong exact answers cannot enter a plan", async (t) => {
    const f = await fixture(t);
    const saved = structuredClone(f.profile);
    for (const mutate of [
        (p) => {
            p.repeats = 2;
        },
        (p) => {
            p.serviceChecks = false;
        },
        (p) => {
            delete p.benchmark;
        },
        (p) => {
            p.cases[0].checks[1].canonicalSha256 = "f".repeat(64);
        },
    ]) {
        const changed = structuredClone(saved);
        mutate(changed);
        await put(join(f.httpPlanDirectory, "profile.json"), changed);
        await assert.rejects(freezeComparison(f));
    }
});

test("shared identity digest drift, shared mode settings and symlinks fail closed", async (t) => {
    const f = await fixture(t);
    await put(join(f.root, "settings.json"), { changed: true });
    await assert.rejects(freezeComparison(f), { code: "COMPARISON_IDENTITY" });
    await put(join(f.root, "settings.json"), { nested: { "gremlinserver.rest_execution_mode": "forward" } });
    f.request.sharedEvidence.settings.sha256 = bytesDigest(await readFile(join(f.root, "settings.json")));
    await f.save();
    await assert.rejects(freezeComparison(f), { code: "COMPARISON_SETTINGS" });
    await rm(join(f.root, "settings.json"));
    await symlink(join(f.root, "build.json"), join(f.root, "settings.json"));
    await assert.rejects(freezeComparison(f), { code: "COMPARISON_FILE" });
});

test("changed suite file is rejected before freezing", async (t) => {
    const f = await fixture(t);
    await writeFile(join(f.httpPlanDirectory, "benchmark/protocol.json"), "{}\n");
    await assert.rejects(freezeComparison(f), { code: "COMPARISON_SUITE" });
});

test("FIFO and directory identity inputs fail promptly without a writer", async (t) => {
    const f = await fixture(t);
    const path = join(f.root, "settings.json");
    await rm(path);
    await mkdir(path);
    await assert.rejects(freezeComparison(f), { code: "COMPARISON_FILE" });
    await rm(path, { recursive: true });
    await run("mkfifo", [path]);
    // A subprocess watchdog makes a regression in pre-fstat open visible without hanging this test process.
    await assert.rejects(
        run(
            process.execPath,
            [component, "--input", f.inputFile, "--http-plan", f.httpPlanDirectory, "--out", f.outputDirectory],
            { timeout: 3000 },
        ),
        (error) => {
            assert.equal(error.code, 6);
            assert.equal(JSON.parse(error.stderr).error.code, "COMPARISON_FILE");
            return true;
        },
    );
});

test("semantic verification rejects a forged schedule even when its file hash is updated", async (t) => {
    const f = await fixture(t);
    await freezeComparison(f);
    const plan = await read(join(f.outputDirectory, "plan.json"));
    plan.blocks[0].order = plan.blocks[0].order === "AB" ? "BA" : "AB";
    await relock(f.outputDirectory, "plan.json", plan);
    await assert.rejects(verifyComparison(f.outputDirectory), { code: "COMPARISON_PLAN_CHANGED" });
});

test("semantic verification rejects extra arm mutations and omitted target inputs", async (t) => {
    const f = await fixture(t);
    await freezeComparison(f);
    const profile = await read(join(f.outputDirectory, "profiles/B.json"));
    profile.responseEncoding = profile.responseEncoding === "gzip" ? "identity" : "gzip";
    await relock(f.outputDirectory, "profiles/B.json", profile);
    await assert.rejects(verifyComparison(f.outputDirectory), { code: "COMPARISON_PROFILE_CHANGED" });
    const other = join(f.root, "other");
    await freezeComparison({ ...f, outputDirectory: other });
    const targets = await read(join(other, "target-files.json"));
    targets.pop();
    await relock(other, "target-files.json", targets);
    await assert.rejects(verifyComparison(other), { code: "COMPARISON_TARGET_EVIDENCE" });
});

test("unlisted files, path traversal, changed content and existing output are refused", async (t) => {
    const f = await fixture(t);
    const first = await freezeComparison(f);
    await assert.rejects(freezeComparison(f), { code: "COMPARISON_OUTPUT_EXISTS" });
    assert.equal((await verifyComparison(f.outputDirectory)).planDigest, first.planDigest);
    await writeFile(join(f.outputDirectory, "unexpected.txt"), "extra");
    await assert.rejects(verifyComparison(f.outputDirectory), { code: "COMPARISON_LOCK" });
    await rm(join(f.outputDirectory, "unexpected.txt"));
    const lock = await read(join(f.outputDirectory, "plan.lock.json"));
    lock.files[0].path = "../outside";
    lock.contentDigest = valueHash(lock.files);
    await put(join(f.outputDirectory, "plan.lock.json"), lock);
    await assert.rejects(verifyComparison(f.outputDirectory), { code: "COMPARISON_LOCK" });
});

test("CLI help/describe/structured errors and successful freeze perform no target HTTP", async (t) => {
    const f = await fixture(t);
    let requests = 0;
    const server = http.createServer((_, response) => {
        requests++;
        response.end("unexpected");
    });
    await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
    t.after(() => new Promise((resolve) => server.close(resolve)));
    f.profile.endpoint = `http://127.0.0.1:${server.address().port}/gremlin`;
    await f.saveProfile();
    const help = await run(process.execPath, [component, "--help"]);
    assert.match(help.stdout, /plan only/);
    const info = await run(process.execPath, [component, "--describe"]);
    assert.deepEqual(JSON.parse(info.stdout), describe());
    const success = await run(process.execPath, [
        component,
        "--input",
        f.inputFile,
        "--http-plan",
        f.httpPlanDirectory,
        "--out",
        f.outputDirectory,
    ]);
    assert.equal(JSON.parse(success.stdout).data.executed, false);
    const verified = await run(process.execPath, [component, "--verify", f.outputDirectory]);
    assert.equal(JSON.parse(verified.stdout).data.verified, true);
    assert.equal(requests, 0);
    await assert.rejects(run(process.execPath, [component, "--unknown", "secret-not-echoed"]), (error) => {
        assert.equal(error.code, 6);
        assert.equal(JSON.parse(error.stderr).error.code, "COMPARISON_ARGUMENTS");
        assert(!error.stderr.includes("secret-not-echoed"));
        return true;
    });
});

test("comparison reserves three attachment slots and revalidates generated profiles", async (t) => {
    const f = await fixture(t);
    const entries = [];
    for (let index = 0; index < 30; index++) {
        const path = join(f.root, `attachment-${index}.json`);
        await put(path, { index });
        const raw = await readFile(path);
        entries.push({ path, bytes: raw.length, sha256: bytesDigest(raw) });
    }
    f.profile.target.evidenceFiles = entries.map((e) => e.path);
    await f.saveProfile();
    await put(join(f.httpPlanDirectory, "target-evidence.json"), {
        schemaVersion: 1,
        kind: "operator-supplied-target-evidence",
        files: entries,
        attestation: "Fixture",
    });
    await assert.rejects(freezeComparison(f), (e) => e.code === "COMPARISON_ATTACHMENT_LIMIT");
    f.profile.target.evidenceFiles.pop();
    entries.pop();
    await f.saveProfile();
    await put(join(f.httpPlanDirectory, "target-evidence.json"), {
        schemaVersion: 1,
        kind: "operator-supplied-target-evidence",
        files: entries,
        attestation: "Fixture",
    });
    await freezeComparison(f);
    assert.equal((await read(join(f.outputDirectory, "profiles/A.json"))).target.evidenceFiles.length, 32);
    assert.equal((await verifyComparison(f.outputDirectory)).verified, true);
});

test("Go comparison plans freeze executor identity and reserve both live host gates without changing workload", async (t) => {
    const f = await fixture(t);
    await freezeComparison(f);
    const node = await read(join(f.outputDirectory, "plan.json"));
    f.request.budgets.maxEvidenceBytesPerArm = 16 * 1024 * 1024;
    await f.save();
    f.profile.schemaVersion = 2;
    f.profile.executor = {
        engine: "go-http-v1",
        binary: {
            path: "/explicit/runner",
            sha256: "a".repeat(64),
            sourceSha256: "b".repeat(64),
            goVersion: "go1.27.3",
            goos: "linux",
            goarch: "amd64",
        },
        gomaxprocs: 2,
        cpuQuotaCores: 2,
        maxRssBytes: 1073741824,
        maxPendingBytes: 1048576,
        hostIdentityGate: true,
    };
    delete f.profile.clockReference;
    await f.saveProfile();
    const outputDirectory = join(f.root, "go");
    await freezeComparison({ ...f, outputDirectory });
    assert.equal((await verifyComparison(outputDirectory)).verified, true);
    const go = await read(join(outputDirectory, "plan.json")),
        a = await read(join(outputDirectory, "profiles/A.json")),
        b = await read(join(outputDirectory, "profiles/B.json"));
    assert.deepEqual(a.executor, b.executor);
    assert.deepEqual(a.executor, f.profile.executor);
    assert.deepEqual(a.cases, f.profile.cases);
    assert.deepEqual(a.loads, f.profile.loads);
    assert.equal(go.budgets.workflowMaxMilliseconds - node.budgets.workflowMaxMilliseconds, 160000);
    assert.equal(go.estimate.overheads.liveProcessIdentity.safetyAllowanceMilliseconds, 1280000);
    assert.equal(go.estimate.overheads.liveProcessIdentity.normalMilliseconds, null);
    assert.equal(go.performanceAccepted, false);
    f.profile.executor.hostIdentityGate = false;
    await f.saveProfile();
    await assert.rejects(freezeComparison({ ...f, outputDirectory: join(f.root, "go-no-host-gate") }), {
        code: "COMPARISON_GO_IDENTITY_GATE",
    });
});
