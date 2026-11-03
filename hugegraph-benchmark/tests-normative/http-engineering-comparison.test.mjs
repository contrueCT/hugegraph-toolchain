/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, mkdir, cp, readFile, writeFile, rm, symlink } from "node:fs/promises";
import { join, resolve } from "node:path";
import { tmpdir } from "node:os";
import { performance } from "node:perf_hooks";
import { bytesDigest } from "../components/http/common.mjs";
import { valueHash } from "../components/paired/core.mjs";
import { freezeComparison } from "../components/http/comparison-plan.mjs";
import { verifyEngineeringComparison } from "../components/http/engineering-comparison.mjs";
import { observerConditionsFixture } from "./fixtures/http-observer-conditions.mjs";

const put = (path, value) => writeFile(path, JSON.stringify(value, null, 2) + "\n");
const read = async (path) => JSON.parse(await readFile(path, "utf8"));
async function fixture(
    t,
    includeIndex = true,
    mutateProfile = () => {},
    extraContributions = [],
    observation = null,
    customize = () => {},
    retainDirectory = null,
    maxTotalMilliseconds = 7200000,
) {
    const root = await mkdtemp(join(retainDirectory ?? tmpdir(), "hgbench-engineering-comparison-"));
    if (!retainDirectory) t.after(() => rm(root, { recursive: true, force: true }));
    const httpPlanDirectory = join(root, "http");
    await mkdir(httpPlanDirectory);
    await cp(resolve("benchmarks/http-serving-v0.1"), join(httpPlanDirectory, "benchmark"), { recursive: true });
    const catalog = await read(join(httpPlanDirectory, "benchmark/catalog.json")),
        answers = await read(join(httpPlanDirectory, "benchmark/answers.json")),
        settings = await read(resolve("examples/http-suite-functional.json")),
        { caseIds, purpose, ...base } = settings,
        profile = {
            ...base,
            kind: "http-benchmark-profile",
            name: "engineering-fixture",
            serviceChecks: true,
            warmupMs: 0,
            durationMs: 100,
            requestTimeoutMs: 100,
            drainMs: 100,
            repeats: 1,
            loads: [{ id: "open-10", model: "open", ratePerSecond: 10, maxInFlight: 4, maxStartDelayMs: 100 }],
            target: {
                label: "protocol fixture",
                build: "fixture",
                mode: "forward",
                environment: "fixture",
                evidenceFiles: [],
            },
            cases: ["scalar-add"].map((id) => ({
                id,
                body: catalog.cases.find((c) => c.id === id).body,
                expectedStatus: 200,
                checks: [
                    { pointer: "/status/code", equals: 200 },
                    {
                        pointer: "/result/data",
                        canonicalSha256: answers.cases.find((c) => c.id === id).canonicalSha256,
                    },
                ],
            })),
            benchmark: {
                suiteId: catalog.suiteId,
                version: catalog.version,
                bundleSha256: bytesDigest(await readFile(join(httpPlanDirectory, "benchmark/bundle.lock.json"))),
                caseIds: ["scalar-add"],
                coverage: "subset",
                purpose,
            },
        },
        build = { sourceCommit: "fixture", files: [{ path: "lib/server.jar", bytes: 1, sha256: "a".repeat(64) }] },
        target = {
            kind: "hugegraph-http-environment-settings",
            image: "sha256:" + "b".repeat(64),
            javaHome: "/fixture/jdk",
            budget: { cpus: 2, memoryBytes: 1024, pids: 16, tmpfsBytes: 256 },
            jvm: { activeProcessorCount: 2 },
            server: { workerThreads: 4 },
        },
        environment = {
            kind: "http-comparison-host-environment",
            kernel: { release: "fixture" },
            cpu: { model: "fixture" },
            memoryBytes: 4096,
            docker: { serverVersion: "fixture" },
            node: { version: process.version },
        },
        contribution = {
            id: "driverHeadroom",
            rawVerified: true,
            status: "passed",
            scope: {
                caseIds: ["scalar-add"],
                modes: ["forward"],
                ratesPerSecond: [10],
                maxInFlight: 4,
                warmupMs: 0,
                durationMs: 100,
                kind: "local-protocol-fixture",
            },
            gates: {},
            evidenceDigests: {},
            conditions: {
                build: { value: build },
                settings: { value: target },
                environment: { value: environment },
                runtime: null,
                sourceSelection: null,
                profiles: [{ id: "fixture", value: structuredClone(profile) }],
            },
            limits: ["Synthetic calibration contribution; no actual HugeGraph requests or performance claim"],
        },
        contributions = [contribution, ...extraContributions],
        index = {
            schemaVersion: 1,
            kind: "http-calibration-evidence-index",
            policyId: "engineering-v1",
            contributions,
            fullElevenCaseIds: catalog.cases.map((c) => c.id),
            limits: [],
        },
        indexFile = join(root, "calibration-evidence-index.json");
    if (observation) {
        contribution.conditions.observationArms = observation.arms;
        target.jvmObservation = { preset: "protocol-fixture-only" };
        const runtime = {
                kind: "http-comparison-runtime",
                driver: { cpus: observation.formalDriverCpus ?? 1 },
                collector: { intervalMs: 1000 },
            },
            runtimePath = join(root, "runtime.json");
        contribution.conditions.runtime = { value: { ...runtime, driver: { cpus: 1 } } };
        await put(runtimePath, runtime);
        profile.target.evidenceFiles.push(runtimePath);
    }
    await customize({ contribution, profile, root });
    await put(indexFile, index);
    mutateProfile(profile, catalog, answers);
    if (includeIndex) profile.target.evidenceFiles.push(indexFile);
    await put(join(httpPlanDirectory, "profile.json"), profile);
    const targetFiles = [];
    for (const path of profile.target.evidenceFiles) {
        const raw = await readFile(path);
        targetFiles.push({ path, bytes: raw.length, sha256: bytesDigest(raw) });
    }
    await put(join(httpPlanDirectory, "target-evidence.json"), {
        schemaVersion: 1,
        kind: "operator-supplied-target-evidence",
        files: targetFiles,
        attestation: "Protocol fixture only",
    });
    const sharedEvidence = {};
    for (const [role, value] of Object.entries({ build, environment, settings: target })) {
        const path = join(root, role + ".json");
        await put(path, value);
        sharedEvidence[role] = { path: role + ".json", sha256: bytesDigest(await readFile(path)) };
    }
    const request = {
        schemaVersion: 1,
        kind: "http-comparison-request",
        id: "engineering-fixture",
        purpose: "functional",
        comparison: "forward-embedded",
        modes: { A: "forward", B: "embedded" },
        blockIds: ["b1", "b2"],
        orderSeed: "fixture",
        sharedEvidence,
        analysis: {
            familyAlpha: 0.05,
            minimumPairs: 2,
            bootstrapReplicates: 80000,
            minimumTailDraws: 20,
            maxWork: 1000000000,
            maxMilliseconds: 60000,
            resampleSeed: "fixture",
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
            maxTotalMilliseconds,
        },
    };
    const inputFile = join(root, "request.json"),
        outputDirectory = join(root, "comparison");
    await put(inputFile, request);
    await freezeComparison({ inputFile, httpPlanDirectory, outputDirectory });
    const comparison = {
        planDirectory: outputDirectory,
        planLockSha256: bytesDigest(await readFile(join(outputDirectory, "plan.lock.json"))),
        calibrationIndexFile: indexFile,
        calibrationIndexSha256: bytesDigest(await readFile(indexFile)),
    };
    const verify = (extra = {}) =>
        verifyEngineeringComparison({
            comparison,
            index,
            contributions,
            deadline: performance.now() + 10000,
            maxBytes: 8 * 1024 * 1024,
            ...extra,
        });
    return { root, comparison, index, contributions, verify };
}

test("no formal plan remains unknown; cancellation and expired budget are not converted to a gate pass", async () => {
    const options = { comparison: null, deadline: performance.now() + 1000, maxBytes: 4096 };
    assert.equal((await verifyEngineeringComparison(options)).gate.status, "unknown");
    await assert.rejects(verifyEngineeringComparison({ ...options, signal: AbortSignal.abort() }), {
        code: "ENGINEERING_COMPARISON_CANCELLED",
    });
    await assert.rejects(verifyEngineeringComparison({ ...options, deadline: performance.now() - 1 }), {
        code: "ENGINEERING_COMPARISON_DEADLINE",
    });
});

test("real comparison verifier binds exact index bytes; frozen scope is distinct from uncovered embedded/runtime", async (t) => {
    const f = await fixture(t);
    const result = await f.verify();
    assert.equal(result.gate.status, "passed");
    assert.equal(result.verification.verified, true);
    assert(result.mismatches.some((m) => m.field === "mode" && m.formal.value === "embedded"));
    assert(result.unknownConditions.some((m) => m.field === "driver.declared-resources-and-collector"));
    assert.equal(result.performanceAccepted, false);
    assert.equal(result.inferenceAccepted, false);
    const moved = join(f.root, "moved");
    await cp(f.comparison.planDirectory, moved, { recursive: true });
    const again = await f.verify({ comparison: { ...f.comparison, planDirectory: moved } });
    assert.deepEqual(again, result);
});

test("an external index or changed reference is insufficient without an actual locked target copy", async (t) => {
    const f = await fixture(t, false);
    assert.equal((await f.verify()).gate.reason, "ENGINEERING_CALIBRATION_INDEX_NOT_FROZEN");
    assert.equal(
        (await f.verify({ comparison: { ...f.comparison, calibrationIndexSha256: "0".repeat(64) } })).gate.reason,
        "ENGINEERING_COMPARISON_INDEX_DIGEST",
    );
    const alias = join(f.root, "index-alias.json");
    await symlink(f.comparison.calibrationIndexFile, alias);
    assert.equal(
        (await f.verify({ comparison: { ...f.comparison, calibrationIndexFile: alias } })).gate.status,
        "failed",
    );
});

test("all eleven cases, higher open rate, in-flight and windows remain explicit uncovered differences", async (t) => {
    const f = await fixture(t, true, (profile, catalog, answers) => {
        profile.cases = catalog.cases.map((item) => ({
            id: item.id,
            body: item.body,
            expectedStatus: 200,
            checks: [
                { pointer: "/status/code", equals: 200 },
                {
                    pointer: "/result/data",
                    canonicalSha256: answers.cases.find((c) => c.id === item.id).canonicalSha256,
                },
            ],
        }));
        profile.benchmark.caseIds = profile.cases.map((c) => c.id);
        profile.benchmark.coverage = "full";
        profile.loads[0].ratePerSecond = 80;
        profile.loads[0].maxInFlight = 64;
        profile.warmupMs = 1000;
        profile.durationMs = 1000;
    });
    const result = await f.verify();
    assert.equal(result.gate.status, "passed");
    assert.equal(result.conditions.caseIds.length, 11);
    assert.equal(result.mismatches.filter((m) => m.field === "caseIds").length, 10);
    for (const field of ["load.open-10", "profile.warmupMs", "profile.durationMs"])
        assert(result.mismatches.some((m) => m.field === field));
    assert.equal(result.performanceAccepted, false);
});

test("old implementation is unknown while locked input tampering remains failed", async (t) => {
    const f = await fixture(t),
        directory = f.comparison.planDirectory;
    const implementation = await read(join(directory, "implementation.json"));
    implementation.files[0].sha256 = "0".repeat(64);
    await put(join(directory, "implementation.json"), implementation);
    const lock = await read(join(directory, "plan.lock.json")),
        raw = await readFile(join(directory, "implementation.json"));
    Object.assign(
        lock.files.find((v) => v.path === "implementation.json"),
        { bytes: raw.length, sha256: bytesDigest(raw) },
    );
    lock.contentDigest = valueHash(lock.files);
    await put(join(directory, "plan.lock.json"), lock);
    f.comparison.planLockSha256 = bytesDigest(await readFile(join(directory, "plan.lock.json")));
    const result = await f.verify();
    assert.equal(result.gate.status, "unknown");
    assert.equal(result.gate.reason, "COMPARISON_IMPLEMENTATION");
    await writeFile(join(directory, "inputs/build.json"), "{}\n");
    assert.equal((await f.verify()).gate.reason, "ENGINEERING_COMPARISON_LOCKED_FILE");
});

test("the explicitly unselected AA alternative is not a missing condition while selected AA and other missing readers remain visible", async (t) => {
    const f = await fixture(t, true, undefined, [
        { id: "aa005", rawVerified: false, gates: {}, conditions: null },
        {
            id: "repeatabilityAa",
            rawVerified: false,
            gates: { "repeatability-aa": { status: "unknown" } },
            conditions: null,
        },
        { id: "observerCost", rawVerified: false, gates: {}, conditions: null },
    ]);
    const result = await f.verify();
    assert.equal(result.gate.status, "passed");
    assert(!result.unknownConditions.some((v) => v.contributionId === "aa005"));
    assert(result.unknownConditions.some((v) => v.contributionId === "repeatabilityAa"));
    assert(result.unknownConditions.some((v) => v.contributionId === "observerCost"));
});

test("only complete reader-verified observation tuples match declared CPU and collector/JVM flags; matching never expands scope", async (t) => {
    const field = "observer.actual-collector-switch-and-per-arm-driver-resources";
    const matched = await fixture(t, true, undefined, [], {
        arms: [{ id: "fixture", driverCpus: 1, collector: true, jvmObservation: true }],
    });
    const result = await matched.verify();
    assert.equal(result.gate.status, "passed");
    assert(!result.mismatches.some((v) => v.field === field));
    assert(!result.unknownConditions.some((v) => v.field === field));
    assert(result.mismatches.some((v) => v.field === "mode"));
    assert.deepEqual(result.conditions.declaredObservationTuple, {
        driverCpus: 1,
        collector: true,
        jvmObservation: true,
    });
    assert.equal(result.performanceAccepted, false);
    const actualTwo = await fixture(t, true, undefined, [], {
        formalDriverCpus: 2,
        arms: [{ id: "fixture", driverCpus: 2, collector: true, jvmObservation: true }],
    });
    const twoResult = await actualTwo.verify();
    assert(!twoResult.mismatches.some((v) => [field, "driver.declared-resources-and-collector"].includes(v.field)));
    assert(!twoResult.unknownConditions.some((v) => v.field === field));
    const different = await fixture(t, true, undefined, [], {
        arms: [{ id: "fixture", driverCpus: 2, collector: false, jvmObservation: false }],
    });
    assert((await different.verify()).mismatches.some((v) => v.field === field));
    for (const arms of [
        [{ id: "fixture", driverCpus: 1, collector: true }],
        [],
        [{ id: "other", driverCpus: 1, collector: true, jvmObservation: true }],
        [
            { id: "fixture", driverCpus: 1, collector: true, jvmObservation: true },
            { id: "fixture", driverCpus: 1, collector: true, jvmObservation: true },
        ],
    ]) {
        const incomplete = await fixture(t, true, undefined, [], { arms });
        const unknown = await incomplete.verify();
        assert(unknown.unknownConditions.some((v) => v.field === field));
        assert(!unknown.mismatches.some((v) => v.field === field));
    }
});

test("a locked eleven-case formal plan discloses fixed arithmetic-control differences without granting benchmark coverage; common anchors still block", async (t) => {
    const retainDirectory = process.env.HGBENCH_METHOD_CONTROL_SCOPE_EVIDENCE ?? null;
    const full = (profile, catalog, answers) => {
        profile.cases = catalog.cases.map((item) => ({
            id: item.id,
            body: item.body,
            expectedStatus: 200,
            checks: [
                { pointer: "/status/code", equals: 200 },
                {
                    pointer: "/result/data",
                    canonicalSha256: answers.cases.find((c) => c.id === item.id).canonicalSha256,
                },
            ],
        }));
        profile.benchmark.caseIds = profile.cases.map((c) => c.id);
        profile.benchmark.coverage = "full";
        profile.warmupMs = 30000;
        profile.durationMs = 120000;
        profile.maxRequestsPerPhase = 2000;
    };
    const sourceFiles = await Promise.all(
        [
            "runner.mjs",
            "workflow.mjs",
            "trace-writer.mjs",
            "common.mjs",
            "suite-evidence.mjs",
            "hugegraph-environment.py",
            "hugegraph-service.py",
            "hugegraph-mode-probe.py",
        ].map(async (name) => {
            const path = "components/http/" + name,
                raw = await readFile(resolve(path));
            return { path, bytes: raw.length, sha256: bytesDigest(raw) };
        }),
    );
    const make = (drift = null) =>
        fixture(
            t,
            true,
            full,
            [],
            {
                arms: [{ id: "fixture", driverCpus: 1, collector: true, jvmObservation: true }],
            },
            async ({ contribution, profile, root }) => {
                contribution.id = "knownRegression";
                contribution.scope = {
                    kind: "fixed-positive-method-control",
                    caseIds: ["arithmetic-control"],
                    modes: ["forward"],
                    ratesPerSecond: [20],
                    maxInFlight: 4,
                    warmupMs: 10000,
                    durationMs: 60000,
                };
                contribution.gates = {
                    "known-regression-detection": {
                        status: "passed",
                        reason: "Synthetic method-control projection only",
                    },
                };
                const control = structuredClone(contribution.conditions.profiles[0].value);
                delete control.benchmark;
                control.warmupMs = 10000;
                control.durationMs = 60000;
                control.maxRequestsPerPhase = 2000;
                control.loads = [
                    { id: "open-20", model: "open", ratePerSecond: 20, maxInFlight: 4, maxStartDelayMs: 1000 },
                ];
                control.cases = [
                    {
                        id: "arithmetic-control",
                        body: { gremlin: "1 + 1" },
                        expectedStatus: 200,
                        checks: [{ pointer: "/result/data", equals: 2 }],
                    },
                ];
                contribution.conditions.profiles = Array.from({ length: 16 }, (_, index) => {
                    const value = structuredClone(control);
                    if (index % 2) value.cases[0].body.gremlin = "Thread.sleep(10); 1 + 1";
                    return { id: "control-" + index, value };
                });
                contribution.conditions.observationArms = contribution.conditions.profiles.map((p) => ({
                    id: p.id,
                    driverCpus: 1,
                    collector: false,
                    jvmObservation: false,
                }));
                contribution.conditions.settings = structuredClone(contribution.conditions.settings);
                delete contribution.conditions.settings.value.jvmObservation;
                contribution.conditions.runtime = structuredClone(contribution.conditions.runtime);
                contribution.conditions.runtime.value.collector = null;
                const source = {
                    kind: "actual-current-source-file-identities-for-protocol-fixture",
                    files: sourceFiles,
                };
                const sourcePath = join(root, "formal-source-selection.json");
                await put(sourcePath, source);
                profile.target.evidenceFiles.push(sourcePath);
                contribution.conditions.sourceSelection = { value: structuredClone(source) };
                if (drift === "code") contribution.conditions.sourceSelection.value.files[0].sha256 = "0".repeat(64);
                if (drift === "build") {
                    contribution.conditions.build = structuredClone(contribution.conditions.build);
                    contribution.conditions.build.value.files[0].sha256 = "0".repeat(64);
                }
                if (drift === "host") {
                    contribution.conditions.environment = structuredClone(contribution.conditions.environment);
                    contribution.conditions.environment.value.cpu.model = "different-host";
                }
            },
            retainDirectory,
            8000000,
        );
    const original = await make(),
        before = structuredClone(original.contributions),
        result = await original.verify();
    assert.equal(result.gate.status, "passed");
    assert.equal(result.conditions.caseIds.length, 11);
    assert.equal(result.mismatches.length, 0);
    assert.equal(result.unknownConditions.length, 0);
    assert.deepEqual(original.contributions, before);
    assert.equal(result.methodControlDisclosures.length, 1);
    const disclosure = result.methodControlDisclosures[0];
    assert.equal(disclosure.blocking, false);
    assert.deepEqual(disclosure.originalScope.caseIds, ["arithmetic-control"]);
    assert.deepEqual(disclosure.originalScope.modes, ["forward"]);
    assert.equal(disclosure.differences.filter((row) => row.field === "caseIds").length, 11);
    for (const field of [
        "mode",
        "load.open-10",
        "profile.warmupMs",
        "profile.durationMs",
        "observer.actual-collector-switch-and-per-arm-driver-resources",
    ])
        assert(disclosure.differences.some((row) => row.field === field));
    assert(disclosure.unknownConditions.some((row) => row.field === "benchmark"));
    assert.equal(result.performanceAccepted, false);
    for (const [drift, expected] of [
        ["code", "measurement.componentDigest.components/http/runner.mjs"],
        ["build", "build.distributionMembers"],
        ["host", "host.static"],
    ]) {
        const changed = await make(drift),
            current = await changed.verify();
        assert(current.mismatches.some((row) => row.field === expected));
        if (retainDirectory) await put(join(changed.root, "review-result.json"), current);
    }
    if (retainDirectory) await put(join(original.root, "review-result.json"), result);
});

async function observerFixture(t, { jvm = true, collector = true, mutate = () => {}, formal = () => {} } = {}) {
    return fixture(
        t,
        true,
        undefined,
        [],
        { arms: [{ id: "fixture", driverCpus: 1, collector: true, jvmObservation: true }] },
        async ({ contribution, profile, root }) => {
            const formalTarget = contribution.conditions.settings.value,
                snapshots = await observerConditionsFixture(t, {
                    profile: contribution.conditions.profiles[0].value,
                    settings: contribution.conditions.settings.value,
                    runtime: contribution.conditions.runtime.value,
                }),
                actual = await snapshots.read();
            contribution.id = "observerCost";
            contribution.conditions.settings = actual.settings;
            contribution.conditions.settingsVariants = actual.settingsVariants;
            contribution.conditions.profiles = actual.profiles;
            contribution.conditions.observationArms = actual.observationArms;
            const source = {
                files: await Promise.all(
                    [
                        "runner.mjs",
                        "workflow.mjs",
                        "trace-writer.mjs",
                        "common.mjs",
                        "suite-evidence.mjs",
                        "hugegraph-environment.py",
                        "hugegraph-service.py",
                        "hugegraph-mode-probe.py",
                    ].map(async (name) => {
                        const path = "components/http/" + name,
                            raw = await readFile(resolve(path));
                        return { path, bytes: raw.length, sha256: bytesDigest(raw) };
                    }),
                ),
            };
            contribution.conditions.sourceSelection = { value: structuredClone(source) };
            const sourceFile = join(root, "formal-source.json");
            await put(sourceFile, source);
            profile.target.evidenceFiles.push(sourceFile);
            // The existing fixture writes this original target as the formal
            // settings after customization; the observer snapshots above are
            // independent clones and keep their actual on/off association.
            for (const key of Object.keys(formalTarget)) delete formalTarget[key];
            Object.assign(formalTarget, structuredClone(actual.settingsVariants[jvm ? 1 : 0].value));
            const runtimeFile = join(root, "runtime.json"),
                runtime = await read(runtimeFile);
            if (!collector) runtime.collector = null;
            await formal({ target: formalTarget, runtime, root });
            await put(runtimeFile, runtime);
            await mutate(contribution.conditions);
        },
    );
}

test("observer locked on/off settings and actual per-arm collector runtime match only the three measured combinations", async (t) => {
    const fields = [
        "target.resources-and-jvm",
        "driver.declared-resources-and-collector",
        "observer.actual-collector-switch-and-per-arm-driver-resources",
    ];
    for (const [jvm, collector] of [
        [true, true],
        [false, true],
        [true, false],
    ]) {
        const f = await observerFixture(t, { jvm, collector }),
            result = await f.verify();
        assert.equal(result.gate.status, "passed");
        assert(!result.mismatches.some((v) => fields.includes(v.field)));
        assert(!result.unknownConditions.some((v) => fields.includes(v.field)));
        assert.equal(result.conditions.declaredObservationTuple.jvmObservation, jvm);
        assert.equal(result.conditions.declaredObservationTuple.collector, collector);
        assert(result.mismatches.some((v) => v.field === "mode"));
        assert.equal(result.performanceAccepted, false);
    }
    const unmeasured = await observerFixture(t, { jvm: false, collector: false }),
        result = await unmeasured.verify();
    assert(result.mismatches.some((v) => v.field === fields[2]));
    assert(!result.mismatches.some((v) => fields.slice(0, 2).includes(v.field)));
    assert.equal(result.performanceAccepted, false);
});

test("observer wrong formal configuration remains mismatched while missing variants or bad arm associations stay unknown", async (t) => {
    const changed = await observerFixture(t, {
        formal: ({ target }) => {
            target.server.workerThreads++;
        },
    });
    assert((await changed.verify()).mismatches.some((v) => v.field === "target.resources-and-jvm"));
    for (const mutate of [
        (c) => {
            c.settingsVariants.pop();
        },
        (c) => {
            c.observationArms[0].settingsSha256 = "0".repeat(64);
        },
        (c) => {
            c.observationArms[0].settingsVariantId = "wrong";
        },
        (c) => {
            c.observationArms[0].cellId = c.observationArms[1].cellId;
        },
        (c) => {
            c.observationArms[0].id = "not-the-actual-arm";
            c.profiles[0].id = "not-the-actual-arm";
        },
        (c) => {
            c.runtime.value.collector = null;
        },
    ]) {
        const incomplete = await observerFixture(t, { mutate }),
            result = await incomplete.verify();
        assert(result.unknownConditions.some((v) => v.field === "driver.declared-resources-and-collector"));
        assert.equal(result.performanceAccepted, false);
    }
});

test("observer variant selection retains the original build, host and all measurement code anchors", async (t) => {
    for (const [mutate, field] of [
        [
            (c) => {
                c.build.value = structuredClone(c.build.value);
                c.build.value.files[0].sha256 = "0".repeat(64);
            },
            "build.distributionMembers",
        ],
        [
            (c) => {
                c.environment.value = structuredClone(c.environment.value);
                c.environment.value.cpu.model = "different-host";
            },
            "host.static",
        ],
        [
            (c) => {
                c.sourceSelection.value.files[7].sha256 = "0".repeat(64);
            },
            "measurement.componentDigest.components/http/hugegraph-mode-probe.py",
        ],
    ]) {
        const f = await observerFixture(t, { mutate }),
            result = await f.verify();
        assert(result.mismatches.some((v) => v.field === field));
        assert.equal(result.methodControlDisclosures.length, 0);
        assert.equal(result.performanceAccepted, false);
    }
});
