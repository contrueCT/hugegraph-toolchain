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
import { writeFile, readFile, mkdir, readdir, lstat } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { randomBytes, createHash } from "node:crypto";
import { planHttpCapacity, verifyHttpCapacityPlan } from "../dist/http-capacity.js";
import {
    planHttpCapacityConfirmation,
    verifyHttpCapacityConfirmationPlan,
} from "../dist/http-capacity-confirmation.js";
import { reportCapacity, reportCapacityConfirmation } from "../components/http/capacity-evidence.mjs";
import { localFixtureContext, emitPairedFixture } from "./http-paired-evidence.fixture.mjs";
const MiB = 1024 ** 2;
const read = async (path) => JSON.parse(await readFile(path, "utf8"));
const put = async (path, value) => writeFile(path, JSON.stringify(value, null, 2) + "\n");
const one = (rows, predicate) => {
    const found = rows.filter(predicate);
    assert.equal(found.length, 1);
    return found[0];
};

// The HTTP workflows/requests/raw traces below are newly executed on localhost.
// JVM, Docker, procfs, resource and controller monotonic observations are explicitly
// synthetic protocol fixtures. Passing this test is not HugeGraph performance,
// genuine independent-environment confirmation, or engineering acceptance.
async function positiveProtocol(t, singleObservation = false) {
    let completed = false;
    const evidenceDirectory = process.env.HGBENCH_CAPACITY_POSITIVE_EVIDENCE
        ? resolve(process.env.HGBENCH_CAPACITY_POSITIVE_EVIDENCE)
        : null;
    if (evidenceDirectory) await mkdir(evidenceDirectory);
    const context = await localFixtureContext(t, {
        preserveDirectory: () => !completed || Boolean(evidenceDirectory),
        ...(evidenceDirectory ? { temporaryDirectory: evidenceDirectory } : {}),
    });
    const { root, shared } = context;
    t.diagnostic(`Actual localhost HTTP + synthetic lifecycle/resources; failed-attempt directory: ${root}`);
    const settings = {
        ...context.settings,
        purpose: "calibration",
        mode: "forward",
        budget: { ...context.settings.budget, wallSeconds: 2467 },
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
            memoryBytes: 1024 * MiB,
            pids: 128,
            image: settings.image,
            python: "/usr/bin/python3",
            wallSeconds: 2196,
            shutdownSeconds: 5,
            maxLogBytes: MiB,
            tmpfsBytes: 16 * MiB,
        },
        collector: { durationMs: 2076000, intervalMs: 1000, maxSamples: 2077, maxOutputBytes: 32 * MiB },
        supervisor: {
            commandMs: 45000,
            pollMs: 1000,
            maxCommandBytes: 2 * MiB,
            minFreeBytes: 0,
            maxWorkBytes: 8 * 1024 ** 3,
        },
    };
    for (const [name, value] of Object.entries({
        build: shared.build,
        environment: shared.environment,
        settings,
        runtime,
    }))
        await put(join(root, `${name}.json`), value);
    await put(join(root, "project.json"), {
        schemaVersion: 1,
        kind: "http-benchmark-project",
        id: "capacity-positive-protocol",
        label: "Actual localhost HTTP with synthetic lifecycle/resources",
        description: "Explicit protocol integration fixture; no HugeGraph performance or acceptance claim",
        comparison: "aa",
        aaMode: "forward",
        sources: Object.fromEntries(
            ["build", "settings", "environment", "runtime"].map((role) => [role, `${role}.json`]),
        ),
    });
    const policy = await read(new URL("../examples/http-capacity-policy.json", import.meta.url));
    Object.assign(policy, {
        id: "positive-protocol",
        rates: [200],
        exploration: { warmupMs: 0, durationMs: 1000, pairs: 2 },
        confirmation: { ...policy.confirmation, warmupMs: 0, durationMs: 1000 },
        http: { ...policy.http, responseEncoding: "identity", maxInFlight: 4, requestTimeoutMs: 500, drainMs: 500 },
    });
    if (singleObservation) {
        policy.schemaVersion = 2;
        policy.exploration = { mode: "single-run", runs: 1, warmupMs: 0, durationMs: 1000 };
    }
    const explorationArms = singleObservation ? 1 : 4;
    await put(join(root, "policy.json"), policy);
    const explorationDirectory = join(root, "exploration");
    await planHttpCapacity(join(root, "project.json"), join(root, "policy.json"), { out: explorationDirectory });
    await verifyHttpCapacityPlan(explorationDirectory);
    const exploration = await read(join(explorationDirectory, "capacity-plan.json"));
    assert.equal(exploration.points.length, 11);
    assert.equal(exploration.expectedArms, 11 * explorationArms);
    if (singleObservation) {
        assert.equal(exploration.schemaVersion, 2);
        assert.equal(exploration.budgetMode, "total");
    }
    const point = one(exploration.points, (p) => p.caseId === "scalar-add");
    const runtimeSettings = {
        sourceRoot: runtime.sourceRoot,
        nodeHome: runtime.nodeHome,
        driver: runtime.driver,
        collector: runtime.collector,
    };
    const explorationId = randomBytes(16).toString("hex"),
        confirmationId = randomBytes(16).toString("hex");
    assert.notEqual(explorationId, confirmationId);
    const first = await emitPairedFixture({
        root,
        planDirectory: join(explorationDirectory, point.comparisonDirectory),
        settings,
        runtimeSettings,
        executionDirectory: join(root, "exploration-execution"),
        executionId: explorationId,
        timeBaseNs: 0,
    });
    await put(join(root, "exploration-set.json"), {
        schemaVersion: 1,
        kind: "http-capacity-execution-set",
        planDigest: exploration.planDigest,
        points: [{ id: point.id, executionDirectory: "exploration-execution" }],
    });
    const reportDirectory = join(root, "exploration-report");
    await reportCapacity({
        planDirectory: explorationDirectory,
        inputFile: join(root, "exploration-set.json"),
        outputDirectory: reportDirectory,
        timeoutMs: 45000,
        maxBytes: 16 * MiB,
    });
    const source = await read(join(reportDirectory, "report.json"));
    assert.equal(source.plannedPoints, 11);
    assert.equal(source.validPoints, 1, JSON.stringify(source.points.find((p) => p.id === point.id)));
    assert.equal(source.expectedArms, 11 * explorationArms);
    if (singleObservation) assert.equal(source.algorithm, "capacity-curves-v2-single-run");
    assert.equal(source.points.filter((p) => !p.measurementValid).length, 10);
    const originalPoint = one(source.points, (p) => p.id === point.id);
    assert.equal(originalPoint.arms.length, explorationArms);
    if (singleObservation) {
        assert.equal(first.plan.schemaVersion, 3);
        assert.equal(first.plan.comparison, "single-observation");
        assert.deepEqual(Object.keys(first.plan.variants), ["O"]);
        assert.equal(originalPoint.arms[0].arm, "O");
        assert.equal(originalPoint.throughput.median, originalPoint.arms[0].correctCompletedInWindowPerSecond);
    }
    assert(originalPoint.arms.every((a) => a.measurementValid && a.counts.correct >= 100));
    assert.equal(originalPoint.latency.p99.status, "unknown");
    assert.equal(source.curves.find((c) => c.caseId === "scalar-add").highestObserved.status, "unconfirmed");
    assert.equal(source.performanceAccepted, false);
    const requestFile = join(root, "confirmation-request.json"),
        confirmationDirectory = join(root, "confirmation");
    await put(requestFile, {
        schemaVersion: singleObservation ? 2 : 1,
        kind: "http-capacity-confirmation-request",
        id: "capacity-confirm-positive-protocol",
        exploration: { plan: "exploration", executionSet: "exploration-set.json", report: "exploration-report" },
        project: "project.json",
        sourceReview: { maxMilliseconds: 45000, maxBytes: 16 * MiB },
        budgets: {
            point: {
                ...policy.pointBudgets,
                maxTotalMilliseconds: 8400000,
                maxTotalEvidenceBytes: 8 * 256 * MiB + 64 * MiB,
            },
            controller: policy.controllerBudgets,
            study: policy.studyBudgets,
        },
    });
    await planHttpCapacityConfirmation(requestFile, { out: confirmationDirectory });
    await verifyHttpCapacityConfirmationPlan(confirmationDirectory);
    const confirmation = await read(join(confirmationDirectory, "confirmation-plan.json"));
    assert.equal(confirmation.selection.selections.length, 11);
    if (singleObservation) {
        assert.equal(confirmation.schemaVersion, 2);
        assert.equal(confirmation.budgetMode, "total");
    }
    assert.equal(confirmation.points.length, 1);
    const selected = confirmation.points[0];
    assert.equal(selected.caseId, "scalar-add");
    assert.equal(selected.expectedArms, 8);
    assert.equal(selected.sourcePoints[0].pointId, point.id);
    const second = await emitPairedFixture({
        root,
        planDirectory: join(confirmationDirectory, selected.comparisonDirectory),
        settings,
        runtimeSettings,
        executionDirectory: join(root, "confirmation-execution"),
        executionId: confirmationId,
        timeBaseNs: 100000000000,
    });
    const beforeIds = first.plan.blocks.flatMap((b) => b.arms.map((a) => a.runId));
    const afterIds = second.plan.blocks.flatMap((b) => b.arms.map((a) => a.runId));
    assert.equal(beforeIds.length, explorationArms);
    assert.equal(afterIds.length, 8);
    if (singleObservation) {
        assert.equal(second.plan.schemaVersion, 3);
        assert.equal(second.plan.comparison, "aa");
        assert.deepEqual(Object.keys(second.plan.variants), ["A", "B"]);
    }
    assert.equal(new Set([...beforeIds, ...afterIds]).size, explorationArms + 8);
    await put(join(root, "confirmation-set.json"), {
        schemaVersion: 1,
        kind: "http-capacity-execution-set",
        planDigest: confirmation.planDigest,
        points: [{ id: selected.id, executionDirectory: "confirmation-execution" }],
    });
    const confirmedDirectory = join(root, "confirmation-report");
    await reportCapacityConfirmation({
        planDirectory: confirmationDirectory,
        inputFile: join(root, "confirmation-set.json"),
        outputDirectory: confirmedDirectory,
        timeoutMs: 60000,
        maxBytes: 64 * MiB,
    });
    const report = await read(join(confirmedDirectory, "report.json"));
    assert.equal(report.curves.length, 11);
    assert.equal(report.plannedPoints, 1);
    assert.equal(report.expectedArms, 8);
    assert.equal(report.validPoints, 1, JSON.stringify(report.points));
    assert.equal(report.points[0].independence.status, "verified");
    assert(report.points[0].arms.every((a) => a.measurementValid && a.counts.correct >= 100));
    assert.equal(report.points[0].arms.length, 8);
    assert.equal(report.points[0].latency?.p99?.status ?? report.modePoints[0].latency.p99.status, "unknown");
    const curve = one(report.curves, (c) => c.caseId === "scalar-add");
    assert(curve.confirmedHighest !== null);
    assert.equal(curve.confirmedHighest.pointId, selected.id);
    assert.equal(report.curves.filter((c) => c.confirmedHighest === null).length, 10);
    for (const key of ["engineeringAccepted", "statisticsCertified", "performanceAccepted"])
        assert.equal(report[key], false);
    const originalProof = await read(join(reportDirectory, "points", `${point.id}.json`));
    const newProof = await read(join(confirmedDirectory, "points", `${selected.id}.json`));
    const manifests = [...originalProof.verification.rows, ...newProof.verification.rows].map(
        (row) => row.rawEvidenceSha256,
    );
    assert.equal(new Set(manifests).size, explorationArms + 8, "Fresh raw proofs, not copied exploration evidence");
    if (evidenceDirectory) {
        const digest = (raw) => createHash("sha256").update(raw).digest("hex");
        const files = [];
        async function inventory(folder = "") {
            for (const name of await readdir(join(root, folder))) {
                const path = folder ? `${folder}/${name}` : name;
                const info = await lstat(join(root, path));
                assert(!info.isSymbolicLink());
                if (info.isDirectory()) await inventory(path);
                else {
                    assert(info.isFile() && info.size <= 8 * MiB, "Bounded retained fixture member");
                    const raw = await readFile(join(root, path));
                    files.push({ path, bytes: raw.length, sha256: digest(raw) });
                    assert(files.length <= 10000, "Bounded retained fixture members");
                    assert(files.reduce((n, file) => n + file.bytes, 0) <= 64 * MiB, "Bounded retained fixture bytes");
                }
            }
        }
        await inventory();
        files.sort((a, b) => a.path.localeCompare(b.path, "en"));
        const provenance = {
            schemaVersion: 1,
            kind: "http-capacity-positive-protocol-fixture",
            scope: "Actual localhost HTTP requests/raw and public planning/report IO; explicitly synthetic JVM/Docker/procfs/lifecycle/resource/monotonic observations",
            hugeGraphPerformanceEvidence: false,
            genuineIndependentEnvironmentEvidence: false,
            engineeringAccepted: false,
            statisticsCertified: false,
            performanceAccepted: false,
            fixtureDirectory: root,
            originalMeasurementsRetained: true,
            duplicatedRaw: false,
            cleanup:
                "Local HTTP server is closed by the test; this opt-in fixture directory is retained for review and may be removed after evidence review",
            policy: {
                rates: policy.rates,
                warmupMs: 0,
                durationMs: 1000,
                minimumObservations: 100,
                p99MinimumObservations: 1000,
            },
            exploration: {
                planDigest: exploration.planDigest,
                executionId: explorationId,
                plannedPoints: 11,
                expectedArms: 11 * explorationArms,
                actualArms: explorationArms,
                validPoints: 1,
                unmeasuredPoints: 10,
                arms: originalProof.verification.rows.map((row) => ({
                    runId: row.runId,
                    rawManifestSha256: row.rawEvidenceSha256,
                    scheduled: row.httpSummary.runs[0].scheduled,
                    correct: row.httpSummary.runs[0].correct,
                })),
            },
            confirmation: {
                planDigest: confirmation.planDigest,
                executionId: confirmationId,
                selections: 11,
                expectedArms: 8,
                actualArms: 8,
                validPoints: 1,
                unavailableSelections: 10,
                independence: "Synthetic same-boot sequential controller windows; fresh actual HTTP raw",
                arms: newProof.verification.rows.map((row) => ({
                    runId: row.runId,
                    rawManifestSha256: row.rawEvidenceSha256,
                    scheduled: row.httpSummary.runs[0].scheduled,
                    correct: row.httpSummary.runs[0].correct,
                })),
            },
            inventory: { members: files.length, bytes: files.reduce((n, f) => n + f.bytes, 0), files },
        };
        await put(join(evidenceDirectory, "provenance.json"), provenance);
        for (const [name, value] of Object.entries({
            "exploration-plan.json": exploration,
            "exploration-report.json": source,
            "confirmation-plan.json": confirmation,
            "confirmation-report.json": report,
        }))
            await put(join(evidenceDirectory, name), value);
    }
    t.diagnostic(
        `Protocol IO passed: frozen full eleven-case plan → ${explorationArms} new HTTP arm(s) → independently replayed candidate → 8 new HTTP arms → descriptive confirmation; synthetic runtime provenance only.`,
    );
    completed = true;
}

test(
    "public capacity exploration selects actual localhost raw and independently reports eight fresh protocol arms",
    { timeout: 120000 },
    (t) => positiveProtocol(t),
);
test(
    "single-observation capacity parent independently selects one real O and confirms eight fresh protocol arms",
    { timeout: 120000 },
    (t) => positiveProtocol(t, true),
);
