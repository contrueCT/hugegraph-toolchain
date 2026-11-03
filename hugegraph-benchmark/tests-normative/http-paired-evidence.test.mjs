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
import { mkdtemp, mkdir, readFile, writeFile, readdir, lstat, rm, cp, symlink } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve, dirname } from "node:path";
import { createHash } from "node:crypto";
import { fixture, seal } from "./http-paired-evidence.fixture.mjs";

import { summarizeCapacity } from "../components/http/capacity-evidence.mjs";
import { capacityHtml } from "../components/http/capacity-report.mjs";
import { normalizePairedEvidence, writePairedEvidence } from "../components/http/paired-evidence.mjs";

const hash = (raw) => createHash("sha256").update(raw).digest("hex");
const read = async (path) => JSON.parse(await readFile(path, "utf8"));
const put = async (path, value) => {
    await mkdir(dirname(path), { recursive: true });
    await writeFile(path, JSON.stringify(value, null, 2) + "\n");
};
const MiB = 1024 * 1024;

async function altered(f, name, callback, reseal = true) {
    const root = join(f.root, name);
    await cp(f.executionDirectory, root, { recursive: true });
    const base = `arms/${f.first.runId}`;
    await callback(root, base);
    if (reseal) await seal(root, f.plan.planDigest);
    return root;
}

test("formal control samples retain exact nanosecond order and reject unsafe or failed controls", async (t) => {
    const f = await fixture(t, { formalControls: true });
    const input = { planDirectory: f.planDirectory, executionDirectory: f.executionDirectory };
    assert.equal((await normalizePairedEvidence(input)).verification.valid, true);
    for (const [name, patch, issue] of [
        [
            "reversed",
            (v) => {
                v.first.atMonotonicNs = "10";
                v.last.atMonotonicNs = "9";
            },
            "FORMAL_CONTROLS_ORDER",
        ],
        [
            "unsafe-number",
            (v) => {
                v.first.atMonotonicNs = 2 ** 53;
            },
            "UNSAFE_MONOTONIC_TIMESTAMP",
        ],
        [
            "negative-time",
            (v) => {
                v.first.atMonotonicNs = "-1";
            },
            "UNSAFE_MONOTONIC_TIMESTAMP",
        ],
        [
            "swap-enabled",
            (v) => {
                v.maximumSwapTotalBytes = 4096;
            },
            "FORMAL_CONTROLS_FAILED",
        ],
        [
            "gc-enabled",
            (v) => {
                v.last.memoryMonitorThreshold = 0.85;
            },
            "FORMAL_CONTROLS_SAMPLE",
        ],
    ]) {
        await t.test(name, async () => {
            const executionDirectory = await altered(f, "formal-" + name, async (root, base) => {
                const path = join(root, base, "environment/formal-controls.json"),
                    value = await read(path);
                patch(value);
                await put(path, value);
            });
            const result = await normalizePairedEvidence({ planDirectory: f.planDirectory, executionDirectory });
            assert.equal(result.verification.valid, false);
            assert(
                result.verification.rows[0].issues.includes(issue),
                JSON.stringify(result.verification.rows[0].issues),
            );
        });
    }
    for (const stringValues of [true, false]) {
        await t.test("forward cross-width times " + (stringValues ? "strings" : "numbers"), async () => {
            const executionDirectory = await altered(f, "formal-forward-" + stringValues, async (root, base) => {
                const path = join(root, base, "environment/formal-controls.json"),
                    value = await read(path);
                value.first.atMonotonicNs = stringValues ? "9" : 9;
                value.last.atMonotonicNs = stringValues ? "10" : 10;
                await put(path, value);
            });
            const result = await normalizePairedEvidence({ planDirectory: f.planDirectory, executionDirectory });
            assert.equal(result.verification.valid, true, JSON.stringify(result.verification.rows[0].issues));
        });
    }
});

test("HTTP paired bridge reanalyzes real raw workflow and preserves invalid predeclared arms", async (t) => {
    const f = await fixture(t);
    const input = { planDirectory: f.planDirectory, executionDirectory: f.executionDirectory };
    const good = await normalizePairedEvidence(input);
    assert.equal(good.verification.valid, true, JSON.stringify(good.verification.rows.map((r) => r.issues)));
    assert.equal(good.normalized.blocks.length, 2);
    assert.equal(good.analysis.performanceAccepted, false);
    assert.equal(good.verification.gates.rawEvidence.status, "passed");
    assert.equal(good.verification.gates.runtimeArtifacts.status, "consistent");
    assert.match(good.verification.gates.calibration, /pending/);
    assert(good.verification.rows.every((r) => Object.keys(r.values).length === 2));
    const producedImplementation = await read(
        join(f.executionDirectory, `arms/${f.first.runId}`, "raw/implementation.json"),
    );
    for (const file of ["report-view.mjs", "build-info.mjs"]) {
        assert(producedImplementation.files.some((entry) => entry.file === file));
        for (const change of ["missing", "duplicate", "changed-sha"]) {
            await t.test(`${file} ${change} stays invalid after coherent outer resealing`, async () => {
                const executionDirectory = await altered(f, `implementation-${file}-${change}`, async (root, base) => {
                    const dir = join(root, base, "raw"),
                        path = join(dir, "implementation.json"),
                        implementation = await read(path);
                    if (change === "missing")
                        implementation.files = implementation.files.filter((entry) => entry.file !== file);
                    else if (change === "duplicate")
                        implementation.files.push(implementation.files.find((entry) => entry.file === file));
                    else implementation.files.find((entry) => entry.file === file).sha256 = "0".repeat(64);
                    await put(path, implementation);
                    const rawLockPath = join(dir, "raw.lock.json"),
                        rawLock = await read(rawLockPath),
                        body = await readFile(path),
                        entry = rawLock.files.find((entry) => entry.path === "implementation.json");
                    entry.bytes = body.length;
                    entry.sha256 = hash(body);
                    await put(rawLockPath, rawLock);
                    const receiptPath = join(root, base, "receipt.json"),
                        receipt = await read(receiptPath);
                    receipt.rawLockSha256 = hash(await readFile(rawLockPath));
                    await put(receiptPath, receipt);
                });
                const result = await normalizePairedEvidence({ planDirectory: f.planDirectory, executionDirectory });
                assert.equal(result.verification.valid, false);
                assert(
                    result.verification.rows[0].issues.includes(
                        change === "changed-sha" ? "HTTP_IMPLEMENTATION_DIFFER" : "HTTP_IMPLEMENTATION_INVENTORY",
                    ),
                    JSON.stringify(result.verification.rows[0].issues),
                );
            });
        }
    }
    await t.test("HTTP summaries are explicit and leave default output bytes unchanged", async () => {
        assert.equal(
            JSON.stringify(await normalizePairedEvidence({ ...input, includeHttpSummaries: false })),
            JSON.stringify(good),
        );
        assert(good.verification.rows.every((row) => !Object.hasOwn(row, "httpSummary")));
        const expanded = await normalizePairedEvidence({ ...input, includeHttpSummaries: true });
        for (const row of expanded.verification.rows) {
            assert.equal(row.httpSummary.rawManifestSha256, row.rawEvidenceSha256);
            assert.equal(row.httpSummary.runs.length, 1);
            assert.equal(
                row.httpSummary.runs[0].correctCompletedInWindowPerSecond,
                row.values[f.plan.metricBindings.find((binding) => binding.kind === "goodput").id].value,
            );
            delete row.httpSummary;
        }
        assert.equal(JSON.stringify(expanded), JSON.stringify(good));
        await assert.rejects(
            normalizePairedEvidence({ ...input, includeHttpSummaries: "true" }),
            /HTTP_SUMMARY_OPTION/,
        );
    });
    await t.test("independent rebuild and locked new output", async () => {
        const rebuilt = await normalizePairedEvidence(input);
        assert.deepEqual(rebuilt, good);
        const outputDirectory = join(f.root, "analysis");
        assert.equal((await writePairedEvidence({ ...input, outputDirectory })).valid, true);
        assert.deepEqual(await read(join(outputDirectory, "observations.json")), good.normalized);
        assert.match(await readFile(join(outputDirectory, "report.html"), "utf8"), /<!doctype html>/i);
        const implementation = await read(join(outputDirectory, "implementation.json"));
        assert(implementation.files.some((f) => f.path === "components/http/resource-evidence.mjs"));
        assert(implementation.files.some((f) => f.path === "components/http/paired-report.mjs"));
        await assert.rejects(writePairedEvidence({ ...input, outputDirectory }), /EEXIST/);
    });
    await t.test("correct answers with driver pressure are invalid without a correctness failure", async () => {
        const executionDirectory = await altered(f, "quality-only-failure", async (root, base) => {
            const dir = join(root, base, "raw");
            const runFile = join(dir, "runs/r0001.json");
            const run = await read(runFile);
            run.phases.find((phase) => phase.name === "measurement").eventLoopUtilization = 0.9;
            await put(runFile, run);
            const lock = await read(join(dir, "raw.lock.json"));
            for (const file of lock.files) {
                const raw = await readFile(join(dir, file.path));
                file.bytes = raw.length;
                file.sha256 = hash(raw);
            }
            await put(join(dir, "raw.lock.json"), lock);
            const receipt = await read(join(root, base, "receipt.json"));
            receipt.rawLockSha256 = hash(await readFile(join(dir, "raw.lock.json")));
            await put(join(root, base, "receipt.json"), receipt);
        });
        const result = await normalizePairedEvidence({ planDirectory: f.planDirectory, executionDirectory });
        const row = result.verification.rows[0];
        assert.equal(row.status, "invalid");
        assert(row.issues.includes("HTTP_QUALITY_FAILED"));
        assert(!row.issues.includes("HTTP_CORRECTNESS_FAILED"));
        assert.equal(result.verification.valid, false);
        assert(result.analysis.cells.every((cell) => cell.issues.some((issue) => issue.startsWith("invalid:"))));
    });
    for (const [name, patch, expected] of [
        [
            "wrong-order",
            async (root, base) => {
                const p = join(root, base, "receipt.json"),
                    v = await read(p);
                v.armStartedMonotonicNs = "1";
                await put(p, v);
            },
            "ARM_TIME_ENVELOPE",
        ],
        [
            "wrong-mode",
            async (root, base) => {
                const p = join(root, base, "environment/work/mode-post/result.json"),
                    v = await read(p);
                v.observedMode = v.observedMode === "forward" ? "embedded" : "forward";
                await put(p, v);
            },
            "MODE_PROBE_IDENTITY",
        ],
        [
            "unsafe-nanoseconds",
            async (root, base) => {
                const p = join(root, base, "environment/work/mode-pre/result.json"),
                    v = await read(p);
                v.finishedMonotonicNs = 2 ** 53;
                await put(p, v);
            },
            "UNSAFE_MONOTONIC_TIMESTAMP",
        ],
        [
            "wrong-window",
            async (root, base) => {
                const p = join(root, base, "receipt.json"),
                    v = await read(p);
                v.workflowStartedMonotonicNs = String(BigInt(v.armStartedMonotonicNs) + 1000000000n);
                await put(p, v);
            },
            "MODE_PROBE_INSIDE_WORKFLOW",
        ],
        [
            "missing-arm",
            async (root, base) => {
                const p = join(root, "execution.json"),
                    v = await read(p);
                v.receipts = v.receipts.filter((r) => r.runId !== f.first.runId);
                await put(p, v);
            },
            "ARM_MISSING",
        ],
        [
            "forced-stop",
            async (root, base) => {
                const p = join(root, base, "environment/work/service-result.json"),
                    v = await read(p);
                v.outcomes.at(-1).termination.killSent = true;
                await put(p, v);
            },
            "ENVIRONMENT_STOP_FAILED",
        ],
        [
            "stack-byte-reseal",
            async (root, base) => {
                const dir = join(root, base, "environment/work/mode-post"),
                    p = join(dir, "thread-dump.stdout.txt");
                const raw = Buffer.from((await readFile(p, "utf8")).replace("GremlinAPI.post", "GremlinAPI.other"));
                await writeFile(p, raw);
                const rec = await read(join(dir, "thread-dump.json"));
                rec.stdout = { bytes: raw.length, sha256: hash(raw) };
                await put(join(dir, "thread-dump.json"), rec);
            },
            "MODE_STACK_MISMATCH",
        ],
        [
            "raw-lock-ref",
            async (root, base) => {
                const p = join(root, base, "receipt.json"),
                    v = await read(p);
                v.rawLockSha256 = "f".repeat(64);
                await put(p, v);
            },
            "ARM_RAW_IDENTITY_OR_REPLAY",
        ],
        [
            "different-build",
            async (root, base) => {
                const dir = join(root, base, "environment"),
                    p = join(dir, "plan.json"),
                    v = await read(p);
                v.sources.distribution.files.find((f) => f.type === "file").sha256 = "f".repeat(64);
                await put(p, v);
                const lock = await read(join(dir, "plan.lock.json"));
                lock.planSha256 = hash(await readFile(p));
                await put(join(dir, "plan.lock.json"), lock);
            },
            "ENVIRONMENT_BUILD_DIFFER",
        ],
        [
            "different-profile",
            async (root, base) => {
                const dir = join(root, base, "raw"),
                    profile = await read(join(dir, "profile.json"));
                profile.name = "different-resealed-profile";
                await put(join(dir, "profile.json"), profile);
                const request = await read(join(dir, "requests/r0001.json"));
                request.profile = profile;
                await put(join(dir, "requests/r0001.json"), request);
                const lock = await read(join(dir, "raw.lock.json"));
                for (const f of lock.files) {
                    const raw = await readFile(join(dir, f.path));
                    f.bytes = raw.length;
                    f.sha256 = hash(raw);
                }
                await put(join(dir, "raw.lock.json"), lock);
                const receipt = await read(join(root, base, "receipt.json"));
                receipt.rawLockSha256 = hash(await readFile(join(dir, "raw.lock.json")));
                await put(join(root, base, "receipt.json"), receipt);
            },
            "HTTP_PROFILE_DIFFER",
        ],
        [
            "runtime-content",
            async (root, base) => {
                const p = join(root, base, "controller/runtime.json"),
                    v = await read(p);
                v.planDigest = "f".repeat(64);
                await put(p, v);
            },
            "CONTROLLER_RUNTIME_IDENTITY",
        ],
        [
            "controller-failure-after-valid-raw",
            async (root, base) => {
                const p = join(root, base, "receipt.json"),
                    value = await read(p);
                value.failure = "driver-OOM";
                await put(p, value);
            },
            "ARM_CONTROLLER_FAILURE",
        ],
        [
            "controller-cleanup-failure-after-valid-raw",
            async (root, base) => {
                const p = join(root, base, "receipt.json"),
                    value = await read(p);
                value.cleanupErrors = ["driver-container-still-running"];
                await put(p, value);
            },
            "ARM_CONTROLLER_FAILURE",
        ],
        [
            "mode-command-combined-budget",
            async (root, base) => {
                const dir = join(root, base, "environment/work/mode-pre");
                const request = await read(join(dir, "request.json"));
                request.budgets.max_command_bytes = 4096;
                await put(join(dir, "request.json"), request);
                for (const stem of ["attach", "thread-dump"]) {
                    const path = join(dir, `${stem}.json`),
                        command = await read(path);
                    command.maxBytes = 4096;
                    if (stem === "thread-dump") {
                        const raw = Buffer.alloc(4096, 120);
                        await writeFile(join(dir, `${stem}.stderr.txt`), raw);
                        command.stderr = { bytes: raw.length, sha256: hash(raw), redacted: false };
                    }
                    await put(path, command);
                }
            },
            "MODE_COMMAND_COMBINED_BUDGET",
        ],
        [
            "resource-origin-conflict-is-not-live",
            async (root, base) => {
                const dir = join(root, base, "collector"),
                    p = join(dir, "summary.json"),
                    v = await read(p);
                v.sourceOrigin = "fixture";
                await put(p, v);
                const lock = await read(join(dir, "manifest.json"));
                const f = lock.files.find((f) => f.path === "summary.json"),
                    raw = await readFile(p);
                f.bytes = raw.length;
                f.sha256 = hash(raw);
                await put(join(dir, "manifest.json"), lock);
            },
            "COLLECTOR_CLOCK_KIND",
        ],
        [
            "raw-trace-tamper",
            async (root, base) => {
                const p = join(root, base, "raw/traces/r0001.ndjson");
                await writeFile(p, "{}\n");
            },
            "RAW_OR_RUNTIME_REVERIFICATION_FAILED",
        ],
    ])
        await t.test(name, async () => {
            const executionDirectory = await altered(f, name, patch);
            const result = await normalizePairedEvidence({ planDirectory: f.planDirectory, executionDirectory });
            assert.equal(result.verification.valid, false);
            assert(
                result.verification.rows[0].issues.includes(expected),
                JSON.stringify(result.verification.rows[0].issues),
            );
            assert.equal(result.normalized.blocks.length, good.normalized.blocks.length);
            assert.equal(result.normalized.policy.metrics.length, good.normalized.policy.metrics.length);
            assert(result.analysis.cells.every((cell) => cell.issues.length > 0));
        });
    await t.test("controller terminal failures cannot promote complete arm evidence", async () => {
        const executionDirectory = await altered(f, "terminal-failure", async (root) => {
            const p = join(root, "terminal.json"),
                value = await read(p);
            value.status = "failed";
            value.failure = "source-changed-after-final-arm";
            value.runtimeReverified = false;
            await put(p, value);
        });
        const result = await normalizePairedEvidence({ planDirectory: f.planDirectory, executionDirectory });
        assert.equal(result.verification.valid, false);
        assert(result.verification.globalIssues.includes("EXECUTION_TERMINAL_FAILED"));
        assert(result.verification.rows.every((r) => r.status === "invalid"));
        assert(result.analysis.cells.every((c) => c.issues.length > 0));
    });
    await t.test("overlapping sequential arms invalidate the whole family", async () => {
        const executionDirectory = await altered(f, "overlap", async (root) => {
            const arm = f.plan.blocks[0].arms[1],
                base = join(root, `arms/${arm.runId}`);
            const p = join(base, "receipt.json"),
                v = await read(p);
            v.armStartedMonotonicNs = "9000000000";
            await put(p, v);
        });
        const result = await normalizePairedEvidence({ planDirectory: f.planDirectory, executionDirectory });
        assert(result.verification.globalIssues.includes("ARM_ORDER_OR_OVERLAP"));
        assert(result.verification.rows.every((r) => r.status === "invalid"));
        assert(result.analysis.cells.every((c) => c.issues.length > 0));
    });
    await t.test("unsealed bytes and unlisted files are rejected before normalization", async () => {
        const executionDirectory = await altered(
            f,
            "unsealed",
            async (root, base) => writeFile(join(root, base, "raw/traces/r0001.ndjson"), "{}\n"),
            false,
        );
        await assert.rejects(
            normalizePairedEvidence({ planDirectory: f.planDirectory, executionDirectory }),
            /EVIDENCE_FILE_SIZE|EXECUTION_FILE_DIGEST/,
        );
    });
    await t.test("symlinks rejected without following external payload", async () => {
        const executionDirectory = await altered(
            f,
            "symlink",
            async (root) => symlink("/etc/passwd", join(root, "unexpected")),
            false,
        );
        await assert.rejects(
            normalizePairedEvidence({ planDirectory: f.planDirectory, executionDirectory }),
            /SYMLINK/,
        );
    });
});

test("capacity localhost raw projects a complete point without claiming a verified parent or HugeGraph result", async (t) => {
    const f = await fixture(t, {
        comparison: "aa",
        durationMs: 1000,
        ratePerSecond: 100,
        minimumObservations: 100,
        maxInFlight: 4,
    });
    const result = await normalizePairedEvidence({
        planDirectory: f.planDirectory,
        executionDirectory: f.executionDirectory,
        includeHttpSummaries: true,
    });
    assert.equal(result.verification.valid, true, JSON.stringify(result.verification.rows.map((r) => r.issues)));
    const caseIds = ["scalar-add", ...Array.from({ length: 10 }, (_, i) => `explicit-projection-fixture-${i}`)];
    // The parent here is explicitly a projection fixture. It is not accepted by the public parent-plan verifier.
    const parent = {
        schemaVersion: 1,
        kind: "http-capacity-plan",
        phase: "exploration",
        planDigest: "a".repeat(64),
        policySha256: "b".repeat(64),
        suite: { caseIds },
        modes: ["forward"],
        expectedArms: 44,
        policy: {
            analysis: { latency: { minimumObservations: 100 }, goodput: { minimumObservations: 100 } },
            tailPolicy: { p99MinimumObservations: 1000 },
            slo: null,
        },
        points: caseIds.map((caseId, index) => ({
            id: `fixture-${index}`,
            index,
            caseId,
            mode: "forward",
            ratePerSecond: 100,
            expectedArms: 4,
            comparisonPlanDigest: f.plan.planDigest,
        })),
    };
    const summary = summarizeCapacity(parent, [
        { id: "fixture-0", status: "reverified", issues: [], executionDirectory: "fixture-only", result },
    ]);
    assert.equal(summary.plannedPoints, 11);
    assert.equal(summary.expectedArms, 44);
    assert.equal(summary.validPoints, 1);
    const point = summary.points[0];
    assert.equal(point.measurementValid, true, JSON.stringify(point.arms));
    assert.equal(point.throughput.median, 100);
    assert.equal(point.throughput.minimum, 100);
    assert.equal(point.throughput.maximum, 100);
    assert(point.arms.every((arm) => arm.counts.correctCompletedInWindow === 100));
    assert.equal(point.latency.p99.status, "unknown");
    assert(point.arms.every((arm) => arm.latency.p99.minimumObservations === 1000));
    assert.equal(summary.curves[0].highestObserved.status, "unconfirmed");
    assert.equal(summary.performanceAccepted, false);
    const html = capacityHtml(summary);
    assert.match(html, /20/);
    assert.match(html, /report.json/);
    assert.match(html, /1000/);
    if (process.env.HGBENCH_CAPACITY_FIXTURE_PREVIEW === "1") {
        const output = resolve(".hgbench/http-capacity-report-preview");
        await mkdir(output);
        await put(join(output, "report.json"), summary);
        await writeFile(join(output, "report.html"), html);
        await writeFile(
            join(output, "README.md"),
            "# Localhost protocol fixture preview\n\nActual localhost HTTP records with explicitly synthetic lifecycle/resource evidence and a synthetic parent projection. This is not a HugeGraph capacity score, public parent-plan verification, or performance acceptance. Raw temporary fixture files are removed by the test.\n",
        );
    }
});
