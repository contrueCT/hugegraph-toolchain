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
import { readFile, writeFile, readdir, symlink, access } from "node:fs/promises";
import { join, resolve, dirname } from "node:path";
import { createHash } from "node:crypto";
import { execFile, spawn } from "node:child_process";
import { promisify } from "node:util";
import {
    capacityArm,
    markCapacityAssociations,
    reportCapacityConfirmation,
    runCapacityWorker,
} from "../components/http/capacity-evidence.mjs";
import {
    markConfirmationIndependence,
    summarizeCapacityConfirmation,
} from "../components/http/capacity-confirmation-report.mjs";
import { capacityHtml } from "../components/http/capacity-report.mjs";
import { planHttpCapacityConfirmation } from "../dist/http-capacity-confirmation.js";
import { missingExploration } from "./http-capacity-confirmation.fixture.mjs";

const MiB = 1024 ** 2,
    sha = (value) => createHash("sha256").update(value).digest("hex"),
    command = promisify(execFile);
// Arithmetic/association fixtures only. These fabricated rows are never passed to the public report as verified evidence.
function protocol() {
    const cases = Array.from({ length: 11 }, (_, i) => "case-" + i),
        modes = ["forward", "embedded"];
    const plan = {
        kind: "http-capacity-confirmation-plan",
        phase: "confirmation",
        planDigest: "a".repeat(64),
        exploration: { planDigest: "b".repeat(64) },
        suite: { caseIds: cases },
        modes,
        policy: {
            analysis: { latency: { minimumObservations: 100 }, goodput: { minimumObservations: 100 } },
            tailPolicy: { p99MinimumObservations: 1000 },
            slo: null,
        },
        selection: {
            selections: cases.flatMap((caseId) =>
                modes.map((mode) => ({
                    caseId,
                    mode,
                    status: caseId === cases[0] ? "provisional-candidate" : "unknown",
                    candidatePointId: caseId === cases[0] ? "old-candidate" : null,
                    neighborhood: [],
                    confirmed: false,
                })),
            ),
        },
        points: [
            {
                id: "new-forward-aa",
                index: 0,
                caseId: cases[0],
                ratePerSecond: 10,
                comparison: "aa",
                modes: { A: "forward", B: "forward" },
                expectedArms: 8,
                sourcePoints: [],
            },
            {
                id: "new-ab",
                index: 1,
                caseId: cases[0],
                ratePerSecond: 20,
                comparison: "forward-embedded",
                modes: { A: "forward", B: "embedded" },
                expectedArms: 8,
                sourcePoints: [],
            },
            {
                id: "new-embedded-aa",
                index: 2,
                caseId: cases[0],
                ratePerSecond: 40,
                comparison: "aa",
                modes: { A: "embedded", B: "embedded" },
                expectedArms: 8,
                sourcePoints: [],
            },
        ],
    };
    const records = plan.points.map((point, index) => ({
        id: point.id,
        actualDirectory: "/synthetic/new/" + index,
        executionDirectory: "new/" + index,
        executionId: String(index + 1).repeat(32),
        runIds: Array.from({ length: 8 }, (_, i) => point.id + "-run-" + i),
        issues: [],
        status: "reverified",
        window: { boot: "same-boot", start: String(200 + index * 200), end: String(300 + index * 200) },
        result: {
            verification: {
                valid: true,
                globalIssues: [],
                rows: Array.from({ length: 8 }, (_, i) => {
                    const arm = i % 2 ? "B" : "A",
                        mode = point.modes[arm],
                        value = index === 0 ? (i === 7 ? 9 : 1) : index === 1 ? (mode === "forward" ? 12 : 15) : 10,
                        correct = value * 120;
                    return {
                        runId: point.id + "-run-" + i,
                        arm,
                        blockId: "confirm-0" + (1 + (i >> 1)),
                        expectedMode: mode,
                        status: "ok",
                        issues: [],
                        rawVerified: true,
                        rawEvidenceSha256: sha(point.id + i),
                        runtimeArtifactsConsistent: true,
                        driverVerified: true,
                        resourcesVerified: true,
                        httpSummary: {
                            rawManifestSha256: sha(point.id + i),
                            gates: { sampleAccounting: "passed", correctness: "passed" },
                            runs: [
                                {
                                    caseId: point.caseId,
                                    model: "open",
                                    repeat: 0,
                                    ratePerSecond: point.ratePerSecond,
                                    correctCompletedInWindowPerSecond: value,
                                    durationMs: 120000,
                                    elapsedWithDrainMs: 120001,
                                    scheduled: point.ratePerSecond * 120,
                                    sent: correct,
                                    completed: correct,
                                    correct,
                                    failed: 0,
                                    dropped: 0,
                                    late: 0,
                                    counts: { correct },
                                    issues: [],
                                    offeredPerSecond: point.ratePerSecond,
                                    admittedPerSecond: value,
                                    correctEndToEndMs: { count: 1000, p50: 1, p95: 2, p99: 3 },
                                    driver: { fixture: true },
                                },
                            ],
                        },
                    };
                }),
            },
        },
    }));
    const source = {
        schemaVersion: 1,
        kind: "http-capacity-source-independence",
        planDigest: plan.exploration.planDigest,
        executions: [
            {
                pointId: "old",
                actualDirectory: "/synthetic/old",
                executionId: "f".repeat(32),
                runIds: ["old-run"],
                rawManifestSha256s: ["e".repeat(64)],
                window: { boot: "same-boot", start: "1", end: "200" },
            },
        ],
        unavailable: [],
    };
    return { plan, records, source };
}
const summarize = (f) => {
    markCapacityAssociations(f.records);
    markConfirmationIndependence(f.records, f.source);
    return summarizeCapacityConfirmation(f.plan, f.records, f.source, capacityArm);
};

test("confirmation uses all eight AA arms and four actual AB arms per mode; adjacent new points may win", () => {
    const f = protocol(),
        result = summarize(f),
        forward = result.curves[0],
        embedded = result.curves[1];
    assert.equal(result.points.length, 3);
    assert.equal(result.modePoints.length, 4);
    assert.equal(result.expectedArms, 24);
    assert.equal(result.curves.length, 22);
    assert.equal(result.selection.selections.length, 22);
    assert.equal(result.modePoints[0].expectedArms, 8);
    assert.equal(result.modePoints[0].throughput.median, 1);
    const ab = result.modePoints.filter((row) => row.evidencePointId === "new-ab");
    assert.deepEqual(
        ab.map((row) => [row.mode, row.expectedArms, row.throughput.median]),
        [
            ["forward", 4, 12],
            ["embedded", 4, 15],
        ],
    );
    assert.equal(forward.confirmedHighest.correctRequestsPerSecond, 12);
    assert.equal(embedded.confirmedHighest.correctRequestsPerSecond, 15);
    assert.equal(embedded.confirmedHighest.pointId, "new-ab");
    assert(result.curves.slice(2).every((row) => row.confirmedHighest === null));
    assert.equal(result.performanceAccepted, false);
    assert.equal(result.engineeringAccepted, false);
    const html = capacityHtml(result);
    assert(html.includes("容量独立确认"));
    assert(html.includes("points/new-ab.json"));
    assert(!html.includes("points/new-ab-forward.json"));
});
test("missing arms, incorrect actual mode and original quality failures cannot cherry-pick a passing mode", () => {
    for (const mutate of [
        (row) => row.result.verification.rows.pop(),
        (row) => row.result.verification.rows[0].issues.push("HTTP_CORRECTNESS_FAILED"),
        (row) => (row.result.verification.rows[0].expectedMode = "embedded"),
        (row) => row.result.verification.globalIssues.push("EXECUTION_FAILED"),
        (row) => (row.result.verification.rows[0].runId = row.result.verification.rows[1].runId),
    ]) {
        const f = protocol();
        f.records = [f.records[1]];
        f.plan.points = [f.plan.points[1]];
        mutate(f.records[0]);
        const result = summarize(f);
        assert.equal(result.validPoints, 0);
        assert.equal(result.points[0].arms.length, 8);
        assert(result.curves.every((row) => row.confirmedHighest === null));
        assert.equal(result.modePoints.length, 2);
        assert(result.modePoints.every((row) => row.throughput.median === null));
    }
});
test("confirmation refuses original run, raw, physical execution and execution-id reuse", () => {
    for (const mutate of [
        (f) => (f.records[0].actualDirectory = f.source.executions[0].actualDirectory),
        (f) => (f.records[0].executionId = f.source.executions[0].executionId),
        (f) => (f.records[0].runIds[0] = "old-run"),
        (f) => (f.records[0].result.verification.rows[0].rawEvidenceSha256 = "e".repeat(64)),
    ]) {
        const f = protocol();
        mutate(f);
        const result = summarize(f);
        assert.equal(result.points[0].independence.status, "invalid");
        assert.equal(result.points[0].confirmationEligible, false);
    }
});
test("same-boot causal boundaries allow equality but reject overlap/reversed order and preserve cross-boot unknown", () => {
    assert.equal(summarize(protocol()).points[0].independence.status, "verified");
    for (const [window, expected, code] of [
        [{ boot: "same-boot", start: "1", end: "250" }, "invalid", "CONFIRMATION_EXPLORATION_WINDOWS_OVERLAP"],
        [{ boot: "same-boot", start: "400", end: "500" }, "invalid", "CONFIRMATION_PRECEDES_EXPLORATION"],
        [{ boot: "other-boot", start: "1", end: "100" }, "unknown", "CONFIRMATION_SOURCE_CLOCK_DOMAIN_UNKNOWN"],
    ]) {
        const f = protocol();
        f.source.executions[0].window = window;
        const result = summarize(f);
        assert.equal(result.points[0].independence.status, expected);
        assert(result.points[0].issues.includes(code));
        assert.equal(result.points[0].confirmationEligible, false);
    }
    const f = protocol();
    f.source.unavailable.push({ pointId: "unfinished", reason: "terminal missing" });
    const result = summarize(f);
    assert(result.curves.every((row) => row.confirmedHighest === null));
});
test("P99 never pools samples and zero goodput remains visible without qualification", () => {
    const f = protocol();
    f.records[0].result.verification.rows.forEach((row) => (row.httpSummary.runs[0].correctEndToEndMs.count = 999));
    let result = summarize(f);
    assert.equal(result.modePoints[0].latency.p99.status, "unknown");
    assert.equal(result.modePoints[0].latency.p50.value, 1);
    const zero = protocol();
    zero.records[0].result.verification.rows.forEach((row) => {
        const run = row.httpSummary.runs[0];
        run.correctCompletedInWindowPerSecond = 0;
        run.correct = 0;
        run.counts.correct = 0;
    });
    result = summarize(zero);
    assert.equal(result.modePoints[0].diagnosticObservedThroughput.median, 0);
    assert.equal(result.modePoints[0].confirmationEligible, false);
});
test("all missing and zero selected points preserve all eleven by mode rows without inventing confirmed values", () => {
    const f = protocol();
    f.records = [];
    let result = summarize(f);
    assert(result.points.every((row) => row.arms.length === 8));
    assert(result.modePoints.every((row) => row.arms.length === row.expectedArms));
    assert(result.curves.every((row) => row.confirmedHighest === null));
    f.plan.points = [];
    result = summarize(f);
    assert.equal(result.plannedPoints, 0);
    assert.equal(result.evidenceValid, false);
    assert.equal(result.curves.length, 22);
    assert.equal(result.expectedArms, 0);
});

test(
    "real independently replayed zero-candidate confirmation reports all unavailable rows and budgets temporary source work",
    { timeout: 120000 },
    async (t) => {
        const f = await missingExploration(t),
            plan = join(f.root, "confirmation");
        await planHttpCapacityConfirmation(f.requestFile, { out: plan });
        const saved = JSON.parse(await readFile(join(plan, "confirmation-plan.json")));
        const inputFile = join(f.root, "confirmation-set.json");
        await writeFile(
            inputFile,
            JSON.stringify({
                schemaVersion: 1,
                kind: "http-capacity-execution-set",
                planDigest: saved.planDigest,
                points: [],
            }) + "\n",
        );
        const out = join(f.root, "confirmation-report"),
            options = { planDirectory: plan, inputFile, outputDirectory: out, timeoutMs: 60000, maxBytes: 64 * MiB };
        const reply = await reportCapacityConfirmation(options),
            summary = JSON.parse(await readFile(join(out, "report.json")));
        assert.equal(reply.points, 0);
        assert.equal(reply.validPoints, 0);
        assert.equal(summary.curves.length, 11);
        assert(summary.curves.every((row) => row.confirmedHighest === null));
        assert.equal(summary.sourceIndependence.unavailable.length, 2);
        assert.equal(summary.workBudget.sourceReviewReservedBytes, 24 * MiB);
        assert.equal(summary.performanceAccepted, false);
        await assert.rejects(
            reportCapacityConfirmation({ ...options, outputDirectory: join(f.root, "too-small"), maxBytes: 24 * MiB }),
            /SOURCE_BYTE_RESERVE/,
        );
        await assert.rejects(access(join(f.root, "too-small")), { code: "ENOENT" });
        const alias = join(f.root, "original-execution-alias");
        await symlink(join(f.root, "partial-execution"), alias);
        await assert.rejects(
            reportCapacityConfirmation({ ...options, outputDirectory: join(alias, "forbidden") }),
            /OUTPUT_OVERLAP/,
        );
        assert.deepEqual(await readdir(join(f.root, "partial-execution")), []);
        const rebuilt = join(f.root, "confirmation-report-second");
        await reportCapacityConfirmation({ ...options, outputDirectory: rebuilt });
        assert((await readFile(join(out, "report.json"))).equals(await readFile(join(rebuilt, "report.json"))));
    },
);

async function processes() {
    const output = (
        await command("ps", ["-axo", "pid=,ppid=,pgid=,stat=,command="], { timeout: 2000, maxBuffer: 8 * MiB })
    ).stdout;
    return output.split("\n").flatMap((line) => {
        const match = /^\s*(\d+)\s+(\d+)\s+(\d+)\s+(\S+)\s+(.*)$/.exec(line);
        return match
            ? [
                  {
                      pid: Number(match[1]),
                      parent: Number(match[2]),
                      group: Number(match[3]),
                      state: match[4],
                      argv: match[5],
                  },
              ]
            : [];
    });
}
const sleep = (milliseconds) => new Promise((done) => setTimeout(done, milliseconds));
async function pauseRealNestedWorker(root, cleanup) {
    const deadline = Date.now() + 4000;
    while (Date.now() < deadline) {
        const rows = await processes(),
            outer = rows.find(
                (r) => r.argv.includes("__verify-confirmation-plan") && r.argv.includes(root + "/confirmation"),
            ),
            inner = rows.find(
                (r) =>
                    r.parent === outer?.pid &&
                    r.argv.includes("__verify-plan") &&
                    r.argv.includes(root + "/exploration"),
            ),
            grandchild = rows.find((r) => r.parent === inner?.pid && r.group === inner?.pid);
        if (!outer || !inner || !grandchild) {
            await sleep(5);
            continue;
        }
        assert.equal(outer.group, outer.pid);
        assert.equal(inner.group, inner.pid);
        assert.notEqual(inner.pid, process.pid);
        cleanup.add(inner.pid);
        cleanup.add(outer.pid);
        // Real fault injection into this fixture's observed independent group: TERM cannot wake stopped processes.
        process.kill(-inner.pid, "SIGSTOP");
        const paused = await processes();
        if (paused.find((r) => r.pid === grandchild.pid)?.state.startsWith("T")) {
            const match = /--out\s+(\S+)/.exec(inner.argv);
            return {
                pids: [outer.pid, inner.pid, grandchild.pid],
                temporaryDirectory: dirname(match[1]),
                innerGroup: inner.pid,
            };
        }
        process.kill(-inner.pid, "SIGCONT");
    }
    throw new Error("Did not observe this fixture's actual nested Node worker and grandchild");
}
async function allGone(pids) {
    const deadline = Date.now() + 3000;
    while (Date.now() < deadline) {
        const rows = await processes();
        if (pids.every((pid) => !rows.some((row) => row.pid === pid))) return;
        await sleep(20);
    }
    assert.fail("A captured owned worker/grandchild remained after bounded cleanup: " + pids.join(","));
}

test(
    "real confirmation verifier cancellation and timeout reap its stopped inner worker group and grandchild",
    { timeout: 120000 },
    async (t) => {
        const f = await missingExploration(t),
            plan = join(f.root, "confirmation"),
            cleanup = new Set();
        t.after(() => {
            for (const group of cleanup) {
                try {
                    process.kill(-group, "SIGKILL");
                } catch (error) {
                    if (error.code !== "ESRCH") throw error;
                }
            }
        });
        await planHttpCapacityConfirmation(f.requestFile, { out: plan });
        for (const mode of ["cancel", "timeout"]) {
            const controller = new AbortController(),
                running = runCapacityWorker(
                    [
                        "__verify-confirmation-plan",
                        "--plan",
                        plan,
                        "--out",
                        join(f.root, "never-verified-" + mode + ".json"),
                    ],
                    mode === "timeout" ? 5000 : 60000,
                    controller.signal,
                );
            const nested = await pauseRealNestedWorker(f.root, cleanup),
                stoppedAt = Date.now();
            if (mode === "cancel") controller.abort();
            const result = await running;
            assert.equal(
                result.failure,
                mode === "cancel" ? "CAPACITY_REPORT_CANCELLED" : "CAPACITY_REPORT_WORKER_TIMEOUT",
            );
            assert(Date.now() - stoppedAt >= 2500, "inner group requires its real three-second KILL cleanup");
            assert(
                Date.now() - stoppedAt < 14000,
                "outer seven-second cleanup is a bound, not a renewed execution budget",
            );
            await allGone(nested.pids);
            await assert.rejects(access(nested.temporaryDirectory), { code: "ENOENT" });
            await assert.rejects(access(join(f.root, "never-verified-" + mode + ".json")), { code: "ENOENT" });
            cleanup.delete(nested.innerGroup);
            cleanup.delete(nested.pids[0]);
        }
    },
);

test(
    "public confirmation report SIGTERM reaps the component, both detached verifier groups and stopped grandchild",
    { timeout: 90000 },
    async (t) => {
        const f = await missingExploration(t),
            plan = join(f.root, "confirmation"),
            cleanup = new Set();
        t.after(() => {
            for (const group of cleanup) {
                try {
                    process.kill(-group, "SIGKILL");
                } catch (error) {
                    if (error.code !== "ESRCH") throw error;
                }
            }
        });
        await planHttpCapacityConfirmation(f.requestFile, { out: plan });
        const saved = JSON.parse(await readFile(join(plan, "confirmation-plan.json"))),
            input = join(f.root, "confirmation-set.json"),
            output = join(f.root, "cancelled-public-report");
        await writeFile(
            input,
            JSON.stringify({
                schemaVersion: 1,
                kind: "http-capacity-execution-set",
                planDigest: saved.planDigest,
                points: [],
            }) + "\n",
        );
        const child = spawn(
            process.execPath,
            [
                resolve("dist/cli.js"),
                "http",
                "capacity",
                "confirmation",
                "report",
                input,
                "--plan",
                plan,
                "--out",
                output,
                "--timeout-ms",
                "60000",
                "--max-bytes",
                String(64 * MiB),
            ],
            { detached: true, stdio: ["ignore", "pipe", "pipe"] },
        );
        cleanup.add(child.pid);
        const buffers = { stdout: [], stderr: [] };
        let bytes = 0;
        for (const name of ["stdout", "stderr"])
            child[name].on("data", (chunk) => {
                if (bytes < MiB) buffers[name].push(chunk.subarray(0, MiB - bytes));
                bytes += chunk.length;
            });
        const closed = new Promise((accept, reject) => {
            child.once("error", reject);
            child.once("close", (code, signal) => accept({ code, signal }));
        });
        const nested = await pauseRealNestedWorker(f.root, cleanup),
            rows = await processes(),
            outer = rows.find((row) => row.pid === nested.pids[0]),
            component = rows.find((row) => row.pid === outer.parent);
        assert.equal(component.parent, child.pid);
        assert(component.argv.includes("capacity-evidence.mjs report-confirmation"));
        assert.equal(component.group, component.pid);
        cleanup.add(component.pid);
        const stoppedAt = Date.now();
        child.kill("SIGTERM");
        const result = await closed;
        assert.equal(result.code, 143, Buffer.concat(buffers.stderr).toString());
        assert.equal(result.signal, null);
        assert(bytes <= MiB);
        const reply = JSON.parse(Buffer.concat(buffers.stdout).toString());
        assert.equal(reply.ok, false);
        assert.equal(reply.error.code, "HTTP_CALIBRATION_CANCELLED");
        assert(Date.now() - stoppedAt >= 2500, "the real stopped inner group requires its three-second KILL cleanup");
        assert(
            Date.now() - stoppedAt < 15000,
            "public supervision retains the report owner for bounded nested cleanup",
        );
        await allGone([child.pid, component.pid, ...nested.pids]);
        await assert.rejects(access(nested.temporaryDirectory), { code: "ENOENT" });
        await assert.rejects(access(join(output, "evidence.lock.json")), { code: "ENOENT" });
        await assert.rejects(access(join(output, "report.json")), { code: "ENOENT" });
        cleanup.clear();
    },
);
