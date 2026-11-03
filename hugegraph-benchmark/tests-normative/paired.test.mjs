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
import { mkdtemp, readFile, writeFile, rm, symlink } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { createHash } from "node:crypto";
import {
    analyze,
    balancedOrders,
    budgetRelation,
    medianReference,
    nearestRank,
    Sampler,
} from "../components/paired/core.mjs";
import { runAnalysis } from "../components/paired/analyzer.mjs";
import { simulatedInput, wilson } from "../components/paired/model-check.mjs";
import { submitTask, waitTask, taskDirectory } from "../dist/tasks.js";

const near = (a, b) => assert(Math.abs(a - b) < 1e-12, `${a} != ${b}`);
function input(pairs = 20) {
    return simulatedInput({ seed: "unit-fixed", model: "iid-normal", trial: 0, pairs, replicates: 4000, shift: 0.2 });
}
async function directory(t) {
    const root = await mkdtemp(join(tmpdir(), "hgbench-paired-"));
    t.after(() => rm(root, { recursive: true, force: true }));
    return root;
}

test("paired estimator uses per-block effects, not a ratio of arm medians", () => {
    const x = input(3);
    [1, 2, 100].forEach((a, i) => {
        x.blocks[i].baseline.values["latency-1"].value = a;
        x.blocks[i].candidate.values["latency-1"].value = [1.1, 20, 110][i];
    });
    const c = analyze(x).cells[0];
    near(c.point.relativeMedian, 0.1);
    assert.equal(c.point.baselineMedian, 2);
    assert.equal(c.point.candidateMedian, 20);
    assert.equal(c.bootstrapIntervals, null);
    assert(c.warnings.includes("insufficient-independent-pairs"));
});

test("latency and throughput use the same deterioration direction and paired draws", () => {
    const x = input(),
        lower = analyze(x).cells[0];
    x.policy.metrics[0].better = "higher";
    for (const b of x.blocks) {
        const a = b.baseline.values["latency-1"].value;
        b.candidate.values["latency-1"].value = 2 * a - b.candidate.values["latency-1"].value;
    }
    const higher = analyze(x).cells[0];
    near(higher.point.relativeMedian, lower.point.relativeMedian);
    near(higher.bootstrapIntervals.relative.low, lower.bootstrapIntervals.relative.low);
    near(higher.bootstrapIntervals.relative.high, lower.bootstrapIntervals.relative.high);
    assert(higher.point.relativeMedian > 0);
    const rerun = analyze(x);
    assert.deepEqual(rerun, analyze(x));
    assert.equal(rerun.performanceAccepted, false);
    assert.equal(rerun.automaticGate, "disabled");
});

test("relative and absolute intervals both consume the fixed primary family budget", () => {
    const x = input(),
        r = analyze(x);
    assert.equal(r.familyStatements, 4);
    assert.equal(r.perTailProbability, 0.00625);
    assert.equal(r.expectedBootstrapTailDraws, 25);
    near(r.cells[0].nominalIntervalConfidence, 0.9875);
    delete x.blocks[0].candidate.values["latency-2"];
    const bad = analyze(x);
    assert.equal(bad.familyStatements, 4);
    assert.equal(bad.perTailProbability, r.perTailProbability);
    assert.deepEqual(bad.cells[0].bootstrapIntervals, r.cells[0].bootstrapIntervals);
    assert.equal(bad.cells[1].bootstrapIntervals, null);
    assert.equal(bad.cells[1].point, null);
    assert.equal(bad.verdict, "quality-issues");
});

test("failed runs, missing blocks, changed conditions and wrong order suppress family inference", () => {
    const mutations = [
        (x) => {
            x.blocks[0].candidate.status = "correctness-failed";
        },
        (x) => {
            x.blocks.pop();
        },
        (x) => {
            x.blocks[0].baseline.conditionsDigest = "0".repeat(64);
        },
        (x) => {
            x.blocks[0].candidate.variantDigest = "0".repeat(64);
        },
        (x) => {
            x.blocks[0].observedOrder = x.blocks[0].observedOrder === "AB" ? "BA" : "AB";
        },
    ];
    for (const mutate of mutations) {
        const x = input();
        mutate(x);
        const r = analyze(x);
        assert.equal(r.verdict, "quality-issues");
        assert(r.cells.every((c) => c.issues.length && c.point === null && c.bootstrapIntervals === null));
    }
});

test("duplicate physical runs and undeclared metrics or blocks are rejected", () => {
    const x = input();
    x.blocks[1].baseline = { ...x.blocks[0].baseline };
    assert.throws(() => analyze(x), /Physical run reference reused/);
    const y = input();
    y.blocks[1].id = y.blocks[0].id;
    assert.throws(() => analyze(y), /duplicate block/);
    const z = input();
    z.blocks[0].baseline.values.undeclared = { value: 1, observations: 1 };
    assert.throws(() => analyze(z), /Unexpected observed metric/);
});

test("invalid, insufficient and zero-baseline observations are never silently dropped", () => {
    const cases = [
        [
            (x) => {
                x.blocks[0].baseline.values["latency-1"].value = 0;
            },
            "zero-baseline",
        ],
        [
            (x) => {
                x.blocks[0].baseline.values["latency-1"].observations = 9999;
            },
            "insufficient-observations",
        ],
        [
            (x) => {
                x.blocks[0].candidate.values["latency-1"].value = Infinity;
            },
            "missing-or-invalid-value",
        ],
        [
            (x) => {
                x.blocks[0].baseline.values["latency-1"].value = Number.MIN_VALUE;
            },
            "numerical-overflow",
        ],
    ];
    for (const [change, issue] of cases) {
        const x = input();
        change(x);
        const c = analyze(x).cells[0];
        assert.equal(c.bootstrapIntervals, null);
        assert.equal(c.point, null);
        assert(c.issues.some((i) => i.startsWith(issue)));
    }
});

test("tail precision, degeneracy and bounded work prevent false certainty", () => {
    const x = input();
    x.policy.bootstrapReplicates = 1000;
    assert.equal(analyze(x).cells[0].bootstrapIntervals, null);
    x.policy.bootstrapReplicates = 4000;
    for (const b of x.blocks) b.candidate.values["latency-1"].value = b.baseline.values["latency-1"].value;
    const c = analyze(x).cells[0];
    assert.equal(c.bootstrapIntervals.relative.low, 0);
    assert.equal(c.budgetRelation, "not-assessed");
    assert(c.warnings.some((w) => w.startsWith("degenerate")));
    x.policy.maxWork = 1;
    assert.throws(() => analyze(x), /work exceeds/);
    x.policy.maxWork = 1000000000;
    x.policy.bootstrapReplicates = 1000000;
    x.policy.maxMilliseconds = 1;
    assert.throws(() => analyze(x), /deadline exhausted/);
});

test("practical significance crosses both floors; budget overlap remains inconclusive", () => {
    const m = { relativeBudget: 0.05, absoluteBudget: 5 };
    const rel = (r, a) =>
        budgetRelation({ relative: { low: r[0], high: r[1] }, absolute: { low: a[0], high: a[1] } }, m);
    assert.equal(rel([0.06, 0.2], [6, 20]), "above-both-budgets");
    assert.equal(rel([-0.2, -0.06], [-20, -6]), "below-both-budgets");
    assert.equal(rel([0.06, 0.2], [1, 4]), "within-at-least-one-budget");
    assert.equal(rel([0.03, 0.2], [3, 20]), "crosses-budget");
    assert.equal(rel([0.05, 0.2], [5, 20]), "crosses-budget");
});

test("order-statistic reference matches exact binomial tails, including unbounded small-N", () => {
    const r = medianReference(
        Array.from({ length: 20 }, (_, i) => i + 1),
        0.00625,
    );
    // sum C(20,j), j=0..4 = 1+20+190+1140+4845 = 6196.
    assert.equal(r.k, 5);
    assert.equal(r.low, 5);
    assert.equal(r.high, 16);
    near(r.iidMedianCoverageLowerBound, 1 - (2 * 6196) / 1048576);
    const s = medianReference([1, 2, 3], 0.00625);
    assert.equal(s.finite, false);
    assert.equal(s.low, null);
    assert.equal(s.high, null);
    assert.equal(nearestRank([1, 2, 3, 4], 0.5), 2);
    assert.equal(nearestRank([1, 2, 3, 4], 1), 4);
});

test("counter sampler, order allocation and simulation are reproducible", () => {
    const a = new Sampler("fixed"),
        b = new Sampler("fixed");
    assert.deepEqual(
        Array.from({ length: 100 }, () => a.index(19)),
        Array.from({ length: 100 }, () => b.index(19)),
    );
    const ids = Array.from({ length: 21 }, (_, i) => `b${i}`),
        order = balancedOrders(ids, "fixed");
    assert.equal(Object.values(order).filter((x) => x === "AB").length, 11);
    assert.deepEqual(order, balancedOrders(ids, "fixed"));
    assert.deepEqual(input(), input());
    const zero = wilson(0, 200);
    near(zero.wilson95[1], 3.841458820694124 / (200 + 3.841458820694124));
    near(zero.wilson95[0], 0);
    assert.throws(() => wilson(201, 200));
});

test("analysis bundle is deterministic, bounded, HTML-escaped and exclusive", async (t) => {
    const root = await directory(t),
        file = join(root, "input.json"),
        x = input();
    x.policy.metrics[0].unit = "</script><script>alert(1)</script>";
    await writeFile(file, JSON.stringify(x));
    await runAnalysis(file, join(root, "one"));
    await runAnalysis(file, join(root, "two"));
    for (const f of ["analysis.json", "report.html", "analysis.lock.json"])
        assert.deepEqual(await readFile(join(root, "one", f)), await readFile(join(root, "two", f)));
    const report = await readFile(join(root, "one/report.html"), "utf8");
    assert(!report.includes(x.policy.metrics[0].unit));
    assert(report.includes("&lt;/script&gt;"));
    assert(report.includes("Content-Security-Policy"));
    assert(report.includes("数学模拟数据（未访问数据库）"));
    const lock = JSON.parse(await readFile(join(root, "one/analysis.lock.json"), "utf8"));
    for (const f of lock.files) {
        const body = await readFile(join(root, "one", f.path));
        assert.equal(body.length, f.bytes);
        assert.equal(createHash("sha256").update(body).digest("hex"), f.sha256);
    }
    await assert.rejects(runAnalysis(file, join(root, "one")), /EEXIST/);
    await symlink(file, join(root, "link"));
    await assert.rejects(runAnalysis(join(root, "link"), join(root, "no")), /regular JSON/);
    await writeFile(join(root, "oversize"), Buffer.alloc(8 * 1024 * 1024 + 1));
    await assert.rejects(runAnalysis(join(root, "oversize"), join(root, "no")), /8 MiB/);
});

test("TS task drives the paired component once and preserves diagnostic acceptance", async (t) => {
    const root = await directory(t),
        file = join(root, "input.json");
    await writeFile(file, JSON.stringify(input()));
    const source = join(root, "task.json"),
        frozen = join(root, "frozen.json");
    await writeFile(
        source,
        JSON.stringify({
            schemaVersion: 1,
            kind: "component-task",
            name: "paired-component-test",
            command: [process.execPath, "components/paired/workflow.mjs", "--input", file],
            cwd: resolve("."),
            trackedFiles: [
                "components/paired/workflow.mjs",
                "components/paired/core.mjs",
                "components/paired/analyzer.mjs",
                file,
            ],
            effect: "filesystem",
            timeoutMs: 15000,
            terminateGraceMs: 1000,
            maxLogBytes: 1048576,
        }),
    );
    await promisify(execFile)(process.execPath, [resolve("dist/cli.js"), "task", "plan", source, "--out", frozen]);
    const plan = JSON.parse(await readFile(frozen, "utf8"));
    const task = await submitTask(join(root, "tasks"), plan, "paired-once");
    const done = await waitTask(join(root, "tasks"), task.taskId, 20000);
    assert.equal(done.state, "completed", JSON.stringify(done));
    assert.equal((await submitTask(join(root, "tasks"), plan, "paired-once")).reused, true);
    const taskDir = taskDirectory(join(root, "tasks"), task.taskId);
    const report = JSON.parse(await readFile(join(taskDir, "output/analysis.json"), "utf8"));
    assert.equal(report.performanceAccepted, false);
    assert.equal(report.cells.length, 2);
});
