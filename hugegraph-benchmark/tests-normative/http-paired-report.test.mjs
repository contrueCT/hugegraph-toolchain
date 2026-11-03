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
import { html } from "../components/http/paired-report.mjs";
const fixture = () => ({
    reportContext: {
        metricBindings: [
            { id: "metric-1", caseId: "scalar-add", loadId: "open-2", valuePath: "correctEndToEndMs.p99" },
        ],
    },
    verification: {
        valid: false,
        statisticsEligible: false,
        planDigest: "a".repeat(64),
        executionLockSha256: "b".repeat(64),
        globalIssues: ["EXECUTION_FAILED"],
        gates: { resources: "pending" },
        rows: [
            {
                runId: "bad-arm",
                blockId: "b01",
                arm: "A",
                status: "invalid",
                issues: ["ARM_MISSING"],
                resourcesVerified: false,
                resourceCoverage: "partial",
                resourceMissingMetrics: { "driver.io.stat": { samples: 10, lastReason: "missing" } },
            },
        ],
    },
    analysis: {
        nominalFamilyConfidence: 0.95,
        familyStatements: 2,
        cells: [
            {
                metric: { id: "metric-1", unit: "ms" },
                observedPairs: 0,
                plannedPairs: 2,
                point: null,
                bootstrapIntervals: null,
                budgetRelation: "not-assessed",
                issues: ["missing-run:b01"],
                warnings: ["insufficient-independent-pairs"],
                pairs: [],
            },
        ],
    },
});
test("paired HTML preserves failed arms, missing resources, uncalibrated status and actual output links", () => {
    const output = html(fixture());
    for (const text of [
        "scalar-add",
        "open-2",
        "correctEndToEndMs.p99",
        "EXECUTION_FAILED",
        "ARM_MISSING",
        "driver.io.stat",
        "missing",
        "尚未接受性能结论",
        "0 / 1 有效",
        "未满足",
        "预检、预热、测量、排空及协调成本",
    ])
        assert(output.includes(text), text);
    for (const file of ["analysis.json", "verification.json", "observations.json", "evidence.lock.json"])
        assert(output.includes(`href="${file}"`));
    assert(!output.includes('href="input.json"'));
    assert(!output.includes('href="analysis.lock.json"'));
    assert(output.includes('data-issues="2"'));
    assert(output.includes("default-src 'none'"));
    assert(!output.includes('src="http'));
});
test("paired report escapes evidence text and attributes without hiding original diagnostic content", () => {
    const input = fixture(),
        payload = '<img src=x onerror="alert(1)">';
    input.reportContext.metricBindings[0].caseId = payload;
    input.verification.rows[0].issues = [payload];
    input.verification.rows[0].jvmObservation = { issues: [payload], clockAlignment: "unverified" };
    input.verification.rows[0].phaseClocks = [{ hostAssociation: "unverified" }];
    input.verification.gates = { diagnostic: "</pre><script>alert(2)</script>" };
    const output = html(input);
    assert(!output.includes(payload));
    assert(!output.includes("</pre><script>alert(2)</script>"));
    assert(output.includes("&lt;img src=x onerror=&quot;alert(1)&quot;&gt;"));
    assert(output.includes("&lt;/pre&gt;&lt;script&gt;alert(2)&lt;/script&gt;"));
    assert.equal((output.match(/<script>/g) || []).length, 1);
});
test("saved verification context restores labels and explicitly identifies the experiment and modes", () => {
    const input = fixture();
    input.verification.reportContext = {
        ...input.reportContext,
        purpose: "functional",
        comparison: "forward-embedded",
        modes: { A: "forward", B: "embedded" },
        suite: {
            suiteId: "hgbench-http-serving-v0.1",
            version: "0.1.0-draft.1",
            bundleSha256: "c".repeat(64),
            caseIds: ["scalar-add"],
        },
    };
    delete input.reportContext;
    input.verification.rows[0].expectedMode = "forward";
    const output = html(input);
    for (const text of [
        "scalar-add · open-2",
        "correctEndToEndMs.p99",
        "实验用途：<code>functional</code>",
        "比较类型：<code>forward-embedded</code>",
        "基线 A = <code>forward</code>",
        "候选 B = <code>embedded</code>",
        "计划模式：<code>forward</code>",
        "hgbench-http-serving-v0.1",
        "0.1.0-draft.1",
        "c".repeat(64),
        "尚未接受性能结论",
        "未满足",
    ])
        assert(output.includes(text), text);
});
test("report does not invent absent purpose, comparison, modes or suite metadata", () => {
    const input = fixture();
    const output = html(input);
    for (const text of ["实验用途：", "比较类型：", "预定模式：", "计划模式：", "<summary>题集", "forward", "embedded"])
        assert(!output.includes(text), text);
    input.reportContext.modes = { A: "forward" };
    const partial = html(input);
    assert(partial.includes("基线 A = <code>forward</code>"));
    assert(!partial.includes("候选 B ="));
});
test("new experiment metadata is escaped in visible labels and suite details", () => {
    const input = fixture();
    const payload = '<script>alert("metadata")</script>';
    input.reportContext = {
        ...input.reportContext,
        purpose: payload,
        comparison: payload,
        modes: { A: payload, B: payload },
        suite: { suiteId: payload, version: payload },
    };
    input.verification.rows[0].expectedMode = payload;
    const output = html(input);
    assert(!output.includes(payload));
    assert(output.includes("&lt;script&gt;alert(&quot;metadata&quot;)&lt;/script&gt;"));
    assert.equal((output.match(/<script>/g) || []).length, 1);
});
