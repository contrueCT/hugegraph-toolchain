/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
// Synthetic presentation fixtures only. No reader, task, calibration or HTTP execution.
import test from "node:test";
import assert from "node:assert/strict";
import {
    ENGINEERING_CASES,
    ENGINEERING_GATES,
    renderEngineeringAcceptanceHtml,
} from "../components/http/engineering-acceptance.mjs";

function fixture() {
    return {
        engineering: {
            status: "unknown",
            accepted: false,
            gates: ENGINEERING_GATES.map((id) => ({
                id,
                status: id === "formal-parameter-freeze" ? "passed" : "unknown",
                reason: "Original gate reason: " + id,
                scope: [{ kind: "synthetic-only", caseIds: ["scalar-add"], modes: ["forward"], ratesPerSecond: [10] }],
                limits: ["Original gate limit: " + id],
            })),
        },
        coverage: {
            fullElevenStatus: "unknown",
            cases: ENGINEERING_CASES.map((id) => ({
                id,
                status: "unknown",
                gateCoverage: ENGINEERING_GATES.map((gateId) => ({
                    id: gateId,
                    status: gateId === "formal-parameter-freeze" ? "passed" : "unknown",
                    coverage:
                        gateId === "known-regression-detection"
                            ? "fixed-method-control; no per-case target claim"
                            : "original-only-coverage",
                })),
                limits: ["Original case limit: " + id],
            })),
        },
        comparison: {
            status: "passed",
            gate: { id: "formal-parameter-freeze", reason: "Exact content lock verified" },
            conditions: { modes: ["forward", "embedded"], durationMs: 120000 },
            mismatches: [
                {
                    contributionId: "observerCost",
                    field: "load.open-80",
                    formal: { value: { ratePerSecond: 80 }, sha256: "a".repeat(64) },
                    calibrated: [{ value: { ratePerSecond: 10 }, sha256: "b".repeat(64) }],
                },
            ],
            unknownConditions: [
                {
                    contributionId: "driverHeadroom",
                    field: "measurement.nodeExecutableSha256",
                    reason: "Original Node digest is not established",
                },
                "ORIGINAL_COMPARISON_VERSION_UNKNOWN",
            ],
            methodControlDisclosures: [
                {
                    contributionId: "knownRegression",
                    kind: "fixed-method-positive-control",
                    blocking: false,
                    gateId: "known-regression-detection",
                    originalGateStatus: "passed",
                    originalScope: { caseIds: ["arithmetic-control"], modes: ["forward"], ratesPerSecond: [20] },
                    interpretation: "Original positive control only; no target capacity claim",
                    differences: [
                        {
                            contributionId: "knownRegression",
                            field: "mode",
                            formal: { value: "embedded" },
                            calibrated: [{ value: "forward" }],
                        },
                    ],
                    unknownConditions: [
                        {
                            contributionId: "knownRegression",
                            field: "target.javaExecutableSha256",
                            reason: "Control binary provenance not established",
                        },
                    ],
                },
            ],
            limits: ["Original comparison limitation"],
        },
        limits: ["Synthetic renderer fixture; no performance evidence"],
        performanceAccepted: false,
        qualificationAccepted: false,
    };
}

test("engineering HTML retains mismatch, unknown, method-control and all per-case gate fields", () => {
    const html = renderEngineeringAcceptanceHtml(fixture());
    for (const text of [
        "Actual mismatches (1)",
        "load.open-80",
        "ratePerSecond",
        "80",
        "10",
        "a".repeat(64),
        "b".repeat(64),
        "Unknown conditions (2)",
        "measurement.nodeExecutableSha256",
        "Original Node digest is not established",
        "ORIGINAL_COMPARISON_VERSION_UNKNOWN",
        "Known-method control disclosures (1)",
        "blocking: false",
        "fixed-method-positive-control",
        "arithmetic-control",
        "Original positive control only; no target capacity claim",
        "Control binary provenance not established",
        "exact comparison and calibration-index content lock",
        "Original comparison limitation",
    ])
        assert.ok(html.includes(text), text);
    assert.equal((html.match(/class="case-details"/g) ?? []).length, 11);
    assert.equal((html.match(/7 recorded gate results and limits/g) ?? []).length, 11);
    for (const id of ENGINEERING_CASES) assert.ok(html.includes("Original case limit: " + id));
    for (const id of ENGINEERING_GATES) {
        assert.ok(html.includes("Original gate limit: " + id));
        assert.equal((html.match(new RegExp("<code>" + id + "</code>", "g")) ?? []).length, 11);
    }
    assert.ok(html.includes("fixed-method-control; no per-case target claim"));
});

test("engineering HTML escapes all dynamic diagnostic text and requires no script or external assets", () => {
    const report = fixture(),
        hostile = '</pre><script>alert("x")</script><img src=x onerror="alert(1)">&\'';
    report.comparison.mismatches[0].field = hostile;
    report.comparison.mismatches[0].formal.value = hostile;
    report.comparison.mismatches[0].calibrated[0].value = hostile;
    report.comparison.unknownConditions[0].reason = hostile;
    report.comparison.methodControlDisclosures[0].interpretation = hostile;
    report.coverage.cases[0].limits = [hostile];
    report.engineering.gates[0].limits = [hostile];
    const html = renderEngineeringAcceptanceHtml(report);
    assert.ok(html.includes("&lt;script&gt;"));
    assert.ok(html.includes("&quot;x&quot;"));
    assert.ok(html.includes("&amp;&#39;"));
    assert.doesNotMatch(
        html,
        /<script\b|<img\b|<iframe\b|<link\b|<script\s+src=|<style[^>]+src=|(?:src|href)=["']https?:|@import\b|url\(/i,
    );
});

test("engineering HTML states absent comparison and keeps legacy reports without new arrays readable", () => {
    const report = fixture();
    report.comparison = null;
    let html = renderEngineeringAcceptanceHtml(report);
    assert.ok(html.includes("No formal comparison was supplied"));
    assert.ok(html.includes("parameter freeze and calibrated-condition match remain unknown"));
    assert.equal((html.match(/class="case-details"/g) ?? []).length, 11);
    report.comparison = { status: "unknown", limits: [] };
    html = renderEngineeringAcceptanceHtml(report);
    assert.ok(html.includes("Actual mismatches (0)"));
    assert.ok(html.includes("Unknown conditions (0)"));
    assert.ok(html.includes("No method-control disclosure recorded"));
});

test("engineering HTML leaves all original judgments, JSON and calibration references untouched", () => {
    const report = fixture();
    report.calibrationIndexSha256 = "c".repeat(64);
    report.sourceInputSha256 = "d".repeat(64);
    const before = JSON.stringify(report),
        freeze = (value) => {
            if (value && typeof value === "object") {
                Object.values(value).forEach(freeze);
                Object.freeze(value);
            }
        };
    freeze(report);
    assert.equal(renderEngineeringAcceptanceHtml(report), renderEngineeringAcceptanceHtml(report));
    assert.equal(JSON.stringify(report), before);
    assert.equal(report.engineering.accepted, false);
    assert.equal(report.comparison.status, "passed");
    assert.equal(report.coverage.fullElevenStatus, "unknown");
    assert.equal(report.performanceAccepted, false);
});
