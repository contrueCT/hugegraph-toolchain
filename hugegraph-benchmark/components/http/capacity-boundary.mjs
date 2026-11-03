/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

const finite = (value) => typeof value === "number" && Number.isFinite(value) && value >= 0;
const allowed = /^(warmup|measurement):(driver-pressure-observed|dropped-arrivals)$/;

/** Internal projection of a freshly independently verified condition, never an evidence verifier. */
export function projectCapacityBoundary({ point, policy, verification }) {
    const issues = [],
        symptoms = [],
        rows = verification?.rows ?? [];
    const single = policy?.schemaVersion === 2;
    const expectedArms = single ? 1 : 4;
    if (
        single &&
        (policy.exploration?.mode !== "single-run" ||
            policy.exploration?.runs !== 1 ||
            verification?.kind !== "http-observation-evidence-verification" ||
            verification?.design !== "single-observation" ||
            rows.some((row) => row.arm !== "O"))
    )
        issues.push("BOUNDARY_OBSERVATION_DESIGN_DIFFER");
    if (policy?.continuation?.schemaVersion !== 1 || policy.continuation.mode !== "verified-case-range")
        issues.push("CONTINUATION_NOT_PREREGISTERED");
    if (
        !point ||
        rows.length !== expectedArms ||
        point.expectedArms !== expectedArms ||
        verification?.planDigest !== point.comparisonPlanDigest ||
        !Array.isArray(verification.globalIssues) ||
        verification.globalIssues.length
    )
        issues.push("BOUNDARY_CONDITION_INCOMPLETE");
    if (verification?.valid !== rows.every((row) => row.status === "ok")) issues.push("BOUNDARY_UNEXPLAINED_VALIDITY");
    const ids = new Set(),
        raw = new Set();
    for (const row of rows) {
        if (
            !row.rawVerified ||
            !row.runtimeArtifactsConsistent ||
            !row.driverVerified ||
            !row.resourcesVerified ||
            !row.runId ||
            ids.has(row.runId) ||
            !row.rawEvidenceSha256 ||
            raw.has(row.rawEvidenceSha256) ||
            row.expectedMode !== point?.mode ||
            !["ok", "invalid"].includes(row.status) ||
            !Array.isArray(row.issues) ||
            row.issues.some((s) => s !== "HTTP_QUALITY_FAILED") ||
            (row.metricQuality && row.metricQuality.status !== "passed")
        )
            issues.push("BOUNDARY_RAW_ENVIRONMENT_OR_QUALITY_UNPROVEN");
        ids.add(row.runId);
        raw.add(row.rawEvidenceSha256);
        const report = row.httpSummary,
            runs = report?.runs ?? [],
            run = runs[0];
        if (
            runs.length !== 1 ||
            report?.gates?.sampleAccounting !== "passed" ||
            report?.gates?.correctness !== "passed" ||
            !run ||
            run.caseId !== point.caseId ||
            run.model !== "open" ||
            run.repeat !== 0 ||
            run.ratePerSecond !== point.ratePerSecond
        ) {
            issues.push("BOUNDARY_HTTP_ACCOUNTING_OR_ANSWER_UNPROVEN");
            continue;
        }
        if (
            !Array.isArray(run.issues) ||
            run.issues.some((s) => !allowed.test(s)) ||
            run.failed !== 0 ||
            !run.counts ||
            run.counts.ok !== run.correct ||
            run.correct !== run.correctEndToEndMs?.count ||
            Object.entries(run.counts ?? {}).some(
                ([key, count]) => count && !["ok", "scheduler-late", "concurrency-limit"].includes(key),
            ) ||
            !finite(run.correctCompletedInWindowPerSecond) ||
            !finite(run.durationMs) ||
            run.durationMs <= 0 ||
            Math.round((run.correctCompletedInWindowPerSecond * run.durationMs) / 1000) <
                policy.analysis.goodput.minimumObservations ||
            !Number.isSafeInteger(run.correctEndToEndMs?.count) ||
            run.correctEndToEndMs.count < policy.analysis.latency.minimumObservations
        )
            issues.push("BOUNDARY_UNEXPLAINED_FAILURE_OR_SAMPLES");
        if (run.issues?.some((s) => allowed.test(s))) symptoms.push("driver-range-observed");
        if (row.status === "invalid" && !run.issues?.length) issues.push("BOUNDARY_UNEXPLAINED_INVALID_ROW");
        if (policy.slo) {
            const q = policy.slo.endToEndQuantile,
                count = run.correctEndToEndMs?.count,
                minimum =
                    q === "p99"
                        ? Math.max(1000, policy.analysis.latency.minimumObservations)
                        : policy.analysis.latency.minimumObservations;
            if (!Number.isSafeInteger(count) || count < minimum || !finite(run.correctEndToEndMs?.[q]))
                issues.push("BOUNDARY_SLO_LATENCY_UNKNOWN");
            else if (run.correctEndToEndMs[q] > policy.slo.maxLatencyMs)
                symptoms.push("predeclared-latency-slo-exceeded");
        }
    }
    return {
        schemaVersion: 1,
        kind: "http-capacity-boundary-projection",
        pointId: point?.id,
        comparisonPlanDigest: verification?.planDigest,
        executionLockSha256: verification?.executionLockSha256,
        action: issues.length ? "stop-study" : symptoms.length ? "stop-case" : "continue",
        issues: [...new Set(issues)],
        symptoms: [...new Set(symptoms)],
        plannedArms: expectedArms,
        observedArms: rows.length,
        rawQualityValid: verification?.valid === true,
        cleanupProven: false,
        targetCapacityUpperBound: "unknown",
        performanceAccepted: false,
        scope: "new exploration only; preserved original quality; owned absence must be checked before advancing",
    };
}
