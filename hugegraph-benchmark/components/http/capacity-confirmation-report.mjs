/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
const finite = (value) => typeof value === "number" && Number.isFinite(value) && value >= 0;
const median = (values) => {
    const sorted = [...values].sort((a, b) => a - b),
        middle = sorted.length >> 1;
    return sorted.length ? (sorted.length % 2 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2) : null;
};
const unique = (values) => [...new Set(values)];
const need = (condition, code) => {
    if (!condition) throw Object.assign(new Error(code), { code });
};
const ns = (value) => typeof value === "string" && /^(0|[1-9][0-9]{0,18})$/.test(value);
const windowValid = (window) =>
    window &&
    typeof window.boot === "string" &&
    window.boot.length > 0 &&
    ns(window.start) &&
    ns(window.end) &&
    BigInt(window.end) >= BigInt(window.start);

/** Internal projection. The bounded report worker obtains source receipts by replay, never from caller JSON. */
export function markConfirmationIndependence(records, source) {
    need(
        source?.schemaVersion === 1 &&
            source.kind === "http-capacity-source-independence" &&
            Array.isArray(source.executions) &&
            Array.isArray(source.unavailable),
        "CONFIRMATION_SOURCE_RECEIPT",
    );
    for (const record of records) {
        const invalid = [],
            unknown = [];
        if (source.unavailable.length) unknown.push("CONFIRMATION_EXPLORATION_TERMINAL_OR_PROOF_UNKNOWN");
        if (!windowValid(record.window)) unknown.push("CONFIRMATION_EXECUTION_WINDOW_UNKNOWN");
        const rows = record.result?.verification?.rows ?? [],
            raw = rows.map((row) => row.rawEvidenceSha256).filter(Boolean);
        for (const original of source.executions) {
            if (record.actualDirectory && record.actualDirectory === original.actualDirectory)
                invalid.push("CONFIRMATION_EXPLORATION_DIRECTORY_REUSED");
            if (record.executionId && record.executionId === original.executionId)
                invalid.push("CONFIRMATION_EXPLORATION_EXECUTION_REUSED");
            if ((record.runIds ?? []).some((id) => original.runIds.includes(id)))
                invalid.push("CONFIRMATION_EXPLORATION_RUN_REUSED");
            if (raw.some((sha) => original.rawManifestSha256s.includes(sha)))
                invalid.push("CONFIRMATION_EXPLORATION_RAW_REUSED");
            if (!windowValid(original.window)) {
                unknown.push("CONFIRMATION_EXPLORATION_WINDOW_UNKNOWN");
                continue;
            }
            if (!windowValid(record.window)) continue;
            if (original.window.boot !== record.window.boot) {
                unknown.push("CONFIRMATION_SOURCE_CLOCK_DOMAIN_UNKNOWN");
                continue;
            }
            if (BigInt(record.window.start) < BigInt(original.window.end)) {
                invalid.push(
                    BigInt(record.window.end) > BigInt(original.window.start)
                        ? "CONFIRMATION_EXPLORATION_WINDOWS_OVERLAP"
                        : "CONFIRMATION_PRECEDES_EXPLORATION",
                );
            }
        }
        const status = invalid.length ? "invalid" : unknown.length ? "unknown" : "verified";
        record.independence = {
            status,
            issues: unique([...invalid, ...unknown]),
            scope: "locked-controller-identities-and-same-boot-monotonic-execution-windows; no wall-clock inference",
        };
        record.issues.push(...record.independence.issues);
        record.issues = unique(record.issues);
    }
    return records;
}

function throughput(arms, eligible) {
    const values = arms.map((arm) => arm.correctCompletedInWindowPerSecond);
    return {
        median: eligible ? median(values) : null,
        minimum: eligible ? Math.min(...values) : null,
        maximum: eligible ? Math.max(...values) : null,
        aggregation: eligible
            ? "median-of-all-independent-confirmation-arms-in-this-mode; no pooling-or-exploration-fallback"
            : "unavailable-incomplete-invalid-or-independence-unknown",
    };
}
function latency(arms, eligible) {
    return Object.fromEntries(
        ["p50", "p95", "p99"].map((quantile) => {
            const enough = eligible && arms.length > 0 && arms.every((arm) => finite(arm.latency[quantile].value));
            return [
                quantile,
                {
                    status: enough ? "descriptive" : "unknown",
                    value: enough ? median(arms.map((arm) => arm.latency[quantile].value)) : null,
                    aggregation: "median-of-mode-specific-arm-quantiles; not-a-pooled-request-quantile",
                },
            ];
        }),
    );
}

/** No statistical engine: reuse the existing independently verified arm projection, then group by actual mode. */
export function summarizeCapacityConfirmation(plan, records, source, projectArm) {
    need(
        plan?.kind === "http-capacity-confirmation-plan" &&
            plan.phase === "confirmation" &&
            Array.isArray(plan.points) &&
            plan.points.length <= 66 &&
            plan.suite?.caseIds?.length === 11 &&
            new Set(plan.suite.caseIds).size === 11 &&
            Array.isArray(plan.modes) &&
            plan.modes.length >= 1 &&
            plan.modes.length <= 2 &&
            plan.selection?.selections?.length === 11 * plan.modes.length,
        "CONFIRMATION_REPORT_PLAN_SCOPE",
    );
    const points = plan.points.map((point) => {
        need(
            point.expectedArms === 8 && ["aa", "forward-embedded"].includes(point.comparison),
            "CONFIRMATION_ARM_DENOMINATOR",
        );
        const record = records.find((candidate) => candidate.id === point.id),
            verification = record?.result?.verification,
            rows = verification?.rows ?? [],
            issues = [...(record?.issues ?? ["CAPACITY_POINT_NOT_STARTED"]), ...(verification?.globalIssues ?? [])];
        if (rows.length !== 8) issues.push("CONFIRMATION_ARM_DENOMINATOR_INCOMPLETE");
        const arms = Array.from({ length: 8 }, (_, index) => {
            const row = rows[index],
                armName = row ? row.arm : index % 2 ? "B" : "A",
                expectedMode = armName === "A" ? point.modes.A : armName === "B" ? point.modes.B : null;
            const arm = projectArm({ ...point, mode: expectedMode }, row, plan.policy);
            if (row && (!expectedMode || row.expectedMode !== expectedMode))
                arm.issues.push("CONFIRMATION_ARM_MODE_DIFFER");
            arm.mode = expectedMode;
            arm.measurementValid = arm.measurementValid && expectedMode !== null && arm.issues.length === 0;
            return arm;
        });
        const pairs = new Map();
        for (const row of rows) {
            const key = row.blockId,
                pair = pairs.get(key) ?? [];
            pair.push(row.arm);
            pairs.set(key, pair);
        }
        if (
            pairs.size !== 4 ||
            [...pairs.values()].some((pair) => pair.length !== 2 || !pair.includes("A") || !pair.includes("B")) ||
            new Set(rows.map((row) => row.runId)).size !== 8
        )
            issues.push("CONFIRMATION_PAIR_IDENTITIES_INCOMPLETE");
        const independent = record?.independence?.status === "verified";
        if (!independent && !issues.some((issue) => issue.startsWith("CONFIRMATION_")))
            issues.push("CONFIRMATION_INDEPENDENCE_UNKNOWN");
        const measurementValid =
            verification?.valid === true &&
            rows.length === 8 &&
            arms.every((arm) => arm.measurementValid) &&
            issues.length === 0 &&
            independent;
        return {
            id: point.id,
            index: point.index,
            caseId: point.caseId,
            ratePerSecond: point.ratePerSecond,
            comparison: point.comparison,
            modes: point.modes,
            sourcePoints: point.sourcePoints,
            comparisonPlanDigest: point.comparisonPlanDigest,
            executionDirectory: record?.executionDirectory ?? null,
            status: record?.status ?? "not-started",
            expectedArms: 8,
            arms,
            measurementValid,
            confirmationEligible: measurementValid,
            independence: record?.independence ?? { status: "unknown", issues: ["CONFIRMATION_INDEPENDENCE_UNKNOWN"] },
            issues: unique(issues),
            capacityQualified: false,
        };
    });
    const modePoints = points.flatMap((point) =>
        unique([point.modes.A, point.modes.B]).map((mode) => {
            const arms = point.arms.filter((arm) => arm.mode === mode),
                expectedArms = point.comparison === "aa" ? 8 : 4,
                eligible =
                    point.measurementValid &&
                    arms.length === expectedArms &&
                    arms.every((arm) => arm.measurementValid && finite(arm.correctCompletedInWindowPerSecond));
            const diagnostic =
                arms.length === expectedArms &&
                arms.every((arm) => arm.rawCountVerified && finite(arm.correctCompletedInWindowPerSecond));
            return {
                ...point,
                id: point.id + "-" + mode,
                evidencePointId: point.id,
                mode,
                arms,
                expectedArms,
                measurementValid: eligible,
                confirmationEligible: eligible,
                throughput: throughput(arms, eligible),
                latency: latency(arms, eligible),
                diagnosticObservedThroughput: {
                    status: diagnostic ? "diagnostic-only" : "unavailable",
                    qualification: false,
                    median: diagnostic ? median(arms.map((arm) => arm.correctCompletedInWindowPerSecond)) : null,
                },
                slo: plan.policy.slo === null ? "not-requested" : "unknown",
                confirmation: eligible ? "independently-remeasured-descriptive" : "unknown",
                headroom: "unknown",
                warmupStable: null,
                observerCost: "unknown",
            };
        }),
    );
    const curves = plan.suite.caseIds.flatMap((caseId) =>
        plan.modes.map((mode) => {
            const selected = modePoints.filter((point) => point.caseId === caseId && point.mode === mode),
                eligible = selected
                    .filter((point) => point.confirmationEligible)
                    .sort((a, b) => b.throughput.median - a.throughput.median || a.ratePerSecond - b.ratePerSecond),
                highest = eligible[0],
                selection = plan.selection.selections.find((row) => row.caseId === caseId && row.mode === mode);
            need(selection, "CONFIRMATION_SELECTION_DENOMINATOR");
            const confirmedHighest = highest
                ? {
                      status: "descriptive-independent-confirmation",
                      pointId: highest.evidencePointId,
                      modePointId: highest.id,
                      ratePerSecond: highest.ratePerSecond,
                      correctRequestsPerSecond: highest.throughput.median,
                      armCount: highest.expectedArms,
                      aggregation: highest.throughput.aggregation,
                      capacityQualified: false,
                      headroom: "unknown",
                      warmupStable: null,
                      observerCost: "unknown",
                  }
                : null;
            return {
                caseId,
                mode,
                selection,
                pointIds: selected.map((point) => point.id),
                confirmedHighest,
                highestObserved: confirmedHighest ?? {
                    status: "unavailable",
                    pointId: null,
                    correctRequestsPerSecond: null,
                    reason: "no-complete-independent-valid-confirmation-point; no-exploration-fallback",
                },
                capacityQualified: false,
                sloCapacity: plan.policy.slo === null ? { status: "not-requested" } : { status: "unknown" },
            };
        }),
    );
    return {
        schemaVersion: 1,
        kind: "http-capacity-confirmation-report",
        algorithm: "capacity-confirmation-v1",
        phase: "confirmation",
        planDigest: plan.planDigest,
        explorationPlanDigest: plan.exploration.planDigest,
        suite: plan.suite,
        modes: plan.modes,
        selection: plan.selection,
        plannedPoints: points.length,
        expectedArms: points.length * 8,
        validPoints: points.filter((point) => point.measurementValid).length,
        evidenceValid: points.length > 0 && points.every((point) => point.measurementValid),
        points,
        modePoints,
        curves,
        sourceIndependence: source,
        engineeringAccepted: false,
        statisticsCertified: false,
        performanceAccepted: false,
        limits: [
            "Only new confirmation arms contribute to the mode-specific descriptive score; exploration never fills failed observations",
            "A/A uses all eight arms; balanced cross-mode comparisons use four actual arms per mode",
            "Independence unknown, original quality failures and missing arms prevent confirmation",
            "No SLO capacity, sustainable limit, driver-headroom acceptance, warmup/observer qualification or statistical certification",
        ],
    };
}
