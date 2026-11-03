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

import { createHash } from "node:crypto";
import { performance } from "node:perf_hooks";

export const ALGORITHM = "paired-median-percentile-bootstrap-v1";
const assert = (condition, message) => {
    if (!condition) throw new Error(message);
};
const integer = (value, low, high) => Number.isSafeInteger(value) && value >= low && value <= high;
const finite = (value) => typeof value === "number" && Number.isFinite(value);
const identifier = (value) => typeof value === "string" && /^[A-Za-z0-9][A-Za-z0-9._-]{0,95}$/.test(value);
const shaPattern = /^[a-f0-9]{64}$/;

export function canonical(value) {
    if (Array.isArray(value)) return value.map(canonical);
    if (value !== null && typeof value === "object")
        return Object.fromEntries(
            Object.keys(value)
                .sort()
                .map((key) => [key, canonical(value[key])]),
        );
    return value;
}
export const valueHash = (value) =>
    createHash("sha256")
        .update(JSON.stringify(canonical(value)))
        .digest("hex");

// Versioned, reproducible uniform index draws. Rejection avoids modulo bias for non-power-of-two N.
export class Sampler {
    constructor(seed) {
        assert(typeof seed === "string" && seed.length > 0 && seed.length <= 128, "Invalid sampler seed");
        this.seed = Buffer.from(seed, "utf8");
        this.counter = 0n;
        this.offset = 32;
        this.bytes = null;
    }
    word() {
        if (this.offset === 32) {
            const counter = Buffer.alloc(8);
            counter.writeBigUInt64BE(this.counter++);
            this.bytes = createHash("sha256").update("hgbench-paired-v1\0").update(this.seed).update(counter).digest();
            this.offset = 0;
        }
        const result = this.bytes.readUInt32BE(this.offset);
        this.offset += 4;
        return result;
    }
    index(size) {
        assert(integer(size, 1, 0xffffffff), "Invalid sample size");
        const cutoff = Math.floor(0x100000000 / size) * size;
        let word;
        do {
            word = this.word();
        } while (word >= cutoff);
        return word % size;
    }
    uniform() {
        return (this.word() + 0.5) / 0x100000000;
    }
}

export function balancedOrders(ids, seed) {
    const order = ids.map((_, i) => (i < Math.ceil(ids.length / 2) ? "AB" : "BA"));
    const random = new Sampler(seed);
    for (let i = order.length - 1; i > 0; i--) {
        const j = random.index(i + 1);
        [order[i], order[j]] = [order[j], order[i]];
    }
    return Object.fromEntries(ids.map((id, i) => [id, order[i]]));
}

function sortedMedian(values) {
    const middle = Math.floor(values.length / 2);
    return values.length % 2 ? values[middle] : values[middle - 1] / 2 + values[middle] / 2;
}
export function median(values) {
    assert(values.length > 0 && values.every(finite), "Median requires finite observations");
    return sortedMedian(Float64Array.from(values).sort());
}
export function nearestRank(sorted, probability) {
    assert(sorted.length > 0 && probability >= 0 && probability <= 1, "Invalid percentile request");
    return sorted[Math.max(0, Math.min(sorted.length - 1, Math.ceil(probability * sorted.length) - 1))];
}

// Independent order-statistic reference for an IID population median, with conservative binomial tails.
export function medianReference(values, tail) {
    const sorted = Float64Array.from(values).sort(),
        n = sorted.length;
    let mass = 2 ** -n,
        cumulative = 0,
        k = 0,
        actualTail = 0;
    for (let j = 0; j < Math.floor(n / 2); j++) {
        cumulative += mass;
        if (cumulative > tail) break;
        k = j + 1;
        actualTail = cumulative;
        mass *= (n - j) / (j + 1);
    }
    return k
        ? { finite: true, low: sorted[k - 1], high: sorted[n - k], k, iidMedianCoverageLowerBound: 1 - 2 * actualTail }
        : { finite: false, low: null, high: null, k: 0, iidMedianCoverageLowerBound: 1 };
}

function lagOne(values) {
    const center = values.reduce((a, b) => a + b / values.length, 0);
    let numerator = 0,
        denominator = 0;
    for (let i = 0; i < values.length; i++) {
        const x = values[i] - center;
        denominator += x * x;
        if (i) numerator += x * (values[i - 1] - center);
    }
    const value = numerator / denominator;
    return Number.isFinite(value) ? value : null;
}

export function validatePolicy(policy) {
    assert(policy?.schemaVersion === 1 && policy.kind === "paired-analysis-policy", "Unsupported policy");
    assert(identifier(policy.id) && policy.algorithm === ALGORITHM, "Unsupported algorithm or policy ID");
    assert(
        Array.isArray(policy.blockIds) &&
            integer(policy.blockIds.length, 2, 1000) &&
            policy.blockIds.every(identifier) &&
            new Set(policy.blockIds).size === policy.blockIds.length,
        "Invalid planned blocks",
    );
    assert(Array.isArray(policy.metrics) && integer(policy.metrics.length, 1, 128), "Invalid primary family");
    assert(new Set(policy.metrics.map((m) => m.id)).size === policy.metrics.length, "Duplicate primary metric");
    for (const m of policy.metrics) {
        assert(
            identifier(m.id) && typeof m.unit === "string" && m.unit.length > 0 && m.unit.length <= 64,
            "Invalid metric identity/unit",
        );
        assert(["lower", "higher"].includes(m.better), "Metric direction must be lower or higher");
        assert(
            finite(m.relativeBudget) && m.relativeBudget >= 0 && finite(m.absoluteBudget) && m.absoluteBudget >= 0,
            "Invalid practical effect budgets",
        );
        assert(integer(m.minimumObservations, 1, Number.MAX_SAFE_INTEGER), "Invalid per-run observation minimum");
    }
    assert(
        finite(policy.familyAlpha) && policy.familyAlpha >= 0.001 && policy.familyAlpha <= 0.2,
        "Invalid family alpha",
    );
    assert(integer(policy.minimumPairs, 2, 1000), "Invalid minimum independent pairs");
    assert(integer(policy.bootstrapReplicates, 1000, 1000000), "Invalid bootstrap replicate count");
    assert(integer(policy.minimumTailDraws, 20, 1000), "Tail resolution requires at least 20 expected draws");
    assert(
        integer(policy.maxWork, 1, 1000000000) && integer(policy.maxMilliseconds, 1, 300000),
        "Invalid analysis resource budget",
    );
    for (const seed of [policy.resampleSeed, policy.orderSeed]) new Sampler(seed);
    assert(
        shaPattern.test(policy.baselineDigest) && shaPattern.test(policy.candidateDigest),
        "Missing variant identities",
    );
    assert(
        policy.contexts &&
            Object.keys(policy.contexts).length === policy.blockIds.length &&
            policy.blockIds.every((id) => shaPattern.test(policy.contexts[id])),
        "Missing frozen block conditions",
    );
    return policy;
}

export function bootstrapPairs(relative, absolute, policy, tail, guard = () => {}) {
    const n = relative.length,
        count = policy.bootstrapReplicates;
    const r = new Float64Array(count),
        a = new Float64Array(count);
    const rs = new Float64Array(n),
        as = new Float64Array(n);
    // Same seed and N for each metric preserve shared block draws across the complete primary family.
    const random = new Sampler(policy.resampleSeed);
    for (let iteration = 0; iteration < count; iteration++) {
        if ((iteration & 255) === 0) guard();
        for (let j = 0; j < n; j++) {
            const index = random.index(n);
            rs[j] = relative[index];
            as[j] = absolute[index];
        }
        rs.sort();
        as.sort();
        r[iteration] = sortedMedian(rs);
        a[iteration] = sortedMedian(as);
    }
    r.sort();
    a.sort();
    guard();
    return {
        relative: { low: nearestRank(r, tail), high: nearestRank(r, 1 - tail) },
        absolute: { low: nearestRank(a, tail), high: nearestRank(a, 1 - tail) },
        bootstrapDistributionSha256: valueHash({ relative: Array.from(r), absolute: Array.from(a) }),
    };
}

export function budgetRelation(intervals, metric) {
    const r = intervals.relative,
        a = intervals.absolute;
    if (r.low > metric.relativeBudget && a.low > metric.absoluteBudget) return "above-both-budgets";
    if (r.high < -metric.relativeBudget && a.high < -metric.absoluteBudget) return "below-both-budgets";
    if (
        (r.low >= -metric.relativeBudget && r.high <= metric.relativeBudget) ||
        (a.low >= -metric.absoluteBudget && a.high <= metric.absoluteBudget)
    )
        return "within-at-least-one-budget";
    return "crosses-budget";
}

export function analyze(input) {
    assert(input?.schemaVersion === 1 && input.kind === "normalized-paired-observations", "Unsupported paired input");
    const policy = validatePolicy(input.policy),
        started = performance.now();
    assert(Array.isArray(input.blocks) && input.blocks.length <= 1000, "Invalid observed block inventory");
    const guard = () => assert(performance.now() - started <= policy.maxMilliseconds, "Analysis deadline exhausted");
    const planned = new Set(policy.blockIds),
        byId = new Map(),
        runIds = new Set();
    const orders = balancedOrders(policy.blockIds, policy.orderSeed),
        globalIssues = [];
    for (const block of input.blocks) {
        assert(planned.has(block.id) && !byId.has(block.id), "Unexpected or duplicate block");
        assert(["AB", "BA"].includes(block.observedOrder), "Invalid observed order");
        byId.set(block.id, block);
        if (block.observedOrder !== orders[block.id]) globalIssues.push(`order-mismatch:${block.id}`);
        for (const [arm, variant] of [
            ["baseline", policy.baselineDigest],
            ["candidate", policy.candidateDigest],
        ]) {
            const run = block[arm];
            assert(run && identifier(run.runId) && shaPattern.test(run.evidenceSha256), "Missing sealed run reference");
            const key = `${run.evidenceSha256}:${run.runId}`;
            assert(!runIds.has(key), "Physical run reference reused across arms or blocks");
            runIds.add(key);
            assert(
                ["ok", "invalid", "correctness-failed", "product-failed"].includes(run.status),
                "Invalid run quality status",
            );
            if (run.status !== "ok") globalIssues.push(`${run.status}:${block.id}:${arm}`);
            if (run.variantDigest !== variant || run.conditionsDigest !== policy.contexts[block.id])
                globalIssues.push(`incomparable:${block.id}:${arm}`);
            assert(
                run.values && Object.keys(run.values).every((id) => policy.metrics.some((m) => m.id === id)),
                "Unexpected observed metric",
            );
        }
    }
    for (const id of policy.blockIds) if (!byId.has(id)) globalIssues.push(`missing-block:${id}`);
    // Both relative and absolute statements belong to the predeclared family. Failed metrics keep their allocation.
    const statements = policy.metrics.length * 2,
        tail = policy.familyAlpha / (2 * statements);
    const work = policy.blockIds.length * policy.bootstrapReplicates * policy.metrics.length;
    assert(work <= policy.maxWork, "Bootstrap work exceeds the frozen resource budget");
    const precision = policy.bootstrapReplicates * tail >= policy.minimumTailDraws;
    const cells = [];
    for (const metric of policy.metrics) {
        guard();
        const relative = [],
            absolute = [],
            pairs = [],
            issues = [...globalIssues];
        for (const id of policy.blockIds) {
            const block = byId.get(id);
            if (!block) continue;
            const a = block.baseline.values[metric.id],
                b = block.candidate.values[metric.id];
            if (
                !a ||
                !b ||
                !finite(a.value) ||
                !finite(b.value) ||
                a.value < 0 ||
                b.value < 0 ||
                !integer(a.observations, 0, Number.MAX_SAFE_INTEGER) ||
                !integer(b.observations, 0, Number.MAX_SAFE_INTEGER)
            ) {
                issues.push(`missing-or-invalid-value:${id}`);
                continue;
            }
            pairs.push({
                blockId: id,
                observedOrder: block.observedOrder,
                baseline: a.value,
                candidate: b.value,
                baselineObservations: a.observations,
                candidateObservations: b.observations,
            });
            if (Math.min(a.observations, b.observations) < metric.minimumObservations)
                issues.push(`insufficient-observations:${id}`);
            if (a.value === 0) {
                issues.push(`zero-baseline:${id}`);
                continue;
            }
            const sign = metric.better === "lower" ? 1 : -1;
            const r = sign * (b.value / a.value - 1),
                d = sign * (b.value - a.value);
            if (!finite(r) || !finite(d)) {
                issues.push(`numerical-overflow:${id}`);
                continue;
            }
            relative.push(r);
            absolute.push(d);
        }
        const clean = issues.length === 0 && relative.length === policy.blockIds.length;
        const enoughPairs = policy.blockIds.length >= policy.minimumPairs;
        const warnings = [];
        if (!enoughPairs) warnings.push("insufficient-independent-pairs");
        if (!precision) warnings.push("insufficient-bootstrap-tail-resolution");
        const degenerate = clean && (new Set(relative).size < 2 || new Set(absolute).size < 2);
        if (degenerate) warnings.push("degenerate-paired-effects; empirical-bootstrap-does-not-establish-uncertainty");
        const intervals =
            clean && enoughPairs && precision ? bootstrapPairs(relative, absolute, policy, tail, guard) : null;
        cells.push({
            metric,
            plannedPairs: policy.blockIds.length,
            observedPairs: pairs.length,
            pairs,
            issues,
            warnings,
            point: clean
                ? {
                      relativeMedian: median(relative),
                      absoluteMedian: median(absolute),
                      baselineMedian: median(pairs.map((p) => p.baseline)),
                      candidateMedian: median(pairs.map((p) => p.candidate)),
                      relativeLagOneCorrelation: lagOne(relative),
                  }
                : null,
            nominalIntervalConfidence: 1 - policy.familyAlpha / statements,
            bootstrapIntervals: intervals,
            orderStatisticReference: clean
                ? { relative: medianReference(relative, tail), absolute: medianReference(absolute, tail) }
                : null,
            budgetRelation: intervals && !degenerate ? budgetRelation(intervals, metric) : "not-assessed",
            inferenceAccepted: false,
        });
    }
    guard();
    return {
        schemaVersion: 1,
        kind: "paired-statistical-analysis",
        algorithm: ALGORITHM,
        policySha256: valueHash(policy),
        inputValueSha256: valueHash(input),
        familyStatements: statements,
        nominalFamilyConfidence: 1 - policy.familyAlpha,
        perTailProbability: tail,
        expectedBootstrapTailDraws: policy.bootstrapReplicates * tail,
        orderAssignments: orders,
        cells,
        verdict: cells.some((c) => c.issues.length) ? "quality-issues" : "uncalibrated-statistical-analysis",
        performanceAccepted: false,
        automaticGate: "disabled",
        gates: {
            rawEvidence: "caller-supplied-references; independent-adapter-verification-required",
            blockIndependence: "assumed-not-proven; lag-correlation-is-descriptive-only",
            coverage: "requires-model-checks-and-real-A/A-calibration",
            practicalBudgets: "requires-calibration",
        },
        interpretation:
            "Positive effects mean deterioration. Block pairs, never individual requests, are resampled together. Bonferroni allocations require valid marginal coverage; they cannot repair bootstrap undercoverage. Budget relations are numerical diagnostics, not accepted regression/equivalence verdicts.",
    };
}
