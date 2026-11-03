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

// Coverage is calculated from completed source shards, never from the number of shards.
export function importCoverage(jobs, acknowledged, minimumEdgeCoverage = 0.95, requiredEdgeLabels = []) {
    if (!Number.isFinite(minimumEdgeCoverage) || minimumEdgeCoverage <= 0 || minimumEdgeCoverage > 1)
        throw Error("minimumEdgeCoverage must be greater than 0 and at most 1");
    if (!Number.isSafeInteger(acknowledged) || acknowledged < 0 || acknowledged > jobs.length)
        throw Error("Invalid acknowledged prefix");
    const labels = new Map();
    const seen = new Set();
    for (const [index, job] of jobs.entries()) {
        const vertex = job.mapping.vertices.length === 1 && job.mapping.edges.length === 0;
        const edge = job.mapping.edges.length === 1 && job.mapping.vertices.length === 0;
        if ((!vertex && !edge) || seen.has(job.request.shard.id)) throw Error("Invalid or duplicate source shard");
        seen.add(job.request.shard.id);
        const category = vertex ? "vertices" : "edges";
        const label = job.mapping[category][0].label ?? "unknown";
        const key = category + ":" + label;
        const row = labels.get(key) ?? { category, label, expected: 0n, acknowledged: 0n };
        if (!/^(0|[1-9][0-9]*)$/.test(job.request.shard.records)) throw Error("Invalid source record count");
        const count = BigInt(job.request.shard.records);
        row.expected += count;
        if (index < acknowledged) row.acknowledged += count;
        labels.set(key, row);
    }
    const totals = Object.fromEntries(
        ["vertices", "edges"].map((category) => {
            const rows = [...labels.values()].filter((r) => r.category === category);
            return [
                category,
                {
                    expected: rows.reduce((s, r) => s + r.expected, 0n),
                    acknowledged: rows.reduce((s, r) => s + r.acknowledged, 0n),
                },
            ];
        }),
    );
    // Decimal threshold uses integer arithmetic, including for counts above JS safe integers.
    const [coefficient, exponent = "0"] = String(minimumEdgeCoverage).split("e");
    const digits = coefficient.replace(".", "");
    const scale = (coefficient.split(".")[1]?.length ?? 0) - Number(exponent);
    const numerator = BigInt(digits) * 10n ** BigInt(Math.max(0, -scale));
    const denominator = 10n ** BigInt(Math.max(0, scale));
    const requiredLabelsComplete = requiredEdgeLabels.every((label) => {
        const row = labels.get("edges:" + label);
        return row && row.acknowledged === row.expected;
    });
    return {
        minimumEdgeCoverage,
        accepted:
            totals.vertices.expected > 0n &&
            totals.vertices.acknowledged === totals.vertices.expected &&
            totals.edges.expected > 0n &&
            totals.edges.acknowledged * denominator >= totals.edges.expected * numerator &&
            requiredLabelsComplete,
        requiredEdgeLabels,
        requiredLabelsComplete,
        fullImport: Object.values(totals).every((row) => row.acknowledged === row.expected),
        basis: "acknowledged-source-records",
        contentVerified: false,
        datasetVerified: false,
        counts: Object.fromEntries(
            Object.entries(totals).map(([category, row]) => [
                category,
                {
                    expected: String(row.expected),
                    acknowledged: String(row.acknowledged),
                    fraction: row.expected ? Number(row.acknowledged) / Number(row.expected) : 0,
                },
            ]),
        ),
        labels: [...labels.values()].map((row) => ({
            ...row,
            expected: String(row.expected),
            acknowledged: String(row.acknowledged),
            missing: String(row.expected - row.acknowledged),
        })),
        completedShards: acknowledged,
        remainingShards: jobs.length - acknowledged,
    };
}
