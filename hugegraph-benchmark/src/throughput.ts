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

import { z } from "zod";

// Shared policy only: workload adapters own requests, answers and execution.
export const throughputSettingsSchema = z
    .strictObject({
        concurrency: z.array(z.number().int().min(1).max(256)).min(1).max(16),
        exploreWarmupMs: z.number().int().min(0).max(60000),
        exploreDurationMs: z.number().int().min(100).max(300000),
        confirmWarmupMs: z.number().int().min(0).max(60000),
        confirmDurationMs: z.number().int().min(100).max(300000),
        confirmRepeats: z.number().int().min(1).max(10),
        confirmNeighbors: z.boolean(),
        plateauGain: z.number().min(0).max(0.5),
        maxRequestsPerPhase: z.number().int().min(1).max(1000000),
        maxTraceBytes: z.number().int().min(4096).max(1073741824),
        maxTotalTraceBytes: z.number().int().min(4096).max(68719476736),
        minFreeBytes: z.number().int().min(0).max(1099511627776),
    })
    .superRefine((v, c) => {
        if (v.concurrency.some((n, i) => i > 0 && n <= v.concurrency[i - 1]!))
            c.addIssue({ code: "custom", message: "Concurrency must be strictly increasing" });
    });
export type ThroughputSettings = z.infer<typeof throughputSettingsSchema>;
const limits = {
    maxRequestsPerPhase: 1000000,
    maxTraceBytes: 1073741824,
    maxTotalTraceBytes: 8589934592,
    minFreeBytes: 2147483648,
};
export const throughputPresets = {
    quick: {
        ...limits,
        concurrency: [1, 4, 16],
        exploreWarmupMs: 2000,
        exploreDurationMs: 8000,
        confirmWarmupMs: 5000,
        confirmDurationMs: 30000,
        confirmRepeats: 1,
        confirmNeighbors: false,
        plateauGain: 0.05,
    },
    standard: {
        ...limits,
        concurrency: [1, 2, 4, 8, 16, 32],
        exploreWarmupMs: 5000,
        exploreDurationMs: 10000,
        confirmWarmupMs: 5000,
        confirmDurationMs: 30000,
        confirmRepeats: 3,
        confirmNeighbors: true,
        plateauGain: 0.05,
    },
} satisfies Record<string, ThroughputSettings>;
export const graphThroughputPresets = {
    quick: {
        ...throughputPresets.quick,
        exploreWarmupMs: 5000,
        exploreDurationMs: 15000,
        confirmWarmupMs: 5000,
        confirmDurationMs: 60000,
    },
    standard: {
        ...throughputPresets.standard,
        exploreWarmupMs: 10000,
        exploreDurationMs: 20000,
        confirmWarmupMs: 15000,
        confirmDurationMs: 60000,
    },
} satisfies Record<string, ThroughputSettings>;
export type ThroughputPoint = { concurrency: number; goodput: number; eligible: boolean };
export function selectThroughputPoints(points: ThroughputPoint[], settings: ThroughputSettings) {
    const valid = points.filter((p) => p.eligible && p.goodput > 0);
    if (!valid.length)
        return { candidate: null, confirmation: [], plateauObserved: false, reason: "no-eligible-point" };
    const best = valid.reduce((a, b) => (b.goodput > a.goodput ? b : a));
    // Choose the least concurrency within the declared tolerance of the best observation.
    const candidate = valid.find((p) => p.goodput >= best.goodput * (1 - settings.plateauGain))!;
    const index = points.indexOf(candidate);
    const plateauObserved =
        index < points.length - 1 &&
        points[index + 1]!.eligible &&
        points[index + 1]!.goodput <= candidate.goodput * (1 + settings.plateauGain);
    const neighbor = points[index + 1] ?? points[index - 1];
    return {
        candidate: candidate.concurrency,
        confirmation: [candidate.concurrency, ...(settings.confirmNeighbors && neighbor ? [neighbor.concurrency] : [])],
        plateauObserved,
        reason: plateauObserved ? "candidate-platform-observed" : "platform-not-established",
    };
}
export function throughputFixedMs(cases: number, s: ThroughputSettings) {
    return (
        cases *
        (s.concurrency.length * (s.exploreWarmupMs + s.exploreDurationMs) +
            Math.min(s.confirmNeighbors ? 2 : 1, s.concurrency.length) *
                s.confirmRepeats *
                (s.confirmWarmupMs + s.confirmDurationMs))
    );
}

export function httpPointHealth(row: { correct: number; failed: number; dropped: number; issues: string[] }) {
    const unknown = row.issues.filter((s) => s.endsWith(":go-resource-isolation-unverified"));
    const blockers = row.issues.filter((s) => !unknown.includes(s));
    return {
        eligible: row.correct > 0 && row.failed === 0 && row.dropped === 0 && blockers.length === 0,
        blockers,
        unknown,
        qualification: "observation-only",
    };
}

// Both tracks supply an adapter; sequence, selection and fresh confirmation windows stay shared.
export async function runThroughputSchedule<T>(
    cases: T[],
    settings: ThroughputSettings,
    adapter: {
        explore(item: T): Promise<ThroughputPoint[]>;
        selected(item: T, selection: ReturnType<typeof selectThroughputPoints>): Promise<void>;
        confirm(item: T, concurrency: number[]): Promise<void>;
        completed(item: T, confirmed: boolean): Promise<void>;
    },
) {
    for (const item of cases) {
        const points = await adapter.explore(item);
        const selection = selectThroughputPoints(points, settings);
        await adapter.selected(item, selection);
        if (selection.confirmation.length) await adapter.confirm(item, selection.confirmation);
        await adapter.completed(item, selection.confirmation.length > 0);
    }
}
