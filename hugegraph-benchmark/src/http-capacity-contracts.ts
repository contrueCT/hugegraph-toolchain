/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
import { z } from "zod";
import { httpSuiteSettingsSchema } from "./http-contracts.js";

const positive = z.number().int().positive();
const nonnegative = z.number().int().nonnegative();
const threshold = z.strictObject({
    relativeBudget: z.number().finite().nonnegative(),
    absoluteBudget: z.number().finite().nonnegative(),
    minimumObservations: positive.min(100),
});
export const httpCapacityPolicyV1Schema = z.strictObject({
    schemaVersion: z.literal(1),
    kind: z.literal("http-capacity-policy"),
    id: z.string().regex(/^[A-Za-z0-9][A-Za-z0-9_-]{0,39}$/),
    scope: z.literal("all-eleven-single-case"),
    status: z.literal("provisional-calibration-pending"),
    // Omission preserves the legacy whole-study stop contract and its exact serialized policy.
    continuation: z
        .strictObject({
            schemaVersion: z.literal(1),
            mode: z.literal("verified-case-range"),
            maxVerificationMilliseconds: positive.min(1000).max(3600000),
            maxVerificationBytes: positive.min(1048576).max(268435456),
        })
        .optional(),
    rates: z.array(z.number().finite().min(0.1).max(10000)).min(1).max(16),
    http: httpSuiteSettingsSchema
        .pick({
            responseEncoding: true,
            requestTimeoutMs: true,
            drainMs: true,
            maxRequestsPerPhase: true,
            maxResponseBytes: true,
            maxTraceBytes: true,
        })
        .extend({ maxInFlight: positive.max(256), maxStartDelayMs: positive.max(1000) }),
    exploration: z.strictObject({
        warmupMs: nonnegative.max(60000),
        durationMs: positive.min(100).max(300000),
        pairs: z.literal(2),
    }),
    confirmation: z.strictObject({
        warmupMs: nonnegative.max(60000),
        durationMs: positive.min(100).max(300000),
        pairs: z.literal(4),
        selection: z.literal("candidate-and-adjacent-points"),
        requiresNewPlan: z.literal(true),
    }),
    tailPolicy: z.strictObject({ p99MinimumObservations: z.literal(1000), belowMinimum: z.literal("unknown") }),
    analysis: z.strictObject({
        familyAlpha: z.number().positive().max(0.2),
        minimumPairs: z.literal(2),
        bootstrapReplicates: positive.max(1000000),
        minimumTailDraws: positive,
        maxWork: positive,
        maxMilliseconds: positive.max(300000),
        latency: threshold.extend({ quantile: z.literal("p50") }),
        goodput: threshold,
    }),
    pointBudgets: z.strictObject({
        prepareMs: positive.max(300000),
        startMs: positive.max(300000),
        modeProbeMs: positive.min(10000).max(180000),
        settleMs: nonnegative.max(300000),
        stopMs: positive.max(300000),
        sealAndAnalyzeMs: positive.max(300000),
        maxEvidenceBytesPerArm: positive.min(4096).max(1024 ** 3),
        maxTotalEvidenceBytes: positive.max(1024 ** 4),
        maxTotalMilliseconds: positive.max(7 * 86400000),
    }),
    controllerBudgets: z.strictObject({
        perPointMilliseconds: positive.max(86400000),
        totalMilliseconds: positive.max(86400000),
    }),
    studyBudgets: z.strictObject({
        maxTotalMilliseconds: positive.max(7 * 86400000),
        maxEvidenceBytes: positive.max(128 * 1024 ** 3),
        maxPlanBytes: positive.min(4096).max(256 * 1024 ** 2),
    }),
    slo: z
        .strictObject({
            endToEndQuantile: z.enum(["p50", "p95", "p99"]),
            maxLatencyMs: z.number().finite().positive(),
            maxErrorRate: z.number().finite().min(0).max(1),
            maxTimeoutRate: z.number().finite().min(0).max(1),
        })
        .nullable()
        .default(null),
});
// Version 2 uses one independent observation per exploratory point. Confirmation
// retains the paired policy above; exploratory observations never become pairs.
export const httpCapacityPolicyV2Schema = httpCapacityPolicyV1Schema.extend({
    schemaVersion: z.literal(2),
    budgetMode: z.enum(["total", "stage-sum"]).default("total"),
    exploration: z.strictObject({
        mode: z.literal("single-run"),
        runs: z.literal(1),
        warmupMs: nonnegative.max(60000),
        durationMs: positive.min(100).max(300000),
    }),
});
export const httpCapacityPolicySchema = z.discriminatedUnion("schemaVersion", [
    httpCapacityPolicyV1Schema,
    httpCapacityPolicyV2Schema,
]);
export type HttpCapacityPolicy = z.infer<typeof httpCapacityPolicySchema>;
export const isSingleRunCapacity = (policy: HttpCapacityPolicy): boolean => policy.schemaVersion === 2;
