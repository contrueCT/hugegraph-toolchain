/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
import { z } from "zod";
import { httpSuiteSettingsSchema, httpGoProfileSchema } from "./http-contracts.js";

const positive = z.number().int().positive();
const reference = z.string().min(1).max(2048);
const nodeProjectSchema = z.strictObject({
    schemaVersion: z.literal(1),
    kind: z.literal("http-benchmark-project"),
    id: z.string().regex(/^[A-Za-z0-9][A-Za-z0-9_-]{0,39}$/),
    label: z.string().min(1).max(256),
    description: z.string().min(1).max(1024),
    comparison: z.enum(["forward-embedded", "aa"]).default("forward-embedded"),
    aaMode: z.enum(["forward", "embedded"]).default("forward"),
    sources: z.strictObject({ build: reference, settings: reference, environment: reference, runtime: reference }),
});
export const httpProjectSchema = z.discriminatedUnion("schemaVersion", [
    nodeProjectSchema,
    nodeProjectSchema.extend({
        schemaVersion: z.literal(2),
        executor: httpGoProfileSchema.shape.executor.extend({ hostIdentityGate: z.literal(true) }),
    }),
]);
export type HttpProject = z.infer<typeof httpProjectSchema>;
export const httpPresetPlanOptionsSchema = z.strictObject({
    preset: z.enum(["quick", "standard", "full"]).default("quick"),
    out: reference,
    dryRun: z.boolean().default(false),
    budgetMs: positive.max(7 * 86400000).optional(),
    budgetMode: z.enum(["total", "stage-sum"]).default("total"),
});
const threshold = z.strictObject({
    relativeBudget: z.number().finite().nonnegative(),
    absoluteBudget: z.number().finite().nonnegative(),
    minimumObservations: positive,
});
export const httpPresetDefinitionSchema = z.strictObject({
    schemaVersion: z.literal(1),
    kind: z.literal("http-benchmark-preset"),
    id: z.enum(["quick", "standard", "full"]),
    version: z.literal("0.1.0"),
    planImplemented: z.boolean(),
    realExecutionValidated: z.literal(false),
    scope: z.string().min(1),
    engineeringTargetMilliseconds: z.strictObject({ low: positive, high: positive.nullable() }),
    design: z.strictObject({
        coverage: z.string().min(1),
        loadDesign: z.string().min(1),
        repeats: z.string().min(1),
        primary: z.string().min(1),
        samplePolicy: z.string().min(1),
        calibration: z.string().min(1),
    }),
    workload: httpSuiteSettingsSchema
        .pick({
            caseIds: true,
            responseEncoding: true,
            loads: true,
            repeats: true,
            warmupMs: true,
            durationMs: true,
            requestTimeoutMs: true,
            drainMs: true,
            maxRequestsPerPhase: true,
            maxResponseBytes: true,
            maxTraceBytes: true,
        })
        .nullable(),
    analysis: z
        .strictObject({
            familyAlpha: z.number().positive().max(0.2),
            minimumPairs: positive,
            bootstrapReplicates: positive,
            minimumTailDraws: positive,
            maxWork: positive,
            maxMilliseconds: positive,
            latency: threshold.extend({ quantile: z.enum(["p50", "p95", "p99"]) }),
            goodput: threshold,
        })
        .nullable(),
    lifecycleBudgets: z
        .strictObject({
            prepareMs: positive,
            startMs: positive,
            modeProbeMs: positive,
            settleMs: z.number().int().nonnegative(),
            stopMs: positive,
            sealAndAnalyzeMs: positive,
            maxEvidenceBytesPerArm: positive,
            maxTotalEvidenceBytes: positive,
            maxTotalMilliseconds: positive,
        })
        .nullable(),
    blockIds: z.array(z.string().min(1)).nullable(),
    limitations: z.array(z.string().min(1)).min(1),
});
export type HttpPreset = z.infer<typeof httpPresetDefinitionSchema>;
