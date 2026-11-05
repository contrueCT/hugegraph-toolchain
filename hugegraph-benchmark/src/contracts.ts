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

import { httpGeneratorRequestSchema } from "./http-generator-contracts.js";
import { graphProjectSchema } from "./graph-project-contracts.js";
import { z } from "zod";
import {
    httpProfileSchema,
    httpGoProfileSchema,
    httpSuiteSettingsSchema,
    graphImportSnapshotSchema,
} from "./http-contracts.js";
import { httpEnvironmentSettingsSchema } from "./http-environment-contracts.js";
import { httpRuntimeProvenanceSpecSchema } from "./http-runtime-contracts.js";
import { httpProjectSchema, httpPresetDefinitionSchema, httpPresetPlanOptionsSchema } from "./http-preset-contracts.js";
import { httpCapacityPolicySchema } from "./http-capacity-contracts.js";
import { httpCapacityExecutionRuntimeSchema } from "./http-capacity-execution-contracts.js";
import { httpCapacityConfirmationRequestSchema } from "./http-capacity-confirmation-contracts.js";
import { httpKnownRegressionInputSchema } from "./http-known-regression-contracts.js";
import { httpHeadroomInputSchema } from "./http-headroom-contracts.js";
import { httpObserverCostInputSchema } from "./http-observer-cost-contracts.js";
import { httpEngineeringAcceptanceInputSchema } from "./http-engineering-acceptance-contracts.js";
import { taskPlanSchema, frozenTaskSchema, taskStateSchema, taskEventSchema } from "./task-contracts.js";
import { loaderConfigSchema, loaderJobSchema } from "./sf100-loader.js";
import { sf100RequestSchema, sf100ReceiptSchema, sf100ObservationSchema } from "./sf100-protocol.js";
import { sf100PlanSchema, sf100ComponentManifestSchema } from "./sf100.js";

export const PROTOCOL_VERSION = 1;
export const TOOL_VERSION = "0.1.0";
const id = z.string().regex(/^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$/);
const config = z.record(z.string(), z.json()).default({});
const checksum = z.string().regex(/^[a-f0-9]{64}$/);
const finite = z.number().finite().nonnegative();

export const planSchema = z.strictObject({
    schemaVersion: z.literal(1),
    name: id,
    suite: z.literal("controller-fixture-v1"),
    executor: z.string().min(1),
    analyzer: z.string().min(1),
    cases: z.array(z.strictObject({ id, config })).min(1).max(32),
    variants: z.array(z.strictObject({ id, config })).length(2),
    blocks: z.number().int().min(1).max(20).default(2),
    seed: z.number().int().min(0).max(0xffffffff).default(3346),
    componentTimeoutMs: z.number().int().min(100).max(300000).default(30000),
});
export type Plan = z.infer<typeof planSchema>;

export const manifestSchema = z.strictObject({
    protocolVersion: z.literal(1),
    id,
    version: z.string().min(1),
    kind: z.enum(["executor", "analyzer"]),
    command: z.array(z.string().min(1)).min(1).max(32),
    trackedFiles: z.array(z.string().min(1)).min(1).max(32),
});
export const componentSchema = z.strictObject({
    manifest: manifestSchema,
    manifestPath: z.string(),
    command: z.array(z.string()).min(1),
    files: z.array(z.strictObject({ path: z.string(), sha256: checksum })).min(1),
});
export type Component = z.infer<typeof componentSchema>;

export const capabilitySchema = z.strictObject({
    protocolVersion: z.literal(1),
    id,
    version: z.string(),
    kind: z.enum(["executor", "analyzer"]),
    scopes: z.array(z.literal("controller-fixture")).min(1),
    configurationSchema: z.json(),
});

export const resolvedSchema = z.strictObject({
    schemaVersion: z.literal(1),
    toolVersion: z.literal(TOOL_VERSION),
    scope: z.literal("controller-fixture"),
    plan: planSchema,
    executor: componentSchema,
    analyzer: componentSchema,
    digest: checksum,
});
export type Resolved = z.infer<typeof resolvedSchema>;

export const executionRequestSchema = z.strictObject({
    protocolVersion: z.literal(1),
    kind: z.literal("execution"),
    scope: z.literal("controller-fixture"),
    experimentId: id,
    runId: id,
    caseId: id,
    variantId: id,
    block: z.number().int().nonnegative(),
    seed: z.number().int(),
    caseConfig: config,
    variantConfig: config,
});
export type ExecutionRequest = z.infer<typeof executionRequestSchema>;

export const executionResultSchema = z.strictObject({
    protocolVersion: z.literal(1),
    kind: z.literal("execution"),
    scope: z.literal("controller-fixture"),
    experimentId: id,
    runId: id,
    caseId: id,
    variantId: id,
    block: z.number().int().nonnegative(),
    samples: z
        .array(z.strictObject({ durationMs: finite, correct: z.boolean() }))
        .min(1)
        .max(100000),
    validation: z.strictObject({ checked: z.number().int().positive(), failed: z.number().int().nonnegative() }),
});
export type ExecutionResult = z.infer<typeof executionResultSchema>;

export const analysisRequestSchema = z.strictObject({
    protocolVersion: z.literal(1),
    kind: z.literal("analysis"),
    scope: z.literal("controller-fixture"),
    experimentId: id,
    planDigest: checksum,
    plan: planSchema,
    results: z.array(executionResultSchema).min(1),
});

export const analysisResultSchema = z.strictObject({
    protocolVersion: z.literal(1),
    kind: z.literal("analysis"),
    scope: z.literal("controller-fixture"),
    experimentId: id,
    planDigest: checksum,
    verdict: z.enum(["descriptive-only", "correctness-failed"]),
    summary: z.json(),
    reportHtml: z
        .string()
        .min(1)
        .max(4 * 1024 * 1024),
});
export type AnalysisResult = z.infer<typeof analysisResultSchema>;

export const stateSchema = z.strictObject({
    schemaVersion: z.literal(1),
    experimentId: id,
    planDigest: checksum,
    state: z.enum(["queued", "running", "completed", "failed", "cancelled"]),
    phase: z.string(),
    createdAt: z.string(),
    updatedAt: z.string(),
    workerPid: z.number().int().optional(),
    completedRuns: z.number().int().nonnegative(),
    totalRuns: z.number().int().positive(),
    verdict: z.enum(["descriptive-only", "correctness-failed"]).optional(),
    error: z.strictObject({ code: z.string(), message: z.string() }).optional(),
    parentExperimentId: id.optional(),
});
export type State = z.infer<typeof stateSchema>;
export const terminal = (state: State) => ["completed", "failed", "cancelled"].includes(state.state);

export const schemas: Record<string, z.ZodType> = {
    "http-generator-calibration-request": httpGeneratorRequestSchema,
    "http-profile": httpProfileSchema,
    "http-go-profile": httpGoProfileSchema,
    "http-suite-settings": httpSuiteSettingsSchema,
    "http-project": httpProjectSchema,
    "http-preset-definition": httpPresetDefinitionSchema,
    "http-preset-plan-options": httpPresetPlanOptionsSchema,
    "http-capacity-policy": httpCapacityPolicySchema,
    "http-capacity-execution-runtime": httpCapacityExecutionRuntimeSchema,
    "http-capacity-confirmation-request": httpCapacityConfirmationRequestSchema,
    "http-known-regression-evidence-input": httpKnownRegressionInputSchema,
    "http-driver-headroom-evidence-input": httpHeadroomInputSchema,
    "http-observer-cost-evidence-input": httpObserverCostInputSchema,
    "http-engineering-acceptance-input": httpEngineeringAcceptanceInputSchema,
    "http-environment-settings": httpEnvironmentSettingsSchema,
    "http-runtime-provenance-spec": httpRuntimeProvenanceSpecSchema,
    "task-plan": taskPlanSchema,
    "task-frozen": frozenTaskSchema,
    "task-state": taskStateSchema,
    "task-event": taskEventSchema,
    "sf100-request": sf100RequestSchema,
    "sf100-receipt": sf100ReceiptSchema,
    "sf100-load-observation": sf100ObservationSchema,
    "sf100-loader-config": loaderConfigSchema,
    "sf100-loader-job": loaderJobSchema,
    "graph-import-snapshot": graphImportSnapshotSchema,
    "graph-project": graphProjectSchema,
    "sf100-plan": sf100PlanSchema,
    "sf100-component": sf100ComponentManifestSchema,
    plan: planSchema,
    resolved: resolvedSchema,
    component: manifestSchema,
    capabilities: capabilitySchema,
    "executor-request": executionRequestSchema,
    "executor-result": executionResultSchema,
    "analyzer-request": analysisRequestSchema,
    "analyzer-result": analysisResultSchema,
    state: stateSchema,
};
