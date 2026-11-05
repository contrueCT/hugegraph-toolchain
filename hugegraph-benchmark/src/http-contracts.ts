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

const id = z.string().regex(/^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$/);
const integer = (min: number, max: number) => z.number().int().min(min).max(max);
const sha256 = z.string().regex(/^[a-f0-9]{64}$/);
const pointer = z
    .string()
    .regex(/^(?:\/(?:[^~]|~[01])*)?$/)
    .max(256);
export const httpAnswerCheckSchema = z.union([
    z.strictObject({ pointer, equals: z.json() }),
    z.strictObject({ pointer, canonicalSha256: sha256 }),
]);
// Opt-in graph request protocol. Existing HTTP cases remain fixed POST.
export const graphRuleSchema = z.discriminatedUnion("kind", [
    z.strictObject({ kind: z.literal("vertex"), id: z.string().min(1), label: z.string().min(1), absent: z.boolean() }),
    z.strictObject({
        kind: z.literal("id-set"),
        pointer,
        allowed: z.array(z.string().min(1)).max(100000),
        count: integer(0, 100000),
    }),
    z.strictObject({
        kind: z.literal("shortest-path"),
        pointer,
        source: z.string().min(1),
        target: z.string().min(1),
        distance: integer(-1, 3),
        edges: z.array(z.tuple([z.string().min(1), z.string().min(1)])).max(100000),
    }),
]);
export const graphRequestSchema = z.strictObject({
    id,
    stratum: id,
    method: z.enum(["GET", "POST"]),
    url: z.url(),
    body: z.json(),
    expectedStatus: integer(100, 599),
    rule: graphRuleSchema,
});
export const graphImportSnapshotSchema = z.strictObject({
    schemaVersion: z.literal(1),
    kind: z.literal("graph-import-snapshot"),
    sourceManifestSha256: sha256,
    acknowledgedPrefixSha256: sha256,
    vertices: integer(1, Number.MAX_SAFE_INTEGER),
    edges: integer(1, Number.MAX_SAFE_INTEGER),
    expectedVertices: integer(1, Number.MAX_SAFE_INTEGER),
    expectedEdges: integer(1, Number.MAX_SAFE_INTEGER),
    writersStopped: z.literal(true),
    basis: z.literal("loader-acknowledgements"),
    vertexContentVerified: z.boolean(),
    edgeContentVerified: z.boolean(),
    labels: z
        .array(
            z.strictObject({
                category: z.enum(["vertices", "edges"]),
                label: z.string().min(1),
                expected: z.string().regex(/^(0|[1-9][0-9]*)$/),
                acknowledged: z.string().regex(/^(0|[1-9][0-9]*)$/),
                missing: z.string().regex(/^(0|[1-9][0-9]*)$/),
            }),
        )
        .min(1),
});
export const graphBindingSchema = z.strictObject({
    suiteId: z.literal("hgbench-graph-query-v0.1"),
    version: z.literal("0.1.0"),
    bundleSha256: sha256,
    dataset: z.strictObject({
        id: z.string().min(1).max(256),
        scaleFactor: z.union([z.literal(1), z.literal(10), z.literal(100)]),
        manifestSha256: sha256,
        vertices: integer(1, Number.MAX_SAFE_INTEGER),
        edges: integer(1, Number.MAX_SAFE_INTEGER),
    }),
    imported: z
        .strictObject({
            snapshot: graphImportSnapshotSchema,
            artifactSha256: sha256,
            minimumEdgeCoverage: z.number().positive().max(1),
        })
        .optional(),
    reference: z.strictObject({ origin: z.literal("independent-csv"), artifactSha256: sha256 }),
    selection: z.literal("sequence-modulo-pool-v1"),
});
export const httpBenchmarkBindingSchema = z.strictObject({
    suiteId: z.literal("hgbench-http-serving-v0.1"),
    version: z.literal("0.1.0-draft.1"),
    bundleSha256: sha256,
    caseIds: z.array(id).min(1).max(16),
    coverage: z.enum(["full", "subset"]),
    purpose: z.enum(["functional", "calibration"]),
});
export const httpClockReferenceSchema = z.strictObject({
    python: z
        .string()
        .regex(/^\/[^\x00-\x1f]+$/)
        .max(2048),
    spec: z
        .strictObject({
            schemaVersion: z.literal(1),
            kind: z.literal("http-runner-sanity-spec"),
            sourceOrigin: z.enum(["fixture", "linux-procfs"]),
            startupMs: integer(100, 10000),
            exchangeMs: integer(100, 10000),
            finishMs: integer(50, 2000),
            wallMs: integer(1000, 600000),
            maxFrameBytes: integer(4096, 16384),
            maxRawBytes: integer(16384, 262144),
            maxStderrBytes: integer(0, 4096),
        })
        .superRefine((spec, context) => {
            if (spec.startupMs + 2 * spec.exchangeMs + spec.finishMs > spec.wallMs)
                context.addIssue({
                    code: "custom",
                    message: "Clock reference wall budget does not cover its exchanges",
                });
            if (4 * spec.maxFrameBytes > spec.maxRawBytes)
                context.addIssue({
                    code: "custom",
                    message: "Clock reference raw budget must reserve at least four frames",
                });
        }),
});
export const httpProfileSchema = z.strictObject({
    schemaVersion: z.literal(1),
    kind: z.literal("http-benchmark-profile"),
    name: id,
    benchmark: httpBenchmarkBindingSchema.optional(),
    graph: graphBindingSchema.optional(),
    serviceChecks: z.boolean().default(false),
    clockReference: httpClockReferenceSchema.optional(),
    endpoint: z.url(),
    responseEncoding: z.enum(["identity", "gzip"]).default("identity"),
    authorizationEnv: z
        .string()
        .regex(/^[A-Z][A-Z0-9_]{0,63}$/)
        .nullable()
        .default(null),
    target: z.strictObject({
        label: z.string().min(1).max(256),
        build: z.string().min(1).max(256),
        mode: z.string().min(1).max(128),
        environment: z.string().min(1).max(2048),
        evidenceFiles: z.array(z.string().min(1)).max(32).default([]),
    }),
    cases: z
        .array(
            z.strictObject({
                id,
                body: z.json(),
                expectedStatus: integer(100, 599).default(200),
                checks: z.array(httpAnswerCheckSchema).min(1).max(32),
                requestPool: z.array(graphRequestSchema).min(1).max(100000).optional(),
            }),
        )
        .min(1)
        .max(16),
    loads: z
        .array(
            z.discriminatedUnion("model", [
                z.strictObject({ id, model: z.literal("closed"), concurrency: integer(1, 256) }),
                z.strictObject({
                    id,
                    model: z.literal("open"),
                    ratePerSecond: z.number().min(0.1).max(10000),
                    maxInFlight: integer(1, 256),
                    maxStartDelayMs: integer(1, 1000),
                }),
            ]),
        )
        .min(1)
        .max(16),
    repeats: integer(1, 20),
    warmupMs: integer(0, 60000),
    durationMs: integer(100, 300000),
    requestTimeoutMs: integer(10, 60000),
    drainMs: integer(10, 60000),
    maxRequestsPerPhase: integer(1, 1000000),
    maxResponseBytes: integer(256, 4194304),
    maxTraceBytes: integer(4096, 1073741824),
});
export type HttpProfile = z.infer<typeof httpProfileSchema>;

export const httpGoProfileSchema = httpProfileSchema.omit({ clockReference: true }).extend({
    schemaVersion: z.literal(2),
    executor: z.strictObject({
        engine: z.literal("go-http-v1"),
        binary: z.strictObject({
            path: z.string().min(1).max(2048),
            sha256,
            sourceSha256: sha256,
            goVersion: z.literal("go1.27.3"),
            goos: z.enum(["linux", "darwin"]),
            goarch: z.enum(["amd64", "arm64"]),
        }),
        gomaxprocs: integer(1, 32),
        maxPendingBytes: integer(65536, 1048576),
        cpuQuotaCores: z.number().finite().min(0.1).max(32),
        maxRssBytes: integer(67108864, 34359738368),
        hostIdentityGate: z.boolean().optional(),
    }),
});
export type HttpGoProfile = z.infer<typeof httpGoProfileSchema>;
export const httpAnyProfileSchema = z.union([httpProfileSchema, httpGoProfileSchema]);

export const httpSuiteSettingsSchema = httpProfileSchema
    .omit({ cases: true, benchmark: true, serviceChecks: true })
    .extend({
        kind: z.literal("http-suite-settings"),
        caseIds: z.array(id).min(1).max(16),
        purpose: z.enum(["functional", "calibration"]),
    });
