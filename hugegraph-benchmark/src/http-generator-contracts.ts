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
import { httpGoProfileSchema } from "./http-contracts.js";
const absolute = z
    .string()
    .max(2048)
    .regex(/^\/[^,\x00-\x1f]+$/);
export const httpGeneratorRequestSchema = z.strictObject({
    schemaVersion: z.literal(1),
    kind: z.literal("http-generator-calibration-request"),
    name: z.string().regex(/^[a-zA-Z0-9][a-zA-Z0-9_-]{0,31}$/),
    purpose: z.enum(["engineering-calibration", "development-verification"]),
    binary: httpGoProfileSchema.shape.executor.shape.binary,
    runtime: z.strictObject({
        image: z.string().regex(/^sha256:[a-f0-9]{64}$/),
        docker: absolute,
        hostPython: absolute,
        containerPython: absolute,
        nodeRoot: absolute,
        clockProof: absolute,
    }),
    timing: z.strictObject({
        warmupMs: z.number().int().min(0).max(60000),
        durationMs: z.number().int().min(100).max(60000),
        requestTimeoutMs: z.number().int().min(100).max(5000),
        drainMs: z.number().int().min(100).max(5000),
    }),
    budget: z.strictObject({
        wallMs: z.number().int().min(10000).max(28800000),
        mode: z.enum(["total", "per-arm"]).default("total"),
        cleanupReserveMs: z.number().int().min(5000).max(100000),
        maxEvidenceBytes: z.number().int().min(16777216).max(4294967296),
        minFreeBytes: z.number().int().min(0).max(1099511627776),
    }),
});
