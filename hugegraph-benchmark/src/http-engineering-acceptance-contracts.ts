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

const path = z
    .string()
    .min(1)
    .max(4096)
    .regex(/^[^\r\n\0]+$/);
const sha = z.string().regex(/^[a-f0-9]{64}$/);
const reader = z.strictObject({ inputFile: path, sha256: sha }).nullable();
const currentAa = z.strictObject({
    planDirectory: path,
    planLockSha256: sha,
    executionSetFile: path,
    executionSetSha256: sha,
});
const evidenceReaders = {
    knownRegression: reader,
    driverHeadroom: reader,
    observerCost: reader,
};

export const httpEngineeringAcceptanceInputSchema = z.strictObject({
    schemaVersion: z.literal(1),
    kind: z.literal("http-engineering-acceptance-input"),
    evidence: z.union([
        z.strictObject({ aa005: z.null(), repeatabilityAa: currentAa.nullable(), ...evidenceReaders }),
        z.strictObject({
            aa005: z.strictObject({ evidenceRoot: path }),
            repeatabilityAa: z.null(),
            ...evidenceReaders,
        }),
    ]),
    comparison: z
        .strictObject({
            planDirectory: path,
            planLockSha256: sha,
            calibrationIndexFile: path,
            calibrationIndexSha256: sha,
        })
        .nullable(),
});

export const httpEngineeringAcceptanceOptionsSchema = z.strictObject({
    timeoutMs: z.number().int().min(1000).max(86400000),
    maxBytes: z
        .number()
        .int()
        .min(1024 ** 2)
        .max(8 * 1024 ** 3),
});
