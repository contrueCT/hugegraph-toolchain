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

export const httpObserverCostInputSchema = z.strictObject({
    schemaVersion: z.literal(1),
    kind: z.literal("http-observer-cost-evidence-input"),
    operationVersion: z.literal("http-observer-cost-003"),
    planDirectory: path,
    executionDirectory: path,
    reference: z.strictObject({
        planLockSha256: sha,
        executionSpecSha256: sha,
        sourceSelectionSha256: z.literal("2b878425d6b5f9968777a14c3bd1622a6fc3349b7d5f88c139c773556dfde89a"),
    }),
});

export const httpObserverCostReportOptionsSchema = z.strictObject({
    timeoutMs: z.number().int().min(1000).max(86400000),
    maxBytes: z
        .number()
        .int()
        .min(1024 ** 2)
        .max(8 * 1024 ** 3),
});
