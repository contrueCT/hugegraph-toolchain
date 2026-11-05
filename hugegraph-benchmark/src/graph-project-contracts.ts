/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import { z } from "zod";
const positive = z.number().int().positive().safe();
export const graphProjectSchema = z.strictObject({
    schemaVersion: z.literal(1),
    kind: z.literal("graph-benchmark-project"),
    scaleFactor: z.union([z.literal(1), z.literal(10), z.literal(100)]).default(1),
    dataset: z.string().optional(),
    profile: z.string().min(1),
    settings: z.string().optional(),
    graphUrl: z.url(),
    traversalSource: z.string().regex(/^[a-zA-Z0-9_-]{1,128}$/),
    loaderHome: z.string().min(1),
    minimumEdgeCoverage: z.number().positive().max(1).default(0.95),
    loader: z
        .strictObject({
            batchSize: positive.default(2000),
            batchThreads: positive.max(256).default(8),
            parserThreads: positive.max(256).default(2),
            heapMiB: positive.default(1024),
            timeoutSeconds: positive.max(3600).default(120),
            processTimeoutMs: positive.max(86400000).default(3600000),
            maxLogBytes: positive.max(64 * 1024 ** 2).default(16 * 1024 ** 2),
        })
        .default(() => ({
            batchSize: 2000,
            batchThreads: 8,
            parserThreads: 2,
            heapMiB: 1024,
            timeoutSeconds: 120,
            processTimeoutMs: 3600000,
            maxLogBytes: 16 * 1024 ** 2,
        })),
});
