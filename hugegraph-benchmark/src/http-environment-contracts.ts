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

const integer = (low: number, high: number) => z.number().int().min(low).max(high);
const absolute = z
    .string()
    .startsWith("/")
    .regex(/^[^\r\n\0,]+$/);
export const httpJvmObservationSchema = z.strictObject({
    schemaVersion: z.literal(1),
    kind: z.literal("http-jvm-observation-spec"),
    preset: z.literal("hotspot17-gc-jit-v1"),
    capture: z.literal("supervisor-stdout"),
    maxRawBytes: integer(4096, 268435456),
    windowMs: integer(100, 60000),
    parser: z.strictObject({
        maxLineBytes: integer(256, 65536),
        maxEvents: integer(1, 1000000),
        maxWindows: integer(1, 10000),
        maxOutputBytes: integer(2101248, 536870912),
        timeoutMs: integer(100, 300000),
    }),
});
export const httpEnvironmentSettingsSchema = z.strictObject({
    schemaVersion: z.literal(1),
    kind: z.literal("hugegraph-http-environment-settings"),
    purpose: z.enum(["functional", "calibration"]),
    distribution: absolute,
    javaHome: absolute,
    image: z.string().regex(/^sha256:[a-f0-9]{64}$/),
    python: absolute,
    dockerSocket: absolute,
    mode: z.enum(["forward", "embedded"]),
    jvmObservation: httpJvmObservationSchema.optional(),
    budget: z.strictObject({
        cpus: z.number().min(0.1).max(64),
        memoryBytes: integer(536870912, 137438953472),
        pids: integer(64, 4096),
        wallSeconds: integer(5, 86400),
        startupSeconds: integer(1, 3600),
        shutdownSeconds: integer(1, 30),
        maxAllocatedBytes: integer(1048576, 68719476736),
        maxLogicalBytes: integer(1048576, 137438953472),
        maxLogBytes: integer(4096, 268435456),
        minFreeBytes: integer(0, 1125899906842624),
        tmpfsBytes: integer(4194304, 1073741824),
        pollSeconds: z.number().min(0.1).max(5),
    }),
    jvm: z.strictObject({
        heapMinBytes: integer(16777216, 137438953472),
        heapMaxBytes: integer(16777216, 137438953472),
        maxDirectMemoryBytes: integer(16777216, 137438953472),
        activeProcessorCount: integer(1, 64),
    }),
    server: z.strictObject({
        gremlinPool: integer(1, 256),
        bossThreads: integer(1, 256),
        workerThreads: integer(1, 256),
        maxWorkQueueSize: integer(1, 65536),
        httpWorkerThreads: integer(2, 256),
        maxRoute: integer(1, 256),
        timeoutSeconds: integer(1, 3600),
        idleTimeoutSeconds: integer(1, 3600),
        connectionMaxRequests: integer(1, 1000000),
        maxContentLengthBytes: integer(4096, 4194304),
        resultIterationBatchSize: integer(1, 4096),
    }),
});
