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

const positive = z.number().int().positive();
const absolute = z
    .string()
    .startsWith("/")
    .max(2048)
    .regex(/^[^\r\n\0,]+$/);
export const httpCapacityExecutionRuntimeSchema = z.strictObject({
    schemaVersion: z.literal(1),
    kind: z.literal("http-capacity-execution-runtime"),
    cleanWindow: z.strictObject({
        systemctlExecutable: absolute,
        requireClean: z.literal(true),
        units: z
            .array(z.string().regex(/^[A-Za-z0-9][A-Za-z0-9_.@:-]{0,200}\.service$/))
            .min(1)
            .max(32),
    }),
    budgets: z.strictObject({
        prepareMilliseconds: positive.min(1000).max(300000),
        commandMilliseconds: positive.min(1000).max(300000),
        maxCommandBytes: positive.min(65536).max(16 * 1024 ** 2),
        maxWorkBytes: positive.min(1024 ** 2).max(128 * 1024 ** 3),
        minFreeBytes: z
            .number()
            .int()
            .nonnegative()
            .max(1024 ** 5),
        cleanupMilliseconds: z.literal(20000),
    }),
});
