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

const name = z.string().regex(/^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$/);
const sha = z.string().regex(/^[a-f0-9]{64}$/);
export const taskPlanSchema = z.strictObject({
    schemaVersion: z.literal(1),
    kind: z.literal("component-task"),
    name,
    command: z.array(z.string().min(1)).min(1).max(128),
    cwd: z.string().min(1),
    trackedFiles: z.array(z.string().min(1)).min(1).max(128),
    effect: z.enum(["read-only", "filesystem", "database"]),
    timeoutMs: z
        .number()
        .int()
        .min(100)
        .max(7 * 24 * 60 * 60 * 1000),
    terminateGraceMs: z.number().int().min(100).max(30000).default(2000),
    maxLogBytes: z
        .number()
        .int()
        .min(1024)
        .max(64 * 1024 * 1024)
        .default(1024 * 1024),
});
export const frozenTaskSchema = z.strictObject({
    schemaVersion: z.literal(1),
    kind: z.literal("frozen-component-task"),
    plan: taskPlanSchema,
    files: z
        .array(z.strictObject({ path: z.string(), sha256: sha }))
        .min(1)
        .max(256),
    digest: sha,
});
export type FrozenTask = z.infer<typeof frozenTaskSchema>;
export const taskStateSchema = z.strictObject({
    schemaVersion: z.literal(1),
    taskId: name,
    planDigest: sha,
    state: z.enum(["queued", "running", "completed", "failed", "cancelled"]),
    phase: z.enum(["queued", "preflight", "execute", "seal", "finished"]),
    createdAt: z.string().datetime(),
    updatedAt: z.string().datetime(),
    dispatched: z.boolean(),
    effect: z.enum(["none", "unknown"]),
    processExitCode: z.number().int().nullable(),
    error: z.strictObject({ code: z.string(), message: z.string() }).nullable(),
});
export type TaskState = z.infer<typeof taskStateSchema>;
export const taskEventSchema = z.strictObject({
    schemaVersion: z.literal(1),
    taskId: name,
    sequence: z.number().int().positive(),
    at: z.string().datetime(),
    type: z.string(),
    data: z.json(),
});
export const taskOwnerSchema = z.strictObject({
    pid: z.number().int().positive(),
    host: z.string(),
    start: z.string(),
    token: z.string().uuid(),
});
export type ProcessResult = {
    dispatched: boolean;
    exitCode: number | null;
    signal: string | null;
    error: { code: string; message: string } | null;
    stdoutBytes: number;
    stderrBytes: number;
};
export const taskTerminal = (s: TaskState) => ["completed", "failed", "cancelled"].includes(s.state);
