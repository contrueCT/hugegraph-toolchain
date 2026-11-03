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

const MiB = 1024 * 1024;
const integer = (minimum: number, maximum: number) => z.number().int().min(minimum).max(maximum);

export const httpRuntimeProvenanceSpecSchema = z
    .strictObject({
        schemaVersion: z.literal(1),
        kind: z.literal("http-runtime-provenance-spec"),
        sourceOrigin: z.literal("linux-procfs"),
        budgets: z.strictObject({
            wallMs: integer(5000, 120000),
            commandMs: integer(500, 15000),
            handshakeMs: integer(500, 20000),
            cleanupMs: integer(1000, 10000),
            maxRawBytes: integer(64 * 1024, MiB),
            maxEvidenceBytes: integer(MiB, 16 * MiB),
            maxExecutableBytes: integer(MiB, 512 * MiB),
            maxHashBytes: integer(64 * MiB, 4 * 1024 * MiB),
        }),
        container: z.strictObject({
            cpus: z.number().min(0.1).max(1),
            memoryBytes: integer(64 * MiB, 512 * MiB),
            pids: integer(16, 64),
            tmpfsBytes: integer(4 * MiB, 32 * MiB),
        }),
    })
    .superRefine((value, context) => {
        if (3 * value.budgets.handshakeMs > value.budgets.wallMs)
            context.addIssue({ code: "custom", message: "Three runtime handshakes must fit within wallMs" });
        if (value.budgets.maxRawBytes >= value.budgets.maxEvidenceBytes)
            context.addIssue({ code: "custom", message: "Raw output must leave room for locked runtime and sources" });
    });
