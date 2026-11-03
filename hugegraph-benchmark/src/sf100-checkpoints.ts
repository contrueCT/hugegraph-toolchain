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

import { randomUUID } from "node:crypto";
import { mkdir, open, link, unlink, lstat } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { BenchError, hash, readJson } from "./io.js";
import {
    sf100RequestSchema,
    sf100ReceiptSchema,
    validateSf100Request,
    validateSf100Receipt,
    verifyProtocolArtifacts,
} from "./sf100-protocol.js";
import { z } from "zod";

// An exclusive hard link publishes a fully written file without replacing an existing checkpoint.
async function publish(path: string, value: unknown): Promise<boolean> {
    const temporary = join(dirname(path), `.${randomUUID()}.tmp`);
    let created = false;
    try {
        const file = await open(temporary, "wx", 0o600);
        try {
            await file.writeFile(JSON.stringify(value) + "\n");
            await file.sync();
        } finally {
            await file.close();
        }
        try {
            await link(temporary, path);
            created = true;
        } catch (error) {
            if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
        }
        if (!created) {
            if ((await lstat(path)).isSymbolicLink())
                throw new BenchError("CHECKPOINT_CONFLICT", "Checkpoint is a symbolic link", 5);
            const existing = await readJson(path, z.json());
            if (hash(existing) !== hash(value))
                throw new BenchError(
                    "CHECKPOINT_CONFLICT",
                    "Immutable request or receipt differs from the saved checkpoint",
                    5,
                );
        }
        const directory = await open(dirname(path), "r");
        try {
            await directory.sync();
        } finally {
            await directory.close();
        }
        return created;
    } finally {
        await unlink(temporary).catch((error: NodeJS.ErrnoException) => {
            if (error.code !== "ENOENT") throw error;
        });
    }
}

export async function checkpointSf100(root: string, requestFile: string, receiptFile?: string, artifactRoot?: string) {
    const request = validateSf100Request(await readJson(requestFile, sf100RequestSchema));
    const directory = resolve(root, "sf100-checkpoints", request.requestId);
    const requestPath = join(directory, "request.json");
    const receiptPath = join(directory, "receipt.json");
    if (receiptFile) {
        if (!artifactRoot) throw new BenchError("ARTIFACT_ROOT_REQUIRED", "Sealing a receipt requires --artifact-root");
        try {
            if ((await lstat(directory)).isSymbolicLink() || (await lstat(requestPath)).isSymbolicLink())
                throw new BenchError("CHECKPOINT_CONFLICT", "Saved intent must not be a symbolic link", 5);
        } catch (error) {
            if ((error as NodeJS.ErrnoException).code === "ENOENT")
                throw new BenchError("CHECKPOINT_MISSING", "Record the dispatch intent before sealing its receipt", 3);
            throw error;
        }
        // A dispatch intent must exist before accepting the completion record.
        const saved = validateSf100Request(await readJson(requestPath, sf100RequestSchema));
        if (hash(saved) !== hash(request))
            throw new BenchError("CHECKPOINT_CONFLICT", "Request differs from recorded dispatch intent", 5);
        const receipt = validateSf100Receipt(request, await readJson(receiptFile, sf100ReceiptSchema));
        await verifyProtocolArtifacts(artifactRoot, receipt.artifacts);
        const created = await publish(receiptPath, receipt);
        return {
            requestPath,
            receiptPath,
            reused: !created,
            requestDigest: hash(request),
            state: "receipt-sealed",
            componentStatus: receipt.status,
            effect: receipt.effect,
            executed: false,
            liveDatabaseVerified: false,
        };
    }
    if (artifactRoot) throw new BenchError("CLI_USAGE", "--artifact-root is only used when sealing a receipt");
    const firstCreated = await mkdir(directory, { recursive: true, mode: 0o700 });
    if (firstCreated) {
        let parent = dirname(directory);
        const stop = dirname(firstCreated);
        while (true) {
            const fd = await open(parent, "r");
            try {
                await fd.sync();
            } finally {
                await fd.close();
            }
            if (parent === stop || parent === dirname(parent)) break;
            parent = dirname(parent);
        }
    }
    if ((await lstat(directory)).isSymbolicLink())
        throw new BenchError("CHECKPOINT_CONFLICT", "Checkpoint directory must not be a symbolic link", 5);
    const created = await publish(requestPath, request);
    return {
        requestPath,
        receiptPath: null,
        reused: !created,
        requestDigest: hash(request),
        state: "dispatch-intent-recorded",
        executed: false,
        liveDatabaseVerified: false,
    };
}
