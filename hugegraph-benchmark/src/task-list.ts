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

import { opendir, lstat, realpath } from "node:fs/promises";
import { hostname } from "node:os";
import { join, resolve } from "node:path";
import { z } from "zod";
import { BenchError, hash } from "./io.js";
import { taskSnapshot, taskDirectory } from "./tasks.js";

const taskId = /^task-[a-f0-9-]{16,64}$/;
const cursorSchema = z.strictObject({
    version: z.literal(1),
    scope: z.string().regex(/^[a-f0-9]{64}$/),
    membership: z.string().regex(/^[a-f0-9]{64}$/),
    after: z.string().regex(taskId),
});
const maxEntries = 10000;

async function inventory(root: string) {
    const path = join(resolve(root), "tasks");
    let info;
    try {
        info = await lstat(path, { bigint: true });
    } catch (error) {
        if ((error as NodeJS.ErrnoException).code === "ENOENT") return null;
        throw error;
    }
    if (!info.isDirectory()) throw new BenchError("TASK_STORE_INVALID", "Task store must be a regular directory");
    const scope = hash({ host: hostname(), path: await realpath(path), dev: String(info.dev), ino: String(info.ino) });
    const entries: string[] = [];
    let scanned = 0;
    for await (const entry of await opendir(path)) {
        if (++scanned > maxEntries)
            throw new BenchError("TASK_STORE_LIMIT", "Task store exceeds 10000 entries; use separate run roots");
        // Include damaged task entries as well: never hide a reserved task ID
        // merely because its state is missing or its entry is not a directory.
        if (taskId.test(entry.name)) entries.push(entry.name);
    }
    entries.sort();
    return { scope, membership: hash(entries), entries };
}

function decodeCursor(text: string) {
    try {
        if (text.length > 1024 || !/^[A-Za-z0-9_-]+$/.test(text)) throw new Error();
        const buffer = Buffer.from(text, "base64url");
        if (buffer.toString("base64url") !== text) throw new Error();
        return cursorSchema.parse(JSON.parse(buffer.toString("utf8")));
    } catch {
        throw new BenchError("INVALID_CURSOR", "Use the opaque nextCursor returned by task list");
    }
}

export async function listTasks(root: string, limit = 20, cursor?: string) {
    if (!Number.isInteger(limit) || limit < 1 || limit > 100)
        throw new BenchError("INVALID_LIMIT", "Task list limit must be 1..100");
    const decoded = cursor === undefined ? null : decodeCursor(cursor);
    const before = await inventory(root);
    if (decoded) {
        if (!before || decoded.scope !== before.scope)
            throw new BenchError("CURSOR_SCOPE_MISMATCH", "Task cursor belongs to a different host or store");
        if (decoded.membership !== before.membership)
            throw new BenchError(
                "CURSOR_STALE",
                "Task membership changed; restart task list without a cursor",
                4,
                true,
            );
        if (!before.entries.includes(decoded.after)) throw new BenchError("INVALID_CURSOR", "Unknown cursor position");
    }
    const offset = decoded ? before!.entries.indexOf(decoded.after) + 1 : 0;
    const ids = (before?.entries ?? []).slice(offset, offset + limit);
    const items = [];
    for (const id of ids) {
        try {
            const snapshot = await taskSnapshot(root, id);
            items.push({
                taskId: id,
                evidenceDir: snapshot.evidenceDir,
                available: true,
                snapshot,
                error: null,
            });
        } catch (error) {
            const rawCode = (error as NodeJS.ErrnoException).code;
            const code =
                error instanceof BenchError
                    ? error.code
                    : rawCode === "ENOENT"
                      ? "TASK_METADATA_MISSING"
                      : "TASK_METADATA_UNREADABLE";
            items.push({
                taskId: id,
                evidenceDir: taskDirectory(root, id),
                available: false,
                snapshot: null,
                error: { code, message: "Inspect this task's metadata; do not automatically resubmit it" },
            });
        }
    }
    const after = await inventory(root);
    if (before?.scope !== after?.scope || before?.membership !== after?.membership)
        throw new BenchError("CURSOR_STALE", "Task membership changed during reading; restart the listing", 4, true);
    const hasMore = offset + items.length < (before?.entries.length ?? 0);
    const nextCursor = hasMore
        ? Buffer.from(
              JSON.stringify({ version: 1, scope: before!.scope, membership: before!.membership, after: ids.at(-1) }),
          ).toString("base64url")
        : null;
    return {
        schemaVersion: 1,
        root: resolve(root),
        observedAt: new Date().toISOString(),
        order: "task-id-ascending",
        consistency: "membership-checked-live-states",
        totalTasks: before?.entries.length ?? 0,
        items,
        entryErrors: items.filter((item) => !item.available).length,
        hasMore,
        nextCursor,
        benchmarkValid: false,
        resultScope: "component-process-lifecycle",
    };
}
