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

import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, mkdir, writeFile, rm, stat, symlink, rename } from "node:fs/promises";
import { tmpdir, hostname } from "node:os";
import { join } from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { listTasks } from "../dist/task-list.js";
import { taskSnapshot } from "../dist/tasks.js";

const id = (n) => `task-${n.toString(16).padStart(32, "0")}`;
async function rootFor(t) {
    const path = await mkdtemp(join(tmpdir(), "hgbench-list-"));
    t.after(() => rm(path, { recursive: true, force: true }));
    return path;
}
async function entry(root, n, patch = {}) {
    const dir = join(root, "tasks", id(n));
    await mkdir(dir, { recursive: true });
    const state = {
        schemaVersion: 1,
        taskId: id(n),
        planDigest: "a".repeat(64),
        state: "completed",
        phase: "finished",
        createdAt: "2026-01-01T00:00:00.000Z",
        updatedAt: "2026-01-01T00:00:00.000Z",
        dispatched: true,
        effect: "unknown",
        processExitCode: 0,
        error: null,
        ...patch,
    };
    await writeFile(join(dir, "state.json"), JSON.stringify(state));
    return dir;
}

test("task list: empty stores stay absent; invalid cursors and limits fail closed", async (t) => {
    const root = await rootFor(t);
    const empty = await listTasks(root);
    assert.equal(empty.totalTasks, 0);
    assert.deepEqual(empty.items, []);
    assert.equal(empty.nextCursor, null);
    await assert.rejects(stat(join(root, "tasks")), (e) => e.code === "ENOENT");
    for (const limit of [0, 101, 1.5, NaN])
        await assert.rejects(listTasks(root, limit), (e) => e.code === "INVALID_LIMIT");
    for (const cursor of ["", "bad=", "a".repeat(1025), Buffer.from("{}").toString("base64url")])
        await assert.rejects(listTasks(root, 1, cursor), (e) => e.code === "INVALID_CURSOR");
    const target = await rootFor(t);
    await symlink(target, join(root, "tasks"));
    await assert.rejects(listTasks(root), (e) => e.code === "TASK_STORE_INVALID");
});

test("task list: pages are ordered, root scoped and detect membership changes while states stay live", async (t) => {
    const root = await rootFor(t);
    for (const n of [3, 1, 2]) await entry(root, n);
    const first = await listTasks(root, 1);
    assert.equal(first.totalTasks, 3);
    assert.equal(first.items[0].taskId, id(1));
    assert.equal(first.hasMore, true);
    assert.equal(first.benchmarkValid, false);
    await entry(root, 2, { state: "failed", processExitCode: 1 });
    const rest = await listTasks(root, 2, first.nextCursor);
    assert.deepEqual(
        rest.items.map((item) => item.taskId),
        [id(2), id(3)],
    );
    assert.equal(rest.items[0].snapshot.state, "failed");
    assert.equal(rest.items[0].snapshot.effect, "unknown");
    assert.equal(rest.nextCursor, null);
    const other = await rootFor(t);
    for (const n of [1, 2, 3]) await entry(other, n);
    await assert.rejects(listTasks(other, 1, first.nextCursor), (e) => e.code === "CURSOR_SCOPE_MISMATCH");
    await entry(root, 4);
    await assert.rejects(listTasks(root, 1, first.nextCursor), (e) => e.code === "CURSOR_STALE" && e.retryable);
    const latest = await listTasks(root, 1);
    await rm(join(root, "tasks", id(4)), { recursive: true });
    await assert.rejects(listTasks(root, 1, latest.nextCursor), (e) => e.code === "CURSOR_STALE");
    const beforeReplace = await listTasks(root, 1);
    await rename(join(root, "tasks"), join(root, "old-tasks"));
    for (const n of [1, 2, 3]) await entry(root, n);
    await assert.rejects(listTasks(root, 1, beforeReplace.nextCursor), (e) => e.code === "CURSOR_SCOPE_MISMATCH");
});

test("task list: reserved, damaged, mismatched and unsafe entries remain explicit without hiding healthy tasks", async (t) => {
    const root = await rootFor(t);
    const healthy = await entry(root, 1);
    await mkdir(join(root, "tasks", id(2)));
    await writeFile(join(await entry(root, 3), "state.json"), "not json");
    await entry(root, 4, { taskId: id(1) });
    await symlink(healthy, join(root, "tasks", id(5)));
    await mkdir(join(root, "tasks", id(6)));
    await symlink(join(healthy, "state.json"), join(root, "tasks", id(6), "state.json"));
    await writeFile(join(await entry(root, 7), "state.json"), " ".repeat(65537));
    const fifo = join(await entry(root, 8), "state.json");
    await rm(fifo);
    await promisify(execFile)("mkfifo", [fifo]);
    await entry(root, 9, { state: "running", phase: "execute", processExitCode: null });
    const foreign = await entry(root, 10, { state: "running", phase: "execute", processExitCode: null });
    await writeFile(
        join(foreign, "owner.json"),
        JSON.stringify({
            pid: process.pid,
            host: `${hostname()}-other`,
            start: "unknown",
            token: "00000000-0000-4000-8000-000000000000",
        }),
    );
    await writeFile(join(root, "tasks", "unrelated-file"), "ignored");
    const result = await listTasks(root);
    assert.equal(result.totalTasks, 10);
    assert.equal(result.entryErrors, 7);
    assert.equal(result.items[0].snapshot.state, "completed");
    assert.deepEqual(
        result.items.slice(1, 8).map((item) => item.error.code),
        [
            "TASK_METADATA_MISSING",
            "TASK_METADATA_INVALID",
            "TASK_IDENTITY_INVALID",
            "TASK_DIRECTORY_INVALID",
            "TASK_METADATA_UNREADABLE",
            "TASK_METADATA_TOO_LARGE",
            "TASK_METADATA_INVALID",
        ],
    );
    assert.equal(result.items[8].snapshot.interrupted, true);
    assert.equal(result.items[9].snapshot.ownerStatus, "different-host");
    assert.equal(result.items[9].snapshot.interrupted, false);
    await assert.rejects(taskSnapshot(root, id(4)), (e) => e.code === "TASK_IDENTITY_INVALID");
});

test("task list: scan cap includes unrelated entries and never returns a silently truncated store", async (t) => {
    const root = await rootFor(t);
    const path = join(root, "tasks");
    await mkdir(path);
    for (let base = 0; base < 10001; base += 50)
        await Promise.all(
            Array.from({ length: Math.min(50, 10001 - base) }, (_, i) =>
                writeFile(join(path, `unrelated-${base + i}`), ""),
            ),
        );
    await assert.rejects(listTasks(root), (e) => e.code === "TASK_STORE_LIMIT");
});
