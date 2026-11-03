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
import { chmod, lstat, mkdir, mkdtemp, readdir, rm, symlink, writeFile } from "node:fs/promises";
import { hostname, tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { taskRequestId, taskSnapshot } from "../dist/tasks.js";

const id = "task-" + "a".repeat(32);
async function fixture(t) {
    const base = await mkdtemp(join(tmpdir(), "hgbench-task-status-"));
    t.after(() => rm(base, { recursive: true, force: true }));
    return base;
}
async function record(root, patch = {}) {
    const directory = join(root, "tasks", id);
    await mkdir(directory, { recursive: true });
    await writeFile(
        join(directory, "state.json"),
        JSON.stringify({
            schemaVersion: 1,
            taskId: id,
            planDigest: "b".repeat(64),
            state: "completed",
            phase: "finished",
            createdAt: new Date().toISOString(),
            updatedAt: new Date().toISOString(),
            dispatched: true,
            effect: "unknown",
            processExitCode: 0,
            error: null,
            ...patch,
        }),
    );
    return directory;
}

test("task status: missing root, registry and task stay scoped and never create records", async (t) => {
    const base = await fixture(t);
    for (const element of ["root", "store", "task"]) {
        const root = join(base, element);
        if (element !== "root") await mkdir(root);
        if (element === "task") await mkdir(join(root, "tasks"));
        await assert.rejects(taskSnapshot(root, id), (error) => {
            assert.equal(error.code, "TASK_NOT_FOUND");
            assert.equal(error.exitCode, 3);
            assert.equal(error.retryable, false);
            assert.equal(error.details.host, hostname());
            assert.equal(error.details.root, resolve(root));
            assert.equal(error.details.taskId, id);
            assert.equal(error.details.missingElement, element);
            assert.equal(error.details.componentOutcome, "unknown");
            assert.equal(error.details.automaticReplayAllowed, false);
            return true;
        });
        await assert.rejects(lstat(join(root, "tasks", id)), (error) => error.code === "ENOENT");
        if (element === "root") await assert.rejects(lstat(root), (error) => error.code === "ENOENT");
        else assert.deepEqual(await readdir(root), element === "task" ? ["tasks"] : []);
    }
});

test("task status: public CLI returns a missing-record JSON error for the original request selector", async (t) => {
    const base = await fixture(t);
    const root = join(base, "uncreated-root");
    await assert.rejects(
        promisify(execFile)(process.execPath, [
            "dist/cli.js",
            "--root",
            root,
            "task",
            "status",
            "--request-id",
            "not-submitted",
        ]),
        (error) => {
            assert.equal(error.code, 3);
            assert.equal(error.stderr, "");
            const response = JSON.parse(error.stdout);
            assert.equal(response.schemaVersion, 1);
            assert.equal(response.ok, false);
            assert.equal(response.error.code, "TASK_NOT_FOUND");
            assert.equal(response.error.details.taskId, taskRequestId("not-submitted"));
            assert.equal(response.error.details.automaticReplayAllowed, false);
            assert.equal(response.error.details.componentOutcome, "unknown");
            return true;
        },
    );
    await assert.rejects(lstat(root), (error) => error.code === "ENOENT");
});

test("task status: malformed root, registry and task entries cannot be mistaken for missing records", async (t) => {
    const base = await fixture(t);
    const fileRoot = join(base, "file-root");
    await writeFile(fileRoot, "occupied");
    await assert.rejects(taskSnapshot(fileRoot, id), (error) => error.code === "TASK_STORE_INVALID");
    const brokenRoot = join(base, "broken-root");
    await symlink(join(base, "absent-target"), brokenRoot);
    await assert.rejects(taskSnapshot(brokenRoot, id), (error) => error.code === "TASK_STORE_INVALID");
    const root = join(base, "root");
    await mkdir(root);
    await writeFile(join(root, "tasks"), "occupied");
    await assert.rejects(taskSnapshot(root, id), (error) => error.code === "TASK_STORE_INVALID");
    await rm(join(root, "tasks"));
    const healthy = join(base, "healthy");
    await record(healthy);
    await symlink(join(healthy, "tasks"), join(root, "tasks"));
    await assert.rejects(taskSnapshot(root, id), (error) => error.code === "TASK_STORE_INVALID");
    await rm(join(root, "tasks"));
    await mkdir(join(root, "tasks"));
    await symlink(join(healthy, "tasks", id), join(root, "tasks", id));
    await assert.rejects(taskSnapshot(root, id), (error) => error.code === "TASK_DIRECTORY_INVALID");
    const alias = join(base, "healthy-alias");
    await symlink(healthy, alias);
    assert.equal((await taskSnapshot(alias, id)).state, "completed");
});

test("task status: reserved, corrupt and unreadable metadata remain distinct from absent tasks", async (t) => {
    const root = await fixture(t);
    const directory = join(root, "tasks", id);
    await mkdir(directory, { recursive: true });
    await assert.rejects(taskSnapshot(root, id), (error) => {
        assert.equal(error.code, "TASK_METADATA_MISSING");
        assert.equal(error.details.automaticReplayAllowed, false);
        return true;
    });
    await writeFile(join(directory, "state.json"), "not-json");
    await assert.rejects(taskSnapshot(root, id), (error) => error.code === "TASK_METADATA_INVALID");
    await rm(join(directory, "state.json"));
    const healthy = join(root, "healthy");
    await record(healthy);
    await symlink(join(healthy, "tasks", id, "state.json"), join(directory, "state.json"));
    await assert.rejects(taskSnapshot(root, id), (error) => error.code === "TASK_METADATA_UNREADABLE");
    await rm(join(directory, "state.json"));
    await record(root, { state: "running", phase: "execute", processExitCode: null });
    assert.equal((await taskSnapshot(root, id)).ownerStatus, "missing");
    await symlink(join(healthy, "tasks", id, "state.json"), join(directory, "owner.json"));
    await assert.rejects(taskSnapshot(root, id), (error) => error.code === "TASK_METADATA_UNREADABLE");
});

test("task status: real permission failures are unreadable, never missing records", async (t) => {
    if (process.getuid?.() === 0) return t.skip("root bypasses POSIX mode permissions");
    const root = await fixture(t);
    const directory = await record(root);
    const state = join(directory, "state.json");
    await chmod(state, 0);
    try {
        await assert.rejects(taskSnapshot(root, id), (error) => {
            assert.equal(error.code, "TASK_METADATA_UNREADABLE");
            assert.equal(error.details.cause, "EACCES");
            return true;
        });
    } finally {
        await chmod(state, 0o600);
    }
    await chmod(join(root, "tasks"), 0);
    try {
        await assert.rejects(taskSnapshot(root, id), (error) => {
            assert.equal(error.code, "TASK_STORE_UNREADABLE");
            assert.equal(error.details.cause, "EACCES");
            assert.equal(error.details.automaticReplayAllowed, false);
            return true;
        });
    } finally {
        await chmod(join(root, "tasks"), 0o700);
    }
});
