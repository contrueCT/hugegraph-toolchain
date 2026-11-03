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
import { spawn, execFile } from "node:child_process";
import { closeSync, openSync, constants } from "node:fs";
import { mkdir, open, readFile, rename, realpath, lstat, stat } from "node:fs/promises";
import { dirname, join, resolve, isAbsolute } from "node:path";
import { hostname } from "node:os";
import { promisify } from "node:util";
import { z } from "zod";
import { BenchError, delay, fileHash, hash, readJson } from "./io.js";
import { controllerIdentity, executable, packageRoot } from "./plans.js";
import {
    frozenTaskSchema,
    taskPlanSchema,
    taskStateSchema,
    taskEventSchema,
    taskOwnerSchema,
    taskTerminal,
} from "./task-contracts.js";
import type { FrozenTask, TaskState } from "./task-contracts.js";
import { verifyProtocolArtifacts } from "./sf100-protocol.js";

export async function durableJson(path: string, value: unknown) {
    const temporary = `${path}.${randomUUID()}.tmp`;
    const file = await open(temporary, "wx", 0o600);
    try {
        await file.writeFile(JSON.stringify(value) + "\n");
        await file.sync();
    } finally {
        await file.close();
    }
    await rename(temporary, path);
    const dir = await open(dirname(path), "r");
    try {
        await dir.sync();
    } finally {
        await dir.close();
    }
}
export function taskDirectory(root: string, id: string) {
    if (!/^task-[a-f0-9-]{16,64}$/.test(id)) throw new BenchError("INVALID_TASK_ID", "Invalid task ID");
    return join(resolve(root), "tasks", id);
}
export function taskRequestId(key: string) {
    if (!/^[\x21-\x7e]{1,128}$/.test(key))
        throw new BenchError("INVALID_REQUEST_ID", "request-id must be 1..128 visible ASCII characters");
    return `task-${hash(key).slice(0, 32)}`;
}
export async function freezeTask(file: string): Promise<FrozenTask> {
    const input = await readJson(file, taskPlanSchema);
    return freezeTaskPlan(input, dirname(resolve(file)));
}
// In-process controllers use the same freezer without writing and rereading a template.
export async function freezeTaskPlan(value: unknown, relativeTo = process.cwd()): Promise<FrozenTask> {
    const input = taskPlanSchema.parse(value);
    const cwd = await realpath(resolve(relativeTo, input.cwd));
    if (!(await stat(cwd)).isDirectory()) throw new BenchError("INVALID_CWD", "cwd must be a directory");
    const program = await executable(input.command[0]!, cwd);
    const trackedFiles = await Promise.all(input.trackedFiles.map((p) => realpath(resolve(cwd, p))));
    const plan = { ...input, cwd, trackedFiles, command: [program, ...input.command.slice(1)] };
    const paths = [...new Set([program, ...trackedFiles])].sort();
    const files = await Promise.all(
        paths.map(async (path) => {
            if (!(await lstat(path)).isFile())
                throw new BenchError("INVALID_TRACKED_FILE", `Not a regular file: ${path}`);
            return { path, sha256: await fileHash(path) };
        }),
    );
    const content = { schemaVersion: 1 as const, kind: "frozen-component-task" as const, plan, files };
    return { ...content, digest: hash(content) };
}
export async function verifyTask(input: FrozenTask) {
    const task = frozenTaskSchema.parse(input);
    const { digest, ...content } = task;
    if (hash(content) !== digest) throw new BenchError("PLAN_DIGEST_MISMATCH", "Frozen task changed", 5);
    if (!isAbsolute(task.plan.cwd) || !isAbsolute(task.plan.command[0]!) || task.files.some((f) => !isAbsolute(f.path)))
        throw new BenchError("TASK_IDENTITY_INVALID", "Frozen task paths must be absolute", 5);
    const required = new Set([task.plan.command[0], ...task.plan.trackedFiles]);
    if (required.size !== task.files.length || task.files.some((f) => !required.delete(f.path)) || required.size)
        throw new BenchError("TASK_IDENTITY_INVALID", "Frozen file coverage differs from command/tracked files", 5);
    for (const file of task.files) {
        if (!(await lstat(file.path)).isFile() || (await fileHash(file.path)) !== file.sha256)
            throw new BenchError("COMPONENT_CHANGED", `Tracked file changed: ${file.path}`, 5);
    }
}
export async function processStart(pid: number): Promise<string | null> {
    try {
        if (process.platform === "linux") {
            const raw = await readFile(`/proc/${pid}/stat`, "utf8");
            const fields = raw.slice(raw.lastIndexOf(")") + 2).split(" ");
            if (["Z", "X"].includes(fields[0]!)) return null;
            return `${(await readFile("/proc/sys/kernel/random/boot_id", "utf8")).trim()}:${fields[19]}`;
        }
        const { stdout } = await promisify(execFile)("/bin/ps", ["-p", String(pid), "-o", "lstart=", "-o", "stat="], {
            timeout: 2000,
        });
        const text = stdout.trim();
        if (!text || /\sZ\S*$/.test(text)) return null;
        return text.replace(/\s+\S+$/, "");
    } catch (error) {
        const code = (error as { code?: string | number }).code;
        if (code === "ENOENT" || code === "ESRCH" || code === 1) return null;
        throw error;
    }
}
// Task metadata is small. Read from one regular-file descriptor with a hard
// byte bound, including if a damaged file grows between stat and read.
async function taskMetadata<T>(path: string, schema: z.ZodType<T>): Promise<T> {
    const file = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
    try {
        if (!(await file.stat()).isFile())
            throw new BenchError("TASK_METADATA_INVALID", "Task metadata must be a regular file");
        const data = Buffer.alloc(65537);
        let length = 0;
        while (length < data.length) {
            const { bytesRead } = await file.read(data, length, data.length - length, length);
            if (!bytesRead) break;
            length += bytesRead;
        }
        if (length > 65536) throw new BenchError("TASK_METADATA_TOO_LARGE", "Task metadata exceeds 64 KiB");
        try {
            return schema.parse(JSON.parse(data.subarray(0, length).toString("utf8")));
        } catch {
            throw new BenchError("TASK_METADATA_INVALID", "Task metadata does not match its schema");
        }
    } finally {
        await file.close();
    }
}
type TaskLookupScope = { host: string; root: string; taskId: string };
function lookupDetails(scope: TaskLookupScope, path: string, cause?: string) {
    return { ...scope, path, cause, componentOutcome: "unknown", automaticReplayAllowed: false };
}
function metadataError(error: unknown, path: string, scope: TaskLookupScope) {
    if (error instanceof BenchError) return error;
    const cause = (error as NodeJS.ErrnoException).code;
    return new BenchError(
        cause === "ENOENT" ? "TASK_METADATA_MISSING" : "TASK_METADATA_UNREADABLE",
        cause === "ENOENT"
            ? "The existing task record has missing metadata; inspect the original record before any new attempt"
            : "Cannot read existing task metadata; inspect permissions and the original record before any new attempt",
        3,
        false,
        lookupDetails(scope, path, cause),
    );
}
async function lookupDirectory(path: string, element: "root" | "store" | "task", scope: TaskLookupScope) {
    const invalidCode = element === "task" ? "TASK_DIRECTORY_INVALID" : "TASK_STORE_INVALID";
    let info;
    try {
        info = await lstat(path);
    } catch (error) {
        const cause = (error as NodeJS.ErrnoException).code;
        if (cause === "ENOENT")
            throw new BenchError(
                "TASK_NOT_FOUND",
                "No local task record exists on this host at the selected --root; component outcome is unknown. Do not automatically resubmit",
                3,
                false,
                { ...lookupDetails(scope, path, cause), missingElement: element },
            );
        throw new BenchError(
            ["ENOTDIR", "ELOOP"].includes(cause ?? "") ? invalidCode : "TASK_STORE_UNREADABLE",
            "Cannot inspect the local task store; check its path, permissions and original metadata before any new attempt",
            3,
            false,
            lookupDetails(scope, path, cause),
        );
    }
    // A caller may select a directory alias as --root; the registry and task
    // entries themselves must remain real directories, as required by task list.
    if (element === "root" && info.isSymbolicLink()) {
        try {
            info = await stat(path);
        } catch (error) {
            const cause = (error as NodeJS.ErrnoException).code;
            throw new BenchError(
                ["ENOENT", "ENOTDIR", "ELOOP"].includes(cause ?? "") ? invalidCode : "TASK_STORE_UNREADABLE",
                "The selected --root alias is invalid or unreadable; inspect it before any new attempt",
                3,
                false,
                lookupDetails(scope, path, cause),
            );
        }
    }
    if (!info.isDirectory())
        throw new BenchError(
            invalidCode,
            element === "task" ? "Task entry must be a directory, not a symlink" : "Task store must be a directory",
            2,
            false,
            lookupDetails(scope, path),
        );
}
export async function taskSnapshot(root: string, id: string) {
    const dir = taskDirectory(root, id);
    const scope = { host: hostname(), root: resolve(root), taskId: id };
    await lookupDirectory(scope.root, "root", scope);
    await lookupDirectory(join(scope.root, "tasks"), "store", scope);
    await lookupDirectory(dir, "task", scope);
    const stateFile = join(dir, "state.json");
    const state = await taskMetadata(stateFile, taskStateSchema).catch((error: unknown) => {
        throw metadataError(error, stateFile, scope);
    });
    if (state.taskId !== id) throw new BenchError("TASK_IDENTITY_INVALID", "Task state ID differs from directory");
    let ownerStatus: "missing" | "alive" | "gone" | "unavailable" | "different-host" = "missing";
    if (!taskTerminal(state)) {
        try {
            const owner = await taskMetadata(join(dir, "owner.json"), taskOwnerSchema);
            if (owner.host !== hostname()) ownerStatus = "different-host";
            else {
                try {
                    ownerStatus = (await processStart(owner.pid)) === owner.start ? "alive" : "gone";
                } catch {
                    ownerStatus = "unavailable";
                }
            }
        } catch (error) {
            if ((error as NodeJS.ErrnoException).code !== "ENOENT")
                throw metadataError(error, join(dir, "owner.json"), scope);
        }
    }
    const age = Date.now() - Date.parse(state.updatedAt);
    const interrupted = !taskTerminal(state) && (ownerStatus === "gone" || (ownerStatus === "missing" && age > 10000));
    return {
        ...state,
        terminal: taskTerminal(state) || interrupted,
        interrupted,
        ownerStatus,
        stalled: !taskTerminal(state) && age > 15000,
        evidenceDir: dir,
        pollAfterMs: taskTerminal(state) || interrupted ? 0 : 1000,
        benchmarkValid: false,
        resultScope: "component-process-lifecycle",
        recovery:
            interrupted || state.effect === "unknown"
                ? "inspect-component-evidence-before-new-attempt"
                : "inspect-outcome",
    };
}
export async function submitTask(root: string, frozen: FrozenTask, key?: string) {
    frozen = frozenTaskSchema.parse(frozen);
    const { digest, ...content } = frozen;
    if (hash(content) !== digest) throw new BenchError("PLAN_DIGEST_MISMATCH", "Frozen task changed", 5);
    if (!["linux", "darwin"].includes(process.platform))
        throw new BenchError("UNSUPPORTED_PLATFORM", "Component tasks require Linux or macOS", 3);
    const id = key ? taskRequestId(key) : `task-${randomUUID()}`;
    const dir = taskDirectory(root, id);
    await mkdir(dirname(dir), { recursive: true, mode: 0o700 });
    try {
        await mkdir(dir, { mode: 0o700 });
    } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
        for (let i = 0; i < 20; i++) {
            try {
                const submission = await readJson(join(dir, "submission.json"), z.object({ planDigest: z.string() }));
                if (submission.planDigest !== frozen.digest)
                    throw new BenchError("REQUEST_ID_CONFLICT", "request-id belongs to different inputs", 5);
                return { ...(await taskSnapshot(root, id)), reused: true };
            } catch (error) {
                if (error instanceof BenchError && error.code === "REQUEST_ID_CONFLICT") throw error;
                await delay(100);
            }
        }
        throw new BenchError(
            "SUBMISSION_IN_PROGRESS",
            `Reserved task ${id}; query the same request-id before retrying`,
            4,
            true,
        );
    }
    const now = new Date().toISOString();
    const state: TaskState = {
        schemaVersion: 1,
        taskId: id,
        planDigest: frozen.digest,
        state: "queued",
        phase: "queued",
        createdAt: now,
        updatedAt: now,
        dispatched: false,
        effect: "none",
        processExitCode: null,
        error: null,
    };
    await durableJson(join(dir, "plan.json"), frozen);
    await durableJson(join(dir, "state.json"), state);
    const token = randomUUID();
    try {
        await verifyTask(frozen);
        await durableJson(join(dir, "controller.json"), await controllerIdentity());
        await durableJson(join(dir, "launch.json"), { token });
        await durableJson(join(dir, "submission.json"), { planDigest: frozen.digest });
        const log = openSync(join(dir, "controller.log"), "ax", 0o600);
        try {
            const child = spawn(
                process.execPath,
                [join(packageRoot, "dist/cli.js"), "__task-worker", resolve(root), id, token],
                {
                    cwd: packageRoot,
                    detached: true,
                    stdio: ["ignore", log, log],
                },
            );
            await new Promise<void>((done, fail) => {
                child.once("spawn", done);
                child.once("error", fail);
            });
            child.unref();
        } finally {
            closeSync(log);
        }
    } catch (error) {
        await durableJson(join(dir, "state.json"), {
            ...state,
            state: "failed",
            phase: "finished",
            error: { code: "TASK_SUBMISSION_FAILED", message: String(error) },
            updatedAt: new Date().toISOString(),
        });
        await durableJson(join(dir, "submission.json"), { planDigest: frozen.digest });
        throw error;
    }
    return { ...(await taskSnapshot(root, id)), reused: false };
}
export async function taskEvents(root: string, id: string, after = 0, limit = 100) {
    if (!Number.isSafeInteger(after) || after < 0 || !Number.isInteger(limit) || limit < 1 || limit > 1000)
        throw new BenchError("INVALID_CURSOR", "after-cursor must be nonnegative; limit must be 1..1000");
    await taskSnapshot(root, id);
    const file = join(taskDirectory(root, id), "events.ndjson");
    let raw = "";
    try {
        if ((await stat(file)).size > 1024 * 1024)
            throw new BenchError("EVENT_LIMIT", "Lifecycle event log exceeds 1 MiB", 6);
        raw = await readFile(file, "utf8");
    } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    }
    const rows = raw
        .slice(0, raw.lastIndexOf("\n") + 1)
        .split("\n")
        .filter(Boolean)
        .map((line) => taskEventSchema.parse(JSON.parse(line)));
    rows.forEach((row, i) => {
        if (row.sequence !== i + 1 || row.taskId !== id)
            throw new BenchError("EVENT_IDENTITY_INVALID", "Lifecycle event identity/sequence is invalid", 6);
    });
    if (after > rows.length) throw new BenchError("INVALID_CURSOR", "Cursor is ahead of this task");
    const events = rows.slice(after, after + limit);
    return { events, nextCursor: after + events.length, hasMore: rows.length > after + events.length };
}
export async function waitTask(root: string, id: string, timeoutMs: number, after?: number) {
    const deadline = Date.now() + timeoutMs;
    while (true) {
        const state = await taskSnapshot(root, id);
        const page = after === undefined ? null : await taskEvents(root, id, after);
        if (state.terminal || page?.events.length || Date.now() >= deadline) return { ...state, ...(page ?? {}) };
        await delay(Math.min(200, Math.max(1, deadline - Date.now())));
    }
}
export async function cancelTask(root: string, id: string) {
    const state = await taskSnapshot(root, id);
    if (!state.terminal) {
        const path = join(taskDirectory(root, id), "cancel.json");
        // Idempotent mailbox; only the owning worker sends process signals.
        await durableJson(path, { taskId: id, requestedAt: new Date().toISOString() });
    }
    return { ...state, cancellationRequested: !state.terminal, cancellationComplete: state.state === "cancelled" };
}
export async function taskLogs(root: string, id: string, stream: "stdout" | "stderr", bytes = 16384) {
    await taskSnapshot(root, id);
    const path = join(taskDirectory(root, id), `${stream}.log`);
    try {
        const file = await open(path, "r");
        try {
            const size = (await file.stat()).size;
            const start = Math.max(0, size - bytes);
            const buffer = Buffer.alloc(Math.min(size, bytes));
            const { bytesRead } = await file.read(buffer, 0, buffer.length, start);
            return {
                path,
                offset: start,
                totalBytes: size,
                truncated: start > 0,
                text: buffer.subarray(0, bytesRead).toString("utf8"),
            };
        } finally {
            await file.close();
        }
    } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
        return { path, text: "", pending: true };
    }
}

export async function taskArtifacts(root: string, id: string) {
    const state = await taskSnapshot(root, id);
    if (!state.terminal || state.interrupted)
        throw new BenchError(
            "TASK_EVIDENCE_NOT_SEALED",
            "Task has no authoritative sealed completion; inspect partial logs",
            4,
            !state.terminal,
        );
    const manifest = await readJson(
        join(state.evidenceDir, "evidence.json"),
        z.strictObject({
            schemaVersion: z.literal(1),
            taskId: z.string(),
            files: z
                .array(
                    z.strictObject({
                        path: z.string(),
                        bytes: z.number().int().nonnegative(),
                        sha256: z.string().regex(/^[a-f0-9]{64}$/),
                    }),
                )
                .max(32),
        }),
    );
    if (manifest.taskId !== id) throw new BenchError("TASK_EVIDENCE_IDENTITY", "Evidence belongs to another task", 6);
    for (const required of ["plan.json", "controller.json", "submission.json", "events.ndjson", "outcome.json"])
        if (!manifest.files.some((f) => f.path === required))
            throw new BenchError("TASK_EVIDENCE_INCOMPLETE", `Missing lifecycle evidence: ${required}`, 6);
    await verifyProtocolArtifacts(
        state.evidenceDir,
        manifest.files.map((f) => ({ ...f, bytes: String(f.bytes) })),
    );
    return { ...manifest, integrityVerified: true, benchmarkValid: false };
}
