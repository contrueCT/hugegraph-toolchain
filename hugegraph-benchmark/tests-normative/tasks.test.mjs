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
import { mkdtemp, readFile, writeFile, rm, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { execFile, fork } from "node:child_process";
import { promisify } from "node:util";
import {
    freezeTask,
    submitTask,
    waitTask,
    taskSnapshot,
    cancelTask,
    taskEvents,
    taskLogs,
    taskArtifacts,
    processStart,
} from "../dist/tasks.js";
import { hash } from "../dist/io.js";

async function fixture(t, script, options = {}) {
    const root = await mkdtemp(join(tmpdir(), "hgbench-task-test-"));
    const ids = [];
    t.after(async () => {
        for (const id of ids) {
            await cancelTask(root, id);
            await waitTask(root, id, 5000);
        }
        await rm(root, { recursive: true, force: true });
    });
    await writeFile(join(root, "component.mjs"), script);
    const source = {
        schemaVersion: 1,
        kind: "component-task",
        name: "contract-check",
        command: [process.execPath, "component.mjs"],
        cwd: ".",
        trackedFiles: ["component.mjs"],
        effect: "filesystem",
        timeoutMs: 5000,
        terminateGraceMs: 150,
        maxLogBytes: 2048,
        ...options,
    };
    await writeFile(join(root, "task.json"), JSON.stringify(source));
    const plan = await freezeTask(join(root, "task.json"));
    return {
        root,
        plan,
        run: async (key) => {
            const result = await submitTask(root, plan, key);
            ids.push(result.taskId);
            return result;
        },
    };
}
async function until(fn, ms = 7000) {
    const end = Date.now() + ms;
    while (true) {
        const result = await fn();
        if (result) return result;
        if (Date.now() > end) throw new Error("condition timed out");
        await new Promise((r) => setTimeout(r, 30));
    }
}
async function childPid(root, id) {
    const path = join(root, "tasks", id, "child-pid");
    return until(async () => {
        try {
            return Number(await readFile(path, "utf8"));
        } catch {
            return false;
        }
    });
}
const descendants = `import {spawn} from 'node:child_process';
import {writeFileSync} from 'node:fs';
import {join} from 'node:path';
const child=spawn(process.execPath,['-e', 'process.on("SIGTERM",()=>{}); setInterval(()=>{},1000)'],{stdio:'ignore'});
writeFileSync(join(process.env.HGBENCH_TASK_DIR,'child-pid'),String(child.pid));
process.on('SIGTERM',()=>{});
setInterval(()=>{},1000);`;

test("tasks: concurrent duplicate submission runs once; changed plan conflicts; logs/events remain bounded", async (t) => {
    const f = await fixture(
        t,
        `import {appendFileSync} from 'node:fs'; appendFileSync('calls','x'); console.log('component output');`,
        { effect: "read-only" },
    );
    const submissions = await Promise.all(Array.from({ length: 4 }, () => f.run("same-key")));
    assert.equal(new Set(submissions.map((s) => s.taskId)).size, 1);
    assert.equal(submissions.filter((s) => !s.reused).length, 1);
    const id = submissions[0].taskId;
    const state = await waitTask(f.root, id, 10000);
    assert.equal(state.state, "completed", JSON.stringify(state));
    assert.equal(state.benchmarkValid, false);
    assert.equal(await readFile(join(f.root, "calls"), "utf8"), "x");
    const events = await taskEvents(f.root, id, 0, 1);
    assert.equal(events.events.length, 1);
    assert.equal(events.hasMore, true);
    const rest = await taskEvents(f.root, id, events.nextCursor);
    assert.deepEqual(
        rest.events.map((e) => e.type),
        ["dispatch-intent", "finished"],
    );
    await assert.rejects(taskEvents(f.root, id, 999), (e) => e.code === "INVALID_CURSOR");
    assert.match((await taskLogs(f.root, id, "stdout")).text, /component output/);
    const changed = structuredClone(f.plan);
    changed.plan.timeoutMs += 100;
    const { digest, ...content } = changed;
    changed.digest = hash(content);
    await assert.rejects(submitTask(f.root, changed, "same-key"), (e) => e.code === "REQUEST_ID_CONFLICT");
    // Recovering an existing submission must work even if component files later change.
    await writeFile(join(f.root, "component.mjs"), "throw new Error('changed')");
    assert.equal((await f.run("same-key")).reused, true);
    const manifest = JSON.parse(await readFile(join(state.evidenceDir, "evidence.json"), "utf8"));
    assert.ok(manifest.files.some((file) => file.path === "process-result.json"));
    assert.equal((await taskArtifacts(f.root, id)).integrityVerified, true);
    await writeFile(join(state.evidenceDir, "stdout.log"), "changed evidence");
    await assert.rejects(taskArtifacts(f.root, id), (e) => e.code === "SF100_PROTOCOL_INVALID");
});
test("tasks: source drift is rejected before dispatch", async (t) => {
    const f = await fixture(t, "console.log('original')");
    await writeFile(join(f.root, "component.mjs"), "console.log('drift')");
    await assert.rejects(f.run("drift"), (e) => e.code === "COMPONENT_CHANGED");
});
test("tasks: nonzero exit and output overflow retain failure and partial logs", async (t) => {
    for (const [script, expected] of [
        ["process.exit(7)", "COMPONENT_FAILED"],
        ["console.log('x'.repeat(65536));setInterval(()=>{},1000)", "TASK_LOG_LIMIT"],
    ]) {
        const f = await fixture(t, script);
        const submitted = await f.run();
        const state = await waitTask(f.root, submitted.taskId, 10000);
        assert.equal(state.state, "failed", JSON.stringify(state));
        assert.equal(state.error.code, expected);
        assert.equal(state.effect, "unknown");
        if (expected === "TASK_LOG_LIMIT")
            assert.ok((await stat(join(state.evidenceDir, "stdout.log"))).size <= f.plan.plan.maxLogBytes);
    }
});
test("tasks: cancel escalates to KILL for stubborn descendants and retains unknown database effects", async (t) => {
    const f = await fixture(t, descendants, { effect: "database" });
    const submitted = await f.run();
    const id = submitted.taskId;
    const pid = await childPid(f.root, id);
    const before = await waitTask(f.root, id, 0);
    assert.equal(before.terminal, false);
    await cancelTask(f.root, id);
    await cancelTask(f.root, id);
    const state = await waitTask(f.root, id, 10000);
    assert.equal(state.state, "cancelled", JSON.stringify(state));
    assert.equal(state.effect, "unknown");
    await until(async () => (await processStart(pid)) === null);
    assert.equal((await cancelTask(f.root, id)).cancellationComplete, true);
});
test("tasks: deadline terminates the component group", async (t) => {
    const f = await fixture(t, descendants, { timeoutMs: 1200 });
    const submitted = await f.run();
    const pid = await childPid(f.root, submitted.taskId);
    const state = await waitTask(f.root, submitted.taskId, 10000);
    assert.equal(state.error.code, "TASK_TIMEOUT", JSON.stringify(state));
    await until(async () => (await processStart(pid)) === null);
});
test("tasks: worker SIGKILL causes guardian to stop descendants without replay", async (t) => {
    // This case injects owner death; its outer timeout must not compete with the fault under test.
    const f = await fixture(t, descendants, { timeoutMs: 30000 });
    const submitted = await f.run();
    const id = submitted.taskId;
    const pid = await childPid(f.root, id);
    const owner = JSON.parse(await readFile(join(f.root, "tasks", id, "owner.json"), "utf8"));
    let observed = await taskSnapshot(f.root, id);
    assert.equal(observed.terminal, false, JSON.stringify(observed));
    assert.equal(observed.phase, "execute", JSON.stringify(observed));
    assert.equal(observed.ownerStatus, "alive", JSON.stringify(observed));
    process.kill(owner.pid, "SIGKILL");
    try {
        await until(async () => {
            observed = await taskSnapshot(f.root, id);
            assert(
                !observed.terminal || observed.interrupted,
                `Task reached an unrelated terminal state before owner-death observation: ${JSON.stringify(observed)}`,
            );
            return observed.ownerStatus === "gone";
        }, 10000);
    } catch (error) {
        throw new Error(`Owner death was not established: ${JSON.stringify(observed)}`, { cause: error });
    }
    const result = await until(async () => {
        try {
            return JSON.parse(await readFile(join(observed.evidenceDir, "process-result.json"), "utf8"));
        } catch {
            return null;
        }
    });
    assert.equal(result.error?.code, "WORKER_DISCONNECTED", JSON.stringify(result));
    await until(async () => (await processStart(pid)) === null);
    const state = await until(async () => {
        observed = await taskSnapshot(f.root, id);
        assert(!observed.terminal || observed.interrupted, JSON.stringify(observed));
        return observed.interrupted ? observed : null;
    });
    assert.equal(state.ownerStatus, "gone", JSON.stringify(state));
    assert.equal(state.effect, "unknown", JSON.stringify(state));
});
test("tasks: leader exit cleans descendants that inherit stdout", async (t) => {
    const script = descendants
        .replace("{stdio:'ignore'}", "{stdio:'inherit'}")
        .replace("setInterval(()=>{},1000);", "setTimeout(()=>process.exit(0),150);");
    const f = await fixture(t, script);
    const submitted = await f.run();
    const pid = await childPid(f.root, submitted.taskId);
    const state = await waitTask(f.root, submitted.taskId, 10000);
    assert.equal(state.state, "completed", JSON.stringify(state));
    await until(async () => (await processStart(pid)) === null);
});
test("tasks: log persistence failure stops the component and cannot become a successful result", async (t) => {
    const f = await fixture(t, "console.log('x'.repeat(65536));setInterval(()=>{},1000)");
    await writeFile(join(f.root, "stdout.log"), "occupied");
    const guard = fork(new URL("../dist/task-process.js", import.meta.url), [], {
        stdio: ["ignore", "ignore", "ignore", "ipc"],
    });
    t.after(() => {
        if (guard.connected) guard.disconnect();
    });
    const result = await new Promise((resolve, reject) => {
        guard.once("message", (message) => resolve(message.result));
        guard.once("error", reject);
        guard.once("exit", () => reject(new Error("Guardian exited without result")));
        guard.send({ dir: f.root, task: f.plan });
    });
    assert.equal(result.error.code, "EVIDENCE_WRITE_FAILED");
});
test("tasks: CLI help/schema/dry-run and real specification component complete without a database", async (t) => {
    const root = await mkdtemp(join(tmpdir(), "hgbench-task-cli-"));
    t.after(() => rm(root, { recursive: true, force: true }));
    const cli = (args) => promisify(execFile)(process.execPath, ["dist/cli.js", "--root", root, ...args]);
    assert.match((await cli(["task", "run", "--help"])).stdout, /request-id/);
    assert.match((await cli(["task", "list", "--help"])).stdout, /CURSOR_STALE/);
    const commands = JSON.parse((await cli(["schema", "command:task"])).stdout).data;
    assert.ok(commands.commands.some((command) => command.name === "list"));
    assert.ok(JSON.parse((await cli(["schema", "task-plan"])).stdout).data.properties.command);
    const plan = join(root, "frozen.json");
    await cli(["task", "plan", "examples/spec-check-task.json", "--out", plan]);
    assert.equal(JSON.parse((await cli(["task", "run", "--plan", plan, "--dry-run"])).stdout).data.executed, false);
    await assert.rejects(stat(join(root, "tasks")), (e) => e.code === "ENOENT");
    const result = JSON.parse((await cli(["task", "run", "--plan", plan, "--request-id", "spec"])).stdout);
    assert.equal(result.data.state, "completed");
    assert.equal(result.data.benchmarkValid, false);
    const spec = JSON.parse((await taskLogs(root, result.data.taskId, "stdout")).text);
    const listed = JSON.parse((await cli(["task", "list", "--limit", "1"])).stdout);
    assert.equal(listed.data.items[0].taskId, result.data.taskId);
    assert.equal(listed.data.items[0].snapshot.state, "completed");
    assert.equal(listed.data.items[0].snapshot.benchmarkValid, false);
    assert.ok(listed.nextActions.some((action) => action.argv.includes("status")));
    assert.equal(spec.ok, true);
});
