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

// Offline preparation uses synthetic comparison replies. Task identity is also
// checked through real public CLI plan/run/status/cancel; no Docker/Java/measurement.
import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, mkdir, writeFile, readFile, symlink, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve, isAbsolute } from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { taskRequestId } from "../dist/tasks.js";
const exec = promisify(execFile),
    MiB = 1024 * 1024,
    cli = resolve("dist/cli.js");
const put = (path, value) => writeFile(path, JSON.stringify(value));
const read = async (path) => JSON.parse(await readFile(path, "utf8"));

test("cohort public CLI exposes strict capability and lifecycle help", async () => {
    const reply = JSON.parse((await exec(process.execPath, [cli, "http", "calibration", "describe"])).stdout);
    assert.equal(reply.data.workflow.runtimeKind, "http-calibration-runtime");
    assert.equal(reply.data.workflow.qualificationAccepted, false);
    for (const action of ["prepare", "status", "stop"])
        assert.match((await exec(process.execPath, [cli, "http", "calibration", action, "--help"])).stdout, /cohort/);
});

test(
    "public offline cohort preparation freezes a task and absolute recovery paths; stop does not dispatch",
    {
        skip: process.getuid?.() === 0 ? "cohort preparation requires a nonroot owner" : false,
    },
    async (t) => {
        const root = await mkdtemp(join(tmpdir(), "hgbench-cohort-cli-"));
        t.after(() => rm(root, { recursive: true, force: true }));
        const source = join(root, "source"),
            node = join(root, "node"),
            calibration = join(root, "calibration");
        await mkdir(join(source, "dist"), { recursive: true });
        await mkdir(join(node, "bin"), { recursive: true });
        await mkdir(calibration);
        await symlink(process.execPath, join(node, "bin/node"));
        await put(join(calibration, "plan.json"), { fixture: true });
        await put(join(calibration, "plan.lock.json"), { fixture: true });
        const verification = {
            planDigest: "a".repeat(64),
            contentDigest: "b".repeat(64),
            experiments: ["e01", "e02"].map((id) => ({
                id,
                directory: "experiments/" + id,
                planDigest: "c".repeat(64),
            })),
        };
        await writeFile(
            join(source, "dist/cli.js"),
            `
        const fs = require('node:fs'), path = require('node:path'), args = process.argv.slice(2);
        let data;
        if (args.slice(0,3).join(' ') === 'http calibration verify') data = ${JSON.stringify(verification)};
        else if (args.slice(0,3).join(' ') === 'http comparison prepare') {
            const out = args[args.indexOf('--out') + 1]; fs.mkdirSync(out);
            const taskPlan = path.join(out,'task.json');
            fs.writeFileSync(taskPlan, JSON.stringify({digest:'e'.repeat(64),plan:{timeoutMs:1000}}));
            data = {attempt:out,executed:false,taskPlan};
        } else throw new Error('unexpected command');
        process.stdout.write(JSON.stringify({ok:true,data}));
    `,
        );
        const runtime = {
            schemaVersion: 1,
            kind: "http-calibration-runtime",
            comparisonRuntime: {
                schemaVersion: 1,
                kind: "http-comparison-runtime",
                sourceRoot: source,
                nodeHome: node,
                hostPython: process.env.HGBENCH_PYTHON ?? "/usr/bin/python3",
                dockerExecutable: "/usr/bin/docker",
                driver: {
                    image: "sha256:" + "a".repeat(64),
                    python: "/usr/bin/python3",
                    cpus: 1,
                    memoryBytes: 512 * MiB,
                    pids: 128,
                    tmpfsBytes: 64 * MiB,
                    wallSeconds: 600,
                    shutdownSeconds: 1,
                    maxLogBytes: MiB,
                },
                collector: { intervalMs: 100, durationMs: 300000, maxSamples: 3001, maxOutputBytes: MiB },
                supervisor: {
                    pollMs: 100,
                    commandMs: 1000,
                    maxCommandBytes: MiB,
                    minFreeBytes: 0,
                    maxWorkBytes: 64 * MiB,
                },
            },
            cleanWindow: {
                systemctlExecutable: "/usr/bin/systemctl",
                requireClean: true,
                units: ["retained-job.service"],
            },
            budgets: {
                prepareMilliseconds: 60000,
                maxRunMilliseconds: 300000,
                pollMilliseconds: 1000,
                commandMilliseconds: 1000,
                maxCommandBytes: MiB,
                maxWorkBytes: 64 * MiB,
                minFreeBytes: 0,
                cleanupMilliseconds: 10000,
            },
        };
        const runtimeFile = join(root, "runtime.json"),
            out = join(root, "cohort");
        await put(runtimeFile, { ...runtime, budgets: Object.fromEntries(Object.entries(runtime.budgets).reverse()) });
        const invoke = async (...args) =>
            JSON.parse(
                (
                    await exec(process.execPath, [cli, "http", "calibration", ...args], {
                        timeout: 30000,
                        maxBuffer: 2 * MiB,
                    })
                ).stdout,
            );
        const prepared = await invoke("prepare", runtimeFile, "--plan", calibration, "--out", out);
        assert.equal(prepared.data.executed, false);
        assert.equal(prepared.data.children.length, 2);
        for (const child of prepared.data.children) assert.equal(child.taskId, taskRequestId(child.requestId));
        assert.equal(prepared.data.replayAllowed, false);
        assert.ok(isAbsolute(prepared.data.taskPlan));
        assert.ok(prepared.nextActions.find((x) => x.argv.includes(prepared.data.taskPlan)));
        assert.ok(prepared.nextActions.find((x) => x.argv.at(-1) === out));
        const task = await read(prepared.data.taskPlan);
        assert.equal(task.plan.terminateGraceMs, 30000);
        assert.equal(task.plan.timeoutMs, 320000);
        assert.equal(task.plan.effect, "database");
        assert.equal((await invoke("status", out)).data.state, "planned");
        assert.equal((await invoke("stop", out)).data.stopRequested, true);
        assert.equal((await invoke("stop", out)).data.stopRequested, true);
        await assert.rejects(readFile(join(out, "dispatch.json")), { code: "ENOENT" });
        await assert.rejects(invoke("prepare", runtimeFile, "--plan", calibration, "--out", out), (error) => {
            const reply = JSON.parse(error.stdout);
            assert.equal(reply.ok, false);
            assert.equal(reply.error.code, "HTTP_CALIBRATION_FAILED");
            return error.code === 6;
        });
        runtime.cleanWindow.units = [];
        await put(runtimeFile, runtime);
        await assert.rejects(invoke("prepare", runtimeFile, "--plan", calibration, "--out", join(root, "invalid")));
        await assert.rejects(readFile(join(root, "invalid/workflow.json")), { code: "ENOENT" });
    },
);

test("Python predeclared cohort task ID matches real public CLI submit/status/cancel", async (t) => {
    const root = await mkdtemp(join(tmpdir(), "hgbench-cohort-real-task-"));
    const taskRoot = join(root, "task-state");
    const invoke = async (...args) =>
        JSON.parse(
            (
                await exec(process.execPath, [cli, "--root", taskRoot, "task", ...args], {
                    timeout: 15000,
                    maxBuffer: MiB,
                })
            ).stdout,
        ).data;
    let submittedId;
    t.after(async () => {
        try {
            if (submittedId) {
                await invoke("cancel", "--id", submittedId);
                const terminal = await invoke("wait", "--id", submittedId, "--timeout-ms", "5000");
                assert.equal(terminal.terminal, true, "fixture task must be reaped before removing its evidence");
            }
        } finally {
            await rm(root, { recursive: true, force: true });
        }
    });
    const requestIds = [
        "calibration:0123456789abcdef0123456789abcdef:e01",
        'quote"back\\slash',
        "x".repeat(128),
        "",
        "contains space",
        "汉",
        "🙂",
        "\ud800",
        "\n",
        "\x7f",
        "x".repeat(129),
    ];
    const python = process.env.HGBENCH_PYTHON ?? "/usr/bin/python3";
    const pythonSource = `
import importlib.util,json,sys
from pathlib import Path
path=Path(sys.argv[1])
spec=importlib.util.spec_from_file_location('cohort_task_id_contract',path)
module=importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
results=[]
for value in json.loads(sys.argv[2]):
    try: results.append({'taskId':module.task_id(value)})
    except module.COMMON.WorkflowError: results.append({'rejected':True})
print(json.dumps(results))
`;
    const expected = JSON.parse(
        (
            await exec(
                python,
                [
                    "-B",
                    "-c",
                    pythonSource,
                    resolve("components/http/calibration-workflow.py"),
                    JSON.stringify(requestIds),
                ],
                { timeout: 10000, maxBuffer: MiB },
            )
        ).stdout,
    );
    for (const [index, key] of requestIds.entries()) {
        if (expected[index].rejected) assert.throws(() => taskRequestId(key), { code: "INVALID_REQUEST_ID" });
        else assert.equal(expected[index].taskId, taskRequestId(key));
    }
    await writeFile(join(root, "component.mjs"), "setInterval(() => {}, 1000);\n");
    await put(join(root, "task.source.json"), {
        schemaVersion: 1,
        kind: "component-task",
        name: "cohort-task-id-contract",
        command: [process.execPath, "component.mjs"],
        cwd: root,
        trackedFiles: ["component.mjs"],
        effect: "read-only",
        timeoutMs: 30000,
        terminateGraceMs: 100,
        maxLogBytes: 1024,
    });
    const frozenFile = join(root, "task.json");
    const planned = await invoke("plan", join(root, "task.source.json"), "--out", frozenFile);
    assert.equal(planned.executed, false);
    const submitted = await invoke("run", "--plan", frozenFile, "--request-id", requestIds[0], "--detach");
    submittedId = submitted.taskId;
    assert.equal(submittedId, expected[0].taskId);
    assert.equal(submitted.planDigest, planned.frozen.digest);
    assert.notEqual(submitted.reused, true);
    const snapshot = await invoke("status", "--request-id", requestIds[0]);
    assert.equal(snapshot.taskId, expected[0].taskId);
    assert.equal(snapshot.planDigest, planned.frozen.digest);
    assert.equal(snapshot.interrupted, false);
    const cancelled = await invoke("cancel", "--id", expected[0].taskId);
    assert.equal(cancelled.taskId, expected[0].taskId);
    assert.equal(cancelled.planDigest, planned.frozen.digest);
    const terminal = await invoke("wait", "--id", expected[0].taskId, "--timeout-ms", "5000");
    assert.equal(terminal.taskId, expected[0].taskId);
    assert.equal(terminal.planDigest, planned.frozen.digest);
    assert.equal(terminal.terminal, true);
    assert.equal(terminal.state, "cancelled");
    assert.equal(terminal.interrupted, false);
    assert.equal(terminal.benchmarkValid, false);
});
