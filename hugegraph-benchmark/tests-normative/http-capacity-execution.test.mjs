/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, writeFile, chmod, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { pathToFileURL } from "node:url";
import {
    httpCapacityExecution,
    httpCapacityExecutionRuntimeSchema,
    prepareHttpCapacityExecution,
} from "../dist/http-capacity-execution.js";

const delay = (milliseconds) => new Promise((resolve) => setTimeout(resolve, milliseconds));
async function ready(path) {
    const end = Date.now() + 5000;
    while (Date.now() < end) {
        try {
            return JSON.parse(await readFile(path, "utf8"));
        } catch {
            await delay(10);
        }
    }
    throw new Error("Local fixture did not become ready");
}
function gone(pid) {
    assert.throws(() => process.kill(pid, 0), { code: "ESRCH" });
}
async function fakePeer(t, mode = "hang") {
    const root = await mkdtemp(join(tmpdir(), "hgbench-capacity-wrapper-"));
    t.after(() => rm(root, { recursive: true, force: true }));
    const program = join(root, "local-protocol-fixture"),
        marker = join(root, "ready.json");
    // A local subprocess fixture only; it never pretends to provide a valid preparation or runtime proof.
    await writeFile(
        program,
        `#!/usr/bin/env python3
import json,os,pathlib,subprocess,sys,time
child=subprocess.Popen([sys.executable,'-B','-c','import time; time.sleep(60)'])
pathlib.Path(${JSON.stringify(marker)}).write_text(json.dumps({'pid':os.getpid(),'child':child.pid}))
${mode === "overflow" ? "sys.stdout.write('x'*(16*1024*1024+1)); sys.stdout.flush()" : ""}
time.sleep(60)
`,
    );
    await chmod(program, 0o700);
    return { root, program, marker };
}

test("capacity executor describes existing generic task and honest cancellation limits", async () => {
    const result = await httpCapacityExecution("describe");
    assert.equal(result.kind, "http-capacity-workflow-capability");
    assert.equal(result.cleanupMilliseconds, 20000);
    assert.equal(result.terminateGraceMilliseconds, 30000);
    assert.equal(result.performanceAccepted, false);
});

test("execution runtime has no resource overrides and 90-second cleanup is rejected", () => {
    const runtime = {
        schemaVersion: 1,
        kind: "http-capacity-execution-runtime",
        cleanWindow: { systemctlExecutable: "/usr/bin/systemctl", requireClean: true, units: ["inactive.service"] },
        budgets: {
            prepareMilliseconds: 60000,
            commandMilliseconds: 1000,
            maxCommandBytes: 1048576,
            maxWorkBytes: 1073741824,
            minFreeBytes: 0,
            cleanupMilliseconds: 20000,
        },
    };
    assert.equal(httpCapacityExecutionRuntimeSchema.safeParse(runtime).success, true);
    assert.equal(httpCapacityExecutionRuntimeSchema.safeParse({ ...runtime, comparisonRuntime: {} }).success, false);
    assert.equal(
        httpCapacityExecutionRuntimeSchema.safeParse({
            ...runtime,
            budgets: { ...runtime.budgets, cleanupMilliseconds: 90000 },
        }).success,
        false,
    );
});

test("already aborted command starts no producer", async () => {
    const controller = new AbortController();
    controller.abort();
    await assert.rejects(httpCapacityExecution("describe", undefined, { signal: controller.signal }), {
        code: "HTTP_CAPACITY_CANCELLED",
    });
});

test("confirmation wrapper tracks the strict confirmation files using the same generic task", async (t) => {
    const root = await mkdtemp(join(tmpdir(), "hgbench-confirmation-wrapper-"));
    t.after(() => rm(root, { recursive: true, force: true }));
    const runtime = {
        schemaVersion: 1,
        kind: "http-capacity-execution-runtime",
        cleanWindow: { systemctlExecutable: "/fixture/systemctl", requireClean: true, units: ["fixture.service"] },
        budgets: {
            prepareMilliseconds: 60000,
            commandMilliseconds: 1000,
            maxCommandBytes: 1048576,
            maxWorkBytes: 1073741824,
            minFreeBytes: 0,
            cleanupMilliseconds: 20000,
        },
    };
    const input = join(root, "runtime.json"),
        peer = join(root, "explicit-local-protocol-fixture");
    await writeFile(input, JSON.stringify(runtime));
    // This fake transport checks wrapper/task compilation only. It provides no valid scientific
    // plan, source-report proof, target identity or runnable benchmark and never invokes a task.
    const program = (invalidFiles) => `#!/usr/bin/env python3
import json,pathlib,sys
out=pathlib.Path(sys.argv[sys.argv.index('--out')+1]); out.mkdir()
runtime=json.loads(pathlib.Path(sys.argv[sys.argv.index('--runtime')+1]).read_text())
(out/'capacity').mkdir()
for name in ['capacity-workflow.py','calibration-workflow.py','comparison-workflow.py','comparison-runtime.json','execution-runtime.json','workflow.lock.json']:
    (out/name).write_text('explicit local protocol fixture; not runnable')
(out/'workflow.json').write_text(json.dumps({'runtime':{'budgets':runtime['budgets'],'comparisonRuntime':{'hostPython':str(pathlib.Path(sys.executable).resolve())}},'maxRunMilliseconds':300000}))
(out/'capacity/confirmation-plan.json').write_text(json.dumps({'phase':'confirmation','planDigest':'a'*64}))
(out/'capacity/confirmation.lock.json').write_text('{}')
print(json.dumps({'ok':True,'data':{'attempt':str(out),'executed':False,'planSha256':'b'*64,'maxRunMilliseconds':300000,'phase':'confirmation','planFiles':${JSON.stringify(invalidFiles ? ["../foreign.json", "confirmation.lock.json"] : ["confirmation-plan.json", "confirmation.lock.json"])}}}))
`;
    await writeFile(peer, program(false));
    await chmod(peer, 0o700);
    const previous = process.env.HGBENCH_PYTHON;
    process.env.HGBENCH_PYTHON = peer;
    try {
        const reply = await prepareHttpCapacityExecution(input, join(root, "inert-plan"), join(root, "attempt"));
        const task = JSON.parse(await readFile(reply.taskPlan, "utf8"));
        assert.equal(task.plan.name, "http-capacity-confirmation");
        assert.equal(task.plan.timeoutMs, 300000);
        assert.equal(task.plan.terminateGraceMs, 30000);
        assert(task.plan.trackedFiles.some((f) => f.endsWith("/capacity/confirmation-plan.json")));
        assert(task.plan.trackedFiles.some((f) => f.endsWith("/capacity/confirmation.lock.json")));
        assert(!task.plan.trackedFiles.some((f) => f.endsWith("/capacity/capacity-plan.json")));
        assert.equal(reply.executed, false);
        assert.equal(reply.performanceAccepted, false);
        await writeFile(peer, program(true));
        await assert.rejects(prepareHttpCapacityExecution(input, join(root, "inert-plan"), join(root, "rejected")), {
            code: "HTTP_CAPACITY_PROTOCOL",
        });
    } finally {
        if (previous === undefined) delete process.env.HGBENCH_PYTHON;
        else process.env.HGBENCH_PYTHON = previous;
    }
});

test("AbortSignal terminates the component and its real descendant group", async (t) => {
    const f = await fakePeer(t),
        previous = process.env.HGBENCH_PYTHON;
    process.env.HGBENCH_PYTHON = f.program;
    const controller = new AbortController();
    try {
        const running = httpCapacityExecution("describe", undefined, { signal: controller.signal });
        const peer = await ready(f.marker);
        controller.abort();
        await assert.rejects(running, { code: "HTTP_CAPACITY_CANCELLED" });
        gone(peer.pid);
        gone(peer.child);
    } finally {
        if (previous === undefined) delete process.env.HGBENCH_PYTHON;
        else process.env.HGBENCH_PYTHON = previous;
    }
});

test("owner SIGTERM reaches the same group cleanup instead of leaving the helper detached", async (t) => {
    const f = await fakePeer(t);
    const script = `import { httpCapacityExecution } from ${JSON.stringify(pathToFileURL(resolve("dist/http-capacity-execution.js")).href)};
try { await httpCapacityExecution('describe'); process.exitCode=2; }
catch(error) { console.log(JSON.stringify({code:error.code,exitCode:error.exitCode})); }`;
    const owner = spawn(process.execPath, ["--input-type=module", "-e", script], {
        env: { ...process.env, HGBENCH_PYTHON: f.program },
        stdio: ["ignore", "pipe", "pipe"],
    });
    t.after(() => {
        if (owner.exitCode === null) owner.kill("SIGKILL");
    });
    let stdout = "",
        stderr = "";
    owner.stdout.on("data", (part) => (stdout += part));
    owner.stderr.on("data", (part) => (stderr += part));
    const peer = await ready(f.marker),
        exited = once(owner, "close");
    owner.kill("SIGTERM");
    const [code] = await exited;
    assert.equal(code, 0, stderr);
    assert.deepEqual(JSON.parse(stdout), { code: "HTTP_CAPACITY_CANCELLED", exitCode: 143 });
    gone(peer.pid);
    gone(peer.child);
});

test("component output overflow fails boundedly and stops its descendant group", async (t) => {
    const f = await fakePeer(t, "overflow"),
        previous = process.env.HGBENCH_PYTHON;
    process.env.HGBENCH_PYTHON = f.program;
    try {
        await assert.rejects(httpCapacityExecution("describe"), { code: "HTTP_CAPACITY_OUTPUT" });
        const peer = await ready(f.marker);
        gone(peer.pid);
        gone(peer.child);
    } finally {
        if (previous === undefined) delete process.env.HGBENCH_PYTHON;
        else process.env.HGBENCH_PYTHON = previous;
    }
});
