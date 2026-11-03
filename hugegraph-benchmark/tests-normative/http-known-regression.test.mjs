/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
import test from "node:test";
import assert from "node:assert/strict";
import { readFile, writeFile, rename, symlink, lstat, readdir } from "node:fs/promises";
import { join, resolve } from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import {
    reportKnownRegression,
    validateKnownRegressionInput,
    verifyKnown002Lifecycle,
    verifyKnown002CommandClosure,
    checkKnownRegressionDeadline,
} from "../components/http/known-regression-evidence.mjs";
import {
    KNOWN002,
    known002Policy,
    summarizeKnown002,
    knownTaskId,
    knownSha,
} from "../components/http/known002-contract.mjs";
import { knownFixture, knownLifecycle, get, put } from "./http-known-regression.fixture.mjs";
const MiB = 1024 ** 2,
    run = promisify(execFile);
const opts = (f, out = "report") => ({
    inputFile: f.inputFile,
    outputDirectory: join(f.root, out),
    timeoutMs: 10000,
    maxBytes: MiB,
});
const exists = (p) =>
    lstat(p).then(
        () => true,
        (e) => {
            if (e.code === "ENOENT") return false;
            throw e;
        },
    );

test("the fixed 529 source inventory fixture is the exact original selection, not executed code", async () => {
    const raw = await readFile(new URL("./fixtures/known002-source-selection.lock.json", import.meta.url));
    assert.equal(knownSha(raw), KNOWN002.sourceSelectionSha256);
    assert.equal(JSON.parse(raw).files.length, 529);
    assert.equal(knownTaskId("known-002"), "task-" + knownSha(JSON.stringify("known-002")).slice(0, 32));
});
test("seal deadline checks reject late cancellation and elapsed overall budget", () => {
    const now = performance.now();
    assert.doesNotThrow(() => checkKnownRegressionDeadline({ aborted: false }, now, 10000));
    assert.throws(() => checkKnownRegressionDeadline({ aborted: true }, now, 10000), { code: "KNOWN_CANCELLED" });
    assert.throws(() => checkKnownRegressionDeadline({ aborted: false }, now - 2000, 1000), { code: "KNOWN_DEADLINE" });
});
test("public offline IO retains every unexecuted arm as unknown and ignores operator passed flags", async (t) => {
    const f = await knownFixture(t);
    await put(join(f.execution, "result.json"), {
        rows: Array(16).fill({ status: "verified", cleanupComplete: true }),
        status: "passed",
    });
    const result = await reportKnownRegression(opts(f));
    assert.equal(result.status, "unknown");
    const report = await get(result.reportJson);
    assert.equal(report.plannedArms, 16);
    assert.equal(report.plannedPairs, 8);
    assert.equal(report.arms.length, 16);
    assert.equal(report.verifiedArms, 0);
    assert.deepEqual(report.verification.issues, ["KNOWN_NOT_EXECUTED"]);
    assert.equal(report.provenance.fullSourceBytesReverified, false);
    assert.equal(report.inferenceAccepted, false);
    assert.equal(report.performanceAccepted, false);
    const lock = await get(result.reportLock);
    for (const file of lock.files)
        assert.equal(file.sha256, knownSha(await readFile(join(result.directory, file.path))));
});
test("prepared execution claims cannot replace missing original HTTP raw or hide the complete denominator", async (t) => {
    const f = await knownFixture(t);
    f.sequence.mode = "prepare";
    for (const a of f.sequence.physicalArms) a.preparedTargetTask = true;
    f.spec.runnable = true;
    await put(join(f.plan, "sequence.json"), f.sequence);
    await f.reseal();
    const dispatch = {
        executed: true,
        executionSpecSha256: f.input.reference.executionSpecSha256,
        hostBootId: "00000000-0000-0000-0000-000000000001",
        startedMonotonicNs: "1",
    };
    await put(join(f.execution, "dispatch.json"), dispatch);
    await put(join(f.plan, "known-delay-dispatch.json"), dispatch);
    await put(join(f.execution, "commands/00001.json"), {
        argv: ["not-executed-fixture"],
        stdout: "",
        stderr: "",
        exitCode: 0,
        error: null,
        startedMonotonicNs: "2",
        finishedMonotonicNs: "3",
    });
    await put(join(f.execution, "result.json"), { status: "passed", rows: Array(16).fill({ status: "verified" }) });
    const result = await reportKnownRegression(opts(f));
    const report = await get(result.reportJson);
    assert.equal(report.status, "unknown");
    assert.equal(report.verification.valid, false);
    assert.equal(report.arms.length, 16);
    assert.equal(report.verifiedArms, 0);
    assert.equal(report.analysis.cells[0].plannedPairs, 8);
    assert.equal(report.analysis.cells[0].observedPairs, 0);
    assert.ok(report.arms.every((a) => !a.valid && a.issues.some((i) => i.includes("ENOENT"))));
});
test("搬移 keeps original absolute JSON unchanged and produces byte-identical scoped report", async (t) => {
    const f = await knownFixture(t);
    const one = await reportKnownRegression(opts(f, "before"));
    const before = await readFile(one.reportJson),
        source = await readFile(join(f.plan, "sequence.json"));
    const moved = join(f.root, "moved");
    await rename(f.plan, moved);
    f.input.planDirectory = "moved";
    await put(f.inputFile, f.input);
    const two = await reportKnownRegression(opts(f, "after"));
    const a = JSON.parse(before),
        b = await get(two.reportJson);
    // Only the caller's explicitly changed input path and its digest differ; original source/plan bytes do not.
    delete a.references.inputSha256;
    delete b.references.inputSha256;
    assert.deepEqual(a, b);
    assert.deepEqual(await readFile(join(moved, "sequence.json")), source);
});
test("re-sealing plan and spec cannot change fixed E2E threshold, script, observation count or single metric", async (t) => {
    for (const [label, path, mutate, code] of [
        [
            "threshold",
            "policy.json",
            (v) => {
                v.metrics[0].absoluteBudget = 1;
            },
            "KNOWN_POLICY",
        ],
        [
            "metric",
            "policy.json",
            (v) => {
                v.metrics[0].id = "response-p99";
            },
            "KNOWN_POLICY",
        ],
        [
            "observations",
            "policy.json",
            (v) => {
                v.metrics[0].minimumObservations = 100;
            },
            "KNOWN_POLICY",
        ],
        [
            "script",
            "profiles/B.json",
            (v) => {
                v.cases[0].body.gremlin = "1 + 1";
            },
            "KNOWN_FIXED_PROFILE",
        ],
    ]) {
        const f = await knownFixture(t),
            file = join(f.plan, path),
            value = await get(file);
        mutate(value);
        await put(file, value);
        await f.reseal();
        await assert.rejects(reportKnownRegression(opts(f, label)), { code });
        const partial = await get(join(f.root, label, "partial.json"));
        assert.equal(partial.status, "unknown");
        assert.deepEqual(partial.verification.issues, [code]);
        assert.equal(await exists(join(f.root, label, "report.lock.json")), false);
    }
});
test("input/reference strictness, changed plan member, symlink and FIFO cannot supply verified evidence", async (t) => {
    const f = await knownFixture(t);
    for (const mutate of [
        (v) => {
            v.reference.sourceSelectionSha256 = "a".repeat(64);
        },
        (v) => {
            v.status = "passed";
        },
        (v) => {
            v.reference.planLockSha256 = "bad";
        },
        (v) => {
            v.planDirectory = "bad\npath";
        },
        (v) => {
            v.executionDirectory = "bad\rpath";
        },
    ]) {
        const v = structuredClone(f.input);
        mutate(v);
        assert.throws(() => validateKnownRegressionInput(v));
    }
    await writeFile(join(f.plan, "policy.json"), "{}\n");
    await assert.rejects(reportKnownRegression(opts(f, "changed")), { code: "KNOWN_PLAN_MEMBER_CHANGED" });
    const g = await knownFixture(t);
    await symlink(g.inputFile, join(g.execution, "untrusted.json"));
    await assert.rejects(reportKnownRegression(opts(g, "symlink")), { code: "KNOWN_INPUT_TYPE_OR_BUDGET" });
    if (["darwin", "linux"].includes(process.platform)) {
        const h = await knownFixture(t);
        await run("mkfifo", [join(h.execution, "not-a-file")]);
        await assert.rejects(reportKnownRegression(opts(h, "fifo")), { code: "KNOWN_INPUT_TYPE_OR_BUDGET" });
    }
});
test("create-only output, output/input overlap and cancelled worker retain honest partials", async (t) => {
    const f = await knownFixture(t);
    await assert.rejects(reportKnownRegression({ ...opts(f), outputDirectory: join(f.plan, "report") }), {
        code: "KNOWN_OUTPUT_OVERLAP",
    });
    await reportKnownRegression(opts(f));
    await assert.rejects(reportKnownRegression(opts(f)), { code: "EEXIST" });
    const controller = new AbortController();
    controller.abort();
    await assert.rejects(reportKnownRegression({ ...opts(f, "cancelled"), signal: controller.signal }), {
        code: "KNOWN_CANCELLED",
    });
    const partial = await get(join(f.root, "cancelled/partial.json"));
    assert.equal(partial.status, "unknown");
    assert.equal(partial.plannedPairs, 8);
    assert.equal(await exists(join(f.root, "cancelled/report.lock.json")), false);
    await assert.rejects(reportKnownRegression({ ...opts(f, "deadline"), timeoutMs: 1000 }), {
        code: "KNOWN_DEADLINE",
    });
    assert.equal((await get(join(f.root, "deadline/partial.json"))).status, "unknown");
});
test("output cannot pollute original measured source, including a symlink alias or distributed tool sources", async (t) => {
    const f = await knownFixture(t, { localSource: true }),
        original = f.spec.sourceRoot,
        before = await readdir(original),
        alias = join(f.root, "source-alias");
    await symlink(original, alias);
    for (const outputDirectory of [join(original, "new-report"), join(alias, "new-report")])
        await assert.rejects(reportKnownRegression({ ...opts(f), outputDirectory }), { code: "KNOWN_OUTPUT_OVERLAP" });
    assert.deepEqual(await readdir(original), before);
    for (const directory of ["tools", "examples", "docs", "tests-normative", "adapters"])
        await assert.rejects(
            reportKnownRegression({ ...opts(f), outputDirectory: resolve(directory, "known-source-protection-test") }),
            { code: "KNOWN_OUTPUT_OVERLAP" },
        );
    const result = await reportKnownRegression(opts(f));
    assert.equal(result.status, "unknown");
    assert.deepEqual(await readdir(original), before);
});

test("raw task/inspect/normal-stop command protocol reconstructs a lifecycle without operator row booleans", () => {
    const f = knownLifecycle(),
        proof = verifyKnown002Lifecycle(f);
    assert.equal(proof.driverCid, "4".repeat(64));
    assert.equal(proof.targetCid, "3".repeat(64));
    assert.equal(proof.httpDigest, f.httpTask.digest);
});
test("real Docker absent [] output is accepted only for the exact stopped driver and target CID", () => {
    assert.doesNotThrow(() => verifyKnown002Lifecycle(knownLifecycle()));
    const empty = knownLifecycle();
    for (const c of empty.commands.filter((c) => c.exitCode === 1)) c.stdout = "";
    assert.doesNotThrow(() => verifyKnown002Lifecycle(empty));
    for (const which of [0, 1])
        for (const mutate of [
            (c) => {
                c.stdout = '[{"Id":"still-exists"}]\n';
            },
            (c) => {
                c.stderr = "Error response from daemon: No such container: " + "f".repeat(64) + "\n";
            },
            (c) => {
                c.exitCode = 0;
            },
            (c) => {
                c.error = "timeout";
            },
        ]) {
            const f = knownLifecycle();
            mutate(f.commands.filter((c) => c.exitCode === 1)[which]);
            assert.throws(() => verifyKnown002Lifecycle(f));
        }
});
test("fixed final paired-analyzer argv is allowed once, bound to original dispatch root and never consumed", () => {
    const f = knownLifecycle();
    const context = f.context;
    const result = { valid: true, ...verifyKnown002Lifecycle(f), runId: f.arm.runId, javaIdentity: { pid: 77 } };
    const dispatch = { executionRoot: "/original/execution" };
    const c = {
        index: 999,
        exitCode: 0,
        error: null,
        stdout: "untrusted saved analysis",
        stderr: "",
        argv: [
            context.spec.node,
            context.spec.sourceRoot + "/components/paired/analyzer.mjs",
            "--input",
            dispatch.executionRoot + "/verified-observations.json",
            "--output",
            dispatch.executionRoot + "/paired-analysis",
        ],
    };
    assert.doesNotThrow(() => verifyKnown002CommandClosure(context, [result], [...f.commands, c], dispatch));
    for (const change of [
        (commands) => commands.push(structuredClone(c)),
        (commands) => {
            commands.at(-1).argv[3] = "/another/execution/verified-observations.json";
        },
        (commands) => {
            commands.at(-1).exitCode = 1;
        },
    ]) {
        const commands = structuredClone([...f.commands, c]);
        change(commands);
        assert.throws(() => verifyKnown002CommandClosure(context, [result], commands, dispatch));
    }
});
test("wrong task or interrupted response, force-stop, missing running inspect, nonexact CID absence and order are refused", () => {
    for (const mutate of [
        (f) => {
            f.httpTerminal.taskId = "task-wrong";
        },
        (f) => {
            f.httpTerminal.planDigest = "e".repeat(64);
        },
        (f) => {
            f.httpTerminal.interrupted = true;
        },
        (f) => {
            f.driverFinal.State.ExitCode = 137;
        },
        (f) => {
            const lastRunningInspect = f.commands
                .filter(
                    (c) =>
                        c.argv[1] === "inspect" &&
                        c.argv[4] === f.driverFinal.Id &&
                        c.exitCode === 0 &&
                        JSON.parse(c.stdout)[0].State.Running,
                )
                .at(-1);
            f.commands = f.commands.filter((c) => c !== lastRunningInspect);
        },
        (f) => {
            f.commands.find((c) => c.exitCode === 1).stderr = "Error: No such object: another-container";
        },
        (f) => {
            f.commands.find((c) => c.argv[1] === "stop").error = "deadline";
        },
        (f) => {
            f.commands.find((c) => c.argv[1] === "rm").index = 0;
        },
        (f) => {
            f.commands.push(structuredClone(f.commands.find((c) => c.argv.includes(f.arm.requestId))));
        },
    ]) {
        const f = knownLifecycle();
        mutate(f);
        assert.throws(() => verifyKnown002Lifecycle(f));
    }
});
test("target final receipt must be normally stopped and share stable identity with validated environment final", () => {
    const updateInspect = (f) => {
        f.commands.find((c) => c.argv[1] === "inspect" && c.argv[4] === f.targetFinal.Id && c.exitCode === 0).stdout =
            JSON.stringify([f.targetFinal]);
    };
    const dynamic = knownLifecycle();
    dynamic.targetFinal.State.Pid = 0;
    dynamic.environmentFinal.State.Pid = 123;
    dynamic.environmentFinal.State.FinishedAt = "different later inspect";
    dynamic.targetFinal.Config.Labels["extra-observed-label"] = "unrelated";
    updateInspect(dynamic);
    assert.doesNotThrow(() => verifyKnown002Lifecycle(dynamic));
    for (const mutate of [
        (f) => {
            f.targetFinal.State.Running = true;
        },
        (f) => {
            f.targetFinal.State.OOMKilled = true;
        },
        (f) => {
            f.targetFinal.State.Status = "running";
        },
        (f) => {
            f.targetFinal.State.ExitCode = 137;
        },
        (f) => {
            delete f.targetFinal.State;
        },
        (f) => {
            f.environmentFinal.Id = "9".repeat(64);
        },
        (f) => {
            f.environmentFinal.Name = "/hgbench-http-" + "c".repeat(32);
        },
        (f) => {
            f.environmentFinal.Image = "sha256:" + "c".repeat(64);
        },
        (f) => {
            f.environmentFinal.Config.User = "1001:1001";
        },
        (f) => {
            f.environmentFinal.Config.Labels["org.apache.hugegraph.hgbench.http.attempt"] = "c".repeat(32);
        },
        (f) => {
            f.environmentFinal.Config.Labels["org.apache.hugegraph.hgbench.http.plan"] = "c".repeat(64);
        },
        (f) => {
            delete f.environmentFinal;
        },
    ]) {
        const f = knownLifecycle();
        mutate(f);
        updateInspect(f);
        assert.throws(() => verifyKnown002Lifecycle(f));
    }
});
const closureArm = (f) => ({
    valid: true,
    ...verifyKnown002Lifecycle(f),
    runId: f.arm.runId,
    javaIdentity: { pid: 77 },
});
const verifyClosure = (f) =>
    verifyKnown002CommandClosure(f.context, [closureArm(f)], f.commands, { executionRoot: "/original/execution" });
test("each lifecycle requires exact pre and post unit observations with original seven fields and inactive predecessors", () => {
    const good = knownLifecycle();
    assert.doesNotThrow(() => verifyClosure(good));
    for (const c of good.commands.filter((c) => c.argv[0] === "/usr/bin/systemctl")) {
        if (c.argv[3] === good.context.sequence.protectedObservedUnit)
            c.stdout = c.stdout.replace(
                "ActiveState=inactive\nSubState=dead\nMainPID=0\nControlGroup=",
                "ActiveState=active\nSubState=running\nMainPID=123\nControlGroup=/protected",
            );
        else
            c.stdout = c.stdout
                .replace("LoadState=loaded", "LoadState=not-found")
                .replace("Result=success", "Result=exit-code");
    }
    // d6 is observed, while GC'd inactive units do not claim historical success.
    assert.doesNotThrow(() => verifyClosure(good));
    for (const mutate of [
        (f) => {
            f.commands = f.commands.filter((c) => c.argv[0] !== "/usr/bin/systemctl");
        },
        (f, jobs) => {
            f.commands = f.commands.filter((c) => c !== jobs[0]);
        },
        (f, jobs) => {
            f.commands = f.commands.filter((c) => c !== jobs.at(-1));
        },
        (_f, jobs) => {
            jobs[0].stdout = jobs[0].stdout.replace("ActiveState=inactive", "ActiveState=active");
        },
        (_f, jobs) => {
            jobs[3].stdout = jobs[3].stdout.replace("MainPID=0", "MainPID=123");
        },
        (_f, jobs) => {
            jobs[0].stdout = jobs[0].stdout.replace("ControlGroup=\n", "ControlGroup=/busy\n");
        },
        (_f, jobs) => {
            jobs[0].stdout = jobs[0].stdout.replace("Result=success\n", "");
        },
        (_f, jobs) => {
            jobs[0].stdout += "Result=success\n";
        },
        (_f, jobs) => {
            jobs[0].stdout = jobs[0].stdout.replace("LoadState=loaded", "LoadState=error");
        },
        (_f, jobs) => {
            jobs[0].stdout = jobs[0].stdout.replace("Id=hgbench-sf100-convert-v1.service", "Id=wrong.service");
        },
        (_f, jobs) => {
            jobs[0].exitCode = 1;
        },
        (_f, jobs) => {
            jobs[0].error = "deadline";
        },
        (_f, jobs) => {
            jobs[0].argv = structuredClone(jobs[1].argv);
            jobs[0].stdout = jobs[1].stdout;
        },
    ]) {
        const f = knownLifecycle(),
            jobs = f.commands.filter((c) => c.argv[0] === "/usr/bin/systemctl");
        mutate(f, jobs);
        assert.throws(() => verifyClosure(f));
    }
});
test("adjacent post and pre idle windows cannot reuse a unit sweep to fill a deleted phase", () => {
    const first = knownLifecycle(),
        second = knownLifecycle({ blockId: "b02", targetCid: "5".repeat(64), driverCid: "6".repeat(64) });
    const context = {
        ...first.context,
        sequence: { ...first.context.sequence, physicalArms: [first.arm, second.arm] },
    };
    const arms = [closureArm(first), closureArm(second)];
    const check = (commands) => {
        const indexed = commands.map((c, index) => ({ ...c, index }));
        verifyKnown002CommandClosure(context, arms, indexed, { executionRoot: "/original/execution" });
    };
    assert.doesNotThrow(() => check([...first.commands, ...second.commands]));
    const n =
        first.context.sequence.declaredHeavyUnits.length + first.context.sequence.additionalInactiveUnits.length + 1;
    for (const [source, phase] of [
        [first, "pre"],
        [first, "post"],
        [second, "pre"],
        [second, "post"],
    ]) {
        const jobs = source.commands.filter((c) => c.argv[0] === "/usr/bin/systemctl"),
            removed = new Set(phase === "pre" ? jobs.slice(0, n) : jobs.slice(n));
        assert.throws(() => check([...first.commands, ...second.commands].filter((c) => !removed.has(c))));
    }
});
test("re-sealed unit declarations cannot omit fixed heavy units, replace d6, or omit prepared AA005 predecessor", async (t) => {
    for (const mutate of [
        (f) => {
            f.sequence.declaredHeavyUnits.pop();
        },
        (f) => {
            f.sequence.protectedObservedUnit = "another.service";
        },
        (f) => {
            f.sequence.mode = "prepare";
            for (const a of f.sequence.physicalArms) a.preparedTargetTask = true;
            f.spec.runnable = true;
            f.sequence.additionalInactiveUnits = [];
        },
    ]) {
        const f = await knownFixture(t);
        mutate(f);
        await put(join(f.plan, "sequence.json"), f.sequence);
        await f.reseal();
        await assert.rejects(reportKnownRegression(opts(f)), { code: "KNOWN_JOB_DECLARATIONS" });
    }
});
test("original statistical engine retains 8 pair denominator, 1000 per arm, degenerate and crossed uncertainty", () => {
    const policy = known002Policy({ test: "synthetic projection only, never raw-verification evidence" });
    const arms = KNOWN002.blocks.flatMap((blockId, i) =>
        ["A", "B"].map((arm) => ({
            blockId,
            arm,
            runId: blockId + arm,
            rawManifestSha256: knownSha(blockId + arm),
            valid: true,
            observations: 1200,
            endToEndP99: arm === "A" ? 2 + i * 0.01 : 12 + i * 0.1,
        })),
    );
    assert.equal(summarizeKnown002(policy, arms).status, "passed");
    for (const mutate of [
        (a) => a.pop(),
        (a) => {
            a[0].observations = 999;
        },
        (a) => {
            a[0].valid = false;
        },
        (a) => {
            for (const v of a) v.endToEndP99 = v.arm === "A" ? 2 : 12;
        },
    ]) {
        const copy = structuredClone(arms);
        mutate(copy);
        assert.equal(summarizeKnown002(policy, copy).status, "unknown");
    }
    const noDelay = structuredClone(arms);
    noDelay.forEach((a, i) => {
        a.endToEndP99 = 2 + (i % 7) * 0.001;
    });
    assert.equal(summarizeKnown002(policy, noDelay).status, "failed");
});
