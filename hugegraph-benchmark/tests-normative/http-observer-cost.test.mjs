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
import { readFile, writeFile, rename, symlink, lstat, mkdir } from "node:fs/promises";
import { join } from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import {
    reportObserverCost,
    validateObserverCostInput,
    observerJobSnapshot,
    verifyObserverAnchor,
    observer003AllowedCommands,
    verifyObserverCommandClosure,
    verifyObserverJobUsage,
    verifyObserverTargetReceipt,
    verifyObserverDriverCommandCid,
    checkObserverDeadline,
} from "../components/http/observer-cost-evidence.mjs";
import {
    OBSERVER003 as O,
    summarizeObserver003,
    observer003Policy,
    observer003Schedule,
    observerSha,
    verifyObserverTask,
} from "../components/http/observer003-contract.mjs";
import { valueHash, balancedOrders } from "../components/paired/core.mjs";
import { observerFixture, observerArms, get, put } from "./http-observer-cost.fixture.mjs";
const MiB = 1048576,
    run = promisify(execFile),
    opts = (f, out = "report") => ({
        inputFile: f.inputFile,
        outputDirectory: join(f.root, out),
        timeoutMs: 15000,
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
test("observer driver command CID accepts the exact original Docker stop warning only for its locked argv", async () => {
    // Original003 commands/00053.json metadata only; no raw replay or runtime qualification.
    const receipt = JSON.parse(
            await readFile(new URL("./fixtures/http-observer-driver-stop-warning.json", import.meta.url)),
        ),
        cid = "8b26a048e46897ad5635345e66c18e947f3b10c0527e780b6cc4df7bc2f0a94d",
        argv = ["/usr/bin/docker", "stop", "--time", "5", cid];
    assert.deepEqual(receipt.argv, argv);
    assert.equal(receipt.stdout, "Flag --time has been deprecated, use --timeout instead\n" + cid + "\n");
    assert.doesNotThrow(() => verifyObserverDriverCommandCid(receipt, argv, cid));
    for (const stdout of [cid, cid + "\n"])
        assert.doesNotThrow(() => verifyObserverDriverCommandCid({ ...receipt, stdout }, argv, cid));
});
test("observer driver command CID keeps create start remove strict and rejects unknown warning or CID contents", () => {
    const cid = "a".repeat(64),
        warning = "Flag --time has been deprecated, use --timeout instead\n",
        stop = ["/usr/bin/docker", "stop", "--time", "5", cid],
        receipt = { argv: stop, exitCode: 0, error: null, stdout: warning + cid + "\n", stderr: "" };
    for (const argv of [
        ["/usr/bin/docker", "create", "--name", "fixture", "sha256:" + "b".repeat(64)],
        ["/usr/bin/docker", "start", cid],
        ["/usr/bin/docker", "rm", cid],
    ]) {
        assert.doesNotThrow(() => verifyObserverDriverCommandCid({ ...receipt, argv, stdout: cid + "\n" }, argv, cid));
        assert.throws(() => verifyObserverDriverCommandCid({ ...receipt, argv }, argv, cid), {
            code: "OBSERVER_DRIVER_COMMAND_CID",
        });
    }
    for (const stdout of [
        "Another warning\n" + cid + "\n",
        warning.replace("instead", "instead.") + cid + "\n",
        warning.replace("\n", "\r\n") + cid + "\n",
        "\n" + warning + cid + "\n",
        warning + cid + "\n\n",
        warning + "b".repeat(64) + "\n",
        warning + cid.slice(0, 12) + "\n",
        warning + cid + "\n" + cid + "\n",
        warning + warning + cid + "\n",
        warning + cid + "\nextra\n",
        warning,
        cid + "\n" + cid,
        null,
    ])
        assert.throws(() => verifyObserverDriverCommandCid({ ...receipt, stdout }, stop, cid), {
            code: "OBSERVER_DRIVER_COMMAND_CID",
        });
    for (const patch of [
        { exitCode: 1 },
        { error: "command-failed" },
        { argv: [...stop.slice(0, -1), "b".repeat(64)] },
        { argv: ["/other/docker", ...stop.slice(1)] },
    ])
        assert.throws(() => verifyObserverDriverCommandCid({ ...receipt, ...patch }, stop, cid), {
            code: "OBSERVER_DRIVER_COMMAND_CID",
        });
    // Even a separately matched command is outside the one allowed warning form.
    for (const argv of [
        ["/usr/bin/docker", "stop", "--timeout", "5", cid],
        ["/usr/bin/docker", "stop", "--time", "30", cid],
        [...stop, cid],
    ])
        assert.throws(() => verifyObserverDriverCommandCid({ ...receipt, argv }, argv, cid), {
            code: "OBSERVER_DRIVER_COMMAND_CID",
        });
});
test("observer003 synthetic paired projection discloses an effect as measured, without full-family qualification", () => {
    const policies = Object.fromEntries(["J", "C"].map((g) => [g, observer003Policy(g, { fixture: true })])),
        arms = observerArms(policies),
        result = summarizeObserver003(policies, arms);
    assert.equal(result.status, "passed");
    assert.equal(result.evidenceVerified, true);
    assert.equal(result.scopedCostMeasured, true);
    assert.equal(result.groups.length, 2);
    for (const g of result.groups) {
        assert.equal(g.analysis.cells.length, 6);
        assert(g.analysis.cells.every((c) => c.budgetRelation === "above-both-budgets" && c.observedPairs === 4));
    }
    // These are explicitly synthetic normalized values; no public/raw gate was minted.
    for (const a of arms) for (const v of Object.values(a.values)) v.observations = 999;
    assert.equal(summarizeObserver003(policies, arms).status, "unknown");
});
test("degenerate effects and an invalid/missing arm keep the original16arm/4pair denominator unknown", () => {
    const policies = Object.fromEntries(["J", "C"].map((g) => [g, observer003Policy(g, { fixture: true })])),
        arms = observerArms(policies);
    for (const a of arms)
        for (const [id, v] of Object.entries(a.values))
            v.value = policies[a.group].metrics.findIndex((m) => m.id === id) + 10;
    const degenerate = summarizeObserver003(policies, arms);
    assert.equal(degenerate.status, "unknown");
    assert(
        degenerate.groups.every((g) =>
            g.analysis.cells.every((c) => c.warnings.some((w) => w.startsWith("degenerate"))),
        ),
    );
    arms[0].valid = false;
    const missing = summarizeObserver003(policies, arms);
    assert.equal(missing.evidenceVerified, false);
    assert.equal(missing.groups[0].analysis.cells[0].observedPairs, 3);
});
test("public offline IO ignores saved passed flags and preserves all unexecuted arms/other8cases", async (t) => {
    const f = await observerFixture(t);
    await put(join(f.execution, "result.json"), {
        status: "passed",
        rows: Array(16).fill({ status: "verified", cleanupComplete: true }),
    });
    const output = await reportObserverCost(opts(f)),
        r = await get(output.reportJson);
    assert.equal(output.status, "unknown");
    assert.equal(output.evidenceVerified, false);
    assert.equal(output.scopedCostMeasured, false);
    assert.equal(r.arms.length, 16);
    assert.equal(r.unmeasuredCases.length, 8);
    assert.equal(r.plannedCaseRuns, 48);
    assert.deepEqual(r.verification.issues, ["OBSERVER_NOT_EXECUTED"]);
    assert.equal(r.performanceAccepted, false);
    assert.equal(r.inferenceAccepted, false);
    for (const m of (await get(output.reportLock)).files)
        assert.equal(m.sha256, observerSha(await readFile(join(output.directory, m.path))));
    const relocated = join(f.root, "moved");
    await mkdir(relocated);
    await rename(f.plan, join(relocated, "plan"));
    await rename(f.execution, join(relocated, "execution"));
    await writeFile(join(relocated, "input.json"), await readFile(f.inputFile));
    const moved = await reportObserverCost({ ...opts(f, "moved-report"), inputFile: join(relocated, "input.json") });
    assert.deepEqual(await readFile(moved.reportJson), await readFile(output.reportJson));
});
test("prepared claims with absent actual HTTP remain unknown rather than trusting operatorrows", async (t) => {
    const f = await observerFixture(t);
    f.sequence.mode = "prepare";
    f.spec.runnable = true;
    await put(join(f.plan, "sequence.json"), f.sequence);
    await f.reseal();
    const dispatch = {
        executed: true,
        executionSpecSha256: f.input.reference.executionSpecSha256,
        executionRoot: "/original/observer003/execution",
        hostBootId: "00000000-0000-0000-0000-000000000001",
        startedMonotonicNs: "1",
    };
    await put(join(f.plan, "observer-cost-dispatch.json"), dispatch);
    await put(join(f.execution, "dispatch.json"), dispatch);
    await put(join(f.execution, "commands/00001.json"), {
        argv: ["offline-not-executed"],
        stdout: "",
        stderr: "",
        exitCode: 0,
        error: null,
        startedMonotonicNs: "2",
        finishedMonotonicNs: "3",
    });
    const r = await get((await reportObserverCost(opts(f))).reportJson);
    assert.equal(r.status, "unknown");
    assert(r.arms.every((a) => !a.valid && a.issues.length));
    assert.equal(r.groups[0].analysis.cells.length, 6);
});
test("external references and resealed narrower policies are rejected without changing original inputs", async (t) => {
    const f = await observerFixture(t),
        old = await readFile(join(f.plan, "groups/J/policy.json"));
    const p = await get(join(f.plan, "groups/J/policy.json"));
    p.metrics.pop();
    await put(join(f.plan, "groups/J/policy.json"), p);
    await f.reseal();
    await assert.rejects(reportObserverCost(opts(f)), { code: "OBSERVER_POLICY" });
    assert.equal(await exists(join(f.root, "report/report.lock.json")), false);
    assert(await exists(join(f.root, "report/partial.json")));
    await writeFile(join(f.plan, "groups/J/policy.json"), old);
    await f.reseal();
    f.input.reference.planLockSha256 = "f".repeat(64);
    await put(f.inputFile, f.input);
    await assert.rejects(reportObserverCost(opts(f, "bad-ref")), { code: "OBSERVER_PLAN_LOCK" });
});
test("input controls/output aliases/create-only and symlink/FIFO reads are bounded", async (t) => {
    const f = await observerFixture(t);
    for (const p of ["bad\npath", "bad\rpath", "bad\0path"])
        assert.throws(() => validateObserverCostInput({ ...f.input, planDirectory: p }), {
            code: "OBSERVER_INPUT_IDENTITY",
        });
    await assert.rejects(reportObserverCost({ ...opts(f), outputDirectory: join(f.plan, "report") }), {
        code: "OBSERVER_OUTPUT_OVERLAP",
    });
    await reportObserverCost(opts(f));
    await assert.rejects(reportObserverCost(opts(f)), (e) => e.code === "EEXIST");
    await writeFile(join(f.root, "foreign"), "untrusted");
    await symlink(join(f.root, "foreign"), join(f.execution, "symlink"));
    await assert.rejects(reportObserverCost(opts(f, "symlink-report")), { code: "OBSERVER_FILE_BOUND" });
    const f2 = await observerFixture(t);
    await run("mkfifo", [join(f2.execution, "fifo")]);
    await assert.rejects(reportObserverCost(opts(f2, "fifo-report")), { code: "OBSERVER_FILE_BOUND" });
});
test("pre-aborted calls preserve partial16arms without a successlock and malformed budgets reject", async (t) => {
    const f = await observerFixture(t),
        abort = new AbortController();
    abort.abort();
    await assert.rejects(reportObserverCost({ ...opts(f), signal: abort.signal }), { code: "OBSERVER_CANCELLED" });
    const partial = await get(join(f.root, "report/partial.json"));
    assert.equal(partial.arms.length, 16);
    assert.equal(await exists(join(f.root, "report/report.lock.json")), false);
    await assert.rejects(reportObserverCost({ ...opts(f, "badbudget"), maxBytes: 1 }), {
        code: "OBSERVER_REPORT_BUDGET",
    });
    await assert.rejects(reportObserverCost({ ...opts(f, "lowtime"), timeoutMs: 1000 }), { code: "OBSERVER_DEADLINE" });
    assert.equal((await get(join(f.root, "lowtime/partial.json"))).status, "unknown");
    assert.equal(await exists(join(f.root, "lowtime/report.lock.json")), false);
});
test("unit absence is scoped and exact7field; active/missing/duplicate/error records reject", () => {
    const unit = "owned.service",
        raw = `Id=${unit}\nLoadState=not-found\nActiveState=inactive\nSubState=dead\nMainPID=0\nControlGroup=\nResult=success\n`,
        good = observerJobSnapshot(unit, raw, 0, false);
    assert.equal(good.stateEvidence, "unit-absent");
    assert.equal(good.priorTaskSuccessProven, false);
    for (const changed of [
        raw.replace("MainPID=0", "MainPID=7"),
        raw.replace("LoadState=not-found", "LoadState=error"),
        raw + "Id=again\n",
        raw.replace("Result=success\n", ""),
    ])
        assert.throws(() => observerJobSnapshot(unit, changed, 0, false));
    assert.throws(() => observerJobSnapshot(unit, raw, 1, false));
});
test("raw anchor checks do not invent role in the actual Kernel identity and reject PID/start/owner drift", () => {
    const id = { pid: 42, startTimeTicks: "123", bootId: "b" },
        rec = {
            role: "driver",
            hostPid: 42,
            statRaw: "42 (name with ) spaces) S " + Array(18).fill("0").join(" ") + " 123",
            statusRaw: "Uid:\t1000\t1000\t1000\t1000\nGid:\t1000\t1000\t1000\t1000\n",
            cgroupRaw: "0::/docker/test\n",
        },
        info = { State: { Pid: 42 } };
    assert.deepEqual(verifyObserverAnchor(rec, id, info, "1000:1000", "driver"), {
        hostPid: 42,
        startTimeTicks: "123",
        cgroupMember: "/docker/test",
    });
    assert(!Object.hasOwn(id, "role"));
    assert.throws(() => verifyObserverAnchor({ ...rec, hostPid: 43 }, id, info, "1000:1000", "driver"));
    assert.throws(() => verifyObserverAnchor(rec, { ...id, startTimeTicks: "9" }, info, "1000:1000", "driver"));
    assert.throws(() => verifyObserverAnchor(rec, id, info, "2:1000", "driver"));
});
test("frozen task terminal requires original request/digest, completed uninterrupted normal exit", () => {
    const content = {
            schemaVersion: 1,
            kind: "frozen-component-task",
            plan: { command: ["fixture"], timeoutMs: 10 },
            files: [],
        },
        plan = { ...content, digest: valueHash(content) },
        request = "synthetic-observer-task",
        terminal = {
            taskId: "task-" + observerSha(JSON.stringify(request)).slice(0, 32),
            planDigest: plan.digest,
            state: "completed",
            terminal: true,
            interrupted: false,
            processExitCode: 0,
            error: null,
        };
    verifyObserverTask(plan, terminal, request);
    for (const patch of [
        { state: "failed" },
        { interrupted: true },
        { processExitCode: 143 },
        { planDigest: "f".repeat(64) },
        { reused: true },
    ])
        assert.throws(() => verifyObserverTask(plan, { ...terminal, ...patch }, request));
});
test("actual command closure binds two final analyzers to executionroot, rejects offcollector and aliencode", async (t) => {
    const f = await observerFixture(t),
        ctx = {
            spec: f.spec,
            sequence: {
                ...f.sequence,
                declaredInactiveUnits: [
                    "hgbench-sf100-convert-v1.service",
                    "hgbench-sf100-reference-convert-v1.service",
                    "hgbench-sf100-update-audit-v1.service",
                    "hgbench-http-aa-cohort-005.service",
                    "hgbench-http-known-delay-002.service",
                    "hgbench-http-driver-headroom-002.service",
                    "hgbench-http-observer-cost-002.service",
                ],
                protectedObservedUnit: "hgbench-sf100-docker-environment-v6.service",
            },
        },
        dispatch = { executionRoot: "/original/execution" },
        arms = f.sequence.physicalArms.map((a, i) => ({
            ...a,
            valid: true,
            targetCid: i.toString(16).padStart(64, "a"),
            driverCid: i.toString(16).padStart(64, "b"),
            lastCommandIndex: 0,
            declaredCommands: observer003AllowedCommands(a, ctx, {
                target: "a".repeat(64),
                driver: "b".repeat(64),
                java: 12,
                jcmd: "/opt/jdk/bin/jcmd",
            }),
        }));
    const commands = [],
        units = [...ctx.sequence.declaredInactiveUnits, ctx.sequence.protectedObservedUnit],
        props = ["Id", "LoadState", "ActiveState", "SubState", "MainPID", "ControlGroup", "Result"];
    const add = (argv) =>
        commands.push({ argv, index: commands.length, exitCode: 0, error: null, stdout: "", stderr: "" });
    for (const u of units)
        for (let i = 0; i < 32; i++)
            add(["/usr/bin/systemctl", "--user", "show", u, ...props.flatMap((p) => ["-p", p])]);
    for (const g of ["J", "C"])
        add([
            f.spec.node,
            f.spec.sourceRoot + "/components/paired/analyzer.mjs",
            "--input",
            dispatch.executionRoot + "/group-" + g + "/verified-observations.json",
            "--output",
            dispatch.executionRoot + "/group-" + g + "/paired-analysis",
        ]);
    assert.doesNotThrow(() => verifyObserverCommandClosure(commands, ctx, dispatch, arms));
    ctx.sequence.declaredInactiveUnits.push(ctx.sequence.declaredInactiveUnits[0]);
    assert.doesNotThrow(() => verifyObserverCommandClosure(commands, ctx, dispatch, arms));
    ctx.sequence.declaredInactiveUnits.pop();
    const firstUnit = ctx.sequence.declaredInactiveUnits.shift();
    assert.throws(() => verifyObserverCommandClosure(commands, ctx, dispatch, arms), { code: "OBSERVER_UNIT_SCOPE" });
    ctx.sequence.declaredInactiveUnits.unshift(firstUnit);
    add(["/bin/sh", "-c", "not executed"]);
    assert.throws(() => verifyObserverCommandClosure(commands, ctx, dispatch, arms), {
        code: "OBSERVER_UNDECLARED_COMMAND",
    });
    commands.pop();
    const off = arms.find((a) => !a.collector);
    add([
        f.spec.node,
        f.spec.sourceRoot + "/dist/cli.js",
        "--root",
        off.directory + "/collector-tasks",
        "task",
        "run",
        "--plan",
        off.directory + "/collector-task.json",
        "--detach",
        "--request-id",
        off.runId + "-collector",
    ]);
    assert.throws(() => verifyObserverCommandClosure(commands, ctx, dispatch, arms), {
        code: "OBSERVER_UNDECLARED_COMMAND",
    });
});

test("adjacent pre/post records cannot be shared and targetfinal permits dynamic differences only", () => {
    assert.doesNotThrow(() => verifyObserverJobUsage([{ jobCommandIndices: [1, 2] }, { jobCommandIndices: [3, 4] }]));
    assert.throws(() => verifyObserverJobUsage([{ jobCommandIndices: [1, 2] }, { jobCommandIndices: [2, 3] }]), {
        code: "OBSERVER_UNIT_RECORD_REUSED",
    });
    const sealed = {
        Id: "a".repeat(64),
        Name: "/owned",
        Image: "sha256:" + "b".repeat(64),
        Config: { User: "1000:1000", Labels: { "org.apache.hugegraph.hgbench.environment.attempt": "fixed" } },
        State: { Status: "exited", Running: false, OOMKilled: false, ExitCode: 0, FinishedAt: "earlier" },
    };
    verifyObserverTargetReceipt({ ...sealed, State: { ...sealed.State, FinishedAt: "later" } }, sealed);
    for (const patch of [{ Running: true }, { OOMKilled: true }, { Status: "running" }, { ExitCode: 137 }])
        assert.throws(() => verifyObserverTargetReceipt({ ...sealed, State: { ...sealed.State, ...patch } }, sealed), {
            code: "OBSERVER_TARGET_FINAL_STATE",
        });
    assert.throws(() => verifyObserverTargetReceipt({ ...sealed, Id: "c".repeat(64) }, sealed), {
        code: "OBSERVER_TARGET_FINAL_IDENTITY",
    });
});

test("cross-budget uncertainty is disclosed as measured; late seals cannot retain success", () => {
    const policies = Object.fromEntries(["J", "C"].map((g) => [g, observer003Policy(g, { fixture: true })])),
        arms = observerArms(policies);
    for (const a of arms)
        for (const [id, v] of Object.entries(a.values)) {
            const base = 20 + policies[a.group].metrics.findIndex((m) => m.id === id);
            v.value = base + (a.arm === "B" ? [-4, 0.2, 2, 4][O.pairs.indexOf(a.blockId)] : 0);
        }
    const result = summarizeObserver003(policies, arms);
    assert.equal(result.status, "passed");
    assert(result.groups.every((g) => g.analysis.cells.every((c) => c.budgetRelation === "crosses-budget")));
    const now = performance.now();
    assert.doesNotThrow(() => checkObserverDeadline({ aborted: false }, now, 10000));
    assert.throws(() => checkObserverDeadline({ aborted: true }, now, 10000), { code: "OBSERVER_CANCELLED" });
    assert.throws(() => checkObserverDeadline({ aborted: false }, now - 2000, 1000), { code: "OBSERVER_DEADLINE" });
});

test("relocked same-case profile cannot substitute a different suite bundle", async (t) => {
    const f = await observerFixture(t);
    const profile = await get(join(f.plan, "arms/J-p01-A/profile.json"));
    profile.benchmark.bundleSha256 = "f".repeat(64);
    await put(join(f.plan, "arms/J-p01-A/profile.json"), profile);
    await f.reseal();
    await assert.rejects(reportObserverCost(opts(f)), { code: "OBSERVER_PROFILE_BUNDLE" });
    assert.equal(await exists(join(f.root, "report/report.lock.json")), false);
});
