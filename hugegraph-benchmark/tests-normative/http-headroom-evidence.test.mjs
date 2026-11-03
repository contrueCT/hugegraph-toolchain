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
import { readFile, rename, symlink, lstat } from "node:fs/promises";
import { join } from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import {
    HEADROOM002,
    headroomGrid,
    headroomDeclaredUnits,
    headroomSha,
    headroomTaskId,
    headroomTolerance,
    assessHeadroomRun,
    assessHeadroomCells,
    verifyHeadroomProfile,
    headroomProfile,
} from "../components/http/headroom002-contract.mjs";
import {
    reportHeadroom,
    validateHeadroomInput,
    verifyHeadroomPreregistration,
    verifyHeadroomMounts,
    verifyHeadroomLifecycle,
    verifyHeadroomCommandClosure,
    verifyHeadroomPhysicalIdentities,
    verifyHeadroomUnit,
    rebuildHeadroomCpu,
    verifyHeadroomCpuEndpoints,
    checkHeadroomDeadline,
} from "../components/http/headroom-evidence.mjs";
import { headroomFixture, registrationFixture, headroomLifecycle, put, get } from "./http-headroom.fixture.mjs";
const MiB = 1024 ** 2,
    run = promisify(execFile),
    options = (f, out = "report") => ({
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
const jsonSha = (v) => headroomSha(JSON.stringify(v, null, 2) + "\n");
function phaseFixture() {
    return {
        phases: ["warmup", "measurement"].map((name, i) => ({
            name,
            durationMs: [30000, 120000][i],
            phaseEndedEarly: false,
            totals: { records: [300, 1200][i], correct: [300, 1200][i], errors: 0, dropped: 0, late: 0 },
            driverWholePhase: { averageCpuCores: 0.2, eventLoopUtilization: 0.3 },
            windows: Array.from({ length: [3, 12][i] }, (_, j) => ({
                index: j,
                startMs: j * 10000,
                endMs: (j + 1) * 10000,
                metrics: { correctResponseMs: { count: 100, p50: 10, p95: 20, p99: null, p99Status: "unknown" } },
            })),
        })),
    };
}
function runFixture() {
    return {
        caseId: "scalar-add",
        correct: 1200,
        failed: 0,
        dropped: 0,
        late: 0,
        correctEndToEndMs: { p99: 30 },
        correctCompletedInWindowPerSecond: 10,
        issues: [],
    };
}
function completeArms() {
    return headroomGrid().map((a) => ({
        ...a,
        valid: true,
        cells: HEADROOM002.cases.map((caseId) => ({
            ...assessHeadroomRun({ ...runFixture(), caseId }, phaseFixture()),
            caseId,
        })),
    }));
}

test("criterion and fixed protocol fixtures preserve exact preregistered bytes with no private draft dependency", async () => {
    const raw = await readFile(new URL("../presets/http/headroom-engineering-3case-v1.json", import.meta.url));
    assert.equal(headroomSha(raw), HEADROOM002.criteriaSha256);
    const fixed = await get(new URL("./fixtures/headroom002-fixed-contracts.json", import.meta.url));
    for (const [file, key] of [
        ["settings.json", "settingsSha256"],
        ["budgets.json", "budgetsSha256"],
        ["assessment-contract.json", "assessmentContractSha256"],
    ])
        assert.equal(headroomSha(fixed.files[file]), HEADROOM002[key]);
    assert.equal(headroomTaskId("headroom-002"), "task-" + headroomSha(JSON.stringify("headroom-002")).slice(0, 32));
    assert.throws(() => headroomTaskId("é"));
});
test("all36 synthetic cells can pass only the limited engineering scope; comparisons are18+18 and no performance acceptance", () => {
    const result = assessHeadroomCells(completeArms());
    assert.equal(result.status, "passed");
    assert.equal(result.limitedScopeEngineeringAccepted, true);
    assert.equal(result.cells.length, 36);
    assert.equal(result.p99Comparisons.length, 36);
    assert.equal(result.p99Comparisons.filter((p) => p.kind === "cpu-sensitivity").length, 18);
    assert.equal(result.measurementSteadyState, "unassessed");
    assert.equal(result.statisticalEquivalenceAccepted, false);
    assert.equal(result.inferenceAccepted, false);
    assert.equal(result.performanceAccepted, false);
    assert.equal(headroomTolerance(10, 11).status, "passed");
    assert.equal(headroomTolerance(10, 11.001).status, "failed");
    assert.equal(headroomTolerance(100, 111).status, "failed");
});
test("missing arms, lowP99 samples, empty transitions and global missing provenance retain unknown full denominator", () => {
    for (const alter of [
        (a) => a.pop(),
        (a) => (a[0].cells[0].observations = 999),
        (a) => (a[0].cells[0].transitions = []),
    ]) {
        const arms = completeArms();
        alter(arms);
        const result = assessHeadroomCells(arms);
        assert.equal(result.status, "unknown");
        assert.equal(result.cells.length, 36);
        assert.equal(result.p99Comparisons.length, 36);
    }
    assert.equal(assessHeadroomCells(completeArms(), ["missing-launch"]).status, "unknown");
    const arms = completeArms();
    arms[0].cells[0].p99 = 50;
    assert.equal(assessHeadroomCells(arms).status, "failed");
});
test("all original analyzer issues affect acceptance; failures preserved while unclassified issues are unknown", () => {
    for (const issue of [
        "measurement:cancelled",
        "warmup:request-budget-exhausted",
        "warmup:request-errors-or-wrong-answers",
    ])
        assert.equal(assessHeadroomRun({ ...runFixture(), issues: [issue] }, phaseFixture()).qualityStatus, "failed");
    assert.equal(
        assessHeadroomRun({ ...runFixture(), issues: ["measurement:response-encoding-varies"] }, phaseFixture())
            .qualityStatus,
        "unknown",
    );
    assert.equal(
        assessHeadroomRun({ ...runFixture(), issues: ["measurement:driver-pressure-observed"] }, phaseFixture())
            .pressureStatus,
        "failed",
    );
    const phases = phaseFixture();
    phases.phases[0].totals.dropped = 1;
    assert.equal(assessHeadroomRun(runFixture(), phases).qualityStatus, "failed");
    assert.equal(assessHeadroomRun(runFixture(), { phases: [] }).pressureStatus, "unknown");
    assert.equal(assessHeadroomRun(runFixture(), { phases: [] }).qualityStatus, "unknown");
});
test("warmup uses exact two late and two early complete arrival windows, metric sample counts and asymmetric criterion", () => {
    const phases = phaseFixture();
    phases.phases[0].windows[1].metrics.correctResponseMs.p50 = 10;
    phases.phases[0].windows[2].metrics.correctResponseMs.p50 = 30;
    phases.phases[1].windows[0].metrics.correctResponseMs.p50 = 20;
    phases.phases[1].windows[1].metrics.correctResponseMs.p50 = 24;
    const result = assessHeadroomRun(runFixture(), phases);
    assert.equal(result.transitions[0].reference, 20);
    assert.equal(result.transitions[0].candidate, 22);
    assert.equal(result.transitions[0].status, "passed");
    phases.phases[1].windows[0].metrics.correctResponseMs.count = 49;
    assert.equal(assessHeadroomRun(runFixture(), phases).transitions[0].status, "unknown");
    phases.phases[1].windows[0].metrics.correctResponseMs.count = 100;
    phases.phases[0].windows[1].startMs = 9999;
    assert.equal(assessHeadroomRun(runFixture(), phases).qualityStatus, "unknown");
    const duplicate = phaseFixture();
    duplicate.phases.push(duplicate.phases[0]);
    assert.equal(assessHeadroomRun(runFixture(), duplicate).qualityStatus, "unknown");
});
test("Node CPU is independently recomputed while recorded ELU never claims raw-counter derivation", () => {
    const phases = ["warmup", "measurement"].map((name) => ({
        name,
        elapsedMs: 1000,
        cpu: { user: 100000, system: 100000 },
        averageCpuCores: 0.2,
        eventLoopUtilization: 0.3,
    }));
    assert.equal(rebuildHeadroomCpu(phases)[0].eluIndependentlyDerived, false);
    phases[0].averageCpuCores = 0;
    assert.throws(() => rebuildHeadroomCpu(phases), { code: "HEADROOM_PHASE_CPU_ELU" });
    assert.throws(() => rebuildHeadroomCpu([]), { code: "HEADROOM_PHASE_INVENTORY" });
});
test("controller systemd+proc evidence proves launch binding before process creation without a self-reported dispatch timestamp", () => {
    const ref = { planLockSha256: "a".repeat(64), executionSpecSha256: "b".repeat(64) },
        context = {
            spec: { planRoot: "/original/plan", planLockSha256: ref.planLockSha256 },
            runtime: { hostPython: "/usr/bin/python3.12" },
        },
        dispatch = {
            pid: 123,
            startTicks: "456",
            executionRoot: "/original/execution",
            executionSpecSha256: ref.executionSpecSha256,
            hostBootId: "00000000-0000-0000-0000-000000000001",
        },
        f = registrationFixture(context, ref, dispatch);
    assert.equal(
        verifyHeadroomPreregistration(f.binding, f.launch, f.controller, dispatch, context, f.bindingSha).verified,
        true,
    );
    for (const mutate of [
        (x) => (x.launch.finishedMonotonicNs = "3000"),
        (x) => (x.controller.procStatRaw = x.controller.procStatRaw.replace("456", "457")),
        (x) => (x.controller.systemd.stdout += "MainPID=123\n"),
        (x) =>
            (x.controller.systemd.stdout = x.controller.systemd.stdout.replace("ExecMainPID=123", "ExecMainPID=124")),
        (x) => (x.launch.bindingSha256 = "0".repeat(64)),
        (x) => (x.binding.preDispatchLaunchRecordReference.path = "../launch.json"),
    ]) {
        const changed = structuredClone(f);
        mutate(changed);
        assert.throws(() =>
            verifyHeadroomPreregistration(
                changed.binding,
                changed.launch,
                changed.controller,
                dispatch,
                context,
                f.bindingSha,
            ),
        );
    }
});
test("strict inactivity accepts loaded or absent units, preserves protected observation and rejects live/aliased/incomplete properties", () => {
    const argv = [
            "/usr/bin/systemctl",
            "--user",
            "show",
            "old.service",
            ...["Id", "LoadState", "ActiveState", "SubState", "MainPID", "ControlGroup", "Result"].flatMap((p) => [
                "-p",
                p,
            ]),
        ],
        c = {
            argv,
            exitCode: 0,
            error: null,
            stdout: "Id=old.service\nLoadState=not-found\nActiveState=inactive\nSubState=dead\nMainPID=0\nControlGroup=\nResult=\n",
        };
    assert.equal(verifyHeadroomUnit(c, "old.service").historicalSuccessProven, false);
    assert.doesNotThrow(() =>
        verifyHeadroomUnit({ ...c, stdout: c.stdout.replace("not-found", "loaded") }, "old.service"),
    );
    for (const stdout of [
        c.stdout.replace("inactive", "active"),
        c.stdout.replace("MainPID=0", "MainPID=5"),
        c.stdout.replace("ControlGroup=", "ControlGroup=/live"),
        c.stdout + "Result=success\n",
        c.stdout.replace("Id=old.service", "Id=alias.service"),
    ])
        assert.throws(() => verifyHeadroomUnit({ ...c, stdout }, "old.service"));
    assert.equal(
        verifyHeadroomUnit({ ...c, stdout: c.stdout.replace("inactive", "active") }, "old.service", true)
            .protectedObservedOnly,
        true,
    );
});
test("workflow cgroup CPU rederives raw endpoints with actual quota and exact live identity instead of accepting saved delta", () => {
    const cid = "a".repeat(64),
        identity = { pid: 12, startTimeTicks: "456", bootId: "boot" },
        base = {
            kind: "http-driver-workflow-cpu-endpoint",
            containerId: cid,
            identity,
            procStatRaw: "12 (driver) S " + Array(18).fill("0").join(" ") + " 456",
            procCgroupRaw: "0::/docker/" + cid + "\n",
            cpuMaxRaw: "200000 100000\n",
        };
    const a = {
            ...base,
            label: "before",
            beforeMonotonicNs: "1000000000",
            afterMonotonicNs: "1100000000",
            cpuStatRaw: "usage_usec 1000\n",
        },
        b = {
            ...base,
            label: "after",
            beforeMonotonicNs: "1900000000",
            afterMonotonicNs: "2000000000",
            cpuStatRaw: "usage_usec 201000\n",
        };
    assert.equal(
        verifyHeadroomCpuEndpoints(a, b, { driverCpus: 2 }, cid, "boot", ["1200000000", "1800000000"]).averageCpuCores,
        0.2,
    );
    assert.throws(
        () => verifyHeadroomCpuEndpoints(a, b, { driverCpus: 1 }, cid, "boot", ["1200000000", "1800000000"]),
        { code: "HEADROOM_CPU_ENDPOINT_QUOTA" },
    );
    assert.throws(
        () =>
            verifyHeadroomCpuEndpoints(a, { ...b, cpuStatRaw: "usage_usec 0\n" }, { driverCpus: 2 }, cid, "boot", [
                "1200000000",
                "1800000000",
            ]),
        { code: "HEADROOM_CPU_ENDPOINT_WINDOW" },
    );
});
test("public offline IO keeps all12/36 unexecuted evidence unknown despite arbitrary private passed booleans", async (t) => {
    const f = await headroomFixture(t);
    await put(join(f.execution, "result.json"), {
        limitedScopeEngineeringAccepted: true,
        rows: Array(12).fill({ status: "passed", warmupDerived: true }),
    });
    const out = await reportHeadroom(options(f)),
        report = await get(out.reportJson);
    assert.equal(out.status, "unknown");
    assert.equal(report.limitedScopeEngineeringAccepted, false);
    assert.equal(report.cells.length, 36);
    assert.equal(report.arms.length, 12);
    assert.equal(report.verifiedPhysicalArms, 0);
    assert.deepEqual(report.verification.issues, ["HEADROOM_NOT_EXECUTED"]);
    assert.equal(report.measurementSteadyState, "unassessed");
    for (const file of (await get(out.reportLock)).files)
        assert.equal(headroomSha(await readFile(join(out.directory, file.path))), file.sha256);
});
test("prepared preregistration remains unknown without original raw; missing/late controller proof cannot manufacture passed", async (t) => {
    const f = await headroomFixture(t);
    await f.register();
    const out = await reportHeadroom(options(f)),
        report = await get(out.reportJson);
    assert.equal(report.registration.verified, true);
    assert.equal(report.status, "unknown");
    assert.equal(report.cells.length, 36);
    assert.equal(report.verifiedPhysicalArms, 0);
    assert.ok(report.arms.every((a) => !a.valid && a.issues.some((i) => i.includes("ENOENT"))));
    const controller = await get(join(f.root, "controller.json"));
    controller.systemd.stdout = controller.systemd.stdout.replace(
        "ExecMainStartTimestampMonotonic=3",
        "ExecMainStartTimestampMonotonic=1",
    );
    await put(join(f.root, "controller.json"), controller);
    f.input.reference.controllerStartEvidenceSha256 = jsonSha(controller);
    await put(f.inputFile, f.input);
    const bad = await get((await reportHeadroom(options(f, "late"))).reportJson);
    assert.equal(bad.status, "unknown");
    assert.deepEqual(bad.verification.issues, ["HEADROOM_REGISTRATION_TOO_LATE"]);
});
test("copied roots preserve report conclusions and stable original source bytes", async (t) => {
    const f = await headroomFixture(t),
        one = await get((await reportHeadroom(options(f, "before"))).reportJson),
        original = await readFile(join(f.plan, "sequence.json"));
    await rename(f.plan, join(f.root, "moved"));
    f.input.planDirectory = "moved";
    await put(f.inputFile, f.input);
    const two = await get((await reportHeadroom(options(f, "after"))).reportJson);
    delete one.references.inputSha256;
    delete two.references.inputSha256;
    assert.deepEqual(one, two);
    assert.deepEqual(original, await readFile(join(f.root, "moved/sequence.json")));
});
test("no overlapping output or source aliases, strict inputs and insufficient overall deadline write no output", async (t) => {
    const f = await headroomFixture(t);
    assert.throws(() => validateHeadroomInput({ ...f.input, unexpected: true }));
    assert.throws(() => validateHeadroomInput({ ...f.input, controllerStartEvidenceFile: "" }));
    await assert.rejects(reportHeadroom({ ...options(f), outputDirectory: join(f.plan, "bad") }), {
        code: "HEADROOM_OUTPUT_OVERLAP",
    });
    await symlink(f.plan, join(f.root, "alias"));
    await assert.rejects(reportHeadroom({ ...options(f), outputDirectory: join(f.root, "alias/bad") }), {
        code: "HEADROOM_OUTPUT_OVERLAP",
    });
    assert.equal(await exists(join(f.plan, "bad")), false);
    await assert.rejects(reportHeadroom({ ...options(f, "too-short"), timeoutMs: 1000 }), {
        code: "HEADROOM_DEADLINE",
    });
    assert.equal(await exists(join(f.root, "too-short")), false);
    checkHeadroomDeadline({ aborted: false }, performance.now(), 1000);
    assert.throws(() => checkHeadroomDeadline({ aborted: true }, performance.now(), 1000), {
        code: "HEADROOM_CANCELLED",
    });
});
test("cancelled worker returns bounded partial with complete denominator; component public argv is report/input/out", async (t) => {
    const f = await headroomFixture(t),
        signal = new AbortController();
    signal.abort();
    await assert.rejects(reportHeadroom({ ...options(f, "cancelled"), signal: signal.signal }), {
        code: "HEADROOM_CANCELLED",
    });
    const partial = await get(join(f.root, "cancelled/partial.json"));
    assert.equal(partial.cells.length, 36);
    assert.equal(partial.limitedScopeEngineeringAccepted, false);
    assert.equal(await exists(join(f.root, "cancelled/report.lock.json")), false);
    const reply = await run(
        process.execPath,
        [
            "components/http/headroom-evidence.mjs",
            "report",
            "--input",
            f.inputFile,
            "--out",
            join(f.root, "cli"),
            "--timeout-ms",
            "15000",
            "--max-bytes",
            String(MiB),
        ],
        { timeout: 18000 },
    );
    assert.equal(JSON.parse(reply.stdout).data.status, "unknown");
});
test("profile observations, scripts and IF4 cannot be changed by resealing declarations", () => {
    const arm = headroomGrid()[0],
        profile = headroomProfile(arm, []);
    assert.doesNotThrow(() => verifyHeadroomProfile(profile, arm, []));
    for (const edit of [
        (p) => (p.loads[0].maxInFlight = 8),
        (p) => (p.durationMs = 100),
        (p) => (p.cases[0].body.gremlin = "2"),
    ]) {
        const copy = structuredClone(profile);
        edit(copy);
        assert.throws(() => verifyHeadroomProfile(copy, arm, []), { code: "HEADROOM_PROFILE" });
    }
});

test("actual Docker mount-order receipts preserve all attributes across six original headroom arms", async () => {
    const fixture = JSON.parse(await readFile(new URL("./fixtures/http-headroom-mount-order.json", import.meta.url)));
    assert.equal(fixture.samples.length, 6);
    for (const sample of fixture.samples) {
        assert.notDeepEqual(sample.receiptMounts, sample.sealedMounts);
        const before = structuredClone(sample);
        verifyHeadroomMounts(sample.receiptMounts, sample.sealedMounts);
        assert.deepEqual(sample, before);
    }
    const f = headroomLifecycle(2),
        targetCommand = f.commands.find((c) => c.stdout === JSON.stringify([f.targetFinal]));
    assert.ok(targetCommand);
    f.targetFinal.Mounts = structuredClone(fixture.samples[0].receiptMounts);
    f.proof.containerFinal.Mounts = structuredClone(fixture.samples[0].sealedMounts);
    targetCommand.stdout = JSON.stringify([f.targetFinal]);
    assert.equal(verifyHeadroomLifecycle(f).driverCid, f.driverFinal.Id);
});

test("mount identity rejects changed attributes, missing objects and duplicate destinations", async () => {
    const fixture = JSON.parse(await readFile(new URL("./fixtures/http-headroom-mount-order.json", import.meta.url))),
        sample = fixture.samples[0];
    for (const key of ["Source", "Destination", "RW", "Type", "Mode", "Propagation", "AdditionalAttribute"]) {
        const changed = structuredClone(sample.receiptMounts);
        changed[0][key] = typeof changed[0][key] === "boolean" ? !changed[0][key] : "changed";
        assert.throws(() => verifyHeadroomMounts(changed, sample.sealedMounts), {
            code: "HEADROOM_TARGET_FINAL_ENVIRONMENT",
        });
    }
    for (const changed of [undefined, null, {}, [null], [{}]])
        assert.throws(() => verifyHeadroomMounts(changed, sample.sealedMounts), {
            code: "HEADROOM_TARGET_FINAL_MOUNT_IDENTITY",
        });
    assert.throws(() => verifyHeadroomMounts(sample.receiptMounts.slice(1), sample.sealedMounts), {
        code: "HEADROOM_TARGET_FINAL_ENVIRONMENT",
    });
    for (const changedSource of [false, true]) {
        const duplicate = structuredClone(sample.receiptMounts),
            item = structuredClone(duplicate[0]);
        if (changedSource) item.Source += "-other";
        duplicate.push(item);
        assert.throws(() => verifyHeadroomMounts(duplicate, duplicate), {
            code: "HEADROOM_TARGET_FINAL_MOUNT_IDENTITY",
        });
        assert.throws(() => verifyHeadroomMounts(sample.receiptMounts, duplicate), {
            code: "HEADROOM_TARGET_FINAL_MOUNT_IDENTITY",
        });
    }
});

test("declared CPU1/2 lifecycle accepts one foreground original task and exact absence; rejects OOM/target mismatch/replay", () => {
    for (const cpu of [1, 2]) {
        const f = headroomLifecycle(cpu);
        assert.equal(verifyHeadroomLifecycle(f).driverCid, f.driverFinal.Id);
    }
    for (const mutate of [
        (f) => (f.targetFinal.State.OOMKilled = true),
        (f) => (f.targetFinal.State.Running = true),
        (f) => (f.targetFinal.State.Status = "dead"),
        (f) => (f.proof.containerFinal.State.ExitCode = 1),
        (f) => (f.driverFinal.HostConfig.NanoCpus = 1000000000),
        (f) => (f.httpTerminal.reused = true),
        (f) => f.commands.push({ ...f.commands.find((c) => c.argv.includes("run") && !c.argv.includes("--detach")) }),
    ]) {
        const f = headroomLifecycle(2);
        mutate(f);
        assert.throws(() => verifyHeadroomLifecycle(f));
    }
});

test("each original pre/post unit set is required and neighboring arms cannot reuse one post as next pre", () => {
    const f = headroomLifecycle(2),
        l = verifyHeadroomLifecycle(f),
        row = {
            ...f.arm,
            ...l,
            valid: true,
            javaIdentity: { pid: 77 },
            finalCommandFinishedMonotonicNs: f.commands.find((c) => c.argv.includes("report")).finishedMonotonicNs,
        };
    assert.doesNotThrow(() => verifyHeadroomCommandClosure(f.context, [row], f.commands));
    const first = f.commands.findIndex((c) => c.argv.includes("run"));
    const missing = f.commands.filter(
        (c, i) =>
            !(i < first && c.argv[0] === "/usr/bin/systemctl" && c.argv.includes("hgbench-sf100-convert-v1.service")),
    );
    assert.throws(() => verifyHeadroomCommandClosure(f.context, [row], missing), {
        code: "HEADROOM_PRE_POST_UNIT_MISSING",
    });
    const g = headroomLifecycle(2, { blockId: "b02", targetCid: "5".repeat(64), driverCid: "6".repeat(64) }),
        gl = verifyHeadroomLifecycle(g),
        offset = 100000n;
    for (const c of g.commands) {
        c.index += f.commands.length;
        c.startedMonotonicNs = String(BigInt(c.startedMonotonicNs) + offset);
        c.finishedMonotonicNs = String(BigInt(c.finishedMonotonicNs) + offset);
    }
    const gr = {
        ...g.arm,
        ...gl,
        valid: true,
        javaIdentity: { pid: 77 },
        start: String(BigInt(gl.start) + offset),
        finalCommandFinishedMonotonicNs: g.commands.find((c) => c.argv.includes("report")).finishedMonotonicNs,
    };
    f.context.sequence.physicalArms.push(g.arm);
    const all = [...f.commands, ...g.commands];
    assert.doesNotThrow(() => verifyHeadroomCommandClosure(f.context, [row, gr], all));
    const reused = all.filter(
        (c) =>
            !(
                BigInt(c.startedMonotonicNs) >= offset &&
                BigInt(c.startedMonotonicNs) < BigInt(gr.start) &&
                c.argv[0] === "/usr/bin/systemctl"
            ),
    );
    assert.throws(() => verifyHeadroomCommandClosure(f.context, [row, gr], reused), {
        code: "HEADROOM_UNIT_WINDOW_REUSED",
    });
});

test("re-sealing cannot shrink original heavy/predecessor unit denominator or change protected d6", async (t) => {
    const f = await headroomFixture(t);
    const units = headroomDeclaredUnits(f.sequence);
    assert.equal(units.length, 4);
    const copy = structuredClone(f.sequence);
    copy.declaredInactiveUnits.push(copy.declaredInactiveUnits[0]);
    assert.deepEqual(headroomDeclaredUnits(copy), units);
    copy.mode = "prepare";
    assert.throws(() => headroomDeclaredUnits(copy), { code: "HEADROOM_PREDECESSOR_UNITS" });
    f.sequence.declaredInactiveUnits.shift();
    await f.reseal();
    await assert.rejects(reportHeadroom(options(f)), { code: "HEADROOM_JOB_DECLARATIONS" });
    assert.equal((await get(join(f.root, "report/partial.json"))).cells.length, 36);
});

test("output cannot pollute a selected measurement source/runtime outside the reader install root", async (t) => {
    const f = await headroomFixture(t);
    f.spec.sourceRoot = f.sequence.sourceRoot = f.runtime.sourceRoot = join(f.root, "original-source");
    await put(join(f.plan, "runtime-selection.json"), f.runtime);
    await f.reseal();
    await assert.rejects(reportHeadroom({ ...options(f), outputDirectory: join(f.spec.sourceRoot, "pollution") }), {
        code: "HEADROOM_OUTPUT_OVERLAP",
    });
    assert.equal(await exists(join(f.spec.sourceRoot, "pollution")), false);
});

test("local workflow run IDs may repeat across twelve fresh arms while raw/physical identities remain globally unique", () => {
    const arms = headroomGrid().map((a, i) => ({
        ...a,
        runId: "physical-" + i,
        valid: true,
        rawManifestSha256: String(i).padStart(64, "0"),
        driverCid: "driver-" + i,
        targetCid: "target-" + i,
        cells: [{ runId: "r0001" }, { runId: "r0002" }, { runId: "r0003" }],
        javaIdentity: { bootId: "sameboot", pid: i + 1, startTimeTicks: String(i + 10) },
        start: String(i * 1000 + 100),
        finalCommandFinishedMonotonicNs: String(i * 1000 + 900),
    }));
    assert.equal(verifyHeadroomPhysicalIdentities(arms), true);
    const copied = structuredClone(arms);
    copied[1].rawManifestSha256 = copied[0].rawManifestSha256;
    assert.throws(() => verifyHeadroomPhysicalIdentities(copied), { code: "HEADROOM_PHYSICAL_IDENTITY_REUSED" });
    const duplicate = structuredClone(arms);
    duplicate[0].cells[1].runId = "r0001";
    assert.throws(() => verifyHeadroomPhysicalIdentities(duplicate), { code: "HEADROOM_PHYSICAL_IDENTITY_REUSED" });
});
test("every systemctl record belongs to one pre/post set; extra active observations and malformed protected load state reject", () => {
    const f = headroomLifecycle(2),
        l = verifyHeadroomLifecycle(f),
        row = {
            ...f.arm,
            ...l,
            valid: true,
            javaIdentity: { pid: 77 },
            finalCommandFinishedMonotonicNs: f.commands.find((c) => c.argv.includes("report")).finishedMonotonicNs,
        };
    const extra = {
        ...f.commands[0],
        index: f.commands.length,
        startedMonotonicNs: "999000",
        finishedMonotonicNs: "999900",
        stdout: f.commands[0].stdout.replace("ActiveState=inactive", "ActiveState=active"),
    };
    assert.throws(() => verifyHeadroomCommandClosure(f.context, [row], [...f.commands, extra]), {
        code: "HEADROOM_UNIT_UNASSOCIATED",
    });
    const protectedCommand = f.commands.find((c) => c.argv.includes(f.context.sequence.protectedObservedUnit));
    assert.throws(
        () =>
            verifyHeadroomUnit(
                { ...protectedCommand, stdout: protectedCommand.stdout.replace("LoadState=loaded", "LoadState=error") },
                f.context.sequence.protectedObservedUnit,
                true,
            ),
        { code: "HEADROOM_UNIT_LOAD_STATE" },
    );
});
