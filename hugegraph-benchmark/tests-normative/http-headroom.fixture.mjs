/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
// Synthetic offline protocol records only. No HugeGraph, containers, JVM, measured headroom or warmup proof.
import { mkdtemp, mkdir, readFile, writeFile, readdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, dirname } from "node:path";
import {
    HEADROOM002,
    headroomSha,
    headroomGrid,
    headroomProfile,
    headroom002Operations,
    headroomTaskId,
} from "../components/http/headroom002-contract.mjs";
import { valueHash } from "../components/paired/core.mjs";
export const put = async (p, v) => {
    await mkdir(dirname(p), { recursive: true });
    await writeFile(p, JSON.stringify(v, null, 2) + "\n");
};
export const get = async (p) => JSON.parse(await readFile(p, "utf8"));
export function registrationFixture(context, reference, dispatch) {
    const binding = {
        schemaVersion: 1,
        kind: "http-headroom-engineering-assessment-binding",
        criteriaSha256: HEADROOM002.criteriaSha256,
        planLockSha256: reference.planLockSha256,
        executionSpecSha256: reference.executionSpecSha256,
        sourceSelectionSha256: HEADROOM002.sourceSelectionSha256,
        preparedPlanDirectory: context.spec.planRoot,
        executionDirectory: dispatch.executionRoot,
        preDispatchLaunchRecordReference: { path: "launch.json" },
    };
    const bindingSha = headroomSha(JSON.stringify(binding, null, 2) + "\n");
    const launch = {
        schemaVersion: 1,
        kind: "http-headroom-engineering-launch",
        operationVersion: HEADROOM002.operationVersion,
        bindingSha256: bindingSha,
        planLockSha256: reference.planLockSha256,
        executionSpecSha256: reference.executionSpecSha256,
        sourceSelectionSha256: HEADROOM002.sourceSelectionSha256,
        hostBootId: dispatch.hostBootId,
        startedMonotonicNs: "1000",
        finishedMonotonicNs: "2000",
        argv: [
            context.runtime.hostPython,
            "-B",
            "/operator/http-driver-headroom-002-execute.py",
            "run",
            "--execution",
            dispatch.executionRoot,
        ],
    };
    const controller = {
        schemaVersion: 1,
        kind: "http-headroom-controller-start-evidence",
        hostBootId: dispatch.hostBootId,
        observedMonotonicNs: "4000",
        systemd: {
            argv: [
                "/usr/bin/systemctl",
                "--user",
                "show",
                "hgbench-http-driver-headroom-002.service",
                ...["MainPID", "ExecMainPID", "ExecMainStartTimestampMonotonic", "ControlGroup", "ActiveState"].flatMap(
                    (p) => ["-p", p],
                ),
            ],
            stdout:
                "MainPID=" +
                dispatch.pid +
                "\nExecMainPID=" +
                dispatch.pid +
                "\nExecMainStartTimestampMonotonic=3\nControlGroup=/user.slice/hgbench\nActiveState=active\n",
            exitCode: 0,
        },
        procStatRaw:
            dispatch.pid +
            " (python tricky ) name) S " +
            Array(18).fill("0").join(" ") +
            " " +
            dispatch.startTicks +
            "\n",
    };
    return { binding, bindingSha, launch, controller };
}
export async function headroomFixture(t) {
    const root = await mkdtemp(join(tmpdir(), "hgbench-headroom002-"));
    t.after(() => rm(root, { recursive: true, force: true }));
    const plan = join(root, "plan"),
        execution = join(root, "execution"),
        inputFile = join(root, "input.json");
    await mkdir(plan);
    await mkdir(execution);
    const fixed = await get(new URL("./fixtures/headroom002-fixed-contracts.json", import.meta.url));
    for (const [p, raw] of Object.entries(fixed.files)) await writeFile(join(plan, p), raw);
    const settings = await get(join(plan, "settings.json")),
        runtime = {
            sourceRoot: "/original/aa005/source",
            nodeHome: "/original/node",
            hostPython: "/usr/bin/python3.12",
            dockerExecutable: "/usr/bin/docker",
            driver: {
                image: settings.image,
                python: "/usr/local/bin/python3",
                cpus: 1,
                memoryBytes: 805306368,
                pids: 128,
                tmpfsBytes: 67108864,
                wallSeconds: 900,
                shutdownSeconds: 5,
                maxLogBytes: 1048576,
            },
        };
    await put(join(plan, "runtime-selection.json"), runtime);
    await put(join(plan, "build.json"), { sourceCommit: "d1239ef88d2d9b4b9b23fb920bcf7f6a387053d7" });
    await put(join(plan, "environment.json"), { kind: "synthetic-headroom-environment" });
    await put(join(plan, "source-verification.json"), { membersVerified: 529 });
    await writeFile(
        join(plan, "source-selection.lock.json"),
        await readFile(new URL("./fixtures/known002-source-selection.lock.json", import.meta.url)),
    );
    const sequence = {
        schemaVersion: 1,
        kind: "http-driver-headroom-operation",
        generatorVersion: 3,
        mode: "draft",
        executed: false,
        dispatchImplemented: false,
        attemptToken: "a".repeat(16),
        owner: "1000:1000",
        sourceRoot: runtime.sourceRoot,
        declaredInactiveUnits: [
            "hgbench-sf100-convert-v1.service",
            "hgbench-sf100-reference-convert-v1.service",
            "hgbench-sf100-update-audit-v1.service",
        ],
        protectedObservedUnit: "hgbench-sf100-docker-environment-v6.service",
        physicalArms: [],
    };
    const spec = {
        schemaVersion: 1,
        kind: "http-driver-headroom-execution-spec",
        planRoot: "/original/headroom002/plan",
        sourceRoot: runtime.sourceRoot,
        node: runtime.nodeHome + "/bin/node",
        nodeExecutable: { path: runtime.nodeHome + "/bin/node", bytes: 123, sha256: "2".repeat(64) },
        generatorSha256: HEADROOM002.generatorSha256,
        executorSha256: HEADROOM002.executorSha256,
        fixedLifecycleSha256: HEADROOM002.fixedLifecycleSha256,
        fixedPlanningHelperSha256: HEADROOM002.fixedPlanningHelperSha256,
        physicalArms: 12,
        runnable: false,
        owner: sequence.owner,
        driver: runtime.driver,
        dockerExecutable: runtime.dockerExecutable,
        budgets: { maxWorkBytes: 16 * 1024 ** 3, operationMs: 26000000 },
        observationStratum: { collector: true, jvmObservation: true, maxInFlight: 4 },
        windowPolicy: HEADROOM002.windowPolicy,
    };
    const map = {
        "start-fresh-target": "submitTarget",
        ready: "ready",
        "mode-pre": "modePre",
        "create-driver": "createDriver",
        "start-driver": "startDriver",
        "plan-http-inside-driver": "prepareDriver",
        "run-http": "waitHttp",
        "http-artifacts": "httpArtifacts",
        "mode-post": "modePost",
        "stop-target": "stopTarget",
        "target-terminal": "targetTerminal",
        "remove-owned-target": "removeTarget",
        "independent-http-rebuild": "rebuildHttp",
        "jvm-original": "parseJvmOriginal",
        "phase-windows": "phaseWindows",
    };
    for (const grid of headroomGrid()) {
        const runId = "hhr-" + sequence.attemptToken + "-" + grid.cellId,
            arm = {
                ...grid,
                runId,
                requestId: runId + "-http",
                targetTaskRequestId: runId + "-target",
                collectorRequestId: runId + "-collector",
                directory: spec.planRoot + "/arms/" + grid.cellId,
                collector: true,
                jvmObservation: true,
                preparedTargetTask: false,
            };
        const ops = headroom002Operations(
            arm,
            { ...sequence, runtimeNode: spec.node, nodeHome: runtime.nodeHome, hostPython: runtime.hostPython },
            spec,
        );
        arm.operations = Object.entries(map).map(([id, budgetKey]) => ({ budgetKey, argvTemplate: ops[id] }));
        arm.operations.push({
            budgetKey: "cleanupDriver",
            argvTemplates: [ops["stop-driver"], ops["remove-owned-driver"]],
        });
        sequence.physicalArms.push(arm);
        await put(
            join(plan, "arms", arm.cellId, "driver-evidence/profile.json"),
            headroomProfile(
                arm,
                ["build", "environment", "settings", "source-verification", "arm-context"].map(
                    (n) => "/evidence/" + n + ".json",
                ),
            ),
        );
    }
    spec.expectedTasks = sequence.physicalArms.map((a) => ({
        runId: a.runId,
        httpTaskId: headroomTaskId(a.requestId),
        targetTaskId: headroomTaskId(a.targetTaskRequestId),
        collectorTaskId: headroomTaskId(a.collectorRequestId),
    }));
    const input = {
        schemaVersion: 1,
        kind: "http-driver-headroom-evidence-input",
        operationVersion: HEADROOM002.operationVersion,
        planDirectory: "plan",
        executionDirectory: "execution",
        assessmentBindingFile: "binding.json",
        preDispatchLaunchRecordFile: "launch.json",
        controllerStartEvidenceFile: "controller.json",
        reference: {
            sourceSelectionSha256: HEADROOM002.sourceSelectionSha256,
            assessmentCriteriaSha256: HEADROOM002.criteriaSha256,
            assessmentBindingSha256: "0".repeat(64),
            preDispatchLaunchRecordSha256: "0".repeat(64),
            controllerStartEvidenceSha256: "0".repeat(64),
        },
    };
    async function reseal() {
        await put(join(plan, "sequence.json"), sequence);
        const files = [];
        async function walk(dir, rel = "") {
            for (const n of (await readdir(dir, { withFileTypes: true })).sort((a, b) =>
                a.name.localeCompare(b.name),
            )) {
                const p = rel ? rel + "/" + n.name : n.name;
                if (p === "plan.lock.json" || p === "headroom-dispatch.json") continue;
                if (n.isDirectory()) await walk(join(dir, n.name), p);
                else {
                    const raw = await readFile(join(dir, n.name));
                    files.push({ path: p, bytes: raw.length, sha256: headroomSha(raw) });
                }
            }
        }
        await walk(plan);
        files.sort((a, b) => a.path.localeCompare(b.path));
        await put(join(plan, "plan.lock.json"), {
            kind: "http-driver-headroom-operation-lock",
            generatorVersion: 3,
            generator: { sha256: HEADROOM002.generatorSha256 },
            fixedPlanningHelper: { sha256: HEADROOM002.fixedPlanningHelperSha256 },
            contentDigest: valueHash(files),
            files,
        });
        input.reference.planLockSha256 = headroomSha(await readFile(join(plan, "plan.lock.json")));
        spec.planLockSha256 = input.reference.planLockSha256;
        await put(join(execution, "execution-spec.json"), spec);
        input.reference.executionSpecSha256 = headroomSha(await readFile(join(execution, "execution-spec.json")));
        await put(join(execution, "execution.lock.json"), {
            kind: "http-driver-headroom-execution-lock",
            specSha256: input.reference.executionSpecSha256,
        });
        await put(inputFile, input);
    }
    async function register() {
        sequence.mode = "prepare";
        sequence.declaredInactiveUnits.push(
            "hgbench-http-aa-cohort-005.service",
            "hgbench-http-known-delay-002.service",
        );
        spec.runnable = true;
        for (const arm of sequence.physicalArms) arm.preparedTargetTask = true;
        await reseal();
        const dispatch = {
            executed: true,
            executionSpecSha256: input.reference.executionSpecSha256,
            executionRoot: "/original/headroom002/execution",
            hostBootId: "00000000-0000-0000-0000-000000000001",
            pid: 123,
            startTicks: "456",
        };
        await put(join(execution, "dispatch.json"), dispatch);
        await put(join(plan, "headroom-dispatch.json"), dispatch);
        const r = registrationFixture({ spec, runtime }, input.reference, dispatch);
        for (const [file, key, v] of [
            ["binding.json", "assessmentBindingSha256", r.binding],
            ["launch.json", "preDispatchLaunchRecordSha256", r.launch],
            ["controller.json", "controllerStartEvidenceSha256", r.controller],
        ]) {
            await put(join(root, file), v);
            input.reference[key] = headroomSha(await readFile(join(root, file)));
        }
        await put(inputFile, input);
        await put(join(execution, "commands/00001.json"), {
            argv: ["not-executed-protocol-fixture"],
            stdout: "",
            stderr: "",
            exitCode: 0,
            error: null,
            startedMonotonicNs: "5000",
            finishedMonotonicNs: "6000",
        });
        return dispatch;
    }
    await reseal();
    return { root, plan, execution, inputFile, input, spec, runtime, sequence, reseal, register };
}

export function headroomLifecycle(
    driverCpus = 2,
    { blockId = "b01", targetCid = "3".repeat(64), driverCid = "4".repeat(64) } = {},
) {
    const heavyUnits = [
        "hgbench-sf100-convert-v1.service",
        "hgbench-sf100-reference-convert-v1.service",
        "hgbench-sf100-update-audit-v1.service",
    ];
    const arm = {
        ...headroomGrid().find((a) => a.driverCpus === driverCpus && a.blockId === blockId),
        runId: "hkd-" + "a".repeat(16) + "-" + blockId + "-a",
        directory: "/original/plan/arms/" + blockId + "-A",
    };
    arm.requestId = arm.runId + "-http";
    arm.collectorRequestId = arm.runId + "-collector";
    const sequence = {
        sourceRoot: "/original/source",
        nodeHome: "/original/node",
        runtimeNode: "/original/node/bin/node",
        hostPython: "/usr/bin/python3.12",
        declaredHeavyUnits: [...heavyUnits],
        declaredInactiveUnits: [
            ...heavyUnits,
            "hgbench-http-aa-cohort-005.service",
            "hgbench-http-known-delay-002.service",
        ],
        additionalInactiveUnits: ["hgbench-http-aa-cohort-005.service", "hgbench-http-known-delay-002.service"],
        protectedObservedUnit: "hgbench-sf100-docker-environment-v6.service",
        physicalArms: [arm],
    };
    const spec = {
        planRoot: "/original/plan",
        node: "/original/node/bin/node",
        owner: "1000:1000",
        dockerExecutable: "/usr/bin/docker",
        driver: { image: "sha256:" + "1".repeat(64), python: "/usr/local/bin/python3" },
        sourceRoot: "/original/source",
    };
    const driverFinal = {
        Id: driverCid,
        Name: "/" + arm.runId + "-driver",
        Image: spec.driver.image,
        Config: {
            Labels: { "org.apache.hugegraph.hgbench.driver-headroom.request": arm.runId },
            User: spec.owner,
            WorkingDir: "/source",
            Entrypoint: [spec.driver.python],
            Cmd: ["-B", "-c", "import time; time.sleep(900)"],
        },
        HostConfig: {
            NetworkMode: "container:" + targetCid,
            ReadonlyRootfs: true,
            Memory: 805306368,
            MemorySwap: 805306368,
            NanoCpus: driverCpus * 1000000000,
            PidsLimit: 128,
            Privileged: false,
            CapDrop: ["ALL"],
            SecurityOpt: ["no-new-privileges:true"],
            Init: true,
            RestartPolicy: { Name: "no" },
            LogConfig: { Type: "none" },
            Tmpfs: { "/tmp": "rw,nosuid,nodev,noexec,size=67108864" },
        },
        Mounts: [
            { Source: sequence.sourceRoot, Destination: "/source", RW: false },
            { Source: sequence.nodeHome, Destination: "/node", RW: false },
            { Source: arm.directory + "/driver-evidence", Destination: "/evidence", RW: true },
        ],
        State: { Running: false, OOMKilled: false, Status: "exited", ExitCode: 143 },
    };
    const targetFinal = {
            Id: targetCid,
            Name: "/hgbench-http-" + "a".repeat(32),
            Image: spec.driver.image,
            Config: {
                User: spec.owner,
                Labels: {
                    "org.apache.hugegraph.hgbench.http.attempt": "a".repeat(32),
                    "org.apache.hugegraph.hgbench.http.plan": "b".repeat(64),
                },
            },
            Mounts: [],
            State: { Running: false, OOMKilled: false, Status: "exited", ExitCode: 0 },
        },
        environmentFinal = structuredClone(targetFinal),
        ready = { container: { id: targetCid }, ready: { jcmdExecutable: "/opt/jdk/bin/jcmd", java: { pid: 77 } } };
    const task = (plan) => {
        const v = { schemaVersion: 1, kind: "frozen-component-task", plan, files: [] };
        return { ...v, digest: valueHash(v) };
    };
    const httpTask = task({
            timeoutMs: 609000,
            terminateGraceMs: 5000,
            command: ["/node/bin/node", "/source/components/http/workflow.mjs", "--plan", "/evidence/http-plan"],
        }),
        targetTask = task({});
    const terminal = (request, plan) => ({
        taskId: headroomTaskId(request),
        planDigest: plan.digest,
        state: "completed",
        terminal: true,
        interrupted: false,
        processExitCode: 0,
        error: null,
    });
    const httpTerminal = terminal(arm.requestId, httpTask),
        targetTerminal = terminal(arm.runId + "-target", targetTask);
    const bindings = {
        "target.container.id": targetCid,
        "driver.container.id": driverCid,
        "target.ready.jcmdExecutable": "/opt/jdk/bin/jcmd",
        "target.ready.java.pid": 77,
        "http.task.id": headroomTaskId(arm.requestId),
        "collector.task.id": headroomTaskId(arm.collectorRequestId),
    };
    const ops = Object.fromEntries(
        Object.entries(headroom002Operations(arm, sequence, spec)).map(([k, v]) => [
            k,
            v.map((w) => w.replace(/\{\{([^}]+)\}\}/g, (_, key) => String(bindings[key]))),
        ]),
    );
    const commands = [];
    const add = (argv, stdout = "", exitCode = 0, stderr = "") => {
        const index = commands.length;
        const c = {
            index,
            argv,
            stdout: typeof stdout === "string" ? stdout : JSON.stringify(stdout),
            exitCode,
            stderr,
            error: null,
            startedMonotonicNs: String(index * 1000 + 100),
            finishedMonotonicNs: String(index * 1000 + 900),
        };
        commands.push(c);
        return c;
    };
    const cli = (id, data) => add(ops[id], { ok: true, data });
    const inspect = (cid) => [spec.dockerExecutable, "inspect", "--type", "container", cid];
    const jobs = () => {
        for (const unit of [
            ...sequence.declaredHeavyUnits,
            ...sequence.additionalInactiveUnits,
            sequence.protectedObservedUnit,
        ])
            add(
                [
                    "/usr/bin/systemctl",
                    "--user",
                    "show",
                    unit,
                    "-p",
                    "Id",
                    "-p",
                    "LoadState",
                    "-p",
                    "ActiveState",
                    "-p",
                    "SubState",
                    "-p",
                    "MainPID",
                    "-p",
                    "ControlGroup",
                    "-p",
                    "Result",
                ],
                "Id=" +
                    unit +
                    "\nLoadState=loaded\nActiveState=inactive\nSubState=dead\nMainPID=0\nControlGroup=\nResult=success\n",
            );
    };
    jobs();
    cli("start-fresh-target", { taskId: targetTerminal.taskId, planDigest: targetTask.digest });
    cli("ready", ready);
    const pre = add(ops["mode-pre"]);
    add(ops["create-driver"]);
    add(ops["start-driver"]);
    const running = structuredClone(driverFinal);
    running.State = { Running: true, OOMKilled: false, Status: "running", ExitCode: 0 };
    add(inspect(driverCid), [running]);
    add(ops["plan-http-inside-driver"]);
    cli("run-http", httpTerminal);
    cli("http-artifacts", {});
    add(inspect(driverCid), [running]);
    add(ops["stop-driver"]);
    add(inspect(driverCid), [driverFinal]);
    add(ops["remove-owned-driver"]);
    add(inspect(driverCid), "[]\n", 1, "Error response from daemon: No such container: " + driverCid + "\n");
    const post = add(ops["mode-post"]);
    cli("stop-target", {});
    cli("target-terminal", targetTerminal);
    add(inspect(targetCid), [targetFinal]);
    add(ops["remove-owned-target"]);
    add(inspect(targetCid), "[]\n", 1, "Error response from daemon: No such container: " + targetCid + "\n");
    add(ops["independent-http-rebuild"]);
    jobs();
    const proof = {
        containerId: targetCid,
        containerFinal: structuredClone(targetFinal),
        ready: ready.ready,
        pre: { window: [BigInt(pre.startedMonotonicNs) + 10n, BigInt(pre.finishedMonotonicNs) - 10n] },
        post: { window: [BigInt(post.startedMonotonicNs) + 10n, BigInt(post.finishedMonotonicNs) - 10n] },
    };
    return {
        arm,
        context: { sequence, spec },
        proof,
        ready,
        httpTask,
        targetTask,
        httpTerminal,
        targetTerminal,
        driverFinal,
        targetFinal,
        environmentFinal,
        commands,
    };
}
