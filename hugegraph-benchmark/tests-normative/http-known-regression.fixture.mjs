/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
// Offline protocol fixtures only: no actual HugeGraph, JVM, containers, or measurements.
import { mkdtemp, mkdir, writeFile, readFile, cp, readdir, rm } from "node:fs/promises";
import { join, dirname } from "node:path";
import { tmpdir } from "node:os";
import {
    KNOWN002,
    known002Policy,
    known002Operations,
    knownTaskId,
    knownSha,
} from "../components/http/known002-contract.mjs";
import { valueHash, balancedOrders } from "../components/paired/core.mjs";
export const put = async (p, v) => {
    await mkdir(dirname(p), { recursive: true });
    await writeFile(p, JSON.stringify(v, null, 2) + "\n");
};
export const get = async (p) => JSON.parse(await readFile(p, "utf8"));
const heavyUnits = [
    "hgbench-sf100-convert-v1.service",
    "hgbench-sf100-reference-convert-v1.service",
    "hgbench-sf100-update-audit-v1.service",
];
export async function knownFixture(t, { localSource = false } = {}) {
    const root = await mkdtemp(join(tmpdir(), "hgbench-known002-"));
    t.after(() => rm(root, { recursive: true, force: true }));
    const plan = join(root, "plan"),
        execution = join(root, "execution");
    await mkdir(plan);
    await mkdir(execution);
    const origin = "/original/known002/plan",
        sourceRoot = localSource ? join(root, "measured-source") : "/original/aa005/source",
        nodeHome = "/original/node";
    if (localSource) await mkdir(sourceRoot);
    const runtime = {
        sourceRoot,
        nodeHome,
        dockerExecutable: "/usr/bin/docker",
        driver: {
            image: "sha256:" + "1".repeat(64),
            python: "/usr/local/bin/python3",
            cpus: 1,
            memoryBytes: 805306368,
            pids: 128,
            tmpfsBytes: 67108864,
            wallSeconds: 2196,
            shutdownSeconds: 5,
            maxLogBytes: 1048576,
        },
    };
    await put(join(plan, "runtime-selection.json"), runtime);
    await put(join(plan, "build.json"), { sourceCommit: KNOWN002.commit, distribution: "/original/build", files: [] });
    await put(join(plan, "environment.json"), { schemaVersion: 1, kind: "http-known-delay-environment-draft" });
    await put(join(plan, "source-verification.json"), {
        membersVerified: 529,
        additionalSourceMembersExcludedFromClaim: true,
    });
    await put(join(plan, "settings.json"), {
        mode: "forward",
        purpose: "calibration",
        image: runtime.driver.image,
        budget: { cpus: 2, memoryBytes: 2 * 1024 ** 3, wallSeconds: 600, startupSeconds: 150, shutdownSeconds: 20 },
    });
    await cp(
        new URL("./fixtures/known002-source-selection.lock.json", import.meta.url),
        join(plan, "source-selection.lock.json"),
    );
    const sourceSha = knownSha(await readFile(join(plan, "source-selection.lock.json")));
    const sourceDigests = {};
    for (const n of ["build", "environment", "settings"])
        sourceDigests[n] = knownSha(await readFile(join(plan, n + ".json")));
    const policy = known002Policy({ ...sourceDigests, sourceLock: sourceSha, mode: "forward", driver: runtime.driver });
    await put(join(plan, "policy.json"), policy);
    const sequence = {
        schemaVersion: 1,
        kind: "http-known-delay-public-operation-sequence",
        operationVersion: KNOWN002.operationVersion,
        mode: "draft",
        id: KNOWN002.id,
        attemptToken: "a".repeat(16),
        sourceRoot,
        node: nodeHome + "/bin/node",
        blockOrder: balancedOrders(KNOWN002.blocks, policy.orderSeed),
        executed: false,
        dispatchImplemented: false,
        declaredHeavyUnits: [...heavyUnits],
        additionalInactiveUnits: ["hgbench-http-aa-cohort-005.service"],
        protectedObservedUnit: "hgbench-sf100-docker-environment-v6.service",
        physicalArms: [],
    };
    const spec = {
        schemaVersion: 1,
        kind: "http-known-delay-execution-spec",
        planRoot: origin,
        sourceRoot,
        operationVersion: KNOWN002.operationVersion,
        sourceLockSha256: sourceSha,
        executorSha256: KNOWN002.executorSha256,
        generatorSha256: KNOWN002.generatorSha256,
        node: sequence.node,
        nodeExecutable: { path: sequence.node, bytes: 123, sha256: "2".repeat(64) },
        dockerExecutable: runtime.dockerExecutable,
        driver: { ...runtime.driver },
        owner: "1000:1000",
        runnable: false,
        physicalArms: 16,
        budgets: { maxWorkBytes: 8 * 1024 ** 3, operationMs: 9000000, maxCommandBytes: 2097152, maxCommands: 8192 },
    };
    for (const block of KNOWN002.blocks)
        for (const arm of sequence.blockOrder[block]) {
            const runId = "hkd-" + sequence.attemptToken + "-" + block + "-" + arm.toLowerCase();
            const row = {
                blockId: block,
                arm,
                runId,
                requestId: runId + "-http",
                directory: origin + "/arms/" + block + "-" + arm,
                preparedTargetTask: false,
            };
            row.operations = Object.entries(
                known002Operations(row, { ...sequence, runtimeNode: nodeHome + "/bin/node", nodeHome }, spec),
            ).map(([id, argv]) => ({ id, argv }));
            sequence.physicalArms.push(row);
            const profile = {
                schemaVersion: 1,
                kind: "http-benchmark-profile",
                name: KNOWN002.id + "-" + arm.toLowerCase(),
                endpoint: "http://127.0.0.1:18080/gremlin",
                authorizationEnv: null,
                responseEncoding: "gzip",
                serviceChecks: true,
                target: {
                    label: "hugegraph-known-delay-positive-control",
                    build: KNOWN002.commit,
                    mode: "forward",
                    environment:
                        "Artificial sleep10 request calibration; custom workload; same candidate and forward mode; not a standard-suite score or forward/embedded comparison.",
                    evidenceFiles: ["build", "environment", "settings", "source-verification"].map(
                        (n) => origin + "/" + n + ".json",
                    ),
                },
                cases: [
                    {
                        id: "arithmetic-control",
                        body: { gremlin: KNOWN002.scripts[arm], bindings: {}, language: "gremlin-groovy" },
                        expectedStatus: 200,
                        checks: [
                            { pointer: "/status/code", equals: 200 },
                            { pointer: "/result/data", equals: [2] },
                        ],
                    },
                ],
                loads: [{ id: "open-20", model: "open", ratePerSecond: 20, maxInFlight: 4, maxStartDelayMs: 1000 }],
                repeats: 1,
                warmupMs: 10000,
                durationMs: 60000,
                requestTimeoutMs: 2000,
                drainMs: 2000,
                maxRequestsPerPhase: 2000,
                maxResponseBytes: 65536,
                maxTraceBytes: 134217728,
            };
            await put(join(plan, "profiles", arm + ".json"), profile);
            await put(join(plan, "arms", block + "-" + arm, "profile.json"), profile);
            profile.target.evidenceFiles = profile.target.evidenceFiles.map((f) => "/evidence/" + f.split("/").at(-1));
            await put(join(plan, "arms", block + "-" + arm, "driver-evidence/profile.json"), profile);
        }
    await put(join(plan, "sequence.json"), sequence);
    await put(join(plan, "budgets.json"), {
        physicalArms: 16,
        pairs: 8,
        minimumObservationsPerArm: 1000,
        httpTaskTimeoutMs: 145000,
        httpDeadlineMs: 85000,
        plannedMeasurementRequests: 1200,
    });
    spec.expectedTasks = sequence.physicalArms.map((a) => ({
        runId: a.runId,
        httpTaskId: knownTaskId(a.requestId),
        targetTaskId: knownTaskId(a.runId + "-target"),
    }));
    const input = {
        schemaVersion: 1,
        kind: "http-known-regression-evidence-input",
        operationVersion: KNOWN002.operationVersion,
        planDirectory: "plan",
        executionDirectory: "execution",
        reference: { sourceSelectionSha256: sourceSha },
    };
    async function reseal() {
        const files = [];
        async function walk(p = "") {
            for (const e of await readdir(join(plan, p), { withFileTypes: true })) {
                const name = p ? p + "/" + e.name : e.name;
                if (name === "plan.lock.json") continue;
                if (e.isDirectory()) await walk(name);
                else {
                    const raw = await readFile(join(plan, name));
                    files.push({ path: name, bytes: raw.length, sha256: knownSha(raw) });
                }
            }
        }
        await walk();
        files.sort((a, b) => a.path.localeCompare(b.path));
        await put(join(plan, "plan.lock.json"), {
            schemaVersion: 1,
            kind: "http-known-delay-operation-lock",
            generator: { bytes: 37314, sha256: KNOWN002.generatorSha256 },
            files,
            contentDigest: valueHash(files),
        });
        input.reference.planLockSha256 = knownSha(await readFile(join(plan, "plan.lock.json")));
        spec.planLockSha256 = input.reference.planLockSha256;
        await put(join(execution, "execution-spec.json"), spec);
        input.reference.executionSpecSha256 = knownSha(await readFile(join(execution, "execution-spec.json")));
        await put(join(execution, "execution.lock.json"), {
            schemaVersion: 1,
            kind: "http-known-delay-execution-lock",
            specSha256: input.reference.executionSpecSha256,
        });
        await put(join(root, "input.json"), input);
    }
    await reseal();
    return { root, plan, execution, input, inputFile: join(root, "input.json"), spec, sequence, policy, reseal };
}

export function knownLifecycle({ blockId = "b01", targetCid = "3".repeat(64), driverCid = "4".repeat(64) } = {}) {
    const arm = {
        blockId,
        arm: "A",
        runId: "hkd-" + "a".repeat(16) + "-" + blockId + "-a",
        directory: "/original/plan/arms/" + blockId + "-A",
    };
    arm.requestId = arm.runId + "-http";
    const sequence = {
        sourceRoot: "/original/source",
        nodeHome: "/original/node",
        runtimeNode: "/original/node/bin/node",
        declaredHeavyUnits: [...heavyUnits],
        additionalInactiveUnits: ["hgbench-http-aa-cohort-005.service"],
        protectedObservedUnit: "hgbench-sf100-docker-environment-v6.service",
        physicalArms: [arm],
    };
    const spec = {
        owner: "1000:1000",
        dockerExecutable: "/usr/bin/docker",
        driver: { image: "sha256:" + "1".repeat(64), python: "/usr/local/bin/python3" },
        node: "/original/node/bin/node",
        sourceRoot: "/original/source",
    };
    const driverFinal = {
        Id: driverCid,
        Name: "/" + arm.runId + "-driver",
        Image: spec.driver.image,
        Config: {
            Labels: { "org.apache.hugegraph.hgbench.known-delay.request": arm.runId },
            User: spec.owner,
            WorkingDir: "/source",
            Entrypoint: [spec.driver.python],
            Cmd: ["-B", "-c", "import time; time.sleep(180)"],
        },
        HostConfig: {
            NetworkMode: "container:" + targetCid,
            ReadonlyRootfs: true,
            Memory: 805306368,
            MemorySwap: 805306368,
            NanoCpus: 1000000000,
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
            State: { Running: false, OOMKilled: false, Status: "exited", ExitCode: 0 },
        },
        environmentFinal = structuredClone(targetFinal),
        ready = { container: { id: targetCid }, ready: { jcmdExecutable: "/opt/jdk/bin/jcmd", java: { pid: 77 } } };
    const task = (plan) => {
        const v = { schemaVersion: 1, kind: "frozen-component-task", plan, files: [] };
        return { ...v, digest: valueHash(v) };
    };
    const httpTask = task({
            timeoutMs: 145000,
            terminateGraceMs: 5000,
            command: ["/node/bin/node", "/source/components/http/workflow.mjs", "--plan", "/evidence/http-plan"],
        }),
        targetTask = task({});
    const terminal = (request, plan) => ({
        taskId: knownTaskId(request),
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
        "http.task.id": knownTaskId(arm.requestId),
    };
    const ops = Object.fromEntries(
        Object.entries(known002Operations(arm, sequence, spec)).map(([k, v]) => [
            k,
            v.map((w) => w.replace(/\{\{([^}]+)\}\}/g, (_, key) => String(bindings[key]))),
        ]),
    );
    ops["mode-post"][ops["mode-post"].indexOf("--total-timeout-seconds") + 1] = "30";
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
