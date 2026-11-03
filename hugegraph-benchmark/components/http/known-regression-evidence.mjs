/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
import { constants } from "node:fs";
import { open, realpath, lstat, readdir, mkdir, writeFile, rm } from "node:fs/promises";
import { resolve, dirname, join, sep, basename } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { createHash } from "node:crypto";
import { spawn } from "node:child_process";
import { performance } from "node:perf_hooks";
import { isDeepStrictEqual } from "node:util";
import { analyze as analyzeHttp } from "./analyzer.mjs";
import { verifyControlledEnvironment } from "./paired-evidence.mjs";
import { valueHash, balancedOrders } from "../paired/core.mjs";
import {
    KNOWN002,
    needKnown as need,
    sameKnown as same,
    knownSha as hash,
    knownTaskId,
    knownNs as ns,
    known002Policy,
    known002Operations,
    verifyKnown002Profile,
    known002Driver,
    verifyKnown002Task,
    summarizeKnown002,
} from "./known002-contract.mjs";

const SELF = fileURLToPath(import.meta.url),
    ROOT = resolve(dirname(SELF), "../..");
const MiB = 1024 ** 2,
    SHA = /^[a-f0-9]{64}$/,
    CID = SHA;
const HEAVY_UNITS = [
        "hgbench-sf100-convert-v1.service",
        "hgbench-sf100-reference-convert-v1.service",
        "hgbench-sf100-update-audit-v1.service",
    ],
    PROTECTED_UNIT = "hgbench-sf100-docker-environment-v6.service",
    PREDECESSOR_UNIT = "hgbench-http-aa-cohort-005.service";
const encode = (v) => Buffer.from(JSON.stringify(v, null, 2) + "\n");
const exact = (v, keys) => same(Object.keys(v ?? {}).sort(), [...keys].sort(), "KNOWN_INPUT_FIELDS");
const inside = (a, b) => a === b || a.startsWith(b + sep);
async function bounded(path, cap = 16 * MiB) {
    const fd = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
    try {
        const a = await fd.stat();
        need(a.isFile() && a.size <= cap, "KNOWN_INPUT_TYPE_OR_BUDGET");
        const raw = Buffer.alloc(a.size + 1);
        let n = 0;
        while (n < raw.length) {
            const r = await fd.read(raw, n, raw.length - n, n);
            if (!r.bytesRead) break;
            n += r.bytesRead;
        }
        const b = await fd.stat();
        need(
            n === a.size && b.size === a.size && a.mtimeMs === b.mtimeMs && a.ctimeMs === b.ctimeMs,
            "KNOWN_INPUT_CHANGED",
        );
        return raw.subarray(0, n);
    } finally {
        await fd.close();
    }
}
const read = async (path, cap) => JSON.parse(new TextDecoder("utf8", { fatal: true }).decode(await bounded(path, cap)));
const save = async (path, value, cap) => {
    const raw = encode(value);
    need(raw.length <= cap, "KNOWN_OUTPUT_BYTE_LIMIT");
    await writeFile(path, raw, { flag: "wx", mode: 0o600 });
    return { path: basename(path), bytes: raw.length, sha256: hash(raw) };
};
async function canonical(path) {
    const suffix = [];
    let p = resolve(path);
    while (true) {
        try {
            return join(await realpath(p), ...suffix.reverse());
        } catch (e) {
            if (e.code !== "ENOENT") throw e;
        }
        suffix.push(basename(p));
        const parent = dirname(p);
        need(p !== parent, "KNOWN_OUTPUT_PATH");
        p = parent;
    }
}
export function validateKnownRegressionInput(value) {
    exact(value, ["schemaVersion", "kind", "operationVersion", "planDirectory", "executionDirectory", "reference"]);
    exact(value.reference, ["planLockSha256", "executionSpecSha256", "sourceSelectionSha256"]);
    need(
        value.schemaVersion === 1 &&
            value.kind === "http-known-regression-evidence-input" &&
            value.operationVersion === KNOWN002.operationVersion &&
            [value.planDirectory, value.executionDirectory].every(
                (p) => typeof p === "string" && p.length > 0 && p.length <= 4096 && !/[\r\n\0]/.test(p),
            ) &&
            Object.values(value.reference).every((v) => typeof v === "string" && SHA.test(v)) &&
            value.reference.sourceSelectionSha256 === KNOWN002.sourceSelectionSha256,
        "KNOWN_INPUT_IDENTITY",
    );
    return value;
}
export function checkKnownRegressionDeadline(signal, begun, timeoutMs) {
    need(!signal.aborted, "KNOWN_CANCELLED");
    need(performance.now() - begun < timeoutMs, "KNOWN_DEADLINE");
}
function limits(timeoutMs, maxBytes) {
    need(Number.isSafeInteger(timeoutMs) && timeoutMs >= 1000 && timeoutMs <= 86400000, "KNOWN_TIME_LIMIT");
    need(Number.isSafeInteger(maxBytes) && maxBytes >= MiB && maxBytes <= 8 * 1024 ** 3, "KNOWN_BYTE_LIMIT");
}
async function inventory(root, cap) {
    const files = [];
    let total = 0,
        entries = 0;
    async function walk(relative, depth) {
        need(depth <= 20, "KNOWN_INPUT_DEPTH");
        for (const name of (await readdir(join(root, relative))).sort()) {
            need(++entries <= 32768, "KNOWN_INPUT_MEMBERS");
            const p = relative ? relative + "/" + name : name;
            const full = join(root, p),
                s = await lstat(full);
            if (s.isDirectory()) {
                await walk(p, depth + 1);
                continue;
            }
            need(s.isFile() && s.size <= 256 * MiB && total + s.size <= cap, "KNOWN_INPUT_TYPE_OR_BUDGET");
            total += s.size;
            const fd = await open(full, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
            let count = 0;
            const digest = createHash("sha256");
            try {
                const a = await fd.stat();
                need(a.isFile() && a.size === s.size, "KNOWN_INPUT_CHANGED");
                const buffer = Buffer.alloc(64 * 1024);
                while (true) {
                    const r = await fd.read(buffer, 0, buffer.length, count);
                    if (!r.bytesRead) break;
                    count += r.bytesRead;
                    need(count <= a.size, "KNOWN_INPUT_CHANGED");
                    digest.update(buffer.subarray(0, r.bytesRead));
                }
                const b = await fd.stat();
                need(
                    count === a.size && b.size === a.size && a.mtimeMs === b.mtimeMs && a.ctimeMs === b.ctimeMs,
                    "KNOWN_INPUT_CHANGED",
                );
            } finally {
                await fd.close();
            }
            files.push({ path: p, bytes: count, sha256: digest.digest("hex") });
        }
    }
    await walk("", 0);
    return { files, bytes: total };
}
function template(op, bindings) {
    return op.map((word) =>
        word.replace(/\{\{([^}]+)\}\}/g, (_, key) => {
            need(bindings[key] !== undefined, "KNOWN_LIVE_BINDING");
            return String(bindings[key]);
        }),
    );
}
function commandData(command) {
    need(command.exitCode === 0 && command.error === null, "KNOWN_COMMAND_FAILED");
    const value = JSON.parse(command.stdout);
    need(value.ok === true && value.data && typeof value.data === "object", "KNOWN_COMMAND_RESPONSE");
    return value.data;
}
async function planContract(plan, execution, reference) {
    const lockRaw = await bounded(join(plan, "plan.lock.json")),
        lock = JSON.parse(lockRaw);
    need(
        hash(lockRaw) === reference.planLockSha256 &&
            lock.schemaVersion === 1 &&
            lock.kind === "http-known-delay-operation-lock" &&
            lock.generator?.sha256 === KNOWN002.generatorSha256 &&
            Array.isArray(lock.files) &&
            lock.files.length <= 20000 &&
            lock.contentDigest === valueHash(lock.files),
        "KNOWN_PLAN_LOCK",
    );
    const locked = new Map();
    let bytes = 0;
    for (const f of lock.files) {
        need(
            typeof f.path === "string" &&
                /^[A-Za-z0-9_./-]+$/.test(f.path) &&
                !f.path.startsWith("/") &&
                !f.path.split("/").some((p) => ["", ".", ".."].includes(p)) &&
                !locked.has(f.path) &&
                Number.isSafeInteger(f.bytes) &&
                f.bytes >= 0 &&
                SHA.test(f.sha256),
            "KNOWN_PLAN_MEMBER",
        );
        bytes += f.bytes;
        need(bytes <= 256 * MiB, "KNOWN_PLAN_BYTE_LIMIT");
        const raw = await bounded(join(plan, f.path));
        need(raw.length === f.bytes && hash(raw) === f.sha256, "KNOWN_PLAN_MEMBER_CHANGED");
        locked.set(f.path, f);
    }
    const load = async (p) => {
        need(locked.has(p), "KNOWN_PLAN_REQUIRED_MEMBER");
        return read(join(plan, p));
    };
    const sourceRaw = await bounded(join(plan, "source-selection.lock.json"));
    need(hash(sourceRaw) === reference.sourceSelectionSha256, "KNOWN_SOURCE_SELECTION");
    const source = JSON.parse(sourceRaw);
    need(source.files?.length === 529 && new Set(source.files.map((f) => f.path)).size === 529, "KNOWN_SOURCE_MEMBERS");
    const sequence = await load("sequence.json"),
        runtime = await load("runtime-selection.json"),
        settings = await load("settings.json");
    const build = await load("build.json"),
        environment = await load("environment.json"),
        policy = await load("policy.json");
    const specRaw = await bounded(join(execution, "execution-spec.json")),
        spec = JSON.parse(specRaw);
    const executionLock = await read(join(execution, "execution.lock.json"));
    need(
        hash(specRaw) === reference.executionSpecSha256 &&
            executionLock.kind === "http-known-delay-execution-lock" &&
            executionLock.specSha256 === reference.executionSpecSha256 &&
            spec.schemaVersion === 1 &&
            spec.kind === "http-known-delay-execution-spec" &&
            spec.planLockSha256 === reference.planLockSha256 &&
            spec.sourceLockSha256 === reference.sourceSelectionSha256 &&
            spec.executorSha256 === KNOWN002.executorSha256 &&
            spec.generatorSha256 === KNOWN002.generatorSha256 &&
            spec.operationVersion === KNOWN002.operationVersion &&
            spec.sourceRoot === sequence.sourceRoot &&
            spec.physicalArms === 16,
        "KNOWN_EXECUTION_SPEC",
    );
    need(
        sequence.schemaVersion === 1 &&
            sequence.kind === "http-known-delay-public-operation-sequence" &&
            sequence.operationVersion === KNOWN002.operationVersion &&
            sequence.id === KNOWN002.id &&
            ["draft", "prepare"].includes(sequence.mode) &&
            sequence.executed === false &&
            sequence.dispatchImplemented === false &&
            /^[a-f0-9]{16}$/.test(sequence.attemptToken) &&
            sequence.physicalArms?.length === 16 &&
            typeof spec.planRoot === "string" &&
            spec.planRoot.startsWith("/") &&
            typeof spec.node === "string" &&
            spec.node.startsWith("/") &&
            typeof sequence.node === "string" &&
            sequence.node.startsWith("/") &&
            typeof spec.sourceRoot === "string" &&
            spec.sourceRoot.startsWith("/") &&
            /^[0-9]+:[0-9]+$/.test(spec.owner),
        "KNOWN_SEQUENCE",
    );
    same(sequence.declaredHeavyUnits, HEAVY_UNITS, "KNOWN_JOB_DECLARATIONS");
    need(
        sequence.protectedObservedUnit === PROTECTED_UNIT &&
            Array.isArray(sequence.additionalInactiveUnits) &&
            sequence.additionalInactiveUnits.every(
                (unit) =>
                    typeof unit === "string" &&
                    /^[A-Za-z0-9][A-Za-z0-9_.@:-]{0,239}\.service$/.test(unit) &&
                    unit !== PROTECTED_UNIT,
            ) &&
            (sequence.mode !== "prepare" || sequence.additionalInactiveUnits.includes(PREDECESSOR_UNIT)),
        "KNOWN_JOB_DECLARATIONS",
    );
    need(
        spec.runnable === (sequence.mode === "prepare") &&
            spec.planRoot !== spec.sourceRoot &&
            build.sourceCommit === KNOWN002.commit &&
            settings.purpose === "calibration" &&
            settings.mode === "forward" &&
            !settings.jvmObservation &&
            settings.budget?.cpus === 2 &&
            settings.budget?.memoryBytes === 2 * 1024 ** 3 &&
            settings.budget?.wallSeconds === 600 &&
            settings.budget?.startupSeconds === 150 &&
            settings.budget?.shutdownSeconds === 20 &&
            spec.driver?.cpus === 1 &&
            spec.driver?.memoryBytes === 805306368 &&
            spec.driver?.pids === 128 &&
            spec.driver?.python === "/usr/local/bin/python3" &&
            spec.driver?.tmpfsBytes === 67108864 &&
            /^sha256:[a-f0-9]{64}$/.test(spec.driver?.image) &&
            settings.image === spec.driver.image &&
            runtime.driver.image === settings.image &&
            runtime.dockerExecutable === spec.dockerExecutable &&
            SHA.test(spec.nodeExecutable?.sha256),
        "KNOWN_RESOURCE_SCOPE",
    );
    const shared = {
        build: locked.get("build.json").sha256,
        environment: locked.get("environment.json").sha256,
        settings: locked.get("settings.json").sha256,
        sourceLock: reference.sourceSelectionSha256,
        mode: "forward",
        driver: runtime.driver,
    };
    same(policy, known002Policy(shared), "KNOWN_POLICY");
    same(sequence.blockOrder, balancedOrders(KNOWN002.blocks, policy.orderSeed), "KNOWN_ORDER");
    const expected = KNOWN002.blocks.flatMap((b) => sequence.blockOrder[b].split("").map((arm) => [b, arm]));
    const profiles = {};
    const hostEvidence = ["build", "environment", "settings", "source-verification"].map(
        (n) => spec.planRoot + "/" + n + ".json",
    );
    for (const arm of ["A", "B"])
        profiles[arm] = verifyKnown002Profile(await load("profiles/" + arm + ".json"), arm, hostEvidence);
    const actualSequence = { ...sequence, runtimeNode: runtime.nodeHome + "/bin/node", nodeHome: runtime.nodeHome };
    for (let i = 0; i < expected.length; i++) {
        const [block, letter] = expected[i],
            arm = sequence.physicalArms[i];
        need(
            arm.blockId === block &&
                arm.arm === letter &&
                arm.runId === "hkd-" + sequence.attemptToken + "-" + block + "-" + letter.toLowerCase() &&
                arm.requestId === arm.runId + "-http" &&
                arm.directory === spec.planRoot + "/arms/" + block + "-" + letter &&
                arm.preparedTargetTask === (sequence.mode === "prepare"),
            "KNOWN_ARM_IDENTITY",
        );
        same(await load("arms/" + block + "-" + letter + "/profile.json"), profiles[letter], "KNOWN_ARM_PROFILE");
        const insideProfile = await load("arms/" + block + "-" + letter + "/driver-evidence/profile.json");
        verifyKnown002Profile(
            insideProfile,
            letter,
            hostEvidence.map((f) => "/evidence/" + basename(f)),
        );
        const ops = known002Operations(arm, actualSequence, spec);
        need(
            Array.isArray(arm.operations) && new Set(arm.operations.map((o) => o.id)).size === arm.operations.length,
            "KNOWN_OPERATIONS",
        );
        for (const [id, argv] of Object.entries(ops)) {
            const op = arm.operations.find((v) => v.id === id);
            same(op?.argv ?? op?.argvTemplate, argv, "KNOWN_OPERATION_TEMPLATE");
        }
    }
    same(
        spec.expectedTasks,
        sequence.physicalArms.map((a) => ({
            runId: a.runId,
            httpTaskId: knownTaskId(a.requestId),
            targetTaskId: knownTaskId(a.runId + "-target"),
        })),
        "KNOWN_TASK_IDENTITIES",
    );
    const budgets = await load("budgets.json");
    need(
        budgets.physicalArms === 16 &&
            budgets.pairs === 8 &&
            budgets.minimumObservationsPerArm === 1000 &&
            budgets.httpTaskTimeoutMs === 145000 &&
            budgets.httpDeadlineMs === 85000 &&
            budgets.plannedMeasurementRequests === 1200 &&
            spec.budgets?.maxWorkBytes === 8 * 1024 ** 3 &&
            spec.budgets?.operationMs === 9000000 &&
            spec.budgets?.maxCommandBytes === 2 * MiB &&
            spec.budgets?.maxCommands === 8192,
        "KNOWN_BUDGET_CONTRACT",
    );
    return { sequence: actualSequence, settings, build, environment, policy, spec, source, profiles, locked };
}

async function commandsAt(execution) {
    const names = (await readdir(join(execution, "commands"))).sort();
    need(names.length > 0 && names.length <= 8192, "KNOWN_COMMAND_INVENTORY");
    const commands = [];
    let last = 0n;
    for (let i = 0; i < names.length; i++) {
        need(names[i] === String(i + 1).padStart(5, "0") + ".json", "KNOWN_COMMAND_INVENTORY");
        const c = await read(join(execution, "commands", names[i]), 2 * MiB);
        need(
            Array.isArray(c.argv) &&
                c.argv.length <= 256 &&
                c.argv.every((v) => typeof v === "string" && v.length <= 8192) &&
                typeof c.stdout === "string" &&
                typeof c.stderr === "string" &&
                Buffer.byteLength(c.stdout) + Buffer.byteLength(c.stderr) <= 2 * MiB &&
                Number.isInteger(c.exitCode),
            "KNOWN_COMMAND_RECORD",
        );
        const start = ns(c.startedMonotonicNs),
            end = ns(c.finishedMonotonicNs);
        need(last <= start && start < end, "KNOWN_COMMAND_ORDER");
        last = end;
        commands.push({ ...c, index: i });
    }
    return commands;
}
export function verifyKnown002Lifecycle({
    arm,
    context,
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
}) {
    const { sequence, spec } = context,
        targetCid = proof.containerId;
    same(ready.container.id, targetCid, "KNOWN_READY_CID");
    same(ready.ready, proof.ready, "KNOWN_READY_RECEIPT");
    const driverCid = known002Driver(driverFinal, arm, sequence, spec, targetCid);
    same(targetFinal.Id, targetCid, "KNOWN_TARGET_FINAL_ID");
    need(
        targetFinal.State?.Running === false &&
            targetFinal.State?.OOMKilled === false &&
            targetFinal.State?.Status === "exited" &&
            targetFinal.State?.ExitCode === 0,
        "KNOWN_TARGET_STOP",
    );
    const targetIdentity = (info) => {
        const labels = info?.Config?.Labels,
            attempt = labels?.["org.apache.hugegraph.hgbench.http.attempt"],
            plan = labels?.["org.apache.hugegraph.hgbench.http.plan"];
        need(
            CID.test(info?.Id) &&
                typeof info.Name === "string" &&
                /^\/hgbench-http-[a-f0-9]{32}$/.test(info.Name) &&
                /^sha256:[a-f0-9]{64}$/.test(info.Image) &&
                /^[0-9]+:[0-9]+$/.test(info.Config?.User) &&
                /^[a-f0-9]{32}$/.test(attempt) &&
                SHA.test(plan),
            "KNOWN_TARGET_FINAL_IDENTITY",
        );
        return { Id: info.Id, Name: info.Name, Image: info.Image, User: info.Config.User, attempt, plan };
    };
    // The environment consumer already validates its final inspect. Bind only stable ownership identity here.
    same(targetIdentity(targetFinal), targetIdentity(environmentFinal), "KNOWN_TARGET_ENVIRONMENT_FINAL");
    need(
        driverFinal.State?.Running === false &&
            driverFinal.State?.OOMKilled === false &&
            driverFinal.State?.Status === "exited" &&
            [0, 143, -15].includes(driverFinal.State?.ExitCode),
        "KNOWN_DRIVER_STOP",
    );
    const httpDigest = verifyKnown002Task(httpTask, httpTerminal, arm.requestId);
    verifyKnown002Task(targetTask, targetTerminal, arm.runId + "-target");
    need(
        httpTask.plan.timeoutMs === 145000 &&
            httpTask.plan.terminateGraceMs === 5000 &&
            httpTask.plan.command?.[0] === "/node/bin/node" &&
            httpTask.plan.command?.[1] === "/source/components/http/workflow.mjs" &&
            httpTask.plan.command?.[2] === "--plan" &&
            httpTask.plan.command?.[3] === "/evidence/http-plan",
        "KNOWN_HTTP_TASK_COMMAND",
    );
    const bindings = {
        "target.container.id": targetCid,
        "driver.container.id": driverCid,
        "target.ready.jcmdExecutable": ready.ready.jcmdExecutable,
        "target.ready.java.pid": ready.ready.java.pid,
        "http.task.id": knownTaskId(arm.requestId),
    };
    const ops = Object.fromEntries(
        Object.entries(known002Operations(arm, sequence, spec)).map(([k, argv]) => [k, template(argv, bindings)]),
    );
    ops["mode-post"][ops["mode-post"].indexOf("--total-timeout-seconds") + 1] = "30";
    const once = (id) => {
        const records = commands.filter((c) => isDeepStrictEqual(c.argv, ops[id]));
        need(
            records.length === 1 && records[0].exitCode === 0 && records[0].error === null,
            "KNOWN_COMMAND_REQUIRED:" + id,
        );
        return records[0];
    };
    const targetStart = once("start-fresh-target"),
        before = once("mode-pre"),
        create = once("create-driver"),
        start = once("start-driver"),
        plan = once("plan-http-inside-driver"),
        run = once("run-http"),
        artifacts = once("http-artifacts"),
        stopDriver = once("stop-driver"),
        removeDriver = once("remove-owned-driver"),
        after = once("mode-post"),
        stopTarget = once("stop-target"),
        removeTarget = once("remove-owned-target"),
        rebuild = once("independent-http-rebuild");
    const steps = [
        targetStart,
        before,
        create,
        start,
        plan,
        run,
        artifacts,
        stopDriver,
        removeDriver,
        after,
        stopTarget,
        removeTarget,
        rebuild,
    ];
    need(
        steps.every((c, i) => !i || steps[i - 1].index < c.index),
        "KNOWN_LIFECYCLE_ORDER",
    );
    const startReply = commandData(targetStart);
    need(
        startReply.taskId === knownTaskId(arm.runId + "-target") &&
            startReply.planDigest === targetTask.digest &&
            startReply.reused !== true,
        "KNOWN_TARGET_SUBMISSION",
    );
    same(commandData(run), httpTerminal, "KNOWN_HTTP_TERMINAL_COMMAND");
    const targetStatuses = commands.filter(
        (c) =>
            isDeepStrictEqual(c.argv, ops["target-terminal"]) &&
            c.index > stopTarget.index &&
            c.index < removeTarget.index,
    );
    need(
        targetStatuses.some((c) => isDeepStrictEqual(commandData(c), targetTerminal)),
        "KNOWN_TARGET_TERMINAL_COMMAND",
    );
    const readyCommands = commands.filter(
        (c) => isDeepStrictEqual(c.argv, ops.ready) && c.index > targetStart.index && c.index < before.index,
    );
    need(
        readyCommands.some((c) => isDeepStrictEqual(commandData(c), ready)),
        "KNOWN_READY_COMMAND",
    );
    const inspect = (cid) => [spec.dockerExecutable, "inspect", "--type", "container", cid];
    const driverInspects = commands.filter(
        (c) =>
            isDeepStrictEqual(c.argv, inspect(driverCid)) || isDeepStrictEqual(c.argv, inspect(arm.runId + "-driver")),
    );
    let running = false,
        final = false,
        beforeRun = false;
    for (const c of driverInspects) {
        if (c.exitCode !== 0) continue;
        need(c.error === null, "KNOWN_DRIVER_INSPECT_ERROR");
        const values = JSON.parse(c.stdout);
        need(values.length === 1, "KNOWN_DRIVER_INSPECT");
        known002Driver(values[0], arm, sequence, spec, targetCid, driverCid);
        if (
            c.index > start.index &&
            c.index < plan.index &&
            values[0].State?.Running === true &&
            !values[0].State.OOMKilled
        )
            beforeRun = true;
        if (
            c.index > artifacts.index &&
            c.index < stopDriver.index &&
            values[0].State?.Running === true &&
            !values[0].State.OOMKilled
        )
            running = true;
        if (c.index > stopDriver.index && c.index < removeDriver.index && isDeepStrictEqual(values[0], driverFinal))
            final = true;
    }
    need(beforeRun && running && final, "KNOWN_DRIVER_RUNNING_AND_FINAL");
    let absenceEnd;
    for (const [cid, remove] of [
        [driverCid, removeDriver],
        [targetCid, removeTarget],
    ]) {
        const absent = commands.filter((c) => isDeepStrictEqual(c.argv, inspect(cid)) && c.index > remove.index);
        need(
            absent.length === 1 &&
                absent[0].exitCode !== 0 &&
                absent[0].error === null &&
                (absent[0].stdout.trim() === "" || /^\[\s*\]$/.test(absent[0].stdout.trim())) &&
                new RegExp(
                    "^Error(?: response from daemon)?: No such (?:object|container): " + cid + "\\s*$",
                    "i",
                ).test(absent[0].stderr.trim()),
            "KNOWN_EXACT_CID_ABSENCE",
        );
        if (cid === targetCid) absenceEnd = absent[0].finishedMonotonicNs;
    }
    const targetInspects = commands.filter(
        (c) =>
            isDeepStrictEqual(c.argv, inspect(targetCid)) && c.index > stopTarget.index && c.index < removeTarget.index,
    );
    need(
        targetInspects.some(
            (c) => c.error === null && c.exitCode === 0 && isDeepStrictEqual(JSON.parse(c.stdout), [targetFinal]),
        ),
        "KNOWN_TARGET_FINAL_COMMAND",
    );
    need(
        ns(before.startedMonotonicNs) <= proof.pre.window[0] &&
            proof.pre.window[1] <= ns(before.finishedMonotonicNs) &&
            ns(after.startedMonotonicNs) <= proof.post.window[0] &&
            proof.post.window[1] <= ns(after.finishedMonotonicNs),
        "KNOWN_MODE_COMMAND_WINDOW",
    );
    return {
        driverCid,
        targetCid,
        httpDigest,
        start: targetStart.startedMonotonicNs,
        end: absenceEnd,
        commandWindow: [targetStart.index, removeTarget.index],
    };
}

function unitArgv(unit) {
    return [
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
    ];
}
function verifyJobCommands(context, arms, commands, units) {
    const jobs = commands.filter((c) => c.argv[0] === "/usr/bin/systemctl"),
        used = new Set();
    const verify = (records) => {
        need(records.length === units.length, "KNOWN_JOB_PHASE_INCOMPLETE");
        const seen = new Set();
        for (const c of records) {
            const unit = c.argv[3];
            need(
                units.includes(unit) &&
                    !seen.has(unit) &&
                    !used.has(c.index) &&
                    isDeepStrictEqual(c.argv, unitArgv(unit)) &&
                    c.exitCode === 0 &&
                    c.error === null,
                "KNOWN_JOB_COMMAND",
            );
            const lines = c.stdout.split(/\r\n|[\r\n]/);
            if (lines.at(-1) === "") lines.pop();
            const fields = {};
            for (const line of lines) {
                const at = line.indexOf("=");
                need(at > 0 && !Object.hasOwn(fields, line.slice(0, at)), "KNOWN_JOB_FIELDS");
                fields[line.slice(0, at)] = line.slice(at + 1);
            }
            same(
                Object.keys(fields).sort(),
                ["Id", "LoadState", "ActiveState", "SubState", "MainPID", "ControlGroup", "Result"].sort(),
                "KNOWN_JOB_FIELDS",
            );
            need(fields.Id === unit && ["loaded", "not-found"].includes(fields.LoadState), "KNOWN_JOB_IDENTITY");
            need(
                unit === context.sequence.protectedObservedUnit ||
                    (fields.ActiveState === "inactive" &&
                        fields.SubState === "dead" &&
                        fields.MainPID === "0" &&
                        fields.ControlGroup === ""),
                "KNOWN_CONFLICTING_JOB_ACTIVE",
            );
            seen.add(unit);
            used.add(c.index);
        }
    };
    let cursor = -1;
    for (let i = 0; i < context.sequence.physicalArms.length; i++) {
        const planned = context.sequence.physicalArms[i],
            observed = arms.find((a) => a.runId === planned.runId);
        need(observed?.valid === true, "KNOWN_JOB_ARM_UNVERIFIED");
        const ops = known002Operations(planned, context.sequence, context.spec),
            starts = commands.filter((c) => isDeepStrictEqual(c.argv, ops["start-fresh-target"])),
            rebuilds = commands.filter((c) => isDeepStrictEqual(c.argv, ops["independent-http-rebuild"]));
        need(starts.length === 1 && rebuilds.length === 1, "KNOWN_JOB_LIFECYCLE_WINDOW");
        const start = starts[0].index,
            rebuild = rebuilds[0].index;
        need(cursor < start && start < rebuild, "KNOWN_JOB_LIFECYCLE_WINDOW");
        verify(jobs.filter((c) => cursor < c.index && c.index < start));
        const next = context.sequence.physicalArms[i + 1],
            nextStart = next
                ? commands.find((c) =>
                      isDeepStrictEqual(
                          c.argv,
                          known002Operations(next, context.sequence, context.spec)["start-fresh-target"],
                      ),
                  )?.index
                : Infinity;
        need(nextStart !== undefined && rebuild < nextStart, "KNOWN_JOB_LIFECYCLE_WINDOW");
        // Previous post and next pre share an idle gap. Consume post once, then require a separate next pre.
        const post = jobs.filter((c) => rebuild < c.index && c.index < nextStart).slice(0, units.length);
        verify(post);
        cursor = post.at(-1)?.index ?? rebuild;
    }
    need(used.size === jobs.length, "KNOWN_JOB_COMMAND_OUTSIDE_PHASE");
}
export function verifyKnown002CommandClosure(context, arms, commands, dispatch) {
    const allowed = [];
    for (const arm of arms) {
        if (!arm.valid) continue;
        const planned = context.sequence.physicalArms.find((v) => v.runId === arm.runId);
        const ops = known002Operations(planned, context.sequence, context.spec);
        const bindings = {
            "target.container.id": arm.targetCid,
            "driver.container.id": arm.driverCid,
            "target.ready.jcmdExecutable": "/opt/jdk/bin/jcmd",
            "target.ready.java.pid": arm.javaIdentity.pid,
            "http.task.id": knownTaskId(planned.requestId),
        };
        for (const [id, raw] of Object.entries(ops)) {
            if (["wait-original-http", "driver-inspect-before-cleanup"].includes(id)) continue;
            const argv = template(raw, bindings);
            if (id === "mode-post") argv[argv.indexOf("--total-timeout-seconds") + 1] = "30";
            allowed.push(argv);
        }
        for (const selector of [arm.targetCid, arm.driverCid, arm.runId + "-driver"])
            allowed.push([context.spec.dockerExecutable, "inspect", "--type", "container", selector]);
    }
    const units = [
        ...new Set([
            ...context.sequence.declaredHeavyUnits,
            ...context.sequence.additionalInactiveUnits,
            context.sequence.protectedObservedUnit,
        ]),
    ];
    for (const unit of units) allowed.push(unitArgv(unit));
    need(
        typeof dispatch.executionRoot === "string" &&
            dispatch.executionRoot.startsWith("/") &&
            !/[\r\n\0]/.test(dispatch.executionRoot),
        "KNOWN_DISPATCH_ROOT",
    );
    // The original operator runs this once after collecting its arms. Its saved analysis is not evidence for our result.
    const finalAnalysis = [
        context.spec.node,
        context.spec.sourceRoot + "/components/paired/analyzer.mjs",
        "--input",
        dispatch.executionRoot + "/verified-observations.json",
        "--output",
        dispatch.executionRoot + "/paired-analysis",
    ];
    const analysisCommands = commands.filter((c) => isDeepStrictEqual(c.argv, finalAnalysis));
    need(
        analysisCommands.length <= 1 && analysisCommands.every((c) => c.exitCode === 0 && c.error === null),
        "KNOWN_ANALYSIS_COMMAND",
    );
    allowed.push(finalAnalysis);
    need(
        commands.every((c) => allowed.some((argv) => isDeepStrictEqual(c.argv, argv))),
        "KNOWN_UNDECLARED_COMMAND",
    );
    verifyJobCommands(context, arms, commands, units);
}

async function verifyArm(plan, execution, context, arm, commands, dispatch) {
    const relative = "arms/" + arm.blockId + "-" + arm.arm,
        dir = join(plan, relative),
        raw = join(dir, "driver-evidence/http-plan/output");
    const report = await analyzeHttp(raw);
    const profile = await read(join(dir, "driver-evidence/profile.json"));
    same(report.profile, profile, "KNOWN_RAW_PROFILE");
    verifyKnown002Profile(
        profile,
        arm.arm,
        ["build", "environment", "settings", "source-verification"].map((n) => "/evidence/" + n + ".json"),
    );
    need(
        report.runs?.length === 1 &&
            report.gates.correctness === "passed" &&
            report.gates.sampleAccounting === "passed",
        "KNOWN_HTTP_GATES",
    );
    const run = report.runs[0];
    need(
        run.caseId === "arithmetic-control" &&
            run.loadId === "open-20" &&
            run.model === "open" &&
            run.repeat === 0 &&
            run.scheduled === 1200 &&
            run.correct >= 1000 &&
            run.correctEndToEndMs.count === run.correct &&
            run.failed === 0 &&
            run.dropped === 0 &&
            run.late === 0 &&
            run.issues.length === 0,
        "KNOWN_HTTP_QUALITY",
    );
    const implementation = await read(join(raw, "implementation.json"));
    need(implementation.nodeExecutableSha256 === context.spec.nodeExecutable.sha256, "KNOWN_ACTUAL_NODE");
    same(
        implementation.files.map((f) => f.file).sort(),
        [...KNOWN002.implementationFiles].sort(),
        "KNOWN_HTTP_IMPLEMENTATION_INVENTORY",
    );
    for (const f of implementation.files)
        need(
            f.sha256 === context.source.files.find((s) => s.path === "components/http/" + f.file)?.sha256,
            "KNOWN_HTTP_SOURCE_BINDING",
        );
    const httpTask = await read(join(dir, "driver-evidence/http-plan/task.json"));
    for (const file of KNOWN002.implementationFiles)
        need(
            httpTask.files?.find((f) => f.path === "/source/components/http/" + file)?.sha256 ===
                context.source.files.find((f) => f.path === "components/http/" + file)?.sha256,
            "KNOWN_TASK_SOURCE_BINDING",
        );
    need(
        httpTask.files?.find((f) => f.path === "/node/bin/node")?.sha256 === context.spec.nodeExecutable.sha256,
        "KNOWN_TASK_NODE_BINDING",
    );
    for (const file of ["profile.json", "target-evidence.json"]) {
        const planRaw = await bounded(join(dir, "driver-evidence/http-plan", file));
        need(
            hash(planRaw) === hash(await bounded(join(raw, file))) &&
                httpTask.files.find((f) => f.path === "/evidence/http-plan/" + file)?.sha256 === hash(planRaw),
            "KNOWN_TASK_INPUT_BINDING",
        );
    }
    const target = await read(join(raw, "target-evidence.json"));
    same(
        target.files.map((f) => ({ bytes: f.bytes, sha256: f.sha256 })),
        ["build", "environment", "settings", "source-verification"].map((n) => {
            const f = context.locked.get(n + ".json");
            return { bytes: f.bytes, sha256: f.sha256 };
        }),
        "KNOWN_TARGET_ATTACHMENTS",
    );
    const normalizedSettings = structuredClone(context.settings);
    delete normalizedSettings.mode;
    const proof = await verifyControlledEnvironment(
        join(dir, "environment"),
        report,
        { build: context.build, settings: normalizedSettings, environment: context.environment },
        "forward",
        dispatch,
    );
    proof.ready = await read(join(dir, "environment/work/ready.json"));
    const loadReceipt = (suffix) => read(join(execution, arm.runId + suffix + ".json"));
    const lifecycle = verifyKnown002Lifecycle({
        arm,
        context,
        proof,
        ready: await loadReceipt("-ready"),
        httpTerminal: await loadReceipt("-http-terminal"),
        targetTerminal: await loadReceipt("-target-terminal"),
        driverFinal: await loadReceipt("-driver-final"),
        targetFinal: await loadReceipt("-target-final"),
        environmentFinal: await read(join(dir, "environment/container-final.json")),
        httpTask,
        targetTask: await read(join(dir, "environment/task.json")),
        commands,
    });
    const rawManifestSha256 = hash(await bounded(join(raw, "raw.lock.json")));
    return {
        runId: arm.runId,
        blockId: arm.blockId,
        arm: arm.arm,
        valid: true,
        issues: [],
        rawManifestSha256,
        endToEndP99: run.correctEndToEndMs.p99,
        observations: run.correct,
        diagnostics: {
            responseP50: run.correctResponseMs.p50,
            responseP99: run.correctResponseMs.p99,
            endToEndP50: run.correctEndToEndMs.p50,
            endToEndP95: run.correctEndToEndMs.p95,
            goodput: run.correctCompletedInWindowPerSecond,
        },
        ...lifecycle,
        javaIdentity: {
            bootId: proof.pre.identity.bootId,
            pid: proof.pre.identity.pid,
            startTimeTicks: String(proof.pre.identity.startTimeTicks),
            containerId: proof.containerId,
        },
    };
}
const limitsText = [
    "Only the preregistered forward arithmetic sleep10 control; not the standard eleven cases, a throughput capacity result, or 1ms sensitivity.",
    "JVM observation and sustained resource collector are off; fine host/JVM/GC/JIT attribution is unavailable and is not a prerequisite for this scoped control.",
    "The fixed 529-member source selection is digest-bound; its full physical source/dependency tree and runtime provenance are not independently reread or attested.",
    "Original command and environment records are offline producer evidence, not an external execution attestation or current container-absence observation.",
    "Pre/run/post command order and endpoint JVM/container identities are checked; raw measurement timestamps are not independently mapped into the host command clock domain.",
];
async function worker(inputFile, output, maxBytes) {
    const inputRaw = await bounded(inputFile, MiB),
        input = validateKnownRegressionInput(JSON.parse(inputRaw));
    const plan = await realpath(resolve(dirname(inputFile), input.planDirectory)),
        execution = await realpath(resolve(dirname(inputFile), input.executionDirectory));
    const beforePlan = await inventory(plan, 8 * 1024 ** 3),
        beforeExecution = await inventory(execution, 8 * 1024 ** 3 - beforePlan.bytes);
    const context = await planContract(plan, execution, input.reference);
    const issues = [],
        arms = context.sequence.physicalArms.map((a) => ({
            runId: a.runId,
            blockId: a.blockId,
            arm: a.arm,
            valid: false,
            issues: ["KNOWN_ARM_NOT_VERIFIED"],
        }));
    let commands = [],
        dispatch;
    try {
        need(context.spec.runnable === true && context.sequence.mode === "prepare", "KNOWN_NOT_EXECUTED");
        dispatch = await read(join(execution, "dispatch.json"));
        need(
            dispatch.executed === true &&
                dispatch.executionSpecSha256 === input.reference.executionSpecSha256 &&
                /^[a-f0-9-]{36}$/.test(dispatch.hostBootId),
            "KNOWN_DISPATCH_IDENTITY",
        );
        same(await read(join(plan, "known-delay-dispatch.json")), dispatch, "KNOWN_PLAN_DISPATCH");
        commands = await commandsAt(execution);
        need(
            commands[0] && ns(dispatch.startedMonotonicNs) <= ns(commands[0].startedMonotonicNs),
            "KNOWN_DISPATCH_WINDOW",
        );
        for (let i = 0; i < arms.length; i++) {
            try {
                arms[i] = await verifyArm(
                    plan,
                    execution,
                    context,
                    context.sequence.physicalArms[i],
                    commands,
                    dispatch,
                );
            } catch (e) {
                arms[i].issues = [e.code ?? e.message];
            }
        }
        const seen = new Set();
        let end = 0n;
        for (const arm of arms.filter((a) => a.valid)) {
            for (const key of [
                "raw:" + arm.rawManifestSha256,
                "driver:" + arm.driverCid,
                "target:" + arm.targetCid,
                "java:" + arm.javaIdentity.bootId + ":" + arm.javaIdentity.pid + ":" + arm.javaIdentity.startTimeTicks,
            ]) {
                if (seen.has(key)) {
                    arm.valid = false;
                    arm.issues.push("KNOWN_PHYSICAL_IDENTITY_REUSED");
                    issues.push("KNOWN_PHYSICAL_IDENTITY_REUSED");
                }
                seen.add(key);
            }
            if (ns(arm.start) < end) {
                arm.valid = false;
                arm.issues.push("KNOWN_ARM_WINDOWS_OVERLAP");
            }
            end = ns(arm.end);
        }
        if (arms.every((a) => a.valid)) verifyKnown002CommandClosure(context, arms, commands, dispatch);
    } catch (e) {
        issues.push(e.code ?? e.message);
    }
    same(await inventory(plan, 8 * 1024 ** 3), beforePlan, "KNOWN_PLAN_CHANGED");
    same(await inventory(execution, 8 * 1024 ** 3 - beforePlan.bytes), beforeExecution, "KNOWN_EXECUTION_CHANGED");
    need(hash(await bounded(inputFile, MiB)) === hash(inputRaw), "KNOWN_INPUT_CHANGED");
    const summary = summarizeKnown002(context.policy, arms, issues);
    const report = {
        schemaVersion: 1,
        kind: "http-known-regression-evidence",
        gateId: "known-regression-detection",
        status: summary.status,
        operationVersion: KNOWN002.operationVersion,
        scope: "forward-arithmetic-sleep10-end-to-end-p99-positive-control",
        plannedArms: 16,
        plannedPairs: 8,
        verifiedArms: arms.filter((a) => a.valid).length,
        verification: { valid: summary.valid, issues, inputClosureStable: true },
        detection: summary.detection,
        arms,
        analysis: summary.analysis,
        negativeControl: {
            metric: "correctCompletedInWindowPerSecond",
            absoluteRunDifferenceBudget: 1,
            scope: "descriptive low-rate count control, not an inference gate or throughput-capacity result",
            pairs: KNOWN002.blocks.map((id) => {
                const a = arms.find((v) => v.blockId === id && v.arm === "A"),
                    b = arms.find((v) => v.blockId === id && v.arm === "B");
                const difference = a.valid && b.valid ? b.diagnostics.goodput - a.diagnostics.goodput : null;
                return {
                    id,
                    difference,
                    withinDeclaredDiagnosticBudget: difference === null ? null : Math.abs(difference) <= 1,
                };
            }),
        },
        references: {
            inputSha256: hash(inputRaw),
            ...input.reference,
            planInventorySha256: valueHash(beforePlan.files),
            executionInventorySha256: valueHash(beforeExecution.files),
        },
        provenance: {
            sourceSelectionMembersBound: 529,
            fullSourceBytesReverified: false,
            installedDependenciesAttested: false,
            readerFiles: await Promise.all(
                [
                    "known-regression-evidence.mjs",
                    "known002-contract.mjs",
                    "analyzer.mjs",
                    "paired-evidence.mjs",
                    "../paired/core.mjs",
                ].map(async (p) => ({ path: p, sha256: hash(await bounded(join(dirname(SELF), p))) })),
            ),
        },
        limits: limitsText,
        inferenceAccepted: false,
        performanceAccepted: false,
    };
    await save(join(output, "worker-result.json"), report, Math.min(16 * MiB, maxBytes - 65536));
}

async function runWorker(inputFile, output, milliseconds, maxBytes, signal) {
    return new Promise((done) => {
        const child = spawn(process.execPath, [SELF, "--worker", inputFile, output, String(maxBytes)], {
            detached: true,
            stdio: ["ignore", "pipe", "pipe"],
            env: { PATH: process.env.PATH || "/usr/bin:/bin" },
        });
        let size = 0,
            failure = null,
            killer,
            stopped = false;
        const chunks = [];
        const group = (s) => {
            if (child.pid)
                try {
                    process.kill(-child.pid, s);
                } catch (e) {
                    if (e.code !== "ESRCH") failure ||= "KNOWN_GROUP_SIGNAL";
                }
        };
        const stop = (reason) => {
            failure ||= reason;
            if (!stopped) {
                stopped = true;
                group("SIGTERM");
                killer = setTimeout(() => group("SIGKILL"), 1000);
            }
        };
        const abort = () => stop("KNOWN_CANCELLED");
        const timer = setTimeout(() => stop("KNOWN_DEADLINE"), milliseconds);
        signal.addEventListener("abort", abort, { once: true });
        for (const stream of [child.stdout, child.stderr])
            stream.on("data", (c) => {
                size += c.length;
                if (size > 65536) stop("KNOWN_WORKER_OUTPUT_LIMIT");
                else chunks.push(c);
            });
        child.once("error", () => {
            failure ||= "KNOWN_WORKER_SPAWN";
        });
        child.once("close", (code, sig) => {
            clearTimeout(timer);
            clearTimeout(killer);
            signal.removeEventListener("abort", abort);
            group("SIGKILL");
            done({ code, signal: sig, failure, output: Buffer.concat(chunks).toString("utf8") });
        });
        if (signal.aborted) abort();
    });
}
export async function reportKnownRegression({
    inputFile,
    outputDirectory,
    timeoutMs,
    maxBytes,
    signal: externalSignal,
}) {
    limits(timeoutMs, maxBytes);
    need(
        [inputFile, outputDirectory].every(
            (p) => typeof p === "string" && p.length > 0 && p.length <= 4096 && !/[\r\n\0]/.test(p),
        ),
        "KNOWN_INPUT_PATH",
    );
    const begun = performance.now(),
        original = await bounded(inputFile, MiB),
        input = validateKnownRegressionInput(JSON.parse(original));
    const inputPath = await realpath(inputFile),
        plan = await realpath(resolve(dirname(inputPath), input.planDirectory)),
        execution = await realpath(resolve(dirname(inputPath), input.executionDirectory)),
        output = await canonical(outputDirectory);
    const originalSpec = await read(join(execution, "execution-spec.json"));
    need(
        typeof originalSpec.sourceRoot === "string" &&
            originalSpec.sourceRoot.startsWith("/") &&
            originalSpec.sourceRoot.length <= 4096 &&
            !/[\r\n\0]/.test(originalSpec.sourceRoot),
        "KNOWN_SOURCE_PATH",
    );
    const sourcePaths = await Promise.all(
        [
            "components",
            "src",
            "dist",
            "node_modules",
            "benchmarks",
            "presets",
            "release",
            "tools",
            "examples",
            "docs",
            "tests-normative",
            "adapters",
            "package.json",
            "package-lock.json",
        ].map((p) => canonical(join(ROOT, p))),
    );
    need(!inside(plan, execution) && !inside(execution, plan), "KNOWN_INPUT_ROOT_OVERLAP");
    const protectedPaths = [inputPath, plan, execution, await canonical(originalSpec.sourceRoot), ...sourcePaths];
    need(
        protectedPaths.every((p) => !inside(output, p) && !inside(p, output)),
        "KNOWN_OUTPUT_OVERLAP",
    );
    await mkdir(output, { mode: 0o700 });
    const cancellation = new AbortController(),
        interrupt = () => cancellation.abort(),
        signal = cancellation.signal;
    externalSignal?.addEventListener("abort", interrupt, { once: true });
    process.on("SIGTERM", interrupt);
    process.on("SIGINT", interrupt);
    if (externalSignal?.aborted) interrupt();
    try {
        // Reserve worker group cleanup and writing a partial receipt inside the same overall budget.
        const result = await runWorker(
            inputPath,
            output,
            Math.max(1, timeoutMs - 1200 - (performance.now() - begun)),
            maxBytes,
            signal,
        );
        let workerCode;
        try {
            workerCode = JSON.parse(result.output).error?.code;
        } catch {
            /* Bounded diagnostics may be incomplete after cancellation. */
        }
        need(
            result.code === 0 && !result.failure && !signal.aborted && performance.now() - begun < timeoutMs,
            result.failure ?? (signal.aborted ? "KNOWN_CANCELLED" : (workerCode ?? "KNOWN_WORKER_FAILED")),
        );
        const report = await read(join(output, "worker-result.json"));
        need(report.references.inputSha256 === hash(original), "KNOWN_INPUT_CHANGED");
        checkKnownRegressionDeadline(signal, begun, timeoutMs);
        const snapshot = { ...input, planDirectory: plan, executionDirectory: execution };
        // Do not hold two physical copies of the potentially capped report at once.
        await rm(join(output, "worker-result.json"));
        const files = [
            await save(join(output, "input.json"), snapshot, maxBytes),
            await save(join(output, "report.json"), report, maxBytes),
        ];
        need(files.reduce((n, f) => n + f.bytes, 0) + 65536 <= maxBytes, "KNOWN_OUTPUT_BYTE_LIMIT");
        checkKnownRegressionDeadline(signal, begun, timeoutMs);
        await save(
            join(output, "report.lock.json"),
            { schemaVersion: 1, kind: "http-known-regression-evidence-lock", contentDigest: valueHash(files), files },
            maxBytes - files.reduce((n, f) => n + f.bytes, 0),
        );
        checkKnownRegressionDeadline(signal, begun, timeoutMs);
        return {
            kind: "http-known-regression-evidence",
            directory: output,
            reportJson: join(output, "report.json"),
            reportLock: join(output, "report.lock.json"),
            gateId: "known-regression-detection",
            status: report.status,
            performanceAccepted: false,
            inferenceAccepted: false,
        };
    } catch (e) {
        await rm(join(output, "worker-result.json"), { force: true });
        await rm(join(output, "report.lock.json"), { force: true });
        await save(
            join(output, "partial.json"),
            {
                schemaVersion: 1,
                kind: "http-known-regression-evidence-partial",
                gateId: "known-regression-detection",
                status: "unknown",
                verification: { valid: false, issues: [e.code ?? e.message] },
                plannedArms: 16,
                plannedPairs: 8,
                arms: KNOWN002.blocks.flatMap((blockId) =>
                    ["A", "B"].map((arm) => ({
                        blockId,
                        arm,
                        valid: false,
                        issues: ["KNOWN_VERIFICATION_INCOMPLETE"],
                    })),
                ),
                references: input.reference,
                performanceAccepted: false,
                inferenceAccepted: false,
            },
            maxBytes,
        ).catch(() => {});
        throw e;
    } finally {
        process.removeListener("SIGTERM", interrupt);
        process.removeListener("SIGINT", interrupt);
        externalSignal?.removeEventListener("abort", interrupt);
    }
}

async function main() {
    const argv = process.argv.slice(2);
    if (argv[0] === "--worker") {
        need(argv.length === 4, "KNOWN_WORKER_ARGUMENTS");
        await worker(argv[1], argv[2], Number(argv[3]));
        return;
    }
    if (argv.length === 1 && argv[0] === "--help") {
        process.stdout.write(
            "Offline known002 evidence: --input INPUT --output NEW --timeout-ms N --max-bytes N\n--max-bytes caps output (1MiB..8GiB); combined input inventory is fixed at 8GiB.\nNo Docker/HTTP/SSH or execution of supplied commands. Unknown/failed reports exit 0; inspect gate status.\n",
        );
        return;
    }
    const args = {};
    for (let i = 0; i < argv.length; i += 2) {
        need(
            ["--input", "--output", "--timeout-ms", "--max-bytes"].includes(argv[i]) && argv[i + 1] && !args[argv[i]],
            "KNOWN_ARGUMENTS",
        );
        args[argv[i]] = argv[i + 1];
    }
    need(Object.keys(args).length === 4, "KNOWN_ARGUMENTS");
    const data = await reportKnownRegression({
        inputFile: args["--input"],
        outputDirectory: args["--output"],
        timeoutMs: Number(args["--timeout-ms"]),
        maxBytes: Number(args["--max-bytes"]),
    });
    process.stdout.write(JSON.stringify({ ok: true, data }) + "\n");
}
if (process.argv[1] && pathToFileURL(resolve(process.argv[1])).href === import.meta.url)
    main().catch((e) => {
        process.stderr.write(
            JSON.stringify({ ok: false, error: { code: e.code ?? "KNOWN_EVIDENCE_ERROR", message: e.message } }) + "\n",
        );
        process.exitCode = 6;
    });
