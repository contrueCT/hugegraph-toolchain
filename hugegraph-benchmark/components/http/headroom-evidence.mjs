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
import { analyzePhaseWindows } from "./phase-windows.mjs";
import { summarizeCompletionWindows } from "./completion-windows.mjs";
import { verifyControlledEnvironment } from "./paired-evidence.mjs";
import { verifyResourceEvidence } from "./resource-evidence.mjs";
import { verifyJvmEvidence } from "./jvm-evidence.mjs";
import { valueHash } from "../paired/core.mjs";
import {
    HEADROOM002,
    needHeadroom as need,
    sameHeadroom as same,
    headroomSha as hash,
    headroomNs as ns,
    headroomTaskId,
    headroomGrid,
    headroomDeclaredUnits,
    verifyHeadroomProfile,
    verifyHeadroomTask,
    verifyHeadroomDriver,
    headroom002Operations,
    assessHeadroomRun,
    assessHeadroomCells,
} from "./headroom002-contract.mjs";
const SELF = fileURLToPath(import.meta.url),
    ROOT = resolve(dirname(SELF), "../..");
const MiB = 1024 ** 2,
    INPUT_CAP = 16 * 1024 ** 3,
    SHA = /^[a-f0-9]{64}$/;
const encode = (v) => Buffer.from(JSON.stringify(v, null, 2) + "\n");
const exact = (v, keys) => same(Object.keys(v ?? {}).sort(), [...keys].sort(), "HEADROOM_FIELDS");
const inside = (a, b) => a === b || a.startsWith(b + sep);
async function bounded(path, cap = 16 * MiB) {
    const fd = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
    try {
        const a = await fd.stat();
        need(a.isFile() && a.size <= cap, "HEADROOM_INPUT_TYPE_OR_BUDGET");
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
            "HEADROOM_INPUT_CHANGED",
        );
        return raw.subarray(0, n);
    } finally {
        await fd.close();
    }
}
const read = async (path, cap) => JSON.parse(new TextDecoder("utf8", { fatal: true }).decode(await bounded(path, cap)));
const save = async (path, value, cap) => {
    const raw = encode(value);
    need(raw.length <= cap, "HEADROOM_OUTPUT_BYTE_LIMIT");
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
        need(p !== parent, "HEADROOM_OUTPUT_PATH");
        p = parent;
    }
}
function limits(timeoutMs, maxBytes) {
    need(Number.isSafeInteger(timeoutMs) && timeoutMs >= 1000 && timeoutMs <= 86400000, "HEADROOM_TIME_LIMIT");
    need(Number.isSafeInteger(maxBytes) && maxBytes >= MiB && maxBytes <= 8 * 1024 ** 3, "HEADROOM_BYTE_LIMIT");
}
async function inventory(root, cap) {
    const files = [];
    let total = 0,
        entries = 0;
    async function walk(relative, depth) {
        need(depth <= 20, "HEADROOM_INPUT_DEPTH");
        for (const name of (await readdir(join(root, relative))).sort()) {
            need(++entries <= 32768, "HEADROOM_INPUT_MEMBERS");
            const p = relative ? relative + "/" + name : name;
            const full = join(root, p),
                s = await lstat(full);
            if (s.isDirectory()) {
                await walk(p, depth + 1);
                continue;
            }
            need(s.isFile() && s.size <= 256 * MiB && total + s.size <= cap, "HEADROOM_INPUT_TYPE_OR_BUDGET");
            total += s.size;
            const fd = await open(full, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
            let count = 0;
            const digest = createHash("sha256");
            try {
                const a = await fd.stat();
                need(a.isFile() && a.size === s.size, "HEADROOM_INPUT_CHANGED");
                const buffer = Buffer.alloc(64 * 1024);
                while (true) {
                    const r = await fd.read(buffer, 0, buffer.length, count);
                    if (!r.bytesRead) break;
                    count += r.bytesRead;
                    need(count <= a.size, "HEADROOM_INPUT_CHANGED");
                    digest.update(buffer.subarray(0, r.bytesRead));
                }
                const b = await fd.stat();
                need(
                    count === a.size && b.size === a.size && a.mtimeMs === b.mtimeMs && a.ctimeMs === b.ctimeMs,
                    "HEADROOM_INPUT_CHANGED",
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
            need(bindings[key] !== undefined, "HEADROOM_LIVE_BINDING");
            return String(bindings[key]);
        }),
    );
}
function commandData(command) {
    need(command.exitCode === 0 && command.error === null, "HEADROOM_COMMAND_FAILED");
    const value = JSON.parse(command.stdout);
    need(value.ok === true && value.data && typeof value.data === "object", "HEADROOM_COMMAND_RESPONSE");
    return value.data;
}
async function commandsAt(execution) {
    const names = (await readdir(join(execution, "commands"))).sort();
    need(names.length > 0 && names.length <= 8192, "HEADROOM_COMMAND_INVENTORY");
    const commands = [];
    let last = 0n;
    for (let i = 0; i < names.length; i++) {
        need(names[i] === String(i + 1).padStart(5, "0") + ".json", "HEADROOM_COMMAND_INVENTORY");
        const c = await read(join(execution, "commands", names[i]), 2 * MiB);
        need(
            Array.isArray(c.argv) &&
                c.argv.length <= 256 &&
                c.argv.every((v) => typeof v === "string" && v.length <= 8192) &&
                typeof c.stdout === "string" &&
                typeof c.stderr === "string" &&
                Buffer.byteLength(c.stdout) + Buffer.byteLength(c.stderr) <= 2 * MiB &&
                Number.isInteger(c.exitCode),
            "HEADROOM_COMMAND_RECORD",
        );
        const start = ns(c.startedMonotonicNs),
            end = ns(c.finishedMonotonicNs);
        need(last <= start && start < end, "HEADROOM_COMMAND_ORDER");
        last = end;
        commands.push({ ...c, index: i });
    }
    return commands;
}
export function verifyHeadroomMounts(receipt, sealed) {
    const ordered = (mounts) => {
        need(Array.isArray(mounts), "HEADROOM_TARGET_FINAL_MOUNT_IDENTITY");
        const destinations = new Set();
        for (const mount of mounts) {
            need(
                mount !== null &&
                    typeof mount === "object" &&
                    !Array.isArray(mount) &&
                    typeof mount.Destination === "string" &&
                    mount.Destination.length > 0 &&
                    !destinations.has(mount.Destination),
                "HEADROOM_TARGET_FINAL_MOUNT_IDENTITY",
            );
            destinations.add(mount.Destination);
        }
        return [...mounts].sort((a, b) => (a.Destination < b.Destination ? -1 : a.Destination > b.Destination ? 1 : 0));
    };
    same(ordered(receipt), ordered(sealed), "HEADROOM_TARGET_FINAL_ENVIRONMENT");
}
export function verifyHeadroomLifecycle({
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
    commands,
}) {
    const { sequence, spec } = context,
        targetCid = proof.containerId;
    same(ready.container.id, targetCid, "HEADROOM_READY_CID");
    same(ready.ready, proof.ready, "HEADROOM_READY_RECEIPT");
    const driverCid = verifyHeadroomDriver(driverFinal, arm, sequence, spec, targetCid);
    same(targetFinal.Id, targetCid, "HEADROOM_TARGET_FINAL_ID");
    need(
        targetFinal.State?.Running === false &&
            targetFinal.State?.OOMKilled === false &&
            targetFinal.State?.Status === "exited" &&
            targetFinal.State?.ExitCode === 0,
        "HEADROOM_TARGET_STOP",
    );
    for (const key of ["Id", "Name", "Image", "Config", "HostConfig", "State"])
        same(targetFinal[key], proof.containerFinal?.[key], "HEADROOM_TARGET_FINAL_ENVIRONMENT");
    verifyHeadroomMounts(targetFinal.Mounts, proof.containerFinal?.Mounts);
    need(
        driverFinal.State?.Running === false &&
            driverFinal.State?.OOMKilled === false &&
            driverFinal.State?.Status === "exited" &&
            [0, 143, -15].includes(driverFinal.State?.ExitCode),
        "HEADROOM_DRIVER_STOP",
    );
    const httpDigest = verifyHeadroomTask(httpTask, httpTerminal, arm.requestId);
    verifyHeadroomTask(targetTask, targetTerminal, arm.runId + "-target");
    need(
        httpTask.plan.timeoutMs === 609000 &&
            httpTask.plan.terminateGraceMs === 5000 &&
            httpTask.plan.command?.[0] === "/node/bin/node" &&
            httpTask.plan.command?.[1] === "/source/components/http/workflow.mjs" &&
            httpTask.plan.command?.[2] === "--plan" &&
            httpTask.plan.command?.[3] === "/evidence/http-plan",
        "HEADROOM_HTTP_TASK_COMMAND",
    );
    const bindings = {
        "target.container.id": targetCid,
        "driver.container.id": driverCid,
        "target.ready.jcmdExecutable": ready.ready.jcmdExecutable,
        "target.ready.java.pid": ready.ready.java.pid,
        "http.task.id": headroomTaskId(arm.requestId),
        "collector.task.id": headroomTaskId(arm.collectorRequestId),
    };
    const ops = Object.fromEntries(
        Object.entries(headroom002Operations(arm, sequence, spec)).map(([k, argv]) => [k, template(argv, bindings)]),
    );

    const once = (id) => {
        const records = commands.filter((c) => isDeepStrictEqual(c.argv, ops[id]));
        need(
            records.length === 1 && records[0].exitCode === 0 && records[0].error === null,
            "HEADROOM_COMMAND_REQUIRED:" + id,
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
        "HEADROOM_LIFECYCLE_ORDER",
    );
    const startReply = commandData(targetStart);
    need(
        startReply.taskId === headroomTaskId(arm.runId + "-target") &&
            startReply.planDigest === targetTask.digest &&
            startReply.reused !== true,
        "HEADROOM_TARGET_SUBMISSION",
    );
    same(commandData(run), httpTerminal, "HEADROOM_HTTP_TERMINAL_COMMAND");
    const targetStatuses = commands.filter(
        (c) =>
            isDeepStrictEqual(c.argv, ops["target-terminal"]) &&
            c.index > stopTarget.index &&
            c.index < removeTarget.index,
    );
    need(
        targetStatuses.some((c) => isDeepStrictEqual(commandData(c), targetTerminal)),
        "HEADROOM_TARGET_TERMINAL_COMMAND",
    );
    const readyCommands = commands.filter(
        (c) => isDeepStrictEqual(c.argv, ops.ready) && c.index > targetStart.index && c.index < before.index,
    );
    need(
        readyCommands.some((c) => isDeepStrictEqual(commandData(c), ready)),
        "HEADROOM_READY_COMMAND",
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
        need(c.error === null, "HEADROOM_DRIVER_INSPECT_ERROR");
        const values = JSON.parse(c.stdout);
        need(values.length === 1, "HEADROOM_DRIVER_INSPECT");
        verifyHeadroomDriver(values[0], arm, sequence, spec, targetCid, driverCid);
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
    need(beforeRun && running && final, "HEADROOM_DRIVER_RUNNING_AND_FINAL");
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
            "HEADROOM_EXACT_CID_ABSENCE",
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
        "HEADROOM_TARGET_FINAL_COMMAND",
    );
    need(
        ns(before.startedMonotonicNs) <= proof.pre.window[0] &&
            proof.pre.window[1] <= ns(before.finishedMonotonicNs) &&
            ns(after.startedMonotonicNs) <= proof.post.window[0] &&
            proof.post.window[1] <= ns(after.finishedMonotonicNs),
        "HEADROOM_MODE_COMMAND_WINDOW",
    );
    return {
        commandHandles: { run, artifacts, stopDriver, removeDriver, after, removeTarget, rebuild, create, start, plan },
        operations: ops,
        driverCid,
        targetCid,
        httpDigest,
        start: targetStart.startedMonotonicNs,
        end: absenceEnd,
        commandWindow: [targetStart.index, removeTarget.index],
    };
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
                    if (e.code !== "ESRCH") failure ||= "HEADROOM_GROUP_SIGNAL";
                }
        };
        const stop = (reason) => {
            failure ||= reason;
            if (!stopped) {
                stopped = true;
                group("SIGTERM");
                killer = setTimeout(() => group("SIGKILL"), 3000);
            }
        };
        const abort = () => stop("HEADROOM_CANCELLED");
        const timer = setTimeout(() => stop("HEADROOM_DEADLINE"), milliseconds);
        signal.addEventListener("abort", abort, { once: true });
        for (const stream of [child.stdout, child.stderr])
            stream.on("data", (c) => {
                size += c.length;
                if (size > 65536) stop("HEADROOM_WORKER_OUTPUT_LIMIT");
                else chunks.push(c);
            });
        child.once("error", () => {
            failure ||= "HEADROOM_WORKER_SPAWN";
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

export function validateHeadroomInput(value) {
    exact(value, [
        "schemaVersion",
        "kind",
        "operationVersion",
        "planDirectory",
        "executionDirectory",
        "assessmentBindingFile",
        "preDispatchLaunchRecordFile",
        "controllerStartEvidenceFile",
        "reference",
    ]);
    exact(value.reference, [
        "planLockSha256",
        "executionSpecSha256",
        "sourceSelectionSha256",
        "assessmentCriteriaSha256",
        "assessmentBindingSha256",
        "preDispatchLaunchRecordSha256",
        "controllerStartEvidenceSha256",
    ]);
    need(
        value.schemaVersion === 1 &&
            value.kind === "http-driver-headroom-evidence-input" &&
            value.operationVersion === HEADROOM002.operationVersion,
        "HEADROOM_INPUT",
    );
    for (const key of [
        "planDirectory",
        "executionDirectory",
        "assessmentBindingFile",
        "preDispatchLaunchRecordFile",
        "controllerStartEvidenceFile",
    ])
        need(
            typeof value[key] === "string" &&
                value[key].length > 0 &&
                value[key].length <= 4096 &&
                !/[\r\n\0]/.test(value[key]),
            "HEADROOM_INPUT_PATH",
        );
    need(
        Object.values(value.reference).every((v) => typeof v === "string" && SHA.test(v)) &&
            value.reference.sourceSelectionSha256 === HEADROOM002.sourceSelectionSha256 &&
            value.reference.assessmentCriteriaSha256 === HEADROOM002.criteriaSha256,
        "HEADROOM_REFERENCE",
    );
    return value;
}
function procStat(raw, pid) {
    need(typeof raw === "string" && raw.startsWith(String(pid) + " (") && raw.includes(")"), "HEADROOM_PROC_STAT");
    const fields = raw
        .slice(raw.lastIndexOf(")") + 1)
        .trim()
        .split(/\s+/);
    need(
        fields.length >= 20 && !["Z", "X", "x"].includes(fields[0]) && /^[0-9]+$/.test(fields[19]),
        "HEADROOM_PROC_IDENTITY",
    );
    return { startTicks: fields[19] };
}
export function verifyHeadroomPreregistration(binding, launch, controller, dispatch, context, bindingSha) {
    exact(binding, [
        "schemaVersion",
        "kind",
        "criteriaSha256",
        "planLockSha256",
        "executionSpecSha256",
        "sourceSelectionSha256",
        "preparedPlanDirectory",
        "executionDirectory",
        "preDispatchLaunchRecordReference",
    ]);
    exact(binding.preDispatchLaunchRecordReference, ["path"]);
    need(
        binding.schemaVersion === 1 &&
            binding.kind === "http-headroom-engineering-assessment-binding" &&
            binding.criteriaSha256 === HEADROOM002.criteriaSha256 &&
            binding.planLockSha256 === context.spec.planLockSha256 &&
            binding.executionSpecSha256 === dispatch.executionSpecSha256 &&
            binding.sourceSelectionSha256 === HEADROOM002.sourceSelectionSha256 &&
            binding.preparedPlanDirectory === context.spec.planRoot &&
            binding.executionDirectory === dispatch.executionRoot,
        "HEADROOM_BINDING",
    );
    need(
        typeof binding.preDispatchLaunchRecordReference.path === "string" &&
            /^[A-Za-z0-9_.-]{1,240}$/.test(binding.preDispatchLaunchRecordReference.path) &&
            ![".", ".."].includes(binding.preDispatchLaunchRecordReference.path),
        "HEADROOM_BINDING_LAUNCH_PATH",
    );
    exact(launch, [
        "schemaVersion",
        "kind",
        "operationVersion",
        "bindingSha256",
        "planLockSha256",
        "executionSpecSha256",
        "sourceSelectionSha256",
        "hostBootId",
        "startedMonotonicNs",
        "finishedMonotonicNs",
        "argv",
    ]);
    need(
        launch.schemaVersion === 1 &&
            launch.kind === "http-headroom-engineering-launch" &&
            launch.operationVersion === HEADROOM002.operationVersion &&
            launch.bindingSha256 === bindingSha &&
            launch.planLockSha256 === binding.planLockSha256 &&
            launch.executionSpecSha256 === binding.executionSpecSha256 &&
            launch.sourceSelectionSha256 === binding.sourceSelectionSha256 &&
            launch.hostBootId === dispatch.hostBootId,
        "HEADROOM_LAUNCH_BINDING",
    );
    need(
        Array.isArray(launch.argv) &&
            launch.argv.length === 6 &&
            launch.argv[0] === context.runtime.hostPython &&
            launch.argv[1] === "-B" &&
            typeof launch.argv[2] === "string" &&
            launch.argv[2].startsWith("/") &&
            launch.argv[2].endsWith("/http-driver-headroom-002-execute.py") &&
            launch.argv[3] === "run" &&
            launch.argv[4] === "--execution" &&
            launch.argv[5] === dispatch.executionRoot,
        "HEADROOM_LAUNCH_COMMAND",
    );
    exact(controller, ["schemaVersion", "kind", "hostBootId", "observedMonotonicNs", "systemd", "procStatRaw"]);
    exact(controller.systemd, ["argv", "stdout", "exitCode"]);
    need(
        controller.schemaVersion === 1 &&
            controller.kind === "http-headroom-controller-start-evidence" &&
            controller.hostBootId === dispatch.hostBootId &&
            controller.systemd.exitCode === 0,
        "HEADROOM_CONTROLLER_START",
    );
    const argv = controller.systemd.argv,
        properties = ["MainPID", "ExecMainPID", "ExecMainStartTimestampMonotonic", "ControlGroup", "ActiveState"];
    need(
        Array.isArray(argv) &&
            argv.length === 14 &&
            isDeepStrictEqual(argv.slice(0, 4), [
                "/usr/bin/systemctl",
                "--user",
                "show",
                "hgbench-http-driver-headroom-002.service",
            ]),
        "HEADROOM_SYSTEMD_COMMAND",
    );
    const requested = [];
    for (let i = 4; i < argv.length; i += 2) {
        need(argv[i] === "-p", "HEADROOM_SYSTEMD_COMMAND");
        requested.push(argv[i + 1]);
    }
    same(requested.sort(), [...properties].sort(), "HEADROOM_SYSTEMD_PROPERTIES");
    const fields = {};
    for (const line of controller.systemd.stdout.trimEnd().split("\n")) {
        const at = line.indexOf("=");
        need(at > 0 && !Object.hasOwn(fields, line.slice(0, at)), "HEADROOM_SYSTEMD_FIELDS");
        fields[line.slice(0, at)] = line.slice(at + 1);
    }
    exact(fields, properties);
    need(
        fields.MainPID === String(dispatch.pid) &&
            fields.ExecMainPID === String(dispatch.pid) &&
            Number.isSafeInteger(dispatch.pid) &&
            dispatch.pid > 0 &&
            typeof fields.ControlGroup === "string" &&
            fields.ControlGroup.startsWith("/") &&
            fields.ControlGroup.length > 1 &&
            ["active", "activating"].includes(fields.ActiveState) &&
            /^[1-9][0-9]*$/.test(fields.ExecMainStartTimestampMonotonic),
        "HEADROOM_SYSTEMD_IDENTITY",
    );
    need(procStat(controller.procStatRaw, dispatch.pid).startTicks === dispatch.startTicks, "HEADROOM_CONTROLLER_PROC");
    const started = BigInt(fields.ExecMainStartTimestampMonotonic) * 1000n;
    need(
        ns(launch.startedMonotonicNs) <= ns(launch.finishedMonotonicNs) &&
            ns(launch.finishedMonotonicNs) < started &&
            ns(controller.observedMonotonicNs) >= started,
        "HEADROOM_REGISTRATION_TOO_LATE",
    );
    return {
        verified: true,
        controllerPid: dispatch.pid,
        controllerStartMonotonicNs: String(started),
        hostBootId: dispatch.hostBootId,
        source: "recorded-systemd-and-procfs-producer-evidence",
        externalAttestation: false,
    };
}
async function headroomPlan(plan, execution, reference) {
    const lockRaw = await bounded(join(plan, "plan.lock.json")),
        lock = JSON.parse(lockRaw);
    need(
        hash(lockRaw) === reference.planLockSha256 &&
            lock.kind === "http-driver-headroom-operation-lock" &&
            lock.generatorVersion === 3 &&
            lock.generator?.sha256 === HEADROOM002.generatorSha256 &&
            lock.fixedPlanningHelper?.sha256 === HEADROOM002.fixedPlanningHelperSha256 &&
            Array.isArray(lock.files) &&
            lock.files.length <= 20000 &&
            lock.contentDigest === valueHash(lock.files),
        "HEADROOM_PLAN_LOCK",
    );
    const locked = new Map();
    let bytes = 0;
    for (const f of lock.files) {
        need(
            typeof f.path === "string" &&
                /^[A-Za-z0-9_./-]{1,512}$/.test(f.path) &&
                !f.path.startsWith("/") &&
                !f.path.split("/").some((x) => ["", ".", ".."].includes(x)) &&
                !locked.has(f.path) &&
                Number.isSafeInteger(f.bytes) &&
                f.bytes >= 0 &&
                SHA.test(f.sha256),
            "HEADROOM_PLAN_MEMBER",
        );
        bytes += f.bytes;
        need(bytes <= 256 * MiB, "HEADROOM_PLAN_BYTE_LIMIT");
        const raw = await bounded(join(plan, f.path));
        need(raw.length === f.bytes && hash(raw) === f.sha256, "HEADROOM_PLAN_MEMBER_CHANGED");
        locked.set(f.path, f);
    }
    const load = async (p) => {
        need(locked.has(p), "HEADROOM_REQUIRED_PLAN_MEMBER");
        return read(join(plan, p));
    };
    const sourceRaw = await bounded(join(plan, "source-selection.lock.json"));
    need(hash(sourceRaw) === reference.sourceSelectionSha256, "HEADROOM_SOURCE_SELECTION");
    const source = JSON.parse(sourceRaw);
    need(
        source.files?.length === 529 && new Set(source.files.map((f) => f.path)).size === 529,
        "HEADROOM_SOURCE_MEMBERS",
    );
    const sequence = await load("sequence.json"),
        runtime = await load("runtime-selection.json"),
        settings = await load("settings.json"),
        build = await load("build.json"),
        environment = await load("environment.json");
    for (const [name, key] of [
        ["settings.json", "settingsSha256"],
        ["budgets.json", "budgetsSha256"],
        ["assessment-contract.json", "assessmentContractSha256"],
    ])
        need(locked.get(name)?.sha256 === HEADROOM002[key], "HEADROOM_FIXED_CONTRACT");
    const specRaw = await bounded(join(execution, "execution-spec.json")),
        spec = JSON.parse(specRaw),
        executionLock = await read(join(execution, "execution.lock.json"));
    need(
        hash(specRaw) === reference.executionSpecSha256 &&
            executionLock.kind === "http-driver-headroom-execution-lock" &&
            executionLock.specSha256 === reference.executionSpecSha256 &&
            spec.schemaVersion === 1 &&
            spec.kind === "http-driver-headroom-execution-spec" &&
            spec.planLockSha256 === reference.planLockSha256 &&
            spec.generatorSha256 === HEADROOM002.generatorSha256 &&
            spec.executorSha256 === HEADROOM002.executorSha256 &&
            spec.fixedLifecycleSha256 === HEADROOM002.fixedLifecycleSha256 &&
            spec.fixedPlanningHelperSha256 === HEADROOM002.fixedPlanningHelperSha256 &&
            spec.physicalArms === 12 &&
            spec.sourceRoot === sequence.sourceRoot &&
            spec.runnable === (sequence.mode === "prepare"),
        "HEADROOM_EXECUTION_SPEC",
    );
    need(
        sequence.kind === "http-driver-headroom-operation" &&
            sequence.generatorVersion === 3 &&
            ["draft", "prepare"].includes(sequence.mode) &&
            sequence.executed === false &&
            sequence.dispatchImplemented === false &&
            /^[a-f0-9]{16}$/.test(sequence.attemptToken) &&
            /^[0-9]+:[0-9]+$/.test(spec.owner) &&
            sequence.owner === spec.owner &&
            sequence.physicalArms?.length === 12 &&
            spec.budgets?.maxWorkBytes === INPUT_CAP &&
            spec.budgets.operationMs === 26000000 &&
            build.sourceCommit === "d1239ef88d2d9b4b9b23fb920bcf7f6a387053d7",
        "HEADROOM_SEQUENCE",
    );
    same(spec.windowPolicy, HEADROOM002.windowPolicy, "HEADROOM_WINDOW_POLICY");
    same(spec.observationStratum, { collector: true, jvmObservation: true, maxInFlight: 4 }, "HEADROOM_OBSERVERS");
    need(
        settings.image === runtime.driver.image &&
            settings.image === spec.driver.image &&
            runtime.dockerExecutable === spec.dockerExecutable &&
            (sequence.mode === "draft" || runtime.sourceRoot === spec.sourceRoot) &&
            SHA.test(spec.nodeExecutable?.sha256),
        "HEADROOM_RUNTIME",
    );
    headroomDeclaredUnits(sequence);
    const actualSequence = {
        ...sequence,
        nodeHome: runtime.nodeHome,
        runtimeNode: runtime.nodeHome + "/bin/node",
        hostPython: runtime.hostPython,
    };
    const identities = [];
    for (let i = 0; i < 12; i++) {
        const expected = headroomGrid()[i],
            arm = sequence.physicalArms[i];
        for (const key of Object.keys(expected)) same(arm[key], expected[key], "HEADROOM_ARM_GRID");
        need(
            arm.runId === "hhr-" + sequence.attemptToken + "-" + arm.cellId &&
                arm.requestId === arm.runId + "-http" &&
                arm.targetTaskRequestId === arm.runId + "-target" &&
                arm.collectorRequestId === arm.runId + "-collector" &&
                arm.directory === spec.planRoot + "/arms/" + arm.cellId &&
                arm.collector === true &&
                arm.jvmObservation === true &&
                arm.preparedTargetTask === (sequence.mode === "prepare"),
            "HEADROOM_ARM_IDENTITY",
        );
        const profile = await load("arms/" + arm.cellId + "/driver-evidence/profile.json");
        verifyHeadroomProfile(
            profile,
            arm,
            ["build", "environment", "settings", "source-verification", "arm-context"].map(
                (n) => "/evidence/" + n + ".json",
            ),
        );
        const templates = headroom002Operations(arm, actualSequence, spec),
            mapping = {
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
        for (const [id, budgetKey] of Object.entries(mapping)) {
            const op = arm.operations.find((o) => o.budgetKey === budgetKey);
            same(op?.argvTemplate, templates[id], "HEADROOM_OPERATION_TEMPLATE");
        }
        same(
            arm.operations.find((o) => o.budgetKey === "cleanupDriver")?.argvTemplates,
            [templates["stop-driver"], templates["remove-owned-driver"]],
            "HEADROOM_DRIVER_CLEANUP_TEMPLATE",
        );
        identities.push({
            runId: arm.runId,
            httpTaskId: headroomTaskId(arm.requestId),
            targetTaskId: headroomTaskId(arm.targetTaskRequestId),
            collectorTaskId: headroomTaskId(arm.collectorRequestId),
        });
    }
    same(spec.expectedTasks, identities, "HEADROOM_TASK_IDENTITIES");
    return { spec, sequence: actualSequence, runtime, settings, build, environment, source, locked };
}
export function rebuildHeadroomCpu(phases) {
    same(
        phases.map((p) => p.name),
        ["warmup", "measurement"],
        "HEADROOM_PHASE_INVENTORY",
    );
    return phases.map((p) => {
        need(
            ["warmup", "measurement"].includes(p.name) &&
                Number.isFinite(p.elapsedMs) &&
                p.elapsedMs > 0 &&
                [p.cpu?.user, p.cpu?.system].every((v) => Number.isSafeInteger(v) && v >= 0),
            "HEADROOM_PHASE_CPU",
        );
        const cpu = (p.cpu.user + p.cpu.system) / (p.elapsedMs * 1000);
        need(
            cpu === p.averageCpuCores &&
                Number.isFinite(p.eventLoopUtilization) &&
                p.eventLoopUtilization >= 0 &&
                p.eventLoopUtilization <= 1,
            "HEADROOM_PHASE_CPU_ELU",
        );
        return {
            phase: p.name,
            averageCpuCores: cpu,
            recordedEventLoopUtilization: p.eventLoopUtilization,
            eluIndependentlyDerived: false,
        };
    });
}
function procAnchor(record, identity, cid, owner) {
    need(
        record.hostPid === identity.pid &&
            procStat(record.statRaw, record.hostPid).startTicks === identity.startTimeTicks,
        "HEADROOM_ANCHOR_STAT",
    );
    const membership = record.cgroupRaw.split(/\r?\n/).filter((x) => x.startsWith("0::"));
    need(membership.length === 1 && membership[0].includes(cid), "HEADROOM_ANCHOR_CGROUP");
    for (const [key, want] of [
        ["Uid", owner.split(":")[0]],
        ["Gid", owner.split(":")[1]],
    ]) {
        const rows = [...record.statusRaw.matchAll(new RegExp("^" + key + ":\\s+(.+)$", "gm"))];
        need(rows.length === 1 && rows[0][1].trim().split(/\s+/)[1] === want, "HEADROOM_ANCHOR_OWNER");
    }
    return membership[0].slice(3);
}
async function headroomObservers(dir, context, arm, dispatch, lifecycle, commands) {
    const { run, artifacts, stopDriver } = lifecycle.commandHandles,
        ops = lifecycle.operations;
    const once = (id) => {
        const found = commands.filter((c) => isDeepStrictEqual(c.argv, ops[id]));
        need(
            found.length === 1 && found[0].exitCode === 0 && found[0].error === null,
            "HEADROOM_OBSERVER_COMMAND:" + id,
        );
        return found[0];
    };
    const planCommand = once("collector-plan"),
        start = once("collector-start"),
        artifact = once("collector-artifacts"),
        verify = once("resource-verify"),
        originalJvm = once("jvm-original"),
        independentJvm = once("jvm-verify"),
        windows = once("phase-windows");
    need(
        planCommand.index < start.index &&
            start.index < run.index &&
            artifacts.index < artifact.index &&
            artifact.index < verify.index &&
            verify.index < stopDriver.index &&
            lifecycle.commandHandles.removeTarget.index < originalJvm.index &&
            originalJvm.index < independentJvm.index &&
            independentJvm.index < windows.index,
        "HEADROOM_OBSERVER_ORDER",
    );
    const spec = await read(join(dir, "collector-spec.json")),
        executedSpec = await read(join(dir, "collector/spec.json"));
    same(spec, executedSpec, "HEADROOM_COLLECTOR_SPEC");
    const bindingTemplate = await read(join(dir, "collector-binding-template.json"));
    const { dispatchable: ignoredDispatch, roles: ignoredRoles, kind: ignoredKind, ...baseSpec } = bindingTemplate;
    same(spec, { ...baseSpec, kind: "http-resource-collection", roles: spec.roles }, "HEADROOM_COLLECTOR_LATE_BINDING");
    need(
        spec.kind === "http-resource-collection" &&
            spec.intervalMs === 1000 &&
            spec.durationMs === 800000 &&
            spec.maxSamples === 801 &&
            spec.maxOutputBytes === 32 * MiB &&
            spec.roles?.length === 2,
        "HEADROOM_COLLECTOR_LIMITS",
    );
    const anchors = await read(join(dir, "collector-anchor-before.json")),
        first = await read(join(dir, "collector-first-sample.json"));
    same(first.identities, anchors.identities, "HEADROOM_FIRST_ANCHORS");
    const expectedRoles = {};
    for (const [role, cid, cpus, memoryBytes, pids] of [
        ["target", lifecycle.targetCid, 2, 2 * 1024 ** 3, 256],
        ["driver", lifecycle.driverCid, arm.driverCpus, 805306368, 128],
    ]) {
        const identity = anchors.identities[role],
            record = anchors.records[role];
        need(
            identity.bootId === dispatch.hostBootId &&
                spec.roles.find((r) => r.role === role)?.hostPid === identity.pid,
            "HEADROOM_COLLECTOR_ANCHOR",
        );
        const cgroupMember = procAnchor(record, identity, cid, context.spec.owner);
        const inspected = commands
            .filter(
                (c) =>
                    c.exitCode === 0 &&
                    c.error === null &&
                    c.argv[0] === context.spec.dockerExecutable &&
                    c.argv[1] === "inspect",
            )
            .flatMap((c) => {
                try {
                    return JSON.parse(c.stdout);
                } catch {
                    return [];
                }
            })
            .find((i) => i.Id === cid && i.State?.Running === true && i.State.Pid === identity.pid);
        need(inspected, "HEADROOM_COLLECTOR_DOCKER_PID");
        expectedRoles[role] = {
            hostPid: identity.pid,
            startTimeTicks: identity.startTimeTicks,
            cgroupMember,
            limits: { cpus, memoryBytes, pids },
        };
    }
    const marker = await read(join(dir, "collector-stop.json"));
    need(
        ns(run.finishedMonotonicNs) <= ns(marker.workflowFinishedMonotonicNs) &&
            ns(marker.workflowFinishedMonotonicNs) <= ns(artifacts.startedMonotonicNs),
        "HEADROOM_HTTP_HOST_ENVELOPE",
    );
    const afterAnchors = await read(join(dir, "collector-verified.json"));
    same(afterAnchors.anchorAfter.identities, anchors.identities, "HEADROOM_COLLECTOR_FINAL_ANCHORS");
    for (const [role, cid] of [
        ["target", lifecycle.targetCid],
        ["driver", lifecycle.driverCid],
    ])
        procAnchor(afterAnchors.anchorAfter.records[role], anchors.identities[role], cid, context.spec.owner);
    const componentSha256 = context.source.files.find(
        (f) => f.path === "components/http/resource-collector.py",
    )?.sha256;
    const resource = await verifyResourceEvidence(join(dir, "collector"), {
        hostBootId: dispatch.hostBootId,
        expectedSpec: spec,
        expectedRoles,
        componentSha256,
        stopMarkerPath: join(dir, "collector-stop.json"),
        workflowWindow: [run.startedMonotonicNs, marker.workflowFinishedMonotonicNs],
    });
    need(
        resource.status === "verified" &&
            resource.sourceOrigin === "linux-procfs" &&
            resource.qualityIssues.length === 0 &&
            ns(resource.firstFinishedMonotonicNs) <= ns(run.startedMonotonicNs),
        "HEADROOM_RESOURCE_VERIFICATION",
    );
    need(
        first.sample.index === 0 && first.sample.finishedMonotonicNs === resource.firstFinishedMonotonicNs,
        "HEADROOM_FIRST_SAMPLE",
    );
    const collectorSpecSha = hash(await bounded(join(dir, "collector-spec.json")));
    const task = await read(join(dir, "collector-task.json"));
    const submitted = commandData(start);
    need(
        submitted.taskId === headroomTaskId(arm.collectorRequestId) &&
            submitted.planDigest === task.digest &&
            submitted.reused !== true,
        "HEADROOM_COLLECTOR_SUBMISSION",
    );
    const terminal = commands
        .filter(
            (c) =>
                isDeepStrictEqual(c.argv, ops["collector-status"]) && c.index > run.index && c.index < artifact.index,
        )
        .map((c) => {
            try {
                return commandData(c);
            } catch {
                return null;
            }
        })
        .find((v) => v?.state === "completed");
    verifyHeadroomTask(task, terminal ?? {}, arm.collectorRequestId);
    same(
        task.plan.command,
        [
            context.runtime.hostPython,
            "-B",
            context.sequence.sourceRoot + "/components/http/resource-collector.py",
            "collect",
            "--spec",
            arm.directory + "/collector-spec.json",
            "--out",
            arm.directory + "/collector",
            "--json",
        ],
        "HEADROOM_COLLECTOR_COMMAND",
    );
    need(
        task.files?.some(
            (f) =>
                f.path === context.sequence.sourceRoot + "/components/http/resource-collector.py" &&
                f.sha256 === componentSha256,
        ),
        "HEADROOM_COLLECTOR_SOURCE",
    );
    need(
        task.plan.timeoutMs === 810000 &&
            task.plan.terminateGraceMs === 5000 &&
            task.plan.command?.[0] === context.runtime.hostPython &&
            task.plan.command?.[2] === context.sequence.sourceRoot + "/components/http/resource-collector.py" &&
            task.files?.some((f) => f.path === arm.directory + "/collector-spec.json" && f.sha256 === collectorSpecSha),
        "HEADROOM_COLLECTOR_TASK",
    );
    const env = join(dir, "environment"),
        ready = await read(join(env, "work/ready.json"));
    const jvm = await verifyJvmEvidence(env, join(dir, "jvm-original"), {
        settings: context.settings,
        hostBootId: dispatch.hostBootId,
        environmentPlanSha256: hash(await bounded(join(env, "plan.json"))),
        processIdentity: { ...ready.java, hostBootId: dispatch.hostBootId },
    });
    need(
        jvm.evidenceIntegrity === "passed" &&
            jvm.parserRecomputed &&
            jvm.captureComplete &&
            jvm.rawComplete &&
            jvm.observationStatus === "completed",
        "HEADROOM_JVM_VERIFICATION",
    );
    return { resource, jvm, firstSampleFinishedMonotonicNs: resource.firstFinishedMonotonicNs };
}
async function verifyHeadroomArm(plan, execution, context, arm, commands, dispatch) {
    const dir = join(plan, "arms", arm.cellId),
        raw = join(dir, "driver-evidence/http-plan/output"),
        report = await analyzeHttp(raw),
        profile = await read(join(dir, "driver-evidence/profile.json"));
    same(report.profile, profile, "HEADROOM_RAW_PROFILE");
    verifyHeadroomProfile(
        profile,
        arm,
        ["build", "environment", "settings", "source-verification", "arm-context"].map(
            (n) => "/evidence/" + n + ".json",
        ),
    );
    need(
        report.runs.length === 3 &&
            new Set(report.runs.map((r) => r.caseId)).size === 3 &&
            report.gates.sampleAccounting === "passed" &&
            report.gates.evidenceIntegrity === "passed",
        "HEADROOM_RAW_RUNS",
    );
    const impl = await read(join(raw, "implementation.json"));
    need(impl.nodeExecutableSha256 === context.spec.nodeExecutable.sha256, "HEADROOM_ACTUAL_NODE");
    same(
        impl.files.map((f) => f.file).sort(),
        [...HEADROOM002.implementationFiles].sort(),
        "HEADROOM_HTTP_IMPLEMENTATION",
    );
    const task = await read(join(dir, "driver-evidence/http-plan/task.json"));
    for (const f of impl.files) {
        const expected = context.source.files.find((s) => s.path === "components/http/" + f.file)?.sha256;
        need(
            f.sha256 === expected &&
                task.files?.some((p) => p.path === "/source/components/http/" + f.file && p.sha256 === expected),
            "HEADROOM_HTTP_SOURCE",
        );
    }
    need(
        task.files?.some((f) => f.path === "/node/bin/node" && f.sha256 === context.spec.nodeExecutable.sha256),
        "HEADROOM_TASK_NODE",
    );
    for (const name of ["profile.json", "target-evidence.json"]) {
        const bytes = await bounded(join(dir, "driver-evidence/http-plan", name));
        need(
            hash(bytes) === hash(await bounded(join(raw, name))) &&
                task.files?.some((f) => f.path === "/evidence/http-plan/" + name && f.sha256 === hash(bytes)),
            "HEADROOM_HTTP_TASK_INPUT",
        );
    }
    const target = await read(join(raw, "target-evidence.json"));
    same(
        target.files.map((f) => ({ bytes: f.bytes, sha256: f.sha256 })),
        ["build", "environment", "settings", "source-verification"]
            .map((n) => {
                const f = context.locked.get(n + ".json");
                return { bytes: f.bytes, sha256: f.sha256 };
            })
            .concat([
                {
                    bytes: context.locked.get("arms/" + arm.cellId + "/arm-context.json").bytes,
                    sha256: context.locked.get("arms/" + arm.cellId + "/arm-context.json").sha256,
                },
            ]),
        "HEADROOM_TARGET_ATTACHMENTS",
    );
    const settings = structuredClone(context.settings);
    delete settings.mode;
    const proof = await verifyControlledEnvironment(
        join(dir, "environment"),
        report,
        { build: context.build, settings, environment: context.environment },
        "forward",
        dispatch,
    );
    proof.ready = await read(join(dir, "environment/work/ready.json"));
    proof.containerFinal = await read(join(dir, "environment/container-final.json"));
    const receipt = (suffix) => read(join(execution, arm.runId + suffix + ".json"));
    const lifecycle = verifyHeadroomLifecycle({
        arm,
        context,
        proof,
        ready: await receipt("-ready"),
        httpTask: task,
        targetTask: await read(join(dir, "environment/task.json")),
        httpTerminal: await receipt("-http-terminal"),
        targetTerminal: await receipt("-target-terminal"),
        driverFinal: await receipt("-driver-final"),
        targetFinal: await receipt("-target-final"),
        commands,
    });
    const observers = await headroomObservers(dir, context, arm, dispatch, lifecycle, commands);
    const phase = await analyzePhaseWindows(raw, HEADROOM002.windowPolicy),
        completion = await summarizeCompletionWindows(raw, { windowMs: 10000, minimumLatencyObservations: 50 });
    need(
        phase.rawManifestSha256 === report.rawManifestSha256 &&
            completion.rawManifestSha256 === report.rawManifestSha256,
        "HEADROOM_WINDOW_SOURCE",
    );
    const endpoints = verifyHeadroomCpuEndpoints(
        await receipt("-driver-cpu-before"),
        await receipt("-driver-cpu-after"),
        arm,
        lifecycle.driverCid,
        dispatch.hostBootId,
        [lifecycle.commandHandles.run.startedMonotonicNs, lifecycle.commandHandles.run.finishedMonotonicNs],
    );
    const cells = [];
    for (const run of report.runs) {
        need(
            HEADROOM002.cases.includes(run.caseId) &&
                run.loadId === "open-" + arm.ratePerSecond &&
                run.repeat === 0 &&
                run.scheduled === arm.ratePerSecond * 120,
            "HEADROOM_RUN_SCOPE",
        );
        const original = await read(join(raw, "runs/" + run.id + ".json"));
        rebuildHeadroomCpu(original.phases);
        cells.push({
            runId: run.id,
            physicalArmRunId: arm.runId,
            ...assessHeadroomRun(
                run,
                phase.runs.find((r) => r.id === run.id),
            ),
        });
    }
    const { commandHandles, operations, ...identity } = lifecycle;
    return {
        ...arm,
        operations: undefined,
        workflowDriverCpu: endpoints,
        finalCommandFinishedMonotonicNs: lifecycle.operations["phase-windows"]
            ? commands.find((c) => isDeepStrictEqual(c.argv, lifecycle.operations["phase-windows"])).finishedMonotonicNs
            : null,
        valid: true,
        issues: [],
        ...identity,
        rawManifestSha256: report.rawManifestSha256,
        cells,
        completionWindows: completion.runs.map((r) => ({
            caseId: r.caseId,
            windows: r.windows,
            drain: r.drain,
            definitions: completion.definitions,
        })),
        resourceSummary: observers.resource,
        jvmSummary: { evidenceIntegrity: observers.jvm.evidenceIntegrity, parserRecomputed: true },
        javaIdentity: {
            bootId: proof.pre.identity.bootId,
            pid: proof.pre.identity.pid,
            startTimeTicks: String(proof.pre.identity.startTimeTicks),
        },
    };
}

function unitCommand(unit) {
    return [
        "/usr/bin/systemctl",
        "--user",
        "show",
        unit,
        ...["Id", "LoadState", "ActiveState", "SubState", "MainPID", "ControlGroup", "Result"].flatMap((p) => [
            "-p",
            p,
        ]),
    ];
}
export function verifyHeadroomUnit(command, unit, protectedOnly = false) {
    same(command.argv, unitCommand(unit), "HEADROOM_UNIT_COMMAND");
    need(command.exitCode === 0 && command.error === null && typeof command.stdout === "string", "HEADROOM_UNIT_QUERY");
    const fields = {};
    for (const line of command.stdout.trimEnd().split("\n")) {
        const at = line.indexOf("=");
        need(at > 0 && !Object.hasOwn(fields, line.slice(0, at)), "HEADROOM_UNIT_FIELDS");
        fields[line.slice(0, at)] = line.slice(at + 1);
    }
    exact(fields, ["Id", "LoadState", "ActiveState", "SubState", "MainPID", "ControlGroup", "Result"]);
    need(fields.Id === unit && /^(0|[1-9][0-9]*)$/.test(fields.MainPID), "HEADROOM_UNIT_IDENTITY");
    need(["loaded", "not-found"].includes(fields.LoadState), "HEADROOM_UNIT_LOAD_STATE");
    need(
        protectedOnly ||
            (["loaded", "not-found"].includes(fields.LoadState) &&
                fields.ActiveState === "inactive" &&
                fields.SubState === "dead" &&
                fields.MainPID === "0" &&
                fields.ControlGroup === ""),
        "HEADROOM_UNIT_ACTIVE",
    );
    return { unit, protectedObservedOnly: protectedOnly, properties: fields, historicalSuccessProven: false };
}
export function verifyHeadroomCommandClosure(context, arms, commands) {
    const allowed = [];
    const units = headroomDeclaredUnits(context.sequence);
    for (const unit of units) allowed.push(unitCommand(unit));
    const consumedUnits = new Set();
    for (const arm of arms) {
        const planned = context.sequence.physicalArms.find((a) => a.runId === arm.runId);
        const bound = {
            "target.container.id": arm.targetCid,
            "driver.container.id": arm.driverCid,
            "target.ready.jcmdExecutable": "/opt/jdk/bin/jcmd",
            "target.ready.java.pid": arm.javaIdentity.pid,
            "http.task.id": headroomTaskId(planned.requestId),
            "collector.task.id": headroomTaskId(planned.collectorRequestId),
        };
        for (const [id, argv] of Object.entries(headroom002Operations(planned, context.sequence, context.spec)))
            if (id !== "wait-original-http") allowed.push(template(argv, bound));
        for (const selector of [arm.targetCid, arm.driverCid, arm.runId + "-driver"])
            allowed.push([context.spec.dockerExecutable, "inspect", "--type", "container", selector]);
        allowed.push(
            [context.spec.dockerExecutable, "inspect", arm.targetCid],
            [
                context.spec.dockerExecutable,
                "stats",
                "--no-stream",
                "--format",
                "{{json .}}",
                arm.targetCid,
                arm.driverCid,
            ],
        );
        const first = commands.find((c) => c.startedMonotonicNs === arm.start),
            last = commands.find((c) => c.finishedMonotonicNs === arm.finalCommandFinishedMonotonicNs);
        need(first && last, "HEADROOM_ARM_COMMAND_WINDOW");
        const previous =
            arms.indexOf(arm) > 0 ? BigInt(arms[arms.indexOf(arm) - 1].finalCommandFinishedMonotonicNs) : 0n;
        const next = arms.indexOf(arm) + 1 < arms.length ? BigInt(arms[arms.indexOf(arm) + 1].start) : null;
        for (const unit of units) {
            const matches = commands.filter((c) => isDeepStrictEqual(c.argv, unitCommand(unit)));
            const pre = matches.filter((c) => ns(c.startedMonotonicNs) >= previous && c.index < first.index);
            const post = matches.filter(
                (c) => c.index > last.index && (next === null || ns(c.finishedMonotonicNs) < next),
            );
            need(pre.length >= 1 && post.length >= 1, "HEADROOM_PRE_POST_UNIT_MISSING");
            for (const c of [pre.at(-1), post[0]]) {
                need(!consumedUnits.has(c.index), "HEADROOM_UNIT_WINDOW_REUSED");
                consumedUnits.add(c.index);
                verifyHeadroomUnit(c, unit, unit === context.sequence.protectedObservedUnit);
            }
        }
    }
    need(
        consumedUnits.size === commands.filter((c) => c.argv[0] === "/usr/bin/systemctl").length,
        "HEADROOM_UNIT_UNASSOCIATED",
    );
    need(
        commands.every((c) => allowed.some((argv) => isDeepStrictEqual(c.argv, argv))),
        "HEADROOM_UNDECLARED_COMMAND",
    );
}
export function verifyHeadroomCpuEndpoints(before, after, arm, driverCid, hostBootId, httpWindow) {
    const parse = (e, label) => {
        need(
            e.kind === "http-driver-workflow-cpu-endpoint" &&
                e.label === label &&
                e.containerId === driverCid &&
                e.identity?.bootId === hostBootId,
            "HEADROOM_CPU_ENDPOINT_IDENTITY",
        );
        const start = ns(e.beforeMonotonicNs),
            end = ns(e.afterMonotonicNs);
        need(start <= end, "HEADROOM_CPU_ENDPOINT_TIME");
        need(
            procStat(e.procStatRaw, e.identity.pid).startTicks === e.identity.startTimeTicks,
            "HEADROOM_CPU_ENDPOINT_PROC",
        );
        const cg = e.procCgroupRaw
            .trim()
            .split("\n")
            .filter((v) => v.startsWith("0::"));
        need(cg.length === 1 && cg[0].includes(driverCid), "HEADROOM_CPU_ENDPOINT_CGROUP");
        const fields = {};
        for (const line of e.cpuStatRaw.trim().split("\n")) {
            const [key, value, ...extra] = line.trim().split(/\s+/);
            need(
                key && !Object.hasOwn(fields, key) && extra.length === 0 && /^[0-9]+$/.test(value),
                "HEADROOM_CPU_ENDPOINT_COUNTER",
            );
            fields[key] = value;
        }
        need(fields.usage_usec !== undefined, "HEADROOM_CPU_ENDPOINT_COUNTER");
        const max = e.cpuMaxRaw.trim().split(/\s+/);
        need(
            max.length === 2 &&
                max.every((v) => /^[1-9][0-9]*$/.test(v)) &&
                BigInt(max[0]) === BigInt(arm.driverCpus) * BigInt(max[1]),
            "HEADROOM_CPU_ENDPOINT_QUOTA",
        );
        return { start, end, usage: BigInt(fields.usage_usec), cgroup: cg[0] };
    };
    const a = parse(before, "before"),
        b = parse(after, "after");
    same(before.identity, after.identity, "HEADROOM_CPU_ENDPOINT_ANCHOR");
    same(a.cgroup, b.cgroup, "HEADROOM_CPU_ENDPOINT_ANCHOR");
    same(before.cpuMaxRaw, after.cpuMaxRaw, "HEADROOM_CPU_ENDPOINT_QUOTA_CHANGED");
    need(
        a.end <= ns(httpWindow[0]) && ns(httpWindow[1]) <= b.start && a.start < b.end && b.usage >= a.usage,
        "HEADROOM_CPU_ENDPOINT_WINDOW",
    );
    const elapsedNanoseconds = b.end - a.start,
        usageMicros = b.usage - a.usage;
    return {
        usageMicros: String(usageMicros),
        envelopeElapsedNanoseconds: String(elapsedNanoseconds),
        averageCpuCores: (Number(usageMicros) * 1000) / Number(elapsedNanoseconds),
        scope: "whole HTTP workflow including warmup, CLI and wrapper; descriptive only",
        quotaCpus: arm.driverCpus,
        independentlyRecomputed: true,
    };
}
export function verifyHeadroomPhysicalIdentities(arms) {
    const seen = new Set();
    let end = 0n;
    for (const arm of arms.filter((a) => a.valid)) {
        for (const key of [
            "arm:" + arm.runId,
            "raw:" + arm.rawManifestSha256,
            ...arm.cells.map((c) => "run:" + arm.runId + "/" + c.runId),
            "driver:" + arm.driverCid,
            "target:" + arm.targetCid,
            "java:" + arm.javaIdentity.bootId + ":" + arm.javaIdentity.pid + ":" + arm.javaIdentity.startTimeTicks,
        ]) {
            need(!seen.has(key), "HEADROOM_PHYSICAL_IDENTITY_REUSED");
            seen.add(key);
        }
        need(ns(arm.start) >= end, "HEADROOM_ARM_WINDOWS_OVERLAP");
        end = ns(arm.finalCommandFinishedMonotonicNs);
    }
    return true;
}
const limitText = [
    "Scope is the preregistered three cases at 10, 20 and 40 arrivals/s, IF4, two CPU quotas and two blocks; no all-eleven, wider-rate, target capacity or statistical-equivalence acceptance.",
    "Window response P50/P95 compares two late-warmup with two early-measurement arrival cohorts; full measurement steady state is unassessed. Completion cohorts and drain are separate descriptive counts, with no 95% completion gate.",
    "Whole-phase Node CPU is recomputed from preserved user/system microseconds and elapsed milliseconds; ELU is source-bound recorded utilization because active/idle counters are unavailable.",
    "Collector, Docker, systemd and procfs records are offline producer evidence, not external execution attestation or a current container-absence observation. Raw HTTP clocks are not independently aligned to the host command clock.",
    "The exact original 529-member source selection is digest-bound. The entire physical source tree and installed dependencies are not reread or attested.",
];
async function worker(inputFile, output, maxBytes) {
    const inputRaw = await bounded(inputFile, MiB),
        input = validateHeadroomInput(JSON.parse(inputRaw));
    const plan = await realpath(resolve(dirname(inputFile), input.planDirectory)),
        execution = await realpath(resolve(dirname(inputFile), input.executionDirectory));
    const criterion = await bounded(join(ROOT, "presets/http/headroom-engineering-3case-v1.json"), MiB);
    need(hash(criterion) === HEADROOM002.criteriaSha256, "HEADROOM_CRITERIA_CHANGED");
    const fixedBytes = inputRaw.length + criterion.length;
    const beforePlan = await inventory(plan, INPUT_CAP - fixedBytes),
        beforeExecution = await inventory(execution, INPUT_CAP - fixedBytes - beforePlan.bytes);
    const context = await headroomPlan(plan, execution, input.reference),
        issues = [],
        assets = [];
    const arms = context.sequence.physicalArms.map((a) => ({
        runId: a.runId,
        ...headroomGrid().find((g) => g.cellId === a.cellId),
        valid: false,
        issues: ["HEADROOM_ARM_NOT_VERIFIED"],
    }));
    let registration = null;
    try {
        need(context.spec.runnable === true && context.sequence.mode === "prepare", "HEADROOM_NOT_EXECUTED");
        const dispatch = await read(join(execution, "dispatch.json"));
        need(
            dispatch.executed === true &&
                dispatch.executionSpecSha256 === input.reference.executionSpecSha256 &&
                /^[a-f0-9-]{36}$/.test(dispatch.hostBootId) &&
                Number.isSafeInteger(dispatch.pid) &&
                dispatch.pid > 0 &&
                /^[0-9]+$/.test(dispatch.startTicks),
            "HEADROOM_DISPATCH_IDENTITY",
        );
        same(await read(join(plan, "headroom-dispatch.json")), dispatch, "HEADROOM_PLAN_DISPATCH");
        const data = [];
        for (const [key, digest] of [
            ["assessmentBindingFile", "assessmentBindingSha256"],
            ["preDispatchLaunchRecordFile", "preDispatchLaunchRecordSha256"],
            ["controllerStartEvidenceFile", "controllerStartEvidenceSha256"],
        ]) {
            const path = await realpath(resolve(dirname(inputFile), input[key])),
                raw = await bounded(path, MiB);
            need(hash(raw) === input.reference[digest], "HEADROOM_ASSESSMENT_REFERENCE");
            assets.push({ path, bytes: raw.length, sha256: hash(raw) });
            data.push(JSON.parse(raw));
        }
        need(
            beforePlan.bytes + beforeExecution.bytes + fixedBytes + assets.reduce((n, a) => n + a.bytes, 0) <=
                INPUT_CAP,
            "HEADROOM_INPUT_BYTE_LIMIT",
        );
        need(
            (await canonical(resolve(dirname(assets[0].path), data[0].preDispatchLaunchRecordReference.path))) ===
                assets[1].path,
            "HEADROOM_BINDING_LAUNCH_REFERENCE",
        );
        registration = verifyHeadroomPreregistration(...data, dispatch, context, assets[0].sha256);
        const commands = await commandsAt(execution);
        need(
            ns(commands[0].startedMonotonicNs) >= ns(registration.controllerStartMonotonicNs),
            "HEADROOM_CONTROLLER_COMMAND_WINDOW",
        );
        for (let i = 0; i < arms.length; i++)
            try {
                arms[i] = await verifyHeadroomArm(
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
        verifyHeadroomPhysicalIdentities(arms);
        if (arms.every((a) => a.valid)) verifyHeadroomCommandClosure(context, arms, commands);
    } catch (e) {
        issues.push(e.code ?? e.message);
    }
    for (const asset of assets)
        need(hash(await bounded(asset.path, MiB)) === asset.sha256, "HEADROOM_ASSESSMENT_CHANGED");
    same(await inventory(plan, INPUT_CAP - fixedBytes), beforePlan, "HEADROOM_PLAN_CHANGED");
    same(
        await inventory(execution, INPUT_CAP - fixedBytes - beforePlan.bytes),
        beforeExecution,
        "HEADROOM_EXECUTION_CHANGED",
    );
    need(hash(await bounded(inputFile, MiB)) === hash(inputRaw), "HEADROOM_INPUT_CHANGED");
    const summary = assessHeadroomCells(arms, issues),
        readerFiles = [];
    for (const file of [
        "headroom-evidence.mjs",
        "headroom002-contract.mjs",
        "paired-evidence.mjs",
        "resource-evidence.mjs",
        "jvm-evidence.mjs",
        "phase-windows.mjs",
        "completion-windows.mjs",
        ...HEADROOM002.implementationFiles,
        "../paired/core.mjs",
    ]) {
        const raw = await bounded(join(dirname(SELF), file));
        readerFiles.push({ path: file, bytes: raw.length, sha256: hash(raw) });
    }
    const report = {
        schemaVersion: 1,
        kind: "http-driver-headroom-evidence",
        gateId: "driver-headroom",
        operationVersion: HEADROOM002.operationVersion,
        ...summary,
        verifiedPhysicalArms: arms.filter((a) => a.valid).length,
        arms,
        verification: { valid: issues.length === 0 && arms.every((a) => a.valid), issues, inputClosureStable: true },
        registration,
        references: {
            inputSha256: hash(inputRaw),
            ...input.reference,
            planInventorySha256: valueHash(beforePlan.files),
            executionInventorySha256: valueHash(beforeExecution.files),
        },
        criteria: JSON.parse(criterion),
        provenance: {
            sourceSelectionMembersBound: 529,
            fullSourceBytesReverified: false,
            installedDependenciesAttested: false,
            externalAttestation: false,
            readerFiles,
        },
        limits: limitText,
    };
    await save(join(output, "worker-result.json"), report, Math.min(16 * MiB, maxBytes - 65536));
}
export function checkHeadroomDeadline(signal, begun, timeoutMs) {
    need(!signal.aborted, "HEADROOM_CANCELLED");
    need(performance.now() - begun < timeoutMs, "HEADROOM_DEADLINE");
}
export async function reportHeadroom({ inputFile, outputDirectory, timeoutMs, maxBytes, signal: externalSignal }) {
    limits(timeoutMs, maxBytes);
    const begun = performance.now(),
        inputPath = await realpath(inputFile),
        original = await bounded(inputPath, MiB),
        input = validateHeadroomInput(JSON.parse(original)),
        output = await canonical(outputDirectory);
    const plan = await realpath(resolve(dirname(inputPath), input.planDirectory)),
        execution = await realpath(resolve(dirname(inputPath), input.executionDirectory));
    const selectedSpec = await read(join(execution, "execution-spec.json"), MiB),
        selectedRuntime = await read(join(plan, "runtime-selection.json"), MiB),
        selectedSettings = await read(join(plan, "settings.json"), MiB);
    const selectedSourcePaths = await Promise.all(
        [
            selectedSpec.sourceRoot,
            selectedRuntime.nodeHome,
            selectedSettings.distribution,
            selectedSettings.javaHome,
        ].map((p) => {
            need(
                typeof p === "string" && p.startsWith("/") && p.length <= 4096 && !/[\r\n\0]/.test(p),
                "HEADROOM_SELECTED_SOURCE_PATH",
            );
            return canonical(p);
        }),
    );
    const assetPaths = await Promise.all(
        ["assessmentBindingFile", "preDispatchLaunchRecordFile", "controllerStartEvidenceFile"].map((k) =>
            canonical(resolve(dirname(inputPath), input[k])),
        ),
    );
    const sourcePaths = await Promise.all(
        ["components", "src", "dist", "node_modules", "benchmarks", "presets", "release", "package.json"].map((p) =>
            canonical(join(ROOT, p)),
        ),
    );
    need(!inside(plan, execution) && !inside(execution, plan), "HEADROOM_INPUT_ROOT_OVERLAP");
    need(
        [inputPath, plan, execution, ...assetPaths, ...sourcePaths, ...selectedSourcePaths].every(
            (p) => !inside(output, p) && !inside(p, output),
        ),
        "HEADROOM_OUTPUT_OVERLAP",
    );
    // Three seconds for descendant cleanup, plus bounded final receipts, are reserved inside the declared overall budget.
    need(timeoutMs - (performance.now() - begun) > 4200, "HEADROOM_DEADLINE");
    await mkdir(output, { mode: 0o700 });
    const cancellation = new AbortController(),
        interrupt = () => cancellation.abort(),
        signal = cancellation.signal;
    externalSignal?.addEventListener("abort", interrupt, { once: true });
    process.on("SIGTERM", interrupt);
    process.on("SIGINT", interrupt);
    if (externalSignal?.aborted) interrupt();
    try {
        const result = await runWorker(
            inputPath,
            output,
            Math.max(1, timeoutMs - 4200 - (performance.now() - begun)),
            maxBytes,
            signal,
        );
        let code;
        try {
            code = JSON.parse(result.output).error?.code;
        } catch {}
        need(
            result.code === 0 && !result.failure && !signal.aborted,
            result.failure ?? (signal.aborted ? "HEADROOM_CANCELLED" : (code ?? "HEADROOM_WORKER_FAILED")),
        );
        checkHeadroomDeadline(signal, begun, timeoutMs);
        const report = await read(join(output, "worker-result.json"));
        need(
            report.references.inputSha256 === hash(original) && hash(await bounded(inputPath, MiB)) === hash(original),
            "HEADROOM_INPUT_CHANGED",
        );
        const snapshot = {
                ...input,
                planDirectory: plan,
                executionDirectory: execution,
                assessmentBindingFile: assetPaths[0],
                preDispatchLaunchRecordFile: assetPaths[1],
                controllerStartEvidenceFile: assetPaths[2],
            },
            snapshotBytes = encode(snapshot),
            reportBytes = encode(report);
        need(snapshotBytes.length + reportBytes.length + 65536 <= maxBytes, "HEADROOM_OUTPUT_BYTE_LIMIT");
        checkHeadroomDeadline(signal, begun, timeoutMs);
        await rm(join(output, "worker-result.json"));
        const files = [
            await save(join(output, "input.json"), snapshot, maxBytes),
            await save(join(output, "report.json"), report, maxBytes - snapshotBytes.length),
        ];
        checkHeadroomDeadline(signal, begun, timeoutMs);
        await save(
            join(output, "report.lock.json"),
            { schemaVersion: 1, kind: "http-driver-headroom-evidence-lock", contentDigest: valueHash(files), files },
            maxBytes - files.reduce((n, f) => n + f.bytes, 0),
        );
        checkHeadroomDeadline(signal, begun, timeoutMs);
        return {
            kind: "http-driver-headroom-evidence",
            directory: output,
            reportJson: join(output, "report.json"),
            reportLock: join(output, "report.lock.json"),
            gateId: "driver-headroom",
            status: report.status,
            limitedScopeEngineeringAccepted: report.limitedScopeEngineeringAccepted,
            inferenceAccepted: false,
            performanceAccepted: false,
        };
    } catch (e) {
        await rm(join(output, "worker-result.json"), { force: true });
        await rm(join(output, "report.lock.json"), { force: true });
        await save(
            join(output, "partial.json"),
            {
                schemaVersion: 1,
                kind: "http-driver-headroom-evidence-partial",
                status: "unknown",
                gateId: "driver-headroom",
                verification: { valid: false, issues: [e.code ?? e.message] },
                ...assessHeadroomCells([]),
                references: input.reference,
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
        need(argv.length === 4, "HEADROOM_WORKER_ARGUMENTS");
        await worker(argv[1], argv[2], Number(argv[3]));
        return;
    }
    if (argv.length === 1 && argv[0] === "--help") {
        process.stdout.write(
            "Read-only headroom002 engineering evidence: report --input INPUT --out NEW --timeout-ms N --max-bytes N\nCombined input cap16GiB. Output cap1MiB..8GiB, overall timeout1s..24h including cleanup; insufficient time retains partial or rejects before writes. No Docker/HTTP/SSH. Unknown/failed reports exit0; inspect scoped status.\n",
        );
        return;
    }
    need(argv.shift() === "report", "HEADROOM_ARGUMENTS");
    const args = {};
    for (let i = 0; i < argv.length; i += 2) {
        need(
            ["--input", "--out", "--timeout-ms", "--max-bytes"].includes(argv[i]) && argv[i + 1] && !args[argv[i]],
            "HEADROOM_ARGUMENTS",
        );
        args[argv[i]] = argv[i + 1];
    }
    need(Object.keys(args).length === 4, "HEADROOM_ARGUMENTS");
    const data = await reportHeadroom({
        inputFile: args["--input"],
        outputDirectory: args["--out"],
        timeoutMs: Number(args["--timeout-ms"]),
        maxBytes: Number(args["--max-bytes"]),
    });
    process.stdout.write(JSON.stringify({ ok: true, data }) + "\n");
}
if (process.argv[1] && pathToFileURL(resolve(process.argv[1])).href === import.meta.url)
    main().catch((e) => {
        process.stderr.write(
            JSON.stringify({ ok: false, error: { code: e.code ?? "HEADROOM_EVIDENCE_ERROR", message: e.message } }) +
                "\n",
        );
        process.exitCode = 6;
    });
