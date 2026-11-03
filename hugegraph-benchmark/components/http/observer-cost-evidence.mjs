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
import { open, mkdir, writeFile, rm, readdir, lstat, realpath } from "node:fs/promises";
import { join, resolve, dirname, basename, sep } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { createHash } from "node:crypto";
import { spawn } from "node:child_process";
import { performance } from "node:perf_hooks";
import { isDeepStrictEqual } from "node:util";
import { analyze as analyzeHttp } from "./analyzer.mjs";
import { verifyControlledEnvironment } from "./paired-evidence.mjs";
import { verifyResourceEvidence } from "./resource-evidence.mjs";
import { verifyJvmEvidence } from "./jvm-evidence.mjs";
import { valueHash, balancedOrders } from "../paired/core.mjs";
import {
    OBSERVER003 as O,
    observerNeed as need,
    observerSame as same,
    observerSha as hash,
    observerNs as ns,
    observerTaskId as taskId,
    observer003Scope,
    observer003Policy,
    observer003Schedule,
    observer003Driver,
    verifyObserverTask,
    verifyObserverProfile,
    summarizeObserver003,
} from "./observer003-contract.mjs";

const SELF = fileURLToPath(import.meta.url),
    ROOT = resolve(dirname(SELF), "../.."),
    MiB = 1024 ** 2,
    INPUT_CAP = 32 * 1024 ** 3;
const SHA = /^[a-f0-9]{64}$/,
    encode = (v) => Buffer.from(JSON.stringify(v, null, 2) + "\n");
const inside = (a, b) => a === b || a.startsWith(b + sep);
const pathOk = (p) => typeof p === "string" && p.length > 0 && p.length <= 4096 && !/[\r\n\0]/.test(p);
const exact = (v, fields) => same(Object.keys(v ?? {}).sort(), fields.sort(), "OBSERVER_INPUT_FIELDS");
async function bytes(path, cap = 16 * MiB) {
    const fd = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
    try {
        const a = await fd.stat();
        need(a.isFile() && a.size <= cap, "OBSERVER_FILE_BOUND");
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
            "OBSERVER_INPUT_CHANGED",
        );
        return raw.subarray(0, n);
    } finally {
        await fd.close();
    }
}
const json = async (p, cap) => JSON.parse(new TextDecoder("utf8", { fatal: true }).decode(await bytes(p, cap)));
const save = async (p, value, cap) => {
    const b = encode(value);
    need(b.length <= cap, "OBSERVER_OUTPUT_BOUND");
    await writeFile(p, b, { flag: "wx", mode: 0o600 });
    return { path: basename(p), bytes: b.length, sha256: hash(b) };
};
async function canonical(p) {
    const tail = [];
    p = resolve(p);
    while (true) {
        try {
            return join(await realpath(p), ...tail.reverse());
        } catch (e) {
            if (e.code !== "ENOENT") throw e;
        }
        tail.push(basename(p));
        const parent = dirname(p);
        need(parent !== p, "OBSERVER_OUTPUT_PATH");
        p = parent;
    }
}
async function inventory(root, cap) {
    const files = [];
    let total = 0,
        count = 0;
    async function walk(rel, depth) {
        need(depth <= 24, "OBSERVER_INPUT_DEPTH");
        for (const name of (await readdir(join(root, rel))).sort()) {
            need(++count <= 100000, "OBSERVER_INPUT_MEMBERS");
            const path = rel ? rel + "/" + name : name,
                full = join(root, path),
                s = await lstat(full);
            if (s.isDirectory()) {
                await walk(path, depth + 1);
                continue;
            }
            need(s.isFile() && s.size <= 512 * MiB && total + s.size <= cap, "OBSERVER_FILE_BOUND");
            total += s.size;
            const fd = await open(full, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
            const digest = createHash("sha256");
            let n = 0;
            try {
                const a = await fd.stat();
                need(a.isFile() && a.size === s.size, "OBSERVER_INPUT_CHANGED");
                const b = Buffer.alloc(65536);
                while (true) {
                    const r = await fd.read(b, 0, b.length, n);
                    if (!r.bytesRead) break;
                    n += r.bytesRead;
                    need(n <= a.size, "OBSERVER_INPUT_CHANGED");
                    digest.update(b.subarray(0, r.bytesRead));
                }
                const z = await fd.stat();
                need(
                    n === a.size && z.size === a.size && a.mtimeMs === z.mtimeMs && a.ctimeMs === z.ctimeMs,
                    "OBSERVER_INPUT_CHANGED",
                );
            } finally {
                await fd.close();
            }
            files.push({ path, bytes: n, sha256: digest.digest("hex") });
        }
    }
    await walk("", 0);
    return { files, bytes: total };
}
export function validateObserverCostInput(v) {
    exact(v, ["schemaVersion", "kind", "operationVersion", "planDirectory", "executionDirectory", "reference"]);
    exact(v.reference, ["planLockSha256", "executionSpecSha256", "sourceSelectionSha256"]);
    need(
        v.schemaVersion === 1 &&
            v.kind === "http-observer-cost-evidence-input" &&
            v.operationVersion === O.id &&
            pathOk(v.planDirectory) &&
            pathOk(v.executionDirectory) &&
            Object.values(v.reference).every((s) => SHA.test(s)) &&
            v.reference.sourceSelectionSha256 === O.sourceSha256,
        "OBSERVER_INPUT_IDENTITY",
    );
    return v;
}
async function planContext(plan, execution, ref) {
    const lockRaw = await bytes(join(plan, "plan.lock.json")),
        lock = JSON.parse(lockRaw);
    need(
        hash(lockRaw) === ref.planLockSha256 &&
            lock.kind === "http-observer-cost-plan-lock" &&
            lock.generator?.sha256 === O.generatorSha256 &&
            lock.fixedPlanningHelper?.sha256 === O.helperSha256 &&
            Array.isArray(lock.files) &&
            lock.files.length <= 20000 &&
            valueHash(lock.files) === lock.contentDigest,
        "OBSERVER_PLAN_LOCK",
    );
    const members = new Map();
    let total = 0;
    for (const f of lock.files) {
        need(
            typeof f.path === "string" &&
                /^[A-Za-z0-9_./-]+$/.test(f.path) &&
                !f.path.startsWith("/") &&
                !f.path.split("/").some((v) => ["", ".", ".."].includes(v)) &&
                !members.has(f.path),
            "OBSERVER_PLAN_MEMBER",
        );
        const raw = await bytes(join(plan, f.path));
        total += raw.length;
        need(total <= 256 * MiB && raw.length === f.bytes && hash(raw) === f.sha256, "OBSERVER_PLAN_CHANGED");
        members.set(f.path, f);
    }
    const load = async (p) => {
        need(members.has(p), "OBSERVER_PLAN_REQUIRED");
        return json(join(plan, p));
    };
    const sequence = await load("sequence.json"),
        specRaw = await bytes(join(execution, "execution-spec.json")),
        spec = JSON.parse(specRaw);
    const executionLock = await json(join(execution, "execution.lock.json"));
    need(
        hash(specRaw) === ref.executionSpecSha256 &&
            executionLock.specSha256 === ref.executionSpecSha256 &&
            executionLock.kind === "http-observer-cost-execution-lock" &&
            spec.kind === "http-observer-cost-execution-spec" &&
            spec.executorSha256 === O.executorSha256 &&
            spec.generatorSha256 === O.generatorSha256 &&
            spec.sourceRoot === sequence.sourceRoot &&
            spec.planLockSha256 === ref.planLockSha256 &&
            spec.physicalArms === 16 &&
            pathOk(spec.planRoot) &&
            spec.planRoot.startsWith("/") &&
            pathOk(spec.node) &&
            spec.node.startsWith("/") &&
            SHA.test(spec.nodeExecutable?.sha256) &&
            /^[0-9]+:[0-9]+$/.test(spec.owner),
        "OBSERVER_EXECUTION_SPEC",
    );
    const sourceRaw = await bytes(join(plan, "source-selection.lock.json"));
    need(hash(sourceRaw) === ref.sourceSelectionSha256, "OBSERVER_SOURCE_SELECTION");
    const source = JSON.parse(sourceRaw);
    need(
        source.files.length === 529 && new Set(source.files.map((f) => f.path)).size === 529,
        "OBSERVER_SOURCE_INVENTORY",
    );
    need(
        source.files.find((f) => f.path === "benchmarks/http-serving-v0.1/bundle.lock.json")?.sha256 === O.bundleSha256,
        "OBSERVER_SOURCE_BUNDLE",
    );
    same(await load("scope.json"), observer003Scope(), "OBSERVER_SCOPE");
    need(
        sequence.kind === "http-observer-cost-003-operation-recipe" &&
            sequence.id === O.id &&
            ["draft", "prepare"].includes(sequence.mode) &&
            spec.runnable === (sequence.mode === "prepare") &&
            sequence.executed === false &&
            sequence.dispatchImplemented === false &&
            sequence.physicalArms?.length === 16 &&
            /^[a-f0-9]{16}$/.test(sequence.attemptToken),
        "OBSERVER_SEQUENCE",
    );
    same(sequence.groupOrder, ["J", "C"], "OBSERVER_GROUP_ORDER");
    const runtime = await load("runtime-selection.json");
    same(runtime, spec.runtime, "OBSERVER_RUNTIME");
    same(await load("budgets.json"), spec.budgets, "OBSERVER_BUDGET_SPEC");
    need(
        spec.budgets.httpTaskTimeoutMs === 609000 &&
            spec.budgets.workflowDeadlineMs === 549000 &&
            spec.budgets.operationMaximumMs === 30000000 &&
            spec.budgets.operationCleanupMs === 90000 &&
            spec.budgets.maxWorkBytes === INPUT_CAP,
        "OBSERVER_FIXED_BUDGET",
    );
    need(
        runtime.driver.cpus === 1 &&
            runtime.driver.memoryBytes === 805306368 &&
            runtime.driver.wallSeconds === 960 &&
            runtime.collector.durationMs === 800000 &&
            runtime.collector.maxSamples === 801 &&
            runtime.collector.maxOutputBytes === 32 * MiB,
        "OBSERVER_RUNTIME_BUDGET",
    );
    const on = await load("settings-jvm-on.json"),
        off = await load("settings-jvm-off.json");
    same(
        off,
        Object.fromEntries(Object.entries(on).filter(([k]) => k !== "jvmObservation")),
        "OBSERVER_SINGLE_VARIABLE",
    );
    need(
        on.mode === "forward" &&
            on.purpose === "calibration" &&
            on.budget.cpus === 2 &&
            on.budget.memoryBytes === 2 * 1024 ** 3 &&
            on.budget.wallSeconds === 1500 &&
            on.image === runtime.driver.image &&
            on.jvmObservation?.parser?.timeoutMs === 10000,
        "OBSERVER_SETTINGS",
    );
    const build = await load("build.json");
    need(build.sourceCommit === O.commit, "OBSERVER_BUILD");
    const shared = {
        buildSha256: members.get("build.json").sha256,
        mode: "forward",
        sourceLockSha256: ref.sourceSelectionSha256,
        sameTargetAndDriverResources: true,
        cases: O.cases,
    };
    const policies = {};
    for (const group of ["J", "C"]) {
        policies[group] = await load("groups/" + group + "/policy.json");
        same(policies[group], observer003Policy(group, shared), "OBSERVER_POLICY");
    }
    const schedule = observer003Schedule(sequence.blockOrders);
    for (let i = 0; i < schedule.length; i++) {
        const a = sequence.physicalArms[i],
            expected = schedule[i];
        for (const [k, v] of Object.entries(expected)) same(a[k], v, "OBSERVER_ARM_SCHEDULE");
        need(
            a.runId === "hoc3-" + sequence.attemptToken + "-" + a.cellId.toLowerCase() &&
                a.directory === spec.planRoot + "/arms/" + a.cellId &&
                a.httpRequestId === a.runId + "-http" &&
                a.targetRequestId === a.runId + "-target" &&
                a.collectorRequestId === (a.collector ? a.runId + "-collector" : null),
            "OBSERVER_ARM_IDENTITY",
        );
        same(await load("arms/" + a.cellId + "/settings.json"), a.jvmObservation ? on : off, "OBSERVER_ARM_SETTINGS");
        verifyObserverProfile(await load("arms/" + a.cellId + "/profile.json"));
    }
    same(
        spec.expectedTasks,
        sequence.physicalArms.map((a) => ({
            runId: a.runId,
            http: taskId(a.httpRequestId),
            target: taskId(a.targetRequestId),
            collector: a.collector ? taskId(a.collectorRequestId) : null,
        })),
        "OBSERVER_TASK_IDS",
    );
    return { sequence, spec, source, members, policies, build, environment: await load("environment.json") };
}
const reply = (c) => {
    need(c.exitCode === 0 && c.error === null, "OBSERVER_COMMAND_FAILED");
    const v = JSON.parse(c.stdout);
    need(v.ok === true && v.data, "OBSERVER_COMMAND_REPLY");
    return v.data;
};
const match = (commands, argv) => commands.filter((c) => isDeepStrictEqual(c.argv, argv));
const once = (commands, argv) => {
    const a = match(commands, argv);
    need(a.length === 1 && a[0].exitCode === 0 && a[0].error === null, "OBSERVER_COMMAND_ONCE");
    return a[0];
};
async function readCommands(execution) {
    const names = (await readdir(join(execution, "commands"))).sort();
    need(names.length > 0 && names.length <= 8192, "OBSERVER_COMMAND_INVENTORY");
    const out = [];
    let end = 0n;
    for (let i = 0; i < names.length; i++) {
        need(names[i] === String(i + 1).padStart(5, "0") + ".json", "OBSERVER_COMMAND_INVENTORY");
        const c = await json(join(execution, "commands", names[i]), 2 * MiB);
        need(
            Array.isArray(c.argv) &&
                c.argv.length <= 256 &&
                c.argv.every((s) => typeof s === "string" && s.length <= 16384) &&
                typeof c.stdout === "string" &&
                typeof c.stderr === "string" &&
                Buffer.byteLength(c.stdout) + Buffer.byteLength(c.stderr) <= 2 * MiB &&
                Number.isInteger(c.exitCode) &&
                ns(c.startedMonotonicNs) >= end &&
                ns(c.finishedMonotonicNs) > ns(c.startedMonotonicNs),
            "OBSERVER_COMMAND_RECORD",
        );
        end = ns(c.finishedMonotonicNs);
        out.push({ ...c, index: i });
    }
    return out;
}
export function observer003Commands(a, c, ids) {
    const node = c.spec.runtime.nodeHome + "/bin/node",
        host = [node, c.spec.sourceRoot + "/dist/cli.js"],
        docker = c.spec.runtime.dockerExecutable,
        insideCli = [docker, "exec", "--user", c.spec.owner, ids.driver, "/node/bin/node", "/source/dist/cli.js"];
    const env = a.directory + "/environment",
        evidence = a.directory + "/driver-evidence",
        runtime = c.spec.runtime,
        d = runtime.driver;
    const create = [
        docker,
        "create",
        "--name",
        a.runId + "-driver",
        "--cidfile",
        a.directory + "/driver.id",
        "--pull=never",
        "--restart",
        "no",
        "--init",
        "--label",
        "org.apache.hugegraph.hgbench.observer-cost.request=" + a.runId,
        "--network",
        "container:" + ids.target,
        "--user",
        c.spec.owner,
        "--read-only",
        "--cap-drop",
        "ALL",
        "--security-opt",
        "no-new-privileges:true",
        "--cpus",
        "1",
        "--memory",
        String(d.memoryBytes),
        "--memory-swap",
        String(d.memoryBytes),
        "--pids-limit",
        String(d.pids),
        "--tmpfs",
        "/tmp:rw,nosuid,nodev,noexec,size=" + d.tmpfsBytes,
        "--log-driver",
        "none",
        "--stop-timeout",
        "5",
        "--workdir",
        "/source",
    ];
    for (const [source, dest, ro] of [
        [c.spec.sourceRoot, "/source", true],
        [runtime.nodeHome, "/node", true],
        [evidence, "/evidence", false],
    ])
        create.push("--mount", "type=bind,src=" + source + ",dst=" + dest + (ro ? ",readonly" : ""));
    create.push("--entrypoint", d.python, d.image, "-B", "-c", "import time; time.sleep(960)");
    const probe = [
        docker,
        "exec",
        "--user",
        c.spec.owner,
        ids.target,
        c.spec.runtime.driver.python,
        "-B",
        "/control/hugegraph-mode-probe.py",
        "--endpoint",
        "http://127.0.0.1:18080/gremlin",
        "--jcmd",
        ids.jcmd,
        "--java-pid",
        String(ids.java),
        "--expected-mode",
        "forward",
        "--total-timeout-seconds",
        "45",
    ];
    return {
        create,
        target: [
            ...host,
            "--root",
            a.directory + "/target-tasks",
            "task",
            "run",
            "--plan",
            env + "/task.json",
            "--detach",
            "--request-id",
            a.targetRequestId,
        ],
        ready: [...host, "http", "environment", "status", env],
        pre: [...probe, "--out", "/work/mode-pre"],
        post: [...probe, "--out", "/work/mode-post"],
        generate: [
            ...insideCli,
            "http",
            "generate",
            "/evidence/suite-settings.json",
            "--out",
            "/evidence/profile.json",
        ],
        plan: [...insideCli, "http", "plan", "/evidence/profile.json", "--out", "/evidence/http-plan"],
        http: [
            ...insideCli,
            "--root",
            "/evidence/tasks",
            "task",
            "run",
            "--plan",
            "/evidence/http-plan/task.json",
            "--request-id",
            a.httpRequestId,
        ],
        artifacts: [...insideCli, "--root", "/evidence/tasks", "task", "artifacts", "--id", taskId(a.httpRequestId)],
        start: [docker, "start", ids.driver],
        driverStop: [docker, "stop", "--time", "5", ids.driver],
        driverRemove: [docker, "rm", ids.driver],
        targetStop: [...host, "http", "environment", "stop", env],
        targetStatus: [
            ...host,
            "--root",
            a.directory + "/target-tasks",
            "task",
            "status",
            "--request-id",
            a.targetRequestId,
        ],
        targetRemove: [docker, "rm", ids.target],
        rebuild: [...host, "http", "report", evidence + "/http-plan/output", "--out", a.directory + "/rebuilt-http"],
    };
}
const RESOURCE_VERIFY_SCRIPT =
    'import {readFileSync} from "node:fs"; import {pathToFileURL} from "node:url"; const m=await import(pathToFileURL(process.argv[1])); console.log(JSON.stringify(await m.verifyResourceEvidence(process.argv[2],JSON.parse(readFileSync(process.argv[3],"utf8")))));';
const JVM_VERIFY_SCRIPT =
    'import {readFileSync} from "node:fs"; import {pathToFileURL} from "node:url"; const m=await import(pathToFileURL(process.argv[1])); console.log(JSON.stringify(await m.verifyJvmEvidence(process.argv[2],process.argv[3],JSON.parse(readFileSync(process.argv[4],"utf8")))));';
const JOB_PROPERTIES = ["Id", "LoadState", "ActiveState", "SubState", "MainPID", "ControlGroup", "Result"];
const REQUIRED_UNITS = [
    "hgbench-sf100-convert-v1.service",
    "hgbench-sf100-reference-convert-v1.service",
    "hgbench-sf100-update-audit-v1.service",
    "hgbench-http-aa-cohort-005.service",
    "hgbench-http-known-delay-002.service",
    "hgbench-http-driver-headroom-002.service",
    "hgbench-http-observer-cost-002.service",
];
const PROTECTED_UNIT = "hgbench-sf100-docker-environment-v6.service";
export function observerJobSnapshot(unit, raw, exitCode, protectedOnly) {
    need(exitCode === 0 && typeof raw === "string", "OBSERVER_UNIT_COMMAND");
    const fields = {};
    for (const line of raw.trimEnd().split("\n")) {
        const at = line.indexOf("=");
        need(at > 0 && !Object.hasOwn(fields, line.slice(0, at)), "OBSERVER_UNIT_FIELDS");
        fields[line.slice(0, at)] = line.slice(at + 1);
    }
    same(Object.keys(fields).sort(), [...JOB_PROPERTIES].sort(), "OBSERVER_UNIT_FIELDS");
    need(
        fields.Id === unit &&
            ["loaded", "not-found"].includes(fields.LoadState) &&
            /^(0|[1-9][0-9]*)$/.test(fields.MainPID),
        "OBSERVER_UNIT_IDENTITY",
    );
    const inactive =
        fields.ActiveState === "inactive" &&
        fields.SubState === "dead" &&
        fields.MainPID === "0" &&
        fields.ControlGroup === "";
    need((fields.LoadState !== "not-found" || inactive) && (protectedOnly || inactive), "OBSERVER_UNIT_STATE");
    return {
        unit,
        raw,
        observed: fields,
        protectedObservedOnly: protectedOnly,
        stateEvidence: inactive
            ? fields.LoadState === "not-found"
                ? "unit-absent"
                : "loaded-inactive"
            : "protected-state-observed",
        priorTaskSuccessProven: false,
        wholeHostExclusivityProven: false,
    };
}
const jobArgv = (unit) => ["/usr/bin/systemctl", "--user", "show", unit, ...JOB_PROPERTIES.flatMap((p) => ["-p", p])];
function unitList(ctx) {
    const units = ctx.sequence.declaredInactiveUnits;
    need(
        Array.isArray(units) &&
            units.length <= 256 &&
            units.every((u) => /^[A-Za-z0-9_.@-]+\.service$/.test(u)) &&
            REQUIRED_UNITS.every((u) => units.includes(u)) &&
            !units.includes(PROTECTED_UNIT) &&
            ctx.sequence.protectedObservedUnit === PROTECTED_UNIT,
        "OBSERVER_UNIT_SCOPE",
    );
    return [...new Set(units), PROTECTED_UNIT];
}
export function observer003AllowedCommands(a, c, ids) {
    const ops = observer003Commands(a, c, ids),
        env = a.directory + "/environment",
        host = [c.spec.runtime.nodeHome + "/bin/node", c.spec.sourceRoot + "/dist/cli.js"],
        specHost = [c.spec.node, c.spec.sourceRoot + "/dist/cli.js"],
        out = Object.values(ops);
    for (const id of [ids.target, ids.driver, a.runId + "-driver"])
        out.push([c.spec.runtime.dockerExecutable, "inspect", "--type", "container", id]);
    if (a.collector)
        out.push(
            [
                ...host,
                "task",
                "plan",
                a.directory + "/collector-task.source.json",
                "--out",
                a.directory + "/collector-task.json",
            ],
            [
                ...host,
                "--root",
                a.directory + "/collector-tasks",
                "task",
                "run",
                "--plan",
                a.directory + "/collector-task.json",
                "--detach",
                "--request-id",
                a.collectorRequestId,
            ],
            [
                ...specHost,
                "--root",
                a.directory + "/collector-tasks",
                "task",
                "status",
                "--request-id",
                a.collectorRequestId,
            ],
            [
                ...host,
                "--root",
                a.directory + "/collector-tasks",
                "task",
                "artifacts",
                "--id",
                taskId(a.collectorRequestId),
            ],
            [
                c.spec.node,
                "--input-type=module",
                "-e",
                RESOURCE_VERIFY_SCRIPT,
                c.spec.sourceRoot + "/components/http/resource-evidence.mjs",
                a.directory + "/collector",
                a.directory + "/collector-verification-context.json",
            ],
        );
    // wait_original uses the canonical node binary; recipe template commands retain node-home/bin/node.
    out.push([
        ...specHost,
        "--root",
        a.directory + "/target-tasks",
        "task",
        "status",
        "--request-id",
        a.targetRequestId,
    ]);
    if (a.jvmObservation)
        out.push(
            [
                c.spec.runtime.hostPython,
                "-B",
                env + "/jvm-observation.py",
                "--json",
                "parse",
                "--plan",
                env + "/jvm-observation",
                "--raw",
                env + "/work/server.log",
                "--launch",
                env + "/work/jvm-launch.json",
                "--capture",
                env + "/work/jvm-capture.json",
                "--out",
                a.directory + "/jvm-rebuilt",
            ],
            [
                c.spec.node,
                "--input-type=module",
                "-e",
                JVM_VERIFY_SCRIPT,
                c.spec.sourceRoot + "/components/http/jvm-evidence.mjs",
                env,
                a.directory + "/jvm-rebuilt",
                a.directory + "/jvm-verification-context.json",
            ],
        );
    return out;
}
export function verifyObserverCommandClosure(commands, ctx, dispatch, arms) {
    need(
        arms.length === 16 && arms.every((a) => a.valid && Array.isArray(a.declaredCommands)),
        "OBSERVER_COMMAND_CLOSURE_ARMS",
    );
    const units = unitList(ctx),
        allowed = arms.flatMap((a) => a.declaredCommands).concat(units.map(jobArgv));
    for (const group of ["J", "C"]) {
        const argv = [
            ctx.spec.node,
            ctx.spec.sourceRoot + "/components/paired/analyzer.mjs",
            "--input",
            dispatch.executionRoot + "/group-" + group + "/verified-observations.json",
            "--output",
            dispatch.executionRoot + "/group-" + group + "/paired-analysis",
        ];
        const c = once(commands, argv);
        need(c.index > Math.max(...arms.map((a) => a.lastCommandIndex)), "OBSERVER_GROUP_ANALYSIS_ORDER");
        allowed.push(argv);
    }
    for (const unit of units) need(match(commands, jobArgv(unit)).length === 32, "OBSERVER_UNIT_COMMAND_COUNT");
    for (const c of commands)
        need(
            allowed.some((a) => isDeepStrictEqual(a, c.argv)),
            "OBSERVER_UNDECLARED_COMMAND",
        );
    // An inspect-absent receipt is the only expected nonzero command on the complete normal path.
    const absentIds = new Set(arms.flatMap((a) => [a.targetCid, a.driverCid]));
    for (const c of commands.filter((c) => c.exitCode !== 0 || c.error !== null))
        need(
            c.error === null &&
                c.argv.length === 5 &&
                c.argv[0] === ctx.spec.runtime.dockerExecutable &&
                c.argv[1] === "inspect" &&
                c.argv[2] === "--type" &&
                c.argv[3] === "container" &&
                absentIds.has(c.argv[4]) &&
                /^(?:|\[\s*\])$/.test(c.stdout.trim()) &&
                new RegExp(
                    "^Error(?: response from daemon)?: No such (?:object|container): " + c.argv[4] + "\\s*$",
                    "i",
                ).test(c.stderr.trim()),
            "OBSERVER_COMMAND_FAILURE",
        );
}
async function verifyJobs(execution, arm, ctx, commands, start, finish) {
    const units = unitList(ctx),
        used = [];
    for (const phase of ["pre", "post"]) {
        const saved = await json(join(execution, arm.runId + "-jobs-" + phase + ".json"));
        need(Array.isArray(saved) && saved.length === units.length, "OBSERVER_UNIT_RECEIPTS");
        for (let i = 0; i < units.length; i++) {
            const candidates = match(commands, jobArgv(units[i])).filter((c) =>
                phase === "pre"
                    ? c.index < start && c.index > finish.previous
                    : c.index > finish.current && c.index < finish.next,
            );
            need(candidates.length > 0, "OBSERVER_UNIT_COMMAND_WINDOW");
            const c = phase === "pre" ? candidates.at(-1) : candidates[0];
            same(
                saved[i],
                observerJobSnapshot(units[i], c.stdout, c.exitCode, units[i] === PROTECTED_UNIT),
                "OBSERVER_UNIT_RECEIPT",
            );
            used.push(c.index);
        }
    }
    return used;
}
export function verifyObserverJobUsage(arms) {
    const used = arms.flatMap((a) => a.jobCommandIndices);
    need(
        used.length > 0 && used.every(Number.isInteger) && new Set(used).size === used.length,
        "OBSERVER_UNIT_RECORD_REUSED",
    );
}
export function verifyObserverTargetReceipt(receipt, sealed) {
    same(
        {
            Id: receipt.Id,
            Name: receipt.Name,
            Image: receipt.Image,
            User: receipt.Config?.User,
            Labels: receipt.Config?.Labels,
        },
        {
            Id: sealed.Id,
            Name: sealed.Name,
            Image: sealed.Image,
            User: sealed.Config?.User,
            Labels: sealed.Config?.Labels,
        },
        "OBSERVER_TARGET_FINAL_IDENTITY",
    );
    need(
        receipt.State?.Status === "exited" &&
            receipt.State.Running === false &&
            receipt.State.OOMKilled === false &&
            receipt.State.ExitCode === 0,
        "OBSERVER_TARGET_FINAL_STATE",
    );
}
export function verifyObserverDriverCommandCid(receipt, expectedArgv, driverId) {
    need(
        SHA.test(driverId) &&
            receipt?.exitCode === 0 &&
            receipt.error === null &&
            Array.isArray(expectedArgv) &&
            isDeepStrictEqual(receipt.argv, expectedArgv) &&
            typeof receipt.stdout === "string",
        "OBSERVER_DRIVER_COMMAND_CID",
    );
    const driverStop =
        expectedArgv.length === 5 && isDeepStrictEqual(expectedArgv.slice(1), ["stop", "--time", "5", driverId]);
    // Docker emits this exact --time warning on stdout in the original003 receipts.
    // Keep all other commands and warning/output forms subject to the original CID check.
    need(
        receipt.stdout.trim() === driverId ||
            (driverStop &&
                receipt.stdout === "Flag --time has been deprecated, use --timeout instead\n" + driverId + "\n"),
        "OBSERVER_DRIVER_COMMAND_CID",
    );
}
function absent(commands, docker, id, after) {
    const rows = match(commands, [docker, "inspect", "--type", "container", id]).filter((r) => r.index > after);
    need(
        rows.length === 1 &&
            rows[0].exitCode !== 0 &&
            rows[0].error === null &&
            /^(?:|\[\s*\])$/.test(rows[0].stdout.trim()) &&
            new RegExp("^Error(?: response from daemon)?: No such (?:object|container): " + id + "\\s*$", "i").test(
                rows[0].stderr.trim(),
            ),
        "OBSERVER_CID_ABSENCE",
    );
    return rows[0];
}
export function verifyObserverAnchor(record, identity, inspect, owner, role) {
    need(
        record.hostPid === inspect.State.Pid &&
            record.hostPid === identity.pid &&
            record.role === role &&
            record.statRaw.startsWith(String(identity.pid) + " ("),
        "OBSERVER_ANCHOR_PID",
    );
    const fields = record.statRaw
        .slice(record.statRaw.lastIndexOf(")") + 1)
        .trim()
        .split(/\s+/);
    need(
        fields.length >= 20 && !["Z", "X", "x"].includes(fields[0]) && fields[19] === String(identity.startTimeTicks),
        "OBSERVER_ANCHOR_START",
    );
    for (const [label, expected] of [
        ["Uid", owner.split(":")[0]],
        ["Gid", owner.split(":")[1]],
    ]) {
        const matches = [...record.statusRaw.matchAll(new RegExp("^" + label + ":\\s+(.+)$", "gm"))];
        need(matches.length === 1 && matches[0][1].trim().split(/\s+/)[1] === expected, "OBSERVER_ANCHOR_OWNER");
    }
    const cg = record.cgroupRaw.split(/\r?\n/).filter((l) => l.startsWith("0::"));
    need(cg.length === 1, "OBSERVER_ANCHOR_CGROUP");
    return { hostPid: identity.pid, startTimeTicks: identity.startTimeTicks, cgroupMember: cg[0].slice(3) };
}
async function collectorArm(dir, arm, ctx, commands, lifecycle, dispatch) {
    if (!arm.collector) {
        const names = await readdir(dir);
        need(
            !names.some((n) => n.startsWith("collector") && n !== "collector-off.json"),
            "OBSERVER_COLLECTOR_OFF_DATA",
        );
        need(!commands.some((c) => c.argv.includes(arm.runId + "-collector")), "OBSERVER_COLLECTOR_OFF_COMMAND");
        return { enabled: false, verified: true };
    }
    const spec = await json(join(dir, "collector-spec.json")),
        template = await json(join(dir, "collector-binding-template.json")),
        anchors = await json(join(dir, "collector-anchor-before.json")),
        first = await json(join(dir, "collector-first-sample.json")),
        saved = await json(join(dir, "collector-verified.json")),
        task = await json(join(dir, "collector-task.json"));
    const expectedSpec = Object.fromEntries(
        Object.entries(template).filter(([k]) => !["kind", "dispatchable", "roles"].includes(k)),
    );
    expectedSpec.kind = "http-resource-collection";
    expectedSpec.roles = ["target", "driver"].map((role) => ({ role, hostPid: anchors.identities[role].pid }));
    same(spec, expectedSpec, "OBSERVER_COLLECTOR_BINDING");
    need(
        spec.durationMs === 800000 &&
            spec.intervalMs === 1000 &&
            spec.maxSamples === 801 &&
            spec.maxOutputBytes === 32 * MiB &&
            spec.filesystemPath === ctx.spec.planRoot &&
            template.dispatchable === false,
        "OBSERVER_COLLECTOR_BUDGET",
    );
    const stop = await json(join(dir, "collector-stop.json"));
    need(
        ns(stop.workflowFinishedMonotonicNs) >= ns(lifecycle.http.finishedMonotonicNs) &&
            ns(stop.stopRequestedMonotonicNs) >= ns(stop.workflowFinishedMonotonicNs),
        "OBSERVER_COLLECTOR_STOP_WINDOW",
    );
    const expectedRoles = {};
    for (const role of ["target", "driver"]) {
        const id = anchors.identities[role];
        need(id.bootId === dispatch.hostBootId, "OBSERVER_COLLECTOR_BOOT");
        same(first.identities[role], anchors.identities[role], "OBSERVER_FIRST_IDENTITY");
        same(saved.anchorAfter.identities[role], anchors.identities[role], "OBSERVER_END_IDENTITY");
        const inspected = lifecycle.inspectRunning(role);
        expectedRoles[role] = {
            ...verifyObserverAnchor(anchors.records[role], id, inspected, ctx.spec.owner, role),
            limits: {
                cpus: role === "target" ? 2 : 1,
                memoryBytes: role === "target" ? 2 * 1024 ** 3 : 805306368,
                pids: role === "target" ? 256 : 128,
            },
        };
        verifyObserverAnchor(first.records[role], id, inspected, ctx.spec.owner, role);
        verifyObserverAnchor(saved.anchorAfter.records[role], id, inspected, ctx.spec.owner, role);
    }
    verifyObserverTask(task, saved.terminal, arm.collectorRequestId);
    need(
        ns(first.sample.finishedMonotonicNs) <= ns(lifecycle.http.startedMonotonicNs),
        "OBSERVER_FIRST_SAMPLE_AFTER_HTTP",
    );
    const taskSource = await json(join(dir, "collector-task.source.json")),
        expectedTaskSource = {
            schemaVersion: 1,
            kind: "component-task",
            name: "observer-cost-collector",
            command: [
                ctx.spec.runtime.hostPython,
                "-B",
                ctx.spec.sourceRoot + "/components/http/resource-collector.py",
                "collect",
                "--spec",
                arm.directory + "/collector-spec.json",
                "--out",
                arm.directory + "/collector",
                "--json",
            ],
            cwd: arm.directory,
            trackedFiles: [
                ctx.spec.sourceRoot + "/components/http/resource-collector.py",
                arm.directory + "/collector-spec.json",
            ],
            effect: "filesystem",
            timeoutMs: 810000,
            terminateGraceMs: 5000,
            maxLogBytes: MiB,
        };
    same(taskSource, expectedTaskSource, "OBSERVER_COLLECTOR_TASK_SOURCE_PLAN");
    need(pathOk(task.plan.command[0]) && task.plan.command[0].startsWith("/"), "OBSERVER_COLLECTOR_PYTHON");
    same(
        task.plan,
        { ...expectedTaskSource, command: [task.plan.command[0], ...expectedTaskSource.command.slice(1)] },
        "OBSERVER_COLLECTOR_FROZEN_TASK",
    );
    const host = [ctx.spec.node, ctx.spec.sourceRoot + "/dist/cli.js"],
        root = arm.directory + "/collector-tasks";
    const startArgs = [
        ...host,
        "--root",
        root,
        "task",
        "run",
        "--plan",
        arm.directory + "/collector-task.json",
        "--detach",
        "--request-id",
        arm.collectorRequestId,
    ];
    // Recipe uses the selected node-home entry; original() uses spec.node for subsequent status queries.
    const startRows = match(commands, startArgs).concat(
        ctx.spec.node === ctx.spec.runtime.nodeHome + "/bin/node"
            ? []
            : match(commands, [ctx.spec.runtime.nodeHome + "/bin/node", ...startArgs.slice(1)]),
    );
    const planCommand = [
        ctx.spec.runtime.nodeHome + "/bin/node",
        ctx.spec.sourceRoot + "/dist/cli.js",
        "task",
        "plan",
        arm.directory + "/collector-task.source.json",
        "--out",
        arm.directory + "/collector-task.json",
    ];
    const planned = once(commands, planCommand);
    need(
        startRows.length === 1 &&
            planned.index < startRows[0].index &&
            startRows[0].index < lifecycle.http.index &&
            reply(startRows[0]).planDigest === task.digest &&
            reply(startRows[0]).taskId === taskId(arm.collectorRequestId) &&
            !reply(startRows[0]).reused,
        "OBSERVER_COLLECTOR_SUBMIT",
    );
    const statusArgs = [...host, "--root", root, "task", "status", "--request-id", arm.collectorRequestId];
    need(
        match(commands, statusArgs).some(
            (c) =>
                c.index > lifecycle.http.index &&
                c.index < lifecycle.driverStop.index &&
                isDeepStrictEqual(reply(c), saved.terminal),
        ),
        "OBSERVER_COLLECTOR_TERMINAL_COMMAND",
    );
    const artifacts = [
            ctx.spec.runtime.nodeHome + "/bin/node",
            ctx.spec.sourceRoot + "/dist/cli.js",
            "--root",
            root,
            "task",
            "artifacts",
            "--id",
            taskId(arm.collectorRequestId),
        ],
        artifact = once(commands, artifacts),
        independent = once(commands, [
            ctx.spec.node,
            "--input-type=module",
            "-e",
            RESOURCE_VERIFY_SCRIPT,
            ctx.spec.sourceRoot + "/components/http/resource-evidence.mjs",
            arm.directory + "/collector",
            arm.directory + "/collector-verification-context.json",
        ]);
    need(
        artifact.index > lifecycle.http.index &&
            independent.index > artifact.index &&
            independent.index < lifecycle.driverStop.index,
        "OBSERVER_COLLECTOR_REVIEW_ORDER",
    );
    const sourceEntry = ctx.source.files.find((f) => f.path === "components/http/resource-collector.py");
    need(
        task.files?.find((f) => f.path === ctx.spec.sourceRoot + "/components/http/resource-collector.py")?.sha256 ===
            sourceEntry.sha256 &&
            task.files.find((f) => f.path === arm.directory + "/collector-spec.json")?.sha256 ===
                hash(await bytes(join(dir, "collector-spec.json"))),
        "OBSERVER_COLLECTOR_TASK_SOURCE",
    );
    const verified = await verifyResourceEvidence(join(dir, "collector"), {
        hostBootId: dispatch.hostBootId,
        expectedSpec,
        expectedRoles,
        componentSha256: sourceEntry.sha256,
        stopMarkerPath: join(dir, "collector-stop.json"),
        workflowWindow: [lifecycle.http.startedMonotonicNs, stop.workflowFinishedMonotonicNs],
    });
    need(
        verified.sourceOrigin === "linux-procfs" &&
            verified.qualityIssues.length === 0 &&
            verified.coverage === "observed-metrics-present",
        "OBSERVER_COLLECTOR_QUALITY",
    );
    const firstLine = (await bytes(join(dir, "collector/samples.ndjson"), 32 * MiB)).toString("utf8").split("\n", 1)[0];
    same(JSON.parse(firstLine), first.sample, "OBSERVER_FIRST_SAMPLE_RAW");
    return {
        enabled: true,
        verified: true,
        sampleCount: verified.sampleCount,
        summarySha256: verified.summarySha256,
        observedResources: verified.observedResources,
        clockAlignment: "unverified",
        coverageScope: "host foreground CLI envelope, not raw phase timestamps",
    };
}
async function verifyArm(plan, execution, arm, ctx, commands, dispatch) {
    const dir = join(plan, "arms", arm.cellId),
        raw = join(dir, "driver-evidence/http-plan/output"),
        report = await analyzeHttp(raw),
        expected = await json(join(dir, "profile.json"));
    expected.target.evidenceFiles = expected.target.evidenceFiles.map((p) => "/evidence/" + basename(p));
    same(report.profile, expected, "OBSERVER_RAW_PROFILE");
    verifyObserverProfile(report.profile);
    need(
        report.runs.length === 3 && report.gates.correctness === "passed" && report.gates.sampleAccounting === "passed",
        "OBSERVER_HTTP_GATES",
    );
    const values = {};
    for (let i = 0; i < O.cases.length; i++) {
        const r = report.runs[i];
        need(
            r.caseId === O.cases[i] &&
                r.loadId === "open-10" &&
                r.repeat === 0 &&
                r.scheduled === 1200 &&
                r.correct >= 1000 &&
                r.failed === 0 &&
                r.dropped === 0 &&
                r.late === 0 &&
                r.issues.length === 0 &&
                r.correctEndToEndMs.count === r.correct &&
                r.correctResponseMs.count === r.correct,
            "OBSERVER_HTTP_QUALITY",
        );
        for (const [metric, value] of [
            ["correctEndToEndMs.p99", r.correctEndToEndMs.p99],
            ["correctResponseMs.p50", r.correctResponseMs.p50],
        ])
            values[r.caseId + "." + metric] = { value, observations: r.correct };
    }
    const implementation = await json(join(raw, "implementation.json")),
        httpTask = await json(join(dir, "driver-evidence/http-plan/task.json"));
    const required = [
        "workflow.mjs",
        "runner.mjs",
        "trace-writer.mjs",
        "direct-phase-clock.mjs",
        "runner-sanity.mjs",
        "phase-clock-reference.py",
        "clock-correlation.mjs",
        "analyzer.mjs",
        "common.mjs",
        "suite-evidence.mjs",
        "probe.mjs",
    ];
    same(implementation.files.map((f) => f.file).sort(), required.sort(), "OBSERVER_IMPLEMENTATION_INVENTORY");
    need(implementation.nodeExecutableSha256 === ctx.spec.nodeExecutable.sha256, "OBSERVER_ACTUAL_NODE");
    for (const f of implementation.files)
        need(
            f.sha256 === ctx.source.files.find((s) => s.path === "components/http/" + f.file)?.sha256 &&
                httpTask.files.find((s) => s.path === "/source/components/http/" + f.file)?.sha256 === f.sha256,
            "OBSERVER_SOURCE_BINDING",
        );
    need(
        httpTask.plan.timeoutMs === 609000 &&
            httpTask.plan.terminateGraceMs === 5000 &&
            httpTask.plan.effect === "database",
        "OBSERVER_HTTP_TASK_BUDGET",
    );
    same(
        httpTask.plan.command,
        ["/node/bin/node", "/source/components/http/workflow.mjs", "--plan", "/evidence/http-plan"],
        "OBSERVER_HTTP_TASK_COMMAND",
    );
    need(
        httpTask.files.find((f) => f.path === "/node/bin/node")?.sha256 === ctx.spec.nodeExecutable.sha256,
        "OBSERVER_TASK_NODE_BINDING",
    );
    for (const name of ["profile.json", "target-evidence.json"]) {
        const inputRaw = await bytes(join(dir, "driver-evidence/http-plan", name));
        need(
            hash(inputRaw) === hash(await bytes(join(raw, name))) &&
                httpTask.files.find((f) => f.path === "/evidence/http-plan/" + name)?.sha256 === hash(inputRaw),
            "OBSERVER_TASK_INPUT_BINDING",
        );
    }
    const target = await json(join(raw, "target-evidence.json")),
        attached = [
            "build.json",
            "environment.json",
            "source-verification.json",
            "scope.json",
            "arms/" + arm.cellId + "/settings.json",
            "arms/" + arm.cellId + "/arm-context.json",
        ];
    same(
        target.files.map((f) => ({ bytes: f.bytes, sha256: f.sha256 })),
        attached.map((n) => {
            const f = ctx.members.get(n);
            need(f, "OBSERVER_TARGET_REQUIRED");
            return { bytes: f.bytes, sha256: f.sha256 };
        }),
        "OBSERVER_TARGET_ATTACHMENTS",
    );
    const settings = await json(join(dir, "settings.json")),
        normalized = structuredClone(settings);
    delete normalized.mode;
    const env = await verifyControlledEnvironment(
        join(dir, "environment"),
        report,
        { build: ctx.build, settings: normalized },
        "forward",
        dispatch,
    );
    const receipt = (suffix) => json(join(execution, arm.runId + suffix + ".json"));
    const ready = await receipt("-ready"),
        final = await receipt("-driver-final"),
        targetFinal = await receipt("-target-final"),
        beforeDriver = await receipt("-driver-prestop"),
        beforeTarget = await receipt("-target-prestop"),
        httpTerminal = await receipt("-http-terminal"),
        targetTerminal = await receipt("-target-terminal");
    same(ready.ready, await json(join(dir, "environment/work/ready.json")), "OBSERVER_READY");
    verifyObserverTargetReceipt(targetFinal, await json(join(dir, "environment/container-final.json")));
    const driverId = observer003Driver(final, arm, ctx, env.containerId);
    observer003Driver(beforeDriver, arm, ctx, env.containerId, driverId);
    need(
        final.State.Running === false &&
            final.State.OOMKilled === false &&
            [0, 143, -15].includes(final.State.ExitCode) &&
            beforeDriver.State.Running === true &&
            !beforeDriver.State.OOMKilled,
        "OBSERVER_DRIVER_NORMAL_STOP",
    );
    same(
        {
            Id: beforeTarget.Id,
            Name: beforeTarget.Name,
            Image: beforeTarget.Image,
            User: beforeTarget.Config?.User,
            Labels: beforeTarget.Config?.Labels,
        },
        {
            Id: targetFinal.Id,
            Name: targetFinal.Name,
            Image: targetFinal.Image,
            User: targetFinal.Config?.User,
            Labels: targetFinal.Config?.Labels,
        },
        "OBSERVER_TARGET_PRESTOP_IDENTITY",
    );
    need(
        beforeTarget.State?.Running === true && beforeTarget.State.OOMKilled === false,
        "OBSERVER_TARGET_PRESTOP_STATE",
    );
    verifyObserverTask(httpTask, httpTerminal, arm.httpRequestId);
    verifyObserverTask(await json(join(dir, "environment/task.json")), targetTerminal, arm.targetRequestId);
    const ops = observer003Commands(arm, ctx, {
            driver: driverId,
            target: env.containerId,
            java: ready.ready.java.pid,
            jcmd: ready.ready.jcmdExecutable,
        }),
        selected = {};
    for (const id of [
        "target",
        "pre",
        "create",
        "start",
        "generate",
        "plan",
        "http",
        "artifacts",
        "driverStop",
        "driverRemove",
        "post",
        "targetStop",
        "targetRemove",
        "rebuild",
    ])
        selected[id] = once(commands, ops[id]);
    const ordered = Object.values(selected);
    need(
        ordered.every((r, i) => !i || ordered[i - 1].index < r.index),
        "OBSERVER_COMMAND_ORDER",
    );
    same(reply(selected.http), httpTerminal, "OBSERVER_HTTP_TERMINAL_COMMAND");
    for (const key of ["create", "start", "driverStop", "driverRemove"])
        verifyObserverDriverCommandCid(selected[key], ops[key], driverId);
    need(selected.targetRemove.stdout.trim() === env.containerId, "OBSERVER_TARGET_REMOVE_CID");
    need(
        reply(selected.target).taskId === taskId(arm.targetRequestId) &&
            reply(selected.target).planDigest === targetTerminal.planDigest &&
            !reply(selected.target).reused,
        "OBSERVER_TARGET_SUBMIT",
    );
    need(
        match(commands, ops.ready).some(
            (c) =>
                c.index > selected.target.index && c.index < selected.pre.index && isDeepStrictEqual(reply(c), ready),
        ),
        "OBSERVER_READY_COMMAND",
    );
    need(
        commands
            .filter(
                (c) =>
                    isDeepStrictEqual(c.argv, ops.targetStatus) ||
                    isDeepStrictEqual(c.argv, [ctx.spec.node, ...ops.targetStatus.slice(1)]),
            )
            .some(
                (c) =>
                    c.index > selected.targetStop.index &&
                    c.index < selected.targetRemove.index &&
                    isDeepStrictEqual(reply(c), targetTerminal),
            ),
        "OBSERVER_TARGET_TERMINAL_COMMAND",
    );
    const docker = ctx.spec.runtime.dockerExecutable;
    const inspectRunning = (role) => {
        const id = role === "target" ? env.containerId : driverId;
        const rows = match(commands, [docker, "inspect", "--type", "container", id]).filter(
            (c) =>
                c.index > selected.start.index &&
                c.index < selected.driverStop.index &&
                c.exitCode === 0 &&
                c.error === null,
        );
        need(rows.length > 0, "OBSERVER_RUNNING_INSPECT");
        const infos = rows.map((c) => JSON.parse(c.stdout)[0]);
        const info = infos.find((v) => v.Id === id && v.State.Running && !v.State.OOMKilled);
        need(info, "OBSERVER_RUNNING_INSPECT");
        if (role === "driver") observer003Driver(info, arm, ctx, env.containerId, driverId);
        return info;
    };
    need(
        match(commands, [docker, "inspect", "--type", "container", driverId]).some(
            (c) =>
                c.index > selected.driverStop.index &&
                c.index < selected.driverRemove.index &&
                c.exitCode === 0 &&
                c.error === null &&
                isDeepStrictEqual(JSON.parse(c.stdout), [final]),
        ),
        "OBSERVER_DRIVER_FINAL_COMMAND",
    );
    need(
        match(commands, [docker, "inspect", "--type", "container", env.containerId]).some(
            (c) =>
                c.index > selected.targetStop.index &&
                c.index < selected.targetRemove.index &&
                c.exitCode === 0 &&
                c.error === null &&
                isDeepStrictEqual(JSON.parse(c.stdout), [targetFinal]),
        ),
        "OBSERVER_TARGET_FINAL_COMMAND",
    );
    absent(commands, docker, driverId, selected.driverRemove.index);
    const last = absent(commands, docker, env.containerId, selected.targetRemove.index);
    const startIndices = ctx.sequence.physicalArms
        .map((a) =>
            match(
                commands,
                observer003Commands(a, ctx, {
                    driver: driverId,
                    target: env.containerId,
                    java: ready.ready.java.pid,
                    jcmd: ready.ready.jcmdExecutable,
                }).target,
            ),
        )
        .map((v) => v[0]?.index)
        .filter(Number.isInteger)
        .sort((a, b) => a - b);
    const at = startIndices.indexOf(selected.target.index);
    const prevStart = at ? startIndices[at - 1] : -1,
        nextStart = startIndices[at + 1] ?? commands.length;
    const jobCommandIndices = await verifyJobs(execution, arm, ctx, commands, selected.target.index, {
        previous: prevStart,
        current: selected.rebuild.index,
        next: nextStart,
    });
    for (const [id, expectedInfo] of [
        [driverId, beforeDriver],
        [env.containerId, beforeTarget],
    ])
        need(
            match(commands, [docker, "inspect", "--type", "container", id]).some(
                (c) =>
                    c.index > selected.http.index &&
                    c.index < selected.driverStop.index &&
                    c.exitCode === 0 &&
                    c.error === null &&
                    isDeepStrictEqual(JSON.parse(c.stdout), [expectedInfo]),
            ),
            "OBSERVER_PRESTOP_COMMAND",
        );
    const lifecycle = { ...selected, inspectRunning };
    const declaredCommands = observer003AllowedCommands(arm, ctx, {
        driver: driverId,
        target: env.containerId,
        java: ready.ready.java.pid,
        jcmd: ready.ready.jcmdExecutable,
    });
    const collector = await collectorArm(dir, arm, ctx, commands, lifecycle, dispatch);
    let jvm;
    if (arm.jvmObservation) {
        const extra = declaredCommands.slice(-2),
            parsed = once(commands, extra[0]),
            rechecked = once(commands, extra[1]);
        need(parsed.index > selected.rebuild.index && rechecked.index > parsed.index, "OBSERVER_JVM_COMMAND_ORDER");
        jvm = await verifyJvmEvidence(join(dir, "environment"), join(dir, "jvm-rebuilt"), {
            settings,
            hostBootId: dispatch.hostBootId,
            environmentPlanSha256: env.planSha,
            processIdentity: { ...env.pre.identity, hostBootId: dispatch.hostBootId },
        });
        need(
            jvm.evidenceIntegrity === "passed" &&
                jvm.parserRecomputed === true &&
                jvm.captureComplete === true &&
                jvm.rawComplete === true &&
                jvm.observationStatus === "completed",
            "OBSERVER_JVM_QUALITY",
        );
        jvm = {
            enabled: true,
            verified: true,
            captureComplete: true,
            parserRecomputed: true,
            clockAlignment: "unverified",
            phaseAttribution: "unavailable",
        };
    } else {
        need(!settings.jvmObservation, "OBSERVER_JVM_OFF_SETTING");
        const files = await readdir(join(dir, "environment/work"));
        need(
            !files.some((n) => ["jvm-launch.json", "jvm-runtime.json", "jvm-capture.json"].includes(n)),
            "OBSERVER_JVM_OFF_DATA",
        );
        jvm = { enabled: false, verified: true, applicationStdoutStillCaptured: true };
    }
    return {
        runId: arm.runId,
        group: arm.group,
        blockId: arm.blockId,
        arm: arm.arm,
        valid: true,
        issues: [],
        values,
        rawManifestSha256: hash(await bytes(join(raw, "raw.lock.json"))),
        targetCid: env.containerId,
        driverCid: driverId,
        start: selected.target.startedMonotonicNs,
        end: last.finishedMonotonicNs,
        declaredCommands,
        jobCommandIndices,
        lastCommandIndex: Math.max(
            ...commands
                .filter((c) => declaredCommands.some((argv) => isDeepStrictEqual(argv, c.argv)))
                .map((c) => c.index),
        ),
        javaIdentity: {
            bootId: env.pre.identity.bootId,
            pid: env.pre.identity.pid,
            startTimeTicks: env.pre.identity.startTimeTicks,
        },
        collector,
        jvm,
    };
}
const limitations = [
    "Only the three declared cases/open10/forward/source/resource stratum; other eight cases remain unknown.",
    "Two conditional effects cannot be added. A measured nonzero cost is disclosed, not automatically rejected.",
    "Correctness and trace stay on; their counterfactual whole-system cost is unknown. Unit self-times do not establish throughput without them.",
    "Raw phase-to-host/JVM clock, GC/JIT causal attribution, general capacity and empirical bootstrap coverage are unqualified.",
    "Source inventory and measured components are digest-bound; full physical source/dependency/runtime provenance is not independently reread or attested.",
    "Declared predecessor unit inactivity is observed; prior task success and whole-host exclusivity are not inferred from unit absence.",
];
async function worker(inputFile, out, maxBytes) {
    const inputRaw = await bytes(inputFile, MiB),
        input = validateObserverCostInput(JSON.parse(inputRaw)),
        plan = await realpath(resolve(dirname(inputFile), input.planDirectory)),
        execution = await realpath(resolve(dirname(inputFile), input.executionDirectory));
    const beforePlan = await inventory(plan, INPUT_CAP),
        beforeExecution = await inventory(execution, INPUT_CAP - beforePlan.bytes);
    const ctx = await planContext(plan, execution, input.reference),
        issues = [],
        arms = ctx.sequence.physicalArms.map((a) => ({
            runId: a.runId,
            group: a.group,
            blockId: a.blockId,
            arm: a.arm,
            valid: false,
            issues: ["OBSERVER_ARM_UNVERIFIED"],
        }));
    try {
        need(ctx.spec.runnable === true && ctx.sequence.mode === "prepare", "OBSERVER_NOT_EXECUTED");
        const dispatch = await json(join(execution, "dispatch.json"));
        same(await json(join(plan, "observer-cost-dispatch.json")), dispatch, "OBSERVER_DISPATCH_CLAIM");
        need(
            dispatch.executed === true &&
                dispatch.executionSpecSha256 === input.reference.executionSpecSha256 &&
                /^[a-f0-9-]{36}$/.test(dispatch.hostBootId) &&
                pathOk(dispatch.executionRoot) &&
                dispatch.executionRoot.startsWith("/"),
            "OBSERVER_DISPATCH",
        );
        const commands = await readCommands(execution);
        need(
            ns(dispatch.startedMonotonicNs) <= ns(commands[0].startedMonotonicNs) &&
                ns(commands.at(-1).finishedMonotonicNs) - ns(dispatch.startedMonotonicNs) <= 30000000n * 1000000n,
            "OBSERVER_DISPATCH_TIME",
        );
        for (let i = 0; i < arms.length; i++)
            try {
                arms[i] = await verifyArm(plan, execution, ctx.sequence.physicalArms[i], ctx, commands, dispatch);
            } catch (e) {
                arms[i].issues = [e.code ?? e.message];
            }
        if (arms.every((a) => a.valid)) {
            verifyObserverCommandClosure(commands, ctx, dispatch, arms);
            verifyObserverJobUsage(arms);
        }
        const seen = new Set();
        let end = 0n;
        for (const a of arms.filter((r) => r.valid)) {
            for (const id of [
                "raw:" + a.rawManifestSha256,
                "cid:" + a.targetCid,
                "cid:" + a.driverCid,
                "java:" + a.javaIdentity.bootId + ":" + a.javaIdentity.pid + ":" + a.javaIdentity.startTimeTicks,
            ]) {
                if (seen.has(id)) {
                    a.valid = false;
                    a.issues.push("OBSERVER_IDENTITY_REUSED");
                }
                seen.add(id);
            }
            if (ns(a.start) < end) {
                a.valid = false;
                a.issues.push("OBSERVER_OVERLAPPING_ARMS");
            }
            end = ns(a.end);
        }
    } catch (e) {
        issues.push(e.code ?? e.message);
    }
    const summary = summarizeObserver003(ctx.policies, arms, issues);
    same(await inventory(plan, INPUT_CAP), beforePlan, "OBSERVER_PLAN_CHANGED");
    same(await inventory(execution, INPUT_CAP - beforePlan.bytes), beforeExecution, "OBSERVER_EXECUTION_CHANGED");
    same(await bytes(inputFile, MiB), inputRaw, "OBSERVER_INPUT_CHANGED");
    for (const a of arms) {
        delete a.declaredCommands;
        delete a.lastCommandIndex;
    }
    await save(
        join(out, "worker-result.json"),
        {
            schemaVersion: 1,
            kind: "http-observer-cost-evidence",
            gateId: "observer-and-validator-cost",
            ...summary,
            verification: { valid: summary.evidenceVerified, issues, inputClosureStable: true },
            scope: observer003Scope(),
            plannedArms: 16,
            plannedCaseRuns: 48,
            arms,
            unmeasuredCases: O.unmeasured.map((id) => ({ id, status: "unknown" })),
            fullElevenStatus: "unknown",
            conditionalCostsAdditive: false,
            validatorTrace: { enabled: true, counterfactualWholeSystemCost: "unknown" },
            limitations,
            references: {
                inputSha256: hash(inputRaw),
                ...input.reference,
                planInventorySha256: valueHash(beforePlan.files),
                executionInventorySha256: valueHash(beforeExecution.files),
            },
            provenance: {
                sourceSelectionMembersBound: 529,
                fullSourceBytesReverified: false,
                readerFiles: await Promise.all(
                    [
                        "observer-cost-evidence.mjs",
                        "observer003-contract.mjs",
                        "analyzer.mjs",
                        "paired-evidence.mjs",
                        "resource-evidence.mjs",
                        "jvm-evidence.mjs",
                        "../paired/core.mjs",
                    ].map(async (path) => ({ path, sha256: hash(await bytes(join(dirname(SELF), path))) })),
                ),
            },
            inferenceAccepted: false,
            performanceAccepted: false,
        },
        Math.min(maxBytes - 65536, 16 * MiB),
    );
}
async function runWorker(input, out, timeout, maxBytes, signal, hardDeadline) {
    return new Promise((done) => {
        const child = spawn(process.execPath, [SELF, "_worker", input, out, String(maxBytes)], {
            detached: true,
            stdio: ["ignore", "pipe", "pipe"],
            env: {
                PATH: process.env.PATH || "/usr/bin:/bin",
                ...(process.env.HGBENCH_PYTHON ? { HGBENCH_PYTHON: process.env.HGBENCH_PYTHON } : {}),
            },
        });
        let failure,
            killer,
            stopping = false,
            total = 0;
        const output = [];
        const group = (sig) => {
            if (child.pid)
                try {
                    process.kill(-child.pid, sig);
                } catch (e) {
                    if (e.code !== "ESRCH") failure ||= "OBSERVER_GROUP_SIGNAL";
                }
        };
        const stop = (code) => {
            failure ||= code;
            if (!stopping) {
                stopping = true;
                group("SIGTERM");
                killer = setTimeout(
                    () => group("SIGKILL"),
                    Math.max(1, Math.min(3000, hardDeadline - performance.now())),
                );
            }
        };
        const abort = () => stop("OBSERVER_CANCELLED"),
            timer = setTimeout(() => stop("OBSERVER_DEADLINE"), timeout);
        signal.addEventListener("abort", abort, { once: true });
        for (const s of [child.stdout, child.stderr])
            s.on("data", (b) => {
                total += b.length;
                if (total > 65536) stop("OBSERVER_WORKER_OUTPUT");
                else output.push(b);
            });
        child.once("error", () => {
            failure ||= "OBSERVER_WORKER_SPAWN";
        });
        child.once("close", (code) => {
            clearTimeout(timer);
            clearTimeout(killer);
            signal.removeEventListener("abort", abort);
            group("SIGKILL");
            done({ code, failure, output: Buffer.concat(output).toString("utf8") });
        });
        if (signal.aborted) abort();
    });
}
export function checkObserverDeadline(signal, begun, timeoutMs) {
    need(!signal.aborted, "OBSERVER_CANCELLED");
    need(performance.now() - begun < timeoutMs, "OBSERVER_DEADLINE");
}
export async function reportObserverCost({ inputFile, outputDirectory, timeoutMs, maxBytes, signal: externalSignal }) {
    need(
        Number.isSafeInteger(timeoutMs) &&
            timeoutMs >= 1000 &&
            timeoutMs <= 86400000 &&
            Number.isSafeInteger(maxBytes) &&
            maxBytes >= MiB &&
            maxBytes <= 8 * 1024 ** 3,
        "OBSERVER_REPORT_BUDGET",
    );
    need(pathOk(inputFile) && pathOk(outputDirectory), "OBSERVER_INPUT_PATH");
    const begun = performance.now(),
        raw = await bytes(inputFile, MiB),
        input = validateObserverCostInput(JSON.parse(raw)),
        file = await realpath(inputFile),
        plan = await realpath(resolve(dirname(file), input.planDirectory)),
        execution = await realpath(resolve(dirname(file), input.executionDirectory)),
        out = await canonical(outputDirectory);
    const originalSpec = await json(join(execution, "execution-spec.json"));
    need(pathOk(originalSpec.sourceRoot) && originalSpec.sourceRoot.startsWith("/"), "OBSERVER_SOURCE_PATH");
    const protectedPaths = [
        file,
        plan,
        execution,
        await canonical(originalSpec.sourceRoot),
        ...(await Promise.all(
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
        )),
    ];
    need(
        !inside(plan, execution) &&
            !inside(execution, plan) &&
            protectedPaths.every((p) => !inside(out, p) && !inside(p, out)),
        "OBSERVER_OUTPUT_OVERLAP",
    );
    await mkdir(out, { mode: 0o700 });
    const cancellation = new AbortController(),
        interrupt = () => cancellation.abort(),
        signal = cancellation.signal;
    const check = () => checkObserverDeadline(signal, begun, timeoutMs);
    externalSignal?.addEventListener("abort", interrupt, { once: true });
    process.on("SIGTERM", interrupt);
    process.on("SIGINT", interrupt);
    if (externalSignal?.aborted) interrupt();
    try {
        check();
        need(timeoutMs - (performance.now() - begun) > 3500, "OBSERVER_DEADLINE");
        const reply = await runWorker(
            file,
            out,
            Math.max(1, timeoutMs - 3500 - (performance.now() - begun)),
            maxBytes,
            signal,
            begun + timeoutMs,
        );
        let code;
        try {
            code = JSON.parse(reply.output).error?.code;
        } catch {}
        need(reply.code === 0 && !reply.failure, reply.failure ?? code ?? "OBSERVER_WORKER_FAILED");
        check();
        const report = await json(join(out, "worker-result.json"));
        need(report.references.inputSha256 === hash(raw), "OBSERVER_INPUT_CHANGED");
        await rm(join(out, "worker-result.json"));
        same(await bytes(file, MiB), raw, "OBSERVER_INPUT_CHANGED");
        check();
        const files = [
            await save(join(out, "input.json"), input, maxBytes),
            await save(join(out, "report.json"), report, maxBytes),
        ];
        const total = files.reduce((n, f) => n + f.bytes, 0);
        need(total + 65536 <= maxBytes, "OBSERVER_OUTPUT_BOUND");
        check();
        await save(
            join(out, "report.lock.json"),
            { schemaVersion: 1, kind: "http-observer-cost-evidence-lock", files, contentDigest: valueHash(files) },
            maxBytes - total,
        );
        check();
        return {
            kind: "http-observer-cost-evidence",
            directory: out,
            reportJson: join(out, "report.json"),
            reportLock: join(out, "report.lock.json"),
            status: report.status,
            evidenceVerified: report.evidenceVerified,
            scopedCostMeasured: report.scopedCostMeasured,
            inferenceAccepted: false,
            performanceAccepted: false,
        };
    } catch (e) {
        await rm(join(out, "worker-result.json"), { force: true });
        await rm(join(out, "report.lock.json"), { force: true });
        await save(
            join(out, "partial.json"),
            {
                schemaVersion: 1,
                kind: "http-observer-cost-evidence-partial",
                status: "unknown",
                evidenceVerified: false,
                scopedCostMeasured: false,
                scope: observer003Scope(),
                plannedArms: 16,
                plannedCaseRuns: 48,
                arms: observer003Schedule(
                    Object.fromEntries(
                        ["J", "C"].map((g) => [g, balancedOrders(O.pairs, O.id + ":" + g + ":order-v1")]),
                    ),
                ).map((a) => ({ ...a, valid: false, issues: ["OBSERVER_ARM_UNVERIFIED"] })),
                issues: [e.code ?? e.message],
                references: input.reference,
                inferenceAccepted: false,
                performanceAccepted: false,
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
    const a = process.argv.slice(2);
    if (a[0] === "_worker") {
        need(a.length === 4, "OBSERVER_ARGUMENTS");
        await worker(a[1], a[2], Number(a[3]));
        return;
    }
    if (a.includes("--help")) {
        process.stdout.write(
            "report --input INPUT --out NEW --timeout-ms N --max-bytes N\nOutput cap1MiB..8GiB; fixed combined input cap32GiB. Offline only; no target requests or saved commands executed.\n",
        );
        return;
    }
    need(a.shift() === "report", "OBSERVER_ARGUMENTS");
    const args = {};
    for (let i = 0; i < a.length; i += 2) {
        need(
            ["--input", "--out", "--timeout-ms", "--max-bytes"].includes(a[i]) && a[i + 1] && !args[a[i]],
            "OBSERVER_ARGUMENTS",
        );
        args[a[i]] = a[i + 1];
    }
    need(Object.keys(args).length === 4, "OBSERVER_ARGUMENTS");
    const data = await reportObserverCost({
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
            JSON.stringify({ ok: false, error: { code: e.code ?? "OBSERVER_ERROR", message: e.message } }) + "\n",
        );
        process.exitCode = 6;
    });
