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

import { constants, createReadStream } from "node:fs";
import { open, lstat, readdir, mkdir, writeFile } from "node:fs/promises";
import { createHash } from "node:crypto";
import { createInterface } from "node:readline";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { isDeepStrictEqual } from "node:util";
import { analyze as analyzeHttp } from "./analyzer.mjs";
import { verifyDriverEvidence, verifyResourceEvidence, ResourceEvidenceError } from "./resource-evidence.mjs";
import { verifyJvmEvidence, JvmEvidenceError } from "./jvm-evidence.mjs";
import { verifyComparison, validateMetricRoles } from "./comparison-plan.mjs";
import { html } from "./paired-report.mjs";
import { analyze as analyzePairs, valueHash } from "../paired/core.mjs";

const ROOT = dirname(fileURLToPath(import.meta.url));
const MiB = 1024 * 1024;
const SHA = /^[a-f0-9]{64}$/;
const UUID = /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/;
const LABEL = "org.apache.hugegraph.hgbench.http";
const CONTROL = [
    "hugegraph-environment.py",
    "hugegraph-service.py",
    "hugegraph-mode-probe.py",
    "jvm-clock-capture.py",
    "conf/gremlin-server.yaml",
    "conf/rest-server.properties",
    "conf/graphs/hugegraph.properties",
    "conf/log4j2.xml",
    "conf/java-security.properties",
];
const encode = (value) => Buffer.from(JSON.stringify(value, null, 2) + "\n");
const hash = (raw) => createHash("sha256").update(raw).digest("hex");

export class PairedEvidenceError extends Error {
    constructor(code, message) {
        super(message);
        this.code = code;
    }
}
function need(condition, code, message = code) {
    if (!condition) throw new PairedEvidenceError(code, message);
}
function same(actual, expected, code) {
    need(isDeepStrictEqual(actual, expected), code);
}
function pathName(path) {
    need(
        typeof path === "string" &&
            /^[A-Za-z0-9@._+-][A-Za-z0-9@._+/-]*$/.test(path) &&
            path.split("/").every((p) => p && p !== "." && p !== ".."),
        "EVIDENCE_PATH",
    );
    return path;
}
function ns(value) {
    need(
        (typeof value === "string" && /^(0|[1-9][0-9]{0,24})$/.test(value)) ||
            (Number.isSafeInteger(value) && value >= 0),
        "UNSAFE_MONOTONIC_TIMESTAMP",
    );
    return BigInt(value);
}
function integer(n, low = 0) {
    return Number.isSafeInteger(n) && n >= low;
}
function span(start, end, maxMs, code) {
    const a = ns(start),
        b = ns(end);
    need(b >= a && b - a <= BigInt(Math.ceil(maxMs * 1e6)), code);
    return [a, b];
}
async function bytes(path, cap = 8 * MiB) {
    const handle = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
    try {
        const before = await handle.stat();
        need(before.isFile() && before.size <= cap, "EVIDENCE_FILE_BUDGET");
        const raw = Buffer.alloc(before.size + 1);
        let size = 0;
        while (size < raw.length) {
            const result = await handle.read(raw, size, raw.length - size, size);
            if (!result.bytesRead) break;
            size += result.bytesRead;
        }
        const after = await handle.stat();
        need(
            size === before.size && after.size === before.size && after.mtimeMs === before.mtimeMs,
            "EVIDENCE_CHANGED",
        );
        return raw.subarray(0, size);
    } finally {
        await handle.close();
    }
}
async function json(path, cap) {
    try {
        return JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(await bytes(path, cap)));
    } catch (error) {
        if (error instanceof PairedEvidenceError) throw error;
        throw new PairedEvidenceError("EVIDENCE_JSON", "Missing or invalid bounded JSON evidence");
    }
}
async function fileHash(path, expectedBytes) {
    const handle = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
    try {
        const before = await handle.stat();
        need(before.isFile() && before.size === expectedBytes, "EVIDENCE_FILE_SIZE");
        const digest = createHash("sha256"),
            buffer = Buffer.alloc(64 * 1024);
        let size = 0;
        while (size <= expectedBytes) {
            const { bytesRead } = await handle.read(buffer, 0, Math.min(buffer.length, expectedBytes + 1 - size), size);
            if (!bytesRead) break;
            digest.update(buffer.subarray(0, bytesRead));
            size += bytesRead;
        }
        const after = await handle.stat();
        need(
            size === expectedBytes && before.size === after.size && before.mtimeMs === after.mtimeMs,
            "EVIDENCE_CHANGED",
        );
        return digest.digest("hex");
    } finally {
        await handle.close();
    }
}
async function inventory(root, prefix = "", out = []) {
    need(prefix.split("/").length <= 12, "EVIDENCE_TREE_DEPTH");
    need((await lstat(join(root, prefix))).isDirectory(), "EVIDENCE_DIRECTORY");
    for (const name of await readdir(join(root, prefix))) {
        const path = pathName(prefix ? `${prefix}/${name}` : name),
            s = await lstat(join(root, path));
        need(!s.isSymbolicLink(), "EVIDENCE_SYMLINK");
        if (s.isDirectory()) await inventory(root, path, out);
        else {
            need(s.isFile() && out.length < 100000, "EVIDENCE_FILE_INVENTORY");
            out.push(path);
        }
    }
    return out.sort();
}
async function verifyExecution(root, plan) {
    const lockRaw = await bytes(join(root, "execution.lock.json"), 16 * MiB),
        lock = JSON.parse(lockRaw);
    need(
        lock.schemaVersion === 1 &&
            lock.kind === "http-comparison-execution-lock" &&
            lock.planDigest === plan.planDigest &&
            Array.isArray(lock.files) &&
            lock.files.length <= 100000,
        "EXECUTION_LOCK",
    );
    const paths = lock.files.map((f) => pathName(f.path));
    need(
        new Set(paths).size === paths.length &&
            !paths.includes("execution.lock.json") &&
            lock.files.every((f) => integer(f.bytes) && SHA.test(f.sha256)),
        "EXECUTION_LOCK",
    );
    const totalBytes = lock.files.reduce((n, f) => n + f.bytes, lockRaw.length);
    need(
        Number.isSafeInteger(totalBytes) && totalBytes <= plan.budgets.maxTotalEvidenceBytes,
        "EXECUTION_EVIDENCE_BUDGET",
    );
    same(await inventory(root), [...paths, "execution.lock.json"].sort(), "EXECUTION_INVENTORY");
    need(lock.contentDigest === valueHash(lock.files), "EXECUTION_CONTENT_DIGEST");
    for (const entry of lock.files)
        need((await fileHash(join(root, entry.path), entry.bytes)) === entry.sha256, "EXECUTION_FILE_DIGEST");
    need(paths.includes("execution.json"), "EXECUTION_REGISTRATION_MISSING");
    return { lock, lockSha256: hash(lockRaw), totalBytes };
}
function buildFiles(source) {
    need(Array.isArray(source) && source.length > 0 && source.length <= 100000, "BUILD_INVENTORY");
    const files = source
        .filter((f) => !f.type || f.type === "file")
        .map((f) => {
            need(integer(f.bytes) && SHA.test(f.sha256), "BUILD_INVENTORY");
            return { path: pathName(f.path), bytes: f.bytes, sha256: f.sha256 };
        })
        .sort((a, b) => a.path.localeCompare(b.path, "en"));
    need(new Set(files.map((f) => f.path)).size === files.length, "BUILD_INVENTORY");
    return files;
}
function sharedContract(shared, execution) {
    const { build, environment: env, settings } = shared;
    need(
        /^[a-f0-9]{40}$/.test(build.sourceCommit) &&
            typeof build.distribution === "string" &&
            build.distribution.startsWith("/") &&
            typeof build.testsRun === "boolean",
        "SHARED_BUILD_CONTRACT",
    );
    buildFiles(build.files);
    need(
        settings.schemaVersion === 1 &&
            settings.kind === "hugegraph-http-environment-settings" &&
            !Object.hasOwn(settings, "mode") &&
            settings.distribution === build.distribution,
        "SHARED_SETTINGS_CONTRACT",
    );
    need(
        env.schemaVersion === 1 &&
            env.kind === "http-comparison-host-environment" &&
            UUID.test(env.hostBootId) &&
            env.hostBootId === execution.hostBootId &&
            typeof env.kernel?.release === "string" &&
            typeof env.kernel?.architecture === "string" &&
            typeof env.cpu?.model === "string" &&
            integer(env.cpu?.logicalProcessors, 1) &&
            integer(env.memoryBytes, 1) &&
            typeof env.docker?.serverVersion === "string" &&
            env.docker?.imageId === settings.image &&
            /^sha256:[a-f0-9]{64}$/.test(settings.image) &&
            /^v[0-9]+\.[0-9]+\.[0-9]+/.test(env.node?.version) &&
            Array.isArray(env.coRunningJobs),
        "SHARED_ENVIRONMENT_CONTRACT",
    );
}

function verifyContainer(info, plan, planSha) {
    const settings = plan.settings,
        b = settings.budget,
        c = info.Config,
        h = info.HostConfig;
    need(
        SHA.test(info.Id) &&
            info.Name === "/" + plan.containerName &&
            info.Image === settings.image &&
            c?.Labels?.[LABEL + ".attempt"] === plan.attemptId &&
            c.Labels[LABEL + ".plan"] === planSha,
        "CONTAINER_IDENTITY",
    );
    need(
        plan.owner.uid > 0 &&
            c.User === `${plan.owner.uid}:${plan.owner.gid}` &&
            h?.NetworkMode === "none" &&
            !Object.keys(h.PortBindings || {}).length &&
            !h.PublishAllPorts &&
            !Object.values(info.NetworkSettings?.Ports || {}).some(Boolean) &&
            h.ReadonlyRootfs === true &&
            h.Memory === b.memoryBytes &&
            h.MemorySwap === b.memoryBytes &&
            h.NanoCpus === Math.round(b.cpus * 1e9) &&
            h.PidsLimit === b.pids &&
            isDeepStrictEqual(h.CapDrop, ["ALL"]) &&
            !(h.CapAdd || []).length &&
            !h.Privileged &&
            h.SecurityOpt?.includes("no-new-privileges:true") &&
            h.RestartPolicy?.Name === "no" &&
            !h.PidMode &&
            !(h.Devices || []).length &&
            !(h.VolumesFrom || []).length &&
            h.LogConfig?.Type === "none",
        "CONTAINER_RESOURCE_ISOLATION",
    );
    same(c.Entrypoint, [settings.python], "CONTAINER_COMMAND");
    same(
        c.Cmd,
        [
            "-B",
            "/control/hugegraph-service.py",
            "--plan",
            "/control/plan.json",
            "--plan-sha256",
            planSha,
            "--work",
            "/work",
        ],
        "CONTAINER_COMMAND",
    );
    need(c.WorkingDir === "/work", "CONTAINER_COMMAND");
    same(h.Tmpfs, { "/tmp": `rw,exec,nosuid,nodev,size=${b.tmpfsBytes},mode=1777` }, "CONTAINER_TMPFS");
    need(
        Array.isArray(info.Mounts) &&
            info.Mounts.every((m) => m.Type === "bind" || (m.Type === "tmpfs" && m.Destination === "/tmp")),
        "CONTAINER_MOUNTS",
    );
    const binds = info.Mounts.filter((m) => m.Type === "bind");
    need(binds.length === 5 && new Set(binds.map((m) => m.Destination)).size === 5, "CONTAINER_MOUNTS");
    same(
        Object.fromEntries(binds.map((m) => [m.Destination, [m.Source, m.RW]])),
        {
            "/opt/hugegraph": [settings.distribution, false],
            "/opt/jdk": [settings.javaHome, false],
            "/control": [plan.root, false],
            "/work": [plan.root + "/work", true],
            "/work/scripts": [settings.distribution + "/scripts", false],
        },
        "CONTAINER_MOUNTS",
    );
}

function parseStack(raw, expected) {
    const text = new TextDecoder("utf-8", { fatal: true }).decode(raw);
    need(
        !text.includes("\0") && text.includes("Full thread dump ") && /^JNI global refs:/m.test(text),
        "MODE_STACK_INCOMPLETE",
    );
    const sections = [];
    let current;
    const prefix = "org.apache.hugegraph.api.gremlin.";
    for (const line of text.split(/\r?\n/)) {
        if (line.startsWith('"')) {
            current = { header: line, frames: [] };
            sections.push(current);
        } else if (/^(JNI global refs:|Found )/.test(line)) current = undefined;
        else if (current) {
            const frame = line.match(
                /^\s+at (?:[A-Za-z0-9_.@-]+\/)?(?:[A-Za-z0-9_.@-]*\/)?([A-Za-z0-9_.$]+)\.([A-Za-z0-9_$<>]+)\([^\r\n]*\)\s*$/,
            );
            if (frame) current.frames.push(`${frame[1]}.${frame[2]}`);
        }
    }
    const matched = [];
    for (const section of sections) {
        const apis = section.frames.flatMap((f, i) => (f === prefix + "GremlinAPI.post" ? [i] : []));
        const modes = ["forward", "embedded"].filter((mode) =>
            section.frames.some((f) =>
                ["post", "execute"].some(
                    (method) =>
                        f === prefix + (mode === "forward" ? "Forward" : "Embedded") + "GremlinRestExecutor." + method,
                ),
            ),
        );
        if (!apis.length && !modes.length) continue;
        need(
            apis.length === 1 &&
                modes.length === 1 &&
                modes[0] === expected &&
                /^"[^"\r\n]*"\s+.*\btid=0x[0-9a-fA-F]+\s+.*\bnid=/.test(section.header),
            "MODE_STACK_MISMATCH",
        );
        const method = prefix + (expected === "forward" ? "Forward" : "Embedded") + "GremlinRestExecutor.post";
        const posts = section.frames.flatMap((f, i) => (f === method ? [i] : []));
        need(posts.length === 1 && posts[0] < apis[0], "MODE_STACK_CALL_ORDER");
        matched.push({ threadHeader: section.header, frames: section.frames });
    }
    need(matched.length === 1, "MODE_STACK_AMBIGUOUS");
    return matched[0];
}
async function verifyMode(dir, env, envSha, ready, container, expected, bootId) {
    const result = await json(join(dir, "result.json")),
        request = await json(join(dir, "request.json"));
    need(
        result.schemaVersion === 1 &&
            result.kind === "hugegraph-http-mode-probe" &&
            result.status === "passed" &&
            result.modeVerified === true &&
            !result.errorCode &&
            result.expectedMode === expected &&
            result.observedMode === expected &&
            request.expectedMode === expected &&
            request.method === "POST" &&
            request.endpoint === env.endpoint &&
            result.componentSha256 === env.controlFiles.find((f) => f.path === "hugegraph-mode-probe.py").sha256,
        "MODE_PROBE_IDENTITY",
    );
    same(result.identityBefore, result.identityAfter, "MODE_PROCESS_CHANGED");
    const id = result.identityBefore;
    need(
        id.pid === ready.java.pid &&
            String(id.startTimeTicks) === String(ready.java.startTimeTicks) &&
            id.bootId === bootId &&
            id.effectiveUid === env.owner.uid &&
            id.effectiveGid === env.owner.gid &&
            id.javaExecutable === ready.javaExecutable &&
            id.jcmdExecutable === ready.jcmdExecutable &&
            ["pid", "net", "mnt", "user"].every((key) => typeof id.namespaces?.[key] === "string") &&
            SHA.test(id.cgroupSha256) &&
            Array.isArray(id.listenerSocketInodes) &&
            id.listenerSocketInodes.length > 0,
        "MODE_PROCESS_IDENTITY",
    );
    // The probe executes inside this exact container; hostname is Docker's recorded UTS identity.
    need(id.hostname === container.Config.Hostname, "MODE_CONTAINER_IDENTITY");
    for (const [file, field] of [
        ["bin/java", "javaExecutableSha256"],
        ["bin/jcmd", "jcmdExecutableSha256"],
    ])
        need(id[field] === env.sources.javaHome.files.find((f) => f.path === file)?.sha256, "MODE_JDK_IDENTITY");
    const window = span(
        result.startedMonotonicNs,
        result.finishedMonotonicNs,
        request.budgets.total_timeout_seconds * 1000,
        "MODE_TOTAL_DEADLINE",
    );
    need(
        Number.isFinite(result.elapsedSeconds) &&
            Math.abs(result.elapsedSeconds - Number(window[1] - window[0]) / 1e9) < 1e-6,
        "MODE_TIME_ACCOUNTING",
    );
    need(
        request.body?.language === "gremlin-groovy" &&
            request.body.gremlin === `Thread.sleep(${request.budgets.sleep_ms}); nonce` &&
            /^hgbench-mode-[a-f0-9]{32}$/.test(request.body.bindings?.nonce),
        "MODE_SENTINEL_REQUEST",
    );
    same(Object.keys(request.body).sort(), ["bindings", "gremlin", "language"], "MODE_SENTINEL_REQUEST");
    same(Object.keys(request.body.bindings), ["nonce"], "MODE_SENTINEL_REQUEST");
    // Python writes sorted evidence JSON, but sends this protocol-defined insertion order on the wire.
    const body = Buffer.from(
        JSON.stringify({
            gremlin: request.body.gremlin,
            bindings: { nonce: request.body.bindings.nonce },
            language: request.body.language,
        }),
    );
    need(body.length === request.bodyBytes && hash(body) === request.bodySha256, "MODE_SENTINEL_REQUEST");
    const r = result.request;
    need(
        r?.errorCode === null &&
            r.httpStatus === 200 &&
            r.contentTypeAccepted === true &&
            r.contentEncodingAccepted === true &&
            r.timerStarted === true &&
            r.workerStarted === true &&
            !r.cancelRequested &&
            integer(r.bodyBytes, 1) &&
            r.bodyBytes <= request.budgets.max_response_bytes &&
            integer(r.wireBytes, 1) &&
            r.wireBytes <= request.budgets.max_wire_response_bytes &&
            SHA.test(r.bodySha256) &&
            SHA.test(r.wireSha256),
        "MODE_SENTINEL_COMPLETION",
    );
    const [sent, complete] = span(
        r.sentMonotonicNs,
        r.completeMonotonicNs,
        request.budgets.request_timeout_seconds * 1000,
        "MODE_REQUEST_DEADLINE",
    );
    need(
        window[0] <= ns(r.startedMonotonicNs) &&
            ns(r.startedMonotonicNs) <= sent &&
            complete <= ns(r.finishedMonotonicNs) &&
            ns(r.finishedMonotonicNs) <= window[1],
        "MODE_REQUEST_WINDOW",
    );
    for (const [stem, command] of [
        ["attach", "VM.version"],
        ["thread-dump", "Thread.print"],
    ]) {
        const record = await json(join(dir, `${stem}.json`));
        same(record.argv, [id.jcmdExecutable, String(id.pid), command], "MODE_COMMAND_IDENTITY");
        need(
            record.exitCode === 0 &&
                record.errorCode === null &&
                record.leaderReaped === true &&
                ["already-absent", "sigkill-sent"].includes(record.groupCleanup),
            "MODE_COMMAND_FAILED",
        );
        const [start, end] = span(
            record.startedMonotonicNs,
            record.finishedMonotonicNs,
            request.budgets.command_timeout_seconds * 1000,
            "MODE_COMMAND_DEADLINE",
        );
        need(start >= window[0] && start < end && end <= window[1], "MODE_COMMAND_WINDOW");
        const output = {};
        for (const stream of ["stdout", "stderr"]) {
            output[stream] = await bytes(join(dir, `${stem}.${stream}.txt`), MiB);
            need(
                record[stream].bytes === output[stream].length &&
                    record[stream].sha256 === hash(output[stream]) &&
                    output[stream].length <= request.budgets.max_command_bytes,
                "MODE_COMMAND_BYTES",
            );
        }
        need(
            record.maxBytes === request.budgets.max_command_bytes &&
                output.stdout.length + output.stderr.length <= request.budgets.max_command_bytes,
            "MODE_COMMAND_COMBINED_BUDGET",
        );
        if (command === "Thread.print") {
            need(sent <= start && end <= complete, "MODE_CAPTURE_OUTSIDE_REQUEST");
            same(parseStack(output.stdout, expected), result.threadEvidence, "MODE_STACK_SUMMARY");
        } else
            need(
                output.stdout.includes(Buffer.from("JDK 17.")) &&
                    new RegExp(`^${id.pid}:\\s*$`, "m").test(output.stdout.toString()),
                "MODE_JAVA_VERSION",
            );
    }
    return { window, identity: id, rawResult: result };
}

export async function verifyControlledEnvironment(dir, raw, shared, expected, execution) {
    const planRaw = await bytes(join(dir, "plan.json")),
        plan = JSON.parse(planRaw),
        planSha = hash(planRaw);
    const lock = await json(join(dir, "plan.lock.json"));
    need(
        plan.schemaVersion === 1 &&
            plan.kind === "hugegraph-http-environment-plan" &&
            /^[a-f0-9]{32}$/.test(plan.attemptId) &&
            lock.planSha256 === planSha &&
            lock.attemptId === plan.attemptId &&
            plan.settings.mode === expected &&
            plan.containerName === "hgbench-http-" + plan.attemptId &&
            raw.profile.endpoint === plan.endpoint,
        "ENVIRONMENT_PLAN_IDENTITY",
    );
    const settings = structuredClone(plan.settings);
    delete settings.mode;
    same(settings, shared.settings, "ENVIRONMENT_SETTINGS_DIFFER");
    need(
        plan.sources.distribution.path === shared.build.distribution &&
            plan.sources.javaHome.path === settings.javaHome,
        "ENVIRONMENT_SOURCE_PATH",
    );
    same(buildFiles(plan.sources.distribution.files), buildFiles(shared.build.files), "ENVIRONMENT_BUILD_DIFFER");
    const controls = [...CONTROL];
    if (settings.jvmObservation)
        controls.push(
            "jvm-observation.py",
            "jvm-observation/spec.json",
            "jvm-observation/plan.json",
            "jvm-observation/plan.lock.json",
        );
    same(plan.controlFiles.map((f) => f.path).sort(), controls.sort(), "ENVIRONMENT_CONTROL_INVENTORY");
    const conf = {};
    for (const file of plan.controlFiles) {
        const content = await bytes(join(dir, pathName(file.path)));
        need(content.length === file.bytes && hash(content) === file.sha256, "ENVIRONMENT_CONTROL_DIGEST");
        if (file.path.endsWith(".py"))
            need(file.sha256 === hash(await bytes(join(ROOT, file.path))), "ENVIRONMENT_IMPLEMENTATION_DIFFER");
        else conf[file.path] = content.toString("utf8");
    }
    const properties = (text) => {
        const entries = text
            .split(/\r?\n/)
            .filter((line) => line && !/^\s*[#!]/.test(line))
            .map((line) => {
                const i = line.indexOf("=");
                need(i > 0, "ENVIRONMENT_PROPERTIES");
                return [line.slice(0, i), line.slice(i + 1)];
            });
        need(new Set(entries.map(([key]) => key)).size === entries.length, "ENVIRONMENT_PROPERTIES");
        return Object.fromEntries(entries);
    };
    const server = settings.server;
    const rest = {
        graphs: "/control/conf/graphs",
        "restserver.url": "http://127.0.0.1:18080",
        "gremlinserver.url": "http://127.0.0.1:18182",
        "gremlinserver.rest_execution_mode": expected,
        "restserver.max_worker_threads": server.httpWorkerThreads,
        "gremlinserver.max_route": server.maxRoute,
        "gremlinserver.timeout": server.timeoutSeconds,
        "restserver.request_timeout": server.timeoutSeconds,
        "restserver.connection_idle_timeout": server.idleTimeoutSeconds,
        "restserver.connection_max_requests": server.connectionMaxRequests,
        "memory_monitor.threshold": 1,
        "log.slow_query_body_limit": 0,
    };
    same(
        properties(conf["conf/rest-server.properties"]),
        Object.fromEntries(Object.entries(rest).map(([key, value]) => [key, String(value)])),
        "ENVIRONMENT_SETTINGS_CONFIG_DIFFER",
    );
    same(
        properties(conf["conf/graphs/hugegraph.properties"]),
        {
            "gremlin.graph": "org.apache.hugegraph.HugeFactory",
            backend: "rocksdb",
            serializer: "binary",
            store: "hugegraph",
            "rocksdb.data_path": "/work/data",
            "rocksdb.wal_path": "/work/data",
            "vertex.cache_capacity": "1000",
            "edge.cache_capacity": "1000",
        },
        "ENVIRONMENT_GRAPH_CONFIG",
    );
    for (const [key, value] of Object.entries({
        host: "127.0.0.1",
        port: 18182,
        gremlinPool: server.gremlinPool,
        threadPoolBoss: server.bossThreads,
        threadPoolWorker: server.workerThreads,
        maxWorkQueueSize: server.maxWorkQueueSize,
        evaluationTimeout: server.timeoutSeconds * 1000,
        maxContentLength: server.maxContentLengthBytes,
        resultIterationBatchSize: server.resultIterationBatchSize,
    })) {
        const entries = conf["conf/gremlin-server.yaml"].split(/\r?\n/).filter((line) => line.startsWith(key + ":"));
        same(entries, [`${key}: ${value}`], "ENVIRONMENT_SETTINGS_CONFIG_DIFFER");
    }
    need(
        plan.endpoint === "http://127.0.0.1:18080/gremlin" &&
            plan.controlFiles.find((f) => f.path === "conf/java-security.properties").sha256 ===
                plan.sources.distribution.files.find((f) => f.path === "conf/java-security.properties")?.sha256,
        "ENVIRONMENT_SECURITY_CONFIG",
    );
    const modeLines = conf["conf/rest-server.properties"].match(/^gremlinserver\.rest_execution_mode=.*$/gm);
    same(modeLines, [`gremlinserver.rest_execution_mode=${expected}`], "ENVIRONMENT_CONFIG_MODE");
    conf["conf/rest-server.properties"] = conf["conf/rest-server.properties"].replace(
        /^gremlinserver\.rest_execution_mode=.*$/m,
        "gremlinserver.rest_execution_mode=__arm__",
    );
    if (plan.formalControlsContract) {
        need(plan.formalControlsContract === "hg-monitor-off-host-swap-off-v1", "FORMAL_CONTROLS_CONTRACT");
        const controls = await json(join(dir, "formal-controls.json"));
        need(
            controls.schemaVersion === 1 &&
                controls.kind === "http-formal-environment-controls" &&
                controls.contract === plan.formalControlsContract &&
                controls.attemptId === plan.attemptId &&
                controls.planSha256 === planSha &&
                integer(controls.checks, 2) &&
                controls.allPassed === true &&
                controls.maximumSwapTotalBytes === 0 &&
                controls.maximumActiveSwapAreas === 0,
            "FORMAL_CONTROLS_FAILED",
        );
        for (const sample of [controls.first, controls.last])
            need(
                sample?.passed === true &&
                    sample.swapTotalBytes === 0 &&
                    sample.activeSwapAreas === 0 &&
                    sample.memoryMonitorDisabled === true &&
                    sample.memoryMonitorThreshold === 1 &&
                    sample.hostBootId === execution.hostBootId,
                "FORMAL_CONTROLS_SAMPLE",
            );
        need(ns(controls.last.atMonotonicNs) >= ns(controls.first.atMonotonicNs), "FORMAL_CONTROLS_ORDER");
    }
    const ready = await json(join(dir, "work/ready.json"));
    const service = await json(join(dir, "work/service-result.json"));
    const supervisor = await json(join(dir, "supervisor-result.json"));
    for (const record of [ready, service, supervisor])
        need(record.attemptId === plan.attemptId && record.planSha256 === planSha, "ENVIRONMENT_RECEIPT_IDENTITY");
    need(
        ready.kind === "hugegraph-http-ready" &&
            ready.declaredMode === expected &&
            ready.endpoint === plan.endpoint &&
            ready.javaExecutable === "/opt/jdk/bin/java" &&
            ready.jcmdExecutable === "/opt/jdk/bin/jcmd" &&
            ready.java.phase === "server" &&
            integer(ready.java.pid, 1),
        "ENVIRONMENT_READY",
    );
    need(
        service.schemaVersion === 1 &&
            service.kind === "hugegraph-http-service-result" &&
            supervisor.schemaVersion === 1 &&
            supervisor.kind === "hugegraph-http-environment-result",
        "ENVIRONMENT_RECEIPT_TYPE",
    );
    same(
        service.outcomes?.map((o) => o.phase),
        ["java-version", "init", "server"],
        "ENVIRONMENT_PHASE_INVENTORY",
    );
    need(
        service.outcomes.slice(0, 2).every((o) => o.reason === "process-exit" && o.exitCodeBeforeShutdown === 0),
        "ENVIRONMENT_INITIALIZATION",
    );
    const last = service.outcomes?.at(-1),
        term = last?.termination;
    need(
        service.error === null &&
            supervisor.error === null &&
            supervisor.created === true &&
            last?.phase === "server" &&
            last.reason === "stop-requested" &&
            last.ready === true &&
            term?.termSent === true &&
            term.killSent === false &&
            term.mainReaped === true &&
            term.groupGone === true &&
            term.cleanupComplete === true &&
            [0, 143, -15].includes(term.exitCode),
        "ENVIRONMENT_STOP_FAILED",
    );
    need(
        integer(service.logBytes) &&
            service.logBytes <= settings.budget.maxLogBytes &&
            Number.isFinite(service.elapsedSeconds) &&
            service.elapsedSeconds >= 0 &&
            service.elapsedSeconds <= settings.budget.wallSeconds + settings.budget.shutdownSeconds + 2,
        "ENVIRONMENT_SERVICE_BUDGET",
    );
    const before = await json(join(dir, "container-before-start.json"));
    const final = await json(join(dir, "container-final.json"));
    verifyContainer(before, plan, planSha);
    verifyContainer(final, plan, planSha);
    need(
        before.Id === final.Id &&
            before.State.Status === "created" &&
            final.State.Status === "exited" &&
            final.State.Running === false &&
            final.State.OOMKilled === false &&
            final.State.ExitCode === 0,
        "ENVIRONMENT_CONTAINER_STOP",
    );
    const pre = await verifyMode(
        join(dir, "work/mode-pre"),
        plan,
        planSha,
        ready,
        final,
        expected,
        execution.hostBootId,
    );
    const post = await verifyMode(
        join(dir, "work/mode-post"),
        plan,
        planSha,
        ready,
        final,
        expected,
        execution.hostBootId,
    );
    same(pre.identity, post.identity, "MODE_PROCESS_REPLACED_DURING_ARM");
    return {
        planSha,
        attemptId: plan.attemptId,
        containerId: final.Id,
        pre,
        post,
        plan,
        invariantDigest: valueHash({
            sources: plan.sources,
            conf,
            settings,
            owner: plan.owner,
            classpath: plan.classpath,
            initClasspath: plan.initClasspath,
        }),
    };
}

async function verifyRuntime(dir, execution, planContentDigest) {
    const lockRaw = await bytes(join(dir, "runtime.lock.json")),
        lock = JSON.parse(lockRaw);
    need(
        hash(lockRaw) === execution.runtimeDigest &&
            [1, 2].includes(lock.schemaVersion) &&
            lock.kind === "http-comparison-runtime-lock" &&
            lock.planDigest === execution.planDigest &&
            lock.runtime?.path === "runtime.json",
        "CONTROLLER_RUNTIME_LOCK",
    );
    const raw = await bytes(join(dir, "runtime.json"), 16 * MiB),
        runtime = JSON.parse(raw);
    need(
        lock.runtime.bytes === raw.length &&
            lock.runtime.sha256 === hash(raw) &&
            runtime.schemaVersion === lock.schemaVersion &&
            (runtime.schemaVersion === 1 || runtime.settings?.schemaVersion === 2) &&
            runtime.kind === "http-comparison-runtime-plan" &&
            runtime.planDigest === execution.planDigest &&
            runtime.planContentDigest === planContentDigest &&
            SHA.test(runtime.componentSha256),
        "CONTROLLER_RUNTIME_IDENTITY",
    );
    const source = buildFiles(runtime.identities?.source),
        node = buildFiles(runtime.identities?.node);
    for (const role of ["hostPython", "dockerExecutable"]) {
        const identity = runtime.identities[role];
        need(
            typeof identity?.path === "string" &&
                identity.path.startsWith("/") &&
                integer(identity.bytes, 1) &&
                SHA.test(identity.sha256),
            "CONTROLLER_EXECUTABLE_IDENTITY",
        );
    }
    need(
        runtime.componentSha256 === source.find((f) => f.path === "components/http/comparison-workflow.py")?.sha256 &&
            runtime.componentSha256 === hash(await bytes(join(ROOT, "comparison-workflow.py"))),
        "CONTROLLER_COMPONENT_IDENTITY",
    );
    return { source, node, runtime };
}

async function verifyRaw(dir, profile, targets, shared, runtime) {
    const report = await analyzeHttp(dir); // Recompute accounting, checks, quantiles and suite; ignore saved analyses.
    const actual = structuredClone(report.profile),
        expected = structuredClone(profile);
    actual.target.evidenceFiles = [];
    expected.target.evidenceFiles = [];
    same(actual, expected, "HTTP_PROFILE_DIFFER");
    need(report.profile.repeats === 1 && report.profile.serviceChecks === true, "HTTP_REQUIRED_CHECKS");
    const target = await json(join(dir, "target-evidence.json"));
    same(
        target.files.map((f) => ({ bytes: f.bytes, sha256: f.sha256 })),
        targets.map((f) => ({ bytes: f.bytes, sha256: f.sha256 })),
        "HTTP_TARGET_IDENTITY",
    );
    const implementation = await json(join(dir, "implementation.json"));
    const go = profile.schemaVersion === 2;
    need(go === (runtime.runtime.schemaVersion === 2), "HTTP_RUNTIME_ENGINE_DIFFER");
    if (go) {
        const spec = profile.executor.binary;
        same(runtime.runtime.settings.executor.binary, spec, "HTTP_GO_EXECUTOR_DIFFER");
        need(
            profile.executor.hostIdentityGate === true &&
                runtime.runtime.identities.goBinary.path === spec.path &&
                runtime.runtime.identities.goBinary.sha256 === spec.sha256 &&
                integer(runtime.runtime.identities.goBinary.bytes, 1) &&
                runtime.runtime.identities.goBinary.bytes <= 32 * MiB,
            "HTTP_GO_BINARY_IDENTITY",
        );
        const prefix = "components/http-go/";
        const goFiles = runtime.source
            .filter((f) => f.path.startsWith(prefix) && (f.path.endsWith(".go") || f.path === prefix + "go.mod"))
            .map((f) => ({ path: f.path.slice(prefix.length), bytes: f.bytes, sha256: f.sha256 }))
            .sort((a, b) => (a.path < b.path ? -1 : a.path > b.path ? 1 : 0));
        need(goFiles.length > 0 && valueHash(goFiles) === spec.sourceSha256, "HTTP_GO_SOURCE_IDENTITY");
        const registration = await json(join(dir, "executor.json"));
        same(registration.sourceFiles, goFiles, "HTTP_GO_SOURCE_REGISTRATION");
        need(
            registration.toolchainLockSha256 ===
                runtime.source.find((f) => f.path === "release/http-go-toolchain.json")?.sha256,
            "HTTP_GO_TOOLCHAIN_LOCK",
        );
    }
    need(
        implementation.node === shared.environment.node.version &&
            implementation.nodeExecutableSha256 === runtime.node.find((f) => f.path === "bin/node")?.sha256,
        "HTTP_NODE_IDENTITY",
    );
    const expectedFiles = [
        "workflow.mjs",
        "runner.mjs",
        "trace-writer.mjs",
        "direct-phase-clock.mjs",
        "runner-sanity.mjs",
        "phase-clock-reference.py",
        "clock-correlation.mjs",
        "analyzer.mjs",
        "report-view.mjs",
        "build-info.mjs",
        "common.mjs",
        "suite-evidence.mjs",
        "probe.mjs",
        ...(go ? ["go-engine.mjs"] : []),
    ];
    same(implementation.files.map((f) => f.file).sort(), [...expectedFiles].sort(), "HTTP_IMPLEMENTATION_INVENTORY");
    for (const file of implementation.files)
        need(
            file.sha256 === hash(await bytes(join(ROOT, file.file))) &&
                file.sha256 === runtime.source.find((f) => f.path === `components/http/${file.file}`)?.sha256,
            "HTTP_IMPLEMENTATION_DIFFER",
        );
    return report;
}
export async function metricValues(raw, directory, bindings) {
    const values = {},
        minimumElapsed = [];
    for (const binding of bindings) {
        const candidates = raw.runs.filter(
            (r) => r.caseId === binding.caseId && r.loadId === binding.loadId && r.repeat === binding.repeat,
        );
        need(candidates.length === 1, "HTTP_METRIC_RUN_AMBIGUOUS");
        const run = candidates[0];
        let value = run;
        for (const key of binding.valuePath.split(".")) value = value?.[key];
        let observations;
        if (binding.kind === "latency") {
            observations = run;
            for (const key of binding.observations.split(".")) observations = observations?.[key];
        } else {
            observations = 0;
            const stream = createReadStream(join(directory, `traces/${run.id}.ndjson`), {
                flags: constants.O_RDONLY | constants.O_NOFOLLOW,
            });
            const lines = createInterface({ input: stream, crlfDelay: Infinity });
            try {
                for await (const line of lines) {
                    const sample = JSON.parse(line);
                    if (sample.phase === "measurement" && sample.outcome === "ok" && sample.endMs <= run.durationMs)
                        observations++;
                }
            } finally {
                lines.close();
                stream.destroy();
            }
            need(value === observations / (run.durationMs / 1000), "HTTP_GOODPUT_ACCOUNTING");
        }
        need(Number.isFinite(value) && value >= 0 && integer(observations), "HTTP_METRIC_VALUE");
        values[binding.id] = { value, observations };
    }
    for (const run of raw.runs) {
        const saved = await json(join(directory, `runs/${run.id}.json`));
        minimumElapsed.push(saved.phases.reduce((sum, phase) => sum + phase.elapsedMs, 0));
    }
    return { values, minimumWorkflowMs: minimumElapsed.reduce((a, b) => a + b, 0) };
}

// Called only after the comparison plan is independently reconstructed. Missing
// quality observations keep their predeclared metric rows and cannot qualify inference.
export function metricRoleEvidence(plan, values) {
    if (plan.schemaVersion === 1) return { inferenceValues: values };
    need([2, 3].includes(plan.schemaVersion), "HTTP_METRIC_ROLES_VERSION");
    const single = plan.schemaVersion === 3 && plan.comparison === "single-observation";
    if (!single) validateMetricRoles(plan.metricRoles, { allowGoodputInference: plan.schemaVersion === 3 });
    for (const binding of plan.metricBindings)
        need(
            ["latency", "goodput"].includes(binding.kind) &&
                binding.role === (single ? "descriptive" : plan.metricRoles[binding.kind]) &&
                Number.isSafeInteger(binding.minimumObservations) &&
                binding.minimumObservations >= 1,
            "HTTP_METRIC_ROLE_BINDING",
        );
    const inferenceBindings = plan.metricBindings.filter((binding) => binding.role === "inference");
    need(
        plan.schemaVersion === 3
            ? plan.metricBindings.length > 0 &&
                  plan.metricBindings.length % 2 === 0 &&
                  new Set(plan.metricBindings.map((binding) => binding.id)).size === plan.metricBindings.length &&
                  plan.metricBindings.every(
                      (binding) =>
                          plan.metricBindings.filter(
                              (other) =>
                                  other.caseId === binding.caseId &&
                                  other.loadId === binding.loadId &&
                                  other.repeat === binding.repeat,
                          ).length === 2,
                  )
            : inferenceBindings.length > 0 &&
                  plan.metricBindings.length === 2 * inferenceBindings.length &&
                  new Set(plan.metricBindings.map((binding) => binding.id)).size === plan.metricBindings.length &&
                  inferenceBindings.every(
                      (binding) =>
                          plan.metricBindings.filter(
                              (other) =>
                                  other.role === "quality" &&
                                  other.caseId === binding.caseId &&
                                  other.loadId === binding.loadId &&
                                  other.repeat === binding.repeat,
                          ).length === 1,
                  ),
        "HTTP_METRIC_ROLE_INVENTORY",
    );
    need(
        Object.keys(values).every((id) => plan.metricBindings.some((binding) => binding.id === id)),
        "HTTP_METRIC_VALUE_UNDECLARED",
    );
    same(
        (single ? plan.metricBindings : inferenceBindings).map((binding) => binding.id).sort(),
        plan.analysisPolicy.metrics.map((metric) => metric.id).sort(),
        "HTTP_INFERENCE_FAMILY_DIFFER",
    );
    const metrics = plan.metricBindings
        .filter((binding) => single || binding.role === "quality")
        .map((binding) => {
            const observed = values[binding.id],
                issues = [];
            if (!observed) issues.push("HTTP_QUALITY_METRIC_MISSING");
            else if (
                !Number.isFinite(observed.value) ||
                observed.value < 0 ||
                !Number.isSafeInteger(observed.observations) ||
                observed.observations < 0
            )
                issues.push("HTTP_QUALITY_METRIC_VALUE");
            else if (observed.observations < binding.minimumObservations)
                issues.push("HTTP_QUALITY_METRIC_OBSERVATIONS_INSUFFICIENT");
            const insufficient = single && issues.includes("HTTP_QUALITY_METRIC_OBSERVATIONS_INSUFFICIENT");
            return {
                metricId: binding.id,
                caseId: binding.caseId,
                loadId: binding.loadId,
                valuePath: binding.valuePath,
                role: single ? "descriptive" : "quality",
                value: insufficient ? null : (observed?.value ?? null),
                ...(single ? { observedValue: observed?.value ?? null } : {}),
                observations: observed?.observations ?? null,
                minimumObservations: binding.minimumObservations,
                status: !observed || insufficient ? "unknown" : issues.length ? "failed" : "passed",
                issues,
            };
        });
    return {
        inferenceValues: Object.fromEntries(
            inferenceBindings
                .filter((binding) => Object.hasOwn(values, binding.id))
                .map((binding) => [binding.id, values[binding.id]]),
        ),
        quality: {
            status: metrics.some((metric) => metric.status === "failed")
                ? "failed"
                : metrics.every((metric) => metric.status === "passed")
                  ? "passed"
                  : "unknown",
            metrics,
            issues: metrics.flatMap((metric) => metric.issues.map((issue) => `${issue}:${metric.metricId}`)),
        },
    };
}

export async function normalizePairedEvidence(options) {
    return normalizeHttpEvidenceCore(options, false);
}
export async function normalizeObservationEvidence(options) {
    return normalizeHttpEvidenceCore(options, true);
}
export async function normalizeHttpEvidence(options) {
    return normalizeHttpEvidenceCore(options, null);
}
async function normalizeHttpEvidenceCore(
    { planDirectory, executionDirectory, includeHttpSummaries = false },
    requireSingle,
) {
    need(typeof includeHttpSummaries === "boolean", "HTTP_SUMMARY_OPTION");
    const planRoot = resolve(planDirectory),
        root = resolve(executionDirectory);
    const verifiedPlan = await verifyComparison(planRoot);
    const planLock = await json(join(planRoot, "plan.lock.json"));
    need(valueHash(planLock.files) === verifiedPlan.contentDigest, "PLAN_CHANGED_DURING_ANALYSIS");
    async function planJson(path) {
        const entry = planLock.files.find((file) => file.path === path);
        const raw = await bytes(join(planRoot, path));
        need(entry && entry.bytes === raw.length && entry.sha256 === hash(raw), "PLAN_CHANGED_DURING_ANALYSIS");
        return JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(raw));
    }
    const plan = await planJson("plan.json");
    const single = plan.schemaVersion === 3 && plan.comparison === "single-observation";
    need(
        requireSingle === null || requireSingle === single,
        "HTTP_EVIDENCE_DESIGN",
        "Use the verifier for the frozen observation or paired design",
    );
    const verified = await verifyExecution(root, plan),
        execution = await json(join(root, "execution.json"));
    need(
        execution.schemaVersion === 1 &&
            execution.kind === "http-comparison-execution" &&
            execution.planDigest === plan.planDigest &&
            execution.planContentDigest === verifiedPlan.contentDigest &&
            /^[a-f0-9]{32}$/.test(execution.executionId) &&
            UUID.test(execution.hostBootId) &&
            SHA.test(execution.runtimeDigest) &&
            Array.isArray(execution.receipts),
        "EXECUTION_REGISTRATION",
    );
    const registered = ns(execution.registeredMonotonicNs);
    const shared = Object.fromEntries(
        await Promise.all(
            ["build", "environment", "settings"].map(async (role) => [
                role,
                await planJson(plan.sharedEvidence[role].path),
            ]),
        ),
    );
    sharedContract(shared, execution);
    const profiles = Object.fromEntries(
        await Promise.all(
            Object.keys(plan.variants).map(async (arm) => [arm, await planJson(plan.variants[arm].profileFile)]),
        ),
    );
    const targets = await planJson("target-files.json");
    const scheduled = plan.blocks.flatMap((block) => block.arms.map((arm) => ({ ...arm, blockId: block.id })));
    const expectedIds = new Set(scheduled.map((arm) => arm.runId));
    need(
        execution.receipts.every((r) => expectedIds.has(r.runId) && r.path === `arms/${r.runId}/receipt.json`) &&
            new Set(execution.receipts.map((r) => r.runId)).size === execution.receipts.length,
        "EXECUTION_RECEIPTS",
    );
    need(
        verified.lock.files.every(
            (f) =>
                ["execution.json", "terminal.json"].includes(f.path) ||
                scheduled.some((arm) => f.path.startsWith(`arms/${arm.runId}/`)),
        ),
        "EXECUTION_UNOWNED_FILES",
    );
    const rows = [],
        globalIssues = [];
    let priorEnd = registered,
        firstStart,
        invariant,
        terminalFinish;
    try {
        need(
            verified.lock.files.some((f) => f.path === "terminal.json"),
            "EXECUTION_TERMINAL_MISSING",
        );
        const terminal = await json(join(root, "terminal.json"));
        need(
            terminal.schemaVersion === 1 &&
                terminal.kind === "http-comparison-execution-terminal" &&
                terminal.executionId === execution.executionId &&
                terminal.planDigest === plan.planDigest &&
                terminal.runtimeDigest === execution.runtimeDigest,
            "EXECUTION_TERMINAL_IDENTITY",
        );
        terminalFinish = ns(terminal.finishedMonotonicNs);
        span(
            execution.registeredMonotonicNs,
            terminal.finishedMonotonicNs,
            plan.budgets.maxTotalMilliseconds,
            "EXECUTION_TIME_BUDGET",
        );
        need(
            terminal.status === "completed" && terminal.failure === null && terminal.runtimeReverified === true,
            "EXECUTION_TERMINAL_FAILED",
        );
    } catch (error) {
        globalIssues.push(error instanceof PairedEvidenceError ? error.code : "EXECUTION_TERMINAL_INVALID");
    }
    const seenRaw = new Set(),
        seenContainer = new Set(),
        seenAttempt = new Set();
    const seenGoProcesses = new Set();
    for (const arm of scheduled) {
        const row = { ...arm, issues: [], status: "invalid", values: {} };
        if (plan.schemaVersion >= 2) row.metricQuality = metricRoleEvidence(plan, {}).quality;
        rows.push(row);
        try {
            const ref = execution.receipts.find((r) => r.runId === arm.runId);
            need(ref, "ARM_MISSING");
            const receipt = await json(join(root, ref.path));
            row.receiptSha256 = hash(await bytes(join(root, ref.path)));
            need(
                receipt.schemaVersion === 1 &&
                    receipt.kind === "http-comparison-arm-receipt" &&
                    receipt.executionId === execution.executionId &&
                    receipt.planDigest === plan.planDigest &&
                    receipt.runId === arm.runId &&
                    receipt.arm === arm.arm &&
                    receipt.blockId === arm.blockId &&
                    receipt.position === arm.position,
                "ARM_IDENTITY",
            );
            need(
                receipt.failure === null && Array.isArray(receipt.cleanupErrors) && receipt.cleanupErrors.length === 0,
                "ARM_CONTROLLER_FAILURE",
            );
            const base = `arms/${arm.runId}/`;
            need(
                receipt.rawDirectory === base + "raw" && receipt.environmentDirectory === base + "environment",
                "ARM_ARTIFACT_PATH",
            );
            const [start, finish] = span(
                receipt.armStartedMonotonicNs,
                receipt.armFinishedMonotonicNs,
                arm.maxMilliseconds,
                "ARM_TIME_BUDGET",
            );
            const [workflowStart, workflowEnd] = span(
                receipt.workflowStartedMonotonicNs,
                receipt.workflowFinishedMonotonicNs,
                plan.budgets.workflowMaxMilliseconds,
                "WORKFLOW_TIME_BUDGET",
            );
            need(start >= registered && start <= workflowStart && workflowEnd <= finish, "ARM_TIME_ENVELOPE");
            if (start < priorEnd) globalIssues.push("ARM_ORDER_OR_OVERLAP");
            priorEnd = finish;
            firstStart ??= start;
            row.armWindow = [String(start), String(finish)];
            const armBytes = verified.lock.files
                .filter((f) => f.path.startsWith(base))
                .reduce((n, f) => n + f.bytes, 0);
            need(armBytes <= arm.maxEvidenceBytes, "ARM_EVIDENCE_BUDGET");
            const runtime = await verifyRuntime(join(root, base, "controller"), execution, verifiedPlan.contentDigest);
            const rawDirectory = join(root, receipt.rawDirectory);
            const report = await verifyRaw(rawDirectory, profiles[arm.arm], targets, shared, runtime);
            if (profiles[arm.arm].schemaVersion === 2) {
                const reader = await import("./go-clock-evidence.mjs"),
                    controller = join(root, base, "controller");
                const proof = await reader.readGoHostClockProof(
                    join(controller, "go-clock-proof"),
                    profiles[arm.arm].executor.binary,
                );
                need(
                    proof.hostBootId === execution.hostBootId &&
                        proof.hostPythonExecutableSha256 === runtime.runtime.identities.hostPython.sha256 &&
                        proof.imageId === runtime.runtime.settings.driver.image,
                    "HTTP_GO_CLOCK_RUNTIME",
                );
                for (const entry of runtime.runtime.identities.goClockProof)
                    need(
                        hash(await bytes(join(controller, "go-clock-proof", pathName(entry.path)))) === entry.sha256,
                        "HTTP_GO_CLOCK_SNAPSHOT",
                    );
                for (const entry of runtime.runtime.identities.goControlFiles)
                    need(
                        hash(await bytes(join(controller, pathName(entry.path)))) === entry.sha256 &&
                            entry.sha256 ===
                                runtime.source.find((f) => f.path === "components/http/" + entry.path)?.sha256,
                        "HTTP_GO_CONTROL_SOURCE",
                    );
                const locations = report.runs.map((r) => `runs/${r.id}.json`);
                for (const phase of ["preflight", "postcheck"])
                    for (let i = 0; i < profiles[arm.arm].cases.length; i++)
                        locations.push(`${phase}/p${String(i + 1).padStart(4, "0")}.json`);
                const links = [];
                for (const relative of locations) {
                    const run = await json(join(rawDirectory, relative));
                    const observation = await reader.readGoProcessObservations(
                        join(controller, "go-process-observations", relative + ".identity"),
                        join(rawDirectory, relative + ".identity"),
                    );
                    const association = reader.validateGoMeasurementAssociation(
                        observation,
                        run,
                        profiles[arm.arm],
                        proof,
                    );
                    const key = association.hostPid + ":" + association.startTicks;
                    need(!seenGoProcesses.has(key), "HTTP_GO_LOGICAL_PROCESS_REPLAY");
                    seenGoProcesses.add(key);
                    links.push({ relative, ...association });
                }
                const summary = await json(join(controller, "go-process-summary.json"));
                need(
                    summary.schemaVersion === 1 &&
                        summary.kind === "http-go-comparison-observer-summary" &&
                        summary.processCount === locations.length &&
                        summary.gateCount === 2 * locations.length,
                    "HTTP_GO_OBSERVER_INVENTORY",
                );
                row.goProcessAssociations = links;
                row.goProcessHostAssociationVerified = true;
            }
            if (includeHttpSummaries)
                row.httpSummary = {
                    rawManifestSha256: report.rawManifestSha256,
                    gates: report.gates,
                    verdict: report.verdict,
                    runs: report.runs,
                };
            row.rawEvidenceSha256 = report.rawManifestSha256;
            need(
                receipt.rawLockSha256 === report.rawManifestSha256 && !seenRaw.has(report.rawManifestSha256),
                "ARM_RAW_IDENTITY_OR_REPLAY",
            );
            seenRaw.add(report.rawManifestSha256);
            row.rawVerified = true;
            row.phaseClocks = report.runs.map((run) => ({
                id: run.id,
                caseId: run.caseId,
                loadId: run.loadId,
                ...run.phaseClock,
            }));
            const environment = await verifyControlledEnvironment(
                join(root, receipt.environmentDirectory),
                report,
                shared,
                arm.expectedMode,
                execution,
            );
            need(
                receipt.environmentPlanSha256 === environment.planSha &&
                    !seenContainer.has(environment.containerId) &&
                    !seenAttempt.has(environment.attemptId),
                "ARM_ENVIRONMENT_IDENTITY_OR_REPLAY",
            );
            seenContainer.add(environment.containerId);
            seenAttempt.add(environment.attemptId);
            need(
                start <= environment.pre.window[0] &&
                    environment.pre.window[1] <= workflowStart &&
                    workflowEnd <= environment.post.window[0] &&
                    environment.post.window[1] <= finish,
                "MODE_PROBE_INSIDE_WORKFLOW",
            );
            span(
                environment.pre.window[0].toString(),
                environment.pre.window[1].toString(),
                plan.budgets.modeProbeMs,
                "MODE_PRE_PLAN_BUDGET",
            );
            span(
                environment.post.window[0].toString(),
                environment.post.window[1].toString(),
                plan.budgets.modeProbeMs,
                "MODE_POST_PLAN_BUDGET",
            );
            need(
                workflowStart - environment.pre.window[1] >= BigInt(plan.budgets.settleMs) * 1000000n,
                "SETTLE_WINDOW_TOO_SHORT",
            );
            invariant ??= environment.invariantDigest;
            if (invariant !== environment.invariantDigest) globalIssues.push("ARMS_HAVE_DIFFERENT_RUNTIME_INPUTS");
            row.jvmObservation = await verifyJvmEvidence(
                join(root, receipt.environmentDirectory),
                join(root, base, "jvm"),
                {
                    settings: shared.settings,
                    environmentPlanSha256: environment.planSha,
                    hostBootId: execution.hostBootId,
                    processIdentity: environment.pre.identity,
                    clockExpected: {
                        modeProbes: { pre: environment.pre.rawResult, post: environment.post.rawResult },
                        modeProbeComponentSha256: environment.plan.controlFiles.find(
                            (f) => f.path === "hugegraph-mode-probe.py",
                        ).sha256,
                        captureComponentSha256: environment.plan.controlFiles.find(
                            (f) => f.path === "jvm-clock-capture.py",
                        ).sha256,
                    },
                },
            );
            const controller = join(root, base, "controller");
            const driver = await verifyDriverEvidence(controller, {
                arm,
                receipt,
                execution,
                runtime: runtime.runtime,
                target: environment,
                targetPlan: environment.plan,
            });
            need(!seenContainer.has(driver.containerId), "ARM_DRIVER_REPLAY");
            seenContainer.add(driver.containerId);
            const componentSha = runtime.source.find((f) => f.path === "components/http/resource-collector.py")?.sha256;
            need(
                componentSha === hash(await bytes(join(ROOT, "resource-collector.py"))),
                "COLLECTOR_IMPLEMENTATION_DIFFER",
            );
            const roles = await json(join(controller, "collector-roles.json"));
            need(
                roles.runId === arm.runId &&
                    roles.targetJavaHostPid === driver.expectedRoles.target.hostPid &&
                    roles.driverInitHostPid === driver.expectedRoles.driver.hostPid,
                "COLLECTOR_ROLE_BINDING",
            );
            const resource = await verifyResourceEvidence(join(root, base, "collector"), {
                hostBootId: execution.hostBootId,
                componentSha256: componentSha,
                clockExpected: { hostPythonExecutableSha256: runtime.runtime.identities.hostPython.sha256 },
                stopMarkerPath: join(controller, "collector-stop.json"),
                workflowWindow: [receipt.workflowStartedMonotonicNs, receipt.workflowFinishedMonotonicNs],
                expectedRoles: driver.expectedRoles,
                expectedSpec: {
                    ...runtime.runtime.settings.collector,
                    filesystemPath: driver.root,
                    roles: [
                        { role: "target", hostPid: roles.targetJavaHostPid },
                        { role: "driver", hostPid: roles.driverInitHostPid },
                    ],
                },
            });
            need(resource.sourceOrigin === "linux-procfs", "COLLECTOR_FIXTURE_IS_NOT_RUNTIME_EVIDENCE");
            same(resource.spec, roles.spec, "COLLECTOR_ROLE_SPEC_DIFFER");
            const window = await json(join(controller, "collector-window.json"));
            const marker = await json(join(controller, "collector-stop.json"));
            need(
                window.runId === arm.runId &&
                    window.workflowStartedMonotonicNs === receipt.workflowStartedMonotonicNs &&
                    window.workflowFinishedMonotonicNs === receipt.workflowFinishedMonotonicNs &&
                    window.summarySha256 === resource.summarySha256 &&
                    window.firstSampleStartedMonotonicNs === resource.firstStartedMonotonicNs &&
                    window.lastSampleFinishedMonotonicNs === resource.lastFinishedMonotonicNs &&
                    window.measurementPhaseAbsoluteAnchors === false,
                "COLLECTOR_WINDOW_RECEIPT",
            );
            same(window.stopMarker, marker, "COLLECTOR_STOP_MARKER");
            need(
                ns(resource.finishedMonotonicNs) <= environment.post.window[0] && resource.qualityIssues.length === 0,
                "COLLECTOR_WINDOW_OR_RESOURCE_FAILURE",
            );
            row.driverVerified = true;
            row.resourcesVerified = true;
            row.resourceCoverage = resource.coverage;
            row.resourceSamples = resource.sampleCount;
            row.resourceObserved = resource.observedResources;
            row.resourceSourceOrigin = resource.sourceOrigin;
            row.resourceMissingMetrics = resource.missingMetrics;
            row.collectorClock = resource.clockIdentity;
            const metrics = await metricValues(report, rawDirectory, plan.metricBindings);
            need(
                Number(workflowEnd - workflowStart) / 1e6 + 1 >= metrics.minimumWorkflowMs,
                "WORKFLOW_ENVELOPE_TOO_SHORT",
            );
            row.values = metrics.values;
            if (plan.schemaVersion >= 2) {
                row.metricQuality = metricRoleEvidence(plan, metrics.values).quality;
                row.issues.push(...row.metricQuality.issues);
            }
            row.runtimeArtifactsConsistent = true;
            if (report.gates.correctness !== "passed") {
                row.status = "correctness-failed";
                row.issues.push("HTTP_CORRECTNESS_FAILED");
            } else if (
                report.runs.some((r) => r.issues.length) ||
                (plan.schemaVersion >= 2 && row.metricQuality.status !== "passed")
            ) {
                row.status = "invalid";
                row.issues.push("HTTP_QUALITY_FAILED");
            } else {
                row.status = "ok";
            }
        } catch (error) {
            if (typeof error?.message === "string" && /^HTTP_GO/.test(error.message))
                row.engineDiagnostic = error.message.slice(0, 1000);
            row.issues.push(
                error instanceof PairedEvidenceError ||
                    error instanceof ResourceEvidenceError ||
                    error instanceof JvmEvidenceError
                    ? error.code
                    : "RAW_OR_RUNTIME_REVERIFICATION_FAILED",
            );
        }
    }
    if (firstStart !== undefined && priorEnd - registered > BigInt(plan.budgets.maxTotalMilliseconds) * 1000000n)
        globalIssues.push("EXECUTION_TIME_BUDGET");
    if (terminalFinish !== undefined && terminalFinish < priorEnd) globalIssues.push("EXECUTION_FINISHED_BEFORE_ARM");
    // Recheck the complete lock after all readers. Concurrent edits cannot produce a mixed accepted snapshot.
    const after = await verifyExecution(root, plan);
    need(after.lockSha256 === verified.lockSha256, "EXECUTION_CHANGED_DURING_ANALYSIS");
    const afterPlan = await verifyComparison(planRoot);
    need(
        afterPlan.contentDigest === verifiedPlan.contentDigest && afterPlan.planDigest === verifiedPlan.planDigest,
        "PLAN_CHANGED_DURING_ANALYSIS",
    );
    if (globalIssues.length)
        for (const row of rows) {
            row.status = "invalid";
            row.issues.push(...globalIssues);
        }
    const blocks = single
        ? []
        : plan.blocks.map((block) => {
              const arms = Object.fromEntries(
                  block.arms.map((arm) => {
                      const row = rows.find((r) => r.runId === arm.runId);
                      return [
                          arm.arm === "A" ? "baseline" : "candidate",
                          {
                              runId: arm.runId,
                              // Missing evidence is a hashed explicit adapter rejection, never a made-up raw manifest.
                              evidenceSha256:
                                  row.rawEvidenceSha256 || valueHash({ runId: row.runId, issues: row.issues }),
                              evidenceKind: row.rawEvidenceSha256 ? "http-raw-manifest" : "adapter-rejection",
                              variantDigest: arm.variantDigest,
                              conditionsDigest: block.conditionsDigest,
                              status: row.status,
                              values: metricRoleEvidence(plan, row.values).inferenceValues,
                          },
                      ];
                  }),
              );
              return { id: block.id, observedOrder: block.order, ...arms };
          });
    const all = rows.every((r) => r.status === "ok");
    const reportContext = {
        metricBindings: plan.metricBindings,
        ...(plan.schemaVersion >= 2
            ? { comparisonSchemaVersion: plan.schemaVersion, ...(!single ? { metricRoles: plan.metricRoles } : {}) }
            : {}),
        comparison: plan.comparison,
        purpose: plan.purpose,
        suite: plan.suite,
        modes: Object.fromEntries(Object.keys(plan.variants).map((arm) => [arm, plan.variants[arm].mode])),
    };
    const verification = {
        schemaVersion: 1,
        reportContext,
        kind: single ? "http-observation-evidence-verification" : "http-paired-evidence-verification",
        ...(single ? { design: "single-observation", inferenceAccepted: false } : {}),
        planDigest: plan.planDigest,
        planContentDigest: verifiedPlan.contentDigest,
        executionLockSha256: verified.lockSha256,
        valid: all,
        rows,
        globalIssues,
        performanceAccepted: false,
        gates: {
            ...(plan.schemaVersion >= 2
                ? {
                      metricQuality: {
                          status: rows.every((row) => row.metricQuality.status === "passed") ? "passed" : "failed",
                          plannedMetrics: rows.reduce((sum, row) => sum + row.metricQuality.metrics.length, 0),
                          passedMetrics: rows.reduce(
                              (sum, row) =>
                                  sum + row.metricQuality.metrics.filter((metric) => metric.status === "passed").length,
                              0,
                          ),
                          failedMetrics: rows.reduce(
                              (sum, row) =>
                                  sum + row.metricQuality.metrics.filter((metric) => metric.status === "failed").length,
                              0,
                          ),
                          unknownMetrics: rows.reduce(
                              (sum, row) =>
                                  sum +
                                  row.metricQuality.metrics.filter((metric) => metric.status === "unknown").length,
                              0,
                          ),
                          scope: "All declared goodput metrics retain raw accounting, correctness/quality and minimum observation checks; no throughput inference or capacity claim",
                      },
                  }
                : {}),
            rawEvidence: {
                status: rows.every((r) => r.rawVerified) ? "passed" : "failed",
                scope: "Locked HTTP suite, request inventory, byte/accounting/timeline, answer checks and metrics reanalyzed",
                limits: "Response bodies are runner-checked hashes; server provenance is not established by HTTP alone",
            },
            runtimeArtifacts: {
                status: rows.every((r) => r.runtimeArtifactsConsistent) ? "consistent" : "failed",
                scope: "Same frozen distribution/JDK/config; recorded target Docker identity/isolation and unforced stop; raw mode stacks",
                limits: "Artifact consistency, not independent live attestation; mode sentinel response bodies are not retained",
            },
            workflowWindow: Object.values(profiles).some((p) => p.schemaVersion === 2)
                ? "Controller envelope includes warmup/measurement/drain; each Go process/host link is verified separately; fine collector/JVM phase attribution is not granted"
                : "Controller envelope includes warmup/measurement/drain; new raw phase brackets are revalidated, host association remains unverified; legacy phase clocks unavailable",
            preRegistration:
                "Locked controller registration precedes dispatch in same boot clock; not a third-party timestamp",
            environment: "pending-measurement-window-interference-evidence",
            driverLifecycle: {
                status: rows.every((r) => r.driverVerified) ? "passed" : "failed",
                scope: "Driver inspect identity, read-only mounts, limits, normal exit and procfs PID/starttime/namespace mapping",
            },
            resources: {
                status: rows.every((r) => r.resourcesVerified) ? "passed" : "failed",
                scope: "Collector lock, original samples, identities, counters, summaries and controlled-stop workflow coverage",
                limits: "Missing metrics remain explicit; final identity read is producer-attested; host exclusivity and overhead uncalibrated",
            },
            jvmObservation: {
                status: shared.settings.jvmObservation
                    ? rows.every((r) => r.jvmObservation?.status === "observed")
                        ? "observed"
                        : "partial-or-unavailable"
                    : "not-enabled",
                scope: "Frozen startup preset, raw stdout/capture, process/JDK identity and independent parser reconstruction",
                limits: "JVM-to-host clock and phase attribution unverified; observer overhead and warmup stability uncalibrated",
            },
            driverHeadroom: "pending-independent-calibration",
            calibration: "pending-A/A-regression-saturation",
        },
    };
    if (Object.values(profiles).some((p) => p.schemaVersion === 2)) {
        verification.executorEngine = "go-http-v1";
        verification.gates.goProcessHostAssociation = {
            status: rows.every((r) => r.goProcessHostAssociationVerified) ? "passed" : "failed",
            scope: "Actual held Go process identities and raw phase intervals; collector/JVM fine attribution and calibration remain separate",
        };
    }
    if (single) {
        const observations = rows.map((row) => ({
            runId: row.runId,
            observationId: row.blockId,
            arm: "O",
            expectedMode: row.expectedMode,
            status: row.status,
            issues: row.issues,
            evidenceSha256: row.rawEvidenceSha256 ?? null,
            metrics: row.metricQuality.metrics,
        }));
        verification.statisticsEligible = false;
        const normalized = {
            schemaVersion: 1,
            kind: "normalized-http-observations",
            origin: "http-single-observation-adapter-v1",
            design: "single-observation",
            policy: plan.analysisPolicy,
            observations,
            verification: { executionLockSha256: verified.lockSha256, planDigest: plan.planDigest, valid: all },
            performanceAccepted: false,
        };
        const analysis = {
            schemaVersion: 1,
            kind: "http-single-observation-analysis",
            algorithm: "http-single-observation-v1",
            design: "single-observation",
            plannedObservations: 1,
            observedComplete: all ? 1 : 0,
            observations,
            statisticsEligible: false,
            inferenceAccepted: false,
            performanceAccepted: false,
            limits: "One fresh-process observation; no pairs, repeatability, confidence interval or statistical acceptance",
        };
        return { normalized, verification, analysis, reportContext };
    }
    const normalized = {
        schemaVersion: 1,
        kind: "normalized-paired-observations",
        origin:
            plan.schemaVersion >= 2
                ? `http-paired-evidence-adapter-v${plan.schemaVersion}`
                : "http-paired-evidence-adapter-v1",
        policy: plan.analysisPolicy,
        blocks,
        verification: { executionLockSha256: verified.lockSha256, planDigest: plan.planDigest, valid: all },
        performanceAccepted: false,
    };
    const analysis = analyzePairs(normalized);
    verification.statisticsEligible = all && analysis.cells.every((cell) => cell.issues.length === 0);
    analysis.gates.rawEvidence = verification.gates.rawEvidence;
    analysis.gates.httpRuntimeEvidence = verification.gates.runtimeArtifacts;
    return { normalized, verification, analysis, reportContext };
}

export async function writePairedEvidence({ planDirectory, executionDirectory, outputDirectory }) {
    const result = await normalizeHttpEvidence({ planDirectory, executionDirectory });
    const output = resolve(outputDirectory);
    await mkdir(output, { mode: 0o700 });
    const files = [];
    let totalBytes = 0;
    for (const [path, value] of [
        ["observations.json", result.normalized],
        ["verification.json", result.verification],
        ["analysis.json", result.analysis],
    ]) {
        const raw = encode(value);
        totalBytes += raw.length;
        need(raw.length <= 32 * MiB && totalBytes <= 63 * MiB, "ADAPTER_OUTPUT_BUDGET");
        await writeFile(join(output, path), raw, { flag: "wx", mode: 0o600 });
        files.push({ path, bytes: raw.length, sha256: hash(raw) });
    }
    const single = result.verification.design === "single-observation";
    const report = Buffer.from(single ? observationHtml(result) : html(result));
    need(report.length + totalBytes <= 63 * MiB, "ADAPTER_OUTPUT_BUDGET");
    totalBytes += report.length;
    await writeFile(join(output, "report.html"), report, { flag: "wx", mode: 0o600 });
    files.push({ path: "report.html", bytes: report.length, sha256: hash(report) });
    const sourceFiles = [];
    for (const path of [
        "components/http/paired-evidence.mjs",
        "components/http/resource-evidence.mjs",
        "components/http/collector-clock-evidence.mjs",
        "components/http/jvm-evidence.mjs",
        "components/http/jvm-observation.py",
        "components/http/jvm-clock-capture.py",
        "components/http/paired-report.mjs",
        "components/http/analyzer.mjs",
        "components/http/comparison-plan.mjs",
        "components/http/dependency-lock.mjs",
        "components/http/common.mjs",
        "components/http/suite-evidence.mjs",
        "components/http/probe.mjs",
        "components/http/runner.mjs",
        "components/http/trace-writer.mjs",
        "components/http/direct-phase-clock.mjs",
        "components/http/runner-sanity.mjs",
        "components/http/phase-clock-reference.py",
        "components/http/clock-correlation.mjs",
        "components/http/workflow.mjs",
        "components/paired/core.mjs",
        "src/http-contracts.ts",
        "dist/http-contracts.js",
        "package.json",
        "release/dependency-lock.json",
        ...(result.verification.executorEngine === "go-http-v1"
            ? [
                  "components/http/go-engine.mjs",
                  "components/http/go-clock-evidence.mjs",
                  "components/http/go-clock-probe.py",
                  "components/http/go-process-observer.py",
                  "components/http/go-comparison-runtime.py",
                  "release/http-go-toolchain.json",
              ]
            : []),
    ]) {
        const raw = await bytes(resolve(ROOT, "../..", path));
        sourceFiles.push({ path, bytes: raw.length, sha256: hash(raw) });
    }
    const implementation = {
        files: sourceFiles,
        dependencyBoundary:
            "Local adapter/renderer/analyzer/contracts are content-bound; package lock is declarative, not installed-package attestation",
        componentSha256: hash(await bytes(fileURLToPath(import.meta.url))),
        node: process.version,
        effects: ["read-local-evidence", "create-new-output-directory"],
    };
    const raw = encode(implementation);
    await writeFile(join(output, "implementation.json"), raw, { flag: "wx", mode: 0o600 });
    files.push({ path: "implementation.json", bytes: raw.length, sha256: hash(raw) });
    await writeFile(
        join(output, "evidence.lock.json"),
        encode({
            schemaVersion: 1,
            kind: single ? "http-observation-adapter-output-lock" : "http-paired-adapter-output-lock",
            files,
        }),
        { flag: "wx", mode: 0o600 },
    );
    return {
        directory: output,
        valid: result.verification.valid,
        statisticsEligible: result.verification.statisticsEligible,
        armCount: result.verification.rows.length,
        performanceAccepted: false,
    };
}
function observationHtml(result) {
    const escape = (value) =>
        String(value)
            .replaceAll("&", "&amp;")
            .replaceAll("<", "&lt;")
            .replaceAll(">", "&gt;")
            .replaceAll('"', "&quot;");
    return `<!doctype html><html lang="en"><meta charset="utf-8"><title>HTTP single observation</title><style>body{font:16px system-ui;max-width:1100px;margin:2rem auto;padding:0 1rem}pre{white-space:pre-wrap;overflow-wrap:anywhere}</style><h1>HTTP single observation</h1><p>Evidence valid: ${escape(result.verification.valid)}. One fresh-process observation; statistical inference and performance acceptance remain false.</p><p>No paired effect, confidence interval, repeatability or capacity certification is computed.</p><h2>Observation and descriptive metrics</h2><pre>${escape(JSON.stringify(result.analysis, null, 2))}</pre><details><summary>Complete verification</summary><pre>${escape(JSON.stringify(result.verification, null, 2))}</pre></details></html>\n`;
}
const HELP = `HTTP paired/single-observation evidence adapter — offline verification; no Docker or HTTP execution.
Usage: node components/http/paired-evidence.mjs --plan FROZEN_PLAN --execution SEALED_EXECUTION --out NEW_DIRECTORY
All options are required once. Reads locked original HTTP evidence and controlled-environment receipts.
Preserves every predeclared block/arm/metric; missing, altered, misordered or failed arms invalidate inference.
Version 3 single-observation verifies the actual O lifecycle and emits descriptive observations without a paired analysis or confidence interval.
Writes observations.json, verification.json, analysis.json, report.html and a content lock. Never overwrites an output.
Requires comparison-plan's frozen implementation and typed build/settings/host inputs.
Controller receipt/lock schema and verification limits: docs/http-paired-evidence.md
Exit 0: evidence conversion completed (check valid); exit 6: unsafe, corrupt or unsupported input. Performance stays false.`;
if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
    if (process.argv.length === 3 && process.argv[2] === "--help") process.stdout.write(HELP + "\n");
    else
        try {
            const argv = process.argv.slice(2),
                args = {};
            need(argv.length === 6, "ADAPTER_ARGUMENTS", "Run --help");
            for (let i = 0; i < argv.length; i += 2) {
                need(
                    ["--plan", "--execution", "--out"].includes(argv[i]) && !Object.hasOwn(args, argv[i]),
                    "ADAPTER_ARGUMENTS",
                    "Unknown or repeated option; run --help",
                );
                args[argv[i]] = argv[i + 1];
            }
            process.stdout.write(
                JSON.stringify({
                    ok: true,
                    data: await writePairedEvidence({
                        planDirectory: args["--plan"],
                        executionDirectory: args["--execution"],
                        outputDirectory: args["--out"],
                    }),
                }) + "\n",
            );
        } catch (error) {
            process.stderr.write(
                JSON.stringify({
                    ok: false,
                    error: {
                        code: error instanceof PairedEvidenceError ? error.code : "ADAPTER_IO_OR_CONTRACT",
                        message:
                            error instanceof PairedEvidenceError
                                ? error.message
                                : "Cannot read or write bounded evidence",
                        retryable: false,
                        nextAction: "Inspect retained evidence; use a new plan for any new execution",
                    },
                }) + "\n",
            );
            process.exitCode = 6;
        }
}
