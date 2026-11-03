/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
import { constants, createReadStream } from "node:fs";
import { open, realpath, readdir, mkdtemp, mkdir, writeFile, copyFile, rm } from "node:fs/promises";
import { createHash } from "node:crypto";
import { spawn } from "node:child_process";
import { tmpdir } from "node:os";
import { dirname, join, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { isDeepStrictEqual } from "node:util";

const MiB = 1024 * 1024,
    SHA = /^[a-f0-9]{64}$/,
    HERE = dirname(fileURLToPath(import.meta.url)),
    PARSED = [
        "spec.json",
        "plan.json",
        "plan.lock.json",
        "launch.json",
        "capture.json",
        "server.log",
        "events.ndjson",
        "windows.ndjson",
        "summary.json",
        "manifest.json",
    ].sort();
export class JvmEvidenceError extends Error {
    constructor(code) {
        super(code);
        this.code = code;
    }
}
function need(value, code) {
    if (!value) throw new JvmEvidenceError(code);
}
function same(a, b, code) {
    need(isDeepStrictEqual(a, b), code);
}
const hash = (raw) => createHash("sha256").update(raw).digest("hex");
function integer(value, low = 0, high = Number.MAX_SAFE_INTEGER) {
    return Number.isSafeInteger(value) && value >= low && value <= high;
}
function uint(value) {
    need(typeof value === "string" && /^(0|[1-9][0-9]{0,24})$/.test(value), "JVM_INTEGER_PRECISION");
    return BigInt(value);
}
async function member(root, name, cap, content = true) {
    need(
        typeof name === "string" &&
            !name.startsWith("/") &&
            !name.split("/").some((p) => !p || p === "." || p === ".."),
        "JVM_MEMBER_PATH",
    );
    const path = join(root, name),
        actual = await realpath(path);
    need(actual.startsWith(root + sep), "JVM_MEMBER_CONTAINMENT");
    const handle = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
    try {
        const before = await handle.stat({ bigint: true });
        need(before.isFile() && before.size <= BigInt(cap), "JVM_MEMBER_BUDGET");
        const hashed = createHash("sha256"),
            chunks = [];
        let count = 0;
        for await (const chunk of createReadStream(path, { fd: handle.fd, autoClose: false })) {
            count += chunk.length;
            need(count <= cap && count <= Number(before.size), "JVM_MEMBER_CHANGED");
            hashed.update(chunk);
            if (content) chunks.push(chunk);
        }
        const after = await handle.stat({ bigint: true });
        need(
            count === Number(before.size) &&
                ["dev", "ino", "size", "mtimeNs", "ctimeNs"].every((k) => before[k] === after[k]),
            "JVM_MEMBER_CHANGED",
        );
        return {
            path: name,
            bytes: count,
            sha256: hashed.digest("hex"),
            ...(content ? { raw: Buffer.concat(chunks) } : {}),
        };
    } finally {
        await handle.close();
    }
}
const identity = ({ path, bytes, sha256 }) => ({ path, bytes, sha256 });
const decode = (raw) => JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(raw));
const json = async (root, name, cap = 4 * MiB) => decode((await member(root, name, cap)).raw);
function base64(value, cap) {
    need(
        typeof value === "string" && value.length <= Math.ceil(cap / 3) * 4 && /^[A-Za-z0-9+/]*={0,2}$/.test(value),
        "JVM_RAW_BASE64",
    );
    const result = Buffer.from(value, "base64");
    need(result.length <= cap && result.toString("base64") === value, "JVM_RAW_BASE64");
    return result;
}
function expectedArgv(plan, argument) {
    const jvm = plan.settings.jvm;
    return [
        "/opt/jdk/bin/java",
        "-Xms" + jvm.heapMinBytes,
        "-Xmx" + jvm.heapMaxBytes,
        "-XX:MaxDirectMemorySize=" + jvm.maxDirectMemoryBytes,
        "-XX:ActiveProcessorCount=" + jvm.activeProcessorCount,
        "@/opt/hugegraph/bin/jvm-module.options",
        "-Djava.io.tmpdir=/tmp",
        "-Dlog4j.configurationFile=/control/conf/log4j2.xml",
        argument,
        "-Djava.security.properties=/control/conf/java-security.properties",
        "-cp",
        plan.classpath.map((p) => "/opt/hugegraph/" + p).join(":"),
        "org.apache.hugegraph.bootstrap.HugeGraphServerBootstrap",
        "true",
        "/control/conf/gremlin-server.yaml",
        "/control/conf/rest-server.properties",
    ];
}
function fileProof(proof, expected) {
    need(
        proof &&
            expected?.type === "file" &&
            SHA.test(proof.sha256) &&
            proof.sha256 === expected.sha256 &&
            proof.bytes === expected.bytes &&
            integer(proof.bytes, 1) &&
            integer(proof.deviceMajor, 0, 0xffffffff) &&
            integer(proof.deviceMinor, 0, 0xffffffff) &&
            uint(proof.inode) > 0n,
        "JVM_JDK_MEMBER_IDENTITY",
    );
}
function processStat(raw, pid, start) {
    const text = raw.toString("utf8"),
        tail = text
            .slice(text.lastIndexOf(")") + 2)
            .trim()
            .split(/\s+/);
    need(
        text.startsWith(`${pid} (`) && tail.length >= 20 && !["Z", "X", "x"].includes(tail[0]) && tail[19] === start,
        "JVM_PROCESS_STAT",
    );
    uint(tail[19]);
}
function runtimeIdentity(runtime, launch, env, envSha, context) {
    need(
        runtime?.schemaVersion === 1 &&
            runtime.kind === "http-jvm-observation-runtime" &&
            runtime.planSha256 === launch.planSha256 &&
            runtime.environmentPlanSha256 === envSha &&
            runtime.attemptId === env.attemptId,
        "JVM_RUNTIME_BINDING",
    );
    const p = runtime.process,
        declared = launch.process;
    need(
        p?.pid === declared.pid &&
            p.startTimeTicksBefore === declared.startTimeTicks &&
            p.startTimeTicksAfter === declared.startTimeTicks &&
            p.hostBootId === declared.hostBootId &&
            p.executablePath === "/opt/jdk/bin/java",
        "JVM_PROCESS_IDENTITY",
    );
    need(uint(runtime.observedBeforeMonotonicNs) <= uint(runtime.observedAfterMonotonicNs), "JVM_RUNTIME_TIME");
    const cmdline = base64(p.cmdlineBase64, 131072);
    need(cmdline.at(-1) === 0, "JVM_CMDLINE_TERMINATION");
    same(cmdline.subarray(0, -1).toString("utf8").split("\0"), launch.argv, "JVM_ACTUAL_ARGV");
    const files = env.sources.javaHome.files;
    fileProof(
        p.java,
        files.find((f) => f.path === "bin/java"),
    );
    fileProof(
        p.libjvm,
        files.find((f) => f.path === "lib/server/libjvm.so"),
    );
    need(
        p.java.sha256 === launch.jdk.javaSha256 && p.libjvm.sha256 === launch.jdk.libjvmSha256,
        "JVM_LOADED_JDK_BINDING",
    );
    const maps = base64(p.mapsBase64, MiB).toString("utf8").trimEnd().split("\n");
    let executable = false,
        mappings = 0;
    for (const line of maps) {
        if (!/(?:^|\/)libjvm\.so(?: \(deleted\))?$/.test(line)) continue;
        const match =
            /^([0-9a-f]+)-([0-9a-f]+)\s+([r-][w-][x-][ps])\s+([0-9a-f]+)\s+([0-9a-f]+):([0-9a-f]+)\s+([0-9]+)\s+(.+)$/.exec(
                line,
            );
        need(
            match &&
                BigInt("0x" + match[1]) < BigInt("0x" + match[2]) &&
                match[8] === "/opt/jdk/lib/server/libjvm.so" &&
                BigInt("0x" + match[5]) === BigInt(p.libjvm.deviceMajor) &&
                BigInt("0x" + match[6]) === BigInt(p.libjvm.deviceMinor) &&
                BigInt(match[7]) === uint(p.libjvm.inode),
            "JVM_LOADED_LIBJVM_MAP",
        );
        mappings++;
        executable ||= match[3].includes("x");
    }
    need(mappings > 0 && executable, "JVM_LOADED_LIBJVM_MISSING");
    let rawProcessIdentity = "unknown";
    if (p.statBeforeBase64 !== undefined || p.statAfterBase64 !== undefined) {
        processStat(base64(p.statBeforeBase64, 65536), p.pid, declared.startTimeTicks);
        processStat(base64(p.statAfterBase64, 65536), p.pid, declared.startTimeTicks);
        rawProcessIdentity = "passed";
    }
    let externalProcessIdentity = "unknown";
    if (context.processIdentity) {
        const supplied = context.processIdentity;
        need(
            supplied.pid === p.pid &&
                String(supplied.startTimeTicks) === declared.startTimeTicks &&
                (supplied.bootId ?? supplied.hostBootId) === p.hostBootId &&
                (supplied.javaExecutableSha256 === undefined || supplied.javaExecutableSha256 === p.java.sha256),
            "JVM_EXTERNAL_PROCESS_IDENTITY",
        );
        externalProcessIdentity = "passed";
    }
    if (context.hostBootId !== undefined) same(p.hostBootId, context.hostBootId, "JVM_HOST_BOOT_IDENTITY");
    return {
        actualArgv: "passed",
        loadedLibjvm: "passed",
        rawProcessIdentity,
        externalProcessIdentity,
        hostBoot: context.hostBootId === undefined ? "unknown" : "passed",
        mappings,
        process: declared,
    };
}

const CLOCK_BOOT = /^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/;
const CLOCK_NAMESPACE = /^time:\[[0-9]{1,20}\]$/;
function clockObject(value, fields, code) {
    need(value !== null && typeof value === "object" && !Array.isArray(value), code);
    same(Object.keys(value).sort(), [...fields].sort(), code);
}
function clockText(value, cap) {
    return typeof value === "string" && value.length > 0 && Buffer.byteLength(value) <= cap && !value.includes("\0");
}
function clockOffsets(raw) {
    need(clockText(raw, 256), "JVM_CLOCK_OFFSETS");
    const rows = raw
        .trim()
        .split("\n")
        .map((line) => line.trim().split(/\s+/));
    need(
        rows.length === 2 &&
            rows[0][0] === "monotonic" &&
            rows[1][0] === "boottime" &&
            rows.every(
                (row) =>
                    row.length === 3 &&
                    /^-?[0-9]{1,12}$/.test(row[1]) &&
                    /^[0-9]{1,9}$/.test(row[2]) &&
                    BigInt(row[2]) < 1000000000n,
            ),
        "JVM_CLOCK_OFFSETS",
    );
    return Object.fromEntries(
        rows.map(([name, seconds, nanos]) => [
            name,
            { seconds: BigInt(seconds).toString(), nanoseconds: BigInt(nanos).toString() },
        ]),
    );
}
function clockFile(value, digest, source) {
    clockObject(value, ["sourcePath", "path", "metadata", "sha256", "digestRead"], "JVM_CLOCK_EXECUTABLE_FIELDS");
    need(
        value.sourcePath === source &&
            value.digestRead === digest &&
            clockText(value.path, 4096) &&
            value.path.startsWith("/") &&
            !value.path.endsWith(" (deleted)"),
        "JVM_CLOCK_EXECUTABLE_SOURCE",
    );
    clockObject(
        value.metadata,
        ["bytes", "deviceMajor", "deviceMinor", "inode", "mtimeNs", "ctimeNs"],
        "JVM_CLOCK_EXECUTABLE_METADATA",
    );
    for (const field of ["bytes", "inode", "mtimeNs", "ctimeNs"]) uint(value.metadata[field]);
    need(
        uint(value.metadata.bytes) > 0n &&
            uint(value.metadata.bytes) <= 268435456n &&
            uint(value.metadata.inode) > 0n &&
            integer(value.metadata.deviceMajor, 0, 0xffffffff) &&
            integer(value.metadata.deviceMinor, 0, 0xffffffff),
        "JVM_CLOCK_EXECUTABLE_METADATA",
    );
    need(digest ? SHA.test(value.sha256) : value.sha256 === null, "JVM_CLOCK_EXECUTABLE_DIGEST");
}
function clockProcess(value, role, digest) {
    const fields = [
        "pid",
        "statRaw",
        "statAfterRaw",
        "startTimeTicks",
        "startTimeTicksAfter",
        "bootId",
        "timeNamespace",
        "timeForChildrenNamespace",
        "timeOffsetsRaw",
        "executable",
    ];
    if (role === "capture") fields.push("python", "clock");
    clockObject(value, fields, "JVM_CLOCK_PROCESS_FIELDS");
    need(
        integer(value.pid, 1, 4194304) &&
            CLOCK_BOOT.test(value.bootId) &&
            CLOCK_NAMESPACE.test(value.timeNamespace) &&
            CLOCK_NAMESPACE.test(value.timeForChildrenNamespace),
        "JVM_CLOCK_PROCESS_IDENTITY",
    );
    for (const [raw, start] of [
        [value.statRaw, value.startTimeTicks],
        [value.statAfterRaw, value.startTimeTicksAfter],
    ]) {
        need(clockText(raw, 4096), "JVM_CLOCK_STAT_RAW");
        processStat(Buffer.from(raw), value.pid, start);
        need(
            /^[RSDTtWIPK]$/.test(
                raw
                    .slice(raw.lastIndexOf(") ") + 2)
                    .trim()
                    .split(/\s+/)[0],
            ),
            "JVM_CLOCK_STAT_RAW",
        );
    }
    clockOffsets(value.timeOffsetsRaw);
    clockFile(value.executable, digest, role === "capture" ? "/proc/self/exe" : `/proc/${value.pid}/exe`);
    if (role === "capture") {
        clockObject(value.python, ["reportedExecutable", "version", "implementation"], "JVM_CLOCK_PYTHON_FIELDS");
        need(
            clockText(value.python.reportedExecutable, 4096) &&
                value.python.reportedExecutable.startsWith("/") &&
                clockText(value.python.version, 4096) &&
                /^[a-z][a-z0-9_]{0,63}$/.test(value.python.implementation),
            "JVM_CLOCK_PYTHON",
        );
        clockObject(
            value.clock,
            ["api", "implementation", "monotonic", "adjustable", "resolutionSeconds"],
            "JVM_CLOCK_INFO_FIELDS",
        );
        need(
            value.clock.api === "time.monotonic_ns" &&
                clockText(value.clock.implementation, 256) &&
                typeof value.clock.monotonic === "boolean" &&
                typeof value.clock.adjustable === "boolean" &&
                Number.isFinite(value.clock.resolutionSeconds) &&
                value.clock.resolutionSeconds > 0,
            "JVM_CLOCK_INFO",
        );
    }
}
function clockChanges(before, after) {
    const changed = [];
    if (before.captureComponentSha256 !== null && before.captureComponentSha256 !== after.captureComponentSha256)
        changed.push("captureComponentSha256");
    for (const role of ["capture", "jvm"]) {
        const first = before[role],
            last = after[role];
        if (first === null || last === null) continue;
        for (const field of [
            "pid",
            "startTimeTicks",
            "bootId",
            "timeNamespace",
            "timeForChildrenNamespace",
            "timeOffsetsRaw",
            "python",
            "clock",
        ])
            if (first[field] != null && !isDeepStrictEqual(first[field], last[field])) changed.push(`${role}.${field}`);
        for (const point of [first, last])
            if (point.startTimeTicks !== null && point.startTimeTicks !== point.startTimeTicksAfter)
                changed.push(`${role}.process`);
        if (first.executable && last.executable)
            for (const field of ["sourcePath", "path", "metadata"])
                if (!isDeepStrictEqual(first.executable[field], last.executable[field]))
                    changed.push(`${role}.executable.${field}`);
    }
    return [...new Set(changed)].sort();
}
function clockSnapshot(point, origin, digest) {
    clockObject(
        point,
        [
            "sourceOrigin",
            "captureComponentSha256",
            "startedMonotonicNs",
            "finishedMonotonicNs",
            "capture",
            "jvm",
            "unavailable",
        ],
        "JVM_CLOCK_SNAPSHOT_FIELDS",
    );
    need(
        point.sourceOrigin === origin && uint(point.startedMonotonicNs) <= uint(point.finishedMonotonicNs),
        "JVM_CLOCK_SNAPSHOT_WINDOW",
    );
    need(
        point.unavailable !== null &&
            typeof point.unavailable === "object" &&
            !Array.isArray(point.unavailable) &&
            Object.keys(point.unavailable).length <= 21 &&
            Object.entries(point.unavailable).every(
                ([key, reason]) =>
                    /^(source|component|(?:capture|jvm)\.(?:statRaw|statAfterRaw|startTimeTicks|startTimeTicksAfter|bootId|timeNamespace|timeForChildrenNamespace|timeOffsetsRaw|executable|clock))$/.test(
                        key,
                    ) && clockText(reason, 128),
            ),
        "JVM_CLOCK_UNAVAILABLE",
    );
    for (const role of ["capture", "jvm"])
        need(
            point[role] === null || (typeof point[role] === "object" && !Array.isArray(point[role])),
            "JVM_CLOCK_PARTIAL_PROCESS",
        );
    if (origin === "fixture" || point.unavailable.source) {
        need(
            point.capture === null && point.jvm === null && point.captureComponentSha256 === null,
            "JVM_CLOCK_NONPRODUCTION_SOURCE",
        );
        same(
            point.unavailable,
            { source: origin === "fixture" ? "fixture-has-no-production-clock-identity" : "platform-not-linux" },
            "JVM_CLOCK_NONPRODUCTION_SOURCE",
        );
    }
    if (Object.keys(point.unavailable).length) return false;
    need(SHA.test(point.captureComponentSha256), "JVM_CLOCK_CAPTURE_COMPONENT");
    clockProcess(point.capture, "capture", digest);
    clockProcess(point.jvm, "jvm", false);
    return true;
}
function modeClock(probe, label, launch) {
    need(
        probe?.schemaVersion === 1 && probe.kind === "hugegraph-http-mode-probe" && SHA.test(probe.componentSha256),
        "JVM_CLOCK_MODE_RECEIPT",
    );
    const value = probe.clockIdentity;
    clockObject(
        value,
        [
            "schemaVersion",
            "kind",
            "sourceOrigin",
            "captureComponentSha256",
            "before",
            "after",
            "status",
            "changedFields",
            "verified",
            "hostAssociation",
            "executableHashMaxBytes",
            "executableHashMaxNs",
        ],
        "JVM_CLOCK_FIELDS",
    );
    need(
        value.schemaVersion === 1 &&
            value.kind === "http-jvm-mode-clock-identity" &&
            ["linux-procfs", "fixture"].includes(value.sourceOrigin) &&
            value.verified === false &&
            value.hostAssociation === "unverified" &&
            value.executableHashMaxBytes === 268435456 &&
            value.executableHashMaxNs === "2000000000" &&
            ["observed", "unknown"].includes(value.status),
        "JVM_CLOCK_KIND",
    );
    need(Buffer.byteLength(JSON.stringify(value)) <= 65536, "JVM_CLOCK_RAW_BUDGET");
    let complete = value.sourceOrigin === "linux-procfs";
    for (const [point, digest] of [
        [value.before, true],
        [value.after, false],
    ])
        if (!clockSnapshot(point, value.sourceOrigin, digest)) complete = false;
    same(value.captureComponentSha256, value.before.captureComponentSha256, "JVM_CLOCK_CAPTURE_COMPONENT_BINDING");
    const changes = clockChanges(value.before, value.after);
    same(value.changedFields, changes, "JVM_CLOCK_CHANGED_FIELDS_REBUILD");
    need(value.status === (complete && changes.length === 0 ? "observed" : "unknown"), "JVM_CLOCK_STATUS_REBUILD");
    need(
        integer(probe.startedMonotonicNs) &&
            integer(probe.finishedMonotonicNs) &&
            BigInt(probe.startedMonotonicNs) <= uint(value.before.startedMonotonicNs) &&
            uint(value.before.finishedMonotonicNs) <= uint(value.after.startedMonotonicNs) &&
            uint(value.after.finishedMonotonicNs) <= BigInt(probe.finishedMonotonicNs),
        "JVM_CLOCK_MODE_WINDOW",
    );
    if (!complete || changes.length) return { label, observed: false, value };
    for (const [point, identity] of [
        [value.before, probe.identityBefore],
        [value.after, probe.identityAfter],
    ]) {
        const j = point.jvm;
        need(
            identity?.pid === j.pid &&
                identity.startTimeTicks === j.startTimeTicks &&
                identity.bootId === j.bootId &&
                identity.javaExecutable === j.executable.path &&
                identity.javaExecutableSha256 === launch.jdk.javaSha256 &&
                j.pid === launch.process.pid &&
                j.startTimeTicks === launch.process.startTimeTicks &&
                j.bootId === launch.process.hostBootId,
            "JVM_CLOCK_MODE_PROCESS_BINDING",
        );
        need(point.capture.bootId === j.bootId, "JVM_CLOCK_CAPTURE_BOOT_BINDING");
    }
    return { label, observed: true, value };
}
function initialClock(value) {
    if (!value) return null;
    need(value.domain === "linux-clock-monotonic" && value.alignment === "unverified", "JVM_INITIAL_CLOCK_KIND");
    for (const key of ["namespace", "timeForChildren", "offsets"]) {
        clockObject(value[key], ["available", "value", "reason"], "JVM_INITIAL_CLOCK_FIELDS");
        need(typeof value[key].available === "boolean", "JVM_INITIAL_CLOCK_AVAILABLE");
        if (!value[key].available) return null;
        need(value[key].reason === null, "JVM_INITIAL_CLOCK_REASON");
    }
    need(
        CLOCK_NAMESPACE.test(value.namespace.value) && CLOCK_NAMESPACE.test(value.timeForChildren.value),
        "JVM_INITIAL_CLOCK_NAMESPACE",
    );
    return {
        timeNamespace: value.namespace.value,
        timeForChildrenNamespace: value.timeForChildren.value,
        timeOffsets: clockOffsets(value.offsets.value),
    };
}
function clockDomain(value) {
    return {
        bootId: value.bootId,
        timeNamespace: value.timeNamespace,
        timeForChildrenNamespace: value.timeForChildrenNamespace,
        timeOffsets: clockOffsets(value.timeOffsetsRaw),
    };
}
function clockParent(raw) {
    const field = raw
        .slice(raw.lastIndexOf(") ") + 2)
        .trim()
        .split(/\s+/)[1];
    need(
        typeof field === "string" && /^[1-9][0-9]{0,6}$/.test(field) && Number(field) <= 4194304,
        "JVM_INITIAL_CAPTURE_PARENT",
    );
    return Number(field);
}

/** Independently reconstruct raw clock identities. No phase or performance gate is granted here. */
export function verifyJvmClockIdentity(runtime, launch, context = {}) {
    const expected = context.clockExpected ?? {},
        probes = expected.modeProbes,
        pending = [
            "initial-capture-runtime-source",
            "initial-clock-read-window",
            "independent-container-runtime-binding",
            "jdk-binary-to-fixed-source-mapping",
            "loaded-libjvm-window-identity",
            "host-collector-domain-binding",
            "phase-attribution",
            "observer-overhead-calibration",
        ];
    const result = {
        schemaVersion: 1,
        kind: "http-jvm-clock-identity-verification",
        rawStatus: "unknown",
        hostDomain: "unverified",
        verified: false,
        performanceAccepted: false,
        domain: null,
        bindings: {},
        pendingGates: pending,
    };
    if (!probes?.pre?.clockIdentity || !probes?.post?.clockIdentity) {
        pending.push("mode-clock-receipts-unavailable");
        return result;
    }
    need(
        runtime?.schemaVersion === 1 &&
            runtime.kind === "http-jvm-observation-runtime" &&
            runtime.process?.pid === launch?.process?.pid &&
            runtime.process.startTimeTicksBefore === launch.process.startTimeTicks &&
            runtime.process.startTimeTicksAfter === launch.process.startTimeTicks &&
            runtime.process.hostBootId === launch.process.hostBootId &&
            CLOCK_BOOT.test(launch.process.hostBootId) &&
            integer(launch.process.pid, 1, 4194304) &&
            SHA.test(launch.jdk.javaSha256) &&
            SHA.test(launch.jdk.libjvmSha256) &&
            runtime.process.java?.sha256 === launch.jdk.javaSha256 &&
            runtime.process.libjvm?.sha256 === launch.jdk.libjvmSha256,
        "JVM_CLOCK_INITIAL_PROCESS",
    );
    need(uint(runtime.observedBeforeMonotonicNs) <= uint(runtime.observedAfterMonotonicNs), "JVM_CLOCK_RECEIPT_ORDER");
    for (const raw of [runtime.process.statBeforeBase64, runtime.process.statAfterBase64])
        processStat(base64(raw, 65536), launch.process.pid, launch.process.startTimeTicks);
    const pre = modeClock(probes.pre, "pre", launch),
        post = modeClock(probes.post, "post", launch);
    if (!pre.observed || !post.observed) {
        pending.push("mode-clock-observations-unavailable");
        return result;
    }
    const first = initialClock(runtime.clock?.jvm),
        capture = initialClock(runtime.clock?.capture);
    if (first === null || capture === null) {
        pending.push("initial-clock-observations-unavailable");
        return result;
    }
    const domain = { bootId: launch.process.hostBootId, ...first };
    same(first, capture, "JVM_INITIAL_CAPTURE_DOMAIN");
    need(
        uint(runtime.observedAfterMonotonicNs) <= uint(pre.value.before.startedMonotonicNs) &&
            uint(pre.value.after.finishedMonotonicNs) <= uint(post.value.before.startedMonotonicNs),
        "JVM_CLOCK_RECEIPT_ORDER",
    );
    for (const probe of [pre, post]) {
        for (const point of [probe.value.before, probe.value.after]) {
            same(point.captureComponentSha256, pre.value.captureComponentSha256, "JVM_CLOCK_CAPTURE_COMPONENT_BINDING");
            same(clockDomain(point.jvm), domain, "JVM_CLOCK_DOMAIN_CHANGED");
            same(clockDomain(point.capture), domain, "JVM_CAPTURE_CLOCK_DOMAIN");
            const meta = point.jvm.executable.metadata,
                java = runtime.process.java;
            need(
                meta.deviceMajor === java.deviceMajor &&
                    meta.deviceMinor === java.deviceMinor &&
                    meta.inode === java.inode &&
                    uint(meta.bytes) === BigInt(java.bytes),
                "JVM_CLOCK_JAVA_METADATA",
            );
            need(
                point.capture.clock.implementation === "clock_gettime(CLOCK_MONOTONIC)" &&
                    point.capture.clock.monotonic === true &&
                    point.capture.clock.adjustable === false,
                "JVM_CLOCK_MONOTONIC_IMPLEMENTATION",
            );
            same(
                point.jvm.executable.metadata,
                pre.value.before.jvm.executable.metadata,
                "JVM_CLOCK_JAVA_METADATA_CHANGED",
            );
            same(
                point.capture.executable.metadata,
                pre.value.before.capture.executable.metadata,
                "JVM_CLOCK_PYTHON_METADATA_CHANGED",
            );
            same(point.capture.python, pre.value.before.capture.python, "JVM_CLOCK_PYTHON_CHANGED");
        }
        same(
            probe.value.before.capture.executable.sha256,
            pre.value.before.capture.executable.sha256,
            "JVM_CLOCK_PYTHON_BINARY_CHANGED",
        );
        for (const [field, actual] of [
            ["modeProbeComponentSha256", probes[probe.label].componentSha256],
            ["captureComponentSha256", probe.value.captureComponentSha256],
            ["containerPythonExecutableSha256", probe.value.before.capture.executable.sha256],
            ["containerPythonVersion", probe.value.before.capture.python.version],
            ["containerPythonImplementation", probe.value.before.capture.python.implementation],
        ]) {
            if (expected[field] === undefined) {
                pending.push(field);
                result.bindings[field] = "unknown";
            } else {
                need(typeof expected[field] === "string" && expected[field] === actual, "JVM_CLOCK_EXTERNAL_SOURCE");
                result.bindings[field] = "matched-expectation";
            }
        }
    }
    if (expected.jdkMonotonicSource !== undefined) {
        const source = expected.jdkMonotonicSource;
        clockObject(
            source,
            ["kind", "fixedSourceRevision", "sourceDigests", "javaExecutableSha256", "libjvmSha256", "clockApi"],
            "JVM_CLOCK_JDK_SOURCE_FIELDS",
        );
        need(
            source.kind === "http-jdk-monotonic-source-expectation" &&
                source.fixedSourceRevision === "30ef840c3736270330fc0a26849c2456406facfe" &&
                source.clockApi === "CLOCK_MONOTONIC" &&
                source.javaExecutableSha256 === launch.jdk.javaSha256 &&
                source.libjvmSha256 === launch.jdk.libjvmSha256 &&
                Array.isArray(source.sourceDigests) &&
                source.sourceDigests.length > 0 &&
                source.sourceDigests.length <= 8 &&
                source.sourceDigests.every((v) => SHA.test(v)),
            "JVM_CLOCK_JDK_SOURCE_EXPECTATION",
        );
        result.bindings.jdkMonotonicSource = "matched-expectation";
    } else pending.push("jdk-monotonic-source-expectation");
    if (runtime.captureIdentity !== undefined) {
        const value = runtime.captureIdentity;
        clockObject(
            value,
            [
                "schemaVersion",
                "kind",
                "sourceOrigin",
                "captureComponentSha256",
                "snapshot",
                "verified",
                "hostAssociation",
                "executableHashMaxBytes",
                "executableHashMaxNs",
            ],
            "JVM_INITIAL_CAPTURE_FIELDS",
        );
        need(
            value.schemaVersion === 1 &&
                value.kind === "http-jvm-initial-clock-identity" &&
                ["linux-procfs", "fixture"].includes(value.sourceOrigin) &&
                value.verified === false &&
                value.hostAssociation === "unverified" &&
                value.executableHashMaxBytes === 268435456 &&
                value.executableHashMaxNs === "2000000000",
            "JVM_INITIAL_CAPTURE_KIND",
        );
        need(Buffer.byteLength(JSON.stringify(value)) <= 65536, "JVM_CLOCK_RAW_BUDGET");
        const observed = clockSnapshot(value.snapshot, value.sourceOrigin, true),
            point = value.snapshot;
        same(value.captureComponentSha256, point.captureComponentSha256, "JVM_CLOCK_CAPTURE_COMPONENT_BINDING");
        if (!observed || clockChanges(point, point).length) {
            pending.push("initial-capture-observations-unavailable");
            return result;
        }
        for (const role of ["capture", "jvm"]) {
            same(clockDomain(point[role]), domain, "JVM_INITIAL_CAPTURE_DOMAIN");
            same(
                point[role].executable.metadata,
                pre.value.before[role].executable.metadata,
                "JVM_INITIAL_CAPTURE_EXECUTABLE",
            );
        }
        need(
            point.jvm.pid === launch.process.pid &&
                point.jvm.startTimeTicks === launch.process.startTimeTicks &&
                point.jvm.startTimeTicksAfter === launch.process.startTimeTicks &&
                point.jvm.executable.path === runtime.process.executablePath,
            "JVM_INITIAL_CAPTURE_PROCESS",
        );
        for (const raw of [
            point.jvm.statRaw,
            point.jvm.statAfterRaw,
            base64(runtime.process.statBeforeBase64, 65536).toString("utf8"),
            base64(runtime.process.statAfterBase64, 65536).toString("utf8"),
            ...[pre, post].flatMap((probe) => [
                probe.value.before.jvm.statRaw,
                probe.value.before.jvm.statAfterRaw,
                probe.value.after.jvm.statRaw,
                probe.value.after.jvm.statAfterRaw,
            ]),
        ])
            same(clockParent(raw), point.capture.pid, "JVM_INITIAL_CAPTURE_PARENT");
        same(point.capture.python, pre.value.before.capture.python, "JVM_INITIAL_CAPTURE_PYTHON");
        same(point.capture.clock, pre.value.before.capture.clock, "JVM_INITIAL_CAPTURE_CLOCK");
        same(
            point.capture.executable.sha256,
            pre.value.before.capture.executable.sha256,
            "JVM_INITIAL_CAPTURE_EXECUTABLE",
        );
        same(point.captureComponentSha256, pre.value.captureComponentSha256, "JVM_CLOCK_CAPTURE_COMPONENT_BINDING");
        const begin = uint(runtime.clock.observedBeforeMonotonicNs),
            end = uint(runtime.clock.observedAfterMonotonicNs);
        need(
            uint(runtime.observedBeforeMonotonicNs) <= begin &&
                begin <= uint(point.startedMonotonicNs) &&
                uint(point.finishedMonotonicNs) <= end &&
                end <= uint(runtime.observedAfterMonotonicNs),
            "JVM_INITIAL_CLOCK_READ_WINDOW",
        );
        result.bindings.initialCapture = "raw-consistent";
        result.bindings.initialClockWindow = "bounded";
        pending.splice(pending.indexOf("initial-clock-read-window"), 1);
    }
    result.rawStatus = "raw-consistent";
    result.hostDomain = "candidate";
    result.domain = domain;
    result.pendingGates = [...new Set(pending)];
    return result;
}
const parserOwners = new Set(),
    OWNER_SIGNALS = ["SIGTERM", "SIGINT"];
let ownerListeners = null,
    ownerSignal = null,
    externalSignalControl = false,
    restoringDefaultSignal = false;
function interrupted() {
    return new JvmEvidenceError("JVM_PARSER_INTERRUPTED");
}
function registerParserOwner() {
    if (restoringDefaultSignal) throw interrupted();
    const owner = { signal: ownerSignal, cancel: null };
    parserOwners.add(owner);
    return owner;
}
function checkParserOwner(owner) {
    if (owner.signal) throw interrupted();
}
function listenForOwnerSignals() {
    if (ownerListeners) return;
    ownerListeners = new Map();
    for (const signal of OWNER_SIGNALS) {
        const listener = () => {
            // Prepend our listener so an existing once-handler is observed before it removes itself.
            externalSignalControl ||= process.rawListeners(signal).some((fn) => fn !== listener);
            ownerSignal ||= signal;
            for (const owner of parserOwners) {
                owner.signal ||= ownerSignal;
                owner.cancel?.();
            }
        };
        ownerListeners.set(signal, listener);
        process.prependListener(signal, listener);
    }
}
async function releaseParserOwner(owner) {
    parserOwners.delete(owner);
    if (parserOwners.size) return;
    if (ownerListeners) {
        for (const [signal, listener] of ownerListeners) process.removeListener(signal, listener);
        ownerListeners = null;
    }
    const signal = ownerSignal;
    if (signal && !externalSignalControl && process.listenerCount(signal) === 0) {
        // Refuse new parser owners while the original default termination is being delivered.
        restoringDefaultSignal = true;
        process.kill(process.pid, signal);
        await new Promise((done) => setImmediate(done));
        return;
    }
    ownerSignal = null;
    externalSignalControl = false;
}
async function parseAgain(owner, python, script, args, timeoutMs) {
    checkParserOwner(owner);
    listenForOwnerSignals();
    await new Promise((ok, reject) => {
        let output = 0,
            failure,
            timer,
            killTimer,
            closed = false,
            settled = false,
            killApplied = false;
        const child = spawn(python, ["-I", "-B", script, "parse", ...args], {
            detached: true,
            stdio: ["ignore", "pipe", "pipe"],
            env: { PATH: "/usr/bin:/usr/local/bin:/bin", LANG: "C.UTF-8", LC_ALL: "C.UTF-8" },
        });
        const groupAlive = () => {
            if (!Number.isSafeInteger(child.pid)) return false;
            try {
                process.kill(-child.pid, 0);
                return true;
            } catch (error) {
                return error.code !== "ESRCH";
            }
        };
        const finish = () => {
            if (settled || !closed) return;
            settled = true;
            clearTimeout(timer);
            clearTimeout(killTimer);
            owner.cancel = null;
            if (failure) reject(failure);
            else ok();
        };
        const stop = (code) => {
            failure ||= new JvmEvidenceError(code);
            if (killTimer || killApplied || settled) return;
            try {
                process.kill(-child.pid, "SIGTERM");
            } catch {}
            killTimer = setTimeout(() => {
                killApplied = true;
                try {
                    process.kill(-child.pid, "SIGKILL");
                } catch {}
                if (closed) finish();
            }, 1000);
        };
        owner.cancel = () => stop("JVM_PARSER_INTERRUPTED");
        for (const stream of [child.stdout, child.stderr])
            stream.on("data", (raw) => {
                output += raw.length;
                if (output > 65536) stop("JVM_PARSER_OUTPUT_BUDGET");
            });
        child.on("error", () => {
            failure ||= new JvmEvidenceError("JVM_PARSER_EXECUTION");
        });
        child.on("close", (code, signal) => {
            closed = true;
            clearTimeout(timer);
            if (signal || ![0, 2].includes(code)) failure ||= new JvmEvidenceError("JVM_PARSER_EXECUTION");
            // A leader can close its pipes before other members of its detached group exit.
            if (groupAlive() && !killApplied) stop("JVM_PARSER_GROUP_REMAINED");
            else finish();
        });
        timer = setTimeout(() => stop("JVM_PARSER_TIMEOUT"), timeoutMs);
    });
}
function unavailable(enabled, reason) {
    return {
        schemaVersion: 1,
        kind: "http-jvm-evidence-verification",
        enabled,
        status: "unavailable",
        evidenceIntegrity: "unavailable",
        parserRecomputed: false,
        identity: null,
        gc: null,
        jit: null,
        issues: [reason],
        pendingGates: [reason, "jvm-to-host-clock", "phase-attribution", "observer-overhead-calibration"],
        clockAlignment: "unverified",
        phaseAttribution: "unavailable",
        performanceAccepted: false,
    };
}

// context.settings is the caller's predeclared environment settings, never inferred from observed output.
// Optional context.environmentPlanSha256/hostBootId/processIdentity bind this observation to an already verified arm.
export async function verifyJvmEvidence(environmentDir, parsedDir, context = {}) {
    if (context.settings?.jvmObservation === undefined) return unavailable(false, "jvm-observation-not-enabled");
    let temporary, owner;
    try {
        const root = await realpath(resolve(environmentDir)),
            parsed = await realpath(resolve(parsedDir));
        const planFile = await member(root, "plan.json", 16 * MiB),
            env = decode(planFile.raw),
            envSha = planFile.sha256;
        const envLock = await json(root, "plan.lock.json");
        need(
            env.schemaVersion === 1 &&
                env.kind === "hugegraph-http-environment-plan" &&
                /^[a-f0-9]{32}$/.test(env.attemptId) &&
                envLock.planSha256 === envSha &&
                envLock.attemptId === env.attemptId,
            "JVM_ENVIRONMENT_PLAN",
        );
        if (context.environmentPlanSha256 !== undefined)
            same(envSha, context.environmentPlanSha256, "JVM_ENVIRONMENT_PLAN");
        const declaredSettings = structuredClone(env.settings),
            expectedSettings = structuredClone(context.settings);
        delete declaredSettings.mode;
        delete expectedSettings.mode;
        same(declaredSettings, expectedSettings, "JVM_DECLARED_SETTINGS");
        need(env.jvmObservation?.planDirectory === "jvm-observation", "JVM_OBSERVATION_PLAN");
        const frozen = new Map();
        for (const name of [
            "jvm-observation.py",
            "jvm-observation/spec.json",
            "jvm-observation/plan.json",
            "jvm-observation/plan.lock.json",
        ]) {
            const actual = await member(root, name, MiB),
                entries = env.controlFiles.filter((f) => f.path === name);
            need(entries.length === 1, "JVM_CONTROL_INVENTORY");
            same(identity(actual), entries[0], "JVM_CONTROL_DIGEST");
            frozen.set(name, actual);
        }
        const trusted = await member(await realpath(HERE), "jvm-observation.py", MiB);
        need(trusted.sha256 === frozen.get("jvm-observation.py").sha256, "JVM_PARSER_IMPLEMENTATION");
        const observation = decode(frozen.get("jvm-observation/plan.json").raw),
            spec = decode(frozen.get("jvm-observation/spec.json").raw);
        need(
            observation.componentSha256 === trusted.sha256 &&
                observation.clockAlignment === "unverified" &&
                observation.performanceAccepted === false &&
                env.jvmObservation.planSha256 === frozen.get("jvm-observation/plan.json").sha256,
            "JVM_PARSER_PLAN_IDENTITY",
        );
        same(spec, context.settings.jvmObservation, "JVM_SPEC_DIFFER");
        need(
            integer(spec.maxRawBytes, 4096, 256 * MiB) &&
                integer(spec.parser?.maxOutputBytes, spec.maxRawBytes + 2 * MiB, 512 * MiB) &&
                integer(spec.parser?.timeoutMs, 100, 300000),
            "JVM_PARSER_BUDGET",
        );
        const launchFile = await member(root, "work/jvm-launch.json", 256 * 1024),
            launch = decode(launchFile.raw),
            captureFile = await member(root, "work/jvm-capture.json", 65536),
            capture = decode(captureFile.raw);
        same(launch.jdk, env.jvmObservation.jdk, "JVM_LAUNCH_JDK");
        same(launch.argv, expectedArgv(env, observation.javaArguments[0]), "JVM_PLANNED_ARGV");
        need(
            launch.planSha256 === env.jvmObservation.planSha256 &&
                capture.planSha256 === launch.planSha256 &&
                capture.launchSha256 === launchFile.sha256,
            "JVM_CAPTURE_BINDING",
        );
        const runtimeFile = await member(root, "work/jvm-runtime.json", 4 * MiB),
            runtime = decode(runtimeFile.raw);
        need(runtime.launchSha256 === launchFile.sha256, "JVM_RUNTIME_LAUNCH");
        const verifiedIdentity = runtimeIdentity(runtime, launch, env, envSha, context);
        const serverCapture = await json(root, "work/server-capture.json", 65536),
            service = await json(root, "work/service-result.json");
        need(
            serverCapture.kind === "hugegraph-http-pipe-capture" && serverCapture.stream === "stdout+stderr",
            "JVM_SERVER_CAPTURE_KIND",
        );
        for (const name of [
            "startedMonotonicNs",
            "finishedMonotonicNs",
            "readBytes",
            "retainedBytes",
            "discardedBytes",
            "eof",
            "capHit",
            "forcedKill",
            "error",
        ])
            same(capture[name], serverCapture[name], "JVM_SERVER_CAPTURE_DIFFER");
        same(
            capture.raw,
            { bytes: serverCapture.retainedBytes, sha256: serverCapture.sha256 },
            "JVM_SERVER_CAPTURE_DIFFER",
        );
        const server = service.outcomes?.find((o) => o.phase === "server");
        need(
            service.attemptId === env.attemptId &&
                service.planSha256 === envSha &&
                server?.termination &&
                capture.cleanupComplete === server.termination.cleanupComplete &&
                capture.forcedKill === server.termination.killSent,
            "JVM_TERMINATION_BINDING",
        );
        const raw = await member(root, "work/server.log", spec.maxRawBytes, false);
        same(capture.raw, { bytes: raw.bytes, sha256: raw.sha256 }, "JVM_RAW_CAPTURE_DIGEST");
        need(
            uint(capture.startedMonotonicNs) <= uint(runtime.observedBeforeMonotonicNs) &&
                uint(runtime.observedAfterMonotonicNs) <= uint(capture.finishedMonotonicNs),
            "JVM_RUNTIME_CAPTURE_WINDOW",
        );
        const inventory = await readdir(parsed, { withFileTypes: true });
        need(
            inventory.every((f) => f.isFile()),
            "JVM_PARSED_REGULAR_FILES",
        );
        same(inventory.map((f) => f.name).sort(), PARSED, "JVM_PARSED_INVENTORY");
        const manifest = await json(parsed, "manifest.json", MiB);
        need(
            manifest.schemaVersion === 1 &&
                manifest.kind === "http-jvm-observation-evidence" &&
                manifest.planSha256 === launch.planSha256 &&
                manifest.componentSha256 === trusted.sha256 &&
                manifest.performanceAccepted === false &&
                Array.isArray(manifest.files),
            "JVM_MANIFEST_IDENTITY",
        );
        same(
            manifest.files.map((f) => f.path).sort(),
            PARSED.filter((p) => p !== "manifest.json"),
            "JVM_MANIFEST_INVENTORY",
        );
        const observed = new Map();
        let total = 0;
        for (const name of PARSED) {
            const file = await member(parsed, name, spec.parser.maxOutputBytes, false);
            total += file.bytes;
            need(total <= spec.parser.maxOutputBytes, "JVM_PARSED_OUTPUT_BUDGET");
            if (name !== "manifest.json")
                same(
                    identity(file),
                    manifest.files.find((f) => f.path === name),
                    "JVM_MANIFEST_DIGEST",
                );
            observed.set(name, file);
        }
        // Keep preparation owners visible to a concurrently running parser's cancellation.
        // Signal listeners are installed only when a parser is actually about to spawn.
        owner = registerParserOwner();
        checkParserOwner(owner);
        temporary = await realpath(await mkdtemp(join(tmpdir(), "hgbench-jvm-reverify-")));
        await mkdir(join(temporary, "plan"), { mode: 0o700 });
        await writeFile(join(temporary, "parser.py"), trusted.raw, { mode: 0o600, flag: "wx" });
        for (const name of ["spec.json", "plan.json", "plan.lock.json"])
            await writeFile(join(temporary, "plan", name), frozen.get("jvm-observation/" + name).raw, {
                mode: 0o600,
                flag: "wx",
            });
        await writeFile(join(temporary, "launch.json"), launchFile.raw, { mode: 0o600, flag: "wx" });
        await writeFile(join(temporary, "capture.json"), captureFile.raw, { mode: 0o600, flag: "wx" });
        await copyFile(join(root, "work/server.log"), join(temporary, "server.log"), constants.COPYFILE_EXCL);
        const copied = await member(temporary, "server.log", spec.maxRawBytes, false);
        same({ bytes: copied.bytes, sha256: copied.sha256 }, capture.raw, "JVM_REPLAY_INPUT_CHANGED");
        await parseAgain(
            owner,
            process.env.HGBENCH_PYTHON || "python3",
            join(temporary, "parser.py"),
            [
                "--plan",
                join(temporary, "plan"),
                "--raw",
                join(temporary, "server.log"),
                "--launch",
                join(temporary, "launch.json"),
                "--capture",
                join(temporary, "capture.json"),
                "--out",
                join(temporary, "parsed"),
            ],
            spec.parser.timeoutMs + 5000,
        );
        for (const name of PARSED) {
            checkParserOwner(owner);
            const computed = await member(join(temporary, "parsed"), name, spec.parser.maxOutputBytes, false);
            same(identity(computed), identity(observed.get(name)), "JVM_RECOMPUTED_MEMBER_DIFFER");
        }
        const summary = await json(join(temporary, "parsed"), "summary.json", MiB);
        checkParserOwner(owner);
        need(
            summary.clockAlignment === "unverified" &&
                summary.phaseAttribution === "unavailable" &&
                summary.performanceAccepted === false &&
                summary.warmupStable === null &&
                summary.compilationSuccesses === null &&
                summary.compilationCpuNs === null,
            "JVM_ANALYSIS_BOUNDARY",
        );
        const pendingGates = ["jvm-to-host-clock", "phase-attribution", "observer-overhead-calibration"];
        for (const [name, state] of Object.entries(verifiedIdentity)) if (state === "unknown") pendingGates.push(name);
        const clockIdentity = verifyJvmClockIdentity(runtime, launch, context);
        return {
            schemaVersion: 1,
            kind: "http-jvm-evidence-verification",
            enabled: true,
            status: summary.status === "completed" && pendingGates.length === 3 ? "observed" : "partial",
            evidenceIntegrity: "passed",
            parserRecomputed: true,
            parserSha256: trusted.sha256,
            identity: verifiedIdentity,
            clockIdentity,
            captureComplete: summary.captureComplete,
            rawComplete: summary.rawComplete,
            observationStatus: summary.status,
            events: summary.events,
            gc: { pauseEnds: summary.counts["gc-pause-end"] ?? 0, reportedPauseNs: summary.gcPauseReportedNs },
            jit: {
                begin: summary.counts["jit-begin"] ?? 0,
                nativeWrappers: summary.counts["jit-native-wrapper"] ?? 0,
                notEntrant: summary.counts["jit-not-entrant"] ?? 0,
                zombie: summary.counts["jit-zombie"] ?? 0,
                repeatNoInstall: summary.counts["jit-repeat-no-install"] ?? 0,
                unknown: summary.counts["unknown-jit"] ?? 0,
                compilationSuccesses: null,
                compilationCpuNs: null,
            },
            issues: summary.issues,
            pendingGates,
            clockAlignment: "unverified",
            phaseAttribution: "unavailable",
            performanceAccepted: false,
        };
    } catch (error) {
        if (error.code === "ENOENT") return unavailable(true, "missing-jvm-evidence");
        throw error;
    } finally {
        try {
            if (temporary) await rm(temporary, { recursive: true, force: true });
            if (owner) checkParserOwner(owner);
        } finally {
            if (owner) await releaseParserOwner(owner);
        }
    }
}
