/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
import { isDeepStrictEqual } from "node:util";

const SHA = /^[a-f0-9]{64}$/;
const BOOT = /^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/;
const MAX_EXECUTABLE_BYTES = 256 * 1024 * 1024;
const MAX_HASH_NS = "2000000000";
const RUNTIME_GATES = ["hostBootId", "componentSha256", "pythonExecutable", "pythonVersion"];
const REMAINING_GATES = ["host-reference-clock-binding", "predeclared-clock-quality-thresholds"];
const CAPTURE_FIELDS = [
    "sourceOrigin",
    "availability",
    "pid",
    "startTimeTicks",
    "bootId",
    "statRaw",
    "timeNamespace",
    "timeForChildrenNamespace",
    "timeOffsetsRaw",
    "timeOffsets",
    "python",
    "clock",
    "executable",
    "unavailable",
    "captureDrift",
    "startedMonotonicNs",
    "finishedMonotonicNs",
];
const STABLE_FIELDS = [
    "sourceOrigin",
    "pid",
    "startTimeTicks",
    "bootId",
    "timeNamespace",
    "timeForChildrenNamespace",
    "timeOffsets",
    "python",
    "clock",
];

export class CollectorClockEvidenceError extends Error {
    constructor(code) {
        super(code);
        this.code = code;
    }
}
function need(value, code) {
    if (!value) throw new CollectorClockEvidenceError(code);
}
function same(actual, expected, code) {
    need(isDeepStrictEqual(actual, expected), code);
}
function object(value) {
    return value !== null && typeof value === "object" && !Array.isArray(value);
}
function keys(value, fields, code) {
    need(object(value), code);
    same(Object.keys(value).sort(), [...fields].sort(), code);
}
function uint(value) {
    need(typeof value === "string" && /^(0|[1-9][0-9]{0,24})$/.test(value), "COLLECTOR_CLOCK_INTEGER");
    return BigInt(value);
}
function pid(value) {
    return Number.isSafeInteger(value) && value >= 1 && value <= 4194304;
}
function text(value, cap) {
    return (
        typeof value === "string" && value.length > 0 && Buffer.byteLength(value) <= cap && !/[\x00-\x1f]/.test(value)
    );
}
function absolute(value) {
    return text(value, 4096) && value.startsWith("/");
}
function offsets(raw) {
    need(typeof raw === "string" && Buffer.byteLength(raw) <= 256, "COLLECTOR_CLOCK_OFFSETS_RAW");
    const rows = raw
        .trim()
        .split("\n")
        .map((line) => line.trim().split(/\s+/));
    need(
        rows.length === 2 &&
            rows[0][0] === "monotonic" &&
            rows[1][0] === "boottime" &&
            rows.every(
                (v) =>
                    v.length === 3 &&
                    /^-?[0-9]{1,12}$/.test(v[1]) &&
                    /^[0-9]{1,9}$/.test(v[2]) &&
                    BigInt(v[2]) < 1000000000n,
            ),
        "COLLECTOR_CLOCK_OFFSETS_RAW",
    );
    return Object.fromEntries(
        rows.map(([name, seconds, nanoseconds]) => [
            name,
            {
                seconds: BigInt(seconds).toString(),
                nanoseconds: BigInt(nanoseconds).toString(),
            },
        ]),
    );
}
function statIdentity(raw, capture) {
    need(
        typeof raw === "string" && Buffer.byteLength(raw) <= 4096 && raw.startsWith(`${capture.pid} (`),
        "COLLECTOR_CLOCK_RAW_STAT",
    );
    const end = raw.lastIndexOf(") "),
        fields = raw
            .slice(end + 2)
            .trim()
            .split(/\s+/);
    need(
        end >= 3 && fields.length >= 20 && /^[RSDTtWIPK]$/.test(fields[0]) && fields[19] === capture.startTimeTicks,
        "COLLECTOR_CLOCK_RAW_STAT",
    );
    uint(capture.startTimeTicks);
}
function clockInfo(value) {
    keys(
        value,
        ["api", "implementation", "monotonic", "adjustable", "resolutionSeconds"],
        "COLLECTOR_CLOCK_INFO_FIELDS",
    );
    need(
        value.api === "time.monotonic_ns" &&
            text(value.implementation, 256) &&
            typeof value.monotonic === "boolean" &&
            typeof value.adjustable === "boolean" &&
            Number.isFinite(value.resolutionSeconds) &&
            value.resolutionSeconds > 0,
        "COLLECTOR_CLOCK_INFO",
    );
}
function pythonInfo(value) {
    keys(value, ["reportedExecutable", "version", "implementation"], "COLLECTOR_CLOCK_PYTHON_FIELDS");
    need(
        absolute(value.reportedExecutable) &&
            typeof value.version === "string" &&
            value.version.length > 0 &&
            Buffer.byteLength(value.version) <= 4096 &&
            !value.version.includes("\0") &&
            typeof value.implementation === "string" &&
            /^[a-z][a-z0-9_]{0,63}$/.test(value.implementation),
        "COLLECTOR_CLOCK_PYTHON",
    );
}
function executable(value, digestRead, missing) {
    keys(value, ["sourcePath", "metadata", "sha256", "digestRead"], "COLLECTOR_CLOCK_EXECUTABLE_FIELDS");
    need(value.sourcePath === "/proc/self/exe" && value.digestRead === digestRead, "COLLECTOR_CLOCK_EXECUTABLE_SOURCE");
    if (value.metadata !== null) {
        keys(value.metadata, ["bytes", "device", "inode", "mtimeNs", "ctimeNs"], "COLLECTOR_CLOCK_EXECUTABLE_METADATA");
        for (const item of Object.values(value.metadata)) uint(item);
        if (!missing)
            need(
                uint(value.metadata.bytes) > 0n && uint(value.metadata.bytes) <= BigInt(MAX_EXECUTABLE_BYTES),
                "COLLECTOR_CLOCK_EXECUTABLE_BUDGET",
            );
    } else need(missing, "COLLECTOR_CLOCK_EXECUTABLE_MISSING");
    if (digestRead && !missing) need(SHA.test(value.sha256), "COLLECTOR_CLOCK_EXECUTABLE_DIGEST");
    else need(value.sha256 === null || (digestRead && SHA.test(value.sha256)), "COLLECTOR_CLOCK_EXECUTABLE_DIGEST");
}
function capture(value, origin, digestRead) {
    keys(value, CAPTURE_FIELDS, "COLLECTOR_CLOCK_CAPTURE_FIELDS");
    need(
        value.sourceOrigin === origin && ["observed", "unknown"].includes(value.availability),
        "COLLECTOR_CLOCK_CAPTURE_SOURCE",
    );
    need(uint(value.startedMonotonicNs) <= uint(value.finishedMonotonicNs), "COLLECTOR_CLOCK_CAPTURE_WINDOW");
    need(object(value.unavailable) && Object.keys(value.unavailable).length <= 9, "COLLECTOR_CLOCK_UNAVAILABLE");
    for (const [key, reason] of Object.entries(value.unavailable))
        need(
            [
                "source",
                "stat",
                "statAfter",
                "bootId",
                "timeNamespace",
                "timeForChildrenNamespace",
                "timeOffsets",
                "clock",
                "executable",
            ].includes(key) && text(reason, 128),
            "COLLECTOR_CLOCK_UNAVAILABLE",
        );
    need(
        Array.isArray(value.captureDrift) &&
            value.captureDrift.length <= 2 &&
            new Set(value.captureDrift).size === value.captureDrift.length &&
            value.captureDrift.every((key) => ["process", "executable"].includes(key)),
        "COLLECTOR_CLOCK_CAPTURE_DRIFT",
    );
    const unavailable = value.unavailable;
    if (origin === "fixture" || unavailable.source) {
        same(
            unavailable,
            { source: origin === "fixture" ? "fixture-has-no-production-clock-identity" : "platform-not-linux" },
            "COLLECTOR_CLOCK_NONPRODUCTION_SOURCE",
        );
        for (const key of [
            "pid",
            "startTimeTicks",
            "bootId",
            "statRaw",
            "timeNamespace",
            "timeForChildrenNamespace",
            "timeOffsetsRaw",
            "timeOffsets",
            "python",
            "clock",
            "executable",
        ])
            need(value[key] === null, "COLLECTOR_CLOCK_NONPRODUCTION_IDENTITY");
        same(value.captureDrift, [], "COLLECTOR_CLOCK_NONPRODUCTION_IDENTITY");
        need(value.availability === "unknown", "COLLECTOR_CLOCK_NONPRODUCTION_IDENTITY");
        return;
    }
    need(pid(value.pid), "COLLECTOR_CLOCK_PID");
    if (value.statRaw === null || value.startTimeTicks === null) {
        need(
            value.statRaw === null && value.startTimeTicks === null && unavailable.stat,
            "COLLECTOR_CLOCK_STAT_MISSING",
        );
    } else {
        need(!unavailable.stat, "COLLECTOR_CLOCK_STAT_MISSING");
        statIdentity(value.statRaw, value);
    }
    if (unavailable.statAfter && value.startTimeTicks !== null)
        need(value.captureDrift.includes("process"), "COLLECTOR_CLOCK_CAPTURE_DRIFT");
    for (const key of ["bootId", "timeNamespace", "timeForChildrenNamespace"]) {
        if (value[key] === null) need(unavailable[key], "COLLECTOR_CLOCK_IDENTITY_MISSING");
        else {
            need(
                !unavailable[key] &&
                    typeof value[key] === "string" &&
                    (key === "bootId" ? BOOT : /^time:\[[0-9]{1,20}\]$/).test(value[key]),
                "COLLECTOR_CLOCK_HOST_IDENTITY",
            );
        }
    }
    if (value.timeOffsetsRaw === null || value.timeOffsets === null)
        need(
            value.timeOffsetsRaw === null && value.timeOffsets === null && unavailable.timeOffsets,
            "COLLECTOR_CLOCK_OFFSETS_MISSING",
        );
    else {
        need(!unavailable.timeOffsets, "COLLECTOR_CLOCK_OFFSETS_MISSING");
        same(value.timeOffsets, offsets(value.timeOffsetsRaw), "COLLECTOR_CLOCK_OFFSETS_REBUILD");
    }
    pythonInfo(value.python);
    if (value.clock === null) need(unavailable.clock, "COLLECTOR_CLOCK_INFO_MISSING");
    else {
        need(!unavailable.clock, "COLLECTOR_CLOCK_INFO_MISSING");
        clockInfo(value.clock);
    }
    executable(value.executable, digestRead, Boolean(unavailable.executable));
    same(
        value.captureDrift.includes("executable"),
        unavailable.executable === "clock-executable-changed-during-read",
        "COLLECTOR_CLOCK_CAPTURE_DRIFT",
    );
    same(
        value.availability,
        Object.keys(unavailable).length || value.captureDrift.length ? "unknown" : "observed",
        "COLLECTOR_CLOCK_CAPTURE_AVAILABILITY",
    );
}
function changes(before, after) {
    const result = new Set([...before.captureDrift, ...after.captureDrift]);
    for (const field of STABLE_FIELDS)
        if (before[field] !== null && !isDeepStrictEqual(before[field], after[field])) result.add(field);
    const old = before.executable,
        current = after.executable;
    if (
        old &&
        old.metadata !== null &&
        (!current || old.sourcePath !== current.sourcePath || !isDeepStrictEqual(old.metadata, current.metadata))
    )
        result.add("executable.metadata");
    return [...result].sort();
}
function binding(actual, expected, code) {
    if (expected === undefined) return "unverified";
    if (actual === null || actual === undefined) return "unavailable";
    same(actual, expected, code);
    return "matched";
}

// Pure raw validation: callers still verify enclosing file digests and sampled records independently.
// Matching externally supplied runtime values never establishes a shared host clock or its quality.
export function validateCollectorClockIdentity(summary, options = {}) {
    need(object(summary) && object(options), "COLLECTOR_CLOCK_INPUT");
    if (!Object.hasOwn(summary, "clockIdentity"))
        return {
            status: "unavailable",
            rawValidated: false,
            sourceOrigin: summary.sourceOrigin ?? null,
            reason: "legacy-no-collector-clock-identity",
            verified: false,
            hostAssociation: "unverified",
            performanceAccepted: false,
            runtimeSource: { status: "unavailable", bindings: {}, pendingGates: [...RUNTIME_GATES] },
            pendingGates: [...RUNTIME_GATES, ...REMAINING_GATES],
            identity: null,
            window: null,
            changedFields: [],
        };
    const value = summary.clockIdentity;
    keys(
        value,
        [
            "schemaVersion",
            "kind",
            "sourceOrigin",
            "availability",
            "verified",
            "hostAssociation",
            "before",
            "after",
            "changedFields",
            "executableHashMaxBytes",
            "executableHashMaxNs",
        ],
        "COLLECTOR_CLOCK_FIELDS",
    );
    need(
        value.schemaVersion === 1 &&
            value.kind === "http-collector-clock-identity" &&
            ["fixture", "linux-procfs"].includes(value.sourceOrigin) &&
            value.sourceOrigin === summary.sourceOrigin &&
            value.verified === false &&
            value.hostAssociation === "unverified" &&
            value.executableHashMaxBytes === MAX_EXECUTABLE_BYTES &&
            value.executableHashMaxNs === MAX_HASH_NS,
        "COLLECTOR_CLOCK_KIND",
    );
    need(
        summary.schemaVersion === 1 &&
            summary.kind === "http-resource-summary" &&
            pid(summary.collectorPid) &&
            SHA.test(summary.componentSha256) &&
            absolute(summary.pythonExecutable) &&
            text(summary.pythonVersion, 64) &&
            ["completed", "partial"].includes(summary.status),
        "COLLECTOR_CLOCK_SUMMARY",
    );
    keys(summary.clock, ["implementation", "resolutionSeconds", "adjustable"], "COLLECTOR_CLOCK_SUMMARY_CLOCK");
    need(
        text(summary.clock.implementation, 256) &&
            Number.isFinite(summary.clock.resolutionSeconds) &&
            summary.clock.resolutionSeconds > 0 &&
            typeof summary.clock.adjustable === "boolean",
        "COLLECTOR_CLOCK_SUMMARY_CLOCK",
    );
    capture(value.before, value.sourceOrigin, true);
    capture(value.after, value.sourceOrigin, false);
    const before = value.before,
        after = value.after,
        changed = changes(before, after);
    same(value.changedFields, changed, "COLLECTOR_CLOCK_CHANGED_FIELDS_REBUILD");
    const observed = before.availability === "observed" && after.availability === "observed" && changed.length === 0;
    same(value.availability, observed ? "observed" : "unknown", "COLLECTOR_CLOCK_AVAILABILITY_REBUILD");
    if (changed.length)
        need(summary.status === "partial" && summary.identityContinuous === false, "COLLECTOR_CLOCK_DRIFT_TERMINAL");
    if (before.pid !== null) same(before.pid, summary.collectorPid, "COLLECTOR_CLOCK_SUMMARY_PID");
    if (before.bootId !== null) same(before.bootId, summary.host?.bootId, "COLLECTOR_CLOCK_SUMMARY_BOOT");
    if (before.python !== null) {
        same(before.python.reportedExecutable, summary.pythonExecutable, "COLLECTOR_CLOCK_SUMMARY_PYTHON");
        same(before.python.version.split(/\s+/)[0], summary.pythonVersion, "COLLECTOR_CLOCK_SUMMARY_PYTHON");
    }
    if (before.clock !== null) {
        const { api, monotonic, ...common } = before.clock;
        same(common, summary.clock, "COLLECTOR_CLOCK_SUMMARY_CLOCK");
    }
    const start = uint(summary.startedMonotonicNs),
        finish = uint(summary.finishedMonotonicNs);
    need(
        uint(before.finishedMonotonicNs) <= start &&
            start <= uint(after.startedMonotonicNs) &&
            uint(after.finishedMonotonicNs) <= finish,
        "COLLECTOR_CLOCK_COLLECTION_WINDOW",
    );
    need(
        Number.isSafeInteger(summary.samples) && summary.samples >= 0 && summary.samples <= 100000,
        "COLLECTOR_CLOCK_SAMPLE_COUNT",
    );
    let first = null,
        last = null;
    if (summary.samples) {
        const hasFirst = Object.hasOwn(summary, "firstSampleStartedMonotonicNs"),
            hasLast = Object.hasOwn(summary, "lastSampleFinishedMonotonicNs");
        need(hasFirst === hasLast, "COLLECTOR_CLOCK_SAMPLE_WINDOW");
        if (options.sampleWindow !== undefined) {
            need(
                Array.isArray(options.sampleWindow) && options.sampleWindow.length === 2,
                "COLLECTOR_CLOCK_SAMPLE_WINDOW",
            );
            options.sampleWindow.forEach(uint);
            if (hasFirst)
                same(
                    options.sampleWindow,
                    [summary.firstSampleStartedMonotonicNs, summary.lastSampleFinishedMonotonicNs],
                    "COLLECTOR_CLOCK_SAMPLE_WINDOW_BINDING",
                );
        }
        need(hasFirst || options.sampleWindow !== undefined, "COLLECTOR_CLOCK_SAMPLE_WINDOW");
        [first, last] = hasFirst
            ? [summary.firstSampleStartedMonotonicNs, summary.lastSampleFinishedMonotonicNs]
            : options.sampleWindow;
        need(
            start <= uint(first) && uint(first) <= uint(last) && uint(last) <= uint(after.startedMonotonicNs),
            "COLLECTOR_CLOCK_SAMPLE_WINDOW",
        );
    } else
        need(
            !Object.hasOwn(summary, "firstSampleStartedMonotonicNs") &&
                !Object.hasOwn(summary, "lastSampleFinishedMonotonicNs"),
            "COLLECTOR_CLOCK_SAMPLE_WINDOW",
        );
    need(Buffer.byteLength(JSON.stringify(value)) <= 65536, "COLLECTOR_CLOCK_BYTE_BUDGET");
    const expected = options.expected ?? {};
    need(object(expected), "COLLECTOR_CLOCK_EXPECTATIONS");
    for (const supplied of [options.componentSha256, expected.hostPythonExecutableSha256])
        if (supplied !== undefined)
            need(typeof supplied === "string" && SHA.test(supplied), "COLLECTOR_CLOCK_EXPECTATIONS");
    if (options.hostBootId !== undefined)
        need(typeof options.hostBootId === "string" && BOOT.test(options.hostBootId), "COLLECTOR_CLOCK_EXPECTATIONS");
    if (expected.hostPythonVersion !== undefined)
        need(text(expected.hostPythonVersion, 64), "COLLECTOR_CLOCK_EXPECTATIONS");
    const bindings = {
        hostBootId: binding(before.bootId, options.hostBootId, "COLLECTOR_CLOCK_BOOT_BINDING"),
        componentSha256: binding(summary.componentSha256, options.componentSha256, "COLLECTOR_CLOCK_COMPONENT_BINDING"),
        pythonExecutable: binding(
            before.executable?.sha256,
            expected.hostPythonExecutableSha256,
            "COLLECTOR_CLOCK_PYTHON_EXECUTABLE_BINDING",
        ),
        pythonVersion: binding(
            before.python === null ? null : summary.pythonVersion,
            expected.hostPythonVersion,
            "COLLECTOR_CLOCK_PYTHON_VERSION_BINDING",
        ),
    };
    const pending = RUNTIME_GATES.filter((key) => bindings[key] !== "matched");
    if (!observed) pending.push("collector-clock-observations-unavailable");
    const runtimeStatus =
        value.sourceOrigin === "fixture" || !observed ? "unavailable" : pending.length ? "unverified" : "matched";
    return {
        status: observed ? "raw-consistent" : "unknown",
        rawValidated: true,
        sourceOrigin: value.sourceOrigin,
        verified: false,
        hostAssociation: "unverified",
        performanceAccepted: false,
        runtimeSource: { status: runtimeStatus, bindings, pendingGates: pending },
        pendingGates: [...pending, ...REMAINING_GATES],
        changedFields: changed,
        identity:
            before.pid === null
                ? null
                : {
                      pid: before.pid,
                      startTimeTicks: before.startTimeTicks,
                      bootId: before.bootId,
                      timeNamespace: before.timeNamespace,
                      timeForChildrenNamespace: before.timeForChildrenNamespace,
                      timeOffsets: structuredClone(before.timeOffsets),
                      python: structuredClone(before.python),
                      clock: structuredClone(before.clock),
                      executableSha256: before.executable?.sha256 ?? null,
                  },
        window: {
            beforeStartNs: before.startedMonotonicNs,
            beforeFinishNs: before.finishedMonotonicNs,
            collectionStartNs: summary.startedMonotonicNs,
            collectionFinishNs: summary.finishedMonotonicNs,
            firstSampleStartNs: first,
            lastSampleFinishNs: last,
            afterStartNs: after.startedMonotonicNs,
            afterFinishNs: after.finishedMonotonicNs,
        },
    };
}
