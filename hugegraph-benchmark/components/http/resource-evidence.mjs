/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import { constants, createReadStream } from "node:fs";
import { open, lstat, readdir } from "node:fs/promises";
import { createHash } from "node:crypto";
import { createInterface } from "node:readline";
import { join, dirname } from "node:path";
import { isDeepStrictEqual } from "node:util";
import { validateCollectorClockIdentity, CollectorClockEvidenceError } from "./collector-clock-evidence.mjs";

const MiB = 1024 * 1024,
    SHA = /^[a-f0-9]{64}$/;
const METRICS = [
    "cpu.stat",
    "cpu.max",
    "memory.current",
    "memory.peak",
    "memory.max",
    "memory.swap.current",
    "memory.swap.max",
    "memory.stat",
    "memory.events",
    "pids.current",
    "pids.max",
    "pids.events",
    "io.stat",
    "cpu.pressure",
    "memory.pressure",
    "io.pressure",
    "cgroup.events",
];
export class ResourceEvidenceError extends Error {
    constructor(code) {
        super(code);
        this.code = code;
    }
}
function need(value, code) {
    if (!value) throw new ResourceEvidenceError(code);
}
function same(a, b, code) {
    need(isDeepStrictEqual(a, b), code);
}
function integer(value, low, high) {
    return Number.isSafeInteger(value) && value >= low && value <= high;
}
function uint(value) {
    need(typeof value === "string" && /^(0|[1-9][0-9]{0,24})$/.test(value), "RESOURCE_INTEGER_PRECISION");
    return BigInt(value);
}
function counter(value) {
    const result = uint(value);
    need(result <= 18446744073709551615n, "RESOURCE_COUNTER_RANGE");
    return result;
}
function absolute(value) {
    return typeof value === "string" && value.startsWith("/") && value.length <= 2048 && !/[\x00-\x1f]/.test(value);
}
const hash = (raw) => createHash("sha256").update(raw).digest("hex");
async function bytes(path, cap = 8 * MiB) {
    const handle = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
    try {
        const before = await handle.stat();
        need(before.isFile() && before.size <= cap, "RESOURCE_FILE_BOUND");
        const raw = Buffer.alloc(before.size + 1);
        let count = 0;
        while (count < raw.length) {
            const r = await handle.read(raw, count, raw.length - count, count);
            if (!r.bytesRead) break;
            count += r.bytesRead;
        }
        const after = await handle.stat();
        need(
            count === before.size && after.size === before.size && after.mtimeMs === before.mtimeMs,
            "RESOURCE_FILE_CHANGED",
        );
        return raw.subarray(0, count);
    } finally {
        await handle.close();
    }
}
async function json(path, cap) {
    return JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(await bytes(path, cap)));
}
function keys(value, names, code) {
    same(Object.keys(value || {}).sort(), [...names].sort(), code);
}
function validateSpec(spec) {
    keys(
        spec,
        [
            "schemaVersion",
            "kind",
            "intervalMs",
            "durationMs",
            "maxSamples",
            "maxOutputBytes",
            "filesystemPath",
            "roles",
            "controlledStop",
        ],
        "RESOURCE_SPEC_FIELDS",
    );
    need(spec.schemaVersion === 1 && spec.kind === "http-resource-collection", "RESOURCE_SPEC_IDENTITY");
    for (const [field, low, high] of [
        ["intervalMs", 100, 60000],
        ["durationMs", 100, 86400000],
        ["maxSamples", 1, 100000],
        ["maxOutputBytes", 131072, 64 * MiB],
    ])
        need(integer(spec[field], low, high), "RESOURCE_SPEC_BUDGET");
    need(spec.intervalMs <= spec.durationMs && absolute(spec.filesystemPath), "RESOURCE_SPEC_BUDGET");
    keys(spec.controlledStop, ["path", "token"], "RESOURCE_STOP_SPEC");
    need(absolute(spec.controlledStop.path) && /^[a-f0-9]{32}$/.test(spec.controlledStop.token), "RESOURCE_STOP_SPEC");
    need(
        Array.isArray(spec.roles) &&
            spec.roles.length >= 1 &&
            spec.roles.length <= 8 &&
            new Set(spec.roles.map((r) => r.role)).size === spec.roles.length,
        "RESOURCE_ROLES",
    );
    for (const role of spec.roles) {
        keys(
            role,
            role.hostPid === undefined ? ["role", "anchorPid", "cgroupPath"] : ["role", "hostPid"],
            "RESOURCE_ROLE_FIELDS",
        );
        need(
            /^[a-z][a-z0-9_-]{0,31}$/.test(role.role) &&
                integer(role.hostPid ?? role.anchorPid, 1, 4194304) &&
                (role.hostPid !== undefined || absolute(role.cgroupPath)),
            "RESOURCE_ROLE_IDENTITY",
        );
    }
}
function counterMap(value) {
    if (value !== null) {
        need(value && typeof value === "object" && !Array.isArray(value), "RESOURCE_COUNTER_MAP");
        for (const v of Object.values(value)) counter(v);
    }
}
function delta(before, after) {
    if (before === null || after === null) return null;
    counterMap(before);
    counterMap(after);
    const out = {};
    for (const key of [...new Set([...Object.keys(before), ...Object.keys(after)])].sort()) {
        if (!Object.hasOwn(before, key) || !Object.hasOwn(after, key)) out[key] = null;
        else {
            const d = uint(after[key]) - uint(before[key]);
            need(d >= 0n, "RESOURCE_COUNTER_RESET");
            out[key] = String(d);
        }
    }
    return out;
}
function pressure(value) {
    if (value === null) return null;
    need(
        value &&
            typeof value === "object" &&
            Object.keys(value).length > 0 &&
            Object.keys(value).every((k) => ["some", "full"].includes(k)),
        "RESOURCE_PRESSURE",
    );
    const totals = {};
    for (const [name, values] of Object.entries(value)) {
        keys(values, ["avg10", "avg60", "avg300", "total"], "RESOURCE_PRESSURE");
        for (const key of ["avg10", "avg60", "avg300"])
            need(Number.isFinite(values[key]) && values[key] >= 0 && values[key] <= 100, "RESOURCE_PRESSURE");
        counter(values.total);
        totals[name] = values.total;
    }
    return totals;
}
function validateValues(role) {
    keys(role.values, METRICS, "RESOURCE_METRIC_INVENTORY");
    need(role.unavailable && typeof role.unavailable === "object", "RESOURCE_MISSING_REASONS");
    same(
        Object.keys(role.unavailable).sort(),
        METRICS.filter((k) => role.values[k] === null).sort(),
        "RESOURCE_MISSING_REASONS",
    );
    need(
        Object.values(role.unavailable).every((v) => typeof v === "string" && v.length > 0 && v.length <= 128),
        "RESOURCE_MISSING_REASONS",
    );
    for (const key of METRICS) {
        const value = role.values[key];
        if (value === null) continue;
        if (key.endsWith(".pressure")) pressure(value);
        else if (key === "cpu.max") {
            keys(value, ["quotaMicros", "periodMicros"], "RESOURCE_CPU_LIMIT");
            need(counter(value.periodMicros) > 0n, "RESOURCE_CPU_LIMIT");
            if (value.quotaMicros !== "max") need(counter(value.quotaMicros) > 0n, "RESOURCE_CPU_LIMIT");
        } else if (key === "io.stat") {
            need(typeof value === "object" && !Array.isArray(value), "RESOURCE_IO_SHAPE");
            for (const [device, counters] of Object.entries(value)) {
                need(/^[0-9]+:[0-9]+$/.test(device) && counters !== null, "RESOURCE_IO_DEVICE");
                counterMap(counters);
            }
        } else if (["cpu.stat", "memory.stat", "memory.events", "pids.events", "cgroup.events"].includes(key)) {
            counterMap(value);
            need(Object.keys(value).length > 0, "RESOURCE_EMPTY_COUNTERS");
        } else if (value !== "max" || !["memory.max", "memory.swap.max", "pids.max"].includes(key)) counter(value);
    }
    for (const [family, required] of [
        ["cpu.stat", ["usage_usec"]],
        ["memory.events", ["oom", "oom_kill"]],
        ["pids.events", ["max"]],
        ["cgroup.events", ["populated"]],
    ])
        if (role.values[family] !== null)
            need(
                required.every((key) => Object.hasOwn(role.values[family], key)),
                "RESOURCE_REQUIRED_COUNTER_MISSING",
            );
}
function roleInterval(before, after) {
    const elapsed = uint(after.finishedMonotonicNs) - uint(before.finishedMonotonicNs);
    need(elapsed > 0n, "RESOURCE_SAMPLE_INTERVAL");
    const a = before.values,
        b = after.values,
        counters = {};
    for (const key of ["cpu.max", "memory.max", "memory.swap.max", "pids.max"])
        if (a[key] !== null && b[key] !== null) same(a[key], b[key], "RESOURCE_CONTROLS_CHANGED");
    for (const key of ["cpu.stat", "memory.events", "pids.events"]) counters[key] = delta(a[key], b[key]);
    for (const key of ["cpu.pressure", "memory.pressure", "io.pressure"])
        counters[key] = delta(pressure(a[key]), pressure(b[key]));
    counters["io.stat"] = null;
    if (a["io.stat"] !== null && b["io.stat"] !== null) {
        counters["io.stat"] = {};
        for (const device of [...new Set([...Object.keys(a["io.stat"]), ...Object.keys(b["io.stat"])])].sort())
            counters["io.stat"][device] = delta(a["io.stat"][device] ?? null, b["io.stat"][device] ?? null);
    }
    const usage = counters["cpu.stat"]?.usage_usec;
    return {
        elapsedNs: String(elapsed),
        counters,
        averageCpuCores: usage == null ? null : (Number(uint(usage)) * 1000) / Number(elapsed),
    };
}
function resourceSummary(result, role) {
    if (!Object.hasOwn(result, role.role))
        result[role.role] = {
            samples: 0,
            maximumObservedMemoryCurrentBytes: null,
            maximumObservedPids: null,
            cumulativeMemoryPeakBytesAtLastSample: null,
            cpuUsageMicros: null,
            cpuCoveredIntervalNs: "0",
            cpuObservedIntervals: 0,
            averageCpuCoresOverObservedIntervals: null,
            oomKillsWithinObservedIntervals: null,
            pidFailuresWithinObservedIntervals: null,
        };
    const observed = result[role.role];
    observed.samples++;
    for (const [field, target] of [
        ["memory.current", "maximumObservedMemoryCurrentBytes"],
        ["pids.current", "maximumObservedPids"],
    ]) {
        const value = role.values[field];
        if (value !== null)
            observed[target] = String(
                uint(value) > uint(observed[target] ?? "0") ? uint(value) : uint(observed[target] ?? "0"),
            );
    }
    observed.cumulativeMemoryPeakBytesAtLastSample = role.values["memory.peak"];
    if (!role.interval) return;
    const c = role.interval.counters,
        usage = c["cpu.stat"]?.usage_usec;
    if (usage != null) {
        observed.cpuUsageMicros = String(uint(observed.cpuUsageMicros ?? "0") + uint(usage));
        observed.cpuCoveredIntervalNs = String(uint(observed.cpuCoveredIntervalNs) + uint(role.interval.elapsedNs));
        observed.cpuObservedIntervals++;
        observed.averageCpuCoresOverObservedIntervals =
            (Number(uint(observed.cpuUsageMicros)) * 1000) / Number(uint(observed.cpuCoveredIntervalNs));
    }
    for (const [family, field, target] of [
        ["memory.events", "oom_kill", "oomKillsWithinObservedIntervals"],
        ["pids.events", "max", "pidFailuresWithinObservedIntervals"],
    ]) {
        const value = c[family]?.[field];
        if (value != null) observed[target] = String(uint(observed[target] ?? "0") + uint(value));
    }
}
function sameNumeric(a, b, code) {
    // Python integer true division and JS double multiplication may differ by one rounding step.
    if (typeof a === "number" || typeof b === "number") {
        need(
            typeof a === "number" &&
                typeof b === "number" &&
                Number.isFinite(a) &&
                Number.isFinite(b) &&
                Math.abs(a - b) <= 4 * Number.EPSILON * Math.max(1, Math.abs(a), Math.abs(b)),
            code,
        );
    } else if (a && b && typeof a === "object" && typeof b === "object") {
        same(Object.keys(a).sort(), Object.keys(b).sort(), code);
        for (const key of Object.keys(a)) sameNumeric(a[key], b[key], code);
    } else same(a, b, code);
}
function validateHost(host) {
    keys(
        host,
        ["cpuTicks", "memoryBytes", "loadAverage", "disks", "pressure", "filesystem", "unavailable"],
        "RESOURCE_HOST_INVENTORY",
    );
    const missing = [];
    for (const key of ["cpuTicks", "memoryBytes", "loadAverage", "disks"]) if (host[key] === null) missing.push(key);
    counterMap(host.cpuTicks);
    if (host.cpuTicks !== null)
        keys(
            host.cpuTicks,
            ["user", "nice", "system", "idle", "iowait", "irq", "softirq", "steal"],
            "RESOURCE_HOST_CPU_FIELDS",
        );
    if (host.memoryBytes !== null) {
        keys(
            host.memoryBytes,
            ["MemTotal", "MemAvailable", "MemFree", "Buffers", "Cached", "SwapTotal", "SwapFree"],
            "RESOURCE_HOST_MEMORY_FIELDS",
        );
        for (const [key, value] of Object.entries(host.memoryBytes)) {
            if (value === null) missing.push(`memory.${key}`);
            else uint(value);
        }
    }
    if (host.loadAverage !== null)
        need(
            Array.isArray(host.loadAverage) &&
                host.loadAverage.length === 3 &&
                host.loadAverage.every((v) => Number.isFinite(v) && v >= 0),
            "RESOURCE_HOST_LOAD",
        );
    if (host.disks !== null) {
        need(typeof host.disks === "object" && !Array.isArray(host.disks), "RESOURCE_HOST_DISK");
        for (const [device, counters] of Object.entries(host.disks)) {
            need(/^[0-9]+:[0-9]+$/.test(device) && typeof counters.name === "string", "RESOURCE_HOST_DISK");
            keys(
                counters,
                [
                    "name",
                    "readOperations",
                    "readSectors512",
                    "readMillis",
                    "writeOperations",
                    "writeSectors512",
                    "writeMillis",
                    "inflight",
                    "ioMillis",
                    "weightedIoMillis",
                ],
                "RESOURCE_HOST_DISK",
            );
            const { name, ...values } = counters;
            counterMap(values);
        }
    }
    keys(host.pressure, ["cpu", "memory", "io"], "RESOURCE_HOST_PRESSURE");
    for (const [key, value] of Object.entries(host.pressure)) {
        if (value === null) missing.push(`${key}.pressure`);
        else pressure(value);
    }
    same(Object.keys(host.unavailable).sort(), missing.sort(), "RESOURCE_HOST_MISSING_REASONS");
}
function checkIdentity(identity, spec, bootId, expected) {
    keys(
        identity,
        ["pid", "startTimeTicks", "cgroupPath", "mountId", "bootId", "device", "inode", "cgroupType"],
        "RESOURCE_PROCESS_IDENTITY",
    );
    need(
        identity.pid === (spec.hostPid ?? spec.anchorPid) &&
            identity.bootId === bootId &&
            absolute(identity.cgroupPath) &&
            ["domain", "domain threaded", "domain invalid", "threaded"].includes(identity.cgroupType),
        "RESOURCE_PROCESS_IDENTITY",
    );
    for (const field of ["startTimeTicks", "mountId", "device", "inode"]) counter(identity[field]);
    if (spec.cgroupPath) same(identity.cgroupPath, spec.cgroupPath, "RESOURCE_CGROUP_PATH");
    if (expected) {
        need(
            identity.pid === expected.hostPid &&
                identity.startTimeTicks === expected.startTimeTicks &&
                identity.cgroupPath.endsWith(expected.cgroupMember),
            "RESOURCE_PROCESS_BINDING",
        );
    }
}
function checkLimits(values, expected) {
    if (!expected) return;
    if (values["cpu.max"] !== null) {
        const v = values["cpu.max"];
        need(
            v.quotaMicros !== "max" &&
                (() => {
                    const quota = uint(v.quotaMicros);
                    const derived = (BigInt(Math.round(expected.cpus * 1e9)) * uint(v.periodMicros)) / 1000000000n;
                    return quota >= derived - 1n && quota <= derived + 1n;
                })(),
            "RESOURCE_CPU_LIMIT_DIFFER",
        );
    }
    for (const [field, value] of [
        ["memory.max", String(expected.memoryBytes)],
        ["memory.swap.max", "0"],
        ["pids.max", String(expected.pids)],
    ])
        if (values[field] !== null) same(values[field], value, "RESOURCE_LIMIT_DIFFER");
}

/** Rebuild collector accounting from original samples; no live /proc/Docker access. */
export async function verifyResourceEvidence(directory, options) {
    need((await lstat(directory)).isDirectory(), "RESOURCE_DIRECTORY");
    same(
        (await readdir(directory)).sort(),
        ["manifest.json", "samples.ndjson", "spec.json", "summary.json"],
        "RESOURCE_INVENTORY",
    );
    const manifest = await json(join(directory, "manifest.json")),
        specRaw = await bytes(join(directory, "spec.json"));
    const spec = JSON.parse(specRaw),
        summaryRaw = await bytes(join(directory, "summary.json")),
        summary = JSON.parse(summaryRaw);
    validateSpec(spec);
    need(
        manifest.schemaVersion === 1 &&
            manifest.kind === "http-resource-evidence" &&
            manifest.sealed === true &&
            manifest.collectionStatus === "completed" &&
            Array.isArray(manifest.files),
        "RESOURCE_MANIFEST",
    );
    same(
        manifest.files.map((f) => f.path).sort(),
        ["samples.ndjson", "spec.json", "summary.json"],
        "RESOURCE_MANIFEST",
    );
    const entries = Object.fromEntries(manifest.files.map((f) => [f.path, f]));
    let totalBytes = (await bytes(join(directory, "manifest.json"))).length;
    for (const [path, raw] of [
        ["spec.json", specRaw],
        ["summary.json", summaryRaw],
    ]) {
        need(entries[path].bytes === raw.length && entries[path].sha256 === hash(raw), "RESOURCE_MANIFEST_DIGEST");
        totalBytes += raw.length;
    }
    const sampleEntry = entries["samples.ndjson"];
    need(
        integer(sampleEntry.bytes, 1, spec.maxOutputBytes) &&
            SHA.test(sampleEntry.sha256) &&
            totalBytes + sampleEntry.bytes <= spec.maxOutputBytes,
        "RESOURCE_OUTPUT_BUDGET",
    );
    const samplePath = join(directory, "samples.ndjson"),
        info = await lstat(samplePath);
    need(info.isFile() && info.size === sampleEntry.bytes, "RESOURCE_SAMPLE_FILE");
    need(
        summary.schemaVersion === 1 &&
            summary.kind === "http-resource-summary" &&
            summary.status === "completed" &&
            summary.reason === "controlled-stop" &&
            summary.sealed === true &&
            summary.identityContinuous === true &&
            summary.sampleFileComplete === true &&
            summary.rejectedObservationPreserved === false &&
            summary.specSha256 === hash(specRaw),
        "RESOURCE_TERMINAL",
    );
    same(summary.sampleFile, sampleEntry, "RESOURCE_SUMMARY_SAMPLE_FILE");
    need(
        summary.host?.bootId === options.hostBootId &&
            typeof summary.host.pidNamespace === "string" &&
            ["fixture", "linux-procfs"].includes(summary.sourceOrigin),
        "RESOURCE_HOST_IDENTITY",
    );
    if (options.componentSha256) same(summary.componentSha256, options.componentSha256, "RESOURCE_COMPONENT_IDENTITY");
    if (options.expectedSpec)
        for (const [key, value] of Object.entries(options.expectedSpec)) same(spec[key], value, "RESOURCE_SPEC_DIFFER");
    const start = uint(summary.startedMonotonicNs),
        finish = uint(summary.finishedMonotonicNs);
    const deadline = start + BigInt(spec.durationMs) * 1000000n,
        interval = BigInt(spec.intervalMs) * 1000000n;
    need(
        finish >= start && summary.overheadCalibrated === false && summary.performanceAccepted === false,
        "RESOURCE_TIME_RANGE",
    );
    need(Array.isArray(summary.identities) && summary.identities.length === spec.roles.length, "RESOURCE_IDENTITIES");
    const identities = summary.identities.map(({ role, ...identity }, index) => {
        same(role, spec.roles[index].role, "RESOURCE_ROLE_ORDER");
        checkIdentity(identity, spec.roles[index], options.hostBootId, options.expectedRoles?.[role]);
        return identity;
    });
    const missing = {},
        observed = {};
    let previous,
        first,
        count = 0,
        maximumCapture = 0n,
        maximumLateness = 0n;
    let observedSkipped = 0n,
        lastSlot = -1n,
        sampleBytes = 0;
    const digest = createHash("sha256"),
        stream = createReadStream(samplePath, {
            flags: constants.O_RDONLY | constants.O_NOFOLLOW,
            end: sampleEntry.bytes,
        });
    let streamFailure;
    stream.on("error", (error) => {
        streamFailure ??= error;
    });
    stream.on("data", (chunk) => {
        sampleBytes += chunk.length;
        if (sampleBytes > sampleEntry.bytes) {
            streamFailure = new ResourceEvidenceError("RESOURCE_READ_BYTE_BUDGET");
            stream.destroy(streamFailure);
            return;
        }
        digest.update(chunk);
    });
    const lines = createInterface({ input: stream, crlfDelay: Infinity });
    try {
        for await (const line of lines) {
            if (streamFailure) throw streamFailure;
            need(sampleBytes <= sampleEntry.bytes && line.length > 0, "RESOURCE_SAMPLE_FILE");
            const row = JSON.parse(line);
            need(
                row.schemaVersion === 1 &&
                    row.kind === "http-resource-sample" &&
                    row.index === count &&
                    count < spec.maxSamples,
                "RESOURCE_SAMPLE_SEQUENCE",
            );
            const scheduled = uint(row.scheduledMonotonicNs),
                begun = uint(row.startedMonotonicNs),
                ended = uint(row.finishedMonotonicNs);
            need(scheduled >= start && (scheduled - start) % interval === 0n, "RESOURCE_SCHEDULE");
            const slot = (scheduled - start) / interval;
            need(
                slot > lastSlot &&
                    (count > 0 || slot === 0n) &&
                    scheduled <= begun &&
                    begun <= ended &&
                    ended < deadline &&
                    ended <= finish &&
                    (!previous || begun >= uint(previous.finishedMonotonicNs)),
                "RESOURCE_SAMPLE_TIMELINE",
            );
            observedSkipped += slot - lastSlot - 1n;
            lastSlot = slot;
            same(row.captureDurationNs, String(ended - begun), "RESOURCE_CAPTURE_ACCOUNTING");
            same(row.latenessNs, String(begun - scheduled), "RESOURCE_LATENESS_ACCOUNTING");
            maximumCapture = ended - begun > maximumCapture ? ended - begun : maximumCapture;
            maximumLateness = begun - scheduled > maximumLateness ? begun - scheduled : maximumLateness;
            const host = row.host;
            validateHost(host);
            need(
                host &&
                    host.unavailable &&
                    absolute(host.filesystem?.path) &&
                    (!options.expectedSpec?.filesystemPath ||
                        host.filesystem.path === options.expectedSpec.filesystemPath),
                "RESOURCE_HOST_SAMPLE",
            );
            uint(host.filesystem.device);
            need(uint(host.filesystem.availableBytes) <= uint(host.filesystem.totalBytes), "RESOURCE_FILESYSTEM");
            if (previous) same(host.filesystem.device, previous.host.filesystem.device, "RESOURCE_FILESYSTEM_CHANGED");
            counterMap(host.cpuTicks);
            same(
                row.hostCpuDeltaTicks,
                previous ? delta(previous.host.cpuTicks, host.cpuTicks) : null,
                "RESOURCE_HOST_DELTA",
            );
            need(Array.isArray(row.roles) && row.roles.length === spec.roles.length, "RESOURCE_SAMPLE_ROLES");
            let priorRoleEnd = begun;
            for (const [index, role] of row.roles.entries()) {
                same(role.role, spec.roles[index].role, "RESOURCE_ROLE_ORDER");
                same(role.identity, identities[index], "RESOURCE_IDENTITY_CHANGED");
                const a = uint(role.startedMonotonicNs),
                    b = uint(role.finishedMonotonicNs);
                need(priorRoleEnd <= a && a <= b && b <= ended, "RESOURCE_ROLE_WINDOW");
                priorRoleEnd = b;
                validateValues(role);
                checkLimits(role.values, options.expectedRoles?.[role.role]?.limits);
                sameNumeric(
                    role.interval,
                    previous ? roleInterval(previous.roles[index], role) : null,
                    "RESOURCE_INTERVAL_REBUILD",
                );
                resourceSummary(observed, role);
            }
            const unavailable = Object.fromEntries(Object.entries(host.unavailable).map(([k, v]) => [`host.${k}`, v]));
            for (const role of row.roles)
                for (const [k, v] of Object.entries(role.unavailable)) unavailable[`${role.role}.${k}`] = v;
            for (const [key, reason] of Object.entries(unavailable)) {
                need(typeof reason === "string" && reason.length > 0, "RESOURCE_MISSING_REASONS");
                const old = (missing[key] ??= { samples: 0, lastReason: reason });
                old.samples++;
                old.lastReason = reason;
            }
            for (const key of ["processCpuNs", "peakRssBytes"]) {
                uint(row.collector[key]);
                if (previous)
                    need(uint(row.collector[key]) >= uint(previous.collector[key]), "RESOURCE_COLLECTOR_COUNTER_RESET");
            }
            first ??= row;
            previous = row;
            count++;
        }
    } finally {
        lines.close();
        stream.destroy();
    }
    if (streamFailure) throw streamFailure;
    need(sampleBytes === sampleEntry.bytes && digest.digest("hex") === sampleEntry.sha256, "RESOURCE_MANIFEST_DIGEST");
    need(count > 0 && count === summary.samples && sampleBytes === summary.sampleBytes, "RESOURCE_SAMPLE_ACCOUNTING");
    same(summary.firstSampleStartedMonotonicNs, first.startedMonotonicNs, "RESOURCE_SUMMARY_WINDOW");
    same(summary.lastSampleFinishedMonotonicNs, previous.finishedMonotonicNs, "RESOURCE_SUMMARY_WINDOW");
    same(summary.maximumCaptureNs, String(maximumCapture), "RESOURCE_MAX_CAPTURE");
    same(summary.maximumLatenessNs, String(maximumLateness), "RESOURCE_MAX_LATENESS");
    need(
        integer(summary.skippedSlots, 0, 864000) &&
            BigInt(summary.skippedSlots) >= observedSkipped &&
            BigInt(summary.skippedSlots) <= (finish - start) / interval - BigInt(count) + 1n,
        "RESOURCE_SKIPPED_SLOTS",
    );
    same(missing, summary.missingMetrics, "RESOURCE_MISSING_SUMMARY");
    sameNumeric(observed, summary.observedResources, "RESOURCE_SUMMARY_REBUILD");
    same(summary.coverage, Object.keys(missing).length ? "partial" : "observed-metrics-present", "RESOURCE_COVERAGE");
    uint(summary.collectorCpuNs);
    need(
        uint(summary.collector.processCpuNs) >= uint(previous.collector.processCpuNs) &&
            uint(summary.collector.peakRssBytes) >= uint(previous.collector.peakRssBytes),
        "RESOURCE_COLLECTOR_TERMINAL",
    );
    const stopRaw = await bytes(options.stopMarkerPath, 4096),
        stop = JSON.parse(stopRaw);
    keys(
        stop,
        [
            "schemaVersion",
            "kind",
            "token",
            "specSha256",
            "hostBootId",
            "workflowFinishedMonotonicNs",
            "stopRequestedMonotonicNs",
        ],
        "RESOURCE_STOP_FIELDS",
    );
    need(
        stop.schemaVersion === 1 &&
            stop.kind === "http-resource-stop" &&
            stop.token === spec.controlledStop.token &&
            stop.specSha256 === hash(specRaw) &&
            stop.hostBootId === options.hostBootId,
        "RESOURCE_STOP_BINDING",
    );
    same(
        summary.controlledStop,
        { request: stop, sha256: hash(stopRaw), bytes: stopRaw.length, coveringSampleIndex: count - 1 },
        "RESOURCE_STOP_SUMMARY",
    );
    const requested = uint(stop.stopRequestedMonotonicNs),
        workflowEnd = uint(stop.workflowFinishedMonotonicNs);
    need(
        start <= workflowEnd &&
            workflowEnd <= requested &&
            requested <= uint(previous.startedMonotonicNs) &&
            uint(previous.finishedMonotonicNs) <= finish,
        "RESOURCE_STOP_COVERAGE",
    );
    if (options.workflowWindow) {
        const [a, b] = options.workflowWindow.map(uint);
        need(
            a <= b &&
                uint(first.finishedMonotonicNs) <= a &&
                workflowEnd === b &&
                uint(previous.startedMonotonicNs) >= b,
            "RESOURCE_WORKFLOW_COVERAGE",
        );
    }
    const issues = [];
    for (const [role, value] of Object.entries(observed)) {
        for (const field of ["oomKillsWithinObservedIntervals", "pidFailuresWithinObservedIntervals"])
            if (value[field] !== null && uint(value[field]) > 0n) issues.push(`${role}:${field}`);
    }
    let clockIdentity;
    try {
        clockIdentity = validateCollectorClockIdentity(summary, {
            hostBootId: options.hostBootId,
            componentSha256: options.componentSha256,
            expected: options.clockExpected,
            sampleWindow: [first.startedMonotonicNs, previous.finishedMonotonicNs],
        });
    } catch (error) {
        if (error instanceof CollectorClockEvidenceError) throw new ResourceEvidenceError(error.code);
        throw error;
    }
    return {
        status: "verified",
        sourceOrigin: summary.sourceOrigin,
        spec,
        identities: summary.identities,
        sampleCount: count,
        firstStartedMonotonicNs: first.startedMonotonicNs,
        finishedMonotonicNs: summary.finishedMonotonicNs,
        firstFinishedMonotonicNs: first.finishedMonotonicNs,
        lastFinishedMonotonicNs: previous.finishedMonotonicNs,
        observedResources: observed,
        missingMetrics: missing,
        summarySha256: hash(summaryRaw),
        coverage: summary.coverage,
        qualityIssues: issues,
        clockIdentity,
        terminalIdentityScope: "All archived sample identities agree; terminal /proc continuity is producer-attested",
        skippedSlotsScope: "Observed schedule gaps exact; final post-write skipped slots bounded by terminal time",
        performanceAccepted: false,
    };
}

function procRecord(row, uid, gid) {
    need(
        integer(row.hostPid, 1, 4194304) &&
            typeof row.statRaw === "string" &&
            row.statRaw.startsWith(String(row.hostPid) + " (") &&
            row.statRaw.includes(")"),
        "DRIVER_PROC_STAT",
    );
    const tail = row.statRaw
        .slice(row.statRaw.lastIndexOf(")") + 1)
        .trim()
        .split(/\s+/);
    need(tail.length >= 20 && !["Z", "X", "x"].includes(tail[0]), "DRIVER_PROC_NOT_LIVE");
    const ticks = tail[19];
    counter(ticks);
    function values(key) {
        const matched = [...row.statusRaw.matchAll(new RegExp(`^${key}:\\s+(.+)$`, "gm"))];
        need(matched.length === 1, "DRIVER_PROC_STATUS");
        return matched[0][1].trim().split(/\s+/);
    }
    const namespacePids = values("NSpid");
    namespacePids.forEach((v) => need(uint(v) >= 1n && uint(v) <= 4194304n, "DRIVER_NAMESPACE_PID_RANGE"));
    need(
        namespacePids[0] === String(row.hostPid) &&
            values("Uid")[1] === String(uid) &&
            values("Gid")[1] === String(gid),
        "DRIVER_PROC_OWNER",
    );
    const membership = row.cgroupRaw.split(/\r?\n/).filter((l) => l.startsWith("0::"));
    need(membership.length === 1 && absolute(membership[0].slice(3)), "DRIVER_PROC_CGROUP");
    keys(row.namespaces, ["pid", "net", "mnt", "user"], "DRIVER_PROC_NAMESPACES");
    for (const [name, value] of Object.entries(row.namespaces))
        need(new RegExp(`^${name}:\\[[0-9]+\\]$`).test(value), "DRIVER_PROC_NAMESPACES");
    return {
        hostPid: row.hostPid,
        containerPid: Number(namespacePids.at(-1)),
        startTimeTicks: ticks,
        cgroupMember: membership[0].slice(3),
        namespaces: row.namespaces,
    };
}

/** Validate driver inspect receipts and /proc mappings before relating collector anchors. */
export async function verifyDriverEvidence(directory, { arm, receipt, execution, runtime, target, targetPlan }) {
    const settings = runtime.settings,
        d = settings.driver;
    const records = await Promise.all(
        ["before-start", "started", "final"].map((name) => json(join(directory, `driver-${name}.json`))),
    );
    const [before, running, final] = records,
        owner = targetPlan.owner;
    const root = dirname(dirname(dirname(targetPlan.root))),
        evidence = `${root}/work/${arm.runId}/driver`;
    const label = "org.apache.hugegraph.hgbench.comparison";
    for (const info of records) {
        const h = info.HostConfig,
            c = info.Config;
        need(
            SHA.test(info.Id) &&
                info.Id === before.Id &&
                info.Name === `/hgbench-pair-${execution.executionId.slice(0, 12)}-${arm.runId.slice(-24)}` &&
                info.Id !== target.containerId &&
                info.Image === d.image &&
                c.Labels?.[label + ".execution"] === execution.executionId &&
                c.Labels[label + ".run"] === arm.runId &&
                c.Labels[label + ".runtime"] === execution.runtimeDigest,
            "DRIVER_CONTAINER_IDENTITY",
        );
        need(
            c.User === `${owner.uid}:${owner.gid}` &&
                h.NetworkMode === "container:" + target.containerId &&
                h.ReadonlyRootfs === true &&
                h.Memory === d.memoryBytes &&
                h.MemorySwap === d.memoryBytes &&
                h.NanoCpus === Math.round(d.cpus * 1e9) &&
                h.PidsLimit === d.pids &&
                h.Init === true &&
                isDeepStrictEqual(h.CapDrop, ["ALL"]) &&
                !(h.CapAdd || []).length &&
                !h.Privileged &&
                h.SecurityOpt?.includes("no-new-privileges:true") &&
                h.RestartPolicy?.Name === "no" &&
                h.LogConfig?.Type === "none" &&
                !h.PidMode &&
                !(h.Devices || []).length &&
                !Object.keys(h.PortBindings || {}).length &&
                !h.PublishAllPorts &&
                !Object.values(info.NetworkSettings?.Ports || {}).some(Boolean),
            "DRIVER_ISOLATION",
        );
        same(h.Tmpfs, { "/tmp": `rw,nosuid,nodev,noexec,size=${d.tmpfsBytes}` }, "DRIVER_TMPFS");
        same(c.Entrypoint, [d.python], "DRIVER_COMMAND");
        same(
            c.Cmd,
            ["-B", "/control/comparison-workflow.py", "_driver", "--arm", arm.arm, "--run-id", arm.runId],
            "DRIVER_COMMAND",
        );
        need(c.WorkingDir === "/source", "DRIVER_COMMAND");
        need(
            info.Mounts.length === (settings.schemaVersion === 2 ? 6 : 5) &&
                info.Mounts.every((m) => m.Type === "bind") &&
                new Set(info.Mounts.map((m) => m.Destination)).size === (settings.schemaVersion === 2 ? 6 : 5),
            "DRIVER_MOUNTS",
        );
        same(
            Object.fromEntries(info.Mounts.map((m) => [m.Destination, [m.Source, m.RW]])),
            {
                "/source": [settings.sourceRoot, false],
                "/node": [settings.nodeHome, false],
                "/comparison": [root + "/comparison", false],
                "/control": [root, false],
                "/evidence": [evidence, true],
                ...(settings.schemaVersion === 2
                    ? { [settings.executor.binary.path]: [settings.executor.binary.path, false] }
                    : {}),
            },
            "DRIVER_MOUNTS",
        );
    }
    need(
        before.State.Status === "created" &&
            running.State.Running === true &&
            integer(running.State.Pid, 1, 4194304) &&
            final.State.Status === "exited" &&
            final.State.Running === false &&
            final.State.OOMKilled === false &&
            final.State.ExitCode === 0 &&
            !final.State.Error,
        "DRIVER_TERMINAL",
    );
    const ready = await json(join(directory, "driver-ready.json")),
        go = await json(join(directory, "go.json"));
    const release = await json(join(directory, "release.json")),
        workflow = await json(join(directory, "workflow-result.json"));
    need(
        [ready, go, release, workflow].every((r) => r.runId === arm.runId) &&
            ready.bootId === execution.hostBootId &&
            go.runtimeDigest === execution.runtimeDigest &&
            workflow.ok === true &&
            typeof workflow.taskId === "string",
        "DRIVER_WORKFLOW_IDENTITY",
    );
    same(workflow.startedMonotonicNs, receipt.workflowStartedMonotonicNs, "DRIVER_WORKFLOW_WINDOW");
    same(workflow.finishedMonotonicNs, receipt.workflowFinishedMonotonicNs, "DRIVER_WORKFLOW_WINDOW");
    need(
        uint(receipt.armStartedMonotonicNs) <= uint(ready.atMonotonicNs) &&
            uint(ready.atMonotonicNs) <= uint(go.atMonotonicNs) &&
            uint(go.atMonotonicNs) <= uint(workflow.startedMonotonicNs) &&
            uint(workflow.finishedMonotonicNs) <= uint(release.atMonotonicNs) &&
            BigInt(target.post.window[1]) <= uint(release.atMonotonicNs) &&
            uint(release.atMonotonicNs) <= uint(receipt.armFinishedMonotonicNs),
        "DRIVER_WORKFLOW_ORDER",
    );
    const startedWall = Date.parse(running.State.StartedAt),
        endedWall = Date.parse(final.State.FinishedAt);
    need(
        Number.isFinite(startedWall) &&
            Number.isFinite(endedWall) &&
            endedWall >= startedWall &&
            endedWall - startedWall <= (d.wallSeconds + d.shutdownSeconds) * 1000,
        "DRIVER_WALL_BUDGET",
    );
    const proof = await json(join(directory, "process-identities.json"));
    need(
        proof.schemaVersion === 1 &&
            proof.kind === "http-comparison-process-identities" &&
            proof.hostBootId === execution.hostBootId &&
            uint(proof.recordedMonotonicNs) >= uint(ready.atMonotonicNs) &&
            uint(proof.recordedMonotonicNs) <= uint(go.atMonotonicNs),
        "DRIVER_PROC_SNAPSHOT",
    );
    same(proof.roles.map((r) => r.role).sort(), ["driver", "driver-supervisor", "target"], "DRIVER_PROC_ROLES");
    const roles = Object.fromEntries(proof.roles.map((r) => [r.role, procRecord(r, owner.uid, owner.gid)]));
    need(
        roles.driver.hostPid === running.State.Pid &&
            roles.driver.containerPid === 1 &&
            integer(ready.pid, 1, 4194304) &&
            roles["driver-supervisor"].containerPid === ready.pid &&
            roles["driver-supervisor"].startTimeTicks === String(ready.startTimeTicks) &&
            roles.target.containerPid === target.pre.identity.pid &&
            roles.target.startTimeTicks === String(target.pre.identity.startTimeTicks),
        "DRIVER_PROC_MAPPING",
    );
    same(roles.target.namespaces, target.pre.identity.namespaces, "DRIVER_TARGET_NAMESPACES");
    same(roles.driver.namespaces, roles["driver-supervisor"].namespaces, "DRIVER_SUPERVISOR_NAMESPACES");
    need(
        roles.driver.namespaces.net === roles.target.namespaces.net &&
            roles.driver.cgroupMember === roles["driver-supervisor"].cgroupMember &&
            roles.driver.cgroupMember.includes(before.Id) &&
            roles.target.cgroupMember.includes(target.containerId),
        "DRIVER_CGROUP_BINDING",
    );
    return {
        containerId: before.Id,
        workflow,
        driverReady: ready,
        expectedRoles: {
            target: { ...roles.target, limits: targetPlan.settings.budget },
            driver: { ...roles.driver, limits: d },
        },
        root,
        evidence,
        sourceOrigin: "recorded-inspect-and-procfs",
        performanceAccepted: false,
    };
}
