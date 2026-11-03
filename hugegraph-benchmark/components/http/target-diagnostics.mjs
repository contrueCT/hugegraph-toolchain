/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
import { open, realpath } from "node:fs/promises";
import { constants } from "node:fs";
import { dirname, resolve, sep } from "node:path";
import { createHash } from "node:crypto";

const MiB = 1024 * 1024;
const hash = (b) => createHash("sha256").update(b).digest("hex");
const need = (v, message) => {
    if (!v) throw new Error("TARGET_DIAGNOSTICS: " + message);
};
async function regular(path, cap) {
    const fd = await open(path, constants.O_RDONLY | constants.O_NONBLOCK | constants.O_NOFOLLOW);
    try {
        const before = await fd.stat({ bigint: true });
        need(before.isFile() && before.size <= BigInt(cap), "file byte budget");
        const raw = await fd.readFile();
        const after = await fd.stat({ bigint: true });
        need(
            ["dev", "ino", "size", "mtimeNs", "ctimeNs"].every((k) => before[k] === after[k]) &&
                BigInt(raw.length) === before.size,
            "input changed",
        );
        return raw;
    } finally {
        await fd.close();
    }
}
function exact(value, fields) {
    need(
        value &&
            typeof value === "object" &&
            !Array.isArray(value) &&
            Object.keys(value).sort().join() === [...fields].sort().join(),
        "unexpected fields",
    );
}
function timestamp(value) {
    if (typeof value !== "string" || !/^\d{4}-\d\d-\d\dT.*(?:Z|[+-]\d\d:?\d\d)$/.test(value)) return null;
    const ms = Date.parse(value.replace(/(\.\d{3})\d+/, "$1").replace(/([+-]\d\d)(\d\d)$/, "$1:$2"));
    return Number.isFinite(ms) ? ms : null;
}
function uint(value) {
    if (typeof value === "string" && /^(0|[1-9][0-9]{0,18})$/.test(value)) return BigInt(value).toString();
    if (Number.isSafeInteger(value) && value >= 0) return String(value);
    return null;
}
function hostSample(row, format) {
    let time, swap;
    if (format === "sf100-host-guard-v1") {
        time = row.atUnix * 1000;
        swap = uint(row.swapTotalBytes);
    } else if (format === "http-standalone-observer-v1") {
        time = row.time * 1000;
        const match = typeof row.hostMemory === "string" && /^SwapTotal:\s+(\d+) kB$/m.exec(row.hostMemory);
        swap = match ? (BigInt(match[1]) * 1024n).toString() : null;
    } else {
        need(row.schemaVersion === 1 && row.kind === "http-resource-sample", "resource sample identity");
        time = timestamp(row.observedAt);
        swap = uint(row.host?.memoryBytes?.SwapTotal);
    }
    need(Number.isFinite(time) && time > 0, "host sample timestamp");
    return { time, swapTotalBytes: swap };
}
function swapWindow(samples, start, end, maxGapMs) {
    let first = -1,
        last = -1;
    for (let i = 0; i < samples.length; i++) {
        if (samples[i].time <= start) first = i;
        if (samples[i].time >= end) {
            last = i;
            break;
        }
    }
    if (first < 0 || last < 0 || first >= last)
        return { status: "unknown", reason: "sample-range-does-not-enclose-window" };
    const covering = samples.slice(first, last + 1);
    const maximumGapMs = Math.max(...covering.slice(1).map((s, i) => s.time - covering[i].time));
    if (maximumGapMs > maxGapMs || covering.some((s) => s.swapTotalBytes === null))
        return {
            status: "unknown",
            reason: maximumGapMs > maxGapMs ? "sample-gap" : "swap-metric-unavailable",
            maximumGapMs,
        };
    return {
        status: covering.every((s) => s.swapTotalBytes === "0") ? "sampled-disabled" : "enabled-in-covering-samples",
        samples: covering.length,
        maximumGapMs,
    };
}

// Offline diagnostic association only. Existing formal clock and quality gates stay unchanged.
// Digests prove input identity; they do not attest the target JVM, host or a complete GC capture.
export async function readTargetDiagnostics(file, planSha256, windows) {
    const manifestRaw = await regular(file, 65536),
        manifest = JSON.parse(manifestRaw);
    exact(manifest, ["schemaVersion", "kind", "planSha256", "maxSampleGapMs", "hostSamples", "gcLogs"]);
    need(
        manifest.schemaVersion === 1 &&
            manifest.kind === "throughput-target-diagnostics" &&
            manifest.planSha256 === planSha256,
        "attempt plan binding",
    );
    need(
        Number.isInteger(manifest.maxSampleGapMs) && manifest.maxSampleGapMs >= 100 && manifest.maxSampleGapMs <= 60000,
        "sample gap budget",
    );
    need(Array.isArray(manifest.gcLogs) && manifest.gcLogs.length <= 16 && windows.length <= 1000, "input/work budget");
    const base = await realpath(dirname(resolve(file))),
        files = [{ path: "manifest.json", raw: manifestRaw }],
        paths = new Set();
    let used = 0;
    async function artifact(fact, fields) {
        exact(fact, fields);
        need(
            typeof fact.path === "string" &&
                fact.path.length <= 2048 &&
                !fact.path.includes("\\") &&
                !fact.path.split("/").some((p) => !p || p === "." || p === "..") &&
                !paths.has(fact.path),
            "unique relative input path",
        );
        need(fact.path !== "manifest.json", "input collides with manifest");
        need(
            [...paths].every((p) => !p.startsWith(fact.path + "/") && !fact.path.startsWith(p + "/")),
            "overlapping input paths",
        );
        paths.add(fact.path);
        const path = await realpath(resolve(base, fact.path));
        need(path.startsWith(base + sep), "input escapes manifest directory");
        need(
            Number.isSafeInteger(fact.bytes) &&
                fact.bytes >= 0 &&
                fact.bytes <= 32 * MiB &&
                /^[a-f0-9]{64}$/.test(fact.sha256),
            "input fact",
        );
        used += fact.bytes;
        need(used <= 64 * MiB, "combined input byte budget");
        const raw = await regular(path, fact.bytes);
        need(raw.length === fact.bytes && hash(raw) === fact.sha256, "input digest/size mismatch");
        files.push({ path: fact.path, raw });
        return new TextDecoder("utf-8", { fatal: true }).decode(raw);
    }
    let samples = [];
    if (manifest.hostSamples !== null) {
        const fact = manifest.hostSamples;
        need(
            ["sf100-host-guard-v1", "http-standalone-observer-v1", "http-resource-collector-v1"].includes(fact.format),
            "host sample format",
        );
        const raw = await artifact(fact, ["path", "bytes", "sha256", "format"]);
        need(!raw || raw.endsWith("\n"), "truncated host sample input");
        const lines = raw.trim() ? raw.trimEnd().split("\n") : [];
        need(
            lines.length <= 20000 && lines.every((l) => l.length > 0 && Buffer.byteLength(l) <= 65536),
            "sample count/line budget",
        );
        samples = lines.map((l) => hostSample(JSON.parse(l), fact.format));
        need(
            samples.every((s, i) => i === 0 || s.time > samples[i - 1].time),
            "host wall-clock reversal/duplicate timestamp",
        );
    }
    const pauses = [],
        logs = [],
        eventIdentities = new Set();
    for (const fact of manifest.gcLogs) {
        need(typeof fact.role === "string" && /^[a-z][a-z0-9-]{0,31}$/.test(fact.role), "GC role label");
        const raw = await artifact(fact, ["path", "bytes", "sha256", "role"]);
        let unknownPauseLines = 0,
            count = 0,
            offset = 0;
        for (const line of raw.split(/(?<=\n)/)) {
            need(Buffer.byteLength(line) <= 65536, "GC line byte budget");
            const match = /^\[([^\]]+)\](?:\[[^\]\n]*\])*\s+GC\((\d+)\) (Pause .+) ([0-9]+(?:\.[0-9]+)?)ms\r?\n$/.exec(
                line,
            );
            if (match) {
                const end = timestamp(match[1]),
                    durationMs = Number(match[4]);
                if (end !== null && Number.isFinite(durationMs) && durationMs >= 0 && durationMs <= 86400000) {
                    const eventIdentity = JSON.stringify([fact.role, match[2], match[3], end, durationMs]);
                    need(!eventIdentities.has(eventIdentity), "duplicate pause in provided logs");
                    eventIdentities.add(eventIdentity);
                    pauses.push({
                        role: fact.role,
                        gcId: match[2],
                        title: match[3],
                        start: end - durationMs,
                        end,
                        durationMs,
                        path: fact.path,
                        byteOffset: offset,
                    });
                    count++;
                    need(pauses.length <= 100000, "GC event count budget");
                } else unknownPauseLines++;
            } else if (/GC\(\d+\) Pause /.test(line) && !/\[[^\]]*gc,start[^\]]*\]/.test(line)) unknownPauseLines++;
            offset += Buffer.byteLength(line);
        }
        logs.push({
            path: fact.path,
            role: fact.role,
            recognizedPauses: count,
            unknownPauseLines,
            completeCaptureVerified: false,
        });
    }
    need(windows.length * (samples.length + pauses.length) <= 50000000, "window association work budget");
    const rows = windows.map((window) => {
        const origin = timestamp(window.startedAt),
            phase = window.phase;
        const offset = phase?.start?.sinceOriginMs;
        if (
            origin === null ||
            !Number.isFinite(offset) ||
            offset < 0 ||
            !Number.isFinite(window.durationMs) ||
            window.durationMs <= 0
        )
            return {
                id: window.id,
                caseId: window.caseId,
                stage: window.stage,
                phase: phase?.name ?? null,
                status: "unknown",
                reason: "raw-wall-clock-window-unavailable",
            };
        const start = origin + offset,
            end = start + window.durationMs;
        const events = pauses.filter((p) => p.start < end && p.end > start);
        return {
            id: window.id,
            caseId: window.caseId,
            stage: window.stage,
            loadId: window.loadId,
            repeat: window.repeat,
            phase: phase.name,
            status: "wall-clock-diagnostic-only",
            start: new Date(start).toISOString(),
            end: new Date(end).toISOString(),
            swap: swapWindow(samples, start, end, manifest.maxSampleGapMs),
            gc: {
                status: logs.length ? "provided-log-events-only" : "unavailable",
                loggedPauses: logs.length ? events.length : null,
                loggedPauseOverlapMs: logs.length
                    ? events.reduce((n, p) => n + Math.max(0, Math.min(end, p.end) - Math.max(start, p.start)), 0)
                    : null,
                examples: events.slice(0, 8),
                omittedExamples: Math.max(0, events.length - 8),
                completeCaptureVerified: false,
            },
        };
    });
    return {
        files,
        summary: {
            schemaVersion: 1,
            kind: "throughput-target-diagnostics-result",
            manifestSha256: hash(manifestRaw),
            planSha256,
            association:
                "approximate-wall-clock; Go startedAt is not an exact clock-origin anchor; host/JVM association and clock error are unverified",
            targetIdentity: "operator-supplied-unverified",
            sampleCount: samples.length,
            logs,
            rows,
            scope: "Diagnostics only. Logged overlap sums process-role pauses, which may overlap each other. No causal attribution, zero-GC claim, automatic point exclusion or formal environment acceptance.",
            performanceAccepted: false,
        },
    };
}
