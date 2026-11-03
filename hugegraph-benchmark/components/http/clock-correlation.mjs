/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
import { constants, readFileSync, readlinkSync, openSync, closeSync, fstatSync, readSync } from "node:fs";
import { open, readdir } from "node:fs/promises";
import { createHash, randomBytes } from "node:crypto";
import { performance } from "node:perf_hooks";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { isDeepStrictEqual } from "node:util";

const HERE = fileURLToPath(import.meta.url),
    TOKEN = /^[a-f0-9]{32}$/,
    SHA = /^[a-f0-9]{64}$/;
const MiB = 1024 * 1024;
const contexts = new WeakMap();
const verifiedObjects = new WeakSet();
function registered(value) {
    const freeze = (item) => {
        if (item && typeof item === "object") {
            for (const child of Object.values(item)) freeze(child);
            Object.freeze(item);
        }
    };
    freeze(value);
    verifiedObjects.add(value);
    return value;
}
const hash = (v) => createHash("sha256").update(v).digest("hex");
function fileHash(path) {
    const fd = openSync(path, "r"),
        h = createHash("sha256"),
        chunk = Buffer.alloc(1024 * 1024);
    try {
        const before = fstatSync(fd);
        need(before.isFile() && before.size <= 1024 * MiB, "CLOCK_RUNTIME_FILE");
        let count;
        while ((count = readSync(fd, chunk, 0, chunk.length, null))) h.update(chunk.subarray(0, count));
        const after = fstatSync(fd);
        need(before.size === after.size && before.mtimeMs === after.mtimeMs, "CLOCK_INPUT_CHANGED");
        return h.digest("hex");
    } finally {
        closeSync(fd);
    }
}
export class ClockCorrelationError extends Error {
    constructor(code) {
        super(code);
        this.code = code;
    }
}
function need(value, code) {
    if (!value) throw new ClockCorrelationError(code);
}
function same(a, b, code) {
    need(isDeepStrictEqual(a, b), code);
}
function keys(value, expected, code) {
    need(value && typeof value === "object" && !Array.isArray(value), code);
    same(Object.keys(value).sort(), [...expected].sort(), code);
}
function uint(v) {
    need(typeof v === "string" && /^(0|[1-9][0-9]{0,24})$/.test(v), "CLOCK_INTEGER_PRECISION");
    return BigInt(v);
}
function integer(v, low, high) {
    return Number.isSafeInteger(v) && v >= low && v <= high;
}
function finiteMs(v) {
    need(typeof v === "number" && Number.isFinite(v) && v >= 0 && v <= 1e12, "CLOCK_PERFORMANCE_RANGE");
}
export function validateClockSpec(s) {
    keys(
        s,
        [
            "schemaVersion",
            "kind",
            "sourceOrigin",
            "sampleCount",
            "intervalMs",
            "exchangeTimeoutMs",
            "wallMs",
            "maxEvidenceBytes",
            "maxRttNs",
            "maxMappingWidthNs",
            "maxDriftPpm",
        ],
        "CLOCK_SPEC_FIELDS",
    );
    need(s.schemaVersion === 1 && s.kind === "http-clock-correlation-spec", "CLOCK_SPEC_KIND");
    need(["linux-procfs", "fixture"].includes(s.sourceOrigin), "CLOCK_SOURCE_ORIGIN");
    for (const [key, low, high] of [
        ["sampleCount", 2, 1000],
        ["intervalMs", 1, 60000],
        ["exchangeTimeoutMs", 100, 10000],
        ["wallMs", 1000, 120000],
        ["maxEvidenceBytes", 65536, 16 * MiB],
        ["maxDriftPpm", 0, 100000],
    ])
        need(integer(s[key], low, high), "CLOCK_SPEC_BUDGET");
    need((s.sampleCount - 1) * s.intervalMs < s.wallMs, "CLOCK_SPEC_BUDGET");
    need(
        uint(s.maxRttNs) > 0n &&
            uint(s.maxRttNs) <= BigInt(s.exchangeTimeoutMs) * 1000000n &&
            uint(s.maxMappingWidthNs) > 0n &&
            uint(s.maxMappingWidthNs) <= BigInt(s.wallMs) * 1000000n,
        "CLOCK_SPEC_BUDGET",
    );
    return s;
}

// Same-process context; never substitutes another process's performance origin.
export function createClockContext({ sourceOrigin, sessionId }) {
    need(["linux-procfs", "fixture"].includes(sourceOrigin) && TOKEN.test(sessionId), "CLOCK_CONTEXT");
    const context = {
        sourceOrigin,
        sessionId,
        processToken: randomBytes(16).toString("hex"),
        runtime: {
            name: "node",
            version: process.version,
            uv: process.versions.uv,
            executable: process.execPath,
            executableSha256: fileHash(process.execPath),
            componentSha256: hash(readFileSync(HERE)),
        },
    };
    context.initialIdentity = structuredClone(nodeIdentity(context));
    const handle = Object.freeze({ sessionId, sourceOrigin });
    contexts.set(handle, context);
    return handle;
}
function nodeIdentity(c) {
    let bootId = null,
        startTimeTicks = null,
        timeNamespace = null,
        timeForChildrenNamespace = null,
        timeOffsets = null;
    if (c.sourceOrigin === "linux-procfs") {
        need(process.platform === "linux", "CLOCK_LINUX_REQUIRED");
        const stat = () => {
            const raw = readFileSync("/proc/self/stat", "utf8");
            const tail = raw
                .slice(raw.lastIndexOf(")") + 2)
                .trim()
                .split(/\s+/);
            need(tail.length >= 20 && !["Z", "X", "x"].includes(tail[0]), "CLOCK_PROCESS_STATE");
            uint(tail[19]);
            return tail[19];
        };
        startTimeTicks = stat();
        bootId = readFileSync("/proc/sys/kernel/random/boot_id", "utf8").trim();
        timeNamespace = readlinkSync("/proc/self/ns/time");
        timeForChildrenNamespace = readlinkSync("/proc/self/ns/time_for_children");
        timeOffsets = readFileSync("/proc/self/timens_offsets", "utf8");
        same(stat(), startTimeTicks, "CLOCK_PROCESS_CHANGED");
    }
    return {
        sourceOrigin: c.sourceOrigin,
        platform: process.platform,
        pid: process.pid,
        processToken: c.processToken,
        bootId,
        startTimeTicks,
        timeNamespace,
        timeForChildrenNamespace,
        timeOffsets,
        runtime: structuredClone(c.runtime),
    };
}
export function captureAnchor(handle, { sequence, nonce }) {
    const context = contexts.get(handle);
    need(context, "CLOCK_CONTEXT");
    need(integer(sequence, 0, 999) && TOKEN.test(nonce), "CLOCK_ANCHOR_REQUEST");
    const before = nodeIdentity(context);
    same(before, context.initialIdentity, "CLOCK_NODE_IDENTITY_CHANGED");
    const hrtimeBeforeNs = process.hrtime.bigint().toString();
    const performanceMs = performance.now();
    const hrtimeAfterNs = process.hrtime.bigint().toString();
    const after = nodeIdentity(context);
    same(before, after, "CLOCK_NODE_IDENTITY_CHANGED");
    return {
        schemaVersion: 1,
        kind: "http-clock-anchor",
        sessionId: context.sessionId,
        sequence,
        nonce,
        hrtimeBeforeNs,
        performanceMs,
        hrtimeAfterNs,
        identityBefore: before,
        identityAfter: after,
    };
}

// Exact conversion of a binary64 millisecond value to a rational number of nanoseconds.
function rational(v) {
    // adjacent(maxAllowedMs) is one ULP outside the public input range.
    need(typeof v === "number" && Number.isFinite(v) && v >= 0 && v <= 2e12, "CLOCK_PERFORMANCE_RANGE");
    const b = Buffer.alloc(8);
    b.writeDoubleBE(v);
    const bits = b.readBigUInt64BE(),
        exp = Number((bits >> 52n) & 2047n),
        frac = bits & ((1n << 52n) - 1n);
    const sig = exp === 0 ? frac : frac + (1n << 52n),
        power = (exp || 1) - 1023 - 52;
    return power >= 0 ? [(sig * 1000000n) << BigInt(power), 1n] : [sig * 1000000n, 1n << BigInt(-power)];
}
function add(a, b) {
    return [a[0] * b[1] + b[0] * a[1], a[1] * b[1]];
}
function floor(a) {
    return a[0] / a[1];
}
function ceil(a) {
    return (a[0] + a[1] - 1n) / a[1];
}
function adjacent(v, step) {
    const b = Buffer.alloc(8);
    b.writeDoubleBE(v);
    b.writeBigUInt64BE(b.readBigUInt64BE() + BigInt(step));
    return b.readDoubleBE();
}
// All values whose correctly rounded binary64 subtraction could have produced relativeMs.
export function relativePerformanceBounds(originMs, relativeMs) {
    finiteMs(originMs);
    finiteMs(relativeMs);
    relativeMs = relativeMs === 0 ? 0 : relativeMs;
    const center = rational(relativeMs);
    const low = relativeMs === 0 ? [0n, 1n] : add(rational(adjacent(relativeMs, -1)), center);
    const high = add(center, rational(adjacent(relativeMs, 1)));
    if (relativeMs !== 0) low[1] *= 2n;
    high[1] *= 2n;
    return [floor(add(rational(originMs), low)), ceil(add(rational(originMs), high))];
}
function bounds(v) {
    const r = rational(v);
    return [floor(r), ceil(r)];
}
function identity(i, origin, runtime) {
    keys(
        i,
        [
            "sourceOrigin",
            "platform",
            "pid",
            "processToken",
            "bootId",
            "startTimeTicks",
            "timeNamespace",
            "timeForChildrenNamespace",
            "timeOffsets",
            "runtime",
        ],
        "CLOCK_IDENTITY_FIELDS",
    );
    need(i.sourceOrigin === origin && integer(i.pid, 1, 4194304) && TOKEN.test(i.processToken), "CLOCK_IDENTITY");
    need(
        i.runtime?.name === runtime &&
            typeof i.runtime.version === "string" &&
            i.runtime.version.length <= 256 &&
            typeof i.runtime.executable === "string" &&
            i.runtime.executable.startsWith("/") &&
            SHA.test(i.runtime.executableSha256) &&
            SHA.test(i.runtime.componentSha256),
        "CLOCK_RUNTIME_IDENTITY",
    );
    if (origin === "linux-procfs") {
        need(
            i.platform === "linux" &&
                /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/.test(i.bootId) &&
                uint(i.startTimeTicks) > 0n &&
                /^time:\[[0-9]+\]$/.test(i.timeNamespace) &&
                /^time:\[[0-9]+\]$/.test(i.timeForChildrenNamespace),
            "CLOCK_LINUX_IDENTITY",
        );
        need(
            typeof i.timeOffsets === "string" &&
                i.timeOffsets.length <= 256 &&
                /^monotonic\s+-?\d+\s+\d+\s*\nboottime\s+-?\d+\s+\d+\s*\n?$/.test(i.timeOffsets.trimStart()),
            "CLOCK_TIME_OFFSETS",
        );
    } else {
        for (const key of ["bootId", "startTimeTicks", "timeNamespace", "timeForChildrenNamespace", "timeOffsets"])
            need(i[key] === null, "CLOCK_FIXTURE_IDENTITY");
    }
}
function compatibleRate(aLow, aHigh, bLow, bHigh, ppm) {
    const million = 1000000n,
        tolerance = BigInt(ppm);
    return aLow * (million - tolerance) <= bHigh * million && aHigh * (million + tolerance) >= bLow * million;
}

export function verifyClockRecords(spec, session, records, terminal) {
    validateClockSpec(spec);
    need(
        session.schemaVersion === 1 &&
            session.kind === "http-clock-correlation-session" &&
            TOKEN.test(session.sessionId),
        "CLOCK_SESSION",
    );
    need(session.sourceOrigin === spec.sourceOrigin, "CLOCK_SOURCE_ORIGIN");
    identity(session.hostIdentity, spec.sourceOrigin, "python");
    const clock = session.hostClock;
    need(
        clock?.api === "time.monotonic_ns" &&
            clock.monotonic === true &&
            clock.adjustable === false &&
            typeof clock.implementation === "string" &&
            clock.implementation.length <= 128 &&
            Number.isFinite(clock.resolutionSeconds) &&
            clock.resolutionSeconds > 0 &&
            clock.resolutionSeconds <= 0.001,
        "CLOCK_HOST_CLOCK",
    );
    if (spec.sourceOrigin === "linux-procfs")
        need(clock.implementation === "clock_gettime(CLOCK_MONOTONIC)", "CLOCK_HOST_CLOCK");
    const hostResolutionNs = BigInt(Math.ceil(clock.resolutionSeconds * 1000000000));
    need(
        terminal.schemaVersion === 1 &&
            terminal.kind === "http-clock-correlation-result" &&
            terminal.sessionId === session.sessionId &&
            terminal.status === "completed" &&
            terminal.error === null &&
            terminal.exitCode === 0 &&
            terminal.inputsReverified === true &&
            terminal.performanceAccepted === false &&
            terminal.sampleCount === spec.sampleCount,
        "CLOCK_CAPTURE_INCOMPLETE",
    );
    const started = uint(session.startedMonotonicNs),
        finished = uint(terminal.finishedMonotonicNs);
    need(finished >= started && finished - started <= BigInt(spec.wallMs) * 1000000n, "CLOCK_WALL_BUDGET");
    need(Array.isArray(records) && records.length === spec.sampleCount, "CLOCK_SAMPLE_COUNT");
    const anchors = [],
        nonces = new Set();
    let wireBytes = 0;
    let previous = null,
        nodeIdentity = null;
    for (let index = 0; index < records.length; index++) {
        const row = records[index];
        keys(
            row,
            [
                "sequence",
                "requestWire",
                "responseWire",
                "sendMonotonicNs",
                "receiveMonotonicNs",
                "hostBefore",
                "hostAfter",
            ],
            "CLOCK_SAMPLE_FIELDS",
        );
        need(
            row.sequence === index &&
                typeof row.requestWire === "string" &&
                typeof row.responseWire === "string" &&
                Buffer.byteLength(row.requestWire) <= 4096 &&
                Buffer.byteLength(row.responseWire) <= 16384 &&
                row.requestWire.endsWith("\n") &&
                row.responseWire.endsWith("\n"),
            "CLOCK_FRAME_BUDGET",
        );
        const request = JSON.parse(row.requestWire),
            response = JSON.parse(row.responseWire);
        wireBytes += Buffer.byteLength(row.responseWire);
        keys(request, ["schemaVersion", "sessionId", "sequence", "nonce"], "CLOCK_REQUEST_FIELDS");
        keys(
            response,
            [
                "schemaVersion",
                "kind",
                "sessionId",
                "sequence",
                "nonce",
                "hrtimeBeforeNs",
                "performanceMs",
                "hrtimeAfterNs",
                "identityBefore",
                "identityAfter",
            ],
            "CLOCK_RESPONSE_FIELDS",
        );
        need(
            request.schemaVersion === 1 &&
                request.sessionId === session.sessionId &&
                request.sequence === index &&
                TOKEN.test(request.nonce) &&
                !nonces.has(request.nonce),
            "CLOCK_NONCE",
        );
        nonces.add(request.nonce);
        need(
            response.schemaVersion === 1 &&
                response.kind === "http-clock-anchor" &&
                response.sessionId === session.sessionId &&
                response.sequence === index &&
                response.nonce === request.nonce,
            "CLOCK_RESPONSE_BINDING",
        );
        same(row.hostBefore, session.hostIdentity, "CLOCK_HOST_IDENTITY_CHANGED");
        same(row.hostAfter, session.hostIdentity, "CLOCK_HOST_IDENTITY_CHANGED");
        identity(response.identityBefore, spec.sourceOrigin, "node");
        same(response.identityBefore, response.identityAfter, "CLOCK_NODE_IDENTITY_CHANGED");
        nodeIdentity ??= response.identityBefore;
        same(response.identityBefore, nodeIdentity, "CLOCK_NODE_IDENTITY_CHANGED");
        same(response.identityBefore.runtime.executableSha256, session.nodeExecutable.sha256, "CLOCK_NODE_EXECUTABLE");
        same(response.identityBefore.runtime.executable, session.nodeExecutable.path, "CLOCK_NODE_EXECUTABLE");
        same(response.identityBefore.runtime.componentSha256, session.components.node, "CLOCK_NODE_COMPONENT");
        same(session.hostIdentity.runtime.componentSha256, session.components.python, "CLOCK_HOST_COMPONENT");
        need(terminal.childPid === nodeIdentity.pid, "CLOCK_CHILD_IDENTITY");
        if (spec.sourceOrigin === "linux-procfs") {
            same(nodeIdentity.bootId, session.hostIdentity.bootId, "CLOCK_BOOT_MISMATCH");
            // Offset namespaces may differ in a future transport, but this stdio child must inherit the host namespace.
            for (const key of ["timeNamespace", "timeForChildrenNamespace", "timeOffsets"])
                same(nodeIdentity[key], session.hostIdentity[key], "CLOCK_NAMESPACE_MISMATCH");
        }
        const send = uint(row.sendMonotonicNs),
            receive = uint(row.receiveMonotonicNs),
            hrBefore = uint(response.hrtimeBeforeNs),
            hrAfter = uint(response.hrtimeAfterNs),
            p = bounds(response.performanceMs);
        need(started <= send && send <= receive && receive <= finished, "CLOCK_HOST_ORDER");
        need(receive - send <= uint(spec.maxRttNs), "CLOCK_RTT_BUDGET");
        need(
            hrBefore <= hrAfter &&
                compatibleRate(
                    0n,
                    receive - send + 2n * hostResolutionNs,
                    hrAfter - hrBefore,
                    hrAfter - hrBefore,
                    spec.maxDriftPpm,
                ),
            "CLOCK_ANCHOR_ORDER",
        );
        if (previous) {
            need(
                previous.receive <= send &&
                    previous.hrAfter <= hrBefore &&
                    previous.performanceMs <= response.performanceMs,
                "CLOCK_MONOTONIC_ORDER",
            );
            need(send - previous.receive >= BigInt(spec.intervalMs) * 1000000n, "CLOCK_SAMPLE_INTERVAL");
            const hostLow = send - previous.receive,
                hostHigh = receive - previous.send,
                hrLow = hrBefore - previous.hrAfter,
                hrHigh = hrAfter - previous.hrBefore,
                perfLow = p[0] > previous.p[1] ? p[0] - previous.p[1] : 0n,
                perfHigh = p[1] - previous.p[0];
            need(compatibleRate(hostLow, hostHigh, hrLow, hrHigh, spec.maxDriftPpm), "CLOCK_OBSERVED_HOST_RATE");
            need(compatibleRate(hrLow, hrHigh, perfLow, perfHigh, spec.maxDriftPpm), "CLOCK_OBSERVED_NODE_RATE");
        }
        const anchor = {
            nonce: response.nonce,
            sequence: index,
            performanceMs: response.performanceMs,
            hostIntervalNs: [send.toString(), receive.toString()],
            hrtimeIntervalNs: [hrBefore.toString(), hrAfter.toString()],
        };
        anchors.push(anchor);
        previous = { send, receive, hrBefore, hrAfter, p, performanceMs: response.performanceMs };
    }
    need(
        terminal.stdoutBytes === wireBytes &&
            terminal.stderr === "" &&
            terminal.stderrBytes === 0 &&
            terminal.stderrSha256 === hash(""),
        "CLOCK_TRANSPORT_ACCOUNTING",
    );
    return registered({
        schemaVersion: 1,
        kind: "http-clock-correlation-verification",
        verified: true,
        sessionId: session.sessionId,
        sourceOrigin: spec.sourceOrigin,
        hostBootId: session.hostIdentity.bootId,
        hostResolutionNs: hostResolutionNs.toString(),
        nodeIdentityDigest: hash(JSON.stringify(nodeIdentity)),
        sampleCount: anchors.length,
        maxMappingWidthNs: spec.maxMappingWidthNs,
        anchors,
        performanceAccepted: false,
        scope: "causal-intervals-only; observed-rate-gate; no-UTC-or-offset-fit; no-runner-integration",
    });
}

export function mapClockEvent(verified, event) {
    need(verifiedObjects.has(verified), "CLOCK_NOT_VERIFIED");
    const direct = event.anchorNonce !== undefined;
    keys(
        event,
        direct
            ? ["sessionId", "nodeIdentityDigest", "anchorNonce"]
            : ["sessionId", "nodeIdentityDigest", "phaseOriginNonce", "relativeMs"],
        "CLOCK_EVENT_FIELDS",
    );
    need(
        event.sessionId === verified.sessionId && event.nodeIdentityDigest === verified.nodeIdentityDigest,
        "CLOCK_EVENT_IDENTITY",
    );
    const base = {
        schemaVersion: 1,
        kind: "http-clock-event-mapping",
        sessionId: verified.sessionId,
        sourceOrigin: verified.sourceOrigin,
        performanceAccepted: false,
        method: "causal-outer-interval",
    };
    let left, right;
    if (direct) {
        left = right = verified.anchors.find((a) => a.nonce === event.anchorNonce);
        need(left, "CLOCK_EVENT_ANCHOR");
    } else {
        const origin = verified.anchors.find((a) => a.nonce === event.phaseOriginNonce);
        need(origin, "CLOCK_EVENT_ANCHOR");
        const [low, high] = relativePerformanceBounds(origin.performanceMs, event.relativeMs);
        // Equality of rounded readings does not prove event ordering. Require strict bracketing.
        left = verified.anchors.filter((a) => bounds(a.performanceMs)[1] < low).at(-1);
        right = verified.anchors.find((a) => bounds(a.performanceMs)[0] > high);
        if (!left || !right)
            return { ...base, usable: false, reason: "outside-strict-anchor-coverage", hostIntervalNs: null };
    }
    const interval = [left.hostIntervalNs[0], right.hostIntervalNs[1]],
        width = uint(interval[1]) - uint(interval[0]),
        usable = width <= uint(verified.maxMappingWidthNs);
    return {
        ...base,
        usable,
        reason: usable ? null : "mapping-width-exceeds-budget",
        hostIntervalNs: interval,
        uncertaintyWidthNs: width.toString(),
        bracketingNonces: [left.nonce, right.nonce],
    };
}
async function bounded(path, cap) {
    const f = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
    try {
        const before = await f.stat();
        need(before.isFile() && before.size <= cap, "CLOCK_FILE_BUDGET");
        const b = Buffer.alloc(before.size + 1);
        let n = 0;
        while (n < b.length) {
            const r = await f.read(b, n, b.length - n, n);
            if (!r.bytesRead) break;
            n += r.bytesRead;
        }
        const after = await f.stat();
        need(n === before.size && after.size === before.size && after.mtimeMs === before.mtimeMs, "CLOCK_FILE_CHANGED");
        return b.subarray(0, n);
    } finally {
        await f.close();
    }
}
export async function verifyClockEvidence(directory) {
    const manifestRaw = await bounded(join(directory, "manifest.json"), 16384),
        manifest = JSON.parse(manifestRaw);
    need(manifest.schemaVersion === 1 && manifest.kind === "http-clock-correlation-manifest", "CLOCK_MANIFEST");
    const names = ["result.json", "samples.ndjson", "session.json", "spec.json"];
    same((await readdir(directory)).sort(), [...names, "manifest.json"].sort(), "CLOCK_INVENTORY");
    need(Array.isArray(manifest.files) && manifest.files.length === names.length, "CLOCK_INVENTORY");
    const payload = {},
        raws = {};
    let total = manifestRaw.length;
    for (let i = 0; i < names.length; i++) {
        const e = manifest.files[i];
        need(e.path === names[i] && integer(e.bytes, 0, 16 * MiB) && SHA.test(e.sha256), "CLOCK_MANIFEST_ENTRY");
        const raw = await bounded(join(directory, e.path), e.bytes);
        need(raw.length === e.bytes && hash(raw) === e.sha256, "CLOCK_DIGEST");
        total += raw.length;
        need(total <= 16 * MiB, "CLOCK_OUTPUT_BUDGET");
        raws[e.path] = raw;
        payload[e.path] = new TextDecoder("utf8", { fatal: true }).decode(raw);
    }
    const spec = validateClockSpec(JSON.parse(payload["spec.json"]));
    need(total <= spec.maxEvidenceBytes, "CLOCK_OUTPUT_BUDGET");
    const samples = payload["samples.ndjson"];
    need(samples.endsWith("\n"), "CLOCK_TRACE_TERMINATOR");
    const lines = samples.slice(0, -1).split("\n", spec.sampleCount + 1);
    need(lines.length === spec.sampleCount, "CLOCK_SAMPLE_COUNT");
    const result = verifyClockRecords(
        spec,
        JSON.parse(payload["session.json"]),
        lines.map(JSON.parse),
        JSON.parse(payload["result.json"]),
    );
    same(await bounded(join(directory, "manifest.json"), 16384), manifestRaw, "CLOCK_MANIFEST_CHANGED");
    for (const name of names)
        same(await bounded(join(directory, name), raws[name].length), raws[name], "CLOCK_FILE_CHANGED");
    return registered({ ...result, evidenceDigest: hash(manifestRaw) });
}
async function peer(origin, session) {
    const context = createClockContext({ sourceOrigin: origin, sessionId: session });
    let buffer = Buffer.alloc(0),
        sequence = 0;
    for await (const chunk of process.stdin) {
        buffer = Buffer.concat([buffer, chunk]);
        need(buffer.length <= 4096, "CLOCK_REQUEST_BUDGET");
        let end;
        while ((end = buffer.indexOf(10)) !== -1) {
            const request = JSON.parse(buffer.subarray(0, end).toString("utf8"));
            buffer = buffer.subarray(end + 1);
            keys(request, ["schemaVersion", "sessionId", "sequence", "nonce"], "CLOCK_REQUEST_FIELDS");
            need(
                request.schemaVersion === 1 && request.sessionId === session && request.sequence === sequence++,
                "CLOCK_REQUEST_ORDER",
            );
            const response = captureAnchor(context, request);
            await new Promise((ok, fail) =>
                process.stdout.write(JSON.stringify(response) + "\n", (error) => (error ? fail(error) : ok())),
            );
        }
    }
    need(buffer.length === 0, "CLOCK_PARTIAL_REQUEST");
}
async function main(args) {
    if (args.length === 0 || args.includes("--help") || args.includes("-h")) {
        console.log(
            "Clock correlation: verify --evidence DIR | map --evidence DIR --event JSON_FILE\n" +
                "Capture: python3 components/http/clock-correlation.py capture --spec SPEC --out NEW --node ABS_NODE\n" +
                "Mapping uses causal outer intervals; no extrapolation or UTC alignment. Fixture evidence is labelled.\n" +
                "The internal peer command is a bounded stdio transport, not a background service.",
        );
        return;
    }
    if (args[0] === "peer" && args.length === 3) {
        await peer(args[1], args[2]);
        return;
    }
    need(["verify", "map"].includes(args[0]), "CLOCK_COMMAND");
    need(args[1] === "--evidence" && args[2] && args.length === (args[0] === "verify" ? 3 : 5), "CLOCK_ARGUMENTS");
    const verified = await verifyClockEvidence(resolve(args[2]));
    if (args[0] === "verify") console.log(JSON.stringify({ ok: true, data: verified }));
    else {
        need(args[3] === "--event", "CLOCK_ARGUMENTS");
        const result = mapClockEvent(verified, JSON.parse(await bounded(resolve(args[4]), 16384)));
        console.log(JSON.stringify({ ok: result.usable, data: result }));
        if (!result.usable) process.exitCode = 6;
    }
}
if (process.argv[1] && resolve(process.argv[1]) === HERE)
    main(process.argv.slice(2)).catch((e) => {
        const output = JSON.stringify({ ok: false, error: e.code || e.message });
        if (process.argv[2] === "peer") console.error(output);
        else console.log(output);
        process.exitCode = 6;
    });
