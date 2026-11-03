/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
import { constants, readFileSync, readlinkSync } from "node:fs";
import { open } from "node:fs/promises";
import { createHash, randomBytes } from "node:crypto";
import { performance } from "node:perf_hooks";
import { fileURLToPath } from "node:url";
import { isDeepStrictEqual } from "node:util";
import { valueDigest } from "./common.mjs";
import {
    startRunnerSanity,
    captureRunnerSanity,
    finishRunnerSanity,
    stopRunnerSanity,
    verifyRunnerSanityRaw,
    runnerSanityEvidenceBudget,
} from "./runner-sanity.mjs";

const HERE = fileURLToPath(import.meta.url),
    MAX_BYTES = 65536;
const CONTRACT = "linux-node22.14-cpython-monotonic-v1";
const OFFICIAL_BINARY = "1abce2374a485bddae3c27b17a3e3143e2780232026e627c4fe74ddde3f380a1";
const PENDING = [
    "runtime-provenance-lock",
    "host-python-clock",
    "host-namespace-binding",
    "raw-roundtrip-sanity",
    "predeclared-boundary-width",
    "component-source-lock",
];
const TOKEN = randomBytes(16).toString("hex"),
    handles = new WeakMap();
const hrtime = process.hrtime.bigint.bind(process.hrtime),
    now = performance.now.bind(performance);
let cachedRuntime;
export class DirectPhaseClockError extends Error {
    constructor(code) {
        super(code);
        this.code = code;
    }
}
function need(value, code) {
    if (!value) throw new DirectPhaseClockError(code);
}
function same(a, b, code) {
    need(isDeepStrictEqual(a, b), code);
}
function decimal(value) {
    need(typeof value === "string" && /^(0|[1-9][0-9]{0,24})$/.test(value), "DIRECT_CLOCK_INTEGER_PRECISION");
    return BigInt(value);
}
function boundedText(path, cap = 16384) {
    const value = readFileSync(path, "utf8");
    need(Buffer.byteLength(value) <= cap, "DIRECT_CLOCK_PROC_BUDGET");
    return value;
}
function processSnapshot() {
    const snapshot = {
        platform: process.platform,
        arch: process.arch,
        pid: process.pid,
        processToken: TOKEN,
        availability: "unavailable",
        reasons: [],
        bootId: null,
        startTimeTicks: null,
        statRaw: null,
        statusRaw: null,
        namespaces: { time: null, time_for_children: null, pid: null, mnt: null, user: null },
        timeOffsets: null,
    };
    if (process.platform !== "linux") {
        snapshot.reasons.push("platform-not-linux");
        return snapshot;
    }
    const attempt = (key, action) => {
        try {
            return action();
        } catch (error) {
            snapshot.reasons.push(`${key}:${error.code || "invalid"}`);
            return null;
        }
    };
    const before = attempt("stat", () => boundedText("/proc/self/stat"));
    const ticks = (raw) => {
        need(typeof raw === "string", "DIRECT_CLOCK_PROC_STAT");
        const tail = raw
            .slice(raw.lastIndexOf(")") + 2)
            .trim()
            .split(/\s+/);
        need(tail.length >= 20 && !["Z", "X", "x"].includes(tail[0]), "DIRECT_CLOCK_PROC_STAT");
        decimal(tail[19]);
        return tail[19];
    };
    snapshot.statRaw = before;
    snapshot.startTimeTicks = attempt("stat-identity", () => ticks(before));
    snapshot.statusRaw = attempt("status", () => boundedText("/proc/self/status"));
    snapshot.bootId = attempt("boot", () => boundedText("/proc/sys/kernel/random/boot_id", 128).trim());
    for (const name of Object.keys(snapshot.namespaces))
        snapshot.namespaces[name] = attempt(`namespace-${name}`, () => readlinkSync(`/proc/self/ns/${name}`));
    snapshot.timeOffsets = attempt("offsets", () => boundedText("/proc/self/timens_offsets", 256));
    const after = attempt("stat-after", () => ticks(boundedText("/proc/self/stat")));
    if (snapshot.startTimeTicks !== null) need(after === snapshot.startTimeTicks, "DIRECT_CLOCK_IDENTITY_DRIFT");
    if (!snapshot.reasons.length) snapshot.availability = "available";
    return snapshot;
}
function invariant(snapshot) {
    const { statRaw, statusRaw, ...stable } = snapshot;
    // CPU, memory and running state in /proc change normally. Bind only process identity and namespaces.
    return stable;
}
function validateSnapshot(snapshot) {
    need(
        snapshot &&
            Number.isSafeInteger(snapshot.pid) &&
            snapshot.pid >= 1 &&
            snapshot.pid <= 4194304 &&
            /^[a-f0-9]{32}$/.test(snapshot.processToken) &&
            ["available", "unavailable"].includes(snapshot.availability) &&
            Array.isArray(snapshot.reasons) &&
            snapshot.reasons.every((r) => typeof r === "string" && r.length <= 128),
        "DIRECT_CLOCK_PROCESS_IDENTITY",
    );
    if (snapshot.platform !== "linux") {
        need(
            snapshot.availability === "unavailable" && snapshot.reasons.includes("platform-not-linux"),
            "DIRECT_CLOCK_PLATFORM_IDENTITY",
        );
        for (const name of ["bootId", "startTimeTicks", "statRaw", "statusRaw", "timeOffsets"])
            need(snapshot[name] === null, "DIRECT_CLOCK_PLATFORM_IDENTITY");
        need(
            Object.values(snapshot.namespaces).every((v) => v === null),
            "DIRECT_CLOCK_PLATFORM_IDENTITY",
        );
        return;
    }
    if (snapshot.statRaw !== null) {
        const raw = snapshot.statRaw,
            tail = raw
                .slice(raw.lastIndexOf(")") + 2)
                .trim()
                .split(/\s+/);
        need(
            Buffer.byteLength(raw) <= 16384 &&
                raw.startsWith(`${snapshot.pid} (`) &&
                tail.length >= 20 &&
                !["Z", "X", "x"].includes(tail[0]) &&
                tail[19] === snapshot.startTimeTicks,
            "DIRECT_CLOCK_PROC_STAT",
        );
        decimal(snapshot.startTimeTicks);
    }
    if (snapshot.statusRaw !== null) {
        need(Buffer.byteLength(snapshot.statusRaw) <= 16384, "DIRECT_CLOCK_PROC_BUDGET");
        const pid = /^Pid:\s+(\d+)$/m.exec(snapshot.statusRaw),
            nspid = /^NSpid:\s+([0-9\t ]+)$/m.exec(snapshot.statusRaw);
        need(pid && Number(pid[1]) === snapshot.pid, "DIRECT_CLOCK_PROC_STATUS");
        if (nspid) {
            const chain = nspid[1].trim().split(/\s+/).map(decimal);
            need(
                chain.every((p) => p >= 1n && p <= 4194304n) && chain.at(-1) === BigInt(snapshot.pid),
                "DIRECT_CLOCK_PROC_STATUS",
            );
        }
    }
    if (snapshot.bootId !== null)
        need(
            /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/.test(snapshot.bootId),
            "DIRECT_CLOCK_BOOT_IDENTITY",
        );
    for (const [name, value] of Object.entries(snapshot.namespaces))
        if (value !== null)
            need(
                new RegExp(`^${name === "time_for_children" ? "time" : name}:\\[[0-9]+\\]$`).test(value),
                "DIRECT_CLOCK_NAMESPACE",
            );
    if (snapshot.timeOffsets !== null) {
        need(Buffer.byteLength(snapshot.timeOffsets) <= 256, "DIRECT_CLOCK_TIME_OFFSETS");
        const offsets = snapshot.timeOffsets
            .trim()
            .split("\n")
            .map((s) => s.trim().split(/\s+/));
        need(
            offsets.length === 2 &&
                offsets[0][0] === "monotonic" &&
                offsets[1][0] === "boottime" &&
                offsets.every(
                    (line) =>
                        line.length === 3 &&
                        /^-?\d{1,12}$/.test(line[1]) &&
                        /^\d{1,9}$/.test(line[2]) &&
                        Number(line[2]) <= 999999999,
                ),
            "DIRECT_CLOCK_TIME_OFFSETS",
        );
    }
    if (snapshot.availability === "available")
        need(
            !snapshot.reasons.length &&
                [
                    snapshot.statRaw,
                    snapshot.statusRaw,
                    snapshot.bootId,
                    snapshot.startTimeTicks,
                    snapshot.timeOffsets,
                    ...Object.values(snapshot.namespaces),
                ].every((v) => v !== null),
            "DIRECT_CLOCK_PROCESS_IDENTITY",
        );
    else need(snapshot.reasons.length > 0, "DIRECT_CLOCK_PROCESS_IDENTITY");
}
function metadata(stat) {
    return {
        bytes: Number(stat.size),
        device: stat.dev.toString(),
        inode: stat.ino.toString(),
        mtimeNs: stat.mtimeNs.toString(),
        ctimeNs: stat.ctimeNs.toString(),
    };
}
async function fileIdentity(path, hashContent) {
    const f = await open(path, constants.O_RDONLY | constants.O_NONBLOCK);
    try {
        const before = await f.stat({ bigint: true });
        need(before.isFile() && before.size <= 256n * 1024n * 1024n, "DIRECT_CLOCK_RUNTIME_BUDGET");
        let sha256;
        if (hashContent) {
            const hash = createHash("sha256"),
                buffer = Buffer.alloc(1024 * 1024);
            let count = 0;
            while (true) {
                const { bytesRead } = await f.read(buffer, 0, buffer.length, count);
                if (!bytesRead) break;
                count += bytesRead;
                need(count <= Number(before.size), "DIRECT_CLOCK_RUNTIME_DRIFT");
                hash.update(buffer.subarray(0, bytesRead));
            }
            need(count === Number(before.size), "DIRECT_CLOCK_RUNTIME_DRIFT");
            sha256 = hash.digest("hex");
        }
        same(metadata(before), metadata(await f.stat({ bigint: true })), "DIRECT_CLOCK_RUNTIME_DRIFT");
        return { path, ...metadata(before), ...(hashContent ? { sha256 } : {}) };
    } finally {
        await f.close();
    }
}
function runtimeFlags() {
    return {
        node: process.version,
        uv: process.versions.uv,
        platform: process.platform,
        arch: process.arch,
        nodeSharedLibuv: process.config.variables.node_shared_libuv ?? null,
        injectionEnvironmentPresent: Object.fromEntries(
            ["LD_PRELOAD", "LD_AUDIT", "LD_LIBRARY_PATH", "NODE_OPTIONS"].map((key) => [
                key,
                Boolean(process.env[key]),
            ]),
        ),
        execArgvSha256: createHash("sha256").update(JSON.stringify(process.execArgv)).digest("hex"),
    };
}
function contractCandidate(runtime, identity) {
    return runtime.node === "v22.14.0" &&
        runtime.uv === "1.49.2" &&
        runtime.platform === "linux" &&
        runtime.arch === "x64" &&
        runtime.nodeSharedLibuv === false &&
        runtime.files?.executable.sha256 === OFFICIAL_BINARY &&
        runtime.files?.executable.bytes === 120177224 &&
        !Object.values(runtime.injectionEnvironmentPresent).some(Boolean) &&
        identity.availability === "available"
        ? CONTRACT
        : null;
}
function validateRuntime(runtime) {
    const sha = /^[a-f0-9]{64}$/;
    need(
        runtime &&
            typeof runtime.node === "string" &&
            /^v\d+\.\d+\.\d+/.test(runtime.node) &&
            typeof runtime.uv === "string" &&
            runtime.uv.length <= 64 &&
            sha.test(runtime.execArgvSha256),
        "DIRECT_CLOCK_RUNTIME_IDENTITY",
    );
    same(
        Object.keys(runtime.injectionEnvironmentPresent).sort(),
        ["LD_PRELOAD", "LD_AUDIT", "LD_LIBRARY_PATH", "NODE_OPTIONS"].sort(),
        "DIRECT_CLOCK_RUNTIME_IDENTITY",
    );
    need(
        Object.values(runtime.injectionEnvironmentPresent).every((v) => typeof v === "boolean"),
        "DIRECT_CLOCK_RUNTIME_IDENTITY",
    );
    if (runtime.files === null) {
        need(
            typeof runtime.unavailable === "string" && runtime.unavailable.length <= 128,
            "DIRECT_CLOCK_RUNTIME_IDENTITY",
        );
        return;
    }
    need(
        runtime.unavailable === null && runtime.files?.executable && runtime.files?.component,
        "DIRECT_CLOCK_RUNTIME_IDENTITY",
    );
    for (const file of [runtime.files.executable, runtime.files.component]) {
        need(
            typeof file.path === "string" &&
                file.path.startsWith("/") &&
                file.path.length <= 4096 &&
                Number.isSafeInteger(file.bytes) &&
                file.bytes > 0 &&
                file.bytes <= 256 * 1024 * 1024 &&
                sha.test(file.sha256),
            "DIRECT_CLOCK_RUNTIME_IDENTITY",
        );
        for (const name of ["device", "inode", "mtimeNs", "ctimeNs"]) decimal(file[name]);
    }
    need(
        typeof runtime.files.executable.reportedPath === "string" &&
            runtime.files.executable.reportedPath.startsWith("/"),
        "DIRECT_CLOCK_RUNTIME_IDENTITY",
    );
}
async function runtimeIdentity() {
    if (!cachedRuntime)
        cachedRuntime = (async () => {
            const executableSource = process.platform === "linux" ? "/proc/self/exe" : process.execPath;
            const [executable, component] = await Promise.all([
                fileIdentity(executableSource, true),
                fileIdentity(HERE, true),
            ]);
            return { executable: { ...executable, reportedPath: process.execPath }, component };
        })().catch((error) => {
            cachedRuntime = undefined;
            throw error;
        });
    const saved = await cachedRuntime;
    for (const info of [saved.executable, saved.component]) {
        const { sha256, reportedPath, ...expected } = info;
        same(await fileIdentity(info.path, false), expected, "DIRECT_CLOCK_RUNTIME_DRIFT");
    }
    return structuredClone(saved);
}
function binding(input) {
    return {
        id: input.id ?? null,
        caseId: input.caseId ?? null,
        loadId: input.loadId ?? null,
        repeat: input.repeat ?? null,
        probePhase: input.probePhase ?? null,
    };
}
export async function prepareDirectPhaseClock(input, { signal } = {}) {
    const preparedStart = hrtime(),
        identityBefore = processSnapshot(),
        flags = runtimeFlags();
    let files = null,
        unavailable = null;
    try {
        files = await runtimeIdentity();
    } catch (error) {
        if (error.code === "DIRECT_CLOCK_RUNTIME_DRIFT") throw error;
        unavailable = error.code || "runtime-file-unavailable";
    }
    const handle = Object.freeze({});
    const state = {
        sessionId: randomBytes(16).toString("hex"),
        runIdentity: binding(input),
        identityBefore,
        flags,
        files,
        unavailable,
        preparedIntervalNs: null,
        finished: false,
        reference: null,
        referenceRaw: undefined,
        referenceMarker: undefined,
    };
    handles.set(handle, state);
    if (input.clockReference !== undefined) {
        try {
            const config = input.clockReference;
            same(Object.keys(config).sort(), ["python", "spec"], "DIRECT_CLOCK_REFERENCE_CONFIG");
            need(/^[a-f0-9]{64}$/.test(input.profileDigest), "DIRECT_CLOCK_PROFILE_DIGEST");
            state.referenceMarker = {
                required: true,
                profileDigest: input.profileDigest,
                specDigest: valueDigest(config.spec),
                maxEvidenceBytes: runnerSanityEvidenceBudget(config.spec),
            };
            state.reference = await startRunnerSanity({
                python: config.python,
                spec: config.spec,
                binding: {
                    sessionId: state.sessionId,
                    runIdentity: state.runIdentity,
                    profileDigest: input.profileDigest,
                },
                signal,
            });
            await captureRunnerSanity(state.reference, "pre");
        } catch (error) {
            state.finished = true;
            state.referenceRaw = error.evidence;
            if (state.reference && !state.referenceRaw)
                state.referenceRaw = await stopRunnerSanity(state.reference, error.code || "DIRECT_CLOCK_PREPARE");
            error.clockReference = state.referenceRaw;
            throw error;
        }
    }
    state.preparedIntervalNs = [preparedStart.toString(), hrtime().toString()];
    return handle;
}

// Only these three clock reads occur at each existing phase boundary. No /proc, hashes, JSON or IPC here.
export function captureDirectPhaseBoundary(handle) {
    const state = handles.get(handle);
    need(state && !state.finished, "DIRECT_CLOCK_CONTEXT");
    const hrtimeBeforeNs = hrtime(),
        performanceMs = now(),
        hrtimeAfterNs = hrtime();
    return Object.freeze({ hrtimeBeforeNs, performanceMs, hrtimeAfterNs });
}
export function recordDirectPhaseClock(handle, name, start, end, elapsedMs) {
    const state = handles.get(handle);
    need(state && !state.finished, "DIRECT_CLOCK_CONTEXT");
    const wire = (value) => ({
        hrtimeBeforeNs: value.hrtimeBeforeNs.toString(),
        performanceMs: value.performanceMs,
        hrtimeAfterNs: value.hrtimeAfterNs.toString(),
    });
    const clock = {
        schemaVersion: 1,
        kind: "http-direct-phase-clock",
        status: "raw-unverified",
        verified: false,
        sessionId: state.sessionId,
        phase: name,
        windowSemantics: "actual-phase-including-drain",
        start: wire(start),
        end: wire(end),
    };
    validatePair(clock, elapsedMs);
    return clock;
}
function validatePair(clock, elapsedMs) {
    need(
        clock.schemaVersion === 1 &&
            clock.kind === "http-direct-phase-clock" &&
            clock.status === "raw-unverified" &&
            clock.verified === false &&
            clock.windowSemantics === "actual-phase-including-drain",
        "DIRECT_CLOCK_RAW_KIND",
    );
    for (const boundary of [clock.start, clock.end]) {
        need(
            Number.isFinite(boundary.performanceMs) &&
                boundary.performanceMs >= 0 &&
                decimal(boundary.hrtimeBeforeNs) <= decimal(boundary.hrtimeAfterNs),
            "DIRECT_CLOCK_BOUNDARY_ORDER",
        );
    }
    need(
        decimal(clock.start.hrtimeAfterNs) <= decimal(clock.end.hrtimeBeforeNs) &&
            clock.start.performanceMs <= clock.end.performanceMs,
        "DIRECT_CLOCK_PHASE_ORDER",
    );
    need(
        Number.isFinite(elapsedMs) &&
            elapsedMs >= 0 &&
            elapsedMs === clock.end.performanceMs - clock.start.performanceMs,
        "DIRECT_CLOCK_ELAPSED_MISMATCH",
    );
}
export async function finishDirectPhaseClock(handle, phases) {
    const state = handles.get(handle);
    need(state && !state.finished, "DIRECT_CLOCK_CONTEXT");
    state.finished = true;
    const finishStart = hrtime();
    if (state.reference) {
        try {
            await captureRunnerSanity(state.reference, "post");
            state.referenceRaw = await finishRunnerSanity(state.reference);
        } catch (error) {
            state.referenceRaw = error.evidence;
            error.clockReference = state.referenceRaw;
            throw error;
        }
    }
    const identityAfter = processSnapshot();
    same(invariant(identityAfter), invariant(state.identityBefore), "DIRECT_CLOCK_IDENTITY_DRIFT");
    same(runtimeFlags(), state.flags, "DIRECT_CLOCK_RUNTIME_DRIFT");
    if (state.files) same(await runtimeIdentity(), state.files, "DIRECT_CLOCK_RUNTIME_DRIFT");
    const runtime = { ...state.flags, files: state.files, unavailable: state.unavailable };
    const session = {
        schemaVersion: 1,
        kind: "http-direct-phase-clock-session",
        status: state.files && identityAfter.availability === "available" ? "raw-unverified" : "unavailable",
        verified: false,
        sessionId: state.sessionId,
        runIdentity: state.runIdentity,
        identityBefore: state.identityBefore,
        identityAfter,
        runtime,
        preparedIntervalNs: state.preparedIntervalNs,
        finishedIntervalNs: [finishStart.toString(), hrtime().toString()],
        contractCandidate: contractCandidate(runtime, identityAfter),
        pendingGates: [...PENDING],
        maxEvidenceBytes: MAX_BYTES,
        performanceAccepted: false,
        ...(state.referenceMarker ? { clockReference: state.referenceMarker } : {}),
    };
    const clockPhases = phases.map((phase) => phase.clock);
    const evidenceBytes = Buffer.byteLength(JSON.stringify({ session, phases: clockPhases }));
    need(evidenceBytes <= MAX_BYTES, "DIRECT_CLOCK_EVIDENCE_BUDGET");
    return { ...session, evidenceBytes };
}

export function getDirectPhaseClockReferenceRaw(handle) {
    const state = handles.get(handle);
    need(state && state.finished, "DIRECT_CLOCK_CONTEXT");
    return state.referenceRaw === undefined ? undefined : structuredClone(state.referenceRaw);
}

export async function stopDirectPhaseClock(handle, reason = "DIRECT_CLOCK_CALLER_STOPPED") {
    const state = handles.get(handle);
    need(state, "DIRECT_CLOCK_CONTEXT");
    state.finished = true;
    if (state.reference && state.referenceRaw === undefined)
        state.referenceRaw = await stopRunnerSanity(state.reference, reason);
    return getDirectPhaseClockReferenceRaw(handle);
}

function validateReference(run, session, profile) {
    const marker = session.clockReference,
        raw = run.clockReference,
        expected = profile?.clockReference;
    if (profile !== undefined && expected === undefined)
        need(marker === undefined && raw === undefined, "DIRECT_CLOCK_REFERENCE_UNEXPECTED");
    if (expected === undefined && marker === undefined && raw === undefined) return undefined;
    need(marker && raw, "DIRECT_CLOCK_REFERENCE_REQUIRED");
    same(
        Object.keys(marker).sort(),
        ["required", "profileDigest", "specDigest", "maxEvidenceBytes"].sort(),
        "DIRECT_CLOCK_REFERENCE_MARKER",
    );
    need(marker.required === true, "DIRECT_CLOCK_REFERENCE_MARKER");
    const checked = verifyRunnerSanityRaw(raw);
    need(
        marker.maxEvidenceBytes === runnerSanityEvidenceBudget(raw.spec) &&
            marker.profileDigest === raw.binding.profileDigest &&
            marker.specDigest === valueDigest(raw.spec),
        "DIRECT_CLOCK_REFERENCE_MARKER",
    );
    if (expected !== undefined) {
        need(
            marker.profileDigest === valueDigest(profile) && marker.specDigest === valueDigest(expected.spec),
            "DIRECT_CLOCK_REFERENCE_PROFILE",
        );
        same(raw.spec, expected.spec, "DIRECT_CLOCK_REFERENCE_PROFILE");
    }
    same(raw.binding.sessionId, session.sessionId, "DIRECT_CLOCK_REFERENCE_SESSION");
    same(raw.binding.runIdentity, session.runIdentity, "DIRECT_CLOCK_REFERENCE_RUN");
    same(raw.callerPid, session.identityBefore.pid, "DIRECT_CLOCK_REFERENCE_PID");
    const pre = JSON.parse(raw.frames[4].raw).anchor,
        post = JSON.parse(raw.frames[8].raw).anchor,
        identity = pre.identityBefore;
    same(identity.platform, session.runtime.platform, "DIRECT_CLOCK_REFERENCE_RUNTIME");
    same(identity.runtime.version, session.runtime.node, "DIRECT_CLOCK_REFERENCE_RUNTIME");
    same(identity.runtime.uv, session.runtime.uv, "DIRECT_CLOCK_REFERENCE_RUNTIME");
    if (session.runtime.files) {
        same(
            identity.runtime.executableSha256,
            session.runtime.files.executable.sha256,
            "DIRECT_CLOCK_REFERENCE_RUNTIME",
        );
        same(
            identity.runtime.executable,
            session.runtime.files.executable.reportedPath,
            "DIRECT_CLOCK_REFERENCE_RUNTIME",
        );
    }
    if (raw.sourceOrigin === "linux-procfs") {
        for (const [direct, anchor] of [
            [session.identityBefore.bootId, identity.bootId],
            [session.identityBefore.startTimeTicks, identity.startTimeTicks],
            [session.identityBefore.namespaces.time, identity.timeNamespace],
            [session.identityBefore.namespaces.time_for_children, identity.timeForChildrenNamespace],
            [session.identityBefore.timeOffsets, identity.timeOffsets],
        ])
            if (direct !== null) same(direct, anchor, "DIRECT_CLOCK_REFERENCE_IDENTITY");
    }
    need(
        decimal(session.preparedIntervalNs[0]) <= decimal(pre.hrtimeBeforeNs) &&
            decimal(pre.hrtimeAfterNs) <= decimal(session.preparedIntervalNs[1]) &&
            decimal(session.finishedIntervalNs[0]) <= decimal(post.hrtimeBeforeNs) &&
            decimal(post.hrtimeAfterNs) <= decimal(session.finishedIntervalNs[1]) &&
            pre.performanceMs <= run.phases[0].clock.start.performanceMs &&
            run.phases.at(-1).clock.end.performanceMs <= post.performanceMs,
        "DIRECT_CLOCK_REFERENCE_ENCLOSURE",
    );
    return checked;
}

// Raw consistency only. This function deliberately cannot certify host/collector clock equality.
export function validateDirectPhaseClockRaw(run, profile) {
    if (run.phaseClock === undefined) {
        need(
            profile?.clockReference === undefined && run.clockReference === undefined,
            "DIRECT_CLOCK_REFERENCE_REQUIRED",
        );
        need(
            Array.isArray(run.phases) && run.phases.every((phase) => phase.clock === undefined),
            "DIRECT_CLOCK_MISSING_SESSION",
        );
        return {
            available: false,
            rawValidated: false,
            verified: false,
            reason: "legacy-no-phase-clock",
            hostAssociation: "unavailable",
            performanceAccepted: false,
        };
    }
    const session = run.phaseClock;
    need(
        session.schemaVersion === 1 &&
            session.kind === "http-direct-phase-clock-session" &&
            ["raw-unverified", "unavailable"].includes(session.status) &&
            session.verified === false &&
            session.performanceAccepted === false &&
            /^[a-f0-9]{32}$/.test(session.sessionId),
        "DIRECT_CLOCK_SESSION_KIND",
    );
    same(session.pendingGates, PENDING, "DIRECT_CLOCK_PENDING_GATES");
    validateSnapshot(session.identityBefore);
    validateSnapshot(session.identityAfter);
    validateRuntime(session.runtime);
    same(session.runtime.platform, session.identityBefore.platform, "DIRECT_CLOCK_RUNTIME_IDENTITY");
    same(session.runtime.arch, session.identityBefore.arch, "DIRECT_CLOCK_RUNTIME_IDENTITY");
    same(
        session.contractCandidate,
        contractCandidate(session.runtime, session.identityAfter),
        "DIRECT_CLOCK_CONTRACT_CANDIDATE",
    );
    need(
        session.status ===
            (session.runtime.files && session.identityAfter.availability === "available"
                ? "raw-unverified"
                : "unavailable"),
        "DIRECT_CLOCK_AVAILABILITY",
    );
    same(
        session.runIdentity,
        binding({
            id: run.id,
            caseId: run.caseId,
            loadId: run.loadId,
            repeat: run.repeat,
            probePhase: run.kind === "http-functional-probe" ? run.phases[0]?.name : null,
        }),
        "DIRECT_CLOCK_RUN_BINDING",
    );
    same(invariant(session.identityBefore), invariant(session.identityAfter), "DIRECT_CLOCK_IDENTITY_DRIFT");
    need(Array.isArray(run.phases) && run.phases.length >= 1 && run.phases.length <= 2, "DIRECT_CLOCK_PHASES");
    const preparation = session.preparedIntervalNs.map(decimal),
        finish = session.finishedIntervalNs.map(decimal);
    need(
        preparation.length === 2 && finish.length === 2 && preparation[0] <= preparation[1] && finish[0] <= finish[1],
        "DIRECT_CLOCK_SESSION_ORDER",
    );
    let previous = preparation[1];
    const boundaries = [];
    for (const phase of run.phases) {
        const clock = phase.clock;
        need(
            clock && clock.sessionId === session.sessionId && clock.phase === phase.name,
            "DIRECT_CLOCK_PHASE_BINDING",
        );
        validatePair(clock, phase.elapsedMs);
        need(
            previous <= decimal(clock.start.hrtimeBeforeNs) && decimal(clock.end.hrtimeAfterNs) <= finish[0],
            "DIRECT_CLOCK_SESSION_ORDER",
        );
        previous = decimal(clock.end.hrtimeAfterNs);
        boundaries.push({
            phase: phase.name,
            startBracketWidthNs: (decimal(clock.start.hrtimeAfterNs) - decimal(clock.start.hrtimeBeforeNs)).toString(),
            endBracketWidthNs: (decimal(clock.end.hrtimeAfterNs) - decimal(clock.end.hrtimeBeforeNs)).toString(),
        });
    }
    const { evidenceBytes, ...bare } = session;
    const bytes = Buffer.byteLength(JSON.stringify({ session: bare, phases: run.phases.map((p) => p.clock) }));
    need(
        session.maxEvidenceBytes === MAX_BYTES && evidenceBytes === bytes && bytes <= MAX_BYTES,
        "DIRECT_CLOCK_EVIDENCE_BUDGET",
    );
    const reference = validateReference(run, session, profile);
    return {
        available: true,
        rawValidated: true,
        verified: false,
        hostAssociation: "unverified",
        pendingGates: [...PENDING],
        boundaries,
        performanceAccepted: false,
        ...(reference ? { clockReference: reference } : {}),
    };
}
