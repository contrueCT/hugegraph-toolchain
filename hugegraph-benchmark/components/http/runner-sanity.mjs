/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
import { spawn } from "node:child_process";
import { readFile, stat, realpath, open } from "node:fs/promises";
import { constants } from "node:fs";
import { createHash } from "node:crypto";
import { isDeepStrictEqual } from "node:util";
import { performance } from "node:perf_hooks";
import { fileURLToPath } from "node:url";
import { isAbsolute, resolve } from "node:path";
import { createClockContext, captureAnchor } from "./clock-correlation.mjs";

const HERE = fileURLToPath(import.meta.url),
    PEER = fileURLToPath(new URL("./phase-clock-reference.py", import.meta.url)),
    ANCHOR = fileURLToPath(new URL("./clock-correlation.mjs", import.meta.url)),
    sessions = new WeakMap();
const sha = (raw) => createHash("sha256").update(raw).digest("hex");
const PENDING = ["external-runtime-source-lock", "host-collector-namespace-binding", "predeclared-quality-thresholds"];
export class RunnerSanityError extends Error {
    constructor(code) {
        super(code);
        this.code = code;
    }
}
function need(value, code) {
    if (!value) throw new RunnerSanityError(code);
}
function same(a, b, code) {
    need(isDeepStrictEqual(a, b), code);
}
function uint(v) {
    need(typeof v === "string" && /^(0|[1-9][0-9]{0,24})$/.test(v), "SR_INTEGER_PRECISION");
    return BigInt(v);
}
function parseRaw(raw) {
    const text = new TextDecoder("utf-8", { fatal: true }).decode(raw);
    const value = JSON.parse(text, (_key, v) => {
        need(typeof v !== "number" || Number.isFinite(v), "SR_NONFINITE_JSON");
        return v;
    });
    // JSON.parse validates grammar; this bounded token walk additionally rejects duplicate object keys.
    const tokens = /\s*("(?:\\.|[^"\\])*"|[{}\[\],:]|-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?|true|false|null)/gy;
    const stack = [];
    let match;
    while ((match = tokens.exec(text))) {
        const token = match[1],
            top = stack.at(-1);
        if (token === "{") stack.push({ keys: new Set(), wantKey: true });
        else if (token === "[") stack.push(null);
        else if (token === "}" || token === "]") stack.pop();
        else if (token === "," && top) top.wantKey = true;
        else if (token.startsWith('"') && top?.wantKey) {
            const key = JSON.parse(token);
            need(!top.keys.has(key), "SR_DUPLICATE_JSON_KEY");
            top.keys.add(key);
            top.wantKey = false;
        }
        need(stack.length <= 32, "SR_JSON_DEPTH");
    }
    return value;
}
export function validateRunnerSanitySpec(s) {
    same(
        Object.keys(s).sort(),
        [
            "schemaVersion",
            "kind",
            "sourceOrigin",
            "startupMs",
            "exchangeMs",
            "finishMs",
            "wallMs",
            "maxFrameBytes",
            "maxRawBytes",
            "maxStderrBytes",
        ].sort(),
        "SR_SPEC_FIELDS",
    );
    need(s.schemaVersion === 1 && s.kind === "http-runner-sanity-spec", "SR_SPEC_KIND");
    need(["fixture", "linux-procfs"].includes(s.sourceOrigin), "SR_SOURCE_ORIGIN");
    for (const [key, low, high] of [
        ["startupMs", 100, 10000],
        ["exchangeMs", 100, 10000],
        ["finishMs", 50, 2000],
        ["wallMs", 1000, 600000],
        ["maxFrameBytes", 4096, 16384],
        ["maxRawBytes", 16384, 262144],
        ["maxStderrBytes", 0, 4096],
    ])
        need(Number.isSafeInteger(s[key]) && s[key] >= low && s[key] <= high, "SR_SPEC_BUDGET");
    need(
        s.startupMs + 2 * s.exchangeMs + s.finishMs <= s.wallMs && 4 * s.maxFrameBytes <= s.maxRawBytes,
        "SR_SPEC_BUDGET",
    );
    return s;
}
export function runnerSanityEvidenceBudget(spec) {
    return 4 * validateRunnerSanitySpec(spec).maxRawBytes + 65536;
}
function validateBinding(b) {
    same(Object.keys(b).sort(), ["sessionId", "runIdentity", "profileDigest"].sort(), "SR_BINDING_FIELDS");
    need(/^[a-f0-9]{32}$/.test(b.sessionId) && /^[a-f0-9]{64}$/.test(b.profileDigest), "SR_BINDING");
    same(Object.keys(b.runIdentity).sort(), ["id", "caseId", "loadId", "repeat", "probePhase"].sort(), "SR_BINDING");
    for (const key of ["id", "caseId", "loadId"])
        need(
            typeof b.runIdentity[key] === "string" && /^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$/.test(b.runIdentity[key]),
            "SR_BINDING",
        );
    need(
        Number.isSafeInteger(b.runIdentity.repeat) &&
            b.runIdentity.repeat >= 0 &&
            b.runIdentity.repeat <= 20 &&
            [null, "preflight", "postcheck"].includes(b.runIdentity.probePhase),
        "SR_BINDING",
    );
}
function meta(s) {
    return {
        bytes: s.size.toString(),
        device: s.dev.toString(),
        inode: s.ino.toString(),
        mtimeNs: s.mtimeNs.toString(),
        ctimeNs: s.ctimeNs.toString(),
    };
}
async function source(path, hash = true) {
    const before = await stat(path, { bigint: true });
    need(before.isFile() && before.size > 0n && before.size <= 1048576n, "SR_SOURCE_BUDGET");
    const value = { path, metadata: meta(before) };
    if (hash) value.sha256 = sha(await readFile(path));
    same(meta(before), meta(await stat(path, { bigint: true })), "SR_SOURCE_DRIFT");
    return value;
}
function rawRecord(direction, raw) {
    return { direction, raw: raw.toString("utf8"), bytes: raw.length, sha256: sha(raw) };
}
function frameFields(value) {
    const fields = {
        init: ["kind", "binding"],
        ready: ["kind", "binding", "identity", "files"],
        sample: ["kind", "binding", "stage"],
        challenge: ["kind", "binding", "stage", "sequence", "nonce"],
        anchor: ["kind", "binding", "stage", "sequence", "nonce", "anchor"],
        receipt: [
            "kind",
            "binding",
            "stage",
            "sequence",
            "sentMonotonicNs",
            "receivedMonotonicNs",
            "challengeRaw",
            "responseRaw",
            "challengeSha256",
            "responseSha256",
        ],
        finish: ["kind", "binding"],
        finished: ["kind", "binding", "identity", "files", "performanceAccepted"],
    };
    need(value && typeof value === "object" && !Array.isArray(value) && fields[value.kind], "SR_FRAME_KIND");
    same(Object.keys(value).sort(), fields[value.kind].sort(), "SR_FRAME_FIELDS");
}
function state(handle) {
    const s = sessions.get(handle);
    need(s && !s.finished, "SR_CONTEXT");
    return s;
}
function snapshot(s) {
    const e = {
        schemaVersion: 1,
        kind: "http-runner-sanity-raw",
        status: s.success ? "completed" : "partial",
        verified: false,
        performanceAccepted: false,
        sourceOrigin: s.spec.sourceOrigin,
        spec: s.spec,
        binding: s.binding,
        callerPid: process.pid,
        peerPid: s.child.pid ?? null,
        clientSources: s.sources,
        frames: s.frames,
        partialStdout: s.buffer.toString("utf8"),
        stderr: s.stderr.toString("utf8"),
        stderrBytes: s.stderrActualBytes,
        stderrRetainedBytes: s.stderr.length,
        stderrSha256: sha(s.stderr),
        wireBytes: s.wireBytes,
        failure: s.failure,
        terminal: { ...s.terminal },
        pendingGates: [...PENDING],
    };
    need(Buffer.byteLength(JSON.stringify(e)) <= s.spec.maxRawBytes * 4 + 65536, "SR_EVIDENCE_BUDGET");
    return e;
}
function fail(s, code) {
    if (!s.failure) s.failure = code;
    if (s.waiter) {
        const reject = s.waiter.reject;
        clearTimeout(s.waiter.timer);
        s.waiter = null;
        reject(new RunnerSanityError(s.failure));
    }
    if (!s.terminal.closed && !s.terminal.termSent) s.terminal.termSent = s.child.kill("SIGTERM");
}
function deliver(s) {
    if (!s.waiter || !s.queue.length) return;
    const waiter = s.waiter;
    s.waiter = null;
    clearTimeout(waiter.timer);
    waiter.resolve(s.queue.shift());
}
async function receive(s, kind, ms) {
    if (s.failure) throw new RunnerSanityError(s.failure);
    const remaining = Math.min(ms, s.deadline - performance.now());
    need(remaining > 0, "SR_DEADLINE");
    const raw = s.queue.length
        ? s.queue.shift()
        : await new Promise((resolve, reject) => {
              s.waiter = { resolve, reject, timer: setTimeout(() => fail(s, "SR_DEADLINE"), remaining) };
              if (s.terminal.closed) fail(s, "SR_PEER_EOF");
          });
    let value;
    try {
        value = parseRaw(raw);
    } catch (error) {
        throw error instanceof RunnerSanityError ? error : new RunnerSanityError("SR_INVALID_JSON");
    }
    need(value.kind === kind, "SR_FRAME_KIND");
    frameFields(value);
    same(value.binding, s.binding, "SR_FRAME_BINDING");
    return value;
}
async function send(s, value) {
    need(!s.failure && performance.now() < s.deadline, s.failure || "SR_DEADLINE");
    const raw = Buffer.from(JSON.stringify(value) + "\n");
    need(raw.length <= s.spec.maxFrameBytes, "SR_FRAME_BYTE_BUDGET");
    s.wireBytes += raw.length;
    need(s.wireBytes <= s.spec.maxRawBytes, "SR_RAW_BYTE_BUDGET");
    const record = rawRecord("node-to-python", raw);
    record.writeCompleted = false;
    s.frames.push(record);
    await new Promise((resolve, reject) =>
        s.child.stdin.write(raw, (error) => {
            if (error) reject(new RunnerSanityError("SR_INPUT_WRITE"));
            else {
                record.writeCompleted = true;
                resolve();
            }
        }),
    );
}
async function cleanupOnce(s, normal = false) {
    s.child.stdin.end();
    if (!normal && !s.terminal.closed && !s.terminal.termSent) {
        s.terminal.termSent = s.child.kill("SIGTERM");
    }
    const terminate = setTimeout(
        () => {
            if (!s.terminal.closed) {
                s.terminal.killSent = s.child.kill("SIGKILL");
                s.terminal.forced = true;
            }
        },
        Math.max(25, Math.floor(s.spec.finishMs / 2)),
    );
    let cleanupTimer;
    const end = new Promise((resolve) => {
        cleanupTimer = setTimeout(() => {
            if (!s.terminal.closed) {
                s.child.stdin.destroy();
                s.child.stdout.destroy();
                s.child.stderr.destroy();
                fail(s, "SR_CLEANUP_DEADLINE");
            }
            resolve();
        }, s.spec.finishMs + 100);
    });
    await Promise.race([s.closed, end]);
    clearTimeout(terminate);
    clearTimeout(cleanupTimer);
    clearTimeout(s.wallTimer);
    s.signal?.removeEventListener("abort", s.onAbort);
    if (s.waiter) {
        clearTimeout(s.waiter.timer);
        s.waiter = null;
    }
}
function cleanup(s, normal = false) {
    if (!s.cleanupPromise) s.cleanupPromise = cleanupOnce(s, normal);
    return s.cleanupPromise;
}
export async function startRunnerSanity({ python, spec, binding, signal }) {
    spec = structuredClone(validateRunnerSanitySpec(spec));
    binding = structuredClone(binding);
    validateBinding(binding);
    need(signal === undefined || signal instanceof AbortSignal, "SR_ABORT_SIGNAL");
    need(!signal?.aborted, "SR_CALLER_CANCELLED");
    need(typeof python === "string" && isAbsolute(python), "SR_PYTHON_PATH");
    python = await realpath(python);
    const sources = await Promise.all([source(HERE), source(PEER), source(ANCHOR)]);
    const context = createClockContext({ sourceOrigin: spec.sourceOrigin, sessionId: binding.sessionId });
    need(!signal?.aborted, "SR_CALLER_CANCELLED");
    const child = spawn(python, ["-I", "-S", "-B", PEER, "--spec-json", JSON.stringify(spec)], {
        stdio: ["pipe", "pipe", "pipe"],
        env: { LC_ALL: "C", LANG: "C" },
        detached: false,
    });
    const s = {
        spec,
        binding,
        context,
        child,
        sources,
        signal,
        stage: 0,
        finished: false,
        success: false,
        failure: null,
        deadline: performance.now() + spec.wallMs,
        frames: [],
        buffer: Buffer.alloc(0),
        stderr: Buffer.alloc(0),
        stderrActualBytes: 0,
        queue: [],
        waiter: null,
        wireBytes: 0,
        terminal: {
            closed: false,
            stdoutEof: false,
            stderrEof: false,
            exitCode: null,
            signal: null,
            termSent: false,
            killSent: false,
            forced: false,
        },
    };
    const handle = Object.freeze({});
    sessions.set(handle, s);
    s.closed = new Promise((resolve) =>
        child.once("close", (code, signal) => {
            Object.assign(s.terminal, { closed: true, exitCode: code, signal });
            if (s.waiter && !s.queue.length) fail(s, "SR_PEER_EOF");
            resolve();
        }),
    );
    s.wallTimer = setTimeout(() => {
        fail(s, "SR_WALL_DEADLINE");
        void cleanup(s).catch(() => fail(s, "SR_CLEANUP_DEADLINE"));
    }, spec.wallMs);
    s.onAbort = () => {
        fail(s, "SR_CALLER_CANCELLED");
        void cleanup(s).catch(() => fail(s, "SR_CLEANUP_DEADLINE"));
    };
    signal?.addEventListener("abort", s.onAbort, { once: true });
    child.once("error", () => fail(s, "SR_PEER_START"));
    child.stdin.on("error", () => fail(s, "SR_INPUT_WRITE"));
    child.stdout.once("end", () => {
        s.terminal.stdoutEof = true;
    });
    child.stderr.once("end", () => {
        s.terminal.stderrEof = true;
    });
    child.stderr.on("data", (raw) => {
        s.stderrActualBytes += raw.length;
        const room = spec.maxStderrBytes - s.stderr.length;
        s.stderr = Buffer.concat([s.stderr, raw.subarray(0, Math.max(0, room))]);
        s.wireBytes += raw.length;
        if (raw.length > room) fail(s, "SR_STDERR_BUDGET");
        if (s.wireBytes > spec.maxRawBytes) fail(s, "SR_RAW_BYTE_BUDGET");
    });
    child.stdout.on("data", (raw) => {
        s.wireBytes += raw.length;
        if (s.wireBytes > spec.maxRawBytes) {
            fail(s, "SR_RAW_BYTE_BUDGET");
            return;
        }
        s.buffer = Buffer.concat([s.buffer, raw]);
        if (s.buffer.length > spec.maxFrameBytes) {
            s.buffer = s.buffer.subarray(0, spec.maxFrameBytes);
            fail(s, "SR_FRAME_BYTE_BUDGET");
            return;
        }
        let at;
        while ((at = s.buffer.indexOf(10)) !== -1) {
            const frame = s.buffer.subarray(0, at + 1);
            s.buffer = s.buffer.subarray(at + 1);
            if (s.queue.length) {
                fail(s, "SR_UNSOLICITED_OUTPUT");
                return;
            }
            s.frames.push(rawRecord("python-to-node", frame));
            s.queue.push(frame);
            deliver(s);
        }
    });
    try {
        await send(s, { kind: "init", binding });
        const ready = await receive(s, "ready", spec.startupMs);
        need(ready.identity.pid === child.pid && ready.identity.parentPid === process.pid, "SR_REFERENCE_PARENT");
        return handle;
    } catch (error) {
        fail(s, error.code || "SR_START_FAILED");
        await cleanup(s);
        s.finished = true;
        error.evidence = snapshot(s);
        throw error;
    }
}
export async function captureRunnerSanity(handle, stage) {
    const s = state(handle);
    try {
        need(stage === ["pre", "post"][s.stage], "SR_STAGE");
        await send(s, { kind: "sample", binding: s.binding, stage });
        const challenge = await receive(s, "challenge", s.spec.exchangeMs);
        need(
            challenge.sequence === s.stage && /^[a-f0-9]{32}$/.test(challenge.nonce) && challenge.stage === stage,
            "SR_CHALLENGE",
        );
        // H is always taken in this caller process. No supplied anchor or alternate Node subprocess.
        const anchor = captureAnchor(s.context, { sequence: s.stage, nonce: challenge.nonce });
        await send(s, { kind: "anchor", binding: s.binding, stage, sequence: s.stage, nonce: challenge.nonce, anchor });
        const receipt = await receive(s, "receipt", s.spec.exchangeMs);
        need(receipt.stage === stage && receipt.sequence === s.stage, "SR_RECEIPT");
        s.stage++;
        return structuredClone(receipt);
    } catch (error) {
        fail(s, error.code || "SR_EXCHANGE_FAILED");
        await cleanup(s);
        s.finished = true;
        error.evidence = snapshot(s);
        throw error;
    }
}
export async function finishRunnerSanity(handle) {
    const s = state(handle);
    try {
        need(s.stage === 2, "SR_MISSING_EXCHANGE");
        await send(s, { kind: "finish", binding: s.binding });
        await receive(s, "finished", s.spec.finishMs);
        await cleanup(s, true);
        need(
            s.terminal.closed &&
                s.terminal.exitCode === 0 &&
                !s.terminal.signal &&
                s.terminal.stdoutEof &&
                s.terminal.stderrEof &&
                !s.terminal.forced &&
                !s.stderr.length &&
                !s.buffer.length &&
                !s.queue.length &&
                !s.failure,
            "SR_REFERENCE_TERMINAL",
        );
        for (const original of s.sources) {
            const { sha256, ...metadata } = original;
            same(await source(original.path, false), metadata, "SR_SOURCE_DRIFT");
        }
        s.success = true;
        s.finished = true;
        const evidence = snapshot(s);
        verifyRunnerSanityRaw(evidence);
        return evidence;
    } catch (error) {
        fail(s, error.code || "SR_FINISH_FAILED");
        await cleanup(s);
        s.finished = true;
        error.evidence = snapshot(s);
        throw error;
    }
}
export async function stopRunnerSanity(handle, reason = "SR_CALLER_CANCELLED") {
    const s = sessions.get(handle);
    need(s, "SR_SESSION_CONTEXT");
    if (s.finished) return snapshot(s);
    fail(s, reason);
    await cleanup(s);
    s.finished = true;
    return snapshot(s);
}

function stableIdentity(i) {
    const { statRaw, ...value } = i;
    return value;
}
function identity(i, origin) {
    need(
        i.sourceOrigin === origin && Number.isSafeInteger(i.pid) && i.pid > 0 && i.pid <= 4194304,
        "SR_PROCESS_IDENTITY",
    );
    if (origin === "fixture") {
        for (const key of ["bootId", "startTimeTicks", "timeNamespace", "timeForChildrenNamespace", "timeOffsets"])
            need(i[key] === null, "SR_FIXTURE_IDENTITY");
        return;
    }
    need(i.platform === "linux" && /^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(i.bootId), "SR_BOOT_IDENTITY");
    uint(i.startTimeTicks);
    for (const key of ["timeNamespace", "timeForChildrenNamespace"])
        need(/^time:\[[0-9]{1,20}\]$/.test(i[key]), "SR_TIME_NAMESPACE");
    const offsets = i.timeOffsets
        .trim()
        .split("\n")
        .map((line) => line.trim().split(/\s+/));
    need(
        offsets.length === 2 &&
            offsets[0][0] === "monotonic" &&
            offsets[1][0] === "boottime" &&
            offsets.every(
                (v) =>
                    v.length === 3 &&
                    /^-?[0-9]{1,12}$/.test(v[1]) &&
                    /^[0-9]{1,9}$/.test(v[2]) &&
                    Number(v[2]) < 1000000000,
            ),
        "SR_TIME_OFFSETS",
    );
    if (i.statRaw !== undefined) {
        const parts = i.statRaw
            .slice(i.statRaw.lastIndexOf(")") + 2)
            .trim()
            .split(/\s+/);
        need(
            i.statRaw.startsWith(`${i.pid} (`) &&
                parts.length >= 20 &&
                parts[19] === i.startTimeTicks &&
                !["Z", "X", "x"].includes(parts[0]),
            "SR_RAW_PROCESS_IDENTITY",
        );
    }
}
function fileProof(file, withDigest) {
    need(
        typeof file.path === "string" &&
            isAbsolute(file.path) &&
            file.metadata &&
            (!withDigest || /^[a-f0-9]{64}$/.test(file.sha256)),
        "SR_SOURCE_IDENTITY",
    );
    for (const key of ["bytes", "device", "inode", "mtimeNs", "ctimeNs"]) uint(file.metadata[key]);
    need(uint(file.metadata.bytes) > 0n && uint(file.metadata.bytes) <= 268435456n, "SR_SOURCE_IDENTITY");
}
function nodeIdentity(i, origin, componentSha256) {
    identity(i, origin);
    same(
        Object.keys(i).sort(),
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
        ].sort(),
        "SR_NODE_IDENTITY_FIELDS",
    );
    need(
        typeof i.platform === "string" &&
            i.platform.length <= 32 &&
            typeof i.processToken === "string" &&
            /^[a-f0-9]{32}$/.test(i.processToken),
        "SR_NODE_PROCESS_TOKEN",
    );
    const r = i.runtime;
    need(r && typeof r === "object" && !Array.isArray(r), "SR_NODE_RUNTIME");
    same(
        Object.keys(r).sort(),
        ["name", "version", "uv", "executable", "executableSha256", "componentSha256"].sort(),
        "SR_NODE_RUNTIME_FIELDS",
    );
    need(
        r.name === "node" &&
            typeof r.version === "string" &&
            /^v\d+\.\d+\.\d+(?:[-+][\w.-]+)?$/.test(r.version) &&
            typeof r.uv === "string" &&
            /^\d+\.\d+\.\d+(?:[-+][\w.-]+)?$/.test(r.uv) &&
            typeof r.executable === "string" &&
            isAbsolute(r.executable) &&
            r.executable.length <= 4096 &&
            /^[a-f0-9]{64}$/.test(r.executableSha256) &&
            /^[a-f0-9]{64}$/.test(r.componentSha256),
        "SR_NODE_RUNTIME",
    );
    same(r.componentSha256, componentSha256, "SR_ANCHOR_SOURCE_BINDING");
}
function referenceIdentity(i, origin) {
    identity(i, origin);
    same(
        Object.keys(i).sort(),
        [
            "sourceOrigin",
            "platform",
            "pid",
            "parentPid",
            "bootId",
            "startTimeTicks",
            "timeNamespace",
            "timeForChildrenNamespace",
            "timeOffsets",
            "statRaw",
            "python",
            "clock",
        ].sort(),
        "SR_REFERENCE_IDENTITY_FIELDS",
    );
    need(
        Number.isSafeInteger(i.parentPid) &&
            i.parentPid >= 1 &&
            i.parentPid <= 4194304 &&
            typeof i.platform === "string" &&
            i.platform.length <= 32,
        "SR_REFERENCE_PARENT",
    );
    need(i.python && i.clock, "SR_REFERENCE_RUNTIME");
    same(
        Object.keys(i.python).sort(),
        ["version", "executable", "implementation"].sort(),
        "SR_REFERENCE_RUNTIME_FIELDS",
    );
    need(
        typeof i.python.version === "string" &&
            i.python.version.length > 0 &&
            i.python.version.length <= 4096 &&
            typeof i.python.executable === "string" &&
            isAbsolute(i.python.executable) &&
            typeof i.python.implementation === "string" &&
            /^[a-z][a-z0-9_]{0,63}$/.test(i.python.implementation),
        "SR_REFERENCE_RUNTIME",
    );
    same(
        Object.keys(i.clock).sort(),
        ["api", "implementation", "monotonic", "adjustable", "resolutionSeconds"].sort(),
        "SR_REFERENCE_CLOCK_FIELDS",
    );
    need(
        i.clock.api === "time.monotonic_ns" &&
            typeof i.clock.implementation === "string" &&
            i.clock.implementation.length > 0 &&
            i.clock.implementation.length <= 256 &&
            typeof i.clock.monotonic === "boolean" &&
            typeof i.clock.adjustable === "boolean" &&
            Number.isFinite(i.clock.resolutionSeconds) &&
            i.clock.resolutionSeconds > 0,
        "SR_REFERENCE_CLOCK",
    );
    need(
        origin === "fixture"
            ? i.statRaw === null
            : typeof i.statRaw === "string" && Buffer.byteLength(i.statRaw) <= 4096,
        "SR_REFERENCE_STAT",
    );
}
export function verifyRunnerSanityRaw(e) {
    need(
        e.schemaVersion === 1 &&
            e.kind === "http-runner-sanity-raw" &&
            e.status === "completed" &&
            e.verified === false &&
            e.performanceAccepted === false &&
            e.failure === null,
        "SR_RAW_KIND",
    );
    validateRunnerSanitySpec(e.spec);
    validateBinding(e.binding);
    same(e.pendingGates, PENDING, "SR_PENDING_GATES");
    need(
        e.sourceOrigin === e.spec.sourceOrigin &&
            e.frames.length === 12 &&
            e.partialStdout === "" &&
            e.stderrBytes === 0 &&
            e.stderrRetainedBytes === 0 &&
            e.stderr === "" &&
            e.stderrSha256 === sha(Buffer.alloc(0)),
        "SR_RAW_FRAMES",
    );
    need(
        e.terminal.closed === true &&
            e.terminal.stdoutEof === true &&
            e.terminal.stderrEof === true &&
            e.terminal.exitCode === 0 &&
            e.terminal.signal === null &&
            e.terminal.forced === false &&
            e.terminal.termSent === false &&
            e.terminal.killSent === false,
        "SR_RAW_TERMINAL",
    );
    let total = 0;
    const frames = e.frames.map((f, index) => {
        const raw = Buffer.from(f.raw);
        need(
            raw.length === f.bytes &&
                raw.length <= e.spec.maxFrameBytes &&
                sha(raw) === f.sha256 &&
                raw.at(-1) === 10 &&
                !f.raw.slice(0, -1).includes("\n"),
            "SR_FRAME_DIGEST",
        );
        same(f.direction, index % 2 ? "python-to-node" : "node-to-python", "SR_FRAME_DIRECTION");
        if (index % 2 === 0) need(f.writeCompleted === true, "SR_FRAME_WRITE");
        total += raw.length;
        let value;
        try {
            value = parseRaw(raw);
        } catch (error) {
            throw error instanceof RunnerSanityError ? error : new RunnerSanityError("SR_INVALID_JSON");
        }
        same(value.binding, e.binding, "SR_FRAME_BINDING");
        frameFields(value);
        return value;
    });
    need(
        total === e.wireBytes &&
            total <= e.spec.maxRawBytes &&
            Buffer.byteLength(JSON.stringify(e)) <= e.spec.maxRawBytes * 4 + 65536,
        "SR_RAW_BYTE_BUDGET",
    );
    same(
        frames.map((f) => f.kind),
        [
            "init",
            "ready",
            "sample",
            "challenge",
            "anchor",
            "receipt",
            "sample",
            "challenge",
            "anchor",
            "receipt",
            "finish",
            "finished",
        ],
        "SR_FRAME_SEQUENCE",
    );
    const first = frames[1],
        last = frames[11];
    need(Array.isArray(e.clientSources) && e.clientSources.length === 3, "SR_CLIENT_SOURCE_INVENTORY");
    for (const file of e.clientSources) fileProof(file, true);
    same(
        e.clientSources.map((f) => f.path.split("/").at(-1)),
        ["runner-sanity.mjs", "phase-clock-reference.py", "clock-correlation.mjs"],
        "SR_CLIENT_SOURCE_INVENTORY",
    );
    referenceIdentity(first.identity, e.sourceOrigin);
    referenceIdentity(last.identity, e.sourceOrigin);
    same(stableIdentity(first.identity), stableIdentity(last.identity), "SR_REFERENCE_IDENTITY_DRIFT");
    need(first.identity.pid === e.peerPid && first.identity.parentPid === e.callerPid, "SR_REFERENCE_PARENT");
    need(first.files.length === 2 && first.files.every((f) => /^[a-f0-9]{64}$/.test(f.sha256)), "SR_REFERENCE_FILES");
    for (const file of first.files) fileProof(file, true);
    same(first.files[0], e.clientSources[1], "SR_REFERENCE_SOURCE_BINDING");
    if (first.identity.platform === "linux")
        need(first.files[1].path === "/proc/self/exe", "SR_REFERENCE_EXECUTABLE_BINDING");
    for (const file of last.files) fileProof(file, false);
    same(
        first.files.map(({ sha256, ...rest }) => rest),
        last.files,
        "SR_REFERENCE_FILE_DRIFT",
    );
    const samples = [];
    for (const [sequence, base] of [
        [0, 2],
        [1, 6],
    ]) {
        const [request, challenge, response, receipt] = frames.slice(base, base + 4),
            stage = ["pre", "post"][sequence];
        need(
            [request, challenge, response, receipt].every((f) => f.stage === stage) &&
                [challenge, response, receipt].every((f) => f.sequence === sequence) &&
                /^[a-f0-9]{32}$/.test(challenge.nonce) &&
                response.nonce === challenge.nonce,
            "SR_EXCHANGE_BINDING",
        );
        same(receipt.challengeRaw, e.frames[base + 1].raw, "SR_CHALLENGE_RAW");
        same(receipt.responseRaw, e.frames[base + 2].raw, "SR_RESPONSE_RAW");
        need(
            receipt.challengeSha256 === e.frames[base + 1].sha256 &&
                receipt.responseSha256 === e.frames[base + 2].sha256,
            "SR_RECEIPT_DIGEST",
        );
        const a = response.anchor;
        need(
            a.kind === "http-clock-anchor" &&
                a.schemaVersion === 1 &&
                a.sessionId === e.binding.sessionId &&
                a.sequence === sequence &&
                a.nonce === challenge.nonce &&
                Number.isFinite(a.performanceMs) &&
                a.performanceMs >= 0,
            "SR_ANCHOR_BINDING",
        );
        nodeIdentity(a.identityBefore, e.sourceOrigin, e.clientSources[2].sha256);
        nodeIdentity(a.identityAfter, e.sourceOrigin, e.clientSources[2].sha256);
        same(a.identityBefore, a.identityAfter, "SR_NODE_IDENTITY_DRIFT");
        need(a.identityBefore.pid === e.callerPid, "SR_ACTUAL_CALLER");
        const S = uint(receipt.sentMonotonicNs),
            R = uint(receipt.receivedMonotonicNs),
            H0 = uint(a.hrtimeBeforeNs),
            H1 = uint(a.hrtimeAfterNs);
        need(S <= R && H0 <= H1, "SR_EXCHANGE_TIME_ORDER");
        if (e.sourceOrigin === "linux-procfs") {
            for (const key of ["bootId", "timeNamespace", "timeForChildrenNamespace", "timeOffsets"])
                same(a.identityBefore[key], first.identity[key], "SR_PEER_NAMESPACE_MISMATCH");
            need(S <= H0 && H1 <= R, "SR_OBSERVED_CLOCK_INCOMPATIBLE");
        }
        samples.push({ stage, S, R, H0, H1, performanceMs: a.performanceMs, identity: a.identityBefore });
    }
    same(samples[0].identity, samples[1].identity, "SR_NODE_IDENTITY_DRIFT");
    need(frames[3].nonce !== frames[7].nonce, "SR_NONCE_REUSE");
    need(
        samples[0].R <= samples[1].S &&
            samples[0].H1 <= samples[1].H0 &&
            samples[0].performanceMs <= samples[1].performanceMs,
        "SR_PRE_POST_ORDER",
    );
    return {
        rawSanity: "passed",
        sourceOrigin: e.sourceOrigin,
        verified: false,
        hostAssociation: "unverified",
        pendingGates: [...PENDING],
        performanceAccepted: false,
        samples: samples.map((s) => ({
            stage: s.stage,
            roundTripNs: (s.R - s.S).toString(),
            localBracketNs: (s.H1 - s.H0).toString(),
            observedContainment: e.sourceOrigin === "linux-procfs" ? true : null,
        })),
    };
}

if (process.argv[1] && resolve(process.argv[1]) === HERE) {
    if (process.argv.slice(2).join(" ") === "--help") {
        process.stdout.write(
            "Actual-caller clock sanity raw verifier\n" +
                "  node runner-sanity.mjs verify --evidence RAW.json\n" +
                "Read-only, bounded raw frame/digest/identity validation; never launches a peer.\n" +
                "Library: startRunnerSanity, captureRunnerSanity(pre/post), finishRunnerSanity, stopRunnerSanity.\n" +
                "Optional HTTP profile clockReference enables actual-runner pre/post collection.\n" +
                "Success remains verified=false with external runtime/source,\n" +
                "collector namespace and quality-threshold gates pending. Exit 0=raw sanity only; 6=invalid.\n",
        );
    } else {
        let file;
        try {
            need(
                process.argv.length === 5 && process.argv[2] === "verify" && process.argv[3] === "--evidence",
                "SR_ARGUMENTS",
            );
            file = await open(process.argv[4], constants.O_RDONLY | constants.O_NONBLOCK | constants.O_NOFOLLOW);
            const before = await file.stat({ bigint: true }),
                cap = 4 * 262144 + 65536;
            need(before.isFile() && before.size > 0n && before.size <= BigInt(cap), "SR_EVIDENCE_BUDGET");
            const buffer = Buffer.alloc(Number(before.size) + 1);
            let count = 0;
            while (count < buffer.length) {
                const { bytesRead } = await file.read(buffer, count, buffer.length - count, count);
                if (!bytesRead) break;
                count += bytesRead;
            }
            same(meta(before), meta(await file.stat({ bigint: true })), "SR_EVIDENCE_DRIFT");
            need(count === Number(before.size), "SR_EVIDENCE_DRIFT");
            const result = verifyRunnerSanityRaw(parseRaw(buffer.subarray(0, count)));
            process.stdout.write(JSON.stringify({ ok: true, data: result }) + "\n");
        } catch (error) {
            process.stdout.write(
                JSON.stringify({
                    ok: false,
                    error: { code: error.code || "SR_INVALID_EVIDENCE" },
                    verified: false,
                    performanceAccepted: false,
                }) + "\n",
            );
            process.exitCode = 6;
        } finally {
            await file?.close();
        }
    }
}
