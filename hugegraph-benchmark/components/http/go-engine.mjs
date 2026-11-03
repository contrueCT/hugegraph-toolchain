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

import { execFile, spawn } from "node:child_process";
import { promisify, isDeepStrictEqual } from "node:util";
import { lstat, realpath, readFile } from "node:fs/promises";
import { digest, json, valueDigest, bytesDigest } from "./common.mjs";

export const GO_ENGINE = "go-http-v1";
const need = (v, message) => {
    if (!v) throw new Error(`HTTP_GO: ${message}`);
};
const finite = (v) => Number.isFinite(v) && v >= 0;
const integer = (v) => Number.isSafeInteger(v) && v >= 0;
export const isGo = (p) => p.schemaVersion === 2 && p.executor?.engine === GO_ENGINE;
export async function inspectGoBinary(spec) {
    const path = await realpath(spec.path),
        info = await lstat(path);
    need(
        info.isFile() && info.size > 0 && info.size <= 32 * 1024 * 1024 && info.mode & 0o111,
        "invalid executable file",
    );
    need((await digest(path)) === spec.sha256, "registered binary SHA differs");
    const result = await promisify(execFile)(path, ["version"], { timeout: 10000, maxBuffer: 65536 });
    const identity = JSON.parse(result.stdout);
    validateGoIdentity(identity, spec);
    need(identity.bytes === info.size, "binary size differs");
    return { path, identity };
}
export function validateGoIdentity(v, spec) {
    need(
        typeof spec.sourceSha256 === "string" && /^[a-f0-9]{64}$/.test(spec.sourceSha256),
        "unregistered source digest",
    );
    need(
        v?.schemaVersion === 1 &&
            v.kind === "hgbench-http-go-identity" &&
            v.engine === GO_ENGINE &&
            v.requestSchemaVersion === 2 &&
            v.rawSchemaVersion === 2 &&
            v.binarySha256 === spec.sha256 &&
            v.sourceSha256 === spec.sourceSha256 &&
            v.goVersion === spec.goVersion &&
            v.goos === spec.goos &&
            v.goarch === spec.goarch &&
            v.buildSettings?.CGO_ENABLED === "0" &&
            v.buildSettings?.["-trimpath"] === "true",
        "binary protocol/build identity differs",
    );
}
export async function executeGo(profile, args) {
    return (await createGoExecution(profile))(args);
}
export async function createGoExecution(profile) {
    const frozen = structuredClone(profile);
    await inspectGoBinary(frozen.executor.binary);
    // The native runner independently checks its actual SHA/build against each
    // frozen request before HTTP. Repeating a separate `version` process for
    // every probe/run provides no additional request-identity guarantee.
    return (args) => executeRegisteredGo(frozen, args);
}
async function executeRegisteredGo(profile, args) {
    if (profile.executor.hostIdentityGate) {
        const index = args.indexOf("--output");
        need(index >= 0 && typeof args[index + 1] === "string", "identity gate requires an explicit result path");
        args = [...args, "--identity-gate", args[index + 1] + ".identity"];
    }
    await new Promise((resolve, reject) => {
        const env = {
            PATH: "/usr/bin:/bin",
            LANG: "C",
            LC_ALL: "C",
            GOMAXPROCS: String(profile.executor.gomaxprocs),
            GOGC: "100",
            GOMEMLIMIT: "off",
            GODEBUG: "",
        };
        if (profile.authorizationEnv) env[profile.authorizationEnv] = process.env[profile.authorizationEnv];
        const child = spawn(profile.executor.binary.path, ["run", ...args], {
            env,
            stdio: ["ignore", "inherit", "inherit"],
        });
        child.on("error", reject);
        child.on("close", (code, signal) =>
            code === 0 ? resolve() : reject(new Error(`Go runner exited ${code} (${signal})`)),
        );
    });
}

export const GO_IDENTITY_FILES = ["ready.json", "ready-release.json", "finished.json", "finished-release.json"];
export function goIdentityFiles(result) {
    return GO_IDENTITY_FILES.map((name) => result + ".identity/" + name);
}
export async function verifyGoIdentityGates(result, run, profile) {
    need(
        profile.executor?.hostIdentityGate === true && run.processIdentityAfter,
        "declared live identity gate missing",
    );
    for (const stage of ["ready", "finished"]) {
        const markerRaw = await readFile(result + ".identity/" + stage + ".json");
        const releaseRaw = await readFile(result + ".identity/" + stage + "-release.json");
        need(markerRaw.length <= 65536 && releaseRaw.length <= 4096, "identity gate metadata exceeds bound");
        const marker = JSON.parse(markerRaw),
            release = JSON.parse(releaseRaw);
        need(
            marker.schemaVersion === 1 &&
                marker.kind === "http-go-live-process-marker" &&
                marker.id === run.id &&
                marker.stage === stage &&
                /^[a-f0-9]{32}$/.test(marker.nonce) &&
                marker.performanceAccepted === false,
            "identity marker differs",
        );
        validateGoIdentity(marker.binary, profile.executor.binary);
        const expected = structuredClone(run.phaseClock);
        if (stage === "finished") expected.process = run.processIdentityAfter;
        need(isDeepStrictEqual(marker.clock, expected), "identity gate clock differs from run");
        need(
            release.schemaVersion === 1 &&
                release.kind === "http-go-live-process-release" &&
                release.id === run.id &&
                release.stage === stage &&
                release.nonce === marker.nonce &&
                release.markerSha256 === bytesDigest(markerRaw),
            "identity gate release differs",
        );
    }
    return {
        status: "producer-handshake-verified",
        independentHostObservationRequired: true,
        performanceAccepted: false,
    };
}
export async function runGoProbe(profile, input, trace, request, output) {
    // Request evidence is already create-only and locked by the enclosing workflow.
    await executeGo(profile, ["--request", request, "--trace", trace, "--output", output]);
    return json(output);
}
function clockNs(v) {
    need(typeof v === "string" && /^(0|[1-9][0-9]{0,18})$/.test(v), "invalid monotonic nanoseconds");
    return BigInt(v);
}
export function validateGoRaw(run, p) {
    need(
        isGo(p) &&
            run?.schemaVersion === 2 &&
            run.engine === GO_ENGINE &&
            ["http-run", "http-functional-probe"].includes(run.kind) &&
            run.performanceAccepted === false,
        "raw engine/protocol differs",
    );
    const e = p.executor,
        c = run.client,
        identity = run.phaseClock;
    need(
        c &&
            c.engine === GO_ENGINE &&
            !Object.hasOwn(c, "node") &&
            c.binarySha256 === e.binary.sha256 &&
            c.goVersion === e.binary.goVersion &&
            c.goos === e.binary.goos &&
            c.goarch === e.binary.goarch &&
            c.gomaxprocs === e.gomaxprocs &&
            c.http === "1.1" &&
            c.keepAlive === true &&
            c.retries === 0 &&
            c.redirects === 0 &&
            c.proxy === "none" &&
            c.tlsVerification === "default-enabled" &&
            c.postReplay === "nonempty-body-getbody-nil" &&
            c.gcPercent === 100 &&
            c.goMemoryLimit === "off" &&
            c.nodeEluApplicable === false &&
            c.compression === p.responseEncoding &&
            c.buildSettings?.CGO_ENABLED === "0" &&
            c.buildSettings?.["-trimpath"] === "true",
        "client identity/policy differs",
    );
    need(
        identity?.contract === "go-monotonic-bracket-v1" &&
            identity.goVersion === c.goVersion &&
            identity.goos === c.goos &&
            identity.goarch === c.goarch &&
            Number.isSafeInteger(identity.pid) &&
            identity.pid > 0 &&
            identity.externalCorrelationAccepted === false &&
            Array.isArray(run.phases) &&
            run.phases.length >= 1 &&
            run.phases.length <= 2,
        "clock identity differs",
    );
    let lower,
        upper,
        previous = 0;
    const boundary = (b) => {
        need(b && finite(b.sinceOriginMs) && b.sinceOriginMs >= previous, "clock boundary ordering differs");
        previous = b.sinceOriginMs;
        if (c.goos === "linux") {
            const before = clockNs(b.beforeNs),
                after = clockNs(b.afterNs);
            need(before <= after && !b.error, "clock bracket invalid");
            const delta = BigInt(Math.round(b.sinceOriginMs * 1e6));
            // A microsecond bounds floating conversion; wide raw brackets retain their actual uncertainty.
            const lo = before - delta - 1000n,
                hi = after - delta + 1000n;
            lower = lower === undefined || lo > lower ? lo : lower;
            upper = upper === undefined || hi < upper ? hi : upper;
            need(lower <= upper, "Go elapsed and raw Linux monotonic brackets disagree");
        } else
            need(
                b.beforeNs === null && b.afterNs === null && typeof b.error === "string",
                "non-Linux clock unavailable evidence differs",
            );
    };
    need(identity.origin.sinceOriginMs === 0, "clock origin differs");
    if (Object.hasOwn(run, "processIdentityAfter"))
        need(isDeepStrictEqual(run.processIdentityAfter, identity.process), "Go process identity changed during run");
    boundary(identity.origin);
    if (c.goos === "linux") {
        need(
            /^[a-f0-9-]{36}$/.test(identity.process?.bootId) && /^[0-9]+$/.test(identity.process?.startTicks),
            "Linux process identity missing",
        );
        for (const n of ["pid", "mnt", "time", "cgroup"])
            need(
                new RegExp(`^${n}:\\[[0-9]+\\]$`).test(identity.process[n + "Namespace"]),
                "Linux namespace identity missing",
            );
    }
    for (const phase of run.phases) {
        need(phase.clock?.contract === identity.contract && finite(phase.elapsedMs), "phase clock contract differs");
        boundary(phase.clock.start);
        boundary(phase.clock.end);
        need(
            Math.abs(phase.elapsedMs - (phase.clock.end.sinceOriginMs - phase.clock.start.sinceOriginMs)) < 1e-6,
            "phase elapsed differs from clock boundaries",
        );
        need(
            phase.eventLoopUtilization === null &&
                phase.eventLoopDelayMs === null &&
                phase.resources?.contract === "go-process-cgroup-v1" &&
                phase.resources.nodeEluApplicable === false &&
                phase.resources.gomaxprocs === e.gomaxprocs &&
                phase.resources.declaredQuotaCores === e.cpuQuotaCores &&
                phase.resources.declaredMaxRssBytes === e.maxRssBytes,
            "resource engine/declared limits differ",
        );
        const r = phase.resources,
            a = r.start,
            b = r.end;
        for (const s of [a, b])
            need(
                s &&
                    finite(s.heapAllocBytes) &&
                    finite(s.heapSysBytes) &&
                    integer(s.numGc) &&
                    integer(s.gcPauseNs) &&
                    integer(s.goroutines) &&
                    s.goroutines > 0,
                "invalid Go runtime resource snapshot",
            );
        for (const snapshot of [a, b]) {
            need(
                (snapshot.cpuUsec === -1 || finite(snapshot.cpuUsec)) &&
                    (snapshot.maxRssKiB === -1 || integer(snapshot.maxRssKiB)) &&
                    (snapshot.quotaCores === null ||
                        (Number.isFinite(snapshot.quotaCores) && snapshot.quotaCores > 0)) &&
                    typeof snapshot.cgroupPath === "string" &&
                    snapshot.cgroup &&
                    typeof snapshot.cgroup === "object" &&
                    !Array.isArray(snapshot.cgroup) &&
                    Object.values(snapshot.cgroup).every(integer),
                "invalid process/cgroup counters",
            );
        }
        need(
            b.numGc >= a.numGc &&
                b.gcPauseNs >= a.gcPauseNs &&
                r.numGc === b.numGc - a.numGc &&
                r.gcPauseNs === b.gcPauseNs - a.gcPauseNs,
            "GC accounting differs",
        );
        if (a.cpuUsec >= 0 && b.cpuUsec >= a.cpuUsec) {
            const cores = (b.cpuUsec - a.cpuUsec) / (phase.elapsedMs * 1000);
            need(
                finite(phase.averageCpuCores) &&
                    Math.abs(phase.averageCpuCores - cores) < 1e-9 &&
                    finite(r.cpuFractionOfDeclaredQuota) &&
                    Math.abs(r.cpuFractionOfDeclaredQuota - cores / e.cpuQuotaCores) < 1e-9,
                "CPU accounting differs",
            );
        } else
            need(phase.averageCpuCores === null && r.cpuFractionOfDeclaredQuota === null, "unavailable CPU invented");
        need(phase.maxRssKiB === (b.maxRssKiB < 0 ? null : b.maxRssKiB), "RSS accounting differs");
        const x = a.cgroup?.throttled_usec,
            y = b.cgroup?.throttled_usec;
        need(r.throttledUsec === (integer(x) && integer(y) && y >= x ? y - x : null), "throttling accounting differs");
    }
    return {
        contract: identity.contract,
        engine: GO_ENGINE,
        externalCorrelationAccepted: false,
        processClockVerified: c.goos === "linux",
        phases: run.phases.map((s) => ({ name: s.name, ...s.clock })),
    };
}
export function goResourceIssues(phase, p) {
    const issues = [],
        r = phase.resources;
    if (
        phase.averageCpuCores === null ||
        r.start.quotaCores === null ||
        r.end.quotaCores === null ||
        Math.abs(r.start.quotaCores - p.executor.cpuQuotaCores) > 1e-6 ||
        Math.abs(r.end.quotaCores - p.executor.cpuQuotaCores) > 1e-6 ||
        r.throttledUsec === null
    )
        issues.push(`${phase.name}:go-resource-isolation-unverified`);
    if (
        r.cpuFractionOfDeclaredQuota > 0.8 ||
        r.throttledUsec > 0 ||
        phase.late > 0 ||
        phase.maxRssKiB * 1024 > p.executor.maxRssBytes
    )
        issues.push(`${phase.name}:driver-pressure-observed`);
    return issues;
}
export function validateGoWriter(run, bytes, p) {
    const w = run.traceWriter;
    need(w?.mode === "go-async-byte-bounded-v1" && w.complete === true, "trace writer incomplete");
    for (const k of [
        "maximumPendingBytes",
        "peakPendingBytes",
        "enqueuedBytes",
        "persistedBytes",
        "records",
        "writeCalls",
    ])
        need(integer(w[k]), "invalid writer counter");
    need(bytes === 0 || (w.maximumPendingBytes > 0 && w.peakPendingBytes > 0), "missing pending byte peak");
    need(
        w.maximumPendingBytes <= p.executor.maxPendingBytes &&
            w.peakPendingBytes <= w.maximumPendingBytes &&
            w.enqueuedBytes === bytes &&
            w.persistedBytes === bytes &&
            run.traceBytes === bytes &&
            w.records === run.phases.reduce((sum, v) => sum + v.scheduled, 0) &&
            w.writeCalls >= w.records,
        "trace writer byte/count accounting differs",
    );
    for (const k of ["serializationAndEnqueueMs", "asynchronousWriteWaitMs", "finalFlushWaitMs", "finalSyncMs"])
        need(finite(w[k]), "invalid writer timing");
    return structuredClone(w);
}

export function validateGoRegistration(registration, p) {
    need(
        registration?.schemaVersion === 1 && registration.kind === "http-go-executor-registration",
        "invalid registration",
    );
    validateGoIdentity(registration.identity, p.executor.binary);
    const members = registration.sourceFiles;
    need(
        Array.isArray(members) &&
            members.length > 0 &&
            members.length <= 64 &&
            members.every(
                (f) =>
                    typeof f.path === "string" &&
                    /^(?:[a-zA-Z0-9_-]+\/)*[a-zA-Z0-9_.-]+$/.test(f.path) &&
                    !f.path.split("/").includes("..") &&
                    (f.path.endsWith(".go") || f.path === "go.mod") &&
                    integer(f.bytes) &&
                    f.bytes <= 8 * 1024 * 1024 &&
                    /^[a-f0-9]{64}$/.test(f.sha256),
            ) &&
            new Set(members.map((f) => f.path)).size === members.length &&
            members.every((f, i) => i === 0 || members[i - 1].path < f.path) &&
            members.reduce((s, f) => s + f.bytes, 0) <= 8 * 1024 * 1024 &&
            valueDigest(members) === p.executor.binary.sourceSha256 &&
            /^[a-f0-9]{64}$/.test(registration.toolchainLockSha256),
        "source registration differs",
    );
}
