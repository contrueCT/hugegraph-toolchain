/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { associatePhaseHost } from "../components/http/phase-host-evidence.mjs";
import { validateCollectorClockIdentity } from "../components/http/collector-clock-evidence.mjs";
import { validateDirectPhaseClockRaw } from "../components/http/direct-phase-clock.mjs";
import { verifyRunnerSanityRaw } from "../components/http/runner-sanity.mjs";
import { valueDigest } from "../components/http/common.mjs";

// Entirely synthetic protocol records: no process, Docker, HTTP, file proof or benchmark is executed.
// These records exercise raw consistency. Even a complete set of assertions cannot certify provenance.
const SHA = (raw) => createHash("sha256").update(raw).digest("hex");
const BOOT = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
const OFFICIAL_NODE = "1abce2374a485bddae3c27b17a3e3143e2780232026e627c4fe74ddde3f380a1";
const CONTRACT = "linux-node22.14-cpython-monotonic-v1";
const sourceNames = [
    "runner.mjs",
    "direct-phase-clock.mjs",
    "runner-sanity.mjs",
    "clock-correlation.mjs",
    "phase-clock-reference.py",
    "resource-collector.py",
];
const sources = Object.fromEntries(sourceNames.map((name, i) => [name, String(i + 1).repeat(64)]));
const offsetsRaw = "monotonic           0         0\nboottime            0         0\n";
const offsets = { monotonic: { seconds: "0", nanoseconds: "0" }, boottime: { seconds: "0", nanoseconds: "0" } };
const clock = {
    api: "time.monotonic_ns",
    implementation: "clock_gettime(CLOCK_MONOTONIC)",
    monotonic: true,
    adjustable: false,
    resolutionSeconds: 1e-9,
};
const statRaw = (pid, start) => `${pid} (fixture) R 1 ${pid} ${pid} 0 -1 0 0 0 0 0 1 0 0 0 20 0 1 0 ${start} 1000\n`;
const metadata = { bytes: "10000", device: "1", inode: "2", mtimeNs: "3", ctimeNs: "4" };
const frame = (value, direction) => {
    const raw = JSON.stringify(value) + "\n";
    return {
        direction,
        raw,
        bytes: Buffer.byteLength(raw),
        sha256: SHA(raw),
        ...(direction === "node-to-python" ? { writeCompleted: true } : {}),
    };
};
function account(run) {
    const { evidenceBytes, ...session } = run.phaseClock;
    run.phaseClock.evidenceBytes = Buffer.byteLength(
        JSON.stringify({ session, phases: run.phases.map((p) => p.clock) }),
    );
}
function changeFrame(run, index, patch) {
    const raw = run.clockReference,
        value = JSON.parse(raw.frames[index].raw);
    patch(value);
    raw.frames[index] = frame(value, raw.frames[index].direction);
    if ([3, 4, 7, 8].includes(index))
        changeFrame(run, index < 6 ? 5 : 9, (receipt) => {
            const kind = index === 3 || index === 7 ? "challenge" : "response";
            receipt[kind + "Raw"] = raw.frames[index].raw;
            receipt[kind + "Sha256"] = raw.frames[index].sha256;
        });
    raw.wireBytes = raw.frames.reduce((sum, value) => sum + value.bytes, 0);
}
function updateLocks(f) {
    account(f.run);
    f.external.runDigest = valueDigest(f.run);
    f.external.profileDigest = valueDigest(f.profile);
    f.external.collector.summaryDigest = valueDigest(f.collector.summary);
}
function updateCollector(f) {
    const summary = f.collector.summary;
    f.collector.verification = {
        status: "verified",
        sourceOrigin: summary.sourceOrigin,
        summarySha256: "9".repeat(64),
        sampleCount: summary.samples,
        firstStartedMonotonicNs: summary.firstSampleStartedMonotonicNs,
        firstFinishedMonotonicNs: String(BigInt(summary.firstSampleStartedMonotonicNs) + 1000n),
        lastFinishedMonotonicNs: summary.lastSampleFinishedMonotonicNs,
        finishedMonotonicNs: summary.finishedMonotonicNs,
        clockIdentity: validateCollectorClockIdentity(summary),
        performanceAccepted: false,
    };
}
function fixture() {
    const spec = {
        schemaVersion: 1,
        kind: "http-runner-sanity-spec",
        sourceOrigin: "linux-procfs",
        startupMs: 1000,
        exchangeMs: 1000,
        finishMs: 500,
        wallMs: 10000,
        maxFrameBytes: 8192,
        maxRawBytes: 65536,
        maxStderrBytes: 0,
    };
    const profile = {
        schemaVersion: 1,
        kind: "http-benchmark-profile",
        name: "synthetic-clock-contract",
        clockReference: { python: "/usr/bin/python3.12", spec },
        endpoint: "http://fixture.invalid/gremlin",
        cases: [{ id: "c1", body: { gremlin: "1+1" }, checks: [{ pointer: "/result/data", equals: [2] }] }],
        loads: [{ id: "l1", model: "closed", concurrency: 1 }],
        repeats: 1,
        serviceChecks: true,
        warmupMs: 99,
        durationMs: 200,
        drainMs: 100,
        requestTimeoutMs: 100,
        maxTraceBytes: 65536,
        maxRequestsPerPhase: 10,
        maxResponseBytes: 4096,
        responseEncoding: "identity",
        authorizationEnv: null,
        target: {
            label: "synthetic",
            build: "synthetic",
            mode: "synthetic",
            environment: "synthetic",
            evidenceFiles: [],
        },
    };
    const runIdentity = { id: "r1", caseId: "c1", loadId: "l1", repeat: 0, probePhase: null },
        sessionId = "c".repeat(32);
    const binding = { sessionId, runIdentity, profileDigest: valueDigest(profile) };
    const nodeIdentity = {
        sourceOrigin: "linux-procfs",
        platform: "linux",
        pid: 25,
        processToken: "b".repeat(32),
        bootId: BOOT,
        startTimeTicks: "42",
        timeNamespace: "time:[4026531834]",
        timeForChildrenNamespace: "time:[4026531834]",
        timeOffsets: offsetsRaw,
        runtime: {
            name: "node",
            version: "v22.14.0",
            uv: "1.49.2",
            executable: "/node/bin/node",
            executableSha256: OFFICIAL_NODE,
            componentSha256: sources["clock-correlation.mjs"],
        },
    };
    const peerIdentity = {
        sourceOrigin: "linux-procfs",
        platform: "linux",
        pid: 26,
        parentPid: 25,
        bootId: BOOT,
        startTimeTicks: "43",
        timeNamespace: "time:[4026531834]",
        timeForChildrenNamespace: "time:[4026531834]",
        timeOffsets: offsetsRaw,
        statRaw: statRaw(26, "43"),
        python: {
            implementation: "cpython",
            version: "3.12.3 (main, synthetic) [GCC]",
            executable: "/usr/bin/python3.12",
        },
        clock,
    };
    const clientSources = ["runner-sanity.mjs", "phase-clock-reference.py", "clock-correlation.mjs"].map((name) => ({
        path: "/source/components/http/" + name,
        metadata: { ...metadata },
        sha256: sources[name],
    }));
    const peerFiles = [
        structuredClone(clientSources[1]),
        { path: "/proc/self/exe", metadata: { ...metadata }, sha256: "7".repeat(64) },
    ];
    const frames = [];
    const push = (value) => {
        const result = frame(value, frames.length % 2 ? "python-to-node" : "node-to-python");
        frames.push(result);
        return result;
    };
    push({ kind: "init", binding });
    push({ kind: "ready", binding, identity: peerIdentity, files: peerFiles });
    for (const [sequence, stage, h, p] of [
        [0, "pre", 10000000000n, 1000.0005],
        [1, "post", 10302000000n, 1302.0005],
    ]) {
        const nonce = String(sequence + 1).repeat(32);
        push({ kind: "sample", binding, stage });
        const challenge = push({ kind: "challenge", binding, stage, sequence, nonce });
        const response = push({
            kind: "anchor",
            binding,
            stage,
            sequence,
            nonce,
            anchor: {
                schemaVersion: 1,
                kind: "http-clock-anchor",
                sessionId,
                sequence,
                nonce,
                hrtimeBeforeNs: h.toString(),
                hrtimeAfterNs: (h + 1000n).toString(),
                performanceMs: p,
                identityBefore: nodeIdentity,
                identityAfter: nodeIdentity,
            },
        });
        push({
            kind: "receipt",
            binding,
            stage,
            sequence,
            sentMonotonicNs: (h - 100000n).toString(),
            receivedMonotonicNs: (h + 100000n).toString(),
            challengeRaw: challenge.raw,
            responseRaw: response.raw,
            challengeSha256: challenge.sha256,
            responseSha256: response.sha256,
        });
    }
    push({ kind: "finish", binding });
    push({
        kind: "finished",
        binding,
        identity: peerIdentity,
        files: peerFiles.map(({ sha256, ...rest }) => rest),
        performanceAccepted: false,
    });
    const raw = {
        schemaVersion: 1,
        kind: "http-runner-sanity-raw",
        status: "completed",
        verified: false,
        performanceAccepted: false,
        sourceOrigin: "linux-procfs",
        spec,
        binding,
        callerPid: 25,
        peerPid: 26,
        clientSources,
        frames,
        partialStdout: "",
        stderr: "",
        stderrBytes: 0,
        stderrRetainedBytes: 0,
        stderrSha256: SHA(""),
        wireBytes: frames.reduce((n, f) => n + f.bytes, 0),
        failure: null,
        terminal: {
            closed: true,
            stdoutEof: true,
            stderrEof: true,
            exitCode: 0,
            signal: null,
            termSent: false,
            killSent: false,
            forced: false,
        },
        pendingGates: [
            "external-runtime-source-lock",
            "host-collector-namespace-binding",
            "predeclared-quality-thresholds",
        ],
    };
    const proc = {
        platform: "linux",
        arch: "x64",
        pid: 25,
        processToken: "a".repeat(32),
        availability: "available",
        reasons: [],
        bootId: BOOT,
        startTimeTicks: "42",
        statRaw: statRaw(25, "42"),
        statusRaw: "Name:\tnode\nPid:\t25\nNSpid:\t25\n",
        namespaces: {
            time: "time:[4026531834]",
            time_for_children: "time:[4026531834]",
            pid: "pid:[1]",
            mnt: "mnt:[2]",
            user: "user:[3]",
        },
        timeOffsets: offsetsRaw,
    };
    const file = (path, sha256, bytes = 10000) => ({
        path,
        sha256,
        bytes,
        device: "1",
        inode: "2",
        mtimeNs: "3",
        ctimeNs: "4",
    });
    const phase = (name, h0, h1, p0, p1) => ({
        name,
        durationMs: p1 - p0,
        elapsedMs: p1 - p0,
        clock: {
            schemaVersion: 1,
            kind: "http-direct-phase-clock",
            status: "raw-unverified",
            verified: false,
            sessionId,
            phase: name,
            windowSemantics: "actual-phase-including-drain",
            start: { hrtimeBeforeNs: h0.toString(), hrtimeAfterNs: (h0 + 1000n).toString(), performanceMs: p0 },
            end: { hrtimeBeforeNs: h1.toString(), hrtimeAfterNs: (h1 + 1000n).toString(), performanceMs: p1 },
        },
    });
    const run = {
        schemaVersion: 1,
        kind: "http-run",
        ...runIdentity,
        phases: [
            phase("warmup", 10001000000n, 10100000000n, 1001.0005, 1100.0005),
            phase("measurement", 10101000000n, 10301000000n, 1101.0005, 1301.0005),
        ],
        clockReference: raw,
        performanceAccepted: false,
        phaseClock: {
            schemaVersion: 1,
            kind: "http-direct-phase-clock-session",
            status: "raw-unverified",
            verified: false,
            sessionId,
            runIdentity,
            identityBefore: proc,
            identityAfter: structuredClone(proc),
            runtime: {
                node: "v22.14.0",
                uv: "1.49.2",
                platform: "linux",
                arch: "x64",
                nodeSharedLibuv: false,
                injectionEnvironmentPresent: {
                    LD_PRELOAD: false,
                    LD_AUDIT: false,
                    LD_LIBRARY_PATH: false,
                    NODE_OPTIONS: false,
                },
                execArgvSha256: valueDigest([]),
                unavailable: null,
                files: {
                    executable: { ...file("/proc/self/exe", OFFICIAL_NODE, 120177224), reportedPath: "/node/bin/node" },
                    component: file(
                        "/source/components/http/direct-phase-clock.mjs",
                        sources["direct-phase-clock.mjs"],
                    ),
                },
            },
            preparedIntervalNs: ["9990000000", "10000200000"],
            finishedIntervalNs: ["10301500000", "10302200000"],
            contractCandidate: CONTRACT,
            pendingGates: [
                "runtime-provenance-lock",
                "host-python-clock",
                "host-namespace-binding",
                "raw-roundtrip-sanity",
                "predeclared-boundary-width",
                "component-source-lock",
            ],
            maxEvidenceBytes: 65536,
            performanceAccepted: false,
            clockReference: {
                required: true,
                profileDigest: valueDigest(profile),
                specDigest: valueDigest(spec),
                maxEvidenceBytes: 4 * spec.maxRawBytes + 65536,
            },
        },
    };
    const before = {
        sourceOrigin: "linux-procfs",
        availability: "observed",
        pid: 1000,
        startTimeTicks: "40",
        bootId: BOOT,
        statRaw: statRaw(1000, "40"),
        timeNamespace: "time:[4026531834]",
        timeForChildrenNamespace: "time:[4026531834]",
        timeOffsetsRaw: offsetsRaw,
        timeOffsets: structuredClone(offsets),
        python: {
            reportedExecutable: "/usr/bin/python3.12",
            version: "3.12.3 (main, synthetic) [GCC]",
            implementation: "cpython",
        },
        clock,
        executable: {
            sourcePath: "/proc/self/exe",
            digestRead: true,
            metadata: { ...metadata },
            sha256: "8".repeat(64),
        },
        unavailable: {},
        captureDrift: [],
        startedMonotonicNs: "9700000000",
        finishedMonotonicNs: "9700010000",
    };
    const after = structuredClone(before);
    Object.assign(after, { startedMonotonicNs: "10501000000", finishedMonotonicNs: "10501001000" });
    Object.assign(after.executable, { digestRead: false, sha256: null });
    const summary = {
        schemaVersion: 1,
        kind: "http-resource-summary",
        sourceOrigin: "linux-procfs",
        status: "completed",
        identityContinuous: true,
        collectorPid: 1000,
        componentSha256: sources["resource-collector.py"],
        pythonExecutable: "/usr/bin/python3.12",
        pythonVersion: "3.12.3",
        host: { bootId: BOOT },
        clock: { implementation: clock.implementation, adjustable: false, resolutionSeconds: 1e-9 },
        samples: 2,
        startedMonotonicNs: "9800000000",
        finishedMonotonicNs: "10600000000",
        firstSampleStartedMonotonicNs: "9900000000",
        lastSampleFinishedMonotonicNs: "10500000000",
        clockIdentity: {
            schemaVersion: 1,
            kind: "http-collector-clock-identity",
            sourceOrigin: "linux-procfs",
            availability: "observed",
            verified: false,
            hostAssociation: "unverified",
            before,
            after,
            changedFields: [],
            executableHashMaxBytes: 268435456,
            executableHashMaxNs: "2000000000",
        },
    };
    const knownSources = {
        version: "3.12.3",
        pytimeSha256: "033751b3c90dd12cd40b86af11388a0a201bc44deca0e96b52b4a13da8be82c9",
        timeModuleSha256: "55ee3d5dfaab9ef4044f7537749ba1d4b17b8dc55796763ed65e4a3383075607",
    };
    const f = {
        run,
        profile,
        collector: { summary },
        external: {
            runtimeDigest: "c".repeat(64),
            probedRuntimeDigest: "a".repeat(64),
            runtimeProofDigest: "b".repeat(64),
            node: { version: "v22.14.0", uv: "1.49.2", executableSha256: OFFICIAL_NODE },
            peer: {
                imageId: "sha256:" + "d".repeat(64),
                version: "3.12.3",
                implementation: "cpython",
                executableSha256: "7".repeat(64),
            },
            collector: {
                hostBootId: BOOT,
                version: "3.12.3",
                executableSha256: "8".repeat(64),
                componentSha256: sources["resource-collector.py"],
            },
            sources,
            pythonClockSources: { peer: { ...knownSources }, collector: { ...knownSources } },
            quality: { maxBoundaryWidthNs: "5000", maxRoundTripNs: "500000", maxAnchorWidthNs: "5000" },
            runtimeBinding: {
                raw: {
                    schemaVersion: 1,
                    kind: "http-runtime-binding",
                    sourceOrigin: "linux-procfs",
                    runtimeDigest: "a".repeat(64),
                    driver: {
                        imageId: "sha256:" + "d".repeat(64),
                        pythonExecutableSha256: "7".repeat(64),
                        pythonVersion: "3.12.3",
                        pythonImplementation: "cpython",
                    },
                    node: { executableSha256: OFFICIAL_NODE, version: "v22.14.0", uv: "1.49.2" },
                    proofDigest: "b".repeat(64),
                },
                verification: { verified: true, status: "verified" },
            },
        },
    };
    updateCollector(f);
    updateLocks(f);
    return f;
}
function runtimeFixture(f = fixture()) {
    const file = (path, sha256, bytes = 1000) => ({ path, sha256, bytes, type: "file", mode: 292 });
    const runtime = {
        schemaVersion: 1,
        kind: "http-comparison-runtime-plan",
        planDigest: "d".repeat(64),
        planContentDigest: "e".repeat(64),
        componentSha256: "f".repeat(64),
        settings: { hostPython: "/usr/bin/python3.12", driver: { image: f.external.peer.imageId } },
        identities: {
            source: [
                ...Object.entries(sources).map(([name, sha]) => file("components/http/" + name, sha)),
                file("components/http/comparison-workflow.py", "f".repeat(64)),
            ],
            node: [file("bin/node", OFFICIAL_NODE, 120177224)],
            hostPython: { path: "/usr/bin/python3.12", bytes: 10000, sha256: f.external.collector.executableSha256 },
            dockerExecutable: {},
            javaHome: [],
            distribution: [],
        },
    };
    function seal() {
        const runtimeRaw = JSON.stringify(runtime) + "\n";
        const lockRaw =
            JSON.stringify({
                schemaVersion: 1,
                kind: "http-comparison-runtime-lock",
                planDigest: runtime.planDigest,
                runtime: { path: "runtime.json", bytes: Buffer.byteLength(runtimeRaw), sha256: SHA(runtimeRaw) },
            }) + "\n";
        f.external.experimentRuntime = { runtimeRaw, lockRaw };
        f.external.runtimeDigest = SHA(lockRaw);
    }
    seal();
    return { f, runtime, seal };
}

test("new experiment byte-locked inventory supplies runner sources independently of older probe selection", () => {
    const { f } = runtimeFixture();
    const result = associatePhaseHost(f);
    assert.equal(result.gates.experimentRuntime.status, "passed");
    assert.equal(result.gates.componentSources.status, "passed");
    assert.deepEqual(result.pendingGates, ["runtimeProof"]);
    assert.notEqual(result.bindings.runtimeDigest, result.bindings.probedRuntimeDigest);
    assert.equal(result.hostAssociation, "unverified");
    assert.equal(result.phases[0].hostIntervals, null);
    for (const mutate of [
        (r) => {
            r.settings.driver.image = "sha256:" + "0".repeat(64);
        },
        (r) => {
            r.identities.node[0].sha256 = "0".repeat(64);
        },
        (r) => {
            r.identities.node[0].bytes++;
        },
        (r) => {
            r.identities.hostPython.sha256 = "0".repeat(64);
        },
        (r) => {
            r.componentSha256 = "0".repeat(64);
        },
    ]) {
        const { f, runtime, seal } = runtimeFixture();
        mutate(runtime);
        seal();
        assert.equal(associatePhaseHost(f).gates.experimentRuntime.status, "failed");
    }
    const changedSource = runtimeFixture();
    changedSource.runtime.identities.source[0].sha256 = "0".repeat(64);
    changedSource.seal();
    assert.equal(associatePhaseHost(changedSource.f).gates.componentSources.status, "failed");
    const duplicate = runtimeFixture();
    duplicate.runtime.identities.source.push({ ...duplicate.runtime.identities.source[0] });
    duplicate.seal();
    assert.throws(() => associatePhaseHost(duplicate.f), /INVENTORY_PATH/);
    const changedRuntimeBytes = runtimeFixture().f;
    changedRuntimeBytes.external.experimentRuntime.runtimeRaw += "\n";
    assert.throws(() => associatePhaseHost(changedRuntimeBytes), /RUNTIME_DERIVATION/);
    const changedLockBytes = runtimeFixture().f;
    changedLockBytes.external.experimentRuntime.lockRaw += "\n";
    assert.equal(associatePhaseHost(changedLockBytes).gates.experimentRuntime.status, "failed");
});

test("pure candidate retains exact phase intervals and cannot promote a hand-written runtime attestation", () => {
    const f = fixture(),
        original = structuredClone(f),
        result = associatePhaseHost(f);
    assert.equal(validateDirectPhaseClockRaw(f.run, f.profile).rawValidated, true);
    assert.equal(verifyRunnerSanityRaw(f.run.clockReference).rawSanity, "passed");
    assert.deepEqual(f, original);
    assert.equal(result.status, "unverified");
    assert.equal(result.verified, false);
    assert.equal(result.hostAssociation, "unverified");
    assert.equal(result.performanceAccepted, false);
    assert.equal(result.runtimeProofVerifierConnected, true);
    assert.deepEqual(result.pendingGates, ["experimentRuntime", "componentSources", "runtimeProof"]);
    assert.ok(result.gates.runtimeProof.pending.includes("runtime-source-unverified"));
    assert.ok(Object.values(result.gates).every((g) => g.failures.length === 0));
    const measurement = result.phases[1];
    assert.deepEqual(measurement.startNs, ["10101000000", "10101001000"]);
    assert.deepEqual(measurement.endNs, ["10301000000", "10301001000"]);
    assert.deepEqual(measurement.outerNs, ["10101000000", "10301001000"]);
    assert.deepEqual(measurement.certainInnerNs, ["10101001000", "10301000000"]);
    assert.deepEqual(measurement.durationNs, ["199999000", "200001000"]);
    assert.equal(measurement.hostIntervals, null);
    assert.equal(measurement.clockDomain, "node-hrtime-local-candidate");
    assert.equal(result.performanceOrigin.compatible, true);
    assert.notEqual(
        f.run.phaseClock.identityBefore.processToken,
        JSON.parse(f.run.clockReference.frames[4].raw).anchor.identityBefore.processToken,
    );
    result.phases[0].outerNs[0] = "0";
    assert.deepEqual(f, original);
});

test("peer 3.12.15 and host 3.12.3 independently bind reviewed source contracts and an unknown version stays pending", () => {
    const f = fixture();
    for (const index of [1, 11])
        changeFrame(f.run, index, (v) => {
            v.identity.python.version = "3.12.15 (synthetic image) [GCC]";
        });
    f.external.peer.version = "3.12.15";
    f.external.pythonClockSources.peer.version = "3.12.15";
    f.external.pythonClockSources.peer.timeModuleSha256 =
        "81ce33b3f784a9abae1663576424faabb9c584d84f36daa4c27d0e16195c76b0";
    f.external.runtimeBinding.raw.driver.pythonVersion = "3.12.15";
    updateLocks(f);
    const result = associatePhaseHost(f);
    assert.equal(result.gates.peerRuntime.status, "passed");
    assert.equal(result.gates.collectorRuntime.status, "passed");
    assert.equal(result.gates.pythonSourceContracts.status, "passed");
    assert.equal(result.gates.runtimeProof.failures.length, 0);
    assert.notEqual(result.bindings.runtimeDigest, result.bindings.probedRuntimeDigest);
    assert.equal(result.verified, false);
    const wrongSource = structuredClone(f);
    wrongSource.external.pythonClockSources.peer.timeModuleSha256 =
        wrongSource.external.pythonClockSources.collector.timeModuleSha256;
    assert.equal(associatePhaseHost(wrongSource).gates.pythonSourceContracts.status, "failed");
    for (const index of [1, 11])
        changeFrame(f.run, index, (v) => {
            v.identity.python.version = "3.12.16 (unreviewed synthetic) [GCC]";
        });
    f.external.peer.version = "3.12.16";
    f.external.pythonClockSources.peer.version = "3.12.16";
    f.external.runtimeBinding.raw.driver.pythonVersion = "3.12.16";
    updateLocks(f);
    const unknown = associatePhaseHost(f);
    assert.equal(unknown.gates.pythonSourceContracts.status, "pending");
    assert.ok(unknown.gates.pythonSourceContracts.pending.includes("peer:source-contract-pending"));
});

test("every predeclared width gate retains bad observations rather than selecting or averaging anchors", () => {
    for (const [key, value, marker] of [
        ["maxBoundaryWidthNs", "999", "boundary-width"],
        ["maxRoundTripNs", "199999", "roundtrip-width"],
        ["maxAnchorWidthNs", "999", "anchor-width"],
    ]) {
        const f = fixture();
        f.external.quality[key] = value;
        const result = associatePhaseHost(f);
        assert.equal(result.gates.quality.status, "failed");
        assert.ok(result.gates.quality.failures.some((v) => v.includes(marker)));
        assert.equal(result.roundtrips.length, 2);
        assert.equal(result.phases.length, 2);
        assert.equal(result.performanceAccepted, false);
    }
    const missing = fixture();
    delete missing.external.quality.maxRoundTripNs;
    const result = associatePhaseHost(missing);
    assert.equal(result.gates.quality.status, "pending");
    assert.ok(result.gates.quality.pending.includes("maxRoundTripNs"));
});

test("fully resealed causal time forgery and incompatible performance origins remain ineligible", () => {
    for (const [index, patch] of [
        [
            5,
            (r) => {
                r.receivedMonotonicNs = "10002000000";
            },
        ],
        [
            5,
            (r) => {
                r.sentMonotonicNs = "9980000000";
            },
        ],
        [
            9,
            (r) => {
                r.sentMonotonicNs = "10300000000";
            },
        ],
        [
            9,
            (r) => {
                r.receivedMonotonicNs = "10303000000";
            },
        ],
    ]) {
        const f = fixture();
        changeFrame(f.run, index, patch);
        updateLocks(f);
        assert.equal(validateDirectPhaseClockRaw(f.run, f.profile).rawValidated, true);
        const result = associatePhaseHost(f);
        assert.equal(result.gates.causalEnclosure.status, "failed");
        assert.equal(result.verified, false);
    }
    const f = fixture();
    for (const phase of f.run.phases) {
        phase.clock.start.performanceMs += 0.25;
        phase.clock.end.performanceMs += 0.25;
        phase.elapsedMs = phase.clock.end.performanceMs - phase.clock.start.performanceMs;
    }
    updateLocks(f);
    assert.equal(validateDirectPhaseClockRaw(f.run, f.profile).rawValidated, true);
    assert.equal(associatePhaseHost(f).gates.performanceOrigin.status, "failed");
});

test("large integer-cast then division rounding retains the true origin and exact nanosecond strings", () => {
    const f = fixture(),
        delta = 500000000000000032n,
        realOrigin = 9000000000n;
    // At this offset treating performance.now() as a single rounded division
    // would wrongly narrow the origin to [9000000001,9000000003] ns.
    for (const index of [4, 8])
        changeFrame(f.run, index, (v) => {
            v.anchor.hrtimeBeforeNs = (BigInt(v.anchor.hrtimeBeforeNs) + delta).toString();
            v.anchor.hrtimeAfterNs = (BigInt(v.anchor.hrtimeBeforeNs) + 1n).toString();
            v.anchor.performanceMs = Number(BigInt(v.anchor.hrtimeBeforeNs) - realOrigin) / 1e6;
        });
    for (const index of [5, 9])
        changeFrame(f.run, index, (v) => {
            v.sentMonotonicNs = (BigInt(v.sentMonotonicNs) + delta).toString();
            v.receivedMonotonicNs = (BigInt(v.receivedMonotonicNs) + delta).toString();
        });
    for (const phase of f.run.phases) {
        for (const boundary of [phase.clock.start, phase.clock.end]) {
            boundary.hrtimeBeforeNs = (BigInt(boundary.hrtimeBeforeNs) + delta).toString();
            boundary.hrtimeAfterNs = (BigInt(boundary.hrtimeBeforeNs) + 1n).toString();
            boundary.performanceMs = Number(BigInt(boundary.hrtimeBeforeNs) - realOrigin) / 1e6;
        }
        phase.elapsedMs = phase.clock.end.performanceMs - phase.clock.start.performanceMs;
    }
    for (const key of ["preparedIntervalNs", "finishedIntervalNs"])
        f.run.phaseClock[key] = f.run.phaseClock[key].map((v) => (BigInt(v) + delta).toString());
    const s = f.collector.summary;
    for (const capture of [s.clockIdentity.before, s.clockIdentity.after])
        for (const key of ["startedMonotonicNs", "finishedMonotonicNs"])
            capture[key] = (BigInt(capture[key]) + delta).toString();
    for (const key of [
        "startedMonotonicNs",
        "finishedMonotonicNs",
        "firstSampleStartedMonotonicNs",
        "lastSampleFinishedMonotonicNs",
    ])
        s[key] = (BigInt(s[key]) + delta).toString();
    updateCollector(f);
    updateLocks(f);
    const result = associatePhaseHost(f);
    assert.equal(result.performanceOrigin.compatible, true);
    assert.ok(BigInt(result.performanceOrigin.originIntervalNs[0]) <= realOrigin);
    assert.ok(realOrigin <= BigInt(result.performanceOrigin.originIntervalNs[1]));
    assert.equal(result.gates.causalEnclosure.status, "passed");
    assert.equal(result.phases[1].outerNs[0], "500000010101000032");
    assert.equal(result.verified, false);
});

test("collector results are rebuilt and namespace, coverage and external contents are independently checked", () => {
    const f = fixture();
    f.collector.verification.clockIdentity.identity.pid++;
    assert.throws(() => associatePhaseHost(f), /COLLECTOR_RAW_REBUILD/);
    const raw = fixture();
    raw.collector.summary.clockIdentity.before.timeOffsets.monotonic.seconds = "1";
    assert.throws(() => associatePhaseHost(raw), /OFFSETS_REBUILD/);
    const namespace = fixture();
    for (const capture of [
        namespace.collector.summary.clockIdentity.before,
        namespace.collector.summary.clockIdentity.after,
    ])
        capture.timeNamespace = "time:[12345]";
    updateCollector(namespace);
    updateLocks(namespace);
    assert.equal(associatePhaseHost(namespace).gates.hostNamespace.status, "failed");
    const coverage = fixture();
    coverage.collector.summary.firstSampleStartedMonotonicNs = "10000000000";
    updateCollector(coverage);
    updateLocks(coverage);
    assert.equal(associatePhaseHost(coverage).gates.collectorCoverage.status, "failed");
    const expected = fixture();
    expected.external.runDigest = "0".repeat(64);
    expected.external.node.executableSha256 = "0".repeat(64);
    expected.external.sources["phase-clock-reference.py"] = "0".repeat(64);
    expected.external.collector.executableSha256 = "0".repeat(64);
    const result = associatePhaseHost(expected);
    for (const key of ["contentLocks", "nodeRuntime", "componentSources", "collectorRuntime"])
        assert.equal(result.gates[key].status, "failed");
});

test("profile binding, deleted raw blocks and asserted producer verification cannot bypass independent raw validators", () => {
    for (const mutate of [
        (f) => {
            delete f.run.clockReference;
            delete f.run.phaseClock.clockReference;
            account(f.run);
        },
        (f) => {
            delete f.run.phaseClock;
        },
        (f) => {
            f.profile.name = "other-profile";
        },
        (f) => {
            f.run.clockReference.verified = true;
        },
        (f) => {
            f.run.phaseClock.verified = true;
            account(f.run);
        },
        (f) => {
            delete f.run.phases[0].clock;
            account(f.run);
        },
    ]) {
        const f = fixture();
        mutate(f);
        assert.throws(() => associatePhaseHost(f));
    }
    const f = fixture();
    f.external.runtimeBinding = f.external.runtimeBinding.raw;
    assert.equal(associatePhaseHost(f).gates.runtimeProof.status, "pending");
    f.external = {};
    const missing = associatePhaseHost(f);
    assert.equal(missing.status, "unverified");
    assert.equal(missing.verified, false);
    assert.ok(missing.pendingGates.includes("contentLocks"));
    delete f.collector;
    assert.equal(associatePhaseHost(f).gates.collectorRaw.status, "pending");
});

test("invalid expectation fields and numeric quality limits fail explicitly instead of silently defaulting", () => {
    for (const mutate of [
        (f) => {
            f.external.verified = true;
        },
        (f) => {
            f.external.quality.maxRoundTripNs = 500000;
        },
        (f) => {
            f.external.peer.imageId = "ubuntu:latest";
        },
        (f) => {
            f.external.runtimeBinding.raw.driver.untrusted = true;
        },
        (f) => {
            f.external.pythonClockSources.peer.pytimeSha256 = "invalid";
        },
    ]) {
        const f = fixture();
        mutate(f);
        assert.throws(() => associatePhaseHost(f), /PHASE_HOST_/);
    }
});
