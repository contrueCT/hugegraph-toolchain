/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, mkdir, readFile, writeFile, cp, rm, access, symlink, chmod, readdir } from "node:fs/promises";
import { createHash } from "node:crypto";
import { execFile, spawn } from "node:child_process";
import { promisify } from "node:util";
import { dirname, join, resolve } from "node:path";
import { tmpdir } from "node:os";
import { pathToFileURL } from "node:url";
import { verifyJvmEvidence, verifyJvmClockIdentity } from "../components/http/jvm-evidence.mjs";

// Every JDK, procfs and server receipt here is a synthetic offline fixture. No Java or Docker is started.
const python = process.env.HGBENCH_PYTHON || "python3",
    parser = resolve("components/http/jvm-observation.py");
const hash = (raw) => createHash("sha256").update(raw).digest("hex");
const encoded = (value) => Buffer.from(JSON.stringify(value, null, 2) + "\n");
const read = async (root, path) => JSON.parse(await readFile(join(root, path), "utf8"));
const put = (root, path, value) => writeFile(join(root, path), encoded(value));
const file = async (root, path) => {
    const raw = await readFile(join(root, path));
    return { path, bytes: raw.length, sha256: hash(raw) };
};
const boot = "12345678-1234-1234-1234-123456789abc";
const b64 = (value) => Buffer.from(value).toString("base64");
function statLine(start = "456", pid = 123) {
    const tail = ["S", ...Array(18).fill("0"), start];
    return `${pid} (fixture java) ${tail.join(" ")}\n`;
}
function log(tags, body, uptime) {
    return `[${9000000000000000n + BigInt(uptime)}ns][${uptime}ns][123][124][debug][${tags}] ${body}\n`;
}
async function invoke(args) {
    try {
        return await promisify(execFile)(python, ["-I", "-B", parser, ...args], { timeout: 10000, maxBuffer: 65536 });
    } catch (error) {
        if (error.code === 2) return error;
        throw error;
    }
}
async function parse(f) {
    await invoke([
        "parse",
        "--plan",
        join(f.root, "jvm-observation"),
        "--raw",
        join(f.root, "work/server.log"),
        "--launch",
        join(f.root, "work/jvm-launch.json"),
        "--capture",
        join(f.root, "work/jvm-capture.json"),
        "--out",
        f.parsed,
    ]);
}
async function fixture(t, { eof = true, events = true } = {}) {
    const temp = await mkdtemp(join(tmpdir(), "jvm-evidence-test-")),
        root = join(temp, "environment"),
        parsed = join(temp, "parsed");
    t.after(() => rm(temp, { recursive: true, force: true }));
    await mkdir(root);
    await mkdir(join(root, "work"));
    await cp(parser, join(root, "jvm-observation.py"));
    const spec = {
        schemaVersion: 1,
        kind: "http-jvm-observation-spec",
        preset: "hotspot17-gc-jit-v1",
        capture: "supervisor-stdout",
        maxRawBytes: 2 * 1024 * 1024,
        windowMs: 1000,
        parser: {
            maxLineBytes: 4096,
            maxEvents: 10000,
            maxWindows: 100,
            maxOutputBytes: 8 * 1024 * 1024,
            timeoutMs: 1000,
        },
    };
    await put(root, "input.json", spec);
    await invoke(["plan", "--spec", join(root, "input.json"), "--out", join(root, "jvm-observation")]);
    const observation = await read(root, "jvm-observation/plan.json"),
        planSha256 = (await file(root, "jvm-observation/plan.json")).sha256;
    const jdk = {
        javaSha256: hash(Buffer.from("fixture-java")),
        libjvmSha256: hash(Buffer.from("fixture-libjvm")),
        version: "17.0.17+10",
    };
    const settings = {
        jvmObservation: spec,
        jvm: { heapMinBytes: 512, heapMaxBytes: 1024, maxDirectMemoryBytes: 256, activeProcessorCount: 2 },
    };
    const env = {
        schemaVersion: 1,
        kind: "hugegraph-http-environment-plan",
        attemptId: "a".repeat(32),
        settings,
        classpath: ["lib/fixture.jar"],
        jvmObservation: { planDirectory: "jvm-observation", planSha256, jdk },
        controlFiles: await Promise.all(
            [
                "jvm-observation.py",
                "jvm-observation/spec.json",
                "jvm-observation/plan.json",
                "jvm-observation/plan.lock.json",
            ].map((p) => file(root, p)),
        ),
        sources: {
            javaHome: {
                files: [
                    { path: "bin/java", type: "file", bytes: 12, sha256: jdk.javaSha256 },
                    { path: "lib/server/libjvm.so", type: "file", bytes: 14, sha256: jdk.libjvmSha256 },
                ],
            },
        },
    };
    await put(root, "plan.json", env);
    const envSha = (await file(root, "plan.json")).sha256;
    await put(root, "plan.lock.json", { schemaVersion: 1, attemptId: env.attemptId, planSha256: envSha });
    const launch = {
        schemaVersion: 1,
        kind: "http-jvm-observation-launch",
        planSha256,
        argv: [
            "/opt/jdk/bin/java",
            "-Xms512",
            "-Xmx1024",
            "-XX:MaxDirectMemorySize=256",
            "-XX:ActiveProcessorCount=2",
            "@/opt/hugegraph/bin/jvm-module.options",
            "-Djava.io.tmpdir=/tmp",
            "-Dlog4j.configurationFile=/control/conf/log4j2.xml",
            ...observation.javaArguments,
            "-Djava.security.properties=/control/conf/java-security.properties",
            "-cp",
            "/opt/hugegraph/lib/fixture.jar",
            "org.apache.hugegraph.bootstrap.HugeGraphServerBootstrap",
            "true",
            "/control/conf/gremlin-server.yaml",
            "/control/conf/rest-server.properties",
        ],
        process: { pid: 123, startTimeTicks: "456", hostBootId: boot },
        jdk,
    };
    await put(root, "work/jvm-launch.json", launch);
    const launchSha = (await file(root, "work/jvm-launch.json")).sha256;
    const runtime = {
        schemaVersion: 1,
        kind: "http-jvm-observation-runtime",
        planSha256,
        environmentPlanSha256: envSha,
        attemptId: env.attemptId,
        launchSha256: launchSha,
        observedBeforeMonotonicNs: "10",
        observedAfterMonotonicNs: "20",
        process: {
            pid: 123,
            startTimeTicksBefore: "456",
            startTimeTicksAfter: "456",
            hostBootId: boot,
            cmdlineBase64: b64(launch.argv.join("\0") + "\0"),
            statBeforeBase64: b64(statLine()),
            statAfterBase64: b64(statLine()),
            mapsBase64: b64("1000-2000 r-xp 0000 08:01 1234 /opt/jdk/lib/server/libjvm.so\n"),
            executablePath: "/opt/jdk/bin/java",
            java: { deviceMajor: 8, deviceMinor: 1, inode: "1235", bytes: 12, sha256: jdk.javaSha256 },
            libjvm: { deviceMajor: 8, deviceMinor: 1, inode: "1234", bytes: 14, sha256: jdk.libjvmSha256 },
        },
        clock: { phaseMapping: "unverified" },
    };
    await put(root, "work/jvm-runtime.json", runtime);
    const raw = Buffer.from(
        events
            ? log("gc,start", "GC(0) Pause Young (Normal) (G1 Evacuation Pause)", 1000000000) +
                  log("gc", "GC(0) Pause Young (Normal) (G1 Evacuation Pause) 64M->8M(128M) 1.125ms", 1001125000) +
                  log("jit,compilation", "  12 %     4       fixture.Main::work @ 3 (18 bytes)", 2000000000) +
                  log(
                      "jit,compilation",
                      "  16     n 0       java.lang.System::arraycopy (native)   (static)",
                      2001000000,
                  )
            : "ordinary application line\n",
    );
    await writeFile(join(root, "work/server.log"), raw);
    const capture = {
        schemaVersion: 1,
        kind: "http-jvm-observation-capture",
        planSha256,
        launchSha256: launchSha,
        raw: { bytes: raw.length, sha256: hash(raw) },
        startedMonotonicNs: "1",
        finishedMonotonicNs: "99",
        eof,
        capHit: false,
        forcedKill: false,
        cleanupComplete: true,
        error: null,
        readBytes: raw.length,
        retainedBytes: raw.length,
        discardedBytes: 0,
    };
    await put(root, "work/jvm-capture.json", capture);
    const {
        planSha256: ignoredPlan,
        launchSha256: ignoredLaunch,
        raw: ignoredRaw,
        cleanupComplete: ignoredCleanup,
        ...rest
    } = capture;
    await put(root, "work/server-capture.json", {
        ...rest,
        kind: "hugegraph-http-pipe-capture",
        stream: "stdout+stderr",
        sha256: hash(raw),
        complete: eof,
    });
    await put(root, "work/service-result.json", {
        attemptId: env.attemptId,
        planSha256: envSha,
        outcomes: [{ phase: "server", termination: { cleanupComplete: true, killSent: false } }],
    });
    const context = {
        settings,
        environmentPlanSha256: envSha,
        hostBootId: boot,
        processIdentity: { pid: 123, startTimeTicks: "456", bootId: boot, javaExecutableSha256: jdk.javaSha256 },
    };
    const f = { temp, root, parsed, context };
    await parse(f);
    return f;
}
async function copy(f, name) {
    const target = join(f.temp, name);
    await cp(f.root, join(target, "environment"), { recursive: true });
    await cp(f.parsed, join(target, "parsed"), { recursive: true });
    return {
        ...f,
        root: join(target, "environment"),
        parsed: join(target, "parsed"),
        context: structuredClone(f.context),
    };
}
async function relockParsed(f) {
    const manifest = await read(f.parsed, "manifest.json");
    for (const entry of manifest.files) Object.assign(entry, await file(f.parsed, entry.path));
    await put(f.parsed, "manifest.json", manifest);
}

function clockFixture(runtime, launch) {
    const ns = "time:[4026531834]",
        rawOffsets = "monotonic 0 0\nboottime 0 0\n";
    const initial = {
        domain: "linux-clock-monotonic",
        alignment: "unverified",
        namespace: { available: true, value: ns, reason: null },
        timeForChildren: { available: true, value: ns, reason: null },
        offsets: { available: true, value: rawOffsets, reason: null },
    };
    runtime.clock = { capture: structuredClone(initial), jvm: structuredClone(initial), phaseMapping: "unverified" };
    const java = runtime.process.java;
    function executable(role, digest) {
        return {
            sourcePath: role === "capture" ? "/proc/self/exe" : "/proc/123/exe",
            path: role === "capture" ? "/usr/bin/python3" : "/opt/jdk/bin/java",
            metadata: {
                bytes: role === "capture" ? "30" : String(java.bytes),
                deviceMajor: java.deviceMajor,
                deviceMinor: java.deviceMinor,
                inode: role === "capture" ? "5678" : java.inode,
                mtimeNs: "1",
                ctimeNs: "2",
            },
            sha256: digest ? "b".repeat(64) : null,
            digestRead: digest,
        };
    }
    function snapshot(start, pid, digest) {
        const base = { bootId: boot, timeNamespace: ns, timeForChildrenNamespace: ns, timeOffsetsRaw: rawOffsets };
        return {
            sourceOrigin: "linux-procfs",
            captureComponentSha256: "c".repeat(64),
            startedMonotonicNs: String(start),
            finishedMonotonicNs: String(start + 10),
            unavailable: {},
            capture: {
                ...base,
                pid,
                startTimeTicks: "42",
                startTimeTicksAfter: "42",
                statRaw: statLine("42", pid),
                statAfterRaw: statLine("42", pid),
                executable: executable("capture", digest),
                python: {
                    reportedExecutable: "/usr/bin/python3",
                    version: "3.11.9 fixture",
                    implementation: "cpython",
                },
                clock: {
                    api: "time.monotonic_ns",
                    implementation: "clock_gettime(CLOCK_MONOTONIC)",
                    monotonic: true,
                    adjustable: false,
                    resolutionSeconds: 1e-9,
                },
            },
            jvm: {
                ...base,
                pid: 123,
                startTimeTicks: "456",
                startTimeTicksAfter: "456",
                statRaw: statLine(),
                statAfterRaw: statLine(),
                executable: executable("jvm", false),
            },
        };
    }
    function probe(start, pid) {
        const identity = {
            pid: 123,
            startTimeTicks: "456",
            bootId: boot,
            javaExecutable: "/opt/jdk/bin/java",
            javaExecutableSha256: launch.jdk.javaSha256,
        };
        return {
            schemaVersion: 1,
            kind: "hugegraph-http-mode-probe",
            componentSha256: "a".repeat(64),
            startedMonotonicNs: start - 10,
            finishedMonotonicNs: start + 40,
            identityBefore: structuredClone(identity),
            identityAfter: structuredClone(identity),
            clockIdentity: {
                schemaVersion: 1,
                kind: "http-jvm-mode-clock-identity",
                sourceOrigin: "linux-procfs",
                captureComponentSha256: "c".repeat(64),
                before: snapshot(start, pid, true),
                after: snapshot(start + 20, pid, false),
                status: "observed",
                changedFields: [],
                verified: false,
                hostAssociation: "unverified",
                executableHashMaxBytes: 268435456,
                executableHashMaxNs: "2000000000",
            },
        };
    }
    return {
        clockExpected: {
            modeProbes: { pre: probe(100, 501), post: probe(200, 502) },
            modeProbeComponentSha256: "a".repeat(64),
            captureComponentSha256: "c".repeat(64),
            containerPythonExecutableSha256: "b".repeat(64),
            containerPythonVersion: "3.11.9 fixture",
            containerPythonImplementation: "cpython",
            jdkMonotonicSource: {
                kind: "http-jdk-monotonic-source-expectation",
                fixedSourceRevision: "30ef840c3736270330fc0a26849c2456406facfe",
                sourceDigests: ["c".repeat(64)],
                javaExecutableSha256: launch.jdk.javaSha256,
                libjvmSha256: launch.jdk.libjvmSha256,
                clockApi: "CLOCK_MONOTONIC",
            },
        },
    };
}

function initialCaptureFixture(runtime, launch) {
    const context = clockFixture(runtime, launch),
        point = structuredClone(context.clockExpected.modeProbes.pre.clockIdentity.before);
    point.startedMonotonicNs = "30";
    point.finishedMonotonicNs = "40";
    point.capture.pid = 503;
    point.capture.statRaw = point.capture.statAfterRaw = statLine("42", 503);
    const javaStat = statLine().replace(" S 0 ", " S 503 ");
    point.jvm.statRaw = point.jvm.statAfterRaw = javaStat;
    runtime.process.statBeforeBase64 = runtime.process.statAfterBase64 = b64(javaStat);
    for (const probe of Object.values(context.clockExpected.modeProbes))
        for (const point of [probe.clockIdentity.before, probe.clockIdentity.after])
            point.jvm.statRaw = point.jvm.statAfterRaw = javaStat;
    runtime.observedAfterMonotonicNs = "50";
    runtime.clock.observedBeforeMonotonicNs = "25";
    runtime.clock.observedAfterMonotonicNs = "45";
    runtime.captureIdentity = {
        schemaVersion: 1,
        kind: "http-jvm-initial-clock-identity",
        sourceOrigin: "linux-procfs",
        captureComponentSha256: "c".repeat(64),
        snapshot: point,
        verified: false,
        hostAssociation: "unverified",
        executableHashMaxBytes: 268435456,
        executableHashMaxNs: "2000000000",
    };
    return context;
}

test("initial actual capture Python raw identity and full read brackets bind the same JVM parent", async (t) => {
    const f = await fixture(t),
        runtime = await read(f.root, "work/jvm-runtime.json"),
        launch = await read(f.root, "work/jvm-launch.json"),
        context = initialCaptureFixture(runtime, launch);
    const result = verifyJvmClockIdentity(runtime, launch, context);
    assert.equal(result.rawStatus, "raw-consistent");
    assert.equal(result.bindings.initialCapture, "raw-consistent");
    assert.equal(result.bindings.initialClockWindow, "bounded");
    assert(!result.pendingGates.includes("initial-clock-read-window"));
    assert(result.pendingGates.includes("independent-container-runtime-binding"));
    assert(result.pendingGates.includes("loaded-libjvm-window-identity"));
    assert.equal(result.verified, false);
});

test("initial capture unknown or resealed process/source/window contradictions cannot gain a domain", async (t) => {
    const f = await fixture(t),
        original = await read(f.root, "work/jvm-runtime.json"),
        launch = await read(f.root, "work/jvm-launch.json");
    const runtime = structuredClone(original),
        context = initialCaptureFixture(runtime, launch);
    runtime.captureIdentity.snapshot.unavailable["capture.executable"] = "read-unavailable";
    runtime.captureIdentity.snapshot.capture.executable = null;
    assert.equal(verifyJvmClockIdentity(runtime, launch, context).hostDomain, "unverified");
    for (const [name, mutate, expected] of [
        [
            "helper",
            (r) => {
                r.captureIdentity.snapshot.captureComponentSha256 = r.captureIdentity.captureComponentSha256 =
                    "d".repeat(64);
            },
            /JVM_CLOCK_CAPTURE_COMPONENT_BINDING/,
        ],
        [
            "python",
            (r) => {
                r.captureIdentity.snapshot.capture.executable.sha256 = "d".repeat(64);
            },
            /JVM_INITIAL_CAPTURE_EXECUTABLE/,
        ],
        [
            "python-ctime",
            (r) => {
                r.captureIdentity.snapshot.capture.executable.metadata.ctimeNs = "3";
            },
            /JVM_INITIAL_CAPTURE_EXECUTABLE/,
        ],
        [
            "namespace",
            (r) => {
                r.captureIdentity.snapshot.capture.timeNamespace = "time:[3]";
            },
            /JVM_INITIAL_CAPTURE_DOMAIN/,
        ],
        [
            "PPID",
            (r) => {
                r.captureIdentity.snapshot.jvm.statRaw = statLine().replace(" S 0 ", " S 504 ");
            },
            /JVM_INITIAL_CAPTURE_PARENT/,
        ],
        [
            "read-finish",
            (r) => {
                r.clock.observedAfterMonotonicNs = "39";
            },
            /JVM_INITIAL_CLOCK_READ_WINDOW/,
        ],
        [
            "runtime-finish",
            (r) => {
                r.observedAfterMonotonicNs = "44";
            },
            /JVM_INITIAL_CLOCK_READ_WINDOW/,
        ],
        [
            "read-start",
            (r) => {
                r.clock.observedBeforeMonotonicNs = "31";
            },
            /JVM_INITIAL_CLOCK_READ_WINDOW/,
        ],
    ])
        await t.test(name, () => {
            const r = structuredClone(original),
                c = initialCaptureFixture(r, launch);
            mutate(r);
            assert.throws(() => verifyJvmClockIdentity(r, launch, c), expected);
        });
});

test("JVM clock raw identities bind initial, pre/post and remain candidate with independent gates pending", async (t) => {
    const f = await fixture(t),
        runtime = await read(f.root, "work/jvm-runtime.json"),
        launch = await read(f.root, "work/jvm-launch.json"),
        context = clockFixture(runtime, launch);
    const result = verifyJvmClockIdentity(runtime, launch, context);
    assert.equal(result.rawStatus, "raw-consistent");
    assert.equal(result.hostDomain, "candidate");
    assert.equal(result.verified, false);
    assert.equal(result.performanceAccepted, false);
    assert(result.pendingGates.includes("jdk-binary-to-fixed-source-mapping"));
    assert(result.pendingGates.includes("initial-capture-runtime-source"));
    assert(result.pendingGates.includes("independent-container-runtime-binding"));
    assert(result.pendingGates.includes("host-collector-domain-binding"));
    assert.equal(result.bindings.jdkMonotonicSource, "matched-expectation");
    assert.equal(result.domain.timeOffsets.monotonic.seconds, "0");
    const missingHelperLock = structuredClone(context);
    delete missingHelperLock.clockExpected.captureComponentSha256;
    const unlocked = verifyJvmClockIdentity(runtime, launch, missingHelperLock);
    assert.equal(unlocked.bindings.captureComponentSha256, "unknown");
    assert(unlocked.pendingGates.includes("captureComponentSha256"));
    assert.equal(unlocked.verified, false);
    // Mode pre and post run as distinct Python processes; dynamic stat values are also allowed to change.
    context.clockExpected.modeProbes.post.clockIdentity.after.jvm.statRaw = statLine().replace(" S ", " R ");
    assert.equal(verifyJvmClockIdentity(runtime, launch, context).rawStatus, "raw-consistent");
    await put(f.root, "work/jvm-runtime.json", runtime);
    const integrated = await verifyJvmEvidence(f.root, f.parsed, { ...f.context, ...context });
    assert.equal(integrated.clockIdentity.hostDomain, "candidate");
    assert.equal(integrated.clockAlignment, "unverified");
    assert.equal(integrated.phaseAttribution, "unavailable");
});

test("JVM clock missing/failed reads remain unknown independently of mode status", async (t) => {
    const f = await fixture(t),
        runtime = await read(f.root, "work/jvm-runtime.json"),
        launch = await read(f.root, "work/jvm-launch.json"),
        context = clockFixture(runtime, launch);
    assert.equal(verifyJvmClockIdentity(runtime, launch).rawStatus, "unknown");
    const point = context.clockExpected.modeProbes.pre.clockIdentity.before;
    point.unavailable["jvm.timeOffsetsRaw"] = "clock-time-offsets-malformed";
    point.jvm.timeOffsetsRaw = null;
    context.clockExpected.modeProbes.pre.clockIdentity.status = "unknown";
    context.clockExpected.modeProbes.pre.clockIdentity.changedFields = [];
    assert.equal(verifyJvmClockIdentity(runtime, launch, context).hostDomain, "unverified");
    assert.equal(context.clockExpected.modeProbes.pre.identityBefore.startTimeTicks, "456");
});

test("JVM clock fixture origin and unavailable startup fields cannot become a production candidate", async (t) => {
    const f = await fixture(t),
        runtime = await read(f.root, "work/jvm-runtime.json"),
        launch = await read(f.root, "work/jvm-launch.json"),
        context = clockFixture(runtime, launch);
    const value = context.clockExpected.modeProbes.pre.clockIdentity;
    value.sourceOrigin = "fixture";
    value.captureComponentSha256 = null;
    value.status = "unknown";
    for (const point of [value.before, value.after]) {
        point.sourceOrigin = "fixture";
        point.captureComponentSha256 = null;
        point.capture = null;
        point.jvm = null;
        point.unavailable = { source: "fixture-has-no-production-clock-identity" };
    }
    assert.equal(verifyJvmClockIdentity(runtime, launch, context).rawStatus, "unknown");
    value.sourceOrigin = "linux-procfs";
    assert.throws(() => verifyJvmClockIdentity(runtime, launch, context), /JVM_CLOCK_SNAPSHOT_WINDOW/);
    const complete = clockFixture(runtime, launch);
    runtime.clock.jvm.namespace = { available: false, value: null, reason: "read-unavailable" };
    assert.equal(verifyJvmClockIdentity(runtime, launch, complete).hostDomain, "unverified");
});

test("JVM clock raw byte, hash and exact executable source boundaries are enforced", async (t) => {
    const f = await fixture(t),
        original = await read(f.root, "work/jvm-runtime.json"),
        launch = await read(f.root, "work/jvm-launch.json");
    for (const [name, mutate, expected] of [
        [
            "raw-size",
            (c) => {
                c.clockExpected.modeProbes.pre.clockIdentity.before.jvm.statRaw = "x".repeat(65537);
            },
            /JVM_CLOCK_RAW_BUDGET/,
        ],
        [
            "hash-limit",
            (c) => {
                c.clockExpected.modeProbes.pre.clockIdentity.executableHashMaxNs = "3000000000";
            },
            /JVM_CLOCK_KIND/,
        ],
        [
            "digest-delete",
            (c) => {
                c.clockExpected.modeProbes.pre.clockIdentity.before.capture.executable.sha256 = null;
            },
            /JVM_CLOCK_EXECUTABLE_DIGEST/,
        ],
        [
            "digest-after",
            (c) => {
                c.clockExpected.modeProbes.pre.clockIdentity.after.capture.executable.sha256 = "b".repeat(64);
            },
            /JVM_CLOCK_EXECUTABLE_DIGEST/,
        ],
        [
            "exe-source",
            (c) => {
                c.clockExpected.modeProbes.pre.clockIdentity.before.capture.executable.sourcePath = "/usr/bin/python3";
            },
            /JVM_CLOCK_EXECUTABLE_SOURCE/,
        ],
        [
            "deleted-exe",
            (c) => {
                c.clockExpected.modeProbes.pre.clockIdentity.before.capture.executable.path += " (deleted)";
            },
            /JVM_CLOCK_EXECUTABLE_SOURCE/,
        ],
        [
            "reported-verified",
            (c) => {
                c.clockExpected.modeProbes.pre.clockIdentity.verified = true;
            },
            /JVM_CLOCK_KIND/,
        ],
        [
            "noncanonical-ns",
            (c) => {
                c.clockExpected.modeProbes.pre.clockIdentity.before.startedMonotonicNs = "0100";
            },
            /JVM_INTEGER_PRECISION/,
        ],
        [
            "outer-window",
            (c) => {
                c.clockExpected.modeProbes.pre.startedMonotonicNs = 101;
            },
            /JVM_CLOCK_MODE_WINDOW/,
        ],
        [
            "java-inode",
            (c) => {
                for (const p of ["before", "after"])
                    c.clockExpected.modeProbes.post.clockIdentity[p].jvm.executable.metadata.inode = "9999";
            },
            /JVM_CLOCK_JAVA_METADATA/,
        ],
    ])
        await t.test(name, () => {
            const runtime = structuredClone(original),
                context = clockFixture(runtime, launch);
            mutate(context);
            assert.throws(() => verifyJvmClockIdentity(runtime, launch, context), expected);
        });
});

test("JVM clock rejects resealed identity, namespaces, offsets, binary, source and ordering contradictions", async (t) => {
    const f = await fixture(t),
        original = await read(f.root, "work/jvm-runtime.json"),
        launch = await read(f.root, "work/jvm-launch.json");
    for (const [name, mutate, code] of [
        [
            "pid",
            (r, c) => {
                c.clockExpected.modeProbes.pre.clockIdentity.before.jvm.statRaw = statLine("456", 124);
            },
            /JVM_PROCESS_STAT/,
        ],
        [
            "start",
            (r, c) => {
                c.clockExpected.modeProbes.pre.clockIdentity.before.jvm.startTimeTicksAfter = "457";
            },
            /JVM_PROCESS_STAT/,
        ],
        [
            "namespace",
            (r, c) => {
                for (const p of ["before", "after"])
                    c.clockExpected.modeProbes.post.clockIdentity[p].jvm.timeNamespace = "time:[7]";
            },
            /JVM_CLOCK_DOMAIN_CHANGED/,
        ],
        [
            "offset-overflow",
            (r, c) => {
                c.clockExpected.modeProbes.pre.clockIdentity.before.jvm.timeOffsetsRaw =
                    "monotonic 0 1000000000\nboottime 0 0\n";
            },
            /JVM_CLOCK_OFFSETS/,
        ],
        [
            "offset-shift",
            (r, c) => {
                for (const p of ["before", "after"])
                    c.clockExpected.modeProbes.post.clockIdentity[p].jvm.timeOffsetsRaw =
                        "monotonic 1 0\nboottime 0 0\n";
            },
            /JVM_CLOCK_DOMAIN_CHANGED/,
        ],
        [
            "ctime",
            (r, c) => {
                for (const p of ["before", "after"])
                    c.clockExpected.modeProbes.post.clockIdentity[p].capture.executable.metadata.ctimeNs = "3";
            },
            /JVM_CLOCK_PYTHON_METADATA_CHANGED/,
        ],
        [
            "binary",
            (r, c) => {
                c.clockExpected.modeProbes.post.clockIdentity.before.capture.executable.sha256 = "d".repeat(64);
            },
            /JVM_CLOCK_PYTHON_BINARY_CHANGED/,
        ],
        [
            "clock-api",
            (r, c) => {
                for (const p of ["before", "after"])
                    c.clockExpected.modeProbes.post.clockIdentity[p].capture.clock.implementation =
                        "mach_absolute_time()";
            },
            /JVM_CLOCK_MONOTONIC_IMPLEMENTATION/,
        ],
        [
            "missing-runtime",
            (r, c) => {
                delete c.clockExpected.modeProbes.pre.clockIdentity.before.capture.python;
            },
            /JVM_CLOCK_PROCESS_FIELDS/,
        ],
        [
            "source-lock",
            (r, c) => {
                c.clockExpected.modeProbeComponentSha256 = "e".repeat(64);
            },
            /JVM_CLOCK_EXTERNAL_SOURCE/,
        ],
        [
            "jdk-source",
            (r, c) => {
                c.clockExpected.jdkMonotonicSource.libjvmSha256 = "e".repeat(64);
            },
            /JVM_CLOCK_JDK_SOURCE_EXPECTATION/,
        ],
        [
            "initial-capture",
            (r, c) => {
                r.clock.capture.namespace.value = "time:[6]";
            },
            /JVM_INITIAL_CAPTURE_DOMAIN/,
        ],
        [
            "initial-raw",
            (r, c) => {
                r.process.statAfterBase64 = b64(statLine("457"));
            },
            /JVM_PROCESS_STAT/,
        ],
        [
            "initial-order",
            (r, c) => {
                r.observedAfterMonotonicNs = "101";
            },
            /JVM_CLOCK_RECEIPT_ORDER/,
        ],
        [
            "status",
            (r, c) => {
                c.clockExpected.modeProbes.pre.clockIdentity.status = "unknown";
            },
            /JVM_CLOCK_STATUS_REBUILD/,
        ],
    ])
        await t.test(name, () => {
            const runtime = structuredClone(original),
                context = clockFixture(runtime, launch);
            mutate(runtime, context);
            assert.throws(() => verifyJvmClockIdentity(runtime, launch, context), code);
        });
});

test("JVM evidence is unavailable by default and does not inspect undeclared paths", async () => {
    const result = await verifyJvmEvidence("/missing", "/missing");
    assert.equal(result.enabled, false);
    assert.equal(result.status, "unavailable");
    assert.equal(result.performanceAccepted, false);
    assert.equal(result.clockAlignment, "unverified");
    const missing = await verifyJvmEvidence("/missing", "/missing", { settings: { jvmObservation: {} } });
    assert.equal(missing.enabled, true);
    assert.equal(missing.status, "unavailable");
});

test("JVM verifier independently reparses raw logs and binds actual process, JDK and loaded maps", async (t) => {
    const listenersBefore = [process.rawListeners("SIGTERM"), process.rawListeners("SIGINT")];
    const f = await fixture(t),
        result = await verifyJvmEvidence(f.root, f.parsed, f.context);
    assert.equal(result.evidenceIntegrity, "passed");
    assert.equal(result.parserRecomputed, true);
    assert.equal(result.status, "observed");
    assert.equal(result.identity.rawProcessIdentity, "passed");
    assert.equal(result.identity.externalProcessIdentity, "passed");
    assert.equal(result.identity.loadedLibjvm, "passed");
    assert.deepEqual(result.gc, { pauseEnds: 1, reportedPauseNs: "1125000" });
    assert.equal(result.jit.begin, 1);
    assert.equal(result.jit.nativeWrappers, 1);
    assert.equal(result.jit.unknown, 0);
    assert.equal(result.jit.compilationSuccesses, null);
    assert.equal(result.clockAlignment, "unverified");
    assert.equal(result.phaseAttribution, "unavailable");
    assert.equal(result.performanceAccepted, false);
    assert(result.pendingGates.includes("observer-overhead-calibration"));
    const noExternal = await verifyJvmEvidence(f.root, f.parsed, { settings: f.context.settings });
    assert.equal(noExternal.status, "partial");
    assert.equal(noExternal.identity.hostBoot, "unknown");
    assert.deepEqual([process.rawListeners("SIGTERM"), process.rawListeners("SIGINT")], listenersBefore);
});

test("partial capture and zero events remain visible without a JVM stability claim", async (t) => {
    for (const options of [{ eof: false }, { events: false }]) {
        const f = await fixture(t, options);
        if (options.eof === false) {
            const receipt = await read(f.root, "work/server-capture.json");
            receipt.complete = true; // The verifier must reconstruct this claim from the primitive fields.
            await put(f.root, "work/server-capture.json", receipt);
        }
        const result = await verifyJvmEvidence(f.root, f.parsed, f.context);
        assert.equal(result.evidenceIntegrity, "passed");
        assert.equal(result.status, "partial");
        assert.equal(result.observationStatus, "partial");
        assert.equal(result.performanceAccepted, false);
        if (options.eof === false) assert.equal(result.captureComplete, false);
        if (options.events === false) {
            assert.equal(result.events, 0);
            assert.equal(result.jit.compilationSuccesses, null);
        }
    }
});

test("JVM verifier rejects resealed forged parsed outputs and contradictory raw process receipts", async (t) => {
    const original = await fixture(t);
    for (const [name, patch, expected] of [
        [
            "settings",
            async (f) => {
                f.context.settings.jvm.heapMaxBytes++;
            },
            /JVM_DECLARED_SETTINGS/,
        ],
        [
            "summary",
            async (f) => {
                const s = await read(f.parsed, "summary.json");
                s.gcPauseReportedNs = "0";
                await put(f.parsed, "summary.json", s);
                await relockParsed(f);
            },
            /JVM_RECOMPUTED_MEMBER_DIFFER/,
        ],
        [
            "events",
            async (f) => {
                await writeFile(join(f.parsed, "events.ndjson"), "{}\n");
                await relockParsed(f);
            },
            /JVM_RECOMPUTED_MEMBER_DIFFER/,
        ],
        ["raw", async (f) => writeFile(join(f.root, "work/server.log"), "replacement\n"), /JVM_RAW_CAPTURE_DIGEST/],
        [
            "maps",
            async (f) => {
                const r = await read(f.root, "work/jvm-runtime.json");
                r.process.mapsBase64 = b64("1000-2000 r-xp 0000 08:01 9999 /opt/jdk/lib/server/libjvm.so\n");
                await put(f.root, "work/jvm-runtime.json", r);
            },
            /JVM_LOADED_LIBJVM_MAP/,
        ],
        [
            "stat",
            async (f) => {
                const r = await read(f.root, "work/jvm-runtime.json");
                r.process.statAfterBase64 = b64(statLine("457"));
                await put(f.root, "work/jvm-runtime.json", r);
            },
            /JVM_PROCESS_STAT/,
        ],
        [
            "argv",
            async (f) => {
                const r = await read(f.root, "work/jvm-runtime.json");
                r.process.cmdlineBase64 = b64("/opt/jdk/bin/java\0different\0");
                await put(f.root, "work/jvm-runtime.json", r);
            },
            /JVM_ACTUAL_ARGV/,
        ],
        [
            "jdk",
            async (f) => {
                const r = await read(f.root, "work/jvm-runtime.json");
                r.process.libjvm.sha256 = "0".repeat(64);
                await put(f.root, "work/jvm-runtime.json", r);
            },
            /JVM_JDK_MEMBER_IDENTITY/,
        ],
        [
            "external",
            async (f) => {
                f.context.processIdentity.pid++;
            },
            /JVM_EXTERNAL_PROCESS_IDENTITY/,
        ],
        [
            "server-capture",
            async (f) => {
                const r = await read(f.root, "work/server-capture.json");
                r.eof = false;
                await put(f.root, "work/server-capture.json", r);
            },
            /JVM_SERVER_CAPTURE_DIFFER/,
        ],
        ["extra", async (f) => writeFile(join(f.parsed, "unexpected"), "extra"), /JVM_PARSED_INVENTORY/],
        [
            "symlink",
            async (f) => {
                await rm(join(f.parsed, "summary.json"));
                await symlink(join(original.parsed, "summary.json"), join(f.parsed, "summary.json"));
            },
            /JVM_PARSED_REGULAR_FILES/,
        ],
    ])
        await t.test(name, async () => {
            const f = await copy(original, name);
            await patch(f);
            await assert.rejects(verifyJvmEvidence(f.root, f.parsed, f.context), expected);
        });
});

test("an observer script whose content differs from the local trusted parser is never executed", async (t) => {
    const f = await fixture(t),
        marker = join(f.temp, "must-not-exist"),
        component = join(f.root, "jvm-observation.py");
    await writeFile(component, `from pathlib import Path\nPath(${JSON.stringify(marker)}).write_text('executed')\n`);
    const env = await read(f.root, "plan.json");
    Object.assign(
        env.controlFiles.find((x) => x.path === "jvm-observation.py"),
        await file(f.root, "jvm-observation.py"),
    );
    await put(f.root, "plan.json", env);
    const envSha = (await file(f.root, "plan.json")).sha256;
    await put(f.root, "plan.lock.json", { schemaVersion: 1, attemptId: env.attemptId, planSha256: envSha });
    f.context.environmentPlanSha256 = envSha;
    await assert.rejects(verifyJvmEvidence(f.root, f.parsed, f.context), /JVM_PARSER_IMPLEMENTATION/);
    await assert.rejects(access(marker), /ENOENT/);
});

test("parser subprocess output is bounded, its process is reaped and temporary inputs are removed", async (t) => {
    const f = await fixture(t),
        executable = join(f.temp, "noisy-python"),
        marker = join(f.temp, "child.json");
    const old = process.env.HGBENCH_PYTHON;
    await writeFile(
        executable,
        "#!/usr/bin/env python3\nimport json,os,sys,time\n" +
            `open(${JSON.stringify(marker)},'w').write(json.dumps({'pid':os.getpid(),'parser':sys.argv[3]}))\n` +
            "sys.stdout.write('x'*131072)\nsys.stdout.flush()\ntime.sleep(20)\n",
    );
    await chmod(executable, 0o700);
    try {
        process.env.HGBENCH_PYTHON = executable;
        await assert.rejects(verifyJvmEvidence(f.root, f.parsed, f.context), /JVM_PARSER_OUTPUT_BUDGET/);
    } finally {
        if (old === undefined) delete process.env.HGBENCH_PYTHON;
        else process.env.HGBENCH_PYTHON = old;
    }
    const child = await read(f.temp, "child.json");
    assert.throws(() => process.kill(child.pid, 0), /ESRCH/);
    await assert.rejects(access(dirname(child.parser)), /ENOENT/);
});

async function cancellableOwner(t, handler, count = 2, earlyExit = false, initialSignal = "SIGTERM") {
    const f = await fixture(t),
        executable = join(f.temp, "stubborn-python"),
        markers = join(f.temp, "children"),
        configuration = join(f.temp, "owner-input.json"),
        wrapper = join(f.temp, "owner.mjs"),
        result = join(f.temp, "owner-result.json");
    await mkdir(markers);
    await writeFile(
        executable,
        "#!/usr/bin/env python3\nimport json,os,signal,sys,time\n" +
            "signal.signal(signal.SIGTERM,signal.SIG_IGN)\n" +
            (earlyExit
                ? `try:\n open(${JSON.stringify(join(f.temp, "first-child"))},'x').close()\n early=True\nexcept FileExistsError:\n early=False\n`
                : "early=False\n") +
            `open(${JSON.stringify(markers)}+'/child-'+str(os.getpid())+'.json','w').write(json.dumps({'pid':os.getpid(),'parser':sys.argv[3],'early':early}))\n` +
            "if early:sys.exit(7)\n" +
            "time.sleep(20)\n",
    );
    await chmod(executable, 0o700);
    await put(f.temp, "owner-input.json", {
        root: f.root,
        parsed: f.parsed,
        context: f.context,
        handler,
        count,
        result,
    });
    await writeFile(
        wrapper,
        `import {readFile,writeFile} from 'node:fs/promises';
import {verifyJvmEvidence} from ${JSON.stringify(pathToFileURL(resolve("components/http/jvm-evidence.mjs")).href)};
const input=JSON.parse(await readFile(process.argv[2],'utf8'));let hits=0;
if(input.handler==='once')process.once('SIGTERM',()=>hits++);
if(input.handler==='late')process.on('SIGUSR1',()=>process.on('SIGTERM',()=>hits++));
const settled=await Promise.allSettled(Array.from({length:input.count},()=>verifyJvmEvidence(input.root,input.parsed,input.context)));
await writeFile(input.result,JSON.stringify({hits,codes:settled.map(r=>r.status==='rejected'?r.reason.code:'resolved'),
listeners:{SIGTERM:process.listenerCount('SIGTERM'),SIGINT:process.listenerCount('SIGINT')}}));
`,
    );
    let stdout = "",
        stderr = "";
    const child = spawn(process.execPath, [wrapper, configuration], {
        env: { ...process.env, HGBENCH_PYTHON: executable },
        stdio: ["ignore", "pipe", "pipe"],
    });
    child.stdout.on("data", (raw) => {
        stdout += raw;
    });
    child.stderr.on("data", (raw) => {
        stderr += raw;
    });
    const closed = new Promise((done) => {
        child.on("error", (error) => done({ error }));
        child.on("close", (code, signal) => done({ code, signal, stdout, stderr }));
    });
    const timer = setTimeout(() => child.kill("SIGKILL"), 10000);
    const records = [];
    try {
        const deadline = Date.now() + 5000;
        while (true) {
            const names = await readdir(markers);
            if (names.length === count) {
                for (const name of names) records.push(await read(markers, name));
                break;
            }
            assert(Date.now() < deadline && child.exitCode === null && child.signalCode === null, stderr);
            await new Promise((done) => setTimeout(done, 15));
        }
        if (earlyExit) {
            const record = records.find((r) => r.early);
            assert(record);
            while (true) {
                try {
                    await access(dirname(record.parser));
                } catch (error) {
                    if (error.code === "ENOENT") break;
                    throw error;
                }
                assert(Date.now() < deadline, "early parser owner did not clean its temporary directory");
                await new Promise((done) => setTimeout(done, 15));
            }
            assert.throws(() => process.kill(record.pid, 0), /ESRCH/);
        }
        const signalledAt = Date.now();
        child.kill(initialSignal);
        await new Promise((done) => setTimeout(done, 50));
        if (handler === "late") child.kill("SIGUSR1");
        else {
            child.kill("SIGTERM");
            child.kill("SIGINT");
        }
        const ended = await closed;
        assert(Date.now() - signalledAt >= 850, JSON.stringify(ended)); // Ignored TERM requires the KILL grace.
        for (const record of records) {
            assert.throws(() => process.kill(record.pid, 0), /ESRCH/);
            await assert.rejects(access(dirname(record.parser)), /ENOENT/);
        }
        return { ...ended, summary: handler === "default" ? null : await read(f.temp, "owner-result.json") };
    } finally {
        clearTimeout(timer);
        if (child.exitCode === null && child.signalCode === null) {
            child.kill("SIGKILL");
            await closed;
        }
        for (const record of records) {
            try {
                process.kill(-record.pid, "SIGKILL");
            } catch {}
        }
    }
}

test("concurrent parser owners reap TERM-ignoring groups and remove temporary files before default signal exit", async (t) => {
    for (const signal of ["SIGTERM", "SIGINT"])
        await t.test(signal, async (t) => {
            const ended = await cancellableOwner(t, "default", 2, false, signal);
            assert.equal(ended.code, null, ended.stderr);
            assert.equal(ended.signal, signal, ended.stderr);
        });
});

test("one parser's early exit leaves another owner's signal cleanup installed", async (t) => {
    const ended = await cancellableOwner(t, "default", 2, true);
    assert.equal(ended.code, null, ended.stderr);
    assert.equal(ended.signal, "SIGTERM", ended.stderr);
});

test("external signal control survives once-handler removal and a handler added during parser cleanup", async (t) => {
    for (const handler of ["once", "late"])
        await t.test(handler, async (t) => {
            const ended = await cancellableOwner(t, handler);
            assert.equal(ended.code, 0, ended.stderr);
            assert.equal(ended.signal, null);
            assert.deepEqual(ended.summary.codes, ["JVM_PARSER_INTERRUPTED", "JVM_PARSER_INTERRUPTED"]);
            assert.equal(ended.summary.hits, handler === "once" ? 1 : 0);
            assert.deepEqual(ended.summary.listeners, { SIGTERM: handler === "late" ? 1 : 0, SIGINT: 0 });
        });
});
