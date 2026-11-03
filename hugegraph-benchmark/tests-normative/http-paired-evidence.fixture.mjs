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

import {
    mkdtemp,
    mkdir,
    readFile,
    writeFile,
    readdir,
    lstat,
    rm,
    cp,
    chmod,
    symlink,
    realpath,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve, dirname } from "node:path";
import { createHash } from "node:crypto";
import http from "node:http";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { planHttp } from "../dist/http.js";
import { freezeComparison } from "../components/http/comparison-plan.mjs";
import { valueHash } from "../components/paired/core.mjs";
import { lifecycleFixture } from "./http-resource-evidence.fixture.mjs";
const run = promisify(execFile);
const hash = (raw) => createHash("sha256").update(raw).digest("hex");
const read = async (path) => JSON.parse(await readFile(path, "utf8"));
const put = async (path, value) => {
    await mkdir(dirname(path), { recursive: true });
    await writeFile(path, JSON.stringify(value, null, 2) + "\n");
};
const bootId = "00000000-0000-0000-0000-000000000001";
const executionId = "e".repeat(32);
const MiB = 1024 * 1024;

export async function seal(root, planDigest) {
    const files = [];
    async function walk(relative = "") {
        for (const name of await readdir(join(root, relative))) {
            const path = relative ? `${relative}/${name}` : name;
            if (path === "execution.lock.json") continue;
            const s = await lstat(join(root, path));
            if (s.isDirectory()) await walk(path);
            else {
                const raw = await readFile(join(root, path));
                files.push({ path, bytes: raw.length, sha256: hash(raw) });
            }
        }
    }
    await walk();
    files.sort((a, b) => (a.path < b.path ? -1 : 1));
    await put(join(root, "execution.lock.json"), {
        schemaVersion: 1,
        kind: "http-comparison-execution-lock",
        planDigest,
        contentDigest: valueHash(files),
        files,
    });
}

async function mode(dir, env, envSha, ready, container, start) {
    // Synthetic JVM/namespace records test the offline contract; these are never live HugeGraph claims.
    const expected = env.settings.mode;
    const identity = {
        pid: ready.java.pid,
        startTimeTicks: ready.java.startTimeTicks,
        effectiveUid: env.owner.uid,
        effectiveGid: env.owner.gid,
        bootId,
        namespaces: { pid: "pid:[1]", net: "net:[2]", mnt: "mnt:[3]", user: "user:[4]" },
        javaExecutable: "/opt/jdk/bin/java",
        jcmdExecutable: "/opt/jdk/bin/jcmd",
        javaExecutableSha256: env.sources.javaHome.files.find((f) => f.path === "bin/java").sha256,
        jcmdExecutableSha256: env.sources.javaHome.files.find((f) => f.path === "bin/jcmd").sha256,
        listenerSocketInodes: ["1"],
        cgroupSha256: "c".repeat(64),
        hostname: container.Config.Hostname,
    };
    const prefix = "org.apache.hugegraph.api.gremlin.";
    const cls = expected === "forward" ? "Forward" : "Embedded";
    const header = '"http-worker" #42 tid=0x123 nid=0x42 waiting';
    const frames = [prefix + cls + "GremlinRestExecutor.post", prefix + "GremlinAPI.post"];
    const dump =
        `Full thread dump OpenJDK 64-Bit Server VM (17):\n${header}\n` +
        frames.map((f) => `    at ${f}(Source.java:1)\n`).join("") +
        "JNI global refs: 1\n";
    const nonce = "hgbench-mode-" + "a".repeat(32);
    const body = { gremlin: "Thread.sleep(600); nonce", bindings: { nonce }, language: "gremlin-groovy" };
    await put(join(dir, "request.json"), {
        method: "POST",
        endpoint: env.endpoint,
        body,
        bodySha256: hash(Buffer.from(JSON.stringify(body))),
        bodyBytes: Buffer.byteLength(JSON.stringify(body)),
        expectedMode: expected,
        acceptEncoding: "gzip",
        authorizationEnvironment: null,
        budgets: {
            sleep_ms: 600,
            capture_delay_ms: 20,
            request_timeout_seconds: 3,
            command_timeout_seconds: 2,
            total_timeout_seconds: 5,
            max_command_bytes: MiB,
            max_response_bytes: 65536,
            max_wire_response_bytes: 65536,
        },
    });
    await run("python3", [
        "-B",
        "-c",
        "import json,sys; p=sys.argv[1]; v=json.load(open(p)); open(p, 'w').write(json.dumps(v, sort_keys=True, indent=2)+'\\n')",
        join(dir, "request.json"),
    ]);
    for (const [stem, command, output, offset] of [
        ["attach", "VM.version", `${identity.pid}:\nOpenJDK JDK 17.0.0\n`, 10000000],
        ["thread-dump", "Thread.print", dump, 200000000],
    ]) {
        const record = {
            argv: [identity.jcmdExecutable, String(identity.pid), command],
            startedMonotonicNs: start + offset,
            finishedMonotonicNs: start + offset + 10000000,
            maxBytes: MiB,
            processGroupId: 300,
            exitCode: 0,
            errorCode: null,
            groupCleanup: "already-absent",
            leaderReaped: true,
        };
        for (const [name, value] of [
            ["stdout", output],
            ["stderr", ""],
        ]) {
            const raw = Buffer.from(value);
            await writeFile(join(dir, `${stem}.${name}.txt`), raw);
            record[name] = { bytes: raw.length, sha256: hash(raw), redacted: false };
        }
        await put(join(dir, `${stem}.json`), record);
    }
    await put(join(dir, "result.json"), {
        schemaVersion: 1,
        kind: "hugegraph-http-mode-probe",
        status: "passed",
        expectedMode: expected,
        modeVerified: true,
        observedMode: expected,
        startedMonotonicNs: start,
        finishedMonotonicNs: start + 1000000000,
        elapsedSeconds: 1,
        componentSha256: env.controlFiles.find((f) => f.path === "hugegraph-mode-probe.py").sha256,
        identityBefore: identity,
        identityAfter: identity,
        threadEvidence: { threadHeader: header, frames },
        request: {
            startedMonotonicNs: start + 50000000,
            sentMonotonicNs: start + 100000000,
            completeMonotonicNs: start + 900000000,
            finishedMonotonicNs: start + 950000000,
            errorCode: null,
            httpStatus: 200,
            contentTypeAccepted: true,
            contentEncodingAccepted: true,
            contentEncoding: "gzip",
            timerStarted: true,
            workerStarted: true,
            bodyBytes: 100,
            bodySha256: "b".repeat(64),
            wireBytes: 100,
            wireSha256: "d".repeat(64),
        },
        performanceAccepted: false,
    });
}

async function environment(dir, settings, position, timeBaseNs = 0) {
    await put(join(dirname(dir), "settings.json"), settings);
    await run(
        "python3",
        [
            "-B",
            resolve("components/http/hugegraph-environment.py"),
            "plan",
            "--settings",
            join(dirname(dir), "settings.json"),
            "--out",
            dir,
        ],
        { timeout: 10000 },
    );
    await rm(join(dirname(dir), "settings.json"));
    const env = await read(join(dir, "plan.json")),
        envSha = hash(await readFile(join(dir, "plan.json")));
    const b = settings.budget,
        label = "org.apache.hugegraph.hgbench.http";
    const id = (position + 1).toString(16).padStart(64, "0");
    const before = {
        Id: id,
        Name: "/" + env.containerName,
        Image: settings.image,
        Config: {
            User: `${env.owner.uid}:${env.owner.gid}`,
            Hostname: id.slice(0, 12),
            Labels: { [label + ".attempt"]: env.attemptId, [label + ".plan"]: envSha },
            Entrypoint: [settings.python],
            WorkingDir: "/work",
            Cmd: [
                "-B",
                "/control/hugegraph-service.py",
                "--plan",
                "/control/plan.json",
                "--plan-sha256",
                envSha,
                "--work",
                "/work",
            ],
        },
        HostConfig: {
            NetworkMode: "none",
            ReadonlyRootfs: true,
            Memory: b.memoryBytes,
            MemorySwap: b.memoryBytes,
            NanoCpus: b.cpus * 1e9,
            PidsLimit: b.pids,
            CapDrop: ["ALL"],
            CapAdd: null,
            Privileged: false,
            SecurityOpt: ["no-new-privileges:true"],
            RestartPolicy: { Name: "no" },
            PidMode: "",
            Devices: [],
            VolumesFrom: null,
            LogConfig: { Type: "none" },
            Tmpfs: { "/tmp": `rw,exec,nosuid,nodev,size=${b.tmpfsBytes},mode=1777` },
        },
        NetworkSettings: { Ports: {} },
        Mounts: [
            ["/opt/hugegraph", settings.distribution, false],
            ["/opt/jdk", settings.javaHome, false],
            ["/control", env.root, false],
            ["/work", env.root + "/work", true],
            ["/work/scripts", settings.distribution + "/scripts", false],
        ].map(([Destination, Source, RW]) => ({ Type: "bind", Destination, Source, RW })),
        State: { Status: "created", Running: false },
    };
    const final = structuredClone(before);
    final.State = { Status: "exited", Running: false, OOMKilled: false, ExitCode: 0 };
    await put(join(dir, "container-before-start.json"), before);
    await put(join(dir, "container-final.json"), final);
    const ready = {
        schemaVersion: 1,
        kind: "hugegraph-http-ready",
        attemptId: env.attemptId,
        planSha256: envSha,
        declaredMode: settings.mode,
        endpoint: env.endpoint,
        java: { pid: 100 + position, phase: "server", startTimeTicks: String(1000 + position) },
        javaExecutable: "/opt/jdk/bin/java",
        jcmdExecutable: "/opt/jdk/bin/jcmd",
    };
    await put(join(dir, "work/ready.json"), ready);
    await put(join(dir, "work/service-result.json"), {
        schemaVersion: 1,
        kind: "hugegraph-http-service-result",
        attemptId: env.attemptId,
        planSha256: envSha,
        error: null,
        logBytes: 100,
        elapsedSeconds: 8,
        outcomes: [
            { phase: "java-version", reason: "process-exit", exitCodeBeforeShutdown: 0 },
            { phase: "init", reason: "process-exit", exitCodeBeforeShutdown: 0 },
            {
                phase: "server",
                reason: "stop-requested",
                ready: true,
                termination: {
                    termSent: true,
                    killSent: false,
                    mainReaped: true,
                    groupGone: true,
                    cleanupComplete: true,
                    exitCode: 143,
                },
            },
        ],
    });
    await put(join(dir, "supervisor-result.json"), {
        schemaVersion: 1,
        kind: "hugegraph-http-environment-result",
        attemptId: env.attemptId,
        planSha256: envSha,
        created: true,
        error: null,
    });
    const start = timeBaseNs + 2000000000 + position * 10000000000;
    for (const [phase, offset] of [
        ["pre", 1000000000],
        ["post", 6000000000],
    ]) {
        const path = join(dir, `work/mode-${phase}`);
        await mkdir(path);
        await mode(path, env, envSha, ready, final, start + offset);
    }
    if (env.formalControlsContract) {
        // Synthetic protocol records only; the fixture never attests this host or a real JVM.
        const sample = (atMonotonicNs) => ({
            atMonotonicNs: String(atMonotonicNs),
            passed: true,
            swapTotalBytes: 0,
            activeSwapAreas: 0,
            memoryMonitorDisabled: true,
            memoryMonitorThreshold: 1,
            hostBootId: bootId,
        });
        await put(join(dir, "formal-controls.json"), {
            schemaVersion: 1,
            kind: "http-formal-environment-controls",
            contract: env.formalControlsContract,
            attemptId: env.attemptId,
            planSha256: envSha,
            checks: 2,
            allPassed: true,
            maximumSwapTotalBytes: 0,
            maximumActiveSwapAreas: 0,
            first: sample(start),
            last: sample(start + 8000000000),
        });
    }
    return { env, envSha, start };
}

/** Actual loopback HTTP and physical tiny source files. JVM/Docker/proc records below remain synthetic. */
export async function localFixtureContext(t, { preserveDirectory = () => false, temporaryDirectory = tmpdir() } = {}) {
    const root = await realpath(await mkdtemp(join(temporaryDirectory, "hgbench-http-paired-")));
    const server = http.createServer((request, response) => {
        request.resume();
        response.setHeader("Content-Type", "application/json");
        response.end('{"status":{"code":200},"result":{"data":[42]}}');
    });
    // Controlled environment v1 fixes this loopback port; no external HugeGraph/Docker is used.
    // Separate node:test files can exercise this fixed protocol port concurrently.
    // Acquire our own listening socket before issuing any HTTP; never use another
    // fixture's server. Contention has a finite deadline and does not change the port.
    const deadline = Date.now() + 45000;
    while (true) {
        try {
            await new Promise((done, fail) => {
                const ready = () => {
                    server.removeListener("error", failed);
                    done();
                };
                const failed = (error) => {
                    server.removeListener("listening", ready);
                    fail(error);
                };
                server.once("error", failed);
                server.once("listening", ready);
                server.listen(18080, "127.0.0.1");
            });
            break;
        } catch (error) {
            if (error.code !== "EADDRINUSE" || Date.now() >= deadline || t.signal.aborted) throw error;
            await new Promise((done) => setTimeout(done, 250));
        }
    }
    t.after(async () => {
        server.closeAllConnections();
        await new Promise((done) => server.close(done));
        if (!preserveDirectory()) await rm(root, { recursive: true, force: true });
    });
    const dist = join(root, "distribution"),
        java = join(root, "jdk");
    const sourceFiles = {
        "bin/jvm-module.options": "--add-opens=java.base/java.lang=ALL-UNNAMED\n",
        "conf/gremlin-server.yaml": "graphs: { hugegraph: conf/graphs/hugegraph.properties }\n",
        "conf/java-security.properties": "package.access=sun.\n",
        "scripts/empty-sample.groovy": "// fixture\n",
        "lib/hugegraph-fixture.jar": "fixture-JAR-not-executed\n",
    };
    for (const [name, raw] of Object.entries(sourceFiles)) {
        await mkdir(dirname(join(dist, name)), { recursive: true });
        await writeFile(join(dist, name), raw);
    }
    for (const [name, raw] of Object.entries({
        "bin/java": "fixture-java\n",
        "bin/jcmd": "fixture-jcmd\n",
        release: 'JAVA_VERSION="17.0.1"\n',
        "conf/security/java.security": "fixture-security\n",
    })) {
        await mkdir(dirname(join(java, name)), { recursive: true });
        await writeFile(join(java, name), raw);
        await chmod(join(java, name), 0o700);
    }
    const settings = {
        schemaVersion: 1,
        kind: "hugegraph-http-environment-settings",
        purpose: "functional",
        distribution: dist,
        javaHome: java,
        image: "sha256:" + "a".repeat(64),
        python: "/usr/bin/python3",
        dockerSocket: "/var/run/docker.sock",
        budget: {
            cpus: 1,
            memoryBytes: 1024 * MiB,
            pids: 128,
            wallSeconds: 120,
            startupSeconds: 30,
            shutdownSeconds: 5,
            maxAllocatedBytes: 16 * MiB,
            maxLogicalBytes: 16 * MiB,
            maxLogBytes: MiB,
            minFreeBytes: 0,
            tmpfsBytes: 16 * MiB,
            pollSeconds: 0.1,
        },
        jvm: {
            heapMinBytes: 128 * MiB,
            heapMaxBytes: 256 * MiB,
            maxDirectMemoryBytes: 64 * MiB,
            activeProcessorCount: 1,
        },
        server: {
            gremlinPool: 1,
            bossThreads: 1,
            workerThreads: 1,
            maxWorkQueueSize: 16,
            httpWorkerThreads: 2,
            maxRoute: 2,
            timeoutSeconds: 10,
            idleTimeoutSeconds: 10,
            connectionMaxRequests: 1000,
            maxContentLengthBytes: 4096,
            resultIterationBatchSize: 16,
        },
    };
    const shared = {
        build: {
            sourceCommit: "d".repeat(40),
            distribution: dist,
            testsRun: false,
            files: Object.entries(sourceFiles).map(([path, raw]) => ({
                path,
                bytes: Buffer.byteLength(raw),
                sha256: hash(Buffer.from(raw)),
            })),
        },
        settings,
        environment: {
            schemaVersion: 1,
            kind: "http-comparison-host-environment",
            hostBootId: bootId,
            kernel: { release: "fixture", architecture: "fixture" },
            cpu: { model: "fixture", logicalProcessors: 1 },
            memoryBytes: 1024 * MiB,
            docker: { serverVersion: "fixture", imageId: settings.image },
            node: { version: process.version },
            coRunningJobs: ["synthetic-runtime-contract-fixture"],
        },
    };
    return { root, server, settings, shared };
}

export async function fixture(
    t,
    {
        comparison = "forward-embedded",
        durationMs = 100,
        ratePerSecond = 20,
        minimumObservations = 1,
        maxInFlight = 1,
        formalControls = false,
    } = {},
) {
    const evidenceDirectory = process.env.HGBENCH_PAIRED_FIXTURE_EVIDENCE
        ? resolve(process.env.HGBENCH_PAIRED_FIXTURE_EVIDENCE)
        : null;
    const { root, settings, shared } = await localFixtureContext(
        t,
        evidenceDirectory ? { preserveDirectory: () => true, temporaryDirectory: evidenceDirectory } : {},
    );
    if (formalControls) settings.purpose = "calibration";
    if (evidenceDirectory) t.diagnostic(`Retained original paired protocol fixture: ${root}`);
    const httpPlan = join(root, "http-plan");
    await mkdir(httpPlan);
    await cp(resolve("benchmarks/http-serving-v0.1"), join(httpPlan, "benchmark"), { recursive: true });
    const catalog = await read(join(httpPlan, "benchmark/catalog.json"));
    const answers = await read(join(httpPlan, "benchmark/answers.json"));
    const defaults = await read(resolve("examples/http-suite-functional.json"));
    const { caseIds, purpose, ...base } = defaults;
    await put(join(httpPlan, "profile.json"), {
        ...base,
        kind: "http-benchmark-profile",
        serviceChecks: true,
        target: {
            label: "local HTTP fixture",
            build: "fixture",
            mode: "forward",
            environment: "fixture",
            evidenceFiles: [],
        },
        warmupMs: 0,
        durationMs,
        requestTimeoutMs: 500,
        drainMs: 500,
        cases: [
            {
                id: "scalar-add",
                body: catalog.cases.find((c) => c.id === "scalar-add").body,
                expectedStatus: 200,
                checks: [
                    { pointer: "/status/code", equals: 200 },
                    {
                        pointer: "/result/data",
                        canonicalSha256: answers.cases.find((c) => c.id === "scalar-add").canonicalSha256,
                    },
                ],
            },
        ],
        loads: [{ id: `open-${ratePerSecond}`, model: "open", ratePerSecond, maxInFlight, maxStartDelayMs: 500 }],
        benchmark: {
            suiteId: catalog.suiteId,
            version: catalog.version,
            bundleSha256: hash(await readFile(join(httpPlan, "benchmark/bundle.lock.json"))),
            caseIds: ["scalar-add"],
            coverage: "subset",
            purpose,
        },
    });
    await put(join(httpPlan, "target-evidence.json"), {
        schemaVersion: 1,
        kind: "operator-supplied-target-evidence",
        files: [],
    });
    const sharedEvidence = {};
    for (const [role, value] of Object.entries(shared)) {
        const path = join(root, role + ".json");
        await put(path, value);
        sharedEvidence[role] = { path, sha256: hash(await readFile(path)) };
    }
    const request = {
        schemaVersion: 1,
        kind: "http-comparison-request",
        id: "paired-fixture",
        purpose: "functional",
        comparison,
        modes: { A: "forward", B: comparison === "aa" ? "forward" : "embedded" },
        blockIds: ["b01", "b02"],
        orderSeed: "paired-fixture-order",
        sharedEvidence,
        analysis: {
            familyAlpha: 0.05,
            minimumPairs: 2,
            bootstrapReplicates: 4000,
            minimumTailDraws: 20,
            maxWork: 1000000,
            maxMilliseconds: 30000,
            resampleSeed: "paired-fixture-resample",
            latency: { relativeBudget: 0.05, absoluteBudget: 1, minimumObservations },
            goodput: { relativeBudget: 0.05, absoluteBudget: 1, minimumObservations },
        },
        budgets: {
            prepareMs: 1000,
            startMs: 30000,
            modeProbeMs: 5000,
            settleMs: 0,
            stopMs: 5000,
            sealAndAnalyzeMs: 5000,
            maxEvidenceBytesPerArm: 8 * MiB,
            maxTotalEvidenceBytes: 128 * MiB,
            maxTotalMilliseconds: 3600000,
        },
    };
    const input = join(root, "request.json"),
        planDirectory = join(root, "plan");
    await put(input, request);
    await freezeComparison({ inputFile: input, httpPlanDirectory: httpPlan, outputDirectory: planDirectory });
    return emitPairedFixture({ root, planDirectory, settings });
}

/**
 * Emits new localhost HTTP raw for every arm of an already frozen comparison.
 * Docker/JVM/proc/lifecycle/resource observations are explicit protocol fixtures,
 * never actual HugeGraph or independent-environment performance acceptance.
 */
export async function emitPairedFixture({
    root,
    planDirectory,
    settings,
    runtimeSettings,
    executionDirectory = join(root, "execution"),
    executionId: selectedExecutionId = executionId,
    timeBaseNs = 0,
}) {
    const plan = await read(join(planDirectory, "plan.json"));
    const lock = await read(join(planDirectory, "plan.lock.json"));
    await mkdir(executionDirectory);
    const runtime = {
        schemaVersion: 1,
        kind: "http-comparison-runtime-plan",
        planDigest: plan.planDigest,
        planContentDigest: lock.contentDigest,
        settings: runtimeSettings ?? {
            sourceRoot: resolve("."),
            nodeHome: dirname(dirname(process.execPath)),
            driver: {
                image: settings.image,
                python: "/usr/bin/python3",
                cpus: 1,
                memoryBytes: 1024 * MiB,
                pids: 128,
                tmpfsBytes: 16 * MiB,
                wallSeconds: 120,
                shutdownSeconds: 5,
                maxLogBytes: MiB,
            },
            collector: { intervalMs: 1000, durationMs: 15000, maxSamples: 20, maxOutputBytes: MiB },
        },
        componentSha256: hash(await readFile(resolve("components/http/comparison-workflow.py"))),
        identities: {
            source: [],
            node: [],
            hostPython: { path: "/fixture/python", bytes: 1, sha256: "b".repeat(64) },
            dockerExecutable: { path: "/fixture/docker", bytes: 1, sha256: "d".repeat(64) },
        },
    };
    for (const file of [
        "workflow.mjs",
        "runner.mjs",
        "trace-writer.mjs",
        "direct-phase-clock.mjs",
        "runner-sanity.mjs",
        "phase-clock-reference.py",
        "clock-correlation.mjs",
        "analyzer.mjs",
        "report-view.mjs",
        "build-info.mjs",
        "common.mjs",
        "suite-evidence.mjs",
        "probe.mjs",
        "comparison-workflow.py",
        "resource-collector.py",
    ]) {
        const raw = await readFile(resolve("components/http", file));
        runtime.identities.source.push({
            path: `components/http/${file}`,
            type: "file",
            bytes: raw.length,
            sha256: hash(raw),
            mode: 420,
        });
    }
    runtime.identities.source.push({
        path: "node_modules/@types/node/index.d.ts",
        type: "file",
        mode: 420,
        bytes: 1,
        sha256: "c".repeat(64),
    });
    const nodeBytes = await readFile(process.execPath);
    runtime.identities.node.push({
        path: "bin/node",
        type: "file",
        bytes: nodeBytes.length,
        sha256: hash(nodeBytes),
        mode: 493,
    });
    const runtimeRaw = Buffer.from(JSON.stringify(runtime, null, 2) + "\n");
    const runtimeLock = {
        schemaVersion: 1,
        kind: "http-comparison-runtime-lock",
        planDigest: plan.planDigest,
        runtime: { path: "runtime.json", bytes: runtimeRaw.length, sha256: hash(runtimeRaw) },
    };
    const runtimeLockRaw = Buffer.from(JSON.stringify(runtimeLock, null, 2) + "\n");
    const execution = {
        schemaVersion: 1,
        kind: "http-comparison-execution",
        executionId: selectedExecutionId,
        planDigest: plan.planDigest,
        planContentDigest: lock.contentDigest,
        hostBootId: bootId,
        registeredMonotonicNs: String(timeBaseNs + 1000000000),
        runtimeDigest: hash(runtimeLockRaw),
        receipts: [],
    };
    for (const block of plan.blocks)
        for (const arm of block.arms) {
            const relative = `arms/${arm.runId}`,
                folder = join(executionDirectory, relative);
            await mkdir(folder, { recursive: true });
            await mkdir(join(folder, "controller"));
            await writeFile(join(folder, "controller/runtime.json"), runtimeRaw);
            await writeFile(join(folder, "controller/runtime.lock.json"), runtimeLockRaw);
            const prepared = await planHttp(join(planDirectory, arm.profileFile), join(root, `http-${arm.runId}`));
            await run(process.execPath, [resolve("components/http/workflow.mjs"), "--plan", prepared.directory], {
                timeout: 15000,
            });
            await cp(prepared.outputDirectory, join(folder, "raw"), { recursive: true });
            const { env, envSha, start } = await environment(
                join(folder, "environment"),
                { ...settings, mode: arm.expectedMode },
                arm.position,
                timeBaseNs,
            );
            const targetIdentity = (await read(join(folder, "environment/work/mode-pre/result.json"))).identityBefore;
            const targetContainer = (await read(join(folder, "environment/container-final.json"))).Id;
            await lifecycleFixture({
                folder,
                env,
                runtime,
                execution,
                arm,
                start,
                targetIdentity,
                targetContainer,
                simulateLinux: true,
            }); // Explicit synthetic Linux-shaped runtime records; never live acceptance.
            const receipt = {
                schemaVersion: 1,
                kind: "http-comparison-arm-receipt",
                failure: null,
                cleanupErrors: [],
                executionId: selectedExecutionId,
                planDigest: plan.planDigest,
                runId: arm.runId,
                blockId: block.id,
                arm: arm.arm,
                position: arm.position,
                armStartedMonotonicNs: String(start),
                workflowStartedMonotonicNs: String(start + 3000000000),
                workflowFinishedMonotonicNs: String(start + 5000000000),
                armFinishedMonotonicNs: String(start + 8000000000),
                rawDirectory: relative + "/raw",
                rawLockSha256: hash(await readFile(join(folder, "raw/raw.lock.json"))),
                environmentDirectory: relative + "/environment",
                environmentPlanSha256: envSha,
            };
            await put(join(folder, "receipt.json"), receipt);
            execution.receipts.push({ runId: arm.runId, path: relative + "/receipt.json" });
        }
    await put(join(executionDirectory, "execution.json"), execution);
    await put(join(executionDirectory, "terminal.json"), {
        schemaVersion: 1,
        kind: "http-comparison-execution-terminal",
        executionId: selectedExecutionId,
        planDigest: plan.planDigest,
        runtimeDigest: execution.runtimeDigest,
        status: "completed",
        failure: null,
        runtimeReverified: true,
        finishedMonotonicNs: String(timeBaseNs + (plan.blocks.flatMap((block) => block.arms).length * 10 + 1) * 1e9),
        performanceAccepted: false,
    });
    await seal(executionDirectory, plan.planDigest);
    return { root, planDirectory, executionDirectory, plan, first: plan.blocks[0].arms[0] };
}
