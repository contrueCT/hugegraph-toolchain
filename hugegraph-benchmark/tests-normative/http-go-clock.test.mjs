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
import { readFile, writeFile, mkdtemp, cp, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import {
    validateGoHostClockProof,
    readGoHostClockProof,
    isVerifiedGoHostClockProof,
    readGoMeasurementAssociation,
    readGoProcessObservations,
    validateGoMeasurementAssociation,
} from "../components/http/go-clock-evidence.mjs";
const spec = {
    sha256: "2".repeat(64),
    sourceSha256: "1".repeat(64),
    goVersion: "go1.27.3",
    goos: "linux",
    goarch: "amd64",
};
function fixture() {
    const process = {
        bootId: "11111111-1111-1111-1111-111111111111",
        startTicks: "123",
        timeOffsets: "monotonic 0 0\nboottime 0 0",
        cgroup: "0::/",
    };
    for (const n of ["pid", "mnt", "time", "time_for_children", "cgroup"])
        process[n + "Namespace"] = (n === "time_for_children" ? "time" : n) + ":[123]";
    const hostProcess = { ...process, hostPid: 1234, namespacePids: [1234, 1], executableSha256: spec.sha256 };
    const boundary = (i) => ({
        sinceOriginMs: i,
        beforeNs: String(100000000 + i * 1000000),
        afterNs: String(100000010 + i * 1000000),
    });
    const binary = {
        schemaVersion: 1,
        kind: "hgbench-http-go-identity",
        engine: "go-http-v1",
        sourceSha256: spec.sourceSha256,
        requestSchemaVersion: 2,
        rawSchemaVersion: 2,
        goVersion: spec.goVersion,
        goos: "linux",
        goarch: "amd64",
        binarySha256: spec.sha256,
        bytes: 12345,
        buildSettings: { CGO_ENABLED: "0", "-trimpath": "true" },
    };
    const clock = {
        contract: "go-monotonic-bracket-v1",
        goos: "linux",
        goarch: "amd64",
        goVersion: spec.goVersion,
        pid: 1,
        process,
        origin: boundary(0),
        externalCorrelationAccepted: false,
    };
    const p = {
        schemaVersion: 1,
        kind: "http-go-host-clock-proof",
        contract: "linux-go-monotonic-host-bracket-v1",
        binarySha256: spec.sha256,
        imageId: "sha256:" + "4".repeat(64),
        owner: "5".repeat(32),
        containerId: "6".repeat(64),
        sourceSha256: "7".repeat(64),
        error: null,
        performanceAccepted: false,
        processBefore: hostProcess,
        processAfter: structuredClone(hostProcess),
        readyRaw:
            JSON.stringify({ schemaVersion: 1, kind: "http-go-clock-ready", engine: "go-http-v1", binary, clock }) +
            "\n",
        host: {
            ...process,
            executableSha256: "3".repeat(64),
            pythonVersion: "3.12.3",
            clock: {
                implementation: "clock_gettime(CLOCK_MONOTONIC)",
                monotonic: true,
                adjustable: false,
                resolution: 1e-9,
                sampleFunction: "time.clock_gettime_ns(time.CLOCK_MONOTONIC)",
            },
        },
        exitCode: 0,
        trailingOutputBytes: 0,
        cleanup: { removed: true, absent: true },
        exchanges: [],
    };
    const container = {
        Id: p.containerId,
        Image: p.imageId,
        Path: "/hgbench-http-go",
        Args: ["clock-probe"],
        Config: {
            Labels: { "org.apache.hugegraph.hgbench.goClock": p.owner },
            User: "1000:1000",
            OpenStdin: true,
            Tty: false,
            Env: ["GODEBUG=", "GOGC=100", "GOMEMLIMIT=off", "GOMAXPROCS=2"],
        },
        HostConfig: {
            ReadonlyRootfs: true,
            NetworkMode: "none",
            NanoCpus: 2000000000,
            Memory: 268435456,
            MemorySwap: 268435456,
            PidsLimit: 64,
            LogConfig: { Type: "none" },
            Init: false,
            CapDrop: ["ALL"],
            CapAdd: null,
            Privileged: false,
            SecurityOpt: ["no-new-privileges"],
        },
        Mounts: [{ Type: "bind", Destination: "/hgbench-http-go", Source: "/frozen/runner", RW: false }],
        State: { Running: true, Pid: 1234, ExitCode: 0, OOMKilled: false },
    };
    p.containerBefore = structuredClone(container);
    p.containerAfterExchanges = structuredClone(container);
    p.containerTerminal = structuredClone(container);
    p.containerTerminal.State.Running = false;
    p.containerTerminal.State.Pid = 0;
    for (let i = 0; i < 5; i++) {
        const nonce = String(i).repeat(32),
            b = boundary(i + 1);
        const req = { schemaVersion: 1, sequence: i, nonce, kind: i === 4 ? "finish" : "sample" };
        const res = {
            schemaVersion: 1,
            sequence: i,
            nonce,
            engine: "go-http-v1",
            kind: i === 4 ? "http-go-clock-finished" : "http-go-clock-sample",
            boundary: b,
        };
        if (i === 4) res.processIdentityAfter = process;
        p.exchanges.push({
            requestRaw: JSON.stringify(req) + "\n",
            responseRaw: JSON.stringify(res) + "\n",
            hostBeforeNs: String(BigInt(b.beforeNs) - 100n),
            hostAfterNs: String(BigInt(b.afterNs) + 100n),
        });
    }
    return p;
}
test("Go clock proof validates independent live PID, namespace and five nonce brackets without granting measurement eligibility", () => {
    const result = validateGoHostClockProof(fixture(), spec);
    assert.equal(result.measurementProcessAssociationAccepted, false);
    assert.equal(result.performanceAccepted, false);
    assert.equal(result.maxRoundTripNs, "210");
    assert.equal(result.maxBoundaryWidthNs, "10");
    assert.equal(isVerifiedGoHostClockProof(result), false);
});
test("Go clock proof fails closed for forged clock, process, runtime, lifecycle and replay evidence", () => {
    const mutations = [
        (p) => {
            p.processAfter.startTicks = "124";
        },
        (p) => {
            p.processBefore.namespacePids = [1234, 2];
        },
        (p) => {
            p.processBefore.executableSha256 = "0".repeat(64);
        },
        (p) => {
            p.host.timeNamespace = "time:[555]";
        },
        (p) => {
            p.host.timeOffsets = "monotonic 1 0\nboottime 0 0";
        },
        (p) => {
            p.exchanges[0].hostBeforeNs = "999999999";
        },
        (p) => {
            const r = JSON.parse(p.exchanges[1].responseRaw);
            r.nonce = "f".repeat(32);
            p.exchanges[1].responseRaw = JSON.stringify(r) + "\n";
        },
        (p) => {
            p.exchanges.splice(1, 1);
        },
        (p) => {
            p.cleanup.absent = false;
        },
        (p) => {
            p.containerBefore.Config.Env[0] = "GODEBUG=http2client=1";
        },
        (p) => {
            p.containerBefore.Config.Env.push("GODEBUG=http2client=1");
        },
        (p) => {
            p.containerBefore.HostConfig.Privileged = true;
        },
        (p) => {
            p.containerBefore.HostConfig.Init = true;
        },
        (p) => {
            p.containerTerminal.State.ExitCode = 1;
        },
        (p) => {
            p.trailingOutputBytes = 1;
        },
        (p) => {
            const r = JSON.parse(p.readyRaw);
            r.clock.externalCorrelationAccepted = true;
            p.readyRaw = JSON.stringify(r) + "\n";
        },
        (p) => {
            p.exchanges[0].hostAfterNs = "999999999";
        },
        (p) => {
            p.containerBefore.Mounts[0].RW = true;
        },
    ];
    for (const mutate of mutations) {
        const p = fixture();
        mutate(p);
        assert.throws(() => validateGoHostClockProof(p, spec), /HTTP_GO/);
    }
    assert.throws(() => validateGoHostClockProof(fixture(), { ...spec, sha256: "0".repeat(64) }), /HTTP_GO/);
});
test(
    "Recorded Linux Go host-clock raw evidence passes the public offline reader",
    { skip: !process.env.HGBENCH_GO_CLOCK_RAW },
    async () => {
        const result = await readGoHostClockProof(process.env.HGBENCH_GO_CLOCK_RAW);
        assert(isVerifiedGoHostClockProof(result));
        assert(Object.isFrozen(result) && Object.isFrozen(result.binary));
        assert.throws(() => {
            result.binary.sha256 = "forged";
        }, TypeError);
        assert.equal(isVerifiedGoHostClockProof(structuredClone(result)), false);
        assert.equal(result.measurementProcessAssociationAccepted, false);
    },
);

test(
    "Live Go measurement gates are independently mapped and enclosed by the same frozen clock proof",
    { skip: !process.env.HGBENCH_GO_PROCESS_RAW },
    async (t) => {
        const root = process.env.HGBENCH_GO_PROCESS_RAW;
        const args = {
            clockProof: join(root, "clock-proof"),
            runFile: join(root, "work/run.json"),
            requestFile: join(root, "work/request.json"),
            markerDirectory: join(root, "work/gate"),
        };
        const result = await readGoMeasurementAssociation(root, args);
        assert.equal(result.status, "passed");
        assert.equal(result.measurementProcessAssociationAccepted, true);
        assert.equal(result.collectorAssociationAccepted, false);
        assert.equal(result.jvmAssociationAccepted, false);
        assert.equal(result.performanceAccepted, false);
        const run = JSON.parse(await readFile(args.runFile)),
            request = JSON.parse(await readFile(args.requestFile));
        const proof = await readGoHostClockProof(args.clockProof, request.profile.executor.binary);
        const observations = await readGoProcessObservations(root, args.markerDirectory);
        assert.throws(
            () => validateGoMeasurementAssociation(structuredClone(observations), run, request.profile, proof),
            /IO-verified process/,
        );
        assert.throws(
            () => validateGoMeasurementAssociation(observations, run, request.profile, structuredClone(proof)),
            /IO-verified clock/,
        );
        const copy = await mkdtemp(join(tmpdir(), "hgbench-process-negative-"));
        t.after(() => rm(copy, { recursive: true, force: true }));
        for (const stage of ["ready", "finished"])
            await cp(join(root, stage + "-host.json"), join(copy, stage + "-host.json"));
        const before = JSON.parse(await readFile(join(copy, "ready-host.json")));
        before.hostAfterNs = String(BigInt(run.phases[0].clock.start.beforeNs) + 1n);
        await writeFile(join(copy, "ready-host.json"), JSON.stringify(before));
        await assert.rejects(readGoMeasurementAssociation(copy, args), /phases not enclosed/);
        await cp(join(root, "ready-host.json"), join(copy, "ready-host.json"));
        const after = JSON.parse(await readFile(join(copy, "finished-host.json")));
        after.processAfter.startTicks = String(BigInt(after.processAfter.startTicks) + 1n);
        await writeFile(join(copy, "finished-host.json"), JSON.stringify(after));
        await assert.rejects(readGoMeasurementAssociation(copy, args), /process changed/);
    },
);
