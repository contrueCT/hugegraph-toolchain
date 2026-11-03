/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
import { readFile, open, readdir } from "node:fs/promises";
import { constants } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { isDeepStrictEqual } from "node:util";
import { bytesDigest, valueDigest } from "./common.mjs";
import { validateGoIdentity, validateGoRaw } from "./go-engine.mjs";
const verified = new WeakSet();
const observationsVerified = new WeakSet();
const CONTRACT = "linux-go-monotonic-host-bracket-v1";
const SHA = /^[a-f0-9]{64}$/;
const NAMESPACES = ["pid", "mnt", "time", "time_for_children", "cgroup"];
const need = (value, message) => {
    if (!value) throw new Error("HTTP_GO_CLOCK: " + message);
};
const same = (a, b, message) => need(isDeepStrictEqual(a, b), message);
function freeze(value) {
    if (value && typeof value === "object") {
        for (const child of Object.values(value)) freeze(child);
        Object.freeze(value);
    }
    return value;
}
async function regular(path, maximum) {
    const file = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
    try {
        const before = await file.stat({ bigint: true });
        need(before.isFile() && before.size <= BigInt(maximum), "raw file bound/type differs");
        const buffer = Buffer.alloc(Number(before.size) + 1);
        let size = 0;
        while (size < buffer.length) {
            const next = await file.read(buffer, size, buffer.length - size, null);
            if (!next.bytesRead) break;
            size += next.bytesRead;
        }
        const after = await file.stat({ bigint: true });
        need(
            BigInt(size) === before.size &&
                ["dev", "ino", "size", "mtimeNs", "ctimeNs"].every((k) => before[k] === after[k]),
            "raw changed while reading",
        );
        return buffer.subarray(0, size);
    } finally {
        await file.close();
    }
}
const ns = (v) => {
    need(typeof v === "string" && /^(0|[1-9][0-9]{0,18})$/.test(v), "invalid integer clock");
    return BigInt(v);
};
function parse(raw) {
    need(
        typeof raw === "string" &&
            Buffer.byteLength(raw) <= 65536 &&
            raw.endsWith("\n") &&
            raw.indexOf("\n") === raw.length - 1,
        "invalid exchange framing",
    );
    return JSON.parse(raw);
}
function offsets(raw) {
    need(typeof raw === "string" && raw.length <= 256, "missing time namespace offsets");
    const rows = raw
        .trim()
        .split("\n")
        .map((x) => x.trim().split(/\s+/));
    need(
        rows.length === 2 &&
            rows[0][0] === "monotonic" &&
            rows[1][0] === "boottime" &&
            rows.every((r) => r.length === 3 && r[1] === "0" && r[2] === "0"),
        "nonzero or malformed time namespace offsets",
    );
}
function identity(host, container, pid) {
    need(
        Number.isSafeInteger(host.hostPid) &&
            host.hostPid > 0 &&
            host.namespacePids?.[0] === host.hostPid &&
            host.namespacePids.at(-1) === pid,
        "host/container PID mapping missing",
    );
    need(
        typeof host.startTicks === "string" &&
            /^[0-9]+$/.test(host.startTicks) &&
            host.startTicks === container.startTicks,
        "process start differs",
    );
    need(
        /^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(host.bootId) && host.bootId === container.bootId,
        "boot identity differs",
    );
    for (const n of NAMESPACES) {
        need(
            new RegExp(`^${n === "time_for_children" ? "time" : n}:\\[[0-9]+\\]$`).test(host[n + "Namespace"]),
            "host namespace missing",
        );
        same(host[n + "Namespace"], container[n + "Namespace"], "container/host namespace differs");
    }
    offsets(host.timeOffsets);
    offsets(container.timeOffsets);
}
function container(info, p, live) {
    need(info?.Id === p.containerId && info.Image === p.imageId, "container/image identity differs");
    need(info.Config?.Labels?.["org.apache.hugegraph.hgbench.goClock"] === p.owner, "container ownership differs");
    const h = info.HostConfig,
        c = info.Config;
    need(
        h.ReadonlyRootfs === true &&
            h.NetworkMode === "none" &&
            h.NanoCpus === 2000000000 &&
            h.Memory === 268435456 &&
            h.MemorySwap === 268435456 &&
            h.PidsLimit === 64 &&
            h.LogConfig?.Type === "none" &&
            h.Init !== true,
        "container limits differ",
    );
    need(
        c.User === "1000:1000" && c.OpenStdin === true && c.Tty === false && info.Path === "/hgbench-http-go",
        "container process differs",
    );
    same(info.Args, ["clock-probe"], "container command differs");
    need(
        h.CapDrop?.includes("ALL") &&
            (!h.CapAdd || h.CapAdd.length === 0) &&
            h.Privileged === false &&
            h.SecurityOpt?.some((s) => s === "no-new-privileges" || s === "no-new-privileges:true"),
        "container isolation differs",
    );
    need(
        info.Mounts?.length === 1 &&
            info.Mounts[0].Type === "bind" &&
            info.Mounts[0].Destination === "/hgbench-http-go" &&
            info.Mounts[0].RW === false,
        "binary mount differs",
    );
    for (const entry of ["GODEBUG=", "GOGC=100", "GOMEMLIMIT=off", "GOMAXPROCS=2"])
        same(
            c.Env.filter((v) => v.startsWith(entry.slice(0, entry.indexOf("=") + 1))),
            [entry],
            "Go runtime environment differs",
        );
    need(
        info.State.Running === live && (live ? info.State.Pid === p.processBefore.hostPid : info.State.Pid === 0),
        "container lifecycle differs",
    );
    if (!live) need(info.State.ExitCode === 0 && info.State.OOMKilled === false, "probe container failed");
}
export function validateGoHostClockProof(p, expected) {
    need(
        p?.schemaVersion === 1 &&
            p.kind === "http-go-host-clock-proof" &&
            p.contract === CONTRACT &&
            p.error === null &&
            p.performanceAccepted === false,
        "proof protocol/failure differs",
    );
    need(
        SHA.test(p.binarySha256) &&
            /^sha256:[a-f0-9]{64}$/.test(p.imageId) &&
            /^[a-f0-9]{32}$/.test(p.owner) &&
            /^[a-f0-9]{64}$/.test(p.containerId),
        "proof identity malformed",
    );
    const ready = parse(p.readyRaw),
        c = ready.clock,
        binary = ready.binary;
    need(
        ready.schemaVersion === 1 && ready.kind === "http-go-clock-ready" && ready.engine === "go-http-v1",
        "ready protocol differs",
    );
    const spec = {
        sha256: p.binarySha256,
        sourceSha256: binary.sourceSha256,
        goVersion: "go1.27.3",
        goos: "linux",
        goarch: binary.goarch,
    };
    validateGoIdentity(binary, expected ?? spec);
    if (expected)
        same(
            spec,
            {
                sha256: expected.sha256,
                sourceSha256: expected.sourceSha256,
                goVersion: expected.goVersion,
                goos: expected.goos,
                goarch: expected.goarch,
            },
            "expected executor differs",
        );
    need(
        c.contract === "go-monotonic-bracket-v1" &&
            c.goos === "linux" &&
            c.goVersion === binary.goVersion &&
            c.goarch === binary.goarch &&
            c.pid === 1 &&
            c.externalCorrelationAccepted === false,
        "Go clock identity differs",
    );
    identity(p.processBefore, c.process, c.pid);
    same(p.processBefore, p.processAfter, "live process changed during exchanges");
    need(p.processBefore.executableSha256 === p.binarySha256, "live executable SHA differs");
    need(
        p.host?.bootId === c.process.bootId && p.host.timeNamespace === c.process.timeNamespace,
        "host clock domain differs",
    );
    offsets(p.host.timeOffsets);
    need(
        SHA.test(p.host.executableSha256) &&
            /^3\.[0-9]+\.[0-9]+$/.test(p.host.pythonVersion) &&
            p.host.clock?.implementation === "clock_gettime(CLOCK_MONOTONIC)" &&
            p.host.clock.monotonic === true &&
            p.host.clock.adjustable === false &&
            Number.isFinite(p.host.clock.resolution) &&
            p.host.clock.resolution > 0 &&
            p.host.clock.resolution <= 0.001 &&
            p.host.clock.sampleFunction === "time.clock_gettime_ns(time.CLOCK_MONOTONIC)",
        "host monotonic clock metadata differs",
    );
    container(p.containerBefore, p, true);
    container(p.containerAfterExchanges, p, true);
    container(p.containerTerminal, p, false);
    need(
        p.exitCode === 0 && p.trailingOutputBytes === 0 && p.cleanup?.removed === true && p.cleanup.absent === true,
        "probe exit/cleanup incomplete",
    );
    need(Array.isArray(p.exchanges) && p.exchanges.length === 5, "exchange count differs");
    const seen = new Set();
    let previous = ns(c.origin.beforeNs),
        lower,
        upper;
    function boundary(b) {
        need(b && Number.isFinite(b.sinceOriginMs) && b.sinceOriginMs >= 0 && !b.error, "invalid Go boundary");
        const before = ns(b.beforeNs),
            after = ns(b.afterNs);
        need(before >= previous && after >= before && after - before <= 1000000n, "boundary ordering/width differs");
        previous = after;
        const delta = BigInt(Math.round(b.sinceOriginMs * 1e6)),
            lo = before - delta - 1000n,
            hi = after - delta + 1000n;
        lower = lower === undefined || lo > lower ? lo : lower;
        upper = upper === undefined || hi < upper ? hi : upper;
        need(lower <= upper, "Go elapsed/raw monotonic mismatch");
        return { before, after };
    }
    need(c.origin.sinceOriginMs === 0, "clock origin differs");
    boundary(c.origin);
    let priorHost = 0n,
        maxRtt = 0n,
        maxWidth = 0n;
    for (let i = 0; i < 5; i++) {
        const e = p.exchanges[i],
            r = parse(e.requestRaw),
            s = parse(e.responseRaw);
        need(
            r.schemaVersion === 1 &&
                r.sequence === i &&
                r.kind === (i === 4 ? "finish" : "sample") &&
                /^[a-f0-9]{32}$/.test(r.nonce) &&
                !seen.has(r.nonce),
            "request nonce/sequence differs",
        );
        seen.add(r.nonce);
        need(
            s.schemaVersion === 1 &&
                s.engine === "go-http-v1" &&
                s.kind === (i === 4 ? "http-go-clock-finished" : "http-go-clock-sample") &&
                s.nonce === r.nonce &&
                s.sequence === i,
            "response nonce/sequence differs",
        );
        const hostBefore = ns(e.hostBeforeNs),
            hostAfter = ns(e.hostAfterNs),
            b = boundary(s.boundary);
        need(
            hostBefore >= priorHost &&
                hostBefore <= b.before &&
                b.after <= hostAfter &&
                hostAfter - hostBefore <= 100000000n,
            "independent host exchange bracket differs",
        );
        priorHost = hostAfter;
        maxRtt = maxRtt > hostAfter - hostBefore ? maxRtt : hostAfter - hostBefore;
        maxWidth = maxWidth > b.after - b.before ? maxWidth : b.after - b.before;
        if (i === 4) same(s.processIdentityAfter, c.process, "Go process identity changed");
    }
    return {
        contract: CONTRACT,
        binary: spec,
        imageId: p.imageId,
        hostBootId: p.host.bootId,
        timeNamespace: p.host.timeNamespace,
        originIntervalNs: { lower: String(lower), upper: String(upper) },
        maxRoundTripNs: String(maxRtt),
        maxBoundaryWidthNs: String(maxWidth),
        scope: "Probe process and its frozen clock implementation only; each measurement process still needs live host identity evidence",
        measurementProcessAssociationAccepted: false,
        performanceAccepted: false,
    };
}
export async function readGoHostClockProof(directory, expected) {
    const root = resolve(directory),
        names = (await readdir(root)).sort();
    same(names, ["producer.py", "proof.json", "raw.lock.json", "stderr.txt"], "raw inventory differs");
    const lockRaw = await regular(join(root, "raw.lock.json"), 65536);
    const lock = JSON.parse(lockRaw);
    need(
        lock.schemaVersion === 1 &&
            lock.kind === "http-go-clock-lock" &&
            Array.isArray(lock.files) &&
            lock.files.length === 3,
        "lock protocol differs",
    );
    same(lock.files.map((f) => f.path).sort(), ["producer.py", "proof.json", "stderr.txt"], "lock members differ");
    let proof;
    for (const f of lock.files) {
        const raw = await regular(join(root, f.path), 1048576);
        need(raw.length === f.bytes && bytesDigest(raw) === f.sha256, "raw SHA/bytes differ");
        if (f.path === "proof.json") proof = JSON.parse(raw);
        if (f.path === "stderr.txt") need(raw.length === 0, "probe stderr is nonempty");
    }
    const producer = await readFile(join(dirname(fileURLToPath(import.meta.url)), "go-clock-probe.py"));
    need(
        proof.sourceSha256 === bytesDigest(producer) &&
            lock.files.find((f) => f.path === "producer.py").sha256 === proof.sourceSha256,
        "producer implementation differs",
    );
    const result = {
        schemaVersion: 1,
        kind: "http-go-host-clock-verification",
        ...validateGoHostClockProof(proof, expected),
        hostSnapshotSourceSha256: proof.sourceSha256,
        hostPythonExecutableSha256: proof.host.executableSha256,
        rawLockSha256: bytesDigest(lockRaw),
        proofDigest: valueDigest(proof),
    };
    freeze(result);
    verified.add(result);
    return result;
}
export const isVerifiedGoHostClockProof = (value) => verified.has(value);

// This closes only the Go process/host clock domain. Collector/JVM windows and
// independent engineering calibration remain separate prerequisites.
export function validateGoMeasurementAssociation(observations, run, profile, proof) {
    need(observationsVerified.has(observations), "independently IO-verified process observations required");
    const { before, after } = observations;
    validateGoRaw(run, profile);
    need(isVerifiedGoHostClockProof(proof), "independently IO-verified clock proof required");
    const spec = profile.executor?.binary;
    same(
        proof.binary,
        {
            sha256: spec.sha256,
            sourceSha256: spec.sourceSha256,
            goVersion: spec.goVersion,
            goos: spec.goos,
            goarch: spec.goarch,
        },
        "measurement executor differs from clock proof",
    );
    need(
        run?.engine === "go-http-v1" && run.schemaVersion === 2 && Array.isArray(run.phases) && run.phases.length > 0,
        "measurement raw missing",
    );
    need(
        run.processIdentityAfter && isDeepStrictEqual(run.processIdentityAfter, run.phaseClock.process),
        "measurement end process identity missing/different",
    );
    let beforeEnd, afterStart;
    let processBefore;
    for (const [stage, observation] of [
        ["ready", before],
        ["finished", after],
    ]) {
        need(
            observation?.schemaVersion === 1 &&
                observation.kind === "http-go-live-process-observation" &&
                observation.error === null &&
                observation.performanceAccepted === false,
            "live process observation failed",
        );
        // Marker JSON is pretty-printed; only exchange messages use one-line framing.
        need(
            typeof observation.markerRaw === "string" &&
                Buffer.byteLength(observation.markerRaw) <= 65536 &&
                typeof observation.releaseRaw === "string" &&
                Buffer.byteLength(observation.releaseRaw) <= 4096,
            "live marker/release bounds differ",
        );
        need(
            observation.clockSnapshotSourceSha256 === proof.hostSnapshotSourceSha256,
            "host snapshot implementation differs from clock proof",
        );
        const marker = JSON.parse(observation.markerRaw),
            ack = JSON.parse(observation.releaseRaw);
        need(
            marker.schemaVersion === 1 &&
                marker.kind === "http-go-live-process-marker" &&
                marker.stage === stage &&
                marker.id === run.id &&
                /^[a-f0-9]{32}$/.test(marker.nonce) &&
                marker.performanceAccepted === false,
            "live marker identity differs",
        );
        validateGoIdentity(marker.binary, spec);
        const expectedClock = structuredClone(run.phaseClock);
        if (stage === "finished") expectedClock.process = run.processIdentityAfter;
        same(marker.clock, expectedClock, "live marker clock differs from raw run");
        need(
            ack.schemaVersion === 1 &&
                ack.kind === "http-go-live-process-release" &&
                ack.stage === stage &&
                ack.id === run.id &&
                ack.nonce === marker.nonce &&
                ack.markerSha256 === bytesDigest(Buffer.from(observation.markerRaw)),
            "live release differs from observed marker",
        );
        identity(observation.processBefore, marker.clock.process, marker.clock.pid);
        same(observation.processBefore, observation.processAfter, "live process changed during host observation");
        need(observation.processBefore.executableSha256 === spec.sha256, "measurement executable differs");
        need(
            /^[a-f0-9]{64}$/.test(observation.containerId) &&
                observation.container?.id === observation.containerId &&
                observation.container.running === true &&
                observation.container.image === proof.imageId &&
                Number.isSafeInteger(observation.container.rootHostPid) &&
                observation.container.rootHostPid > 0,
            "measurement container differs",
        );
        need(
            observation.host?.executableSha256 === proof.hostPythonExecutableSha256 &&
                observation.host?.bootId === proof.hostBootId &&
                observation.host.timeNamespace === proof.timeNamespace &&
                marker.clock.process.bootId === proof.hostBootId &&
                marker.clock.process.timeNamespace === proof.timeNamespace,
            "measurement host clock domain differs",
        );
        offsets(observation.host.timeOffsets);
        const start = ns(observation.hostBeforeNs),
            end = ns(observation.hostAfterNs);
        need(start <= end && end - start <= 10000000000n, "live observation duration differs");
        if (stage === "ready") {
            beforeEnd = end;
            processBefore = observation.processBefore;
        } else {
            afterStart = start;
            same(processBefore, observation.processBefore, "measurement process replaced between gates");
            need(before.containerId === after.containerId, "measurement container replaced");
        }
    }
    need(JSON.parse(before.markerRaw).nonce !== JSON.parse(after.markerRaw).nonce, "live stage nonce replayed");
    need(
        beforeEnd <= ns(run.phases[0].clock.start.beforeNs) && ns(run.phases.at(-1).clock.end.afterNs) <= afterStart,
        "measurement phases not enclosed by independent live observations",
    );
    return freeze({
        schemaVersion: 1,
        kind: "http-go-measurement-host-association",
        contract: CONTRACT,
        status: "passed",
        runId: run.id,
        hostPid: processBefore.hostPid,
        startTicks: processBefore.startTicks,
        hostBootId: proof.hostBootId,
        timeNamespace: proof.timeNamespace,
        clockProofDigest: proof.proofDigest,
        measurementProcessAssociationAccepted: true,
        collectorAssociationAccepted: false,
        jvmAssociationAccepted: false,
        performanceAccepted: false,
    });
}

export async function readGoProcessObservations(directory, markerDirectory) {
    const component = dirname(fileURLToPath(import.meta.url));
    const source = bytesDigest(await regular(join(component, "go-process-observer.py"), 65536));
    const clockSource = bytesDigest(await regular(join(component, "go-clock-probe.py"), 65536));
    const result = {};
    for (const [stage, key] of [
        ["ready", "before"],
        ["finished", "after"],
    ]) {
        const observation = JSON.parse(await regular(join(resolve(directory), stage + "-host.json"), 262144));
        need(
            observation.sourceSha256 === source && observation.clockSnapshotSourceSha256 === clockSource,
            "host observation source differs",
        );
        const marker = await regular(join(resolve(markerDirectory), stage + ".json"), 65536);
        const release = await regular(join(resolve(markerDirectory), stage + "-release.json"), 4096);
        need(
            marker.toString("utf8") === observation.markerRaw && release.toString("utf8") === observation.releaseRaw,
            "actual marker/release differs from independent observation",
        );
        result[key] = observation;
    }
    freeze(result);
    observationsVerified.add(result);
    return result;
}
export async function readGoMeasurementAssociation(directory, { clockProof, runFile, requestFile, markerDirectory }) {
    const run = JSON.parse(await regular(resolve(runFile), 1048576));
    const request = JSON.parse(await regular(resolve(requestFile), 4 * 1048576));
    need(request.schemaVersion === 2 && request.id === run.id, "measurement request differs");
    const proof = await readGoHostClockProof(clockProof, request.profile?.executor?.binary);
    const observations = await readGoProcessObservations(directory, markerDirectory);
    return validateGoMeasurementAssociation(observations, run, request.profile, proof);
}
