/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
import { isDeepStrictEqual } from "node:util";
import { validateDirectPhaseClockRaw } from "./direct-phase-clock.mjs";
import { verifyRunnerSanityRaw } from "./runner-sanity.mjs";
import { validateCollectorClockIdentity } from "./collector-clock-evidence.mjs";
import { isVerifiedRuntimeProof } from "./runtime-provenance-evidence.mjs";
import { valueDigest, bytesDigest } from "./common.mjs";

const CONTRACT = "linux-node22.14-cpython-monotonic-v1";
const NODE_SHA = "1abce2374a485bddae3c27b17a3e3143e2780232026e627c4fe74ddde3f380a1";
const SHA = /^[a-f0-9]{64}$/;
const IMAGE = /^sha256:[a-f0-9]{64}$/;
const BOOT = /^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/;
const PYTHON_CLOCK_SOURCES = {
    "3.12.3": {
        pytimeSha256: "033751b3c90dd12cd40b86af11388a0a201bc44deca0e96b52b4a13da8be82c9",
        timeModuleSha256: "55ee3d5dfaab9ef4044f7537749ba1d4b17b8dc55796763ed65e4a3383075607",
    },
    "3.12.15": {
        pytimeSha256: "033751b3c90dd12cd40b86af11388a0a201bc44deca0e96b52b4a13da8be82c9",
        timeModuleSha256: "81ce33b3f784a9abae1663576424faabb9c584d84f36daa4c27d0e16195c76b0",
    },
};
const SOURCES = [
    "runner.mjs",
    "direct-phase-clock.mjs",
    "runner-sanity.mjs",
    "clock-correlation.mjs",
    "phase-clock-reference.py",
    "resource-collector.py",
];

export class PhaseHostEvidenceError extends Error {
    constructor(code) {
        super(code);
        this.code = code;
    }
}
function need(value, code) {
    if (!value) throw new PhaseHostEvidenceError(code);
}
function object(value) {
    return value !== null && typeof value === "object" && !Array.isArray(value);
}
function same(actual, expected, code) {
    need(isDeepStrictEqual(actual, expected), code);
}
function allowed(value, fields, code) {
    need(object(value) && Object.keys(value).every((key) => fields.includes(key)), code);
}
function exact(value, fields, code) {
    allowed(value, fields, code);
    same(Object.keys(value).sort(), [...fields].sort(), code);
}
function uint(value) {
    need(typeof value === "string" && /^(0|[1-9][0-9]{0,24})$/.test(value), "PHASE_HOST_INTEGER");
    return BigInt(value);
}
function digest(value, max = 4 * 1024 * 1024) {
    let raw;
    try {
        raw = JSON.stringify(value);
    } catch {
        throw new PhaseHostEvidenceError("PHASE_HOST_JSON");
    }
    need(typeof raw === "string" && Buffer.byteLength(raw) <= max, "PHASE_HOST_INPUT_BUDGET");
    return valueDigest(value);
}
function gate(pairs = [], failures = [], pending = []) {
    const failed = [...failures],
        missing = [...pending];
    for (const [label, actual, expected] of pairs) {
        if (expected === undefined || actual === undefined || actual === null) missing.push(label);
        else if (!isDeepStrictEqual(actual, expected)) failed.push(label);
    }
    return {
        status: failed.length ? "failed" : missing.length ? "pending" : "passed",
        failures: [...new Set(failed)],
        pending: [...new Set(missing)],
    };
}
function validateExternal(e) {
    allowed(
        e,
        [
            "runDigest",
            "profileDigest",
            "runtimeDigest",
            "probedRuntimeDigest",
            "runtimeProofDigest",
            "node",
            "peer",
            "collector",
            "sources",
            "pythonClockSources",
            "quality",
            "runtimeBinding",
            "experimentRuntime",
        ],
        "PHASE_HOST_EXTERNAL_FIELDS",
    );
    for (const key of ["runDigest", "profileDigest", "runtimeDigest", "probedRuntimeDigest", "runtimeProofDigest"])
        if (e[key] !== undefined) need(typeof e[key] === "string" && SHA.test(e[key]), "PHASE_HOST_EXPECTED_DIGEST");
    for (const [key, fields] of [
        ["node", ["version", "uv", "executableSha256"]],
        ["peer", ["imageId", "version", "implementation", "executableSha256"]],
        ["collector", ["hostBootId", "version", "executableSha256", "componentSha256", "summaryDigest"]],
    ]) {
        if (e[key] === undefined) continue;
        allowed(e[key], fields, "PHASE_HOST_EXPECTED_FIELDS");
        for (const [field, value] of Object.entries(e[key])) {
            need(typeof value === "string" && value.length > 0 && value.length <= 256, "PHASE_HOST_EXPECTED_VALUE");
            if (field.endsWith("Sha256") || field.endsWith("Digest"))
                need(SHA.test(value), "PHASE_HOST_EXPECTED_DIGEST");
            if (field === "imageId") need(IMAGE.test(value), "PHASE_HOST_EXPECTED_IMAGE");
            if (field === "hostBootId") need(BOOT.test(value), "PHASE_HOST_EXPECTED_BOOT");
        }
    }
    if (e.sources !== undefined) {
        allowed(e.sources, SOURCES, "PHASE_HOST_SOURCE_FIELDS");
        for (const value of Object.values(e.sources))
            need(typeof value === "string" && SHA.test(value), "PHASE_HOST_SOURCE_DIGEST");
    }
    if (e.pythonClockSources !== undefined) {
        allowed(e.pythonClockSources, ["peer", "collector"], "PHASE_HOST_PYTHON_SOURCE_FIELDS");
        for (const value of Object.values(e.pythonClockSources)) {
            allowed(value, ["version", "pytimeSha256", "timeModuleSha256"], "PHASE_HOST_PYTHON_SOURCE_FIELDS");
            for (const [key, item] of Object.entries(value))
                need(
                    typeof item === "string" && (key === "version" ? /^3\.[0-9]+\.[0-9]+$/.test(item) : SHA.test(item)),
                    "PHASE_HOST_PYTHON_SOURCE_VALUE",
                );
        }
    }
    if (e.quality !== undefined) {
        allowed(e.quality, ["maxBoundaryWidthNs", "maxRoundTripNs", "maxAnchorWidthNs"], "PHASE_HOST_QUALITY_FIELDS");
        for (const value of Object.values(e.quality)) uint(value);
    }
}
function parseOffsets(raw) {
    need(typeof raw === "string" && Buffer.byteLength(raw) <= 256, "PHASE_HOST_OFFSETS");
    const rows = raw
        .trim()
        .split("\n")
        .map((line) => line.trim().split(/\s+/));
    need(
        rows.length === 2 &&
            rows[0][0] === "monotonic" &&
            rows[1][0] === "boottime" &&
            rows.every(
                (row) =>
                    row.length === 3 &&
                    /^-?[0-9]{1,12}$/.test(row[1]) &&
                    /^[0-9]{1,9}$/.test(row[2]) &&
                    BigInt(row[2]) < 1000000000n,
            ),
        "PHASE_HOST_OFFSETS",
    );
    return Object.fromEntries(
        rows.map(([key, seconds, nanoseconds]) => [
            key,
            { seconds: BigInt(seconds).toString(), nanoseconds: BigInt(nanoseconds).toString() },
        ]),
    );
}
function fixedPythonClock(identity) {
    return (
        identity?.python?.implementation === "cpython" &&
        identity.clock?.api === "time.monotonic_ns" &&
        identity.clock.implementation === "clock_gettime(CLOCK_MONOTONIC)" &&
        identity.clock.monotonic === true &&
        identity.clock.adjustable === false
    );
}
function pythonSourceContracts(peer, collector, e) {
    const pairs = [],
        pending = [];
    for (const [role, identity, runtimeExpected] of [
        ["peer", peer, e.peer],
        ["collector", collector, e.collector],
    ]) {
        const version = identity?.python?.version.split(/\s+/)[0],
            expected = e.pythonClockSources?.[role];
        pairs.push(
            [`${role}:source-version`, version, expected?.version],
            [`${role}:external-runtime-version`, expected?.version, runtimeExpected?.version],
        );
        const known = PYTHON_CLOCK_SOURCES[version];
        if (!known) pending.push(`${role}:source-contract-pending`);
        else
            for (const key of ["pytimeSha256", "timeModuleSha256"])
                pairs.push([`${role}:${key}`, known[key], expected?.[key]]);
    }
    return gate(pairs, [], pending);
}
function intervals(phase) {
    const { start, end } = phase.clock;
    const [s0, s1, e0, e1] = [start.hrtimeBeforeNs, start.hrtimeAfterNs, end.hrtimeBeforeNs, end.hrtimeAfterNs].map(
        uint,
    );
    return {
        phase: phase.name,
        windowSemantics: "actual-phase-including-drain",
        startNs: [s0.toString(), s1.toString()],
        endNs: [e0.toString(), e1.toString()],
        outerNs: [s0.toString(), e1.toString()],
        certainInnerNs: [s1.toString(), e0.toString()],
        durationNs: [(e0 - s1).toString(), (e1 - s0).toString()],
        startWidthNs: (s1 - s0).toString(),
        endWidthNs: (e1 - e0).toString(),
    };
}

// Exact binary64 rounding cells, converted outwards to integral nanoseconds.
// This only checks the fixed implementation's common performance origin; it never fits a mapping.
function rational(value) {
    const view = new DataView(new ArrayBuffer(8));
    view.setFloat64(0, value);
    const bits = view.getBigUint64(0),
        exponent = Number((bits >> 52n) & 2047n);
    let numerator = bits & ((1n << 52n) - 1n),
        shift = -1074;
    if (exponent) {
        numerator |= 1n << 52n;
        shift = exponent - 1075;
    }
    if (bits >> 63n) numerator = -numerator;
    return shift >= 0 ? [numerator << BigInt(shift), 1n] : [numerator, 1n << BigInt(-shift)];
}
function adjacent(value, upward) {
    if (value === 0) return upward ? Number.MIN_VALUE : -Number.MIN_VALUE;
    const view = new DataView(new ArrayBuffer(8));
    view.setFloat64(0, value);
    view.setBigUint64(0, view.getBigUint64(0) + (upward ? 1n : -1n));
    return view.getFloat64(0);
}
function roundingCell(value) {
    const [n, d] = rational(value),
        [lo, ld] = rational(adjacent(value, false)),
        [hi, hd] = rational(adjacent(value, true));
    return [
        [lo * d + n * ld, 2n * ld * d],
        [hi * d + n * hd, 2n * hd * d],
    ];
}
function floatFromBits(bits) {
    const view = new DataView(new ArrayBuffer(8));
    view.setBigUint64(0, bits);
    return view.getFloat64(0);
}
function floorFloatBits(n, d) {
    let low = 0n,
        high = 0x7fefffffffffffffn;
    while (low < high) {
        const middle = (low + high + 1n) >> 1n,
            [a, b] = rational(floatFromBits(middle));
        if (a * d <= n * b) low = middle;
        else high = middle - 1n;
    }
    return low;
}
function roundingCellNs(value) {
    need(Number.isFinite(value) && value >= 0 && value <= 1e12, "PHASE_HOST_PERFORMANCE_RANGE");
    const [[ln, ld], [un, ud]] = roundingCell(value),
        lowerN = ln > 0n ? ln * 1000000n : 0n,
        upperN = un * 1000000n;
    // Node casts integer elapsed nanoseconds to binary64 before division by 1e6.
    // Invert both roundings. No intermediate BigInt is converted approximately to Number.
    let first = floorFloatBits(lowerN, ld);
    const [fn, fd] = rational(floatFromBits(first));
    if (fn * ld < lowerN * fd) first++;
    const last = floorFloatBits(upperN, ud);
    if (first > last) return null;
    const [[a, b]] = roundingCell(floatFromBits(first)),
        [, [c, d]] = roundingCell(floatFromBits(last));
    const floor = a / b - (a < 0n && a % b !== 0n ? 1n : 0n);
    const ceil = c / d + (c > 0n && c % d !== 0n ? 1n : 0n);
    return [floor, ceil];
}
function performanceCompatibility(boundaries) {
    let low = 0n,
        high;
    for (const boundary of boundaries) {
        const cell = roundingCellNs(boundary.performanceMs);
        if (cell === null)
            return {
                compatible: false,
                originIntervalNs: null,
                rounding: "binary64-integer-cast-and-division-cells-outwards-to-integer-nanoseconds",
            };
        const [p0, p1] = cell,
            a = uint(boundary.hrtimeBeforeNs) - p1,
            b = uint(boundary.hrtimeAfterNs) - p0;
        if (a > low) low = a;
        if (high === undefined || b < high) high = b;
    }
    return {
        compatible: high !== undefined && low <= high,
        originIntervalNs: high !== undefined && low <= high ? [low.toString(), high.toString()] : null,
        rounding: "binary64-integer-cast-and-division-cells-outwards-to-integer-nanoseconds",
    };
}
function runtimeBinding(input, e, session, peer, collectorIdentity) {
    const pending = ["runtime-source-unverified", "independently-verified-runtime-proof-missing"];
    if (input === undefined) return gate([], [], pending);
    let raw;
    if (input?.kind === "http-runtime-binding") raw = input;
    else {
        allowed(input, ["raw", "verification"], "PHASE_HOST_RUNTIME_PACKET");
        raw = input.raw;
    }
    if (raw === undefined) return gate([], [], [...pending, "runtime-binding-raw-missing"]);
    exact(
        raw,
        ["schemaVersion", "kind", "sourceOrigin", "runtimeDigest", "driver", "node", "proofDigest"],
        "PHASE_HOST_RUNTIME_FIELDS",
    );
    need(
        raw.schemaVersion === 1 &&
            raw.kind === "http-runtime-binding" &&
            raw.sourceOrigin === "linux-procfs" &&
            typeof raw.runtimeDigest === "string" &&
            SHA.test(raw.runtimeDigest) &&
            typeof raw.proofDigest === "string" &&
            SHA.test(raw.proofDigest),
        "PHASE_HOST_RUNTIME_KIND",
    );
    exact(
        raw.driver,
        ["imageId", "pythonExecutableSha256", "pythonVersion", "pythonImplementation"],
        "PHASE_HOST_RUNTIME_DRIVER",
    );
    exact(raw.node, ["executableSha256", "version", "uv"], "PHASE_HOST_RUNTIME_NODE");
    need(
        typeof raw.driver.imageId === "string" &&
            IMAGE.test(raw.driver.imageId) &&
            typeof raw.driver.pythonExecutableSha256 === "string" &&
            SHA.test(raw.driver.pythonExecutableSha256) &&
            typeof raw.node.executableSha256 === "string" &&
            SHA.test(raw.node.executableSha256) &&
            [raw.driver.pythonVersion, raw.driver.pythonImplementation, raw.node.version, raw.node.uv].every(
                (v) => typeof v === "string" && v.length > 0 && v.length <= 256,
            ),
        "PHASE_HOST_RUNTIME_VALUES",
    );
    const pairs = [
        ["probed-runtime-digest", raw.runtimeDigest, e.probedRuntimeDigest],
        ["runtime-proof-digest", raw.proofDigest, e.runtimeProofDigest],
        ["driver-image", raw.driver.imageId, e.peer?.imageId],
        ["driver-python-binary", raw.driver.pythonExecutableSha256, peer?.files?.[1]?.sha256],
        ["driver-python-version", raw.driver.pythonVersion, peer?.identity?.python?.version.split(/\s+/)[0]],
        ["driver-python-implementation", raw.driver.pythonImplementation, peer?.identity?.python?.implementation],
        ["driver-node-binary", raw.node.executableSha256, session?.runtime?.files?.executable?.sha256],
        ["driver-node-version", raw.node.version, session?.runtime?.node],
        ["driver-node-uv", raw.node.uv, session?.runtime?.uv],
    ];
    if (!isVerifiedRuntimeProof(input?.verification)) return gate(pairs, [], pending);
    const proof = input.verification,
        expectations = proof.externalExpectations;
    same(raw, proof.runtimeBinding, "PHASE_HOST_RUNTIME_PROOF_REBUILD");
    same(raw, expectations.runtimeBinding, "PHASE_HOST_RUNTIME_PROOF_REBUILD");
    pairs.push(
        ["proof-input-runtime", expectations.probedRuntimeDigest, e.probedRuntimeDigest],
        ["proof-raw-digest", proof.proofDigest, e.runtimeProofDigest],
        ["proof-host-python-binary", expectations.hostPythonExecutableSha256, collectorIdentity?.executableSha256],
        [
            "proof-host-python-version",
            expectations.hostPythonVersion,
            collectorIdentity?.python?.version.split(/\s+/)[0],
        ],
        [
            "proof-host-python-implementation",
            expectations.hostPythonImplementation,
            collectorIdentity?.python?.implementation,
        ],
    );
    return gate(pairs);
}
function experimentRuntime(input, e, session, collectorIdentity) {
    if (input === undefined) return { sources: {}, gate: gate([], [], ["experiment-runtime-inventory-missing"]) };
    exact(input, ["runtimeRaw", "lockRaw"], "PHASE_HOST_EXPERIMENT_RUNTIME_PACKET");
    for (const [field, cap] of [
        ["runtimeRaw", 16 * 1024 * 1024],
        ["lockRaw", 65536],
    ])
        need(
            typeof input[field] === "string" && Buffer.byteLength(input[field]) <= cap,
            "PHASE_HOST_EXPERIMENT_RUNTIME_BUDGET",
        );
    let runtime, lock;
    try {
        runtime = JSON.parse(input.runtimeRaw);
        lock = JSON.parse(input.lockRaw);
    } catch {
        throw new PhaseHostEvidenceError("PHASE_HOST_EXPERIMENT_RUNTIME_JSON");
    }
    exact(lock, ["schemaVersion", "kind", "planDigest", "runtime"], "PHASE_HOST_EXPERIMENT_RUNTIME_LOCK");
    exact(lock.runtime, ["bytes", "path", "sha256"], "PHASE_HOST_EXPERIMENT_RUNTIME_MEMBER");
    exact(
        runtime,
        ["schemaVersion", "kind", "planDigest", "planContentDigest", "componentSha256", "settings", "identities"],
        "PHASE_HOST_EXPERIMENT_RUNTIME_FIELDS",
    );
    exact(
        runtime.identities,
        ["source", "node", "hostPython", "dockerExecutable", "javaHome", "distribution"],
        "PHASE_HOST_EXPERIMENT_RUNTIME_IDENTITIES",
    );
    need(
        lock.schemaVersion === 1 &&
            lock.kind === "http-comparison-runtime-lock" &&
            runtime.schemaVersion === 1 &&
            runtime.kind === "http-comparison-runtime-plan" &&
            SHA.test(runtime.planDigest) &&
            SHA.test(runtime.planContentDigest) &&
            SHA.test(runtime.componentSha256) &&
            lock.planDigest === runtime.planDigest &&
            lock.runtime.path === "runtime.json" &&
            lock.runtime.bytes === Buffer.byteLength(input.runtimeRaw) &&
            lock.runtime.sha256 === bytesDigest(Buffer.from(input.runtimeRaw)),
        "PHASE_HOST_EXPERIMENT_RUNTIME_DERIVATION",
    );
    const inventory = (rows) => {
        need(Array.isArray(rows) && rows.length > 0 && rows.length <= 100000, "PHASE_HOST_EXPERIMENT_INVENTORY");
        const paths = new Set();
        const files = {};
        for (const row of rows) {
            need(
                object(row) &&
                    typeof row.path === "string" &&
                    row.path.length > 0 &&
                    row.path.length <= 1024 &&
                    !row.path.startsWith("/") &&
                    !row.path.split("/").some((p) => p === ".." || p === "." || p === "") &&
                    !paths.has(row.path),
                "PHASE_HOST_EXPERIMENT_INVENTORY_PATH",
            );
            paths.add(row.path);
            if (row.type === "file") {
                exact(row, ["type", "path", "mode", "bytes", "sha256"], "PHASE_HOST_EXPERIMENT_INVENTORY_FILE");
                need(
                    Number.isSafeInteger(row.bytes) &&
                        row.bytes >= 0 &&
                        Number.isSafeInteger(row.mode) &&
                        row.mode >= 0 &&
                        row.mode <= 4095 &&
                        typeof row.sha256 === "string" &&
                        SHA.test(row.sha256),
                    "PHASE_HOST_EXPERIMENT_INVENTORY_FILE",
                );
                files[row.path] = row;
            }
        }
        return files;
    };
    const source = inventory(runtime.identities.source),
        node = inventory(runtime.identities.node),
        host = runtime.identities.hostPython;
    exact(host, ["path", "bytes", "sha256"], "PHASE_HOST_EXPERIMENT_HOST_PYTHON");
    need(
        typeof host.path === "string" &&
            host.path.startsWith("/") &&
            Number.isSafeInteger(host.bytes) &&
            host.bytes > 0 &&
            typeof host.sha256 === "string" &&
            SHA.test(host.sha256) &&
            host.path === runtime.settings?.hostPython,
        "PHASE_HOST_EXPERIMENT_HOST_PYTHON",
    );
    const sources = Object.fromEntries(SOURCES.map((name) => [name, source[`components/http/${name}`]?.sha256]));
    return {
        sources,
        gate: gate([
            ["experiment-runtime-lock", bytesDigest(Buffer.from(input.lockRaw)), e.runtimeDigest],
            ["experiment-driver-image", runtime.settings?.driver?.image, e.peer?.imageId],
            ["experiment-node-binary", node["bin/node"]?.sha256, session?.runtime?.files?.executable?.sha256],
            ["experiment-node-bytes", node["bin/node"]?.bytes, session?.runtime?.files?.executable?.bytes],
            ["experiment-host-python", host.sha256, collectorIdentity?.executableSha256],
            [
                "experiment-controller-source",
                source["components/http/comparison-workflow.py"]?.sha256,
                runtime.componentSha256,
            ],
        ]),
    };
}
function collectorEvidence(packet) {
    if (packet === undefined)
        return { clock: null, summary: null, verification: null, gate: gate([], [], ["collector-evidence-missing"]) };
    exact(packet, ["summary", "verification"], "PHASE_HOST_COLLECTOR_PACKET");
    const { summary, verification } = packet;
    need(
        object(verification) &&
            verification.status === "verified" &&
            verification.performanceAccepted === false &&
            typeof verification.summarySha256 === "string" &&
            SHA.test(verification.summarySha256),
        "PHASE_HOST_COLLECTOR_VERIFICATION",
    );
    const clock = validateCollectorClockIdentity(summary, {
        sampleWindow: [verification.firstStartedMonotonicNs, verification.lastFinishedMonotonicNs],
    });
    for (const key of [
        "status",
        "rawValidated",
        "sourceOrigin",
        "verified",
        "hostAssociation",
        "performanceAccepted",
        "identity",
        "window",
        "changedFields",
    ])
        same(verification.clockIdentity?.[key], clock[key], "PHASE_HOST_COLLECTOR_RAW_REBUILD");
    same(verification.sourceOrigin, summary.sourceOrigin, "PHASE_HOST_COLLECTOR_ORIGIN");
    same(verification.finishedMonotonicNs, summary.finishedMonotonicNs, "PHASE_HOST_COLLECTOR_WINDOW");
    need(
        Number.isSafeInteger(verification.sampleCount) &&
            verification.sampleCount === summary.samples &&
            uint(verification.firstStartedMonotonicNs) <= uint(verification.firstFinishedMonotonicNs) &&
            uint(verification.firstFinishedMonotonicNs) <= uint(verification.lastFinishedMonotonicNs),
        "PHASE_HOST_COLLECTOR_WINDOW",
    );
    return {
        clock,
        summary,
        verification,
        gate: gate(
            [],
            clock.status === "raw-consistent" && clock.rawValidated && clock.sourceOrigin === "linux-procfs"
                ? []
                : ["collector-production-clock-unavailable"],
        ),
    };
}

/** Pure consumer of content-locked observations and an already verified resource-evidence result.
 * Only an in-memory result returned by the independent runtime IO verifier can close its source gate.
 * This function never changes the producers' verified/performance fields or assigns resource counters to phases.
 */
export function associatePhaseHost({ run, profile, collector, external = {} }) {
    need(object(run) && object(profile), "PHASE_HOST_INPUT");
    validateExternal(external);
    const runDigest = digest(run),
        profileDigest = digest(profile),
        externalDigest = digest(external, 40 * 1024 * 1024);
    const direct = validateDirectPhaseClockRaw(run, profile);
    const raw = run.clockReference;
    const sanity = raw === undefined ? null : verifyRunnerSanityRaw(raw);
    const session = run.phaseClock,
        peer = raw ? JSON.parse(raw.frames[1].raw) : null;
    const phases = direct.rawValidated ? run.phases.map(intervals) : [];
    const c = collectorEvidence(collector);
    const gates = {
        contentLocks: gate([
            ["run-digest", runDigest, external.runDigest],
            ["profile-digest", profileDigest, external.profileDigest],
        ]),
        directRaw: gate([], [], direct.rawValidated ? [] : ["actual-phase-clock-missing"]),
        runnerSanityRaw: gate([], [], sanity ? [] : ["actual-runner-sr-missing"]),
        collectorRaw: c.gate,
    };
    const runtime = session?.runtime,
        nodeExpected = external.node;
    gates.nodeRuntime = gate(
        [
            ["expected-node-version", runtime?.node, nodeExpected?.version],
            ["expected-uv-version", runtime?.uv, nodeExpected?.uv],
            ["expected-node-binary", runtime?.files?.executable?.sha256, nodeExpected?.executableSha256],
        ],
        runtime &&
            runtime.node === "v22.14.0" &&
            runtime.uv === "1.49.2" &&
            runtime.platform === "linux" &&
            runtime.arch === "x64" &&
            runtime.nodeSharedLibuv === false &&
            runtime.files?.executable?.sha256 === NODE_SHA &&
            runtime.files.executable.bytes === 120177224 &&
            runtime.execArgvSha256 === valueDigest([]) &&
            Object.values(runtime.injectionEnvironmentPresent).every((v) => v === false) &&
            session.identityBefore.availability === "available" &&
            session.identityAfter.availability === "available"
            ? []
            : ["fixed-node-clock-contract-unavailable"],
    );
    gates.peerRuntime = gate(
        [
            ["expected-peer-binary", peer?.files?.[1]?.sha256, external.peer?.executableSha256],
            ["expected-peer-version", peer?.identity?.python?.version.split(/\s+/)[0], external.peer?.version],
            ["expected-peer-implementation", peer?.identity?.python?.implementation, external.peer?.implementation],
        ],
        raw?.sourceOrigin === "linux-procfs" && fixedPythonClock(peer?.identity)
            ? []
            : ["fixed-peer-clock-contract-unavailable"],
    );
    gates.collectorRuntime = gate(
        [
            [
                "expected-collector-summary",
                c.summary ? digest(c.summary) : undefined,
                external.collector?.summaryDigest,
            ],
            ["expected-collector-binary", c.clock?.identity?.executableSha256, external.collector?.executableSha256],
            ["expected-collector-version", c.summary?.pythonVersion, external.collector?.version],
            ["expected-collector-component", c.summary?.componentSha256, external.collector?.componentSha256],
            ["expected-host-boot", c.clock?.identity?.bootId, external.collector?.hostBootId],
        ],
        fixedPythonClock(c.clock?.identity) ? [] : ["fixed-collector-clock-contract-unavailable"],
    );
    gates.pythonSourceContracts = pythonSourceContracts(peer?.identity, c.clock?.identity, external);
    const experiment = experimentRuntime(external.experimentRuntime, external, session, c.clock?.identity);
    gates.experimentRuntime = experiment.gate;
    const observedSources = {
        "runner.mjs": experiment.sources["runner.mjs"],
        "direct-phase-clock.mjs": runtime?.files?.component?.sha256,
        "runner-sanity.mjs": raw?.clientSources?.[0]?.sha256,
        "clock-correlation.mjs": raw?.clientSources?.[2]?.sha256,
        "phase-clock-reference.py": peer?.files?.[0]?.sha256,
        "resource-collector.py": c.summary?.componentSha256,
    };
    gates.componentSources = gate(
        SOURCES.flatMap((key) => [
            [key, observedSources[key], external.sources?.[key]],
            [`${key}:runtime-inventory`, observedSources[key], experiment.sources[key]],
        ]),
    );
    gates.runtimeProof = runtimeBinding(external.runtimeBinding, external, session, peer, c.clock?.identity);
    const pre = raw ? JSON.parse(raw.frames[4].raw).anchor : null,
        post = raw ? JSON.parse(raw.frames[8].raw).anchor : null;
    const observations = raw
        ? [0, 1].map((index) => {
              const anchor = [pre, post][index],
                  receipt = JSON.parse(raw.frames[index ? 9 : 5].raw);
              return {
                  stage: index ? "post" : "pre",
                  sentNs: receipt.sentMonotonicNs,
                  receivedNs: receipt.receivedMonotonicNs,
                  roundTripNs: (uint(receipt.receivedMonotonicNs) - uint(receipt.sentMonotonicNs)).toString(),
                  anchorWidthNs: (uint(anchor.hrtimeAfterNs) - uint(anchor.hrtimeBeforeNs)).toString(),
              };
          })
        : [];
    const causalFailures = [],
        causalPending = [];
    if (observations.length && session && raw.sourceOrigin === "linux-procfs") {
        if (!(
            uint(session.preparedIntervalNs[0]) <= uint(observations[0].sentNs) &&
            uint(observations[0].receivedNs) <= uint(session.preparedIntervalNs[1])
        ))
            causalFailures.push("pre-exchange-outside-actual-preparation");
        if (!(
            uint(session.finishedIntervalNs[0]) <= uint(observations[1].sentNs) &&
            uint(observations[1].receivedNs) <= uint(session.finishedIntervalNs[1])
        ))
            causalFailures.push("post-exchange-outside-actual-finalization");
        for (const phase of run.phases)
            if (!(
                uint(pre.hrtimeAfterNs) <= uint(phase.clock.start.hrtimeBeforeNs) &&
                uint(observations[0].receivedNs) <= uint(phase.clock.start.hrtimeBeforeNs) &&
                uint(phase.clock.end.hrtimeAfterNs) <= uint(post.hrtimeBeforeNs) &&
                uint(phase.clock.end.hrtimeAfterNs) <= uint(observations[1].sentNs)
            ))
                causalFailures.push(`${phase.name}:actual-causal-enclosure`);
    } else causalPending.push("production-causal-exchanges-missing");
    gates.causalEnclosure = gate([], causalFailures, causalPending);
    const namespacePairs = [],
        namespaceFailures = [],
        namespacePending = [];
    if (pre && raw.sourceOrigin === "linux-procfs" && c.clock?.identity) {
        const identity = pre.identityBefore,
            ci = c.clock.identity;
        namespacePairs.push(
            ["same-boot", identity.bootId, ci.bootId],
            ["same-time-namespace", identity.timeNamespace, ci.timeNamespace],
            ["same-children-time-namespace", identity.timeForChildrenNamespace, ci.timeForChildrenNamespace],
            ["same-time-offsets", parseOffsets(identity.timeOffsets), ci.timeOffsets],
        );
    } else namespacePending.push("production-node-peer-collector-identity-missing");
    gates.hostNamespace = gate(namespacePairs, namespaceFailures, namespacePending);
    const coverageFailures = [],
        coveragePending = [];
    if (
        observations.length &&
        c.clock?.window?.firstSampleStartNs !== null &&
        c.clock?.window?.firstSampleStartNs !== undefined
    ) {
        const w = c.clock.window,
            first = observations[0],
            last = observations[1];
        if (!(
            uint(w.beforeFinishNs) <= uint(first.sentNs) &&
            uint(w.firstSampleStartNs) <= uint(first.sentNs) &&
            uint(last.receivedNs) <= uint(w.lastSampleFinishNs) &&
            uint(last.receivedNs) <= uint(w.afterStartNs)
        ))
            coverageFailures.push("collector-does-not-enclose-both-roundtrips");
    } else coveragePending.push("collector-or-roundtrip-window-missing");
    gates.collectorCoverage = gate([], coverageFailures, coveragePending);
    const qualityFailures = [],
        qualityPending = [];
    for (const key of ["maxBoundaryWidthNs", "maxRoundTripNs", "maxAnchorWidthNs"])
        if (external.quality?.[key] === undefined) qualityPending.push(key);
    if (!phases.length || !observations.length) qualityPending.push("actual-boundaries-or-roundtrips-missing");
    if (external.quality?.maxBoundaryWidthNs !== undefined)
        for (const phase of phases)
            for (const [label, width] of [
                ["start", phase.startWidthNs],
                ["end", phase.endWidthNs],
            ])
                if (uint(width) > uint(external.quality.maxBoundaryWidthNs))
                    qualityFailures.push(`${phase.phase}:${label}:boundary-width`);
    for (const observation of observations) {
        if (
            external.quality?.maxRoundTripNs !== undefined &&
            uint(observation.roundTripNs) > uint(external.quality.maxRoundTripNs)
        )
            qualityFailures.push(`${observation.stage}:roundtrip-width`);
        if (
            external.quality?.maxAnchorWidthNs !== undefined &&
            uint(observation.anchorWidthNs) > uint(external.quality.maxAnchorWidthNs)
        )
            qualityFailures.push(`${observation.stage}:anchor-width`);
    }
    gates.quality = gate([], qualityFailures, qualityPending);
    let performanceOrigin = null;
    if (phases.length && pre && post) {
        performanceOrigin = performanceCompatibility([
            pre,
            ...run.phases.flatMap((p) => [p.clock.start, p.clock.end]),
            post,
        ]);
        gates.performanceOrigin = gate(
            [],
            performanceOrigin.compatible ? [] : ["performance-and-hrtime-origin-incompatible"],
        );
    } else gates.performanceOrigin = gate([], [], ["performance-origin-boundaries-missing"]);
    const verified = Object.values(gates).every((entry) => entry.status === "passed");
    return {
        schemaVersion: 1,
        kind: "http-phase-host-association",
        contract: CONTRACT,
        status: verified ? "verified" : direct.rawValidated ? "unverified" : "unavailable",
        verified,
        hostAssociation: verified ? "verified" : "unverified",
        performanceAccepted: false,
        scope: "fixed-linux-clock-monotonic-phase-boundaries-only; no resource counter attribution or physical precision claim",
        bindings: {
            runDigest,
            profileDigest,
            externalDigest,
            runtimeDigest: external.runtimeDigest ?? null,
            probedRuntimeDigest: external.probedRuntimeDigest ?? null,
            sessionId: session?.sessionId ?? null,
            runIdentity: session ? structuredClone(session.runIdentity) : null,
            collectorVerificationDigest: c.verification ? digest(c.verification) : null,
        },
        gates,
        pendingGates: Object.entries(gates)
            .filter(([, value]) => value.status !== "passed")
            .map(([key]) => key),
        runtimeProofVerifierConnected: true,
        performanceOrigin,
        roundtrips: observations,
        causalEnvelopeNs: observations.length ? [observations[0].receivedNs, observations[1].sentNs] : null,
        phases: phases.map((phase) => ({
            ...phase,
            clockDomain: verified ? "linux-clock-monotonic" : "node-hrtime-local-candidate",
            hostIntervals: verified
                ? {
                      startNs: phase.startNs,
                      endNs: phase.endNs,
                      outerNs: phase.outerNs,
                      certainInnerNs: phase.certainInnerNs,
                      durationNs: phase.durationNs,
                  }
                : null,
        })),
    };
}
