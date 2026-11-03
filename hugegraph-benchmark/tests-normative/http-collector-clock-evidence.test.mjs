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
import { mkdtemp, readdir, readFile, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { createHash } from "node:crypto";
import { validateCollectorClockIdentity } from "../components/http/collector-clock-evidence.mjs";

const ROOT = fileURLToPath(new URL("../", import.meta.url));
const BOOT = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
const sha = (raw) => createHash("sha256").update(raw).digest("hex");
const expected = {
    hostBootId: BOOT,
    componentSha256: "c".repeat(64),
    expected: { hostPythonExecutableSha256: "a".repeat(64), hostPythonVersion: "3.12.3" },
};
function fixture() {
    const clock = {
        api: "time.monotonic_ns",
        implementation: "clock_gettime(CLOCK_MONOTONIC)",
        monotonic: true,
        adjustable: false,
        resolutionSeconds: 1e-9,
    };
    const before = {
        sourceOrigin: "linux-procfs",
        availability: "observed",
        pid: 123,
        startTimeTicks: "42",
        bootId: BOOT,
        statRaw: "123 (python3.12) R 1 123 123 0 -1 0 0 0 0 0 1 0 0 0 20 0 1 0 42 1000\n",
        timeNamespace: "time:[4026531834]",
        timeForChildrenNamespace: "time:[4026531834]",
        timeOffsetsRaw: "monotonic           0         0\nboottime            0         0\n",
        timeOffsets: { monotonic: { seconds: "0", nanoseconds: "0" }, boottime: { seconds: "0", nanoseconds: "0" } },
        python: {
            reportedExecutable: "/usr/bin/python3.12",
            version: "3.12.3 (main, fixture) [GCC 13.3.0]",
            implementation: "cpython",
        },
        clock,
        executable: {
            sourcePath: "/proc/self/exe",
            digestRead: true,
            sha256: "a".repeat(64),
            metadata: { bytes: "10000", device: "1", inode: "2", mtimeNs: "3", ctimeNs: "4" },
        },
        unavailable: {},
        captureDrift: [],
        startedMonotonicNs: "1000000",
        finishedMonotonicNs: "1100000",
    };
    const after = structuredClone(before);
    Object.assign(after, { startedMonotonicNs: "2000000", finishedMonotonicNs: "2100000" });
    Object.assign(after.executable, { digestRead: false, sha256: null });
    return {
        schemaVersion: 1,
        kind: "http-resource-summary",
        sourceOrigin: "linux-procfs",
        status: "completed",
        identityContinuous: true,
        collectorPid: 123,
        componentSha256: "c".repeat(64),
        pythonExecutable: "/usr/bin/python3.12",
        pythonVersion: "3.12.3",
        host: { bootId: BOOT },
        clock: { implementation: clock.implementation, adjustable: false, resolutionSeconds: 1e-9 },
        startedMonotonicNs: "1200000",
        finishedMonotonicNs: "2200000",
        samples: 2,
        firstSampleStartedMonotonicNs: "1300000",
        lastSampleFinishedMonotonicNs: "1900000",
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
}
function fixtureOrigin() {
    const summary = fixture();
    summary.sourceOrigin = summary.clockIdentity.sourceOrigin = "fixture";
    summary.clockIdentity.availability = "unknown";
    for (const capture of [summary.clockIdentity.before, summary.clockIdentity.after]) {
        capture.sourceOrigin = "fixture";
        capture.availability = "unknown";
        capture.unavailable = { source: "fixture-has-no-production-clock-identity" };
        for (const key of [
            "pid",
            "startTimeTicks",
            "bootId",
            "statRaw",
            "timeNamespace",
            "timeForChildrenNamespace",
            "timeOffsetsRaw",
            "timeOffsets",
            "python",
            "clock",
            "executable",
        ])
            capture[key] = null;
    }
    return summary;
}

test("collector raw identity independently rebuilds process, namespace, file continuity and capture enclosure", () => {
    const input = fixture(),
        original = structuredClone(input);
    const result = validateCollectorClockIdentity(input);
    assert.deepEqual(input, original);
    assert.equal(result.status, "raw-consistent");
    assert.equal(result.rawValidated, true);
    assert.equal(result.verified, false);
    assert.equal(result.hostAssociation, "unverified");
    assert.equal(result.performanceAccepted, false);
    assert.equal(result.runtimeSource.status, "unverified");
    assert.equal(result.identity.executableSha256, "a".repeat(64));
    assert.deepEqual(result.window, {
        beforeStartNs: "1000000",
        beforeFinishNs: "1100000",
        collectionStartNs: "1200000",
        collectionFinishNs: "2200000",
        firstSampleStartNs: "1300000",
        lastSampleFinishNs: "1900000",
        afterStartNs: "2000000",
        afterFinishNs: "2100000",
    });
    result.identity.timeOffsets.monotonic.seconds = "9";
    assert.deepEqual(input, original);
});

test("external expected hashes close only their own source bindings and never certify a shared clock", () => {
    const result = validateCollectorClockIdentity(fixture(), expected);
    assert.equal(result.runtimeSource.status, "matched");
    assert.deepEqual(result.runtimeSource.pendingGates, []);
    assert.deepEqual(result.pendingGates, ["host-reference-clock-binding", "predeclared-clock-quality-thresholds"]);
    assert.equal(result.verified, false);
    assert.equal(result.performanceAccepted, false);
    const noVersion = structuredClone(expected);
    delete noVersion.expected.hostPythonVersion;
    const partial = validateCollectorClockIdentity(fixture(), noVersion);
    assert.equal(partial.runtimeSource.status, "unverified");
    assert.equal(partial.runtimeSource.bindings.pythonExecutable, "matched");
    assert.deepEqual(partial.runtimeSource.pendingGates, ["pythonVersion"]);
    for (const [options, error] of [
        [{ hostBootId: "bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee" }, /BOOT_BINDING/],
        [{ componentSha256: "d".repeat(64) }, /COMPONENT_BINDING/],
        [{ expected: { hostPythonExecutableSha256: "b".repeat(64) } }, /PYTHON_EXECUTABLE_BINDING/],
        [{ expected: { hostPythonVersion: "3.12.4" } }, /PYTHON_VERSION_BINDING/],
    ])
        assert.throws(() => validateCollectorClockIdentity(fixture(), options), error);
});

test("legacy, explicit fixture and unavailable Linux captures retain their distinct missing-evidence states", () => {
    const legacy = fixture();
    delete legacy.clockIdentity;
    assert.equal(validateCollectorClockIdentity(legacy).status, "unavailable");
    assert.equal(validateCollectorClockIdentity(legacy).rawValidated, false);
    const synthetic = validateCollectorClockIdentity(fixtureOrigin(), expected);
    assert.equal(synthetic.status, "unknown");
    assert.equal(synthetic.identity, null);
    assert.equal(synthetic.runtimeSource.status, "unavailable");
    assert.equal(synthetic.runtimeSource.bindings.pythonExecutable, "unavailable");
    assert.equal(synthetic.verified, false);
    const missing = fixture();
    for (const capture of [missing.clockIdentity.before, missing.clockIdentity.after]) {
        capture.bootId = null;
        capture.unavailable.bootId = "permission-denied";
        capture.availability = "unknown";
    }
    missing.clockIdentity.availability = "unknown";
    const result = validateCollectorClockIdentity(missing, expected);
    assert.equal(result.status, "unknown");
    assert.equal(result.rawValidated, true);
    assert.equal(result.runtimeSource.bindings.hostBootId, "unavailable");
    assert.ok(result.pendingGates.includes("collector-clock-observations-unavailable"));
});

test("actual drift is independently rebuilt and preserved as unknown only with a partial terminal", () => {
    const changed = fixture();
    changed.clockIdentity.after.executable.metadata.inode = "99";
    changed.clockIdentity.availability = "unknown";
    changed.clockIdentity.changedFields = ["executable.metadata"];
    assert.throws(() => validateCollectorClockIdentity(changed), /DRIFT_TERMINAL/);
    Object.assign(changed, { status: "partial", identityContinuous: false });
    const result = validateCollectorClockIdentity(changed);
    assert.equal(result.status, "unknown");
    assert.deepEqual(result.changedFields, ["executable.metadata"]);
    changed.clockIdentity.changedFields = [];
    assert.throws(() => validateCollectorClockIdentity(changed), /CHANGED_FIELDS_REBUILD/);
});

test("ending executable read loss preserves raw failure evidence without inventing file continuity", () => {
    const summary = fixture();
    Object.assign(summary, { status: "partial", identityContinuous: false });
    const after = summary.clockIdentity.after;
    after.executable.metadata = null;
    after.unavailable.executable = "missing";
    after.availability = "unknown";
    Object.assign(summary.clockIdentity, { availability: "unknown", changedFields: ["executable.metadata"] });
    const result = validateCollectorClockIdentity(summary, expected);
    assert.equal(result.status, "unknown");
    assert.equal(result.runtimeSource.status, "unavailable");
    assert.deepEqual(result.changedFields, ["executable.metadata"]);
    assert.equal(result.verified, false);
    summary.clockIdentity.changedFields = [];
    assert.throws(() => validateCollectorClockIdentity(summary), /CHANGED_FIELDS_REBUILD/);
});

test("signed namespace offsets and multiline Python build descriptions retain their original meaning", () => {
    const summary = fixture();
    for (const capture of [summary.clockIdentity.before, summary.clockIdentity.after]) {
        capture.timeOffsetsRaw = "monotonic -1 999999999\nboottime 2 1\n";
        capture.timeOffsets = {
            monotonic: { seconds: "-1", nanoseconds: "999999999" },
            boottime: { seconds: "2", nanoseconds: "1" },
        };
        capture.python.version = "3.12.3 (main, fixture)\n[GCC 13.3.0]";
    }
    const result = validateCollectorClockIdentity(summary);
    assert.equal(result.status, "raw-consistent");
    assert.equal(result.identity.timeOffsets.monotonic.seconds, "-1");
    assert.equal(result.verified, false);
});

test("accepted sample windows bind independent raw samples without inventing absent timestamps", () => {
    const summary = fixture(),
        sampleWindow = ["1300000", "1900000"];
    assert.equal(validateCollectorClockIdentity(summary, { sampleWindow }).status, "raw-consistent");
    assert.throws(
        () => validateCollectorClockIdentity(summary, { sampleWindow: ["1300001", "1900000"] }),
        /SAMPLE_WINDOW_BINDING/,
    );
    delete summary.firstSampleStartedMonotonicNs;
    assert.throws(() => validateCollectorClockIdentity(summary, { sampleWindow }), /SAMPLE_WINDOW/);
    delete summary.lastSampleFinishedMonotonicNs;
    assert.throws(() => validateCollectorClockIdentity(summary), /SAMPLE_WINDOW/);
    assert.equal(validateCollectorClockIdentity(summary, { sampleWindow }).window.firstSampleStartNs, "1300000");
    summary.samples = 0;
    assert.equal(validateCollectorClockIdentity(summary).window.firstSampleStartNs, null);
});

test("deletion and forged observations fail even after enclosing JSON and digest are resealed", async (t) => {
    const directory = await mkdtemp(join(tmpdir(), "collector-clock-resealed-"));
    t.after(() => rm(directory, { recursive: true, force: true }));
    const mutations = [
        [
            "null identity",
            (s) => {
                s.clockIdentity = null;
            },
            /CLOCK_FIELDS/,
        ],
        [
            "missing raw stat",
            (s) => {
                delete s.clockIdentity.before.statRaw;
            },
            /CAPTURE_FIELDS/,
        ],
        [
            "wrong raw stat PID",
            (s) => {
                s.clockIdentity.before.statRaw = s.clockIdentity.before.statRaw.replace("123 (", "124 (");
            },
            /RAW_STAT/,
        ],
        [
            "wrong raw stat start",
            (s) => {
                s.clockIdentity.before.statRaw = s.clockIdentity.before.statRaw.replace("0 42 1000", "0 43 1000");
            },
            /RAW_STAT/,
        ],
        [
            "zombie raw process",
            (s) => {
                s.clockIdentity.before.statRaw = s.clockIdentity.before.statRaw.replace(") R ", ") Z ");
            },
            /RAW_STAT/,
        ],
        [
            "missing raw offsets",
            (s) => {
                delete s.clockIdentity.before.timeOffsetsRaw;
            },
            /CAPTURE_FIELDS/,
        ],
        [
            "parsed offset forgery",
            (s) => {
                s.clockIdentity.before.timeOffsets.monotonic.seconds = "1";
            },
            /OFFSETS_REBUILD/,
        ],
        [
            "raw offset overflow",
            (s) => {
                s.clockIdentity.before.timeOffsetsRaw = "monotonic 0 1000000000\nboottime 0 0\n";
            },
            /OFFSETS_RAW/,
        ],
        [
            "missing runtime SHA",
            (s) => {
                delete s.clockIdentity.before.executable.sha256;
            },
            /EXECUTABLE_FIELDS/,
        ],
        [
            "ending SHA pretends to rehash",
            (s) => {
                s.clockIdentity.after.executable.sha256 = "a".repeat(64);
            },
            /EXECUTABLE_DIGEST/,
        ],
        [
            "pathname substitutes actual executable",
            (s) => {
                s.clockIdentity.before.executable.sourcePath = "/usr/bin/python3.12";
            },
            /EXECUTABLE_SOURCE/,
        ],
        [
            "inode change concealed",
            (s) => {
                s.clockIdentity.after.executable.metadata.inode = "99";
            },
            /CHANGED_FIELDS_REBUILD/,
        ],
        [
            "namespace change concealed",
            (s) => {
                s.clockIdentity.after.timeNamespace = "time:[99]";
            },
            /CHANGED_FIELDS_REBUILD/,
        ],
        [
            "invented change list",
            (s) => {
                s.clockIdentity.changedFields = ["bootId"];
            },
            /CHANGED_FIELDS_REBUILD/,
        ],
        [
            "unknown claims observed",
            (s) => {
                s.clockIdentity.before.unavailable.bootId = "missing";
            },
            /HOST_IDENTITY/,
        ],
        [
            "missing unknown reason",
            (s) => {
                s.clockIdentity.before.bootId = null;
            },
            /IDENTITY_MISSING/,
        ],
        [
            "collector PID mismatch",
            (s) => {
                s.collectorPid = 124;
            },
            /SUMMARY_PID/,
        ],
        [
            "boot mismatch",
            (s) => {
                s.host.bootId = "bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee";
            },
            /SUMMARY_BOOT/,
        ],
        [
            "runtime version mismatch",
            (s) => {
                s.pythonVersion = "3.12.4";
            },
            /SUMMARY_PYTHON/,
        ],
        [
            "runtime executable mismatch",
            (s) => {
                s.pythonExecutable = "/different/python";
            },
            /SUMMARY_PYTHON/,
        ],
        [
            "reference clock declaration missing",
            (s) => {
                delete s.clockIdentity.before.clock.monotonic;
            },
            /INFO_FIELDS/,
        ],
        [
            "clock summary contradicts capture",
            (s) => {
                s.clock.resolutionSeconds = 1;
            },
            /SUMMARY_CLOCK/,
        ],
        [
            "capture before sampling finishes late",
            (s) => {
                s.clockIdentity.before.finishedMonotonicNs = "1400000";
            },
            /COLLECTION_WINDOW/,
        ],
        [
            "capture after sampling starts early",
            (s) => {
                s.clockIdentity.after.startedMonotonicNs = "1800000";
            },
            /SAMPLE_WINDOW/,
        ],
        [
            "capture after collection ends",
            (s) => {
                s.clockIdentity.after.finishedMonotonicNs = "2300000";
            },
            /COLLECTION_WINDOW/,
        ],
        [
            "unsafe integer nanoseconds",
            (s) => {
                s.clockIdentity.before.startedMonotonicNs = 1000000;
            },
            /INTEGER/,
        ],
        [
            "raised hash byte budget",
            (s) => {
                s.clockIdentity.executableHashMaxBytes *= 2;
            },
            /CLOCK_KIND/,
        ],
        [
            "self-certified clock",
            (s) => {
                s.clockIdentity.verified = true;
            },
            /CLOCK_KIND/,
        ],
        [
            "extra capture field",
            (s) => {
                s.clockIdentity.before.verified = true;
            },
            /CAPTURE_FIELDS/,
        ],
        [
            "fixture invents a host identity",
            (s) => {
                Object.assign(s, fixtureOrigin());
                s.clockIdentity.before.pid = 123;
            },
            /NONPRODUCTION_IDENTITY/,
        ],
    ];
    for (const [name, patch, error] of mutations)
        await t.test(name, async () => {
            const summary = fixture();
            patch(summary);
            const raw = Buffer.from(JSON.stringify(summary) + "\n");
            const entry = { path: "summary.json", bytes: raw.length, sha256: sha(raw) };
            await writeFile(join(directory, "summary.json"), raw);
            await writeFile(join(directory, "manifest.json"), JSON.stringify({ files: [entry] }) + "\n");
            const read = await readFile(join(directory, "summary.json"));
            const proof = JSON.parse(await readFile(join(directory, "manifest.json"), "utf8")).files[0];
            assert.equal(read.length, proof.bytes);
            assert.equal(sha(read), proof.sha256);
            assert.throws(() => validateCollectorClockIdentity(JSON.parse(read)), error);
        });
});

test("frozen v4 and newly available v5 Linux collector captures validate read-only with their own manifests", async (t) => {
    for (const version of ["v4", "v5"]) {
        const arms = join(
            ROOT,
            "evidence",
            `phase2-http-comparison-functional-${version}`,
            "remote/attempt/execution/arms",
        );
        let entries;
        try {
            entries = await readdir(arms, { withFileTypes: true });
        } catch (error) {
            if (error.code !== "ENOENT") throw error;
            entries = [];
        }
        let found = 0;
        for (const arm of entries) {
            if (!arm.isDirectory()) continue;
            const folder = join(arms, arm.name, "collector");
            let raw;
            try {
                raw = await readFile(join(folder, "summary.json"));
            } catch (error) {
                if (error.code !== "ENOENT") throw error;
                continue;
            }
            found++;
            await t.test(`${version}/${arm.name}`, async () => {
                const manifestRaw = await readFile(join(folder, "manifest.json"));
                const entry = JSON.parse(manifestRaw).files.find((item) => item.path === "summary.json");
                assert.equal(entry.bytes, raw.length);
                assert.equal(entry.sha256, sha(raw));
                const summary = JSON.parse(raw),
                    result = validateCollectorClockIdentity(summary);
                assert.equal(result.status, "raw-consistent");
                assert.equal(result.identity.pid, summary.collectorPid);
                assert.equal(result.verified, false);
                assert.equal(result.runtimeSource.status, "unverified");
                assert.deepEqual(await readFile(join(folder, "summary.json")), raw);
                assert.deepEqual(await readFile(join(folder, "manifest.json")), manifestRaw);
            });
        }
        if (!found)
            await t.test(
                `${version} collector evidence has not been imported into this snapshot`,
                { skip: true },
                () => {},
            );
    }
});
