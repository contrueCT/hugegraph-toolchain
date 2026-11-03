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
import { mkdtemp, readFile, writeFile, rm, cp } from "node:fs/promises";
import { join, resolve } from "node:path";
import { tmpdir } from "node:os";
import { promisify } from "node:util";
import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import {
    verifyClockEvidence,
    verifyClockRecords,
    mapClockEvent,
    relativePerformanceBounds,
    createClockContext,
    captureAnchor,
} from "../components/http/clock-correlation.mjs";
const exec = promisify(execFile),
    hash = (b) => createHash("sha256").update(b).digest("hex");
const script = resolve("components/http/clock-correlation.py"),
    read = async (p) => JSON.parse(await readFile(p, "utf8"));
const spec = {
    schemaVersion: 1,
    kind: "http-clock-correlation-spec",
    sourceOrigin: "fixture",
    sampleCount: 4,
    intervalMs: 20,
    exchangeTimeoutMs: 2000,
    wallMs: 10000,
    maxEvidenceBytes: 262144,
    maxRttNs: "2000000000",
    maxMappingWidthNs: "1000000000",
    maxDriftPpm: 1000,
};
const put = (p, v) => writeFile(p, JSON.stringify(v) + "\n");
async function fixture(t) {
    const root = await mkdtemp(join(tmpdir(), "clock-correlation-"));
    t.after(() => rm(root, { recursive: true, force: true }));
    const input = join(root, "input.json"),
        directory = join(root, "capture");
    await put(input, spec);
    const { stdout } = await exec(
        "python3",
        [script, "capture", "--spec", input, "--out", directory, "--node", process.execPath],
        { timeout: 15000 },
    );
    assert.equal(JSON.parse(stdout).ok, true);
    return { root, input, directory, verified: await verifyClockEvidence(directory) };
}
async function data(directory) {
    return {
        spec: await read(join(directory, "spec.json")),
        session: await read(join(directory, "session.json")),
        records: (await readFile(join(directory, "samples.ndjson"), "utf8")).trimEnd().split("\n").map(JSON.parse),
        terminal: await read(join(directory, "result.json")),
    };
}
function verify(d) {
    return verifyClockRecords(d.spec, d.session, d.records, d.terminal);
}
async function reseal(directory) {
    const manifest = await read(join(directory, "manifest.json"));
    for (const e of manifest.files) {
        const raw = await readFile(join(directory, e.path));
        e.bytes = raw.length;
        e.sha256 = hash(raw);
    }
    await put(join(directory, "manifest.json"), manifest);
}
function response(row, patch) {
    const r = JSON.parse(row.responseWire);
    patch(r);
    row.responseWire = JSON.stringify(r) + "\n";
}

test("real Python/Node fixture captures bounded causal evidence and independently rebuilds it", async (t) => {
    const f = await fixture(t),
        v = f.verified;
    assert.equal(v.sourceOrigin, "fixture");
    assert.equal(v.performanceAccepted, false);
    assert.equal(v.sampleCount, 4);
    assert.equal(v.hostBootId, null);
    assert.deepEqual(await verifyClockEvidence(f.directory), v);
    const anchor = v.anchors[1],
        mapped = mapClockEvent(v, {
            sessionId: v.sessionId,
            nodeIdentityDigest: v.nodeIdentityDigest,
            anchorNonce: anchor.nonce,
        });
    assert.equal(mapped.usable, true);
    assert.deepEqual(mapped.hostIntervalNs, anchor.hostIntervalNs);
    const relativeMs = (v.anchors[1].performanceMs + v.anchors[2].performanceMs) / 2 - v.anchors[0].performanceMs;
    const event = {
        sessionId: v.sessionId,
        nodeIdentityDigest: v.nodeIdentityDigest,
        phaseOriginNonce: v.anchors[0].nonce,
        relativeMs,
    };
    const relative = mapClockEvent(v, event);
    assert.equal(relative.usable, true);
    assert.deepEqual(relative.hostIntervalNs, [v.anchors[1].hostIntervalNs[0], v.anchors[2].hostIntervalNs[1]]);
    const narrow = await data(f.directory);
    narrow.spec.maxMappingWidthNs = "1";
    const narrowVerified = verify(narrow);
    assert.equal(mapClockEvent(narrowVerified, event).usable, false);
    assert.deepEqual(mapClockEvent(narrowVerified, event).hostIntervalNs, relative.hostIntervalNs);
    assert.throws(() => mapClockEvent({ ...v }, event), /NOT_VERIFIED/);
    assert.throws(() => mapClockEvent(JSON.parse(JSON.stringify(v)), event), /NOT_VERIFIED/);
    assert.throws(() => (v.anchors[0].hostIntervalNs[1] = "0"), TypeError);
    assert.equal(mapClockEvent(v, { ...event, relativeMs: 1e9 }).reason, "outside-strict-anchor-coverage");
    assert.throws(() => mapClockEvent(v, { ...event, nodeIdentityDigest: "0".repeat(64) }), /EVENT_IDENTITY/);
    await assert.rejects(
        exec("python3", [script, "capture", "--spec", f.input, "--out", f.directory, "--node", process.execPath]),
        (e) => e.code === 6 && JSON.parse(e.stdout).ok === false,
    );
    assert.deepEqual(await verifyClockEvidence(f.directory), v);
    const eventPath = join(f.root, "event.json");
    await put(eventPath, event);
    const cli = await exec(process.execPath, [
        resolve("components/http/clock-correlation.mjs"),
        "map",
        "--evidence",
        f.directory,
        "--event",
        eventPath,
    ]);
    assert.deepEqual(JSON.parse(cli.stdout).data, relative);
});

test("independent records reject resealed identity, order, clock, nonce and completion drift", async (t) => {
    const f = await fixture(t),
        original = await data(f.directory);
    const cases = [
        ["host-boot", (d) => (d.records[1].hostAfter.bootId = "forged"), /HOST_IDENTITY_CHANGED/],
        ["node-process", (d) => response(d.records[1], (r) => r.identityAfter.pid++), /NODE_IDENTITY_CHANGED/],
        [
            "node-restart",
            (d) =>
                response(d.records[1], (r) => {
                    r.identityBefore.processToken = "a".repeat(32);
                    r.identityAfter.processToken = "a".repeat(32);
                }),
            /NODE_IDENTITY_CHANGED/,
        ],
        ["clock-source", (d) => (d.session.hostClock.monotonic = false), /HOST_CLOCK/],
        ["nonce", (d) => response(d.records[1], (r) => (r.nonce = "b".repeat(32))), /RESPONSE_BINDING/],
        ["duplicate", (d) => (d.records[1] = structuredClone(d.records[0])), /FRAME_BUDGET/],
        ["host-reverse", (d) => (d.records[1].receiveMonotonicNs = "0"), /HOST_ORDER/],
        ["host-overlap", (d) => (d.records[1].sendMonotonicNs = d.records[0].sendMonotonicNs), /MONOTONIC_ORDER/],
        ["unsafe-ns-number", (d) => (d.records[1].receiveMonotonicNs = 12345678901234567), /INTEGER_PRECISION/],
        [
            "node-reverse",
            (d) => response(d.records[1], (r) => (r.hrtimeBeforeNs = "0")),
            /ANCHOR_ORDER|MONOTONIC_ORDER/,
        ],
        ["performance-jump", (d) => response(d.records[1], (r) => (r.performanceMs += 10000)), /OBSERVED_NODE_RATE/],
        [
            "hrtime-jump",
            (d) =>
                response(d.records[1], (r) => {
                    r.hrtimeBeforeNs = String(BigInt(r.hrtimeBeforeNs) + 10000000000n);
                    r.hrtimeAfterNs = String(BigInt(r.hrtimeAfterNs) + 10000000000n);
                }),
            /OBSERVED_HOST_RATE/,
        ],
        ["rtt", (d) => (d.spec.maxRttNs = "1"), /RTT_BUDGET/],
        ["missing-sample", (d) => d.records.pop(), /SAMPLE_COUNT/],
        ["failure", (d) => (d.terminal.status = "failed"), /CAPTURE_INCOMPLETE/],
        ["source-change", (d) => (d.terminal.inputsReverified = false), /CAPTURE_INCOMPLETE/],
        ["stdout-accounting", (d) => d.terminal.stdoutBytes++, /TRANSPORT_ACCOUNTING/],
        ["sample-pacing", (d) => (d.spec.intervalMs = 1000), /SAMPLE_INTERVAL/],
        ["wrong-child", (d) => d.terminal.childPid++, /CHILD_IDENTITY/],
        [
            "wall",
            (d) => (d.terminal.finishedMonotonicNs = String(BigInt(d.session.startedMonotonicNs) + 10000000001n)),
            /WALL_BUDGET/,
        ],
    ];
    for (const [name, patch, error] of cases)
        await t.test(name, () => {
            const d = structuredClone(original);
            patch(d);
            assert.throws(() => verify(d), error);
        });
    const changed = join(f.root, "changed");
    await cp(f.directory, changed, { recursive: true });
    const rows = structuredClone(original.records);
    response(rows[1], (r) => (r.performanceMs += 10000));
    await writeFile(join(changed, "samples.ndjson"), rows.map(JSON.stringify).join("\n") + "\n");
    await assert.rejects(verifyClockEvidence(changed), /DIGEST|FILE_BUDGET/);
    await reseal(changed);
    await assert.rejects(verifyClockEvidence(changed), /OBSERVED_NODE_RATE/);
    await writeFile(join(changed, "extra"), "x");
    await assert.rejects(verifyClockEvidence(changed), /INVENTORY/);
});

test("binary64 relative arithmetic is enclosed without unsafe millisecond multiplication", () => {
    for (const origin of [0, 0.1, 123.456, 1e8, 1e11])
        for (const increment of [1e-9, 0.1, 1, 123.456]) {
            const target = origin + increment,
                relative = target - origin;
            const [low, high] = relativePerformanceBounds(origin, relative);
            assert.ok(Number(low) / 1e6 <= target && Number(high) / 1e6 >= target);
            assert.ok(high >= low);
        }
    assert.throws(() => relativePerformanceBounds(1, NaN), /PERFORMANCE_RANGE/);
    assert.throws(() => relativePerformanceBounds(1, -1), /PERFORMANCE_RANGE/);
    assert.deepEqual(relativePerformanceBounds(1, -0), relativePerformanceBounds(1, 0));
    const [largeLow, largeHigh] = relativePerformanceBounds(1e12, 1e12);
    assert.ok(largeLow <= 2000000000000000000n && largeHigh >= 2000000000000000000n);
    const c = createClockContext({ sourceOrigin: "fixture", sessionId: "a".repeat(32) });
    const a = captureAnchor(c, { sequence: 0, nonce: "b".repeat(32) });
    assert.equal(a.sessionId, c.sessionId);
    assert.deepEqual(a.identityBefore, a.identityAfter);
    assert.ok(BigInt(a.hrtimeBeforeNs) <= BigInt(a.hrtimeAfterNs));
    a.identityBefore.runtime.version = "mutated-return-value";
    assert.notEqual(
        captureAnchor(c, { sequence: 1, nonce: "c".repeat(32) }).identityBefore.runtime.version,
        "mutated-return-value",
    );
});

test("Linux-shaped fixtures require same boot, exact process identity and stable time namespaces", async (t) => {
    const f = await fixture(t),
        d = await data(f.directory);
    d.spec.sourceOrigin = d.session.sourceOrigin = "linux-procfs";
    d.session.hostClock.implementation = "clock_gettime(CLOCK_MONOTONIC)";
    const linux = (i) =>
        Object.assign(i, {
            sourceOrigin: "linux-procfs",
            platform: "linux",
            bootId: "01234567-1234-1234-1234-0123456789ab",
            startTimeTicks: "12345",
            timeNamespace: "time:[4026531834]",
            timeForChildrenNamespace: "time:[4026531834]",
            timeOffsets: "monotonic           0         0\nboottime            0         0\n",
        });
    linux(d.session.hostIdentity);
    for (const row of d.records) {
        linux(row.hostBefore);
        linux(row.hostAfter);
        response(row, (r) => {
            linux(r.identityBefore);
            linux(r.identityAfter);
        });
    }
    d.terminal.stdoutBytes = d.records.reduce((sum, row) => sum + Buffer.byteLength(row.responseWire), 0);
    assert.equal(verify(d).sourceOrigin, "linux-procfs"); // Synthetic parser coverage, no Linux runtime claim.
    for (const [key, changed, expected] of [
        ["bootId", "ffffffff-1234-1234-1234-0123456789ab", /BOOT_MISMATCH/],
        ["timeNamespace", "time:[2]", /NAMESPACE_MISMATCH/],
        ["timeOffsets", "monotonic 1 0\nboottime 0 0\n", /NAMESPACE_MISMATCH/],
    ]) {
        const copy = structuredClone(d);
        for (const row of copy.records)
            response(row, (r) => {
                r.identityBefore[key] = r.identityAfter[key] = changed;
            });
        assert.throws(() => verify(copy), expected);
    }
    const unsafe = structuredClone(d);
    response(unsafe.records[0], (r) => (r.identityBefore.pid = 9007199254740993));
    assert.throws(() => verify(unsafe), /CLOCK_IDENTITY/);
    const drift = structuredClone(d);
    response(drift.records[2], (r) => {
        r.identityBefore.startTimeTicks = r.identityAfter.startTimeTicks = "56789";
    });
    assert.throws(() => verify(drift), /NODE_IDENTITY_CHANGED/);
});
