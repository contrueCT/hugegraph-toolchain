/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, writeFile, rm, symlink } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createHash } from "node:crypto";
import { readTargetDiagnostics } from "../components/http/target-diagnostics.mjs";
const sha = (b) => createHash("sha256").update(b).digest("hex");
const plan = "a".repeat(64),
    origin = Date.parse("2026-10-25T00:00:00Z");
const window = {
    id: "r0001",
    caseId: "point",
    stage: "confirmation",
    loadId: "c1",
    repeat: 0,
    startedAt: new Date(origin).toISOString(),
    durationMs: 1000,
    phase: { name: "measurement", start: { sinceOriginMs: 1000 } },
};
const gc = "[2026-10-25T00:00:01.500+0000][1.5s][info][gc] GC(1) Pause Full (System.gc()) 32M->16M(64M) 100.000ms\n";
async function fixture(
    t,
    { samples = [0, 1000, 2000, 3000].map((n) => ({ atUnix: (origin + n) / 1000, swapTotalBytes: 0 })), log = gc } = {},
) {
    const root = await mkdtemp(join(tmpdir(), "hgbench-target-diag-"));
    t.after(() => rm(root, { recursive: true, force: true }));
    const hosts = samples.map((s) => JSON.stringify(s) + "\n").join("");
    await writeFile(join(root, "host.ndjson"), hosts);
    await writeFile(join(root, "gc.log"), log);
    const manifest = {
        schemaVersion: 1,
        kind: "throughput-target-diagnostics",
        planSha256: plan,
        maxSampleGapMs: 1500,
        hostSamples: {
            path: "host.ndjson",
            bytes: Buffer.byteLength(hosts),
            sha256: sha(hosts),
            format: "sf100-host-guard-v1",
        },
        gcLogs: [{ role: "server", path: "gc.log", bytes: Buffer.byteLength(log), sha256: sha(log) }],
    };
    const file = join(root, "manifest.json"),
        save = () => writeFile(file, JSON.stringify(manifest));
    await save();
    return { root, file, manifest, save };
}
test("measurement window keeps approximate GC overlap and sampled swap distinct from formal acceptance", async (t) => {
    const f = await fixture(t),
        result = await readTargetDiagnostics(f.file, plan, [window]);
    assert.equal(result.summary.rows[0].gc.loggedPauses, 1);
    assert.equal(result.summary.rows[0].gc.loggedPauseOverlapMs, 100);
    assert.equal(result.summary.rows[0].swap.status, "sampled-disabled");
    assert.equal(result.summary.rows[0].gc.completeCaptureVerified, false);
    assert.equal(result.summary.performanceAccepted, false);
    assert.match(result.summary.association, /not an exact clock-origin anchor/);
    const warm = { ...window, phase: { name: "warmup", start: { sinceOriginMs: 0 } } };
    assert.equal((await readTargetDiagnostics(f.file, plan, [warm])).summary.rows[0].gc.loggedPauses, 0);
});
test("swap enabled, missing, unbracketed and long-gap samples stay explicit", async (t) => {
    for (const [samples, wanted, reason] of [
        [
            [0, 1000, 2000].map((n) => ({ atUnix: (origin + n) / 1000, swapTotalBytes: n === 1000 ? 4096 : 0 })),
            "enabled-in-covering-samples",
            undefined,
        ],
        [[0, 1000, 2000].map((n) => ({ atUnix: (origin + n) / 1000 })), "unknown", "swap-metric-unavailable"],
        [[{ atUnix: (origin + 1500) / 1000, swapTotalBytes: 0 }], "unknown", "sample-range-does-not-enclose-window"],
        [
            [
                { atUnix: origin / 1000, swapTotalBytes: 0 },
                { atUnix: (origin + 3000) / 1000, swapTotalBytes: 0 },
            ],
            "unknown",
            "sample-gap",
        ],
    ]) {
        const f = await fixture(t, { samples });
        const r = (await readTargetDiagnostics(f.file, plan, [window])).summary.rows[0].swap;
        assert.equal(r.status, wanted);
        assert.equal(r.reason, reason);
    }
});
test("missing logs and malformed/truncated pauses do not establish zero GC", async (t) => {
    const f = await fixture(t, { log: gc.trimEnd() });
    let r = (await readTargetDiagnostics(f.file, plan, [window])).summary;
    assert.equal(r.logs[0].unknownPauseLines, 1);
    assert.equal(r.rows[0].gc.completeCaptureVerified, false);
    f.manifest.gcLogs = [];
    await f.save();
    r = (await readTargetDiagnostics(f.file, plan, [window])).summary;
    assert.equal(r.rows[0].gc.loggedPauses, null);
    assert.equal(r.rows[0].gc.status, "unavailable");
});
test("wrong plan, changed bytes, path escape, duplicate rotated events and wall-clock reversal reject attachment", async (t) => {
    const f = await fixture(t);
    await assert.rejects(readTargetDiagnostics(f.file, "b".repeat(64), [window]), /plan binding/);
    await writeFile(join(f.root, "gc.log"), gc + "tamper");
    await assert.rejects(readTargetDiagnostics(f.file, plan, [window]), /budget|digest/);
    await writeFile(join(f.root, "gc.log"), gc);
    f.manifest.gcLogs[0].path = "../gc.log";
    await f.save();
    await assert.rejects(readTargetDiagnostics(f.file, plan, [window]), /relative input/);
    f.manifest.gcLogs[0].path = "gc.log";
    f.manifest.gcLogs.push({ ...f.manifest.gcLogs[0], path: "rotated.log" });
    await writeFile(join(f.root, "rotated.log"), gc);
    await f.save();
    await assert.rejects(readTargetDiagnostics(f.file, plan, [window]), /duplicate pause/);
    const other = await fixture(t, {
        samples: [
            { atUnix: (origin + 2000) / 1000, swapTotalBytes: 0 },
            { atUnix: origin / 1000, swapTotalBytes: 0 },
        ],
    });
    await assert.rejects(readTargetDiagnostics(other.file, plan, [window]), /wall-clock reversal/);
});
test("existing standalone and passive-collector formats require actual SwapTotal", async (t) => {
    for (const format of ["http-standalone-observer-v1", "http-resource-collector-v1"]) {
        const samples = [0, 1000, 2000, 3000].map((n) =>
            format === "http-standalone-observer-v1"
                ? { time: (origin + n) / 1000, hostMemory: "SwapTotal: 0 kB\n" }
                : {
                      schemaVersion: 1,
                      kind: "http-resource-sample",
                      observedAt: new Date(origin + n).toISOString(),
                      host: { memoryBytes: { SwapTotal: "0" } },
                  },
        );
        const f = await fixture(t, { samples });
        f.manifest.hostSamples.format = format;
        await f.save();
        assert.equal(
            (await readTargetDiagnostics(f.file, plan, [window])).summary.rows[0].swap.status,
            "sampled-disabled",
        );
    }
});
test("archive path symlink cannot escape input root and absent wall-clock anchor stays unknown", async (t) => {
    const f = await fixture(t);
    await symlink(join(f.root, ".."), join(f.root, "escape"));
    f.manifest.gcLogs[0].path = "escape/outside.log";
    await f.save();
    await assert.rejects(readTargetDiagnostics(f.file, plan, [window]), /ENOENT|escapes/);
    f.manifest.gcLogs[0].path = "gc.log";
    await f.save();
    assert.equal(
        (await readTargetDiagnostics(f.file, plan, [{ ...window, startedAt: null }])).summary.rows[0].status,
        "unknown",
    );
});
