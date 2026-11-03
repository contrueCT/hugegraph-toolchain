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
import { mkdtemp, mkdir, readFile, writeFile, cp, rm } from "node:fs/promises";
import { appendFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { tmpdir } from "node:os";
import { createHash } from "node:crypto";
import { verifyResourceEvidence, verifyDriverEvidence } from "../components/http/resource-evidence.mjs";
import { lifecycleFixture } from "./http-resource-evidence.fixture.mjs";
const hash = (b) => createHash("sha256").update(b).digest("hex");
const read = async (p) => JSON.parse(await readFile(p, "utf8"));
const put = (p, v) => writeFile(p, JSON.stringify(v, null, 2) + "\n");
const MiB = 1024 * 1024;
async function fixture(t) {
    const root = await mkdtemp(join(tmpdir(), "http-resource-evidence-"));
    t.after(() => rm(root, { recursive: true, force: true }));
    const folder = join(root, "arm");
    await mkdir(folder);
    const arm = { runId: "arm-" + "a".repeat(40), arm: "A", position: 0 },
        start = 2000000000;
    const execution = {
        hostBootId: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        executionId: "a".repeat(32),
        runtimeDigest: "e".repeat(64),
    };
    const limits = { cpus: 1, memoryBytes: 1024 * MiB, pids: 128 };
    const env = {
        root: `${root}/work/${arm.runId}/environment`,
        owner: { uid: 1000, gid: 1000 },
        settings: { budget: limits },
    };
    const runtime = {
        settings: {
            sourceRoot: resolve("."),
            nodeHome: "/fixture/node",
            driver: {
                ...limits,
                image: "sha256:" + "a".repeat(64),
                python: "/usr/bin/python3",
                wallSeconds: 120,
                shutdownSeconds: 5,
                tmpfsBytes: 16 * MiB,
            },
            collector: { intervalMs: 1000, durationMs: 15000, maxSamples: 20, maxOutputBytes: MiB },
        },
    };
    const targetIdentity = {
        pid: 100,
        startTimeTicks: "1000",
        namespaces: { pid: "pid:[1]", net: "net:[2]", mnt: "mnt:[3]", user: "user:[4]" },
    };
    const targetContainer = "1".repeat(64);
    const generated = await lifecycleFixture({
        folder,
        env,
        runtime,
        execution,
        arm,
        start,
        targetIdentity,
        targetContainer,
    });
    const target = {
        containerId: targetContainer,
        pre: { identity: targetIdentity },
        post: { window: [BigInt(start + 6000000000), BigInt(start + 7000000000)] },
    };
    const receipt = {
        armStartedMonotonicNs: String(start),
        armFinishedMonotonicNs: String(start + 8000000000),
        workflowStartedMonotonicNs: String(start + 3000000000),
        workflowFinishedMonotonicNs: String(start + 5000000000),
    };
    const driverArgs = { arm, receipt, execution, runtime, target, targetPlan: env };
    const driver = await verifyDriverEvidence(join(folder, "controller"), driverArgs);
    const options = {
        hostBootId: execution.hostBootId,
        stopMarkerPath: join(folder, "controller/collector-stop.json"),
        workflowWindow: [receipt.workflowStartedMonotonicNs, receipt.workflowFinishedMonotonicNs],
        componentSha256: generated.summary.componentSha256,
        expectedRoles: driver.expectedRoles,
    };
    return { root, folder, driverArgs, options };
}
async function reseal(folder) {
    const root = join(folder, "collector"),
        manifest = await read(join(root, "manifest.json"));
    const trace = await readFile(join(root, "samples.ndjson"));
    const summary = await read(join(root, "summary.json"));
    summary.sampleBytes = trace.length;
    summary.sampleFile = { path: "samples.ndjson", bytes: trace.length, sha256: hash(trace) };
    await put(join(root, "summary.json"), summary);
    for (const f of manifest.files) {
        const raw = await readFile(join(root, f.path));
        f.bytes = raw.length;
        f.sha256 = hash(raw);
    }
    await put(join(root, "manifest.json"), manifest);
}
async function change(f, name, patch) {
    const folder = join(f.root, name);
    await cp(f.folder, folder, { recursive: true });
    await patch(folder);
    await reseal(folder);
    return folder;
}
async function samples(folder, patch) {
    const p = join(folder, "collector/samples.ndjson");
    const rows = (await readFile(p, "utf8")).trimEnd().split("\n").map(JSON.parse);
    patch(rows);
    await writeFile(p, rows.map((r) => JSON.stringify(r)).join("\n") + "\n");
}

test("resource evidence reconstructs Python producer records and binds Docker/proc lifecycle without running HTTP", async (t) => {
    const f = await fixture(t);
    const verify = (folder) =>
        verifyResourceEvidence(join(folder, "collector"), {
            ...f.options,
            stopMarkerPath: join(folder, "controller/collector-stop.json"),
        });
    const good = await verify(f.folder);
    assert.equal(good.sampleCount, 4);
    assert.equal(good.sourceOrigin, "fixture");
    assert.deepEqual(good.missingMetrics, { "driver.io.stat": { samples: 4, lastReason: "missing" } });
    assert.equal(good.coverage, "partial");
    assert.equal(good.performanceAccepted, false);
    assert.deepEqual(await verify(f.folder), good);
    for (const [name, patch, error] of [
        [
            "cpu-reset",
            (rows) => {
                rows[1].roles[0].values["cpu.stat"].usage_usec = "1";
            },
            /COUNTER_RESET/,
        ],
        [
            "cpu-delta-forged",
            (rows) => {
                rows[1].roles[0].interval.averageCpuCores = 9;
            },
            /INTERVAL_REBUILD/,
        ],
        [
            "empty-cpu",
            (rows) => {
                rows[0].roles[0].values["cpu.stat"] = {};
            },
            /EMPTY_COUNTER/,
        ],
        [
            "oom-counter-omitted",
            (rows) => {
                delete rows[0].roles[0].values["memory.events"].oom_kill;
            },
            /REQUIRED_COUNTER/,
        ],
        [
            "pressure-empty",
            (rows) => {
                rows[0].roles[0].values["cpu.pressure"] = {};
            },
            /PRESSURE/,
        ],
        [
            "pressure-invalid-average",
            (rows) => {
                rows[0].roles[0].values["cpu.pressure"].some.avg10 = 101;
            },
            /PRESSURE/,
        ],
        [
            "counter-outside-u64",
            (rows) => {
                rows[0].roles[0].values["memory.current"] = "18446744073709551616";
            },
            /COUNTER_RANGE/,
        ],
        [
            "io-array",
            (rows) => {
                rows[0].roles[0].values["io.stat"] = [];
            },
            /IO_SHAPE/,
        ],
        [
            "io-null-device",
            (rows) => {
                rows[0].roles[0].values["io.stat"] = { "8:0": null };
            },
            /IO_DEVICE/,
        ],
        [
            "disk-fields-omitted",
            (rows) => {
                rows[0].host.disks = { "8:0": { name: "sda" } };
            },
            /HOST_DISK/,
        ],
        [
            "identity-replaced",
            (rows) => {
                rows[2].roles[0].identity.startTimeTicks = "2000";
            },
            /IDENTITY_CHANGED/,
        ],
        [
            "missing-metric-promoted",
            (rows) => {
                rows[0].roles[1].values["io.stat"] = {};
            },
            /MISSING_REASONS/,
        ],
        [
            "outside-sample",
            (rows) => {
                rows[1].roles[0].finishedMonotonicNs = rows[2].finishedMonotonicNs;
            },
            /ROLE_WINDOW/,
        ],
    ])
        await t.test(name, async () => {
            const folder = await change(f, name, (p) => samples(p, patch));
            await assert.rejects(verify(folder), error);
        });
    await t.test("summary counters cannot be trusted after resealing", async () => {
        const folder = await change(f, "summary-forged", async (p) => {
            const path = join(p, "collector/summary.json"),
                s = await read(path);
            s.observedResources.target.maximumObservedMemoryCurrentBytes = "0";
            await put(path, s);
        });
        await assert.rejects(verify(folder), /SUMMARY_REBUILD/);
    });
    await t.test("stop-marker binding cannot be substituted", async () => {
        const folder = await change(f, "wrong-stop", async (p) => {
            const path = join(p, "controller/collector-stop.json"),
                s = await read(path);
            s.token = "b".repeat(32);
            await put(path, s);
        });
        await assert.rejects(verify(folder), /STOP_BINDING/);
    });
    await t.test("fractional CPU quota accepts only kernel microsecond quantization", async () => {
        const folder = await change(f, "fractional-cpu", (p) =>
            samples(p, (rows) => {
                for (const row of rows) row.roles[0].values["cpu.max"].quotaMicros = "33333";
            }),
        );
        const expectedRoles = structuredClone(f.options.expectedRoles);
        expectedRoles.target.limits.cpus = 0.333333333;
        const options = { ...f.options, expectedRoles, stopMarkerPath: join(folder, "controller/collector-stop.json") };
        assert.equal((await verifyResourceEvidence(join(folder, "collector"), options)).status, "verified");
        expectedRoles.target.limits.cpus = 0.34;
        await assert.rejects(verifyResourceEvidence(join(folder, "collector"), options), /CPU_LIMIT_DIFFER/);
    });
    await t.test("a legal constructor role does not reuse Object prototype properties", async () => {
        const folder = await change(f, "constructor-role", async (p) => {
            await samples(p, (rows) => {
                for (const row of rows) row.roles[1].role = "constructor";
            });
            const dir = join(p, "collector"),
                spec = await read(join(dir, "spec.json"));
            spec.roles[1].role = "constructor";
            await put(join(dir, "spec.json"), spec);
            const specSha = hash(await readFile(join(dir, "spec.json")));
            const markerPath = join(p, "controller/collector-stop.json"),
                marker = await read(markerPath);
            marker.specSha256 = specSha;
            await put(markerPath, marker);
            const markerRaw = await readFile(markerPath),
                summary = await read(join(dir, "summary.json"));
            summary.specSha256 = specSha;
            summary.identities[1].role = "constructor";
            summary.observedResources.constructor = summary.observedResources.driver;
            delete summary.observedResources.driver;
            summary.missingMetrics["constructor.io.stat"] = summary.missingMetrics["driver.io.stat"];
            delete summary.missingMetrics["driver.io.stat"];
            summary.controlledStop = {
                ...summary.controlledStop,
                request: marker,
                bytes: markerRaw.length,
                sha256: hash(markerRaw),
            };
            await put(join(dir, "summary.json"), summary);
        });
        const result = await verifyResourceEvidence(join(folder, "collector"), {
            ...f.options,
            expectedRoles: undefined,
            stopMarkerPath: join(folder, "controller/collector-stop.json"),
        });
        assert.equal(result.observedResources.constructor.samples, 4);
    });
    await t.test("first complete sample must precede actual workflow start", async () => {
        await assert.rejects(
            verifyResourceEvidence(join(f.folder, "collector"), {
                ...f.options,
                workflowWindow: ["2000000000", f.options.workflowWindow[1]],
            }),
            /WORKFLOW_COVERAGE/,
        );
    });
    await t.test("stream growth without newline is bounded before a complete line is parsed", async () => {
        const folder = await change(f, "growth", async () => {});
        let changed = false;
        const options = {
            ...f.options,
            stopMarkerPath: join(folder, "controller/collector-stop.json"),
            get componentSha256() {
                if (!changed) {
                    changed = true;
                    appendFileSync(join(folder, "collector/samples.ndjson"), Buffer.alloc(2 * MiB, 120));
                }
                return f.options.componentSha256;
            },
        };
        await assert.rejects(verifyResourceEvidence(join(folder, "collector"), options), /READ_BYTE_BUDGET/);
        assert.equal(changed, true);
    });
    for (const [name, file, patch, error] of [
        [
            "driver-oom",
            "driver-final.json",
            (v) => {
                v.State.OOMKilled = true;
            },
            /DRIVER_TERMINAL/,
        ],
        [
            "driver-not-exited",
            "driver-final.json",
            (v) => {
                v.State.Running = true;
            },
            /DRIVER_TERMINAL/,
        ],
        [
            "driver-mount-writable",
            "driver-started.json",
            (v) => {
                v.Mounts[0].RW = true;
            },
            /DRIVER_MOUNTS/,
        ],
        [
            "driver-image-changed",
            "driver-final.json",
            (v) => {
                v.Image = "sha256:" + "f".repeat(64);
            },
            /CONTAINER_IDENTITY/,
        ],
        [
            "driver-cpu-changed",
            "driver-final.json",
            (v) => {
                v.HostConfig.NanoCpus = 2e9;
            },
            /DRIVER_ISOLATION/,
        ],
        [
            "driver-pid-reused",
            "process-identities.json",
            (v) => {
                v.roles[1].hostPid++;
            },
            /PROC_STAT/,
        ],
        [
            "driver-unsafe-namespace-pid",
            "process-identities.json",
            (v) => {
                v.roles[1].statusRaw = v.roles[1].statusRaw.replace(/(NSpid:[^\n]+)\t1\n/, "$1\t9007199254740993\n");
            },
            /NAMESPACE_PID_RANGE/,
        ],
        [
            "driver-startticks-outside-u64",
            "process-identities.json",
            (v) => {
                v.roles[1].statRaw = v.roles[1].statRaw.replace("2000 0", "18446744073709551616 0");
            },
            /COUNTER_RANGE/,
        ],
        [
            "driver-zombie",
            "process-identities.json",
            (v) => {
                v.roles[1].statRaw = v.roles[1].statRaw.replace(") S ", ") Z ");
            },
            /PROC_NOT_LIVE/,
        ],
        [
            "driver-namespace-changed",
            "process-identities.json",
            (v) => {
                v.roles[2].namespaces.pid = "pid:[999]";
            },
            /SUPERVISOR_NAMESPACES/,
        ],
        [
            "driver-workflow-swapped",
            "workflow-result.json",
            (v) => {
                v.runId = "arm-other";
            },
            /WORKFLOW_IDENTITY/,
        ],
    ])
        await t.test(name, async () => {
            const folder = await change(f, name, async (p) => {
                const path = join(p, "controller", file),
                    v = await read(path);
                patch(v);
                await put(path, v);
            });
            await assert.rejects(verifyDriverEvidence(join(folder, "controller"), f.driverArgs), error);
        });
});
