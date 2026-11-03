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

import { createReadStream } from "node:fs";
import { createInterface } from "node:readline";
import { mkdir, readFile, readdir, lstat, writeFile } from "node:fs/promises";
import { join, resolve, dirname } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { isDeepStrictEqual } from "node:util";
import { digest, json, put, entry } from "./common.mjs";
import { verifyGeneratorAssets } from "./generator-assets.mjs";
import { httpGeneratorRequestSchema } from "../../dist/http-generator-contracts.js";
import { inspect } from "../../benchmarks/http-serving-v0.1/reference.mjs";
import { analyze } from "./analyzer.mjs";
import { analyzePhaseWindows } from "./phase-windows.mjs";
import { readGoHostClockProof, readGoMeasurementAssociation } from "./go-clock-evidence.mjs";
import { verifyResourceEvidence } from "./resource-evidence.mjs";
const need = (v, m) => {
    if (!v) throw new Error("GENERATOR_EVIDENCE: " + m);
};
const same = (a, b, m) => need(isDeepStrictEqual(a, b), m);
const packageRoot = resolve(dirname(fileURLToPath(import.meta.url)), "../..");
async function inventory(root, files, exact = false) {
    const seen = new Set();
    need(Array.isArray(files) && files.length <= 20000, "inventory bound");
    for (const f of files) {
        need(
            typeof f.path === "string" &&
                !f.path.startsWith("/") &&
                !f.path.split("/").includes("..") &&
                !seen.has(f.path),
            "inventory path",
        );
        seen.add(f.path);
        const path = join(root, f.path),
            info = await lstat(path);
        need(
            info.isFile() && !info.isSymbolicLink() && info.size === f.bytes && (await digest(path)) === f.sha256,
            "inventory hash: " + f.path,
        );
    }
    if (exact) {
        const actual = [];
        async function visit(dir, prefix = "") {
            for (const name of await readdir(dir)) {
                if (!prefix && name === "delivery.lock.json") continue;
                const relative = prefix + name,
                    path = join(dir, name),
                    info = await lstat(path);
                if (info.isDirectory()) await visit(path, relative + "/");
                else {
                    need(info.isFile() && !info.isSymbolicLink(), "regular evidence only");
                    actual.push(relative);
                }
            }
        }
        await visit(root);
        same(actual.sort(), [...seen].sort(), "complete evidence inventory");
    }
}
export async function verifyGeneratorPlan(root, verifySources = true) {
    const plan = await json(join(root, "plan.json")),
        lock = await json(join(root, "plan.lock.json"));
    need(
        plan.schemaVersion === 1 &&
            plan.kind === "http-generator-calibration-plan" &&
            plan.environmentKind === "http-generator-calibration" &&
            plan.performanceAccepted === false &&
            lock.planSha256 === (await digest(join(root, "plan.json"))),
        "plan lock/identity",
    );
    httpGeneratorRequestSchema.parse(plan.request);
    const suite = await inspect();
    same(plan.cases, suite.cases, "full exact eleven cases");
    same(plan.encodings, ["identity", "gzip"], "both encodings");
    same(
        plan.arms,
        [4, 6, 6, 4].map((cpus, i) => ({
            id: `a${i + 1}`,
            block: Math.floor(i / 2) + 1,
            cpus,
            memoryBytes: 3221225472,
            gomaxprocs: 4,
        })),
        "balanced fixed resources",
    );
    need(plan.ratePerSecond === 80 && plan.maxInFlight === 64, "fixed offered load");
    same(plan.provider, { cpus: 1, memoryBytes: 536870912 }, "provider resources");
    await verifyGeneratorAssets(join(root, "assets"));
    await inventory(root, plan.assetsInventory);
    if (verifySources) await inventory(packageRoot, plan.sourceInventory);
    else {
        const required = [
            "generator-evidence.mjs",
            "generator-assets.mjs",
            "analyzer.mjs",
            "resource-evidence.mjs",
            "phase-windows.mjs",
            "go-clock-evidence.mjs",
            "go-engine.mjs",
            "common.mjs",
        ];
        for (const name of required) {
            const file = plan.sourceInventory.find((f) => f.path === "components/http/" + name);
            need(file && (await digest(join(packageRoot, file.path))) === file.sha256, "frozen reader method identity");
        }
    }
    const fixed = 88 * (plan.request.timing.warmupMs + plan.request.timing.durationMs);
    need(
        plan.fixedWindowMs === fixed &&
            plan.plannedMeasurementArrivals === 88 * Math.ceil((80 * plan.request.timing.durationMs) / 1000),
        "workload denominator",
    );
    return plan;
}
function tolerance(a, b) {
    return Number.isFinite(a) && Number.isFinite(b)
        ? {
              differenceMs: Math.abs(a - b),
              budgetMs: Math.max(1, 0.1 * Math.min(a, b)),
              passed: Math.abs(a - b) <= Math.max(1, 0.1 * Math.min(a, b)),
          }
        : { differenceMs: null, budgetMs: null, passed: null };
}
const median = (values) => {
    const sorted = [...values].sort((a, b) => a - b);
    return sorted.length
        ? sorted.length % 2
            ? sorted[(sorted.length - 1) / 2]
            : (sorted[sorted.length / 2 - 1] + sorted[sorted.length / 2]) / 2
        : null;
};
function referenceTolerance(a, b) {
    if (!Number.isFinite(a) || !Number.isFinite(b)) return { differenceMs: null, budgetMs: null, passed: null };
    const differenceMs = Math.abs(a - b),
        budgetMs = Math.max(1, 0.1 * a);
    return { differenceMs, budgetMs, passed: differenceMs <= budgetMs };
}
function warmup(windows) {
    const run = windows.runs[0],
        warm = run.phases.find((p) => p.name === "warmup"),
        measure = run.phases.find((p) => p.name === "measurement");
    const result = [];
    for (const quantile of ["p50", "p95"]) {
        const values = (phase) =>
            phase?.windows
                .filter((w) => w.endMs - w.startMs === 10000)
                .map((w) => w.metrics.correctResponseMs[quantile]) ?? [];
        const a = values(warm),
            b = values(measure);
        const enough = a.length >= 2 && b.length >= 2 && [...a, ...b].every(Number.isFinite);
        result.push({
            quantile,
            transition: referenceTolerance(enough ? median(a.slice(-2)) : null, enough ? median(b.slice(0, 2)) : null),
            trend: referenceTolerance(enough ? median(b.slice(0, 2)) : null, enough ? median(b.slice(-2)) : null),
        });
    }
    return result;
}
function checkContainers(containers, arm, plan, owner) {
    for (const [role, cpu, memory] of [
        ["provider", 1, 536870912],
        ["driver", arm.cpus, arm.memoryBytes],
    ]) {
        const info = containers[role],
            h = info.HostConfig;
        need(
            info.Image === plan.request.runtime.image &&
                /^[a-f0-9]{64}$/.test(info.Id) &&
                info.State.Running &&
                h.NanoCpus === cpu * 1e9 &&
                h.Memory === memory &&
                h.MemorySwap === memory &&
                h.PidsLimit === 128 &&
                info.Config.User === `${plan.user.uid}:${plan.user.gid}` &&
                h.ReadonlyRootfs &&
                h.RestartPolicy.Name === "no" &&
                info.Config.Labels["hgbench.generator.owner"] === owner,
            "actual Docker limits/ownership",
        );
    }
    need(
        containers.provider.Id !== containers.driver.Id &&
            containers.driver.HostConfig.NetworkMode === "container:" + containers.provider.Id,
        "separate cgroups with exact provider network",
    );
}
function providerPressure(samples, window) {
    const started = BigInt(window.startedMonotonicNs),
        finished = BigInt(window.finishedMonotonicNs);
    const relevant = samples
        .filter(
            (s, i) =>
                i && BigInt(s.finishedMonotonicNs) >= started && BigInt(samples[i - 1].startedMonotonicNs) <= finished,
        )
        .map((s) => s.roles.find((r) => r.role === "provider"));
    const issues = [];
    let maxCpuCores = 0,
        throttledUsec = 0;
    for (const role of relevant) {
        const delta = role?.interval;
        if (!delta) {
            issues.push("provider-resource-unavailable");
            continue;
        }
        // CPU averages from the independent collector's original monotonic intervals.
        const cpu = delta;
        if (cpu?.averageCpuCores === undefined || cpu.averageCpuCores === null) issues.push("provider-cpu-unavailable");
        else maxCpuCores = Math.max(maxCpuCores, cpu.averageCpuCores);
        const throttle = delta.counters?.["cpu.stat"]?.throttled_usec;
        if (throttle === undefined || throttle === null) issues.push("provider-throttle-unavailable");
        else throttledUsec += Number(throttle);
    }
    if (!relevant.length) issues.push("provider-resource-unavailable");
    if (maxCpuCores > 0.8 || throttledUsec > 0) issues.push("provider-pressure-observed");
    return {
        samples: relevant.length,
        maxCpuCores,
        throttledUsec,
        issues: [...new Set(issues)],
        scope: "Conservative whole-workflow overlapping collector intervals, including boundary intervals; no request-level attribution",
    };
}
export async function verifyGeneratorWireTrace(path, variant, encoding) {
    const stream = createReadStream(path),
        lines = createInterface({ input: stream, crlfDelay: Infinity });
    let completeResponses = 0;
    try {
        for await (const line of lines) {
            need(line.length > 0 && line.length < 65536, "wire trace line bound");
            const sample = JSON.parse(line);
            if (!sample.completeResponse) continue;
            need(
                sample.bytes === variant.bytes &&
                    sample.bodySha256 === variant.sha256 &&
                    sample.responseEncoding === encoding,
                "fixed wire bytes/encoding differ",
            );
            completeResponses++;
        }
    } finally {
        lines.close();
        stream.destroy();
    }
    return { completeResponses, wireSha256: variant.sha256, bytesPerResponse: variant.bytes, encoding };
}
export async function reportGenerator(root, out) {
    const plan = await verifyGeneratorPlan(root, false),
        output = join(root, "output");
    const lock = await json(join(output, "delivery.lock.json"));
    need(lock.kind === "http-generator-calibration-evidence", "delivery kind");
    await inventory(output, lock.files, true);
    const execution = await json(join(output, "execution.json")),
        result = await json(join(output, "result.json")),
        cleanup = await json(join(output, "cleanup.json"));
    need(
        execution.planSha256 === (await digest(join(root, "plan.json"))) &&
            execution.environmentKind === "http-generator-calibration",
        "execution plan",
    );
    const globalIssues = [];
    let proof;
    try {
        proof = await readGoHostClockProof(join(output, "clock-proof"), plan.request.binary);
        need(
            proof.imageId === plan.request.runtime.image && proof.hostBootId === execution.hostBootId,
            "clock proof image/boot",
        );
    } catch (error) {
        globalIssues.push(String(error));
    }

    const assetsManifest = await json(join(root, "assets/assets.json"));
    const cells = [],
        armReports = [];
    for (const arm of plan.arms) {
        const folder = join(output, arm.id);
        let armEvidence;
        try {
            const before = await json(join(folder, "containers-before.json")),
                after = await json(join(folder, "containers-after.json"));
            checkContainers(before, arm, plan, execution.owner);
            checkContainers(after, arm, plan, execution.owner);
            same(before.provider.Id, after.provider.Id, "provider identity continuous");
            same(before.driver.Id, after.driver.Id, "driver identity continuous");
            const window = await json(join(folder, "window.json")),
                summary = await json(join(folder, "collector/summary.json"));
            const identities = summary.identities;
            const collector = await verifyResourceEvidence(join(folder, "collector"), {
                hostBootId: execution.hostBootId,
                componentSha256: plan.sourceInventory.find((f) => f.path === "components/http/resource-collector.py")
                    .sha256,
                stopMarkerPath: join(folder, "collector-stop.json"),
                workflowWindow: [window.startedMonotonicNs, window.finishedMonotonicNs],
                expectedRoles: Object.fromEntries(
                    ["provider", "driver"].map((role, i) => [
                        role,
                        {
                            hostPid: before[role].State.Pid,
                            startTimeTicks: identities[i].startTimeTicks,
                            cgroupMember: before[role].Id + ".scope",
                            limits: {
                                cpus: role === "provider" ? 1 : arm.cpus,
                                memoryBytes: role === "provider" ? 536870912 : arm.memoryBytes,
                                pids: 128,
                            },
                        },
                    ]),
                ),
            });
            const samples = (await readFile(join(folder, "collector/samples.ndjson"), "utf8"))
                .trim()
                .split("\n")
                .map(JSON.parse);
            const provider = await json(join(folder, "provider/summary.json")),
                ready = await json(join(folder, "provider/ready.json"));
            need(
                provider.status === "completed" &&
                    provider.reason === "SIGTERM" &&
                    !provider.failure &&
                    Object.values(provider.errors).every((v) => v === 0) &&
                    ready.environmentKind === "http-generator-calibration" &&
                    ready.assetsManifestSha256 === (await digest(join(root, "assets/assets.json"))) &&
                    ready.sourceSha256 ===
                        plan.sourceInventory.find((f) => f.path === "components/http/generator-provider.mjs").sha256,
                "provider identity/errors/terminal",
            );
            const captured = await json(join(folder, "provider-host-process.json"));
            need(
                captured.hostPid === before.provider.State.Pid &&
                    captured.executableSha256 === ready.executableSha256 &&
                    captured.readySha256 === (await digest(join(folder, "provider/ready.json"))) &&
                    ready.node.startsWith("v22.") &&
                    ready.executableSha256 === plan.runtimeFiles.find((f) => f.role === "node")?.sha256,
                "provider host PID/executable binding",
            );
            const statStart = captured.statRaw
                .slice(captured.statRaw.lastIndexOf(")") + 1)
                .trim()
                .split(/\s+/)[19];
            need(statStart === identities[0].startTimeTicks, "provider host start binding");
            armEvidence = { before, after, collector, samples, provider };
            armReports.push({ arm: arm.id, evidence: "verified", collector, provider });
        } catch (error) {
            armReports.push({ arm: arm.id, evidence: "unavailable-or-invalid", error: String(error) });
        }
        for (const item of plan.cases)
            for (const encoding of plan.encodings) {
                const cell = {
                    arm: arm.id,
                    block: arm.block,
                    cpus: arm.cpus,
                    caseId: item.id,
                    encoding,
                    status: "unknown",
                    issues: [...globalIssues],
                    performanceAccepted: false,
                };
                try {
                    const unit = join(folder, "units", item.id + "-" + encoding),
                        raw = join(unit, "attempt/output"),
                        analysis = await analyze(raw),
                        profile = analysis.profile;
                    need(
                        profile.schemaVersion === 2 &&
                            profile.executor.hostIdentityGate === true &&
                            profile.executor.cpuQuotaCores === arm.cpus &&
                            profile.executor.maxRssBytes === arm.memoryBytes &&
                            profile.executor.gomaxprocs === 4 &&
                            profile.executor.maxPendingBytes === 1048576 &&
                            profile.responseEncoding === encoding &&
                            profile.endpoint === "http://127.0.0.1:18080/gremlin" &&
                            profile.repeats === 1 &&
                            profile.maxResponseBytes === 2097152 &&
                            profile.serviceChecks &&
                            profile.target.mode === "http-generator-calibration",
                        "raw workload declaration",
                    );
                    same(profile.executor.binary, plan.request.binary, "registered binary");
                    same(profile.cases, [item], "case request/answer");
                    same(
                        profile.loads,
                        [{ id: "open-80", model: "open", ratePerSecond: 80, maxInFlight: 64, maxStartDelayMs: 1000 }],
                        "offered load",
                    );
                    for (const [key, value] of Object.entries(plan.request.timing)) same(profile[key], value, "timing");
                    const manifest = await json(join(raw, "raw.lock.json"));
                    for (const relative of ["preflight/p0001", "runs/r0001", "postcheck/p0001"]) {
                        await readGoMeasurementAssociation(join(unit, "host-processes", relative + ".json.identity"), {
                            clockProof: join(output, "clock-proof"),
                            runFile: join(raw, relative + ".json"),
                            requestFile: join(
                                raw,
                                relative.startsWith("runs/") ? "requests/r0001.json" : relative + ".request.json",
                            ),
                            markerDirectory: join(raw, relative + ".json.identity"),
                        });
                    }
                    const variant = assetsManifest.entries.find((e) => e.id === item.id).variants[encoding];
                    cell.fixedWire = [];
                    for (const path of ["preflight/p0001.ndjson", "traces/r0001.ndjson", "postcheck/p0001.ndjson"])
                        cell.fixedWire.push(await verifyGeneratorWireTrace(join(raw, path), variant, encoding));
                    const implementation = await json(join(raw, "implementation.json"));
                    need(
                        implementation.nodeExecutableSha256 ===
                            plan.runtimeFiles.find((f) => f.role === "node")?.sha256,
                        "actual driver Node executable",
                    );
                    const run = await json(join(raw, "runs/r0001.json")),
                        row = analysis.runs[0];
                    need(manifest.runs.length === 1 && analysis.runs.length === 1, "single cell raw denominator");
                    const windows = await analyzePhaseWindows(raw, {
                        windowMs: 10000,
                        minimumRequestsPerWindow: 50,
                        minimumP99Requests: 1000,
                    });
                    cell.quality = {
                        scheduled: row.scheduled,
                        correct: row.correct,
                        failed: row.failed,
                        dropped: row.dropped,
                        late: row.late,
                        responseP99Ms: row.correctResponseMs.p99,
                        completedInWindowRps: row.correctCompletedInWindowPerSecond,
                    };
                    cell.resources = run.phases.map((p) => ({ phase: p.name, resources: p.resources }));
                    cell.issues.push(...row.issues);
                    if (run.phases.some((p) => p.failed || p.dropped || p.late || p.cancelled || p.budgetExhausted))
                        cell.issues.push("phase-quality-failed");
                    cell.warmup = warmup(windows);
                    if (cell.warmup.some((v) => v.transition.passed !== true || v.trend.passed !== true))
                        cell.issues.push("warmup-or-trend-unqualified");
                    if (row.correct < 1000) cell.issues.push("p99-insufficient-samples");
                    if (!armEvidence) cell.issues.push("arm-environment-unverified");
                    else {
                        const window = await json(join(unit, "workflow.json"));
                        cell.provider = providerPressure(armEvidence.samples, window);
                        cell.issues.push(...cell.provider.issues, ...armEvidence.collector.qualityIssues);
                        const count = armEvidence.provider.counts[item.id + "/" + encoding];
                        const sent = run.phases.reduce((n, p) => n + p.sent, 0) + 2;
                        need(
                            count.accepted === sent &&
                                count.finished === sent &&
                                count.aborted === 0 &&
                                count.bytes ===
                                    sent *
                                        (await json(join(root, "assets/assets.json"))).entries.find(
                                            (e) => e.id === item.id,
                                        ).variants[encoding].bytes,
                            "provider complete send accounting",
                        );
                    }
                    cell.status = cell.issues.length ? "not-qualified" : "eligible";
                } catch (error) {
                    cell.issues.push(String(error));
                }
                cells.push(cell);
            }
    }
    const comparisons = [];
    for (const item of plan.cases)
        for (const encoding of plan.encodings) {
            const group = cells.filter((c) => c.caseId === item.id && c.encoding === encoding);
            const comparisonsForUnit = [
                [0, 1],
                [3, 2],
                [0, 3],
                [1, 2],
            ].map(([a, b]) => ({
                arms: [group[a].arm, group[b].arm],
                ...tolerance(group[a].quality?.responseP99Ms, group[b].quality?.responseP99Ms),
            }));
            comparisons.push({
                caseId: item.id,
                encoding,
                comparisons: comparisonsForUnit,
                status:
                    group.every((c) => c.status === "eligible") && comparisonsForUnit.every((c) => c.passed === true)
                        ? "engineering-pass"
                        : "unknown-or-not-qualified",
            });
        }
    const engineering =
        globalIssues.length === 0 &&
        plan.request.purpose === "engineering-calibration" &&
        plan.request.timing.warmupMs === 30000 &&
        plan.request.timing.durationMs === 60000 &&
        result.state === "completed" &&
        result.completedUnits.length === 88 &&
        cleanup.errors.length === 0 &&
        cleanup.remainingOwned.length === 0 &&
        comparisons.every((c) => c.status === "engineering-pass");
    const report = {
        schemaVersion: 1,
        kind: "http-generator-calibration-report",
        environmentKind: "http-generator-calibration",
        planSha256: await digest(join(root, "plan.json")),
        deliveryLockSha256: await digest(join(output, "delivery.lock.json")),
        plannedUnits: 88,
        executionState: result.state,
        cells,
        globalIssues,
        comparisons,
        armReports,
        cleanup,
        driverHeadroomEngineeringAccepted: engineering,
        performanceAccepted: false,
        statisticalCertificationAccepted: false,
        serverCapacityAccepted: false,
        scope: "Generator at 80rps/IF64, exact Go binary, CPU4/6 memory3GiB GMP4, fixed responses only. Real HugeGraph requires its own per-point checks; no graph performance claim.",
    };
    await mkdir(out, { mode: 0o700 });
    await put(join(out, "report.json"), report);
    const escaped = JSON.stringify(report).replaceAll("<", "\u003c");
    await writeFile(
        join(out, "report.html"),
        `<!doctype html><meta charset="utf-8"><title>HTTP generator calibration</title>
<style>body{font:16px system-ui;margin:2em}table{border-collapse:collapse}td,th{padding:.4em;border:1px solid #ccc}input{padding:.5em}</style>
<h1>HTTP generator calibration</h1><p id="scope"></p><label>Filter <input id="filter"></label><table><thead><tr><th>Arm</th><th>Case</th><th>Encoding</th><th>State</th><th>Correct</th><th>Dropped</th><th>P99 ms</th><th>Issues</th></tr></thead><tbody></tbody></table>
<script type="application/json" id="data">${escaped}</script><script>const d=JSON.parse(document.querySelector('#data').textContent);document.querySelector('#scope').textContent=d.scope+' Engineering accepted: '+d.driverHeadroomEngineeringAccepted;function draw(){const body=document.querySelector('tbody');body.replaceChildren();for(const c of d.cells){if(!JSON.stringify(c).includes(document.querySelector('#filter').value))continue;const tr=body.insertRow();for(const v of [c.arm,c.caseId,c.encoding,c.status,c.quality?.correct,c.quality?.dropped,c.quality?.responseP99Ms,c.issues.join('; ')])tr.insertCell().textContent=v??'unknown';}}document.querySelector('#filter').oninput=draw;draw();</script>`,
        { flag: "wx", mode: 0o600 },
    );
    return report;
}
if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href)
    await entry(async () => {
        const [command, ...args] = process.argv.slice(2);
        const keys = command === "verify" ? ["--plan"] : command === "report" ? ["--plan", "--out"] : [];
        need(keys.length && args.length === keys.length * 2, "use verify --plan PLAN | report --plan PLAN --out NEW");
        const values = {};
        for (let i = 0; i < args.length; i += 2) {
            need(keys.includes(args[i]) && !values[args[i]], "arguments");
            values[args[i]] = args[i + 1];
        }
        need(
            keys.every((k) => values[k]),
            "missing argument",
        );
        const data =
            command === "verify"
                ? {
                      verified: true,
                      executed: false,
                      performanceAccepted: false,
                      plannedUnits: (await verifyGeneratorPlan(values["--plan"])).arms.length * 22,
                  }
                : await reportGenerator(values["--plan"], values["--out"]);
        process.stdout.write(JSON.stringify(data) + "\n");
    }, "Generator reader: verify --plan PLAN | report --plan PLAN --out NEW\nOffline independent evidence verification. No HG capacity or statistical certification.");
