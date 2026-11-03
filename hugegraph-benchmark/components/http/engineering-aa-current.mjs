/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
import { constants } from "node:fs";
import { open, mkdir, writeFile, realpath, lstat, readdir, rm } from "node:fs/promises";
import { join, resolve, dirname, basename, sep, isAbsolute } from "node:path";
import { fileURLToPath } from "node:url";
import { createHash } from "node:crypto";
import { spawn } from "node:child_process";
import { performance } from "node:perf_hooks";
import { isDeepStrictEqual } from "node:util";
import { valueHash } from "../paired/core.mjs";

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "../.."),
    MiB = 1024 ** 2,
    SHA = /^[a-f0-9]{64}$/,
    METADATA_RESERVE = MiB,
    FINISH_RESERVE = 5000;
const encoded = (v) => Buffer.from(JSON.stringify(v, null, 2) + "\n");
const hash = (v) => createHash("sha256").update(v).digest("hex");
const failure = (code) => Object.assign(new Error(code), { code });
const need = (condition, code) => {
    if (!condition) throw failure(code);
};
const parse = (raw) => JSON.parse(new TextDecoder("utf8", { fatal: true }).decode(raw));
const inside = (a, b) => a === b || a.startsWith(b + sep);
const member = (v) =>
    typeof v === "string" &&
    /^[A-Za-z0-9][A-Za-z0-9_./-]*$/.test(v) &&
    !v.split("/").some((x) => ["", ".", ".."].includes(x));
const pathValue = (v) => typeof v === "string" && isAbsolute(v) && v.length <= 4096 && !/[\r\n\0]/.test(v);
function check(signal, deadline) {
    need(!signal?.aborted, "CURRENT_AA_CANCELLED");
    need(performance.now() < deadline, "CURRENT_AA_DEADLINE");
}
async function file(path, cap, signal, deadline, collect = true) {
    check(signal, deadline);
    const fd = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
    try {
        const before = await fd.stat({ bigint: true });
        need(before.isFile() && before.size <= BigInt(cap), "CURRENT_AA_FILE_BUDGET");
        const chunks = [],
            digest = createHash("sha256"),
            buffer = Buffer.alloc(65536);
        let bytes = 0;
        while (true) {
            check(signal, deadline);
            const r = await fd.read(buffer, 0, buffer.length, null);
            if (!r.bytesRead) break;
            bytes += r.bytesRead;
            need(bytes <= Number(before.size), "CURRENT_AA_INPUT_CHANGED");
            digest.update(buffer.subarray(0, r.bytesRead));
            if (collect) chunks.push(Buffer.from(buffer.subarray(0, r.bytesRead)));
        }
        const after = await fd.stat({ bigint: true });
        need(
            bytes === Number(before.size) &&
                ["dev", "ino", "mode", "size", "mtimeNs", "ctimeNs"].every((k) => before[k] === after[k]),
            "CURRENT_AA_INPUT_CHANGED",
        );
        check(signal, deadline);
        return { bytes, sha256: digest.digest("hex"), ...(collect ? { raw: Buffer.concat(chunks, bytes) } : {}) };
    } finally {
        await fd.close();
    }
}
async function canonical(path) {
    const suffix = [];
    let current = resolve(path);
    while (true) {
        try {
            return join(await realpath(current), ...suffix.reverse());
        } catch (e) {
            if (e.code !== "ENOENT") throw e;
        }
        suffix.push(basename(current));
        const parent = dirname(current);
        need(parent !== current, "CURRENT_AA_OUTPUT_PATH");
        current = parent;
    }
}
async function ownInventory(root, signal, deadline) {
    const names = [];
    let entries = 0;
    async function walk(relative, depth) {
        check(signal, deadline);
        need(depth <= 24, "CURRENT_AA_OUTPUT_DEPTH");
        for (const name of (await readdir(join(root, relative))).sort()) {
            need(++entries <= 32768, "CURRENT_AA_OUTPUT_MEMBERS");
            const path = relative ? relative + "/" + name : name,
                stat = await lstat(join(root, path));
            if (stat.isDirectory()) await walk(path, depth + 1);
            else {
                need(stat.isFile(), "CURRENT_AA_OUTPUT_TYPE");
                names.push(path);
            }
        }
    }
    await walk("", 0);
    return names.sort();
}
async function command(planDirectory, inputFile, outputDirectory, maximum, signal, deadline) {
    const argv = [
        join(ROOT, "components/http/calibration-evidence.mjs"),
        "report",
        "--plan",
        planDirectory,
        "--input",
        inputFile,
        "--out",
        outputDirectory,
    ];
    return await new Promise((done) => {
        const child = spawn(process.execPath, argv, {
            detached: true,
            stdio: ["ignore", "pipe", "pipe"],
            env: {
                PATH: process.env.PATH || "/usr/bin:/bin",
                ...(process.env.HGBENCH_PYTHON ? { HGBENCH_PYTHON: process.env.HGBENCH_PYTHON } : {}),
                LANG: "C",
                LC_ALL: "C",
            },
        });
        const output = { stdout: [], stderr: [] };
        let bytes = 0,
            issue = null,
            killer;
        const group = (name) => {
            if (child.pid)
                try {
                    process.kill(-child.pid, name);
                } catch (e) {
                    if (e.code !== "ESRCH") issue ||= "CURRENT_AA_GROUP_SIGNAL";
                }
        };
        const stop = (code) => {
            if (issue) return;
            issue = code;
            group("SIGTERM");
            killer = setTimeout(
                () => group("SIGKILL"),
                Math.max(1, Math.min(4000, deadline - performance.now() - 500)),
            );
        };
        const abort = () => stop("CURRENT_AA_CANCELLED"),
            timer = setTimeout(() => stop("CURRENT_AA_REPORT_DEADLINE"), maximum);
        signal?.addEventListener("abort", abort, { once: true });
        for (const key of ["stdout", "stderr"])
            child[key].on("data", (chunk) => {
                bytes += chunk.length;
                if (bytes > 65536) stop("CURRENT_AA_COMMAND_OUTPUT");
                else output[key].push(chunk);
            });
        child.once("error", () => {
            issue ||= "CURRENT_AA_REPORT_SPAWN";
        });
        child.once("close", (exitCode, terminationSignal) => {
            clearTimeout(timer);
            clearTimeout(killer);
            signal?.removeEventListener("abort", abort);
            group("SIGKILL");
            done({
                argv: [process.execPath, ...argv],
                pid: child.pid ?? null,
                exitCode,
                terminationSignal,
                failure: issue,
                stdout: Buffer.concat(output.stdout).toString("utf8"),
                stderr: Buffer.concat(output.stderr).toString("utf8"),
            });
        });
        if (signal?.aborted) abort();
    });
}
function unknown(plan, digests, reason) {
    const gate = {
        id: "repeatability-aa",
        status: "unknown",
        reason,
        plannedExperiments: plan.experiments.length,
        experimentIds: plan.experiments.map((e) => e.id),
    };
    return {
        id: "repeatabilityAa",
        rawVerified: false,
        status: "unknown",
        verificationMethod: "current fixed calibration report independently re-verifies original inputs",
        gate,
        gates: { "repeatability-aa": gate },
        empiricalCertification: null,
        evidenceDigests: digests,
        references: {},
        scope: {
            kind: "current-version-aa-at-declared-loads",
            caseIds: [],
            modes: [plan.mode],
            ratesPerSecond: [],
            maxInFlight: null,
            warmupMs: null,
            durationMs: null,
            experiments: plan.experiments.length,
            physicalArms: null,
        },
        conditions: {
            build: null,
            settings: null,
            environment: null,
            runtime: null,
            sourceSelection: null,
            profiles: [],
        },
        limits: [
            "Missing, uncertain, degenerate or invalid experiments remain in the original denominator",
            "Current reporter only; no arbitrary old source/script execution or old-version compatibility inference",
            "No live attestation, stochastic independence, throughput capacity or whole-host exclusivity is proven",
        ],
        inferenceAccepted: false,
        performanceAccepted: false,
    };
}

/** Sequential current-version report adapter; all work and group cleanup share the caller's absolute deadline. */
export async function reportCurrentAaContribution({ reference, outputDirectory, signal, deadline, maxBytes }) {
    need(Number.isFinite(deadline) && Number.isSafeInteger(maxBytes) && maxBytes >= MiB, "CURRENT_AA_BUDGET");
    check(signal, deadline);
    need(
        reference &&
            Object.keys(reference).sort().join() ===
                ["planDirectory", "planLockSha256", "executionSetFile", "executionSetSha256"].sort().join() &&
            pathValue(reference.planDirectory) &&
            pathValue(reference.executionSetFile) &&
            SHA.test(reference.planLockSha256) &&
            SHA.test(reference.executionSetSha256) &&
            pathValue(outputDirectory),
        "CURRENT_AA_REFERENCE",
    );
    const planDirectory = await realpath(reference.planDirectory),
        executionSetFile = await canonical(reference.executionSetFile),
        out = await canonical(outputDirectory),
        lockFile = await file(join(planDirectory, "plan.lock.json"), 32 * MiB, signal, deadline),
        setFile = await file(executionSetFile, 8 * MiB, signal, deadline),
        lock = parse(lockFile.raw),
        input = parse(setFile.raw);
    need(
        lockFile.sha256 === reference.planLockSha256 && setFile.sha256 === reference.executionSetSha256,
        "CURRENT_AA_REFERENCE_DIGEST",
    );
    need(
        lock.kind === "http-calibration-plan-lock" &&
            Array.isArray(lock.files) &&
            lock.files.length <= 32768 &&
            lock.contentDigest === valueHash(lock.files),
        "CURRENT_AA_PLAN_LOCK",
    );
    const locked = new Map();
    for (const f of lock.files) {
        need(
            member(f.path) &&
                !locked.has(f.path) &&
                SHA.test(f.sha256) &&
                Number.isSafeInteger(f.bytes) &&
                f.bytes >= 0,
            "CURRENT_AA_PLAN_LOCK",
        );
        locked.set(f.path, f);
    }
    async function lockedJson(path) {
        const expected = locked.get(path);
        need(expected && expected.bytes <= 8 * MiB, "CURRENT_AA_PLAN_MEMBER");
        const checked = await file(join(planDirectory, path), expected.bytes, signal, deadline);
        need(checked.bytes === expected.bytes && checked.sha256 === expected.sha256, "CURRENT_AA_PLAN_MEMBER_DIGEST");
        return { value: parse(checked.raw), sha256: checked.sha256 };
    }
    const plan = (await lockedJson("plan.json")).value;
    need(
        plan.schemaVersion === 2 &&
            plan.kind === "http-calibration-plan" &&
            plan.scenario === "repeatability-aa" &&
            plan.planDigest === lock.planDigest &&
            Array.isArray(plan.experiments) &&
            plan.experiments.length >= 1 &&
            plan.experiments.length <= 10000 &&
            plan.experiments.every((e) => member(e.directory)),
        "CURRENT_AA_V2_PLAN",
    );
    const budget = plan.budgets;
    need(
        Number.isSafeInteger(budget?.maxReportMilliseconds) &&
            budget.maxReportMilliseconds >= 1000 &&
            budget.maxReportMilliseconds <= 86400000 &&
            Number.isSafeInteger(budget.maxReportBytes) &&
            budget.maxReportBytes >= 66 * MiB &&
            budget.maxReportBytes <= 512 * MiB,
        "CURRENT_AA_PLAN_BUDGET",
    );
    need(
        deadline - performance.now() >= budget.maxReportMilliseconds + FINISH_RESERVE,
        "CURRENT_AA_REPORT_TIME_RESERVE",
    );
    need(maxBytes >= budget.maxReportBytes + METADATA_RESERVE, "CURRENT_AA_REPORT_BYTE_RESERVE");
    need(
        input.kind === "http-calibration-execution-set" &&
            input.schemaVersion === 1 &&
            input.planDigest === plan.planDigest &&
            Array.isArray(input.experiments) &&
            input.experiments.length <= plan.experiments.length,
        "CURRENT_AA_EXECUTION_SET",
    );
    const protectedPaths = [planDirectory, executionSetFile];
    for (const e of input.experiments) {
        need(
            typeof e.executionDirectory === "string" &&
                e.executionDirectory.length > 0 &&
                e.executionDirectory.length <= 2048 &&
                !/[\r\n\0]/.test(e.executionDirectory),
            "CURRENT_AA_EXECUTION_PATH",
        );
        protectedPaths.push(await canonical(resolve(dirname(executionSetFile), e.executionDirectory)));
    }
    for (const path of protectedPaths) need(!inside(out, path) && !inside(path, out), "CURRENT_AA_OUTPUT_OVERLAP");
    for (const name of [
        "components",
        "src",
        "dist",
        "release",
        "benchmarks",
        "node_modules",
        "tests-normative",
        "tools",
        "adapters",
        "presets",
        "examples",
        "docs",
    ]) {
        const path = await canonical(join(ROOT, name));
        need(!inside(out, path) && !inside(path, out), "CURRENT_AA_OUTPUT_OVERLAP");
    }
    const digests = { calibrationPlanLock: lockFile.sha256, executionSet: setFile.sha256 },
        result = unknown(plan, digests, "Current report unavailable");
    await mkdir(out, { mode: 0o700 });
    let commandReceipt;
    try {
        check(signal, deadline);
        const maximum = Math.min(budget.maxReportMilliseconds, deadline - performance.now() - FINISH_RESERVE);
        need(maximum >= budget.maxReportMilliseconds, "CURRENT_AA_REPORT_TIME_RESERVE");
        commandReceipt = await command(planDirectory, executionSetFile, join(out, "report"), maximum, signal, deadline);
        await writeFile(join(out, "command.json"), encoded(commandReceipt), { flag: "wx", mode: 0o600 });
        check(signal, deadline);
        if (commandReceipt.failure) throw failure(commandReceipt.failure);
        if (commandReceipt.exitCode !== 0 || commandReceipt.terminationSignal) {
            let code = "CURRENT_AA_REPORT_UNAVAILABLE";
            try {
                code = JSON.parse(commandReceipt.stderr).error?.code || code;
            } catch {
                /* bounded audit retained */
            }
            result.gate.reason = code;
        } else {
            const report = join(out, "report"),
                summaryFile = await file(join(report, "summary.json"), 8 * MiB, signal, deadline),
                summary = parse(summaryFile.raw),
                reportLockFile = await file(join(report, "evidence.lock.json"), 32 * MiB, signal, deadline),
                reportLock = parse(reportLockFile.raw),
                rebuildFile = await file(join(report, "rebuild.lock.json"), 32 * MiB, signal, deadline),
                rebuild = parse(rebuildFile.raw);
            need(
                summary.schemaVersion === 2 &&
                    summary.kind === "http-calibration-report" &&
                    summary.planDigest === plan.planDigest &&
                    summary.executionSetSha256 === setFile.sha256 &&
                    summary.cancelled === false &&
                    reportLock.kind === "http-calibration-report-lock" &&
                    reportLock.planDigest === plan.planDigest &&
                    Array.isArray(reportLock.files) &&
                    reportLock.contentDigest === valueHash(reportLock.files) &&
                    rebuild.kind === "http-calibration-rebuild-lock" &&
                    rebuild.resultContentDigest === reportLock.contentDigest &&
                    Array.isArray(rebuild.files),
                "CURRENT_AA_REPORT_BINDING",
            );
            const all = [...reportLock.files, ...rebuild.files];
            need(
                all.length <= 32768 && new Set(all.map((f) => f.path)).size === all.length,
                "CURRENT_AA_REPORT_INVENTORY",
            );
            let outputBytes = reportLockFile.bytes + rebuildFile.bytes;
            for (const f of all) {
                need(
                    member(f.path) &&
                        SHA.test(f.sha256) &&
                        Number.isSafeInteger(f.bytes) &&
                        f.bytes >= 0 &&
                        outputBytes + f.bytes <= budget.maxReportBytes,
                    "CURRENT_AA_REPORT_INVENTORY",
                );
                const checked = await file(join(report, f.path), f.bytes, signal, deadline, false);
                need(checked.bytes === f.bytes && checked.sha256 === f.sha256, "CURRENT_AA_REPORT_LOCK_DIGEST");
                outputBytes += checked.bytes;
            }
            need(
                isDeepStrictEqual(
                    await ownInventory(report, signal, deadline),
                    [...all.map((f) => f.path), "evidence.lock.json", "rebuild.lock.json"].sort(),
                ),
                "CURRENT_AA_REPORT_INVENTORY",
            );
            need(
                isDeepStrictEqual(
                    summary.experiments?.map((e) => e.id),
                    plan.experiments.map((e) => e.id),
                ),
                "CURRENT_AA_EXPERIMENT_DENOMINATOR",
            );
            const gate = summary.engineering?.gates?.find((g) => g.id === "repeatability-aa");
            need(
                gate && ["passed", "failed", "unknown"].includes(gate.status) && summary.empiricalCertification,
                "CURRENT_AA_ASSESSMENT",
            );
            result.rawVerified =
                summary.evidenceValid === true &&
                summary.experiments.every((e) => e.status === "reverified" && e.evidenceValid === true);
            result.status = gate.status;
            result.gate = gate;
            result.gates = { "repeatability-aa": gate };
            result.empiricalCertification = summary.empiricalCertification;
            result.evidenceDigests.summary = summaryFile.sha256;
            result.evidenceDigests.reportLock = reportLockFile.sha256;
            result.evidenceDigests.rebuildLock = rebuildFile.sha256;
            result.references = {
                summary: { path: "report/summary.json", bytes: summaryFile.bytes, sha256: summaryFile.sha256 },
                reportLock: {
                    path: "report/evidence.lock.json",
                    bytes: reportLockFile.bytes,
                    sha256: reportLockFile.sha256,
                },
                originalExperimentIds: summary.experiments.map((e) => e.id),
                originalExperimentStates: summary.experiments.map((e) => ({
                    id: e.id,
                    state: e.state,
                    issues: e.issues,
                })),
            };
            let shared = null,
                physicalArms = 0;
            for (const e of plan.experiments) {
                const child = (await lockedJson(e.directory + "/plan.json")).value;
                physicalArms += child.budgets.armCount;
                const conditions = {};
                for (const key of ["build", "settings", "environment"])
                    conditions[key] = await lockedJson(e.directory + "/" + child.sharedEvidence[key].path);
                if (shared) need(isDeepStrictEqual(shared, conditions), "CURRENT_AA_SHARED_CONDITIONS");
                else shared = conditions;
                for (const [slot, variant] of Object.entries(child.variants)) {
                    const profile = await lockedJson(e.directory + "/" + variant.profileFile);
                    need(profile.sha256 === variant.profileSha256, "CURRENT_AA_PROFILE_REFERENCE");
                    result.conditions.profiles.push({ id: e.id + "/" + slot, ...profile });
                }
            }
            Object.assign(result.conditions, shared);
            const profiles = result.conditions.profiles.map((p) => p.value),
                only = (values) => {
                    const v = [...new Set(values)];
                    return v.length === 1 ? v[0] : null;
                };
            result.scope = {
                kind: "current-version-aa-at-declared-loads",
                caseIds: [...new Set(profiles.flatMap((p) => p.cases.map((c) => c.id)))].sort(),
                modes: [plan.mode],
                ratesPerSecond: [
                    ...new Set(
                        profiles.flatMap((p) => p.loads.filter((l) => l.model === "open").map((l) => l.ratePerSecond)),
                    ),
                ].sort((a, b) => a - b),
                maxInFlight: only(profiles.flatMap((p) => p.loads.map((l) => l.maxInFlight ?? null))),
                warmupMs: only(profiles.map((p) => p.warmupMs)),
                durationMs: only(profiles.map((p) => p.durationMs)),
                experiments: plan.experiments.length,
                physicalArms,
            };
            result.limits.push(
                "Runtime selection and full measurement source are not inferred from the planner implementation; absent explicit assets remain unknown",
            );
        }
        const lockAfter = await file(join(planDirectory, "plan.lock.json"), 32 * MiB, signal, deadline),
            setAfter = await file(executionSetFile, 8 * MiB, signal, deadline);
        need(lockAfter.sha256 === lockFile.sha256 && setAfter.sha256 === setFile.sha256, "CURRENT_AA_INPUT_CHANGED");
        const raw = encoded(result),
            ownLock = encoded({
                schemaVersion: 1,
                kind: "http-current-aa-contribution-lock",
                files: [{ path: "contribution.json", bytes: raw.length, sha256: hash(raw) }],
            });
        need(
            raw.length + encoded(commandReceipt).length + ownLock.length <= METADATA_RESERVE,
            "CURRENT_AA_METADATA_BYTES",
        );
        check(signal, deadline);
        await writeFile(join(out, "contribution.json"), raw, { flag: "wx", mode: 0o600 });
        check(signal, deadline);
        await writeFile(join(out, "contribution.lock.json"), ownLock, { flag: "wx", mode: 0o600 });
        check(signal, deadline);
        return result;
    } catch (e) {
        await rm(join(out, "contribution.lock.json"), { force: true });
        throw e;
    }
}
