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
import { open, readdir, lstat, realpath } from "node:fs/promises";
import { resolve, join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { createHash } from "node:crypto";
import { spawn } from "node:child_process";
import { performance } from "node:perf_hooks";
import { isDeepStrictEqual } from "node:util";

const MiB = 1024 ** 2,
    ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "../.."),
    SHA = /^[a-f0-9]{64}$/,
    GATE = "formal-parameter-freeze";
const hash = (v) => createHash("sha256").update(v).digest("hex");
const encode = (v) => Buffer.from(JSON.stringify(v, null, 2) + "\n");
const error = (code) => Object.assign(new Error(code), { code });
const need = (v, code) => {
    if (!v) throw error(code);
};
const json = (raw) => JSON.parse(new TextDecoder("utf8", { fatal: true }).decode(raw));
const pathString = (v) => typeof v === "string" && v.length > 0 && v.length <= 4096 && !/[\r\n\0]/.test(v);
const stable = (v) => {
    if (Array.isArray(v)) return v.map(stable);
    if (v && typeof v === "object")
        return Object.fromEntries(
            Object.keys(v)
                .sort()
                .map((k) => [k, stable(v[k])]),
        );
    return v;
};
const valueHash = (v) => hash(JSON.stringify(stable(v)));
function check(signal, deadline) {
    need(!signal?.aborted, "ENGINEERING_COMPARISON_CANCELLED");
    need(performance.now() < deadline, "ENGINEERING_COMPARISON_DEADLINE");
}
async function bounded(path, cap, signal, deadline) {
    check(signal, deadline);
    const fd = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
    try {
        const a = await fd.stat();
        need(a.isFile() && a.size <= cap, "ENGINEERING_COMPARISON_FILE_BUDGET");
        const raw = Buffer.alloc(a.size + 1);
        let n = 0;
        while (n < raw.length) {
            check(signal, deadline);
            const r = await fd.read(raw, n, Math.min(65536, raw.length - n), n);
            if (!r.bytesRead) break;
            n += r.bytesRead;
        }
        const b = await fd.stat();
        need(
            n === a.size && b.size === a.size && b.mtimeMs === a.mtimeMs && b.ctimeMs === a.ctimeMs,
            "ENGINEERING_COMPARISON_INPUT_CHANGED",
        );
        return raw.subarray(0, n);
    } finally {
        await fd.close();
    }
}
async function snapshot(directory, cap, signal, deadline) {
    const rawLock = await bounded(join(directory, "plan.lock.json"), Math.min(cap, MiB), signal, deadline),
        lock = json(rawLock),
        files = new Map();
    need(Array.isArray(lock.files) && lock.files.length > 0 && lock.files.length <= 128, "ENGINEERING_COMPARISON_LOCK");
    let bytes = rawLock.length;
    for (const f of lock.files) {
        need(
            typeof f.path === "string" &&
                /^[A-Za-z0-9_./-]+$/.test(f.path) &&
                !f.path.startsWith("/") &&
                !f.path.split("/").some((p) => ["", ".", ".."].includes(p)) &&
                !files.has(f.path) &&
                Number.isSafeInteger(f.bytes) &&
                f.bytes >= 0 &&
                SHA.test(f.sha256),
            "ENGINEERING_COMPARISON_LOCK",
        );
        need(bytes + f.bytes <= cap, "ENGINEERING_COMPARISON_INPUT_BUDGET");
        const raw = await bounded(join(directory, f.path), f.bytes, signal, deadline);
        need(raw.length === f.bytes && hash(raw) === f.sha256, "ENGINEERING_COMPARISON_LOCKED_FILE");
        bytes += raw.length;
        files.set(f.path, raw);
    }
    const names = [];
    let entries = 0;
    async function walk(relative, depth) {
        need(depth <= 12, "ENGINEERING_COMPARISON_DEPTH");
        for (const name of (await readdir(join(directory, relative))).sort()) {
            check(signal, deadline);
            need(++entries <= 256, "ENGINEERING_COMPARISON_MEMBERS");
            const p = relative ? relative + "/" + name : name,
                s = await lstat(join(directory, p));
            if (s.isDirectory()) await walk(p, depth + 1);
            else {
                need(s.isFile(), "ENGINEERING_COMPARISON_MEMBER_TYPE");
                names.push(p);
            }
        }
    }
    await walk("", 0);
    need(
        isDeepStrictEqual(names.sort(), [...files.keys(), "plan.lock.json"].sort()),
        "ENGINEERING_COMPARISON_INVENTORY",
    );
    return { rawLock, lock, files, bytes };
}
async function verify(directory, signal, deadline) {
    check(signal, deadline);
    need(deadline - performance.now() > 100, "ENGINEERING_COMPARISON_DEADLINE");
    return new Promise((done, reject) => {
        const child = spawn(
            process.execPath,
            [join(ROOT, "components/http/comparison-plan.mjs"), "--verify", directory],
            {
                detached: true,
                stdio: ["ignore", "pipe", "pipe"],
                env: { PATH: process.env.PATH || "/usr/bin:/bin" },
            },
        );
        let size = 0,
            failure = null,
            killer;
        const chunks = { stdout: [], stderr: [] };
        const group = (s) => {
            if (child.pid)
                try {
                    process.kill(-child.pid, s);
                } catch (e) {
                    if (e.code !== "ESRCH") failure ||= error("ENGINEERING_COMPARISON_SIGNAL");
                }
        };
        const stop = (code) => {
            if (failure) return;
            failure = error(code);
            group("SIGTERM");
            killer = setTimeout(() => group("SIGKILL"), Math.max(1, Math.min(50, deadline - performance.now())));
        };
        const abort = () => stop("ENGINEERING_COMPARISON_CANCELLED"),
            timer = setTimeout(
                () => stop("ENGINEERING_COMPARISON_DEADLINE"),
                Math.max(1, deadline - performance.now() - 75),
            );
        signal?.addEventListener("abort", abort, { once: true });
        for (const key of Object.keys(chunks))
            child[key].on("data", (chunk) => {
                size += chunk.length;
                if (size > 65536) stop("ENGINEERING_COMPARISON_WORKER_OUTPUT");
                else chunks[key].push(chunk);
            });
        child.once("error", () => {
            failure ||= error("ENGINEERING_COMPARISON_WORKER_SPAWN");
        });
        child.once("close", (code, sig) => {
            clearTimeout(timer);
            clearTimeout(killer);
            signal?.removeEventListener("abort", abort);
            group("SIGKILL");
            if (failure) return reject(failure);
            try {
                const result = json(Buffer.concat(code === 0 ? chunks.stdout : chunks.stderr));
                if (code !== 0 || sig || result.ok !== true)
                    throw error(result.error?.code || "ENGINEERING_COMPARISON_VERIFY");
                check(signal, deadline);
                done(result.data);
            } catch (e) {
                reject(e);
            }
        });
        if (signal?.aborted) abort();
    });
}
const select = (v, keys) =>
    v && typeof v === "object"
        ? Object.fromEntries(keys.filter((k) => Object.hasOwn(v, k)).map((k) => [k, v[k]]))
        : null;
const filesIdentity = (v) =>
    Array.isArray(v?.files) && v.files.length > 0
        ? v.files.map((f) => select(f, ["path", "bytes", "sha256"])).sort((a, b) => a.path.localeCompare(b.path))
        : null;
const runtimeIdentity = (v) =>
    v?.kind === "http-comparison-runtime"
        ? {
              // CPU is compared against verified per-arm tuples below: a source
              // runtime template can declare 1 CPU while the grid measures 1/2.
              driver: select(v.driver, ["image", "python", "memoryBytes", "pids", "tmpfsBytes"]),
              collector: v.collector ?? null,
              nodeHome: v.nodeHome ?? null,
          }
        : null;
const settingsIdentity = (v) =>
    v?.kind === "hugegraph-http-environment-settings"
        ? {
              image: v.image,
              javaHome: v.javaHome,
              budget: select(v.budget, ["cpus", "memoryBytes", "pids", "tmpfsBytes"]),
              jvm: v.jvm ?? null,
              server: v.server ?? null,
              jvmObservation: v.jvmObservation ?? null,
          }
        : null;
const environmentIdentity = (v) =>
    v?.kind === "http-comparison-host-environment"
        ? select(v, ["kernel", "cpu", "memoryBytes", "docker", "node"])
        : null;
const measurementPaths = [
    "components/http/runner.mjs",
    "components/http/workflow.mjs",
    "components/http/trace-writer.mjs",
    "components/http/common.mjs",
    "components/http/suite-evidence.mjs",
    "components/http/hugegraph-environment.py",
    "components/http/hugegraph-service.py",
    "components/http/hugegraph-mode-probe.py",
];
function measurementIdentity(v) {
    const files = filesIdentity(v);
    if (!files) return null;
    return files.some((f) => f.path === "components/http/runner.mjs") ? files : null;
}
function declaredObservationTuple(formal) {
    const runtime = formal.runtime,
        settings = formal.settings;
    if (
        runtime?.kind !== "http-comparison-runtime" ||
        settings?.kind !== "hugegraph-http-environment-settings" ||
        !Number.isFinite(runtime.driver?.cpus) ||
        runtime.driver.cpus <= 0 ||
        !Object.hasOwn(runtime, "collector") ||
        (runtime.collector !== null && (typeof runtime.collector !== "object" || Array.isArray(runtime.collector))) ||
        (settings.jvmObservation !== undefined &&
            settings.jvmObservation !== null &&
            (typeof settings.jvmObservation !== "object" || Array.isArray(settings.jvmObservation)))
    )
        return null;
    return {
        driverCpus: runtime.driver.cpus,
        collector: runtime.collector !== null,
        jvmObservation: settings.jvmObservation !== undefined && settings.jvmObservation !== null,
    };
}
function actualObservationTuples(conditions) {
    const arms = conditions?.observationArms,
        profiles = conditions?.profiles;
    if (
        !Array.isArray(arms) ||
        !arms.length ||
        !Array.isArray(profiles) ||
        profiles.length !== arms.length ||
        arms.some(
            (a) =>
                typeof a.id !== "string" ||
                !a.id.length ||
                !Number.isFinite(a.driverCpus) ||
                a.driverCpus <= 0 ||
                typeof a.collector !== "boolean" ||
                typeof a.jvmObservation !== "boolean",
        ) ||
        new Set(arms.map((a) => a.id)).size !== arms.length ||
        new Set(profiles.map((p) => p.id)).size !== profiles.length ||
        !isDeepStrictEqual(arms.map((a) => a.id).sort(), profiles.map((p) => p.id).sort())
    )
        return [];
    const tuples = arms.map((a) => select(a, ["driverCpus", "collector", "jvmObservation"]));
    return [...new Map(tuples.map((v) => [JSON.stringify(stable(v)), v])).values()];
}
// Only the observer reader emits these two locked settings and their complete
// physical-arm associations. A variant value alone does not establish a tuple.
function observerConditionVariants(conditions) {
    const variants = conditions?.settingsVariants,
        arms = conditions?.observationArms,
        tuples = actualObservationTuples(conditions);
    if (
        conditions?.settings !== null ||
        !Array.isArray(variants) ||
        variants.length !== 2 ||
        !Array.isArray(arms) ||
        arms.length !== 16 ||
        !tuples.length ||
        new Set(arms.map((a) => a.cellId)).size !== 16 ||
        variants.some(
            (v, i) =>
                v?.id !== ["jvm-off", "jvm-on"][i] ||
                v.path !== "settings-" + v.id + ".json" ||
                !SHA.test(v.sha256) ||
                !settingsIdentity(v.value),
        )
    )
        return null;
    const [off, on] = variants;
    if (
        Object.hasOwn(off.value, "jvmObservation") ||
        !on.value.jvmObservation ||
        typeof on.value.jvmObservation !== "object" ||
        Array.isArray(on.value.jvmObservation) ||
        !isDeepStrictEqual(
            stable(off.value),
            stable(Object.fromEntries(Object.entries(on.value).filter(([k]) => k !== "jvmObservation"))),
        ) ||
        arms.some((a) => {
            const variant = a.jvmObservation ? on : off;
            return (
                !["J", "C"].includes(a.group) ||
                !["p01", "p02", "p03", "p04"].includes(a.blockId) ||
                !["A", "B"].includes(a.arm) ||
                a.cellId !== a.group + "-" + a.blockId + "-" + a.arm ||
                a.jvmObservation !== (a.group === "C" || a.arm === "B") ||
                a.collector !== (a.group === "J" || a.arm === "B") ||
                a.settingsVariantId !== variant.id ||
                a.settingsSha256 !== variant.sha256 ||
                !/^hoc3-[a-f0-9]{16}-[jc]-p0[1-4]-[ab]$/.test(a.id) ||
                !a.id.endsWith("-" + a.cellId.toLowerCase()) ||
                !SHA.test(a.armSettingsSha256)
            );
        })
    )
        return null;
    if (new Set(arms.map((a) => a.id.slice(5, 21))).size !== 1) return null;
    const runtime = runtimeIdentity(conditions.runtime?.value),
        collector = runtime?.collector;
    return {
        settings: variants.map((v) => settingsIdentity(v.value)),
        runtime:
            runtime && collector && typeof collector === "object" && !Array.isArray(collector)
                ? arms.map((a) => ({ ...runtime, collector: a.collector ? runtime.collector : null }))
                : [null],
        tuples,
    };
}
const summary = (v) => ({ sha256: valueHash(v), value: encode(v).length <= 1024 ? v : undefined });
function coverage(formal, contributions, checkNow) {
    const mismatches = [],
        unknownConditions = [],
        methodControlDisclosures = [];
    const unknown = (id, field, reason, disclosure = null) =>
        (disclosure?.unknownConditions ?? unknownConditions).push({ contributionId: id, field, reason });
    const compare = (id, field, current, measured, disclosure = null) => {
        if (
            current === null ||
            current === undefined ||
            !measured.length ||
            measured.some((v) => v === null || v === undefined)
        )
            return unknown(id, field, "Explicit verified fields are unavailable; no value is inferred", disclosure);
        if (!measured.some((v) => isDeepStrictEqual(stable(v), stable(current))))
            (disclosure?.differences ?? mismatches).push({
                contributionId: id,
                field,
                formal: summary(current),
                calibrated: measured.map(summary),
            });
    };
    for (const c of contributions) {
        checkNow();
        if (
            ["aa005", "repeatabilityAa"].includes(c.id) &&
            c.rawVerified === false &&
            c.conditions === null &&
            Object.keys(c.gates ?? {}).length === 0 &&
            contributions.some(
                (other) =>
                    other.id !== c.id &&
                    ["aa005", "repeatabilityAa"].includes(other.id) &&
                    other.gates?.["repeatability-aa"],
            )
        )
            continue;
        const id = c.id,
            p = c.conditions?.profiles?.map((p) => p.value).filter(Boolean) ?? [];
        if (c.rawVerified !== true) {
            unknown(id, "contribution", "Original raw evidence was not independently verified");
            continue;
        }
        const disclosure =
            id === "knownRegression"
                ? {
                      contributionId: id,
                      kind: "fixed-method-positive-control",
                      blocking: false,
                      gateId: "known-regression-detection",
                      originalGateStatus: c.gates?.["known-regression-detection"]?.status ?? c.status,
                      originalScope: structuredClone(c.scope),
                      differences: [],
                      unknownConditions: [],
                      interpretation:
                          "The fixed arithmetic sleep control checks method sensitivity at its original load. Its workload, modes, windows and observer settings need not equal the formal benchmark. It grants no per-case, embedded, headroom or observer-cost coverage. Common build, host and measurement-code anchors remain blocking comparisons.",
                  }
                : null;
        if (disclosure) methodControlDisclosures.push(disclosure);
        const compareScope = (field, current, measured) => compare(id, field, current, measured, disclosure);
        const modes = c.scope?.modes ?? [],
            cases = c.scope?.caseIds ?? [];
        for (const mode of formal.modes) if (!modes.includes(mode)) compareScope("mode", mode, modes);
        for (const item of formal.profile.cases) {
            if (!cases.includes(item.id)) compareScope("caseIds", item.id, cases);
            const measured = p.flatMap((v) => v.cases ?? []).filter((v) => v.id === item.id);
            compareScope("case." + item.id, item, measured);
        }
        compareScope(
            "benchmark",
            select(formal.profile.benchmark, ["suiteId", "version", "bundleSha256"]),
            p.map((v) => select(v.benchmark, ["suiteId", "version", "bundleSha256"])),
        );
        for (const load of formal.profile.loads)
            compareScope(
                "load." + load.id,
                select(load, ["model", "ratePerSecond", "concurrency", "maxInFlight", "maxStartDelayMs"]),
                p
                    .flatMap((v) => v.loads ?? [])
                    .map((v) => select(v, ["model", "ratePerSecond", "concurrency", "maxInFlight", "maxStartDelayMs"])),
            );
        for (const key of [
            "warmupMs",
            "durationMs",
            "requestTimeoutMs",
            "drainMs",
            "repeats",
            "maxRequestsPerPhase",
            "maxResponseBytes",
            "maxTraceBytes",
            "responseEncoding",
            "serviceChecks",
        ])
            compareScope(
                "profile." + key,
                formal.profile[key],
                p.map((v) => v[key]),
            );
        compareScope(
            "profile.clockReferenceEnabled",
            Boolean(formal.profile.clockReference),
            p.map((v) => Boolean(v.clockReference)),
        );
        if (formal.profile.clockReference)
            compareScope(
                "profile.clockReference",
                formal.profile.clockReference,
                p.map((v) => v.clockReference),
            );
        compare(id, "build.distributionMembers", filesIdentity(formal.build), [
            filesIdentity(c.conditions?.build?.value),
        ]);
        const observer = id === "observerCost" ? observerConditionVariants(c.conditions) : null;
        compareScope(
            "target.resources-and-jvm",
            settingsIdentity(formal.settings),
            id === "observerCost" ? (observer?.settings ?? [null]) : [settingsIdentity(c.conditions?.settings?.value)],
        );
        compare(id, "host.static", environmentIdentity(formal.environment), [
            environmentIdentity(c.conditions?.environment?.value),
        ]);
        compareScope(
            "driver.declared-resources-and-collector",
            runtimeIdentity(formal.runtime),
            id === "observerCost" ? (observer?.runtime ?? [null]) : [runtimeIdentity(c.conditions?.runtime?.value)],
        );
        for (const path of measurementPaths)
            compare(
                id,
                "measurement.componentDigest." + path,
                measurementIdentity(formal.sourceSelection)?.find((f) => f.path === path) ?? null,
                [measurementIdentity(c.conditions?.sourceSelection?.value)?.find((f) => f.path === path) ?? null],
            );
        unknown(
            id,
            "measurement.nodeExecutableSha256",
            "Comparison plans do not independently establish the actual Node executable digest",
            disclosure,
        );
        unknown(
            id,
            "target.javaExecutableSha256",
            "A javaHome path is not an independent Java executable digest",
            disclosure,
        );
        compareScope(
            "observer.actual-collector-switch-and-per-arm-driver-resources",
            declaredObservationTuple(formal),
            id === "observerCost" ? (observer?.tuples ?? []) : actualObservationTuples(c.conditions),
        );
    }
    return { mismatches, unknownConditions, methodControlDisclosures };
}

/** Only the parameter/content-lock gate is assessed here. Coverage never upgrades a calibration or performance gate. */
export async function verifyEngineeringComparison({ comparison, index, contributions, signal, deadline, maxBytes }) {
    need(
        Number.isFinite(deadline) && Number.isSafeInteger(maxBytes) && maxBytes >= 4096,
        "ENGINEERING_COMPARISON_BUDGET",
    );
    const base = {
        gate: { id: GATE, status: "unknown", reason: "No formal comparison plan supplied" },
        status: "unknown",
        mismatches: [],
        unknownConditions: [],
        methodControlDisclosures: [],
        conditions: null,
        evidenceDigests: {},
        limits: [
            "Parameter freezing and calibration coverage are separate; neither grants performance acceptance",
            "No fine raw-to-host/JVM clock association or whole-host exclusivity is asserted",
            "Source-selection digests bind declared frozen measurement components; binaries are not re-read here",
            "Future preparation must bind the actual runtime to these declarations; no formal execution is evaluated here",
            "Validator/trace counterfactual cost remains unavailable and is not introduced as another engineering gate",
        ],
        inferenceAccepted: false,
        performanceAccepted: false,
    };
    check(signal, deadline);
    if (comparison === null || comparison === undefined) return base;
    try {
        need(
            Object.keys(comparison).sort().join() ===
                ["planDirectory", "planLockSha256", "calibrationIndexFile", "calibrationIndexSha256"].sort().join() &&
                pathString(comparison.planDirectory) &&
                pathString(comparison.calibrationIndexFile) &&
                SHA.test(comparison.planLockSha256) &&
                SHA.test(comparison.calibrationIndexSha256),
            "ENGINEERING_COMPARISON_REFERENCE",
        );
        need(
            Array.isArray(contributions) && isDeepStrictEqual(index?.contributions, contributions),
            "ENGINEERING_COMPARISON_INDEX_CONTRIBUTIONS",
        );
        need(
            index?.schemaVersion === 1 && index?.kind === "http-calibration-evidence-index",
            "ENGINEERING_COMPARISON_INDEX",
        );
        const directory = await realpath(comparison.planDirectory),
            cap = Math.min(maxBytes, 64 * MiB),
            indexRaw = await bounded(comparison.calibrationIndexFile, Math.min(cap, 8 * MiB), signal, deadline);
        need(
            hash(indexRaw) === comparison.calibrationIndexSha256 && isDeepStrictEqual(json(indexRaw), index),
            "ENGINEERING_COMPARISON_INDEX_DIGEST",
        );
        const first = await snapshot(directory, cap, signal, deadline);
        need(hash(first.rawLock) === comparison.planLockSha256, "ENGINEERING_COMPARISON_PLAN_DIGEST");
        base.evidenceDigests = { planLockSha256: hash(first.rawLock), calibrationIndexSha256: hash(indexRaw) };
        const verified = await verify(directory, signal, deadline),
            last = await snapshot(directory, cap, signal, deadline);
        need(
            first.rawLock.equals(last.rawLock) && [...first.files].every(([k, v]) => v.equals(last.files.get(k))),
            "ENGINEERING_COMPARISON_INPUT_CHANGED",
        );
        need(
            indexRaw.equals(await bounded(comparison.calibrationIndexFile, indexRaw.length, signal, deadline)),
            "ENGINEERING_COMPARISON_INPUT_CHANGED",
        );
        const read = (name) => json(first.files.get(name)),
            targets = read("target-files.json"),
            copies = targets.filter((f) => /^target\/\d{3}\.json$/.test(f.path));
        need(
            copies.some(
                (f) =>
                    f.bytes === indexRaw.length &&
                    f.sha256 === hash(indexRaw) &&
                    first.files.get(f.path)?.equals(indexRaw),
            ),
            "ENGINEERING_CALIBRATION_INDEX_NOT_FROZEN",
        );
        const assets = copies.map((f) => json(first.files.get(f.path))),
            request = read("request.json"),
            profile = read("inputs/profile.source.json"),
            formal = {
                profile,
                modes: [...new Set(Object.values(request.modes))].sort(),
                build: read("inputs/build.json"),
                settings: read("inputs/settings.json"),
                environment: read("inputs/environment.json"),
                runtime: assets.find((v) => v.kind === "http-comparison-runtime") ?? null,
                sourceSelection: assets.find((v) => measurementIdentity(v)) ?? null,
            };
        Object.assign(
            base,
            coverage(formal, contributions, () => check(signal, deadline)),
        );
        base.conditions = {
            modes: formal.modes,
            caseIds: profile.cases.map((v) => v.id),
            loads: profile.loads,
            warmupMs: profile.warmupMs,
            durationMs: profile.durationMs,
            runtimeAttachmentPresent: formal.runtime !== null,
            measurementSourceAttachmentPresent: formal.sourceSelection !== null,
            declaredObservationTuple: declaredObservationTuple(formal),
        };
        base.verification = verified;
        base.evidenceDigests.planDigest = verified.planDigest;
        base.evidenceDigests.contentDigest = verified.contentDigest;
        base.gate = {
            id: GATE,
            status: "passed",
            reason: "Existing comparison verifier passed and exact calibration-index bytes are copied into its frozen target evidence",
        };
        base.status = "passed";
        check(signal, deadline);
        need(encode(base).length <= maxBytes, "ENGINEERING_COMPARISON_OUTPUT_BUDGET");
        return base;
    } catch (e) {
        check(signal, deadline);
        if (
            [
                "ENGINEERING_COMPARISON_CANCELLED",
                "ENGINEERING_COMPARISON_DEADLINE",
                "ENGINEERING_COMPARISON_OUTPUT_BUDGET",
            ].includes(e.code)
        )
            throw e;
        const version = e.code === "COMPARISON_IMPLEMENTATION";
        base.status =
            version || ["ENOENT", "ENGINEERING_COMPARISON_WORKER_SPAWN"].includes(e.code) ? "unknown" : "failed";
        base.gate = { id: GATE, status: base.status, reason: e.code || "ENGINEERING_COMPARISON_INVALID" };
        if (version)
            base.limits.push(
                "Current comparison implementation cannot verify this older plan; its original raw evidence is not classified as invalid",
            );
        return base;
    }
}
