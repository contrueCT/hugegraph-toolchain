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

import { constants } from "node:fs";
import { lstat, open, opendir, realpath } from "node:fs/promises";
import { createHash } from "node:crypto";
import { join, resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { isDeepStrictEqual } from "node:util";
import { valueHash } from "../paired/core.mjs";
import { assessExperiment, assessCohort, assessAcceptance } from "./calibration-evidence.mjs";

// These pins identify one already independently replayed experiment. This is
// intentionally not a general executor or an adapter for arbitrary old reports.
const PINS = Object.freeze({
    source: "2b878425d6b5f9968777a14c3bd1622a6fc3349b7d5f88c139c773556dfde89a",
    manifest: "a558e81bb9f8011f971de232d2afc11d26462d4ac9e842a38ab99d67dad158fe",
    transfer: "7819184488c7f8b388205495ef983e1d2efea122f97253256a3adfd663805914",
    review: "906787ab788dc6dd2ab6ea679ae1e009c1f80c42edfcd3ee341ed2e2ce94bf07",
    plan: "f56e1aba36c173d0d6b747c236227f04143fbe021a7ed5eda60afd8e8aa90300",
    executions: [
        "41b2084da3776375082f07f0d3b740a91ef34fe430bcee2312a075bb8b62223b",
        "58ee197e49c4feb3c53df382b48bc00704eb0b1d27aa947bea59ff9c856be595",
    ],
});
const CALIBRATION = "remote/run/cohort/calibration";
const REVIEW = "local-review-v1";
const IDS = ["http-aa-cohort-005-e1", "http-aa-cohort-005-e2"];
const METHOD = "prior independently replayed immutable evidence chain, no new raw analysis";
const MiB = 1024 * 1024;
const SHA = /^[a-f0-9]{64}$/;

function need(condition, code, detail) {
    if (!condition) {
        const error = new Error(code);
        error.code = code;
        if (detail !== undefined) error.detail = detail;
        throw error;
    }
}

function member(path) {
    need(
        typeof path === "string" &&
            path.length <= 4096 &&
            !path.includes("\\") &&
            !path.includes("\0") &&
            path.split("/").length <= 32 &&
            path.split("/").every((part) => part && part !== "." && part !== ".."),
        "AA005_MEMBER",
    );
    return path;
}

function signature(stat) {
    return [stat.dev, stat.ino, stat.mode, stat.size, stat.mtimeNs, stat.ctimeNs].map(String).join(":");
}

class Reader {
    constructor(root, { signal, deadline, maxBytes }) {
        this.root = root;
        this.signal = signal;
        this.deadline = deadline;
        this.maxBytes = maxBytes;
        this.bytes = 0;
        this.files = new Map();
        this.directories = new Set();
        this.exactTrees = new Map();
    }
    check() {
        need(!this.signal?.aborted, "AA005_CANCELLED");
        need(performance.now() < this.deadline, "AA005_DEADLINE");
    }
    async parents(path) {
        const parts = member(path).split("/");
        parts.pop();
        let current = this.root;
        for (const part of parts) {
            this.check();
            current = join(current, part);
            const stat = await lstat(current);
            need(stat.isDirectory() && !stat.isSymbolicLink(), "AA005_DIRECTORY");
            this.directories.add(current);
        }
    }
    async file(path, expected = null, collect = false) {
        this.check();
        await this.parents(path);
        const full = join(this.root, member(path));
        const before = await lstat(full, { bigint: true });
        need(before.isFile() && !before.isSymbolicLink(), "AA005_REGULAR_FILE", path);
        const size = Number(before.size);
        need(Number.isSafeInteger(size) && size <= this.maxBytes - this.bytes, "AA005_INPUT_BYTES", path);
        need(!collect || size <= 16 * MiB, "AA005_METADATA_BYTES", path);
        if (expected) need(size === expected.bytes, "AA005_FILE_SIZE", path);
        const handle = await open(full, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
        const hash = createHash("sha256");
        const chunks = [];
        let total = 0;
        try {
            need(signature(await handle.stat({ bigint: true })) === signature(before), "AA005_INPUT_CHANGED", path);
            const buffer = Buffer.alloc(Math.min(MiB, Math.max(1, size)));
            while (true) {
                this.check();
                const { bytesRead } = await handle.read(buffer, 0, buffer.length, null);
                if (!bytesRead) break;
                this.bytes += bytesRead;
                total += bytesRead;
                need(this.bytes <= this.maxBytes && total <= size, "AA005_INPUT_BYTES", path);
                hash.update(buffer.subarray(0, bytesRead));
                if (collect) chunks.push(Buffer.from(buffer.subarray(0, bytesRead)));
            }
            this.check();
            need(total === size, "AA005_INPUT_CHANGED", path);
            need(signature(await handle.stat({ bigint: true })) === signature(before), "AA005_INPUT_CHANGED", path);
        } finally {
            await handle.close();
        }
        const result = { bytes: total, sha256: hash.digest("hex"), signature: signature(before) };
        need(signature(await lstat(full, { bigint: true })) === result.signature, "AA005_INPUT_CHANGED", path);
        if (expected) need(result.sha256 === expected.sha256, "AA005_FILE_SHA256", path);
        const prior = this.files.get(path);
        if (prior) need(isDeepStrictEqual(prior, result), "AA005_INPUT_CHANGED", path);
        this.files.set(path, result);
        this.check();
        return collect ? Buffer.concat(chunks, total) : result;
    }
    async json(path, pin) {
        const raw = await this.file(path, this.files.get(path), true);
        if (pin) need(this.files.get(path).sha256 === pin, "AA005_PIN", path);
        let value;
        try {
            value = JSON.parse(raw.toString("utf8"));
        } catch {
            need(false, "AA005_JSON", path);
        }
        this.check();
        return value;
    }
    async tree(path) {
        const files = [];
        const walk = async (directory, prefix, depth) => {
            this.check();
            need(depth <= 32, "AA005_INVENTORY_DEPTH");
            const stat = await lstat(directory);
            need(stat.isDirectory() && !stat.isSymbolicLink(), "AA005_DIRECTORY");
            this.directories.add(directory);
            const dir = await opendir(directory);
            for await (const entry of dir) {
                this.check();
                const relative = prefix ? prefix + "/" + entry.name : entry.name;
                member(relative);
                if (entry.isDirectory()) await walk(join(directory, entry.name), relative, depth + 1);
                else {
                    need(entry.isFile() && !entry.isSymbolicLink(), "AA005_REGULAR_FILE", relative);
                    files.push(relative);
                    need(files.length <= 10000, "AA005_INVENTORY_MEMBERS");
                }
            }
        };
        await walk(join(this.root, member(path)), "", 0);
        return files.sort();
    }
    async exactTree(path, expected) {
        const names = [...expected].sort();
        need(isDeepStrictEqual(await this.tree(path), names), "AA005_EXACT_INVENTORY", path);
        this.exactTrees.set(path, names);
    }
    rows(rows, expectedCount) {
        need(Array.isArray(rows) && rows.length > 0 && rows.length <= 10000, "AA005_LOCK_FILES");
        if (expectedCount !== undefined) need(rows.length === expectedCount, "AA005_LOCK_COUNT");
        const names = new Set();
        for (const row of rows) {
            this.check();
            member(row.path);
            need(
                !names.has(row.path) && Number.isSafeInteger(row.bytes) && row.bytes >= 0 && SHA.test(row.sha256),
                "AA005_LOCK_ENTRY",
            );
            names.add(row.path);
        }
        return names;
    }
    async verifyRows(prefix, rows) {
        this.rows(rows);
        for (const row of rows) {
            const path = prefix + "/" + row.path;
            const checked = this.files.get(path);
            if (checked) need(checked.bytes === row.bytes && checked.sha256 === row.sha256, "AA005_LOCK_CHAIN", path);
            else await this.file(path, row);
        }
    }
    async lock(path, { pin, count, kind, extras = [], exact = true } = {}) {
        const lock = await this.json(path, pin);
        need(lock.schemaVersion === 1 && (!kind || lock.kind === kind), "AA005_LOCK_KIND", path);
        const names = this.rows(lock.files, count);
        if (lock.contentDigest !== undefined)
            need(valueHash(lock.files) === lock.contentDigest, "AA005_CONTENT_DIGEST");
        const prefix = path.slice(0, path.lastIndexOf("/"));
        await this.verifyRows(prefix, lock.files);
        if (exact) await this.exactTree(prefix, [...names, path.slice(path.lastIndexOf("/") + 1), ...extras]);
        return lock;
    }
    ref(path) {
        const file = this.files.get(path);
        need(file, "AA005_UNVERIFIED_REFERENCE", path);
        return { path, bytes: file.bytes, sha256: file.sha256 };
    }
    async finish() {
        const rootStat = await lstat(this.root, { bigint: true });
        need(
            rootStat.isDirectory() &&
                !rootStat.isSymbolicLink() &&
                [rootStat.dev, rootStat.ino].map(String).join(":") === this.rootIdentity,
            "AA005_INPUT_CHANGED",
        );
        for (const [path, file] of this.files) {
            this.check();
            await this.parents(path);
            need(
                signature(await lstat(join(this.root, path), { bigint: true })) === file.signature,
                "AA005_INPUT_CHANGED",
                path,
            );
        }
        for (const [path, names] of this.exactTrees) {
            this.check();
            need(isDeepStrictEqual(await this.tree(path), names), "AA005_INPUT_CHANGED", path);
        }
        this.check();
    }
}

function same(actual, expected, code) {
    need(isDeepStrictEqual(actual, expected), code);
}

async function command(reader, path, name) {
    const record = await reader.json(path);
    need(record.exitCode === 0 && record.failure === null, "AA005_REPLAY_COMMAND", path);
    const reply = JSON.parse(record.stdout);
    need(reply.ok === true && reply.command === name, "AA005_REPLAY_REPLY", path);
    return reply.data;
}

/**
 * Verify the specific immutable A/A005 collection and its earlier independent
 * replay. No selected source, command recorded in evidence, or raw analyzer is
 * executed. Every contribution fails closed if the original chain is missing.
 */
export async function verifyAa005Contribution({ evidenceRoot, signal, deadline, maxBytes } = {}) {
    need(typeof evidenceRoot === "string" && evidenceRoot.length > 0, "AA005_EVIDENCE_ROOT");
    need(Number.isFinite(deadline), "AA005_DEADLINE_ARGUMENT");
    need(Number.isSafeInteger(maxBytes) && maxBytes >= 1 && maxBytes <= 2 * 1024 ** 3, "AA005_BYTES_ARGUMENT");
    need(
        signal === undefined || (typeof signal.aborted === "boolean" && typeof signal.addEventListener === "function"),
        "AA005_SIGNAL_ARGUMENT",
    );
    const reader = new Reader(resolve(evidenceRoot), { signal, deadline, maxBytes });
    reader.check();
    const rootStat = await lstat(reader.root, { bigint: true });
    need(rootStat.isDirectory() && !rootStat.isSymbolicLink(), "AA005_EVIDENCE_ROOT");
    reader.rootIdentity = [rootStat.dev, rootStat.ino].map(String).join(":");
    reader.root = await realpath(reader.root);

    // Pin the small, independently reviewed replay before touching large raw files.
    const reviewLock = await reader.lock(REVIEW + "/review.lock.json", { pin: PINS.review, count: 25 });
    const review = await reader.json(REVIEW + "/review.json");
    need(
        review.kind === "http-aa-original-local-review" &&
            review.identity === "005" &&
            review.rebuildCompleted === true &&
            review.evidenceValid === true &&
            review.frozenReportEvidenceValid === true &&
            review.originalSetUntouched === true &&
            review.originalCohortReportAvailable === false &&
            review.executedMeasurements === false &&
            review.performanceAccepted === false &&
            review.qualificationAccepted === false &&
            Array.isArray(review.unexplainedOutputDifferences) &&
            review.unexplainedOutputDifferences.length === 0,
        "AA005_INDEPENDENT_REPLAY",
    );
    const source = await reader.json("source.lock.json", PINS.source);
    need(
        source.kind === "http-aa-cohort-005-frozen-source" &&
            source.sourceFiles === 529 &&
            source.totalBytes === 5592741,
        "AA005_SOURCE_SELECTION",
    );
    reader.rows(source.files, 529);
    await reader.verifyRows("source", source.files);

    const manifest = await reader.json("remote/transfer-manifest.json", PINS.manifest);
    need(
        manifest.kind === "http-aa005-terminal-transfer" &&
            manifest.schemaVersion === 1 &&
            manifest.sourceLockSha256 === PINS.source,
        "AA005_MANIFEST",
    );
    reader.rows(manifest.files, 3229);
    await reader.verifyRows("remote", manifest.files);
    const transfer = await reader.json("remote/transfer-verification.json", PINS.transfer);
    need(
        transfer.exactInventoryAndHashesVerified === true &&
            transfer.members === 3230 &&
            transfer.payloadBytes === 779315781,
        "AA005_TRANSFER",
    );
    need(
        manifest.files.reduce((sum, row) => sum + row.bytes, 0) === manifest.payloadBytesExcludingManifest,
        "AA005_MANIFEST_BYTES",
    );
    need(
        manifest.payloadBytesExcludingManifest + reader.ref("remote/transfer-manifest.json").bytes ===
            transfer.payloadBytes,
        "AA005_MANIFEST_BYTES",
    );
    await reader.exactTree("remote", [
        ...manifest.files.map((row) => row.path),
        "transfer-manifest.json",
        "transfer-verification.json",
    ]);

    const planLock = await reader.lock(CALIBRATION + "/plan.lock.json", {
        pin: PINS.plan,
        count: 47,
        kind: "http-calibration-plan-lock",
    });
    const plan = await reader.json(CALIBRATION + "/plan.json");
    need(
        plan.id === "http-aa-cohort-005" &&
            plan.schemaVersion === 2 &&
            plan.kind === "http-calibration-plan" &&
            plan.scenario === "repeatability-aa" &&
            plan.mode === "forward",
        "AA005_PLAN",
    );
    need(plan.planDigest === planLock.planDigest && plan.planDigest === review.planDigest, "AA005_PLAN_DIGEST");
    same(
        plan.experiments.map((experiment) => experiment.id),
        IDS,
        "AA005_EXPERIMENTS",
    );
    const verified = await command(reader, REVIEW + "/verify-command.json", "http.calibration.verify");
    need(
        verified.planDigest === plan.planDigest && verified.contentDigest === planLock.contentDigest,
        "AA005_REPLAY_PLAN",
    );
    const replayed = await command(reader, REVIEW + "/report-command.json", "http.calibration.report");
    need(replayed.evidenceValid === true && replayed.experiments === 2, "AA005_REPLAY_REPORT");

    const resultLock = await reader.lock(REVIEW + "/report/evidence.lock.json", {
        kind: "http-calibration-report-lock",
        count: 15,
        extras: ["rebuild.lock.json", ...IDS.map((id) => "rebuild/" + id + "-command.json")],
    });
    const rebuildLock = await reader.json(REVIEW + "/report/rebuild.lock.json");
    need(
        rebuildLock.kind === "http-calibration-rebuild-lock" &&
            rebuildLock.resultContentDigest === resultLock.contentDigest,
        "AA005_REBUILD_LOCK",
    );
    reader.rows(rebuildLock.files, 2);
    await reader.verifyRows(REVIEW + "/report", rebuildLock.files);
    const summary = await reader.json(REVIEW + "/report/summary.json");
    need(
        summary.planDigest === plan.planDigest &&
            summary.planContentDigest === planLock.contentDigest &&
            summary.evidenceValid === true &&
            summary.cancelled === false &&
            summary.performanceAccepted === false &&
            summary.qualificationAccepted === false,
        "AA005_SUMMARY",
    );
    same(
        summary.experiments.map((experiment) => experiment.id),
        IDS,
        "AA005_SUMMARY_EXPERIMENTS",
    );
    const mapping = await reader.json(REVIEW + "/execution-set.local.json");
    const originalMapping = await reader.json("remote/run/cohort/execution-set.json");
    for (const [set, prefix] of [
        [mapping, "../remote/run/cohort/"],
        [originalMapping, ""],
    ]) {
        need(
            set.schemaVersion === 1 &&
                set.kind === "http-calibration-execution-set" &&
                set.planDigest === plan.planDigest,
            "AA005_EXECUTION_SET",
        );
        same(
            set.experiments,
            IDS.map((id) => ({ id, executionDirectory: prefix + "children/" + id + "/execution" })),
            "AA005_EXECUTION_SET",
        );
    }
    need(
        reader.ref(REVIEW + "/execution-set.local.json").sha256 === summary.executionSetSha256 &&
            summary.executionSetSha256 === review.localExecutionSetSha256,
        "AA005_MAPPING_REFERENCE",
    );
    const workflow = await reader.json("remote/run/cohort/workflow.json");
    const workflowLock = await reader.json("remote/run/cohort/workflow.lock.json");
    need(
        workflowLock.planSha256 === reader.ref("remote/run/cohort/workflow.json").sha256 &&
            workflow.calibrationPlanDigest === plan.planDigest &&
            workflow.calibrationContentDigest === planLock.contentDigest,
        "AA005_WORKFLOW",
    );
    same(
        workflow.children.map((child) => child.id),
        IDS,
        "AA005_WORKFLOW_CHILDREN",
    );

    const assessments = [],
        profiles = [],
        instances = [],
        executionIds = new Set(),
        allRunIds = new Set();
    let sharedConditions = null,
        previousWindow = null;
    for (let index = 0; index < IDS.length; index++) {
        reader.check();
        const id = IDS[index],
            frozen = plan.experiments[index],
            record = summary.experiments[index];
        const childPlanRoot = CALIBRATION + "/" + frozen.directory;
        const childPlanLock = await reader.lock(childPlanRoot + "/plan.lock.json", {
            kind: "http-comparison-content-lock",
            count: 20,
        });
        const childPlan = await reader.json(childPlanRoot + "/plan.json");
        need(
            childPlan.planDigest === frozen.planDigest &&
                childPlanLock.planDigest === frozen.planDigest &&
                childPlanLock.contentDigest === frozen.contentDigest,
            "AA005_CHILD_PLAN",
        );
        need(
            childPlan.schemaVersion === 2 && childPlan.comparison === "aa" && childPlan.purpose === "calibration",
            "AA005_CHILD_PLAN",
        );
        same(childPlan.metricRoles, { latency: "inference", goodput: "quality" }, "AA005_METRIC_ROLES");
        need(
            childPlan.metricBindings.length === 22 &&
                childPlan.analysisPolicy.metrics.length === 11 &&
                childPlan.blocks.length === 4,
            "AA005_METRIC_DENOMINATOR",
        );
        const conditions = {};
        for (const key of ["build", "settings", "environment"]) {
            const reference = childPlan.sharedEvidence[key];
            const path = childPlanRoot + "/" + member(reference.path);
            const value = await reader.json(path);
            need(reader.ref(path).sha256 === reference.sha256, "AA005_CONDITION_REFERENCE");
            conditions[key] = { value, sha256: reference.sha256 };
        }
        if (sharedConditions) same(conditions, sharedConditions, "AA005_SHARED_CONDITIONS");
        else sharedConditions = conditions;
        const childRoot = "remote/run/cohort/children/" + id;
        const executionRoot = childRoot + "/execution";
        const executionLock = await reader.lock(executionRoot + "/execution.lock.json", {
            pin: PINS.executions[index],
            kind: "http-comparison-execution-lock",
            count: 1498,
        });
        const execution = await reader.json(executionRoot + "/execution.json");
        const terminal = await reader.json(executionRoot + "/terminal.json");
        need(
            execution.planDigest === frozen.planDigest &&
                execution.planContentDigest === frozen.contentDigest &&
                executionLock.planDigest === frozen.planDigest &&
                record.executionLockSha256 === PINS.executions[index] &&
                record.executionId === execution.executionId &&
                !executionIds.has(execution.executionId),
            "AA005_EXECUTION_IDENTITY",
        );
        executionIds.add(execution.executionId);
        const runIds = childPlan.blocks.flatMap((block) => block.arms.map((arm) => arm.runId));
        need(
            runIds.length === 8 && new Set(runIds).size === 8 && runIds.every((id) => !allRunIds.has(id)),
            "AA005_RUN_DENOMINATOR",
        );
        runIds.forEach((runId) => allRunIds.add(runId));
        same(
            execution.receipts.map((receipt) => receipt.runId),
            runIds,
            "AA005_RECEIPTS",
        );
        same(record.runIds, runIds, "AA005_REPORT_RUN_IDS");
        need(
            terminal.finishedMonotonicNs === record.finishedMonotonicNs &&
                execution.registeredMonotonicNs === record.registeredMonotonicNs &&
                execution.hostBootId === record.hostBootId,
            "AA005_WINDOW_REFERENCE",
        );
        const start = BigInt(execution.registeredMonotonicNs),
            finish = BigInt(terminal.finishedMonotonicNs);
        need(
            start < finish &&
                (!previousWindow || previousWindow.boot !== execution.hostBootId || previousWindow.finish <= start),
            "AA005_EXECUTION_OVERLAP",
        );
        previousWindow = { boot: execution.hostBootId, finish };
        const runtime = await reader.json(childRoot + "/runtime.json");
        const runtimeLock = await reader.json(childRoot + "/runtime.lock.json");
        need(
            execution.runtimeDigest === reader.ref(childRoot + "/runtime.lock.json").sha256 &&
                runtimeLock.runtime.sha256 === reader.ref(childRoot + "/runtime.json").sha256 &&
                runtimeLock.runtime.bytes === reader.ref(childRoot + "/runtime.json").bytes &&
                runtimeLock.runtime.path === "runtime.json" &&
                runtime.planDigest === childPlan.planDigest,
            "AA005_RUNTIME_CHAIN",
        );
        instances.push({
            id,
            executionId: execution.executionId,
            runtime: reader.ref(childRoot + "/runtime.json"),
            runtimeLock: reader.ref(childRoot + "/runtime.lock.json"),
            executionLock: reader.ref(executionRoot + "/execution.lock.json"),
        });
        for (const slot of ["A", "B"]) {
            const variant = childPlan.variants[slot],
                path = childPlanRoot + "/" + member(variant.profileFile);
            const value = await reader.json(path);
            need(
                reader.ref(path).sha256 === variant.profileSha256 && variant.mode === "forward",
                "AA005_PROFILE_REFERENCE",
            );
            profiles.push({ id: id + "/" + slot, value, sha256: variant.profileSha256 });
        }
        const originalRoot = childRoot + "/analysis",
            replayRoot = REVIEW + "/report/experiments/" + id;
        await reader.lock(originalRoot + "/evidence.lock.json", { kind: "http-paired-adapter-output-lock", count: 5 });
        const adapter = await reader.lock(replayRoot + "/evidence.lock.json", {
            kind: "http-paired-adapter-output-lock",
            count: 5,
        });
        for (const name of ["analysis.json", "observations.json", "verification.json", "report.html"])
            same(
                reader.ref(originalRoot + "/" + name).sha256,
                reader.ref(replayRoot + "/" + name).sha256,
                "AA005_CORE_REPLAY_BYTES",
            );
        need(
            record.adapterLockSha256 === reader.ref(replayRoot + "/evidence.lock.json").sha256 &&
                adapter.files.length === 5,
            "AA005_ADAPTER_REFERENCE",
        );
        const verification = await reader.json(replayRoot + "/verification.json");
        const analysis = await reader.json(replayRoot + "/analysis.json");
        need(
            verification.planDigest === childPlan.planDigest &&
                verification.valid === true &&
                verification.statisticsEligible === true &&
                analysis.cells.length === 11,
            "AA005_REPLAY_QUALITY",
        );
        const assessed = assessExperiment(verification, analysis);
        for (const [key, value] of Object.entries(assessed)) same(record[key], value, "AA005_EXPERIMENT_ASSESSMENT");
        assessments.push({ id, ...assessed });
    }
    const cohort = assessCohort(assessments, plan.assessment);
    same(summary.cohort, cohort, "AA005_COHORT_ASSESSMENT");
    same(review.cohort, cohort, "AA005_REVIEW_COHORT");
    const acceptance = assessAcceptance(plan, assessments, cohort);
    for (const [key, value] of Object.entries(acceptance)) same(summary[key], value, "AA005_ACCEPTANCE_ASSESSMENT");
    const gate = acceptance.engineering.gates.find((row) => row.id === "repeatability-aa");
    need(
        gate.status === "unknown" &&
            assessments.every((row) => row.state === "uncertain") &&
            acceptance.empiricalCertification.status === "uncertain",
        "AA005_ORIGINAL_LIMITS",
    );
    const runtimePath = "remote/run/cohort/comparison-runtime.json";
    const runtimeSelection = await reader.json(runtimePath);
    same(runtimeSelection, workflow.runtime.comparisonRuntime, "AA005_RUNTIME_SELECTION");
    for (const profile of profiles) {
        need(
            profile.value.warmupMs === 30000 && profile.value.durationMs === 120000 && profile.value.loads.length === 1,
            "AA005_PROFILE_SCOPE",
        );
        same(
            profile.value.loads[0],
            { id: "open-10", model: "open", ratePerSecond: 10, maxInFlight: 4, maxStartDelayMs: 1000 },
            "AA005_PROFILE_SCOPE",
        );
        same(profile.value.benchmark.caseIds, profiles[0].value.benchmark.caseIds, "AA005_CASE_SCOPE");
    }
    need(profiles[0].value.benchmark.caseIds.length === 11, "AA005_CASE_SCOPE");
    await reader.finish();
    const evidenceDigests = {
        sourceSelection: PINS.source,
        originalManifest: PINS.manifest,
        independentReplay: PINS.review,
        calibrationPlanLock: PINS.plan,
        executionLocks: [...PINS.executions],
        summary: reader.ref(REVIEW + "/report/summary.json").sha256,
    };
    return {
        id: "aa005",
        rawVerified: true,
        status: gate.status,
        verificationMethod: METHOD,
        gate: {
            ...gate,
            reason: "Both complete original experiments remain uncertain within their declared practical budgets",
        },
        gates: {
            "repeatability-aa": {
                ...gate,
                reason: "Both complete original experiments remain uncertain within their declared practical budgets",
            },
        },
        empiricalCertification: acceptance.empiricalCertification,
        evidenceDigests,
        references: {
            sourceSelection: reader.ref("source.lock.json"),
            originalManifest: reader.ref("remote/transfer-manifest.json"),
            independentReplay: reader.ref(REVIEW + "/review.lock.json"),
            calibrationPlanLock: reader.ref(CALIBRATION + "/plan.lock.json"),
            summary: reader.ref(REVIEW + "/report/summary.json"),
            instances,
            originalCoreFilesCompared: 8,
            sourceFilesVerified: 529,
            originalPayloadFilesVerified: manifest.files.length,
            replayPayloadFilesVerified: reviewLock.files.length,
        },
        scope: {
            kind: "same-build-forward-aa-at-declared-fixed-load",
            caseIds: profiles[0].value.benchmark.caseIds,
            modes: ["forward"],
            ratesPerSecond: [10],
            maxInFlight: 4,
            warmupMs: 30000,
            durationMs: 120000,
            experiments: 2,
            physicalArms: 16,
            latencyMetric: "correctEndToEndMs.p99",
            goodputRole: "quality",
        },
        conditions: {
            ...sharedConditions,
            runtime: { value: runtimeSelection, sha256: reader.ref(runtimePath).sha256 },
            sourceSelection: { value: source, sha256: PINS.source },
            profiles,
        },
        limits: {
            method: METHOD,
            newRawAnalysis: false,
            oldCommandsExecuted: false,
            liveAttestation: false,
            originalCohortReportAvailable: false,
            stochasticIndependenceProven: false,
            throughputCapacityAssessed: false,
            wholeHostExclusivityProven: false,
            performanceAccepted: false,
            qualificationAccepted: false,
            inputBytesRead: reader.bytes,
            maxInputBytes: maxBytes,
        },
    };
}
