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

import { spawn } from "node:child_process";
import { mkdir, realpath, writeFile } from "node:fs/promises";
import { basename, dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { performance } from "node:perf_hooks";
import { isDeepStrictEqual } from "node:util";
import { valueHash, budgetRelation } from "../paired/core.mjs";
import { bytesDigest } from "./common.mjs";
import {
    CalibrationError,
    need,
    exact,
    boundedFile,
    parse,
    encoded,
    member,
    save,
    inventory,
    verifyCalibration,
    validateAcceptancePolicy,
    ENGINEERING_AA_MINIMUM_EXPERIMENTS,
    componentMain,
} from "./calibration-plan.mjs";

const MiB = 1024 * 1024;
const HERE = dirname(fileURLToPath(import.meta.url));
const scalar = (value) => Number.isFinite(value);
function uint(value) {
    need(typeof value === "string" && /^(0|[1-9][0-9]{0,18})$/.test(value), "CALIBRATION_MONOTONIC_TIME");
    return BigInt(value);
}

function binomialCdf(k, n, p) {
    if (k < 0) return 0;
    if (k >= n || p === 0) return 1;
    if (p === 1) return 0;
    let logTerm = n * Math.log1p(-p),
        maximum = -Infinity,
        sum = 0;
    for (let j = 0; j <= k; j++) {
        if (logTerm > maximum) {
            sum = sum * Math.exp(maximum - logTerm) + 1;
            maximum = logTerm;
        } else sum += Math.exp(logTerm - maximum);
        logTerm += Math.log(n - j) - Math.log(j + 1) + Math.log(p) - Math.log1p(-p);
    }
    return Math.min(1, Math.exp(maximum) * sum);
}
// Exact binomial-tail inversion model, evaluated in binary64. Bounds retain the
// outer bracket instead of reporting the midpoint as numerical certainty.
export function binomialInterval(successes, trials, confidence) {
    need(
        Number.isSafeInteger(trials) &&
            trials >= 1 &&
            trials <= 10000 &&
            Number.isSafeInteger(successes) &&
            successes >= 0 &&
            successes <= trials &&
            scalar(confidence) &&
            confidence >= 0.8 &&
            confidence <= 0.999999,
        "CALIBRATION_BINOMIAL_INPUT",
    );
    const tail = (1 - confidence) / 2;
    let lower = 0,
        upper = 1;
    if (successes > 0) {
        let lo = 0,
            hi = 1;
        for (let iteration = 0; iteration < 80; iteration++) {
            const p = (lo + hi) / 2;
            if (1 - binomialCdf(successes - 1, trials, p) < tail) lo = p;
            else hi = p;
        }
        lower = lo;
    }
    if (successes < trials) {
        let lo = 0,
            hi = 1;
        for (let iteration = 0; iteration < 80; iteration++) {
            const p = (lo + hi) / 2;
            if (binomialCdf(successes, trials, p) > tail) lo = p;
            else hi = p;
        }
        upper = hi;
    }
    return {
        low: lower,
        high: upper,
        confidence,
        method: "clopper-pearson-binomial-tail-inversion-binary64",
        assumption: "independent-identically-distributed-experiment-family-decisions; not proven by this interval",
    };
}

export function assessExperiment(verification, analysis, externalIssues = []) {
    const issues = [...externalIssues];
    if (verification?.valid !== true) issues.push("EVIDENCE_INVALID");
    if (verification?.statisticsEligible !== true) issues.push("STATISTICS_INELIGIBLE");
    if (!Array.isArray(analysis?.cells) || !analysis.cells.length) issues.push("METRIC_FAMILY_MISSING");
    const cells = (analysis?.cells || []).map((cell) => {
        const intervals = cell.bootstrapIntervals;
        const finite = (interval) =>
            interval && scalar(interval.low) && scalar(interval.high) && interval.low <= interval.high;
        const supported =
            Array.isArray(cell.issues) &&
            cell.issues.length === 0 &&
            Array.isArray(cell.warnings) &&
            cell.warnings.length === 0 &&
            finite(intervals?.relative) &&
            finite(intervals?.absolute) &&
            scalar(cell.metric?.relativeBudget) &&
            cell.metric.relativeBudget >= 0 &&
            scalar(cell.metric?.absoluteBudget) &&
            cell.metric.absoluteBudget >= 0 &&
            ["above-both-budgets", "below-both-budgets", "within-at-least-one-budget", "crosses-budget"].includes(
                cell.budgetRelation,
            );
        const qualified = supported && budgetRelation(intervals, cell.metric) === cell.budgetRelation;
        if (!qualified) issues.push("METRIC_UNQUALIFIED:" + (cell.metric?.id ?? "missing"));
        return {
            metricId: cell.metric?.id ?? null,
            qualified,
            point: cell.point ?? null,
            bootstrapIntervals: intervals ?? null,
            budgetRelation: cell.budgetRelation ?? "not-assessed",
            issues: cell.issues ?? [],
            warnings: cell.warnings ?? [],
            excludesZero: qualified && [intervals.relative, intervals.absolute].some((x) => x.low > 0 || x.high < 0),
        };
    });
    const qualified = issues.length === 0;
    const falsePositive = qualified
        ? cells.some((c) => ["above-both-budgets", "below-both-budgets"].includes(c.budgetRelation))
        : null;
    return {
        evidenceValid: verification?.valid === true && externalIssues.length === 0,
        statisticsEligible: qualified,
        state: !qualified
            ? "unknown"
            : falsePositive
              ? "false-positive-meaningful-change"
              : cells.every((c) => c.budgetRelation === "within-at-least-one-budget")
                ? "equivalent-within-declared-budgets"
                : "uncertain",
        familyFalsePositive: falsePositive,
        familyExcludesZero: qualified ? cells.some((c) => c.excludesZero) : null,
        cells,
        issues: [...new Set(issues)],
    };
}

export function assessCohort(experiments, assessment) {
    const planned = experiments.length,
        classifiable = experiments.filter((e) => typeof e.familyFalsePositive === "boolean");
    const falsePositives = classifiable.filter((e) => e.familyFalsePositive).length,
        unknown = planned - classifiable.length;
    const observed = classifiable.length
        ? binomialInterval(falsePositives, classifiable.length, assessment.confidence)
        : null;
    const allLow = planned ? binomialInterval(falsePositives, planned, assessment.confidence).low : null;
    const allHigh = planned ? binomialInterval(falsePositives + unknown, planned, assessment.confidence).high : null;
    return {
        plannedExperiments: planned,
        classifiableExperiments: classifiable.length,
        unknownExperiments: unknown,
        observedFamilyFalsePositives: falsePositives,
        plannedFalsePositiveCountRange: [falsePositives, falsePositives + unknown],
        classifiableCohortInterval: observed,
        plannedCohortIntervalEnvelope: planned ? { low: allLow, high: allHigh } : null,
        completeCohort: unknown === 0,
        minimumExperiments: assessment.minimumExperiments,
        maximumFamilyFalsePositiveRate: assessment.maximumFamilyFalsePositiveRate,
        rateAssessment:
            unknown || classifiable.length < assessment.minimumExperiments
                ? "insufficient-evidence"
                : observed.high <= assessment.maximumFamilyFalsePositiveRate
                  ? "within-predeclared-rate-bound-under-binomial-assumption"
                  : observed.low > assessment.maximumFamilyFalsePositiveRate
                    ? "above-predeclared-rate-bound-under-binomial-assumption"
                    : "uncertain",
        interpretation:
            "Practical-change family decisions, not all zero-excluding confidence intervals; unknown experiments are never negative trials",
    };
}

// This component can verify the A/A contribution only. Other engineering gates
// deliberately have no operator-supplied passed flag or unsupported evidence adapter.
const ENGINEERING_PENDING_GATES = [
    "independent-clean-measurement-windows",
    "driver-headroom",
    "observer-and-validator-cost",
    "known-regression-detection",
    "warmup-stability",
    "formal-parameter-freeze",
];

export function assessAcceptance(plan, experiments, cohort) {
    if (plan.schemaVersion === 1) return {};
    need(plan.schemaVersion === 2, "CALIBRATION_ACCEPTANCE_VERSION");
    const policy = validateAcceptancePolicy(plan.acceptancePolicy);
    need(
        isDeepStrictEqual(plan.engineeringRequirements, {
            repeatabilityAaMinimumExperiments: ENGINEERING_AA_MINIMUM_EXPERIMENTS,
        }),
        "CALIBRATION_ENGINEERING_REQUIREMENTS",
    );
    const expected = plan.experiments.map((e) => e.id),
        actual = experiments.map((e) => e.id),
        complete =
            expected.length >= ENGINEERING_AA_MINIMUM_EXPERIMENTS &&
            new Set(actual).size === actual.length &&
            isDeepStrictEqual([...expected].sort(), [...actual].sort());
    const valid = (e) =>
        e.evidenceValid === true && e.statisticsEligible === true && Array.isArray(e.issues) && e.issues.length === 0;
    const identitiesBound = new Set(actual).size === actual.length && actual.every((id) => expected.includes(id));
    const meaningfulChange =
        identitiesBound &&
        experiments.some(
            (e) => valid(e) && e.state === "false-positive-meaningful-change" && e.familyFalsePositive === true,
        );
    const repeatability = meaningfulChange
        ? "failed"
        : complete &&
            experiments.every(
                (e) => valid(e) && e.state === "equivalent-within-declared-budgets" && e.familyFalsePositive === false,
            )
          ? "passed"
          : "unknown";
    const gates = [
        {
            id: "repeatability-aa",
            status: repeatability,
            minimumExperiments: ENGINEERING_AA_MINIMUM_EXPERIMENTS,
            plannedExperiments: expected.length,
            evidenceSource: "independently-recomputed-predeclared-experiments",
            experimentIds: expected,
            missingExperimentIds: expected.filter((id) => !actual.includes(id)),
            interpretation:
                "At least two complete equivalent experiments may pass this contribution; meaningful change fails; fewer repeats, uncertain, degenerate, invalid and missing evidence remain unknown",
        },
        ...ENGINEERING_PENDING_GATES.map((id) => ({
            id,
            status: "unknown",
            evidenceSource: null,
            reason: "verified-evidence-adapter-not-connected",
        })),
    ];
    const engineering = {
        policyId: policy.id,
        scope: "A/A contribution plus explicitly unconnected required engineering gates",
        status: gates.some((g) => g.status === "failed")
            ? "failed"
            : gates.every((g) => g.status === "passed")
              ? "passed"
              : "unknown",
        accepted: gates.every((g) => g.status === "passed"),
        gates,
    };
    const certified = cohort.rateAssessment === "within-predeclared-rate-bound-under-binomial-assumption";
    const empiricalCertification = {
        required: policy.empiricalFalsePositiveCertification === "required",
        status: cohort.rateAssessment,
        accepted: certified,
        definition: plan.familyFalsePositiveDefinition,
        plannedExperiments: cohort.plannedExperiments,
        classifiableExperiments: cohort.classifiableExperiments,
        unknownExperiments: cohort.unknownExperiments,
        observedFamilyFalsePositives: cohort.observedFamilyFalsePositives,
        classifiableCohortInterval: cohort.classifiableCohortInterval,
        plannedCohortIntervalEnvelope: cohort.plannedCohortIntervalEnvelope,
        assumption:
            "Independent identically distributed experiment-family decisions; stochastic independence is not proven by the tool",
        interpretation:
            "Optional does not mean certified; the complete original cohort and its unknown denominator are retained",
    };
    return {
        acceptancePolicy: structuredClone(policy),
        engineering,
        empiricalCertification,
        policyAccepted: engineering.accepted && (!empiricalCertification.required || certified),
        policyBlockingReasons: [
            ...gates.filter((g) => g.status !== "passed").map((g) => "engineering:" + g.id),
            ...(empiricalCertification.required && !certified ? ["empirical-false-positive-certification"] : []),
        ],
    };
}

async function bridge({ plan, execution, out, milliseconds, cancelled }) {
    return await new Promise((resolveResult) => {
        const output = { stdout: [], stderr: [] };
        let bytes = 0,
            failure = null,
            timer,
            killTimer;
        const started = process.hrtime.bigint().toString();
        const argv = [join(HERE, "paired-evidence.mjs"), "--plan", plan, "--execution", execution, "--out", out];
        const child = spawn(process.execPath, argv, {
            detached: true,
            stdio: ["ignore", "pipe", "pipe"],
            env: {
                PATH: process.env.PATH || "/usr/local/bin:/usr/bin:/bin",
                ...(process.env.HGBENCH_PYTHON ? { HGBENCH_PYTHON: process.env.HGBENCH_PYTHON } : {}),
                LANG: "C",
                LC_ALL: "C",
            },
        });
        const signal = (name) => {
            if (child.pid)
                try {
                    process.kill(-child.pid, name);
                } catch {}
        };
        const stop = (code) => {
            failure ||= code;
            signal("SIGTERM");
            killTimer ||= setTimeout(() => signal("SIGKILL"), 3000);
        };
        const abort = () => stop("CALIBRATION_CANCELLED");
        cancelled.addEventListener("abort", abort, { once: true });
        for (const key of ["stdout", "stderr"])
            child[key].on("data", (raw) => {
                bytes += raw.length;
                if (bytes <= MiB) output[key].push(raw);
                else stop("CALIBRATION_BRIDGE_OUTPUT_CAP");
            });
        child.on("error", () => {
            failure ||= "CALIBRATION_BRIDGE_SPAWN";
        });
        child.on("close", (exitCode, terminationSignal) => {
            clearTimeout(timer);
            clearTimeout(killTimer);
            cancelled.removeEventListener("abort", abort);
            signal("SIGKILL");
            resolveResult({
                argv: [process.execPath, ...argv],
                startedHrtimeNs: started,
                finishedHrtimeNs: process.hrtime.bigint().toString(),
                exitCode,
                terminationSignal,
                failure,
                stdout: Buffer.concat(output.stdout).toString("utf8"),
                stderr: Buffer.concat(output.stderr).toString("utf8"),
            });
        });
        timer = setTimeout(() => stop("CALIBRATION_BRIDGE_TIMEOUT"), milliseconds);
        if (cancelled.aborted) abort();
    });
}
async function readAdapter(directory) {
    const lock = parse(await boundedFile(join(directory, "evidence.lock.json"), MiB));
    need(lock.kind === "http-paired-adapter-output-lock" && Array.isArray(lock.files), "CALIBRATION_ADAPTER_LOCK");
    const payload = new Map();
    for (const name of [
        "observations.json",
        "verification.json",
        "analysis.json",
        "report.html",
        "implementation.json",
    ]) {
        const raw = await boundedFile(join(directory, name), 32 * MiB);
        need(
            isDeepStrictEqual(
                lock.files.find((f) => f.path === name),
                member(name, raw),
            ),
            "CALIBRATION_ADAPTER_DIGEST",
        );
        payload.set(name, raw);
    }
    return {
        verification: parse(payload.get("verification.json")),
        analysis: parse(payload.get("analysis.json")),
        normalized: parse(payload.get("observations.json")),
        adapterLockSha256: bytesDigest(await boundedFile(join(directory, "evidence.lock.json"), MiB)),
    };
}

export async function reportCalibration({ planDirectory, inputFile, outputDirectory }) {
    const started = performance.now(),
        verified = await verifyCalibration(planDirectory);
    const root = await realpath(resolve(planDirectory)),
        plan = parse(await boundedFile(join(root, "plan.json")));
    const raw = await boundedFile(inputFile),
        input = parse(raw);
    exact(input, ["schemaVersion", "kind", "planDigest", "experiments"], "CALIBRATION_EXECUTION_SET_FIELDS");
    need(
        input.schemaVersion === 1 &&
            input.kind === "http-calibration-execution-set" &&
            input.planDigest === verified.planDigest &&
            Array.isArray(input.experiments),
        "CALIBRATION_EXECUTION_SET",
    );
    const selected = new Map(),
        planned = new Set(plan.experiments.map((e) => e.id));
    for (const row of input.experiments) {
        exact(row, ["id", "executionDirectory"], "CALIBRATION_EXECUTION_FIELDS");
        need(
            planned.has(row.id) &&
                !selected.has(row.id) &&
                typeof row.executionDirectory === "string" &&
                row.executionDirectory.length > 0 &&
                row.executionDirectory.length <= 2048,
            "CALIBRATION_EXECUTION_ID",
        );
        selected.set(row.id, {
            reference: row.executionDirectory,
            path: resolve(dirname(resolve(inputFile)), row.executionDirectory),
        });
    }
    const output = join(await realpath(dirname(resolve(outputDirectory))), basename(resolve(outputDirectory)));
    need(
        output !== root && !output.startsWith(root + "/") && !root.startsWith(output + "/"),
        "CALIBRATION_OUTPUT_OVERLAPS_PLAN",
    );
    for (const { path } of selected.values()) {
        let checked = path;
        try {
            checked = await realpath(path);
        } catch {
            /* An unreadable/missing execution is preserved as unknown during the metadata pass. */
        }
        need(
            output !== checked && !output.startsWith(checked + "/") && !checked.startsWith(output + "/"),
            "CALIBRATION_OUTPUT_OVERLAPS_EXECUTION",
        );
    }
    await mkdir(output, { mode: 0o700 });
    await mkdir(join(output, "experiments"));
    await mkdir(join(output, "rebuild"));
    await writeFile(join(output, "input.json"), raw, { flag: "wx", mode: 0o600 });
    await save(join(output, "plan-reference.json"), {
        planDigest: verified.planDigest,
        contentDigest: verified.contentDigest,
    });
    const cancellation = new AbortController(),
        interrupt = () => cancellation.abort();
    process.on("SIGTERM", interrupt);
    process.on("SIGINT", interrupt);
    const records = [],
        locations = new Map();
    try {
        const seen = { directory: new Map(), executionId: new Map(), runId: new Map(), lock: new Map() };
        let prior;
        for (const experiment of plan.experiments) {
            const record = { id: experiment.id, planDigest: experiment.planDigest, issues: [], status: "missing" };
            records.push(record);
            const supplied = selected.get(experiment.id);
            if (!supplied) {
                record.issues.push("EXECUTION_MISSING");
                continue;
            }
            try {
                record.executionDirectory = supplied.reference;
                const location = await realpath(supplied.path);
                locations.set(record.id, location);
                if (seen.directory.has(location)) {
                    record.issues.push("REUSED_DIRECTORY");
                    seen.directory.get(location).issues.push("REUSED_DIRECTORY");
                } else seen.directory.set(location, record);
                const registration = parse(await boundedFile(join(location, "execution.json")));
                const terminal = parse(await boundedFile(join(location, "terminal.json")));
                need(
                    registration.planDigest === experiment.planDigest &&
                        terminal.planDigest === experiment.planDigest &&
                        typeof registration.executionId === "string" &&
                        /^[a-f0-9]{32}$/.test(registration.executionId) &&
                        terminal.executionId === registration.executionId &&
                        Array.isArray(registration.receipts),
                    "CALIBRATION_REGISTRATION",
                );
                record.executionId = registration.executionId;
                record.executionLockSha256 = bytesDigest(
                    await boundedFile(join(location, "execution.lock.json"), 32 * MiB),
                );
                record.hostBootId = registration.hostBootId;
                record.registeredMonotonicNs = registration.registeredMonotonicNs;
                record.finishedMonotonicNs = terminal.finishedMonotonicNs;
                need(
                    uint(record.registeredMonotonicNs) <= uint(record.finishedMonotonicNs),
                    "CALIBRATION_EXECUTION_TIME",
                );
                record.runIds = registration.receipts.map((r) => r.runId);
                need(
                    record.runIds.every((id) => typeof id === "string") &&
                        new Set(record.runIds).size === record.runIds.length,
                    "CALIBRATION_PHYSICAL_RUN_IDS",
                );
                for (const [kind, values] of [
                    ["executionId", [record.executionId]],
                    ["runId", record.runIds],
                    ["lock", [record.executionLockSha256]],
                ])
                    for (const value of values) {
                        if (seen[kind].has(value)) {
                            record.issues.push("REUSED_" + kind.toUpperCase());
                            seen[kind].get(value).issues.push("REUSED_" + kind.toUpperCase());
                        } else seen[kind].set(value, record);
                    }
                if (
                    prior &&
                    (record.hostBootId !== prior.hostBootId ||
                        uint(record.registeredMonotonicNs) < uint(prior.finishedMonotonicNs))
                ) {
                    record.issues.push("EXPERIMENT_ORDER_OR_BOOT_MISMATCH");
                    prior.issues.push("EXPERIMENT_ORDER_OR_BOOT_MISMATCH");
                }
                prior = record;
                record.status = "registered";
            } catch (error) {
                record.issues.push(error.code || "EXECUTION_METADATA_UNAVAILABLE");
                record.status = "invalid";
            }
        }
        let used = 0;
        for (const record of records) {
            if (cancellation.signal.aborted) record.issues.push("CALIBRATION_CANCELLED");
            if (record.status !== "registered" || record.issues.length) continue;
            const remaining = plan.budgets.maxReportMilliseconds - (performance.now() - started);
            if (remaining <= 60000) {
                record.issues.push("CALIBRATION_REPORT_DEADLINE");
                continue;
            }
            if (used + 66 * MiB > plan.budgets.maxReportBytes) {
                record.issues.push("CALIBRATION_REPORT_BYTE_RESERVE");
                continue;
            }
            const child = plan.experiments.find((e) => e.id === record.id);
            const directory = join(output, "experiments", record.id);
            const command = await bridge({
                plan: join(root, child.directory),
                execution: locations.get(record.id),
                out: directory,
                milliseconds: Math.min(child.reportMaxMilliseconds, remaining - 30000),
                cancelled: cancellation.signal,
            });
            await save(join(output, "rebuild", record.id + "-command.json"), command);
            try {
                need(
                    command.exitCode === 0 && !command.failure && !command.terminationSignal,
                    "CALIBRATION_BRIDGE_FAILED",
                );
                const recomputed = await readAdapter(directory);
                need(
                    recomputed.verification.planDigest === record.planDigest &&
                        recomputed.verification.executionLockSha256 === record.executionLockSha256,
                    "CALIBRATION_REVERIFIED_INPUT_IDENTITY",
                );
                record.assessment = assessExperiment(recomputed.verification, recomputed.analysis, record.issues);
                record.adapterLockSha256 = recomputed.adapterLockSha256;
                record.status = "reverified";
            } catch (error) {
                record.issues.push(error.code || "CALIBRATION_REVERIFICATION_FAILED");
                record.status = "invalid";
            }
            const files = await inventory(output, plan.budgets.maxReportBytes);
            used = files.reduce((sum, f) => sum + f.bytes, 0);
        }
        const after = await verifyCalibration(root);
        need(performance.now() - started <= plan.budgets.maxReportMilliseconds, "CALIBRATION_REPORT_DEADLINE");
        need(
            after.contentDigest === verified.contentDigest &&
                bytesDigest(await boundedFile(inputFile)) === bytesDigest(raw),
            "CALIBRATION_INPUT_CHANGED_DURING_REPORT",
        );
        const experiments = records.map((record) => ({
            ...record,
            ...(record.assessment || assessExperiment(null, null, record.issues)),
            assessment: undefined,
        }));
        const cohort = assessCohort(experiments, plan.assessment);
        const acceptance = assessAcceptance(plan, experiments, cohort);
        const summary = {
            schemaVersion: plan.schemaVersion,
            kind: "http-calibration-report",
            scenario: plan.scenario,
            planDigest: plan.planDigest,
            planContentDigest: verified.contentDigest,
            executionSetSha256: bytesDigest(raw),
            cancelled: cancellation.signal.aborted,
            evidenceValid: experiments.every((e) => e.evidenceValid),
            experiments,
            cohort,
            ...acceptance,
            qualificationStatus: "not-qualified",
            qualificationAccepted: false,
            performanceAccepted: false,
            pendingGates: [
                "independent-clean-measurement-windows",
                "driver-headroom",
                "observer-and-validator-cost",
                "known-regression-detection",
                "warmup-stability",
                "formal-parameter-freeze",
            ],
            limitations: [
                "Offline artifact verification, not independent live attestation",
                "Fresh runs and nonoverlap do not prove stochastic independence",
                "Functional/insufficient observations never count as successful noise calibration",
            ],
        };
        await save(join(output, "summary.json"), summary);
        const allFiles = await inventory(output, plan.budgets.maxReportBytes);
        const files = allFiles.filter((file) => !file.path.startsWith("rebuild/"));
        const lock = {
            schemaVersion: 1,
            kind: "http-calibration-report-lock",
            planDigest: plan.planDigest,
            files,
            contentDigest: valueHash(files),
            scope: "deterministic-inputs-and-derived-reports; rebuild audit has its own lock",
        };
        const rebuild = {
            schemaVersion: 1,
            kind: "http-calibration-rebuild-lock",
            resultContentDigest: lock.contentDigest,
            files: allFiles.filter((file) => file.path.startsWith("rebuild/")),
        };
        need(
            allFiles.reduce((sum, f) => sum + f.bytes, 0) + encoded(lock).length + encoded(rebuild).length <=
                plan.budgets.maxReportBytes,
            "CALIBRATION_BYTE_BUDGET",
        );
        await save(join(output, "evidence.lock.json"), lock);
        await save(join(output, "rebuild.lock.json"), rebuild);
        need(!cancellation.signal.aborted, "CALIBRATION_CANCELLED");
        return {
            directory: output,
            evidenceValid: summary.evidenceValid,
            experiments: experiments.length,
            classifiableExperiments: summary.cohort.classifiableExperiments,
            ...acceptance,
            qualificationAccepted: false,
            performanceAccepted: false,
        };
    } finally {
        process.removeListener("SIGTERM", interrupt);
        process.removeListener("SIGINT", interrupt);
    }
}

if (process.argv[1] && pathToFileURL(resolve(process.argv[1])).href === import.meta.url)
    await componentMain(process.argv.slice(2), {
        report: {
            fields: ["--plan", "--input", "--out"],
            run: (a) =>
                reportCalibration({ planDirectory: a["--plan"], inputFile: a["--input"], outputDirectory: a["--out"] }),
        },
    });
