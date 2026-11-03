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
import { open, mkdir, writeFile, readdir, lstat, realpath } from "node:fs/promises";
import { basename, dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { isDeepStrictEqual } from "node:util";
import { freezeComparison, verifyComparison, validateRequest } from "./comparison-plan.mjs";
import { valueHash } from "../paired/core.mjs";
import { bytesDigest } from "./common.mjs";

const MiB = 1024 * 1024;
const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "../..");
const ID = /^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/;
const identifier = (value) => typeof value === "string" && ID.test(value);
export const ENGINEERING_AA_MINIMUM_EXPERIMENTS = 2;
const SOURCES = [
    "components/http/calibration-plan.mjs",
    "components/http/calibration-evidence.mjs",
    "components/http/calibration-workflow.py",
];
export class CalibrationError extends Error {
    constructor(code) {
        super(code);
        this.code = code;
    }
}
export function need(value, code) {
    if (!value) throw new CalibrationError(code);
}
export function exact(value, fields, code) {
    need(
        value &&
            typeof value === "object" &&
            !Array.isArray(value) &&
            isDeepStrictEqual(Object.keys(value).sort(), [...fields].sort()),
        code,
    );
}
export const encoded = (value) => Buffer.from(JSON.stringify(value, null, 2) + "\n");
export async function boundedFile(path, maximum = 8 * MiB) {
    const fd = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
    try {
        const before = await fd.stat();
        need(before.isFile() && before.size <= maximum, "CALIBRATION_INPUT_FILE");
        const raw = Buffer.alloc(before.size + 1);
        let length = 0;
        while (length < raw.length) {
            const result = await fd.read(raw, length, raw.length - length, length);
            if (!result.bytesRead) break;
            length += result.bytesRead;
        }
        const after = await fd.stat();
        need(
            length === before.size &&
                after.size === before.size &&
                after.mtimeNs === before.mtimeNs &&
                after.mtimeMs === before.mtimeMs,
            "CALIBRATION_INPUT_CHANGED",
        );
        return raw.subarray(0, length);
    } finally {
        await fd.close();
    }
}
export const parse = (raw) => JSON.parse(new TextDecoder("utf8", { fatal: true }).decode(raw));
export const member = (path, raw) => ({ path, bytes: raw.length, sha256: bytesDigest(raw) });
export async function save(path, value) {
    await writeFile(path, encoded(value), { flag: "wx", mode: 0o600 });
}
function integer(value, low, high) {
    return Number.isSafeInteger(value) && value >= low && value <= high;
}
function seed(value) {
    return typeof value === "string" && value.length >= 1 && value.length <= 128;
}

export function validateCalibrationRequest(request) {
    const v2 = request?.schemaVersion === 2;
    exact(
        request,
        [
            "schemaVersion",
            "kind",
            "id",
            "purpose",
            "scenario",
            "comparison",
            "experiments",
            "assessment",
            "budgets",
            ...(v2 ? ["acceptancePolicy"] : []),
        ],
        "CALIBRATION_REQUEST_FIELDS",
    );
    need(
        (request.schemaVersion === 1 || v2) &&
            request.kind === "http-calibration-request" &&
            identifier(request.id) &&
            request.purpose === "calibration" &&
            request.scenario === "repeatability-aa",
        "CALIBRATION_REQUEST_IDENTITY",
    );
    if (v2) validateAcceptancePolicy(request.acceptancePolicy);
    validateRequest(request.comparison);
    need(
        request.comparison.purpose === "calibration" &&
            request.comparison.comparison === "aa" &&
            request.comparison.modes.A === request.comparison.modes.B,
        "CALIBRATION_AA_REQUIRED",
    );
    need(
        Array.isArray(request.experiments) && integer(request.experiments.length, 1, 100),
        "CALIBRATION_EXPERIMENT_COUNT",
    );
    const ids = new Set();
    for (const experiment of request.experiments) {
        exact(experiment, ["id", "orderSeed", "resampleSeed"], "CALIBRATION_EXPERIMENT_FIELDS");
        need(
            identifier(experiment.id) &&
                !ids.has(experiment.id) &&
                seed(experiment.orderSeed) &&
                seed(experiment.resampleSeed),
            "CALIBRATION_EXPERIMENT_IDENTITY",
        );
        ids.add(experiment.id);
    }
    const assessment = request.assessment;
    exact(
        assessment,
        ["confidence", "maximumFamilyFalsePositiveRate", "minimumExperiments"],
        "CALIBRATION_ASSESSMENT_FIELDS",
    );
    need(
        Number.isFinite(assessment.confidence) &&
            assessment.confidence >= 0.8 &&
            assessment.confidence <= 0.999999 &&
            Number.isFinite(assessment.maximumFamilyFalsePositiveRate) &&
            assessment.maximumFamilyFalsePositiveRate > 0 &&
            assessment.maximumFamilyFalsePositiveRate < 0.5 &&
            integer(assessment.minimumExperiments, 1, 10000),
        "CALIBRATION_ASSESSMENT_PARAMETERS",
    );
    const budgets = request.budgets;
    exact(budgets, ["maxPlanBytes", "maxReportBytes", "maxReportMilliseconds"], "CALIBRATION_BUDGET_FIELDS");
    need(
        integer(budgets.maxPlanBytes, MiB, 512 * MiB) &&
            integer(budgets.maxReportBytes, MiB, 512 * MiB) &&
            integer(budgets.maxReportMilliseconds, 1000, 86400000),
        "CALIBRATION_BUDGET",
    );
    need(
        budgets.maxReportMilliseconds >=
            request.experiments.length * Math.max(60000, request.comparison.budgets.sealAndAnalyzeMs) + 60000,
        "CALIBRATION_REPORT_TIME_RESERVE",
    );
    need(budgets.maxReportBytes >= 66 * MiB, "CALIBRATION_REPORT_BYTE_RESERVE");
    return request;
}

export function validateAcceptancePolicy(policy) {
    exact(policy, ["id", "empiricalFalsePositiveCertification"], "CALIBRATION_ACCEPTANCE_POLICY_FIELDS");
    need(
        policy.id === "engineering-v1" && ["optional", "required"].includes(policy.empiricalFalsePositiveCertification),
        "CALIBRATION_ACCEPTANCE_POLICY",
    );
    return policy;
}

export function experimentRequest(request, experiment, inputDirectory) {
    const value = structuredClone(request.comparison);
    value.id = experiment.id;
    value.orderSeed = experiment.orderSeed;
    value.analysis.resampleSeed = experiment.resampleSeed;
    for (const role of ["build", "environment", "settings"])
        value.sharedEvidence[role].path = inputDirectory
            ? resolve(inputDirectory, value.sharedEvidence[role].path)
            : `inputs/${role}.json`;
    return value;
}

export async function inventory(root, cap, ignore = new Set()) {
    const files = [];
    let total = 0,
        entries = 0;
    async function walk(relative = "") {
        need(relative.length <= 2048 && relative.split("/").length <= 64, "CALIBRATION_INVENTORY_DEPTH");
        for (const name of (await readdir(join(root, relative))).sort()) {
            const path = relative ? `${relative}/${name}` : name;
            if (ignore.has(path)) continue;
            need(++entries <= 100000, "CALIBRATION_INVENTORY_COUNT");
            const info = await lstat(join(root, path));
            need(!info.isSymbolicLink(), "CALIBRATION_INVENTORY_SYMLINK");
            if (info.isDirectory()) await walk(path);
            else {
                need(info.isFile() && files.length < 100000, "CALIBRATION_INVENTORY_TYPE");
                total += info.size;
                need(total <= cap, "CALIBRATION_BYTE_BUDGET");
                files.push(member(path, await boundedFile(join(root, path), cap)));
            }
        }
    }
    await walk();
    return files.sort((a, b) => (a.path < b.path ? -1 : a.path > b.path ? 1 : 0));
}
async function implementation() {
    return {
        schemaVersion: 1,
        kind: "http-calibration-implementation",
        files: await Promise.all(SOURCES.map(async (path) => member(path, await boundedFile(join(ROOT, path), MiB)))),
    };
}
function derivePlan(request, children, inputSha256) {
    const value = {
        schemaVersion: request.schemaVersion,
        kind: "http-calibration-plan",
        id: request.id,
        purpose: "calibration",
        scenario: request.scenario,
        inputSha256,
        mode: request.comparison.modes.A,
        assessment: request.assessment,
        ...(request.schemaVersion === 2
            ? {
                  acceptancePolicy: request.acceptancePolicy,
                  engineeringRequirements: { repeatabilityAaMinimumExperiments: ENGINEERING_AA_MINIMUM_EXPERIMENTS },
              }
            : {}),
        budgets: request.budgets,
        experiments: children,
        nullHypothesis: "same-build-mode-workload-and-runtime-settings; fresh-JVM-per-arm",
        statisticalUnit: "independent-physical-run-pairs-within-predeclared-experiments",
        familyFalsePositiveDefinition: "any-qualified-metric-above-or-below-both-predeclared-practical-budgets",
        qualificationAccepted: false,
        performanceAccepted: false,
        executed: false,
    };
    return { ...value, planDigest: valueHash(value) };
}
async function childReportBudget(directory) {
    const plan = parse(await boundedFile(join(directory, "plan.json")));
    const settings = parse(await boundedFile(join(directory, "inputs/settings.json")));
    const observation = settings.jvmObservation;
    let parsing = 0;
    if (observation !== undefined) {
        need(
            Number.isSafeInteger(observation?.parser?.timeoutMs) &&
                observation.parser.timeoutMs >= 100 &&
                observation.parser.timeoutMs <= 300000,
            "CALIBRATION_JVM_REPLAY_BUDGET",
        );
        parsing = plan.budgets.armCount * (observation.parser.timeoutMs + 5000);
    }
    return Math.max(60000, plan.budgets.sealAndAnalyzeMs, parsing + plan.analysisPolicy.maxMilliseconds + 60000);
}
export async function freezeCalibration({ inputFile, httpPlanDirectory, outputDirectory }) {
    const raw = await boundedFile(inputFile),
        request = validateCalibrationRequest(parse(raw));
    const root = join(await realpath(dirname(resolve(outputDirectory))), basename(resolve(outputDirectory)));
    const httpRoot = await realpath(resolve(httpPlanDirectory));
    need(
        root !== httpRoot && !root.startsWith(httpRoot + "/") && !httpRoot.startsWith(root + "/"),
        "CALIBRATION_OUTPUT_OVERLAPS_HTTP_PLAN",
    );
    await mkdir(root, { mode: 0o700 });
    await writeFile(join(root, "request.json"), raw, { flag: "wx", mode: 0o600 });
    await mkdir(join(root, "requests"));
    await mkdir(join(root, "experiments"));
    const children = [];
    for (const experiment of request.experiments) {
        const child = experimentRequest(request, experiment, dirname(resolve(inputFile)));
        const path = join(root, "requests", `${experiment.id}.json`);
        await save(path, child);
        const compiled = await freezeComparison({
            inputFile: path,
            httpPlanDirectory,
            outputDirectory: join(root, "experiments", experiment.id),
        });
        children.push({
            id: experiment.id,
            directory: `experiments/${experiment.id}`,
            planDigest: compiled.planDigest,
            contentDigest: compiled.contentDigest,
            reportMaxMilliseconds: await childReportBudget(join(root, "experiments", experiment.id)),
        });
        await inventory(root, request.budgets.maxPlanBytes);
    }
    need(
        children.reduce((sum, child) => sum + child.reportMaxMilliseconds, 60000) <=
            request.budgets.maxReportMilliseconds,
        "CALIBRATION_REPORT_REPLAY_TIME_RESERVE",
    );
    const plan = derivePlan(request, children, bytesDigest(raw));
    await save(join(root, "plan.json"), plan);
    await save(join(root, "implementation.json"), await implementation());
    const files = await inventory(root, request.budgets.maxPlanBytes);
    const lock = {
        schemaVersion: 1,
        kind: "http-calibration-plan-lock",
        planDigest: plan.planDigest,
        files,
        contentDigest: valueHash(files),
    };
    need(
        files.reduce((sum, f) => sum + f.bytes, 0) + encoded(lock).length <= request.budgets.maxPlanBytes,
        "CALIBRATION_BYTE_BUDGET",
    );
    await save(join(root, "plan.lock.json"), lock);
    return {
        directory: root,
        planDigest: plan.planDigest,
        contentDigest: valueHash(files),
        experiments: children,
        executed: false,
        qualificationAccepted: false,
        performanceAccepted: false,
    };
}
export async function verifyCalibration(directory) {
    const root = resolve(directory),
        raw = await boundedFile(join(root, "request.json"));
    const request = validateCalibrationRequest(parse(raw));
    const plan = parse(await boundedFile(join(root, "plan.json")));
    const lock = parse(await boundedFile(join(root, "plan.lock.json"), 32 * MiB));
    need(
        lock.schemaVersion === 1 &&
            lock.kind === "http-calibration-plan-lock" &&
            lock.planDigest === plan.planDigest &&
            Array.isArray(lock.files),
        "CALIBRATION_PLAN_LOCK",
    );
    const files = await inventory(root, request.budgets.maxPlanBytes, new Set(["plan.lock.json"]));
    need(
        isDeepStrictEqual(files, lock.files) && valueHash(files) === lock.contentDigest,
        "CALIBRATION_CONTENT_CHANGED",
    );
    need(
        isDeepStrictEqual(parse(await boundedFile(join(root, "implementation.json"))), await implementation()),
        "CALIBRATION_IMPLEMENTATION_CHANGED",
    );
    const children = [];
    for (const experiment of request.experiments) {
        const childDirectory = `experiments/${experiment.id}`;
        const verified = await verifyComparison(join(root, childDirectory));
        const child = parse(await boundedFile(join(root, childDirectory, "request.json")));
        need(isDeepStrictEqual(child, experimentRequest(request, experiment)), "CALIBRATION_CHILD_SETTINGS_DRIFT");
        const input = parse(await boundedFile(join(root, "requests", `${experiment.id}.json`)));
        for (const role of ["build", "environment", "settings"])
            input.sharedEvidence[role].path = `inputs/${role}.json`;
        need(isDeepStrictEqual(input, child), "CALIBRATION_CHILD_REQUEST_DRIFT");
        children.push({
            id: experiment.id,
            directory: childDirectory,
            planDigest: verified.planDigest,
            contentDigest: verified.contentDigest,
            reportMaxMilliseconds: await childReportBudget(join(root, childDirectory)),
        });
    }
    need(
        children.reduce((sum, child) => sum + child.reportMaxMilliseconds, 60000) <=
            request.budgets.maxReportMilliseconds,
        "CALIBRATION_REPORT_REPLAY_TIME_RESERVE",
    );
    need(isDeepStrictEqual(plan, derivePlan(request, children, bytesDigest(raw))), "CALIBRATION_DERIVED_PLAN_CHANGED");
    return {
        directory: root,
        planDigest: plan.planDigest,
        contentDigest: lock.contentDigest,
        experiments: children,
        executed: false,
        qualificationAccepted: false,
        performanceAccepted: false,
    };
}
export function describe() {
    return {
        schemaVersion: 1,
        kind: "http-calibration-planner-capability",
        scenario: ["repeatability-aa"],
        requestSchemaVersions: [1, 2],
        acceptancePolicy: {
            schemaVersion: 2,
            id: "engineering-v1",
            empiricalFalsePositiveCertification: ["optional", "required"],
            source: "Frozen request; no manual passed gates or runtime overrides",
            scope: "Engineering and empirical certification are separate; unconnected engineering gates remain unknown",
            repeatabilityAaMinimumExperiments: ENGINEERING_AA_MINIMUM_EXPERIMENTS,
        },
        version2AdditionalRequestFields: ["acceptancePolicy"],
        purpose: ["calibration"],
        effects: ["read-local-files", "create-new-plan-directory"],
        requestFields: [
            "schemaVersion",
            "kind",
            "id",
            "purpose",
            "scenario",
            "comparison",
            "experiments",
            "assessment",
            "budgets",
        ],
        comparison: "Existing strict AA request template; only experiment ID/order/resample seeds are replaced",
        execution: "Use existing public http comparison prepare/task lifecycle for each compiled experiment",
        qualificationAccepted: false,
        performanceAccepted: false,
        documentation: "docs/http-calibration.md",
        acceptanceDocumentation: "docs/http-engineering-acceptance.md",
    };
}
export async function componentMain(args, callbacks) {
    if (!args.length || args.includes("--help")) {
        process.stdout.write(
            "HTTP calibration v1/v2: describe | plan --input REQUEST --http-plan FROZEN --out NEW | " +
                "verify --plan PLAN | report --plan PLAN --input EXECUTION_SET --out NEW\n" +
                "Offline only; repeatability-aa; no performance qualification. docs/http-calibration.md\n" +
                "v2 freezes engineering-v1 with optional|required empirical certification; " +
                "missing engineering evidence stays unknown. docs/http-engineering-acceptance.md\n",
        );
        return;
    }
    try {
        const action = args[0],
            fields = {},
            required = callbacks[action]?.fields;
        need(required && (args.length - 1) % 2 === 0, "CALIBRATION_COMMAND");
        for (let n = 1; n < args.length; n += 2) {
            need(required.includes(args[n]) && !Object.hasOwn(fields, args[n]) && args[n + 1], "CALIBRATION_OPTIONS");
            fields[args[n]] = args[n + 1];
        }
        need(
            required.every((key) => Object.hasOwn(fields, key)),
            "CALIBRATION_OPTIONS",
        );
        const data = await callbacks[action].run(fields);
        process.stdout.write(JSON.stringify(action === "describe" ? data : { ok: true, data }) + "\n");
    } catch (error) {
        process.stderr.write(
            JSON.stringify({
                ok: false,
                error: {
                    code: error.code || "CALIBRATION_IO_OR_INPUT",
                    message: error.message,
                    retryable: false,
                    nextAction: "Inspect retained inputs; use a new output directory",
                },
            }) + "\n",
        );
        process.exitCode = 6;
    }
}
if (process.argv[1] && pathToFileURL(resolve(process.argv[1])).href === import.meta.url)
    await componentMain(process.argv.slice(2), {
        describe: { fields: [], run: async () => describe() },
        plan: {
            fields: ["--input", "--http-plan", "--out"],
            run: (a) =>
                freezeCalibration({
                    inputFile: a["--input"],
                    httpPlanDirectory: a["--http-plan"],
                    outputDirectory: a["--out"],
                }),
        },
        verify: { fields: ["--plan"], run: (a) => verifyCalibration(a["--plan"]) },
    });
