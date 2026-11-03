/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
import { constants } from "node:fs";
import { open, mkdir, mkdtemp, rm, writeFile, opendir, lstat, realpath } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { tmpdir } from "node:os";
import { createHash } from "node:crypto";
import { isDeepStrictEqual } from "node:util";
import { z } from "zod";
import { BenchError } from "./io.js";
import { packageRoot } from "./plans.js";
import { generateHttpProfile } from "./http-suite.js";
import { planHttp } from "./http.js";
import { freezeHttpComparison, httpComparisonPlan, reportHttpComparison } from "./http-comparison.js";
import { httpEnvironmentSettingsSchema } from "./http-environment-contracts.js";
import { httpProjectSchema, httpPresetDefinitionSchema, httpPresetPlanOptionsSchema } from "./http-preset-contracts.js";
import { validateProjectExecutor, projectHttpProfile } from "./http-project-executor.js";

const MiB = 1024 * 1024;
const names = ["quick", "standard", "full"] as const;
const sha = (raw: Buffer) => createHash("sha256").update(raw).digest("hex");
const jsonBytes = (value: unknown) => Buffer.from(JSON.stringify(value, null, 2) + "\n");
const object = z.record(z.string(), z.json());
const catalogSchema = z.strictObject({
    schemaVersion: z.literal(1),
    kind: z.literal("http-preset-catalog"),
    version: z.literal("0.1.0"),
    files: z
        .array(z.strictObject({ id: z.enum(names), path: z.string(), sha256: z.string().regex(/^[a-f0-9]{64}$/) }))
        .length(3),
});

async function bytes(path: string, cap = 8 * MiB): Promise<Buffer> {
    const file = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
    try {
        const before = await file.stat();
        if (!before.isFile() || before.size > cap)
            throw new BenchError("HTTP_PRESET_INPUT_TYPE", "Use a bounded regular JSON file", 2, false, { path, cap });
        const raw = Buffer.alloc(before.size + 1);
        let size = 0;
        while (size < raw.length) {
            const chunk = await file.read(raw, size, raw.length - size, size);
            if (!chunk.bytesRead) break;
            size += chunk.bytesRead;
        }
        const after = await file.stat();
        if (
            size !== before.size ||
            after.size !== before.size ||
            after.mtimeMs !== before.mtimeMs ||
            after.ctimeMs !== before.ctimeMs
        )
            throw new BenchError("HTTP_PRESET_INPUT_CHANGED", "Input changed while it was read", 2, false, { path });
        return raw.subarray(0, size);
    } finally {
        await file.close();
    }
}
function parse<T>(raw: Buffer, schema: z.ZodType<T>, path: string): T {
    try {
        return schema.parse(JSON.parse(new TextDecoder("utf8", { fatal: true }).decode(raw)));
    } catch (error) {
        throw new BenchError("HTTP_PRESET_INPUT_INVALID", "Preset/project JSON does not match its contract", 2, false, {
            path,
            issues: error instanceof z.ZodError ? error.issues : String(error),
            suggestion: "Inspect hgbench schema http-project and http preset show quick",
        });
    }
}
async function save(path: string, value: unknown) {
    await mkdir(dirname(path), { recursive: true, mode: 0o700 });
    await writeFile(path, jsonBytes(value), { flag: "wx", mode: 0o600 });
}
async function catalog() {
    const root = join(packageRoot, "presets/http-v0.1.0");
    const raw = await bytes(join(root, "catalog.json"));
    const value = parse(raw, catalogSchema, "preset catalog");
    if (
        !isDeepStrictEqual(
            value.files.map((file) => file.id),
            [...names],
        )
    )
        throw new BenchError("HTTP_PRESET_CATALOG", "Preset catalog must list quick, standard and full exactly once");
    const presets = await Promise.all(
        value.files.map(async (entry) => {
            if (entry.path !== entry.id + ".json")
                throw new BenchError("HTTP_PRESET_CATALOG", "Unexpected preset file path");
            const raw = await bytes(join(root, entry.path));
            if (sha(raw) !== entry.sha256)
                throw new BenchError(
                    "HTTP_PRESET_CATALOG_CHANGED",
                    "Versioned preset bytes differ from the catalog",
                    2,
                    false,
                    { id: entry.id },
                );
            const definition = parse(raw, httpPresetDefinitionSchema, entry.path);
            if (definition.id !== entry.id || definition.version !== value.version)
                throw new BenchError("HTTP_PRESET_CATALOG", "Preset identity differs from its catalog");
            if (
                definition.planImplemented !==
                Boolean(
                    definition.workload && definition.analysis && definition.lifecycleBudgets && definition.blockIds,
                )
            )
                throw new BenchError(
                    "HTTP_PRESET_CATALOG",
                    "Implemented presets need complete frozen parameters; designs have none",
                );
            return { definition, sha256: entry.sha256 };
        }),
    );
    return { presets, catalogSha256: sha(raw) };
}
export async function showHttpPreset(id = "quick") {
    const data = await catalog();
    const selected = data.presets.find((row) => row.definition.id === id);
    if (!selected)
        throw new BenchError("HTTP_PRESET_UNKNOWN", "Unknown HTTP preset", 2, false, {
            requested: id,
            available: names,
            suggestion: "hgbench http preset list",
        });
    return {
        ...selected.definition,
        definitionSha256: selected.sha256,
        catalogSha256: data.catalogSha256,
        executionReady: false,
        statisticsAccepted: false,
        performanceAccepted: false,
    };
}
export async function listHttpPresets() {
    const data = await catalog();
    return {
        version: "0.1.0",
        defaultPreset: "quick",
        presets: data.presets.map(({ definition: p, sha256 }) => ({
            id: p.id,
            version: p.version,
            scope: p.scope,
            planImplemented: p.planImplemented,
            realExecutionValidated: p.realExecutionValidated,
            engineeringTargetMilliseconds: p.engineeringTargetMilliseconds,
            design: p.design,
            definitionSha256: sha256,
            executionReady: false,
        })),
        performanceAccepted: false,
    };
}

type Input = { path: string; raw: Buffer; value: Record<string, unknown>; sha256: string };
export async function projectInputs(file: string) {
    const path = resolve(file);
    let raw: Buffer;
    try {
        raw = await bytes(path);
    } catch (error) {
        if (error instanceof BenchError) throw error;
        throw new BenchError("HTTP_PRESET_PROJECT_UNREADABLE", "Cannot read the saved HTTP project", 2, false, {
            path,
            suggestion: "Copy examples/http-project.json, then set its four source references once",
        });
    }
    const project = parse(raw, httpProjectSchema, path);
    const sources = {} as Record<"build" | "settings" | "environment" | "runtime", Input>;
    for (const role of ["build", "settings", "environment", "runtime"] as const) {
        const target = resolve(dirname(path), project.sources[role]);
        try {
            const raw = await bytes(target);
            sources[role] = { path: target, raw, value: parse(raw, object, target), sha256: sha(raw) };
        } catch (error) {
            throw new BenchError("HTTP_PRESET_PROJECT_FILE", "A saved project source is missing or invalid", 2, false, {
                role,
                path: target,
                reason: error instanceof BenchError ? error.code : String(error),
                suggestion: "Update this source reference in the project; no workload defaults were changed",
            });
        }
    }
    const target = parse(
        jsonBytes({ ...sources.settings.value, mode: sources.settings.value.mode ?? "forward" }),
        httpEnvironmentSettingsSchema,
        sources.settings.path,
    );
    const runtime = sources.runtime.value;
    if (
        runtime.kind !== "http-comparison-runtime" ||
        ![1, 2].includes(runtime.schemaVersion as number) ||
        !runtime.driver ||
        !runtime.collector ||
        !runtime.supervisor
    )
        throw new BenchError(
            "HTTP_PRESET_PROJECT_RUNTIME",
            "Use an existing comparison runtime with explicit driver, collector and supervisor resources",
        );
    if (sources.environment.value.kind !== "http-comparison-host-environment")
        throw new BenchError(
            "HTTP_PRESET_PROJECT_EVIDENCE",
            "Use the original build inventory and host-environment evidence JSON files",
        );
    validateProjectExecutor(project, runtime);
    return { project, projectPath: path, projectRaw: raw, sources, target, runtime };
}
type Loaded = Awaited<ReturnType<typeof projectInputs>>;

async function expand(
    root: string,
    loaded: Loaded,
    preset: Awaited<ReturnType<typeof showHttpPreset>>,
    selectedBudget: number,
    budgetMode: "total" | "stage-sum",
    planningBudget = selectedBudget,
) {
    const { project, sources } = loaded;
    const workload = preset.workload!,
        analysis = preset.analysis!,
        lifecycle = preset.lifecycleBudgets!;
    const { definitionSha256, catalogSha256, executionReady, statisticsAccepted, performanceAccepted, ...definition } =
        preset;
    const id = project.id + "-" + preset.id;
    await mkdir(join(root, "project"), { mode: 0o700 });
    for (const [role, source] of Object.entries(sources))
        await writeFile(join(root, "project", role + (role === "settings" ? ".original" : "") + ".json"), source.raw, {
            flag: "wx",
            mode: 0o600,
        });
    await writeFile(join(root, "project/project.json"), loaded.projectRaw, { flag: "wx", mode: 0o600 });
    const { mode: ignoredMode, ...settings } = loaded.target;
    const normalizedSettings = { ...settings, purpose: "functional" };
    await save(join(root, "project/settings.json"), normalizedSettings);
    const binding = {
        schemaVersion: 2,
        kind: "http-preset-binding",
        preset: preset.id,
        version: preset.version,
        definitionSha256: preset.definitionSha256,
        catalogSha256: preset.catalogSha256,
        definition,
        scope: preset.scope,
        projectId: project.id,
        planningToolSourceRoot: packageRoot,
        actualCaseIds: workload.caseIds,
        actualCoverage: "subset",
        primaryLatency: analysis.latency.quantile,
        descriptiveLatency: ["p95"],
        reliableP99Claimed: false,
        smallDifferenceClaimed: false,
        budgetMs: selectedBudget,
        budgetMode,
        sourceFiles: Object.fromEntries(
            Object.entries(sources).map(([role, source]) => [
                role,
                { path: source.path, sha256: source.sha256, bytes: source.raw.length },
            ]),
        ),
        normalizations: [
            "mode is selected by each comparison arm",
            "purpose is functional for quick screening; all resource settings are retained",
        ],
        statisticsAccepted: false,
        performanceAccepted: false,
        limitations: preset.limitations,
    };
    await save(join(root, "preset-binding.json"), binding);
    const settingsFile = join(root, "profile.settings.json");
    await save(settingsFile, {
        schemaVersion: 1,
        kind: "http-suite-settings",
        name: id,
        ...workload,
        purpose: "functional",
        endpoint: "http://127.0.0.1:18080/gremlin",
        authorizationEnv: null,
        target: {
            label: project.label,
            build: String(sources.build.value.sourceCommit ?? sources.build.sha256),
            mode: project.comparison === "aa" ? project.aaMode : "forward-embedded",
            environment: project.description,
            evidenceFiles: [join(root, "preset-binding.json")],
        },
    });
    const generated = await generateHttpProfile(settingsFile, join(root, "profile.json"), true);
    const profile = { ...generated, profile: projectHttpProfile(project, generated.profile), written: true };
    await save(join(root, "profile.json"), profile.profile);
    await planHttp(join(root, "profile.json"), join(root, "http-plan"));
    const sharedEvidence = Object.fromEntries(
        await Promise.all(
            ["build", "settings", "environment"].map(async (role) => {
                const path = join(root, "project", role + ".json");
                return [role, { path, sha256: sha(await bytes(path)) }];
            }),
        ),
    );
    const request = {
        schemaVersion: 3,
        kind: "http-comparison-request",
        id,
        purpose: "functional",
        comparison: project.comparison,
        modes: project.comparison === "aa" ? { A: project.aaMode, B: project.aaMode } : { A: "forward", B: "embedded" },
        blockIds: preset.blockIds,
        orderSeed: id + ":" + preset.version + ":order",
        sharedEvidence,
        metricRoles: { latency: "inference", goodput: "inference" },
        analysis: { ...analysis, resampleSeed: id + ":" + preset.version + ":resample" },
        budgets: { ...lifecycle, budgetMode, maxTotalMilliseconds: planningBudget },
    };
    await save(join(root, "comparison-request.json"), request);
    const comparison = await freezeHttpComparison(
        join(root, "comparison-request.json"),
        join(root, "http-plan"),
        join(root, "comparison"),
    );
    return { binding, profile: profile.profile, request, comparison };
}

async function inventory(root: string, prefix = ""): Promise<Array<{ path: string; bytes: number; sha256: string }>> {
    const result: Array<{ path: string; bytes: number; sha256: string }> = [];
    let entries = 0,
        total = 0;
    async function walk(prefix: string, depth: number): Promise<void> {
        if (depth > 12) throw new BenchError("HTTP_PRESET_PLAN_BUDGET", "Preset plan directory depth exceeds twelve");
        for await (const entry of await opendir(join(root, prefix))) {
            if (++entries > 1000)
                throw new BenchError("HTTP_PRESET_PLAN_BUDGET", "Preset plan exceeds 1000 file/directory entries");
            const path = prefix ? prefix + "/" + entry.name : entry.name;
            const info = await lstat(join(root, path));
            if (info.isDirectory() && !info.isSymbolicLink()) await walk(path, depth + 1);
            else {
                if (!info.isFile() || info.isSymbolicLink())
                    throw new BenchError("HTTP_PRESET_LOCK", "Preset plans contain regular files only");
                total += info.size;
                if (total > 64 * MiB || info.size > 8 * MiB)
                    throw new BenchError("HTTP_PRESET_PLAN_BUDGET", "Preset plan exceeds its file/byte budget");
                const raw = await bytes(join(root, path));
                result.push({ path, bytes: raw.length, sha256: sha(raw) });
            }
        }
    }
    await walk(prefix, 0);
    return result.sort((a, b) => (a.path < b.path ? -1 : a.path > b.path ? 1 : 0));
}
function budgetSemantics(mode: "total" | "stage-sum") {
    return mode === "total"
        ? "One controller-start hard deadline covers verification, all arms, cleanup and evidence; a timeout retains partial planned denominators. Stage maxima are caps, not accumulated minimums."
        : "Selected total must cover the sum of every declared stage maximum and final analysis allowance; legacy planning minimum is retained.";
}
function budgetError(selected: number, required: number, mode: "total" | "stage-sum") {
    return new BenchError(
        "HTTP_PRESET_BUDGET_INSUFFICIENT",
        mode === "total"
            ? "The total deadline must cover the fixed workload and minimum cleanup/partial-evidence reserve"
            : "The stage-sum deadline does not cover this unchanged preset and its lifecycle safety allowances",
        3,
        false,
        {
            selectedBudgetMs: selected,
            budgetMode: mode,
            requiredBudgetMs: required,
            normalEtaMilliseconds: null,
            changedParameters: [],
            suggestion: `Create a new plan with --budget-ms ${required}; inspect http preset show quick. The engineering target is not a hard timeout.`,
        },
    );
}
export async function planHttpPreset(
    projectFile: string,
    input: { preset?: string; out: string; dryRun?: boolean; budgetMs?: number; budgetMode?: "total" | "stage-sum" },
) {
    const preset = await showHttpPreset(input.preset ?? "quick");
    if (!preset.planImplemented)
        throw new BenchError(
            "HTTP_PRESET_NOT_IMPLEMENTED",
            "This preset is a versioned design and has no executable implementation",
            3,
            false,
            {
                preset: preset.id,
                version: preset.version,
                suggestion:
                    "Use --preset quick for the implemented planning path; standard/full never fall back silently",
            },
        );
    const checked = httpPresetPlanOptionsSchema.safeParse({ ...input, preset: preset.id });
    if (!checked.success)
        throw new BenchError(
            "HTTP_PRESET_OPTIONS_INVALID",
            "Use a new --out and a positive integer --budget-ms (at most seven days)",
            2,
            false,
            checked.error.issues,
        );
    const options = checked.data,
        loaded = await projectInputs(projectFile);
    const selectedBudget = options.budgetMs ?? preset.lifecycleBudgets!.maxTotalMilliseconds;
    const armCount = 2 * preset.blockIds!.length;
    const fixedWork =
        armCount *
        (preset.workload!.caseIds.length *
            preset.workload!.loads.length *
            preset.workload!.repeats *
            (preset.workload!.warmupMs + preset.workload!.durationMs) +
            preset.lifecycleBudgets!.settleMs);
    const minimumTotal = fixedWork + preset.lifecycleBudgets!.stopMs + 10000;
    if (options.budgetMode === "total" && selectedBudget < minimumTotal)
        throw budgetError(selectedBudget, minimumTotal, options.budgetMode);
    const temp = await mkdtemp(join(tmpdir(), "hgbench-preset-preview-"));
    let preview: Awaited<ReturnType<typeof expand>>;
    try {
        preview = await expand(
            temp,
            loaded,
            preset,
            selectedBudget,
            options.budgetMode,
            options.budgetMode === "stage-sum"
                ? Math.max(selectedBudget, preset.lifecycleBudgets!.maxTotalMilliseconds)
                : selectedBudget,
        );
    } finally {
        await rm(temp, { recursive: true, force: true });
    }
    const required = Number(
        (preview.comparison.estimate as { safetyUpperBoundMilliseconds: number }).safetyUpperBoundMilliseconds,
    );
    if (options.budgetMode === "stage-sum" && selectedBudget < required)
        throw budgetError(selectedBudget, required, options.budgetMode);
    const details = {
        preset: { id: preset.id, version: preset.version, definitionSha256: preset.definitionSha256 },
        scope: preset.scope,
        actualCaseIds: preset.workload!.caseIds,
        actualCoverage: "subset",
        engineeringTargetMilliseconds: { ...preset.engineeringTargetMilliseconds, validated: false },
        estimate: preview.comparison.estimate,
        samplingBounds: preview.comparison.samplingBounds,
        selectedBudgetMs: selectedBudget,
        budgetMode: options.budgetMode,
        budgetSemantics: budgetSemantics(options.budgetMode),
        resources: {
            target: loaded.target.budget,
            jvm: loaded.target.jvm,
            driver: loaded.runtime.driver,
            collector: loaded.runtime.collector,
            supervisor: loaded.runtime.supervisor,
        },
        statisticsAccepted: false,
        performanceAccepted: false,
        executionReady: false,
        runtimeCompatibility: {
            planningToolSourceRoot: packageRoot,
            declaredSourceRoot: loaded.runtime.sourceRoot ?? null,
            sourceRootMatchesCurrentTool:
                typeof loaded.runtime.sourceRoot === "string" &&
                resolve(loaded.runtime.sourceRoot) === resolve(packageRoot),
            actualNodeAndSourceVerification:
                "pending existing comparison prepare; no runtime is changed by this planner",
        },
        pending: [
            "existing comparison prepare must bind actual source/JDK/image and validate runtime resources",
            "real quick runtime validation",
            "repeatability/headroom/observer-cost/warmup qualification",
        ],
        limitations: preset.limitations,
    };
    const directory = resolve(options.out);
    if (options.dryRun)
        return {
            ...details,
            directory,
            written: false,
            executed: false,
            planDigest: null,
            expanded: {
                workload: preset.workload,
                analysis: preview.request.analysis,
                lifecycleBudgets: { ...preview.request.budgets, maxTotalMilliseconds: selectedBudget },
                comparison: loaded.project.comparison,
                modes: preview.request.modes,
                blockIds: preset.blockIds,
                seeds: { order: preview.request.orderSeed, resample: preview.request.analysis.resampleSeed },
            },
        };
    try {
        await mkdir(directory, { mode: 0o700 });
    } catch {
        throw new BenchError(
            "HTTP_PRESET_OUTPUT_EXISTS",
            "Choose a new output directory with an existing parent",
            4,
            false,
            { directory },
        );
    }
    try {
        const built = await expand(directory, loaded, preset, selectedBudget, options.budgetMode);
        const plan = {
            schemaVersion: 2,
            kind: "http-preset-plan",
            ...details,
            estimate: built.comparison.estimate,
            samplingBounds: built.comparison.samplingBounds,
            comparisonPlanDigest: built.comparison.planDigest,
            contentDigest: built.comparison.contentDigest,
            comparisonDirectory: "comparison",
            runtimeFile: "project/runtime.json",
            bindingFile: "preset-binding.json",
        };
        await save(join(directory, "preset-plan.json"), plan);
        const files = await inventory(directory);
        await save(join(directory, "preset.lock.json"), { schemaVersion: 1, kind: "http-preset-plan-lock", files });
        return {
            ...details,
            directory,
            written: true,
            executed: false,
            planDigest: built.comparison.planDigest,
            comparisonDirectory: join(directory, "comparison"),
            runtimeFile: join(directory, "project/runtime.json"),
            nextActions: [
                { kind: "read", argv: ["hgbench", "http", "preset", "verify", directory] },
                {
                    kind: "write",
                    argv: [
                        "hgbench",
                        "http",
                        "comparison",
                        "prepare",
                        join(directory, "project/runtime.json"),
                        "--plan",
                        join(directory, "comparison"),
                        "--out",
                        directory + "-attempt",
                    ],
                },
            ],
        };
    } catch (error) {
        await save(join(directory, "failure.json"), {
            error:
                error instanceof BenchError
                    ? { code: error.code, message: error.message, details: error.details }
                    : String(error),
            partialPreserved: true,
            executed: false,
        });
        throw error;
    }
}

export async function verifyHttpPresetPlan(directory: string) {
    const root = await realpath(resolve(directory));
    const lock = parse(
        await bytes(join(root, "preset.lock.json")),
        z.strictObject({
            schemaVersion: z.literal(1),
            kind: z.literal("http-preset-plan-lock"),
            files: z
                .array(
                    z.strictObject({
                        path: z.string(),
                        bytes: z.number().int().nonnegative(),
                        sha256: z.string().regex(/^[a-f0-9]{64}$/),
                    }),
                )
                .max(1000),
        }),
        "preset lock",
    );
    const actual = (await inventory(root)).filter((row) => row.path !== "preset.lock.json");
    if (!isDeepStrictEqual(actual, lock.files))
        throw new BenchError("HTTP_PRESET_PLAN_CHANGED", "Preset plan files differ from their frozen lock");
    const plan = JSON.parse((await bytes(join(root, "preset-plan.json"))).toString()) as Record<string, unknown>;
    const binding = JSON.parse((await bytes(join(root, "preset-binding.json"))).toString()) as Record<string, unknown>;
    const definition = httpPresetDefinitionSchema.parse(binding.definition);
    const current = await showHttpPreset("quick");
    const {
        definitionSha256,
        catalogSha256,
        executionReady,
        statisticsAccepted,
        performanceAccepted,
        ...currentDefinition
    } = current;
    const comparison = await httpComparisonPlan(["--verify", join(root, "comparison")]);
    const source = JSON.parse((await bytes(join(root, "comparison/inputs/profile.source.json"))).toString());
    const rawPlan = JSON.parse((await bytes(join(root, "comparison/plan.json"))).toString());
    const project = httpProjectSchema.parse(JSON.parse((await bytes(join(root, "project/project.json"))).toString()));
    const request = JSON.parse((await bytes(join(root, "comparison/request.json"))).toString());
    const sourceFile = z.strictObject({
        path: z.string().min(1),
        bytes: z
            .number()
            .int()
            .nonnegative()
            .max(8 * MiB),
        sha256: z.string().regex(/^[a-f0-9]{64}$/),
    });
    const sourceFiles = parse(
        jsonBytes(binding.sourceFiles),
        z.strictObject({ build: sourceFile, settings: sourceFile, environment: sourceFile, runtime: sourceFile }),
        "preset source bindings",
    );
    for (const role of ["build", "settings", "environment", "runtime"] as const) {
        const raw = await bytes(join(root, "project", role + (role === "settings" ? ".original" : "") + ".json"));
        if (raw.length !== sourceFiles[role].bytes || sha(raw) !== sourceFiles[role].sha256)
            throw new BenchError(
                "HTTP_PRESET_BINDING_INVALID",
                "Copied project source differs from the preset evidence",
                2,
                false,
                { role },
            );
    }
    const originalSettings = JSON.parse((await bytes(join(root, "project/settings.original.json"))).toString());
    const { mode: ignoredMode, ...targetSettings } = httpEnvironmentSettingsSchema.parse({
        ...originalSettings,
        mode: originalSettings.mode ?? "forward",
    });
    const normalizedSettings = await bytes(join(root, "project/settings.json"));
    if (
        !normalizedSettings.equals(jsonBytes({ ...targetSettings, purpose: "functional" })) ||
        !normalizedSettings.equals(await bytes(join(root, "comparison/inputs/settings.json")))
    )
        throw new BenchError(
            "HTTP_PRESET_BINDING_INVALID",
            "Normalized settings differ from the original resources or comparison",
        );
    const runtime = JSON.parse((await bytes(join(root, "project/runtime.json"))).toString());
    validateProjectExecutor(project, runtime);
    if (
        project.schemaVersion === 2 &&
        (source.schemaVersion !== 2 || !isDeepStrictEqual(source.executor, project.executor))
    )
        throw new BenchError("HTTP_PRESET_BINDING_INVALID", "Go profile differs from the explicit project executor");
    const expectedResources = {
        target: targetSettings.budget,
        jvm: targetSettings.jvm,
        driver: runtime.driver,
        collector: runtime.collector,
        supervisor: runtime.supervisor,
    };
    const expectedRuntimeCompatibility = {
        planningToolSourceRoot: binding.planningToolSourceRoot,
        declaredSourceRoot: runtime.sourceRoot ?? null,
        sourceRootMatchesCurrentTool:
            typeof runtime.sourceRoot === "string" &&
            typeof binding.planningToolSourceRoot === "string" &&
            resolve(runtime.sourceRoot) === resolve(binding.planningToolSourceRoot),
        actualNodeAndSourceVerification: "pending existing comparison prepare; no runtime is changed by this planner",
    };
    const expectedAnalysis = {
        ...definition.analysis!,
        resampleSeed: project.id + "-quick:" + definition.version + ":resample",
    };
    const budgetV2 = plan.schemaVersion === 2 && binding.schemaVersion === 2;
    if (budgetV2 && binding.budgetMode !== "total" && binding.budgetMode !== "stage-sum")
        throw new BenchError("HTTP_PRESET_BINDING_INVALID", "Unknown frozen preset budget mode");
    const expectedBudgets = {
        ...definition.lifecycleBudgets!,
        ...(budgetV2 ? { budgetMode: binding.budgetMode } : {}),
        maxTotalMilliseconds: binding.budgetMs,
    };
    const expectedIdentity = { id: definition.id, version: definition.version, definitionSha256 };
    if (
        (plan.schemaVersion !== 1 && !budgetV2) ||
        plan.kind !== "http-preset-plan" ||
        !isDeepStrictEqual(plan.preset, expectedIdentity) ||
        plan.scope !== definition.scope ||
        plan.actualCoverage !== "subset" ||
        !isDeepStrictEqual(plan.actualCaseIds, definition.workload!.caseIds) ||
        !isDeepStrictEqual(plan.estimate, comparison.estimate) ||
        !isDeepStrictEqual(plan.samplingBounds, comparison.samplingBounds) ||
        !isDeepStrictEqual(plan.engineeringTargetMilliseconds, {
            ...definition.engineeringTargetMilliseconds,
            validated: false,
        }) ||
        (budgetV2
            ? plan.selectedBudgetMs !== binding.budgetMs ||
              plan.budgetMode !== binding.budgetMode ||
              plan.budgetSemantics !== budgetSemantics(binding.budgetMode as "total" | "stage-sum")
            : plan.selectedSafetyBudgetMs !== binding.budgetMs) ||
        plan.statisticsAccepted !== false ||
        plan.performanceAccepted !== false ||
        plan.executionReady !== false ||
        !isDeepStrictEqual(plan.resources, expectedResources) ||
        !isDeepStrictEqual(plan.runtimeCompatibility, expectedRuntimeCompatibility) ||
        !isDeepStrictEqual(plan.limitations, definition.limitations) ||
        plan.contentDigest !== comparison.contentDigest ||
        plan.comparisonDirectory !== "comparison" ||
        plan.runtimeFile !== "project/runtime.json" ||
        plan.bindingFile !== "preset-binding.json" ||
        (binding.schemaVersion !== 1 && !budgetV2) ||
        binding.kind !== "http-preset-binding" ||
        binding.preset !== definition.id ||
        binding.version !== definition.version ||
        binding.projectId !== project.id ||
        binding.actualCoverage !== "subset" ||
        binding.primaryLatency !== definition.analysis!.latency.quantile ||
        !isDeepStrictEqual(binding.descriptiveLatency, ["p95"]) ||
        binding.reliableP99Claimed !== false ||
        binding.smallDifferenceClaimed !== false ||
        binding.statisticsAccepted !== false ||
        binding.performanceAccepted !== false ||
        !isDeepStrictEqual(binding.limitations, definition.limitations) ||
        definition.id !== "quick" ||
        definition.version !== "0.1.0" ||
        !isDeepStrictEqual(definition, currentDefinition) ||
        binding.definitionSha256 !== definitionSha256 ||
        binding.catalogSha256 !== catalogSha256 ||
        binding.scope !== definition.scope ||
        !isDeepStrictEqual(binding.actualCaseIds, definition.workload!.caseIds) ||
        request.schemaVersion !== (budgetV2 ? 3 : 1) ||
        (budgetV2 && !isDeepStrictEqual(request.metricRoles, { latency: "inference", goodput: "inference" })) ||
        request.id !== project.id + "-quick" ||
        request.purpose !== "functional" ||
        request.comparison !== project.comparison ||
        !isDeepStrictEqual(
            request.modes,
            project.comparison === "aa" ? { A: project.aaMode, B: project.aaMode } : { A: "forward", B: "embedded" },
        ) ||
        request.orderSeed !== project.id + "-quick:" + definition.version + ":order" ||
        !isDeepStrictEqual(request.analysis, expectedAnalysis) ||
        !isDeepStrictEqual(request.budgets, expectedBudgets) ||
        plan.comparisonPlanDigest !== comparison.planDigest ||
        !isDeepStrictEqual(source.benchmark.caseIds, definition.workload!.caseIds) ||
        !isDeepStrictEqual(
            rawPlan.blocks.map((row: { id: string }) => row.id),
            definition.blockIds,
        ) ||
        Object.entries(definition.workload!).some(
            ([key, value]) => !isDeepStrictEqual(key === "caseIds" ? source.benchmark.caseIds : source[key], value),
        ) ||
        rawPlan.metricBindings
            .filter((row: { kind: string }) => row.kind === "latency")
            .some((row: { valuePath: string }) => !row.valuePath.endsWith("." + definition.analysis!.latency.quantile))
    )
        throw new BenchError(
            "HTTP_PRESET_BINDING_INVALID",
            "Frozen preset identity, subset or parameters do not match the comparison",
        );
    const targetFiles = JSON.parse((await bytes(join(root, "comparison/target-files.json"))).toString()) as Array<{
        path: string;
        sha256: string;
    }>;
    const bindingSha256 = sha(await bytes(join(root, "preset-binding.json")));
    if (!targetFiles.some((file) => file.sha256 === bindingSha256))
        throw new BenchError("HTTP_PRESET_BINDING_INVALID", "Comparison does not bind the preset evidence attachment");
    return { root, plan, binding, comparison, definition, verified: true, performanceAccepted: false };
}

export async function reportHttpPreset(execution: string, planDirectory: string, outputDirectory: string) {
    const verified = await verifyHttpPresetPlan(planDirectory);
    const output = resolve(outputDirectory);
    try {
        await mkdir(output, { mode: 0o700 });
    } catch {
        throw new BenchError("HTTP_PRESET_OUTPUT_EXISTS", "Choose a new report directory with an existing parent", 4);
    }
    try {
        const result = await reportHttpComparison(
            execution,
            join(verified.root, "comparison"),
            join(output, "comparison"),
        );
        const header = {
            schemaVersion: 1,
            kind: "http-preset-report",
            preset: verified.plan.preset,
            scope: verified.definition.scope,
            actualCaseIds: verified.definition.workload!.caseIds,
            actualCoverage: "subset",
            primaryLatency: verified.definition.analysis!.latency.quantile,
            descriptiveLatency: ["p95"],
            reliableP99Claimed: false,
            smallDifferenceClaimed: false,
            comparison: result,
            statisticsAccepted: false,
            performanceAccepted: false,
            limitations: verified.definition.limitations,
        };
        await save(join(output, "preset-report.json"), header);
        const escape = (value: unknown) =>
            String(value).replace(
                /[&<>"']/g,
                (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]!,
            );
        await writeFile(
            join(output, "report.html"),
            `<!doctype html><html lang="zh-CN"><meta charset="utf-8"><meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src 'unsafe-inline'"><title>HG-Bench quick</title><style>body{font:16px system-ui;max-width:1000px;margin:32px auto;padding:20px}pre{white-space:pre-wrap}</style><h1>quick 0.1.0 · screener / descriptive</h1><p>三题子集；主指标 P50，P95 为描述值。可信 P99、细微胜负和性能资格均未声明。</p><p><a href="comparison/report.html">查看原配对报告及全部失效项</a> · <a href="preset-report.json">预设范围 JSON</a></p><pre>${escape(JSON.stringify(header, null, 2))}</pre></html>`,
            { flag: "wx", mode: 0o600 },
        );
        await save(join(output, "evidence.lock.json"), {
            schemaVersion: 1,
            kind: "http-preset-report-lock",
            files: await inventory(output),
        });
        return {
            directory: output,
            report: join(output, "report.html"),
            valid: result.valid,
            scope: verified.definition.scope,
            statisticsAccepted: false,
            performanceAccepted: false,
        };
    } catch (error) {
        await save(join(output, "failure.json"), {
            error: String(error),
            partialPreserved: true,
            performanceAccepted: false,
        });
        throw error;
    }
}
