#!/usr/bin/env node
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

import { Command, CommanderError, Option } from "commander";
import { readFile, readdir, stat } from "node:fs/promises";
import { join, resolve } from "node:path";
import { z } from "zod";
import { analysisResultSchema, schemas, TOOL_VERSION } from "./contracts.js";
import { describe } from "./components.js";
import { BenchError, errorInfo, experimentDir, readJson, writeJson } from "./io.js";
import { builtins, freezePlan, loadPlan, packageRoot, resolveComponent } from "./plans.js";
import { cancel, evidence, requestExperiment, resume, snapshot, submit, wait } from "./store.js";
import { work } from "./worker.js";
import { inspectSf100Plan, sf100SuiteId } from "./sf100.js";
import { inspectSf100Exchange, assessSf100Recovery } from "./sf100-protocol.js";
import { checkpointSf100 } from "./sf100-checkpoints.js";
import { planLoaderJobs, runLoaderJob } from "./sf100-loader.js";
import { inspectSpecification } from "./benchmark-spec.js";
import { frozenTaskSchema } from "./task-contracts.js";
import {
    freezeTask,
    verifyTask,
    submitTask,
    taskSnapshot,
    taskRequestId,
    waitTask,
    cancelTask,
    taskEvents,
    taskLogs,
    taskArtifacts,
} from "./tasks.js";
import { taskWork } from "./task-worker.js";
import { listTasks } from "./task-list.js";
import { httpReportAction } from "./http-reports.js";
import { runHttpProfile, httpRunSummary } from "./http-run.js";
import { inspectAnyHttpProfile, planHttp } from "./http.js";
import { generateHttpProfile, inspectHttpSuite } from "./http-suite.js";
import {
    listHttpPresets,
    showHttpPreset,
    planHttpPreset,
    verifyHttpPresetPlan,
    reportHttpPreset,
} from "./http-presets.js";
import { describeHttpCapacity, planHttpCapacity, verifyHttpCapacityPlan } from "./http-capacity.js";
import { httpCapacityExecution, prepareHttpCapacityExecution } from "./http-capacity-execution.js";
import { planHttpCapacityConfirmation, verifyHttpCapacityConfirmationPlan } from "./http-capacity-confirmation.js";
import {
    freezeHttpComparison,
    httpComparisonPlan,
    httpComparisonWorkflow,
    prepareHttpComparison,
    reportHttpComparison,
} from "./http-comparison.js";
import { planHttpEnvironment, httpEnvironmentAction } from "./http-environment.js";
import { probeHttpRuntime, httpRuntimeAction } from "./http-runtime.js";
import {
    describeHttpCalibration,
    freezeHttpCalibration,
    verifyHttpCalibration,
    reportHttpCalibration,
    prepareHttpCalibration,
    httpCalibrationWorkflow,
    reportHttpPhaseWindows,
    reportHttpDriverSelfCost,
    reportHttpCompletionWindows,
    reportHttpCapacity,
    reportHttpCapacityConfirmation,
    reportHttpKnownRegression,
    reportHttpDriverHeadroom,
    reportHttpObserverCost,
    reportHttpEngineeringAcceptance,
} from "./http-calibration.js";
import { describeHttpGenerator, planHttpGenerator, generatorComponent } from "./http-generator.js";
import { execFile } from "node:child_process";
import { promisify } from "node:util";

const program = new Command();
program
    .name("hgbench")
    .version(TOOL_VERSION)
    .description(
        "HugeGraph HTTP and graph benchmark controller; versioned workloads, bounded tasks and offline reports.",
    )
    .helpOption("-h, --help", "Show help without contacting a target")
    .option("--root <directory>", "Local task/evidence store", ".hgbench")
    .addOption(
        new Option("--output <format>", "Output format; JSON also covers command errors")
            .choices(["json", "text"])
            .default("json"),
    )
    .addHelpText(
        "after",
        `
AGENT QUICKSTART
  hgbench benchmark list
  hgbench graph describe
  hgbench graph dataset list
  hgbench schema graph-project
  hgbench graph benchmark graph-project.json --out ./graph-test --dry-run
  hgbench graph status ./graph-test

FULL LDBC SPECIFICATION (optional advanced track)
  hgbench benchmark spec
  hgbench benchmark questions --id IC14
  hgbench schema sf100-plan
  hgbench benchmark validate benchmarks/ldbc-snb-interactive-v1/sf100-plan.json
  hgbench benchmark plan benchmarks/ldbc-snb-interactive-v1/sf100-plan.json

COMPONENT TASKS (Python/Java/other trusted programs)
  hgbench task plan examples/spec-check-task.json --out spec-task.json
  hgbench task run --plan spec-task.json --detach --request-id spec-001
  hgbench task status --request-id spec-001
  hgbench task list --limit 20
  hgbench task --help

CONTROLLER FIXTURE (separate development commands)
  hgbench schema plan
  hgbench plan examples/controller-smoke.json --out resolved-plan.json
  hgbench run --plan resolved-plan.json --detach --request-id demo-001
  hgbench status --request-id demo-001
  hgbench wait --experiment <id> --timeout-ms 30000
  hgbench results show --experiment <id>

Components are trusted local programs, invoked without a shell. Inspect their manifests first.
GRAPH QUERY TOOLCHAIN
  hgbench graph describe
  hgbench graph dataset prepare --out ./sf1 --dry-run
  hgbench graph benchmark graph-project.json --out ./graph-test --dry-run
  hgbench graph reference --help
  hgbench graph run graph.json --preset quick --out ./graph-quick --dry-run

REAL HTTP TOOLCHAIN
  hgbench http --help
  hgbench http suite
  hgbench http generate examples/http-suite-functional.json --out /absolute/profile.json --dry-run
  hgbench http plan examples/http-gremlin.json --out /absolute/new-attempt
  hgbench task run --plan /absolute/new-attempt/task.json --detach --request-id http-001
  hgbench http report /absolute/new-attempt/output --out /absolute/new-report

Use command --help for examples, effects and recovery. Performance acceptance requires calibration.
`,
    );
program.exitOverride();
program.configureOutput({ writeErr: () => undefined });

function root(): string {
    return resolve(program.opts().root);
}
function output(command: string, data: unknown, nextActions: unknown[] = []): void {
    const envelope = { schemaVersion: 1, ok: true, command, data, nextActions };
    process.stdout.write(`${JSON.stringify(envelope, null, program.opts().output === "text" ? 2 : undefined)}\n`);
}
function idFrom(options: { experiment?: string; requestId?: string }): string {
    if (Boolean(options.experiment) === Boolean(options.requestId)) {
        throw new BenchError("INVALID_SELECTOR", "Provide exactly one of --experiment or --request-id");
    }
    return options.experiment ?? requestExperiment(options.requestId!);
}
function next(id: string) {
    return [
        { kind: "read", argv: ["hgbench", "--root", root(), "status", "--experiment", id] },
        { kind: "read", argv: ["hgbench", "--root", root(), "results", "show", "--experiment", id] },
    ];
}
function duration(value: string): number {
    const n = Number(value);
    if (!Number.isInteger(n) || n < 0 || n > 60000) {
        throw new BenchError("INVALID_TIMEOUT", "timeout-ms must be an integer between 0 and 60000");
    }
    return n;
}
function command(name: string, description: string, effects: string, example: string): Command {
    const cmd = program.command(name).description(description);
    cmd.addHelpText("after", `\nEffects: ${effects}\nExample: hgbench ${example}\n`);
    return cmd;
}

const benchmark = command("benchmark", "Discover bundled suites", "read-only, offline", "benchmark list");
const httpCommand = command(
    "http",
    "Plan real HTTP workloads and regenerate descriptive reports",
    "plan freezes a new local attempt; task run sends requests; report is offline",
    "http plan examples/http-gremlin.json --out /absolute/new-attempt",
);
httpCommand
    .command("run")
    .argument("<profile>", "Existing HTTP profile v1/v2; one saved endpoint/workload configuration")
    .requiredOption("--out <directory>", "New run directory; existing runs are never reused")
    .option("--request-id <key>", "Optional stable submission key in this --root")
    .option("--detach", "Return the task ID immediately; task survives CLI/SSH disconnect")
    .option("--budget-ms <n>", "Whole execution cap including reserved termination; defaults to profile-derived bound")
    .option("--budget-mode <mode>", "total (default) or stage-sum (require conservative stage bound)", "total")
    .option("--dry-run", "Validate and show fixed work/deadline; no writes or HTTP requests")
    .description("Plan, submit and produce the existing JSON/HTML report in one command")
    .addHelpText(
        "after",
        "\nExample: hgbench http run profile.json --out /absolute/new-run --detach\nRuns the configured profile once with all of its checks and the original JSON/HTML report.\nrun.json saves task identity and report paths. Query/cancel that original task with task status/wait/cancel --id.\nExisting output directories are never replayed. Process success is separate from correctness and performance qualification.\nA Go profile requiring live host identity gates must use its controlled comparison task; this command preserves that requirement.\n",
    )
    .action(async (file, options) => {
        const run = await runHttpProfile(file, {
            out: options.out,
            root: root(),
            requestId: options.requestId,
            dryRun: Boolean(options.dryRun),
            budgetMs: options.budgetMs === undefined ? undefined : Number(options.budgetMs),
            budgetMode: (() => {
                if (!["total", "stage-sum"].includes(options.budgetMode))
                    throw new BenchError("HTTP_RUN_BUDGET", "budget-mode must be total or stage-sum");
                return options.budgetMode as "total" | "stage-sum";
            })(),
        });
        if (!("taskId" in run) || options.detach)
            return output("http.run", run, "taskId" in run ? taskNext(run.taskId) : []);
        let state = await waitTask(root(), run.taskId, 1000);
        while (!state.terminal) state = await waitTask(root(), run.taskId, 1000);
        output(
            "http.run",
            { ...run, task: state, summary: await httpRunSummary(run.reportJson) },
            taskNext(run.taskId),
        );
        if (state.state !== "completed" || state.interrupted) process.exitCode = 6;
    });

const throughput = httpCommand
    .command("throughput")
    .description("HTTP v0.2 closed-loop quick/standard; shared scheduling and reusable light calibration")
    .addHelpText(
        "after",
        "\nUses an existing generated suite profile and endpoint. No server deployment or JVM restart.\nFirst run performs two fixed-response driver checks (80s fixed windows); later runs reverify matching cached raw.\nForeground orchestration: state.json retains the active child task for status/cancel after interruption.\nOld http preset v0.1 remains frozen. Contract: docs/http-throughput.md\n",
    );
throughput
    .command("describe")
    .description("Show exact v0.2 defaults and settings keys; offline")
    .action(async () => {
        const { throughputPresets } = await import("./throughput.js");
        output("http.throughput.describe", {
            version: "0.2.0",
            track: "http",
            presets: throughputPresets,
            defaultStandardBudgetMs: 5400000,
            calibration: {
                caseIds: ["scalar-add", "rows-4096"],
                rates: [1000, 40],
                warmupMs: 10000,
                durationMs: 30000,
            },
            performanceAccepted: false,
        });
    });
for (const action of ["run", "calibrate"] as const)
    throughput
        .command(action)
        .argument("<profile>", "Generated suite HTTP profile v1/v2, including target and executor")
        .requiredOption("--out <directory>", "New attempt directory; never replay an existing output")
        .option("--preset <name>", "quick (three representative cases) or standard (all eleven)", "quick")
        .option("--settings <file>", "Optional partial scheduling overrides; recorded as a custom protocol")
        .option("--cache <directory>", "Calibration cache; default <root>/calibration")
        .option(
            "--refresh-calibration",
            "Explicitly collect new representative checks; retain old raw and limited results",
        )
        .option(
            "--budget-ms <n>",
            "Whole run cap including verification/report/cleanup; default unchanged-settings HTTP standard: 90 minutes",
        )
        .option("--dry-run", "Offline preview; no files, provider, tasks or HTTP")
        .option("--budget-mode <mode>", "total (default) or stage-sum (require conservative allowances)", "total")
        .description(
            action === "run"
                ? "Explore concurrency, confirm candidates and generate offline SVG reports"
                : "Run/reuse only two representative driver checks, no target calls",
        )
        .addHelpText(
            "after",
            "\nExample: hgbench http throughput run go-profile.json --preset standard --out ./new-standard --dry-run\nThen omit --dry-run to execute. Limits and failed points remain visible; no automatic sample or window reduction.\nSIGINT/SIGTERM cancels the original active child and writes partial results. After abrupt disconnection inspect state.json.\nCalibration covers loopback short/structured responses at declared rates; it does not certify every case or target network.\n",
        )
        .action(async (file, options) => {
            if (!["total", "stage-sum"].includes(options.budgetMode))
                throw new BenchError("THROUGHPUT_BUDGET_MODE", "Use total or stage-sum");
            const abort = new AbortController();
            const stop = () => abort.abort();
            process.on("SIGINT", stop);
            process.on("SIGTERM", stop);
            try {
                const result = await (
                    await import("./http-throughput.js")
                ).runHttpThroughput(file, {
                    ...options,
                    root: root(),
                    signal: abort.signal,
                    dryRun: Boolean(options.dryRun),
                    budgetMs: options.budgetMs === undefined ? undefined : Number(options.budgetMs),
                    calibrationOnly: action === "calibrate",
                });
                output(`http.throughput.${action}`, result);
                if ("status" in result && result.status !== "completed") process.exitCode = 6;
            } finally {
                process.off("SIGINT", stop);
                process.off("SIGTERM", stop);
            }
        });
throughput
    .command("report")
    .argument("<directory>", "Original v0.2 attempt")
    .requiredOption("--out <directory>", "New offline report directory")
    .option(
        "--target-diagnostics <file>",
        "Attach digest-checked GC/swap logs as approximate wall-clock diagnostics; no qualification change",
    )
    .description("Reverify original raw and rebuild throughput/detail reports; no HTTP")
    .addHelpText(
        "after",
        "\nOptional diagnostics manifest: original state.json planSha256, relative host/GC files with bytes/SHA-256.\nExample: hgbench http throughput report ./standard --out ./new-report --target-diagnostics ./observations/manifest.json\nContract and limits: docs/graph-query.md (GC/swap diagnostics). No collector or target is started.\n",
    )
    .action(async (directory, options) => {
        const component = await import(join(packageRoot, "components/http/throughput-report.mjs"));
        output(
            "http.throughput.report",
            await component.reportThroughput(resolve(directory), resolve(options.out), {
                targetDiagnostics: options.targetDiagnostics && resolve(options.targetDiagnostics),
            }),
        );
    });

const graph = program
    .command("graph")
    .description("Graph queries: default SF1, optional SF10/SF100, shared quick/standard scheduler");
const graphDataset = graph.command("dataset").description("Official data preparation; default SF1, no target requests");
graph
    .command("status")
    .argument("<directory>", "Original project, dataset, import or query output")
    .description("Read local stage, imported prefix and query task status; no target requests")
    .action(async (directory) =>
        output("graph.status", await (await import("./graph-project.js")).graphStatus(directory)),
    );
graphDataset
    .command("list")
    .description("Offline sources, scales and resource ceilings")
    .action(async () => output("graph.dataset.list", await (await import("./graph-project.js")).graphDatasets()));
graphDataset
    .command("prepare")
    .requiredOption("--out <directory>", "New prepared dataset and independent reference directory")
    .addOption(
        new Option("--scale <n>", "Official scale factor; quick/standard use the same graph")
            .choices(["1", "10", "100"])
            .default("1"),
    )
    .option("--cache <directory>", "Archive cache; default .hgbench/graph-cache/sf<SCALE>")
    .option("--python <path>", "Trusted Python executable", "python3")
    .option("--resume", "Reuse completed stages of the same preparation; never imports a graph")
    .option("--max-seconds <n>", "Whole preparation time ceiling; see dataset list for scale defaults")
    .option("--max-bytes <n>", "Prepared workspace byte ceiling")
    .option("--max-download-bytes <n>", "Initial archive download ceiling; no update stream is fetched")
    .option("--reserve-free-bytes <n>", "Filesystem free-space floor")
    .option("--dry-run", "Show stages, source and limits without network, writes or archive reads")
    .description("Download, convert and prepare independent five-query answers in one command")
    .addHelpText(
        "after",
        "\nExample: hgbench graph dataset prepare --out ./sf1\nRequires Python3, curl and zstd. The official archive is cached by SHA-256.\nOnly Person/knows CSV is retained for reference; no full CSV copy or reference database deployment.\nSource digest/count/endpoint checks remain; target full-content scans are optional advanced tools.\nInspect prepare-state.json after interruption and use --resume with identical options. Contract: docs/graph-query.md\n",
    )
    .action(async (options) => {
        const abort = new AbortController(),
            stop = () => abort.abort();
        process.on("SIGINT", stop);
        process.on("SIGTERM", stop);
        try {
            for (const key of ["scale", "maxSeconds", "maxBytes", "maxDownloadBytes", "reserveFreeBytes"])
                if (options[key] !== undefined) options[key] = Number(options[key]);
            output(
                "graph.dataset.prepare",
                await (await import("./graph-project.js")).prepareGraphDataset(options, abort.signal),
            );
        } finally {
            process.off("SIGINT", stop);
            process.off("SIGTERM", stop);
        }
    });
graph
    .command("import")
    .argument("<dataset>", "Directory from graph dataset prepare")
    .requiredOption("--out <directory>", "Import state, per-shard receipts and snapshot")
    .requiredOption("--loader-home <directory>", "Built Loader installation supporting --retry-times 0")
    .requiredOption("--graph-url <url>", "Dedicated existing benchmark graph; initially empty, including user schema")
    .requiredOption("--traversal-source <alias>", "Target traversal-source alias")
    .option("--authorization-env <name>", "Environment containing Bearer authorization; values are not saved")
    .option("--min-edge-coverage <fraction>", "Required edge fraction; knows must be complete", "0.95")
    .option("--budget-ms <n>", "Whole import time ceiling", "21600000")
    .option("--resume", "Reuse acknowledged prefix; unknown write outcomes stop without replay")
    .option("--dry-run", "Metadata-only preview; no requests, writes or Loader launches")
    .description("Apply schema and import shards with lightweight acknowledgements and coverage checks")
    .addHelpText(
        "after",
        "\nExample: hgbench graph import ./sf1 --loader-home /opt/hg-loader --graph-url http://localhost:8080/graphs/bench_sf1 --traversal-source __g_bench_sf1 --out ./sf1-import\nThe database is already running; only the named dedicated graph is written. No service restarts or graph deletion.\nDefault: all vertices, complete Person/knows and at least 95% of total edges; --min-edge-coverage 1 selects all.\nSource-validated endpoints plus successful vertex writes allow disabling per-edge endpoint reads.\nNo full target scan is forced. state.json and snapshot.json distinguish acknowledgements from content verification.\nOn interruption inspect the SAME output and use --resume; ambiguous writes require explicit recovery.\n",
    )
    .action(async (dataset, options) => {
        const abort = new AbortController(),
            stop = () => abort.abort();
        process.on("SIGINT", stop);
        process.on("SIGTERM", stop);
        try {
            output(
                "graph.import",
                await (
                    await import("./graph-project.js")
                ).importGraphDataset(dataset, {
                    ...options,
                    minimumEdgeCoverage: Number(options.minEdgeCoverage),
                    budgetMs: Number(options.budgetMs),
                    signal: abort.signal,
                }),
            );
        } finally {
            process.off("SIGINT", stop);
            process.off("SIGTERM", stop);
        }
    });
graph
    .command("benchmark")
    .argument("<project>", "Saved graph-project JSON; inspect hgbench schema graph-project")
    .requiredOption("--out <directory>", "New preparation/import/test workspace")
    .addOption(
        new Option("--preset <name>", "Query measurement presets; dataset scale is independent")
            .choices(["quick", "standard", "both"])
            .default("both"),
    )
    .option("--budget-ms <n>", "Whole chain time ceiling, including preparation and both presets", "21600000")
    .option("--resume", "Reuse completed preparation/import; incomplete measurements are never replayed")
    .option("--dry-run", "Offline project preview; no requests, writes or downloads")
    .description("Prepare default SF1, import and run graph quick/standard from one project")
    .addHelpText(
        "after",
        "\nExample: hgbench graph benchmark examples/graph-project.json --out ./graph-test --dry-run\nScale defaults to 1; set scaleFactor:10 or 100 explicitly. An optional dataset path reuses prepared input/reference.\nUses your running instance and Loader installation. Requires a dedicated empty graph on first import.\nQuery answers are checked; full target scans remain optional. Before formal measurement disable swap and HG memory-monitor manual GC.\nA failed import stops measurements. Results retain selected scale, imported coverage, errors and limitations.\n",
    )
    .action(async (file, options) => {
        const abort = new AbortController(),
            stop = () => abort.abort();
        process.on("SIGINT", stop);
        process.on("SIGTERM", stop);
        try {
            const result = await (
                await import("./graph-project.js")
            ).runGraphProject(file, {
                ...options,
                root: root(),
                budgetMs: Number(options.budgetMs),
                signal: abort.signal,
            });
            output("graph.benchmark", result);
            if ("status" in result && result.status !== "completed") process.exitCode = 6;
        } finally {
            process.off("SIGINT", stop);
            process.off("SIGTERM", stop);
        }
    });
graph
    .command("describe")
    .description("Offline defaults, reference contract and known qualification limits")
    .action(async () => {
        const { graphThroughputPresets, throughputFixedMs } = await import("./throughput.js");
        output("graph.describe", {
            track: "graph",
            suite: "hgbench-graph-query-v0.1",
            cases: (await import("./graph.js")).graphCases,
            presets: graphThroughputPresets,
            fixedWindowMs: Object.fromEntries(
                Object.entries(graphThroughputPresets).map(([k, v]) => [k, throughputFixedMs(5, v)]),
            ),
            reference: "independent CSV adjacency; no SUT-derived answers",
            dataset: "SF1 by default; optional SF10/SF100; both presets use the same selected graph",
            defaultScaleFactor: 1,
            performanceAccepted: false,
        });
    });
const graphReference = graph
    .command("reference")
    .description("Independent SF1/SF10/SF100 CSV reference preparation; local Python component, no target calls");
for (const action of ["index", "export"] as const) {
    const cmd = graphReference
        .command(action)
        .requiredOption("--out <path>", "New index directory or reference JSON; never overwritten")
        .option("--max-seconds <n>", "Whole preparation deadline in seconds", "7200")
        .option("--max-bytes <n>", "Own output budget", "8589934592")
        .option("--min-free-bytes <n>", "Disk free-space floor", "4294967296")
        .option("--python <path>", "Trusted Python executable", "python3")
        .option("--dry-run", "Show component argv without writing or reading data")
        .description(
            action === "index"
                ? "Verify locked Person/knows CSV and build its directed adjacency index"
                : "Export deterministic stratified pools and exact <=3-hop path witnesses",
        )
        .addHelpText(
            "after",
            "\nUse a CPU/memory/disk-bounded Docker environment for remote data preparation.\nNo database is created. Incomplete indexes are refused; use a new output after failure.\nCSV projection verification does not verify a loaded HG instance. Contract: docs/graph-query.md\n",
        );
    if (action === "index")
        cmd.requiredOption("--manifest <file>", "Locked independent same-scale CSV manifest")
            .requiredOption("--data <directory>", "CSV root")
            .requiredOption("--hg-manifest <file>", "Same-scale HG conversion manifest");
    else
        cmd.requiredOption("--index <directory>", "Complete independent adjacency index")
            .option("--seed <n>", "Fixed parameter seed", "3346")
            .option("--pool-size <n>", "Maximum parameters per case", "128")
            .option("--max-visited <n>", "Exact path search work bound; exhaustion fails", "1000000");
    cmd.action(async (options) => {
        const seconds = Number(options.maxSeconds);
        if (!Number.isSafeInteger(seconds) || seconds < 1 || seconds > 604800)
            throw new BenchError("GRAPH_REFERENCE_BUDGET", "max-seconds must be an integer from 1 to 604800");
        const args = [join(packageRoot, "tools/graph-reference.py"), action];
        for (const [name, value] of Object.entries(options)) {
            if (["python", "dryRun"].includes(name)) continue;
            args.push("--" + name.replace(/[A-Z]/g, (c) => "-" + c.toLowerCase()), String(value));
        }
        if (options.dryRun)
            return output("graph.reference." + action, {
                executable: options.python,
                args,
                dispatched: false,
                written: false,
            });
        const abort = new AbortController(),
            stop = () => abort.abort();
        process.on("SIGINT", stop);
        process.on("SIGTERM", stop);
        try {
            const result = await promisify(execFile)(options.python, args, {
                signal: abort.signal,
                timeout: (seconds + 5) * 1000,
                maxBuffer: 1048576,
            });
            output("graph.reference." + action, JSON.parse(result.stdout));
        } finally {
            process.off("SIGINT", stop);
            process.off("SIGTERM", stop);
        }
    });
}
graph
    .command("prepare")
    .argument("<reference>", "Independent graph-query-reference JSON from tools/graph-reference.py")
    .requiredOption("--profile <file>", "Current registered Go HTTP profile; reuses driver/target controls")
    .requiredOption("--graph-url <url>", "Explicit REST graph URL, including graphspace when required")
    .requiredOption("--traversal-source <alias>", "Exact server traversal-source alias, e.g. __g_sf100")
    .requiredOption("--out <file>", "New frozen graph profile; existing output is refused")
    .option("--import-snapshot <file>", "Quiesced import counts and per-label coverage; bound to the report")
    .option(
        "--min-edge-coverage <fraction>",
        "Minimum imported edge fraction, default 0.95 with a snapshot; use 1 for 100%",
    )
    .description("Compile versioned GET/POST requests and independent parameter/reference pools; offline")
    .action(async (reference, options) =>
        output(
            "graph.prepare",
            await (await import("./graph.js")).prepareGraphProfile(reference, options.profile, options),
        ),
    );
for (const action of ["run", "calibrate"] as const)
    graph
        .command(action)
        .argument("<profile>", "Frozen graph profile from graph prepare")
        .requiredOption("--out <directory>", "New attempt; never replay an existing output")
        .option("--preset <name>", "quick or standard; both cover all five cases on the same selected graph", "quick")
        .option("--settings <file>", "Partial scheduling limits; recorded as custom")
        .option("--cache <directory>", "Graph-aware calibration cache, separately keyed from HTTP")
        .option("--refresh-calibration", "Collect new graph-specific representative checks")
        .option("--budget-ms <n>", "Whole run time cap, including preparation and cleanup")
        .option("--budget-mode <mode>", "total (default) or conservative stage-sum", "total")
        .option("--dry-run", "Offline workload/budget preview; no writes or requests")
        .description(
            action === "run"
                ? "Explore concurrency, confirm selected points, validate every response and export SVG reports"
                : "Run/reuse two reference-derived driver checks; no calls to the graph instance",
        )
        .addHelpText(
            "after",
            "\nExample: hgbench graph run graph.json --preset standard --out ./graph-standard --dry-run\nRun against an already imported snapshot bound to the selected dataset. No import, server restart or downscaling occurs.\nCalibration is graph-specific and bounded to its stated representative rates/shapes.\nSIGINT/SIGTERM cancels the original child and retains partial evidence. Contract: docs/graph-query.md\n",
        )
        .action(async (file, options) => {
            if (!["total", "stage-sum"].includes(options.budgetMode))
                throw new BenchError("THROUGHPUT_BUDGET_MODE", "Use total or stage-sum");
            const abort = new AbortController(),
                stop = () => abort.abort();
            process.on("SIGINT", stop);
            process.on("SIGTERM", stop);
            try {
                const result = await (
                    await import("./http-throughput.js")
                ).runHttpThroughput(file, {
                    ...options,
                    root: root(),
                    track: "graph",
                    signal: abort.signal,
                    budgetMs: options.budgetMs === undefined ? undefined : Number(options.budgetMs),
                    calibrationOnly: action === "calibrate",
                });
                output("graph." + action, result);
                if ("status" in result && result.status !== "completed") process.exitCode = 6;
            } finally {
                process.off("SIGINT", stop);
                process.off("SIGTERM", stop);
            }
        });
graph
    .command("report")
    .argument("<directory>", "Original graph attempt")
    .requiredOption("--out <directory>", "New offline JSON/HTML/CSV/SVG output")
    .option(
        "--target-diagnostics <file>",
        "Attach digest-checked GC/swap logs as approximate wall-clock diagnostics; no qualification change",
    )
    .description("Reverify dataset, pools and raw semantic projections; no target calls")
    .addHelpText(
        "after",
        "\nExample: hgbench graph report ./standard --out ./new-report --target-diagnostics ./observations/manifest.json\nThe manifest binds state.json planSha256 and relative raw files by bytes/SHA-256. Contract: docs/graph-query.md\nNo target calls, collector launch, JVM changes or point exclusions. Missing coverage remains unknown.\n",
    )
    .action(async (directory, options) =>
        output(
            "graph.report",
            await (
                await import(join(packageRoot, "components/http/throughput-report.mjs"))
            ).reportThroughput(resolve(directory), resolve(options.out), {
                targetDiagnostics: options.targetDiagnostics && resolve(options.targetDiagnostics),
            }),
        ),
    );

const preset = httpCommand
    .command("preset")
    .description("Versioned quick/standard/full designs; quick compiles to the existing HTTP comparison lifecycle")
    .addHelpText(
        "after",
        "\nquick is the implemented screener/descriptive plan; real runtime calibration is pending.\nstandard/full are design-only and fail explicitly when selected for planning.\nSave one project JSON with build/settings/environment/runtime references; resources are reused, never silently reduced.\nNormal ETA is unknown. The 5–10 minute engineering goal and the lifecycle safety timeout are separate.\nAfter plan: existing http comparison prepare -> task run/status -> http preset report.\nContract: docs/http-presets.md\n",
    );
preset
    .command("list")
    .description("List the three versioned designs and their implementation/validation status; offline")
    .action(async () => output("http.preset.list", await listHttpPresets()));
preset
    .command("show")
    .argument("[id]", "quick (default), standard or full", "quick")
    .description("Show exact parameters, scope and limitations; no execution")
    .action(async (id) => output("http.preset.show", await showHttpPreset(id)));
preset
    .command("plan")
    .requiredOption("--project <file>", "Once-saved http-project JSON; references are relative to that file")
    .option("--preset <id>", "Versioned preset; standard/full are currently design-only", "quick")
    .requiredOption("--out <directory>", "New frozen preset plan directory with an existing parent")
    .option("--dry-run", "Preview through bounded temporary planning; remove temporary files, leave --out untouched")
    .option(
        "--budget-ms <ms>",
        "Whole-run limit in total mode; includes startup, measurement and cleanup; incomplete work remains partial",
    )
    .option(
        "--budget-mode <mode>",
        "total (default): shared time limit; stage-sum: require all worst-case stage allowances",
        "total",
    )
    .description("Expand saved project + quick into ordinary HTTP/comparison plans; sends no HTTP or Docker requests")
    .addHelpText(
        "after",
        "\nExample: hgbench http preset plan --project examples/http-project.json --out /absolute/new-quick --dry-run\nPreview reports fixed work, variable overhead, low-confidence history when available, and the selected hard deadline separately.\ntotal shares one run deadline across startup, measurement and cleanup; stage-sum also requires all worst-case stage allowances. Planning/prepare and offline report have their own limits. No automatic case/pair/rate/sample changes.\nSuccessful planning does not validate the target runtime or qualify statistics/performance.\n",
    )
    .action(async (options) => {
        const result = await planHttpPreset(options.project, {
            preset: options.preset,
            out: options.out,
            dryRun: Boolean(options.dryRun),
            ...(options.budgetMs === undefined ? {} : { budgetMs: Number(options.budgetMs) }),
            budgetMode: options.budgetMode,
        });
        output("http.preset.plan", result, "nextActions" in result ? result.nextActions : []);
    });
preset
    .command("verify")
    .argument("<directory>", "Original frozen preset plan")
    .description("Verify preset/content binding and the existing comparison plan; offline")
    .action(async (directory) => output("http.preset.verify", await verifyHttpPresetPlan(directory)));
preset
    .command("report")
    .argument("<execution>", "Original sealed comparison execution")
    .requiredOption("--plan <directory>", "Original frozen preset plan")
    .requiredOption("--out <directory>", "New report directory")
    .description(
        "Reuse the existing raw-evidence report and add upfront preset scope/limitations; no new statistical engine",
    )
    .addHelpText(
        "after",
        "\nEffects: offline evidence reads, bounded existing analyzer and a new HTML/JSON wrapper.\nAll missing/failed arms, sample minima and original quality gates remain visible.\nquick stays screener/descriptive; reliable P99, fine performance wins and acceptance are not claimed.\n",
    )
    .action(async (execution, options) =>
        output("http.preset.report", await reportHttpPreset(execution, options.plan, options.out)),
    );
const capacity = httpCommand
    .command("capacity")
    .description("Freeze a bounded all-eleven-question throughput ladder using existing comparison tasks")
    .addHelpText(
        "after",
        "\nWorkflow: plan → verify → prepare → task run → report → confirmation plan → prepare → task run → confirmation report.\nplan/verify/report are offline. prepare freezes the existing task lifecycle without starting a workload.\nRunning the returned task uses Docker and sends real target requests. stop affects only the original attempt.\nRecommended policy v2 freezes one single observation with a fresh JVM for each question, mode and arrival rate; legacy v1 retains paired A/A exploration.\nThe plan includes every point, explicit stop rules, confirmation requirements and resource budgets.\nNormal ETA is unknown; fixed measurement time and lifecycle safety limits are reported separately.\nNo SLO is invented. Engineering acceptance and empirical false-positive certification are separate.\nPlanning does not qualify throughput, driver headroom, steady state or any old experiment.\nContract: docs/http-capacity-execution.md\n",
    );
capacity
    .command("describe")
    .description("Show planning and finite executor capabilities; does not dispatch")
    .action(async () =>
        output("http.capacity.describe", {
            ...describeHttpCapacity(),
            executor: await httpCapacityExecution("describe"),
        }),
    );
capacity
    .command("plan")
    .requiredOption("--project <file>", "Saved http-project JSON; preserves its target and driver resources")
    .requiredOption("--policy <file>", "Strict http-capacity-policy JSON with rates, windows and complete budgets")
    .requiredOption("--out <directory>", "New frozen study directory with an existing parent")
    .option("--dry-run", "Preview the unchanged study offline; leave --out untouched")
    .description("Freeze all eleven questions across the declared modes and ordered rates; does not dispatch tasks")
    .addHelpText(
        "after",
        "\nExample: hgbench http capacity plan --project examples/http-project.json --policy examples/http-capacity-policy-v2.json --out /absolute/new-capacity --dry-run\nPolicy v2 runs each case/mode/rate once; four-pair independent confirmation is separate.\nInspect fixed work, variable overhead, total deadline and evidence cap before creating the plan.\nSet budgetMode=total (default) for a shared run deadline or stage-sum for conservative stage reservations.\nOptional continuation.mode=verified-case-range preregisters conservative per-case stopping after independent verification and owned-container absence checks.\nDeclare its verification time/byte allowances; the preview reserves the additional work for every point.\nInsufficient budgets fail explicitly; questions, rates, repetitions and windows are never reduced automatically.\nIndependent confirmation needs a new plan after exploration; explored points alone do not establish a confirmed maximum.\nDiscover the policy with: hgbench schema http-capacity-policy\n",
    )
    .action(async (options) => {
        const result = await planHttpCapacity(options.project, options.policy, {
            out: options.out,
            dryRun: Boolean(options.dryRun),
        });
        output("http.capacity.plan", result, "nextActions" in result ? result.nextActions : []);
    });
capacity
    .command("verify")
    .argument("<directory>", "Original frozen capacity study plan")
    .description("Verify source snapshots, ordered point semantics, budgets and every child comparison; offline")
    .action(async (directory) => output("http.capacity.verify", await verifyHttpCapacityPlan(directory)));
capacity
    .command("prepare")
    .argument("<runtime>", "Strict http-capacity-execution-runtime JSON with explicit lifecycle and storage limits")
    .requiredOption("--plan <directory>", "Frozen exploration plan or nonempty independent confirmation plan")
    .requiredOption("--out <directory>", "New attempt directory outside the immutable source and plan")
    .description("Freeze one finite exploration or confirmation task using the existing comparison lifecycle")
    .addHelpText(
        "after",
        "\nEffects: verify local assets and write a new attempt/task plan; no target requests.\nThe saved comparison runtime is inherited exactly. Deploy and freeze the implementation before preparing.\nConfirmation requires eligible children and explicit source-review time within prepare and controller budgets.\nRun the returned taskPlan with hgbench task run --plan FILE --request-id UNIQUE.\nThe attempt submits each point at most once and preserves all planned points when stopped.\nExploration without an explicit continuation policy, and every confirmation study, stops on any failed or invalid point.\nWith verified-case-range, a proven allowed range symptom stops later points of that question; other questions continue after owned cleanup is proven.\nWrong answers, unknown symptoms, identity ambiguity, drift and unproven cleanup stop the study. Target-capacity upper bounds remain unknown.\nCancellation has a bounded 20-second cleanup window under the existing 30-second task termination grace.\nIncomplete cleanup remains unproven and requires inspection/stop of the original identities.\nUse status or stop after disconnection; do not submit the same attempt again.\nDiscover the contract with: hgbench schema http-capacity-execution-runtime\n",
    )
    .action(async (runtime, options) =>
        output("http.capacity.prepare", await prepareHttpCapacityExecution(runtime, options.plan, options.out)),
    );
capacity
    .command("status")
    .argument("<attempt>", "Original prepared capacity attempt")
    .description("Inspect the original controller and point identity without resubmission")
    .action(async (attempt) => output("http.capacity.status", await httpCapacityExecution("status", attempt)));
capacity
    .command("stop")
    .argument("<attempt>", "Original prepared capacity attempt")
    .description("Record stop and cancel only the original owned point; preserve partial evidence")
    .addHelpText(
        "after",
        "\nEffects: writes the original stop marker and coordinates cancellation with its original task/comparison.\nNo point is replayed. A stop acknowledgement does not prove owned containers are absent.\n",
    )
    .action(async (attempt) => output("http.capacity.stop", await httpCapacityExecution("stop", attempt)));
capacity
    .command("windows")
    .argument("<raw>", "Original sealed HTTP raw directory; no requests are replayed")
    .requiredOption("--out <directory>", "New offline report directory")
    .requiredOption("--window-ms <ms>", "Explicit completion window width in milliseconds")
    .requiredOption("--minimum-latency-observations <count>", "Explicit per-window P50/P95 sample minimum")
    .description("Recompute completed-request throughput, latency and in-flight counts from locked traces")
    .addHelpText(
        "after",
        "\nEffects: bounded offline raw verification and a new JSON report with a content lock.\nCompleted throughput uses endMs, while arrivals use scheduledMs; these are separate cohorts.\nThe last measurement window includes endMs equal to the measurement deadline. Drain completions remain separate.\nHTTP failures, timeouts, unsent arrivals and original quality issues remain visible.\nP99 requires max(1000, the explicit latency sample minimum) observations in each window.\nThis diagnostic does not establish steady state, driver headroom or performance acceptance.\nContract: docs/http-completion-windows.md\n",
    )
    .action(async (raw, options) =>
        output(
            "http.capacity.windows",
            await reportHttpCompletionWindows(raw, options.out, {
                windowMs: Number(options.windowMs),
                minimumLatencyObservations: Number(options.minimumLatencyObservations),
            }),
        ),
    );
capacity
    .command("report")
    .argument("<execution-set>", "Original execution-set.json, including all planned point identities")
    .requiredOption("--plan <directory>", "Original frozen capacity plan")
    .requiredOption("--out <directory>", "New offline JSON/HTML report directory")
    .requiredOption(
        "--timeout-ms <milliseconds>",
        "Component report deadline, 1000..86400000 ms; outer supervision/cleanup may add at most 15 s",
    )
    .requiredOption("--max-bytes <bytes>", "Explicit report output cap, 1 MiB..8 GiB; separate from raw input bounds")
    .description("Reverify original point evidence and render throughput–latency curves offline")
    .addHelpText(
        "after",
        "\nEffects: bounded offline evidence analysis and new report files; no Docker or target requests.\nEvery planned question, rate and arm remains visible, including missing, failed and invalid evidence.\nPoint throughput describes independent arm medians and ranges; P99 stays unknown below 1000 correct observations.\nExploration alone leaves highest observed throughput unconfirmed. Independent confirmation requires a new plan.\nA predeclared optional SLO is inherited from the original policy; no business SLO is invented.\nReport completion does not establish steady state, driver headroom, observer cost or performance acceptance.\n",
    )
    .action(async (input, options) =>
        output(
            "http.capacity.report",
            await reportHttpCapacity(input, options.plan, options.out, {
                timeoutMs: Number(options.timeoutMs),
                maxBytes: Number(options.maxBytes),
            }),
        ),
    );
const capacityConfirmation = capacity
    .command("confirmation")
    .description("Reverify exploration evidence and freeze fresh independent confirmation conditions")
    .addHelpText(
        "after",
        "\nWorkflow: exploration report → confirmation plan → verify → capacity prepare → task run → confirmation report.\nPlanning, verification and reporting are offline and send no target requests. A plan does not execute or confirm throughput.\nSelection independently rebuilds the original report, retaining every question and unavailable candidate.\nEach selected condition needs four new pairs; original workload, build, measurement policy and resources are inherited.\nUse capacity prepare on a nonempty confirmation plan to reuse the same finite task lifecycle.\nContract: docs/http-capacity-confirmation.md\n",
    );
capacityConfirmation
    .command("plan")
    .argument("<request>", "Strict http-capacity-confirmation-request JSON with original evidence and explicit budgets")
    .requiredOption("--out <directory>", "New confirmation plan directory with an existing parent")
    .option("--dry-run", "Reverify and preview offline; leave --out untouched")
    .description("Rebuild source evidence, select candidates and safe immediate neighbors, then freeze fresh plans")
    .addHelpText(
        "after",
        "\nEffects: bounded offline analysis and new files; dry-run uses temporary analysis files only.\nAll eleven questions remain represented. Missing or invalid candidates produce explicit unknown rows.\nNo candidate yields no child plans and no executable task. The result never claims confirmed capacity.\nBudget and immutable-condition violations fail explicitly; rates, arms or sample windows are not silently reduced.\nDiscover the request with: hgbench schema http-capacity-confirmation-request\n",
    )
    .action(async (request, options) => {
        const result = await planHttpCapacityConfirmation(request, {
            out: options.out,
            dryRun: Boolean(options.dryRun),
        });
        output("http.capacity.confirmation.plan", result, "nextActions" in result ? result.nextActions : []);
    });
capacityConfirmation
    .command("verify")
    .argument("<directory>", "Saved confirmation plan with its original source references")
    .description("Rebuild source selection, verify content and child semantics, and preserve unknown qualification")
    .action(async (directory) =>
        output("http.capacity.confirmation.verify", await verifyHttpCapacityConfirmationPlan(directory)),
    );
capacityConfirmation
    .command("report")
    .argument("<execution-set>", "Original confirmation execution-set.json with its frozen plan digest")
    .requiredOption("--plan <directory>", "Frozen independent confirmation plan with accessible exploration references")
    .requiredOption("--out <directory>", "New offline confirmation JSON/HTML report directory")
    .requiredOption(
        "--timeout-ms <milliseconds>",
        "Analysis deadline covering source replay and confirmation evidence; supervision/cleanup may add at most 20 s",
    )
    .requiredOption(
        "--max-bytes <bytes>",
        "Explicit report work cap including source-replay temporary output, 1 MiB..8 GiB",
    )
    .description("Reverify new confirmation arms and report highest observed correct throughput by question and mode")
    .addHelpText(
        "after",
        "\nEffects: bounded offline verification and new files; no workload requests.\nAll question/mode selections remain visible, even with no candidate or no executed confirmation arm.\nScores use eligible new confirmation arms only; exploration results never fill failed confirmation.\nSame-mode A/A uses all eight arms; forward/embedded arms are grouped by their actual mode.\nMissing independence, evidence or quality keeps confirmation unknown. No SLO capacity or overall engineering acceptance is inferred.\n",
    )
    .action(async (input, options) =>
        output(
            "http.capacity.confirmation.report",
            await reportHttpCapacityConfirmation(input, options.plan, options.out, {
                timeoutMs: Number(options.timeoutMs),
                maxBytes: Number(options.maxBytes),
            }),
        ),
    );
const runtimeProvenance = httpCommand
    .command("runtime")
    .description("Independently probe fixed Node and Python runtimes before freezing experiment expectations")
    .addHelpText(
        "after",
        "\nprobe runs three short bounded processes and one isolated Docker container on the Linux host.\nAll executable paths and the immutable image come from an existing comparison runtime lock.\nSupply every resource and evidence budget explicitly; comparison prepare remains offline.\nverify rebuilds from the original proof offline. stop cleans only this probe's exact owned processes/container.\nNever replay an existing proof directory. A successful probe does not qualify performance or prior runs.\nContract: docs/http-runtime-provenance.md\n",
    );
runtimeProvenance
    .command("describe")
    .description("Read the strict runtime provenance spec, budgets and evidence contract")
    .action(async () => output("http.runtime.describe", await httpRuntimeAction("describe")));
runtimeProvenance
    .command("probe")
    .argument("<spec>", "Strict http-runtime-provenance-spec JSON; every budget is explicit")
    .requiredOption("--runtime <file>", "Existing comparison attempt runtime.json with its adjacent lock")
    .requiredOption("--out <directory>", "New proof directory on this Docker host")
    .description("Run a new bounded source/identity probe and seal independently observed runtime expectations")
    .action(async (spec, options) => {
        const result = await probeHttpRuntime(spec, options.runtime, options.out);
        output("http.runtime.probe", result, [
            { kind: "read", argv: ["hgbench", "http", "runtime", "verify", resolve(options.out)] },
        ]);
    });
for (const action of ["verify", "stop"] as const)
    runtimeProvenance
        .command(action)
        .argument("<proof>", "Original runtime proof directory; never creates a replacement probe")
        .description(
            action === "verify"
                ? "Recompute raw runtime identities and fixed source bindings offline"
                : "Request precise cleanup of this runtime probe and preserve its original evidence",
        )
        .action(async (proof) => output(`http.runtime.${action}`, await httpRuntimeAction(action, proof)));
const environment = httpCommand
    .command("environment")
    .description("Control one isolated HugeGraph Docker arm on the current host; use task lifecycle to serve")
    .addHelpText(
        "after",
        "\nRun on the Linux Docker host. plan is offline and freezes JDK/distribution/config; no container is started.\nUse task run --plan <attempt>/task.json --detach --request-id <unique-id> to serve.\nReconnect with environment status and the same task ID. Never replay a dispatched attempt.\nContainer TTL/log/disk guards remain active if SSH or the outer task disappears.\nMode settings are claims; an independent mode probe is still required outside measured windows.\n",
    );
environment
    .command("plan")
    .argument("<settings>", "JSON matching schema http-environment-settings; all budgets explicit")
    .requiredOption("--out <directory>", "New attempt directory; parent must exist")
    .description("Freeze an empty RocksDB environment and generate a generic task; does not access Docker")
    .action(async (settings, options) => {
        const result = await planHttpEnvironment(settings, options.out);
        output("http.environment.plan", result, [
            {
                kind: "execute",
                argv: ["hgbench", "--root", root(), "task", "run", "--plan", result.taskPlan, "--detach"],
            },
        ]);
    });
for (const action of ["status", "stop"] as const)
    environment
        .command(action)
        .argument("<attempt>", "Existing attempt directory on this Docker host")
        .description(
            action === "status"
                ? "Read the exact owned container and service state"
                : "Stop only this attempt's owned container; preserve all evidence and never restart it",
        )
        .action(async (attempt) => output(`http.environment.${action}`, await httpEnvironmentAction(action, attempt)));
const comparison = httpCommand
    .command("comparison")
    .description("Plan, prepare and inspect controlled balanced A/A or forward/embedded experiments")
    .addHelpText(
        "after",
        "\nPlans bind the complete question/load family, same-build settings, ordered arms and explicit budgets.\nplan/prepare are offline; task run executes the prepared task on the Linux Docker host.\nReconnect using the same task ID and attempt; never replay a dispatched attempt.\nreport rebuilds from sealed raw evidence. Exit 0 is not performance acceptance.\nContracts: docs/http-comparison-execution.md and docs/http-comparison-run.md\n",
    );
comparison
    .command("describe")
    .option("--execution", "Read runtime, collector and lifecycle fields for prepare/run")
    .description("Read planner or execution capabilities and required input fields as JSON")
    .action(async (options) =>
        output(
            "http.comparison.describe",
            options.execution
                ? await httpComparisonWorkflow("describe")
                : await httpComparisonPlan(["--describe"], true),
        ),
    );
comparison
    .command("plan")
    .argument("<request>", "Strict http-comparison-request JSON with all experimental choices explicit")
    .requiredOption("--http-plan <directory>", "Existing versioned HTTP plan; repeats=1 and serviceChecks=true")
    .requiredOption("--out <directory>", "New comparison directory; parent must exist")
    .description("Freeze schedule and paired policy offline; performs no measurements")
    .action(async (request, options) => {
        const result = await freezeHttpComparison(request, options.httpPlan, options.out);
        output("http.comparison.plan", result, [
            { kind: "read", argv: ["hgbench", "http", "comparison", "verify", resolve(options.out)] },
        ]);
    });
comparison
    .command("verify")
    .argument("<directory>", "Frozen comparison directory")
    .description("Recompute content, schedule and policy identities offline; does not prove prior execution")
    .action(async (directory) =>
        output("http.comparison.verify", await httpComparisonPlan(["--verify", resolve(directory)])),
    );
comparison
    .command("prepare")
    .argument("<runtime>", "Strict http-comparison-runtime JSON; paths and all resource budgets explicit")
    .requiredOption("--plan <directory>", "Frozen comparison schedule on this Linux Docker host")
    .requiredOption("--out <directory>", "New attempt directory outside frozen source/runtime trees")
    .description("Freeze code, runtimes and resources offline; generate a generic task without starting containers")
    .action(async (runtime, options) => {
        const result = await prepareHttpComparison(runtime, options.plan, options.out);
        output("http.comparison.prepare", result, [
            {
                kind: "execute",
                argv: ["hgbench", "--root", root(), "task", "run", "--plan", result.taskPlan, "--detach"],
            },
        ]);
    });
for (const action of ["status", "stop"] as const)
    comparison
        .command(action)
        .argument("<attempt>", "Existing prepared comparison attempt on the same host")
        .description(
            action === "status"
                ? "Read recorded execution state; use task status for live process state"
                : "Persist cancellation and stop exact owned containers; preserve all evidence",
        )
        .action(async (attempt) => output(`http.comparison.${action}`, await httpComparisonWorkflow(action, attempt)));
comparison
    .command("report")
    .argument("<execution>", "Sealed execution directory containing raw evidence and receipts")
    .requiredOption("--plan <directory>", "Frozen prospective comparison plan")
    .requiredOption("--out <directory>", "New output directory for reconstructed JSON and offline HTML")
    .description("Independently verify raw evidence and rebuild a comparison; exit 0 can still report valid=false")
    .addHelpText(
        "after",
        "\nInspect valid, statisticsEligible and reasons. Functional runs and uncalibrated results have performanceAccepted=false.\nNo HTTP, Docker, retries, sample replacement or prior-execution inference.\n",
    )
    .action(async (execution, options) =>
        output("http.comparison.report", await reportHttpComparison(execution, options.plan, options.out)),
    );
const generator = httpCommand
    .command("generator")
    .description("Calibrate the HTTP generator against frozen correct responses; no HugeGraph capacity claims")
    .addHelpText(
        "after",
        "\nWorkflow: describe → plan --dry-run → plan → verify → task run → task status/wait/cancel → report.\nAll eleven questions, identity/gzip, balanced 4/6/6/4 CPU arms. One dispatch per new plan.\nDocker resources are applied and observed. Failed and unfinished cells remain visible.\nContract: docs/http-go-driver-calibration-v2-proposal.md\n",
    );
generator
    .command("describe")
    .description("Print strict request JSON schema, fixed scope and deadline semantics; offline")
    .action(() => output("http.generator.describe", describeHttpGenerator()));
generator
    .command("plan")
    .argument("<request>", "Strict http-generator-calibration-request JSON; use describe")
    .requiredOption("--out <directory>", "New plan/assets directory; parent must exist")
    .option("--dry-run", "Validate and preview budgets without writes or network")
    .description("Freeze all eleven cases, precomputed response bytes, source identities and the finite task")
    .action(async (request, options) => {
        const result = await planHttpGenerator(request, options.out, Boolean(options.dryRun));
        output(
            "http.generator.plan",
            result,
            result.written && "taskPlan" in result
                ? [
                      { kind: "read", argv: ["hgbench", "http", "generator", "verify", resolve(options.out)] },
                      { kind: "execute", argv: ["hgbench", "task", "run", "--plan", result.taskPlan, "--detach"] },
                  ]
                : [],
        );
    });
generator
    .command("verify")
    .argument("<directory>", "Frozen generator plan directory")
    .description("Verify plan, full asset/reference binding and source hashes offline; no target requests")
    .action(async (directory) => output("http.generator.verify", await generatorComponent("verify", directory)));
generator
    .command("report")
    .argument("<directory>", "Executed generator plan with original output")
    .requiredOption("--out <directory>", "New JSON/HTML report directory")
    .description("Independently rebuild quality, identity, resources and per-case engineering gates offline")
    .action(async (directory, options) =>
        output("http.generator.report", await generatorComponent("report", directory, options.out)),
    );

const calibration = httpCommand
    .command("calibration")
    .description("Freeze independent A/A experiments and assess repeatability from sealed raw evidence")
    .addHelpText(
        "after",
        "\nFirst scenario: repeatability-aa. Explicit seeds, budgets and assessment policy are required.\nplan freezes children; prepare freezes a sequential cohort task; task run executes it explicitly.\nstatus/stop inspect the same cohort after disconnects. No automatic retries or reruns.\nreport independently rebuilds raw evidence; missing/invalid experiments remain visible.\nwindows rebuilds offline warmup/measurement arrival trends, rejects damaged raw and preserves low-sample unknowns.\nA completed A/A experiment does not establish a false-positive rate or performance acceptance.\nUse describe for required fields; docs/http-calibration.md explains qualification limits.\n",
    );
calibration
    .command("describe")
    .description("Read calibration capabilities, strict request fields and budgets as JSON; offline")
    .action(async () => output("http.calibration.describe", await describeHttpCalibration()));
calibration
    .command("plan")
    .argument("<request>", "Strict http-calibration-request JSON; no experimental defaults")
    .requiredOption("--http-plan <directory>", "Existing frozen HTTP workload for all A/A experiments")
    .requiredOption("--out <directory>", "New calibration directory; existing output is never overwritten")
    .description("Freeze independent same-build/same-mode A/A child plans; sends no requests")
    .action(async (request, options) => {
        const result = await freezeHttpCalibration(request, options.httpPlan, options.out);
        output("http.calibration.plan", result, [
            { kind: "read", argv: ["hgbench", "http", "calibration", "verify", resolve(options.out)] },
        ]);
    });
calibration
    .command("verify")
    .argument("<directory>", "Frozen calibration directory")
    .description("Recompute calibration and child plan identities; does not prove execution")
    .action(async (directory) => output("http.calibration.verify", await verifyHttpCalibration(directory)));
calibration
    .command("prepare")
    .argument(
        "<runtime>",
        "Strict http-calibration-runtime JSON with existing comparisonRuntime and clean-window units",
    )
    .requiredOption("--plan <directory>", "Frozen predeclared calibration plan")
    .requiredOption("--out <directory>", "New cohort attempt; existing output is never reused")
    .description("Offline preparation: freeze child comparison tasks and one sequential cohort task")
    .addHelpText(
        "after",
        "\nEffects: bounded local verification and new task files; prepare sends no HTTP or Docker requests.\nThe returned task performs real Linux/Docker work only after explicit task run.\nBefore every experiment it records all declared systemd user service states and refuses unknown/active states.\nThese observations cover declared units only; whole-window exclusivity and qualification remain unproven.\nAfter interruption use status/stop on the same absolute attempt. Never dispatch a new cohort as recovery.\n",
    )
    .action(async (runtime, options) => {
        const result = await prepareHttpCalibration(runtime, options.plan, options.out);
        output("http.calibration.prepare", result, [
            {
                kind: "execute",
                effect: "database",
                argv: [
                    "hgbench",
                    "--root",
                    root(),
                    "task",
                    "run",
                    "--plan",
                    result.taskPlan,
                    "--detach",
                    "--request-id",
                    `calibration-${result.planSha256}`,
                ],
            },
            { kind: "read", argv: ["hgbench", "http", "calibration", "status", resolve(options.out)] },
        ]);
    });
for (const action of ["status", "stop"] as const)
    calibration
        .command(action)
        .argument("<attempt>", "Original absolute cohort attempt directory")
        .description(
            action === "status"
                ? "Read the same cohort and original child task; never dispatch"
                : "Publish durable stop and stop/cancel the exact original active comparison task",
        )
        .action(async (attempt) =>
            output(`http.calibration.${action}`, await httpCalibrationWorkflow(action, attempt)),
        );
calibration
    .command("report")
    .argument("<execution-set>", "Explicit execution-set JSON mapping planned experiments to sealed directories")
    .requiredOption("--plan <directory>", "Frozen prospective calibration plan")
    .requiredOption("--out <directory>", "New report directory; partial results are preserved on failure")
    .description("Independently rebuild A/A evidence and report effects, unknowns and family error intervals")
    .addHelpText(
        "after",
        "\nEffects: offline reads, bounded analysis subprocesses and new report files; no Docker or HTTP.\nUses the declared report deadline plus 10 seconds of outer supervision and up to 5 seconds of cleanup.\nInspect evidence validity, complete experiment counts and qualification status; exit 0 is not qualification.\n",
    )
    .action(async (input, options) =>
        output("http.calibration.report", await reportHttpCalibration(input, options.plan, options.out)),
    );
const phaseWindowInteger = (name: string, minimum: number, maximum: number) => (value: string) => {
    const parsed = Number(value);
    if (!/^[1-9][0-9]*$/.test(value) || !Number.isSafeInteger(parsed) || parsed < minimum || parsed > maximum)
        throw new BenchError("CLI_USAGE", `${name} must be an integer between ${minimum} and ${maximum}`);
    return parsed;
};
calibration
    .command("acceptance")
    .argument("<input>", "Original calibration evidence references; paths relative to this JSON")
    .requiredOption("--out <directory>", "New JSON/HTML acceptance report and calibration evidence index")
    .requiredOption(
        "--timeout-ms <ms>",
        "Shared deadline for all evidence readers, partial output and cleanup",
        phaseWindowInteger("timeout-ms", 1000, 86400000),
    )
    .requiredOption(
        "--max-bytes <bytes>",
        "Shared new output and temporary-file budget, 1 MiB to 8 GiB",
        phaseWindowInteger("max-bytes", 1024 ** 2, 8 * 1024 ** 3),
    )
    .description("Combine independently verified calibration contributions with explicit scope and unknowns; offline")
    .addHelpText(
        "after",
        "\nEffects: bounded local reads, analysis workers and new report files; no HTTP, Docker or SSH.\n" +
            "Rebuilds known/headroom/observer contributions with existing raw readers.\n" +
            "repeatabilityAa reuses the current cohort raw verifier; reserve its original report time/byte budgets.\n" +
            "AA005 verifies its previously independently replayed, immutable evidence chain; it does not rerun the experiment.\n" +
            "Choose at most one A/A source; neither can replace unknown results in the other.\n" +
            "All seven engineering gates and eleven cases remain visible. Missing evidence stays unknown.\n" +
            "Three-case or low-rate results do not cover other cases, modes, or higher rates.\n" +
            "An optional comparison plan must bind the calibration index and retain parameter mismatches.\n" +
            "No operator passed flags or threshold overrides. Exit 0 means report creation, not acceptance.\n" +
            "Performance and qualification acceptance remain false; optional statistical certification is separate.\n" +
            "The CLI adds up to 5 seconds supervision and 5 seconds forced cleanup outside the component deadline.\n" +
            "Discover the input: hgbench schema http-engineering-acceptance-input\n" +
            "Contract: docs/http-engineering-acceptance-report.md\n",
    )
    .action(async (input, options) =>
        output(
            "http.calibration.acceptance",
            await reportHttpEngineeringAcceptance(input, options.out, {
                timeoutMs: options.timeoutMs,
                maxBytes: options.maxBytes,
            }),
        ),
    );
calibration
    .command("known-regression")
    .argument(
        "<input>",
        "Known002 original plan/execution directories and predeclared SHA references; paths relative to this JSON",
    )
    .requiredOption("--out <directory>", "New offline evidence report; partial evidence is preserved")
    .requiredOption(
        "--timeout-ms <ms>",
        "Total component deadline, including partial report cleanup",
        phaseWindowInteger("timeout-ms", 1000, 86400000),
    )
    .requiredOption(
        "--max-bytes <bytes>",
        "Output byte cap, 1 MiB to 8 GiB; combined input inventory has a fixed 8 GiB cap",
        phaseWindowInteger("max-bytes", 1024 ** 2, 8 * 1024 ** 3),
    )
    .description("Independently verify the fixed sleep10 positive control from original evidence; offline")
    .addHelpText(
        "after",
        "\nEffects: bounded local reads and a new report; no HTTP, Docker, or execution of saved commands.\n" +
            "Rebuilds all 16 arms / 8 pairs; E2E P99 uses the original 5% and 5ms thresholds.\n" +
            "Missing, invalid and insufficient observations remain unknown; saved passed flags are not evidence.\n" +
            "Scope: forward arithmetic control with observers off; no all-eleven, 1ms or throughput-capacity claim.\n" +
            "Exit 0 means report creation; inspect status and verification before using the scoped gate.\n" +
            "The CLI adds up to 5 seconds supervision and 5 seconds forced cleanup outside the component deadline.\n" +
            "Discover the contract: hgbench schema http-known-regression-evidence-input\n" +
            "Contract and limitations: docs/http-known-regression-evidence.md\n",
    )
    .action(async (input, options) =>
        output(
            "http.calibration.known-regression",
            await reportHttpKnownRegression(input, options.out, {
                timeoutMs: options.timeoutMs,
                maxBytes: options.maxBytes,
            }),
        ),
    );
calibration
    .command("driver-headroom")
    .argument(
        "<input>",
        "Headroom002 original evidence and prospective assessment/launch bindings; relative to this JSON",
    )
    .requiredOption("--out <directory>", "New offline evidence report directory; never overwrite")
    .requiredOption(
        "--timeout-ms <ms>",
        "Total component deadline, including worker cleanup and seal",
        phaseWindowInteger("timeout-ms", 1000, 86400000),
    )
    .requiredOption(
        "--max-bytes <bytes>",
        "Output byte cap, 1 MiB to 8 GiB; combined input inventory has a fixed 16 GiB cap",
        phaseWindowInteger("max-bytes", 1024 ** 2, 8 * 1024 ** 3),
    )
    .description("Verify three-case driver headroom and warmup transition against predeclared engineering criteria")
    .addHelpText(
        "after",
        "\nEffects: bounded offline reads/analysis and new report files; no HTTP, Docker, or saved command execution.\n" +
            "Scope: 3 cases x 3 rates (10/20/40) x 2 CPU quotas x 2 blocks = 36 cells / 12 JVM arms.\n" +
            "The assessment and its actual plan/spec binding must be recorded before dispatch.\n" +
            "Missing, invalid and insufficient evidence remains unknown; private passed flags are not proof.\n" +
            "P99 CPU/repeat tolerance and P50/P95 warmup transition are engineering observations.\n" +
            "Whole-measurement steady state, full-eleven coverage, 80 rps, capacity and statistical certification remain unassessed.\n" +
            "Exit 0 means report creation. Inspect status and limitedScopeEngineeringAccepted; global performance remains false.\n" +
            "The CLI adds up to 5 seconds supervision and 5 seconds forced cleanup outside the component deadline.\n" +
            "Discover the contract: hgbench schema http-driver-headroom-evidence-input\n" +
            "Contract and limitations: docs/http-headroom-evidence.md\n",
    )
    .action(async (input, options) =>
        output(
            "http.calibration.driver-headroom",
            await reportHttpDriverHeadroom(input, options.out, {
                timeoutMs: options.timeoutMs,
                maxBytes: options.maxBytes,
            }),
        ),
    );
calibration
    .command("observer-cost")
    .argument("<input>", "Observer003 original plan/execution and SHA references; paths relative to this JSON")
    .requiredOption("--out <directory>", "New offline evidence report directory; never overwrite")
    .requiredOption(
        "--timeout-ms <ms>",
        "Total component deadline, including worker cleanup and seal",
        phaseWindowInteger("timeout-ms", 1000, 86400000),
    )
    .requiredOption(
        "--max-bytes <bytes>",
        "Output byte cap, 1 MiB to 8 GiB; combined input inventory has a fixed 32 GiB cap",
        phaseWindowInteger("max-bytes", 1024 ** 2, 8 * 1024 ** 3),
    )
    .description("Independently measure and disclose three-case JVM and collector observation costs")
    .addHelpText(
        "after",
        "\nEffects: bounded offline reads/analysis and new report files; no HTTP, Docker, or saved command execution.\n" +
            "Scope: 3 cases, 16 JVM arms; JVM and collector each have four fresh pairs and six fixed metrics.\n" +
            "The other eight cases remain unknown. Validator and trace stay on; their counterfactual cost is unknown.\n" +
            "The two conditional effects must not be added. An observed cost is disclosed, not hidden by a pass flag.\n" +
            "Missing, invalid, low-sample or unusable inference remains unknown; full planned counts are retained.\n" +
            "Exit 0 means report creation; scopedCostMeasured only accepts valid measurement/disclosure, not negligible cost.\n" +
            "Global performance and inference acceptance remain false. The CLI adds up to 5 seconds supervision\n" +
            "and 5 seconds forced cleanup outside the component deadline.\n" +
            "Discover the contract: hgbench schema http-observer-cost-evidence-input\n" +
            "Contract and limitations: docs/http-observer-cost-evidence.md\n",
    )
    .action(async (input, options) =>
        output(
            "http.calibration.observer-cost",
            await reportHttpObserverCost(input, options.out, {
                timeoutMs: options.timeoutMs,
                maxBytes: options.maxBytes,
            }),
        ),
    );
calibration
    .command("driver-cost")
    .argument("<raw>", "Complete HTTP raw-evidence directory with raw.lock.json; one workflow/arm")
    .requiredOption("--out <file>", "New JSON report file; parent must exist, existing file is never overwritten")
    .description("Offline verified validation/decode and whole-run trace self-cost; no overhead acceptance")
    .addHelpText(
        "after",
        "\nEffects: offline raw reads and one new JSON file; no HTTP, Docker or replay.\nFull raw-lock/count/timeline verification precedes diagnostics; damaged evidence is rejected.\nDecode is a subset of validation. Trace production uses all warmup+measurement records including drops.\nNo per-phase trace allocation, final fsync/page-cache attribution or throughput benefit from disabling checks.\nPerformance and overhead acceptance remain false. Uses a 330-second deadline plus up to 5 seconds cleanup.\nExample: hgbench http calibration driver-cost /absolute/raw --out /absolute/new-cost.json\nContract: docs/http-driver-self-cost.md\n",
    )
    .action(async (raw, options) => {
        const result = await reportHttpDriverSelfCost(raw, options.out);
        output("http.calibration.driver-cost", result, [
            { kind: "read", path: result.reportJson, mediaType: "application/json" },
        ]);
    });
calibration
    .command("windows")
    .argument("<raw>", "Complete HTTP raw-evidence directory with raw.lock.json; one workflow/arm")
    .requiredOption("--out <directory>", "New report directory; existing output is never overwritten")
    .requiredOption(
        "--window-ms <integer>",
        "Explicit arrival window, 1..300000 ms",
        phaseWindowInteger("window-ms", 1, 300000),
    )
    .requiredOption(
        "--minimum-requests-per-window <integer>",
        "Explicit minimum sample count per metric for P50/P95, 1..100000",
        phaseWindowInteger("minimum-requests-per-window", 1, 100000),
    )
    .requiredOption(
        "--minimum-p99-requests <integer>",
        "Explicit additional P99 sample minimum, 1000..100000",
        phaseWindowInteger("minimum-p99-requests", 1000, 100000),
    )
    .description("Offline verified warmup/measurement arrival-window trends; no automatic steady-state acceptance")
    .addHelpText(
        "after",
        "\nEffects: offline reads and a new JSON/HTML report; sends no HTTP or Docker requests.\nReuses full analyzer raw-lock, count and timeline verification; damaged raw is rejected.\nSent requests use sentMs, unsent requests use observedMs; drain and cross-window completions remain visible.\nLow samples remain unknown. P99 needs its separate explicit minimum (at least 1000); no automatic warmup/steady state or performance qualification.\nUses a fixed 330-second component deadline and up to 5 seconds of cleanup.\nExample: hgbench http calibration windows /absolute/raw --out /absolute/new-report --window-ms 10000 --minimum-requests-per-window 100 --minimum-p99-requests 1000\nContract: docs/http-phase-windows.md\n",
    )
    .action(async (raw, options) => {
        const result = await reportHttpPhaseWindows(raw, options.out, {
            windowMs: options.windowMs,
            minimumRequestsPerWindow: options.minimumRequestsPerWindow,
            minimumP99Requests: options.minimumP99Requests,
        });
        output("http.calibration.windows", result, [
            { kind: "read", path: result.reportJson, mediaType: "application/json" },
            { kind: "read", path: result.reportHtml, mediaType: "text/html" },
        ]);
    });
httpCommand
    .command("suite")
    .description("Verify the versioned HTTP question bundle and independent answer hashes; offline/read-only")
    .option("--id <case-id>", "Inspect one exact request, parameter set, answer hash and response shape")
    .option("--require-release", "Exit 3 while formal calibration/reproduction prerequisites remain")
    .addHelpText(
        "after",
        "\nExamples: hgbench http suite; hgbench http suite --id string-1024k\nExit 0 verifies local contracts only. No downloads, database access or output files.\n",
    )
    .action(async (options) => {
        const suite = await inspectHttpSuite();
        if (options.requireRelease)
            throw new BenchError(
                "HTTP_SUITE_RELEASE_NOT_READY",
                "Formal HTTP calibration and reproduction remain",
                3,
                false,
                suite.release,
            );
        const data = options.id ? suite.questions.find((q) => q.id === options.id) : suite;
        if (!data) throw new BenchError("HTTP_SUITE_CASE_UNKNOWN", `Unknown HTTP case: ${options.id}`);
        output("http.suite", data);
    });
httpCommand
    .command("generate")
    .argument("<settings>", "JSON matching schema http-suite-settings; explicit timings, loads and budgets")
    .requiredOption("--out <file>", "New profile JSON; existing files are never overwritten")
    .option("--dry-run", "Validate and return profile without writing a file or contacting a target")
    .description("Generate a runner profile from frozen HTTP questions, with exact coverage and bundle identity")
    .addHelpText(
        "after",
        "\nExample: hgbench http generate examples/http-suite-functional.json --out /tmp/http-profile.json\nUse purpose=functional or calibration; formal profiles require future calibration gates.\nNo default experimental rates or durations are inferred. Next: http validate, http plan, task run.\n",
    )
    .action(async (settings, options) => {
        const result = await generateHttpProfile(settings, options.out, Boolean(options.dryRun));
        output("http.generate", result, [
            options.dryRun
                ? { kind: "write", argv: ["hgbench", "http", "generate", resolve(settings), "--out", result.file] }
                : { kind: "read", argv: ["hgbench", "http", "validate", result.file] },
        ]);
    });
const httpEngine = httpCommand
    .command("engine")
    .description("Inspect, explicitly build and register the independent Go HTTP execution component");
httpEngine
    .command("inspect")
    .argument("<binary>", "Registered or explicitly supplied Go executable; invokes version only")
    .description("Return binary SHA, source digest, Go/platform, build flags and protocol versions; no target calls")
    .action(async (file) =>
        output("http.engine.inspect", await (await import("./http-go.js")).inspectHttpGoBinary(file)),
    );
httpEngine
    .command("build")
    .requiredOption("--go <path>", "Explicit installed Go 1.27.3 executable; never downloads a toolchain")
    .requiredOption("--out <directory>", "New build directory; binary and build receipt retained")
    .description("Build stdlib-only runner offline with frozen flags/source digest; removes its own compilation cache")
    .action(async (options) => {
        const result = await promisify(execFile)(
            process.execPath,
            [
                join(packageRoot, "components/http-go/build.mjs"),
                "--go",
                resolve(options.go),
                "--out",
                resolve(options.out),
            ],
            { timeout: 330000, maxBuffer: 1048576 },
        );
        output("http.engine.build", JSON.parse(result.stdout));
    });
httpEngine
    .command("clock-probe")
    .requiredOption("--binary <path>", "Explicit registered Linux Go executable; no build or download")
    .requiredOption("--image-id <id>", "Locally available immutable Docker sha256 image ID")
    .requiredOption("--out <directory>", "New raw evidence directory; create-only")
    .description("Observe a live isolated Go process and five host clock exchanges; sends no HTTP requests")
    .addHelpText(
        "after",
        "\nRequires Linux, Docker access and readable /proc PID identities. Up to 30s plus cleanup reserve.\nLimits: 2 CPUs, 256 MiB, 64 PIDs, no network, read-only binary/root. Own container is removed.\nNext: http engine clock-verify <out>. Probe proof alone does not qualify a measurement process.\n",
    )
    .action(async (options) => {
        await (await import("./http-go.js")).inspectHttpGoBinary(options.binary);
        const result = await promisify(execFile)(
            "python3",
            [
                join(packageRoot, "components/http/go-clock-probe.py"),
                "--binary",
                resolve(options.binary),
                "--image-id",
                options.imageId,
                "--out",
                resolve(options.out),
            ],
            { timeout: 45000, maxBuffer: 1048576 },
        );
        output("http.engine.clock-probe", JSON.parse(result.stdout));
    });
httpEngine
    .command("clock-verify")
    .argument("<directory>", "Frozen Go host-clock evidence; reads locked files offline")
    .description("Verify source, PID/executable/namespaces, nonce exchanges and Docker lifecycle without target calls")
    .action(async (directory) => {
        const reader = await import(join(packageRoot, "components/http/go-clock-evidence.mjs"));
        output("http.engine.clock-verify", await reader.readGoHostClockProof(directory));
    });
httpEngine
    .command("process-verify")
    .argument("<directory>", "Host observation directory containing ready-host.json and finished-host.json")
    .requiredOption("--clock-proof <directory>", "Independently captured clock evidence for this same Go binary/image")
    .requiredOption("--run <file>", "Original Go run JSON")
    .requiredOption("--request <file>", "Original frozen Go request JSON")
    .requiredOption("--markers <directory>", "Original two-stage Go marker/release directory")
    .description("Verify each live measurement process against host PID/executable/time namespace evidence offline")
    .addHelpText(
        "after",
        "\nReads bounded regular files and verifies original marker/release bytes. No target calls.\nOnly closes the Go process/host clock association; collector/JVM linkage, correctness evidence and calibration remain required.\n",
    )
    .action(async (directory, options) => {
        const reader = await import(join(packageRoot, "components/http/go-clock-evidence.mjs"));
        output(
            "http.engine.process-verify",
            await reader.readGoMeasurementAssociation(directory, {
                clockProof: options.clockProof,
                runFile: options.run,
                requestFile: options.request,
                markerDirectory: options.markers,
            }),
        );
    });
httpEngine
    .command("register")
    .argument("<profile>", "Existing validated Node v1 workload; creates a new Go v2 profile")
    .requiredOption("--binary <path>", "Explicit compiled Go executable; identity and source digest are frozen")
    .requiredOption("--out <file>", "New Go profile; existing files are never overwritten")
    .option("--gomaxprocs <n>", "Fixed Go execution threads", "2")
    .option("--cpu-quota-cores <n>", "Declared externally enforced CPU quota", "2")
    .option("--max-rss-bytes <n>", "Declared externally enforced RSS budget", "1073741824")
    .option("--host-identity-gate", "Require an active outer host observer before/after every run and service check")
    .description(
        "Preserve workload/answers and register Go binary/resources. Requires new calibration; Node clock reference is replaced",
    )
    .addHelpText(
        "after",
        "\nNext: http validate <out>, http plan <out> --out NEW-plan, task run/status/cancel.\nRegistration sends no requests. Use identical binary/resources for compared arms.\n",
    )
    .action(async (file, options) =>
        output(
            "http.engine.register",
            await (
                await import("./http-go.js")
            ).registerHttpGo(file, options.binary, options.out, {
                gomaxprocs: Number(options.gomaxprocs),
                cpuQuotaCores: Number(options.cpuQuotaCores),
                maxRssBytes: Number(options.maxRssBytes),
                ...(options.hostIdentityGate ? { hostIdentityGate: true as const } : {}),
            }),
        ),
    );
httpCommand
    .command("validate")
    .argument("<file>", "HTTP profile JSON; schemas http-profile (Node v1) or http-go-profile (Go v2)")
    .description("Validate profile budgets offline, without creating files or contacting a target")
    .action(async (file) => output("http.validate", { profile: await inspectAnyHttpProfile(file), executed: false }));
httpCommand
    .command("plan")
    .argument("<file>", "HTTP profile; evidenceFiles are relative to its directory")
    .requiredOption("--out <directory>", "New attempt directory, must not already exist")
    .description("Freeze workload, target evidence, components and task budgets; no target calls")
    .addHelpText(
        "after",
        "\nNext: task run --plan <directory>/task.json --request-id <unique-key> [--detach].\nA task's exit 0 proves process completion. Read output/analysis/report.json for quality gates.\nAfter interruption inspect task status; never delete output/ to replay. Use a fresh plan for another attempt.\nCredentials: authorizationEnv names an environment variable; its value is never frozen or logged.\n",
    )
    .action(async (file, options) => {
        const result = await planHttp(file, options.out);
        output("http.plan", result, [
            { kind: "execute", argv: ["hgbench", "task", "run", "--plan", result.taskPlan, "--detach"] },
        ]);
    });
httpCommand
    .command("report")
    .argument("<directory>", "Raw evidence, or a run root containing evidence/ or output/")
    .requiredOption(
        "--out <directory>",
        "New directory; missing parents are created, existing output is never overwritten",
    )
    .option("--events <file>", "Optional diagnostic annotations JSON; never excludes samples")
    .option("--windows", "Include existing phase-window diagnostics (100-sample minimum; offline trace reread)")
    .option("--window-ms <integer>", "Window width; requires --windows", "1000")
    .description("Reverify raw evidence and create offline HTML/JSON/CSV plus individually exportable SVG charts")
    .addHelpText(
        "after",
        "\nEffects: read existing evidence and create a report; no HTTP requests.\nEach chart has an Export SVG button and a matching charts/*.svg file.\nExamples: hgbench http report ./run --out ./reports/new\n          hgbench http report ./run --windows --window-ms 1000 --out ./reports/detail\nContract and annotations: docs/http-reports.md\n",
    )
    .action(async (directory, options) =>
        output(
            "http.report",
            await httpReportAction("report", { ...options, directory, windowMs: Number(options.windowMs) }),
        ),
    );
httpCommand
    .command("compare")
    .option(
        "--input <group=path>",
        "Repeat for each raw input; repeat a group to retain multiple runs",
        (v: string, a: string[]) => [...a, v],
        [],
    )
    .option("--manifest <file>", "Comparison JSON; paths inside are relative to this file")
    .option("--baseline <group>", "Initial relative-change baseline (also selectable in HTML)")
    .option("--title <text>", "Report title")
    .option("--events <file>", "Optional diagnostic event annotations JSON")
    .option("--windows", "Include verified phase-window diagnostics; low-sample windows remain unknown")
    .option("--window-ms <integer>", "Window width when --windows is used", "1000")
    .option("--out <directory>", "New report directory; required except for --dry-run")
    .option("--dry-run", "Verify inputs and print alignment/missing inventory; write no files")
    .description("Offline descriptive comparison of 2–8 groups / up to 32 existing raw inputs; no new tests")
    .addHelpText(
        "after",
        "\nExamples:\n  hgbench http compare --input base=./r1-base --input base=./r2-base --input candidate=./r1-new --out ./comparison\n  hgbench http compare --manifest compare.json --dry-run\n  hgbench http compare --manifest compare.json --windows --out ./comparison\nInputs: raw.lock.json directories or run roots with evidence/ or output/. Standalone report.json is not accepted.\nFull raw verification is mandatory, including dry-run. Duplicate evidence is rejected. Different workload settings split into separate cells.\nAggregation: median and min/max of per-run metrics, retaining every point and failure. No pooled percentiles, statistical winner or capacity claim.\nOutputs: report.html, report.json, summary.csv, runs.csv, inputs.json, charts/*.svg. Each chart exports separately from standalone HTML.\nLimits: 300 seconds, 64 MiB output; split larger comparisons. Round labels do not prove independent JVMs.\nContract and manifest/events examples: docs/http-reports.md\n",
    )
    .action(async (options) =>
        output("http.compare", await httpReportAction("compare", { ...options, windowMs: Number(options.windowMs) })),
    );
program
    .command("version")
    .description("Structured package version, content buildId and report/raw protocol versions; no target access")
    .action(async () => output("version", await httpReportAction("version")));
const suite = {
    id: "controller-fixture-v1",
    maturity: "controller-self-test",
    realDatabase: false,
    scope: "controller-fixture",
    example: join(packageRoot, "examples", "controller-smoke.json"),
    description: "Paired sum computations with known answers and controlled delays; validates orchestration only.",
};
const sf100Suite = {
    id: sf100SuiteId,
    maturity: "contract-draft",
    scale: "100",
    executionSupported: false,
    example: join(packageRoot, "benchmarks/ldbc-snb-interactive-v1/sf100-plan.json"),
    description: "Official LDBC SNB Interactive v1 sources, all 29 operations; offline validation and planning.",
};
const graphSuite = {
    id: "hgbench-graph-query-v0.1",
    maturity: "engineering-candidate",
    defaultScaleFactor: 1,
    optionalScaleFactors: [10, 100],
    executionSupported: true,
    performanceAccepted: false,
    discover: ["hgbench", "graph", "describe"],
    description: "Five graph queries; prepare/import and quick/standard share the selected dataset",
};
benchmark.command("list").action(() =>
    output("benchmark.list", [
        graphSuite,
        {
            id: "hgbench-http-serving-v0.1",
            maturity: "local-specification-candidate",
            executionSupported: true,
            performanceAccepted: false,
            discover: ["hgbench", "http", "suite"],
            description: "Current HTTP main track: deterministic no-graph questions; real calibration pending",
        },
        sf100Suite,
        suite,
    ]),
);
benchmark
    .command("spec")
    .description("Verify the versioned SF100 specification, all 29 question contracts and pinned upstream sources")
    .option("--require-release", "Exit 3 while real calibration, adapter or reproduction prerequisites remain")
    .addHelpText(
        "after",
        "\nEffects: read-only, offline. Requires Python 3.9+. No dataset downloads or database calls.\nExample: hgbench benchmark spec\nExit 0 verifies local integrity, never live correctness or performance.\n",
    )
    .action(async (options) => {
        const report = await inspectSpecification("verify");
        if (options.requireRelease)
            throw new BenchError(
                "SPEC_RELEASE_NOT_READY",
                "Real release prerequisites remain; inspect releaseGates",
                3,
                false,
                report,
            );
        output("benchmark.spec", report);
    });
benchmark
    .command("questions")
    .description("List the 29 SF100 questions or inspect one parameter/result contract")
    .addOption(
        new Option("--id <operation>", "Full question contract, including frozen reference source paths").choices([
            ...Array.from({ length: 14 }, (_, i) => `IC${i + 1}`),
            ...Array.from({ length: 7 }, (_, i) => `IS${i + 1}`),
            ...Array.from({ length: 8 }, (_, i) => `IU${i + 1}`),
        ]),
    )
    .addHelpText(
        "after",
        "\nEffects: read-only, offline. Requires Python 3.9+; verifies suite integrity first.\nExample: hgbench benchmark questions --id IU7\n",
    )
    .action(async (options) => output("benchmark.questions", await inspectSpecification("questions", options.id)));
for (const name of ["validate", "plan"] as const) {
    benchmark
        .command(name)
        .description(
            name === "validate"
                ? "Validate SF100 configuration and report missing prerequisites"
                : "Describe SF100 stages, effects and evidence; executes nothing",
        )
        .argument("<file>", "SF100 source plan JSON; paths are relative to this file")
        .option("--require-ready", "Exit 3 if execution prerequisites are missing (default: report blockers, exit 0)")
        .addHelpText(
            "after",
            `\nEffects: read-only and offline; no downloads, components, database calls or writes.\nExample: hgbench benchmark ${name} benchmarks/ldbc-snb-interactive-v1/sf100-plan.json\nExit codes: 0 valid planning input, 2 invalid input, 3 --require-ready unmet.\nA valid contract is distinct from executionReady; inspect blockers before scheduling.\n`,
        )
        .action(async (file, options) => {
            const report = await inspectSf100Plan(file);
            if (options.requireReady && !report.executionReady) {
                throw new BenchError("SF100_NOT_READY", "SF100 execution prerequisites are not met", 3, false, report);
            }
            output(`benchmark.${name}`, report);
        });
}
benchmark
    .command("exchange")
    .description("Validate an SF100 request/receipt pair; optionally hash local output artifacts")
    .argument("<request>", "Request JSON")
    .option("--receipt <file>", "Receipt JSON to bind to this request")
    .option("--artifact-root <directory>", "Verify receipt files under this root; may read large files")
    .addHelpText(
        "after",
        "\nEffects: read-only, offline; does not execute the component.\nExample: hgbench benchmark exchange request.json --receipt receipt.json\nExit 0 confirms protocol validity, never a database or benchmark pass.\n",
    )
    .action(async (request, options) =>
        output("benchmark.exchange", await inspectSf100Exchange(request, options.receipt, options.artifactRoot)),
    );
benchmark
    .command("recover")
    .description("Compute a recovery action from immutable request, receipt and reconciliation evidence")
    .argument("<request>", "Recorded request JSON")
    .option("--receipt <file>", "Receipt JSON, if available")
    .option(
        "--artifact-root <directory>",
        "Verify outputs and reconciliation evidence under this root; may read large files",
    )
    .option("--observation <file>", "Whole-shard reconciliation observation from a quiesced loader")
    .addHelpText(
        "after",
        "\nEffects: read-only, offline; returns an action and never retries, restores or contacts a database.\nExample: hgbench benchmark recover request.json --receipt receipt.json --artifact-root evidence\nMissing/unknown load outcomes require reconciliation. Partial loads require restore-and-reload.\n",
    )
    .action(async (request, options) =>
        output(
            "benchmark.recover",
            await assessSf100Recovery(request, options.receipt, options.artifactRoot, options.observation),
        ),
    );
benchmark
    .command("checkpoint")
    .description("Record immutable dispatch intent, or seal a receipt after verifying local artifacts")
    .argument("<request>", "Request JSON; requestId must be unique per attempt")
    .option("--receipt <file>", "Seal this receipt; requires an already recorded matching request")
    .option("--artifact-root <directory>", "Required with --receipt; verifies all receipt artifacts")
    .addHelpText(
        "after",
        "\nEffects: writes small metadata under --root/sf100-checkpoints; no component or database execution.\nExample: hgbench --root work benchmark checkpoint request.json\nSame request/receipt is idempotent. Changed bytes/identity conflict (exit 5); use a new requestId for a new attempt.\nA recorded intent without a receipt means outcome unknown.\n",
    )
    .action(async (request, options) =>
        output("benchmark.checkpoint", await checkpointSf100(root(), request, options.receipt, options.artifactRoot)),
    );
benchmark
    .command("loader-plan")
    .description("Prepare one HugeGraph Loader job per immutable SF100 shard")
    .argument("<prepared>", "Sharded prepare-manifest.json")
    .requiredOption("--config <file>", "SF100 loader configuration; inspect schema sf100-loader-config")
    .option("--out <directory>", "Write numbered job JSON files into a NEW directory")
    .addOption(
        new Option("--detail <level>", "Output summary or full job definitions")
            .choices(["summary", "full"])
            .default("summary"),
    )
    .addHelpText(
        "after",
        "\nEffects: reads metadata; optional --out writes jobs. Does not read graph data or start Loader.\nExample: hgbench benchmark loader-plan prepared/prepare-manifest.json --config loader.json --out jobs\nMapping digest binds schema metadata and converter identity; see docs/sf100-data-pipeline.md.\n",
    )
    .action(async (prepared, options) => {
        const result = await planLoaderJobs(prepared, options.config);
        if (options.out) {
            const { mkdir } = await import("node:fs/promises");
            await mkdir(resolve(options.out), { recursive: false });
            for (const [i, job] of result.jobs.entries())
                await writeJson(join(resolve(options.out), `${String(i).padStart(6, "0")}.json`), job);
        }
        output("benchmark.loader-plan", {
            jobCount: result.jobs.length,
            executed: false,
            liveVerified: false,
            order: result.order,
            outputDirectory: options.out ? resolve(options.out) : null,
            ...(options.detail === "full" ? { jobs: result.jobs } : {}),
        });
    });
benchmark
    .command("loader-run")
    .description("Inspect a Loader job; --execute explicitly launches it and retains unknown outcomes")
    .argument("<job>", "Frozen single-shard job JSON")
    .requiredOption("--out <directory>", "NEW attempt directory; parent must exist")
    .option("--execute", "Actually launch the trusted Loader and write to its configured database")
    .addHelpText(
        "after",
        "\nDefault: read-only command preview. --execute: reads shard/runtime files, starts Loader, writes database and evidence.\nExample: hgbench benchmark loader-run jobs/000000.json --out attempts/shard-000000\nSchema, empty target epoch, and vertex prerequisites must be verified before --execute.\nExit 4 after dispatch means database outcome needs reconciliation, even when Loader exits 0. No automatic retry.\n",
    )
    .action(async (job, options) => {
        const result = await runLoaderJob(job, options.out, Boolean(options.execute));
        output("benchmark.loader-run", result);
        if (options.execute) process.exitCode = 4;
    });
benchmark
    .command("show")
    .argument("<id>")
    .action((id) => {
        const selected = [graphSuite, sf100Suite, suite].find((item) => item.id === id);
        if (!selected)
            throw new BenchError("UNKNOWN_SUITE", `Available suites: ${graphSuite.id}, ${sf100Suite.id}, ${suite.id}`);
        output("benchmark.show", selected);
    });

const task = command(
    "task",
    "Manage bounded component processes and their evidence",
    "list/plan/status/wait/events/logs are offline; run executes a trusted program",
    "task plan examples/spec-check-task.json --out spec-task.json",
);
task.addHelpText(
    "after",
    "\nA task records process completion; database effects and benchmark validity require component receipts and validation.\nRun this CLI on the execution host (including over SSH); task IDs are scoped to that host and --root.\nNo automatic replay or generic resume. After a lost connection query the same request-id.\nTimeout/log limits are enforced; CPU, memory and disk quotas belong to the component or host supervisor.\n",
);
function taskId(options: { id?: string; requestId?: string }): string {
    if (Boolean(options.id) === Boolean(options.requestId))
        throw new BenchError("INVALID_SELECTOR", "Provide exactly one of --id or --request-id");
    return options.id ?? taskRequestId(options.requestId!);
}
function taskNext(id: string) {
    return ["status", "events", "logs"].map((cmd) => ({
        kind: "read",
        argv: ["hgbench", "--root", root(), "task", cmd, "--id", id],
    }));
}
const integer = (value: string) => {
    const n = Number(value);
    if (!Number.isSafeInteger(n) || n < 0) throw new BenchError("CLI_USAGE", "Expected a nonnegative safe integer");
    return n;
};
task.command("list")
    .description("Discover existing tasks without a task ID; bounded, read-only pagination")
    .option("--limit <n>", "1..100 tasks per page, in task ID order", integer, 20)
    .option("--cursor <token>", "Opaque nextCursor from this host and --root")
    .addHelpText(
        "after",
        "\nEffects: reads metadata only; starts no process and creates no store. Missing/corrupt entries remain visible with available:false.\nExample: hgbench --root work task list --limit 20\nStates are live observations, not an atomic snapshot. When tasks are added/deleted, CURSOR_STALE (exit 4) requires a fresh listing.\nAt most 10000 store entries are scanned per pass. This command does not verify artifact hashes or benchmark validity.\n",
    )
    .action(async (options) => {
        const result = await listTasks(root(), options.limit, options.cursor);
        const nextActions: unknown[] = result.items
            .filter((item) => item.available)
            .map((item) => ({
                kind: "read",
                argv: ["hgbench", "--root", root(), "task", "status", "--id", item.taskId],
            }));
        if (result.nextCursor)
            nextActions.push({
                kind: "read",
                argv: [
                    "hgbench",
                    "--root",
                    root(),
                    "task",
                    "list",
                    "--limit",
                    String(options.limit),
                    "--cursor",
                    result.nextCursor,
                ],
            });
        output("task.list", result, nextActions);
    });
task.command("plan")
    .description("Freeze command, working directory, tracked code/config and process budgets; executes nothing")
    .argument("<file>", "Task source JSON; cwd is relative to this file, trackedFiles relative to cwd")
    .option("--out <file>", "Write frozen task plan")
    .option("--dry-run", "Read-only, including when --out is supplied")
    .addHelpText(
        "after",
        "\nEffects: reads/hashes tracked local files; --out writes metadata. Does not execute components.\nExample: hgbench task plan examples/spec-check-task.json --out spec-task.json\nLarge datasets should be content-locked/verified by their component, not repeatedly hashed by this controller.\n",
    )
    .action(async (file, options) => {
        const frozen = await freezeTask(file);
        if (options.out && !options.dryRun) await writeJson(resolve(options.out), frozen);
        output("task.plan", {
            executed: false,
            frozen,
            outputPath: options.out && !options.dryRun ? resolve(options.out) : null,
        });
    });
task.command("run")
    .description("Start one frozen component task; foreground waits, --detach returns task ID")
    .requiredOption("--plan <file>", "Frozen task JSON")
    .option("--request-id <key>", "Same key/plan recovers the same submission in this --root")
    .option("--detach", "Worker survives the calling CLI/SSH session")
    .option("--dry-run", "Verify identities without writes or execution")
    .addHelpText(
        "after",
        "\nEffects: writes task metadata/logs and executes the frozen argv without a shell. Inherits environment; never put secrets in argv.\nExample: hgbench task run --plan spec-task.json --detach --request-id spec-001\nExit: 0 submitted/completed process, 5 conflicting key/changed identity, 6 foreground process failure. Completion is not benchmark acceptance.\nAfter connection loss: task status --request-id spec-001. No automatic retry of component effects.\n",
    )
    .action(async (options) => {
        const frozen = await readJson(resolve(options.plan), frozenTaskSchema);
        if (options.dryRun) {
            await verifyTask(frozen);
            return output("task.run", { executed: false, planDigest: frozen.digest, budgets: frozen.plan });
        }
        const submitted = await submitTask(root(), frozen, options.requestId);
        if (options.detach) return output("task.run", submitted, taskNext(submitted.taskId));
        let state = await waitTask(root(), submitted.taskId, 1000);
        while (!state.terminal) state = await waitTask(root(), submitted.taskId, 1000);
        output("task.run", { ...state, reused: submitted.reused }, taskNext(state.taskId));
        if (state.state !== "completed" || state.interrupted) process.exitCode = 6;
    });
task.command("status")
    .description("Read state; ok:true does not imply a successful task or valid benchmark")
    .option("--id <id>", "Task ID")
    .option("--request-id <key>", "Submission key (exactly one selector)")
    .action(async (options) => {
        const id = taskId(options);
        output("task.status", await taskSnapshot(root(), id), taskNext(id));
    });
task.command("wait")
    .description("Wait up to 60 seconds for terminal state or an event after a cursor; timeout exits 0")
    .requiredOption("--id <id>", "Task ID")
    .option("--timeout-ms <ms>", "0..60000", duration, 30000)
    .option("--after-cursor <n>", "Return when a new lifecycle event appears", integer)
    .action(async (options) =>
        output(
            "task.wait",
            await waitTask(root(), options.id, options.timeoutMs, options.afterCursor),
            taskNext(options.id),
        ),
    );
task.command("cancel")
    .description("Request cancellation; only the owning worker signals its component group")
    .requiredOption("--id <id>", "Task ID")
    .addHelpText(
        "after",
        "\nEffects: writes a cancellation mailbox. Owner sends TERM, then KILL after the frozen grace period; keeps partial evidence.\nExample: hgbench task cancel --id <id>\nRepeated calls are safe. Cancellation is not database rollback; inspect task status and component reconciliation evidence.\n",
    )
    .action(async (options) => output("task.cancel", await cancelTask(root(), options.id), taskNext(options.id)));
task.command("events")
    .description("Read a bounded lifecycle event page; cursor is scoped to this task")
    .requiredOption("--id <id>")
    .option("--after-cursor <n>", "Last delivered sequence; default 0", integer, 0)
    .option("--limit <n>", "1..1000 events", integer, 100)
    .action(async (options) =>
        output("task.events", await taskEvents(root(), options.id, options.afterCursor, options.limit)),
    );
task.command("logs")
    .description("Read the last 16 KiB of one component log; returns a bounded JSON envelope")
    .requiredOption("--id <id>")
    .addOption(new Option("--stream <name>").choices(["stdout", "stderr"]).default("stdout"))
    .action(async (options) => output("task.logs", await taskLogs(root(), options.id, options.stream)));

task.command("artifacts")
    .description("Verify sealed lifecycle evidence hashes; interrupted tasks retain unsealed partial logs")
    .requiredOption("--id <id>")
    .action(async (options) => output("task.artifacts", await taskArtifacts(root(), options.id)));

function commandInfo(cmd: Command): unknown {
    return {
        name: cmd.name(),
        description: cmd.description(),
        arguments: cmd.registeredArguments.map((arg) => ({ name: arg.name(), required: arg.required })),
        options: cmd.options.map((opt) => ({
            flags: opt.flags,
            description: opt.description,
            required: opt.mandatory,
            default: opt.defaultValue,
            choices: opt.argChoices,
            takesValue: opt.required || opt.optional,
        })),
        commands: cmd.commands.filter((c) => !c.name().startsWith("__")).map(commandInfo),
    };
}
command("schema", "Inspect data contracts or CLI commands", "read-only, offline", "schema plan")
    .argument("[name]", "Contract name, commands, or command:<path> (for example command:http.calibration.acceptance)")
    .action(async (name?: string) => {
        const names = [...Object.keys(schemas), "ldbc-answer-scorecard"];
        if (!name) return output("schema", { contracts: names, commands: "schema commands" });
        if (name === "ldbc-answer-scorecard") return output("schema", await inspectSpecification("schema"));
        if (name === "commands") return output("schema", commandInfo(program));
        if (name.startsWith("command:")) {
            let cmd: Command | undefined = program;
            for (const part of name.slice(8).split("."))
                cmd = cmd?.commands.find((c) => c.name() === part && !c.name().startsWith("__"));
            if (!cmd)
                throw new BenchError(
                    "UNKNOWN_COMMAND",
                    "Unknown command schema; use a dot-separated public command path, such as command:http.calibration.acceptance",
                );
            return output("schema", commandInfo(cmd));
        }
        const schema = schemas[name];
        if (!schema) throw new BenchError("UNKNOWN_SCHEMA", `Available contracts: ${names.join(", ")}`);
        output("schema", z.toJSONSchema(schema));
    });

command(
    "plan",
    "Freeze inputs and component identities; no components are executed",
    "reads local files; --out writes a file",
    "plan examples/controller-smoke.json --out resolved-plan.json",
)
    .argument("<file>", "Source plan JSON")
    .option("--out <file>", "Write the frozen plan")
    .option("--dry-run", "Preview only; do not write even with --out")
    .action(async (file, options) => {
        const plan = await freezePlan(file);
        if (options.out && !options.dryRun) await writeJson(resolve(options.out), plan);
        output("plan", {
            digest: plan.digest,
            scope: plan.scope,
            totalRuns: plan.plan.cases.length * plan.plan.variants.length * plan.plan.blocks,
            componentTimeoutMs: plan.plan.componentTimeoutMs,
            outputPath: options.out && !options.dryRun ? resolve(options.out) : null,
            resolvedPlan: plan,
            executed: false,
        });
    });

command(
    "doctor",
    "Check runtime and optional component capability handshakes",
    "--plan executes trusted component --describe only; no workload",
    "doctor --plan resolved-plan.json",
)
    .option("--plan <file>", "Frozen plan to validate and probe")
    .action(async (options) => {
        if (process.platform === "win32")
            throw new BenchError("UNSUPPORTED_PLATFORM", "MVP execution requires Linux or macOS", 3);
        const checks: unknown[] = [
            { name: "node", observed: process.version, status: "pass" },
            { name: "platform", observed: process.platform, status: "pass" },
        ];
        if (options.plan) {
            const plan = await loadPlan(options.plan);
            for (const component of [plan.executor, plan.analyzer]) {
                checks.push({ name: component.manifest.id, status: "pass", capabilities: await describe(component) });
            }
        }
        output("doctor", { checks, scope: "controller-fixture" });
    });

const components = command(
    "components",
    "List or probe executor/analyzer components",
    "list is offline; describe runs a trusted program",
    "components describe builtin:fixture",
);
components.command("list").action(() => output("components.list", builtins));
components
    .command("describe")
    .argument("<manifest>")
    .action(async (ref) => {
        const component = await resolveComponent(ref, process.cwd());
        output("components.describe", { component, capabilities: await describe(component) });
    });

command(
    "run",
    "Execute a frozen plan",
    "creates evidence and runs trusted local components; foreground waits",
    "run --plan resolved-plan.json --detach --request-id demo-001",
)
    .requiredOption("--plan <file>", "Frozen plan JSON")
    .option("--detach", "Return after submission; worker survives caller exit")
    .option("--request-id <key>", "Same key and same plan recover the same experiment within --root")
    .option("--dry-run", "Validate identities without creating files or executing components")
    .action(async (options) => {
        const plan = await loadPlan(options.plan);
        if (options.dryRun) return output("run", { executed: false, planDigest: plan.digest, scope: plan.scope });
        if (process.platform === "win32")
            throw new BenchError("UNSUPPORTED_PLATFORM", "MVP execution requires POSIX process groups", 3);
        const submitted = await submit(root(), plan, options.requestId);
        if (options.detach) return output("run", submitted, next(submitted.experimentId));
        let state = submitted;
        while (!state.terminal) state = { ...(await wait(root(), state.experimentId, 1000)), reused: submitted.reused };
        output("run", state, next(state.experimentId));
        if (state.state !== "completed") process.exitCode = 6;
    });

command(
    "status",
    "Read task state; command success does not imply benchmark success",
    "read-only",
    "status --request-id demo-001",
)
    .option("--experiment <id>", "Experiment ID")
    .option("--request-id <key>", "Original submission key")
    .action(async (options) => {
        const id = idFrom(options);
        output("status", await snapshot(root(), id), next(id));
    });

command(
    "wait",
    "Bounded wait; timeout returns terminal:false and exit 0",
    "read-only",
    "wait --experiment <id> --timeout-ms 30000",
)
    .requiredOption("--experiment <id>", "Experiment ID")
    .option("--timeout-ms <ms>", "Wait duration, 0..60000", duration, 30000)
    .action(async (options) =>
        output("wait", await wait(root(), options.experiment, options.timeoutMs), next(options.experiment)),
    );

command(
    "cancel",
    "Request cancellation; repeated calls are safe",
    "stops this experiment's components; retains evidence",
    "cancel --experiment <id>",
)
    .requiredOption("--experiment <id>", "Experiment ID")
    .action(async (options) => output("cancel", await cancel(root(), options.experiment), next(options.experiment)));

command(
    "resume",
    "Restart a failed/cancelled/interrupted experiment as a new linked attempt",
    "reruns all cells; incomplete measurements are never merged",
    "resume --experiment <id> --request-id retry-001",
)
    .requiredOption("--experiment <id>", "Parent experiment ID")
    .option("--request-id <key>", "Idempotency key for the new attempt")
    .action(async (options) => {
        const result = await resume(root(), options.experiment, options.requestId);
        output("resume", result, next(result.experimentId));
    });

async function scorecard(id: string) {
    const state = await snapshot(root(), id);
    if (state.state !== "completed")
        throw new BenchError("RESULT_NOT_READY", `Experiment is ${state.state}`, 4, !state.terminal);
    await evidence(root(), id);
    const result = await readJson(join(experimentDir(root(), id), "analysis", "result.json"), analysisResultSchema);
    const { reportHtml, ...card } = result;
    return card;
}
const results = command(
    "results",
    "Read sealed analysis output",
    "read-only; verifies evidence checksums",
    "results show --experiment <id>",
);
results
    .command("show")
    .requiredOption("--experiment <id>")
    .addOption(new Option("--detail <level>").choices(["summary", "full"]).default("summary"))
    .action(async (options) => {
        const card = await scorecard(options.experiment);
        output(
            "results.show",
            options.detail === "full"
                ? card
                : {
                      experimentId: card.experimentId,
                      scope: card.scope,
                      verdict: card.verdict,
                      planDigest: card.planDigest,
                      reportPath: join(experimentDir(root(), options.experiment), "report.html"),
                      fullResult: [
                          "hgbench",
                          "--root",
                          root(),
                          "results",
                          "show",
                          "--experiment",
                          options.experiment,
                          "--detail",
                          "full",
                      ],
                  },
        );
    });
command(
    "report",
    "Locate the sealed offline HTML report",
    "read-only; verifies evidence; does not open a browser",
    "report --experiment <id>",
)
    .requiredOption("--experiment <id>")
    .action(async (options) => {
        await scorecard(options.experiment);
        output("report", { path: join(experimentDir(root(), options.experiment), "report.html") });
    });
const artifacts = command(
    "artifacts",
    "Inspect sealed evidence",
    "read-only; checksum mismatch is an error",
    "artifacts list --experiment <id>",
);
artifacts
    .command("list")
    .requiredOption("--experiment <id>")
    .action(async (options) => output("artifacts.list", await evidence(root(), options.experiment)));

const docs = command("docs", "Read bundled operating guides", "read-only, offline", "docs read quickstart");
const topics: Record<string, string> = {
    "http-benchmark": "docs/http-benchmark.md",
    quickstart: "README.md",
    "sf100-contract": "docs/sf100-contract.md",
    "sf100-protocol": "docs/sf100-protocol.md",
    "sf100-data-pipeline": "docs/sf100-data-pipeline.md",
    "ldbc-adapter": "docs/ldbc-adapter.md",
    stages: "docs/stages.md",
    progress: "docs/phase1-acceptance.md",
};
docs.command("list").action(() => output("docs.list", Object.keys(topics)));
docs.command("read")
    .argument("<topic>")
    .action(async (topic) => {
        if (!topics[topic])
            throw new BenchError("UNKNOWN_TOPIC", `Available topics: ${Object.keys(topics).join(", ")}`);
        output("docs.read", { topic, content: await readFile(join(packageRoot, topics[topic]!), "utf8") });
    });

async function main(): Promise<void> {
    if (process.argv[2] === "__task-worker") {
        if (!process.argv[3] || !process.argv[4] || !process.argv[5])
            throw new BenchError("WORKER_ARGUMENTS", "Missing task worker arguments");
        await taskWork(process.argv[3], process.argv[4], process.argv[5]);
        return;
    }
    if (process.argv[2] === "__worker") {
        if (!process.argv[3] || !process.argv[4])
            throw new BenchError("WORKER_ARGUMENTS", "Missing internal worker arguments");
        await work(process.argv[3], process.argv[4]);
        return;
    }
    await program.parseAsync();
}

main().catch((error: unknown) => {
    if (error instanceof CommanderError && error.exitCode === 0) return;
    const info = error instanceof CommanderError ? new BenchError("CLI_USAGE", error.message) : errorInfo(error);
    process.stdout.write(
        `${JSON.stringify({
            schemaVersion: 1,
            ok: false,
            error: {
                code: info.code,
                message: info.message,
                retryable: info.retryable,
                details: info.details,
            },
            nextActions: [{ kind: "read", argv: ["hgbench", "--help"] }],
        })}\n`,
    );
    process.exitCode = info.exitCode;
});
