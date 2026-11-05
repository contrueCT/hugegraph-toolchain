/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { mkdir, readFile, readdir, open, unlink, lstat } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { hostname } from "node:os";
import { pathToFileURL } from "node:url";
import { z } from "zod";
import { packageRoot } from "./plans.js";
import { BenchError, fileHash, hash, readJson, writeJson } from "./io.js";
import { durableJson } from "./tasks.js";
import { planLoaderJobs, runLoaderJob, preparedSchema } from "./sf100-loader.js";
import { prepareGraphProfile, graphReferenceSchema } from "./graph.js";
import { graphImportSnapshotSchema } from "./http-contracts.js";
import { inspectHttpInput } from "./http.js";
import { runHttpThroughput } from "./http-throughput.js";

import { graphProjectSchema } from "./graph-project-contracts.js";
const positive = z.number().int().positive().safe();
const sha = z.string().regex(/^[a-f0-9]{64}$/);
const datasetSchema = z.object({
    schemaVersion: z.literal(1),
    kind: z.literal("graph-dataset"),
    scaleFactor: z.union([z.literal(1), z.literal(10), z.literal(100)]),
    prepared: z.literal("prepared/prepare-manifest.json"),
    reference: z.literal("reference.json"),
    vertices: positive,
    edges: positive,
    artifacts: z.record(z.string(), sha),
});
export async function graphDatasets() {
    return JSON.parse(await readFile(join(packageRoot, "benchmarks/graph-query-v0.1/datasets.json"), "utf8"));
}
export async function prepareGraphDataset(
    options: {
        out: string;
        scale?: number;
        cache?: string;
        python?: string;
        resume?: boolean;
        dryRun?: boolean;
        maxSeconds?: number;
        maxBytes?: number;
        maxDownloadBytes?: number;
        reserveFreeBytes?: number;
    },
    signal?: AbortSignal,
) {
    const args = [join(packageRoot, "tools/graph-dataset.py"), "--out", resolve(options.out)];
    for (const [name, value] of Object.entries(options)) {
        if (value === undefined || ["out", "python"].includes(name)) continue;
        const flag = "--" + name.replace(/[A-Z]/g, (c) => "-" + c.toLowerCase());
        if (typeof value === "boolean") {
            if (value) args.push(flag);
        } else args.push(flag, String(value));
    }
    const result = await promisify(execFile)(options.python ?? "python3", args, {
        signal,
        maxBuffer: 8 * 1024 ** 2,
        timeout: ((options.maxSeconds ?? { 1: 7200, 10: 21600, 100: 172800 }[options.scale ?? 1] ?? 7200) + 10) * 1000,
    });
    return JSON.parse(result.stdout);
}

function target(graphUrl: string) {
    const url = new URL(graphUrl);
    const match = /^(?:\/graphspaces\/([a-zA-Z][a-zA-Z0-9_-]*))?\/graphs\/([a-zA-Z][a-zA-Z0-9_-]*)\/?$/.exec(
        url.pathname,
    );
    if (!match || !/^https?:$/.test(url.protocol) || url.username || url.password || url.search || url.hash)
        throw new BenchError("GRAPH_TARGET", "Use an explicit /graphs/NAME or /graphspaces/SPACE/graphs/NAME URL");
    return {
        origin: url.origin,
        host: `${url.protocol}//${url.hostname}`,
        port: Number(url.port || (url.protocol === "https:" ? 443 : 80)),
        graph: match[2]!,
        graphSpace: match[1] ?? "DEFAULT",
    };
}
async function runtimeFiles(home: string) {
    const files: { path: string; sha256: string }[] = [];
    async function walk(path: string) {
        for (const entry of await readdir(path, { withFileTypes: true })) {
            const child = join(path, entry.name);
            if (entry.isSymbolicLink())
                throw new BenchError("GRAPH_LOADER", "Use a Loader installation without symlinks");
            if (entry.isDirectory()) await walk(child);
            else if (entry.isFile()) files.push({ path: child, sha256: await fileHash(child) });
            if (files.length > 5000) throw new BenchError("GRAPH_LOADER", "Loader installation has too many files");
        }
    }
    for (const dir of ["bin", "lib", "conf"]) await walk(join(home, dir));
    return files;
}
async function acquireLock(directory: string) {
    const path = join(directory, ".import.lock");
    try {
        const old = JSON.parse(await readFile(path, "utf8"));
        if (old.hostname !== hostname())
            throw new BenchError("GRAPH_IMPORT_LOCK", "Import lock belongs to another host");
        let alive = true;
        try {
            process.kill(old.pid, 0);
        } catch (e) {
            if ((e as NodeJS.ErrnoException).code === "ESRCH") alive = false;
        }
        if (alive)
            throw new BenchError("GRAPH_IMPORT_LOCK", "The original importer is still running; inspect state.json");
        await unlink(path);
    } catch (e) {
        if ((e as NodeJS.ErrnoException).code !== "ENOENT") throw e;
    }
    const handle = await open(path, "wx", 0o600);
    await handle.writeFile(JSON.stringify({ pid: process.pid, hostname: hostname() }));
    return async () => {
        await handle.close();
        await unlink(path);
    };
}
export async function importGraphDataset(
    datasetDirectory: string,
    options: {
        out: string;
        loaderHome: string;
        graphUrl: string;
        traversalSource: string;
        minimumEdgeCoverage?: number;
        loader?: z.infer<typeof graphProjectSchema>["loader"];
        authorizationEnv?: string | null;
        budgetMs?: number;
        signal?: AbortSignal;
        resume?: boolean;
        dryRun?: boolean;
    },
) {
    const directory = resolve(datasetDirectory),
        out = resolve(options.out);
    const meta = await readJson(join(directory, "dataset.json"), datasetSchema);
    const preparedFile = join(directory, meta.prepared),
        prepared = await readJson(preparedFile, preparedSchema, 32 * 1024 ** 2);
    const referenceFile = join(directory, meta.reference),
        reference = await readJson(referenceFile, graphReferenceSchema, 3 * 1024 ** 2);
    if (
        (await fileHash(preparedFile)) !== meta.artifacts[meta.prepared] ||
        (await fileHash(referenceFile)) !== meta.artifacts[meta.reference] ||
        Number(prepared.sourceAsset.scale) !== meta.scaleFactor ||
        reference.dataset.manifestSha256 !== meta.artifacts[meta.prepared] ||
        reference.dataset.scaleFactor !== meta.scaleFactor ||
        reference.dataset.vertices !== meta.vertices ||
        reference.dataset.edges !== meta.edges
    )
        throw new BenchError("GRAPH_DATASET", "Prepared manifest/reference identity, scale or counts differ");
    for (const category of ["vertices", "edges"] as const)
        if (
            prepared.shards.filter((s) => s.category === category).reduce((sum, s) => sum + BigInt(s.records), 0n) !==
            BigInt(meta[category])
        )
            throw new BenchError("GRAPH_DATASET", "Source shard counts differ from the dataset totals");
    const destination = target(options.graphUrl),
        minimum = options.minimumEdgeCoverage ?? 0.95;
    if (!Number.isFinite(minimum) || minimum <= 0 || minimum > 1)
        throw new BenchError("GRAPH_IMPORT", "minimum-edge-coverage must be greater than zero and at most one");
    const home = resolve(options.loaderHome),
        launcher = join(home, "bin/hugegraph-loader.sh");
    if (!(await lstat(launcher)).isFile()) throw new BenchError("GRAPH_LOADER", "Missing bin/hugegraph-loader.sh");
    const tuning = options.loader ?? graphProjectSchema.shape.loader.parse({});
    const budget = options.budgetMs ?? 6 * 3600000;
    if (!Number.isSafeInteger(budget) || budget < 1000 || budget > 7 * 86400000)
        throw new BenchError("GRAPH_IMPORT_BUDGET", "Import budget must be 1000..604800000 ms");
    const preview = {
        scaleFactor: meta.scaleFactor,
        vertices: meta.vertices,
        edges: meta.edges,
        graphUrl: options.graphUrl,
        minimumEdgeCoverage: minimum,
        verification: "loader-acknowledgements",
        fullContentScan: false,
        requiredCompleteRelation: "person_knows_person",
        budgetMs: budget,
        out,
        prerequisites: [
            "exclusive benchmark graph",
            "empty data and user schema on first import",
            "Loader supporting --retry-times 0",
        ],
    };
    if (options.dryRun) return { ...preview, dispatched: false, written: false };
    const existed = await lstat(out).then(
        () => true,
        (e) => {
            if (e.code === "ENOENT") return false;
            throw e;
        },
    );
    if (existed && !options.resume)
        throw new BenchError("GRAPH_IMPORT_EXISTS", "Use --resume for this import or choose a new output");
    await mkdir(out, { recursive: true, mode: 0o700 });
    const unlock = await acquireLock(out);
    const signal = AbortSignal.any([...(options.signal ? [options.signal] : []), AbortSignal.timeout(budget)]);
    const end = performance.now() + budget;
    const { acknowledgeLoad } = await import(pathToFileURL(join(packageRoot, "tools/ldbc-load-ack.mjs")).href);
    const { importSequential } = await import(pathToFileURL(join(packageRoot, "tools/ldbc-import-sequence.mjs")).href);
    const runtimeCache = new Map<string, string>();
    try {
        const component = {
            protocolVersion: 1,
            kind: "sf100-component",
            role: "loader",
            id: "graph-loader",
            version: "local-installation",
            command: [launcher],
            trackedFiles: await runtimeFiles(home),
            capabilities: {
                scale: String(meta.scaleFactor),
                serializer: "CsvBasic",
                operations: [],
                checkpoint: "reconcile-before-retry",
            },
        };
        const identity = {
            source: meta.artifacts[meta.prepared],
            target: destination,
            alias: options.traversalSource,
            tuning,
            minimum,
            component: hash(component),
            authorizationEnv: options.authorizationEnv ?? null,
        };
        const stateFile = join(out, "state.json");
        let state: any = existed
            ? JSON.parse(await readFile(stateFile, "utf8"))
            : {
                  schemaVersion: 1,
                  kind: "graph-import",
                  identity,
                  stage: "new",
                  completedShards: 0,
                  current: null,
                  acknowledgements: [],
              };
        if (hash(state.identity) !== hash(identity))
            throw new BenchError(
                "GRAPH_IMPORT_CHANGED",
                "Source, target, Loader or options changed; do not replay this import",
            );
        const save = async () => durableJson(stateFile, state);
        const query = async (gremlin: string, expected: string) => {
            signal.throwIfAborted();
            const authorization = options.authorizationEnv ? process.env[options.authorizationEnv] : undefined;
            if (options.authorizationEnv && (!authorization || !/^Bearer [^\s]+$/.test(authorization)))
                throw new BenchError("GRAPH_AUTH", "Use an environment variable containing Bearer authorization");
            const response = await fetch(destination.origin + "/gremlin", {
                method: "POST",
                redirect: "manual",
                headers: {
                    "Content-Type": "application/json",
                    ...(authorization ? { Authorization: authorization } : {}),
                },
                body: JSON.stringify({ gremlin, aliases: { g: options.traversalSource } }),
                signal: AbortSignal.any([
                    signal,
                    AbortSignal.timeout(Math.min(180000, Math.max(1, end - performance.now()))),
                ]),
            });
            if (response.status !== 200) {
                await response.body?.cancel();
                throw new BenchError(
                    "GRAPH_TARGET",
                    `Preparation request failed, HTTP ${response.status}; import stopped`,
                );
            }
            const reader = response.body!.getReader(),
                chunks: Uint8Array[] = [];
            let bytes = 0;
            try {
                for (;;) {
                    const chunk = await reader.read();
                    if (chunk.done) break;
                    bytes += chunk.value.length;
                    if (bytes > 1024 ** 2) throw new BenchError("GRAPH_TARGET", "Preparation response exceeds 1 MiB");
                    chunks.push(chunk.value);
                }
            } finally {
                await reader.cancel();
            }
            const result = JSON.parse(Buffer.concat(chunks).toString("utf8"));
            if (
                (result.status?.code !== undefined && result.status.code !== 200) ||
                JSON.stringify(result.result?.data) !== JSON.stringify([expected])
            )
                throw new BenchError(
                    "GRAPH_TARGET",
                    "Unexpected preparation response; target state requires inspection",
                );
        };
        if (state.stage === "schema-dispatched")
            throw new BenchError(
                "GRAPH_SCHEMA_UNKNOWN",
                "Prior schema write outcome is unknown; inspect the graph before continuing",
            );
        const componentPath = join(out, "component.json");
        await writeJson(componentPath, component);
        const config = {
            schemaVersion: 1,
            planDigest: hash(identity),
            datasetDigest: meta.artifacts[meta.prepared],
            mappingDigest: hash({ schema: prepared.files["schema.groovy"], converter: prepared.sourceConverterSha256 }),
            componentManifest: componentPath,
            componentSha256: await fileHash(componentPath),
            target: {
                ...destination,
                origin: undefined,
                epoch: "graph-" + hash(identity).slice(0, 24),
                topologyDigest: hash(destination),
            },
            schemaAlreadyApplied: true,
            ...tuning,
            checkVertex: false,
            ...(options.authorizationEnv ? { tokenEnv: options.authorizationEnv } : {}),
        };
        delete (config.target as any).origin;
        await writeJson(join(out, "loader.json"), config);
        const planned = await planLoaderJobs(preparedFile, join(out, "loader.json"));
        const rank = (job: any) =>
            job.mapping.vertices.length ? 0 : job.mapping.edges[0].label === "person_knows_person" ? 1 : 2;
        const jobs = planned.jobs.sort((a, b) => rank(a) - rank(b));
        if (state.completedShards !== state.acknowledgements.length || state.completedShards > jobs.length)
            throw new BenchError("GRAPH_IMPORT_STATE", "Acknowledged prefix differs; inspect the original evidence");
        for (const [i, fact] of state.acknowledgements.entries()) {
            const ack = JSON.parse(await readFile(join(out, `shard-${String(i).padStart(6, "0")}/ack.json`), "utf8"));
            if (
                hash(ack) !== fact ||
                ack.requestDigest !== hash(jobs[i]!.request) ||
                ack.records !== jobs[i]!.request.shard!.records
            )
                throw new BenchError(
                    "GRAPH_IMPORT_STATE",
                    "A committed acknowledgement changed; no write was replayed",
                );
        }
        if (state.stage === "new") {
            await save();
            await query(
                "if(g.V().hasNext() || g.E().hasNext() || g.getGraph().schema().getVertexLabels().size()>0 || g.getGraph().schema().getEdgeLabels().size()>0 || g.getGraph().schema().getPropertyKeys().size()>0) throw new IllegalStateException('Use a new empty benchmark graph'); 'hgbench-empty-v1'",
                "hgbench-empty-v1",
            );
            const schema = await readFile(join(dirname(preparedFile), "schema.groovy"), "utf8");
            const expectedSchema = prepared.files["schema.groovy"];
            if (
                !expectedSchema ||
                (await fileHash(join(dirname(preparedFile), "schema.groovy"))) !== expectedSchema.sha256
            )
                throw new BenchError("GRAPH_SCHEMA", "Schema bytes differ from the prepared manifest");
            state.stage = "schema-dispatched";
            await save();
            await query(
                "def graph=g.getGraph(); def schema=graph.schema();\n" + schema + "\n'hgbench-schema-v1'",
                "hgbench-schema-v1",
            );
            state.stage = "loading";
            await save();
        }
        const result = await importSequential({
            jobs,
            prefixCount: state.completedShards,
            counts: { vertices: String(meta.vertices), edges: String(meta.edges) },
            vertexAdmission: "acknowledged-writes",
            minimumEdgeCoverage: minimum,
            requiredEdgeLabels: ["person_knows_person"],
            finalVerification: "write-acknowledgements",
            stopped: () => signal.aborted,
            load: async (job: any, index: number) => {
                const attempt = join(out, `shard-${String(index).padStart(6, "0")}`);
                const jobFile = join(out, `job-${String(index).padStart(6, "0")}.json`);
                const existing = await lstat(attempt).then(
                    () => true,
                    (e) => {
                        if (e.code === "ENOENT") return false;
                        throw e;
                    },
                );
                if (!existing) {
                    await writeJson(jobFile, job);
                    const process = await runLoaderJob(jobFile, attempt, true, { signal, runtimeCache });
                    await writeJson(join(attempt, "process-result.json"), process);
                }
                let ack;
                try {
                    ack = await acknowledgeLoad(job, attempt);
                } catch {
                    throw new BenchError(
                        "GRAPH_IMPORT_UNKNOWN",
                        `Shard ${index} has an unconfirmed write outcome. Inspect ${attempt}; automatic replay is disabled`,
                        4,
                    );
                }
                await writeJson(join(attempt, "ack.json"), ack);
                state.acknowledgements.push(hash(ack));
                state.completedShards = index + 1;
                state.current = null;
                await save();
                return ack;
            },
            event: async (event: any) => {
                state.stage = event.stage;
                if (event.stage === "load") state.current = event.shardIndex;
                await save();
            },
        });
        const coverage = result.coverage;
        const snapshot = graphImportSnapshotSchema.parse({
            schemaVersion: 1,
            kind: "graph-import-snapshot",
            sourceManifestSha256: meta.artifacts[meta.prepared],
            acknowledgedPrefixSha256: hash(state.acknowledgements),
            vertices: Number(result.counts.vertices),
            edges: Number(result.counts.edges),
            expectedVertices: meta.vertices,
            expectedEdges: meta.edges,
            writersStopped: true,
            basis: "loader-acknowledgements",
            vertexContentVerified: false,
            edgeContentVerified: false,
            labels: coverage.labels,
        });
        await durableJson(join(out, "snapshot.json"), snapshot);
        state.stage = "completed";
        state.snapshot = join(out, "snapshot.json");
        state.coverage = coverage;
        await save();
        return {
            ...preview,
            status: "completed",
            snapshot: state.snapshot,
            coverage,
            datasetVerified: false,
            performanceAccepted: false,
        };
    } finally {
        await unlock();
    }
}

export async function runGraphProject(
    file: string,
    options: {
        out: string;
        root: string;
        preset?: string;
        budgetMs?: number;
        dryRun?: boolean;
        resume?: boolean;
        signal?: AbortSignal;
    },
) {
    const config = await readJson(file, graphProjectSchema),
        base = dirname(resolve(file));
    target(config.graphUrl);
    const preset = options.preset ?? "both";
    if (!["quick", "standard", "both"].includes(preset))
        throw new BenchError("GRAPH_PRESET", "Use quick, standard or both");
    const budget = options.budgetMs ?? 6 * 3600000;
    if (!Number.isSafeInteger(budget) || budget < 1000 || budget > 7 * 86400000)
        throw new BenchError("GRAPH_PROJECT_BUDGET", "budget-ms must be 1000..604800000");
    const out = resolve(options.out),
        dataset = config.dataset ? resolve(base, config.dataset) : join(out, "dataset");
    const profile = resolve(base, config.profile),
        loaderHome = resolve(base, config.loaderHome);
    const input = await inspectHttpInput(profile);
    if (input.profile.schemaVersion !== 2) throw new BenchError("GRAPH_EXECUTOR", "Use a registered Go profile");
    if (options.dryRun)
        return {
            scaleFactor: config.scaleFactor,
            dataset,
            out,
            preset,
            budgetMs: budget,
            stages: [
                config.dataset ? "reuse-prepared-dataset" : "prepare-dataset",
                "import-by-acknowledgements",
                "prepare-graph-profile",
                ...(preset === "both" ? ["quick", "standard"] : [preset]),
            ],
            verification:
                "source digest and counts, Loader acknowledgements, complete knows relation, query answers; full target scans are opt-in",
            dispatched: false,
            written: false,
        };
    const signal = AbortSignal.any([...(options.signal ? [options.signal] : []), AbortSignal.timeout(budget)]);
    const end = performance.now() + budget;
    const remaining = () => {
        signal.throwIfAborted();
        return Math.max(1, Math.floor(end - performance.now()));
    };
    const existed = await lstat(out).then(
        () => true,
        (e) => {
            if (e.code === "ENOENT") return false;
            throw e;
        },
    );
    if (existed && !options.resume)
        throw new BenchError("GRAPH_PROJECT_EXISTS", "Choose a new output or --resume the original project");
    await mkdir(out, { recursive: true, mode: 0o700 });
    const unlock = await acquireLock(out);
    try {
        const settings = config.settings ? resolve(base, config.settings) : undefined;
        const identity = {
            config,
            profile: await fileHash(profile),
            settings: settings ? await fileHash(settings) : null,
            dataset,
            loaderHome,
            preset,
        };
        const identityFile = join(out, "project.json");
        if (existed) {
            if (hash(JSON.parse(await readFile(identityFile, "utf8"))) !== hash(identity))
                throw new BenchError(
                    "GRAPH_PROJECT_CHANGED",
                    "Project, profile or selected presets changed; use a new workspace",
                );
        } else await durableJson(identityFile, identity);
        if (!config.dataset) {
            const datasetExists = await lstat(dataset).then(
                () => true,
                (e) => {
                    if (e.code === "ENOENT") return false;
                    throw e;
                },
            );
            await prepareGraphDataset(
                { out: dataset, scale: config.scaleFactor, resume: datasetExists && options.resume },
                signal,
            );
        }
        const meta = await readJson(join(dataset, "dataset.json"), datasetSchema);
        if (meta.scaleFactor !== config.scaleFactor)
            throw new BenchError("GRAPH_SCALE", "Project scale differs from the prepared dataset");
        const imported = await importGraphDataset(dataset, {
            out: join(out, "import"),
            loaderHome,
            graphUrl: config.graphUrl,
            traversalSource: config.traversalSource,
            loader: config.loader,
            authorizationEnv: input.profile.authorizationEnv,
            minimumEdgeCoverage: config.minimumEdgeCoverage,
            budgetMs: remaining(),
            signal,
            resume: options.resume,
        });
        if (!("snapshot" in imported))
            throw new BenchError("GRAPH_IMPORT", "Import did not return a completed snapshot");
        const graphProfile = join(out, "graph.json");
        await prepareGraphProfile(join(dataset, meta.reference), profile, {
            out: graphProfile,
            graphUrl: config.graphUrl,
            traversalSource: config.traversalSource,
            importSnapshot: imported.snapshot,
            minEdgeCoverage: config.minimumEdgeCoverage,
            reuse: options.resume,
        });
        const reports: any[] = [];
        for (const name of preset === "both" ? ["quick", "standard"] : [preset]) {
            const previous = join(out, name);
            const present = await lstat(previous).then(
                () => true,
                (e) => {
                    if (e.code === "ENOENT") return false;
                    throw e;
                },
            );
            if (present) {
                const state = JSON.parse(await readFile(join(previous, "state.json"), "utf8"));
                if (
                    !options.resume ||
                    state.status !== "completed" ||
                    state.inputSha256 !== (await fileHash(graphProfile))
                )
                    throw new BenchError(
                        "GRAPH_MEASUREMENT_EXISTS",
                        `Inspect ${previous}/state.json and its active task. Incomplete measurement attempts are not replayed; graph run can use a NEW output`,
                        4,
                    );
                reports.push({
                    ...state,
                    reportJson: join(previous, "report/report.json"),
                    reportHtml: join(previous, "report/report.html"),
                    reused: true,
                });
                continue;
            }
            const result = await runHttpThroughput(graphProfile, {
                out: join(out, name),
                root: options.root,
                preset: name,
                track: "graph",
                budgetMs: remaining(),
                signal,
                settings,
            });
            reports.push(result);
            if ("status" in result && result.status !== "completed") break;
        }
        const result = {
            status: reports.length && reports.every((r) => r.status === "completed") ? "completed" : "partial",
            scaleFactor: meta.scaleFactor,
            graphProfile,
            imported,
            reports,
            performanceAccepted: false,
        };
        await durableJson(join(out, "project-result.json"), result);
        return result;
    } finally {
        await unlock();
    }
}

export async function graphStatus(directory: string) {
    const out = resolve(directory),
        states: Record<string, unknown> = {};
    for (const file of [
        "prepare-state.json",
        "state.json",
        "dataset/prepare-state.json",
        "import/state.json",
        "quick/state.json",
        "standard/state.json",
    ]) {
        const path = join(out, file);
        let info;
        try {
            info = await lstat(path);
        } catch (e) {
            if ((e as NodeJS.ErrnoException).code === "ENOENT") continue;
            throw e;
        }
        if (!info.isFile() || info.size > 8 * 1024 ** 2)
            throw new BenchError("GRAPH_STATUS", "Use bounded regular state files");
        const state = JSON.parse(await readFile(path, "utf8"));
        states[file] = {
            kind: state.kind,
            stage: state.stage,
            status: state.status,
            scaleFactor: state.scaleFactor ?? state.identity?.scaleFactor,
            completedShards: state.completedShards,
            currentShard: state.current,
            coverage: state.coverage,
            activeTask: state.activeTask,
            failure: state.failure,
            incomplete: state.incomplete,
        };
    }
    if (!Object.keys(states).length)
        throw new BenchError("GRAPH_STATUS", "No graph preparation, import or measurement state was found");
    return {
        directory: out,
        states,
        scope: "Local progress records only; no target queries, scans or process launches",
    };
}
