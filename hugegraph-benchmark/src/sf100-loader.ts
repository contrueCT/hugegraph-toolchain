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

import { z } from "zod";
import { spawn } from "node:child_process";
import { mkdir, open, lstat } from "node:fs/promises";
import { dirname, isAbsolute, join, resolve } from "node:path";
import { BenchError, fileHash, hash, readJson, writeJson } from "./io.js";
import { sf100ComponentManifestSchema, sf100SuiteId } from "./sf100.js";
import {
    sf100RequestSchema,
    validateSf100Request,
    validateSf100Receipt,
    verifyProtocolArtifacts,
} from "./sf100-protocol.js";
import { checkpointSf100 } from "./sf100-checkpoints.js";

const sha = z.string().regex(/^[a-f0-9]{64}$/);
const positive = z.number().int().positive().max(Number.MAX_SAFE_INTEGER);
const count = z.string().regex(/^(0|[1-9][0-9]*)$/);
const input = z.strictObject({
    type: z.literal("file"),
    path: z.string(),
    format: z.literal("JSON"),
    compression: z.enum(["NONE", "GZIP"]).optional(),
});
const encoding = z.strictObject({
    compression: z.literal("gzip"),
    uncompressedBytes: count,
    uncompressedSha256: sha,
});
const mapping = z.object({ label: z.string(), input }).passthrough();
const artifact = z.object({ sha256: sha, bytes: z.union([count, z.number().int().nonnegative().safe()]) });
export const preparedSchema = z
    .object({
        schemaVersion: z.union([z.literal(1), z.literal(2)]),
        scope: z.literal("sharded-loader-input"),
        sourceConverterSha256: sha,
        sourceAsset: z.object({ scale: z.enum(["1", "10", "100"]), role: z.literal("initial"), sha256: sha }),
        files: z.record(z.string(), artifact),
        shards: z
            .array(
                z.object({
                    id: z.string(),
                    category: z.enum(["vertices", "edges"]),
                    label: z.string(),
                    path: z.string(),
                    records: count,
                    bytes: count,
                    sha256: sha,
                    encoding: encoding.optional(),
                }),
            )
            .min(1),
    })
    .refine(
        (value) => value.schemaVersion === 2 || !value.shards.some((shard) => shard.encoding),
        "Compressed shards require prepared schemaVersion 2",
    );
export const loaderConfigSchema = z.strictObject({
    schemaVersion: z.literal(1),
    planDigest: sha,
    datasetDigest: sha,
    mappingDigest: sha,
    componentManifest: z.string().min(1),
    componentSha256: sha,
    target: z.strictObject({
        host: z.string().regex(/^https?:\/\/[a-zA-Z0-9.-]+$/),
        port: positive.max(65535),
        graph: z.string().regex(/^[a-zA-Z][a-zA-Z0-9_-]*$/),
        graphSpace: z
            .string()
            .regex(/^[a-zA-Z][a-zA-Z0-9_-]*$/)
            .optional(),
        epoch: z.string().min(1),
        topologyDigest: sha,
    }),
    schemaAlreadyApplied: z.literal(true),
    batchSize: positive,
    batchThreads: positive,
    parserThreads: positive,
    // Disable only when endpoint vertices and label mappings have already been validated.
    checkVertex: z.boolean().optional(),
    heapMiB: positive,
    timeoutSeconds: positive,
    processTimeoutMs: positive.max(86400000),
    maxLogBytes: positive.max(64 * 1024 * 1024),
    tokenEnv: z
        .string()
        .regex(/^[A-Z][A-Z0-9_]{0,63}$/)
        .optional(),
});
const runtimeSchema = z.strictObject({
    command: z.array(z.string()).min(1),
    files: z.array(z.strictObject({ path: z.string(), sha256: sha })).min(1),
});
export const loaderJobSchema = z
    .strictObject({
        schemaVersion: z.union([z.literal(1), z.literal(2)]),
        kind: z.literal("sf100-loader-job"),
        request: sf100RequestSchema,
        config: loaderConfigSchema,
        runtime: runtimeSchema,
        inputRoot: z.string(),
        mapping: z.strictObject({ vertices: z.array(mapping), edges: z.array(mapping) }),
        encoding: encoding.optional(),
        prerequisites: z.array(z.string()),
        digest: sha,
    })
    .refine((value) => value.schemaVersion === 2 || !value.encoding, "Compressed jobs require schemaVersion 2");
type LoaderJob = z.infer<typeof loaderJobSchema>;

export async function planLoaderJobs(preparedFile: string, configFile: string) {
    const prepared = await readJson(preparedFile, preparedSchema, 32 * 1024 * 1024);
    const config = await readJson(configFile, loaderConfigSchema);
    const manifestPath = resolve(dirname(resolve(configFile)), config.componentManifest);
    const manifest = await readJson(manifestPath, sf100ComponentManifestSchema);
    if (
        manifest.role !== "loader" ||
        manifest.capabilities.scale !== prepared.sourceAsset.scale ||
        manifest.capabilities.checkpoint !== "reconcile-before-retry" ||
        (await fileHash(manifestPath)) !== config.componentSha256
    ) {
        throw new BenchError("LOADER_COMPONENT_INVALID", "Loader manifest role, recovery policy or identity mismatch");
    }
    if (
        config.mappingDigest !==
        hash({
            schema: prepared.files["schema.groovy"],
            converter: prepared.sourceConverterSha256,
        })
    ) {
        throw new BenchError(
            "LOADER_MAPPING_MISMATCH",
            "mappingDigest must bind prepared schema metadata and sourceConverterSha256",
        );
    }
    const inputRoot = dirname(resolve(preparedFile));
    const struct = await readJson(
        join(inputRoot, "struct.json"),
        z.object({ vertices: z.array(mapping), edges: z.array(mapping) }),
        32 * 1024 * 1024,
    );
    const fact = prepared.files["struct.json"];
    if (!fact) throw new BenchError("LOADER_INPUT_INVALID", "Missing frozen struct.json identity");
    await verifyProtocolArtifacts(inputRoot, [{ path: "struct.json", sha256: fact.sha256, bytes: String(fact.bytes) }]);
    if (
        new Set(prepared.shards.map((s) => s.id)).size !== prepared.shards.length ||
        new Set(prepared.shards.map((s) => s.path)).size !== prepared.shards.length
    ) {
        throw new BenchError("LOADER_INPUT_INVALID", "Duplicate shard ID/path");
    }
    const base = dirname(manifestPath);
    const runtime = {
        command: manifest.command.map((arg, i) => (i === 0 || arg.startsWith("./") ? resolve(base, arg) : arg)),
        files: [
            { path: manifestPath, sha256: config.componentSha256 },
            ...manifest.trackedFiles.map((f) => ({ path: resolve(base, f.path), sha256: f.sha256 })),
        ],
    };
    if (!manifest.command[0]!.startsWith("./") && !isAbsolute(manifest.command[0]!)) {
        throw new BenchError(
            "LOADER_COMPONENT_INVALID",
            "Launcher must use an explicit absolute or ./ path, not PATH lookup",
        );
    }
    if (!runtime.files.some((f) => f.path === runtime.command[0]))
        throw new BenchError("LOADER_COMPONENT_INVALID", "Launcher must be tracked by SHA-256");
    const preparedDigest = await fileHash(preparedFile);
    const jobs: LoaderJob[] = [];
    for (const shard of prepared.shards) {
        const rows = struct[shard.category].filter((m) => m.label === shard.label && m.input.path === shard.path);
        const file = prepared.files[shard.path];
        if (rows.length !== 1 || !file || file.sha256 !== shard.sha256 || String(file.bytes) !== shard.bytes)
            throw new BenchError("LOADER_INPUT_INVALID", "Shard/mapping/file inventory mismatch");
        if ((rows[0]!.input.compression === "GZIP") !== Boolean(shard.encoding))
            throw new BenchError(
                "LOADER_INPUT_INVALID",
                "Compression requires matching frozen expanded content identity",
            );
        const request = validateSf100Request({
            protocolVersion: 1,
            kind: "sf100-request",
            suite: prepared.sourceAsset.scale === "100" ? sf100SuiteId : "hgbench-graph-query-v0.1",
            requestId: `load-${hash({ config, prepared: preparedDigest, shard: shard.id }).slice(0, 32)}`,
            attemptId: "initial",
            identity: {
                planDigest: config.planDigest,
                datasetDigest: config.datasetDigest,
                mappingDigest: config.mappingDigest,
                componentDigest: config.componentSha256,
            },
            stage: "load",
            target: { targetDigest: hash(config.target), epoch: config.target.epoch },
            shard: { id: shard.id, records: shard.records },
            repetition: null,
            inputs: [{ name: "shard", path: shard.path, sha256: shard.sha256, bytes: shard.bytes }],
            outputBudgetBytes: String(config.maxLogBytes),
        });
        const content = {
            schemaVersion: shard.encoding ? (2 as const) : (1 as const),
            kind: "sf100-loader-job" as const,
            request,
            config,
            runtime,
            inputRoot,
            ...(shard.encoding ? { encoding: shard.encoding } : {}),
            mapping: {
                vertices: shard.category === "vertices" ? [rows[0]] : [],
                edges: shard.category === "edges" ? [rows[0]] : [],
            },
            prerequisites:
                shard.category === "edges"
                    ? ["all-vertices-reconciled", "schema-applied", "target-epoch-confirmed"]
                    : ["schema-applied", "target-epoch-confirmed"],
        };
        jobs.push(loaderJobSchema.parse({ ...content, digest: hash(content) }));
    }
    return {
        jobs,
        executed: false,
        liveVerified: false,
        order: "vertices-before-edges; reconcile each submitted shard",
    };
}

export function loaderArguments(job: LoaderJob, mappingPath: string): string[] {
    const c = job.config;
    return [
        ...job.runtime.command.slice(1),
        "--file",
        mappingPath,
        "--host",
        c.target.host,
        "--port",
        String(c.target.port),
        "--graph",
        c.target.graph,
        ...(c.target.graphSpace ? ["--graphspace", c.target.graphSpace] : []),
        "--batch-size",
        String(c.batchSize),
        "--batch-insert-threads",
        String(c.batchThreads),
        "--parser-threads",
        String(c.parserThreads),
        "--timeout",
        String(c.timeoutSeconds),
        "--retry-times",
        "0",
        "--batch-failure-fallback",
        "false",
        "--max-read-errors",
        "1",
        "--max-parse-errors",
        "1",
        "--max-insert-errors",
        "1",
        "--incremental-mode",
        "false",
        "--failure-mode",
        "false",
        "--check-vertex",
        String(c.checkVertex ?? true),
        "--create-graph",
        "false",
    ];
}

export async function runLoaderJob(
    jobFile: string,
    output: string,
    execute = false,
    options: { signal?: AbortSignal; runtimeCache?: Map<string, string> } = {},
) {
    const job = await readJson(jobFile, loaderJobSchema, 32 * 1024 * 1024);
    const { digest, ...content } = job;
    if (hash(content) !== digest) throw new BenchError("LOADER_JOB_CHANGED", "Loader job digest mismatch", 5);
    validateSf100Request(job.request);
    if (job.request.stage !== "load" || job.request.target?.targetDigest !== hash(job.config.target))
        throw new BenchError("LOADER_JOB_INVALID", "Load job target/request mismatch");
    if (
        job.request.identity.planDigest !== job.config.planDigest ||
        job.request.identity.datasetDigest !== job.config.datasetDigest ||
        job.request.identity.mappingDigest !== job.config.mappingDigest ||
        job.request.identity.componentDigest !== job.config.componentSha256 ||
        job.request.target?.epoch !== job.config.target.epoch
    )
        throw new BenchError("LOADER_JOB_INVALID", "Job identity differs from request");
    const entries = [...job.mapping.vertices, ...job.mapping.edges];
    if (
        entries.length !== 1 ||
        entries[0]?.input?.path !== job.request.inputs[0]?.path ||
        entries[0]?.input?.format !== "JSON"
    )
        throw new BenchError("LOADER_JOB_INVALID", "Job mapping must reference exactly the requested JSONL shard");
    if ((entries[0]!.input.compression === "GZIP") !== Boolean(job.encoding))
        throw new BenchError("LOADER_JOB_INVALID", "Compression and expanded content identity differ");
    const out = resolve(output);
    const args = loaderArguments(job, join(out, "struct.json"));
    if (!execute)
        return {
            executed: false,
            command: [job.runtime.command[0], ...args],
            environment: { JVM_OPTS: `-Xmx${job.config.heapMiB}m` },
            prerequisites: job.prerequisites,
            outcomePolicy: "always-reconcile-after-dispatch",
        };
    if (process.platform === "win32")
        throw new BenchError("UNSUPPORTED_PLATFORM", "Loader supervision requires POSIX process groups", 3);
    await verifyProtocolArtifacts(job.inputRoot, job.request.inputs);
    for (const f of job.runtime.files) {
        const info = await lstat(f.path, { bigint: true });
        const signature = `${f.sha256}:${info.dev}:${info.ino}:${info.size}:${info.mtimeNs}:${info.ctimeNs}`;
        if (
            !info.isFile() ||
            (options.runtimeCache?.get(f.path) !== signature && (await fileHash(f.path)) !== f.sha256)
        )
            throw new BenchError("LOADER_COMPONENT_CHANGED", `Tracked runtime file changed: ${f.path}`, 5);
        options.runtimeCache?.set(f.path, signature);
    }
    options.signal?.throwIfAborted();
    const token = job.config.tokenEnv ? process.env[job.config.tokenEnv] : undefined;
    if (job.config.tokenEnv && !token)
        throw new BenchError("LOADER_AUTH", "Configured Loader token environment variable is missing");
    await mkdir(out, { recursive: false, mode: 0o700 }); // Never replay into an existing attempt directory.
    const mapping = structuredClone(job.mapping) as {
        vertices: { input: { path: string } }[];
        edges: { input: { path: string } }[];
    };
    for (const entry of [...mapping.vertices, ...mapping.edges])
        entry.input.path = resolve(job.inputRoot, entry.input.path);
    await writeJson(join(out, "struct.json"), mapping);
    await writeJson(join(out, "request.json"), job.request);
    await checkpointSf100(out, join(out, "request.json"));
    const logs = await Promise.all(["stdout.log", "stderr.log"].map((name) => open(join(out, name), "wx", 0o600)));
    const child = spawn(
        job.runtime.command[0]!,
        [...args, ...(token ? ["--token", token.replace(/^Bearer /, "")] : [])],
        {
            cwd: out,
            env: { ...process.env, JVM_OPTS: `-Xmx${job.config.heapMiB}m` },
            stdio: ["ignore", "pipe", "pipe"],
            detached: true,
        },
    );
    let reason = "RECONCILIATION_REQUIRED",
        bytes = 0,
        killer: NodeJS.Timeout | undefined;
    const stop = (code: string) => {
        reason = code;
        if (!child.pid) return;
        try {
            process.kill(-child.pid, "SIGTERM");
        } catch {
            /* Process may already have exited. */
        }
        killer ??= setTimeout(() => {
            try {
                process.kill(-child.pid!, "SIGKILL");
            } catch {
                /* Already stopped. */
            }
        }, 2000);
    };
    const cancelled = () => stop("CANCELLED");
    process.on("SIGINT", cancelled);
    process.on("SIGTERM", cancelled);
    options.signal?.addEventListener("abort", cancelled, { once: true });
    if (options.signal?.aborted) cancelled();
    const timer = setTimeout(() => stop("PROCESS_TIMEOUT"), job.config.processTimeoutMs);
    let spawnFailed = false;
    const completion = new Promise<number | null>((resolveDone) => {
        child.once("error", () => {
            spawnFailed = true;
            reason = "SPAWN_FAILED";
            resolveDone(null);
        });
        child.once("close", resolveDone);
    });
    const drains = [child.stdout!, child.stderr!].map(async (stream, index) => {
        for await (const chunk of stream) {
            if (bytes + chunk.length > job.config.maxLogBytes) {
                stop("LOG_BUDGET_EXCEEDED");
                continue;
            }
            bytes += chunk.length;
            await logs[index]!.write(chunk);
        }
    });
    let exitCode: number | null;
    try {
        [exitCode] = await Promise.all([completion, ...drains]);
    } catch (error) {
        stop("SUPERVISION_FAILED");
        await completion;
        await Promise.allSettled(drains);
        throw error;
    } finally {
        clearTimeout(timer);
        if (killer) clearTimeout(killer);
        process.off("SIGINT", cancelled);
        process.off("SIGTERM", cancelled);
        options.signal?.removeEventListener("abort", cancelled);
        await Promise.all(logs.map((log) => log.close()));
    }
    if (exitCode !== 0 && reason === "RECONCILIATION_REQUIRED") reason = "LOADER_PROCESS_FAILED";
    const artifacts = await Promise.all(
        ["stdout.log", "stderr.log"].map(async (path, index) => ({
            name: index === 0 ? "stdout" : "stderr",
            path,
            sha256: await fileHash(join(out, path)),
            bytes: String((await lstat(join(out, path))).size),
        })),
    );
    const receipt = validateSf100Receipt(job.request, {
        protocolVersion: 1,
        kind: "sf100-receipt",
        requestId: job.request.requestId,
        attemptId: job.request.attemptId,
        requestDigest: hash(job.request),
        status: reason === "CANCELLED" ? "cancelled" : "failed",
        effect: spawnFailed ? "none" : "unknown",
        records: null,
        artifacts,
        error: {
            code: reason,
            message: "Loader process ended; database state must be reconciled before reuse or retry",
        },
    });
    await writeJson(join(out, "receipt.json"), receipt);
    await checkpointSf100(out, join(out, "request.json"), join(out, "receipt.json"), out);
    return {
        executed: !spawnFailed,
        processExitCode: exitCode,
        loadVerified: false,
        receiptPath: join(out, "receipt.json"),
        nextAction: "reconcile",
        reason,
    };
}
