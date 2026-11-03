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

import assert from "node:assert/strict";
import { test } from "node:test";
import { mkdtemp, readFile, writeFile, rm, mkdir, chmod, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createHash } from "node:crypto";
import { gzipSync } from "node:zlib";
import { hash } from "../dist/io.js";
import { createServer } from "node:http";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { planLoaderJobs, runLoaderJob, loaderArguments } from "../dist/sf100-loader.js";
import { assessSf100Recovery } from "../dist/sf100-protocol.js";

const sha = (data) => createHash("sha256").update(data).digest("hex");
async function fixture(
    fn,
    script = '#!/bin/sh\nprintf "%s\n" "$@"\nprintf "%s\n" "$JVM_OPTS"\nexit 0\n',
    options = {},
) {
    const dir = await mkdtemp(join(tmpdir(), "hgbench-loader-"));
    try {
        const put = async (path, value) => {
            await writeFile(join(dir, path), value);
            return { sha256: sha(value), bytes: Buffer.byteLength(value) };
        };
        await mkdir(join(dir, "vertices"));
        await mkdir(join(dir, "edges"));
        const files = {
            "vertices/person-000000.json": await put(
                "vertices/person-000000.json",
                '{"vertexId":"person:1","ldbcId":1}\n',
            ),
            "edges/person_knows_person-000000.json": await put(
                "edges/person_knows_person-000000.json",
                '{"sourceId":"person:1","targetId":"person:1","edgeKey":1}\n',
            ),
            "schema.groovy": await put("schema.groovy", "// test-only schema\n"),
        };
        const struct = {
            vertices: [
                {
                    label: "person",
                    id: "vertexId",
                    input: { type: "file", path: "vertices/person-000000.json", format: "JSON" },
                },
            ],
            edges: [
                {
                    label: "person_knows_person",
                    source: ["sourceId"],
                    target: ["targetId"],
                    input: { type: "file", path: "edges/person_knows_person-000000.json", format: "JSON" },
                },
            ],
        };
        files["struct.json"] = await put("struct.json", JSON.stringify(struct));
        const prepared = {
            schemaVersion: 1,
            scope: "sharded-loader-input",
            sourceAsset: { scale: "100", role: "initial", sha256: "a".repeat(64) },
            sourceConverterSha256: "b".repeat(64),
            files,
            shards: Object.entries(struct).flatMap(([category, entries]) =>
                entries.map((row) => ({
                    id: category + "-" + row.label,
                    category,
                    label: row.label,
                    path: row.input.path,
                    ...files[row.input.path],
                    bytes: String(files[row.input.path].bytes),
                    records: "1",
                })),
            ),
        };
        const preparedFile = join(dir, "prepare-manifest.json");
        await put("prepare-manifest.json", JSON.stringify(prepared));
        const launcher = await put("loader.sh", script);
        await chmod(join(dir, "loader.sh"), 0o700);
        const component = {
            protocolVersion: 1,
            kind: "sf100-component",
            role: "loader",
            id: "mock-loader-not-a-database",
            version: "test",
            command: ["./loader.sh"],
            trackedFiles: [{ path: "loader.sh", sha256: launcher.sha256 }],
            capabilities: {
                scale: "100",
                serializer: "CsvBasic",
                operations: [],
                checkpoint: "reconcile-before-retry",
            },
        };
        const componentHash = await put("component.json", JSON.stringify(component));
        const config = {
            schemaVersion: 1,
            planDigest: "1".repeat(64),
            datasetDigest: "2".repeat(64),
            mappingDigest: hash({ schema: files["schema.groovy"], converter: prepared.sourceConverterSha256 }),
            componentManifest: "component.json",
            componentSha256: componentHash.sha256,
            target: {
                host: "http://example.invalid",
                port: 8080,
                graph: "sf100_test",
                epoch: "test-epoch",
                topologyDigest: "3".repeat(64),
            },
            schemaAlreadyApplied: true,
            batchSize: 500,
            batchThreads: 1,
            parserThreads: 2,
            heapMiB: 256,
            timeoutSeconds: 5,
            processTimeoutMs: 5000,
            maxLogBytes: 4096,
            ...options,
        };
        const configFile = join(dir, "config.json");
        await put("config.json", JSON.stringify(config));
        const result = await planLoaderJobs(preparedFile, configFile);
        const jobFile = join(dir, "job.json");
        await put("job.json", JSON.stringify(result.jobs[0]));
        await fn({ dir, config, prepared, preparedFile, configFile, result, jobFile, put });
    } finally {
        await rm(dir, { recursive: true, force: true });
    }
}

test("loader planning freezes per-shard mappings and previews a fail-fast command without executing", async () => {
    await fixture(async (c) => {
        assert.equal(c.result.jobs.length, 2);
        assert.ok(c.result.jobs[1].prerequisites.includes("all-vertices-reconciled"));
        const out = join(c.dir, "attempt");
        const preview = await runLoaderJob(c.jobFile, out);
        assert.equal(preview.executed, false);
        assert.equal(preview.command[preview.command.indexOf("--retry-times") + 1], "0");
        assert.equal(preview.command[preview.command.indexOf("--batch-failure-fallback") + 1], "false");
        assert.equal(preview.command[preview.command.indexOf("--max-insert-errors") + 1], "1");
        assert.deepEqual(preview.environment, { JVM_OPTS: "-Xmx256m" });
        await assert.rejects(stat(out), { code: "ENOENT" });
    });
});

test("a successful process still produces unknown database outcome and a durable intent", async () => {
    await fixture(async (c) => {
        const out = join(c.dir, "attempt");
        const result = await runLoaderJob(c.jobFile, out, true);
        assert.equal(result.executed, true);
        assert.equal(result.processExitCode, 0);
        assert.equal(result.loadVerified, false);
        assert.equal(result.reason, "RECONCILIATION_REQUIRED");
        const receipt = JSON.parse(await readFile(join(out, "receipt.json"), "utf8"));
        assert.equal(receipt.effect, "unknown");
        assert.equal(receipt.status, "failed");
        assert.equal(
            (await assessSf100Recovery(join(out, "request.json"), join(out, "receipt.json"), out)).action,
            "reconcile",
        );
        assert.ok((await readFile(join(out, "stdout.log"), "utf8")).includes("--retry-times\n0"));
        assert.ok((await readFile(join(out, "stdout.log"), "utf8")).includes("--batch-failure-fallback\nfalse"));
        const struct = JSON.parse(await readFile(join(out, "struct.json"), "utf8"));
        assert.equal(struct.vertices[0].input.path, join(c.dir, "vertices/person-000000.json"));
        await assert.rejects(runLoaderJob(c.jobFile, out, true), { code: "EEXIST" });
    });
});

test("Loader failure preserves an unknown outcome without retry", async () => {
    await fixture(async (c) => {
        const result = await runLoaderJob(c.jobFile, join(c.dir, "attempt"), true);
        assert.equal(result.processExitCode, 7);
        assert.equal(result.reason, "LOADER_PROCESS_FAILED");
    }, "#!/bin/sh\nexit 7\n");
});

test("supervisor kills timed out process groups and seals bounded evidence", async () => {
    await fixture(
        async (c) => {
            const start = Date.now();
            const result = await runLoaderJob(c.jobFile, join(c.dir, "attempt"), true);
            assert.equal(result.reason, "PROCESS_TIMEOUT");
            assert.ok(Date.now() - start < 4000);
        },
        "#!/bin/sh\nsleep 20\n",
        { processTimeoutMs: 30 },
    );
});

test("supervisor enforces shared stdout/stderr budget", async () => {
    await fixture(
        async (c) => {
            const result = await runLoaderJob(c.jobFile, join(c.dir, "attempt"), true);
            assert.equal(result.reason, "LOG_BUDGET_EXCEEDED");
            const receipt = JSON.parse(await readFile(result.receiptPath, "utf8"));
            assert.ok(receipt.artifacts.reduce((n, f) => n + Number(f.bytes), 0) <= 32);
        },
        "#!/bin/sh\nwhile :; do printf 'abcdefghijklmnopqrstuvwxyz0123456789\n'; done\n",
        { maxLogBytes: 32 },
    );
});

test("changed data/runtime files are refused before an attempt directory is created", async () => {
    for (const changed of ["loader.sh", "vertices/person-000000.json"])
        await fixture(async (c) => {
            await writeFile(join(c.dir, changed), "changed");
            const out = join(c.dir, "attempt");
            await assert.rejects(runLoaderJob(c.jobFile, out, true));
            await assert.rejects(stat(out), { code: "ENOENT" });
        });
});

test("planning rejects ambiguous mappings and execution rejects mismatched request paths", async () => {
    await fixture(async (c) => {
        const job = structuredClone(c.result.jobs[0]);
        job.mapping.vertices[0].input.path = "another-file.json";
        const { digest, ...content } = job;
        job.digest = hash(content);
        await writeFile(c.jobFile, JSON.stringify(job));
        await assert.rejects(runLoaderJob(c.jobFile, join(c.dir, "attempt")), /exactly the requested/);
        c.prepared.shards.push(c.prepared.shards[0]);
        await writeFile(c.preparedFile, JSON.stringify(c.prepared));
        await assert.rejects(planLoaderJobs(c.preparedFile, c.configFile), /Duplicate shard/);
    });
});

test("gzip planning retains native Loader compression and binds expanded identity through reconciliation", async () => {
    await fixture(async (c) => {
        const shard = c.prepared.shards[0];
        const oldPath = shard.path;
        const raw = await readFile(join(c.dir, oldPath));
        const stored = gzipSync(raw, { level: 1 });
        shard.path += ".gz";
        shard.bytes = String(stored.length);
        shard.sha256 = sha(stored);
        shard.encoding = { compression: "gzip", uncompressedBytes: String(raw.length), uncompressedSha256: sha(raw) };
        c.prepared.schemaVersion = 2;
        c.prepared.files[shard.path] = await c.put(shard.path, stored);
        delete c.prepared.files[oldPath];
        await rm(join(c.dir, oldPath));
        const struct = JSON.parse(await readFile(join(c.dir, "struct.json"), "utf8"));
        struct.vertices[0].input.path = shard.path;
        struct.vertices[0].input.compression = "GZIP";
        c.prepared.files["struct.json"] = await c.put("struct.json", JSON.stringify(struct));
        await c.put("prepare-manifest.json", JSON.stringify(c.prepared));
        const result = await planLoaderJobs(c.preparedFile, c.configFile);
        const job = result.jobs[0];
        assert.equal(job.schemaVersion, 2);
        assert.deepEqual(job.encoding, shard.encoding);
        assert.equal(job.mapping.vertices[0].input.compression, "GZIP");
        await c.put("job.json", JSON.stringify(job));
        const out = join(c.dir, "gzip-attempt");
        const execution = await runLoaderJob(c.jobFile, out, true);
        assert.equal(execution.loadVerified, false); // fixture launcher, no database
        const native = JSON.parse(await readFile(join(out, "struct.json"), "utf8"));
        assert.equal(native.vertices[0].input.compression, "GZIP");
        const context = await promisify(execFile)(process.execPath, [
            "tools/ldbc-reconcile-context.mjs",
            c.jobFile,
            c.preparedFile,
        ]);
        assert.deepEqual(JSON.parse(context.stdout).job.encoding, shard.encoding);
        c.prepared.schemaVersion = 1;
        await c.put("prepare-manifest.json", JSON.stringify(c.prepared));
        const versionError = (error) =>
            error.code === "SCHEMA_INVALID" && error.details.some((issue) => issue.message.includes("schemaVersion 2"));
        await assert.rejects(planLoaderJobs(c.preparedFile, c.configFile), versionError);
        c.prepared.schemaVersion = 2;
        const legacyJob = structuredClone(job);
        legacyJob.schemaVersion = 1;
        await c.put("job.json", JSON.stringify(legacyJob));
        await assert.rejects(runLoaderJob(c.jobFile, join(c.dir, "legacy")), versionError);
        await c.put("job.json", JSON.stringify(job));
        shard.encoding.uncompressedSha256 = "d".repeat(64);
        await c.put("prepare-manifest.json", JSON.stringify(c.prepared));
        await assert.rejects(
            promisify(execFile)(process.execPath, ["tools/ldbc-reconcile-context.mjs", c.jobFile, c.preparedFile]),
            /does not belong/,
        );
        delete shard.encoding;
        await c.put("prepare-manifest.json", JSON.stringify(c.prepared));
        await assert.rejects(planLoaderJobs(c.preparedFile, c.configFile), /Compression/);
        delete job.encoding;
        const { digest, ...content } = job;
        job.digest = hash(content);
        await c.put("job.json", JSON.stringify(job));
        await assert.rejects(runLoaderJob(c.jobFile, join(c.dir, "invalid")), /Compression/);
    });
});

test("whole-shard reconciler uses exact database content and feeds verified recovery decisions", async () => {
    for (const compression of ["none", "gzip"]) {
        await fixture(async (c) => {
            let state = "correct";
            let expectedScript;
            let calls = 0;
            const server = createServer(async (req, res) => {
                calls++;
                const chunks = [];
                for await (const chunk of req) chunks.push(chunk);
                const body = JSON.parse(Buffer.concat(chunks).toString());
                assert.deepEqual(body.bindings.selectors, [{ id: "person:9007199254740993" }]);
                assert.equal(body.gremlin, expectedScript);
                if (state === "http-error") {
                    res.writeHead(500, { "Content-Type": "application/json" });
                    res.end(JSON.stringify({ status: { code: 598, message: "SECRET-provider-message" } }));
                    return;
                }
                const result = {
                    status: { code: 200 },
                    result: {
                        data: [
                            {
                                graphName: body.bindings.graphName,
                                epochMarker: body.bindings.epochMarker,
                                rows:
                                    state === "absent"
                                        ? [[]]
                                        : [
                                              [
                                                  {
                                                      id: "person:9007199254740993",
                                                      label: "person",
                                                      properties: {
                                                          ldbcId: "EXACT_LONG",
                                                          firstName: state === "correct" ? "甲" : "wrong",
                                                      },
                                                  },
                                              ],
                                          ],
                            },
                        ],
                    },
                };
                res.writeHead(200, { "Content-Type": "application/json" });
                res.end(JSON.stringify(result).replace('"EXACT_LONG"', "9007199254740993"));
            });
            await new Promise((done) => server.listen(0, "127.0.0.1", done));
            try {
                const job = c.result.jobs[0];
                const raw = '{"vertexId":"person:9007199254740993","ldbcId":9007199254740993,"firstName":"甲"}\n';
                const stored = compression === "gzip" ? gzipSync(raw) : Buffer.from(raw);
                if (compression === "gzip") {
                    job.schemaVersion = 2;
                    job.mapping.vertices[0].input.compression = "GZIP";
                    job.encoding = {
                        compression: "gzip",
                        uncompressedBytes: String(Buffer.byteLength(raw)),
                        uncompressedSha256: sha(raw),
                    };
                }
                await c.put(job.request.inputs[0].path, stored);
                job.request.inputs[0].sha256 = sha(stored);
                job.request.inputs[0].bytes = String(stored.length);
                job.config.target.host = "http://127.0.0.1";
                job.config.target.port = server.address().port;
                job.config.maxLogBytes = 32768;
                job.request.outputBudgetBytes = "32768";
                job.request.target.targetDigest = hash(job.config.target);
                const { digest, ...content } = job;
                job.digest = hash(content);
                await c.put("job.json", JSON.stringify(job));
                await c.put("request.json", JSON.stringify(job.request));
                await c.put(
                    "quiescence.json",
                    JSON.stringify({
                        schemaVersion: 1,
                        kind: "sf100-writer-quiescence",
                        requestDigest: hash(job.request),
                        target: job.request.target,
                        graphName: job.config.target.graph,
                        epochMarker: "hgb_epoch_" + sha(job.request.target.epoch).slice(0, 32),
                        validUntil: new Date(Date.now() + 60000).toISOString(),
                        writersStopped: true,
                        inflightWritesDrained: true,
                        exclusiveAccess: true,
                        attestor: "unit-only",
                        basis: "Synthetic loopback server has no writers",
                    }),
                );
                for (const [lookupMode, mode, action] of [
                    ["traversal", "correct", "skip"],
                    ["traversal", "wrong", "restore-and-reload"],
                    ["traversal", "absent", "retry-new-attempt"],
                    ["traversal", "http-error", "reconcile"],
                    ["native-batch", "correct", "skip"],
                    ["native-batch", "wrong", "restore-and-reload"],
                    ["native-batch", "absent", "retry-new-attempt"],
                    ["native-batch", "http-error", "reconcile"],
                ]) {
                    state = mode;
                    expectedScript = await readFile(
                        `adapters/ldbc-gremlin/${lookupMode === "traversal" ? "reconcile.groovy" : "reconcile-batch.groovy"}`,
                        "utf8",
                    );
                    const out = join(c.dir, `${lookupMode}-${mode}`);
                    const callsBefore = calls;
                    let result;
                    try {
                        result = await promisify(execFile)(
                            "python3",
                            [
                                "tools/ldbc-reconcile.py",
                                "--job",
                                c.jobFile,
                                "--quiescence",
                                join(c.dir, "quiescence.json"),
                                "--alias",
                                "__g_DEFAULT-sf100_test",
                                "--endpoint",
                                `http://127.0.0.1:${server.address().port}/gremlin`,
                                ...(lookupMode === "native-batch" ? ["--lookup-mode", lookupMode] : []),
                                "--out",
                                out,
                            ],
                            { timeout: 20000 },
                        );
                    } catch (error) {
                        assert.equal(error.code, 4, error.stderr);
                        result = error;
                    }
                    assert.equal(
                        JSON.parse(result.stdout).disposition,
                        mode === "correct"
                            ? "committed"
                            : mode === "wrong"
                              ? "partial"
                              : mode === "absent"
                                ? "absent"
                                : "unknown",
                    );
                    assert.equal(calls, callsBefore + 1, "no implicit request replay");
                    if (mode === "http-error") {
                        const failure = JSON.parse(result.stdout);
                        assert.equal(failure.counts, null);
                        assert.equal(failure.error.message, "HTTP status 500");
                        assert.equal(failure.error.http.providerCode, 598);
                        assert(!result.stdout.includes("SECRET-provider-message"));
                    }
                    assert.equal(JSON.parse(result.stdout).lookupMode, lookupMode);
                    assert.equal(JSON.parse(result.stdout).scriptSha256, sha(expectedScript));
                    const recovery = await assessSf100Recovery(
                        join(c.dir, "request.json"),
                        undefined,
                        out,
                        join(out, "observation.json"),
                    );
                    assert.equal(recovery.action, action);
                }
            } finally {
                await new Promise((done) => server.close(done));
            }
        });
    }
});

test("full inventory CLI binds the conversion and detects extra records independently of shard content", async () => {
    await fixture(async (c) => {
        const expected = {
            schemaVersion: 1,
            scope: "public-ldbc-initial-graph",
            schema: {
                propertyKeys: {},
                vertexLabels: { person: {} },
                edgeLabels: { person_knows_person: {} },
                indexLabels: {},
            },
            counts: { vertices: { person: "1" }, edges: { person_knows_person: "1" } },
        };
        let mode = "correct";
        const server = createServer(async (req, res) => {
            const chunks = [];
            for await (const chunk of req) chunks.push(chunk);
            const body = JSON.parse(Buffer.concat(chunks).toString());
            assert.deepEqual(Object.keys(body.bindings).sort(), [
                "category",
                "epochMarker",
                "graphName",
                "page",
                "pageSize",
            ]);
            if (mode === "error") {
                res.writeHead(503);
                res.end();
                return;
            }
            const { graphName, epochMarker, category, page, pageSize } = body.bindings;
            assert.equal(pageSize, 1);
            const identity = { graphName, epochMarker };
            const actual =
                category === "schema"
                    ? { ...identity, kind: "schema", schema: expected.schema }
                    : {
                          ...identity,
                          kind: "page",
                          category,
                          page,
                          nextPage: mode === "extra" && category === "vertices" && page === "" ? "second" : null,
                          scanned: "1",
                          counts: category === "vertices" ? { person: "1" } : { person_knows_person: "1" },
                      };
            res.writeHead(200, { "Content-Type": "application/json" });
            res.end(
                JSON.stringify({
                    status: { code: 200 },
                    result: {
                        data: [actual],
                    },
                }),
            );
        });
        await new Promise((done) => server.listen(0, "127.0.0.1", done));
        try {
            const job = c.result.jobs[0];
            job.config.target.host = "http://127.0.0.1";
            job.config.target.port = server.address().port;
            job.config.maxLogBytes = 262144;
            job.request.outputBudgetBytes = "262144";
            job.request.target.targetDigest = hash(job.config.target);
            const { digest, ...content } = job;
            job.digest = hash(content);
            await c.put("job.json", JSON.stringify(job));
            const prepared = c.prepared;
            prepared.files["inventory.expected.json"] = await c.put(
                "inventory.expected.json",
                JSON.stringify(expected),
            );
            prepared.counts = { person: 1, person_knows_person: 1 };
            prepared.vertices = 1;
            prepared.edges = 1;
            const preparedBytes = JSON.stringify(prepared);
            await c.put("prepare-manifest.json", preparedBytes);
            await c.put(
                "inventory-lease.json",
                JSON.stringify({
                    schemaVersion: 1,
                    kind: "sf100-writer-quiescence",
                    requestDigest: hash(job.request),
                    target: job.request.target,
                    graphName: job.config.target.graph,
                    epochMarker: "hgb_epoch_" + sha(job.request.target.epoch).slice(0, 32),
                    validUntil: new Date(Date.now() + 60000).toISOString(),
                    writersStopped: true,
                    inflightWritesDrained: true,
                    exclusiveAccess: true,
                    attestor: "unit-only",
                    basis: "Synthetic server",
                }),
            );
            for (const [state, outcome] of [
                ["correct", "matched"],
                ["extra", "mismatch"],
                ["error", "unknown"],
            ]) {
                mode = state;
                let result;
                try {
                    result = await promisify(execFile)(
                        "python3",
                        [
                            "tools/ldbc-inventory.py",
                            "--job",
                            c.jobFile,
                            "--prepared",
                            c.preparedFile,
                            "--prepared-sha256",
                            sha(preparedBytes),
                            "--quiescence",
                            join(c.dir, "inventory-lease.json"),
                            "--alias",
                            "__g_DEFAULT-sf100_test",
                            "--endpoint",
                            `http://127.0.0.1:${server.address().port}/gremlin`,
                            "--page-size",
                            "1",
                            "--max-pages",
                            "3",
                            "--out",
                            join(c.dir, "inventory-" + state),
                        ],
                        { timeout: 20000 },
                    );
                } catch (error) {
                    assert.equal(error.code, 4, error.stderr);
                    result = error;
                }
                const report = JSON.parse(result.stdout);
                assert.equal(report.outcome, outcome, JSON.stringify(report));
                assert.equal(report.contentChecked, false);
                assert.equal(report.preparedSha256, sha(preparedBytes));
            }
        } finally {
            await new Promise((done) => server.close(done));
        }
    });
});

test("endpoint checks are configurable and remain on for existing jobs", () => {
    const config = {
        target: { host: "http://example.invalid", port: 8080, graph: "g" },
        batchSize: 500,
        batchThreads: 2,
        parserThreads: 1,
        timeoutSeconds: 120,
    };
    for (const value of [undefined, true, false]) {
        const args = loaderArguments(
            { runtime: { command: ["/loader"] }, config: { ...config, checkVertex: value } },
            "/mapping",
        );
        assert.equal(args[args.indexOf("--check-vertex") + 1], String(value ?? true));
    }
});
