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

import { createHash } from "node:crypto";
import { readFile, writeFile, mkdir } from "node:fs/promises";
import { join, dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { isDeepStrictEqual } from "node:util";
import { generate, verify, validate, suiteId } from "../benchmarks/core-read-v0.1/reference.mjs";
import { HugeGraphRest, checkTrace, verifySnapshot } from "../adapters/hugegraph-rest.mjs";

const root = dirname(dirname(fileURLToPath(import.meta.url)));
const json = (x) => JSON.stringify(x, null, 2) + "\n";
const hash = (bytes) => createHash("sha256").update(bytes).digest("hex");
const read = async (file) => JSON.parse(await readFile(file, "utf8"));
const files = ["context.json", "snapshots.json", "operations.json"];
const requiredString = (x) => typeof x === "string" && x.length > 0;

export async function assess(context, snapshots, operations) {
    const locked = await verify();
    if (
        context.suiteId !== suiteId ||
        context.suiteDigest !== locked.suiteDigest ||
        context.profile !== "tiny-conformance-v1" ||
        context.route !== "graph-rest" ||
        !["name", "revision", "backend", "environmentRef"].every((key) => requiredString(context.sut?.[key])) ||
        !/^[a-f0-9]{64}$/.test(context.adapterRevision)
    )
        throw new Error("Invalid submission identity");
    const assets = generate();
    for (const graph of assets.datasets) verifySnapshot(graph, snapshots.before[graph.id]);
    const seen = new Set();
    const results = [];
    if (!Array.isArray(operations)) throw new Error("Invalid operations collection");
    for (const row of operations) {
        const p = assets.parameters.find((p) => p.parameterId === row.parameterId);
        const key = `${row.pass}:${row.parameterId}`;
        if (!p || ![1, 2].includes(row.pass) || seen.has(key)) throw new Error("Unknown or duplicate operation/pass");
        // The conformance order is part of the protocol, including failed records.
        const position = (row.pass - 1) * assets.parameters.length + assets.parameters.indexOf(p);
        if (position !== results.length) throw new Error("Non-prefix or reordered operation sequence");
        seen.add(key);
        if (!Number.isFinite(row.durationMs) || row.durationMs < 0) throw new Error("Invalid duration");
        let status = row.status;
        if (status === "response") {
            const actual = checkTrace(p, row.trace);
            if (!isDeepStrictEqual(actual, row.actual)) throw new Error("Normalized answer differs from HTTP evidence");
            const graph = assets.datasets.find((g) => g.id === p.datasetId);
            const expected = assets.expected.find((e) => e.parameterId === p.parameterId).expected;
            status = validate(graph, p, expected, actual) ? "correct" : "wrong";
            if (status === "correct" && row.durationMs > 30000) status = "timeout";
        } else if (!["timeout", "error", "unsupported"].includes(status)) throw new Error("Invalid result status");
        results.push({
            parameterId: p.parameterId,
            pass: row.pass,
            status,
            resultRef: `operations.json#/${results.length}`,
        });
    }
    const postcheck = snapshots.after !== null;
    if (postcheck) for (const graph of assets.datasets) verifySnapshot(graph, snapshots.after[graph.id]);
    const outcome = results.some((r) => r.status === "wrong")
        ? "correctness-failed"
        : results.length === 72 && results.every((r) => r.status === "correct") && postcheck
          ? "conformant"
          : "incomplete";
    return {
        schemaVersion: 1,
        suiteId,
        suiteDigest: locked.suiteDigest,
        profile: context.profile,
        sut: context.sut,
        route: context.route,
        adapterRevision: context.adapterRevision,
        outcome,
        performanceVerdict: "not-evaluated",
        operations: results,
        evidence: [...files, "manifest.json"],
    };
}

export async function seal(directory, context, snapshots, operations) {
    const values = [context, snapshots, operations];
    const manifest = {};
    for (let i = 0; i < files.length; i++) {
        const contents = json(values[i]);
        await writeFile(join(directory, files[i]), contents, { flag: "wx" });
        manifest[files[i]] = hash(contents);
    }
    await writeFile(join(directory, "manifest.json"), json(manifest), { flag: "wx" });
}

export async function verifyBundle(directory) {
    const manifest = await read(join(directory, "manifest.json"));
    if (!isDeepStrictEqual(Object.keys(manifest).sort(), [...files].sort())) throw new Error("Wrong artifact set");
    for (const name of files)
        if (hash(await readFile(join(directory, name))) !== manifest[name])
            throw new Error(`Evidence checksum mismatch: ${name}`);
    return assess(...(await Promise.all(files.map((name) => read(join(directory, name))))));
}

async function clients(config) {
    if (
        !config.graphs ||
        !["name", "revision", "backend", "environmentRef"].every((key) => requiredString(config.sut?.[key]))
    )
        throw new Error("Provide graph URLs and explicit SUT identity/environment evidence");
    if (Object.values(config.sut).some((value) => String(value).includes("REPLACE_WITH_")))
        throw new Error("Replace example SUT metadata with actual evidence before running");
    const urls = [config.graphs["tiny-numeric"], config.graphs["tiny-string"]];
    if (!urls.every(requiredString) || new Set(urls.map((u) => u.replace(/\/$/, ""))).size !== 2)
        throw new Error("Two distinct dedicated graph URLs are required");
    const authorization = config.authorizationEnv ? process.env[config.authorizationEnv] : undefined;
    if (config.authorizationEnv && !authorization) throw new Error("Authentication environment variable is absent");
    return Object.fromEntries(
        Object.entries(config.graphs).map(([id, url]) => [id, new HugeGraphRest(url, { authorization })]),
    );
}

async function main() {
    const [command, input, output] = process.argv.slice(2);
    if (command === "--help" || command === "-h") {
        console.log(`HG-Bench minimal graph REST conformance harness (no npm dependencies)
Usage:
  node tools/conformance.mjs init <config.json> <new-output-dir>
    WRITES schema/data to two explicit EMPTY dedicated graphs. Never clears or deletes a graph.
  node tools/conformance.mjs run <config.json> <new-output-dir>
    READ-ONLY: full precheck, all 36 parameters twice, postcheck, evidence and scorecard.
  node tools/conformance.mjs verify <evidence-dir>
    OFFLINE: checks hashes, identity, requests, coverage and answers; recomputes the scorecard.
Exit codes: 0 conformant/import verified, 2 incomplete/wrong, 1 invalid/runtime failure.
Authentication: set authorizationEnv in config; credentials are not written to evidence.
See examples/rest-conformance.json and docs/benchmark-local.md. No performance claims.`);
        return;
    }
    if (command === "verify" && input && !output) {
        const card = await verifyBundle(input);
        console.log(json(card));
        if (card.outcome !== "conformant") process.exitCode = 2;
        return;
    }
    if (!["init", "run"].includes(command) || !input || !output || process.argv.length !== 5)
        throw new Error("Invalid arguments. Use --help.");
    const identity = await verify();
    const assets = generate();
    const config = await read(input);
    const adapter = await clients(config);
    await mkdir(output);
    if (command === "init") {
        // Check both graphs before the first mutation. Each import rechecks immediately before writing.
        for (const graph of assets.datasets) await adapter[graph.id].assertEmpty();
        const snapshots = {};
        for (const graph of assets.datasets) {
            snapshots[graph.id] = await adapter[graph.id].importTiny(graph);
            verifySnapshot(graph, snapshots[graph.id]);
        }
        await writeFile(join(output, "import.json"), json({ ...identity, snapshots }), { flag: "wx" });
        console.log(json({ ok: true, scope: "tiny-import", output }));
        return;
    }
    const snapshots = { before: {}, after: null };
    const startedAt = new Date().toISOString();
    for (const graph of assets.datasets) {
        snapshots.before[graph.id] = await adapter[graph.id].snapshotTiny();
        verifySnapshot(graph, snapshots.before[graph.id]);
    }
    const operations = [];
    for (let pass = 1; pass <= 2; pass++)
        for (const parameter of assets.parameters) {
            const result = await adapter[parameter.datasetId].execute(parameter);
            operations.push({ parameterId: parameter.parameterId, pass, ...result });
            // Journal each completed operation so a killed attempt retains its observations.
            await writeFile(join(output, "journal.ndjson"), JSON.stringify(operations.at(-1)) + "\n", { flag: "a" });
        }
    try {
        const after = {};
        for (const graph of assets.datasets) after[graph.id] = await adapter[graph.id].snapshotTiny();
        snapshots.after = after;
    } catch (error) {
        await writeFile(join(output, "postcheck-error.json"), json({ error: String(error) }), { flag: "wx" });
    }
    const context = {
        suiteId,
        suiteDigest: identity.suiteDigest,
        profile: "tiny-conformance-v1",
        route: "graph-rest",
        sut: config.sut,
        adapterRevision: hash(await readFile(join(root, "adapters/hugegraph-rest.mjs"))),
        targets: config.graphs,
        runtime: { node: process.version, platform: process.platform, architecture: process.arch },
        startedAt,
        finishedAt: new Date().toISOString(),
    };
    await seal(output, context, snapshots, operations);
    const scorecard = await verifyBundle(output);
    await writeFile(join(output, "scorecard.json"), json(scorecard), { flag: "wx" });
    console.log(json({ ok: scorecard.outcome === "conformant", outcome: scorecard.outcome, output }));
    if (scorecard.outcome !== "conformant") process.exitCode = 2;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url))
    main().catch((error) => {
        console.error(JSON.stringify({ ok: false, outcome: "invalid", error: String(error) }));
        process.exitCode = 1;
    });
