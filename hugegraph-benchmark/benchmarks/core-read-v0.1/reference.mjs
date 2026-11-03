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
import { readFile, writeFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { isDeepStrictEqual } from "node:util";

export const suiteId = "hgbench-core-read-v0.1-draft";
export const directory = dirname(fileURLToPath(import.meta.url));
const json = (value) => JSON.stringify(value, null, 2) + "\n";
const key = (id) => `${typeof id}:${id}`;
const unique = (ids) => [...new Map(ids.map((id) => [key(id), id])).values()];
const canonical = (ids) => unique(ids).sort((a, b) => (key(a) < key(b) ? -1 : key(a) > key(b) ? 1 : 0));
const sameSet = (a, b) => a.length === unique(a).length && isDeepStrictEqual(canonical(a), canonical(b));

export function generate() {
    const make = (idType) => {
        const encode = (id) => (idType === "numeric" ? id : `v${id}`);
        const vertices = new Map();
        const edges = [];
        const vertex = (id, group = "other") => {
            if (!vertices.has(id))
                vertices.set(id, {
                    id: encode(id),
                    label: "node",
                    properties: { group, rank: id, payload: "x".repeat(256) },
                });
        };
        const edge = (source, target) => {
            vertex(source);
            vertex(target);
            edges.push({ id: `e${edges.length}`, source: encode(source), target: encode(target), label: "link" });
        };
        vertex(1, "wide-root");
        for (let a = 0; a < 8; a++) {
            vertex(10 + a, "wide-middle");
            edge(1, 10 + a);
            for (let b = 0; b < 8; b++) {
                vertex(100 + a * 8 + b, "wide-leaf");
                edge(10 + a, 100 + a * 8 + b);
            }
        }
        vertex(1000, "convergent-root");
        for (let a = 0; a < 8; a++) {
            vertex(1010 + a, "convergent-middle");
            edge(1000, 1010 + a);
            for (let b = 0; b < 16; b++) {
                vertex(1100 + b, "convergent-leaf");
                edge(1010 + a, 1100 + b);
            }
        }
        for (const id of [2000, 2001, 2002, 2003, 2004]) vertex(id, "cycle");
        for (const pair of [
            [2000, 2001],
            [2001, 2002],
            [2002, 2000],
            [2002, 2003],
            [2004, 2000],
        ])
            edge(...pair);
        for (const id of [3000, 3001, 3002, 3003, 3004, 3999]) vertex(id, "path");
        for (const pair of [
            [3000, 3001],
            [3000, 3002],
            [3001, 3003],
            [3002, 3003],
            [3003, 3004],
        ])
            edge(...pair);
        vertex(4000, "hub");
        for (let n = 0; n < 64; n++) edge(4000, 100 + n);
        const parameters = [];
        const add = (caseId, name, operation, args, policy = "exact-set") =>
            parameters.push({
                parameterId: `${caseId}-${idType}-${name}`,
                caseId,
                datasetId: `tiny-${idType}`,
                operation,
                args,
                policy,
            });
        add("CR01", "hit", "point", { id: encode(100) }, "exact-value");
        add("CR01", "miss", "point", { id: encode(9999) }, "exact-value");
        add("CR02", "one", "filter", { group: "hub" });
        add("CR02", "many", "filter", { group: "wide-leaf" });
        add("CR02", "empty", "filter", { group: "missing" });
        add("CR03", "ordinary", "neighbors", { source: encode(10) });
        add("CR03", "hub", "neighbors", { source: encode(4000) });
        add("CR04", "pages", "paged-neighbors", { source: encode(4000), pageSize: 7 }, "paged-set");
        add("CR05", "wide", "kout", { source: encode(1), depth: 2, nearest: true, limit: null });
        add("CR06", "convergent", "kout", { source: encode(1000), depth: 2, nearest: true, limit: null });
        add("CR07", "bounded", "kout", { source: encode(1), depth: 2, nearest: true, limit: 4 }, "bounded-set");
        add("CR08", "nearest", "kout", { source: encode(2004), depth: 4, nearest: true, limit: null });
        add("CR08", "revisit", "kout", { source: encode(2004), depth: 4, nearest: false, limit: null });
        add("CR08", "empty", "kout", { source: encode(3999), depth: 2, nearest: true, limit: null });
        add("CR09", "intersection", "common", { sources: [encode(1010), encode(1011)] });
        add("CR10", "tie", "shortest", { source: encode(3000), target: encode(3004), maxDepth: 6 }, "shortest-path");
        add(
            "CR11",
            "unreachable",
            "shortest",
            { source: encode(3000), target: encode(3999), maxDepth: 6 },
            "shortest-path",
        );
        add("CR12", "materialize", "materialize", { group: "wide-leaf" }, "vertex-set");
        return {
            dataset: { id: `tiny-${idType}`, idType, directed: true, vertices: [...vertices.values()], edges },
            parameters,
        };
    };
    const generated = [make("numeric"), make("string")];
    const datasets = generated.map((item) => item.dataset);
    const parameters = generated.flatMap((item) => item.parameters);
    const expected = parameters.map((parameter) => ({
        parameterId: parameter.parameterId,
        expected: oracle(
            datasets.find((d) => d.id === parameter.datasetId),
            parameter,
        ),
    }));
    return { datasets, parameters, expected };
}

export function oracle(graph, parameter) {
    const { operation, args } = parameter;
    const neighbors = (source) =>
        unique(graph.edges.filter((edge) => edge.source === source).map((edge) => edge.target));
    if (operation === "point") return graph.vertices.find((vertex) => vertex.id === args.id) ?? null;
    if (operation === "filter")
        return canonical(graph.vertices.filter((v) => v.properties.group === args.group).map((v) => v.id));
    if (operation === "materialize") return graph.vertices.filter((v) => v.properties.group === args.group);
    if (operation === "neighbors" || operation === "paged-neighbors") return canonical(neighbors(args.source));
    if (operation === "common")
        return canonical(neighbors(args.sources[0]).filter((v) => neighbors(args.sources[1]).includes(v)));
    if (operation === "kout") {
        let frontier = [args.source];
        const visited = new Set();
        for (let depth = 0; depth < args.depth; depth++) {
            if (args.nearest) for (const vertex of frontier) visited.add(key(vertex));
            frontier = unique(frontier.flatMap(neighbors)).filter((v) => v !== args.source && !visited.has(key(v)));
        }
        // Bounded requests return the full admissible universe and a required count, not an arbitrary golden subset.
        if (args.limit !== null) return { allowed: canonical(frontier), count: Math.min(args.limit, frontier.length) };
        return canonical(frontier);
    }
    if (operation === "shortest") {
        let frontier = [args.source];
        const visited = new Set(frontier.map(key));
        for (let distance = 0; distance <= args.maxDepth; distance++) {
            if (frontier.includes(args.target)) return { distance };
            frontier = unique(frontier.flatMap(neighbors)).filter((v) => !visited.has(key(v)));
            for (const v of frontier) visited.add(key(v));
        }
        return { distance: null };
    }
    throw new Error(`Unsupported operation: ${operation}`);
}

export function validate(graph, parameter, expected, actual) {
    try {
        if (parameter.policy === "exact-value") return isDeepStrictEqual(actual, expected);
        if (parameter.policy === "exact-set") return Array.isArray(actual) && sameSet(actual, expected);
        if (parameter.policy === "bounded-set") {
            return (
                Array.isArray(actual) &&
                actual.length === expected.count &&
                unique(actual).length === actual.length &&
                actual.every((v) => expected.allowed.some((allowed) => typeof allowed === typeof v && allowed === v))
            );
        }
        if (parameter.policy === "vertex-set") {
            return (
                Array.isArray(actual) &&
                sameSet(
                    actual.map((v) => v.id),
                    expected.map((v) => v.id),
                ) &&
                actual.every((v) =>
                    isDeepStrictEqual(
                        v,
                        expected.find((e) => e.id === v.id),
                    ),
                )
            );
        }
        if (parameter.policy === "paged-set") {
            if (!Array.isArray(actual) || !actual.length) return false;
            return (
                actual.every(
                    (page, index) =>
                        Array.isArray(page) &&
                        page.length <= parameter.args.pageSize &&
                        (page.length > 0 || index === actual.length - 1),
                ) && sameSet(actual.flat(), expected)
            );
        }
        if (parameter.policy === "shortest-path") {
            if (expected.distance === null) return actual === null;
            if (
                !Array.isArray(actual) ||
                actual.length !== expected.distance + 1 ||
                actual[0] !== parameter.args.source ||
                actual.at(-1) !== parameter.args.target ||
                unique(actual).length !== actual.length
            )
                return false;
            return actual
                .slice(1)
                .every((target, i) => graph.edges.some((e) => e.source === actual[i] && e.target === target));
        }
        return false;
    } catch {
        return false;
    }
}

export function canonicalAnswer(graph, parameter, expected) {
    if (parameter.policy === "bounded-set") return expected.allowed.slice(0, expected.count);
    if (parameter.policy === "paged-set") {
        const pages = [];
        for (let i = 0; i < expected.length; i += parameter.args.pageSize)
            pages.push(expected.slice(i, i + parameter.args.pageSize));
        return pages.length ? pages : [[]];
    }
    if (parameter.policy === "shortest-path") {
        if (expected.distance === null) return null;
        const queue = [[parameter.args.source]];
        while (queue.length) {
            const path = queue.shift();
            if (path.at(-1) === parameter.args.target) return path;
            if (path.length > parameter.args.maxDepth) continue;
            for (const edge of graph.edges.filter((e) => e.source === path.at(-1) && !path.includes(e.target)))
                queue.push([...path, edge.target]);
        }
        throw new Error("No reference path");
    }
    return expected;
}

const normativeFiles = [
    "spec.md",
    "suite.json",
    "profile.json",
    "scoring-policy.json",
    "scorecard.schema.json",
    "reference.mjs",
];
const dataFiles = ["datasets.json", "parameters.json", "expected.json"];
const digest = (data) => createHash("sha256").update(data).digest("hex");

async function lock() {
    const files = {};
    for (const file of [...normativeFiles, ...dataFiles]) files[file] = digest(await readFile(join(directory, file)));
    const contents = { suiteId, files };
    return { ...contents, suiteDigest: digest(JSON.stringify(contents)) };
}

export async function verify() {
    const stored = JSON.parse(await readFile(join(directory, "suite.lock.json"), "utf8"));
    if (!isDeepStrictEqual(stored, await lock()))
        throw new Error("Suite content changed; review and release a new digest");
    const generated = generate();
    for (const field of ["datasets", "parameters", "expected"]) {
        if ((await readFile(join(directory, `${field}.json`), "utf8")) !== json(generated[field])) {
            throw new Error(`Reproduction mismatch: ${field}`);
        }
    }
    const suite = JSON.parse(await readFile(join(directory, "suite.json"), "utf8"));
    if (
        suite.suiteId !== suiteId ||
        !isDeepStrictEqual(suite.requiredCases, [...new Set(generated.parameters.map((p) => p.caseId))])
    )
        throw new Error("Coverage manifest mismatch");
    let verified = 0;
    for (const p of generated.parameters) {
        const graph = generated.datasets.find((g) => g.id === p.datasetId);
        const answer = generated.expected.find((e) => e.parameterId === p.parameterId).expected;
        if (!validate(graph, p, answer, canonicalAnswer(graph, p, answer)))
            throw new Error(`Oracle failed ${p.parameterId}`);
        verified++;
    }
    return {
        suiteId,
        suiteDigest: stored.suiteDigest,
        cases: suite.requiredCases.length,
        parameters: verified,
        scope: "reference-conformance",
        performanceValidated: false,
    };
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
    try {
        if (process.argv[2] === "generate") {
            const generated = generate();
            for (const field of ["datasets", "parameters", "expected"])
                await writeFile(join(directory, `${field}.json`), json(generated[field]));
            await writeFile(join(directory, "suite.lock.json"), json(await lock()));
            process.stdout.write(json({ ok: true, data: await verify() }));
        } else if (process.argv[2] === "verify") process.stdout.write(json({ ok: true, data: await verify() }));
        else throw new Error("Usage: node reference.mjs generate|verify; generate replaces the draft data and lock");
    } catch (error) {
        process.stderr.write(String(error) + "\n");
        process.exitCode = 1;
    }
}
