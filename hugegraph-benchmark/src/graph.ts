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
import { readFile, writeFile, mkdir } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { z } from "zod";
import {
    graphBindingSchema,
    graphImportSnapshotSchema,
    graphRuleSchema,
    httpGoProfileSchema,
    type HttpGoProfile,
} from "./http-contracts.js";
import { inspectHttpInput, validateHttpProfile } from "./http.js";
import { goBaseProfile } from "./http-go.js";
import { BenchError, hash, fileHash } from "./io.js";
export const graphCases = ["point", "neighbors", "common", "shortest", "gremlin-neighbors"] as const;
const parameterSchema = z.strictObject({
    id: z.string().regex(/^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$/),
    stratum: z.string().regex(/^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$/),
    source: z.string().min(1).max(1024),
    target: z.string().min(1).max(1024).optional(),
    rule: graphRuleSchema,
});
export const graphReferenceSchema = z.strictObject({
    schemaVersion: z.literal(1),
    kind: z.literal("graph-query-reference"),
    dataset: graphBindingSchema.shape.dataset,
    origin: z.literal("independent-csv"),
    provenance: z.record(z.string(), z.json()),
    pools: z.strictObject(
        Object.fromEntries(graphCases.map((id) => [id, z.array(parameterSchema).min(1).max(100000)])) as Record<
            (typeof graphCases)[number],
            z.ZodArray<typeof parameterSchema>
        >,
    ),
});
function bundle(profile: { graph?: HttpGoProfile["graph"]; cases: HttpGoProfile["cases"] }) {
    const { bundleSha256: _, ...binding } = profile.graph!;
    return hash({ binding, cases: profile.cases });
}
export function verifyGraphProfile(p: { graph?: HttpGoProfile["graph"]; cases: HttpGoProfile["cases"] }) {
    if (
        !p.graph ||
        p.graph.bundleSha256 !== bundle(p) ||
        hash(p.cases.map((c) => c.id)) !== hash(graphCases) ||
        p.cases.some((c) => !c.requestPool)
    )
        throw new BenchError(
            "GRAPH_BUNDLE",
            "Frozen five-case graph bundle changed; prepare a new profile from its independent reference",
        );
}
export async function prepareGraphProfile(
    reference: string,
    profile: string,
    options: {
        out: string;
        graphUrl: string;
        traversalSource: string;
        importSnapshot?: string;
        minEdgeCoverage?: string | number;
        reuse?: boolean;
    },
) {
    const raw = await readFile(resolve(reference));
    if (raw.length > 3 * 1024 * 1024)
        throw new BenchError(
            "GRAPH_REFERENCE_SIZE",
            "Reference exceeds 3 MiB; reduce the declared pool or split a custom suite",
        );
    const source = graphReferenceSchema.parse(JSON.parse(raw.toString()));
    if (
        source.dataset.scaleFactor === 100 &&
        (source.dataset.vertices !== 282637871 || source.dataset.edges !== 1775513741)
    )
        throw new BenchError("GRAPH_SCALE", "Full SF100 counts must be declared in the independent reference");
    let imported: z.infer<typeof graphBindingSchema>["imported"];
    if (options.importSnapshot) {
        const raw = await readFile(resolve(options.importSnapshot));
        if (raw.length > 1048576) throw new BenchError("GRAPH_IMPORT", "Import snapshot exceeds 1 MiB");
        const snapshot = graphImportSnapshotSchema.parse(JSON.parse(raw.toString()));
        const minimumEdgeCoverage = Number(options.minEdgeCoverage ?? 0.95);
        validateGraphImport(snapshot, source.dataset, minimumEdgeCoverage);
        imported = { snapshot, minimumEdgeCoverage, artifactSha256: await fileHash(resolve(options.importSnapshot)) };
    } else if (options.minEdgeCoverage !== undefined) {
        throw new BenchError("GRAPH_IMPORT", "--min-edge-coverage requires --import-snapshot");
    }
    const input = await inspectHttpInput(resolve(profile));
    if (input.profile.schemaVersion !== 2)
        throw new BenchError("GRAPH_EXECUTOR", "Register the current Go executor first");
    const base = input.profile;
    const root = new URL(options.graphUrl);
    if (
        !/^https?:$/.test(root.protocol) ||
        root.username ||
        root.password ||
        root.search ||
        root.hash ||
        !/\/graphs\/[^/]+\/?$/.test(root.pathname)
    )
        throw new BenchError("GRAPH_URL", "Use an explicit graph URL without credentials/query/fragment");
    if (!/^[a-zA-Z0-9_-]{1,128}$/.test(options.traversalSource))
        throw new BenchError("GRAPH_ALIAS", "Use an explicit traversal-source alias");
    const prefix = root.pathname.includes("/graphspaces/")
        ? root.pathname.split("/graphspaces/")[0]
        : root.pathname.split("/graphs/")[0];
    const graphRoot = root.href.replace(/\/$/, "") + "/";
    const cases = graphCases.map((id) => ({
        id,
        body: null,
        expectedStatus: 200,
        checks: [{ pointer: "", equals: null }],
        requestPool: source.pools[id].map((p) => {
            const common = { direction: "OUT", label: "person_knows_person", max_degree: "-1" };
            let url: URL,
                body: any = null,
                method: "GET" | "POST" = "GET";
            let rule = p.rule;
            if (id === "point") {
                if (rule.kind !== "vertex" || rule.id !== p.source || rule.label !== "person")
                    throw new BenchError("GRAPH_REFERENCE", "Point reference identity mismatch");
                url = new URL("graph/vertices/" + encodeURIComponent(JSON.stringify(p.source)), graphRoot);
            } else if (id === "gremlin-neighbors") {
                if (rule.kind !== "id-set" || rule.count !== Math.min(100, rule.allowed.length))
                    throw new BenchError("GRAPH_REFERENCE", "Invalid Gremlin neighbor reference");
                url = new URL(prefix + "/gremlin", root.origin);
                method = "POST";
                body = {
                    gremlin: "g.V(userId).out('person_knows_person').id().limit(100)",
                    language: "gremlin-groovy",
                    aliases: { g: options.traversalSource },
                    bindings: { userId: p.source },
                };
                rule = { ...rule, pointer: "/result/data" };
            } else {
                const path = id === "neighbors" ? "kout" : id === "common" ? "sameneighbors" : "shortestpath";
                url = new URL("traversers/" + path, graphRoot);
                const query: Record<string, string> = { ...common };
                if (id === "common") {
                    if (!p.target || rule.kind !== "id-set")
                        throw new BenchError("GRAPH_REFERENCE", "Common-neighbor pair missing");
                    Object.assign(query, {
                        vertex: JSON.stringify(p.source),
                        other: JSON.stringify(p.target),
                        limit: "100",
                    });
                    rule = { ...rule, pointer: "/same_neighbors" };
                } else {
                    query.source = JSON.stringify(p.source);
                    if (id === "neighbors") {
                        if (rule.kind !== "id-set") throw new BenchError("GRAPH_REFERENCE", "Neighbor set missing");
                        Object.assign(query, { max_depth: "1", nearest: "true", capacity: "-1", limit: "100" });
                        rule = { ...rule, pointer: "/vertices" };
                    } else {
                        if (
                            !p.target ||
                            rule.kind !== "shortest-path" ||
                            rule.source !== p.source ||
                            rule.target !== p.target
                        )
                            throw new BenchError("GRAPH_REFERENCE", "Path endpoints differ");
                        Object.assign(query, {
                            target: JSON.stringify(p.target),
                            max_depth: "3",
                            skip_degree: "0",
                            capacity: "-1",
                            with_vertex: "false",
                            with_edge: "false",
                        });
                        rule = { ...rule, pointer: "/path" };
                    }
                }
                for (const [k, v] of Object.entries(query)) url.searchParams.set(k, v);
            }
            if (
                rule.kind === "id-set" &&
                (rule.count !== Math.min(100, rule.allowed.length) ||
                    new Set(rule.allowed).size !== rule.allowed.length)
            )
                throw new BenchError("GRAPH_REFERENCE", "Reference set must be unique and complete before limit 100");
            return {
                id: p.id,
                stratum: p.stratum,
                method,
                url: url.href,
                body,
                expectedStatus: rule.kind === "vertex" && rule.absent ? 404 : 200,
                rule,
            };
        }),
    }));
    const result = httpGoProfileSchema.parse({
        ...base,
        name: "graph-query",
        benchmark: undefined,
        endpoint: root.origin + (prefix || ""),
        cases,
        serviceChecks: true,
        target: {
            ...base.target,
            evidenceFiles: [
                ...new Set([
                    ...base.target.evidenceFiles.map((f) => resolve(dirname(resolve(profile)), f)),
                    resolve(reference),
                    ...(options.importSnapshot ? [resolve(options.importSnapshot)] : []),
                ]),
            ],
        },
        executor: {
            ...base.executor,
            binary: { ...base.executor.binary, path: resolve(dirname(resolve(profile)), base.executor.binary.path) },
        },
        graph: {
            suiteId: "hgbench-graph-query-v0.1",
            version: "0.1.0",
            bundleSha256: "0".repeat(64),
            dataset: source.dataset,
            ...(imported ? { imported } : {}),
            reference: { origin: source.origin, artifactSha256: await fileHash(resolve(reference)) },
            selection: "sequence-modulo-pool-v1",
        },
    });
    result.graph!.bundleSha256 = bundle(result);
    verifyGraphProfile(result);
    validateHttpProfile(goBaseProfile(result));
    await mkdir(dirname(resolve(options.out)), { recursive: true });
    let reused = false;
    if (options.reuse) {
        try {
            const previous = JSON.parse(await readFile(resolve(options.out), "utf8"));
            if (hash(previous) !== hash(result))
                throw new BenchError("GRAPH_PROFILE_CHANGED", "Existing graph profile differs from the current inputs");
            reused = true;
        } catch (e) {
            if ((e as NodeJS.ErrnoException).code !== "ENOENT") throw e;
        }
    }
    if (!reused)
        await writeFile(resolve(options.out), JSON.stringify(result, null, 2) + "\n", { flag: "wx", mode: 0o600 });
    return {
        profile: resolve(options.out),
        graph: result.graph,
        pools: result.cases.map((c) => ({
            caseId: c.id,
            size: c.requestPool!.length,
            strata: [...new Set(c.requestPool!.map((r) => r.stratum))],
        })),
        dispatched: false,
        performanceAccepted: false,
    };
}

export function validateGraphImport(
    snapshot: z.infer<typeof graphImportSnapshotSchema>,
    source: z.infer<typeof graphBindingSchema>["dataset"],
    minimum: number,
) {
    if (
        !Number.isFinite(minimum) ||
        minimum <= 0 ||
        minimum > 1 ||
        snapshot.sourceManifestSha256 !== source.manifestSha256 ||
        snapshot.expectedVertices !== source.vertices ||
        snapshot.expectedEdges !== source.edges ||
        snapshot.vertices !== source.vertices ||
        snapshot.edges > source.edges ||
        snapshot.edges / source.edges < minimum
    )
        throw new BenchError("GRAPH_IMPORT", "Import snapshot identity, counts or minimum edge coverage differs");
    const seen = new Set<string>();
    const totals: Record<"vertices" | "edges", [bigint, bigint]> = { vertices: [0n, 0n], edges: [0n, 0n] };
    for (const row of snapshot.labels) {
        const key = row.category + ":" + row.label;
        const expected = BigInt(row.expected),
            actual = BigInt(row.acknowledged);
        if (seen.has(key) || actual > expected || expected - actual !== BigInt(row.missing))
            throw new BenchError("GRAPH_IMPORT", "Invalid per-label import counts");
        seen.add(key);
        totals[row.category][0] += expected;
        totals[row.category][1] += actual;
    }
    if (
        totals.vertices[0] !== BigInt(source.vertices) ||
        totals.vertices[1] !== BigInt(snapshot.vertices) ||
        totals.edges[0] !== BigInt(source.edges) ||
        totals.edges[1] !== BigInt(snapshot.edges)
    )
        throw new BenchError("GRAPH_IMPORT", "Per-label import totals differ");
    const knows = snapshot.labels.find((r) => r.category === "edges" && r.label === "person_knows_person");
    if (!knows || knows.expected !== knows.acknowledged)
        throw new BenchError("GRAPH_IMPORT", "The frozen query reference requires complete person_knows_person edges");
}
