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

import { isDeepStrictEqual } from "node:util";
import { performance } from "node:perf_hooks";

export function normalizedVertex(value) {
    if (!value || !Object.hasOwn(value, "id") || typeof value.label !== "string" || !value.properties)
        throw new Error("Malformed vertex envelope");
    return { id: value.id, label: value.label, properties: value.properties };
}

function array(body, name) {
    if (!Array.isArray(body?.[name])) throw new Error(`Missing ${name} array`);
    return body[name];
}

function idsFromEdges(body, source) {
    return array(body, "edges").map((edge) => {
        if (edge.outV !== source || edge.label !== "link" || !Object.hasOwn(edge, "inV"))
            throw new Error("Wrong edge direction, source, label or target");
        return edge.inV;
    });
}

export function normalize(parameter, trace) {
    if (!trace.length) throw new Error("No response evidence");
    const { operation, args } = parameter;
    const last = trace.at(-1);
    const body = last.body;
    if (operation !== "paged-neighbors" && trace.length !== 1) throw new Error("Unexpected extra responses");
    if (operation === "point" && last.status === 404) {
        if (
            typeof body?.exception === "string" &&
            body.exception.endsWith("NotFoundException") &&
            typeof body.message === "string" &&
            /vertex/i.test(body.message)
        )
            return null;
        throw new Error("404 does not establish vertex absence");
    }
    if (trace.some((entry) => entry.status !== 200)) throw new Error("Non-success HTTP response");
    if (operation === "point") return normalizedVertex(body);
    if (operation === "filter") return array(body, "vertices").map((v) => normalizedVertex(v).id);
    if (operation === "materialize") return array(body, "vertices").map(normalizedVertex);
    if (operation === "neighbors") return array(body, "vertices");
    if (operation === "kout") return array(body, "vertices");
    if (operation === "common") return array(body, "same_neighbors");
    if (operation === "shortest") {
        const path = array(body, "path");
        return path.length ? path : null;
    }
    if (operation === "paged-neighbors") {
        if (trace.length > 128) throw new Error("Page limit exceeded");
        let cursor = "";
        const seen = new Set();
        return trace.map((entry, index) => {
            if (entry.query.page !== cursor || seen.has(cursor)) throw new Error("Broken pagination chain");
            seen.add(cursor);
            if (!Object.hasOwn(entry.body, "page")) throw new Error("Missing pagination completion marker");
            const next = entry.body.page;
            const exhausted = next === null || next === "";
            if (!exhausted && typeof next !== "string") throw new Error("Malformed cursor");
            if (exhausted !== (index === trace.length - 1)) throw new Error("Incomplete or excessive page sequence");
            const ids = idsFromEdges(entry.body, args.source);
            if (ids.length > args.pageSize || (!exhausted && ids.length === 0)) throw new Error("Invalid page size");
            cursor = next;
            return ids;
        });
    }
    throw new Error(`Unsupported operation: ${operation}`);
}

export function requestFor(parameter, cursor = "") {
    const { operation, args } = parameter;
    const traversal = { direction: "OUT", label: "link", max_degree: -1 };
    if (operation === "point")
        return { path: `graph/vertices/${encodeURIComponent(JSON.stringify(args.id))}`, query: {} };
    if (operation === "filter" || operation === "materialize")
        return {
            path: "graph/vertices",
            query: { label: "node", properties: JSON.stringify({ group: args.group }), limit: -1 },
        };
    if (operation === "neighbors" || operation === "kout")
        return {
            path: "traversers/kout",
            query: {
                ...traversal,
                source: JSON.stringify(args.source),
                max_depth: args.depth ?? 1,
                nearest: args.nearest ?? true,
                capacity: -1,
                limit: args.limit ?? -1,
            },
        };
    if (operation === "common")
        return {
            path: "traversers/sameneighbors",
            query: {
                ...traversal,
                vertex: JSON.stringify(args.sources[0]),
                other: JSON.stringify(args.sources[1]),
                limit: -1,
            },
        };
    if (operation === "shortest")
        return {
            path: "traversers/shortestpath",
            query: {
                ...traversal,
                source: JSON.stringify(args.source),
                target: JSON.stringify(args.target),
                max_depth: args.maxDepth,
                capacity: -1,
                skip_degree: 0,
                with_vertex: false,
                with_edge: false,
            },
        };
    if (operation === "paged-neighbors")
        return {
            path: "graph/edges",
            query: {
                vertex_id: JSON.stringify(args.source),
                direction: "OUT",
                label: "link",
                limit: args.pageSize,
                page: cursor,
            },
        };
    throw new Error(`Unsupported operation: ${operation}`);
}

export function checkTrace(parameter, trace) {
    let cursor = "";
    for (const entry of trace) {
        const expected = requestFor(parameter, cursor);
        if (entry.method !== "GET" || entry.path !== expected.path || !isDeepStrictEqual(entry.query, expected.query))
            throw new Error("Recorded request does not match task semantics");
        cursor = entry.body?.page;
    }
    return normalize(parameter, trace);
}

export class HugeGraphRest {
    constructor(graphUrl, { authorization, timeoutMs = 30000, maxBytes = 64 * 1024 * 1024 } = {}) {
        const url = new URL(graphUrl);
        if (!["http:", "https:"].includes(url.protocol) || url.username || url.password || url.search || url.hash)
            throw new Error("Use a plain http(s) graph URL; pass authentication via an environment variable");
        if (!/\/graphs\/[^/]+\/?$/.test(url.pathname)) throw new Error("URL must identify one explicit graph");
        this.base = url.href.replace(/\/$/, "") + "/";
        this.authorization = authorization;
        this.timeoutMs = timeoutMs;
        this.maxBytes = maxBytes;
    }

    async request(path, query = {}, { method = "GET", body, signal, budget = { bytes: 0 } } = {}) {
        const url = new URL(path, this.base);
        for (const [name, value] of Object.entries(query)) url.searchParams.set(name, String(value));
        const headers = { Accept: "application/json" };
        if (this.authorization) headers.Authorization = this.authorization;
        if (body !== undefined) headers["Content-Type"] = "application/json";
        const response = await fetch(url, {
            method,
            headers,
            body: body === undefined ? undefined : JSON.stringify(body),
            signal: signal ?? AbortSignal.timeout(this.timeoutMs),
            redirect: "error",
        });
        const chunks = [];
        try {
            for await (const chunk of response.body) {
                budget.bytes += chunk.length;
                if (budget.bytes > this.maxBytes) throw new Error("Response evidence limit exceeded");
                chunks.push(chunk);
            }
        } catch (error) {
            await response.body?.cancel().catch(() => {});
            throw error;
        }
        const data = JSON.parse(Buffer.concat(chunks).toString("utf8"));
        return { method, path, query, status: response.status, body: data };
    }

    async execute(parameter) {
        const start = performance.now();
        const trace = [];
        const signal = AbortSignal.timeout(this.timeoutMs);
        const budget = { bytes: 0 };
        try {
            let cursor = "";
            const seen = new Set();
            do {
                if (seen.has(cursor) || trace.length >= 128) throw new Error("Pagination cursor loop or page limit");
                seen.add(cursor);
                const request = requestFor(parameter, cursor);
                const response = await this.request(request.path, request.query, { signal, budget });
                trace.push(response);
                if (parameter.operation !== "paged-neighbors" || response.status !== 200) break;
                if (!Object.hasOwn(response.body, "page")) throw new Error("Missing pagination completion marker");
                cursor = response.body.page;
            } while (cursor !== null && cursor !== "");
            const actual = checkTrace(parameter, trace);
            return { status: "response", durationMs: performance.now() - start, bytes: budget.bytes, trace, actual };
        } catch (error) {
            return {
                status: signal.aborted ? "timeout" : "error",
                durationMs: performance.now() - start,
                bytes: budget.bytes,
                trace,
                error: String(error),
            };
        }
    }

    async require(path, query = {}, options = {}) {
        const response = await this.request(path, query, options);
        if (response.status < 200 || response.status >= 300) throw new Error(`HTTP ${response.status} at ${path}`);
        return response.body;
    }

    async assertEmpty() {
        for (const [path, key] of [
            ["graph/vertices", "vertices"],
            ["graph/edges", "edges"],
            ["schema/propertykeys", "propertykeys"],
            ["schema/vertexlabels", "vertexlabels"],
            ["schema/edgelabels", "edgelabels"],
            ["schema/indexlabels", "indexlabels"],
        ]) {
            const body = await this.require(path, path.startsWith("graph/") ? { limit: 1 } : {});
            if (array(body, key).length) throw new Error("Import requires a completely empty dedicated graph");
        }
    }

    async importTiny(graph) {
        await this.assertEmpty();
        const post = (path, body) => this.require(path, {}, { method: "POST", body });
        for (const [name, data_type] of [
            ["group", "TEXT"],
            ["rank", "LONG"],
            ["payload", "TEXT"],
        ])
            await post("schema/propertykeys", { name, data_type, cardinality: "SINGLE", check_exist: false });
        await post("schema/vertexlabels", {
            name: "node",
            id_strategy: graph.idType === "numeric" ? "CUSTOMIZE_NUMBER" : "CUSTOMIZE_STRING",
            properties: ["group", "rank", "payload"],
            nullable_keys: [],
            enable_label_index: true,
            check_exist: false,
        });
        await post("schema/edgelabels", {
            name: "link",
            source_label: "node",
            target_label: "node",
            frequency: "SINGLE",
            properties: [],
            check_exist: false,
        });
        const index = await post("schema/indexlabels", {
            name: "node_by_group",
            base_type: "VERTEX_LABEL",
            base_value: "node",
            index_type: "SECONDARY",
            fields: ["group"],
            check_exist: false,
        });
        if (index.task_id !== undefined && index.task_id !== 0 && index.task_id !== null) {
            const deadline = performance.now() + this.timeoutMs;
            for (;;) {
                if (performance.now() >= deadline) throw new Error("Index task readiness timeout");
                const task = await this.require(`tasks/${encodeURIComponent(index.task_id)}`);
                const status = String(task.task_status).toLowerCase();
                if (status === "success") break;
                if (["failed", "cancelled"].includes(status)) throw new Error(`Index task ${status}`);
                await new Promise((resolve) => setTimeout(resolve, 100));
            }
        }
        await post("graph/vertices/batch", graph.vertices);
        for (let i = 0; i < graph.edges.length; i += 100)
            await post(
                "graph/edges/batch",
                graph.edges.slice(i, i + 100).map((edge) => ({
                    label: "link",
                    outV: edge.source,
                    inV: edge.target,
                    outVLabel: "node",
                    inVLabel: "node",
                    properties: {},
                })),
            );
        return this.snapshotTiny();
    }

    async snapshotTiny() {
        return {
            vertices: await this.require("graph/vertices", { limit: 10000 }),
            edges: await this.require("graph/edges", { limit: 10000 }),
            indexes: await this.require("schema/indexlabels"),
        };
    }
}

export function verifySnapshot(graph, snapshot) {
    const sort = (items) =>
        [...items].sort((a, b) =>
            JSON.stringify(a) < JSON.stringify(b) ? -1 : JSON.stringify(a) > JSON.stringify(b) ? 1 : 0,
        );
    if (!isDeepStrictEqual(sort(array(snapshot.vertices, "vertices").map(normalizedVertex)), sort(graph.vertices)))
        throw new Error("Imported vertices or properties differ from the locked dataset");
    const edges = array(snapshot.edges, "edges").map((e) => [e.outV, e.inV, e.label]);
    if (!isDeepStrictEqual(sort(edges), sort(graph.edges.map((e) => [e.source, e.target, e.label]))))
        throw new Error("Imported edges differ from the locked dataset");
    const indexes = array(snapshot.indexes, "indexlabels");
    if (
        indexes.length !== 1 ||
        !indexes.some(
            (i) =>
                i.name === "node_by_group" &&
                i.base_value === "node" &&
                String(i.index_type).toUpperCase() === "SECONDARY" &&
                isDeepStrictEqual(i.fields, ["group"]),
        )
    )
        throw new Error("Required index differs from the declared index set");
}
