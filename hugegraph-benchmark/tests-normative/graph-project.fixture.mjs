/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
import { mkdtemp, mkdir, writeFile, rm, chmod, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createHash } from "node:crypto";
import http from "node:http";
export const sha = (raw) => createHash("sha256").update(raw).digest("hex");
export async function fixture(t, scale = 1) {
    const dir = await mkdtemp(join(tmpdir(), "hgbench-graph-project-"));
    t.after(() => rm(dir, { recursive: true, force: true }));
    const data = join(dir, "dataset"),
        home = join(dir, "loader");
    for (const path of ["prepared/vertices", "prepared/edges"]) await mkdir(join(data, path), { recursive: true });
    for (const path of ["bin", "lib", "conf"]) await mkdir(join(home, path), { recursive: true });
    const write = async (path, content) => {
        const raw = typeof content === "string" ? content : JSON.stringify(content, null, 2) + "\n";
        await writeFile(path, raw);
        return { sha256: sha(raw), bytes: Buffer.byteLength(raw) };
    };
    const files = {},
        struct = { vertices: [], edges: [] },
        shards = [];
    for (const [category, label, name, rows] of [
        ["vertices", "person", "person", [{ vertexId: "person:1" }, { vertexId: "person:2" }]],
        ["edges", "background", "background0", [{ sourceId: "person:1", targetId: "person:2", edgeKey: 0 }]],
        ["edges", "person_knows_person", "knows", [{ sourceId: "person:1", targetId: "person:2", edgeKey: 0 }]],
        ["edges", "background", "background1", [{ sourceId: "person:2", targetId: "person:1", edgeKey: 1 }]],
    ]) {
        const path = `${category}/${name}.json`;
        files[path] = await write(join(data, "prepared", path), rows.map((r) => JSON.stringify(r)).join("\n") + "\n");
        struct[category].push({
            label,
            input: { type: "file", path, format: "JSON" },
            ...(category === "vertices" ? { id: "vertexId" } : { source: ["sourceId"], target: ["targetId"] }),
        });
        shards.push({
            id: name,
            category,
            label,
            path,
            records: String(rows.length),
            bytes: String(files[path].bytes),
            sha256: files[path].sha256,
        });
    }
    files["struct.json"] = await write(join(data, "prepared/struct.json"), struct);
    files["schema.groovy"] = await write(
        join(data, "prepared/schema.groovy"),
        "// Fixture only. Not an actual HugeGraph schema.\n",
    );
    const prepared = {
        schemaVersion: 1,
        scope: "sharded-loader-input",
        sourceConverterSha256: "b".repeat(64),
        sourceAsset: { role: "initial", scale: String(scale), sha256: "a".repeat(64) },
        files,
        shards,
    };
    const preparedFact = await write(join(data, "prepared/prepare-manifest.json"), prepared);
    const set = { kind: "id-set", pointer: "/vertices", allowed: ["person:2"], count: 1 };
    const param = (rule, target) => ({
        id: "p0",
        stratum: "fixture",
        source: "person:1",
        ...(target ? { target } : {}),
        rule,
    });
    const reference = {
        schemaVersion: 1,
        kind: "graph-query-reference",
        dataset: {
            id: "HAND-WRITTEN-PROTOCOL-FIXTURE-not-an-SF-benchmark",
            scaleFactor: scale,
            manifestSha256: preparedFact.sha256,
            vertices: 2,
            edges: 3,
        },
        origin: "independent-csv",
        provenance: { scope: "Hand-written protocol answers; no graph performance result" },
        pools: {
            point: [param({ kind: "vertex", id: "person:1", label: "person", absent: false })],
            neighbors: [param(set)],
            "gremlin-neighbors": [param({ ...set, pointer: "/result/data" })],
            common: [param({ ...set, pointer: "/same_neighbors", allowed: [], count: 0 }, "person:2")],
            shortest: [
                param(
                    {
                        kind: "shortest-path",
                        pointer: "/path",
                        source: "person:1",
                        target: "person:2",
                        distance: 1,
                        edges: [["person:1", "person:2"]],
                    },
                    "person:2",
                ),
            ],
        },
    };
    const referenceFact = await write(join(data, "reference.json"), reference);
    await write(join(data, "dataset.json"), {
        schemaVersion: 1,
        kind: "graph-dataset",
        scaleFactor: scale,
        prepared: "prepared/prepare-manifest.json",
        reference: "reference.json",
        vertices: 2,
        edges: 3,
        artifacts: { "prepared/prepare-manifest.json": preparedFact.sha256, "reference.json": referenceFact.sha256 },
    });
    const state = { checks: 0, schemas: 0, loads: [], queries: 0, failLoad: false, rejectSchema: false };
    const server = http.createServer(async (req, res) => {
        const chunks = [];
        for await (const chunk of req) chunks.push(chunk);
        const body = chunks.length ? JSON.parse(Buffer.concat(chunks)) : null;
        let response;
        if (req.url === "/fixture-load") {
            state.loads.push(body);
            response = { failed: state.failLoad };
        } else if (body?.gremlin?.includes("hgbench-empty-v1")) {
            state.checks++;
            response = { status: { code: 200 }, result: { data: ["hgbench-empty-v1"] } };
        } else if (body?.gremlin?.includes("hgbench-schema-v1")) {
            state.schemas++;
            if (state.rejectSchema) {
                res.writeHead(500);
                res.end("{}");
                return;
            }
            response = { status: { code: 200 }, result: { data: ["hgbench-schema-v1"] } };
        } else {
            state.queries++;
            if (req.method === "POST") response = { result: { data: ["person:2"] } };
            else if (req.url.includes("graph/vertices/")) response = { id: "person:1", label: "person" };
            else if (req.url.includes("/kout")) response = { vertices: ["person:2"] };
            else if (req.url.includes("/sameneighbors")) response = { same_neighbors: [] };
            else if (req.url.includes("/shortestpath")) response = { path: ["person:1", "person:2"] };
            else {
                res.writeHead(404);
                res.end("{}");
                return;
            }
        }
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify(response));
    });
    await new Promise((r) => server.listen(0, "127.0.0.1", r));
    t.after(
        () =>
            new Promise((r) => {
                server.closeAllConnections();
                server.close(r);
            }),
    );
    const origin = `http://127.0.0.1:${server.address().port}`;
    await write(
        join(home, "bin/hugegraph-loader.sh"),
        `#!${process.execPath}
const args=process.argv.slice(2), opt=n=>args[args.indexOf(n)+1];
const fs=await import('node:fs/promises');
const mapping=JSON.parse(await fs.readFile(opt('--file'),'utf8'));
const category=mapping.vertices.length?'vertex':'edge';
const entry=mapping.vertices[0]??mapping.edges[0];
const records=(await fs.readFile(entry.input.path,'utf8')).trim().split('\\n').length;
if(opt('--retry-times')!=='0'||opt('--check-vertex')!=='false') process.exit(3);
const response=await fetch(${JSON.stringify(origin)}+'/fixture-load',{method:'POST',body:JSON.stringify({category,label:entry.label,records})});
if((await response.json()).failed) process.exit(2);
for(const kind of ['vertex','edge']) for(const phase of ['parse','insert']) for(const outcome of ['success','failure'])
 console.log(kind+' '+phase+' '+outcome+' : '+(kind===category&&outcome==='success'?records:0));
console.log('All the insert tasks finished');
`,
    );
    // The launcher is extension .sh to match the actual Loader distribution.
    // Node must treat its fixture body as CommonJS-compatible code.
    const launcher = join(home, "bin/hugegraph-loader.sh");
    const script = await readFile(launcher, "utf8");
    await writeFile(
        launcher,
        script.slice(0, script.indexOf("\n") + 1) +
            "(async()=>{\n" +
            script.slice(script.indexOf("\n") + 1) +
            "\n})().catch(e=>{console.error(e);process.exit(1)});\n",
    );
    await chmod(launcher, 0o700);
    return {
        dir,
        data,
        home,
        state,
        origin,
        graphUrl: origin + "/graphspaces/DEFAULT/graphs/bench_sf1",
        traversalSource: "__g_bench_sf1",
        write,
    };
}
