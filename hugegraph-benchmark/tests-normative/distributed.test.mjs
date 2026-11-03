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

import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, readFile, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import {
    facts,
    size,
    neighbors,
    parameterAt,
    expected,
    generateTo,
    cases,
} from "../benchmarks/distributed-read-v0.1/generator.mjs";

test("distributed scales have fixed counts up to billion-edge size without materializing a graph", () => {
    assert.equal(facts("dev").edges, 73664);
    assert.equal(facts("s1").edges, 9428992);
    assert.equal(facts("s2").edges, 150863872);
    assert.equal(facts("s3").edges, 1206910976);
    assert.equal(facts("s3").vertices, 134217728);
    assert.throws(() => size("arbitrary"));
});

test("all development vertices have the declared degree, a global ring edge, no duplicate or self edge", () => {
    const n = size("dev");
    let edges = 0;
    for (let id = 1; id <= n; id++) {
        const targets = neighbors(id, n);
        assert.equal(targets.length, (id - 1) % 1024 === 0 ? 1024 : 8);
        assert.equal(new Set(targets).size, targets.length);
        assert(targets.includes((id % n) + 1));
        assert(!targets.includes(id));
        assert(targets.every((target) => target >= 1 && target <= n));
        edges += targets.length;
    }
    assert.equal(edges, 73664);
});

test("performance parameter streams cover their entire source domains rather than replaying validation samples", () => {
    const n = size("dev");
    for (const [caseId, count, field] of [
        ["DR01", n, "id"],
        ["DR04", n - n / 1024, "source"],
        ["DR05", n / 1024, "source"],
        ["DR03", 4096, "group"],
    ]) {
        const values = new Set(Array.from({ length: count }, (_, i) => parameterAt(caseId, i, "dev").args[field]));
        assert.equal(values.size, count, caseId);
        if (caseId === "DR04") assert([...values].every((v) => (v - 1) % 1024 !== 0));
    }
    for (const caseId of cases) assert.notEqual(expected(parameterAt(caseId, 0, "dev"), "dev"), undefined);
});

test("generated files are byte reproducible and independent SQLite queries verify every golden answer", async () => {
    const root = await mkdtemp(join(tmpdir(), "hgbench-data-test-"));
    const verifier = fileURLToPath(new URL("../tools/verify-distributed.py", import.meta.url));
    try {
        const first = await generateTo("dev", join(root, "one"));
        const second = await generateTo("dev", join(root, "two"));
        assert.deepEqual(first, second);
        await assert.rejects(generateTo("dev", join(root, "one")), /EEXIST/);
        const run = spawnSync("python3", [verifier, join(root, "one")], { encoding: "utf8", timeout: 30000 });
        assert.equal(run.status, 0, run.stdout + run.stderr);
        assert.equal(JSON.parse(run.stdout).validatedParameters, 192);
        // Repairing the file checksum must not make a deliberately wrong golden answer acceptable.
        const path = join(root, "one", "validation.ndjson");
        const lines = (await readFile(path, "utf8")).trimEnd().split("\n");
        const item = JSON.parse(lines[0]);
        item.expected = null;
        lines[0] = JSON.stringify(item);
        const contents = lines.join("\n") + "\n";
        await writeFile(path, contents);
        first.files["validation.ndjson"] = {
            records: lines.length,
            bytes: Buffer.byteLength(contents),
            sha256: createHash("sha256").update(contents).digest("hex"),
        };
        await writeFile(join(root, "one", "manifest.json"), JSON.stringify(first));
        const wrong = spawnSync("python3", [verifier, join(root, "one")], { encoding: "utf8", timeout: 30000 });
        assert.equal(wrong.status, 1);
        assert.match(wrong.stdout, /Incorrect answer/);
    } finally {
        await rm(root, { recursive: true, force: true });
    }
});
