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
import { generate, oracle, validate, canonicalAnswer, verify } from "../benchmarks/core-read-v0.1/reference.mjs";

const assets = generate();
const get = (caseId, suffix) => {
    const parameter = assets.parameters.find((p) => p.parameterId === `${caseId}-numeric-${suffix}`);
    const graph = assets.datasets.find((g) => g.id === parameter.datasetId);
    return { parameter, graph, expected: oracle(graph, parameter) };
};
const check = (item, actual) => validate(item.graph, item.parameter, item.expected, actual);

test("locked draft assets reproduce byte-for-byte and cover all 12 families", async () => {
    const result = await verify();
    assert.equal(result.cases, 12);
    assert.equal(result.parameters, 36);
    assert.equal(result.performanceValidated, false);
    assert.deepEqual(generate(), assets);
});

test("hand-counted topology and expected expansion distinguish all Kout modes", () => {
    const graph = assets.datasets[0];
    assert.equal(graph.vertices.length, 110);
    assert.equal(graph.edges.length, 282);
    assert.deepEqual(new Set(get("CR05", "wide").expected), new Set(Array.from({ length: 64 }, (_, i) => 100 + i)));
    assert.deepEqual(
        new Set(get("CR06", "convergent").expected),
        new Set(Array.from({ length: 16 }, (_, i) => 1100 + i)),
    );
    assert.deepEqual(get("CR08", "nearest").expected, [2003]);
    assert.deepEqual(get("CR08", "revisit").expected, [2000, 2003]);
    assert.deepEqual(get("CR08", "empty").expected, []);
});

test("set validation rejects missing, duplicate, foreign and mistyped IDs", () => {
    const item = get("CR05", "wide");
    assert.equal(check(item, [...item.expected].reverse()), true);
    for (const bad of [
        item.expected.slice(1),
        [...item.expected, item.expected[0]],
        [...item.expected.slice(1), 99999],
        item.expected.map(String),
        null,
    ])
        assert.equal(check(item, bad), false);
});

test("limited traversal admits different legal subsets but requires exact distinct count", () => {
    const item = get("CR07", "bounded");
    assert.equal(check(item, [100, 101, 102, 103]), true);
    assert.equal(check(item, [160, 161, 162, 163]), true);
    for (const bad of [[100], [100, 100, 101, 102], [100, 101, 102, 999], [100, 101, 102, 103, 104]]) {
        assert.equal(check(item, bad), false);
    }
});

test("shortest path validates tied answers, direction, length and absence", () => {
    const item = get("CR10", "tie");
    assert.deepEqual(item.expected, { distance: 3 });
    assert.equal(check(item, [3000, 3001, 3003, 3004]), true);
    assert.equal(check(item, [3000, 3002, 3003, 3004]), true);
    for (const bad of [null, [3000, 3004], [3000, 3003, 3001, 3004], [3004, 3003, 3001, 3000]]) {
        assert.equal(check(item, bad), false);
    }
    const absent = get("CR11", "unreachable");
    assert.equal(check(absent, null), true);
    assert.equal(check(absent, []), false);
});

test("pagination requires all rows exactly once and bounds pages", () => {
    const item = get("CR04", "pages");
    const pages = canonicalAnswer(item.graph, item.parameter, item.expected);
    assert.equal(check(item, pages), true);
    assert.equal(check(item, [...pages, []]), true);
    assert.equal(check(item, pages.slice(1)), false);
    assert.equal(check(item, [item.expected]), false);
    assert.equal(check(item, [[], ...pages]), false);
    assert.equal(check(item, [...pages, pages[0]]), false);
});

test("point and materialization require full projection and exact ID types", () => {
    const item = get("CR12", "materialize");
    assert.equal(check(item, [...item.expected].reverse()), true);
    const missing = structuredClone(item.expected);
    delete missing[0].properties.payload;
    assert.equal(check(item, missing), false);
    const changed = structuredClone(item.expected);
    changed[0].properties.payload += "x";
    assert.equal(check(item, changed), false);
    assert.equal(check(get("CR01", "hit"), null), false);
    assert.equal(check(get("CR01", "miss"), null), true);
});

test("numeric and string datasets have isomorphic topology and typed answers", () => {
    const [numeric, string] = assets.datasets;
    assert.deepEqual(
        string.edges.map((e) => [e.source, e.target]),
        numeric.edges.map((e) => [`v${e.source}`, `v${e.target}`]),
    );
    for (const parameter of assets.parameters) {
        const graph = assets.datasets.find((g) => g.id === parameter.datasetId);
        const expected = oracle(graph, parameter);
        assert.equal(validate(graph, parameter, expected, canonicalAnswer(graph, parameter, expected)), true);
    }
});
