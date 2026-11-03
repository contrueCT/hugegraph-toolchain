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
import { queries, request } from "../adapters/ldbc-short-reads.mjs";

test("LDBC bindings preserve 64-bit IDs and keep data out of query source", () => {
    const result = request("IS1", "9223372036854775807", "hgbench_g");
    assert.equal(result.bindings.vertexId, "person:9223372036854775807");
    assert.equal(result.aliases.g, "hgbench_g");
    assert.equal(request("IS1", "42", "__g_DEFAULT-hgbench_unit").aliases.g, "__g_DEFAULT-hgbench_unit");
    assert.throws(() => request("IS1", "42", "g".repeat(129)), /alias/);
    assert(!result.gremlin.includes("9223372036854775807"));
    assert.equal(request("IS4", "42", "g").bindings.vertexId, "message:42");
    for (const bad of [42, "01", "-1", "9223372036854775808", "1');g.V().drop()"])
        assert.throws(() => request("IS1", bad, "g"));
    assert.throws(() => request("IC1", "42", "g"), /Unsupported/);
    assert.throws(() => request("IS5", "42", "g;drop"), /alias/);
});

test("all seven short-read bindings are exposed and IS2 limit stays outside query source", () => {
    assert.deepEqual(
        Object.keys(queries),
        Array.from({ length: 7 }, (_, i) => `IS${i + 1}`),
    );
    for (const id of Object.keys(queries))
        assert.equal(request(id, "42", "g").bindings.vertexId, `${id <= "IS3" ? "person" : "message"}:42`);
    assert.equal(request("IS2", "42", "g").bindings.queryLimit, 10);
    assert.equal(request("IS2", "42", "g", { limit: 7 }).bindings.queryLimit, 7);
    for (const limit of [0, -1, 1.5, "10", 2147483648]) assert.throws(() => request("IS2", "42", "g", { limit }));
    for (const options of [null, [], { unknown: true }, { limit: 2 }])
        assert.throws(() => request("IS1", "42", "g", options));
});
