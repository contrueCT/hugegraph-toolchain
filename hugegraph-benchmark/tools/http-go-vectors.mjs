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

import { writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { expectedData, inspect } from "../benchmarks/http-serving-v0.1/reference.mjs";
import { argumentsFor, entry, valueDigest } from "../components/http/common.mjs";
export async function goldenVectors() {
    const suite = await inspect();
    const rows = suite.questions.map((q) => ({
        id: q.id,
        category: "suite",
        json: JSON.stringify(expectedData(q)),
        expected: q.answer.canonicalSha256,
    }));
    const edges = [
        ["html", '"<>&"'],
        ["line-separators", '"\u2028\u2029"'],
        ["numeric-keys", '{"10":10,"2":2,"a":0}'],
        ["utf16-keys", '{"\\ue000":1,"\\ud800\\udc00":2}'],
        ["negative-zero", "-0"],
        ["unsafe-integer", "9007199254740993"],
        ["float-boundaries", "[1e-7,1e-6,1e20,1e21,333333333.33333329]"],
        ["lone-surrogate", '"\\ud800"'],
        ["duplicate-keys", '{"x":1,"x":2}'],
        ["overflow", "1e400"],
        ["pointer-escaping", '{"a/b":{"~key":[null,false,"图🙂"]}}'],
    ];
    for (const [id, json] of edges) rows.push({ id, category: "edge", json, expected: valueDigest(JSON.parse(json)) });
    let x = 0xabc124fed8793456n;
    for (let i = 0; i < 10000; i++) {
        x = BigInt.asUintN(64, x ^ (x << 13n));
        x = BigInt.asUintN(64, x ^ (x >> 7n));
        x = BigInt.asUintN(64, x ^ (x << 17n));
        const b = Buffer.alloc(8);
        b.writeBigUInt64LE(x);
        const n = b.readDoubleLE();
        if (!Number.isFinite(n)) continue;
        rows.push({ id: `ieee64-${i}`, category: "ieee64", json: JSON.stringify(n), expected: valueDigest(n) });
    }
    for (const json of [
        "-1e400",
        "-1e-999",
        "5e-324",
        "1.7976931348623157e308",
        "2.2250738585072014e-308",
        '{"__proto__":1,"4294967295":2,"4294967294":3,"00":4,"0":5}',
        '"\\ud800x\\udc00"',
    ])
        rows.push({ id: `extra-${rows.length}`, category: "edge", json, expected: valueDigest(JSON.parse(json)) });
    return { rows, suiteBundleSha256: suite.bundleSha256 };
}
if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href)
    await entry(async () => {
        const args = argumentsFor(["--out"]),
            v = await goldenVectors();
        await writeFile(resolve(args["--out"]), JSON.stringify(v.rows) + "\n", { flag: "wx", mode: 0o600 });
        console.log(
            JSON.stringify({
                vectors: v.rows.length,
                suiteCases: 11,
                node: process.version,
                suiteBundleSha256: v.suiteBundleSha256,
                file: resolve(args["--out"]),
            }),
        );
    }, "Deterministic cross-language codec vectors: --out NEW-json-file. Full 11-question independent JS oracle, UTF-16/numeric/escape edges and 10,000 fixed-seed IEEE64 samples; no target requests. Set HGBENCH_GO_GOLDEN to this file when running Go component tests.");
