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

import { readFile, writeFile } from "node:fs/promises";
import { performance } from "node:perf_hooks";
import { setTimeout as sleep } from "node:timers/promises";

// This fixture tests the controller protocol. It is NOT a HugeGraph workload.
const args = process.argv.slice(2);
if (args.includes("--describe")) {
    process.stdout.write(
        JSON.stringify({
            protocolVersion: 1,
            id: "fixture-executor",
            version: "1.0.0",
            kind: "executor",
            scopes: ["controller-fixture"],
            configurationSchema: {
                type: "object",
                properties: {
                    caseConfig: {
                        type: "object",
                        properties: {
                            size: { type: "integer", minimum: 1, maximum: 1000000 },
                            samples: { type: "integer", minimum: 1, maximum: 1000 },
                        },
                        additionalProperties: false,
                    },
                    variantConfig: {
                        type: "object",
                        properties: {
                            delayMs: { type: "number", minimum: 0, maximum: 1000 },
                            fault: { enum: ["none", "wrong-result", "crash", "malformed", "hang"] },
                        },
                        additionalProperties: false,
                    },
                },
                additionalProperties: false,
            },
        }) + "\n",
    );
} else {
    const option = (name) => args[args.indexOf(name) + 1];
    if (!args.includes("--request") || !args.includes("--output"))
        throw new Error("--request and --output are required");
    const request = JSON.parse(await readFile(option("--request"), "utf8"));
    if (request.protocolVersion !== 1 || request.kind !== "execution" || request.scope !== "controller-fixture") {
        throw new Error("Unsupported protocol or scope");
    }
    const { size = 1000, samples: count = 8 } = request.caseConfig;
    const { delayMs = 0, fault = "none" } = request.variantConfig;
    if (
        Object.keys(request.caseConfig).some((key) => !["size", "samples"].includes(key)) ||
        Object.keys(request.variantConfig).some((key) => !["delayMs", "fault"].includes(key))
    )
        throw new Error("Unknown config key");
    if (
        !Number.isInteger(size) ||
        size < 1 ||
        size > 1000000 ||
        !Number.isInteger(count) ||
        count < 1 ||
        count > 1000 ||
        typeof delayMs !== "number" ||
        !Number.isFinite(delayMs) ||
        delayMs < 0 ||
        delayMs > 1000 ||
        !["none", "wrong-result", "crash", "malformed", "hang"].includes(fault)
    )
        throw new Error("Invalid fixture config");
    if (fault === "crash") throw new Error("Deliberate executor failure for controller testing");
    if (fault === "hang") await sleep(600000);
    const expected = (size * (size + 1)) / 2;
    const samples = [];
    for (let n = 0; n < count; n++) {
        const start = performance.now();
        if (delayMs) await sleep(delayMs);
        let actual = 0;
        for (let i = 1; i <= size; i++) actual += i;
        if (fault === "wrong-result") actual++;
        const durationMs = performance.now() - start;
        samples.push({ durationMs, correct: actual === expected });
    }
    const result = {
        protocolVersion: 1,
        kind: "execution",
        scope: "controller-fixture",
        experimentId: request.experimentId,
        runId: request.runId,
        caseId: request.caseId,
        variantId: request.variantId,
        block: request.block,
        samples,
        validation: { checked: samples.length, failed: samples.filter((sample) => !sample.correct).length },
    };
    await writeFile(option("--output"), fault === "malformed" ? "not json" : JSON.stringify(result) + "\n");
}
