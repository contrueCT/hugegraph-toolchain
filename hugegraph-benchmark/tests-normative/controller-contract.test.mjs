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
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { freezePlan } from "../dist/plans.js";
import { submit, wait, cancel } from "../dist/store.js";

test("fixture controller reaches analysis without requiring unrelated SF100 contracts", async () => {
    const root = await mkdtemp(join(tmpdir(), "hgbench-controller-contract-"));
    let id;
    try {
        const plan = await freezePlan("examples/controller-smoke.json");
        assert.equal(plan.scope, "controller-fixture");
        id = (await submit(root, plan)).experimentId;
        const result = await wait(root, id, 15000);
        assert.equal(result.state, "completed", JSON.stringify(result));
        const analysis = JSON.parse(await readFile(join(result.evidenceDir, "analysis", "result.json"), "utf8"));
        assert.equal(analysis.verdict, "descriptive-only");
    } finally {
        if (id) {
            await cancel(root, id);
            await wait(root, id, 5000);
        }
        await rm(root, { recursive: true, force: true });
    }
});
