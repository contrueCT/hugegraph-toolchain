/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { bytesDigest } from "../components/http/common.mjs";
import { observerConditionsFixture } from "./fixtures/http-observer-conditions.mjs";

test("observer conditions read the locked real-layout variants and all sixteen physical-arm settings", async (t) => {
    const f = await observerConditionsFixture(t),
        result = await f.read();
    assert.equal(f.documents.has("settings.json"), false);
    assert.equal(result.settings, null);
    assert.deepEqual(
        result.settingsVariants.map((v) => v.id),
        ["jvm-off", "jvm-on"],
    );
    assert.equal(result.profiles.length, 16);
    assert.equal(result.observationArms.length, 16);
    assert.equal(result.observationArms.filter((a) => a.jvmObservation).length, 12);
    assert.equal(result.observationArms.filter((a) => a.collector).length, 12);
    for (const arm of result.observationArms) {
        const variant = result.settingsVariants[arm.jvmObservation ? 1 : 0];
        assert.equal(arm.settingsVariantId, variant.id);
        assert.equal(arm.settingsSha256, variant.sha256);
        assert.equal(
            arm.armSettingsSha256,
            bytesDigest(await readFile(join(f.plan, "arms/" + arm.cellId + "/settings.json"))),
        );
        assert.equal(result.profiles.find((p) => p.id === arm.id).id, arm.id);
    }
});

test("observer missing or changed settings members cannot be replaced by one arbitrary settings snapshot", async (t) => {
    const f = await observerConditionsFixture(t);
    f.documents.delete("settings-jvm-off.json");
    f.documents.set("settings.json", f.documents.get("settings-jvm-on.json"));
    await f.seal();
    await assert.rejects(f.read(), { code: "ENGINEERING_CONDITION_MEMBER" });
    const changed = await observerConditionsFixture(t);
    await writeFile(join(changed.plan, "settings-jvm-on.json"), "{}\n");
    await assert.rejects(changed.read(), { code: "ENGINEERING_CONDITION_CHANGED" });
});

test("observer variant differences and per-arm variant substitutions remain structural failures after resealing", async (t) => {
    const different = await observerConditionsFixture(t);
    different.documents.get("settings-jvm-on.json").server.workerThreads = 9;
    await different.seal();
    await assert.rejects(different.read(), { code: "ENGINEERING_OBSERVER_SETTINGS_VARIANTS" });
    const swapped = await observerConditionsFixture(t),
        arm = swapped.documents.get("sequence.json").physicalArms.find((a) => !a.jvmObservation);
    swapped.documents.set("arms/" + arm.cellId + "/settings.json", swapped.documents.get("settings-jvm-on.json"));
    await swapped.seal();
    await assert.rejects(swapped.read(), { code: "ENGINEERING_OBSERVER_ARM_SETTINGS_VARIANT" });
});

test("observer physical-arm association, identity and full sixteen-arm denominator are required", async (t) => {
    for (const mutate of [
        (arms) => {
            arms[0].jvmObservation = !arms[0].jvmObservation;
        },
        (arms) => {
            arms[0].runId = arms[1].runId;
        },
        (arms) => {
            arms.pop();
        },
        (arms) => {
            arms[0] = null;
        },
    ]) {
        const f = await observerConditionsFixture(t);
        mutate(f.documents.get("sequence.json").physicalArms);
        await f.seal();
        await assert.rejects(f.read(), { code: "ENGINEERING_OBSERVER_ARM_SETTINGS_IDENTITY" });
    }
});
