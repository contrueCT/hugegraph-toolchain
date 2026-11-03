/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
import { mkdtemp, mkdir, writeFile, readFile, rm } from "node:fs/promises";
import { dirname, join } from "node:path";
import { tmpdir } from "node:os";
import { bytesDigest } from "../../components/http/common.mjs";
import { balancedOrders } from "../../components/paired/core.mjs";
import { OBSERVER003, observer003Schedule } from "../../components/http/observer003-contract.mjs";
import { readEngineeringConditions } from "../../components/http/engineering-acceptance.mjs";

// Metadata protocol fixture only: no raw, real target, resource proof or accepted
// calibration contribution is manufactured by this helper.
export async function observerConditionsFixture(t, options = {}) {
    const root = await mkdtemp(join(tmpdir(), "hgbench-observer-conditions-"));
    t.after(() => rm(root, { recursive: true, force: true }));
    const plan = join(root, "plan"),
        inputFile = join(root, "input.json"),
        off = structuredClone(options.settings ?? { kind: "hugegraph-http-environment-settings", server: {} }),
        observation = off.jvmObservation ?? { preset: "protocol-fixture-only" };
    delete off.jvmObservation;
    const on = { ...structuredClone(off), jvmObservation: observation },
        sequence = {
            attemptToken: "0123456789abcdef",
            blockOrders: Object.fromEntries(
                ["J", "C"].map((group) => [
                    group,
                    balancedOrders(OBSERVER003.pairs, OBSERVER003.id + ":" + group + ":order-v1"),
                ]),
            ),
        };
    sequence.physicalArms = observer003Schedule(sequence.blockOrders).map((arm) => ({
        ...arm,
        runId: "hoc3-" + sequence.attemptToken + "-" + arm.cellId.toLowerCase(),
    }));
    const documents = new Map([
        ["sequence.json", sequence],
        ["settings-jvm-off.json", off],
        ["settings-jvm-on.json", on],
        ["build.json", { protocolFixture: true }],
        ["environment.json", { protocolFixture: true }],
        ["source-selection.lock.json", { protocolFixture: true }],
        [
            "runtime-selection.json",
            options.runtime ?? {
                kind: "http-comparison-runtime",
                driver: { cpus: 1 },
                collector: { intervalMs: 1000 },
            },
        ],
    ]);
    for (const arm of sequence.physicalArms) {
        documents.set("arms/" + arm.cellId + "/settings.json", structuredClone(arm.jvmObservation ? on : off));
        documents.set(
            "arms/" + arm.cellId + "/profile.json",
            structuredClone(options.profile ?? { name: "metadata-only", cases: OBSERVER003.cases }),
        );
    }
    const reference = {};
    async function seal() {
        await mkdir(plan, { recursive: true });
        const files = [];
        for (const [path, value] of documents) {
            const raw = Buffer.from(JSON.stringify(value, null, 2) + "\n");
            await mkdir(dirname(join(plan, path)), { recursive: true });
            await writeFile(join(plan, path), raw);
            files.push({ path, bytes: raw.length, sha256: bytesDigest(raw) });
        }
        await writeFile(join(plan, "plan.lock.json"), JSON.stringify({ files }, null, 2) + "\n");
        reference.planLockSha256 = bytesDigest(await readFile(join(plan, "plan.lock.json")));
        await writeFile(inputFile, JSON.stringify({ planDirectory: "plan", reference }, null, 2) + "\n");
    }
    await seal();
    return {
        root,
        plan,
        inputFile,
        documents,
        reference,
        seal,
        read: () => readEngineeringConditions(inputFile, "observerCost", reference),
    };
}
