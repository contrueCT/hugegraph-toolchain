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

import { lstat } from "node:fs/promises";
import { join } from "node:path";
import { isDeepStrictEqual } from "node:util";
import { digest, json } from "./common.mjs";

// Inspect locked data only. Never execute a reference program taken from an evidence bundle.
export async function verifySuiteEvidence(root, profile, verifiedFiles) {
    const binding = profile.benchmark;
    if (!binding) return null;
    const folder = join(root, "benchmark"),
        lock = await json(join(folder, "bundle.lock.json"));
    const required = [
        "catalog.json",
        "answers.json",
        "protocol.json",
        "scoring-policy.json",
        "calibration-plan.json",
        "release.json",
        "spec.md",
        "reference.mjs",
    ];
    if (
        binding.bundleSha256 !==
            (verifiedFiles
                ? verifiedFiles.get("benchmark/bundle.lock.json")?.sha256
                : await digest(join(folder, "bundle.lock.json"))) ||
        lock.kind !== "http-serving-bundle" ||
        lock.schemaVersion !== 1 ||
        !isDeepStrictEqual(lock.files.map((f) => f.path).sort(), required.sort())
    )
        throw new Error("HTTP benchmark bundle identity/inventory mismatch");
    for (const file of lock.files) {
        if (verifiedFiles) {
            const checked = verifiedFiles.get(`benchmark/${file.path}`);
            if (!checked || checked.bytes !== file.bytes || checked.bytes > 2097152 || checked.sha256 !== file.sha256)
                throw new Error("HTTP benchmark file mismatch");
            continue;
        }
        const path = join(folder, file.path),
            info = await lstat(path);
        if (!info.isFile() || info.size !== file.bytes || info.size > 2097152 || (await digest(path)) !== file.sha256)
            throw new Error("HTTP benchmark file mismatch");
    }
    const catalog = await json(join(folder, "catalog.json")),
        answers = await json(join(folder, "answers.json"));
    if (
        binding.version !== catalog.version ||
        binding.suiteId !== catalog.suiteId ||
        !Array.isArray(binding.caseIds) ||
        new Set(binding.caseIds).size !== binding.caseIds.length
    )
        throw new Error("HTTP benchmark binding mismatch");
    const cases = binding.caseIds.map((id) => {
        const c = catalog.cases.find((c) => c.id === id),
            a = answers.cases.find((a) => a.id === id);
        if (!c || !a) throw new Error("Unknown HTTP benchmark case");
        return {
            id,
            body: c.body,
            expectedStatus: 200,
            checks: [
                { pointer: "/status/code", equals: 200 },
                { pointer: "/result/data", canonicalSha256: a.canonicalSha256 },
            ],
        };
    });
    if (
        !isDeepStrictEqual(cases, profile.cases) ||
        binding.coverage !== (catalog.cases.every((c) => binding.caseIds.includes(c.id)) ? "full" : "subset")
    )
        throw new Error("HTTP benchmark question/answer/coverage mismatch");
    return {
        ...binding,
        answers: answers.cases.filter((a) => binding.caseIds.includes(a.id)),
        release: await json(join(folder, "release.json")),
        performanceAccepted: false,
    };
}
