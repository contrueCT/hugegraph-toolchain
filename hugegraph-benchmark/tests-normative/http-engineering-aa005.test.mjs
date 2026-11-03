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

import assert from "node:assert/strict";
import { mkdir, mkdtemp, readFile, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { performance } from "node:perf_hooks";
import test from "node:test";
import { verifyAa005Contribution } from "../components/http/engineering-aa005.mjs";

// These tiny rejection fixtures contain no measured data and never stand in for
// the separately audited 779 MB real A/A005 collection.
async function fixture(run) {
    const root = await mkdtemp(join(tmpdir(), "hgbench-aa005-rejection-"));
    try {
        await mkdir(join(root, "local-review-v1"));
        await run(root);
    } finally {
        await rm(root, { recursive: true, force: true });
    }
}

const options = (evidenceRoot, extra = {}) => ({
    evidenceRoot,
    deadline: performance.now() + 5000,
    maxBytes: 2 * 1024 ** 3,
    ...extra,
});

test("A/A005 contribution rejects cancellation and exhausted absolute time before input IO", async () => {
    const controller = new AbortController();
    controller.abort();
    await assert.rejects(verifyAa005Contribution(options("/does-not-exist", { signal: controller.signal })), {
        code: "AA005_CANCELLED",
    });
    await assert.rejects(verifyAa005Contribution(options("/does-not-exist", { deadline: performance.now() - 1 })), {
        code: "AA005_DEADLINE",
    });
});

test("A/A005 contribution has a distinct fixed 2 GiB input ceiling", async () => {
    for (const maxBytes of [0, -1, 1.5, 2 * 1024 ** 3 + 1, NaN]) {
        await assert.rejects(verifyAa005Contribution(options("/does-not-exist", { maxBytes })), {
            code: "AA005_BYTES_ARGUMENT",
        });
    }
});

test("same-shaped accepted report cannot replace the pinned independent replay chain", async () => {
    await fixture(async (root) => {
        const fake = JSON.stringify({ schemaVersion: 1, files: [], scope: "this new local review only" });
        const lock = join(root, "local-review-v1", "review.lock.json");
        await writeFile(lock, fake);
        await writeFile(
            join(root, "local-review-v1", "review.json"),
            JSON.stringify({ rebuildCompleted: true, evidenceValid: true, engineering: { accepted: true } }),
        );
        await assert.rejects(verifyAa005Contribution(options(root)), { code: "AA005_PIN" });
        assert.equal(await readFile(lock, "utf8"), fake);
    });
});

test("input accounting rejects a file before reading beyond the caller's remaining budget", async () => {
    await fixture(async (root) => {
        await writeFile(join(root, "local-review-v1", "review.lock.json"), "{}\n");
        await assert.rejects(verifyAa005Contribution(options(root, { maxBytes: 2 })), {
            code: "AA005_INPUT_BYTES",
        });
    });
});

test("the replay lock and its parent must be regular evidence paths", async () => {
    await fixture(async (root) => {
        await writeFile(join(root, "outside.json"), "{}");
        await symlink(join(root, "outside.json"), join(root, "local-review-v1", "review.lock.json"));
        await assert.rejects(verifyAa005Contribution(options(root)), { code: "AA005_REGULAR_FILE" });
        await rm(join(root, "local-review-v1"), { recursive: true });
        await mkdir(join(root, "other"));
        await writeFile(join(root, "other", "review.lock.json"), "{}");
        await symlink(join(root, "other"), join(root, "local-review-v1"));
        await assert.rejects(verifyAa005Contribution(options(root)), { code: "AA005_DIRECTORY" });
    });
});

test("a missing original chain never yields an unknown-but-verified contribution", async () => {
    await fixture(async (root) => {
        await assert.rejects(verifyAa005Contribution(options(root)), { code: "ENOENT" });
    });
});
