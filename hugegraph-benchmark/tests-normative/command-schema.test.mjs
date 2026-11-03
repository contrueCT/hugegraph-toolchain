/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
import test from "node:test";
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { mkdtemp, readdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

const exec = promisify(execFile);
const cli = (args) => exec(process.execPath, [resolve("dist/cli.js"), ...args], { timeout: 10000 });
const schema = async (selector, extra = []) => JSON.parse((await cli([...extra, "schema", selector])).stdout).data;

test("nested command schema matches the existing public command tree at each depth", async () => {
    const tree = await schema("commands");
    for (const path of ["http", "http.calibration", "http.calibration.acceptance", "http.capacity.confirmation.plan"]) {
        let expected = tree;
        for (const part of path.split(".")) expected = expected.commands.find((c) => c.name === part);
        assert.ok(expected, path);
        assert.deepEqual(await schema("command:" + path), expected);
    }
});

test("querying an execution command does not require its arguments or create a task store", async (t) => {
    const directory = await mkdtemp(join(tmpdir(), "hgbench-command-schema-"));
    t.after(() => rm(directory, { recursive: true, force: true }));
    const result = await schema("command:task.run", ["--root", join(directory, "uncreated")]);
    assert.equal(result.name, "run");
    assert.ok(result.options.some((option) => option.flags === "--plan <file>" && option.required));
    assert.deepEqual(await readdir(directory), []);
});

test("empty, unknown and internal command selectors remain structured usage errors", async () => {
    for (const path of [
        "",
        "http..plan",
        "http.",
        "missing",
        "http.missing",
        "__worker",
        "__task-worker",
        "http.__worker",
    ]) {
        await assert.rejects(cli(["schema", "command:" + path]), (error) => {
            assert.equal(error.code, 2);
            assert.equal(error.stderr, "");
            const response = JSON.parse(error.stdout);
            assert.equal(response.ok, false);
            assert.equal(response.error.code, "UNKNOWN_COMMAND");
            return true;
        });
    }
});
