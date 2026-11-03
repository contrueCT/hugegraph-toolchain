/* Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, writeFile, readFile, readdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { build } from "../components/http-go/build.mjs";

async function compiler(t, version) {
    const root = await mkdtemp(join(tmpdir(), "hgbench-go-build-"));
    t.after(() => rm(root, { recursive: true, force: true }));
    const fake = join(root, "go");
    await writeFile(
        fake,
        `#!${process.execPath}
const fs=require("node:fs"), path=require("node:path");
const config=process.platform==="darwin"?path.join(process.env.HOME,"Library/Application Support"):process.env.XDG_CONFIG_HOME;
if(!fs.statSync(process.env.HOME).isDirectory())process.exit(31);
if(fs.readFileSync(path.join(config,"go/telemetry/mode"),"utf8")!=="off"+String.fromCharCode(10))process.exit(32);
if(process.env.GOENV!=="off"||process.env.GOPROXY!=="off"||process.env.GOTOOLCHAIN!=="local")process.exit(33);
if(process.argv[2]==="version"){console.log("go version ${version} ${process.platform}/${process.arch === "arm64" ? "arm64" : "amd64"}");process.exit(0);}
console.error("controlled compilation failure");process.exit(17);
`,
        { mode: 0o700 },
    );
    return { root, fake };
}

test("Go build reserves private HOME and disables telemetry before go version; failure is retained", async (t) => {
    const { root, fake } = await compiler(t, "go1.27.3"),
        out = join(root, "output");
    await assert.rejects(build(fake, out), /controlled compilation failure/);
    const receipt = JSON.parse(await readFile(join(out, "build.json"), "utf8"));
    assert.equal(receipt.stage, "compile");
    assert.equal(receipt.telemetryMode, "off");
    assert.equal(receipt.identity, null);
    assert.equal(receipt.performanceAccepted, false);
    assert(receipt.sources.files.length > 0);
    assert.deepEqual(await readdir(out), process.platform === "darwin" ? ["Library", "build.json"] : ["build.json"]);
    await assert.rejects(build(fake, out), /EEXIST/);
});

test("Go build records unsupported toolchain refusal before compilation", async (t) => {
    const { root, fake } = await compiler(t, "go1.25.5"),
        out = join(root, "output");
    await assert.rejects(build(fake, out), /pinned Go toolchain/);
    const receipt = JSON.parse(await readFile(join(out, "build.json"), "utf8"));
    assert.equal(receipt.stage, "toolchain-version");
    assert.equal(receipt.identity, null);
    assert.match(receipt.toolchain.version, /go1.25.5/);
});
