/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
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
import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, mkdir, cp, readFile, writeFile, rm, access, symlink, realpath } from "node:fs/promises";
import { tmpdir } from "node:os";
import { resolve, join, posix } from "node:path";
import { pathToFileURL } from "node:url";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { verifyDependencyLock, syncDependencyLock } from "../components/http/dependency-lock.mjs";

const ROOT = resolve("."),
    run = promisify(execFile);
const save = (path, value) => writeFile(path, JSON.stringify(value, null, 4) + "\n");
async function fixture(t) {
    const root = await mkdtemp(join(tmpdir(), "hgbench-release-lock-"));
    t.after(() => rm(root, { recursive: true, force: true }));
    for (const name of ["package.json", "package-lock.json"]) await cp(join(ROOT, name), join(root, name));
    await syncDependencyLock(root);
    return root;
}
test("release snapshot preserves original lock bytes and checks declared dependencies", async (t) => {
    const root = await fixture(t);
    assert.deepEqual(
        await readFile(join(root, "release/dependency-lock.json")),
        await readFile(join(root, "package-lock.json")),
    );
    assert.equal((await verifyDependencyLock(root, { source: true })).path, "release/dependency-lock.json");
    const pkg = JSON.parse(await readFile(join(root, "package.json")));
    pkg.dependencies.commander = "0.0.0";
    await save(join(root, "package.json"), pkg);
    await assert.rejects(verifyDependencyLock(root), /declaration differs/);
});
test("missing, changed or symlink release assets reject without falling back to the source lock", async (t) => {
    const root = await fixture(t),
        asset = join(root, "release/dependency-lock.json"),
        raw = await readFile(asset);
    await rm(asset);
    await assert.rejects(verifyDependencyLock(root), { code: "RELEASE_DEPENDENCY_LOCK" });
    await writeFile(asset, Buffer.concat([raw, Buffer.from(" \n")]));
    await assert.rejects(verifyDependencyLock(root), /digest differs/);
    await rm(asset);
    await symlink(join(root, "package-lock.json"), asset);
    await assert.rejects(verifyDependencyLock(root), { code: "RELEASE_DEPENDENCY_LOCK" });
});
test("release check refuses stale source bytes while installed runtime needs no npm package-lock", async (t) => {
    const root = await fixture(t),
        lock = join(root, "package-lock.json"),
        raw = await readFile(lock);
    await writeFile(lock, Buffer.concat([raw, Buffer.from(" \n")]));
    await assert.rejects(verifyDependencyLock(root, { source: true }), /stale/);
    assert.ok(await verifyDependencyLock(root));
    await rm(lock);
    assert.ok(await verifyDependencyLock(root));
});
test(
    "physical npm package alone plans, verifies and reports failed evidence with only declared production dependencies",
    { timeout: 180000 },
    async (t) => {
        const root = await mkdtemp(join(tmpdir(), "hgbench-isolated-package-"));
        t.after(() => rm(root, { recursive: true, force: true }));
        // prepack stamps release/build.json. Keep that write outside the checkout
        // that parallel source-verification tests are required to keep immutable.
        const source = join(root, "source"),
            sourcePackage = JSON.parse(await readFile(join(ROOT, "package.json")));
        await mkdir(source);
        for (const file of new Set([...sourcePackage.files, "package.json", "package-lock.json"]))
            await cp(join(ROOT, file), join(source, file), { recursive: true });
        const packed = await run("npm", ["pack", "--json", "--pack-destination", root], {
            cwd: source,
            maxBuffer: 8 * 1024 * 1024,
            timeout: 60000,
        });
        const pack = JSON.parse(packed.stdout)[0];
        assert.ok(pack.files.some((file) => file.path === "src/http-contracts.ts"));
        assert.ok(pack.files.some((file) => file.path === "release/dependency-lock.json"));
        assert.ok(!pack.files.some((file) => file.path === "package-lock.json"));
        for (const file of [
            "docs/progress.md",
            "docs/sf100-environment-handoff.md",
            "docs/http-environment-handoff.md",
            "docs/sf100-go-verification-component-decision.md",
            "docs/sf100-preparation-budget-followup.md",
            "docs/remote-cleanup-20261025.md",
        ])
            assert.ok(
                !pack.files.some((entry) => entry.path === file),
                `Local operational document entered package: ${file}`,
            );
        const nativeHelpers = [
            "tools/build-ldbc-loader-text.py",
            "adapters/ldbc-loader/LoaderTextProbe.java",
            "tools/ldbc-reconcile.py",
            "adapters/ldbc-gremlin/reconcile-batch.groovy",
            "tools/ldbc-inventory.py",
            "adapters/ldbc-gremlin/inventory-page.groovy",
            "adapters/ldbc-gremlin/inventory.groovy",
            "tools/ldbc-scan.py",
            "tools/ldbc-import-sequence.mjs",
            "tools/ldbc-import-coverage.mjs",
            "adapters/ldbc-gremlin/scan-page.groovy",
            "tests-normative/fixtures/ldbc-scan-fingerprint-v1.json",
        ];
        for (const file of nativeHelpers)
            assert.ok(
                pack.files.some((entry) => entry.path === file),
                file,
            );
        const consumer = join(root, "consumer"),
            extracted = join(consumer, "node_modules/@hugegraph/hgbench");
        await mkdir(extracted, { recursive: true });
        await run("tar", ["-xzf", join(root, pack.filename), "--strip-components", "1", "-C", extracted], {
            timeout: 30000,
        });
        const packagedFiles = new Set(pack.files.map((file) => file.path));
        for (const file of pack.files.filter(
            (file) =>
                file.path === "README.md" ||
                (file.path.startsWith("docs/") && file.path.endsWith(".md")) ||
                (/^(adapters|components|examples)\//.test(file.path) && file.path.endsWith("/README.md")),
        )) {
            const markdown = await readFile(join(extracted, file.path), "utf8");
            for (const match of markdown.matchAll(/\[[^\]]+\]\(([^)]+)\)/g)) {
                const target = match[1].split("#")[0];
                if (!target || /^[a-z][a-z\d+.-]*:/i.test(target)) continue;
                assert.ok(
                    packagedFiles.has(posix.normalize(posix.join(posix.dirname(file.path), target))),
                    `Packaged document has an unavailable relative link: ${file.path} -> ${target}`,
                );
            }
        }
        for (const file of nativeHelpers) {
            assert.deepEqual(await readFile(join(extracted, file)), await readFile(join(ROOT, file)), file);
        }
        for (const helper of ["ldbc-reconcile.py", "build-ldbc-loader-text.py", "ldbc-inventory.py"]) {
            const help = await run("python3", [join("tools", helper), "--help"], {
                cwd: extracted,
                timeout: 10000,
                env: { ...process.env, PYTHONDONTWRITEBYTECODE: "1" },
            });
            if (helper === "ldbc-reconcile.py") assert.match(help.stdout, /native-batch/);
        }
        // Physically copy only production packages; no checkout node_modules/source symlink or npm network install.
        const pkg = JSON.parse(await readFile(join(extracted, "package.json")));
        for (const name of Object.keys(pkg.dependencies))
            await cp(await realpath(join(ROOT, "node_modules", name)), join(consumer, "node_modules", name), {
                recursive: true,
            });
        await writeFile(join(consumer, "package-lock.json"), '{"consumerLockMustNotBeRead":true}\n');
        await assert.rejects(access(join(extracted, "package-lock.json")), { code: "ENOENT" });
        const env = { ...process.env };
        delete env.NODE_PATH;
        delete env.NODE_TEST_CONTEXT;
        const graphHelp = await run(process.execPath, ["dist/cli.js", "graph", "prepare", "--help"], {
            cwd: extracted,
            env,
            timeout: 10000,
        });
        assert.match(graphHelp.stdout, /--import-snapshot <file>/);
        assert.match(graphHelp.stdout, /--min-edge-coverage <fraction>/);
        const importSchema = JSON.parse(
            (
                await run(process.execPath, ["dist/cli.js", "schema", "graph-import-snapshot"], {
                    cwd: extracted,
                    env,
                    timeout: 10000,
                })
            ).stdout,
        );
        assert.equal(importSchema.ok, true);
        assert.equal(importSchema.data.properties.kind.const, "graph-import-snapshot");
        assert.equal(importSchema.data.properties.writersStopped.const, true);
        const importChecks = await run(process.execPath, ["--test", "tests-normative/ldbc-import-sequence.test.mjs"], {
            cwd: extracted,
            env,
            timeout: 30000,
        });
        assert.match(importChecks.stdout, /# pass 13\b/);
        assert.match(importChecks.stdout, /# fail 0\b/);
        const documentation = JSON.parse(
            (
                await run(process.execPath, ["dist/cli.js", "docs", "list"], {
                    cwd: extracted,
                    env,
                    timeout: 10000,
                })
            ).stdout,
        );
        assert.equal(documentation.ok, true);
        assert.ok(Array.isArray(documentation.data) && documentation.data.includes("progress"));
        for (const topic of documentation.data) {
            const guide = JSON.parse(
                (
                    await run(process.execPath, ["dist/cli.js", "docs", "read", topic], {
                        cwd: extracted,
                        env,
                        timeout: 10000,
                    })
                ).stdout,
            );
            assert.equal(guide.ok, true, topic);
            assert.equal(guide.data.topic, topic);
            assert.ok(guide.data.content.trim().length > 0, topic);
        }
        const result = await run(
            process.execPath,
            [
                "--test",
                "--test-name-pattern=public dry-run|public frozen plan|public report reuses failed raw",
                "tests-normative/http-presets.test.mjs",
            ],
            {
                cwd: extracted,
                env,
                maxBuffer: 8 * 1024 * 1024,
                timeout: 120000,
            },
        );
        assert.match(result.stdout, /# pass 3\b/);
        assert.match(result.stdout, /# fail 0\b/);
        const installed = await import(pathToFileURL(join(extracted, "components/http/dependency-lock.mjs")));
        assert.ok(await installed.verifyDependencyLock());
        const asset = join(extracted, "release/dependency-lock.json"),
            raw = await readFile(asset);
        await rm(asset);
        await assert.rejects(installed.verifyDependencyLock(), { code: "RELEASE_DEPENDENCY_LOCK" });
        await writeFile(asset, Buffer.concat([raw, Buffer.from("\n")]));
        await assert.rejects(installed.verifyDependencyLock(), /digest differs/);
    },
);
