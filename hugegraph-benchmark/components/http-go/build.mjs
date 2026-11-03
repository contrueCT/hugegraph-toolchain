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

import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { readdir, mkdir, rm, realpath, stat, writeFile } from "node:fs/promises";
import { dirname, join, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { argumentsFor, digest, valueDigest, put, json, entry } from "../http/common.mjs";
import { inspectGoBinary } from "../http/go-engine.mjs";
const root = dirname(fileURLToPath(import.meta.url));
export async function sourceInventory(directory = root) {
    const files = [];
    async function visit(folder) {
        for (const item of await readdir(folder, { withFileTypes: true })) {
            const path = join(folder, item.name);
            if (item.isDirectory()) await visit(path);
            else if (item.isFile() && (item.name.endsWith(".go") || item.name === "go.mod"))
                files.push({
                    path: path
                        .slice(directory.length + 1)
                        .split("\\")
                        .join("/"),
                    bytes: (await stat(path)).size,
                    sha256: await digest(path),
                });
        }
    }
    await visit(directory);
    files.sort((a, b) => (a.path < b.path ? -1 : a.path > b.path ? 1 : 0));
    if (files.length > 64 || files.reduce((s, f) => s + f.bytes, 0) > 8 * 1024 * 1024)
        throw new Error("Go source budget exceeded");
    return { files, sourceSha256: valueDigest(files) };
}
export async function build(goPath, output) {
    const executable = await realpath(goPath),
        out = resolve(output),
        sources = await sourceInventory();
    if (out === root || out.startsWith(root + sep))
        throw new Error("Build output must be outside the Go source directory");
    const lock = await json(join(root, "../../release/http-go-toolchain.json"));
    // Reserve before invoking go: even `go version` can initialize its HOME telemetry directory.
    await mkdir(out, { mode: 0o700 });
    const config = process.platform === "darwin" ? join(out, "Library/Application Support") : join(out, "config");
    const telemetry = join(config, "go/telemetry");
    const env = {
        PATH: dirname(executable) + ":/usr/bin:/bin",
        HOME: out,
        XDG_CONFIG_HOME: config,
        GOENV: "off",
        LANG: "C",
        LC_ALL: "C",
        GOTOOLCHAIN: "local",
        GOPROXY: "off",
        GOSUMDB: "off",
        CGO_ENABLED: "0",
        GOMAXPROCS: "2",
        GOCACHE: join(out, "cache"),
        GOTMPDIR: join(out, "tmp"),
    };
    const binary = join(out, "hgbench-http-go"),
        flags = [
            "build",
            "-trimpath",
            "-buildvcs=false",
            `-ldflags=-X=main.sourceSHA256=${sources.sourceSha256}`,
            "-o",
            binary,
            "./cmd/hgbench-http-go",
        ];
    let identity = null,
        error = null,
        version = null,
        stage = "environment";
    const started = Date.now();
    try {
        await mkdir(env.GOCACHE);
        await mkdir(env.GOTMPDIR);
        await mkdir(telemetry, { recursive: true, mode: 0o700 });
        await writeFile(join(telemetry, "mode"), "off\n", { flag: "wx", mode: 0o600 });
        stage = "toolchain-version";
        const result = await promisify(execFile)(executable, ["version"], { env, timeout: 10000, maxBuffer: 65536 });
        version = result.stdout.trim();
        const match = /^go version (go[0-9.]+) (linux|darwin)\/(amd64|arm64)\s*$/.exec(version);
        if (!match || match[1] !== lock.version)
            throw new Error("Build requires the explicitly installed pinned Go toolchain; no automatic download");
        stage = "compile";
        await promisify(execFile)(executable, flags, { cwd: root, env, timeout: 300000, maxBuffer: 1048576 });
        if ((await sourceInventory()).sourceSha256 !== sources.sourceSha256)
            throw new Error("Sources changed during build");
        stage = "identity";
        const spec = {
            path: binary,
            sha256: await digest(binary),
            sourceSha256: sources.sourceSha256,
            goVersion: match[1],
            goos: match[2],
            goarch: match[3],
        };
        identity = (await inspectGoBinary(spec)).identity;
        const unexpected = (await readdir(telemetry)).filter((name) => name !== "mode");
        if (unexpected.length) throw new Error("Go telemetry wrote unexpected data despite off mode");
        stage = "completed";
    } catch (e) {
        error = String(e.message).slice(0, 2000);
    } finally {
        await rm(env.GOCACHE, { recursive: true, force: true });
        await rm(env.GOTMPDIR, { recursive: true, force: true });
        await rm(config, { recursive: true, force: true });
    }
    const receipt = {
        schemaVersion: 1,
        kind: "http-go-build",
        sources,
        toolchain: {
            path: executable,
            sha256: await digest(executable),
            version,
            lockSha256: await digest(join(root, "../../release/http-go-toolchain.json")),
        },
        flags,
        stage,
        telemetryMode: "off",
        identity,
        error,
        elapsedMs: Date.now() - started,
        performanceAccepted: false,
    };
    await put(join(out, "build.json"), receipt);
    if (error) throw new Error(error);
    return { directory: out, binary, identity, buildReceipt: join(out, "build.json"), performanceAccepted: false };
}

if (process.argv[1] && fileURLToPath(import.meta.url) === resolve(process.argv[1]))
    await entry(async () => {
        const args = argumentsFor(["--go", "--out"]);
        console.log(JSON.stringify(await build(args["--go"], args["--out"])));
    }, "Explicit offline Go component build: --go /absolute/path/to/go --out NEW-directory. Requires pinned toolchain. Network/toolchain auto-download disabled; source/binary/build identity retained, own compilation cache removed.");
