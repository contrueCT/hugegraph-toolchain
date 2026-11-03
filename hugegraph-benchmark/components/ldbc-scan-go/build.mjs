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
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { createHash } from "node:crypto";
import { readFile, readdir, mkdir, writeFile, rm, realpath } from "node:fs/promises";
import { dirname, join, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";

const root = dirname(fileURLToPath(import.meta.url));
const sha = (raw) => createHash("sha256").update(raw).digest("hex");
const stable = (v) =>
    Array.isArray(v)
        ? v.map(stable)
        : v && typeof v === "object"
          ? Object.fromEntries(
                Object.keys(v)
                    .sort()
                    .map((k) => [k, stable(v[k])]),
            )
          : v;
export async function sourceInventory() {
    const files = [];
    async function visit(dir) {
        for (const item of await readdir(dir, { withFileTypes: true })) {
            const path = join(dir, item.name);
            if (item.isDirectory()) await visit(path);
            else if (item.isFile() && /(?:\.go|\.mjs|\.jsonl?|go\.mod)$/.test(item.name)) {
                const raw = await readFile(path);
                files.push({
                    path: path
                        .slice(root.length + 1)
                        .split(sep)
                        .join("/"),
                    bytes: raw.length,
                    sha256: sha(raw),
                });
            } else throw new Error("Unexpected source entry");
        }
    }
    await visit(root);
    files.sort((a, b) => (a.path < b.path ? -1 : a.path > b.path ? 1 : 0));
    if (files.length > 64 || files.reduce((n, f) => n + f.bytes, 0) > 8 * 1024 * 1024)
        throw new Error("Go source budget exceeded");
    return { files, sourceSha256: sha(JSON.stringify(stable(files))) };
}
export async function build(
    goPath,
    output,
    goos = process.platform,
    goarch = process.arch === "x64" ? "amd64" : process.arch,
) {
    if (!["darwin", "linux"].includes(goos) || !["amd64", "arm64"].includes(goarch))
        throw new Error("Unsupported target");
    const go = await realpath(goPath),
        out = resolve(output),
        sources = await sourceInventory();
    if (out === root || out.startsWith(root + sep))
        throw new Error("Build output must be outside the component source directory");
    const lock = JSON.parse(await readFile(join(root, "../../release/http-go-toolchain.json"), "utf8"));
    await mkdir(out, { mode: 0o700 });
    const cache = join(out, "cache"),
        tmp = join(out, "tmp"),
        telemetry = join(out, "telemetry"),
        binary = join(out, "hgbench-ldbc-scan-go");
    const env = {
        ...process.env,
        GOENV: "off",
        GOTOOLCHAIN: "local",
        GOPROXY: "off",
        GOSUMDB: "off",
        CGO_ENABLED: "0",
        GOMAXPROCS: "2",
        GOCACHE: cache,
        GOTMPDIR: tmp,
        GOPATH: join(out, "gopath"),
        TEST_TELEMETRY_DIR: telemetry,
        GOOS: goos,
        GOARCH: goarch,
    };
    const flags = [
        "build",
        "-trimpath",
        "-buildvcs=false",
        `-ldflags=-X=main.sourceSHA256=${sources.sourceSha256}`,
        "-o",
        binary,
        ".",
    ];
    let error = null,
        version = null,
        identity = null;
    try {
        for (const dir of [cache, tmp, telemetry]) await mkdir(dir);
        await writeFile(join(telemetry, "mode"), "off\n", { flag: "wx", mode: 0o600 });
        version = (await promisify(execFile)(go, ["version"], { env, timeout: 10000, maxBuffer: 65536 })).stdout.trim();
        if (!version.startsWith(`go version ${lock.version} `))
            throw new Error("Use the explicitly installed pinned Go toolchain; no automatic download");
        await promisify(execFile)(go, flags, { cwd: root, env, timeout: 300000, maxBuffer: 1048576 });
        if ((await sourceInventory()).sourceSha256 !== sources.sourceSha256)
            throw new Error("Sources changed during build");
        if ((await readdir(telemetry)).some((name) => name !== "mode"))
            throw new Error("Go telemetry wrote despite off mode");
        const raw = await readFile(binary);
        identity = {
            path: "hgbench-ldbc-scan-go",
            bytes: raw.length,
            sha256: sha(raw),
            sourceSha256: sources.sourceSha256,
            goVersion: lock.version,
            goos,
            goarch,
        };
    } catch (e) {
        error = String(e.message).slice(0, 2000);
    } finally {
        for (const dir of [cache, tmp, telemetry, env.GOPATH]) await rm(dir, { recursive: true, force: true });
    }
    const result = {
        schemaVersion: 1,
        kind: "ldbc-scan-go-build",
        ok: !error,
        version,
        flags,
        sources,
        identity,
        error,
        networkDependencies: [],
        temporaryBuildCachesRemoved: true,
    };
    await writeFile(join(out, "build.json"), JSON.stringify(result, null, 2) + "\n", { flag: "wx" });
    if (error) throw new Error(`Go verifier build failed; inspect ${join(out, "build.json")}`);
    return result;
}
if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
    const args = process.argv.slice(2);
    if (args.includes("--help") || args.includes("-h")) {
        console.log(
            "Build a standard-library-only SF100 read-only verifier.\n--go PINNED-EXECUTABLE --out NEW-DIRECTORY [--goos linux|darwin] [--goarch amd64|arm64]\nNo toolchain download or network dependency resolution. Source/binary identities and build.json are retained; temporary build caches are removed.\nThe resulting binary --help describes data, lease, evidence and deadline requirements. Building does not qualify SF100 correctness or performance.",
        );
    } else {
        const values = {};
        for (let i = 0; i < args.length; i += 2) {
            if (!["--go", "--out", "--goos", "--goarch"].includes(args[i]) || !args[i + 1] || values[args[i]])
                throw new Error("Invalid or repeated build flag; use --help");
            values[args[i]] = args[i + 1];
        }
        if (!values["--go"] || !values["--out"]) throw new Error("--go and --out are required; use --help");
        const result = await build(values["--go"], values["--out"], values["--goos"], values["--goarch"]);
        console.log(JSON.stringify({ ok: result.ok, identity: result.identity }));
    }
}
