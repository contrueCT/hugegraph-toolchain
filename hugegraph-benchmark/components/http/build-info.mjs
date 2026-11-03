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

import { createHash } from "node:crypto";
import { readFile, readdir } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { join } from "node:path";

const root = fileURLToPath(new URL("../../", import.meta.url));
let cached;
export function buildInfo() {
    return (cached ??= (async () => {
        const files = ["package.json", "release/dependency-lock.json"];
        async function walk(dir) {
            for (const f of await readdir(join(root, dir), { withFileTypes: true })) {
                const path = `${dir}/${f.name}`;
                if (f.isDirectory()) await walk(path);
                else if (f.isFile() && /\.(?:ts|js|mjs|json|go|py|map|mod|sum)$/.test(f.name)) files.push(path);
            }
        }
        for (const dir of ["src", "dist", "components"]) await walk(dir);
        const hash = createHash("sha256");
        for (const f of files.sort()) {
            hash.update(f + "\0");
            hash.update(
                createHash("sha256")
                    .update(await readFile(join(root, f)))
                    .digest(),
            );
        }
        const sourceSha256 = hash.digest("hex");
        const pkg = JSON.parse(await readFile(join(root, "package.json"), "utf8"));
        let stamp = null;
        try {
            stamp = JSON.parse(await readFile(join(root, "release/build.json"), "utf8"));
        } catch (e) {
            if (e.code !== "ENOENT") throw e;
        }
        return {
            version: pkg.version,
            buildId: `sha256:${sourceSha256}`,
            sourceSha256,
            state: stamp?.sourceSha256 === sourceSha256 ? "release" : "development",
            reportVersion: 2,
            httpRawVersions: [1, 2, 3],
        };
    })());
}
