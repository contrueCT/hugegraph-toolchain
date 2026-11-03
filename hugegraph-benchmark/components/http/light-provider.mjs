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

import { prepareGeneratorAssets } from "./generator-assets.mjs";
import { startGeneratorProvider } from "./generator-provider.mjs";
import { join } from "node:path";
import { mkdir } from "node:fs/promises";
const [directory, wall] = process.argv.slice(2);
await mkdir(directory, { mode: 0o700 });
await prepareGeneratorAssets(join(directory, "assets"));
const provider = await startGeneratorProvider({
    assets: join(directory, "assets"),
    output: join(directory, "provider"),
    port: 0,
    wallMs: Number(wall),
});
process.stdout.write(JSON.stringify({ endpoint: provider.endpoint }) + "\n");
for (const signal of ["SIGTERM", "SIGINT"])
    process.once(signal, async () => {
        await provider.stop(signal);
        process.stdin.destroy();
    });

// A vanished foreground controller closes this pipe; the original provider then exits.
process.stdin.resume();
process.stdin.once("end", async () => {
    await provider.stop("controller-eof");
    process.stdin.destroy();
});
