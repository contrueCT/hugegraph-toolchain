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
import { join } from "node:path";
import { BenchError } from "./io.js";
import { packageRoot } from "./plans.js";

// The CLI delegates normative checks to the independently usable Python component.
export async function inspectSpecification(command: "verify" | "questions" | "schema", id?: string): Promise<unknown> {
    try {
        const { stdout } = await promisify(execFile)(
            process.env.HGBENCH_PYTHON ?? "python3",
            [join(packageRoot, "benchmarks/ldbc-snb-interactive-v1/verify.py"), command, ...(id ? ["--id", id] : [])],
            { timeout: 30000, maxBuffer: 2 * 1024 * 1024, windowsHide: true },
        );
        const result = JSON.parse(stdout);
        if (result.schemaVersion !== 1 || result.ok !== true || result.command !== command)
            throw new Error("Unexpected specification component response");
        return result.data;
    } catch (error) {
        const child = error as { code?: number | string; stdout?: string; stderr?: string; message?: string };
        throw new BenchError(
            "SPEC_CHECK_FAILED",
            "Cannot verify the local specification; requires Python 3.9+ and intact bundled files",
            child.code === "ENOENT" ? 3 : 2,
            false,
            {
                componentOutput: child.stdout?.slice(0, 4096),
                diagnostic: child.stderr?.slice(0, 1024) ?? child.message,
            },
        );
    }
}
