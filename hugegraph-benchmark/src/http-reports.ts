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

/** Offline reporting component; no network requests, bounded runtime and structured failures. */
export async function httpReportAction(action: "report" | "compare" | "version", options: object = {}) {
    let stdout: string;
    try {
        ({ stdout } = await promisify(execFile)(
            process.execPath,
            [join(packageRoot, "components/http/reports.mjs"), action, JSON.stringify(options)],
            { timeout: 300000, maxBuffer: 4 * 1024 * 1024, killSignal: "SIGKILL" },
        ));
    } catch (error) {
        const e = error as { stdout?: string; killed?: boolean; code?: string };
        if (!e.stdout || e.killed)
            throw new BenchError(
                "REPORT_COMPONENT",
                "Offline reporting failed or exceeded its 300-second deadline; split inputs and retry with a new output",
                6,
                false,
                { cause: e.code },
            );
        stdout = e.stdout;
    }
    let result;
    try {
        result = JSON.parse(stdout);
    } catch {
        throw new BenchError("REPORT_PROTOCOL", "Report component returned invalid JSON", 6);
    }
    if (result?.ok === false && typeof result.error?.code === "string" && typeof result.error?.message === "string")
        throw new BenchError(result.error.code, result.error.message, 2, false, result.error.details);
    if (result?.ok !== true || !result.data || typeof result.data !== "object")
        throw new BenchError("REPORT_PROTOCOL", "Report component returned an invalid envelope", 6);
    return result.data as Record<string, unknown>;
}
