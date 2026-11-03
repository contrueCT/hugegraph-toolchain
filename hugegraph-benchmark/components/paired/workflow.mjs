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

import { join, resolve } from "node:path";
import { runAnalysis } from "./analyzer.mjs";

async function main() {
    const args = process.argv.slice(2);
    if (args.length === 1 && args[0] === "--help") {
        process.stdout.write(
            "Usage: hgbench task -> node components/paired/workflow.mjs --input NORMALIZED.json\nRequires HGBENCH_TASK_DIR from the task controller; creates its exclusive output/ directory.\nTrack workflow.mjs, analyzer.mjs, core.mjs and the input in the task plan. No network or database access.\nTask success only means analysis completed. Results remain uncalibrated. See docs/paired-analysis.md.\n",
        );
        return;
    }
    if (args.length !== 2 || args[0] !== "--input" || !process.env.HGBENCH_TASK_DIR)
        throw new Error("Expected --input FILE and HGBENCH_TASK_DIR from the task controller");
    const result = await runAnalysis(resolve(args[1]), join(process.env.HGBENCH_TASK_DIR, "output"));
    process.stdout.write(JSON.stringify({ ok: true, kind: "paired-analysis-result", ...result }) + "\n");
}
main().catch((error) => {
    process.stderr.write(JSON.stringify({ ok: false, kind: "paired-workflow-error", message: String(error) }) + "\n");
    process.exitCode = 6;
});
