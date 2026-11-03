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
import { promisify, isDeepStrictEqual } from "node:util";
import { join, resolve, dirname } from "node:path";
import { writeFile } from "node:fs/promises";
import { BenchError, readJson } from "./io.js";
import { packageRoot } from "./plans.js";
import { httpProfileSchema, httpSuiteSettingsSchema } from "./http-contracts.js";
import type { HttpProfile } from "./http-contracts.js";

export const httpSuiteDirectory = join(packageRoot, "benchmarks/http-serving-v0.1");
export type HttpSuite = {
    suiteId: "hgbench-http-serving-v0.1";
    version: "0.1.0-draft.1";
    bundleSha256: string;
    files: string[];
    cases: HttpProfile["cases"];
    questions: Array<{ id: string; shape: string; answer: { resultDataCanonicalBytes: number } }>;
    release: unknown;
};
export async function inspectHttpSuite(): Promise<HttpSuite> {
    try {
        const { stdout } = await promisify(execFile)(
            process.execPath,
            [join(httpSuiteDirectory, "reference.mjs"), "materialize"],
            { timeout: 30000, maxBuffer: 2097152 },
        );
        const result = JSON.parse(stdout);
        if (!result.ok || result.command !== "materialize") throw new Error("Invalid suite component response");
        return result.data;
    } catch (error) {
        const e = error as { stderr?: string; message?: string };
        throw new BenchError("HTTP_SUITE_INVALID", "HTTP suite integrity/answer verification failed", 2, false, {
            diagnostic: e.stderr?.slice(0, 2048) ?? e.message,
        });
    }
}
export async function verifyHttpSuiteProfile(profile: HttpProfile): Promise<HttpSuite | null> {
    if (!profile.benchmark) return null;
    const suite = await inspectHttpSuite(),
        binding = profile.benchmark;
    const selected = binding.caseIds.map((id) => suite.cases.find((c) => c.id === id));
    const full = suite.cases.every((c) => binding.caseIds.includes(c.id));
    if (
        binding.suiteId !== suite.suiteId ||
        binding.version !== suite.version ||
        binding.bundleSha256 !== suite.bundleSha256 ||
        new Set(binding.caseIds).size !== binding.caseIds.length ||
        selected.some((c) => !c) ||
        !isDeepStrictEqual(selected, profile.cases) ||
        binding.coverage !== (full ? "full" : "subset")
    )
        throw new BenchError(
            "HTTP_SUITE_PROFILE_MISMATCH",
            "Suite identity, coverage, request or exact answer changed",
        );
    if (new URL(profile.endpoint).pathname !== "/gremlin")
        throw new BenchError("HTTP_SUITE_ENDPOINT", "HTTP Serving v0.1 requires the /gremlin endpoint");
    const largest = Math.max(
        ...suite.questions.filter((q) => binding.caseIds.includes(q.id)).map((q) => q.answer.resultDataCanonicalBytes),
    );
    if (profile.maxResponseBytes < largest + 4096)
        throw new BenchError(
            "HTTP_SUITE_RESPONSE_BUDGET",
            "Response cap must cover result data plus 4096 envelope bytes",
        );
    return suite;
}
export async function generateHttpProfile(settingsFile: string, outputFile: string, dryRun: boolean) {
    const settings = await readJson(settingsFile, httpSuiteSettingsSchema),
        suite = await inspectHttpSuite();
    if (new Set(settings.caseIds).size !== settings.caseIds.length)
        throw new BenchError("HTTP_DUPLICATE_ID", "Case IDs must be unique");
    const cases = settings.caseIds.map((id) => {
        const item = suite.cases.find((c) => c.id === id);
        if (!item) throw new BenchError("HTTP_SUITE_CASE_UNKNOWN", `Unknown HTTP case: ${id}`);
        return item;
    });
    const { caseIds, purpose, ...base } = settings;
    const profile = httpProfileSchema.parse({
        ...base,
        kind: "http-benchmark-profile",
        serviceChecks: true,
        cases,
        target: {
            ...base.target,
            evidenceFiles: base.target.evidenceFiles.map((p) => resolve(dirname(resolve(settingsFile)), p)),
        },
        benchmark: {
            suiteId: suite.suiteId,
            version: suite.version,
            bundleSha256: suite.bundleSha256,
            caseIds,
            coverage: suite.cases.every((c) => caseIds.includes(c.id)) ? "full" : "subset",
            purpose,
        },
    });
    // Shared validation of actual execution bounds; import lazily to avoid a module initialization cycle.
    const { validateHttpProfile } = await import("./http.js");
    validateHttpProfile(profile);
    await verifyHttpSuiteProfile(profile);
    if (!dryRun) {
        try {
            await writeFile(resolve(outputFile), JSON.stringify(profile, null, 2) + "\n", { flag: "wx", mode: 0o600 });
        } catch (error) {
            if ((error as NodeJS.ErrnoException).code === "EEXIST")
                throw new BenchError("HTTP_PROFILE_EXISTS", "Output profile exists; choose a new path", 4);
            throw error;
        }
    }
    return { profile, file: resolve(outputFile), written: !dryRun, executed: false, performanceAccepted: false };
}
