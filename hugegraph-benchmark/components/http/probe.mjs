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

import { validGraphValue } from "./graph-validation.mjs";
import { validateTraceWriter } from "./trace-writer.mjs";
import { mkdir, readFile, stat } from "node:fs/promises";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { argumentsFor, digest, entry, json, put, expectedCheckDigest } from "./common.mjs";
import { isDeepStrictEqual } from "node:util";
import { run } from "./runner.mjs";
import { validateDirectPhaseClockRaw } from "./direct-phase-clock.mjs";

export async function probe(profile, phase, output, remainingTraceBytes, goExecution) {
    if (!["preflight", "postcheck"].includes(phase)) throw new Error("Unknown probe phase");
    await mkdir(output, { mode: 0o700 });
    const go = profile.schemaVersion === 2 && profile.executor?.engine === "go-http-v1";
    const engine = go ? await import("./go-engine.mjs") : null;
    const execute = go ? (goExecution ?? (await engine.createGoExecution(profile))) : null;
    const cases = [],
        files = [];
    let traceBytes = 0;
    for (const [index, item] of profile.cases.entries()) {
        const id = `p${String(index + 1).padStart(4, "0")}`;
        const trace = `${id}.ndjson`,
            result = `${id}.json`;
        const input = {
            ...(go ? { schemaVersion: 2 } : {}),
            profile,
            case: item,
            load: { id: "probe-one", model: "closed", concurrency: 1 },
            id,
            repeat: 0,
            probePhase: phase,
            remainingTraceBytes: remainingTraceBytes - traceBytes,
        };
        let runResult;
        if (go) {
            const request = join(output, `${id}.request.json`);
            await put(request, input);
            await execute(["--request", request, "--trace", join(output, trace), "--output", join(output, result)]);
            runResult = await json(join(output, result));
        } else {
            runResult = await run(input, join(output, trace));
            await put(join(output, result), runResult);
        }
        traceBytes += runResult.traceBytes;
        const phaseResult = runResult.phases[0];
        const ok =
            phaseResult.scheduled === 1 &&
            phaseResult.sent === 1 &&
            phaseResult.completed === 1 &&
            phaseResult.counts.ok === 1 &&
            !phaseResult.failed &&
            !phaseResult.cancelled;
        cases.push({ id, caseId: item.id, ok });
        for (const path of [
            trace,
            result,
            ...(go
                ? [`${id}.request.json`, ...(profile.executor.hostIdentityGate ? engine.goIdentityFiles(result) : [])]
                : []),
        ])
            files.push({
                path,
                sha256: await digest(join(output, path)),
                bytes: (await stat(join(output, path))).size,
            });
        if (!ok) break; // Fail closed: no measurement after failed preflight; partial evidence remains.
    }
    const result = {
        schemaVersion: go ? 2 : 1,
        ...(go ? { engine: "go-http-v1" } : {}),
        kind: "http-service-probe",
        phase,
        passed: cases.length === profile.cases.length && cases.every((c) => c.ok),
        cases,
        files,
        traceBytes,
        performanceAccepted: false,
        scope: "HTTP protocol/complete-answer reachability only; actual build/mode/environment require independent proof",
    };
    await put(join(output, "result.json"), result);
    return result;
}

export async function verifyProbe(root, phase, profile, verifiedFiles) {
    const go = profile.schemaVersion === 2 && profile.executor?.engine === "go-http-v1";
    const engine = go ? await import("./go-engine.mjs") : null;
    const folder = join(root, phase),
        result = await json(join(folder, "result.json"));
    if (
        result.schemaVersion !== (go ? 2 : 1) ||
        (go && result.engine !== "go-http-v1") ||
        result.kind !== "http-service-probe" ||
        result.phase !== phase ||
        !result.passed ||
        result.cases.length !== profile.cases.length ||
        result.files.length !== profile.cases.length * (go ? (profile.executor.hostIdentityGate ? 7 : 3) : 2)
    )
        throw new Error("Missing or failed HTTP service probe");
    let traceBytes = 0;
    const seen = new Set();
    for (const f of result.files) {
        if (
            !(
                go
                    ? /^p\d{4}\.(?:(?:json|ndjson|request\.json)|json\.identity\/(?:ready|finished)(?:-release)?\.json)$/
                    : /^p\d{4}\.(json|ndjson)$/
            ).test(f.path) ||
            (f.path.includes(".json.identity/") && !profile.executor?.hostIdentityGate) ||
            seen.has(f.path) ||
            !Number.isSafeInteger(f.bytes) ||
            f.bytes < 1 ||
            f.bytes >
                (f.path.endsWith(".json") && profile.clockReference
                    ? 4 * profile.clockReference.spec.maxRawBytes + 131072
                    : f.path.endsWith(".request.json") && profile.graph
                      ? 4194304
                      : 65536)
        )
            throw new Error("Invalid probe file inventory");
        seen.add(f.path);
        // The enclosing analyzer has already read and hashed every raw member.
        // Bind the nested inventory to that check instead of hashing it again.
        const checked = verifiedFiles?.get(`${phase}/${f.path}`);
        if (verifiedFiles) {
            if (!checked || checked.bytes !== f.bytes || checked.sha256 !== f.sha256)
                throw new Error("Probe evidence mismatch");
        } else if (
            (await stat(join(folder, f.path))).size !== f.bytes ||
            (await digest(join(folder, f.path))) !== f.sha256
        )
            throw new Error("Probe evidence mismatch");
    }
    const observations = [];
    for (const [i, c] of profile.cases.entries()) {
        const id = `p${String(i + 1).padStart(4, "0")}`;
        if (
            !isDeepStrictEqual(result.cases[i], { id, caseId: c.id, ok: true }) ||
            !seen.has(`${id}.json`) ||
            !seen.has(`${id}.ndjson`)
        )
            throw new Error("Probe case identity differs");
        const runResult = await json(
            join(folder, `${id}.json`),
            profile.clockReference ? 4 * profile.clockReference.spec.maxRawBytes + 131072 : 65536,
        );
        if (go) {
            engine.validateGoRaw(runResult, profile);
            if (profile.executor.hostIdentityGate) {
                for (const name of engine.goIdentityFiles(`${id}.json`))
                    if (!seen.has(name)) throw new Error("Missing identity gate evidence");
                await engine.verifyGoIdentityGates(join(folder, `${id}.json`), runResult, profile);
            }
        } else validateDirectPhaseClockRaw(runResult, profile);
        const raw = await readFile(join(folder, `${id}.ndjson`), "utf8");
        const records = raw.trimEnd().split("\n");
        traceBytes += Buffer.byteLength(raw);
        if (go) engine.validateGoWriter(runResult, Buffer.byteLength(raw), profile);
        else validateTraceWriter(runResult, Buffer.byteLength(raw));
        if (go) {
            const request = await json(join(folder, `${id}.request.json`));
            if (
                !isDeepStrictEqual(request, {
                    schemaVersion: 2,
                    profile,
                    case: c,
                    load: { id: "probe-one", model: "closed", concurrency: 1 },
                    id,
                    repeat: 0,
                    probePhase: phase,
                    remainingTraceBytes: request.remainingTraceBytes,
                }) ||
                !Number.isSafeInteger(request.remainingTraceBytes) ||
                request.remainingTraceBytes < runResult.traceBytes ||
                request.remainingTraceBytes > profile.maxTraceBytes
            )
                throw new Error("Go probe frozen request differs");
        }
        if (records.length !== 1) throw new Error("Probe must contain exactly one response");
        const sample = JSON.parse(records[0]);
        const phaseResult = runResult.phases[0];
        if (
            runResult.kind !== "http-functional-probe" ||
            runResult.caseId !== c.id ||
            runResult.id !== id ||
            runResult.phases.length !== 1 ||
            runResult.phases[0].name !== phase ||
            runResult.phases[0].counts.ok !== 1 ||
            phaseResult.scheduled !== 1 ||
            phaseResult.sent !== 1 ||
            phaseResult.completed !== 1 ||
            phaseResult.dropped !== 0 ||
            phaseResult.failed !== 0 ||
            phaseResult.cancelled ||
            phaseResult.budgetExhausted ||
            !isDeepStrictEqual(phaseResult.counts, { ok: 1 }) ||
            runResult.traceBytes !== Buffer.byteLength(raw) ||
            (go && (sample.schemaVersion !== 2 || sample.engine !== "go-http-v1")) ||
            sample.sequence !== 0 ||
            sample.phase !== phase ||
            sample.outcome !== "ok" ||
            !sample.completeResponse ||
            sample.status !== (c.requestPool?.[0]?.expectedStatus ?? c.expectedStatus) ||
            sample.scheduledMs !== 0 ||
            !(
                sample.responseEncoding === "identity" ||
                (profile.responseEncoding === "gzip" && sample.responseEncoding === "gzip")
            ) ||
            !Number.isFinite(sample.sentMs) ||
            sample.sentMs < 0 ||
            !Number.isFinite(sample.endMs) ||
            sample.endMs < sample.sentMs ||
            !Number.isSafeInteger(sample.bytes) ||
            sample.bytes < 1 ||
            sample.bytes > profile.maxResponseBytes ||
            !Number.isSafeInteger(sample.decodedBytes) ||
            sample.decodedBytes < 1 ||
            sample.decodedBytes > profile.maxResponseBytes ||
            !/^[a-f0-9]{64}$/.test(sample.bodySha256) ||
            !(c.requestPool
                ? sample.parameterIndex === 0 &&
                  Array.isArray(sample.checkDigests) &&
                  sample.checkDigests.length === 0 &&
                  validGraphValue(sample.graphValue, c.requestPool[0].rule)
                : isDeepStrictEqual(
                      sample.checkDigests,
                      c.checks.map((rule) => ({ present: true, sha256: expectedCheckDigest(rule) })),
                  ))
        )
            throw new Error("Probe answer/accounting evidence differs");
        observations.push({
            caseId: c.id,
            wireBodyBytes: sample.bytes,
            decodedBodyBytes: sample.decodedBytes,
            responseEncoding: sample.responseEncoding,
            startedAt: runResult.startedAt,
            finishedAt: runResult.finishedAt,
        });
    }
    if (result.traceBytes !== traceBytes) throw new Error("Probe trace budget accounting differs");
    return { phase, passed: true, cases: observations, traceBytes };
}
if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href)
    await entry(async () => {
        const args = argumentsFor(["--profile", "--phase", "--output", "--trace-budget"]);
        const budget = Number(args["--trace-budget"]);
        if (!Number.isSafeInteger(budget) || budget < 1 || budget > 268435456)
            throw new Error("Invalid probe trace budget");
        const result = await probe(await json(args["--profile"]), args["--phase"], args["--output"], budget);
        process.stdout.write(
            JSON.stringify({ event: "http-service-probe", phase: result.phase, passed: result.passed }) + "\n",
        );
        if (!result.passed) throw new Error("HTTP service probe failed; inspect partial probe evidence");
    }, "HTTP service probe: --profile profile.json --phase preflight|postcheck --output NEW-directory --trace-budget bytes\nSends exactly one sequential request per case. No retries or warmup; failure preserves partial evidence. Does not verify execution mode.");
