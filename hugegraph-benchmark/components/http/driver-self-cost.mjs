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

import { validateTraceWriter } from "./trace-writer.mjs";
import { createHash } from "node:crypto";
import { constants, createReadStream } from "node:fs";
import { open, readFile, realpath, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { createInterface } from "node:readline";
import { pathToFileURL } from "node:url";
import { analyze } from "./analyzer.mjs";
import { argumentsFor, bytesDigest, entry } from "./common.mjs";

const MAX_JSON_BYTES = 8 * 1024 * 1024;
const MAX_REPORT_BYTES = 16 * 1024 * 1024;
const need = (value, message) => {
    if (!value) throw new Error(`HTTP_DRIVER_SELF_COST: ${message}`);
};

async function boundedManifest(path) {
    const file = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
    try {
        const before = await file.stat();
        need(before.isFile() && before.size <= MAX_JSON_BYTES, "manifest must be regular and <=8MiB");
        const buffer = Buffer.alloc(before.size + 1);
        let used = 0;
        while (used < buffer.length) {
            const { bytesRead } = await file.read(buffer, used, buffer.length - used, used);
            if (!bytesRead) break;
            used += bytesRead;
        }
        const after = await file.stat();
        need(
            used === before.size &&
                after.size === before.size &&
                after.mtimeMs === before.mtimeMs &&
                after.ctimeMs === before.ctimeMs,
            "manifest changed during bounded read",
        );
        return buffer.subarray(0, used);
    } finally {
        await file.close();
    }
}

async function lockedRun(root, path, member) {
    need(member && member.bytes <= MAX_JSON_BYTES, "missing or oversized run JSON");
    const raw = await readFile(join(root, path));
    need(raw.length === member.bytes && bytesDigest(raw) === member.sha256, `raw changed: ${path}`);
    return JSON.parse(raw.toString("utf8"));
}

function phaseCounts(name) {
    return {
        name,
        records: 0,
        sent: 0,
        notSent: 0,
        correct: 0,
        sentErrors: 0,
        completeResponses: 0,
        incompleteResponses: 0,
        completeResponseErrors: 0,
        incompleteResponseErrors: 0,
        validationCount: 0,
        validationTotal: 0,
        decodeCount: 0,
        decodeTotal: 0,
    };
}

function record(phase, sample) {
    phase.records++;
    if (sample.sentMs === null) {
        phase.notSent++;
        return;
    }
    phase.sent++;
    if (sample.outcome === "ok") phase.correct++;
    else phase.sentErrors++;
    if (!sample.completeResponse) {
        phase.incompleteResponses++;
        if (sample.outcome !== "ok") phase.incompleteResponseErrors++;
        return;
    }
    phase.completeResponses++;
    if (sample.outcome !== "ok") phase.completeResponseErrors++;
    phase.validationCount++;
    phase.validationTotal += sample.validationMs;
    if (sample.decodeMs !== null) {
        phase.decodeCount++;
        phase.decodeTotal += sample.decodeMs;
    }
    need(
        Number.isFinite(phase.validationTotal) && Number.isFinite(phase.decodeTotal),
        "recorded duration sum overflow",
    );
}

function phaseResult(p) {
    const distribution = (total, count) => ({
        status: count ? "recorded-descriptive" : "unavailable",
        recordedCompleteResponses: count,
        recordedTotalMs: count ? total : null,
        meanRecordedMs: count ? total / count : null,
        unavailableOrNotRecorded: {
            notSent: p.notSent,
            incompleteResponses: p.incompleteResponses,
            completeResponses: p.completeResponses - count,
        },
    });
    return {
        name: p.name,
        counts: {
            records: p.records,
            sent: p.sent,
            notSent: p.notSent,
            correct: p.correct,
            sentErrors: p.sentErrors,
            completeResponses: p.completeResponses,
            incompleteResponses: p.incompleteResponses,
            completeResponseErrors: p.completeResponseErrors,
            incompleteResponseErrors: p.incompleteResponseErrors,
        },
        validation: distribution(p.validationTotal, p.validationCount),
        decode: distribution(p.decodeTotal, p.decodeCount),
    };
}

async function consumeTrace(root, path, member, phases) {
    need(member && member.bytes <= 268435456, "missing or oversized trace");
    const hash = createHash("sha256"),
        stream = createReadStream(join(root, path));
    let bytes = 0;
    stream.on("data", (chunk) => {
        bytes += chunk.length;
        hash.update(chunk);
        if (bytes > member.bytes) stream.destroy(new Error("HTTP_DRIVER_SELF_COST: trace grew during analysis"));
    });
    const lines = createInterface({ input: stream, crlfDelay: Infinity });
    try {
        for await (const line of lines) {
            need(line.length > 0 && line.length < 65536, "invalid trace line length");
            const sample = JSON.parse(line),
                phase = phases.get(sample.phase);
            need(phase, "unknown phase");
            record(phase, sample);
        }
        need(bytes === member.bytes && hash.digest("hex") === member.sha256, `raw changed: ${path}`);
    } finally {
        lines.close();
        stream.destroy();
    }
    return bytes;
}

export async function analyzeDriverSelfCost(directory) {
    const root = await realpath(directory),
        manifestPath = join(root, "raw.lock.json"),
        manifestSha = bytesDigest(await boundedManifest(manifestPath)),
        verified = await analyze(root);
    need(verified.rawManifestSha256 === manifestSha, "raw manifest changed during verification");
    const manifestRaw = await boundedManifest(manifestPath);
    need(
        manifestRaw.length <= MAX_JSON_BYTES && bytesDigest(manifestRaw) === manifestSha,
        "raw manifest changed or oversized",
    );
    const manifest = JSON.parse(manifestRaw.toString("utf8")),
        inventory = new Map(manifest.files.map((f) => [f.path, f])),
        runs = [];
    for (const [index, id] of manifest.runs.entries()) {
        const raw = await lockedRun(root, `runs/${id}.json`, inventory.get(`runs/${id}.json`)),
            phases = new Map(raw.phases.map((p) => [p.name, phaseCounts(p.name)])),
            traceBytes = await consumeTrace(root, `traces/${id}.ndjson`, inventory.get(`traces/${id}.ndjson`), phases);
        for (const saved of raw.phases) {
            const actual = phases.get(saved.name);
            need(
                actual.records === saved.scheduled &&
                    actual.sent === saved.sent &&
                    actual.notSent === saved.dropped &&
                    actual.sentErrors === saved.failed &&
                    actual.records === actual.sent + actual.notSent &&
                    actual.completeResponses + actual.incompleteResponses === actual.sent,
                "phase sample counts changed",
            );
        }
        need(traceBytes === raw.traceBytes, "run trace bytes changed");
        const records = [...phases.values()].reduce((n, p) => n + p.records, 0),
            recorded = Object.hasOwn(raw, "evidenceWriteMs"),
            asyncWriter = validateTraceWriter(raw, traceBytes);
        need(
            !recorded || (Number.isFinite(raw.evidenceWriteMs) && raw.evidenceWriteMs >= 0),
            "invalid evidenceWriteMs",
        );
        runs.push({
            id,
            caseId: raw.caseId,
            loadId: raw.loadId,
            repeat: raw.repeat,
            phases: [...phases.values()].map(phaseResult),
            traceBytes,
            traceProduction: {
                status: recorded && records ? "recorded-descriptive" : "unavailable",
                recordedEvidenceWriteMs: recorded ? raw.evidenceWriteMs : null,
                recordedRequestsIncludingWarmupAndMeasurement: records,
                meanRecordedMsPerTraceRecord: recorded && records ? raw.evidenceWriteMs / records : null,
                bytesPerTraceRecord: records ? traceBytes / records : null,
                scope: "entire run, all warmup+measurement records including unsent/dropped; never per phase",
                reason: asyncWriter
                    ? "async-writer-costs-reported-separately"
                    : !recorded
                      ? "legacy-missing-evidenceWriteMs"
                      : !records
                        ? "no-recorded-requests"
                        : null,
                ...(asyncWriter
                    ? {
                          asynchronousWriter: asyncWriter,
                          interpretation:
                              "Synchronous serialization/enqueue, asynchronous write waits and final flush/sync are distinct overlapping wall durations; never add or interpret as isolated CPU cost",
                      }
                    : {}),
            },
            qualityIssues: verified.runs[index].issues,
        });
    }
    need(
        bytesDigest(await boundedManifest(manifestPath)) === manifestSha,
        "raw manifest changed during reconstruction",
    );
    return {
        schemaVersion: 1,
        kind: "http-driver-self-cost",
        rawManifestSha256: manifestSha,
        name: verified.name,
        verification: "full-http-analyzer plus consumed run/trace hash, bytes and count recheck",
        quality: { verdict: verified.verdict, gates: verified.gates },
        verifiedServiceChecksExcludedFromCosts: verified.serviceChecks.length,
        runs,
        performanceAccepted: false,
        overheadAccepted: false,
        budgets: { inputRunJsonBytes: MAX_JSON_BYTES, inputTraceBytes: 268435456, outputJsonBytes: MAX_REPORT_BYTES },
        limits: [
            "Recorded performance.now wall durations, not CPU time or isolated resource cost",
            "Decode duration is a subset of validation; never add decode to validation",
            "Null decode on complete responses is not recorded, not an invented zero; incomplete and unsent records remain separate",
            "Run evidenceWriteMs includes recorded JSON serialization and synchronous trace write calls; excludes final fsync, file open/close, other evidence writes, page-cache and background writeback costs",
            "No per-phase trace cost allocation or disabled-validator/trace throughput/resource causal claim",
            "Response bodies are absent from raw evidence; no validation/decode replay is possible",
            "Shared analyzer quality issues remain; these descriptive costs establish no calibration acceptance",
            "Keep input immutable during analysis; service preflight/postcheck costs are outside this workload-only report",
        ],
    };
}

export async function reportDriverSelfCost(directory, outputFile) {
    const result = await analyzeDriverSelfCost(directory),
        encoded = JSON.stringify(result, null, 2) + "\n";
    need(Buffer.byteLength(encoded) <= MAX_REPORT_BYTES, "output JSON budget exceeded");
    await writeFile(outputFile, encoded, { flag: "wx", mode: 0o600 });
    return result;
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href)
    await entry(async () => {
        const args = argumentsFor(["--input", "--output"]),
            result = await reportDriverSelfCost(args["--input"], args["--output"]);
        process.stdout.write(
            JSON.stringify({
                kind: result.kind,
                rawManifestSha256: result.rawManifestSha256,
                runs: result.runs.length,
                performanceAccepted: false,
                overheadAccepted: false,
            }) + "\n",
        );
    }, "HTTP driver self-cost: --input RAW-DIRECTORY --output NEW-report.json\nOffline full raw-lock/count/timeline verification; corrupted evidence rejected, output never overwritten.\nRecorded validation/decode means and run-level trace production only. No stored bodies/replay, per-phase trace allocation or overhead/throughput acceptance. JSON <=16MiB; failure exit 6.");
