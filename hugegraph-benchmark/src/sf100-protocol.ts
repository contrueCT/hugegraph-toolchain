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

import { z } from "zod";
import { lstat, realpath } from "node:fs/promises";
import { isAbsolute, relative, resolve, sep } from "node:path";
import { BenchError, fileHash, hash, readJson } from "./io.js";
import { sf100SuiteId } from "./sf100.js";

const sha = z.string().regex(/^[a-f0-9]{64}$/);
const id = z.string().regex(/^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$/);
const count = z.string().regex(/^(0|[1-9][0-9]{0,19})$/);
const nonzero = z.string().regex(/^[1-9][0-9]{0,19}$/);
const localPath = z
    .string()
    .regex(/^[^\\:]+$/)
    .min(1);
export const protocolArtifactSchema = z.strictObject({ path: localPath, sha256: sha, bytes: count });
const namedArtifact = protocolArtifactSchema.extend({ name: id });
export const sf100IdentitySchema = z.strictObject({
    planDigest: sha,
    datasetDigest: sha,
    mappingDigest: sha,
    componentDigest: sha,
});
const targetSchema = z.strictObject({ targetDigest: sha, epoch: id });
export const sf100RequestSchema = z.strictObject({
    protocolVersion: z.literal(1),
    kind: z.literal("sf100-request"),
    suite: z.literal(sf100SuiteId),
    requestId: id,
    attemptId: id,
    identity: sf100IdentitySchema,
    stage: z.enum(["convert", "load", "validate", "restore", "measure"]),
    target: targetSchema.nullable(),
    shard: z.strictObject({ id, records: count }).nullable(),
    repetition: z.number().int().nonnegative().max(99).nullable(),
    inputs: z.array(namedArtifact).min(1).max(32),
    outputBudgetBytes: nonzero,
});
export type Sf100Request = z.infer<typeof sf100RequestSchema>;
export const sf100ReceiptSchema = z.strictObject({
    protocolVersion: z.literal(1),
    kind: z.literal("sf100-receipt"),
    requestId: id,
    attemptId: id,
    requestDigest: sha,
    status: z.enum(["completed", "failed", "cancelled"]),
    effect: z.enum(["none", "committed", "unknown"]),
    records: count.nullable(),
    artifacts: z.array(namedArtifact).max(32),
    error: z.strictObject({ code: id, message: z.string().min(1).max(4096) }).nullable(),
});
export type Sf100Receipt = z.infer<typeof sf100ReceiptSchema>;
export const sf100ObservationSchema = z.strictObject({
    protocolVersion: z.literal(1),
    kind: z.literal("sf100-load-observation"),
    requestDigest: sha,
    target: targetSchema,
    shardId: id,
    disposition: z.enum(["committed", "absent", "partial", "unknown"]),
    records: count.nullable(),
    scope: z.literal("entire-shard-identities-and-content"),
    writerQuiesced: z.boolean(),
    evidence: z.array(protocolArtifactSchema).min(1).max(16),
});
export type Sf100Observation = z.infer<typeof sf100ObservationSchema>;

function fail(message: string): never {
    throw new BenchError("SF100_PROTOCOL_INVALID", message);
}
function paths(artifacts: { path: string; name?: string }[]) {
    for (const a of artifacts) {
        if (isAbsolute(a.path) || a.path.split("/").some((p) => p === ".." || p === "." || p === "")) {
            fail("Artifact paths must be normalized relative paths without dot segments");
        }
    }
    if (new Set(artifacts.map((a) => a.path)).size !== artifacts.length) fail("Artifact paths must be unique");
    const names = artifacts.filter((a) => a.name !== undefined).map((a) => a.name);
    if (new Set(names).size !== names.length) fail("Artifact names must be unique");
}
export function validateSf100Request(input: unknown): Sf100Request {
    const r = sf100RequestSchema.parse(input);
    paths(r.inputs);
    if ((r.stage === "convert" || r.stage === "load") !== (r.shard !== null))
        fail("Only convert/load requests must name a shard");
    if ((r.stage === "convert") !== (r.target === null))
        fail("Convert has no target; database stages require target identity and epoch");
    if ((r.stage === "measure" || r.stage === "restore") !== (r.repetition !== null))
        fail("Restore/measure require a repetition; other stages must use null");
    const required = {
        convert: ["source"],
        load: ["shard"],
        validate: ["reference", "driver-config"],
        restore: ["restore-config"],
        measure: ["driver-config", "restoration"],
    };
    for (const name of required[r.stage]) if (!r.inputs.some((a) => a.name === name)) fail(`Missing input: ${name}`);
    return r;
}
export function validateSf100Receipt(request: Sf100Request, input: unknown): Sf100Receipt {
    const r = validateSf100Request(request);
    const receipt = sf100ReceiptSchema.parse(input);
    paths(receipt.artifacts);
    if (receipt.requestDigest !== hash(r) || receipt.requestId !== r.requestId || receipt.attemptId !== r.attemptId)
        fail("Receipt belongs to another immutable request/attempt");
    if ((receipt.status === "completed") !== (receipt.error === null))
        fail("Completed receipts must have no error; failed/cancelled receipts require an error");
    if (receipt.status === "completed") {
        if (receipt.effect !== "committed") fail("Completed operation must declare its outputs/effects committed");
        const required = {
            convert: "shard",
            load: "commit-proof",
            validate: "validation",
            restore: "restoration",
            measure: "measurements",
        };
        if (!receipt.artifacts.some((a) => a.name === required[r.stage]))
            fail(`Completed receipt requires ${required[r.stage]}`);
        if (r.shard && receipt.records !== r.shard.records)
            fail("Completed shard record count differs from the request");
    }
    if (receipt.effect === "none" && receipt.records !== null && receipt.records !== "0")
        fail("No-effect receipt cannot declare applied records");
    const total = receipt.artifacts.reduce((n, a) => n + BigInt(a.bytes), 0n);
    if (total > BigInt(r.outputBudgetBytes)) fail("Receipt artifacts exceed the output byte budget");
    return receipt;
}
export async function verifyProtocolArtifacts(root: string, artifacts: z.infer<typeof protocolArtifactSchema>[]) {
    paths(artifacts);
    const base = await realpath(root);
    for (const artifact of artifacts) {
        const path = resolve(base, artifact.path);
        // Check each path component: hashing must never follow a link outside the evidence directory.
        let current = base;
        for (const part of artifact.path.split("/")) {
            current = resolve(current, part);
            if ((await lstat(current)).isSymbolicLink()) fail("Artifact paths must not contain symbolic links");
        }
        const actual = await realpath(path);
        const child = relative(base, actual);
        if (!child || child === ".." || child.startsWith(`..${sep}`) || isAbsolute(child))
            fail("Artifact escapes its evidence root");
        const metadata = await lstat(actual, { bigint: true });
        if (!metadata.isFile() || metadata.size.toString() !== artifact.bytes)
            fail(`Artifact byte count/type mismatch: ${artifact.path}`);
        if ((await fileHash(actual)) !== artifact.sha256) fail(`Artifact checksum mismatch: ${artifact.path}`);
    }
}
export function validateLoadObservation(request: Sf100Request, input: unknown): Sf100Observation {
    const r = validateSf100Request(request);
    if (r.stage !== "load") fail("Reconciliation observations apply only to load requests");
    const o = sf100ObservationSchema.parse(input);
    paths(o.evidence);
    if (o.disposition !== "unknown" && !o.writerQuiesced)
        fail("A decisive observation requires quiesced writers throughout the scan");
    if (o.requestDigest !== hash(r) || hash(o.target) !== hash(r.target) || o.shardId !== r.shard!.id)
        fail("Observation belongs to another request, target epoch or shard");
    if (o.disposition === "committed" && o.records !== r.shard!.records)
        fail("Committed observation requires the complete shard count");
    if (o.disposition === "absent" && o.records !== "0") fail("Absent observation must prove zero records");
    if (o.disposition === "partial" && (o.records === null || BigInt(o.records) > BigInt(r.shard!.records)))
        fail("Invalid partial-shard count");
    return o;
}

export async function inspectSf100Exchange(requestFile: string, receiptFile?: string, artifactRoot?: string) {
    const request = validateSf100Request(await readJson(requestFile, sf100RequestSchema));
    const receipt = receiptFile ? validateSf100Receipt(request, await readJson(receiptFile, sf100ReceiptSchema)) : null;
    if (artifactRoot) {
        if (!receipt) fail("Artifact verification requires a receipt");
        await verifyProtocolArtifacts(artifactRoot, receipt.artifacts);
    }
    return {
        requestDigest: hash(request),
        stage: request.stage,
        requestValid: true,
        receiptValid: receipt ? true : null,
        receiptStatus: receipt?.status ?? null,
        artifactBytesVerified: Boolean(artifactRoot),
        executed: false,
        liveDatabaseVerified: false,
        benchmarkValid: false,
        scope: artifactRoot ? "request-receipt-and-local-artifact-integrity" : "request-and-receipt-metadata-only",
    };
}

export async function assessSf100Recovery(
    requestFile: string,
    receiptFile?: string,
    artifactRoot?: string,
    observationFile?: string,
) {
    const request = validateSf100Request(await readJson(requestFile, sf100RequestSchema));
    const receipt = receiptFile ? validateSf100Receipt(request, await readJson(receiptFile, sf100ReceiptSchema)) : null;
    const result = (action: string, reason: string) => ({
        requestDigest: hash(request),
        action,
        reason,
        executed: false,
        liveDatabaseVerified: false,
        evidenceTrust: "local integrity checked; producer observations require trusted provenance",
    });
    if (observationFile && request.stage !== "load") fail("Only load recovery accepts a reconciliation observation");
    if (request.stage === "load") {
        let observation: Sf100Observation | null = null;
        if (observationFile)
            observation = validateLoadObservation(request, await readJson(observationFile, sf100ObservationSchema));
        else if (receipt?.status === "completed" && artifactRoot) {
            await verifyProtocolArtifacts(artifactRoot, receipt.artifacts);
            const proof = receipt.artifacts.find((a) => a.name === "commit-proof")!;
            observation = validateLoadObservation(
                request,
                await readJson(resolve(artifactRoot, proof.path), sf100ObservationSchema),
            );
            if (observation.disposition !== "committed") fail("Commit receipt contains a non-committed proof");
        }
        if (!observation || !artifactRoot)
            return result("reconcile", "No verified whole-shard observation; do not replay database writes");
        await verifyProtocolArtifacts(artifactRoot, observation.evidence);
        if (receipt?.status === "completed" && ["absent", "partial"].includes(observation.disposition)) {
            return result(
                "restore-and-reload",
                "Completed receipt conflicts with current shard observation; restore a clean epoch",
            );
        }
        if (observation.disposition === "committed")
            return result("skip", "Whole-shard observation accepted for this target epoch");
        if (observation.disposition === "absent")
            return result("retry-new-attempt", "Writer quiesced and whole shard confirmed absent");
        if (observation.disposition === "partial")
            return result("restore-and-reload", "Partial application requires a clean target epoch before reload");
        return result("reconcile", "The observation still cannot establish the outcome");
    }
    if (request.stage === "convert") {
        if (receipt?.status === "completed" && artifactRoot) {
            await verifyProtocolArtifacts(artifactRoot, receipt.artifacts);
            return result("reuse-shard", "Immutable output bytes and request identity verified");
        }
        return result("retry-new-attempt", "Unsealed conversion output cannot be reused; use a new attempt directory");
    }
    return result("restore-new-attempt", "Driver/restore attempts are never resumed by appending partial measurements");
}
