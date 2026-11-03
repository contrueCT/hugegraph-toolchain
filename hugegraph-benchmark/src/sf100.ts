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
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { BenchError, fileHash, hash, readJson } from "./io.js";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
export const sf100SuiteId = "hgbench-snb-interactive-v1-sf100";
export const operationIds = [
    ...Array.from({ length: 14 }, (_, i) => `IC${i + 1}`),
    ...Array.from({ length: 7 }, (_, i) => `IS${i + 1}`),
    ...Array.from({ length: 8 }, (_, i) => `IU${i + 1}`),
];
const sha = z.string().regex(/^[a-f0-9]{64}$/);
const bytes = z.string().regex(/^[1-9][0-9]{0,19}$/);
const positive = z.number().int().positive().max(Number.MAX_SAFE_INTEGER);
const artifact = z.strictObject({ path: z.string().min(1), sha256: sha });
const node = z.strictObject({
    id: z.string().min(1),
    role: z.enum(["server", "pd", "store", "driver"]),
    host: z.string().min(1),
    cpuCores: positive,
    memoryBytes: bytes,
    diskBudgetBytes: bytes,
    version: z.string().min(1),
    configSha256: sha,
});
export const sf100PlanSchema = z.strictObject({
    schemaVersion: z.literal(1),
    kind: z.literal("sf100-benchmark-plan"),
    suite: z.literal(sf100SuiteId),
    catalog: z.string().min(1),
    workload: z.strictObject({
        operations: z.array(z.string()).length(29),
        scheduler: z.literal("official-ldbc-driver"),
        parameterPolicy: z.literal("same-scale-official-parameters-and-driver-short-reads"),
        updatePolicy: z.literal("official-dependencies-and-timestamps"),
    }),
    target: z
        .strictObject({
            backend: z.literal("hstore"),
            graph: z.string().min(1),
            isolation: z.literal("dedicated-benchmark-graph"),
            deployment: z.enum(["multi-host", "single-host-multi-store"]).default("multi-host"),
            replicas: positive,
            partitions: positive,
            nodes: z.array(node).min(4),
        })
        .nullable(),
    budgets: z
        .strictObject({
            downloadPerAssetBytes: bytes,
            expandedBytes: bytes,
            workspaceBytes: bytes,
            reserveBytes: bytes,
            estimate: artifact,
        })
        .nullable(),
    execution: z.strictObject({
        timeCompressionRatio: z.number().finite().positive().nullable(),
        warmupSeconds: positive.nullable(),
        measurementSeconds: positive.nullable(),
        repetitions: z.number().int().min(3).max(100),
        seed: z.number().int().min(0).max(0xffffffff),
        cache: z.strictObject({ mode: z.enum(["warm", "cold"]), procedure: z.string().min(1) }).nullable(),
        restore: z.strictObject({
            method: z.enum(["reload-initial", "verified-snapshot"]),
            snapshot: artifact.nullable(),
            beforeEveryRepetition: z.literal(true),
            restartUpdateStream: z.literal(true),
        }),
    }),
    validation: z.strictObject({
        policy: z.literal("ordered-reference-replay-and-import-integrity"),
        referenceBundle: artifact.nullable(),
        onFailure: z.literal("invalidate-run"),
        missingOperations: z.literal("invalidate-run"),
    }),
    components: z.strictObject({
        converter: artifact.nullable(),
        loader: artifact.nullable(),
        driver: artifact.nullable(),
    }),
    evidence: z.strictObject({
        retainRawResults: z.literal(true),
        retainScheduledAndActualTimes: z.literal(true),
        retainTopologyAndResources: z.literal(true),
        resumeMeasurement: z.literal("new-attempt-from-restored-state"),
    }),
});
export type Sf100Plan = z.infer<typeof sf100PlanSchema>;
const assetSchema = z.strictObject({
    id: z.string(),
    role: z.enum(["initial", "parameters", "updates"]),
    scale: z.literal("100"),
    url: z.string().url(),
    sha256: sha.nullable(),
    bytes: positive.nullable(),
    checksumAuthority: z.string(),
    updatePartitions: positive.optional(),
});
const catalogSchema = z.strictObject({
    schemaVersion: z.literal(1),
    id: z.literal("ldbc-snb-interactive-v1-official"),
    sourcePage: z.string().url(),
    workload: z.literal("SNB Interactive v1"),
    serializer: z.literal("CsvBasic"),
    dateFormatter: z.literal("LongDateFormatter"),
    driver: z.strictObject({ repository: z.string(), commit: z.string().regex(/^[a-f0-9]{40}$/) }),
    reference: z.strictObject({ repository: z.string(), commit: z.string().regex(/^[a-f0-9]{40}$/) }),
    assets: z.array(assetSchema).length(3),
    validationPolicy: z.string(),
});
const coverageSchema = z.object({
    operations: z.array(
        z.object({
            id: z.string(),
            adapterStatus: z.string(),
            fullDriverValidation: z.string(),
        }),
    ),
});

export const sf100ComponentManifestSchema = z.strictObject({
    protocolVersion: z.literal(1),
    kind: z.literal("sf100-component"),
    role: z.enum(["converter", "loader", "driver"]),
    id: z.string().min(1),
    version: z.string().min(1),
    command: z.array(z.string().min(1)).min(1),
    trackedFiles: z.array(artifact).min(1),
    capabilities: z.strictObject({
        scale: z.literal("100"),
        serializer: z.literal("CsvBasic"),
        operations: z.array(z.string()),
        checkpoint: z.enum(["immutable-shard", "reconcile-before-retry", "restart-from-restored-state"]),
    }),
});

function invalid(message: string): never {
    throw new BenchError("SF100_CONTRACT_INVALID", message);
}
export async function inspectSf100Plan(file: string) {
    const plan = await readJson(file, sf100PlanSchema);
    if (
        new Set(plan.workload.operations).size !== 29 ||
        operationIds.some((id) => !plan.workload.operations.includes(id))
    ) {
        invalid("SF100 requires each of IC1..14, IS1..7 and IU1..8 exactly once");
    }
    if (plan.execution.restore.method === "verified-snapshot" && !plan.execution.restore.snapshot) {
        invalid("verified-snapshot requires an immutable snapshot manifest");
    }
    if (plan.execution.restore.method === "reload-initial" && plan.execution.restore.snapshot) {
        invalid("reload-initial must not name a snapshot");
    }
    if (plan.budgets && BigInt(plan.budgets.reserveBytes) >= BigInt(plan.budgets.workspaceBytes)) {
        invalid("Workspace budget must exceed reserved free space");
    }
    if (plan.target) {
        const nodes = plan.target.nodes;
        if (new Set(nodes.map((n) => n.id)).size !== nodes.length) invalid("Node IDs must be unique");
        for (const role of ["server", "pd", "store", "driver"]) {
            if (!nodes.some((n) => n.role === role)) invalid(`Missing target role: ${role}`);
        }
        const stores = nodes.filter((n) => n.role === "store");
        if (plan.target.deployment === "single-host-multi-store") {
            if (new Set(nodes.map((n) => n.host)).size !== 1)
                invalid("Single-host calibration requires the same host identity for every role");
            if (stores.length < 2) invalid("Single-host multi-Store calibration requires at least two Store nodes");
        } else if (new Set(stores.map((n) => n.host)).size < 2) {
            invalid("Distributed track requires at least two Store hosts");
        }
        if (plan.target.replicas > stores.length) invalid("Replica count exceeds Store node count");
    }
    const catalogPath = resolve(dirname(resolve(file)), plan.catalog);
    const catalog = await readJson(catalogPath, catalogSchema);
    const expected = await readJson(
        join(root, "benchmarks/ldbc-snb-interactive-v1/candidates/sf100.json"),
        catalogSchema,
    );
    if (hash(catalog.driver) !== hash(expected.driver) || hash(catalog.reference) !== hash(expected.reference)) {
        invalid("Driver/reference revision differs from the SF100 contract; revise the contract explicitly");
    }
    const sourceIdentity = (a: z.infer<typeof assetSchema>) => ({
        id: a.id,
        role: a.role,
        scale: a.scale,
        url: a.url,
        updatePartitions: a.updatePartitions ?? null,
    });
    if (hash(catalog.assets.map(sourceIdentity)) !== hash(expected.assets.map(sourceIdentity))) {
        invalid("SF100 requires the pinned initial, parameters and numpart-1 update sources in catalog order");
    }
    for (const [index, a] of catalog.assets.entries()) {
        if ((a.sha256 === null) !== (a.bytes === null))
            invalid(`Asset hash and byte count must be frozen together: ${a.id}`);
        const frozen = expected.assets[index]!;
        if (a.sha256 !== null && frozen.sha256 !== null && (a.sha256 !== frozen.sha256 || a.bytes !== frozen.bytes))
            invalid(`Asset content differs from the frozen SF100 suite: ${a.id}; revise the benchmark explicitly`);
    }
    const coverage = await readJson(join(root, "benchmarks/ldbc-snb-interactive-v1/coverage.json"), coverageSchema);
    if (
        coverage.operations.length !== 29 ||
        new Set(coverage.operations.map((o) => o.id)).size !== 29 ||
        operationIds.some((id) => !coverage.operations.some((o) => o.id === id))
    )
        invalid("Bundled coverage must enumerate all 29 operations");
    const blockers: { code: string; field: string; message: string }[] = [];
    const block = (code: string, field: string, message: string) => blockers.push({ code, field, message });
    if (catalog.assets.some((a) => a.sha256 === null))
        block("ASSETS_UNFROZEN", "catalog", "Capture and review SF100 hashes in the dedicated environment");
    if (!plan.target) block("TARGET_REQUIRED", "target", "Declare HStore topology, versions, configs and resources");
    if (!plan.budgets)
        block(
            "BUDGET_REQUIRED",
            "budgets",
            "Provide per-asset, expansion, workspace and reserve budgets with an estimate",
        );
    for (const field of ["timeCompressionRatio", "warmupSeconds", "measurementSeconds", "cache"] as const) {
        if (plan.execution[field] === null)
            block(
                "EXECUTION_SETTING_REQUIRED",
                `execution.${field}`,
                "Pin this setting before execution; no implicit defaults",
            );
    }
    if (!plan.validation.referenceBundle)
        block(
            "REFERENCE_REQUIRED",
            "validation.referenceBundle",
            "Provide the scale-matched validation bundle and provenance",
        );
    const base = dirname(resolve(file));
    const componentIdentities: Record<string, unknown> = {};
    for (const role of ["converter", "loader", "driver"] as const) {
        const ref = plan.components[role];
        if (!ref) {
            block("COMPONENT_REQUIRED", `components.${role}`, `Provide the ${role} component manifest`);
            continue;
        }
        const manifest = await readJson(resolve(base, ref.path), sf100ComponentManifestSchema);
        // Only bounded manifest files are read here; executables/data are not opened.
        if ((await fileHash(resolve(base, ref.path))) !== ref.sha256)
            invalid(`Component manifest digest mismatch: ${role}`);
        if (manifest.role !== role) invalid(`Component role mismatch: ${role}`);
        const checkpoints = {
            converter: "immutable-shard",
            loader: "reconcile-before-retry",
            driver: "restart-from-restored-state",
        };
        if (manifest.capabilities.checkpoint !== checkpoints[role]) invalid(`Unsupported checkpoint policy: ${role}`);
        const ops = manifest.capabilities.operations;
        if (
            role === "driver" &&
            (ops.length !== 29 || new Set(ops).size !== 29 || operationIds.some((id) => !ops.includes(id)))
        ) {
            invalid("Driver must declare all 29 operations exactly once");
        }
        componentIdentities[role] = { sha256: ref.sha256, manifest };
    }
    const unfinished = coverage.operations.filter((o) => o.fullDriverValidation !== "passed");
    if (unfinished.length)
        block(
            "OPERATIONS_UNVALIDATED",
            "workload.operations",
            `${unfinished.length}/29 operations lack full driver validation`,
        );
    block(
        "RUNTIME_NOT_IMPLEMENTED",
        "executor",
        "SF100 component dispatch and live verification are not implemented; this output is a planning report",
    );
    const stages = [
        {
            id: "assets",
            dependsOn: [],
            requires: ["frozen-catalog", "byte-budgets"],
            produces: ["verified-archive-inventory"],
            effect: "network-and-cache-write",
        },
        {
            id: "convert",
            dependsOn: ["assets"],
            requires: ["converter"],
            produces: ["schema", "immutable-shards", "counts-and-endpoint-checks"],
            effect: "workspace-write",
        },
        {
            id: "load",
            dependsOn: ["convert"],
            requires: ["loader", "isolated-target"],
            produces: ["import-receipts", "reconciled-counts"],
            effect: "database-write",
        },
        {
            id: "validate",
            dependsOn: ["load"],
            requires: ["driver", "reference-bundle"],
            produces: ["ordered-replay-validation"],
            effect: "database-read-write",
        },
        {
            id: "restore",
            dependsOn: ["validate"],
            requires: ["restore-proof"],
            produces: ["initial-state-identity", "update-offset-zero"],
            effect: "database-restore",
        },
        {
            id: "measure",
            dependsOn: ["restore"],
            requires: ["cache-procedure", "driver-settings", "environment-observations"],
            produces: ["raw-results", "schedule-and-resource-logs"],
            effect: "database-read-write",
        },
        {
            id: "report",
            dependsOn: ["measure"],
            requires: ["complete-evidence"],
            produces: ["validity-verdict", "per-operation-metrics"],
            effect: "report-write",
        },
    ];
    const identity = { plan, catalog, coverage, components: componentIdentities };
    return {
        schemaVersion: 1,
        kind: "sf100-planning-report",
        suite: sf100SuiteId,
        scale: "100",
        contractValid: true,
        executionReady: false,
        executed: false,
        officialBenchmarkClaimAllowed: false,
        topology: plan.target
            ? {
                  deployment: plan.target.deployment,
                  declaredHosts: new Set(plan.target.nodes.map((n) => n.host)).size,
                  declaredStoreHosts: new Set(plan.target.nodes.filter((n) => n.role === "store").map((n) => n.host))
                      .size,
                  crossHostBehaviorVerified: false,
                  driverSharesStoreHost: plan.target.nodes.some(
                      (n) =>
                          n.role === "driver" &&
                          plan.target!.nodes.some((s) => s.role === "store" && s.host === n.host),
                  ),
                  comparisonGroup: plan.target.deployment,
                  scope:
                      plan.target.deployment === "single-host-multi-store"
                          ? "single-os-host-calibration; shared-cpu-memory-disk; no-cross-host-or-host-failure-claim"
                          : "declared-multi-host; live-physical-identity-and-placement-unverified",
              }
            : null,
        digest: hash(identity),
        identity,
        blockers,
        stages,
        repetitions: {
            count: plan.execution.repetitions,
            repeatStages: ["restore", "measure"],
            failedAttempt: "retain-evidence-and-restart",
        },
        requiredMetrics: [
            "throughput",
            "latency-per-operation",
            "scheduled-start",
            "actual-start",
            "completion",
            "errors",
            "timeouts",
            "late-operations",
            "driver-saturation",
            "server-store-resources",
            "partition-and-replica-placement",
        ],
        resourceEstimate: {
            status: "not-measured",
            basis: plan.budgets?.estimate ?? null,
            totalBytes: null,
            durationSeconds: null,
        },
        verificationScope:
            "local-config-and-component-manifest-only; no-archive-binary-reference-or-live-target-verification",
    };
}
