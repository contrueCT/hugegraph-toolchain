/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
import { createHash } from "node:crypto";
import { isDeepStrictEqual } from "node:util";
import { valueHash, balancedOrders, analyze } from "../paired/core.mjs";
export const OBSERVER003 = Object.freeze({
    id: "http-observer-cost-003",
    cases: ["scalar-add", "rows-4096", "string-1024k"],
    unmeasured: [
        "structured-one",
        "rows-0016",
        "rows-0256",
        "string-0001k",
        "string-0016k",
        "string-0064k",
        "string-0256k",
        "string-unicode",
    ],
    sourceSha256: "2b878425d6b5f9968777a14c3bd1622a6fc3349b7d5f88c139c773556dfde89a",
    generatorSha256: "6d794454072338269855bb5f964130e649b70780b501d652fc9daeb20d85210e",
    executorSha256: "e0e0bbf2f07510d397163c8759a91d86cd663e8b532a70d40db2e4cc058828b0",
    helperSha256: "9148d53e036a7cffbde2dbec77705752cfbb03d48e46556391ed49afed4bae78",
    commit: "d1239ef88d2d9b4b9b23fb920bcf7f6a387053d7",
    bundleSha256: "23ccedd3cec1ae3c58a0a847019255a213ff70e6f7aa6c116217e97ccf492c23",
    pairs: ["p01", "p02", "p03", "p04"],
});
export const observerNeed = (ok, code) => {
    if (!ok) throw Object.assign(new Error(code), { code });
};
export const observerSame = (a, b, code) => observerNeed(isDeepStrictEqual(a, b), code);
export const observerSha = (v) => createHash("sha256").update(v).digest("hex");
export const observerTaskId = (v) => "task-" + observerSha(JSON.stringify(v)).slice(0, 32);
export const observerNs = (v) => {
    observerNeed(typeof v === "string" && /^(0|[1-9][0-9]{0,24})$/.test(v), "OBSERVER_TIME");
    return BigInt(v);
};
export function observer003Scope() {
    return {
        schemaVersion: 1,
        kind: "http-observer-cost-003-scope",
        selectedCaseIds: OBSERVER003.cases,
        unmeasuredCaseIds: OBSERVER003.unmeasured,
        unmeasuredStatus: "unknown",
        physicalArms: 16,
        plannedCaseRuns: 48,
        metricsPerGroup: 6,
        originalScope: "observer002 full eleven cases",
        scopeChange: "prospective three representative cases; no inference for other eight",
        fullElevenCaseObserverGateSatisfied: false,
        performanceAccepted: false,
        qualificationAccepted: false,
    };
}
export function observer003Schedule(orders) {
    const rows = [];
    for (const group of ["J", "C"]) {
        const expected = balancedOrders(OBSERVER003.pairs, OBSERVER003.id + ":" + group + ":order-v1");
        observerSame(orders[group], expected, "OBSERVER_BALANCED_ORDER");
        for (const blockId of OBSERVER003.pairs)
            for (const arm of orders[group][blockId])
                rows.push({
                    group,
                    blockId,
                    arm,
                    cellId: group + "-" + blockId + "-" + arm,
                    jvmObservation: group === "C" || arm === "B",
                    collector: group === "J" || arm === "B",
                });
    }
    return rows;
}
export function observer003Policy(group, shared) {
    return {
        schemaVersion: 1,
        kind: "paired-analysis-policy",
        id: OBSERVER003.id + "-" + group,
        algorithm: "paired-median-percentile-bootstrap-v1",
        blockIds: OBSERVER003.pairs,
        metrics: OBSERVER003.cases.flatMap((id) =>
            ["correctEndToEndMs.p99", "correctResponseMs.p50"].map((field) => ({
                id: id + "." + field,
                unit: "ms",
                better: "lower",
                relativeBudget: 0.05,
                absoluteBudget: 1,
                minimumObservations: 1000,
            })),
        ),
        familyAlpha: 0.025,
        minimumPairs: 4,
        bootstrapReplicates: 80000,
        minimumTailDraws: 20,
        maxWork: 1000000000,
        maxMilliseconds: 60000,
        orderSeed: OBSERVER003.id + ":" + group + ":order-v1",
        resampleSeed: OBSERVER003.id + ":" + group + ":resample-v1",
        baselineDigest: valueHash({ shared, group, arm: "A" }),
        candidateDigest: valueHash({ shared, group, arm: "B" }),
        contexts: Object.fromEntries(OBSERVER003.pairs.map((pair) => [pair, valueHash({ shared, group, pair })])),
    };
}
export function verifyObserverTask(plan, terminal, request) {
    const { digest, ...content } = plan;
    observerNeed(
        plan.schemaVersion === 1 && plan.kind === "frozen-component-task" && digest === valueHash(content),
        "OBSERVER_TASK_PLAN",
    );
    observerNeed(
        terminal.taskId === observerTaskId(request) &&
            terminal.planDigest === digest &&
            terminal.terminal === true &&
            terminal.state === "completed" &&
            terminal.interrupted === false &&
            terminal.reused !== true &&
            terminal.processExitCode === 0 &&
            terminal.error === null,
        "OBSERVER_TASK_TERMINAL",
    );
}
export function verifyObserverProfile(profile) {
    observerSame(
        {
            suiteId: profile.benchmark?.suiteId,
            version: profile.benchmark?.version,
            bundleSha256: profile.benchmark?.bundleSha256,
            coverage: profile.benchmark?.coverage,
        },
        {
            suiteId: "hgbench-http-serving-v0.1",
            version: "0.1.0-draft.1",
            bundleSha256: OBSERVER003.bundleSha256,
            coverage: "subset",
        },
        "OBSERVER_PROFILE_BUNDLE",
    );
    observerNeed(
        profile.schemaVersion === 1 &&
            profile.kind === "http-benchmark-profile" &&
            profile.serviceChecks === true &&
            profile.responseEncoding === "gzip" &&
            profile.authorizationEnv === null &&
            profile.endpoint === "http://127.0.0.1:18080/gremlin" &&
            profile.target?.build === OBSERVER003.commit &&
            profile.target.mode === "forward" &&
            profile.benchmark?.purpose === "calibration" &&
            profile.warmupMs === 30000 &&
            profile.durationMs === 120000 &&
            profile.requestTimeoutMs === 5000 &&
            profile.drainMs === 5000 &&
            profile.repeats === 1 &&
            profile.maxTraceBytes === 268435456 &&
            profile.maxRequestsPerPhase === 2000 &&
            profile.maxResponseBytes === 2097152 &&
            !profile.clockReference,
        "OBSERVER_PROFILE",
    );
    observerSame(profile.benchmark.caseIds, OBSERVER003.cases, "OBSERVER_PROFILE_CASES");
    observerSame(
        profile.cases.map((c) => c.id),
        OBSERVER003.cases,
        "OBSERVER_PROFILE_CASES",
    );
    observerSame(
        profile.loads,
        [{ id: "open-10", model: "open", ratePerSecond: 10, maxInFlight: 4, maxStartDelayMs: 1000 }],
        "OBSERVER_PROFILE_LOAD",
    );
}
export function observer003Driver(info, arm, context, targetId, id) {
    const c = info?.Config,
        h = info?.HostConfig,
        spec = context.spec,
        runtime = spec.runtime;
    observerNeed(
        /^[a-f0-9]{64}$/.test(info?.Id) &&
            (!id || id === info.Id) &&
            info.Name === "/" + arm.runId + "-driver" &&
            info.Image === runtime.driver.image &&
            c?.Labels?.["org.apache.hugegraph.hgbench.observer-cost.request"] === arm.runId &&
            c.User === spec.owner &&
            c.WorkingDir === "/source",
        "OBSERVER_DRIVER_IDENTITY",
    );
    observerSame(c.Entrypoint, [runtime.driver.python], "OBSERVER_DRIVER_ENTRYPOINT");
    observerSame(c.Cmd, ["-B", "-c", "import time; time.sleep(960)"], "OBSERVER_DRIVER_ENTRYPOINT");
    observerNeed(
        h.NetworkMode === "container:" + targetId &&
            h.ReadonlyRootfs === true &&
            h.Memory === 805306368 &&
            h.MemorySwap === h.Memory &&
            h.NanoCpus === 1000000000 &&
            h.PidsLimit === 128 &&
            h.Init === true &&
            !h.Privileged &&
            !h.CapAdd?.length &&
            h.SecurityOpt?.includes("no-new-privileges:true") &&
            h.RestartPolicy?.Name === "no" &&
            h.LogConfig?.Type === "none" &&
            !Object.keys(h.PortBindings ?? {}).length &&
            !h.PublishAllPorts &&
            !h.Devices?.length &&
            !h.PidMode,
        "OBSERVER_DRIVER_ISOLATION",
    );
    observerSame(h.CapDrop, ["ALL"], "OBSERVER_DRIVER_ISOLATION");
    observerSame(h.Tmpfs, { "/tmp": "rw,nosuid,nodev,noexec,size=67108864" }, "OBSERVER_DRIVER_ISOLATION");
    const project = (a) =>
        a
            .map((m) => ({ Source: m.Source, Destination: m.Destination, RW: m.RW }))
            .sort((a, b) => a.Destination.localeCompare(b.Destination));
    observerSame(
        project(info.Mounts),
        project([
            { Source: spec.sourceRoot, Destination: "/source", RW: false },
            { Source: runtime.nodeHome, Destination: "/node", RW: false },
            { Source: arm.directory + "/driver-evidence", Destination: "/evidence", RW: true },
        ]),
        "OBSERVER_DRIVER_MOUNTS",
    );
    return info.Id;
}
export function summarizeObserver003(policies, arms, issues = []) {
    const groups = ["J", "C"].map((group) => {
        const policy = policies[group],
            blocks = [];
        for (const id of OBSERVER003.pairs) {
            const pair = arms.filter((a) => a.group === group && a.blockId === id);
            if (pair.length !== 2 || pair.some((a) => !a.valid)) continue;
            const norm = (arm, variant) => {
                const r = pair.find((p) => p.arm === arm);
                return {
                    runId: r.runId,
                    evidenceSha256: r.rawManifestSha256,
                    variantDigest: variant,
                    conditionsDigest: policy.contexts[id],
                    status: "ok",
                    values: r.values,
                };
            };
            blocks.push({
                id,
                observedOrder: balancedOrders(OBSERVER003.pairs, policy.orderSeed)[id],
                baseline: norm("A", policy.baselineDigest),
                candidate: norm("B", policy.candidateDigest),
            });
        }
        const analysis = analyze({ schemaVersion: 1, kind: "normalized-paired-observations", policy, blocks });
        const usable =
            analysis.cells.length === 6 &&
            analysis.cells.every(
                (c) =>
                    c.observedPairs === 4 &&
                    c.issues.length === 0 &&
                    c.warnings.length === 0 &&
                    c.bootstrapIntervals !== null,
            );
        return {
            id: group,
            plannedPairs: 4,
            plannedMetrics: 6,
            analysis,
            usable,
            effectInterpretation:
                "above/below/cross/within budgets are measured conditional effects; a nonzero cost is not an engineering rejection",
        };
    });
    const evidenceVerified = issues.length === 0 && arms.length === 16 && arms.every((a) => a.valid);
    const scopedCostMeasured = evidenceVerified && groups.every((g) => g.usable);
    return { evidenceVerified, scopedCostMeasured, status: scopedCostMeasured ? "passed" : "unknown", groups };
}
