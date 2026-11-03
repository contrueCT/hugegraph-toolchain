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

export const KNOWN002 = Object.freeze({
    operationVersion: "aa005-async-end-to-end-p99-v1",
    id: "http-known-delay-002",
    sourceSelectionSha256: "2b878425d6b5f9968777a14c3bd1622a6fc3349b7d5f88c139c773556dfde89a",
    generatorSha256: "9148d53e036a7cffbde2dbec77705752cfbb03d48e46556391ed49afed4bae78",
    executorSha256: "015a850c1b0f7ee6fade380869bd61e1b7ac2d2cd1c50cb7f04ff16a7c4c5245",
    commit: "d1239ef88d2d9b4b9b23fb920bcf7f6a387053d7",
    scripts: { A: "1 + 1", B: "Thread.sleep(10); 1 + 1" },
    blocks: Array.from({ length: 8 }, (_, i) => "b" + String(i + 1).padStart(2, "0")),
    implementationFiles: [
        "workflow.mjs",
        "runner.mjs",
        "trace-writer.mjs",
        "direct-phase-clock.mjs",
        "runner-sanity.mjs",
        "phase-clock-reference.py",
        "clock-correlation.mjs",
        "analyzer.mjs",
        "common.mjs",
        "suite-evidence.mjs",
        "probe.mjs",
    ],
});
export const needKnown = (ok, code) => {
    if (!ok) throw Object.assign(new Error(code), { code });
};
export const sameKnown = (a, b, code) => needKnown(isDeepStrictEqual(a, b), code);
export const knownSha = (raw) => createHash("sha256").update(raw).digest("hex");
export const knownTaskId = (request) => "task-" + knownSha(JSON.stringify(request)).slice(0, 32);
export const knownNs = (v) => {
    needKnown(typeof v === "string" && /^[0-9]{1,24}$/.test(v), "KNOWN_COMMAND_TIME");
    return BigInt(v);
};
export function known002Policy(shared) {
    return {
        schemaVersion: 1,
        kind: "paired-analysis-policy",
        id: KNOWN002.id,
        algorithm: "paired-median-percentile-bootstrap-v1",
        blockIds: KNOWN002.blocks,
        metrics: [
            {
                id: "end-to-end-p99",
                unit: "ms",
                better: "lower",
                relativeBudget: 0.05,
                absoluteBudget: 5,
                minimumObservations: 1000,
            },
        ],
        familyAlpha: 0.05,
        minimumPairs: 8,
        bootstrapReplicates: 10000,
        minimumTailDraws: 20,
        maxWork: 1000000,
        maxMilliseconds: 60000,
        orderSeed: KNOWN002.id + ":balanced-order-v1",
        resampleSeed: KNOWN002.id + ":resample-v1",
        baselineDigest: valueHash({ shared, gremlin: KNOWN002.scripts.A }),
        candidateDigest: valueHash({ shared, gremlin: KNOWN002.scripts.B }),
        contexts: Object.fromEntries(KNOWN002.blocks.map((block) => [block, valueHash({ shared, block })])),
    };
}
export function verifyKnown002Profile(profile, arm, evidenceFiles) {
    const expected = {
        schemaVersion: 1,
        kind: "http-benchmark-profile",
        name: KNOWN002.id + "-" + arm.toLowerCase(),
        endpoint: "http://127.0.0.1:18080/gremlin",
        authorizationEnv: null,
        responseEncoding: "gzip",
        serviceChecks: true,
        target: {
            label: "hugegraph-known-delay-positive-control",
            build: KNOWN002.commit,
            mode: "forward",
            environment:
                "Artificial sleep10 request calibration; custom workload; same candidate and forward mode; not a standard-suite score or forward/embedded comparison.",
            evidenceFiles,
        },
        cases: [
            {
                id: "arithmetic-control",
                body: { gremlin: KNOWN002.scripts[arm], bindings: {}, language: "gremlin-groovy" },
                expectedStatus: 200,
                checks: [
                    { pointer: "/status/code", equals: 200 },
                    { pointer: "/result/data", equals: [2] },
                ],
            },
        ],
        loads: [{ id: "open-20", model: "open", ratePerSecond: 20, maxInFlight: 4, maxStartDelayMs: 1000 }],
        repeats: 1,
        warmupMs: 10000,
        durationMs: 60000,
        requestTimeoutMs: 2000,
        drainMs: 2000,
        maxRequestsPerPhase: 2000,
        maxResponseBytes: 65536,
        maxTraceBytes: 134217728,
    };
    sameKnown(profile, expected, "KNOWN_FIXED_PROFILE");
    return expected;
}

// Literal operation templates from the preregistered known002 generator. Never execute them.
export function known002Operations(arm, sequence, spec) {
    const dir = arm.directory,
        env = dir + "/environment",
        evidence = dir + "/driver-evidence";
    const docker = spec.dockerExecutable,
        uid = spec.owner,
        driver = spec.driver;
    const target = "{{target.container.id}}",
        cid = "{{driver.container.id}}";
    const cli = [sequence.runtimeNode, sequence.sourceRoot + "/dist/cli.js"];
    const probe = [
        docker,
        "exec",
        "--user",
        uid,
        target,
        driver.python,
        "-B",
        "/control/hugegraph-mode-probe.py",
        "--endpoint",
        "http://127.0.0.1:18080/gremlin",
        "--jcmd",
        "{{target.ready.jcmdExecutable}}",
        "--java-pid",
        "{{target.ready.java.pid}}",
        "--expected-mode",
        "forward",
        "--total-timeout-seconds",
        "45",
    ];
    const create = [
        docker,
        "create",
        "--name",
        arm.runId + "-driver",
        "--cidfile",
        dir + "/driver.id",
        "--pull=never",
        "--restart",
        "no",
        "--init",
        "--label",
        "org.apache.hugegraph.hgbench.known-delay.request=" + arm.runId,
        "--network",
        "container:" + target,
        "--user",
        uid,
        "--read-only",
        "--cap-drop",
        "ALL",
        "--security-opt",
        "no-new-privileges:true",
        "--cpus",
        "1",
        "--memory",
        "805306368",
        "--memory-swap",
        "805306368",
        "--pids-limit",
        "128",
        "--tmpfs",
        "/tmp:rw,nosuid,nodev,noexec,size=67108864",
        "--log-driver",
        "none",
        "--stop-timeout",
        "5",
        "--workdir",
        "/source",
        "--mount",
        "type=bind,src=" + sequence.sourceRoot + ",dst=/source,readonly",
        "--mount",
        "type=bind,src=" + sequence.nodeHome + ",dst=/node,readonly",
        "--mount",
        "type=bind,src=" + evidence + ",dst=/evidence",
        "--entrypoint",
        driver.python,
        driver.image,
        "-B",
        "-c",
        "import time; time.sleep(180)",
    ];
    const inside = [docker, "exec", "--user", uid, cid, "/node/bin/node", "/source/dist/cli.js"];
    return {
        "start-fresh-target": [
            ...cli,
            "--root",
            dir + "/target-tasks",
            "task",
            "run",
            "--plan",
            env + "/task.json",
            "--detach",
            "--request-id",
            arm.runId + "-target",
        ],
        ready: [...cli, "http", "environment", "status", env],
        "mode-pre": [...probe, "--out", "/work/mode-pre"],
        "create-driver": create,
        "start-driver": [docker, "start", cid],
        "plan-http-inside-driver": [
            ...inside,
            "http",
            "plan",
            "/evidence/profile.json",
            "--out",
            "/evidence/http-plan",
        ],
        "run-http": [
            ...inside,
            "--root",
            "/evidence/tasks",
            "task",
            "run",
            "--plan",
            "/evidence/http-plan/task.json",
            "--request-id",
            arm.requestId,
        ],
        "wait-original-http": [...inside, "--root", "/evidence/tasks", "task", "status", "--request-id", arm.requestId],
        "http-artifacts": [...inside, "--root", "/evidence/tasks", "task", "artifacts", "--id", "{{http.task.id}}"],
        "mode-post": [...probe, "--out", "/work/mode-post"],
        "stop-target": [...cli, "http", "environment", "stop", env],
        "target-terminal": [
            ...cli,
            "--root",
            dir + "/target-tasks",
            "task",
            "status",
            "--request-id",
            arm.runId + "-target",
        ],
        "driver-inspect-before-cleanup": [docker, "inspect", cid],
        "stop-driver": [docker, "stop", "--time", "5", cid],
        "remove-owned-driver": [docker, "rm", cid],
        "remove-owned-target": [docker, "rm", target],
        "independent-http-rebuild": [
            ...cli,
            "http",
            "report",
            evidence + "/http-plan/output",
            "--out",
            dir + "/rebuilt-http",
        ],
    };
}
export function known002Driver(info, arm, sequence, spec, targetCid, expectedCid) {
    needKnown(info && /^[a-f0-9]{64}$/.test(info.Id) && (!expectedCid || info.Id === expectedCid), "KNOWN_DRIVER_ID");
    const c = info.Config,
        h = info.HostConfig;
    needKnown(
        info.Name === "/" + arm.runId + "-driver" &&
            info.Image === spec.driver.image &&
            c.Labels?.["org.apache.hugegraph.hgbench.known-delay.request"] === arm.runId &&
            c.User === spec.owner &&
            c.WorkingDir === "/source",
        "KNOWN_DRIVER_OWNER",
    );
    sameKnown(c.Entrypoint, [spec.driver.python], "KNOWN_DRIVER_ENTRYPOINT");
    sameKnown(c.Cmd, ["-B", "-c", "import time; time.sleep(180)"], "KNOWN_DRIVER_ENTRYPOINT");
    needKnown(
        h.NetworkMode === "container:" + targetCid &&
            h.ReadonlyRootfs === true &&
            h.Memory === 805306368 &&
            h.MemorySwap === h.Memory &&
            h.NanoCpus === 1000000000 &&
            h.PidsLimit === 128 &&
            h.Privileged === false &&
            !h.CapAdd?.length &&
            h.SecurityOpt?.includes("no-new-privileges:true") &&
            h.Init === true &&
            h.RestartPolicy?.Name === "no" &&
            h.LogConfig?.Type === "none" &&
            !Object.keys(h.PortBindings ?? {}).length &&
            !h.Devices?.length &&
            !h.PidMode,
        "KNOWN_DRIVER_ISOLATION",
    );
    sameKnown(h.CapDrop, ["ALL"], "KNOWN_DRIVER_ISOLATION");
    sameKnown(h.Tmpfs, { "/tmp": "rw,nosuid,nodev,noexec,size=67108864" }, "KNOWN_DRIVER_ISOLATION");
    sameKnown(
        info.Mounts.map((m) => ({ Source: m.Source, Destination: m.Destination, RW: m.RW })).sort((a, b) =>
            a.Destination.localeCompare(b.Destination),
        ),
        [
            { Source: sequence.sourceRoot, Destination: "/source", RW: false },
            { Source: sequence.nodeHome, Destination: "/node", RW: false },
            { Source: arm.directory + "/driver-evidence", Destination: "/evidence", RW: true },
        ].sort((a, b) => a.Destination.localeCompare(b.Destination)),
        "KNOWN_DRIVER_MOUNTS",
    );
    return info.Id;
}
export function verifyKnown002Task(plan, terminal, request) {
    const { digest, ...content } = plan;
    needKnown(
        plan.kind === "frozen-component-task" && plan.schemaVersion === 1 && digest === valueHash(content),
        "KNOWN_TASK_PLAN",
    );
    needKnown(
        terminal.taskId === knownTaskId(request) &&
            terminal.planDigest === digest &&
            terminal.state === "completed" &&
            terminal.terminal === true &&
            terminal.interrupted === false &&
            terminal.reused !== true &&
            terminal.processExitCode === 0 &&
            terminal.error === null,
        "KNOWN_TASK_TERMINAL",
    );
    return digest;
}
export function summarizeKnown002(policy, arms, issues = []) {
    // Only this adapter's independently reconstructed arms can enter this function.
    const blocks = [];
    for (const id of KNOWN002.blocks) {
        const a = arms.find((r) => r.blockId === id && r.arm === "A"),
            b = arms.find((r) => r.blockId === id && r.arm === "B");
        if (!a?.valid || !b?.valid) continue;
        const normalized = (r, variant) => ({
            runId: r.runId,
            evidenceSha256: r.rawManifestSha256,
            variantDigest: variant,
            conditionsDigest: policy.contexts[id],
            status: "ok",
            values: { "end-to-end-p99": { value: r.endToEndP99, observations: r.observations } },
        });
        blocks.push({
            id,
            observedOrder: balancedOrders(KNOWN002.blocks, policy.orderSeed)[id],
            baseline: normalized(a, policy.baselineDigest),
            candidate: normalized(b, policy.candidateDigest),
        });
    }
    const analysis = analyze({ schemaVersion: 1, kind: "normalized-paired-observations", policy, blocks });
    const cell = analysis.cells[0];
    const valid = issues.length === 0 && arms.length === 16 && arms.every((a) => a.valid) && blocks.length === 8;
    const eligible =
        valid && cell.issues.length === 0 && cell.warnings.length === 0 && cell.bootstrapIntervals !== null;
    const relation = eligible ? cell.budgetRelation : "not-assessed";
    const detection =
        relation === "above-both-budgets"
            ? "detected"
            : ["below-both-budgets", "within-at-least-one-budget"].includes(relation)
              ? "not-detected"
              : "inconclusive";
    return {
        valid,
        status: detection === "detected" ? "passed" : detection === "not-detected" ? "failed" : "unknown",
        detection: {
            status: detection,
            metric: policy.metrics[0],
            budgetRelation: relation,
            intervals: eligible ? cell.bootstrapIntervals : null,
        },
        analysis,
    };
}
