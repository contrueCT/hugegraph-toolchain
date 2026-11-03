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
import { valueHash } from "../paired/core.mjs";

export const HEADROOM002 = Object.freeze({
    operationVersion: "http-driver-headroom-002",
    id: "http-driver-headroom-002",
    sourceSelectionSha256: "2b878425d6b5f9968777a14c3bd1622a6fc3349b7d5f88c139c773556dfde89a",
    criteriaSha256: "71fe2c244e0308d451e2759c06a457d576cf9afb73c526b00dfb978d4b524a0a",
    generatorSha256: "140f9f6d116f8f49ad94c86ac41985ebe69ca581df49d531892f1351fbeaad7f",
    executorSha256: "88d32a7f731dc93d92d8542ca01ac4827ea3bc83680bca3888a091e60a19315b",
    fixedLifecycleSha256: "015a850c1b0f7ee6fade380869bd61e1b7ac2d2cd1c50cb7f04ff16a7c4c5245",
    fixedPlanningHelperSha256: "9148d53e036a7cffbde2dbec77705752cfbb03d48e46556391ed49afed4bae78",
    settingsSha256: "7e9740caa602089cb0333333691da3b8ef446f7f11831727cbc105c84e7aa2be",
    budgetsSha256: "b5c327f994fbedcb5236ba101e0b14f77ff3e732c7c2b5996824b9176c2301a2",
    assessmentContractSha256: "31e23454fb84fbd513d27cd4f097f240883a87f630e93702b26065b9853e5031",
    windowPolicy: {
        windowMs: 10000,
        minimumRequestsPerWindow: 50,
        minimumP99Requests: 1000,
    },
    cases: ["scalar-add", "rows-4096", "string-1024k"],
    rates: [10, 20, 40],
    cpus: [1, 2],
    blocks: ["b01", "b02"],
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
const PROFILE = {
    schemaVersion: 1,
    kind: "http-benchmark-profile",
    benchmark: {
        suiteId: "hgbench-http-serving-v0.1",
        version: "0.1.0-draft.1",
        bundleSha256: "23ccedd3cec1ae3c58a0a847019255a213ff70e6f7aa6c116217e97ccf492c23",
        caseIds: ["scalar-add", "rows-4096", "string-1024k"],
        coverage: "subset",
        purpose: "calibration",
    },
    serviceChecks: true,
    endpoint: "http://127.0.0.1:18080/gremlin",
    responseEncoding: "gzip",
    authorizationEnv: null,
    target: {
        label: "hugegraph-driver-headroom",
        build: "d1239ef88d2d9b4b9b23fb920bcf7f6a387053d7",
        mode: "forward",
        environment:
            "Finite driver-CPU/warmup diagnostic; fixed target; shared host disclosed; maxInFlight=4 and complete observer stack match AA005; limited 3-case/rate stratum only",
    },
    cases: [
        {
            id: "scalar-add",
            body: {
                gremlin: "x + y",
                bindings: {
                    x: 17,
                    y: 25,
                },
                language: "gremlin-groovy",
            },
            expectedStatus: 200,
            checks: [
                {
                    pointer: "/status/code",
                    equals: 200,
                },
                {
                    pointer: "/result/data",
                    canonicalSha256: "b86b1ea11b28136fe5224b9d1e3017b7efb68d4fae0b90c4940e0c0f89b3907a",
                },
            ],
        },
        {
            id: "rows-4096",
            body: {
                gremlin: "(0..<n).collect { i -> [id: i, name: 'hgbench', value: i * 3, active: (i % 2 == 0)] }",
                bindings: {
                    n: 4096,
                },
                language: "gremlin-groovy",
            },
            expectedStatus: 200,
            checks: [
                {
                    pointer: "/status/code",
                    equals: 200,
                },
                {
                    pointer: "/result/data",
                    canonicalSha256: "1a96eaa26b0b4de3e10e5e87207a797c43059bcaff58f6ba8a42f93c2145980e",
                },
            ],
        },
        {
            id: "string-1024k",
            body: {
                gremlin: "'x' * n",
                bindings: {
                    n: 1048576,
                },
                language: "gremlin-groovy",
            },
            expectedStatus: 200,
            checks: [
                {
                    pointer: "/status/code",
                    equals: 200,
                },
                {
                    pointer: "/result/data",
                    canonicalSha256: "1a94c55550d8dde1ae5809c88f577aa892d39e6fbac5ccfd2ced0373d2f33caf",
                },
            ],
        },
    ],
    repeats: 1,
    warmupMs: 30000,
    durationMs: 120000,
    requestTimeoutMs: 5000,
    drainMs: 5000,
    maxRequestsPerPhase: 6000,
    maxResponseBytes: 2097152,
    maxTraceBytes: 134217728,
};
export const needHeadroom = (ok, code) => {
    if (!ok) throw Object.assign(new Error(code), { code });
};
export const sameHeadroom = (a, b, code) => needHeadroom(isDeepStrictEqual(a, b), code);
export const headroomSha = (raw) => createHash("sha256").update(raw).digest("hex");
export const headroomNs = (v) => {
    needHeadroom(typeof v === "string" && /^[0-9]{1,24}$/.test(v), "HEADROOM_TIME");
    return BigInt(v);
};
export function headroomTaskId(request) {
    needHeadroom(typeof request === "string" && /^[\x20-\x7e]{1,128}$/.test(request), "HEADROOM_REQUEST");
    return "task-" + headroomSha(JSON.stringify(request)).slice(0, 32);
}
export function headroomDeclaredUnits(sequence) {
    const fixed = [
        "hgbench-sf100-convert-v1.service",
        "hgbench-sf100-reference-convert-v1.service",
        "hgbench-sf100-update-audit-v1.service",
    ];
    needHeadroom(
        Array.isArray(sequence.declaredInactiveUnits) &&
            sequence.declaredInactiveUnits.length <= 32 &&
            sequence.declaredInactiveUnits.every(
                (u) => typeof u === "string" && /^[a-zA-Z0-9_.@-]{1,240}\.service$/.test(u),
            ),
        "HEADROOM_JOB_DECLARATIONS",
    );
    const units = [...new Set(sequence.declaredInactiveUnits)];
    sameHeadroom(units.slice(0, 3), fixed, "HEADROOM_JOB_DECLARATIONS");
    needHeadroom(
        sequence.protectedObservedUnit === "hgbench-sf100-docker-environment-v6.service" &&
            !units.includes(sequence.protectedObservedUnit),
        "HEADROOM_JOB_DECLARATIONS",
    );
    if (sequence.mode === "prepare")
        needHeadroom(
            ["hgbench-http-aa-cohort-005.service", "hgbench-http-known-delay-002.service"].every((u) =>
                units.includes(u),
            ),
            "HEADROOM_PREDECESSOR_UNITS",
        );
    return [...units, sequence.protectedObservedUnit];
}
export function headroomGrid() {
    return HEADROOM002.blocks.flatMap((blockId, i) =>
        HEADROOM002.rates.flatMap((ratePerSecond) =>
            (i === 0 ? [1, 2] : [2, 1]).map((driverCpus) => ({
                blockId,
                ratePerSecond,
                driverCpus,
                cellId: blockId + "-r" + ratePerSecond + "-cpu" + driverCpus,
            })),
        ),
    );
}
export function headroomProfile(arm, evidenceFiles) {
    const expected = {
        ...structuredClone(PROFILE),
        name: HEADROOM002.id + "-" + arm.cellId,
        loads: [
            {
                id: "open-" + arm.ratePerSecond,
                model: "open",
                ratePerSecond: arm.ratePerSecond,
                maxInFlight: 4,
                maxStartDelayMs: 1000,
            },
        ],
    };
    expected.target.evidenceFiles = evidenceFiles;
    return expected;
}
export function verifyHeadroomProfile(profile, arm, evidenceFiles) {
    const expected = headroomProfile(arm, evidenceFiles);
    sameHeadroom(profile, expected, "HEADROOM_PROFILE");
    return expected;
}
export function verifyHeadroomTask(plan, terminal, request) {
    const { digest, ...content } = plan;
    needHeadroom(
        plan.kind === "frozen-component-task" && plan.schemaVersion === 1 && digest === valueHash(content),
        "HEADROOM_TASK_PLAN",
    );
    needHeadroom(
        terminal.taskId === headroomTaskId(request) &&
            terminal.planDigest === digest &&
            terminal.state === "completed" &&
            terminal.terminal === true &&
            terminal.interrupted === false &&
            terminal.reused !== true &&
            terminal.processExitCode === 0 &&
            terminal.error === null,
        "HEADROOM_TASK_TERMINAL",
    );
    return digest;
}
export function verifyHeadroomDriver(info, arm, sequence, spec, targetCid, expectedCid) {
    needHeadroom(/^[a-f0-9]{64}$/.test(info?.Id) && (!expectedCid || expectedCid === info.Id), "HEADROOM_DRIVER_ID");
    const c = info.Config,
        h = info.HostConfig,
        d = spec.driver;
    needHeadroom(
        info.Name === "/" + arm.runId + "-driver" &&
            info.Image === d.image &&
            c?.Labels?.["org.apache.hugegraph.hgbench.driver-headroom.request"] === arm.runId &&
            c.User === spec.owner &&
            c.WorkingDir === "/source",
        "HEADROOM_DRIVER_OWNER",
    );
    sameHeadroom(c.Entrypoint, [d.python], "HEADROOM_DRIVER_COMMAND");
    sameHeadroom(c.Cmd, ["-B", "-c", "import time; time.sleep(900)"], "HEADROOM_DRIVER_COMMAND");
    needHeadroom(
        h.NetworkMode === "container:" + targetCid &&
            h.ReadonlyRootfs === true &&
            h.Memory === 805306368 &&
            h.MemorySwap === h.Memory &&
            h.NanoCpus === arm.driverCpus * 1e9 &&
            h.PidsLimit === 128 &&
            h.Privileged === false &&
            !h.CapAdd?.length &&
            h.SecurityOpt?.includes("no-new-privileges:true") &&
            h.Init === true &&
            h.RestartPolicy?.Name === "no" &&
            h.LogConfig?.Type === "none" &&
            !Object.keys(h.PortBindings ?? {}).length &&
            !h.Devices?.length &&
            !h.PidMode &&
            !h.PublishAllPorts &&
            !Object.values(info.NetworkSettings?.Ports ?? {}).some(Boolean),
        "HEADROOM_DRIVER_ISOLATION",
    );
    sameHeadroom(h.CapDrop, ["ALL"], "HEADROOM_DRIVER_ISOLATION");
    sameHeadroom(h.Tmpfs, { "/tmp": "rw,nosuid,nodev,noexec,size=67108864" }, "HEADROOM_DRIVER_ISOLATION");
    const mounts = info.Mounts.map((m) => ({ Source: m.Source, Destination: m.Destination, RW: m.RW })).sort((a, b) =>
        a.Destination.localeCompare(b.Destination),
    );
    sameHeadroom(
        mounts,
        [
            { Source: sequence.sourceRoot, Destination: "/source", RW: false },
            { Source: sequence.nodeHome, Destination: "/node", RW: false },
            { Source: arm.directory + "/driver-evidence", Destination: "/evidence", RW: true },
        ].sort((a, b) => a.Destination.localeCompare(b.Destination)),
        "HEADROOM_DRIVER_MOUNTS",
    );
    return info.Id;
}
export function headroomTolerance(x, y, symmetric = true) {
    if (![x, y].every((v) => Number.isFinite(v) && v >= 0))
        return { status: "unknown", difference: null, budget: null };
    const difference = Math.abs(x - y),
        budget = Math.max(1, 0.1 * (symmetric ? Math.min(x, y) : x));
    return { status: difference <= budget ? "passed" : "failed", difference, budget, reference: x, candidate: y };
}
const median = (v) =>
    [...v]
        .sort((a, b) => a - b)
        .reduce((_, x, i, a) => (i === Math.floor(a.length / 2) ? (a.length % 2 ? x : (a[i - 1] + x) / 2) : _), null);
export function assessHeadroomCells(arms, globalIssues = []) {
    const cells = headroomGrid().flatMap((a) =>
        HEADROOM002.cases.map((caseId) => {
            const found = arms.find((r) => r.cellId === a.cellId);
            const c = found?.valid ? found.cells?.find((c) => c.caseId === caseId) : null;
            return { ...a, caseId, ...(c ?? {}), evidenceValid: !!c && found.valid === true };
        }),
    );
    const p99 = [];
    const compare = (kind, group, a, b) => {
        const valid = a?.evidenceValid && b?.evidenceValid && a.observations >= 1000 && b.observations >= 1000;
        p99.push({
            kind,
            ...group,
            ...(valid ? headroomTolerance(a.p99, b.p99) : { status: "unknown", difference: null, budget: null }),
        });
    };
    for (const caseId of HEADROOM002.cases)
        for (const ratePerSecond of HEADROOM002.rates) {
            for (const blockId of HEADROOM002.blocks)
                compare(
                    "cpu-sensitivity",
                    { caseId, ratePerSecond, blockId },
                    ...HEADROOM002.cpus.map((cpu) =>
                        cells.find(
                            (c) =>
                                c.caseId === caseId &&
                                c.ratePerSecond === ratePerSecond &&
                                c.blockId === blockId &&
                                c.driverCpus === cpu,
                        ),
                    ),
                );
            for (const driverCpus of HEADROOM002.cpus)
                compare(
                    "between-block-repeat",
                    { caseId, ratePerSecond, driverCpus },
                    ...HEADROOM002.blocks.map((block) =>
                        cells.find(
                            (c) =>
                                c.caseId === caseId &&
                                c.ratePerSecond === ratePerSecond &&
                                c.blockId === block &&
                                c.driverCpus === driverCpus,
                        ),
                    ),
                );
        }
    const states = [
        ...cells.map((c) => (c.evidenceValid ? c.qualityStatus : "unknown")),
        ...cells.map((c) => (c.evidenceValid ? c.pressureStatus : "unknown")),
        ...cells.flatMap((c) =>
            c.evidenceValid &&
            c.transitions?.length === 2 &&
            c.transitions[0].metric === "correctResponseMs.p50" &&
            c.transitions[1].metric === "correctResponseMs.p95"
                ? c.transitions.map((t) => t.status)
                : ["unknown", "unknown"],
        ),
        ...p99.map((p) => p.status),
    ];
    const complete =
        arms.length === 12 &&
        arms.every((a) => a.valid === true) &&
        cells.length === 36 &&
        cells.every((c) => c.evidenceValid) &&
        globalIssues.length === 0;
    const status = states.includes("failed")
        ? "failed"
        : complete && states.every((s) => s === "passed")
          ? "passed"
          : "unknown";
    return {
        status,
        limitedScopeEngineeringAccepted: status === "passed",
        plannedPhysicalArms: 12,
        plannedCaseCells: 36,
        cells,
        p99Comparisons: p99,
        measurementSteadyState: "unassessed",
        statisticalEquivalenceAccepted: false,
        empiricalFalsePositiveCertification: "optional-independent",
        inferenceAccepted: false,
        performanceAccepted: false,
    };
}
export function assessHeadroomRun(run, phaseWindows) {
    const phases = phaseWindows?.phases ?? [];
    const phaseShape = phases.length === 2 && phases[0].name === "warmup" && phases[1].name === "measurement";
    const complete =
        phaseShape &&
        phases.every(
            (p, i) =>
                p.durationMs === [30000, 120000][i] &&
                p.phaseEndedEarly === false &&
                p.windows?.length === [3, 12][i] &&
                p.windows.every((w, j) => w.index === j && w.startMs === j * 10000 && w.endMs === (j + 1) * 10000),
        );
    const pressure = phases.map((p) => {
        const x = p.driverWholePhase;
        return !Number.isFinite(x?.averageCpuCores) || !Number.isFinite(x?.eventLoopUtilization)
            ? "unknown"
            : x.averageCpuCores > 0.8 || x.eventLoopUtilization > 0.8
              ? "failed"
              : "passed";
    });
    const warm = phases.find((p) => p.name === "warmup"),
        measure = phases.find((p) => p.name === "measurement");
    const transitions = ["p50", "p95"].map((metric) => {
        const a = warm?.windows?.slice(-2),
            b = measure?.windows?.slice(0, 2),
            ws = [...(a ?? []), ...(b ?? [])];
        const usable =
            complete &&
            ws.length === 4 &&
            ws.every(
                (w) =>
                    w.metrics?.correctResponseMs?.count >= 50 && Number.isFinite(w.metrics.correctResponseMs[metric]),
            );
        return {
            metric: "correctResponseMs." + metric,
            ...(usable
                ? headroomTolerance(
                      median(a.map((w) => w.metrics.correctResponseMs[metric])),
                      median(b.map((w) => w.metrics.correctResponseMs[metric])),
                      false,
                  )
                : { status: "unknown", difference: null, budget: null }),
        };
    });
    const issues = Array.isArray(run.issues) ? run.issues : null;
    const countComplete =
        phaseShape &&
        phases.every((p) =>
            ["errors", "dropped", "late", "correct", "records"].every(
                (k) => Number.isSafeInteger(p.totals?.[k]) && p.totals[k] >= 0,
            ),
        );
    const countFailure =
        countComplete && phases.some((p) => p.totals.errors > 0 || p.totals.dropped > 0 || p.totals.late > 0);
    const knownFailure = issues?.some((x) =>
        /:(request-errors-or-wrong-answers|dropped-arrivals|request-budget-exhausted|cancelled|no-correct-results)$/.test(
            x,
        ),
    );
    const unexplained = issues === null || issues.some((x) => !/:driver-pressure-observed$/.test(x));
    const qualityStatus =
        countFailure || knownFailure || run.failed > 0 || run.dropped > 0 || run.late > 0
            ? "failed"
            : complete && countComplete && run.correct >= 1000 && !unexplained
              ? "passed"
              : "unknown";
    return {
        caseId: run.caseId,
        p99: run.correctEndToEndMs?.p99 ?? null,
        observations: run.correct,
        qualityStatus,
        pressureStatus:
            pressure.includes("failed") || issues?.some((x) => /:driver-pressure-observed$/.test(x))
                ? "failed"
                : complete && pressure.length === 2 && pressure.every((s) => s === "passed")
                  ? "passed"
                  : "unknown",
        transitions,
        wholePhaseDriver: phases.map((p) => ({
            phase: p.name,
            ...p.driverWholePhase,
            eluDerivation: "source-bound-recorded-utilization; raw active/idle unavailable",
        })),
        correctCompletedInWindowPerSecond: run.correctCompletedInWindowPerSecond,
        originalIssues: issues,
        windowCompleteness: complete ? "complete" : "unknown",
    };
}
export function headroom002Operations(arm, sequence, spec) {
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
        "org.apache.hugegraph.hgbench.driver-headroom.request=" + arm.runId,
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
        String(arm.driverCpus),
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
        "import time; time.sleep(900)",
    ];
    const inside = [docker, "exec", "--user", uid, cid, "/node/bin/node", "/source/dist/cli.js"];
    const operations = {
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
    const resourceScript =
        'import {readFileSync} from "node:fs"; import {pathToFileURL} from "node:url"; const m=await import(pathToFileURL(process.argv[1])); console.log(JSON.stringify(await m.verifyResourceEvidence(process.argv[2],JSON.parse(readFileSync(process.argv[3],"utf8")))));';
    const jvmScript =
        'import {readFileSync} from "node:fs"; import {pathToFileURL} from "node:url"; const m=await import(pathToFileURL(process.argv[1])); console.log(JSON.stringify(await m.verifyJvmEvidence(process.argv[2],process.argv[3],JSON.parse(readFileSync(process.argv[4],"utf8")))));';
    return {
        ...operations,
        "collector-plan": [
            ...cli,
            "task",
            "plan",
            dir + "/collector-task.source.json",
            "--out",
            dir + "/collector-task.json",
        ],
        "collector-start": [
            ...cli,
            "--root",
            dir + "/collector-tasks",
            "task",
            "run",
            "--plan",
            dir + "/collector-task.json",
            "--detach",
            "--request-id",
            arm.collectorRequestId,
        ],
        "collector-status": [
            ...cli,
            "--root",
            dir + "/collector-tasks",
            "task",
            "status",
            "--request-id",
            arm.collectorRequestId,
        ],
        "collector-artifacts": [
            ...cli,
            "--root",
            dir + "/collector-tasks",
            "task",
            "artifacts",
            "--id",
            "{{collector.task.id}}",
        ],
        "resource-verify": [
            sequence.runtimeNode,
            "--input-type=module",
            "-e",
            resourceScript,
            sequence.sourceRoot + "/components/http/resource-evidence.mjs",
            dir + "/collector",
            dir + "/collector-verification-context.json",
        ],
        "jvm-original": [
            sequence.hostPython,
            "-B",
            env + "/jvm-observation.py",
            "--json",
            "parse",
            "--plan",
            env + "/jvm-observation",
            "--raw",
            env + "/work/server.log",
            "--launch",
            env + "/work/jvm-launch.json",
            "--capture",
            env + "/work/jvm-capture.json",
            "--out",
            dir + "/jvm-original",
        ],
        "jvm-verify": [
            sequence.runtimeNode,
            "--input-type=module",
            "-e",
            jvmScript,
            sequence.sourceRoot + "/components/http/jvm-evidence.mjs",
            env,
            dir + "/jvm-original",
            dir + "/jvm-verification-context.json",
        ],
        "phase-windows": [
            sequence.runtimeNode,
            spec.planRoot + "/analysis/phase-windows.mjs",
            "report",
            "--input",
            evidence + "/http-plan/output",
            "--output",
            dir + "/phase-windows",
            "--window-ms",
            "10000",
            "--minimum-requests-per-window",
            "50",
            "--minimum-p99-requests",
            "1000",
        ],
    };
}
