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

import { readFile, mkdir, writeFile, open } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { performance } from "node:perf_hooks";
import { createHash } from "node:crypto";
import { analyze, ALGORITHM, Sampler, balancedOrders, valueHash } from "./core.mjs";

const ROOT = dirname(fileURLToPath(import.meta.url));
const assert = (value, message) => {
    if (!value) throw new Error(message);
};
export function wilson(successes, trials) {
    assert(
        Number.isSafeInteger(trials) &&
            trials > 0 &&
            Number.isSafeInteger(successes) &&
            successes >= 0 &&
            successes <= trials,
        "Invalid binomial observations",
    );
    const z = 1.959963984540054,
        p = successes / trials,
        denominator = 1 + (z * z) / trials;
    const center = (p + (z * z) / (2 * trials)) / denominator;
    const half = (z * Math.sqrt((p * (1 - p)) / trials + (z * z) / (4 * trials * trials))) / denominator;
    return { successes, trials, rate: p, wilson95: [Math.max(0, center - half), Math.min(1, center + half)] };
}
function normal(random) {
    return Math.sqrt(-2 * Math.log(random.uniform())) * Math.cos(2 * Math.PI * random.uniform());
}

export function simulatedInput({ seed, model, trial, pairs, replicates, shift, sigma = 0.12 }) {
    const random = new Sampler(`${seed}:data:${model}:${trial}:${shift}`);
    const ids = Array.from({ length: pairs }, (_, i) => `b${String(i + 1).padStart(4, "0")}`);
    const metrics = [100, 200].map((baseline, i) => ({
        id: `latency-${i + 1}`,
        unit: "ms",
        better: "lower",
        relativeBudget: 0.05,
        absoluteBudget: baseline * 0.05,
        minimumObservations: 10000,
    }));
    const policy = {
        schemaVersion: 1,
        kind: "paired-analysis-policy",
        id: `p0-${model}-${trial}`,
        algorithm: ALGORITHM,
        blockIds: ids,
        metrics,
        familyAlpha: 0.05,
        minimumPairs: 20,
        bootstrapReplicates: replicates,
        minimumTailDraws: 20,
        maxWork: 1000000000,
        maxMilliseconds: 60000,
        resampleSeed: `${seed}:resample:${model}:${trial}:${shift}`,
        orderSeed: `${seed}:order:${trial}`,
        baselineDigest: valueHash("simulated-baseline"),
        candidateDigest: valueHash("simulated-candidate"),
        contexts: Object.fromEntries(ids.map((id) => [id, valueHash({ model, id })])),
    };
    const orders = balancedOrders(ids, policy.orderSeed),
        initial = normal(random);
    const state = [0, 1].map(() => Math.sqrt(0.7) * initial + Math.sqrt(0.3) * normal(random));
    const blocks = ids.map((id) => {
        const common = normal(random),
            values = [];
        const divisor =
            model === "iid-t3" ? Math.sqrt(normal(random) ** 2 + normal(random) ** 2 + normal(random) ** 2) : 1;
        for (let i = 0; i < 2; i++) {
            let z;
            const innovation = Math.sqrt(0.7) * common + Math.sqrt(0.3) * normal(random);
            if (model === "iid-t3") {
                z = innovation / divisor;
            } else {
                z = model === "ar1-normal" ? (state[i] = 0.8 * state[i] + 0.6 * innovation) : innovation;
            }
            const effect = Math.max(-0.9, shift + sigma * z); // Positive measurement; lower clipping preserves the population median shift.
            values.push((i ? 200 : 100) * (1 + effect));
        }
        const arm = (name, variantDigest, candidate) => ({
            runId: `${id}-${name}`,
            evidenceSha256: valueHash({ seed, model, trial, shift, id, name }),
            variantDigest,
            conditionsDigest: policy.contexts[id],
            status: "ok",
            values: Object.fromEntries(
                metrics.map((m, i) => [m.id, { value: candidate ? values[i] : i ? 200 : 100, observations: 20000 }]),
            ),
        });
        return {
            id,
            observedOrder: orders[id],
            baseline: arm("A", policy.baselineDigest, false),
            candidate: arm("B", policy.candidateDigest, true),
        };
    });
    return {
        schemaVersion: 1,
        kind: "normalized-paired-observations",
        origin: "simulated-distribution-not-a-database-benchmark",
        policy,
        blocks,
    };
}

export function checkModels(config) {
    assert(config?.schemaVersion === 1 && config.kind === "paired-model-check", "Invalid model-check config");
    assert(
        Number.isSafeInteger(config.trials) && config.trials >= 20 && config.trials <= 2000,
        "Trials must be 20..2000",
    );
    assert(Number.isSafeInteger(config.pairs) && config.pairs >= 20 && config.pairs <= 100, "Pairs must be 20..100");
    assert(
        Number.isSafeInteger(config.replicates) && config.replicates >= 4000 && config.replicates <= 20000,
        "Replicates must be 4000..20000",
    );
    assert(
        Number.isSafeInteger(config.maxMilliseconds) &&
            config.maxMilliseconds >= 1000 &&
            config.maxMilliseconds <= 600000,
        "Invalid model-check deadline",
    );
    assert(
        typeof config.seed === "string" && config.seed.length > 0 && config.seed.length <= 40,
        "Invalid model-check seed",
    );
    const started = performance.now(),
        models = [];
    for (const model of ["iid-normal", "iid-t3", "ar1-normal"]) {
        for (const shift of [0, 0.2]) {
            let covered = 0,
                referenceCovered = 0,
                anyAbove = 0,
                allAbove = 0;
            const trials = [];
            for (let trial = 0; trial < config.trials; trial++) {
                assert(performance.now() - started <= config.maxMilliseconds, "Whole model-check deadline exhausted");
                const input = simulatedInput({
                    seed: config.seed,
                    model,
                    trial,
                    pairs: config.pairs,
                    replicates: config.replicates,
                    shift,
                });
                const result = analyze(input);
                assert(performance.now() - started <= config.maxMilliseconds, "Whole model-check deadline exhausted");
                assert(
                    result.cells.every((c) => c.bootstrapIntervals && c.issues.length === 0),
                    "Simulation unexpectedly lacked intervals",
                );
                const contains = (x, truth) => x.low <= truth && truth <= x.high;
                const familyCovered = result.cells.every(
                    (cell, i) =>
                        contains(cell.bootstrapIntervals.relative, shift) &&
                        contains(cell.bootstrapIntervals.absolute, shift * (i ? 200 : 100)),
                );
                const signCovered = result.cells.every((cell, i) => {
                    const reference = cell.orderStatisticReference;
                    return (
                        (!reference.relative.finite || contains(reference.relative, shift)) &&
                        (!reference.absolute.finite || contains(reference.absolute, shift * (i ? 200 : 100)))
                    );
                });
                const above = result.cells.map((c) => c.budgetRelation === "above-both-budgets");
                covered += Number(familyCovered);
                referenceCovered += Number(signCovered);
                anyAbove += Number(above.some(Boolean));
                allAbove += Number(above.every(Boolean));
                trials.push({
                    trial,
                    inputValueSha256: result.inputValueSha256,
                    familyCovered,
                    orderStatisticFamilyCovered: signCovered,
                    intervals: result.cells.map((c) => ({
                        relative: c.bootstrapIntervals.relative,
                        absolute: c.bootstrapIntervals.absolute,
                        relation: c.budgetRelation,
                        bootstrapSha256: c.bootstrapIntervals.bootstrapDistributionSha256,
                    })),
                });
            }
            models.push({
                model,
                populationRelativeMedian: shift,
                independentBlocks: model !== "ar1-normal",
                pairs: config.pairs,
                familyCoverage: wilson(covered, config.trials),
                orderStatisticFamilyCoverage: wilson(referenceCovered, config.trials),
                anyAboveBothBudgets: wilson(anyAbove, config.trials),
                allAboveBothBudgets: wilson(allAbove, config.trials),
                trials,
            });
        }
    }
    return {
        schemaVersion: 1,
        kind: "paired-model-check-result",
        config,
        algorithm: ALGORITHM,
        models,
        simulation: true,
        databaseExecuted: false,
        performanceAccepted: false,
        calibrationAccepted: false,
        limitations: [
            "Model-conditioned diagnostics, not real A/A or known-product-regression evidence.",
            "AR(1) intentionally violates the independent-block assumption; neither interval method claims coverage there.",
            "Wilson intervals describe Monte Carlo sampling uncertainty; models do not represent every real workload.",
            "Normal/t3 measurements are lower-clipped at a relative effect of -0.9; the known population median is preserved.",
        ],
    };
}

async function main() {
    if (process.argv.includes("--help")) {
        process.stdout.write(
            "Usage: node components/paired/model-check.mjs --config CONFIG.json --output NEW_DIRECTORY\nSimulates IID normal, IID t(3), and correlated AR(1) block effects with known medians. No graph/database traffic.\nChecks family coverage and practical-budget detection under null and +20% shifts; reports binomial uncertainty.\nDoes not accept calibration or performance results. Explicit trials/pairs/replicates/time budget required.\n",
        );
        return;
    }
    const args = process.argv.slice(2),
        options = {};
    for (let i = 0; i < args.length; i += 2) {
        assert(
            ["--config", "--output"].includes(args[i]) && args[i + 1] && !options[args[i]],
            "Expected unique --config/--output",
        );
        options[args[i]] = args[i + 1];
    }
    assert(options["--config"] && options["--output"], "--config and --output required");
    const handle = await open(options["--config"], "r");
    let raw;
    try {
        const stat = await handle.stat();
        assert(stat.isFile() && stat.size <= 16384, "Model config must be a regular file of at most 16 KiB");
        const buffer = Buffer.alloc(16385);
        let size = 0;
        while (size < buffer.length) {
            const { bytesRead } = await handle.read(buffer, size, buffer.length - size, null);
            if (!bytesRead) break;
            size += bytesRead;
        }
        assert(size <= 16384, "Model config exceeds 16 KiB");
        raw = buffer.subarray(0, size);
    } finally {
        await handle.close();
    }
    const config = JSON.parse(raw),
        result = checkModels(config),
        out = resolve(options["--output"]);
    const report = JSON.stringify(result, null, 2) + "\n";
    assert(Buffer.byteLength(report) <= 32 * 1024 * 1024, "Model evidence exceeds 32 MiB");
    const digest = (raw) => createHash("sha256").update(raw).digest("hex"),
        sourceHashes = {};
    for (const name of ["core.mjs", "model-check.mjs"]) sourceHashes[name] = digest(await readFile(join(ROOT, name)));
    const files = {
        "config.json": raw,
        "models.json": Buffer.from(report),
        "implementation.json": Buffer.from(JSON.stringify({ algorithm: ALGORITHM, sourceHashes }, null, 2) + "\n"),
    };
    await mkdir(out, { recursive: false, mode: 0o700 });
    for (const [name, body] of Object.entries(files))
        await writeFile(join(out, name), body, { flag: "wx", mode: 0o600 });
    await writeFile(
        join(out, "models.lock.json"),
        JSON.stringify(
            {
                schemaVersion: 1,
                kind: "paired-model-check-evidence",
                files: Object.entries(files).map(([path, body]) => ({
                    path,
                    bytes: body.length,
                    sha256: digest(body),
                })),
            },
            null,
            2,
        ) + "\n",
        { flag: "wx", mode: 0o600 },
    );
    process.stdout.write(
        JSON.stringify({ ok: true, output: out, models: result.models.length, calibrationAccepted: false }) + "\n",
    );
}
if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href)
    main().catch((error) => {
        process.stderr.write(
            JSON.stringify({ ok: false, kind: "paired-model-check-error", message: String(error) }) + "\n",
        );
        process.exitCode = 6;
    });
