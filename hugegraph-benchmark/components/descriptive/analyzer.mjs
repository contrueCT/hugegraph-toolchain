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

import { readFile, writeFile } from "node:fs/promises";

const args = process.argv.slice(2);
if (args.includes("--describe")) {
    process.stdout.write(
        JSON.stringify({
            protocolVersion: 1,
            id: "descriptive-analyzer",
            version: "1.0.0",
            kind: "analyzer",
            scopes: ["controller-fixture"],
            configurationSchema: { type: "object", additionalProperties: false },
        }) + "\n",
    );
} else {
    const option = (name) => args[args.indexOf(name) + 1];
    if (!args.includes("--request") || !args.includes("--output"))
        throw new Error("--request and --output are required");
    const input = JSON.parse(await readFile(option("--request"), "utf8"));
    if (input.protocolVersion !== 1 || input.kind !== "analysis" || input.scope !== "controller-fixture") {
        throw new Error("Unsupported analysis protocol");
    }
    const quantile = (items, q) => {
        const sorted = [...items].sort((a, b) => a - b);
        return sorted[Math.max(0, Math.ceil(sorted.length * q) - 1)];
    };
    const runs = input.results.map((result) => ({
        runId: result.runId,
        caseId: result.caseId,
        variantId: result.variantId,
        block: result.block,
        samples: result.samples.length,
        failed: result.validation.failed,
        p50Ms: quantile(
            result.samples.map((s) => s.durationMs),
            0.5,
        ),
        p95Ms: quantile(
            result.samples.map((s) => s.durationMs),
            0.95,
        ),
        p99Ms: quantile(
            result.samples.map((s) => s.durationMs),
            0.99,
        ),
    }));
    const failed = runs.some((run) => run.failed > 0);
    const pairs = [];
    for (const item of input.plan.cases) {
        for (let block = 0; block < input.plan.blocks; block++) {
            const select = (id) =>
                runs.find((run) => run.caseId === item.id && run.block === block && run.variantId === id);
            const a = select(input.plan.variants[0].id);
            const b = select(input.plan.variants[1].id);
            if (!a || !b) throw new Error("Incomplete paired result set");
            pairs.push({
                caseId: item.id,
                block,
                baselineP50Ms: a.p50Ms,
                candidateP50Ms: b.p50Ms,
                ratio: failed || a.p50Ms === 0 ? null : b.p50Ms / a.p50Ms,
            });
        }
    }
    const verdict = failed ? "correctness-failed" : "descriptive-only";
    const summary = {
        scope: "controller-fixture",
        verdict,
        runs,
        pairs,
        limitations: [
            "Controller self-test; no HugeGraph server or HTTP container was measured.",
            "Samples include configured fixture delays. No throughput or capacity claim.",
            "Quantiles use nearest rank per run; P99 is descriptive with this sample count.",
            "No significance test, confidence interval, equivalence or regression acceptance is implemented.",
        ],
    };
    const escape = (value) =>
        String(value).replace(
            /[&<>"']/g,
            (c) =>
                ({
                    "&": "&amp;",
                    "<": "&lt;",
                    ">": "&gt;",
                    '"': "&quot;",
                    "'": "&#39;",
                })[c],
        );
    const number = (n) => (n === null ? "not available" : n.toFixed(3));
    const rows = runs
        .map(
            (r) =>
                `<tr><td>${escape(r.caseId)}</td><td>${escape(r.variantId)}</td>` +
                `<td>${r.block + 1}</td><td>${r.samples}</td><td>${r.failed}</td><td>${number(r.p50Ms)}</td>` +
                `<td>${number(r.p95Ms)}</td><td>${number(r.p99Ms)}</td></tr>`,
        )
        .join("\n");
    const reportHtml = `<!doctype html><html lang="en"><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>hgbench · ${escape(input.plan.name)}</title>
<style>body{font:16px system-ui;max-width:1100px;margin:40px auto;padding:0 24px;color:#182b3a;background:#f6f8fb}
h1{margin-bottom:8px}header,section{background:white;padding:24px;border:1px solid #dbe2ea;border-radius:12px;margin:20px 0}
table{border-collapse:collapse;width:100%}th,td{text-align:left;border-bottom:1px solid #e0e5ec;padding:10px}
input{padding:10px;border:1px solid #becbd8;border-radius:6px;width:280px}code{overflow-wrap:anywhere}
.badge{display:inline-block;background:#fff0bc;padding:6px 12px;border-radius:20px}li{margin:8px 0}
.table{overflow:auto}</style>
<header><span class="badge">Controller self-test · ${escape(verdict)}</span>
<h1>${escape(input.plan.name)}</h1><p>Experiment: <code>${escape(input.experimentId)}</code></p>
<p>Plan SHA-256: <code>${escape(input.planDigest)}</code></p></header>
<section><h2>Scope and limitations</h2><ul>${summary.limitations.map((s) => `<li>${escape(s)}</li>`).join("")}</ul></section>
<section><h2>Measured fixture durations</h2><label>Filter case or variant <input id="filter" placeholder="sum-small"></label>
<div class="table"><table id="runs"><thead><tr><th>Case</th><th>Variant</th><th>Block</th><th>Samples</th>
<th>Wrong results</th><th>P50 ms</th><th>P95 ms</th><th>P99 ms</th></tr></thead><tbody>${rows}</tbody></table></div></section>
<section><h2>Paired observations</h2><p>Candidate / baseline P50. Descriptive only.</p><ul>${pairs
        .map((p) => `<li>${escape(p.caseId)}, block ${p.block + 1}: ${number(p.ratio)}</li>`)
        .join("")}</ul></section>
<section><h2>Reproduce and inspect</h2><p>Use <code>hgbench artifacts list --experiment ${escape(input.experimentId)}</code>
to verify checksums. The evidence directory contains plan.json, component identities, requests, raw results and logs.
Use <code>hgbench run --plan &lt;evidence-directory&gt;/plan.json</code> for a fresh experiment.</p></section>
<script>document.getElementById('filter').addEventListener('input',function(){const q=this.value.toLowerCase();
document.querySelectorAll('#runs tbody tr').forEach(r=>{r.hidden=!r.textContent.toLowerCase().includes(q)})})</script></html>`;
    await writeFile(
        option("--output"),
        JSON.stringify({
            protocolVersion: 1,
            kind: "analysis",
            scope: "controller-fixture",
            experimentId: input.experimentId,
            planDigest: input.planDigest,
            verdict,
            summary,
            reportHtml,
        }) + "\n",
    );
}
