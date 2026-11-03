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

import { readFile, writeFile, mkdir, lstat, open } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { createHash } from "node:crypto";
import { analyze, ALGORITHM } from "./core.mjs";

const ROOT = dirname(fileURLToPath(import.meta.url));
const hash = (raw) => createHash("sha256").update(raw).digest("hex");
const escape = (value) =>
    String(value).replace(
        /[&<>"']/g,
        (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c],
    );
const number = (value) => (value === null || value === undefined ? "—" : Number(value).toPrecision(5));

export function html(result) {
    const origin =
        result.dataOrigin === "simulated-distribution-not-a-database-benchmark"
            ? "数学模拟数据（未访问数据库）"
            : result.dataOrigin;
    const range = (v, scale = 1) => (v ? `[${number(v.low * scale)}, ${number(v.high * scale)}]` : "—");
    const rows = result.cells
        .map(
            (cell) => `<tr data-name="${escape(cell.metric.id.toLowerCase())}">
<td><strong>${escape(cell.metric.id)}</strong><br>${escape(cell.metric.unit)} · ${cell.metric.better === "lower" ? "越低越好" : "越高越好"}</td>
<td>${cell.observedPairs} / ${cell.plannedPairs}</td><td>${number(cell.point?.relativeMedian == null ? null : cell.point.relativeMedian * 100)}%</td>
<td>${range(cell.bootstrapIntervals?.relative, 100)}%</td><td>${number(cell.point?.absoluteMedian)}<br>${range(cell.bootstrapIntervals?.absolute)}</td>
<td>${escape(cell.budgetRelation)}</td><td>${cell.issues.length} 项问题 / ${cell.warnings.length} 项提示</td></tr>`,
        )
        .join("");
    return `<!doctype html><html lang="zh-CN"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="Content-Security-Policy" content="default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'">
<title>HG-Bench · 配对统计诊断</title><style>body{font:16px system-ui;max-width:1280px;margin:32px auto;padding:0 22px;background:#f5f7fa;color:#183047}
header,section{background:white;padding:24px;border:1px solid #d7e0e8;border-radius:12px;margin:20px 0}h1{margin:12px 0}code{overflow-wrap:anywhere}
.badge{display:inline-block;padding:6px 12px;border-radius:16px;background:#fff0bb;color:#714900}table{border-collapse:collapse;width:100%}
td,th{text-align:left;padding:10px;border-bottom:1px solid #e1e7ed;vertical-align:top}input{padding:9px;border:1px solid #aabccc;border-radius:6px}
.table{overflow:auto}.muted{color:#52677b}a{color:#075fa9}</style>
<header><span class="badge">尚未校准 · 不作性能验收</span><h1>运行区组的配对效应</h1>
<p>数据来源：${escape(origin)}</p>
<p>正值表示退化。相对变化使用每对运行变化比例的中位数；绝对变化也按整对计算。</p>
<p>输入 SHA-256：<code>${escape(result.inputSha256)}</code></p><p class="muted">算法 ${escape(result.algorithm)}；比较族 ${result.familyStatements} 项区间声明，名义族置信度 ${number(result.nominalFamilyConfidence * 100)}%。</p></header>
<section><p>原始证据、区组独立性、覆盖率与实际影响预算仍需验收。Bonferroni 分配不能修复单项区间覆盖率不足。
本页中的预算关系仅为数值诊断。</p><p><a href="analysis.json">完整分析 JSON</a> · <a href="input.json">冻结输入</a> · <a href="analysis.lock.json">证据清单</a></p></section>
<section><label>过滤指标 <input id="filter" placeholder="指标名称"></label><label><input type="checkbox" id="issues"> 只看存在问题或提示的指标</label>
<div class="table"><table><thead><tr><th>指标</th><th>观察/计划对数</th><th>配对中位数变化</th><th>相对区间</th><th>绝对变化/区间</th><th>预算关系</th><th>质量</th></tr></thead><tbody>${rows}</tbody></table></div></section>
<script>function filter(){const q=document.querySelector('#filter').value.toLowerCase(),bad=document.querySelector('#issues').checked;document.querySelectorAll('tbody tr').forEach(r=>{r.hidden=!r.dataset.name.includes(q)||(bad&&r.lastElementChild.textContent==='0 项问题 / 0 项提示')})}document.querySelector('#filter').addEventListener('input',filter);document.querySelector('#issues').addEventListener('change',filter)</script></html>`;
}

export async function runAnalysis(inputFile, output) {
    const stat = await lstat(inputFile);
    if (!stat.isFile() || stat.isSymbolicLink() || stat.size > 8 * 1024 * 1024)
        throw new Error("Input must be a regular JSON file of at most 8 MiB");
    const handle = await open(inputFile, "r"),
        buffer = Buffer.alloc(8 * 1024 * 1024 + 1);
    let length = 0;
    try {
        while (length < buffer.length) {
            const { bytesRead } = await handle.read(buffer, length, buffer.length - length, null);
            if (!bytesRead) break;
            length += bytesRead;
        }
    } finally {
        await handle.close();
    }
    if (length > 8 * 1024 * 1024) throw new Error("Input grew beyond 8 MiB");
    const raw = buffer.subarray(0, length),
        input = JSON.parse(raw.toString("utf8"));
    const result = {
        ...analyze(input),
        inputSha256: hash(raw),
        dataOrigin: typeof input.origin === "string" ? input.origin : "not-declared; external evidence unverified",
    };
    const root = resolve(output);
    const sources = {};
    for (const name of ["core.mjs", "analyzer.mjs"]) sources[name] = hash(await readFile(join(ROOT, name)));
    const payloads = {
        "input.json": raw,
        "analysis.json": Buffer.from(JSON.stringify(result, null, 2) + "\n"),
        "report.html": Buffer.from(html(result)),
        "implementation.json": Buffer.from(JSON.stringify({ algorithm: ALGORITHM, sources }, null, 2) + "\n"),
    };
    const totalBytes = Object.values(payloads).reduce((n, v) => n + v.length, 0);
    if (totalBytes > 64 * 1024 * 1024) throw new Error("Analysis evidence exceeds 64 MiB");
    await mkdir(root, { recursive: false, mode: 0o700 });
    for (const [name, value] of Object.entries(payloads))
        await writeFile(join(root, name), value, { flag: "wx", mode: 0o600 });
    const files = Object.entries(payloads).map(([path, raw]) => ({ path, bytes: raw.length, sha256: hash(raw) }));
    await writeFile(
        join(root, "analysis.lock.json"),
        JSON.stringify({ schemaVersion: 1, kind: "paired-analysis-evidence", files }, null, 2) + "\n",
        { flag: "wx", mode: 0o600 },
    );
    return { output: root, verdict: result.verdict, metrics: result.cells.length, performanceAccepted: false };
}

async function main() {
    if (process.argv.includes("--help")) {
        process.stdout.write(
            "Usage: node components/paired/analyzer.mjs --input NORMALIZED.json --output NEW_DIRECTORY\nReads at most 8 MiB; no database/network requests. Produces frozen JSON and interactive offline HTML.\nUses predeclared independent block pairs, percentile bootstrap and Bonferroni allocations for relative AND absolute effects.\nFails on invalid policy, duplicated run references, resource exhaustion or an existing output directory.\nAll results remain uncalibrated; raw evidence adapters and real A/A/known-regression calibration are required.\nUse --describe for the component contract. Input format: docs/paired-analysis.md.\n",
        );
        return;
    }
    if (process.argv.includes("--describe")) {
        process.stdout.write(
            JSON.stringify({
                protocolVersion: 1,
                id: "paired-analyzer",
                version: "0.1.0",
                kind: "analyzer",
                scopes: ["normalized-paired-blocks"],
                algorithm: ALGORITHM,
                inputKind: "normalized-paired-observations",
                effect: "file",
                performanceAcceptance: false,
                documentation: "docs/paired-analysis.md",
            }) + "\n",
        );
        return;
    }
    const args = process.argv.slice(2),
        options = {};
    for (let i = 0; i < args.length; i += 2) {
        if (!["--input", "--output"].includes(args[i]) || !args[i + 1] || options[args[i]])
            throw new Error("Expected unique --input and --output");
        options[args[i]] = args[i + 1];
    }
    if (!options["--input"] || !options["--output"]) throw new Error("--input and --output are required");
    const result = await runAnalysis(options["--input"], options["--output"]);
    process.stdout.write(JSON.stringify({ ok: true, kind: "paired-analysis-result", ...result }) + "\n");
}
if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href)
    main().catch((error) => {
        process.stderr.write(
            JSON.stringify({ ok: false, kind: "paired-analysis-error", message: String(error) }) + "\n",
        );
        process.exitCode = 6;
    });
