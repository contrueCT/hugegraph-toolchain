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

const escape = (value) =>
    String(value ?? "—").replace(
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
const number = (value, scale = 1) =>
    typeof value === "number" && Number.isFinite(value)
        ? new Intl.NumberFormat("en-US", { maximumSignificantDigits: 5 }).format(value * scale)
        : "—";
const list = (items) => (items?.length ? `<ul>${items.map((x) => `<li>${escape(x)}</li>`).join("")}</ul>` : "无");
const range = (value, scale = 1) => (value ? `[${number(value.low, scale)}, ${number(value.high, scale)}]` : "—");
const json = (value) => `<pre>${escape(JSON.stringify(value ?? null, null, 2))}</pre>`;

/** Render only independently verified adapter outputs; never infer performance acceptance. */
export function html(result) {
    const { verification: v, analysis: a } = result;
    const context = result.reportContext ?? v.reportContext ?? {};
    const bindings = context.metricBindings ?? v.metricBindings ?? [];
    const present = (value) => typeof value === "string" && value.trim().length > 0;
    const experiment = [
        ["实验用途", context.purpose],
        ["比较类型", context.comparison],
    ]
        .filter(([, value]) => present(value))
        .map(([label, value]) => `${label}：<code>${escape(value)}</code>`)
        .join(" · ");
    const modes = [
        ["基线 A", context.modes?.A],
        ["候选 B", context.modes?.B],
    ]
        .filter(([, value]) => present(value))
        .map(([label, value]) => `${label} = <code>${escape(value)}</code>`)
        .join(" · ");
    const suite = context.suite;
    const suiteDetails =
        suite && typeof suite === "object" && !Array.isArray(suite) && Object.keys(suite).length
            ? `<details><summary>题集${[suite.suiteId, suite.version]
                  .filter(present)
                  .map((x) => " · " + escape(x))
                  .join("")}</summary>${json(suite)}</details>`
            : "";
    const metricRows = a.cells
        .map((cell) => {
            const binding = bindings.find((x) => x.id === cell.metric.id);
            const label = binding ? `${binding.caseId} · ${binding.loadId}` : cell.metric.id;
            const metric = binding?.valuePath ?? cell.metric.unit;
            const warnings = [...cell.issues, ...cell.warnings];
            const pairs = cell.pairs
                .map(
                    (p) =>
                        `<tr><td>${escape(p.blockId ?? p.id)}</td><td>${number(p.baseline)}</td><td>${number(p.candidate)}</td></tr>`,
                )
                .join("");
            return `<article class="metric" data-search="${escape((label + " " + metric + " " + cell.metric.id).toLowerCase())}" data-issues="${warnings.length}">
<div class="metric-title"><h3>${escape(label)}</h3><span>${escape(metric)} · ${escape(cell.metric.unit)}</span></div>
<div class="table"><table><thead><tr><th>观察 / 计划配对</th><th>基线中位数</th><th>候选中位数</th><th>相对效应</th><th>相对区间</th><th>绝对效应 / 区间</th><th>预算关系</th></tr></thead>
<tbody><tr><td>${cell.observedPairs} / ${cell.plannedPairs}</td><td>${number(cell.point?.baselineMedian)}</td><td>${number(cell.point?.candidateMedian)}</td><td>${cell.point ? number(cell.point.relativeMedian, 100) + "%" : "—"}</td><td>${cell.bootstrapIntervals ? range(cell.bootstrapIntervals.relative, 100) + "%" : "—"}</td><td>${number(cell.point?.absoluteMedian)} / ${range(cell.bootstrapIntervals?.absolute)}</td><td>${escape(cell.budgetRelation)}</td></tr></tbody></table></div>
<details><summary>问题与逐对数据 · ${warnings.length} 项</summary>${list(warnings)}<div class="table"><table><thead><tr><th>区组</th><th>基线 A</th><th>候选 B</th></tr></thead><tbody>${pairs}</tbody></table></div><p class="muted">指标 ID：${escape(cell.metric.id)}</p></details></article>`;
        })
        .join("");
    const observation = (row) =>
        `<details><summary>测量时间戳与 JVM 观测</summary>${json({ phaseClocks: row.phaseClocks ?? null, ...(row.goProcessAssociations ? { goProcessAssociations: row.goProcessAssociations } : {}), collectorClock: row.collectorClock ?? null, jvm: row.jvmObservation ?? null })}</details>`;
    const arms = v.rows
        .map(
            (row) =>
                `<tr><td>${escape(row.blockId)} / ${escape(row.arm)}<br><code>${escape(row.runId)}</code>${present(row.expectedMode) ? `<br>计划模式：<code>${escape(row.expectedMode)}</code>` : ""}</td><td>${escape(row.status)}${list(row.issues)}</td><td>${row.rawVerified === true ? "通过" : "未通过"}</td><td>${row.runtimeArtifactsConsistent === true ? "通过" : "未通过"}</td><td>${row.driverVerified === true ? "通过" : "未通过"}</td><td>${row.resourcesVerified === true ? "通过" : "未通过"}<br>${escape(row.resourceCoverage)}<details><summary>采集范围与缺失</summary>${json({ samples: row.resourceSamples ?? null, observed: row.resourceObserved ?? null, missing: row.resourceMissingMetrics ?? null })}</details>${observation(row)}</td></tr>`,
        )
        .join("");
    const roleVersion2 = context.comparisonSchemaVersion === 2;
    const qualityRows = roleVersion2
        ? v.rows
              .flatMap((row) =>
                  (row.metricQuality?.metrics ?? []).map(
                      (metric) =>
                          `<tr><td>${escape(row.blockId)} / ${escape(row.arm)}<br><code>${escape(row.runId)}</code></td><td>${escape(metric.caseId)} · ${escape(metric.loadId)}<br><code>${escape(metric.valuePath)}</code></td><td>${number(metric.value)}</td><td>${number(metric.observations)} / ${number(metric.minimumObservations)}</td><td>${escape(metric.status)}${list(metric.issues)}</td></tr>`,
                  ),
              )
              .join("")
        : "";
    const qualitySection = roleVersion2
        ? `<section><h2>Goodput 质量与描述</h2><p>本计划预声明延迟用于推断，goodput 用于质量与描述。保留每项原始正确吞吐、窗口内完成计数和最小样本要求；缺失或失败使运行无效。这里不生成 goodput 效应区间，也不据此宣称吞吐容量。</p><details><summary>完整指标角色与质量分母</summary>${json({ metricRoles: context.metricRoles, gate: v.gates?.metricQuality ?? null })}</details><div class="table"><table><thead><tr><th>区组 / 运行</th><th>题目 / 负载 / 指标</th><th>正确请求 / 秒</th><th>观察 / 最少请求</th><th>质量状态</th></tr></thead><tbody>${qualityRows}</tbody></table></div></section>`
        : "";
    return `<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta http-equiv="Content-Security-Policy" content="default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'"><title>HG-Bench HTTP 配对报告</title>
<style>:root{color-scheme:light;font-family:system-ui,-apple-system,sans-serif;color:#172d42;background:#eef2f6}body{max-width:1380px;margin:32px auto;padding:0 24px}h1{font-size:34px;margin:12px 0}h2{font-size:23px}h3{font-size:17px;margin:0}header,section{background:#fff;border:1px solid #d7e0e9;border-radius:12px;margin:18px 0;padding:24px}p{line-height:1.65}a{color:#075aa5}code,pre{font-size:12px;overflow-wrap:anywhere}pre{white-space:pre-wrap;max-height:360px;overflow:auto;background:#f3f6f9;padding:12px}.badge{display:inline-block;background:#fff0c3;color:#684a00;padding:7px 12px;border-radius:20px;font-weight:600}.cards{display:flex;gap:16px;flex-wrap:wrap}.card{flex:1;min-width:160px;border:1px solid #d7e0e9;border-radius:8px;padding:15px}.card strong{display:block;font-size:22px;margin-top:7px}.muted{color:#53697e}.table{overflow:auto}table{border-collapse:collapse;width:100%;font-size:14px}th,td{padding:11px 8px;border-bottom:1px solid #e1e7ed;text-align:left;vertical-align:top}th{color:#53697e;font-weight:600}details{margin-top:12px}summary{cursor:pointer;color:#075aa5}li{margin:5px 0;overflow-wrap:anywhere}input{padding:10px;border:1px solid #b6c4d1;border-radius:6px}input[type=search]{width:min(500px,95%)}.filters{display:flex;gap:18px;align-items:center;flex-wrap:wrap}.metric{padding:20px 0;border-top:1px solid #d7e0e9}.metric-title{display:flex;justify-content:space-between;gap:16px;flex-wrap:wrap;margin-bottom:8px}.metric-title span{font-size:13px;color:#53697e}[hidden]{display:none!important}@media(max-width:600px){body{padding:0 10px;margin:12px auto}header,section{padding:16px}h1{font-size:27px}}</style></head><body>
<header><span class="badge">功能 / 校准证据 · 尚未接受性能结论</span><h1>HTTP 配对实验报告</h1><p>按预定区组核验全部运行，再由原始记录重建每项效应。失败或缺失运行保留在报告中。</p>
${experiment ? `<p>${experiment}</p>` : ""}${modes ? `<p>预定模式：${modes}</p>` : ""}${suiteDetails}
<div class="cards"><div class="card">证据有效性<strong>${v.valid ? "通过" : "未通过"}</strong></div><div class="card">统计计算条件<strong>${v.statisticsEligible ? "满足" : "未满足"}</strong></div><div class="card">运行臂<strong>${v.rows.filter((r) => r.status === "ok").length} / ${v.rows.length} 有效</strong></div><div class="card">性能验收<strong>待校准</strong></div></div>
<p class="muted">计划 SHA-256：<code>${escape(v.planDigest)}</code><br>执行快照 SHA-256：<code>${escape(v.executionLockSha256)}</code></p>
<p><a href="analysis.json">分析 JSON</a> · <a href="verification.json">全部核验项</a> · <a href="observations.json">逐对输入</a> · <a href="evidence.lock.json">输出内容锁</a></p></header>
<section><h2>有效性与解释边界</h2><p>全局问题：${list(v.globalIssues)}</p><p>${roleVersion2 ? "正值表示延迟退化；统计族仅含预声明的 inference 指标。" : "正值表示退化；延迟和吞吐按各自方向统一符号。"}统计单位是独立运行配对。名义族置信度 ${number(a.nominalFamilyConfidence, 100)}%，共 ${a.familyStatements} 项区间声明；短功能运行可能无法估计尾延迟或不确定性。</p><p>资源采集覆盖整个工作流，包含预检、预热、测量、排空及协调成本。当前未校准采集开销、驱动余量与主机干扰，缺失指标保留原状。数值区间和预算关系仍须结合真实 A/A 与退化检出验证。</p><details><summary>核验门与剩余条件</summary>${json(v.gates)}</details></section>
<section><h2>${roleVersion2 ? "按题目和负载查看延迟推断" : "按题目和负载查看"}</h2><div class="filters"><input id="filter" type="search" aria-label="过滤题目、负载和指标" placeholder="过滤题目、负载、指标"><label><input id="issues" type="checkbox"> 只看问题或提示</label><span id="count" class="muted"></span></div>${metricRows}<p id="empty" hidden>没有匹配的指标。</p></section>${qualitySection}
<section><h2>全部运行与资源证据</h2><div class="table"><table><thead><tr><th>区组 / 运行</th><th>状态与问题</th><th>原始 HTTP</th><th>目标环境 / 模式</th><th>驱动生命周期</th><th>连续采集</th></tr></thead><tbody>${arms}</tbody></table></div></section>
<script>function filter(){const q=document.querySelector('#filter').value.toLowerCase(),only=document.querySelector('#issues').checked;let n=0;const rows=[...document.querySelectorAll('.metric')];for(const r of rows){r.hidden=!r.dataset.search.includes(q)||(only&&r.dataset.issues==='0');if(!r.hidden)n++}document.querySelector('#count').textContent=n+' / '+rows.length+' 项';document.querySelector('#empty').hidden=n!==0}document.querySelector('#filter').addEventListener('input',filter);document.querySelector('#issues').addEventListener('change',filter);filter()</script></body></html>`;
}
