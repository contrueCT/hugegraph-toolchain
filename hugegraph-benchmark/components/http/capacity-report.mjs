/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
const escape = (v) =>
    String(v ?? "—").replace(
        /[&<>"']/g,
        (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c],
    );
const number = (v) =>
    typeof v === "number" && Number.isFinite(v)
        ? new Intl.NumberFormat("en-US", { maximumSignificantDigits: 6 }).format(v)
        : "—";
const list = (v) => (v.length ? `<ul>${v.map((s) => `<li>${escape(s)}</li>`).join("")}</ul>` : "");
function plot(points, kind) {
    const rows = points.filter(
            (p) =>
                p.measurementValid ||
                (kind === "throughput" &&
                    p.diagnosticObservedThroughput?.median !== null &&
                    p.diagnosticObservedThroughput?.median !== undefined),
        ),
        series =
            kind === "throughput"
                ? [
                      {
                          id: "correct RPS",
                          color: "#075aa5",
                          value: (p) =>
                              p.measurementValid ? p.throughput.median : p.diagnosticObservedThroughput.median,
                      },
                  ]
                : ["p50", "p95", "p99"].map((q, i) => ({
                      id: q,
                      color: ["#075aa5", "#b15a00", "#8a3d9c"][i],
                      value: (p) => p.latency[q].value,
                  }));
    const values = rows.flatMap((p) => series.map((s) => s.value(p)).filter((v) => v !== null));
    if (!values.length) return "";
    const xmax = Math.max(1, ...points.map((p) => p.ratePerSecond)),
        ymax = Math.max(1, ...values) * 1.1;
    const dots = series
        .flatMap((s) =>
            rows
                .filter((p) => s.value(p) !== null)
                .map(
                    (p) =>
                        `<circle cx="${50 + (600 * p.ratePerSecond) / xmax}" cy="${220 - (180 * s.value(p)) / ymax}" r="4" fill="${p.measurementValid ? s.color : "white"}" stroke="${p.measurementValid ? s.color : "#758698"}"><title>${escape(p.id)} · ${escape(s.id)} · offered ${number(p.ratePerSecond)} → ${number(s.value(p))}</title></circle>`,
                ),
        )
        .join("");
    return `<svg viewBox="0 0 700 265" role="img" aria-label="${kind === "throughput" ? "正确吞吐" : "延迟"}已观察点"><path d="M50 20V220H660" fill="none" stroke="#758698"/><text x="50" y="245">0</text><text x="660" y="245" text-anchor="end">${number(xmax)} offered requests/s</text><text x="55" y="16">${number(ymax)} ${kind === "throughput" ? "correct requests/s" : "ms"}</text>${dots}</svg><p>${series.map((s) => `<span style="color:${s.color}">● ${escape(s.id)}</span>`).join(" · ")}</p>`;
}
export function capacityHtml(summary) {
    const confirmation = summary.phase === "confirmation";
    const single = summary.algorithm === "capacity-curves-v2-single-run";
    const visiblePoints = confirmation ? summary.modePoints : summary.points;
    const curves = summary.curves
        .map((curve) => {
            const points = visiblePoints.filter((p) => curve.pointIds.includes(p.id));
            const rows = points
                .map((point) => {
                    const detailId = `arms-${point.id}`;
                    const armRows = point.arms
                        .map(
                            (arm) =>
                                `<tr><td><code>${escape(arm.runId)}</code><br>${escape(arm.blockId)} / ${escape(arm.arm)}</td><td>${escape(arm.status)}${list(arm.issues)}</td><td>${number(arm.correctCompletedInWindowPerSecond)}</td><td>${number(arm.latency.p50.value)} / ${number(arm.latency.p95.value)} / ${number(arm.latency.p99.value)}<br>正确观测 ${number(arm.latency.count)}</td><td><details><summary>计数、driver 与证据 JSON</summary><pre>${escape(JSON.stringify({ counts: arm.counts, rates: arm.rates, driver: arm.driver, evidence: arm.evidence, slo: arm.slo }, null, 2))}</pre></details></td></tr>`,
                        )
                        .join("");
                    return `<tr><td>${number(point.ratePerSecond)}</td><td>${escape(point.status)}<br>${point.measurementValid ? "完整有效观察" : "未满足测量条件"}${list(point.issues)}</td><td>${number(point.throughput.median)}<br>${number(point.throughput.minimum)} — ${number(point.throughput.maximum)}${!point.measurementValid && point.diagnosticObservedThroughput?.median !== null ? `<br>诊断观察 ${number(point.diagnosticObservedThroughput.median)}（无资格）` : ""}</td><td>${number(point.latency.p50.value)} / ${number(point.latency.p95.value)} / ${number(point.latency.p99.value)}</td><td>${escape(point.slo)}<br>${confirmation ? escape(point.confirmation) : "未独立确认"}</td><td>${point.arms.filter((a) => a.measurementValid).length} / ${point.expectedArms} 有效臂${point.status === "reverified" ? `<br><a href="points/${encodeURIComponent(point.evidencePointId ?? point.id)}.json">独立重建 JSON</a>` : ""}</td></tr><tr class="arm-detail-row"><td colspan="6"><details id="${escape(detailId)}"><summary>${escape(point.id)} · 全部 ${point.expectedArms} 个臂的原始观察</summary><div class="table"><table class="arm-table"><thead><tr><th>运行</th><th>质量</th><th>正确RPS</th><th>P50/P95/P99 ms</th><th>计数 / driver / 证据</th></tr></thead><tbody>${armRows}</tbody></table></div></details></td></tr>`;
                })
                .join("");
            const charts = [
                ["正确吞吐", "throughput"],
                ["延迟", "latency"],
            ]
                .map(([label, kind]) => {
                    const chart = plot(points, kind);
                    return chart ? `<div><h3>${label}</h3>${chart}</div>` : "";
                })
                .join("");
            const plots = charts
                ? `<div class="plots">${charts}</div>`
                : `<p class="muted">尚无可绘制数值，全部计划点与缺项保留在下表。</p>`;
            return `<section class="curve" data-search="${escape((curve.caseId + " " + curve.mode).toLowerCase())}"><h2>${escape(curve.caseId)} · ${escape(curve.mode)}</h2><p>最高观察正确吞吐：${number(curve.highestObserved.correctRequestsPerSecond)} requests/s · ${escape(curve.highestObserved.status)}。${confirmation ? "只使用新确认臂的模式内中位数；工程资格和SLO容量均未建立。" : "独立确认结果与容量资格均未建立。"}</p>${confirmation ? `<details><summary>探索选点来源与全部邻点</summary><pre>${escape(JSON.stringify(curve.selection, null, 2))}</pre></details>` : ""}${plots}<div class="table"><table><thead><tr><th>到达率</th><th>点状态</th><th>${single ? "单次观察正确RPS" : "正确RPS 中位数 / 范围"}</th><th>${single ? "单次观察 P50/P95/P99 ms" : "臂分位数的中位数 P50/P95/P99 ms"}</th><th>SLO / 确认</th><th>全部原始臂</th></tr></thead><tbody>${rows}</tbody></table></div></section>`;
        })
        .join("");
    return `<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta http-equiv="Content-Security-Policy" content="default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'"><title>HG-Bench ${confirmation ? "容量独立确认" : "容量探索曲线"}</title><style>body{font-family:system-ui,sans-serif;max-width:1500px;margin:28px auto;padding:0 20px;color:#172d42;background:#eef2f6}header,section{padding:22px;margin:18px 0;background:white;border:1px solid #d7e0e9;border-radius:10px}p{line-height:1.6}table{border-collapse:collapse;width:100%;font-size:13px}td,th{text-align:left;vertical-align:top;padding:9px;border-bottom:1px solid #e1e7ed}.table{overflow:auto}.arm-detail-row>td{background:#f6f8fb;padding:12px}.arm-table{table-layout:auto}.arm-table th:first-child{min-width:180px}.arm-table th:last-child{min-width:240px}.muted{color:#53697e}pre{max-height:300px;max-width:650px;overflow:auto;white-space:pre-wrap;font-size:11px}code{font-size:11px;overflow-wrap:anywhere}.plots{display:flex;flex-wrap:wrap}.plots>div{flex:1;min-width:300px}svg{width:100%;max-height:300px}svg text{font-size:10px;fill:#53697e}li{margin:4px 0}input{padding:10px;min-width:250px}a,summary{color:#075aa5}[hidden]{display:none!important}</style></head><body><header><h1>${confirmation ? "容量独立确认：逐模式正确吞吐与延迟" : "容量探索：正确吞吐—延迟曲线"}</h1><p>${summary.validPoints} / ${summary.plannedPoints} 点满足完整测量条件；全部 ${summary.expectedArms} 个计划臂均保留。${confirmation ? "任务完成不能代替独立证据。A/A使用全部8臂，A/B按实际模式各4臂，来源未知或缺臂不能确认。" : "任务完成不能代替证据有效性，探索最高点保持 unconfirmed。"}</p><p>实心点显示有效观察；灰色空心点仅显示原始正确计数可核验的诊断吞吐，不进入最高有效点候选。不插值容量边界。点为全部预声明臂的中位数，延迟是臂分位数的中位数，不跨臂汇总请求；P99 每臂不足 1000 时未知。缺项与无效点保留原值和原因。</p><p>${confirmation ? "仅描述新的独立复测观察值，失败不回填探索值；driver余量、warmup稳定性、观测成本、工程资格、统计认证及SLO容量仍未验收。" : "driver 余量、warmup 稳定性、观测成本和独立确认均未验收；没有 study-wide 统计认证或 SLO 容量结论。"}</p><p><a href="report.json">完整曲线 JSON</a> · <a href="execution-set.json">输入清单</a> · <a href="evidence.lock.json">内容锁</a></p><p>计划摘要：<code>${escape(summary.planDigest)}</code></p><label>过滤题目或模式 <input id="filter" type="search"></label></header>${curves}<script>document.querySelector('#filter').addEventListener('input',function(){const q=this.value.toLowerCase();for(const row of document.querySelectorAll('.curve'))row.hidden=!row.dataset.search.includes(q)})</script></body></html>`;
}
