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

import { mkdir, writeFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { valueDigest } from "./common.mjs";
import { buildInfo } from "./build-info.mjs";

export const METRICS = [
    {
        id: "throughput",
        label: "正确完成吞吐",
        unit: "次/s",
        direction: "越高越好",
        value: (r) => r.correctCompletedInWindowPerSecond,
    },
    { id: "p50", label: "响应 P50", unit: "ms", direction: "越低越好", value: (r) => r.correctResponseMs.p50 },
    { id: "p95", label: "响应 P95", unit: "ms", direction: "越低越好", value: (r) => r.correctResponseMs.p95 },
    {
        id: "p99",
        label: "响应 P99",
        unit: "ms",
        direction: "越低越好；至少 1000 个正确样本",
        value: (r) => (r.correctResponseMs.count >= 1000 ? r.correctResponseMs.p99 : null),
    },
    {
        id: "errors",
        label: "请求错误比例",
        unit: "% sent",
        direction: "越低越好",
        value: (r) => (r.sent ? (100 * r.failed) / r.sent : null),
    },
    {
        id: "dropped",
        label: "未发送比例",
        unit: "% scheduled",
        direction: "越低越好",
        value: (r) => (r.scheduled ? (100 * r.dropped) / r.scheduled : null),
    },
    {
        id: "late",
        label: "迟到比例",
        unit: "% scheduled",
        direction: "与其它计数可能重叠",
        value: (r) => (r.scheduled ? (100 * r.late) / r.scheduled : null),
    },
    {
        id: "validation",
        label: "驱动校验 P50",
        unit: "ms",
        direction: "单独计时，仍消耗驱动资源",
        value: (r) => r.validationMs.p50,
    },
];
const COLORS = ["#2878c5", "#099b91", "#8061c8", "#df8925", "#cf5d86", "#567786", "#8b8430", "#5a8d42"];
export const escape = (x) =>
    String(x ?? "—").replace(
        /[&<>"']/g,
        (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c],
    );
const n = (x) => (typeof x === "number" && Number.isFinite(x) ? x.toFixed(3).replace(/\.?0+$/, "") || "0" : "—");
const median = (xs) => {
    const a = [...xs].sort((x, y) => x - y),
        m = Math.floor(a.length / 2);
    return a.length ? (a.length % 2 ? a[m] : (a[m - 1] + a[m]) / 2) : null;
};
function stats(values) {
    const valid = values.filter((x) => typeof x === "number" && Number.isFinite(x));
    return {
        count: valid.length,
        total: values.length,
        median: median(valid),
        min: valid.length ? Math.min(...valid) : null,
        max: valid.length ? Math.max(...valid) : null,
        values,
    };
}
function semantics(p, c, l) {
    const { id: ignoredCase, ...question } = c,
        { id: ignoredLoad, ...load } = l;
    return {
        question,
        ...(p.graph ? { track: "graph", graph: p.graph } : {}),
        load,
        encoding: p.responseEncoding,
        warmupMs: p.warmupMs,
        durationMs: p.durationMs,
        requestTimeoutMs: p.requestTimeoutMs,
        drainMs: p.drainMs,
        maxResponseBytes: p.maxResponseBytes,
        maxRequestsPerPhase: p.maxRequestsPerPhase,
        maxTraceBytes: p.maxTraceBytes,
        serviceChecks: p.serviceChecks,
        engine: p.executor?.engine ?? "node-http",
        runner: p.executor?.binary?.sha256 ?? null,
        driver: p.executor
            ? {
                  gomaxprocs: p.executor.gomaxprocs,
                  cpuQuotaCores: p.executor.cpuQuotaCores,
                  maxRssBytes: p.executor.maxRssBytes,
                  maxPendingBytes: p.executor.maxPendingBytes,
                  hostIdentityGate: p.executor.hostIdentityGate ?? false,
              }
            : null,
    };
}
function eventsFor(events, input, row) {
    return events
        .filter((e) => {
            if (
                e.inputId !== input.id ||
                (e.caseId && e.caseId !== row.caseId) ||
                (e.loadId && e.loadId !== row.loadId) ||
                (e.runId && e.runId !== row.id)
            )
                return false;
            if (!e.startUtc) return true;
            const a = Date.parse(row.diagnostics?.startedAt),
                b = Date.parse(row.diagnostics?.finishedAt);
            // Missing anchors remain visible as input-level notes; never invent a clock mapping.
            return !Number.isFinite(a + b) || (Date.parse(e.startUtc) <= b && Date.parse(e.endUtc ?? e.startUtc) >= a);
        })
        .map((e) => ({
            ...e,
            association: e.startUtc
                ? "diagnostic-UTC; includes warmup/drain; not causal proof"
                : "operator-supplied annotation",
        }));
}
export async function buildView(inputs, options = {}) {
    const groups = [...new Map(inputs.map((i) => [i.group, { id: i.group, label: i.label ?? i.group }])).values()].map(
        (g, i) => ({ ...g, color: COLORS[i % COLORS.length] }),
    );
    const baseline = options.baseline ?? groups[0]?.id;
    if (!groups.some((g) => g.id === baseline))
        throw Object.assign(new Error("Baseline must name an input group"), { code: "COMPARE_BASELINE" });
    const cells = new Map();
    for (const input of inputs) {
        const p = input.analysis.profile;
        for (const c of p.cases)
            for (const l of p.loads) {
                const config = {
                        ...semantics(p, c, l),
                        timing: input.analysis.timing,
                        runnerIdentities: [
                            ...new Map(
                                input.analysis.runs.map((r) => [
                                    valueDigest(r.diagnostics?.runnerIdentity ?? null),
                                    r.diagnostics?.runnerIdentity ?? null,
                                ]),
                            ).entries(),
                        ]
                            .sort()
                            .map((x) => x[1]),
                    },
                    fingerprint = valueDigest(config);
                const key = valueDigest([c.id, l.id, fingerprint]).slice(0, 16);
                if (!cells.has(key))
                    cells.set(key, { id: key, caseId: c.id, loadId: l.id, config, fingerprint, groups: [] });
                const cell = cells.get(key);
                let group = cell.groups.find((g) => g.id === input.group);
                if (!group) {
                    group = { id: input.group, runs: [], expected: 0 };
                    cell.groups.push(group);
                }
                group.expected += p.repeats;
                for (const r of input.analysis.runs.filter((r) => r.caseId === c.id && r.loadId === l.id)) {
                    const windows = input.windows?.runs.find((w) => w.id === r.id);
                    group.runs.push({
                        ...r,
                        inputId: input.id,
                        round: input.round ?? null,
                        events: eventsFor(options.events ?? [], input, r),
                        ...(windows
                            ? {
                                  windows: windows.phases.map((phase) => ({
                                      name: phase.name,
                                      durationMs: phase.durationMs,
                                      windows: phase.windows.map((w) => ({
                                          startMs: w.startMs,
                                          endMs: w.endMs,
                                          counts: w.counts,
                                          p50: w.metrics.correctResponseMs.p50,
                                          p95: w.metrics.correctResponseMs.p95,
                                          samples: w.metrics.correctResponseMs.count,
                                      })),
                                      policy: input.windows.policy,
                                      drain: phase.drain,
                                  })),
                              }
                            : {}),
                    });
                }
            }
    }
    const rows = [...cells.values()];
    for (const cell of rows) {
        cell.alignment = rows.some(
            (c) => c.caseId === cell.caseId && c.loadId === cell.loadId && c.fingerprint !== cell.fingerprint,
        )
            ? "different-settings"
            : "matched-workload";
        cell.groups = groups.map((g) => {
            const found = cell.groups.find((x) => x.id === g.id) ?? { id: g.id, runs: [], expected: 0 };
            return {
                ...found,
                presentInProfile: found.expected > 0,
                missing: Math.max(0, found.expected - found.runs.length),
                metrics: Object.fromEntries(METRICS.map((m) => [m.id, stats(found.runs.map(m.value))])),
            };
        });
    }
    return {
        schemaVersion: 1,
        kind: "http-descriptive-comparison",
        reportVersion: 2,
        title: options.title ?? "HG-Bench HTTP 对比",
        reporter: await buildInfo(),
        baseline,
        groups,
        cells: rows,
        performanceAccepted: false,
        inferenceAccepted: false,
        environment: "not-independently-verified",
        changedFactors: options.changedFactors ?? ["target.build"],
        events: options.events ?? [],
        inputs: inputs.map((i) => ({
            id: i.id,
            group: i.group,
            round: i.round ?? null,
            rawManifestSha256: i.analysis.rawManifestSha256,
            target: i.analysis.target,
            gates: i.analysis.gates,
            verdict: i.analysis.verdict,
            benchmark: i.analysis.benchmark ?? null,
            plannedRuns:
                i.analysis.execution?.plannedRuns ??
                i.analysis.profile.cases.length * i.analysis.profile.loads.length * i.analysis.profile.repeats,
            verifiedRuns: i.analysis.runs.length,
            timing: i.analysis.timing,
        })),
        definitions: {
            aggregation:
                "Median of per-run metrics; min/max are observed run ranges, not confidence intervals. No pooled request percentiles.",
            throughput: "Correct responses completed within the measurement window. Offered rate is not peak capacity.",
            errors: "failed / sent; dropped / scheduled; late overlaps other counts",
            p99: "Below 1000 correct samples, keep observed P99 in details but omit chart ranking; threshold does not establish reliable tails.",
            events: "Operator annotations and diagnostic clock association only; no automatic exclusions or causal attribution.",
        },
    };
}
function shorten(value, maximum) {
    const chars = [...String(value)];
    let used = 0;
    for (let i = 0; i < chars.length; i++) {
        used += chars[i].codePointAt(0) > 255 ? 2 : 1;
        if (used > maximum) return chars.slice(0, i).join("") + "…";
    }
    return String(value);
}
function text(x, y, value, size = 16, color = "#24394f", anchor = "start") {
    return `<text x="${x}" y="${y}" font-size="${size}" fill="${color}" text-anchor="${anchor}">${escape(value)}</text>`;
}
function frame(title, subtitle, body, height, footer) {
    return `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 960 ${height}" width="960" height="${height}" role="img"><title>${escape(title)}</title><desc>${escape(subtitle + "; " + footer)}</desc><rect width="960" height="${height}" fill="white"/><g font-family="system-ui,-apple-system,'PingFang SC','Noto Sans CJK SC',sans-serif">${text(26, 36, shorten(title, 78), 23)}${text(26, 64, shorten(subtitle, 125), 14, "#63758b")}${body}${text(26, height - 18, shorten(footer, 132), 13, "#63758b")}</g></svg>`;
}
export function barSvg(view, cell, metric, baseline = null) {
    const baseGroup = cell.groups.find((g) => g.id === baseline);
    const base = baseGroup?.metrics[metric.id];
    const values = cell.groups.map((g) => {
        const stat = g.metrics[metric.id],
            partial = stat.count < Math.max(stat.total, g.expected);
        const delta = baseline !== null;
        const value = delta
            ? base?.median > 0 &&
              stat.median !== null &&
              base.count === Math.max(base.total, baseGroup.expected) &&
              !partial
                ? (stat.median / base.median - 1) * 100
                : null
            : stat.median;
        return { g, stat, partial, value };
    });
    const delta = baseline !== null,
        height = 148 + view.groups.length * 64,
        x0 = 230,
        width = 520;
    const offered =
        !delta && metric.id === "throughput" && cell.config.load.model === "open"
            ? cell.config.load.ratePerSecond
            : null;
    const bound =
        Math.max(1, ...values.map((v) => Math.abs(delta ? (v.value ?? 0) : (v.stat.max ?? 0))), offered ?? 0) * 1.08;
    const scale = (v) => (delta ? x0 + width / 2 + (v / bound) * (width / 2) : x0 + (v / bound) * width);
    let body = "";
    for (let i = 0; i <= 4; i++) {
        const v = delta ? -bound + (i * bound) / 2 : (i * bound) / 4,
            x = scale(v);
        body +=
            `<line x1="${x}" x2="${x}" y1="90" y2="${height - 65}" stroke="#e2e9f1"/>` +
            text(x, height - 43, n(v), 12, "#63758b", "middle");
    }
    if (offered !== null)
        body +=
            `<line x1="${scale(offered)}" x2="${scale(offered)}" y1="87" y2="${height - 65}" stroke="#63758b" stroke-dasharray="4 4"/>` +
            text(scale(offered), 84, `设定 ${n(offered)}`, 12, "#63758b", "middle");
    values.forEach(({ g, stat, partial, value }, i) => {
        const style = view.groups.find((v) => v.id === g.id),
            y = 115 + i * 64;
        const bad = g.runs.some((r) => r.issues.length || r.events?.length) || partial || !g.runs.length;
        body += text(x0 - 16, y + 5, shorten(style.label, 23), 16, "#24394f", "end");
        if (value !== null) {
            const start = delta ? Math.min(scale(0), scale(value)) : x0;
            body += `<rect x="${start}" y="${y - 13}" width="${delta ? Math.abs(scale(value) - scale(0)) : scale(value) - x0}" height="26" fill="${style.color}"${bad ? ' stroke="#b76e15" stroke-width="2"' : ""}/>`;
            if (!delta) {
                body += `<path d="M${scale(stat.min)},${y}H${scale(stat.max)}M${scale(stat.min)},${y - 7}V${y + 7}M${scale(stat.max)},${y - 7}V${y + 7}" stroke="#34495d" fill="none"/>`;
                stat.values.forEach((v) => {
                    if (v !== null) body += `<circle cx="${scale(v)}" cy="${y}" r="3" fill="white" stroke="#34495d"/>`;
                });
            }
        }
        body += text(
            918,
            y + 5,
            value === null ? "N/A" : `${delta && value > 0 ? "+" : ""}${n(value)}${delta ? "%" : ""}${bad ? " ⚠" : ""}`,
            17,
            "#24394f",
            "end",
        );
        body += text(
            x0,
            y + 30,
            `运行 ${g.runs.length}/${g.expected || "缺失"} · 有值 ${stat.count}${partial ? " · 含缺失/样本不足" : ""}`,
            12,
            "#63758b",
        );
    });
    const title = `${cell.caseId} / ${cell.loadId} · ${metric.label}${delta ? "变化" : ""}`;
    return frame(
        title,
        delta
            ? `相对 ${view.groups.find((g) => g.id === baseline).label} · (候选/基线−1)×100% · ${metric.direction}`
            : `${metric.unit} · ${metric.direction} · 描述性结果`,
        body,
        height,
        delta
            ? "⚠ 保留质量/事件/缺项标记；数值差异不表示统计显著。"
            : "柱：运行级中位数；细线：最小–最大；点：逐次记录。范围线不是置信区间。",
    );
}
function windowSvg(cell, run, phase) {
    const height = 330,
        left = 70,
        width = 830,
        max = Math.max(1, ...phase.windows.flatMap((w) => [w.p50 ?? 0, w.p95 ?? 0]));
    let body = "";
    for (let i = 0; i < 5; i++) {
        const y = 95 + i * 35;
        body +=
            `<line x1="${left}" x2="900" y1="${y}" y2="${y}" stroke="#e2e9f1"/>` +
            text(60, y + 5, n(max * (1 - i / 4)), 12, "#63758b", "end");
    }
    for (const [key, color] of [
        ["p50", "#2878c5"],
        ["p95", "#df8925"],
    ]) {
        let prev = null;
        for (const w of phase.windows) {
            if (w[key] === null) {
                prev = null;
                continue;
            }
            const x = left + ((w.startMs + w.endMs) / 2 / phase.durationMs) * width,
                y = 235 - (w[key] / max) * 140;
            if (prev)
                body += `<path d="M${prev[0]},${prev[1]}L${x},${y}" fill="none" stroke="${color}" stroke-width="2"/>`;
            body += `<circle cx="${x}" cy="${y}" r="3" fill="${color}"><title>${escape(`${w.startMs}–${w.endMs}ms / n=${w.samples}; 错误=${w.counts.errors}, 未发送=${w.counts.dropped}`)}</title></circle>`;
            prev = [x, y];
        }
    }
    body +=
        text(left, 261, "0 s", 13, "#63758b") + text(900, 261, `${n(phase.durationMs / 1000)} s`, 13, "#63758b", "end");
    if (!phase.windows.some((w) => w.p50 !== null))
        body += text(480, 167, "此窗口样本不足，未绘制分位数", 18, "#8a641f", "middle");
    return frame(
        `${cell.caseId} / ${cell.loadId} · ${phase.name} 时间窗`,
        `${run.inputId}/${run.id} · P50 蓝 / P95 橙 · 响应 ms`,
        body,
        height,
        `窗宽 ${phase.policy.windowMs}ms，最少 ${phase.policy.minimumRequestsPerWindow} 样本；unknown 留空。固定预热不证明稳态。`,
    );
}
function curveSvg(view, cells) {
    const points = view.groups.map((g) => ({
        g,
        rows: cells
            .map((c) => ({ c, s: c.groups.find((x) => x.id === g.id) }))
            .filter((x) => x.s.metrics.throughput.median !== null && x.s.metrics.p50.median !== null)
            .sort(
                (a, b) =>
                    (a.c.config.load.ratePerSecond ?? a.c.config.load.concurrency) -
                    (b.c.config.load.ratePerSecond ?? b.c.config.load.concurrency),
            ),
    }));
    const maxX = Math.max(1, ...points.flatMap((x) => x.rows.map((r) => r.s.metrics.throughput.median))) * 1.1,
        maxY = Math.max(1, ...points.flatMap((x) => x.rows.map((r) => r.s.metrics.p50.median))) * 1.1;
    let body = "";
    for (let i = 0; i <= 4; i++) {
        const x = 85 + i * 200,
            y = 295 - i * 48;
        body +=
            `<line x1="85" x2="885" y1="${y}" y2="${y}" stroke="#e2e9f1"/>` +
            text(x, 320, n((maxX * i) / 4), 12, "#63758b", "middle") +
            text(73, y + 5, n((maxY * i) / 4), 12, "#63758b", "end");
    }
    for (const { g, rows } of points) {
        let prev = null;
        for (const { c, s } of rows) {
            const x = 85 + (s.metrics.throughput.median / maxX) * 800,
                y = 295 - (s.metrics.p50.median / maxY) * 192;
            if (prev)
                body += `<path d="M${prev[0]},${prev[1]}L${x},${y}" fill="none" stroke="${g.color}" stroke-width="2"/>`;
            body += `<circle cx="${x}" cy="${y}" r="5" fill="${g.color}" stroke="${s.runs.some((r) => r.issues.length || r.events?.length) ? "#b76e15" : g.color}"><title>${escape(`${g.label} / ${c.loadId}: ${n(s.metrics.throughput.median)} 次/s, ${n(s.metrics.p50.median)}ms`)}</title></circle>`;
            prev = [x, y];
        }
    }
    points.forEach(({ g }, i) => {
        body += text(85 + (i % 3) * 280, 350 + Math.floor(i / 3) * 24, shorten(g.label, 35), 14, g.color);
    });
    return frame(
        `${cells[0].caseId} · 吞吐—延迟观察点`,
        "横轴：正确完成 次/s；纵轴：响应 P50 ms；按设定负载顺序连接",
        body,
        395 + (Math.ceil(view.groups.length / 3) - 1) * 24,
        "仅连接已测负载点；单次观察、轮次中位数和错误均保留，不推断峰值容量或 SLO。",
    );
}
const csv = (rows) =>
    rows
        .map((row) =>
            row
                .map((x) => {
                    let s = String(x ?? "");
                    if (/^[=+@\-]/.test(s) && typeof x !== "number") s = "'" + s;
                    return '"' + s.replace(/"/g, '""') + '"';
                })
                .join(","),
        )
        .join("\n") + "\n";
export function renderBundle(view) {
    const units = view.cells.flatMap((c) => c.groups.flatMap((g) => g.runs));
    const windowCount = units.reduce((sum, r) => sum + (r.windows ?? []).reduce((n, p) => n + p.windows.length, 0), 0);
    const estimatedCharts =
        view.cells.length * METRICS.length * (view.groups.length > 1 ? 1 + view.groups.length : 1) +
        units.reduce((n, r) => n + (r.windows?.length ?? 0), 0) +
        view.cells.length;
    if (view.cells.length > 512 || units.length > 8192 || windowCount > 32768 || estimatedCharts > 4096)
        throw Object.assign(
            new Error("Report exceeds 512 cells, 8192 runs, 32768 windows or 4096 charts; split inputs"),
            {
                code: "REPORT_TOO_LARGE",
            },
        );
    const charts = new Map();
    const figure = (id, svg, attributes = "") => {
        charts.set(`charts/${id}.svg`, svg);
        return `<figure ${attributes}><div class="export"><a href="charts/${id}.svg" download="${id}.svg" data-export="${id}.svg">导出此图 SVG</a></div>${svg}</figure>`;
    };
    const totals = view.cells
        .flatMap((c) => c.groups.flatMap((g) => g.runs))
        .reduce(
            (a, r) => {
                for (const k of ["scheduled", "sent", "correct", "failed", "dropped", "late"]) a[k] += r[k];
                return a;
            },
            { scheduled: 0, sent: 0, correct: 0, failed: 0, dropped: 0, late: 0 },
        );
    let body = "";
    for (const cell of view.cells) {
        const warning =
            cell.alignment !== "matched-workload" ||
            cell.groups.some(
                (g) => g.missing || !g.runs.length || g.runs.some((r) => r.issues.length || r.events.length),
            );
        body += `<section class="cell" data-case="${escape(cell.caseId)}" data-load="${escape(cell.loadId)}"><h2>${escape(cell.caseId)} <small>${escape(cell.loadId)} · ${cell.fingerprint.slice(0, 8)}</small></h2><p class="${warning ? "warning" : "muted"}">${warning ? "⚠ 含质量问题、事件或缺项；原值全部保留。" : "已记录指标；性能资格待校准。"} ${cell.alignment === "different-settings" ? "同名单元的设置不同，已拆开呈现。" : ""}</p><div class="plots">`;
        for (const m of METRICS) {
            body += figure(`${cell.id}-${m.id}`, barSvg(view, cell, m), `data-metric="${m.id}"`);
            if (view.groups.length > 1)
                for (const g of view.groups)
                    body += figure(
                        `${cell.id}-${m.id}-vs-${g.id}`,
                        barSvg(view, cell, m, g.id),
                        `data-metric="${m.id}" data-baseline="${g.id}"`,
                    );
        }
        body += `</div><p class="quality">${cell.groups
            .map((g) => {
                const label = view.groups.find((x) => x.id === g.id).label;
                const count = g.runs.reduce(
                    (a, r) => {
                        for (const k of ["correct", "sent", "failed", "dropped", "late"]) a[k] += r[k];
                        return a;
                    },
                    { correct: 0, sent: 0, failed: 0, dropped: 0, late: 0 },
                );
                return `${escape(label)}：正确 ${count.correct}/${count.sent} · 错误 ${count.failed} · 未发 ${count.dropped} · 迟到 ${count.late}`;
            })
            .join("　|　")}</p>`;
        body +=
            '<details><summary>逐次指标、计数、原因与事件</summary><div class="table"><table><thead><tr><th>组 / 轮次 / 运行</th><th>计划 / 正确</th><th>失败 / 未发 / 迟到</th><th>正确/s</th><th>P50 / P95 / P99 ms</th><th>wire / decoded P50 B</th><th>质量 / 事件</th></tr></thead><tbody>';
        for (const g of cell.groups) {
            const label = view.groups.find((x) => x.id === g.id).label;
            if (!g.runs.length) body += `<tr><td>${escape(label)}</td><td colspan="6">缺失，无填充值</td></tr>`;
            for (const r of g.runs)
                body += `<tr><td>${escape(label)}<br>${escape(r.round ?? "独立轮次未声明")}<br>${escape(r.inputId + "/" + r.id)}</td><td>${r.scheduled} / ${r.correct}</td><td>${r.failed} / ${r.dropped} / ${r.late}</td><td>${n(r.correctCompletedInWindowPerSecond)}</td><td>${n(r.correctResponseMs.p50)} / ${n(r.correctResponseMs.p95)} / ${n(r.correctResponseMs.p99)}${r.correct < 1000 ? " ⚠ P99样本不足" : ""}</td><td>${n(r.wireBodyBytes.p50)} / ${n(r.decodedBodyBytes.p50)}</td><td>${escape(r.issues.join(", ") || "无原质量标记")}${r.events.map((e) => `<p class="warning">${escape(e.kind ?? "备注")}: ${escape(e.message)}<br><small>${escape(e.association)}</small></p>`).join("")}</td></tr>`;
        }
        body += "</tbody></table></div>";
        for (const g of cell.groups)
            for (const r of g.runs) {
                body += `<details><summary>${escape(r.inputId + "/" + r.id)} · 状态码 / 调度 / 资源原因</summary>`;
                for (const p of r.diagnostics?.phases ?? [])
                    body += `<p><b>${escape(p.name)}</b>：HTTP ${escape(JSON.stringify(p.statusCounts))}；outcome ${escape(JSON.stringify(p.outcomes))}；错误 ${p.failed}/${p.sent}；未发送 ${p.dropped}/${p.scheduled}。阶段边界漏发 ${p.scheduling.phaseBoundary}，超过启动延迟 ${p.scheduling.maxStartDelay}，原因未定 ${p.scheduling.unresolved}（原因可重叠）。</p><p>驱动平均 ${n(p.resourceSignals.averageCpuCores)} 核；配额占比 ${n(p.resourceSignals.cpuFractionOfDeclaredQuota === null ? null : p.resourceSignals.cpuFractionOfDeclaredQuota * 100)}%；节流 ${n(p.resourceSignals.throttledUsec)} μs；RSS ${n(p.resourceSignals.maxRssKiB)} KiB。单次迟到不直接表述为持续过载。</p><pre>${escape(JSON.stringify(p.examples, null, 2))}</pre>`;
                for (const phase of r.windows ?? [])
                    body += figure(`${cell.id}-${r.inputId}-${r.id}-${phase.name}`, windowSvg(cell, r, phase));
                body += "</details>";
            }
        body += `<details><summary>对齐设置</summary><pre>${escape(JSON.stringify(cell.config, null, 2))}</pre></details></details></section>`;
    }
    const curves = new Map();
    for (const c of view.cells) {
        const { ratePerSecond, concurrency, ...load } = c.config.load;
        const key = valueDigest([c.caseId, { ...c.config, load }]);
        if (!curves.has(key)) curves.set(key, []);
        curves.get(key).push(c);
    }
    for (const [key, cells] of curves)
        if (new Set(cells.map((c) => c.loadId)).size > 1)
            body += `<section class="curve" data-case="${escape(cells[0].caseId)}"><h2>${escape(cells[0].caseId)} · 负载观察</h2>${figure(`curve-${key.slice(0, 16)}`, curveSvg(view, cells))}</section>`;
    const unique = (key) => [...new Set(view.cells.map((c) => c[key]))];
    const select = (id, items) =>
        `<select id="${id}">${items.map(([value, label]) => `<option value="${escape(value)}">${escape(label)}</option>`).join("")}</select>`;
    const html = `<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta http-equiv="Content-Security-Policy" content="default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'"><title>${escape(view.title)}</title><style>
*{box-sizing:border-box}body{font:15px system-ui,-apple-system,'PingFang SC',sans-serif;color:#24394f;background:#f2f5f9;margin:0}main{max-width:1460px;margin:auto;padding:28px}header,section{background:white;border:1px solid #dfe6ee;border-radius:14px;padding:25px;margin:0 0 20px}h1{font-size:30px;margin:12px 0}h2{font-size:21px}small,.muted{color:#62758a}p{line-height:1.7}.badge{background:#fff1d7;color:#825712;border-radius:18px;padding:6px 13px;display:inline-block}.warning{color:#9a6010}.cards{display:flex;gap:12px;flex-wrap:wrap}.card{background:#f4f7fb;border-radius:9px;padding:13px 20px;min-width:130px}.card b{display:block;font-size:25px;margin-top:6px}.legend{display:flex;gap:20px;margin:18px 0;flex-wrap:wrap}.filters{position:sticky;top:0;background:#f2f5f9ee;padding:16px 0;z-index:1;display:flex;gap:15px;align-items:center;flex-wrap:wrap}select{padding:8px;border:1px solid #bccbda;border-radius:6px;max-width:240px}.plots{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px}figure{margin:0;border:1px solid #e3eaf2;border-radius:8px;overflow:hidden}svg{width:100%;height:auto;display:block}.export{text-align:right;padding:8px 14px;background:#f8fafc}a{color:#176eb4}details{margin:16px 0}summary{cursor:pointer;color:#176eb4}pre{white-space:pre-wrap;overflow-wrap:anywhere;background:#f5f7fa;padding:16px;font-size:12px}.table{overflow:auto}table{width:100%;border-collapse:collapse;font-size:13px}td,th{text-align:left;vertical-align:top;padding:10px;border-bottom:1px solid #dfe6ee;min-width:100px}[hidden]{display:none!important}@media(max-width:900px){main{padding:12px}.plots{grid-template-columns:1fr}header,section{padding:15px}}@media print{.filters,.export{display:none}section{break-inside:avoid}}
</style></head><body><main><header><span class="badge">描述性对比 · 性能资格尚未授予</span><h1>${escape(view.title)}</h1><p class="muted">HG-Bench ${escape(view.reporter.version)} · build ${escape(view.reporter.buildId.slice(0, 23))} · ${escape(view.reporter.state)} · 报告 v2</p><div class="cards">${[
        ["输入", view.inputs.length],
        ["对齐单元", view.cells.length],
        ["正确测量", totals.correct],
        ["失败", totals.failed],
        ["未发送", totals.dropped],
    ]
        .map(([k, v]) => `<div class="card">${k}<b>${v}</b></div>`)
        .join(
            "",
        )}</div><div class="legend">${view.groups.map((g) => `<span style="color:${g.color}">● ${escape(g.label)}</span>`).join("")}</div><p>每轮指标分别计算，再取中位数与最小–最大范围；点为逐次运行。固定到达率下的正确吞吐不表示峰值容量。错误、缺项和事件全部保留。</p><details><summary>输入、资格与环境差异</summary><pre>${escape(JSON.stringify({ performanceAccepted: false, changedFactors: view.changedFactors, environment: view.environment, inputs: view.inputs, events: view.events }, null, 2))}</pre></details><a href="summary.csv" download>汇总 CSV</a> · <a href="runs.csv" download>逐次 CSV</a> · <a href="report.json">报告 JSON</a><p class="muted">每张图右上角可独立导出 SVG；目录 charts/ 同时提供所有单图。只复制 HTML 时图表仍可直接导出。</p></header>
<div class="filters"><label>场景 ${select("case", [["", "全部"], ...unique("caseId").map((x) => [x, x])])}</label><label>负载 ${select("load", [["", "全部"], ...unique("loadId").map((x) => [x, x])])}</label><label>指标 ${select(
        "metric",
        METRICS.map((m) => [m.id, m.label]),
    )}</label><label>基线 ${select(
        "baseline",
        view.groups.map((g) => [g.id, g.label]),
    )}</label><span id="shown"></span></div>${body}<footer><p>错误比例为 failed/sent，未发送比例为 dropped/scheduled；late 与其它计数可能重叠。低样本 P99 在原始明细保留，主图留空。UTC 事件是诊断关联，未自动剔除任何样本。</p></footer></main><script>
const el=id=>document.getElementById(id);el('metric').value='throughput';el('baseline').value=${JSON.stringify(view.baseline).replace(/</g, "\\u003c")};
function filter(){let n=0;const cells=[...document.querySelectorAll('.cell')];for(const c of cells){c.hidden=!!((el('case').value&&c.dataset.case!==el('case').value)||(el('load').value&&c.dataset.load!==el('load').value));if(!c.hidden)n++;for(const f of c.querySelectorAll('figure[data-metric]'))f.hidden=f.dataset.metric!==el('metric').value||!!(f.dataset.baseline&&f.dataset.baseline!==el('baseline').value)}for(const c of document.querySelectorAll('.curve'))c.hidden=!!(el('case').value&&c.dataset.case!==el('case').value);el('shown').textContent=n+' / '+cells.length+' 单元'}
for(const id of ['case','load','metric','baseline'])el(id).onchange=filter;filter();
for(const a of document.querySelectorAll('[data-export]'))a.onclick=e=>{e.preventDefault();const svg=a.closest('figure').querySelector('svg');const url=URL.createObjectURL(new Blob([new XMLSerializer().serializeToString(svg)],{type:'image/svg+xml;charset=utf-8'}));const link=document.createElement('a');link.href=url;link.download=a.dataset.export;document.body.append(link);link.click();link.remove();setTimeout(()=>URL.revokeObjectURL(url),1000)};
</script></body></html>`;
    const summaries = [
        [
            "case",
            "load",
            "settingsSha",
            "group",
            "metric",
            "unit",
            "median",
            "min",
            "max",
            "values",
            "expectedRuns",
            "verifiedRuns",
            "missingRuns",
            "alignment",
            "presentInProfile",
        ],
    ];
    const runs = [
        [
            "case",
            "load",
            "settingsSha",
            "group",
            "input",
            "round",
            "run",
            "metric",
            "value",
            "scheduled",
            "correct",
            "failed",
            "dropped",
            "late",
            "issues",
            "events",
        ],
    ];
    for (const c of view.cells)
        for (const g of c.groups)
            for (const m of METRICS) {
                const s = g.metrics[m.id];
                summaries.push([
                    c.caseId,
                    c.loadId,
                    c.fingerprint,
                    g.id,
                    m.id,
                    m.unit,
                    s.median,
                    s.min,
                    s.max,
                    s.count,
                    g.expected,
                    g.runs.length,
                    g.missing,
                    c.alignment,
                    g.presentInProfile,
                ]);
                for (const r of g.runs)
                    runs.push([
                        c.caseId,
                        c.loadId,
                        c.fingerprint,
                        g.id,
                        r.inputId,
                        r.round,
                        r.id,
                        m.id,
                        m.value(r),
                        r.scheduled,
                        r.correct,
                        r.failed,
                        r.dropped,
                        r.late,
                        r.issues.join(";"),
                        r.events.map((e) => e.message).join(";"),
                    ]);
            }
    return new Map([
        ["report.html", html],
        ["summary.csv", csv(summaries)],
        ["runs.csv", csv(runs)],
        [
            "inputs.json",
            JSON.stringify(
                {
                    schemaVersion: 1,
                    groups: view.groups,
                    inputs: view.inputs,
                    alignment: view.cells.map((c) => ({
                        caseId: c.caseId,
                        loadId: c.loadId,
                        fingerprint: c.fingerprint,
                        status: c.alignment,
                        groups: c.groups.map((g) => ({
                            id: g.id,
                            presentInProfile: g.presentInProfile,
                            expected: g.expected,
                            verified: g.runs.length,
                            missing: g.missing,
                        })),
                    })),
                },
                null,
                2,
            ) + "\n",
        ],
        ...charts,
    ]);
}
export function singleReport(analysis, view) {
    return {
        ...analysis,
        reportVersion: 2,
        reporter: view.reporter,
        annotations: view.events,
        reportDefinitions: view.definitions,
    };
}
export async function writeBundle(output, view, report = view) {
    const files = renderBundle(view);
    files.set("report.json", JSON.stringify(report, null, 2) + "\n");
    const bytes = [...files.values()].reduce((n, s) => n + Buffer.byteLength(s), 0);
    if (bytes > 64 * 1024 * 1024)
        throw Object.assign(new Error("Report exceeds 64 MiB; split inputs into smaller comparisons"), {
            code: "REPORT_TOO_LARGE",
        });
    const dir = resolve(output);
    let parentReady = false;
    try {
        await mkdir(dirname(dir), { recursive: true, mode: 0o700 });
        parentReady = true;
        await mkdir(dir, { mode: 0o700 });
    } catch (e) {
        throw Object.assign(
            new Error(
                parentReady && e.code === "EEXIST"
                    ? `Output exists; choose a new --out directory: ${dir}`
                    : `Cannot create output ${dir}; check parent directory and permissions (${e.code})`,
            ),
            {
                code: parentReady && e.code === "EEXIST" ? "REPORT_OUTPUT_EXISTS" : "REPORT_OUTPUT_PATH",
                details: { path: dir, cause: e.code },
            },
        );
    }
    await mkdir(join(dir, "charts"), { mode: 0o700 });
    for (const [name, content] of files) await writeFile(join(dir, name), content, { flag: "wx", mode: 0o600 });
    return {
        report: join(dir, "report.html"),
        json: join(dir, "report.json"),
        charts: files.size - 5,
        bytes,
        performanceAccepted: false,
    };
}
