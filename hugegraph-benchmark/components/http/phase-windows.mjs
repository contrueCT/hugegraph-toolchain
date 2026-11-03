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

import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";
import { mkdir, readFile, realpath, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { createInterface } from "node:readline";
import { pathToFileURL } from "node:url";
import { isDeepStrictEqual } from "node:util";
import { analyze } from "./analyzer.mjs";
import { bytesDigest, digest, entry } from "./common.mjs";

const MAX_WINDOWS = 16384;
const MAX_REPORT_BYTES = 64 * 1024 * 1024;
const METRICS = ["correctResponseMs", "allCompleteResponseMs", "failedDurationMs", "schedulerDelayMs", "validationMs"];
const COUNTS = ["records", "sent", "completed", "correct", "errors", "dropped", "notSent", "late", "completeResponses"];
const need = (value, message) => {
    if (!value) throw new Error(`HTTP_PHASE_WINDOWS: ${message}`);
};

function policyFor(options) {
    need(
        options && Object.keys(options).sort().join(",") === "minimumP99Requests,minimumRequestsPerWindow,windowMs",
        "explicit windowMs, minimumRequestsPerWindow and minimumP99Requests are required",
    );
    for (const [name, low, high] of [
        ["windowMs", 1, 300000],
        ["minimumRequestsPerWindow", 1, 100000],
        ["minimumP99Requests", 1000, 100000],
    ])
        need(Number.isSafeInteger(options[name]) && options[name] >= low && options[name] <= high, `invalid ${name}`);
    return { ...options };
}

function bucket(kind, index, startMs, endMs) {
    return {
        kind,
        index,
        startMs,
        endMs,
        counts: Object.fromEntries(COUNTS.map((key) => [key, 0])),
        outcomes: {},
        completion: {
            terminalOutsideArrivalWindow: 0,
            validationOutsideArrivalWindow: 0,
            terminalInDrain: 0,
            validationInDrain: 0,
        },
        metrics: Object.fromEntries(METRICS.map((key) => [key, []])),
    };
}

function nearestRank(values, policy) {
    values.sort((a, b) => a - b);
    const enough = values.length >= policy.minimumRequestsPerWindow,
        p99Enough = enough && values.length >= policy.minimumP99Requests;
    const observed = {
        p50: values[Math.ceil(values.length * 0.5) - 1] ?? null,
        p95: values[Math.ceil(values.length * 0.95) - 1] ?? null,
        p99: values[Math.ceil(values.length * 0.99) - 1] ?? null,
        max: values.at(-1) ?? null,
    };
    return {
        count: values.length,
        sampleStatus: enough ? "descriptive" : "unknown",
        p99Status: p99Enough ? "descriptive" : "unknown",
        p50: enough ? observed.p50 : null,
        p95: enough ? observed.p95 : null,
        p99: p99Enough ? observed.p99 : null,
        observedNearestRank: observed,
    };
}

function finishBucket(value, policy) {
    value.sampleStatus = value.counts.sent >= policy.minimumRequestsPerWindow ? "descriptive" : "unknown";
    value.flags = [
        ...(value.sampleStatus === "unknown" ? ["insufficient-sent-requests"] : []),
        ...(value.completion.terminalOutsideArrivalWindow ? ["request-terminal-crosses-window"] : []),
        ...(value.completion.validationOutsideArrivalWindow ? ["validation-completion-crosses-window"] : []),
    ];
    for (const key of METRICS) value.metrics[key] = nearestRank(value.metrics[key], policy);
    return value;
}

function addRecord(value, sample, phase) {
    const c = value.counts;
    c.records++;
    value.outcomes[sample.outcome] = (value.outcomes[sample.outcome] ?? 0) + 1;
    if (sample.late) c.late++;
    if (sample.sentMs === null) {
        c.dropped++;
        c.notSent++;
        return;
    }
    c.sent++;
    c.completed++;
    value.metrics.schedulerDelayMs.push(sample.sentMs - sample.scheduledMs);
    if (sample.outcome === "ok") {
        c.correct++;
        value.metrics.correctResponseMs.push(sample.endMs - sample.sentMs);
    } else {
        c.errors++;
        value.metrics.failedDurationMs.push(sample.endMs - sample.sentMs);
    }
    if (sample.completeResponse) {
        c.completeResponses++;
        value.metrics.allCompleteResponseMs.push(sample.endMs - sample.sentMs);
        value.metrics.validationMs.push(sample.validationMs);
    }
    if (sample.endMs > value.endMs) value.completion.terminalOutsideArrivalWindow++;
    if (sample.completeResponse && sample.endMs + sample.validationMs > value.endMs)
        value.completion.validationOutsideArrivalWindow++;
    if (sample.endMs > phase.durationMs) value.completion.terminalInDrain++;
    if (sample.completeResponse && sample.endMs + sample.validationMs > phase.durationMs)
        value.completion.validationInDrain++;
}

async function lockedJson(root, path, inventory) {
    const member = inventory.get(path);
    need(member && member.bytes <= 8 * 1024 * 1024, `missing or oversized JSON ${path}`);
    const raw = await readFile(join(root, path));
    need(raw.length === member.bytes && bytesDigest(raw) === member.sha256, `raw changed after verification: ${path}`);
    return JSON.parse(raw.toString("utf8"));
}

async function readTrace(root, path, member, consume) {
    need(member, `missing trace ${path}`);
    const hash = createHash("sha256"),
        stream = createReadStream(join(root, path));
    let size = 0;
    stream.on("data", (chunk) => {
        size += chunk.length;
        hash.update(chunk);
        if (size > member.bytes) stream.destroy(new Error("HTTP_PHASE_WINDOWS: trace grew after verification"));
    });
    const lines = createInterface({ input: stream, crlfDelay: Infinity });
    try {
        for await (const line of lines) {
            need(line.length > 0 && line.length < 65536, "invalid trace line length");
            consume(JSON.parse(line));
        }
        need(size === member.bytes && hash.digest("hex") === member.sha256, `raw changed after verification: ${path}`);
    } finally {
        lines.close();
        stream.destroy();
    }
}

/** Offline arrival cohorts in the existing per-phase performance.now domain. No host clock association. */
export async function analyzePhaseWindows(directory, options) {
    return (await analyzeWithPhaseWindows(directory, options)).windows;
}

/** Share one full verification pass; there is no caller-supplied trusted-analysis bypass. */
export async function analyzeWithPhaseWindows(directory, options) {
    const policy = policyFor(options),
        root = await realpath(directory);
    const verified = await analyze(root);
    return { analysis: verified, windows: await windowsFromVerified(root, policy, verified) };
}

async function windowsFromVerified(root, policy, verified) {
    const manifestPath = join(root, "raw.lock.json"),
        manifestSha = await digest(manifestPath);
    need(verified.rawManifestSha256 === manifestSha, "raw manifest changed during verification");
    const manifestRaw = await readFile(manifestPath);
    need(bytesDigest(manifestRaw) === manifestSha, "raw manifest changed after verification");
    const manifest = JSON.parse(manifestRaw.toString("utf8")),
        inventory = new Map(manifest.files.map((f) => [f.path, f])),
        profile = await lockedJson(root, "profile.json", inventory);
    need(isDeepStrictEqual(verified.profile, profile), "verified profile differs");
    const expectedWindows =
        manifest.runs.length *
        (Math.ceil(profile.durationMs / policy.windowMs) +
            (profile.warmupMs ? Math.ceil(profile.warmupMs / policy.windowMs) + 1 : 0) +
            1);
    need(Number.isSafeInteger(expectedWindows) && expectedWindows <= MAX_WINDOWS, "window budget exceeded");
    const runs = [];
    for (const id of manifest.runs) {
        const raw = await lockedJson(root, `runs/${id}.json`, inventory),
            states = new Map();
        for (const phase of raw.phases) {
            const windows = Array.from({ length: Math.ceil(phase.durationMs / policy.windowMs) }, (_, index) =>
                bucket(
                    "arrival-window",
                    index,
                    index * policy.windowMs,
                    Math.min((index + 1) * policy.windowMs, phase.durationMs),
                ),
            );
            states.set(phase.name, {
                saved: phase,
                windows,
                drain: bucket("drain-arrivals", null, phase.durationMs, Math.max(phase.durationMs, phase.elapsedMs)),
            });
        }
        await readTrace(root, `traces/${id}.ndjson`, inventory.get(`traces/${id}.ndjson`), (sample) => {
            const state = states.get(sample.phase),
                arrival = sample.sentMs === null ? sample.observedMs : sample.sentMs;
            need(
                state && Number.isFinite(arrival) && arrival >= 0 && arrival <= state.saved.elapsedMs,
                "invalid arrival time",
            );
            const window =
                arrival < state.saved.durationMs ? state.windows[Math.floor(arrival / policy.windowMs)] : state.drain;
            need(window, "arrival has no window");
            addRecord(window, sample, state.saved);
        });
        const phases = [];
        for (const [name, state] of states) {
            const buckets = [...state.windows, state.drain],
                totals = Object.fromEntries(
                    COUNTS.map((key) => [key, buckets.reduce((sum, b) => sum + b.counts[key], 0)]),
                ),
                counts = {};
            for (const b of buckets)
                for (const [outcome, count] of Object.entries(b.outcomes))
                    counts[outcome] = (counts[outcome] ?? 0) + count;
            need(
                totals.records === state.saved.scheduled &&
                    totals.records === totals.correct + totals.errors + totals.dropped &&
                    totals.sent === totals.correct + totals.errors &&
                    totals.notSent === totals.dropped &&
                    isDeepStrictEqual(counts, state.saved.counts),
                "arrival window count conservation failed",
            );
            for (const [key, savedKey] of [
                ["sent", "sent"],
                ["completed", "completed"],
                ["errors", "failed"],
                ["dropped", "dropped"],
                ["late", "late"],
            ])
                need(totals[key] === state.saved[savedKey], `arrival ${key} count differs`);
            phases.push({
                name,
                durationMs: state.saved.durationMs,
                elapsedWithDrainMs: state.saved.elapsedMs,
                observationThroughMs: state.saved.elapsedMs,
                phaseEndedEarly: state.saved.elapsedMs < state.saved.durationMs,
                totals,
                outcomes: counts,
                windows: state.windows.map((b) => finishBucket(b, policy)),
                drain: {
                    arrivals: finishBucket(state.drain, policy),
                    completionOverlay: {
                        requestTerminals: buckets.reduce((n, b) => n + b.completion.terminalInDrain, 0),
                        completeResponseValidations: buckets.reduce((n, b) => n + b.completion.validationInDrain, 0),
                    },
                },
                driverWholePhase: {
                    averageCpuCores: state.saved.averageCpuCores ?? null,
                    eventLoopUtilization: state.saved.eventLoopUtilization ?? null,
                    eventLoopDelayMs: state.saved.eventLoopDelayMs ?? null,
                    maxRssKiB: state.saved.maxRssKiB ?? null,
                    scope: "whole-phase-including-drain; unavailable per window",
                },
            });
        }
        runs.push({ id, caseId: raw.caseId, loadId: raw.loadId, repeat: raw.repeat, phases });
    }
    need((await digest(manifestPath)) === manifestSha, "raw manifest changed during window reconstruction");
    return {
        schemaVersion: 1,
        kind: "http-phase-window-diagnostics",
        rawManifestSha256: manifestSha,
        name: profile.name,
        policy,
        budgets: { maximumWindowsIncludingDrain: MAX_WINDOWS, maximumCombinedReportBytes: MAX_REPORT_BYTES },
        verification: {
            evidenceIntegrity: "passed-by-http-analyzer",
            sampleAccounting: "passed-by-http-analyzer-and-window-conservation",
            timeline: "passed-by-http-analyzer",
            serviceChecks: verified.serviceChecks.length,
        },
        runs,
        performanceAccepted: false,
        warmupStable: null,
        qualification: "not-assessed",
        definitions: {
            clock: "Per-phase performance.now relative milliseconds only; each phase has its own zero",
            ownership:
                "Exactly one arrival cohort: sentMs for sent requests, observedMs for unsent requests; scheduledMs is not used for assignment",
            boundaries:
                "Nominal arrival windows are [startMs,endMs). Arrival >= durationMs belongs to drain. Completion crosses a boundary only when strictly greater than endMs",
            drain: "Arrival counts partition all records. completionOverlay counts existing cohort members finishing after durationMs; never add it to arrival counts",
            errors: "Sent requests whose outcome is not ok. dropped and notSent are equal in the current raw protocol and overlap. late overlaps other counts",
            metrics:
                "Nearest rank; correctResponseMs and allCompleteResponseMs end at complete body, failedDurationMs includes terminal failures. validationMs is separate driver work. Each metric uses its own sample count",
            samples:
                "p50/p95 require minimumRequestsPerWindow; p99 additionally requires minimumP99Requests (>=1000). observedNearestRank preserves raw order statistics even when status is unknown; descriptive never proves stability",
        },
        limits: [
            "Fixed windows are descriptive diagnostics; no automatic warmup, steady-state, repeatability or driver-headroom acceptance",
            "Arrival cohorts include later completions; they are not completion-rate windows",
            "Whole-phase driver counters cannot be attributed to individual windows",
            "No cross-process clock, JVM, collector, resource or server-side attribution",
            "Preflight/postcheck are verified by the analyzer and excluded from workload windows",
            "Evidence must remain immutable while analyzed; raw members consumed here are checked again against the original lock",
        ],
    };
}

function html(result) {
    const data = JSON.stringify(result)
        .replace(/</g, "\\u003c")
        .replace(/\u2028/g, "\\u2028")
        .replace(/\u2029/g, "\\u2029");
    return `<!doctype html><html lang="zh-CN"><meta charset="utf-8"><meta name="viewport" content="width=device-width">
<meta http-equiv="Content-Security-Policy" content="default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'">
<title>HG-Bench HTTP phase windows</title><style>body{font:14px system-ui;margin:24px;color:#172536;background:#f4f6f8}main{max-width:1400px;margin:auto}section{background:white;padding:20px;margin:16px 0}select{padding:6px}table{border-collapse:collapse;width:100%}th,td{padding:7px;text-align:left;border-bottom:1px solid #ddd;white-space:nowrap}pre{white-space:pre-wrap;overflow-wrap:anywhere}svg{width:100%;height:230px}.warning{color:#8d4f09}</style>
<main><h1 id="name"></h1><p class="warning">Arrival 窗口诊断 · performanceAccepted=false · warmupStable=null。低样本显示 unknown；P99 还需满足独立样本门槛。</p>
<section><label>运行 <select id="run"></select></label> <label>阶段 <select id="phase"></select></label><svg id="chart" role="img" aria-label="按 arrival 窗口的正确响应 P50 与 P95 趋势"></svg><p id="policy"></p><div style="overflow:auto"><table><thead><tr><th>arrival ms</th><th>记录/发送/正确</th><th>错误/丢弃/迟到</th><th>正确响应 P50/P95/P99 ms</th><th>校验 P95 ms</th><th>跨窗终止/校验</th><th>样本状态</th></tr></thead><tbody id="rows"></tbody></table></div><pre id="drain"></pre></section>
<section><details><summary>计时定义、限制与完整 JSON</summary><pre id="details"></pre></details></section></main>
<script type="application/json" id="data">${data}</script><script>
const d=JSON.parse(document.getElementById('data').textContent),el=id=>document.getElementById(id),n=v=>v===null?'unknown':v.toFixed(3);el('name').textContent=d.name;el('details').textContent=JSON.stringify(d,null,2);el('policy').textContent=JSON.stringify(d.policy);for(const r of d.runs){const o=document.createElement('option');o.value=r.id;o.textContent=r.id+' '+r.caseId+' / '+r.loadId+' repeat '+r.repeat;el('run').append(o)}function render(){const r=d.runs.find(v=>v.id===el('run').value),p=r.phases.find(v=>v.name===el('phase').value);if(!p)return;el('rows').replaceChildren();const all=[...p.windows,p.drain.arrivals];for(const w of all){const tr=document.createElement('tr'),q=w.metrics.correctResponseMs;for(const v of [(w.kind==='drain-arrivals'?'drain ':'')+w.startMs+'..'+w.endMs,w.counts.records+'/'+w.counts.sent+'/'+w.counts.correct,w.counts.errors+'/'+w.counts.dropped+'/'+w.counts.late,[q.p50,q.p95,q.p99].map(n).join(' / '),n(w.metrics.validationMs.p95),w.completion.terminalOutsideArrivalWindow+'/'+w.completion.validationOutsideArrivalWindow,w.sampleStatus+'; P99 '+q.p99Status]){const td=document.createElement('td');td.textContent=v;tr.append(td)}el('rows').append(tr)}el('drain').textContent='Drain completion overlay (已包含在 arrival cohorts，不能累加): '+JSON.stringify(p.drain.completionOverlay)+'\n全阶段驱动数据: '+JSON.stringify(p.driverWholePhase);const svg=el('chart'),ns='http://www.w3.org/2000/svg';svg.replaceChildren();svg.setAttribute('viewBox','0 0 1100 230');const max=Math.max(1,...p.windows.flatMap(w=>[w.metrics.correctResponseMs.p50??0,w.metrics.correctResponseMs.p95??0]));for(const [metric,color]of [['p50','#197b86'],['p95','#c16d25']]){let points=[];function draw(){if(points.length){const path=document.createElementNS(ns,'polyline');path.setAttribute('points',points.join(' '));path.setAttribute('fill','none');path.setAttribute('stroke',color);svg.append(path);points=[]}}p.windows.forEach((w,i)=>{const v=w.metrics.correctResponseMs[metric];if(v===null){draw();return}const x=50+(i+.5)*1000/p.windows.length,y=190-v/max*160;points.push(x+','+y);const dot=document.createElementNS(ns,'circle');dot.setAttribute('cx',x);dot.setAttribute('cy',y);dot.setAttribute('r',3);dot.setAttribute('fill',color);svg.append(dot)});draw()}const label=document.createElementNS(ns,'text');label.setAttribute('x',50);label.setAttribute('y',220);label.textContent='正确响应 P50(青)/P95(橙)，纵轴最大 '+n(max)+' ms；unknown 留空，drain 单列';svg.append(label)}function phases(){const r=d.runs.find(v=>v.id===el('run').value);el('phase').replaceChildren();for(const p of r.phases){const o=document.createElement('option');o.value=p.name;o.textContent=p.name;el('phase').append(o)}render()}el('run').onchange=phases;el('phase').onchange=render;phases();</script></html>`;
}

export async function reportPhaseWindows(directory, output, options) {
    const result = await analyzePhaseWindows(directory, options),
        rawJson = JSON.stringify(result, null, 2) + "\n",
        rawHtml = html(result);
    need(
        Buffer.byteLength(rawJson) + Buffer.byteLength(rawHtml) <= MAX_REPORT_BYTES,
        "combined report budget exceeded",
    );
    await mkdir(output, { mode: 0o700 });
    await writeFile(join(output, "report.json"), rawJson, { flag: "wx", mode: 0o600 });
    await writeFile(join(output, "report.html"), rawHtml, { flag: "wx", mode: 0o600 });
    return result;
}

const HELP = `HTTP phase windows v1 (offline, no target calls)
  node components/http/phase-windows.mjs describe
  node components/http/phase-windows.mjs report --input RAW-DIRECTORY --output NEW-DIRECTORY --window-ms INTEGER --minimum-requests-per-window INTEGER --minimum-p99-requests INTEGER
  node components/http/phase-windows.mjs --help
All three sample/window settings are explicit. P99 minimum is 1000..100000.
Revalidates the full raw lock/accounting/timeline through analyzer.mjs. Produces report.json + offline report.html, never overwrites. No automatic warmup or performance qualification. Failure exit code 6.`;

export function describePhaseWindows() {
    return {
        schemaVersion: 1,
        kind: "http-phase-window-component",
        commands: ["describe", "report", "--help"],
        requiredReportFlags: [
            "--input",
            "--output",
            "--window-ms",
            "--minimum-requests-per-window",
            "--minimum-p99-requests",
        ],
        limits: {
            windowMs: [1, 300000],
            minimumRequestsPerWindow: [1, 100000],
            minimumP99Requests: [1000, 100000],
            maximumWindowsIncludingDrain: MAX_WINDOWS,
            maximumCombinedReportBytes: MAX_REPORT_BYTES,
        },
        network: false,
        performanceAccepted: false,
        warmupStable: null,
    };
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href)
    await entry(async () => {
        const [command, ...args] = process.argv.slice(2);
        if (command === "describe") {
            need(args.length === 0, "describe accepts no options");
            process.stdout.write(JSON.stringify(describePhaseWindows()) + "\n");
            return;
        }
        need(command === "report", "expected describe or report; use --help");
        const names = describePhaseWindows().requiredReportFlags,
            values = {};
        for (let i = 0; i < args.length; i += 2) {
            need(
                names.includes(args[i]) && args[i + 1] && !Object.hasOwn(values, args[i]),
                "invalid or duplicate report option",
            );
            values[args[i]] = args[i + 1];
        }
        need(
            names.every((key) => Object.hasOwn(values, key)),
            "missing required report option",
        );
        const integer = (key) => {
            need(/^[1-9][0-9]*$/.test(values[key]), `invalid integer ${key}`);
            return Number(values[key]);
        };
        const result = await reportPhaseWindows(values["--input"], values["--output"], {
            windowMs: integer("--window-ms"),
            minimumRequestsPerWindow: integer("--minimum-requests-per-window"),
            minimumP99Requests: integer("--minimum-p99-requests"),
        });
        process.stdout.write(
            JSON.stringify({
                kind: result.kind,
                rawManifestSha256: result.rawManifestSha256,
                runs: result.runs.length,
                performanceAccepted: false,
                warmupStable: null,
            }) + "\n",
        );
    }, HELP);
