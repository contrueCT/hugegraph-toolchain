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

import { mkdir, readFile, writeFile } from "node:fs/promises";
import { join, resolve, dirname } from "node:path";
import { analyze } from "./analyzer.mjs";
import { buildView, writeBundle, escape } from "./report-view.mjs";
import { selectThroughputPoints, httpPointHealth } from "../../dist/throughput.js";
import { valueDigest } from "./common.mjs";
import { readTargetDiagnostics } from "./target-diagnostics.mjs";

export async function reportThroughput(directory, out, options = {}) {
    const state = JSON.parse(await readFile(join(directory, "state.json"), "utf8"));
    const graph = state.track === "graph";
    const title = graph ? "图查询" : "HTTP";
    if (
        state.kind !== (graph ? "graph-throughput-run" : "http-throughput-run") ||
        state.version !== "0.2.0" ||
        !["http", "graph"].includes(state.track)
    )
        throw new Error("Not an HTTP throughput v0.2 attempt");
    const planRaw = await readFile(join(directory, "plan.json"));
    const { createHash } = await import("node:crypto");
    if (state.planSha256 !== createHash("sha256").update(planRaw).digest("hex"))
        throw new Error("Frozen plan digest changed");
    const plan = JSON.parse(planRaw);
    for (const k of [
        "version",
        "track",
        "preset",
        "cases",
        "settings",
        "suite",
        "encoding",
        ...(graph ? ["graph", "parameterPools"] : []),
    ])
        if (valueDigest(state[k]) !== valueDigest(plan[k])) throw new Error("State/plan mismatch: " + k);
    const inputs = [],
        evidenceIssues = [],
        derivedSelections = new Map();
    const labels = new Set();
    for (const [index, stage] of state.stages.entries()) {
        if (stage.stage === "calibration") continue;
        if (!stage.rawDirectory) {
            evidenceIssues.push({ stage: stage.label, reason: "not-dispatched" });
            continue;
        }
        try {
            const allowed = state.cases.some(
                (id) => stage.label === "explore-" + id || stage.label === "confirm-" + id,
            );
            if (!allowed) throw new Error("Unexpected stage path");
            // Attempt layout is fixed; an archived run can be relocated without editing frozen inputs.
            const analysis = await analyze(join(directory, stage.label, "evidence"));
            if (
                analysis.profile.endpoint !== plan.profile.endpoint ||
                analysis.profile.responseEncoding !== plan.encoding ||
                valueDigest({ ...analysis.profile.target, evidenceFiles: [] }) !==
                    valueDigest({ ...plan.profile.target, evidenceFiles: [] })
            )
                throw new Error("Raw target identity differs from frozen plan");
            const p = analysis.profile;
            if (p.cases.length !== 1 || !state.cases.includes(p.cases[0].id)) throw new Error("Unexpected raw case");
            const id = p.cases[0].id;
            if (
                graph &&
                (valueDigest(p.graph) !== valueDigest(plan.profile.graph) ||
                    valueDigest(p.cases[0]) !== valueDigest(plan.profile.cases.find((c) => c.id === id)))
            )
                throw new Error("Graph dataset/reference/parameter pool differs");
            if (labels.has(stage.label)) throw new Error("Duplicate stage");
            labels.add(stage.label);
            const exploration = stage.stage === "exploration";
            if (
                !["exploration", "confirmation"].includes(stage.stage) ||
                stage.label !== (exploration ? "explore-" : "confirm-") + id
            )
                throw new Error("Stage identity mismatch");
            const s = plan.settings,
                selection = derivedSelections.get(id);
            const concurrency = exploration ? s.concurrency : selection?.confirmation;
            if (
                !concurrency ||
                valueDigest(p.loads) !==
                    valueDigest(concurrency.map((n) => ({ id: "c" + n, model: "closed", concurrency: n }))) ||
                p.warmupMs !== (exploration ? s.exploreWarmupMs : s.confirmWarmupMs) ||
                p.durationMs !== (exploration ? s.exploreDurationMs : s.confirmDurationMs) ||
                p.repeats !== (exploration ? 1 : s.confirmRepeats)
            )
                throw new Error("Raw scheduling differs from declared protocol");
            if (exploration)
                derivedSelections.set(
                    id,
                    selectThroughputPoints(
                        analysis.runs.map((r) => ({
                            concurrency: r.concurrency,
                            goodput: r.correctCompletedInWindowPerSecond,
                            eligible: httpPointHealth(r).eligible,
                        })),
                        s,
                    ),
                );
            // Exploration/confirmation have different windows; keep stage metadata without inventing missing variants.
            inputs.push({
                id: "stage-" + index,
                group: "measurement",
                label: title + "测量",
                stage: stage.stage,
                analysis,
            });
        } catch (e) {
            evidenceIssues.push({ stage: stage.label, reason: String(e) });
        }
    }
    const derivedIncomplete = state.calibrationOnly
        ? []
        : state.cases.flatMap((id) => {
              const explore = inputs.find((i) => i.stage === "exploration" && i.analysis.profile.cases[0].id === id);
              const confirm = inputs.find((i) => i.stage === "confirmation" && i.analysis.profile.cases[0].id === id);
              return !explore ||
                  !confirm ||
                  explore.analysis.verdict === "partial" ||
                  confirm.analysis.verdict === "partial"
                  ? [{ caseId: id, stage: !explore ? "exploration" : "confirmation" }]
                  : [];
          });
    const diagnostics = options.targetDiagnostics
        ? await readTargetDiagnostics(
              options.targetDiagnostics,
              state.planSha256,
              inputs.flatMap((input) =>
                  input.analysis.runs.flatMap((row) =>
                      (row.phaseClock?.phases ?? []).flatMap((phase) => {
                          const durationMs =
                              phase.name === "measurement"
                                  ? input.analysis.profile.durationMs
                                  : phase.name === "warmup"
                                    ? input.analysis.profile.warmupMs
                                    : 0;
                          return durationMs > 0
                              ? [
                                    {
                                        id: row.id,
                                        caseId: row.caseId,
                                        stage: input.stage,
                                        loadId: row.loadId,
                                        repeat: row.repeat,
                                        startedAt: row.diagnostics?.startedAt,
                                        durationMs,
                                        phase,
                                    },
                                ]
                              : [];
                      }),
                  ),
              ),
          )
        : null;
    const view = inputs.length
        ? await buildView(inputs, { title: `${title} ${state.preset} / closed-loop v0.2`, baseline: inputs[0]?.group })
        : null;
    const result = {
        ...state,
        evidenceIssues,
        stages: state.stages.map((s) => {
            const { rows, ...rest } = s;
            return rest;
        }),
        status:
            state.status === "completed" && !evidenceIssues.length && !derivedIncomplete.length
                ? "completed"
                : "partial",
        derivedIncomplete,
        cases: state.cases.map((caseId) => {
            const rows = inputs.flatMap((i) =>
                i.analysis.runs.filter((r) => r.caseId === caseId).map((r) => ({ stage: i.stage, ...r })),
            );
            const confirmed = rows.filter(
                (r) =>
                    r.stage === "confirmation" &&
                    r.correct &&
                    !r.failed &&
                    !r.dropped &&
                    r.issues.every((s) => s.endsWith(":go-resource-isolation-unverified")),
            );
            const medians = [...new Set(confirmed.map((r) => r.concurrency))].flatMap((concurrency) => {
                const rs = confirmed.filter((r) => r.concurrency === concurrency);
                if (rs.length !== state.settings.confirmRepeats) return [];
                const xs = rs.map((r) => r.correctCompletedInWindowPerSecond).sort((a, b) => a - b);
                const m = Math.floor(xs.length / 2);
                return [
                    {
                        concurrency,
                        repeats: rs.length,
                        median: xs.length % 2 ? xs[m] : (xs[m - 1] + xs[m]) / 2,
                        range: [xs[0], xs.at(-1)],
                    },
                ];
            });
            return {
                caseId,
                selection: derivedSelections.get(caseId) ?? null,
                calibrationCoverage: rows.map((r) => ({
                    stage: r.stage,
                    loadId: r.loadId,
                    repeat: r.repeat,
                    status:
                        state.calibration?.status === "passed" &&
                        state.calibration.records.some(
                            (c) =>
                                c.caseId === caseId &&
                                c.rate >= r.correctCompletedInWindowPerSecond &&
                                (!graph ||
                                    (Number.isFinite(c.maxDecodedBytes) &&
                                        c.maxDecodedBytes >= r.decodedBodyBytes?.max)),
                        )
                            ? "within-representative-rate-shape"
                            : "outside-or-limited-representative-check",
                    pointHealth: httpPointHealth(r),
                })),
                rows,
                confirmed: medians,
                highestObservedConfirmedGoodput: medians.length ? Math.max(...medians.map((m) => m.median)) : null,
                qualification: "descriptive-only",
                environment: "measurement-window-GC/swap-evidence-not-attached",
            };
        }),
        ...(graph
            ? {
                  datasetVerification: "reference-declared-target-not-verified-by-this-entry",
                  propertyValidation:
                      "point identity and label; complete vertex properties are decoded but not independently compared",
                  referenceScope:
                      "Person/knows projection with independent answers; target import coverage is reported separately from content verification",
              }
            : {}),
        ...(diagnostics
            ? { targetDiagnostics: { ...diagnostics.summary, archive: "target-diagnostics/manifest.json" } }
            : {}),
        performanceAccepted: false,
    };
    if (view) await writeBundle(out, view, result);
    else {
        await mkdir(out, { mode: 0o700 });
        await mkdir(join(out, "charts"));
        await writeFile(join(out, "report.json"), JSON.stringify(result, null, 2) + "\n", { flag: "wx" });
        await writeFile(
            join(out, "report.html"),
            "<!doctype html><html><head><meta charset='utf-8'><title>HTTP throughput</title></head><body><p>No target measurement completed.</p></body></html>",
            { flag: "wx" },
        );
    }
    if (diagnostics) {
        for (const file of diagnostics.files) {
            const path = join(out, "target-diagnostics", file.path);
            await mkdir(dirname(path), { recursive: true });
            await writeFile(path, file.raw, { flag: "wx" });
        }
    }
    // Reuse all metric/detail charts; add the shared closed-loop curve in the same offline document.
    const curves = [];
    for (const c of result.cases) {
        const rows = c.rows;
        if (!rows.length) continue;
        const name = valueDigest(c.caseId).slice(0, 12) + "-concurrency.svg";
        const maxX = Math.max(...rows.map((r) => r.concurrency), 1),
            maxY = Math.max(...rows.map((r) => r.correctCompletedInWindowPerSecond), 1);
        const px = (x) => 70 + (620 * x) / maxX,
            py = (y) => 245 - (190 * y) / maxY;
        const svg = `<svg xmlns="http://www.w3.org/2000/svg" width="760" height="300" viewBox="0 0 760 300"><rect width="760" height="300" fill="white"/><g font-family="sans-serif" font-size="13" fill="#24334b"><text x="30" y="26">${escape(c.caseId)} / correct throughput (req/s)</text><path d="M70 45V245H700" fill="none" stroke="#8995a5"/>${[0, 0.25, 0.5, 0.75, 1].map((k) => `<text x="8" y="${py(k * maxY) + 4}">${(k * maxY).toFixed(1)}</text><path d="M70 ${py(k * maxY)}H700" stroke="#edf0f5"/>`).join("")}${[...new Set(rows.map((r) => r.concurrency))].map((x) => `<text x="${px(x)}" y="267">${x}</text>`).join("")}<text x="300" y="289">concurrency</text>${[
            "exploration",
            "confirmation",
        ]
            .map((stage, i) => {
                const color = i ? "#0a9484" : "#2878c5",
                    rs = rows.filter((r) => r.stage === stage).sort((a, b) => a.concurrency - b.concurrency);
                return `<text x="${420 + i * 150}" y="26" fill="${color}">${stage}</text>${i ? "" : `<polyline points="${rs.map((r) => `${px(r.concurrency)},${py(r.correctCompletedInWindowPerSecond)}`).join(" ")}" fill="none" stroke="${color}"/>`}${rs.map((r) => `<circle cx="${px(r.concurrency)}" cy="${py(r.correctCompletedInWindowPerSecond)}" r="${i ? 5 : 3}" fill="${r.issues.length ? "#d64b54" : color}"><title>${escape(stage)} c=${r.concurrency} repeat=${r.repeat} goodput=${r.correctCompletedInWindowPerSecond} issues=${escape(r.issues.join(","))}</title></circle>`).join("")}`;
            })
            .join("")}</g></svg>`;
        await writeFile(join(out, "charts", name), svg + "\n", { flag: "wx" });
        curves.push(
            `<section><h3>${escape(c.caseId)}</h3><p>${escape(c.selection?.reason ?? "not measured")} · ${state.settings.confirmRepeats}-window confirmation observation: ${c.highestObservedConfirmedGoodput ?? "unknown"} req/s</p>${svg}<p><button onclick="const s=this.closest('section').querySelector('svg');const u=window.URL.createObjectURL(new Blob([s.outerHTML],{type:'image/svg+xml'}));const a=document.createElement('a');a.href=u;a.download='${name}';document.body.append(a);a.click();a.remove();setTimeout(()=>window.URL.revokeObjectURL(u),1000)">Export SVG</button></p></section>`,
        );
    }
    const html = await readFile(join(out, "report.html"), "utf8");
    const diagnosticHtml = diagnostics
        ? `<details><summary>目标 GC / swap：逐窗诊断（不改变性能资格）</summary><p>${escape(diagnostics.summary.association)}. ${escape(diagnostics.summary.scope)}</p><p>以下 GC 数量仅来自已提供日志；日志完整性、目标身份和外部时钟均未验证，0 不代表没有 GC。swap 为有界间隔采样。</p><div class="table"><table><thead><tr><th>case / stage</th><th>load / repeat</th><th>phase</th><th>swap</th><th>logged GC pauses</th><th>overlap ms</th></tr></thead><tbody>${diagnostics.summary.rows.map((row) => `<tr><td>${escape(row.caseId)} / ${escape(row.stage)}</td><td>${escape(row.loadId)} / ${escape(row.repeat)}</td><td>${escape(row.phase)}</td><td>${escape(row.swap?.status ?? row.reason)}</td><td>${escape(row.gc?.loggedPauses ?? "unknown")}</td><td>${escape(row.gc?.loggedPauseOverlapMs ?? "unknown")}</td></tr>`).join("")}</tbody></table></div><pre style="white-space:pre-wrap;overflow-wrap:anywhere">${escape(JSON.stringify(diagnostics.summary.logs, null, 2))}</pre></details>`
        : "";
    const imported = plan.profile.graph?.imported;
    const importHtml = imported
        ? `<p><strong>Imported edges: ${((100 * imported.snapshot.edges) / imported.snapshot.expectedEdges).toFixed(3)}% (${imported.snapshot.edges} / ${imported.snapshot.expectedEdges}); required: ${100 * imported.minimumEdgeCoverage}%.</strong> Basis: Loader write acknowledgements. Full edge content verified: ${imported.snapshot.edgeContentVerified}. Compare only the same recorded import snapshot.</p>`
        : "";
    const intro = `<div style="max-width:1200px;margin:24px auto;padding:24px;font:15px sans-serif;background:#f3f7fc"><h2>${title} ${escape(state.preset)} / throughput v0.2</h2><p>Status: ${escape(result.status)} · calibration: ${escape(state.calibration?.status ?? "unknown")} · performanceAccepted: false</p><p>Closed-loop observations, same existing endpoint. Confirmation uses fresh measurement windows; repeats share the same service JVM unless externally changed. Independent JVM repeats, balanced candidate order and A/A noise qualification are not established by this entry. Red points retain quality issues. No business SLO capacity or engine-only claim. ${diagnostics ? "Target GC/swap diagnostic inputs are attached below; formal window qualification remains unknown." : "Target GC/swap window evidence is not attached by this entry."}</p><p>${escape(state.failure?.message ?? "")} Missing: ${escape(JSON.stringify(state.incomplete))}</p>${importHtml}${curves.join("")}${diagnosticHtml}${graph ? `<details><summary>Dataset / reference / parameter coverage</summary><pre style="white-space:pre-wrap;overflow-wrap:anywhere">${escape(JSON.stringify({ graph: plan.profile.graph, pools: result.cases.map((c) => ({ caseId: c.caseId, windows: c.rows.map((r) => ({ stage: r.stage, loadId: r.loadId, repeat: r.repeat, ...r.parameters })) })) }, null, 2))}</pre></details>` : ""}</div>`;
    await writeFile(join(out, "report.html"), html.replace(/(<body[^>]*>)/, "$1" + intro));
    return {
        status: result.status,
        reportJson: resolve(out, "report.json"),
        reportHtml: resolve(out, "report.html"),
        performanceAccepted: false,
    };
}
