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

import { readFile, stat, realpath } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { analyze } from "./analyzer.mjs";
import { analyzeWithPhaseWindows } from "./phase-windows.mjs";
import { buildView, writeBundle, singleReport } from "./report-view.mjs";
import { buildInfo } from "./build-info.mjs";

const fail = (code, message, details) => {
    throw Object.assign(new Error(message), { code, details });
};
const need = (value, message) => {
    if (!value) fail("REPORT_INPUT", message);
};
const id = (value) => typeof value === "string" && /^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,63}$/.test(value);
const str = (value, max = 256) => typeof value === "string" && value.length > 0 && value.length <= max;
function fields(value, allowed, where) {
    need(value && typeof value === "object" && !Array.isArray(value), `${where} must be an object`);
    need(
        Object.keys(value).every((k) => allowed.includes(k)),
        `${where}: unknown fields (accepted: ${allowed.join(", ")})`,
    );
}
async function jsonFile(path) {
    try {
        const s = await stat(path);
        need(s.isFile() && s.size <= 1024 * 1024, `JSON must be a regular file <=1 MiB: ${path}`);
        return JSON.parse(await readFile(path, "utf8"));
    } catch (e) {
        if (e.code === "REPORT_INPUT") throw e;
        fail("REPORT_INPUT", `Cannot read JSON ${path}: ${e.message}`);
    }
}
async function evidenceAt(path) {
    // Accept documented run roots, never follow report.json's claims or arbitrary paths within it.
    const candidates = [resolve(path), resolve(path, "evidence"), resolve(path, "output")],
        found = [];
    for (const dir of candidates)
        try {
            if ((await stat(join(dir, "raw.lock.json"))).isFile()) found.push(await realpath(dir));
        } catch (e) {
            if (!["ENOENT", "ENOTDIR"].includes(e.code)) throw e;
        }
    need(found.length === 1, `Expected one raw.lock.json in ${path}, evidence/ or output/; found ${found.length}`);
    return found[0];
}
async function loadEvents(path, inputs) {
    if (!path) return [];
    const events = await jsonFile(path);
    need(Array.isArray(events) && events.length <= 256, "events must be an array of at most 256 annotations");
    for (const e of events) {
        fields(e, ["inputId", "kind", "message", "source", "startUtc", "endUtc", "caseId", "loadId", "runId"], "event");
        need(
            inputs.some((i) => i.id === e.inputId),
            `Unknown event inputId: ${e.inputId}`,
        );
        need(str(e.message, 2000) && str(e.source, 1024), "Each event requires a message and source reference");
        if (e.kind !== undefined) need(str(e.kind, 64), "Invalid event kind");
        for (const k of ["caseId", "loadId", "runId"]) if (e[k] !== undefined) need(id(e[k]), `Invalid event ${k}`);
        for (const k of ["startUtc", "endUtc"])
            if (e[k] !== undefined)
                need(
                    str(e[k], 40) && /Z$/.test(e[k]) && Number.isFinite(Date.parse(e[k])),
                    `Invalid event ${k}; use UTC ending Z`,
                );
        need(
            !e.endUtc || (e.startUtc && Date.parse(e.endUtc) >= Date.parse(e.startUtc)),
            "Event endUtc requires startUtc and must not precede it",
        );
    }
    return events;
}
async function inputsFor(options, single) {
    let base = process.cwd(),
        specification;
    if (single)
        specification = {
            title: options.title,
            baseline: "run",
            groups: [{ id: "run", runs: [{ id: "run", path: options.directory }] }],
        };
    else if (options.manifest) {
        need(!options.input?.length, "Use --manifest or --input, not both");
        const path = resolve(options.manifest);
        base = dirname(path);
        specification = await jsonFile(path);
    } else {
        need(
            Array.isArray(options.input) && options.input.length > 0,
            "Provide --manifest or repeated --input group=path",
        );
        const groups = new Map();
        let n = 0;
        for (const item of options.input) {
            const at = item.indexOf("=");
            need(at > 0 && at < item.length - 1, "--input requires group=path");
            const group = item.slice(0, at),
                path = item.slice(at + 1);
            need(id(group), "Group must be 1–64 ASCII letters/digits/._- and start with a letter/digit");
            if (!groups.has(group)) groups.set(group, { id: group, runs: [] });
            groups.get(group).runs.push({ id: `input-${++n}`, path });
        }
        specification = { groups: [...groups.values()] };
    }
    fields(specification, ["title", "baseline", "groups", "changedFactors", "eventsFile"], "manifest");
    need(
        Array.isArray(specification.groups) &&
            specification.groups.length >= (single ? 1 : 2) &&
            specification.groups.length <= 8,
        "Use 2–8 groups (one for http report)",
    );
    if (specification.title !== undefined) need(str(specification.title, 160), "title must contain 1–160 characters");
    if (specification.changedFactors !== undefined)
        need(
            Array.isArray(specification.changedFactors) &&
                specification.changedFactors.length <= 16 &&
                specification.changedFactors.every((x) => str(x, 160)),
            "Invalid changedFactors",
        );
    const inputs = [],
        groups = new Set(),
        ids = new Set();
    for (const g of specification.groups) {
        fields(g, ["id", "label", "runs"], "group");
        need(id(g.id) && !groups.has(g.id), "Group IDs must be valid and unique");
        groups.add(g.id);
        if (g.label !== undefined) need(str(g.label, 80), "Group label must contain 1–80 characters");
        need(Array.isArray(g.runs) && g.runs.length > 0 && g.runs.length <= 32, "Each group requires 1–32 runs");
        for (const r of g.runs) {
            fields(r, ["id", "path", "round"], "run");
            need(id(r.id) && !ids.has(r.id), "Run IDs must be valid and unique");
            ids.add(r.id);
            need(str(r.path, 4096), "Each run needs its raw directory path");
            if (r.round !== undefined) need(str(r.round, 64), "Invalid round label");
            inputs.push({
                id: r.id,
                group: g.id,
                label: g.label ?? g.id,
                round: r.round,
                directory: resolve(base, r.path),
            });
        }
    }
    need(inputs.length <= 32, "At most 32 raw inputs are supported; split comparisons");
    const baseline = options.baseline ?? specification.baseline ?? inputs[0].group;
    need(groups.has(baseline), "Baseline must name an input group");
    if (options.events && specification.eventsFile) fail("REPORT_INPUT", "Choose CLI --events or manifest eventsFile");
    if (specification.eventsFile !== undefined) need(str(specification.eventsFile, 4096), "Invalid eventsFile");
    const eventsPath = options.events
        ? resolve(options.events)
        : specification.eventsFile
          ? resolve(base, specification.eventsFile)
          : null;
    return {
        inputs,
        viewOptions: {
            title: options.title ?? specification.title,
            baseline,
            changedFactors: specification.changedFactors,
            events: await loadEvents(eventsPath, inputs),
        },
    };
}
export async function createReport(options, single = false) {
    need(options.dryRun || str(options.out, 4096), "--out is required unless --dry-run is used");
    if (options.windowMs !== undefined)
        need(
            Number.isSafeInteger(options.windowMs) && options.windowMs >= 1 && options.windowMs <= 300000,
            "window-ms must be an integer in 1..300000",
        );
    const { inputs, viewOptions } = await inputsFor(options, single),
        digests = new Set();
    for (const input of inputs) {
        try {
            input.directory = await evidenceAt(input.directory);
            if (options.windows)
                Object.assign(
                    input,
                    await analyzeWithPhaseWindows(input.directory, {
                        windowMs: options.windowMs ?? 1000,
                        minimumRequestsPerWindow: 100,
                        minimumP99Requests: 1000,
                    }),
                );
            else input.analysis = await analyze(input.directory);
        } catch (e) {
            fail("REPORT_EVIDENCE", `Input ${input.id} (${input.directory}): ${e.message}`, {
                inputId: input.id,
                cause: e.code ?? "RAW_VERIFICATION",
            });
        }
        const sha = input.analysis.rawManifestSha256;
        if (digests.has(sha))
            fail(
                "COMPARE_DUPLICATE_RAW",
                `Duplicate raw evidence: ${input.id}; each raw manifest may be included once`,
                { inputId: input.id, sha256: sha },
            );
        digests.add(sha);
    }
    // Fail typos in selectors, but allow a planned/missing run to carry an input-level note.
    for (const e of viewOptions.events) {
        const a = inputs.find((i) => i.id === e.inputId).analysis;
        if (e.caseId)
            need(
                a.profile.cases.some((c) => c.id === e.caseId),
                `Unknown event caseId ${e.caseId}`,
            );
        if (e.loadId)
            need(
                a.profile.loads.some((l) => l.id === e.loadId),
                `Unknown event loadId ${e.loadId}`,
            );
        if (e.runId)
            need(
                a.runs.some((r) => r.id === e.runId),
                `Unknown event runId ${e.runId}`,
            );
    }
    if (single) {
        if (!viewOptions.title) viewOptions.title = inputs[0].analysis.name;
        inputs[0].label = inputs[0].analysis.target.label || inputs[0].analysis.name;
    }
    const view = await buildView(inputs, viewOptions);
    const inspection = {
        groups: view.groups,
        inputs: view.inputs,
        cells: view.cells.map((c) => ({
            caseId: c.caseId,
            loadId: c.loadId,
            fingerprint: c.fingerprint,
            alignment: c.alignment,
            groups: c.groups.map((g) => ({
                id: g.id,
                presentInProfile: g.presentInProfile,
                expected: g.expected,
                verified: g.runs.length,
                missing: g.missing,
            })),
        })),
        events: view.events.length,
        performanceAccepted: false,
    };
    if (options.dryRun) return { dryRun: true, ...inspection };
    const original = single ? singleReport(inputs[0].analysis, view) : view;
    return {
        ...(await writeBundle(options.out, view, original)),
        groups: view.groups.length,
        inputs: inputs.length,
        cells: view.cells.length,
    };
}
if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
    try {
        const action = process.argv[2],
            options = JSON.parse(process.argv[3] ?? "{}");
        need(["compare", "report", "version"].includes(action), "Expected compare, report or version");
        const data = action === "version" ? await buildInfo() : await createReport(options, action === "report");
        process.stdout.write(JSON.stringify({ ok: true, data }) + "\n");
    } catch (e) {
        process.stdout.write(
            JSON.stringify({
                ok: false,
                error: { code: e.code ?? "REPORT_FAILED", message: e.message, details: e.details },
            }) + "\n",
        );
        process.exitCode = 2;
    }
}
