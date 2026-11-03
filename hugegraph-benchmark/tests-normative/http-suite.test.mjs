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

import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, readFile, writeFile, cp, rm, stat, symlink } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import http from "node:http";
import { inspect, answerFor, expectedData, directory } from "../benchmarks/http-serving-v0.1/reference.mjs";
import { inspectHttpSuite, generateHttpProfile } from "../dist/http-suite.js";
import { inspectHttpProfile, planHttp } from "../dist/http.js";
import { httpAnswerCheckSchema } from "../dist/http-contracts.js";
import { submitTask, waitTask } from "../dist/tasks.js";
import { analyze, report } from "../components/http/analyzer.mjs";
import { digest, expectedCheckDigest } from "../components/http/common.mjs";

const cli = promisify(execFile);
const settings = async () => JSON.parse(await readFile(resolve("examples/http-suite-functional.json"), "utf8"));
async function temp(t) {
    const path = await mkdtemp(join(tmpdir(), "hgbench-http-suite-"));
    t.after(() => rm(path, { recursive: true, force: true }));
    return path;
}
test("HTTP suite freezes all 11 independent answers, shapes and size definitions", async () => {
    const suite = await inspectHttpSuite();
    assert.equal(suite.questions.length, 11);
    assert.equal(new Set(suite.cases.map((c) => c.id)).size, 11);
    const large = suite.questions.find((q) => q.id === "string-1024k");
    assert.equal(large.answer.resultDataCanonicalBytes, 1048580);
    assert.equal(large.answer.payloadUtf8Bytes, 1048576);
    assert.equal(large.answer.resultRows, 1);
    const rows = suite.questions.find((q) => q.id === "rows-4096");
    assert.equal(rows.answer.resultRows, 4096);
    assert(rows.answer.resultDataCanonicalBytes > 200000);
    const unicode = suite.questions.find((q) => q.id === "string-unicode");
    assert(unicode.answer.resultDataCanonicalBytes > unicode.answer.payloadUtf8Bytes);
    for (const q of suite.questions) assert.deepEqual(answerFor(q), q.answer);
    assert.equal(suite.performanceAccepted, false);
    assert.equal(suite.calibration.formalParameters.warmupMs, null);
});
test("HTTP suite rejects changed files, answer locks, duplicate/omitted paths and symlinks", async (t) => {
    const root = await temp(t),
        folder = join(root, "bundle");
    await cp(directory, folder, { recursive: true });
    const catalog = join(folder, "catalog.json"),
        old = await readFile(catalog);
    await writeFile(catalog, Buffer.concat([old, Buffer.from(" ")]));
    await assert.rejects(inspect(folder), /file mismatch/);
    await writeFile(catalog, old);
    const lockPath = join(folder, "bundle.lock.json"),
        lock = JSON.parse(await readFile(lockPath, "utf8"));
    const savedLock = await readFile(lockPath);
    lock.files.push(lock.files[0]);
    await writeFile(lockPath, JSON.stringify(lock));
    await assert.rejects(inspect(folder), /inventory/);
    await writeFile(lockPath, savedLock);
    await rm(catalog);
    await symlink(join(directory, "catalog.json"), catalog);
    await assert.rejects(inspect(folder), /file mismatch/);
});
test("HTTP digest checks reject ambiguous expectations and preserve exact types/order/duplicates", () => {
    const data = [1, "1", true, null, { b: 2, a: [1, 1] }];
    const expected = expectedCheckDigest({ pointer: "", equals: data });
    assert.equal(expectedCheckDigest({ pointer: "", canonicalSha256: expected }), expected);
    assert.notEqual(expectedCheckDigest({ equals: [...data].reverse() }), expected);
    assert.notEqual(expectedCheckDigest({ equals: ["1", "1", true, null, { a: [1, 1], b: 2 }] }), expected);
    assert.notEqual(expectedCheckDigest({ equals: [1, "1", true, null, { a: [1], b: 2 }] }), expected);
    assert.equal(expectedCheckDigest({ equals: [1, "1", true, null, { a: [1, 1], b: 2 }] }), expected);
    for (const bad of [
        { pointer: "" },
        { pointer: "", canonicalSha256: "x" },
        { pointer: "", equals: null, canonicalSha256: expected },
    ]) {
        assert.equal(httpAnswerCheckSchema.safeParse(bad).success, false);
        assert.throws(() => expectedCheckDigest(bad));
    }
});
test("HTTP generation is offline, exclusive, compact, coverage-bound and preserves evidence paths", async (t) => {
    const root = await temp(t),
        source = join(root, "settings.json"),
        out = join(root, "profile.json");
    const s = await settings();
    s.target.evidenceFiles = ["build-proof.json"];
    await writeFile(source, JSON.stringify(s));
    const dry = await generateHttpProfile(source, out, true);
    assert.equal(dry.written, false);
    await assert.rejects(stat(out), /ENOENT/);
    assert.equal(dry.profile.benchmark.coverage, "full");
    assert.equal(dry.profile.target.evidenceFiles[0], join(root, "build-proof.json"));
    assert(Buffer.byteLength(JSON.stringify(dry.profile)) < 12000);
    await generateHttpProfile(source, out, false);
    await inspectHttpProfile(out);
    await assert.rejects(
        generateHttpProfile(source, out, false),
        (e) => e.code === "HTTP_PROFILE_EXISTS" && e.exitCode === 4,
    );
    for (const [patch, code] of [
        [{ caseIds: ["unknown"] }, "HTTP_SUITE_CASE_UNKNOWN"],
        [{ caseIds: [s.caseIds[0], s.caseIds[0]] }, "HTTP_DUPLICATE_ID"],
        [{ maxResponseBytes: 1048576 }, "HTTP_SUITE_RESPONSE_BUDGET"],
        [{ endpoint: "http://127.0.0.1:1/not-gremlin" }, "HTTP_SUITE_ENDPOINT"],
        [{ endpoint: "http://user:secret@127.0.0.1:1/gremlin" }, "HTTP_ENDPOINT_INVALID"],
        [{ maxRequestsPerPhase: 1 }, "HTTP_REQUEST_BUDGET"],
    ]) {
        await writeFile(source, JSON.stringify({ ...s, ...patch }));
        await assert.rejects(generateHttpProfile(source, out, true), (e) => e.code === code);
    }
    const modified = JSON.parse(await readFile(out, "utf8"));
    modified.cases[0].body.gremlin = "1";
    await writeFile(out, JSON.stringify(modified));
    await assert.rejects(inspectHttpProfile(out), (e) => e.code === "HTTP_SUITE_PROFILE_MISMATCH");
});
test("HTTP suite public discovery/help/schema and release exit code are agent-readable", async () => {
    const run = async (args) => JSON.parse((await cli(process.execPath, [resolve("dist/cli.js"), ...args])).stdout);
    const list = await run(["benchmark", "list"]);
    assert(list.data.some((s) => s.id === "hgbench-http-serving-v0.1"));
    assert.equal((await run(["http", "suite", "--id", "string-1024k"])).data.id, "string-1024k");
    assert.equal((await run(["schema", "http-suite-settings"])).ok, true);
    assert.match(
        (await cli(process.execPath, [resolve("dist/cli.js"), "http", "generate", "--help"])).stdout,
        /--dry-run/,
    );
    await assert.rejects(cli(process.execPath, [resolve("dist/cli.js"), "http", "suite", "--require-release"]), (e) => {
        assert.equal(e.code, 3);
        assert.equal(JSON.parse(e.stdout).error.code, "HTTP_SUITE_RELEASE_NOT_READY");
        return true;
    });
});
test("HTTP suite task carries normative evidence through real sockets and rebuilds offline without executing it", async (t) => {
    const root = await temp(t),
        suite = await inspect();
    const server = http.createServer(async (req, res) => {
        let body = "";
        for await (const c of req) body += c;
        const request = JSON.parse(body);
        const question = suite.questions.find((q) => JSON.stringify(q.body) === JSON.stringify(request));
        if (!question) {
            res.writeHead(400);
            res.end("{}");
            return;
        }
        res.setHeader("content-type", "application/json");
        res.end(JSON.stringify({ status: { code: 200 }, result: { data: expectedData(question) } }));
    });
    await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
    t.after(async () => {
        server.closeAllConnections();
        await new Promise((resolve) => server.close(resolve));
    });
    const source = join(root, "settings.json"),
        profile = join(root, "profile.json"),
        attempt = join(root, "attempt");
    const s = await settings();
    s.endpoint = `http://127.0.0.1:${server.address().port}/gremlin`;
    s.caseIds = ["scalar-add", "rows-0016", "string-1024k", "string-unicode"];
    await writeFile(source, JSON.stringify(s));
    const generated = await generateHttpProfile(source, profile, false);
    assert.equal(generated.profile.benchmark.coverage, "subset");
    const planned = await planHttp(profile, attempt),
        frozen = JSON.parse(await readFile(planned.taskPlan, "utf8"));
    const task = await submitTask(join(root, "tasks"), frozen, "http-suite-functional");
    const terminal = await waitTask(join(root, "tasks"), task.taskId, 20000);
    assert.equal(terminal.state, "completed", JSON.stringify(terminal));
    const raw = join(attempt, "output"),
        first = await analyze(raw);
    assert.equal(first.benchmark.coverage, "subset");
    assert.equal(first.benchmark.suiteId, suite.suiteId);
    assert.equal(first.runs.length, 4);
    assert.deepEqual(
        first.serviceChecks.map((c) => c.phase),
        ["preflight", "postcheck"],
    );
    assert(first.serviceChecks.every((c) => c.passed && c.cases.length === 4));
    assert(first.runs.every((r) => r.failed === 0 && r.correct === 2));
    assert.equal(first.performanceAccepted, false);
    await report(raw, join(root, "rebuilt"));
    assert.equal(await digest(join(raw, "analysis/report.json")), await digest(join(root, "rebuilt/report.json")));
    assert.equal(await digest(join(raw, "analysis/report.html")), await digest(join(root, "rebuilt/report.html")));
    // Re-locking a modified raw profile cannot bypass the independently bound normative request.
    const p = JSON.parse(await readFile(join(raw, "profile.json"), "utf8"));
    p.cases[0].checks[1].canonicalSha256 = "0".repeat(64);
    await writeFile(join(raw, "profile.json"), JSON.stringify(p));
    const lockPath = join(raw, "raw.lock.json"),
        lock = JSON.parse(await readFile(lockPath, "utf8"));
    const entry = lock.files.find((f) => f.path === "profile.json");
    entry.bytes = (await stat(join(raw, "profile.json"))).size;
    entry.sha256 = await digest(join(raw, "profile.json"));
    await writeFile(lockPath, JSON.stringify(lock));
    await assert.rejects(analyze(raw), /benchmark question\/answer\/coverage mismatch|Probe answer\/accounting/);
});
