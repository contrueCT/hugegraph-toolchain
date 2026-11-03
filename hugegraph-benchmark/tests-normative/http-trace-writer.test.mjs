/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { setImmediate as turn } from "node:timers/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
    TraceWriter,
    openTraceWriter,
    validateTraceWriter,
    TRACE_QUEUE_BYTES,
} from "../components/http/trace-writer.mjs";

function sink(patch = {}) {
    const chunks = [];
    let synced = 0,
        closed = 0;
    return {
        chunks,
        get synced() {
            return synced;
        },
        get closed() {
            return closed;
        },
        async write(buffer, offset, length) {
            chunks.push(Buffer.from(buffer.subarray(offset, offset + length)));
            return { bytesWritten: length };
        },
        async sync() {
            synced++;
        },
        async close() {
            closed++;
        },
        ...patch,
    };
}

test("trace persistence can remain pending while event-loop work proceeds; partial writes preserve record order", async () => {
    let release;
    const gate = new Promise((r) => {
        release = r;
    });
    let calls = 0;
    const f = sink({
        async write(buffer, offset, length) {
            if (!calls++) await gate;
            const n = Math.min(length, 3);
            this.chunks.push(Buffer.from(buffer.subarray(offset, offset + n)));
            return { bytesWritten: n };
        },
    });
    const writer = new TraceWriter(f, 4096);
    writer.append({ sequence: 0 });
    writer.append({ sequence: 1 });
    await turn();
    assert.equal(calls, 1);
    assert.equal(writer.stats.persistedBytes, 0);
    assert.equal(writer.stats.records, 2);
    release();
    const stats = await writer.finish();
    assert.equal(Buffer.concat(f.chunks).toString(), '{"sequence":0}\n{"sequence":1}\n');
    assert.equal(stats.persistedBytes, stats.enqueuedBytes);
    assert(stats.writeCalls > 2);
    assert.equal(f.synced, 1);
    assert.deepEqual(
        validateTraceWriter({ traceWriter: stats, phases: [{ scheduled: 2 }] }, stats.persistedBytes),
        stats,
    );
    await writer.close();
    await writer.close();
    assert.equal(f.closed, 1);
});

test("pending budget includes the in-flight write and overflow fails without silently losing evidence", async () => {
    let release,
        failures = 0;
    const gate = new Promise((r) => {
        release = r;
    });
    const f = sink({
        async write(buffer, offset, length) {
            await gate;
            return { bytesWritten: length };
        },
    });
    const writer = new TraceWriter(f, 4 * TRACE_QUEUE_BYTES, () => {
        failures++;
    });
    writer.append({ text: "x".repeat(TRACE_QUEUE_BYTES - 20) });
    await turn();
    assert.throws(() => writer.append({ text: "x".repeat(100) }), /pending byte budget/);
    assert.equal(failures, 1);
    release();
    await assert.rejects(writer.finish(), /pending byte budget/);
    assert.equal(writer.stats.complete, false);
    assert.equal(f.synced, 0);
    await writer.close();
});

test("write failure, zero-length write and fsync failure are terminal and never produce a complete receipt", async () => {
    for (const patch of [
        {
            async write() {
                throw new Error("ENOSPC");
            },
        },
        {
            async write() {
                return { bytesWritten: 0 };
            },
        },
        {
            async sync() {
                throw new Error("EIO sync");
            },
        },
    ]) {
        let failures = 0;
        const f = sink(patch),
            writer = new TraceWriter(f, 4096, () => {
                failures++;
            });
        writer.append({ ok: true });
        await assert.rejects(writer.finish(), /ENOSPC|zero-length|EIO sync/);
        assert.equal(writer.stats.complete, false);
        assert.equal(failures, 1);
        assert.throws(() => writer.append({ more: true }));
        await writer.close();
        assert.equal(f.closed, 1);
    }
});

test("total trace budget, create-only file and successful final persistence remain enforced", async (t) => {
    const root = await mkdtemp(join(tmpdir(), "hgbench-trace-writer-"));
    t.after(() => rm(root, { recursive: true, force: true }));
    const path = join(root, "trace.ndjson"),
        writer = await openTraceWriter(path, 100);
    writer.append({ value: "汉字" });
    const stats = await writer.finish();
    await writer.close();
    const bytes = await readFile(path);
    assert.equal(bytes.length, stats.persistedBytes);
    assert.equal(bytes.toString(), '{"value":"汉字"}\n');
    await assert.rejects(openTraceWriter(path, 100), /EEXIST/);
    const small = new TraceWriter(sink(), 2);
    assert.throws(() => small.append({ too: "large" }), /Trace byte budget/);
    await assert.rejects(small.finish(), /Trace byte budget/);
    await small.close();
});

test("receipt rejects changed counts, unfinished writes, mixed legacy cost, invalid limits and times", async () => {
    const writer = new TraceWriter(sink(), 4096);
    writer.append({ ok: true });
    const traceWriter = await writer.finish();
    const run = { traceWriter, phases: [{ scheduled: 1 }] };
    for (const patch of [
        { complete: false },
        { persistedBytes: 0 },
        { records: 0 },
        { maximumPendingBytes: TRACE_QUEUE_BYTES + 1 },
        { peakPendingBytes: 5000 },
        { finalSyncMs: -1 },
    ])
        assert.throws(() =>
            validateTraceWriter({ ...run, traceWriter: { ...traceWriter, ...patch } }, traceWriter.persistedBytes),
        );
    assert.throws(() => validateTraceWriter({ ...run, evidenceWriteMs: 0 }, traceWriter.persistedBytes), /ambiguous/);
    assert.equal(validateTraceWriter({ evidenceWriteMs: 12 }, 20), null);
    await writer.close();
});
