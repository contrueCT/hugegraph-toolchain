/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
import { open } from "node:fs/promises";
import { performance } from "node:perf_hooks";

export const TRACE_QUEUE_BYTES = 1024 * 1024;
const MODE = "async-bounded-v1";
const need = (value, message) => {
    if (!value) throw new Error(`HTTP_TRACE_WRITER: ${message}`);
};

// append is synchronous and bounded; disk writes are serialized on the async fs API.
// Overflow fails the run instead of silently slowing arrivals or dropping evidence.
export class TraceWriter {
    constructor(file, maxBytes, onFailure = () => {}) {
        need(Number.isSafeInteger(maxBytes) && maxBytes >= 0, "invalid byte budget");
        this.file = file;
        this.maxBytes = maxBytes;
        this.onFailure = onFailure;
        this.queue = [];
        this.pendingBytes = 0;
        this.worker = null;
        this.error = null;
        this.accepting = true;
        this.closed = false;
        this.stats = {
            mode: MODE,
            maximumPendingBytes: Math.min(TRACE_QUEUE_BYTES, maxBytes),
            peakPendingBytes: 0,
            enqueuedBytes: 0,
            persistedBytes: 0,
            records: 0,
            writeCalls: 0,
            serializationAndEnqueueMs: 0,
            asynchronousWriteWaitMs: 0,
            finalFlushWaitMs: 0,
            finalSyncMs: 0,
            complete: false,
        };
    }

    fail(error) {
        if (this.error) return;
        this.error = error;
        this.accepting = false;
        this.queue.length = 0;
        this.onFailure(error);
    }

    append(record) {
        if (this.error) throw this.error;
        need(this.accepting, "writer already finishing");
        const started = performance.now();
        try {
            const line = Buffer.from(JSON.stringify(record) + "\n");
            need(this.stats.enqueuedBytes + line.length <= this.maxBytes, "Trace byte budget exhausted");
            need(this.pendingBytes + line.length <= this.stats.maximumPendingBytes, "pending byte budget exhausted");
            this.queue.push(line);
            this.pendingBytes += line.length;
            this.stats.enqueuedBytes += line.length;
            this.stats.records++;
            this.stats.peakPendingBytes = Math.max(this.stats.peakPendingBytes, this.pendingBytes);
            // Defer the writer so append's measured cost excludes asynchronous persistence.
            if (!this.worker) this.worker = Promise.resolve().then(() => this.drain());
        } catch (error) {
            this.fail(error);
            throw error;
        } finally {
            this.stats.serializationAndEnqueueMs += performance.now() - started;
        }
    }

    async drain() {
        try {
            while (this.queue.length && !this.error) {
                const line = this.queue.shift();
                let offset = 0;
                while (offset < line.length && !this.error) {
                    const started = performance.now();
                    let written;
                    try {
                        written = (await this.file.write(line, offset, line.length - offset, null)).bytesWritten;
                    } finally {
                        this.stats.asynchronousWriteWaitMs += performance.now() - started;
                        this.stats.writeCalls++;
                    }
                    need(
                        Number.isSafeInteger(written) && written > 0 && written <= line.length - offset,
                        "invalid or zero-length write",
                    );
                    offset += written;
                    this.stats.persistedBytes += written;
                }
                this.pendingBytes -= line.length;
            }
        } catch (error) {
            this.fail(error);
        } finally {
            this.worker = null;
        }
    }

    async finish() {
        this.accepting = false;
        const started = performance.now();
        if (this.worker) await this.worker;
        this.stats.finalFlushWaitMs += performance.now() - started;
        if (this.error) throw this.error;
        need(this.pendingBytes === 0 && this.stats.enqueuedBytes === this.stats.persistedBytes, "incomplete trace");
        const syncStarted = performance.now();
        try {
            await this.file.sync();
        } catch (error) {
            this.fail(error);
            throw error;
        } finally {
            this.stats.finalSyncMs += performance.now() - syncStarted;
        }
        this.stats.complete = true;
        return { ...this.stats };
    }

    async close() {
        this.accepting = false;
        if (this.worker) await this.worker;
        if (!this.closed) {
            this.closed = true;
            await this.file.close();
        }
    }
}

export async function openTraceWriter(path, maxBytes, onFailure) {
    need(Number.isSafeInteger(maxBytes) && maxBytes >= 0, "invalid byte budget");
    return new TraceWriter(await open(path, "wx", 0o600), maxBytes, onFailure);
}

// Absent metadata means the legacy synchronous writer; no invented old costs.
export function validateTraceWriter(run, traceBytes) {
    if (!Object.hasOwn(run, "traceWriter")) return null;
    const v = run.traceWriter;
    need(v && v.mode === MODE && v.complete === true, "missing completed writer receipt");
    need(!Object.hasOwn(run, "evidenceWriteMs"), "ambiguous synchronous and asynchronous cost metadata");
    for (const key of [
        "maximumPendingBytes",
        "peakPendingBytes",
        "enqueuedBytes",
        "persistedBytes",
        "records",
        "writeCalls",
    ])
        need(Number.isSafeInteger(v[key]) && v[key] >= 0, `invalid ${key}`);
    need(
        v.maximumPendingBytes <= TRACE_QUEUE_BYTES && v.peakPendingBytes <= v.maximumPendingBytes,
        "pending budget differs",
    );
    need(
        v.enqueuedBytes === traceBytes &&
            v.persistedBytes === traceBytes &&
            v.records === run.phases.reduce((sum, phase) => sum + phase.scheduled, 0),
        "trace receipt accounting differs",
    );
    need(v.writeCalls >= v.records && (traceBytes === 0 || v.peakPendingBytes > 0), "write receipt count differs");
    for (const key of ["serializationAndEnqueueMs", "asynchronousWriteWaitMs", "finalFlushWaitMs", "finalSyncMs"])
        need(Number.isFinite(v[key]) && v[key] >= 0, `invalid ${key}`);
    return structuredClone(v);
}
