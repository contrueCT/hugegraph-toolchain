/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
// Private fixed observer. It accepts a bounded request and two fixed release frames only.
import { readFileSync, readlinkSync, openSync, fstatSync, readSync, closeSync } from "node:fs";
import { createHash } from "node:crypto";
import { performance } from "node:perf_hooks";
const FLAGS = [
    "LD_PRELOAD",
    "LD_AUDIT",
    "LD_LIBRARY_PATH",
    "NODE_OPTIONS",
    "PYTHONHOME",
    "PYTHONPATH",
    "PYTHONSTARTUP",
    "PYTHONINSPECT",
    "PYTHONUSERBASE",
];
const need = (value, code) => {
    if (!value) throw new Error(code);
};
function text(path, maximum) {
    const raw = readFileSync(path);
    need(raw.length <= maximum, "PROBE_RAW_BUDGET");
    return raw.toString("utf8");
}
function metadata(s) {
    return {
        bytes: Number(s.size),
        device: s.dev.toString(),
        inode: s.ino.toString(),
        mtimeNs: s.mtimeNs.toString(),
        ctimeNs: s.ctimeNs.toString(),
    };
}
function executable(maximum, hash) {
    const fd = openSync("/proc/self/exe", "r");
    try {
        const before = fstatSync(fd, { bigint: true });
        need(before.isFile() && before.size > 0n && before.size <= BigInt(maximum), "PROBE_EXECUTABLE_BUDGET");
        const result = metadata(before);
        if (hash) {
            const digest = createHash("sha256"),
                block = Buffer.alloc(1024 * 1024);
            let bytes = 0;
            while (true) {
                const count = readSync(fd, block, 0, block.length, null);
                if (!count) break;
                bytes += count;
                need(bytes <= maximum, "PROBE_HASH_BUDGET");
                digest.update(block.subarray(0, count));
            }
            need(bytes === result.bytes, "PROBE_EXECUTABLE_CHANGED");
            result.sha256 = digest.digest("hex");
        }
        need(
            JSON.stringify(metadata(fstatSync(fd, { bigint: true }))) === JSON.stringify(metadata(before)),
            "PROBE_EXECUTABLE_CHANGED",
        );
        return result;
    } finally {
        closeSync(fd);
    }
}
function identity() {
    need(process.platform === "linux", "PROBE_LINUX_REQUIRED");
    const statRaw = text("/proc/self/stat", 4096),
        tail = statRaw
            .slice(statRaw.lastIndexOf(")") + 2)
            .trim()
            .split(/\s+/);
    need(
        statRaw.startsWith(process.pid + " (") &&
            tail.length >= 20 &&
            /^\d+$/.test(tail[19]) &&
            !["Z", "X", "x"].includes(tail[0]),
        "PROBE_STAT",
    );
    return {
        pid: process.pid,
        parentPid: process.ppid,
        statRaw,
        startTimeTicks: tail[19],
        bootId: text("/proc/sys/kernel/random/boot_id", 128).trim(),
        namespaces: Object.fromEntries(
            ["time", "time_for_children", "pid", "mnt", "user"].map((name) => [
                name,
                readlinkSync("/proc/self/ns/" + name),
            ]),
        ),
        timeOffsetsRaw: text("/proc/self/timens_offsets", 256),
        clock: {
            api: "process.hrtime.bigint",
            nowNs: process.hrtime.bigint().toString(),
            performanceNowMs: performance.now(),
            performanceTimeOriginMs: performance.timeOrigin,
        },
        runtime: {
            name: "node",
            version: process.version,
            uv: process.versions.uv,
            platform: process.platform,
            arch: process.arch,
            nodeSharedLibuv: process.config.variables.node_shared_libuv ?? null,
            reportedExecutable: process.execPath,
            execArgvSha256: createHash("sha256").update(JSON.stringify(process.execArgv)).digest("hex"),
            injectionEnvironmentPresent: Object.fromEntries(FLAGS.map((key) => [key, Boolean(process.env[key])])),
        },
    };
}
function emit(value) {
    const raw = JSON.stringify(value) + "\n";
    need(Buffer.byteLength(raw) <= 32768, "PROBE_FRAME_BUDGET");
    process.stdout.write(raw);
}
let buffer = Buffer.alloc(0),
    waiter;
process.stdin.on("data", (chunk) => {
    buffer = Buffer.concat([buffer, chunk]);
    if (buffer.length > 1024) {
        process.exitCode = 6;
        process.exit();
    }
    if (waiter && buffer.includes(10)) waiter();
});
function action(kind, token, timeout) {
    return new Promise((accept, reject) => {
        const timer = setTimeout(() => {
            waiter = undefined;
            reject(new Error("PROBE_HANDSHAKE_DEADLINE"));
        }, timeout);
        const receive = () => {
            try {
                need(buffer.at(-1) === 10 && buffer.indexOf(10) === buffer.length - 1, "PROBE_HANDSHAKE_FRAME");
                const value = JSON.parse(buffer.toString("utf8"));
                need(
                    JSON.stringify(Object.keys(value).sort()) === JSON.stringify(["kind", "token"]) &&
                        value.kind === kind &&
                        value.token === token,
                    "PROBE_HANDSHAKE_IDENTITY",
                );
                buffer = Buffer.alloc(0);
                clearTimeout(timer);
                waiter = undefined;
                accept();
            } catch (error) {
                clearTimeout(timer);
                waiter = undefined;
                reject(error);
            }
        };
        waiter = receive;
        if (buffer.includes(10)) receive();
    });
}
let watchdog;
try {
    need(process.argv.length === 4 && process.argv[2] === "--request", "PROBE_REQUEST_ARGUMENTS");
    const request = JSON.parse(text(process.argv[3], 4096));
    need(
        JSON.stringify(Object.keys(request).sort()) ===
            JSON.stringify(
                [
                    "schemaVersion",
                    "kind",
                    "role",
                    "token",
                    "runtimeDigest",
                    "wallMs",
                    "handshakeMs",
                    "maxExecutableBytes",
                ].sort(),
            ) &&
            request.schemaVersion === 1 &&
            request.kind === "http-runtime-observer-request" &&
            request.role === "host-node" &&
            /^[a-f0-9]{32}$/.test(request.token) &&
            /^[a-f0-9]{64}$/.test(request.runtimeDigest),
        "PROBE_REQUEST",
    );
    need(
        Number.isSafeInteger(request.wallMs) &&
            request.wallMs >= 5000 &&
            request.wallMs <= 120000 &&
            Number.isSafeInteger(request.handshakeMs) &&
            request.handshakeMs >= 500 &&
            request.handshakeMs <= 20000 &&
            Number.isSafeInteger(request.maxExecutableBytes) &&
            request.maxExecutableBytes > 0 &&
            request.maxExecutableBytes <= 512 * 1024 ** 2,
        "PROBE_REQUEST_BUDGET",
    );
    watchdog = setTimeout(() => process.exit(6), request.wallMs);
    emit({ kind: "ready", request, identity: identity(), executable: executable(request.maxExecutableBytes, true) });
    await action("observe", request.token, request.handshakeMs);
    emit({ kind: "final", request, identity: identity(), executable: executable(request.maxExecutableBytes, false) });
    await action("finish", request.token, request.handshakeMs);
} catch {
    process.stderr.write("PROBE_NODE_OBSERVER_FAILED\n");
    process.exitCode = 6;
} finally {
    clearTimeout(watchdog);
    process.stdin.destroy();
}
