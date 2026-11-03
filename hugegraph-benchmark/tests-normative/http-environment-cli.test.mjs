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
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { mkdtemp, mkdir, writeFile, readFile, rm, realpath } from "node:fs/promises";
import { join, resolve } from "node:path";
import { tmpdir } from "node:os";
import { planHttpEnvironment, httpEnvironmentAction } from "../dist/http-environment.js";
import { verifyTask } from "../dist/tasks.js";

const cli = promisify(execFile);
async function fixture(t) {
    const root = await realpath(await mkdtemp(join(tmpdir(), "hgbench-http-environment-")));
    t.after(() => rm(root, { recursive: true, force: true }));
    const dist = join(root, "dist"),
        jdk = join(root, "jdk");
    for (const [base, name, text, mode] of [
        [dist, "lib/hugegraph-test.jar", "fixture-jar", 0o600],
        [dist, "bin/jvm-module.options", "--add-opens=java.base/java.lang=ALL-UNNAMED\n", 0o600],
        [
            dist,
            "conf/gremlin-server.yaml",
            "channelizer: org.apache.tinkerpop.gremlin.server.channel.WsAndHttpChannelizer\ngraphs: {}\n",
            0o600,
        ],
        [dist, "scripts/empty-sample.groovy", "// fixture\n", 0o600],
        [dist, "conf/java-security.properties", "# fixture security properties\n", 0o600],
        [jdk, "bin/java", "#!/bin/sh\nexit 99\n", 0o700],
        [jdk, "bin/jcmd", "#!/bin/sh\nexit 99\n", 0o700],
        [jdk, "release", 'JAVA_VERSION="17.0.17"\n', 0o600],
        [jdk, "conf/security/java.security", "# fixture\n", 0o600],
    ]) {
        const file = join(base, name);
        await mkdir(join(file, ".."), { recursive: true });
        await writeFile(file, text, { mode });
    }
    const settings = {
        schemaVersion: 1,
        kind: "hugegraph-http-environment-settings",
        purpose: "functional",
        distribution: dist,
        javaHome: jdk,
        image: "sha256:" + "1".repeat(64),
        python: "/usr/local/bin/python3",
        dockerSocket: "/var/run/docker.sock",
        mode: "forward",
        budget: {
            cpus: 1,
            memoryBytes: 1073741824,
            pids: 128,
            wallSeconds: 300,
            startupSeconds: 60,
            shutdownSeconds: 5,
            maxAllocatedBytes: 67108864,
            maxLogicalBytes: 134217728,
            maxLogBytes: 1048576,
            minFreeBytes: 0,
            tmpfsBytes: 33554432,
            pollSeconds: 1,
        },
        jvm: {
            heapMinBytes: 16777216,
            heapMaxBytes: 268435456,
            maxDirectMemoryBytes: 67108864,
            activeProcessorCount: 1,
        },
        server: {
            gremlinPool: 2,
            bossThreads: 1,
            workerThreads: 2,
            maxWorkQueueSize: 64,
            httpWorkerThreads: 4,
            maxRoute: 4,
            timeoutSeconds: 30,
            idleTimeoutSeconds: 60,
            connectionMaxRequests: 10000,
            maxContentLengthBytes: 2097152,
            resultIterationBatchSize: 64,
        },
    };
    const file = join(root, "settings.json");
    await writeFile(file, JSON.stringify(settings));
    return { root, file, settings };
}
test("HTTP environment CLI plans a frozen generic task offline and status remains read-only", async (t) => {
    if (process.getuid?.() === 0) {
        t.skip("Environment plans require nonroot ownership");
        return;
    }
    const f = await fixture(t),
        attempt = join(f.root, "attempt");
    const result = await planHttpEnvironment(f.file, attempt);
    assert.equal(result.executed, false);
    assert.equal(result.actualModeVerified, false);
    const task = JSON.parse(await readFile(result.taskPlan, "utf8"));
    assert.equal(task.plan.effect, "database");
    assert.deepEqual(task.plan.command.slice(-3), ["serve", "--attempt", attempt]);
    assert(task.plan.trackedFiles.includes(join(attempt, "hugegraph-service.py")));
    await verifyTask(task);
    const status = await httpEnvironmentAction("status", attempt);
    assert.equal(status.state, "planned");
    assert.equal(status.container, null);
    await assert.rejects(readFile(join(attempt, "dispatch.json")), /ENOENT/);
    await assert.rejects(planHttpEnvironment(f.file, attempt), (e) => e.code === "HTTP_ENVIRONMENT_FAILED");
    await writeFile(join(attempt, "conf/rest-server.properties"), "changed");
    await assert.rejects(verifyTask(task));
    // Changed config cannot prevent reporting/stopping a planned, owned attempt.
    assert.equal((await httpEnvironmentAction("status", attempt)).state, "planned");
});
test("HTTP environment CLI exposes settings and lifecycle help with structured errors", async (t) => {
    const help = (await cli(process.execPath, [resolve("dist/cli.js"), "http", "environment", "--help"])).stdout;
    assert.match(help, /Container TTL/);
    const schema = JSON.parse(
        (await cli(process.execPath, [resolve("dist/cli.js"), "schema", "http-environment-settings"])).stdout,
    );
    assert.equal(schema.ok, true);
    const f = await fixture(t);
    f.settings.mode = "unknown";
    await writeFile(f.file, JSON.stringify(f.settings));
    await assert.rejects(planHttpEnvironment(f.file, join(f.root, "bad")), (e) => e.code === "SCHEMA_INVALID");
    await assert.rejects(readFile(join(f.root, "bad/plan.json")), /ENOENT/);
});
