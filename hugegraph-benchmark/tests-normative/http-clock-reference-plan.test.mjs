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
import { test } from "node:test";
import assert from "node:assert/strict";
import { httpProfileSchema } from "../dist/http-contracts.js";
import { validateHttpProfile, httpDeadline } from "../dist/http.js";
import { validateRunnerSanitySpec } from "../components/http/runner-sanity.mjs";

const reference = () => ({
    python: "/fixed/python3",
    spec: {
        schemaVersion: 1,
        kind: "http-runner-sanity-spec",
        sourceOrigin: "fixture",
        startupMs: 1000,
        exchangeMs: 500,
        finishMs: 500,
        wallMs: 10000,
        maxFrameBytes: 8192,
        maxRawBytes: 65536,
        maxStderrBytes: 2048,
    },
});
const profile = () => ({
    schemaVersion: 1,
    kind: "http-benchmark-profile",
    name: "clock-planning",
    serviceChecks: true,
    endpoint: "http://127.0.0.1:8080/gremlin",
    target: { label: "fixture", build: "fixture", mode: "forward", environment: "fixture", evidenceFiles: [] },
    cases: [{ id: "scalar", body: {}, expectedStatus: 200, checks: [{ pointer: "", equals: 2 }] }],
    loads: [{ id: "one", model: "closed", concurrency: 1 }],
    repeats: 1,
    warmupMs: 0,
    durationMs: 100,
    requestTimeoutMs: 100,
    drainMs: 100,
    maxRequestsPerPhase: 100,
    maxResponseBytes: 4096,
    maxTraceBytes: 1048576,
});

test("optional clock policy stays absent in legacy profiles and reserves each actual run and probe when enabled", () => {
    const base = httpProfileSchema.parse(profile());
    assert.equal(base.clockReference, undefined);
    const enabled = httpProfileSchema.parse({ ...profile(), clockReference: reference() });
    validateHttpProfile(base);
    validateHttpProfile(enabled);
    assert.equal(httpDeadline(enabled) - httpDeadline(base), 3 * 10600);
    const noChecks = { ...enabled, serviceChecks: false, repeats: 2 };
    assert.equal(httpDeadline(noChecks) - httpDeadline({ ...base, serviceChecks: false, repeats: 2 }), 2 * 10600);
});

test("clock reference schema and independent peer contract reject conflicting deadline and raw budgets", () => {
    for (const patch of [
        { startupMs: 10001 },
        { finishMs: 49 },
        { wallMs: 1000 },
        { maxFrameBytes: 16384, maxRawBytes: 16384 },
        { sourceOrigin: "claimed-host" },
        { extra: true },
    ]) {
        const config = reference();
        Object.assign(config.spec, patch);
        assert.equal(httpProfileSchema.safeParse({ ...profile(), clockReference: config }).success, false);
        assert.throws(() => validateRunnerSanitySpec(config.spec));
    }
    for (const python of ["python3", "./python3", "/fixed/python\n3"])
        assert.equal(
            httpProfileSchema.safeParse({ ...profile(), clockReference: { ...reference(), python } }).success,
            false,
        );
    assert.equal(
        httpProfileSchema.safeParse({ ...profile(), clockReference: { ...reference(), enabled: true } }).success,
        false,
    );
});

test("enabled reference evidence cannot bypass the existing whole-workflow metadata or time ceilings", () => {
    const large = {
        ...profile(),
        cases: Array.from({ length: 16 }, (_, i) => ({ ...profile().cases[0], id: `c${i}` })),
        loads: [
            { id: "a", model: "closed", concurrency: 1 },
            { id: "b", model: "closed", concurrency: 1 },
        ],
        clockReference: reference(),
    };
    validateHttpProfile(httpProfileSchema.parse(large));
    large.clockReference.spec.maxRawBytes = 262144;
    assert.throws(() => validateHttpProfile(httpProfileSchema.parse(large)), { code: "HTTP_METADATA_BUDGET" });
    large.clockReference.spec.maxRawBytes = 65536;
    large.clockReference.spec.wallMs = 600000;
    assert.throws(() => validateHttpProfile(httpProfileSchema.parse(large)), { code: "HTTP_TIME_BUDGET" });
});

test("clock policy rejects an allowed peer watchdog too short to enclose the HTTP run or functional probe", () => {
    const configured = { ...profile(), clockReference: reference() };
    configured.durationMs = 9000;
    assert.throws(() => validateHttpProfile(httpProfileSchema.parse(configured)), {
        code: "HTTP_CLOCK_REFERENCE_TIME_BUDGET",
    });
    configured.durationMs = 100;
    configured.requestTimeoutMs = 9000;
    assert.throws(() => validateHttpProfile(httpProfileSchema.parse(configured)), {
        code: "HTTP_CLOCK_REFERENCE_TIME_BUDGET",
    });
});
