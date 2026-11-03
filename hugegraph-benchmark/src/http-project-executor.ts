/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
import { isDeepStrictEqual } from "node:util";
import { httpGoProfileSchema } from "./http-contracts.js";
import type { HttpProject } from "./http-preset-contracts.js";
import { BenchError } from "./io.js";

export function validateProjectExecutor(project: HttpProject, runtime: Record<string, any>) {
    const go = project.schemaVersion === 2;
    if (
        runtime.schemaVersion !== (go ? 2 : 1) ||
        (!go && runtime.executor !== undefined) ||
        (go &&
            (runtime.executor?.engine !== project.executor.engine ||
                !isDeepStrictEqual(runtime.executor?.binary, project.executor.binary) ||
                project.executor.cpuQuotaCores !== runtime.driver?.cpus ||
                project.executor.maxRssBytes !== runtime.driver?.memoryBytes))
    )
        throw new BenchError(
            "HTTP_PROJECT_EXECUTOR_MISMATCH",
            "Project engine, binary and CPU/RSS limits must match the saved comparison runtime",
        );
}

export function projectHttpProfile<T extends Record<string, unknown>>(project: HttpProject, base: T) {
    if (project.schemaVersion === 1) return base;
    const { clockReference: _, ...value } = base;
    return httpGoProfileSchema.parse({ ...value, schemaVersion: 2, executor: project.executor });
}
