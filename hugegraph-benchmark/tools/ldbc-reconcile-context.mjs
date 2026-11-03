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

import { readJson, hash } from "../dist/io.js";
import { loaderJobSchema, preparedSchema, runLoaderJob } from "../dist/sf100-loader.js";
import { verifyProtocolArtifacts } from "../dist/sf100-protocol.js";
const job = await readJson(process.argv[2], loaderJobSchema, 32 * 1024 * 1024);
await runLoaderJob(process.argv[2], ".", false);
await verifyProtocolArtifacts(job.inputRoot, job.request.inputs);
let prepared;
if (process.argv[3]) {
    prepared = await readJson(process.argv[3], preparedSchema, 32 * 1024 * 1024);
    if (
        job.config.mappingDigest !==
        hash({ schema: prepared.files["schema.groovy"], converter: prepared.sourceConverterSha256 })
    )
        throw new Error("Prepared manifest mapping differs from the frozen Loader target");
    const shard = prepared.shards.find((item) => item.id === job.request.shard.id);
    const input = job.request.inputs[0];
    if (
        !shard ||
        shard.path !== input.path ||
        shard.sha256 !== input.sha256 ||
        shard.bytes !== input.bytes ||
        shard.records !== job.request.shard.records ||
        hash(shard.encoding ?? null) !== hash(job.encoding ?? null)
    )
        throw new Error("Loader job does not belong to this prepared manifest");
}
process.stdout.write(JSON.stringify({ job, requestDigest: hash(job.request) }));
