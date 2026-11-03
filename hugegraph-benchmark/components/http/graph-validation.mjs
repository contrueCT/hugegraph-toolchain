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

// Independent offline validation of the retained projection; never trusts the runner verdict.
export function validGraphValue(value, rule) {
    if (rule.kind === "vertex") {
        if (!value || typeof value !== "object" || Array.isArray(value)) return false;
        if (rule.absent)
            return (
                typeof value.exception === "string" &&
                value.exception.endsWith("NotFoundException") &&
                typeof value.message === "string" &&
                /vertex/i.test(value.message)
            );
        return value.id === rule.id && value.label === rule.label;
    }
    if (!Array.isArray(value) || value.some((s) => typeof s !== "string" || !s.length)) return false;
    if (rule.kind === "id-set") {
        const allowed = new Set(rule.allowed);
        return (
            value.length === rule.count && new Set(value).size === value.length && value.every((s) => allowed.has(s))
        );
    }
    if (rule.kind !== "shortest-path") return false;
    if (rule.distance === -1) return value.length === 0;
    const edges = new Set(rule.edges.map((e) => JSON.stringify(e)));
    return (
        value.length === rule.distance + 1 &&
        value[0] === rule.source &&
        value.at(-1) === rule.target &&
        new Set(value).size === value.length &&
        value.slice(1).every((v, i) => edges.has(JSON.stringify([value[i], v])))
    );
}
export function graphResponse(request) {
    const rule = request.rule;
    let value;
    if (rule.kind === "vertex")
        return rule.absent
            ? { exception: "class org.apache.hugegraph.exception.NotFoundException", message: "Vertex does not exist" }
            : { id: rule.id, label: rule.label, properties: {} };
    if (rule.kind === "id-set") value = rule.allowed.slice(0, rule.count);
    else {
        const find = (p) =>
            p.length === rule.distance + 1
                ? p.at(-1) === rule.target
                    ? p
                    : null
                : rule.edges
                      .filter((e) => e[0] === p.at(-1) && !p.includes(e[1]))
                      .map((e) => find([...p, e[1]]))
                      .find(Boolean);
        value = rule.distance < 0 ? [] : find([rule.source]);
        if (!value) throw new Error("Reference path witness does not contain a valid shortest path");
    }
    const parts = rule.pointer
        .slice(1)
        .split("/")
        .map((s) => s.replaceAll("~1", "/").replaceAll("~0", "~"));
    let body = value;
    for (const p of parts.reverse()) body = /^[0-9]+$/.test(p) ? Object.assign([], { [p]: body }) : { [p]: body };
    if (request.method === "POST") body.status = { code: 200 };
    return body;
}
