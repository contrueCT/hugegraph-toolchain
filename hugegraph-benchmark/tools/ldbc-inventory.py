#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#/

"""Verify all public initial graph counts, schema and indexes against a frozen conversion manifest.

Read-only but potentially expensive: full graph count queries require the same resource approval as SF100 validation.
Requires trusted external exclusive access and drained writers. This check supplements full-shard content checks.
"""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import subprocess
import urllib.parse

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("reconciliation", ROOT / "tools/ldbc-reconcile.py")
r = importlib.util.module_from_spec(spec)
spec.loader.exec_module(r)


def load_expected(prepared_file, expected_sha):
    raw = prepared_file.read_bytes()
    if len(raw) > 32 * 1024**2 or r.digest(raw) != expected_sha:
        raise ValueError("Prepared manifest differs from its frozen identity")
    prepared = r.decode(raw)
    artifact = prepared["files"]["inventory.expected.json"]
    path = prepared_file.parent / "inventory.expected.json"
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 4 * 1024**2:
        raise ValueError("Expected inventory must be a bounded regular file")
    content = path.read_bytes()
    if r.digest(content) != artifact["sha256"] or str(len(content)) != str(artifact["bytes"]):
        raise ValueError("Expected inventory identity mismatch")
    expected = r.decode(content)
    if set(expected) != {"schemaVersion", "scope", "schema", "counts"} or expected["schemaVersion"] != 1 or             expected["scope"] != "public-ldbc-initial-graph":
        raise ValueError("Invalid initial inventory contract")
    if set(expected["schema"]) != {"propertyKeys", "vertexLabels", "edgeLabels", "indexLabels"}:
        raise ValueError("Invalid schema inventory")
    if set(expected["counts"]) != {"vertices", "edges"}:
        raise ValueError("Invalid count inventory")
    totals = {"vertices": {}, "edges": {}}
    for shard in prepared["shards"]:
        group = totals[shard["category"]]
        group[shard["label"]] = group.get(shard["label"], 0) + int(shard["records"])
    for category, labels in (("vertices", "vertexLabels"), ("edges", "edgeLabels")):
        if set(expected["counts"][category]) != set(expected["schema"][labels]):
            raise ValueError("Schema/count label inventory mismatch")
        if set(totals[category]) - set(expected["counts"][category]):
            raise ValueError("Undeclared prepared label")
        for label, count in expected["counts"][category].items():
            if not isinstance(count, str) or not re.fullmatch(r"0|[1-9][0-9]*", count) or                     int(count) != totals[category].get(label, 0) or int(count) != prepared["counts"].get(label, 0):
                raise ValueError("Inventory count differs from the full shard/CSV inventory")
        if sum(totals[category].values()) != prepared[category]:
            raise ValueError("Prepared total disagrees with per-label counts")
    return expected, content


def compare(expected, actual):
    if not isinstance(actual, dict) or set(actual) != set(expected) or             actual.get("scope") != expected["scope"] or type(actual.get("schemaVersion")) is not int or actual["schemaVersion"] != 1:
        raise ValueError("Invalid observed inventory envelope")
    return {"schemaMatch": r.exact(actual["schema"]) == r.exact(expected["schema"]),
            "countsMatch": r.exact(actual["counts"]) == r.exact(expected["counts"])}


def observe(context, lease, expected, transport, alias):
    graph, marker = r.lease_check(lease, context)
    script = (ROOT / "adapters/ldbc-gremlin/inventory.groovy").read_text()
    body = {"gremlin": script, "language": "gremlin-groovy", "aliases": {"g": alias},
            "bindings": {"graphName": graph, "epochMarker": marker}}
    data, response_hash = transport.post(body)
    if len(data) != 1 or not isinstance(data[0], dict) or set(data[0]) != {"graphName", "epochMarker", "inventory"}:
        raise ValueError("Invalid inventory response")
    if data[0]["graphName"] != graph or data[0]["epochMarker"] != marker:
        raise ValueError("Target/restore epoch mismatch")
    r.lease_check(lease, context)
    result = compare(expected, data[0]["inventory"])
    return {**result, "matches": all(result.values()), "requestSha256": r.digest(r.encoded(body)),
            "responseSha256": response_hash, "scriptSha256": r.digest(script.encode())}, data[0]["inventory"]


def observe_paged(context, lease, expected, transport, alias, page_size, max_pages, ledger, ledger_budget):
    """Count every public element through bounded backend pages, with no automatic retry/resume."""
    if not 1 <= page_size <= 100000 or not 1 <= max_pages <= 1000000 or ledger_budget < 1:
        raise ValueError("Invalid paged inventory budget")
    graph, marker = r.lease_check(lease, context)
    script = (ROOT / "adapters/ldbc-gremlin/inventory-page.groovy").read_text()
    ledger_bytes, requests = 0, 0
    counts = {"vertices": {}, "edges": {}}

    def call(category, page=""):
        nonlocal ledger_bytes, requests
        r.lease_check(lease, context)
        body = {"gremlin": script, "language": "gremlin-groovy", "aliases": {"g": alias},
                "bindings": {"graphName": graph, "epochMarker": marker, "category": category,
                             "page": page, "pageSize": page_size}}
        data, response_hash = transport.post(body)
        if len(data) != 1 or not isinstance(data[0], dict):
            raise ValueError("Invalid inventory page response")
        row = data[0]
        if row.get("graphName") != graph or row.get("epochMarker") != marker:
            raise ValueError("Target/restore epoch mismatch")
        r.lease_check(lease, context)
        requests += 1
        record = {"sequence": requests, "category": category, "pageSha256": r.digest(page.encode()),
                  "requestSha256": r.digest(r.encoded(body)), "responseSha256": response_hash,
                  "observation": row}
        raw = r.encoded(record) + b"\n"
        if ledger_bytes + len(raw) > ledger_budget:
            raise ValueError("Inventory page evidence budget exceeded")
        ledger.write(raw)
        ledger.flush()
        ledger_bytes += len(raw)
        return row

    before = call("schema")
    if set(before) != {"graphName", "epochMarker", "kind", "schema"} or before["kind"] != "schema":
        raise ValueError("Invalid schema snapshot")
    schema = before["schema"]
    if not isinstance(schema, dict) or set(schema) != {"propertyKeys", "vertexLabels", "edgeLabels", "indexLabels"}:
        raise ValueError("Invalid schema snapshot fields")
    pages = {}
    for category, labels in (("vertices", "vertexLabels"), ("edges", "edgeLabels")):
        if not isinstance(schema[labels], dict):
            raise ValueError("Invalid schema labels")
        counts[category] = {label: 0 for label in schema[labels]}
        page, seen = "", set()
        for number in range(max_pages):
            fingerprint = r.digest(page.encode())
            if fingerprint in seen:
                raise ValueError("Inventory cursor repeated; no automatic retry")
            seen.add(fingerprint)
            row = call(category, page)
            if (set(row) != {"graphName", "epochMarker", "kind", "category", "page", "nextPage", "scanned", "counts"} or
                    row["kind"] != "page" or row["category"] != category or row["page"] != page):
                raise ValueError("Inventory page identity mismatch")
            if (not isinstance(row["scanned"], str) or not re.fullmatch(r"0|[1-9][0-9]*", row["scanned"]) or
                    int(row["scanned"]) > page_size or not isinstance(row["counts"], dict)):
                raise ValueError("Invalid page count or backend page limit")
            public = 0
            for label, value in row["counts"].items():
                if (not isinstance(label, str) or label.startswith("~") or not isinstance(value, str) or
                        not re.fullmatch(r"0|[1-9][0-9]*", value)):
                    raise ValueError("Invalid public label count")
                public += int(value)
                counts[category][label] = counts[category].get(label, 0) + int(value)
            if public > int(row["scanned"]):
                raise ValueError("Page public count exceeds scanned elements")
            page = row["nextPage"]
            if page is None:
                pages[category] = number + 1
                break
            if not isinstance(page, str) or not page or len(page) > 8192:
                raise ValueError("Invalid nonterminal inventory cursor")
        else:
            raise ValueError("Inventory page limit reached before terminal cursor")
    after = call("schema")
    if r.exact(before) != r.exact(after):
        raise ValueError("Schema changed during exclusive inventory scan")
    actual = {"schemaVersion": 1, "scope": "public-ldbc-initial-graph", "schema": schema,
              "counts": {kind: {label: str(count) for label, count in values.items()}
                         for kind, values in counts.items()}}
    result = compare(expected, actual)
    return {**result, "matches": all(result.values()), "mode": "paged-full-scan", "pages": pages,
            "requests": requests, "ledgerBytes": ledger_bytes, "pageSize": page_size,
            "scriptSha256": r.digest(script.encode())}, actual


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--job", type=Path, required=True, help="A frozen Loader job belonging to this preparation/target epoch")
    p.add_argument("--prepared", type=Path, required=True, help="Frozen sharded prepare-manifest.json")
    p.add_argument("--prepared-sha256", required=True, help="Manifest digest from the reviewed execution plan")
    p.add_argument("--quiescence", type=Path, required=True, help="External exclusive-access lease, as used by ldbc-reconcile.py")
    p.add_argument("--endpoint", required=True, help="Frozen target origin plus /gremlin; whole-graph queries may be expensive")
    p.add_argument("--alias", required=True)
    p.add_argument("--authorization-env")
    p.add_argument("--timeout-seconds", type=float, default=30, help="Whole HTTP response deadline; do not retry failed counts")
    p.add_argument("--max-response-bytes", type=int, default=4*1024**2)
    p.add_argument("--page-size", type=int, default=10000, help="Exact scan page size, 1..100000; each request has its own deadline")
    p.add_argument("--max-pages", type=int, default=1000000, help="Maximum pages per element category; no partial success")
    p.add_argument("--legacy-single-request", action="store_true", help="Explicit legacy full-graph request; unsuitable for SF100 deadlines")
    p.add_argument("--out", type=Path, required=True, help="New evidence directory")
    a = p.parse_args()
    if not re.fullmatch(r"[a-f0-9]{64}", a.prepared_sha256) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.-]{0,127}", a.alias):
        p.error("Invalid manifest SHA-256 or traversal alias")
    if (not 0 < a.timeout_seconds <= 3600 or not 1 <= a.max_response_bytes <= 64*1024**2 or
            not 1 <= a.page_size <= 100000 or not 1 <= a.max_pages <= 1000000):
        p.error("Invalid time/response budget")
    expected, content = load_expected(a.prepared, a.prepared_sha256)
    checked = subprocess.run(["node", str(ROOT / "tools/ldbc-reconcile-context.mjs"), str(a.job.resolve()),
                              str(a.prepared.resolve())], capture_output=True, text=True, timeout=60, check=True)
    context = r.decode(checked.stdout)
    lease_raw = a.quiescence.read_bytes()
    lease = r.decode(lease_raw)
    r.lease_check(lease, context)
    target = context["job"]["config"]["target"]
    endpoint, origin = urllib.parse.urlsplit(a.endpoint), urllib.parse.urlsplit(target["host"])
    if endpoint.scheme != origin.scheme or endpoint.hostname != origin.hostname or (endpoint.port or
            (443 if endpoint.scheme == "https" else 80)) != target["port"] or endpoint.path != "/gremlin" or endpoint.query:
        raise ValueError("Inventory must use the frozen Loader target origin and /gremlin path")
    transport = r.Transport(a.endpoint, a.timeout_seconds, a.max_response_bytes, a.authorization_env)
    a.out.mkdir(mode=0o700)
    actual = None
    ledger_path = a.out / "pages.jsonl"
    try:
        if a.legacy_single_request:
            result, actual = observe(context, lease, expected, transport, a.alias)
        else:
            budget = int(context["job"]["request"]["outputBudgetBytes"]) - 2 * len(content) - len(lease_raw) - 65536
            with ledger_path.open("xb") as ledger:
                result, actual = observe_paged(context, lease, expected, transport, a.alias,
                                              a.page_size, a.max_pages, ledger, budget)
        outcome = "matched" if result["matches"] else "mismatch"
    except Exception as error:
        result = {"matches": False, "error": {"type": type(error).__name__, "message": str(error)[:1024]}}
        outcome = "unknown"
    page_evidence = None
    if ledger_path.exists():
        page_hash = hashlib.sha256()
        with ledger_path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                page_hash.update(chunk)
        page_evidence = {"path": "pages.jsonl", "bytes": ledger_path.stat().st_size, "sha256": page_hash.hexdigest()}
    report = {"schemaVersion": 1, "kind": "sf100-inventory-observation", "outcome": outcome,
        "scope": "public-ldbc-initial-graph", "requestDigest": context["requestDigest"],
        "target": context["job"]["request"]["target"], "preparedSha256": a.prepared_sha256,
        "expectedSha256": r.digest(content), "actualSha256": r.digest(r.encoded(actual) + b"\n"),
        "quiescenceSha256": r.digest(lease_raw), "contentChecked": False,
        "trust": "External operator must prove exclusive access and drained writes; system hidden schema is excluded",
        "componentSha256": r.digest(Path(__file__).read_bytes()),
        "transportSourceSha256": r.digest(Path(r.__file__).read_bytes()), **result}
    report["pageEvidence"] = page_evidence
    files = {"inventory.json": r.encoded(report) + b"\n", "expected.json": content,
             "actual.json": r.encoded(actual) + b"\n", "quiescence.json": lease_raw}
    if sum(map(len, files.values())) + (ledger_path.stat().st_size if ledger_path.exists() else 0) > int(context["job"]["request"]["outputBudgetBytes"]):
        raise ValueError("Inventory evidence exceeds the frozen output budget")
    for name, raw in files.items():
        with (a.out / name).open("xb") as stream:
            stream.write(raw)
    print(json.dumps(report))
    return 0 if outcome == "matched" else 4


if __name__ == "__main__":
    raise SystemExit(main())
