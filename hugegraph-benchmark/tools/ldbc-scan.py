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

"""Full sequential source/target content verification using keyed multiset digests.

Reads every record and property. Equality relies on the documented collision assumption.
Requires external exclusive access, drained writers and immutable input/lease evidence.
No retries, sampling, automatic resume, database writes or benchmark acceptance.
"""
import argparse
from contextlib import nullcontext
import datetime as dt
import gzip
import hashlib
import hmac
import importlib.util
import io
import json
import math
from pathlib import Path
import re
import secrets
import signal
import stat
import subprocess
import time
import urllib.parse

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("scan_inventory", ROOT / "tools/ldbc-inventory.py")
inventory = importlib.util.module_from_spec(spec)
spec.loader.exec_module(inventory)
r = inventory.r
PROTOCOL = "hgbench-keyed-logical-multiset-v1"
DOMAIN = PROTOCOL.encode() + b"\0"
MODULUS = 1 << 256
CATEGORIES = ("vertices", "edges")


def need(condition, reason):
    if not condition:
        raise ValueError(reason)


def text(value):
    need(type(value) is str and bool(value), "Expected nonempty string")
    value.encode("utf-8", errors="strict")
    return value


def integer(value):
    need(type(value) is int and -(1 << 63) <= value < (1 << 63), "Expected exact signed long")
    return value


def logical(category, row):
    fields = {"id", "label", "properties"} if category == "vertices" else {
        "sourceId", "targetId", "label", "properties"}
    need(category in {"vertices", "edges"} and type(row) is dict and set(row) == fields,
         "Unexpected logical record shape")
    for key in fields - {"properties"}:
        text(row[key])
    props = r.properties(row["properties"])
    for key, value in props.items():
        text(key)
        if key in {"emails", "languages"}:
            continue  # Existing SET normalizer sorts and rejects duplicate/non-string values.
        if type(value) is int:
            integer(value)
        else:
            need(type(value) is str, "Unexpected SNB property kind")
            value.encode("utf-8", errors="strict")
    required = "ldbcId" if category == "vertices" else "edgeKey"
    integer(props.get(required))
    return {**row, "properties": props}


class Fingerprint:
    """Count and two domain-separated HMAC-SHA256 sums per logical-identity bucket."""
    def __init__(self, labels, buckets=64, *, challenge):
        need(type(challenge) is bytes and len(challenge) == 32, "A fixed 32-byte challenge is required")
        self.challenge = challenge
        need(type(buckets) is int and 1 <= buckets <= 1024 and buckets & (buckets - 1) == 0,
             "Invalid fixed bucket budget")
        need(type(labels) is dict and set(labels) == {"vertices", "edges"}, "Missing category")
        self.labels = {kind: tuple(sorted(text(x) for x in values)) for kind, values in labels.items()}
        need(all(len(v) == len(set(v)) and len(v) <= 128 and all(not x.startswith("~") for x in v)
                 for v in self.labels.values()), "Invalid or duplicate configured label")
        self.buckets = buckets
        self.values = {}

    def tag(self, purpose, raw):
        return hmac.new(self.challenge, DOMAIN + purpose + b"\0" + raw, hashlib.sha256).digest()

    def add(self, category, row):
        row = logical(category, row)
        need(row["label"] in self.labels[category], "Unexpected observed label")
        identity = [category, row["label"], row["id"]] if category == "vertices" else [
            category, row["label"], row["sourceId"], row["targetId"], row["properties"]["edgeKey"]]
        bucket = int.from_bytes(self.tag(b"bucket", r.encoded(identity))[:2], "big") & (self.buckets - 1)
        raw = r.encoded([category, r.exact(row)])
        a, b = [int.from_bytes(self.tag(purpose, raw), "big") for purpose in (b"a", b"b")]
        key = category, row["label"], bucket
        count, sa, sb = self.values.get(key, (0, 0, 0))
        self.values[key] = count + 1, (sa + a) % MODULUS, (sb + b) % MODULUS

    def snapshot(self):
        rows = []
        for category, labels in sorted(self.labels.items()):
            for label in labels:
                for bucket in range(self.buckets):
                    count, a, b = self.values.get((category, label, bucket), (0, 0, 0))
                    rows.append({"category": category, "label": label, "bucket": bucket,
                                 "records": str(count), "sumA": f"{a:064x}", "sumB": f"{b:064x}"})
        return {"protocol": PROTOCOL, "buckets": self.buckets, "rows": rows,
                "challengeCommitment": hashlib.sha256(self.challenge).hexdigest()}


def count(value):
    need(type(value) is str and value.isascii() and value.isdecimal() and str(int(value)) == value,
         "Expected canonical decimal count")
    return int(value)


class PageFold:
    """Reject malformed, repeated, truncated and wrong-target backend page receipts."""
    def __init__(self, category, graph, epoch, fingerprint, page_size=4096, max_pages=600000):
        need(category in {"vertices", "edges"} and type(page_size) is int and type(max_pages) is int and
             1 <= page_size <= 100000 and 1 <= max_pages <= 1000000,
             "Invalid scan budget")
        self.category, self.graph, self.epoch = category, text(graph), text(epoch)
        self.fingerprint = fingerprint
        self.page_size, self.max_pages = page_size, max_pages
        self.next_page, self.requests, self.failed = "", 0, False
        self.seen = set()

    def accept(self, requested_page, response):
        need(not self.failed, "Failed scan cannot continue")
        try:
            need(self.next_page is not None and requested_page == self.next_page, "Unexpected request cursor")
            need(type(requested_page) is str and len(requested_page.encode()) <= 32768, "Cursor byte budget")
            need(self.requests < self.max_pages, "Page budget exhausted")
            marker = hashlib.sha256(requested_page.encode()).digest()
            need(marker not in self.seen, "Repeated page request")
            self.seen.add(marker)
            need(type(response) is dict and set(response) == {
                "graphName", "epochMarker", "kind", "category", "page", "nextPage", "scanned", "hiddenRecords", "rows"},
                "Unexpected page response shape")
            need(response["graphName"] == self.graph and response["epochMarker"] == self.epoch and
                 response["category"] == self.category and response["kind"] == "scan-page" and
                 response["page"] == requested_page, "Wrong graph/epoch/page binding")
            rows = response["rows"]
            scanned, hidden = count(response["scanned"]), count(response["hiddenRecords"])
            need(type(rows) is list and 0 <= hidden <= scanned <= self.page_size and
                 len(rows) == scanned - hidden, "Page record count mismatch")
            next_page = response["nextPage"]
            if next_page is not None:
                text(next_page)
                need(len(next_page.encode()) <= 32768 and
                     hashlib.sha256(next_page.encode()).digest() not in self.seen, "Repeated/oversized next cursor")
            for row in rows:
                self.fingerprint.add(self.category, row)
            self.requests += 1
            self.next_page = next_page
        except Exception:
            self.failed = True
            raise

    def complete(self):
        return not self.failed and self.requests > 0 and self.next_page is None


def file_identity(path):
    value = path.stat()
    need(stat.S_ISREG(value.st_mode) and not path.is_symlink(), "Expected a regular immutable file")
    return value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns


def bounded_json(path, limit):
    identity = file_identity(path)
    need(identity[2] <= limit, "Metadata byte budget exceeded")
    raw = path.read_bytes()
    need(file_identity(path) == identity and len(raw) == identity[2], "Metadata changed while reading")
    return r.decode(raw), raw


def source_fingerprint(prepared, base, categories, fingerprint, guard, record):
    """Stream every frozen JSONL shard, checking stored/expanded bytes, digests and row counts."""
    ids, paths = set(), set()
    totals = {kind: 0 for kind in CATEGORIES}
    source_root = base.resolve(strict=True)
    for shard in prepared["shards"]:
        guard()
        category = shard["category"]
        need(category in CATEGORIES and shard["id"] not in ids and shard["path"] not in paths,
             "Duplicate or invalid shard identity")
        ids.add(shard["id"])
        paths.add(shard["path"])
        artifact = prepared["files"].get(shard["path"])
        need(artifact is not None and artifact["sha256"] == shard["sha256"] and
             str(artifact["bytes"]) == shard["bytes"], "Shard/file identity mismatch")
        relative = Path(shard["path"])
        need(not relative.is_absolute() and ".." not in relative.parts, "Unsafe shard path")
        path = base / relative
        need(path.resolve(strict=True).is_relative_to(source_root), "Shard escaped prepared input root")
        before = file_identity(path)
        need(before[2] == count(shard["bytes"]), "Stored shard byte count differs")
        if category not in categories:
            continue
        encoding = shard.get("encoding")
        need(encoding is None or set(encoding) == {"compression", "uncompressedBytes", "uncompressedSha256"},
             "Invalid expanded source identity")
        if encoding:
            need(encoding["compression"] == "gzip", "Unsupported source compression")
        expected_bytes = count(encoding["uncompressedBytes"] if encoding else shard["bytes"])
        expected_digest = encoding["uncompressedSha256"] if encoding else shard["sha256"]
        need(re.fullmatch(r"[a-f0-9]{64}", expected_digest) is not None, "Invalid expanded source digest")
        expected_records, byte_count, records = count(shard["records"]), 0, 0
        expanded_hash = hashlib.sha256()
        with path.open("rb") as stream:
            stored = r.HashedInput(stream, count(shard["bytes"]))
            with io.BufferedReader(stored) as buffered:
                decoder = gzip.GzipFile(fileobj=buffered, mode="rb") if encoding else nullcontext(buffered)
                with decoder as source:
                    while True:
                        raw = source.readline(min(16 * 1024**2 + 1, expected_bytes - byte_count + 1))
                        if not raw:
                            break
                        need(len(raw) <= 16 * 1024**2 and raw.endswith(b"\n"),
                             "Oversized or unterminated source record")
                        byte_count += len(raw)
                        need(byte_count <= expected_bytes and records < expected_records,
                             "Source exceeds frozen byte or record count")
                        expanded_hash.update(raw)
                        _, item = r.expected(r.decode(raw), category, shard["label"])
                        fingerprint.add(category, item)
                        records += 1
                        if records % 1024 == 0:
                            guard()
            need(stored.count == int(shard["bytes"]) and stored.digest.hexdigest() == shard["sha256"],
                 "Stored source digest or byte count differs")
        need(before == file_identity(path), "Source changed while scanning")
        need(byte_count == expected_bytes and expanded_hash.hexdigest() == expected_digest and
             records == expected_records, "Expanded source digest or record count differs")
        guard()
        totals[category] += records
        record({"shardId": shard["id"], "category": category, "records": str(records),
                "storedBytes": shard["bytes"], "storedSha256": shard["sha256"],
                "expandedBytes": str(byte_count), "expandedSha256": expanded_hash.hexdigest()})
    for category in categories:
        need(totals[category] == prepared[category], "Source category total differs from frozen inventory")
    return totals


class Evidence:
    def __init__(self, out, max_pages, ledger_budget, summary_budget, used_pages=0, used_bytes=0):
        self.out, self.max_pages = out, max_pages
        self.ledger_budget, self.summary_budget = ledger_budget, summary_budget
        self.used_pages, self.used_bytes = used_pages, used_bytes
        self.requests, self.ledger_bytes, self.previous = 0, 0, "0" * 64
        self.ledger = (out / "pages.jsonl").open("xb")

    def save(self, name, value):
        raw = value if type(value) is bytes else r.encoded(value) + b"\n"
        size = sum(p.stat().st_size for p in self.out.iterdir() if p.is_file() and
                   p.name not in {"pages.jsonl", name})
        reserve = 0 if name in {"scan.json", "inventory.json"} else 65536
        need(size + len(raw) + reserve <= self.summary_budget, "Combined summary byte budget exceeded")
        with (self.out / name).open("xb") as stream:
            stream.write(raw)

    def page(self, record):
        need(self.used_pages + self.requests < self.max_pages, "Whole-task page budget exhausted")
        entry = {"sequence": self.requests + 1, "previousEntrySha256": self.previous, **record}
        raw = r.encoded(entry) + b"\n"
        need(self.used_bytes + self.ledger_bytes + len(raw) <= self.ledger_budget,
             "Whole-task page-ledger byte budget exceeded")
        self.ledger.write(raw)
        self.ledger.flush()
        self.previous = r.digest(raw)
        self.requests += 1
        self.ledger_bytes += len(raw)


def observe(context, lease, transport, alias, categories, fingerprint, guard, evidence, page_size):
    graph, marker = r.lease_check(lease, context)
    schema_script = (ROOT / "adapters/ldbc-gremlin/inventory-page.groovy").read_text()
    content_script = (ROOT / "adapters/ldbc-gremlin/scan-page.groovy").read_text()

    def call(category, page=""):
        guard()
        need(evidence.used_pages + evidence.requests < evidence.max_pages, "Whole-task page budget exhausted")
        body = {"gremlin": schema_script if category == "schema" else content_script,
                "language": "gremlin-groovy", "aliases": {"g": alias},
                "bindings": {"graphName": graph, "epochMarker": marker, "category": category,
                             "page": page, "pageSize": page_size}}
        receipt = {"category": category, "pageSha256": r.digest(page.encode()),
                   "requestSha256": r.digest(r.encoded(body))}
        try:
            data, response_hash = transport.post(body)
        except (Exception, KeyboardInterrupt) as error:
            evidence.page({**receipt, "responseSha256": None, "errorType": type(error).__name__})
            raise
        evidence.page({**receipt, "responseSha256": response_hash, "errorType": None})
        guard()
        need(type(data) is list and len(data) == 1 and type(data[0]) is dict, "Invalid scan response envelope")
        row = data[0]
        need(row.get("graphName") == graph and row.get("epochMarker") == marker,
             "Target graph or epoch mismatch")
        return body, row, response_hash

    def schema():
        body, row, response_hash = call("schema")
        need(set(row) == {"graphName", "epochMarker", "kind", "schema"} and row["kind"] == "schema",
             "Invalid schema response")
        return row["schema"]

    before = schema()
    hidden, pages = {}, {}
    for category in categories:
        fold = PageFold(category, graph, marker, fingerprint, page_size=page_size, max_pages=evidence.max_pages)
        hidden[category] = 0
        while not fold.complete():
            page = fold.next_page
            body, row, response_hash = call(category, page)
            fold.accept(page, row)
            hidden[category] += count(row["hiddenRecords"])
        pages[category] = fold.requests
    after = schema()
    need(r.exact(before) == r.exact(after), "Schema changed during exclusive content scan")
    return {"schema": before, "pages": pages, "hiddenRecords": hidden}


def artifact(path, guard=lambda: None):
    guard()
    identity = file_identity(path)
    sha = hashlib.sha256()
    with path.open("rb") as stream:
        for raw in iter(lambda: stream.read(1024**2), b""):
            guard()
            sha.update(raw)
    need(file_identity(path) == identity, "Evidence changed while hashing")
    return {"path": path.name, "bytes": str(identity[2]), "sha256": sha.hexdigest()}


def page_ledger(path, expected_requests, guard=lambda: None):
    guard()
    before = file_identity(path)
    need(before[2] <= 1024**3, "Prior page ledger exceeds byte budget")
    sha, previous, requests, vertices = hashlib.sha256(), "0" * 64, 0, 0
    categories = []
    with path.open("rb") as stream:
        while True:
            raw = stream.readline(8193)
            if not raw:
                break
            need(len(raw) <= 8192 and raw.endswith(b"\n"), "Invalid prior page-ledger line")
            row = r.decode(raw)
            requests += 1
            if requests % 64 == 0:
                guard()
            need(type(row.get("sequence")) is int and row["sequence"] == requests and
                 row.get("previousEntrySha256") == previous and row.get("errorType") is None and
                 row.get("category") in {"schema", "vertices"} and
                 re.fullmatch(r"[a-f0-9]{64}", row.get("responseSha256", "")) is not None,
                 "Prior page-ledger chain or response differs")
            if row["category"] == "vertices":
                vertices += 1
            else:
                categories.append(requests)
            sha.update(raw)
            previous = r.digest(raw)
    need(file_identity(path) == before and requests == expected_requests and categories == [1, requests] and
         vertices == requests - 2, "Prior page-ledger count/scope differs")
    return {"path": path.name, "bytes": str(before[2]), "sha256": sha.hexdigest()}


def prior_usage(path, expected_sha, binding, component_sha, budgets, guard=lambda: None):
    guard()
    report, raw = bounded_json(path, 4 * 1024**2)
    need(r.digest(raw) == expected_sha and report.get("kind") == "sf100-sequential-content-observation" and
         report.get("outcome") == "matched" and report.get("scope") == "vertices" and
         report.get("contentMatches") is True and report.get("schemaMatches") is True and
         report.get("countsMatch") is True and report.get("binding") == binding and report.get("error") is None and
         report.get("protocol") == PROTOCOL and report.get("vertexAdmission") == "content" and
         report.get("datasetVerified") is False and report.get("priorScan") is None and
         report.get("componentSha256") == component_sha, "Prior vertex-content proof identity differs")
    need(report["budgets"] == budgets and
         report["wholeTaskPages"] == report["requests"] and
         report["wholeTaskLedgerBytes"] == report["ledgerBytes"], "Prior scan budget differs")
    need(type(report["requests"]) is int and type(report["ledgerBytes"]) is int and
         0 < report["requests"] < budgets["maxPages"] and 0 <= report["ledgerBytes"] < budgets["ledgerBytes"],
         "Invalid prior scan budget usage")
    names = {"expected.json", "actual.json", "inputs.jsonl", "pages.jsonl", "quiescence.json", "challenge.json"}
    need(len(report["evidence"]) == len(names) and {x["path"] for x in report["evidence"]} == names,
         "Incomplete prior scan evidence")
    for fact in report["evidence"]:
        name = fact["path"]
        actual = (page_ledger(path.parent / name, report["requests"], guard) if name == "pages.jsonl"
                  else artifact(path.parent / name, guard))
        need(actual == fact, "Prior scan evidence digest differs")
        if name == "pages.jsonl":
            need(int(fact["bytes"]) == report["ledgerBytes"], "Prior ledger usage differs")
    wanted, _ = bounded_json(path.parent / "expected.json", 4 * 1024**2)
    actual, _ = bounded_json(path.parent / "actual.json", 4 * 1024**2)
    need(r.exact(wanted) == r.exact(actual) and wanted.get("protocol") == PROTOCOL and
         wanted.get("challengeCommitment") == report["challengeCommitment"], "Prior digest comparison differs")
    return report["requests"], report["ledgerBytes"], report["challengeCommitment"]


def main():
    parser = argparse.ArgumentParser(description=__doc__, epilog=(
        "Effects: reads every selected source and target record; creates bounded evidence only. "
        "Exit 0 means the selected content/schema/count scope matched, 4 means unknown or mismatch. "
        "vertices never qualifies the full dataset. Full/content requires a frozen accepted vertex proof. "
        "The optional acknowledged-writes policy defers vertex content admission until the full scan. "
        "Use hgbench task run for lifecycle management; CPU/memory/I/O and free-space quotas are external."))
    parser.add_argument("--job", type=Path, required=True, help="Frozen Loader job in the same target epoch")
    parser.add_argument("--prepared", type=Path, required=True, help="Full prepared/prepare-manifest.json")
    parser.add_argument("--prepared-sha256", required=True, help="Prepared manifest digest from the frozen plan")
    parser.add_argument("--quiescence", type=Path, required=True, help="Immutable external exclusive-access lease")
    parser.add_argument("--endpoint", required=True, help="Frozen target origin plus /gremlin; no retries")
    parser.add_argument("--alias", required=True, help="Exact target traversal-source alias")
    parser.add_argument("--authorization-env", help="Environment name containing Authorization; never log its value")
    parser.add_argument("--scope", choices=("vertices", "full"), required=True)
    parser.add_argument("--vertex-admission", choices=("content", "acknowledged-writes"), default="content",
                        help="Default content: vertex scan before edges, then full scan. Alternative: final full scan only")
    parser.add_argument("--prior-scan", type=Path, help="Accepted vertices/scan.json for default full scan")
    parser.add_argument("--prior-scan-sha256", help="Frozen SHA-256 of --prior-scan")
    parser.add_argument("--deadline-unix", type=float, required=True, help="Original absolute whole-task deadline; never reset")
    parser.add_argument("--page-size", type=int, default=4096)
    parser.add_argument("--max-pages", type=int, default=600000, help="Whole task, shared across both scans including schema calls")
    parser.add_argument("--ledger-budget-bytes", type=int, default=1024**3, help="Combined page-ledger cap for both scans")
    parser.add_argument("--summary-budget-bytes", type=int, default=4 * 1024**2,
                        help="Combined metadata/source-ledger/digest cap per scan")
    parser.add_argument("--timeout-seconds", type=float, default=210, help="Whole response, bounded again by remaining task time")
    parser.add_argument("--max-response-bytes", type=int, default=32 * 1024**2)
    parser.add_argument("--out", type=Path, required=True, help="New immutable evidence directory; partial scans cannot resume")
    args = parser.parse_args()
    if (not re.fullmatch(r"[a-f0-9]{64}", args.prepared_sha256) or
            not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.-]{0,127}", args.alias) or
            not math.isfinite(args.deadline_unix) or not math.isfinite(args.timeout_seconds) or
            args.deadline_unix <= time.time() or not 0 < args.timeout_seconds <= 3600 or
            not 1 <= args.max_response_bytes <= 64 * 1024**2 or not 1 <= args.page_size <= 100000 or
            not 1 <= args.max_pages <= 1000000 or not 1 <= args.ledger_budget_bytes <= 2 * 1024**3 or
            not 65536 <= args.summary_budget_bytes <= 16 * 1024**2):
        parser.error("Invalid identity, absolute deadline or resource budget")
    if bool(args.prior_scan) != bool(args.prior_scan_sha256):
        parser.error("--prior-scan and --prior-scan-sha256 must be used together")
    if args.prior_scan_sha256 and not re.fullmatch(r"[a-f0-9]{64}", args.prior_scan_sha256):
        parser.error("Invalid prior-scan SHA-256")
    if (args.scope == "vertices" and (args.prior_scan or args.vertex_admission != "content") or
            args.scope == "full" and (args.vertex_admission == "content") != bool(args.prior_scan)):
        parser.error("content requires vertices then full/prior proof; acknowledged-writes requires full without prior")

    deadline = time.monotonic() + args.deadline_unix - time.time()
    def time_guard():
        need(time.monotonic() < deadline and time.time() < args.deadline_unix, "Whole-task absolute deadline reached")
    prepared_identity = file_identity(args.prepared)
    prepared, prepared_raw = bounded_json(args.prepared, 32 * 1024**2)
    need(r.digest(prepared_raw) == args.prepared_sha256, "Prepared manifest differs from frozen plan")
    expected, _ = inventory.load_expected(args.prepared, args.prepared_sha256)
    checked = subprocess.run(["node", str(ROOT / "tools/ldbc-reconcile-context.mjs"),
                              str(args.job.resolve()), str(args.prepared.resolve())],
                             capture_output=True, text=True, timeout=min(60, max(0.001, deadline-time.monotonic())),
                             check=True)
    context = r.decode(checked.stdout)
    need(Path(context["job"]["inputRoot"]).resolve() == args.prepared.parent.resolve(), "Input root differs")
    lease_identity = file_identity(args.quiescence)
    lease, lease_raw = bounded_json(args.quiescence, 65536)
    graph, marker = r.lease_check(lease, context)
    target = context["job"]["config"]["target"]
    endpoint, origin = urllib.parse.urlsplit(args.endpoint), urllib.parse.urlsplit(target["host"])
    need(endpoint.scheme == origin.scheme and endpoint.hostname == origin.hostname and
         (endpoint.port or (443 if endpoint.scheme == "https" else 80)) == target["port"] and
         endpoint.path == "/gremlin" and not endpoint.query, "Scan endpoint differs from frozen Loader target")
    component_sha = r.digest(Path(__file__).read_bytes())
    dependencies = {name: r.digest((ROOT / name).read_bytes()) for name in (
        "tools/ldbc-reconcile.py", "tools/ldbc-inventory.py", "tools/ldbc-reconcile-context.mjs",
        "adapters/ldbc-gremlin/scan-page.groovy", "adapters/ldbc-gremlin/inventory-page.groovy")}
    binding = {"preparedSha256": args.prepared_sha256, "target": context["job"]["request"]["target"],
               "graphName": graph, "epochMarker": marker, "mappingDigest": context["job"]["config"]["mappingDigest"],
               "dependencies": dependencies}
    budgets = {"maxPages": args.max_pages, "ledgerBytes": args.ledger_budget_bytes,
               "summaryBytes": args.summary_budget_bytes, "pageSize": args.page_size,
               "responseBytes": args.max_response_bytes, "deadlineUnix": args.deadline_unix,
               "timeoutSeconds": args.timeout_seconds}
    used_pages, used_bytes, prior_challenge = (prior_usage(args.prior_scan, args.prior_scan_sha256, binding,
                                              component_sha, budgets, time_guard) if args.prior_scan else (0, 0, None))
    transport = r.Transport(args.endpoint, args.timeout_seconds, args.max_response_bytes, args.authorization_env)

    def guard():
        time_guard()
        need(file_identity(args.quiescence) == lease_identity, "Exclusive-access lease changed during scan")
        need(file_identity(args.prepared) == prepared_identity, "Prepared manifest changed during scan")
        r.lease_check(lease, context)
        transport.timeout = min(args.timeout_seconds, deadline - time.monotonic(), args.deadline_unix - time.time())

    guard()
    # Generated only after the external writer/drain/exclusion lease has passed.
    challenge = secrets.token_bytes(32)
    need(hashlib.sha256(challenge).hexdigest() != prior_challenge, "Full scan must use a fresh challenge")
    labels = {kind: expected["counts"][kind] for kind in CATEGORIES}
    wanted, actual = [Fingerprint(labels, challenge=challenge) for _ in range(2)]
    args.out.mkdir(mode=0o700)
    evidence = Evidence(args.out, args.max_pages, args.ledger_budget_bytes, args.summary_budget_bytes,
                        used_pages, used_bytes)
    try:
        evidence.save("challenge.json", {"protocol": PROTOCOL, "challengeHex": challenge.hex(),
                      "commitmentSha256": hashlib.sha256(challenge).hexdigest(),
                      "createdAfterLeaseCheckAt": dt.datetime.now(dt.timezone.utc).isoformat(),
                      "purpose": "multiset verification, not authentication"})
        evidence.save("quiescence.json", lease_raw)
    except BaseException:
        evidence.ledger.close()
        raise
    categories = ("vertices",) if args.scope == "vertices" else CATEGORIES
    outcome, failure, observed = "unknown", None, None
    content_matches = schema_matches = counts_match = False
    source_totals, actual_counts = None, None

    def cancelled(*_):
        raise InterruptedError("Cancelled; no scan resume or automatic retry")

    old_handler = signal.signal(signal.SIGTERM, cancelled)
    try:
        with (args.out / "inputs.jsonl").open("xb") as ledger:
            def source_record(value):
                raw = r.encoded(value) + b"\n"
                size = sum(p.stat().st_size for p in args.out.iterdir() if p.is_file() and p.name != "pages.jsonl")
                need(size + len(raw) + 65536 <= args.summary_budget_bytes, "Source evidence byte budget exceeded")
                ledger.write(raw)
                ledger.flush()
            source_totals = source_fingerprint(prepared, args.prepared.parent, categories, wanted, guard, source_record)
        evidence.save("expected.json", wanted.snapshot())
        observed = observe(context, lease, transport, args.alias, categories, actual, guard, evidence, args.page_size)
        evidence.save("actual.json", actual.snapshot())
        content_matches = r.exact(wanted.snapshot()) == r.exact(actual.snapshot())
        schema_matches = r.exact(expected["schema"]) == r.exact(observed["schema"])
        actual_counts = {kind: {label: str(sum(value[0] for (cat, name, _), value in actual.values.items()
                                              if cat == kind and name == label))
                                for label in labels[kind]} for kind in categories}
        counts_match = all(actual_counts[kind] == expected["counts"][kind] for kind in categories)
        guard()
        need(component_sha == r.digest(Path(__file__).read_bytes()) and all(
             digest == r.digest((ROOT / name).read_bytes()) for name, digest in dependencies.items()),
             "Verification implementation changed during scan")
        outcome = "matched" if content_matches and schema_matches and counts_match else "mismatch"
    except (Exception, KeyboardInterrupt) as error:
        failure = {"type": type(error).__name__, "message": str(error)[:1024]}
        if hasattr(error, "http"):
            failure["http"] = error.http
    finally:
        signal.signal(signal.SIGTERM, old_handler)
        evidence.ledger.close()
    facts = []
    try:
        for name in ("expected.json", "actual.json", "inputs.jsonl", "pages.jsonl", "quiescence.json", "challenge.json"):
            if (args.out / name).exists():
                facts.append(artifact(args.out / name, time_guard))
        time_guard()
    except Exception as error:
        outcome = "unknown"
        failure = {"type": type(error).__name__, "message": str(error)[:1024]}
    report = {"schemaVersion": 1, "kind": "sf100-sequential-content-observation", "protocol": PROTOCOL,
              "scope": args.scope, "vertexAdmission": args.vertex_admission, "outcome": outcome,
              "contentMatches": content_matches, "schemaMatches": schema_matches, "countsMatch": counts_match,
              "binding": binding, "anchorRequestDigest": context["requestDigest"], "componentSha256": component_sha,
              "challengeCommitment": hashlib.sha256(challenge).hexdigest(), "sourceCounts": source_totals,
              "counts": actual_counts, "pages": observed["pages"] if observed else None,
              "hiddenRecords": observed["hiddenRecords"] if observed else None,
              "requests": evidence.requests, "ledgerBytes": evidence.ledger_bytes,
              "wholeTaskPages": used_pages + evidence.requests,
              "wholeTaskLedgerBytes": used_bytes + evidence.ledger_bytes,
              "priorScan": {"sha256": args.prior_scan_sha256} if args.prior_scan else None,
              "budgets": budgets,
              "error": failure, "evidence": facts,
              "datasetVerified": args.scope == "full" and outcome == "matched", "performanceAccepted": False,
              "trust": "External lease must prove exclusive access and drained writers; digest equality assumes no collision"}
    evidence.save("scan.json", report)
    if args.scope == "full" and observed:
        evidence.save("inventory.json", {"schemaVersion": 1, "kind": "sf100-inventory-observation",
                      "outcome": outcome, "matches": outcome == "matched", "schemaMatch": schema_matches,
                      "countsMatch": counts_match, "contentMatches": content_matches,
                      "method": PROTOCOL, "scanSha256": r.digest((args.out / "scan.json").read_bytes()),
                      "actual": {"schemaVersion": 1, "scope": expected["scope"],
                                 "schema": observed["schema"], "counts": actual_counts}})
    print(json.dumps({"report": str(args.out / "scan.json"), "outcome": outcome,
                      "datasetVerified": report["datasetVerified"], "requests": evidence.requests,
                      "wholeTaskPages": report["wholeTaskPages"], "error": failure}))
    return 0 if outcome == "matched" else 4


if __name__ == "__main__":
    raise SystemExit(main())

