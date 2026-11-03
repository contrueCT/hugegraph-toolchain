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

"""Read-only, exact full-shard verification. Requires a trusted external writer-quiescence lease.

Never retries HTTP. Produces an observation for hgbench benchmark recover; it never restarts Loader.
The lease attests that all writers are stopped, in-flight writes drained, and exclusive access held.
This component verifies content/epoch and lease binding/expiry, not the truth of operator attestations.
"""
import argparse
import datetime as dt
from contextlib import nullcontext
import gzip
import hashlib
import http.client
import io
import json
import os
from pathlib import Path
import re
import subprocess
import socket
import threading
import time
import urllib.parse

ROOT = Path(__file__).resolve().parents[1]


def pairs(items):
    result = {}
    for key, value in items:
        if key in result:
            raise ValueError("Duplicate JSON key")
        result[key] = value
    return result


def decode(raw):
    return json.loads(raw, object_pairs_hook=pairs,
                      parse_constant=lambda value: (_ for _ in ()).throw(ValueError("Non-finite JSON")))


def encoded(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def exact(value):
    # Python bool/int and int/float compare equal; explicit type tagging prevents coercion.
    if isinstance(value, dict):
        return ("object", tuple((k, exact(v)) for k, v in sorted(value.items())))
    if isinstance(value, list):
        return ("array", tuple(exact(v) for v in value))
    return (type(value).__name__, value)


def properties(value):
    if not isinstance(value, dict):
        raise ValueError("Properties must be an object")
    result = dict(value)
    for key in ("emails", "languages"):
        if key in result:
            if not isinstance(result[key], list) or any(type(v) is not str for v in result[key]):
                raise ValueError("Invalid SET property")
            if len(set(result[key])) != len(result[key]):
                raise ValueError("Duplicate SET value")
            result[key] = sorted(result[key])
    return result


def expected(row, category, label):
    if category == "vertices":
        identifier = row["vertexId"]
        if not isinstance(identifier, str):
            raise ValueError("Expected string vertex ID")
        selector = {"id": identifier}
        item = {"id": identifier, "label": label,
                "properties": properties({k: v for k, v in row.items() if k != "vertexId"})}
    else:
        if type(row["edgeKey"]) is not int or not -(2**63) <= row["edgeKey"] < 2**63:
            raise ValueError("Invalid exact edgeKey")
        selector = {"sourceId": row["sourceId"], "targetId": row["targetId"], "edgeKey": row["edgeKey"]}
        item = {"sourceId": row["sourceId"], "targetId": row["targetId"], "label": label,
                "properties": properties({k: v for k, v in row.items() if k not in ("sourceId", "targetId")})}
    return selector, item


def http_failure(response, deadline, max_bytes):
    """Retain status and bounded safe codes, never response messages, headers or request text."""
    error = ValueError("HTTP status " + str(response.status))
    details = {"status": response.status, "bodyState": "incomplete", "wirePrefixBytes": 0,
               "wirePrefixSha256": digest(b"")}
    error.http = details
    raw = bytearray()
    limit = min(max_bytes, 65536)
    try:
        while len(raw) <= limit:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("Error response deadline")
            sock = getattr(getattr(response.fp, "raw", None), "_sock", None)
            if sock is not None:
                sock.settimeout(remaining)
            part = response.read1(min(8192, limit + 1 - len(raw)))
            if not part:
                break
            raw.extend(part)
        if len(raw) > limit:
            details["bodyState"] = "wire-limit"
            return error
        encoding = (response.getheader("Content-Encoding") or "identity").lower().strip()
        decoded = bytes(raw)
        if encoding == "gzip":
            with gzip.GzipFile(fileobj=io.BytesIO(raw)) as stream:
                decoded = stream.read(limit + 1)
        elif encoding != "identity":
            details["bodyState"] = "unsupported-encoding"
            return error
        if len(decoded) > limit:
            details["bodyState"] = "decoded-limit"
            return error
        details["bodyState"] = "invalid-json"
        value = decode(decoded)
        if not isinstance(value, dict):
            return error
        details["bodyState"] = "complete-json"
        code = value.get("status", {}).get("code") if isinstance(value.get("status"), dict) else None
        if type(code) is int and 100 <= code <= 599:
            details["providerCode"] = code
        exception = value.get("exception")
        known = {"TimeoutException", "RejectedExecutionException", "IllegalArgumentException",
                 "IllegalStateException", "NotFoundException", "ServerGremlinException", "BackendException",
                 "SecurityException", "NoSuchElementException", "UnsupportedOperationException", "IOException"}
        if isinstance(exception, str) and len(exception) <= 256 and exception.rsplit('.', 1)[-1] in known:
            details["providerException"] = exception.rsplit('.', 1)[-1]
    except Exception:
        # Body timeout/invalid encoding must not replace the already-known HTTP failure.
        pass
    finally:
        prefix = raw[:limit]
        details["wirePrefixBytes"] = len(prefix)
        details["wirePrefixSha256"] = digest(prefix)
        try:
            response.close()
        except OSError:
            pass
    return error


class Transport:
    def __init__(self, endpoint, timeout, max_bytes, auth_env=None):
        url = urllib.parse.urlsplit(endpoint)
        if url.scheme not in ("http", "https") or not url.hostname or url.username or url.password or url.fragment:
            raise ValueError("Explicit HTTP(S) endpoint required, without credentials or fragment")
        self.url, self.timeout, self.max_bytes = url, timeout, max_bytes
        self.authorization = None
        if auth_env:
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", auth_env):
                raise ValueError("Invalid authorization environment name")
            self.authorization = os.environ[auth_env]
            if not self.authorization or "\r" in self.authorization or "\n" in self.authorization:
                raise ValueError("Invalid authorization value")

    def post(self, body):
        deadline = time.monotonic() + self.timeout
        cls = http.client.HTTPSConnection if self.url.scheme == "https" else http.client.HTTPConnection
        connection = cls(self.url.hostname, self.url.port, timeout=self.timeout)
        headers = {"Content-Type": "application/json", "Accept": "application/json", "Accept-Encoding": "gzip"}
        if self.authorization:
            headers["Authorization"] = self.authorization
        def interrupt():
            sock = connection.sock
            if sock is not None:
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
            connection.close()
        watchdog = threading.Timer(self.timeout, interrupt)
        watchdog.daemon = True
        watchdog.start()
        try:
            connection.request("POST", urllib.parse.urlunsplit(("", "", self.url.path or "/", self.url.query, "")),
                               body=encoded(body), headers=headers)
            response = connection.getresponse()
            if response.status != 200:
                raise http_failure(response, deadline, self.max_bytes)
            raw = bytearray()
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("Whole response deadline exceeded")
                # Response owns the socket after Connection: close; use its raw socket in that case.
                sock = connection.sock or getattr(getattr(response.fp, "raw", None), "_sock", None)
                if sock is not None:
                    sock.settimeout(remaining)
                part = response.read1(min(8192, self.max_bytes + 1 - len(raw)))
                if not part:
                    break
                raw.extend(part)
                if len(raw) > self.max_bytes:
                    raise ValueError("Response byte budget exceeded")
            # HugeGraph's REST Gremlin endpoint compresses successful responses.
            # Bound wire and decoded bytes independently; retain the wire digest.
            encoding = (response.getheader("Content-Encoding") or "identity").lower()
            decoded = bytes(raw)
            if encoding == "gzip":
                with gzip.GzipFile(fileobj=io.BytesIO(raw), mode="rb") as compressed:
                    decoded = compressed.read(self.max_bytes + 1)
                if len(decoded) > self.max_bytes:
                    raise ValueError("Decoded response byte budget exceeded")
            elif encoding != "identity":
                raise ValueError("Unsupported response encoding")
            if time.monotonic() > deadline:
                raise TimeoutError("Whole response deadline exceeded during decoding")
            result = decode(decoded)
            status = result.get("status", {}).get("code")
            if type(status) is not int or status != 200 or not isinstance(result.get("result", {}).get("data"), list):
                raise ValueError("Invalid Gremlin response")
            return result["result"]["data"], digest(raw)
        finally:
            watchdog.cancel()
            connection.close()


def lease_check(lease, context):
    request = context["job"]["request"]
    graph = context["job"]["config"]["target"]["graph"]
    marker = "hgb_epoch_" + digest(request["target"]["epoch"].encode())[:32]
    required = {"schemaVersion", "kind", "requestDigest", "target", "graphName", "epochMarker", "validUntil",
                "writersStopped", "inflightWritesDrained", "exclusiveAccess", "attestor", "basis"}
    if set(lease) != required or lease["schemaVersion"] != 1 or lease["kind"] != "sf100-writer-quiescence":
        raise ValueError("Invalid quiescence attestation contract")
    if lease["requestDigest"] != context["requestDigest"] or lease["target"] != request["target"]:
        raise ValueError("Quiescence attestation belongs to another attempt or epoch")
    if lease["graphName"] != graph or lease["epochMarker"] != marker:
        raise ValueError("Quiescence graph/marker mismatch")
    if any(lease[k] is not True for k in ("writersStopped", "inflightWritesDrained", "exclusiveAccess")):
        raise ValueError("All writers must be stopped, drained and excluded")
    if any(not isinstance(lease[k], str) or not lease[k].strip() for k in ("attestor", "basis")):
        raise ValueError("Attestor and verifiable external basis required")
    expiry = dt.datetime.fromisoformat(lease["validUntil"].replace("Z", "+00:00"))
    if expiry.tzinfo is None or expiry <= dt.datetime.now(dt.timezone.utc):
        raise ValueError("Quiescence lease expired or lacks timezone")
    return graph, marker


class HashedInput(io.RawIOBase):
    """Count/hash exactly the stored stream while bounding buffered decoder reads."""
    def __init__(self, source, limit):
        self.source, self.limit, self.count = source, limit, 0
        self.digest = hashlib.sha256()

    def readable(self):
        return True

    def readinto(self, buffer):
        raw = self.source.read(min(len(buffer), self.limit - self.count + 1))
        if self.count + len(raw) > self.limit:
            raise ValueError("Input exceeds frozen stored-byte count")
        self.count += len(raw)
        self.digest.update(raw)
        buffer[:len(raw)] = raw
        return len(raw)


def reconciliation_script(lookup_mode):
    scripts = {"traversal": "reconcile.groovy", "native-batch": "reconcile-batch.groovy"}
    if lookup_mode not in scripts:
        raise ValueError("Unsupported lookup mode")
    return ROOT / "adapters/ldbc-gremlin" / scripts[lookup_mode]


def scan(context, lease, transport, alias, batch_size, ledger, max_evidence_bytes, lookup_mode="traversal"):
    job = context["job"]
    graph, marker = lease_check(lease, context)
    category = "vertices" if job["mapping"]["vertices"] else "edges"
    mapping = job["mapping"][category][0]
    artifact = job["request"]["inputs"][0]
    path = Path(job["inputRoot"]) / artifact["path"]
    counts = {"checked": 0, "matched": 0, "absent": 0, "different": 0}
    written = 0
    script = reconciliation_script(lookup_mode).read_text()
    data_hash = hashlib.sha256()
    encoding = job.get("encoding")
    compressed = mapping.get("input", {}).get("compression") == "GZIP"
    if compressed != bool(encoding) or encoding and encoding.get("compression") != "gzip":
        raise ValueError("Compression and expanded content identity differ")
    expected_bytes = int(encoding["uncompressedBytes"] if encoding else artifact["bytes"])
    expected_hash = encoding["uncompressedSha256"] if encoding else artifact["sha256"]
    expected_records = int(job["request"]["shard"]["records"])
    if min(expected_bytes, expected_records, int(artifact["bytes"])) < 0:
        raise ValueError("Negative shard bounds")

    def batch(rows):
        nonlocal written
        lease_check(lease, context)
        items = [expected(row, category, mapping["label"]) for row in rows]
        body = {"gremlin": script, "language": "gremlin-groovy", "aliases": {"g": alias}, "bindings": {
            "graphName": graph, "epochMarker": marker, "category": category, "label": mapping["label"],
            "selectors": [item[0] for item in items]}}
        result, response_sha = transport.post(body)
        if len(result) != 1 or set(result[0]) != {"graphName", "epochMarker", "rows"}:
            raise ValueError("Invalid reconciliation response shape")
        record = result[0]
        if record["graphName"] != graph or record["epochMarker"] != marker or len(record["rows"]) != len(items):
            raise ValueError("Target/epoch/response cardinality changed")
        before = dict(counts)
        for (_, wanted), observed in zip(items, record["rows"]):
            if not isinstance(observed, list):
                raise ValueError("Invalid identity observation")
            counts["checked"] += 1
            if not observed:
                counts["absent"] += 1
                continue
            for value in observed:
                value["properties"] = properties(value["properties"])
            counts["matched" if len(observed) == 1 and exact(observed[0]) == exact(wanted) else "different"] += 1
        entry = encoded({"firstRecord": before["checked"], "records": len(rows),
            "requestSha256": digest(encoded(body)), "responseSha256": response_sha,
            "counts": {k: counts[k] - before[k] for k in counts}}) + b"\n"
        written += len(entry)
        if written > max_evidence_bytes:
            raise ValueError("Evidence byte budget exceeded")
        ledger.write(entry)
        ledger.flush()
        lease_check(lease, context)

    rows = []
    byte_count = 0
    with path.open("rb") as file:
        stored = HashedInput(file, int(artifact["bytes"]))
        with io.BufferedReader(stored) as buffered:
            decoder = gzip.GzipFile(fileobj=buffered, mode="rb") if compressed else nullcontext(buffered)
            with decoder as source:
                while True:
                    raw = source.readline(min(16 * 1024 * 1024 + 1, expected_bytes - byte_count + 1))
                    if not raw:
                        break
                    if byte_count + len(raw) > expected_bytes:
                        raise ValueError("Input exceeds frozen uncompressed byte budget")
                    if len(raw) > 16 * 1024 * 1024 or not raw.endswith(b"\n"):
                        raise ValueError("Oversized or unterminated JSONL record")
                    if counts["checked"] + len(rows) >= expected_records:
                        raise ValueError("Shard row count exceeds frozen bound")
                    data_hash.update(raw)
                    byte_count += len(raw)
                    rows.append(decode(raw))
                    if len(rows) == batch_size:
                        batch(rows)
                        rows = []
    if rows:
        batch(rows)
    else:
        batch([])  # Also validate epoch for an empty shard and at the end of a full batch.
    if stored.digest.hexdigest() != artifact["sha256"] or str(stored.count) != artifact["bytes"]:
        raise ValueError("Stored input changed while scanning")
    if data_hash.hexdigest() != expected_hash or byte_count != expected_bytes:
        raise ValueError("Input changed while scanning")
    if str(counts["checked"]) != job["request"]["shard"]["records"]:
        raise ValueError("Shard row count mismatch")
    lease_check(lease, context)
    disposition = "committed" if counts["matched"] == counts["checked"] else (
        "absent" if counts["absent"] == counts["checked"] else "partial")
    return disposition, counts


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--job", type=Path, required=True, help="Frozen loader-plan job JSON; build TS controller first")
    parser.add_argument("--quiescence", type=Path, required=True, help="Trusted operator lease JSON; see docs/ldbc-adapter.md")
    parser.add_argument("--alias", required=True, help="Graph traversal alias exposed by the target Gremlin service")
    parser.add_argument("--endpoint", required=True, help="Exact HTTP(S) Gremlin endpoint; no redirects or retries")
    parser.add_argument("--authorization-env", help="Name of environment variable containing complete Authorization value")
    parser.add_argument("--lookup-mode", choices=("traversal", "native-batch"), default="traversal",
                        help="traversal: existing per-ID reads; native-batch: HugeGraph 1.8 native ID batch reads "
                             "for standalone edge labels with edgeKey as the only sort key; exact checks unchanged")
    parser.add_argument("--batch-size", type=int, default=256,
                        help="Records per HTTP request (1-4096); native-batch also batches backend ID reads")
    parser.add_argument("--timeout-seconds", type=float, default=30)
    parser.add_argument("--max-response-bytes", type=int, default=8 * 1024 * 1024)
    parser.add_argument("--out", type=Path, required=True, help="New directory for immutable observation and evidence")
    args = parser.parse_args()
    if not 1 <= args.batch_size <= 4096 or not 0 < args.timeout_seconds <= 3600 or not 1 <= args.max_response_bytes <= 64 * 1024**2:
        parser.error("Batch/time/response budgets are out of range")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.-]{0,127}", args.alias):
        parser.error("Invalid traversal alias")
    context_run = subprocess.run(["node", str(ROOT / "tools/ldbc-reconcile-context.mjs"), str(args.job.resolve())],
                                 capture_output=True, text=True, timeout=60, check=True)
    context = decode(context_run.stdout)
    lease_raw = args.quiescence.read_bytes()
    lease = decode(lease_raw)
    lease_check(lease, context)
    target = context["job"]["config"]["target"]
    endpoint = urllib.parse.urlsplit(args.endpoint)
    origin = urllib.parse.urlsplit(target["host"])
    if endpoint.scheme != origin.scheme or endpoint.hostname != origin.hostname or (endpoint.port or
            (443 if endpoint.scheme == "https" else 80)) != target["port"] or endpoint.path != "/gremlin" or endpoint.query:
        raise ValueError("Reconciliation must use the frozen Loader target origin and /gremlin path")
    transport = Transport(args.endpoint, args.timeout_seconds, args.max_response_bytes, args.authorization_env)
    args.out.mkdir(mode=0o700)
    (args.out / "quiescence.json").write_bytes(lease_raw)
    budget = int(context["job"]["request"]["outputBudgetBytes"]) - len(lease_raw) - 8192
    if budget < 1024:
        raise ValueError("Output budget too small for evidence")
    disposition, counts, failure = "unknown", None, None
    with (args.out / "batches.jsonl").open("xb") as ledger:
        try:
            disposition, counts = scan(context, lease, transport, args.alias, args.batch_size, ledger, budget,
                                       args.lookup_mode)
        except Exception as error:
            failure = {"type": type(error).__name__, "message": str(error)[:1024]}
            if hasattr(error, "http"):
                failure["http"] = error.http
    quiesced = True
    try:
        lease_check(lease, context)
    except ValueError:
        quiesced = False
        disposition = "unknown"
    report = {"schemaVersion": 1, "scope": "entire-shard-identities-and-content", "disposition": disposition,
              "counts": counts, "error": failure, "endpoint": args.endpoint, "alias": args.alias,
              "trust": "Operator attestation must independently prove exclusive access and drained writes; not inferred from Loader exit",
              "extraRecordsOutsideShard": "not-checked; final whole-graph per-label counts required",
              "lookupMode": args.lookup_mode,
              "scriptSha256": digest(reconciliation_script(args.lookup_mode).read_bytes()),
              "componentSha256": digest(Path(__file__).read_bytes())}
    (args.out / "reconciliation.json").write_bytes(encoded(report) + b"\n")
    evidence = []
    for name in ("quiescence.json", "batches.jsonl", "reconciliation.json"):
        raw = (args.out / name).read_bytes()
        evidence.append({"path": name, "sha256": digest(raw), "bytes": str(len(raw))})
    request = context["job"]["request"]
    observation = {"protocolVersion": 1, "kind": "sf100-load-observation", "requestDigest": context["requestDigest"],
                   "target": request["target"], "shardId": request["shard"]["id"], "disposition": disposition,
                   "records": str(counts["matched"]) if counts is not None else None,
                   "scope": "entire-shard-identities-and-content", "writerQuiesced": quiesced, "evidence": evidence}
    (args.out / "observation.json").write_bytes(encoded(observation) + b"\n")
    print(json.dumps({"observation": str(args.out / "observation.json"), **report}))
    return 0 if disposition == "committed" else 4


if __name__ == "__main__":
    raise SystemExit(main())
