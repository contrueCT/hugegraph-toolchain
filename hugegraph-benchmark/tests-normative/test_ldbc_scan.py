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

from collections import Counter
import copy
import datetime as dt
import gzip
import hashlib
import http.server
import importlib.util
import io
import json
from pathlib import Path
import random
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("sequential_scan", ROOT / "tools/ldbc-scan.py")
s = importlib.util.module_from_spec(spec)
spec.loader.exec_module(s)


class SequentialScanTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.deadline = time.time() + 120
        self.v = {"id": "person:9007199254740993>甲", "label": "person", "properties": {
            "ldbcId": 9007199254740993, "firstName": " \t甲\n ", "emails": ["b", "a"]}}
        self.e = {"sourceId": self.v["id"], "targetId": "person:2", "label": "knows",
                  "properties": {"edgeKey": (1 << 63)-1, "stamp": "甲>"}}
        self.labels = {"vertices": ["person"], "edges": ["knows"]}
        self.challenge = bytes(range(32))
        self.epoch = "unit-epoch"
        self.marker = "hgb_epoch_" + s.r.digest(self.epoch.encode())[:32]
        self.context = {"requestDigest": "b" * 64, "job": {
            "request": {"target": {"targetDigest": "a" * 64, "epoch": self.epoch}},
            "inputRoot": str(self.base), "config": {"mappingDigest": "c" * 64,
                "target": {"graph": "unit", "host": "http://127.0.0.1", "port": 8080}}}}
        self.lease = {"schemaVersion": 1, "kind": "sf100-writer-quiescence", "requestDigest": "b"*64,
            "target": self.context["job"]["request"]["target"], "graphName": "unit", "epochMarker": self.marker,
            "validUntil": (dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=5)).isoformat(),
            "writersStopped": True, "inflightWritesDrained": True, "exclusiveAccess": True,
            "attestor": "unit", "basis": "protocol fixture, no actual database"}
        self.expected = {"schemaVersion": 1, "scope": "public-ldbc-initial-graph", "schema": {
            "propertyKeys": {}, "vertexLabels": {"person": {}}, "edgeLabels": {"knows": {}},
            "indexLabels": {}}, "counts": {"vertices": {"person": "1"}, "edges": {"knows": "1"}}}
        source_rows = {"vertices": {"vertexId": self.v["id"], **self.v["properties"]},
                       "edges": {"sourceId": self.e["sourceId"], "targetId": self.e["targetId"],
                                 **self.e["properties"]}}
        self.prepared = {"schemaVersion": 2, "vertices": 1, "edges": 1, "counts": {"person": 1, "knows": 1},
                         "files": {}, "shards": []}
        for category, label in [("vertices", "person"), ("edges", "knows")]:
            raw = s.r.encoded(source_rows[category]) + b"\n"
            stored = gzip.compress(raw, mtime=0)
            name = category + ".jsonl.gz"
            (self.base / name).write_bytes(stored)
            self.prepared["files"][name] = {"bytes": str(len(stored)), "sha256": s.r.digest(stored)}
            self.prepared["shards"].append({"id": category + "-0", "category": category, "label": label,
                "path": name, "bytes": str(len(stored)), "sha256": s.r.digest(stored), "records": "1",
                "encoding": {"compression": "gzip", "uncompressedBytes": str(len(raw)),
                             "uncompressedSha256": s.r.digest(raw)}})
        raw = s.r.encoded(self.expected)
        (self.base / "inventory.expected.json").write_bytes(raw)
        self.prepared["files"]["inventory.expected.json"] = {"bytes": len(raw), "sha256": s.r.digest(raw)}
        (self.base / "prepare-manifest.json").write_bytes(s.r.encoded(self.prepared))
        (self.base / "lease.json").write_bytes(s.r.encoded(self.lease))

    def fp(self, challenge=None):
        return s.Fingerprint(self.labels, challenge=challenge or self.challenge)

    def test_fingerprint_v1_frozen_valid_preimages_tags_and_aggregate(self):
        fixture = json.loads((ROOT / "tests-normative/fixtures/ldbc-scan-fingerprint-v1.json").read_text())
        self.assertEqual(fixture["protocol"], s.PROTOCOL)
        self.assertEqual(fixture["buckets"], 64)
        self.assertEqual(len({case["id"] for case in fixture["cases"]}), 43)
        challenge = bytes.fromhex(fixture["challengeHex"])
        aggregate = s.Fingerprint(fixture["labels"], challenge=challenge)

        class ObservedFingerprint(s.Fingerprint):
            def tag(self, purpose, raw):
                actual = super().tag(purpose, raw)
                self.observed[purpose] = (raw, actual.hex())
                return actual

        accepted = [case for case in fixture["cases"] if case["accepted"]]
        self.assertEqual(len(accepted), 8)
        for case in accepted:
            with self.subTest(vector=case["id"]):
                raw = case["rawUtf8"].encode("utf-8") if "rawUtf8" in case else bytes.fromhex(case["rawHex"])
                row = s.r.decode(raw)
                actual = ObservedFingerprint(fixture["labels"], challenge=challenge)
                actual.observed = {}
                actual.add(case["category"], row)
                expected = case["expected"]
                self.assertEqual(actual.observed[b"bucket"],
                                 (expected["identityUtf8"].encode("utf-8"), expected["bucketTagHex"]))
                self.assertEqual(actual.observed[b"a"],
                                 (expected["recordUtf8"].encode("utf-8"), expected["sumAHex"]))
                self.assertEqual(actual.observed[b"b"],
                                 (expected["recordUtf8"].encode("utf-8"), expected["sumBHex"]))
                self.assertEqual(next(iter(actual.values))[2], expected["bucket"])
                aggregate.add(case["category"], row)
        nonzero = [row for row in aggregate.snapshot()["rows"] if row["records"] != "0"]
        self.assertEqual(nonzero, fixture["expectedAggregateNonzero"])

    def test_fingerprint_v1_frozen_invalid_json_and_types_are_rejected(self):
        fixture = json.loads((ROOT / "tests-normative/fixtures/ldbc-scan-fingerprint-v1.json").read_text())
        rejected = [case for case in fixture["cases"] if not case["accepted"]]
        self.assertEqual(len(rejected), 35)
        for case in rejected:
            with self.subTest(vector=case["id"]), self.assertRaises(ValueError):
                raw = case["rawUtf8"].encode("utf-8") if "rawUtf8" in case else bytes.fromhex(case["rawHex"])
                row = s.r.decode(raw)
                actual = s.Fingerprint(fixture["labels"], challenge=bytes.fromhex(fixture["challengeHex"]))
                actual.add(case["category"], row)

    def page(self, category="vertices", page="", next_page=None, rows=None):
        return {"graphName": "unit", "epochMarker": self.marker, "kind": "scan-page", "category": category,
                "page": page, "nextPage": next_page, "scanned": "1", "hiddenRecords": "0",
                "rows": copy.deepcopy(rows if rows is not None else [self.v if category == "vertices" else self.e])}

    def server(self, mutate=None):
        owner = self
        class Server:
            def __init__(self):
                self.calls = []

            def post(self, body):
                self.calls.append(copy.deepcopy(body))
                b = body["bindings"]
                owner.assertEqual(set(b), {"graphName", "epochMarker", "category", "page", "pageSize"})
                if b["category"] == "schema":
                    row = {"graphName": "unit", "epochMarker": owner.marker, "kind": "schema",
                           "schema": copy.deepcopy(owner.expected["schema"])}
                else:
                    row = owner.page(b["category"], b["page"])
                if mutate:
                    mutate(row, len(self.calls))
                return [row], "f"*64
        return Server()

    def evidence(self, name="direct", max_pages=20, ledger_budget=1024**2, used_pages=0, used_bytes=0):
        out = self.base / name
        out.mkdir()
        evidence = s.Evidence(out, max_pages, ledger_budget, 1024**2, used_pages, used_bytes)
        self.addCleanup(evidence.ledger.close)
        return evidence

    def run_main(self, name, scope="full", policy="acknowledged-writes", prior=None, transport=None):
        out = self.base / name
        argv = ["ldbc-scan.py", "--job", str(self.base / "job.json"), "--prepared",
                str(self.base / "prepare-manifest.json"), "--prepared-sha256",
                s.r.digest((self.base / "prepare-manifest.json").read_bytes()), "--quiescence",
                str(self.base / "lease.json"), "--endpoint",
                "http://127.0.0.1:" + str(self.context["job"]["config"]["target"]["port"]) + "/gremlin", "--alias", "g",
                "--scope", scope, "--vertex-admission", policy, "--max-pages", "20",
                "--deadline-unix", str(self.deadline), "--out", str(out)]
        if prior:
            argv += ["--prior-scan", str(prior), "--prior-scan-sha256", s.r.digest(prior.read_bytes())]
        with patch("sys.argv", argv), patch.object(s.subprocess, "run", return_value=SimpleNamespace(
                stdout=s.r.encoded(self.context))), patch.object(s.r, "Transport", return_value=transport or self.server()), \
                patch("sys.stdout", new_callable=io.StringIO):
            code = s.main()
        return code, json.loads((out / "scan.json").read_text())

    def test_order_sets_and_independent_multiset_oracle(self):
        rng = random.Random(3346)
        for _ in range(100):
            rows = []
            for _ in range(rng.randrange(1, 12)):
                row = copy.deepcopy(self.v)
                row["id"] = "person:" + str(rng.randrange(5))
                rows.append(row)
            changed = copy.deepcopy(rows)
            rng.shuffle(changed)
            changed[0]["properties"]["emails"].reverse()
            if rng.randrange(2):
                changed.append(copy.deepcopy(self.v))
            a, b = self.fp(), self.fp()
            for row in rows:
                a.add("vertices", row)
            for row in changed:
                b.add("vertices", row)
            def oracle(values):
                result = []
                for value in values:
                    value = copy.deepcopy(value)
                    value["properties"]["emails"].sort()
                    result.append(json.dumps(value, sort_keys=True, ensure_ascii=False))
                return Counter(result)
            self.assertEqual(oracle(rows) == oracle(changed), a.snapshot() == b.snapshot())

    def test_endpoint_content_type_multiplicity_and_seed_changes_are_detected(self):
        original = self.fp()
        original.add("edges", self.e)
        for key, value in [("sourceId", "different"), ("targetId", "different"),
                           ("label", "unexpected"), ("properties", {"edgeKey": 0})]:
            changed = copy.deepcopy(self.e)
            changed[key] = value
            actual = self.fp()
            if key == "label":
                with self.assertRaises(ValueError):
                    actual.add("edges", changed)
            else:
                actual.add("edges", changed)
                self.assertNotEqual(original.snapshot(), actual.snapshot())
        duplicate = self.fp()
        duplicate.add("edges", self.e)
        duplicate.add("edges", self.e)
        self.assertNotEqual(original.snapshot(), duplicate.snapshot())
        other_seed = self.fp(b"x"*32)
        other_seed.add("edges", self.e)
        self.assertNotEqual(original.snapshot(), other_seed.snapshot())

    def test_exact_long_text_extra_properties_and_invalid_values(self):
        original = self.fp()
        original.add("vertices", self.v)
        for mutate in [lambda row: row["properties"].update(firstName="甲"),
                       lambda row: row["properties"].update(extra="present"),
                       lambda row: row["properties"].update(ldbcId=9007199254740992)]:
            row = copy.deepcopy(self.v)
            mutate(row)
            actual = self.fp()
            actual.add("vertices", row)
            self.assertNotEqual(original.snapshot(), actual.snapshot())
        for value in [True, 1.0, 1 << 63, -(1 << 63)-1, None]:
            row = copy.deepcopy(self.v)
            row["properties"]["ldbcId"] = value
            with self.assertRaises(ValueError):
                self.fp().add("vertices", row)
        row = copy.deepcopy(self.v)
        row["properties"]["emails"] = ["same", "same"]
        with self.assertRaises(ValueError):
            self.fp().add("vertices", row)

    def test_full_source_stream_checks_both_compressed_and_expanded_identities(self):
        actual, records = self.fp(), []
        totals = s.source_fingerprint(self.prepared, self.base, s.CATEGORIES, actual, lambda: None, records.append)
        expected = self.fp()
        expected.add("vertices", self.v)
        expected.add("edges", self.e)
        self.assertEqual(actual.snapshot(), expected.snapshot())
        self.assertEqual(totals, {"vertices": 1, "edges": 1})
        self.assertEqual(len(records), 2)
        self.assertEqual(records[0]["expandedSha256"], self.prepared["shards"][0]["encoding"]["uncompressedSha256"])

    def test_source_corruption_crc_expansion_counts_paths_and_duplicates_fail_closed(self):
        mutations = [
            lambda p: p["shards"][0]["encoding"].update(uncompressedSha256="0"*64),
            lambda p: p["shards"][0]["encoding"].update(uncompressedBytes="1"),
            lambda p: p["shards"][0].update(records="0"),
            lambda p: p["shards"][0].update(path="../vertices.jsonl.gz"),
            lambda p: p["shards"].append(copy.deepcopy(p["shards"][0])),
            lambda p: p.update(vertices=2),
        ]
        for mutate in mutations:
            prepared = copy.deepcopy(self.prepared)
            mutate(prepared)
            with self.subTest(mutate=mutate), self.assertRaises((ValueError, FileNotFoundError)):
                s.source_fingerprint(prepared, self.base, s.CATEGORIES, self.fp(), lambda: None, lambda _: None)
        path = self.base / self.prepared["shards"][0]["path"]
        raw = bytearray(path.read_bytes())
        raw[-8] ^= 1
        path.write_bytes(raw)
        shard = copy.deepcopy(self.prepared)
        shard["shards"][0]["sha256"] = s.r.digest(raw)
        shard["files"][path.name]["sha256"] = s.r.digest(raw)
        with self.assertRaises((ValueError, OSError)):
            s.source_fingerprint(shard, self.base, s.CATEGORIES, self.fp(), lambda: None, lambda _: None)

    def test_scan_checks_every_category_and_schema_and_chains_receipts(self):
        evidence, actual, server = self.evidence(), self.fp(), self.server()
        result = s.observe(self.context, self.lease, server, "g", s.CATEGORIES, actual,
                           lambda: None, evidence, 4096)
        self.assertEqual(result["pages"], {"vertices": 1, "edges": 1})
        self.assertEqual(result["hiddenRecords"], {"vertices": 0, "edges": 0})
        self.assertEqual(evidence.requests, 4)
        rows = (evidence.out / "pages.jsonl").read_bytes().splitlines(keepends=True)
        for i, raw in enumerate(rows):
            self.assertEqual(json.loads(raw)["previousEntrySha256"], s.r.digest(rows[i-1]) if i else "0"*64)
        expected = self.fp()
        expected.add("vertices", self.v)
        expected.add("edges", self.e)
        self.assertEqual(expected.snapshot(), actual.snapshot())

    def test_page_identity_cursor_counts_types_and_shape_faults_are_rejected(self):
        mutations = [lambda row: row.update(graphName="other"), lambda row: row.update(epochMarker="other"),
                     lambda row: row.update(page="other"), lambda row: row.update(nextPage=""),
                     lambda row: row.update(nextPage="x"*32769), lambda row: row.update(scanned="01"),
                     lambda row: row.update(scanned=1), lambda row: row.update(hiddenRecords="2"),
                     lambda row: row.update(scanned="4097"), lambda row: row.update(extra=1),
                     lambda row: row.update(rows=[])]
        for mutate in mutations:
            fold = s.PageFold("vertices", "unit", self.marker, self.fp())
            row = self.page()
            mutate(row)
            with self.assertRaises(ValueError):
                fold.accept("", row)
            with self.assertRaisesRegex(ValueError, "Failed scan"):
                fold.accept("", self.page())

    def test_cursor_cycles_and_hidden_rows_are_accounted(self):
        fold = s.PageFold("vertices", "unit", self.marker, self.fp())
        hidden = self.page(next_page="next", rows=[])
        hidden["hiddenRecords"] = "1"
        fold.accept("", hidden)
        row = self.page(page="next", next_page="next")
        with self.assertRaisesRegex(ValueError, "Repeated"):
            fold.accept("next", row)
        with self.assertRaises(ValueError):
            s.PageFold("vertices", "unit", self.marker, self.fp(), page_size=True)

    def test_shared_page_budget_and_ledger_budget_stop_without_retry(self):
        for name, params in [("pages", {"max_pages": 10, "used_pages": 8}),
                             ("bytes", {"ledger_budget": 64})]:
            server = self.server()
            evidence = self.evidence(name, **params)
            with self.assertRaisesRegex(ValueError, "budget"):
                s.observe(self.context, self.lease, server, "g", ("vertices",), self.fp(),
                          lambda: None, evidence, 4096)
            self.assertLessEqual(len(server.calls), 2)

    def test_schema_drift_wrong_epoch_and_deadline_guard_prevent_success(self):
        for name, mutation in [("drift", lambda row, n: row["schema"].update(indexLabels={"extra": {}}) if n == 3 else None),
                               ("epoch", lambda row, n: row.update(epochMarker="other") if n == 2 else None)]:
            with self.assertRaises(ValueError):
                s.observe(self.context, self.lease, self.server(mutation), "g", ("vertices",), self.fp(),
                          lambda: None, self.evidence(name), 4096)
        server = self.server()
        def expired():
            raise TimeoutError("deadline")
        with self.assertRaises(TimeoutError):
            s.observe(self.context, self.lease, server, "g", ("vertices",), self.fp(), expired,
                      self.evidence("expired"), 4096)
        self.assertEqual(server.calls, [])

    def test_failed_http_attempt_is_retained_once(self):
        class Failing:
            calls = 0
            def post(self, body):
                self.calls += 1
                raise TimeoutError("bounded")
        server, evidence = Failing(), self.evidence()
        with self.assertRaises(TimeoutError):
            s.observe(self.context, self.lease, server, "g", s.CATEGORIES, self.fp(),
                      lambda: None, evidence, 4096)
        self.assertEqual(server.calls, 1)
        self.assertEqual(evidence.requests, 1)
        self.assertEqual(json.loads((evidence.out / "pages.jsonl").read_text())["errorType"], "TimeoutError")

    def test_default_vertex_gate_then_full_scan_share_budget_and_new_challenge(self):
        code, vertex = self.run_main("vertices", "vertices", "content")
        self.assertEqual(code, 0)
        self.assertFalse(vertex["datasetVerified"])
        code, full = self.run_main("full", policy="content", prior=self.base / "vertices/scan.json")
        self.assertEqual(code, 0)
        self.assertTrue(full["datasetVerified"])
        self.assertEqual(full["wholeTaskPages"], vertex["requests"] + full["requests"])
        self.assertEqual(full["wholeTaskLedgerBytes"], vertex["ledgerBytes"] + full["ledgerBytes"])
        self.assertNotEqual(vertex["challengeCommitment"], full["challengeCommitment"])
        self.assertFalse(full["performanceAccepted"])

    def test_acknowledged_writes_policy_is_explicit_and_full_content_still_required(self):
        code, full = self.run_main("acknowledged")
        self.assertEqual(code, 0)
        self.assertEqual(full["vertexAdmission"], "acknowledged-writes")
        self.assertTrue(full["datasetVerified"])
        with patch("sys.stderr", new_callable=io.StringIO), self.assertRaises(SystemExit) as error:
            self.run_main("no-proof", policy="content")
        self.assertEqual(error.exception.code, 2)

    def test_changed_content_returns_exit4_despite_matching_counts(self):
        server = self.server(lambda row, n: row["rows"][0]["properties"].update(ldbcId=1)
                             if row.get("category") == "vertices" else None)
        code, full = self.run_main("wrong", transport=server)
        self.assertEqual(code, 4)
        self.assertEqual(full["outcome"], "mismatch")
        self.assertTrue(full["countsMatch"])
        self.assertFalse(full["contentMatches"])
        self.assertFalse(full["datasetVerified"])

    def test_changed_lease_fails_before_accepting_the_first_response(self):
        def changed(row, number):
            (self.base / "lease.json").write_text("{}")
        server = self.server(changed)
        code, result = self.run_main("lease-changed", transport=server)
        self.assertEqual(code, 4)
        self.assertEqual(result["outcome"], "unknown")
        self.assertEqual(result["requests"], 1)
        self.assertFalse(result["datasetVerified"])
        self.assertEqual(len(server.calls), 1)

    def test_prior_artifact_tampering_is_detected_before_new_requests(self):
        self.run_main("vertices", "vertices", "content")
        (self.base / "vertices/pages.jsonl").write_text("changed")
        server = self.server()
        with self.assertRaises(ValueError):
            self.run_main("full", policy="content", prior=self.base / "vertices/scan.json", transport=server)
        self.assertEqual(server.calls, [])

    def test_second_scan_cannot_reset_the_whole_task_deadline(self):
        self.run_main("vertices", "vertices", "content")
        self.deadline += 1
        server = self.server()
        with self.assertRaisesRegex(ValueError, "Prior scan budget differs"):
            self.run_main("full", policy="content", prior=self.base / "vertices/scan.json", transport=server)
        self.assertEqual(server.calls, [])

    def test_real_gzip_http_and_failed_attempt_keep_evidence_without_retry(self):
        owner = self
        responses, requests = [], []
        class Handler(http.server.BaseHTTPRequestHandler):
            failed = False
            def do_POST(self):
                requests.append(json.loads(self.rfile.read(int(self.headers["Content-Length"]))))
                owner.assertEqual(self.path, "/gremlin")
                owner.assertEqual(self.headers["Accept-Encoding"], "gzip")
                if Handler.failed:
                    self.send_response(500)
                    raw = b'{"exception":"java.util.concurrent.TimeoutException","message":"private-provider-body"}'
                else:
                    data, _ = owner.server().post(requests[-1])
                    self.send_response(200)
                    raw = gzip.compress(s.r.encoded({"status": {"code": 200}, "result": {"data": data}}), mtime=0)
                    self.send_header("Content-Encoding", "gzip")
                responses.append(raw)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

            def log_message(self, *_):
                pass

        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            port = server.server_address[1]
            self.context["job"]["config"]["target"]["port"] = port
            transport = s.r.Transport(f"http://127.0.0.1:{port}/gremlin", 2, 1024**2)
            code, report = self.run_main("real-http", transport=transport)
            self.assertEqual(code, 0)
            self.assertTrue(report["datasetVerified"])
            ledger = [json.loads(raw) for raw in (self.base / "real-http/pages.jsonl").read_bytes().splitlines()]
            self.assertEqual([row["responseSha256"] for row in ledger], [s.r.digest(raw) for raw in responses])
            Handler.failed = True
            before = len(requests)
            code, failed = self.run_main("failed-http", transport=transport)
            self.assertEqual(code, 4)
            self.assertEqual(len(requests), before + 1)
            self.assertEqual(failed["outcome"], "unknown")
            self.assertEqual(failed["requests"], 1)
            self.assertNotIn("private-provider-body", json.dumps(failed))
        finally:
            server.shutdown()
            server.server_close()
            thread.join(5)

    def test_reusing_vertex_challenge_fails_before_starting_full_scan(self):
        with patch.object(s.secrets, "token_bytes", return_value=b"x"*32):
            self.run_main("vertices", "vertices", "content")
            server = self.server()
            with self.assertRaisesRegex(ValueError, "fresh challenge"):
                self.run_main("full", policy="content", prior=self.base / "vertices/scan.json", transport=server)
            self.assertEqual(server.calls, [])

    def test_evidence_finalization_timeout_cannot_qualify_a_matching_scan(self):
        original = s.artifact
        def deadline(path, guard=lambda: None):
            if path.name == "pages.jsonl":
                raise TimeoutError("Whole-task absolute deadline reached")
            return original(path, guard)
        with patch.object(s, "artifact", side_effect=deadline):
            code, report = self.run_main("finalization-deadline")
        self.assertEqual(code, 4)
        self.assertEqual(report["outcome"], "unknown")
        self.assertTrue(report["contentMatches"])
        self.assertFalse(report["datasetVerified"])


if __name__ == "__main__":
    unittest.main()

