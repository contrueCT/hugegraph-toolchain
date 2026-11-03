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

import copy
import datetime as dt
import gzip
import importlib.util
import io
import http.server
import threading
import time
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from contextlib import redirect_stdout

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("reconcile", ROOT / "tools/ldbc-reconcile.py")
r = importlib.util.module_from_spec(spec)
spec.loader.exec_module(r)


class ReconcileTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.row = {"vertexId": "person:9007199254740993", "ldbcId": 9007199254740993,
                    "firstName": "甲", "emails": ["b", "a"]}
        self.raw = r.encoded(self.row) + b"\n"
        (self.root / "shard.json").write_bytes(self.raw)
        request = {"target": {"targetDigest": "a"*64, "epoch": "unit-epoch"},
                   "inputs": [{"path": "shard.json", "sha256": r.digest(self.raw), "bytes": str(len(self.raw))}],
                   "shard": {"records": "1"}}
        self.context = {"requestDigest": "b"*64, "job": {"request": request, "inputRoot": str(self.root),
             "config": {"target": {"graph": "unit"}}, "mapping": {"vertices": [{"label": "person"}], "edges": []}}}
        self.lease = {"schemaVersion": 1, "kind": "sf100-writer-quiescence", "requestDigest": "b"*64,
            "target": request["target"], "graphName": "unit", "epochMarker": "hgb_epoch_" + r.digest(b"unit-epoch")[:32],
            "validUntil": (dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=5)).isoformat(),
            "writersStopped": True, "inflightWritesDrained": True, "exclusiveAccess": True,
            "attestor": "unit", "basis": "unit fixture only"}

    def run_scan(self, change=lambda value: value, mode="normal", budget=10000, lookup_mode="traversal"):
        owner = self
        class Server:
            def post(self, body):
                b = body["bindings"]
                owner.assertEqual(b["selectors"], [{"id": owner.row["vertexId"]}])
                owner.assertEqual(body["gremlin"], r.reconciliation_script(lookup_mode).read_text())
                observed = r.expected(owner.row, "vertices", "person")[1]
                if mode == "timeout":
                    raise TimeoutError("bounded")
                result = {"graphName": b["graphName"], "epochMarker": b["epochMarker"],
                          "rows": [change([observed])]}
                if mode == "wrong-epoch":
                    result["epochMarker"] = "wrong"
                if mode == "changed-input":
                    (owner.root / "shard.json").write_bytes(b"changed")
                return [result], "c"*64
        return r.scan(self.context, self.lease, Server(), "g", 256, io.BytesIO(), budget, lookup_mode)

    def test_native_batch_keeps_exact_properties_and_text(self):
        self.assertEqual(self.run_scan(lookup_mode="native-batch")[0], "committed")
        def corrupt(rows):
            rows[0]["properties"]["ldbcId"] = float(9007199254740993)
            return rows
        self.assertEqual(self.run_scan(corrupt, lookup_mode="native-batch")[0], "partial")
        self.row["firstName"] = " \t甲\n "
        self.raw = r.encoded(self.row) + b"\n"
        (self.root / "shard.json").write_bytes(self.raw)
        self.context["job"]["request"]["inputs"][0].update(sha256=r.digest(self.raw), bytes=str(len(self.raw)))
        def trim(rows):
            rows[0]["properties"]["firstName"] = rows[0]["properties"]["firstName"].strip()
            return rows
        self.assertEqual(self.run_scan(lookup_mode="native-batch")[0], "committed")
        self.assertEqual(self.run_scan(trim, lookup_mode="native-batch")[0], "partial")

    def test_lookup_modes_are_explicit_and_bounded(self):
        with self.assertRaisesRegex(ValueError, "Unsupported lookup"):
            self.run_scan(lookup_mode="../other.groovy")
        self.assertNotEqual(r.reconciliation_script("traversal"), r.reconciliation_script("native-batch"))

    def test_native_batch_entrypoint_records_script_and_returns_nonzero_on_wrong_content(self):
        self.context["job"]["config"]["target"].update(host="http://127.0.0.1", port=8080)
        self.context["job"]["request"]["outputBudgetBytes"] = "32768"
        self.context["job"]["request"]["shard"]["id"] = "unit-shard"
        lease = self.root / "lease.json"
        lease.write_bytes(r.encoded(self.lease))
        sent = []
        owner = self
        class Transport:
            def post(self, body):
                sent.append(body)
                b = body["bindings"]
                observed = r.expected(owner.row, "vertices", "person")[1]
                if wrong:
                    observed["properties"]["ldbcId"] += 1
                return [{"graphName": b["graphName"], "epochMarker": b["epochMarker"],
                         "rows": [[observed]]}], "c"*64
        for wrong in [False, True]:
            out = self.root / ("native-wrong" if wrong else "native-good")
            argv = ["ldbc-reconcile.py", "--job", str(self.root / "job.json"), "--quiescence", str(lease),
                    "--alias", "g", "--endpoint", "http://127.0.0.1:8080/gremlin",
                    "--lookup-mode", "native-batch", "--out", str(out)]
            with patch("sys.argv", argv), patch.object(r.subprocess, "run", return_value=SimpleNamespace(
                    stdout=r.encoded(self.context))), patch.object(r, "Transport", return_value=Transport()), \
                    patch("sys.stdout", new_callable=io.StringIO):
                self.assertEqual(4 if wrong else 0, r.main())
            report = r.decode((out / "reconciliation.json").read_bytes())
            self.assertEqual("native-batch", report["lookupMode"])
            self.assertEqual(r.digest(r.reconciliation_script("native-batch").read_bytes()), report["scriptSha256"])
            self.assertEqual("partial" if wrong else "committed", report["disposition"])
        self.assertEqual(2, len(sent))

    def test_all_properties_and_exact_long(self):
        self.assertEqual(self.run_scan()[0], "committed")
        def corrupt(rows):
            rows[0]["properties"]["ldbcId"] = float(9007199254740993)
            return rows
        self.assertEqual(self.run_scan(corrupt)[0], "partial")
        def extra(rows):
            rows[0]["properties"]["unexpected"] = "x"
            return rows
        self.assertEqual(self.run_scan(extra)[0], "partial")

    def compressed_input(self, data=None):
        raw = gzip.compress(self.raw if data is None else data, mtime=0)
        path = self.root / "shard.json.gz"
        path.write_bytes(raw)
        self.context["job"]["request"]["inputs"][0] = {
            "path": path.name, "sha256": r.digest(raw), "bytes": str(len(raw))}
        self.context["job"]["mapping"]["vertices"][0]["input"] = {"compression": "GZIP"}
        self.context["job"]["encoding"] = {"compression": "gzip", "uncompressedBytes": str(len(self.raw)),
                                            "uncompressedSha256": r.digest(self.raw)}
        return path, raw

    def test_gzip_exact_content_and_two_identities(self):
        self.compressed_input()
        self.assertEqual(self.run_scan()[0], "committed")
        self.context["job"]["encoding"]["uncompressedSha256"] = "d" * 64
        with self.assertRaisesRegex(ValueError, "changed while scanning"):
            self.run_scan()
        self.context["job"]["encoding"]["uncompressedSha256"] = r.digest(self.raw)
        self.context["job"]["request"]["inputs"][0]["sha256"] = "e" * 64
        with self.assertRaisesRegex(ValueError, "Stored input changed"):
            self.run_scan()

    def test_gzip_truncation_crc_and_extra_member_fail_closed(self):
        for mode in ("truncated", "crc", "concatenated"):
            path, raw = self.compressed_input()
            if mode == "truncated":
                raw = raw[:-4]
            elif mode == "crc":
                raw = bytearray(raw)
                raw[-8] ^= 1
                raw = bytes(raw)
            else:
                raw += gzip.compress(self.raw, mtime=0)
            path.write_bytes(raw)
            self.context["job"]["request"]["inputs"][0].update(sha256=r.digest(raw), bytes=str(len(raw)))
            with self.subTest(mode=mode), self.assertRaises((ValueError, OSError, EOFError)):
                self.run_scan()

    def test_gzip_expansion_is_bounded_before_http(self):
        self.compressed_input(b"x" * 1024**2 + b"\n")
        class NoRequests:
            def post(self, body):
                raise AssertionError("No HTTP before an over-budget record is rejected")
        with self.assertRaisesRegex(ValueError, "uncompressed byte budget"):
            r.scan(self.context, self.lease, NoRequests(), "g", 256, io.BytesIO(), 10000)
        self.context["job"].pop("encoding")
        with self.assertRaisesRegex(ValueError, "Compression"):
            self.run_scan()

    def test_absent_and_duplicate(self):
        self.assertEqual(self.run_scan(lambda _: [])[0], "absent")
        self.assertEqual(self.run_scan(lambda rows: rows * 2)[0], "partial")

    def test_set_order_and_duplicate(self):
        def reorder(rows):
            rows[0]["properties"]["emails"].reverse()
            return rows
        self.assertEqual(self.run_scan(reorder)[0], "committed")
        with self.assertRaises(ValueError):
            r.properties({"emails": ["a", "a"]})

    def test_epoch_lease_count_budget_fail_closed(self):
        with self.assertRaises(ValueError):
            self.run_scan(mode="wrong-epoch")
        with self.assertRaises(TimeoutError):
            self.run_scan(mode="timeout")
        with self.assertRaises(ValueError):
            self.run_scan(budget=1)
        self.context["job"]["request"]["shard"]["records"] = "2"
        with self.assertRaises(ValueError):
            self.run_scan()
        self.lease["writersStopped"] = False
        with self.assertRaises(ValueError):
            self.run_scan()

    def test_expired_lease(self):
        self.lease["validUntil"] = "2000-01-01T00:00:00Z"
        with self.assertRaises(ValueError):
            self.run_scan()

    def test_transport_bounds_full_response_and_does_not_redirect(self):
        mode = ["redirect"]
        received = []
        release = threading.Event()
        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass
            def do_POST(self):
                received.append(mode[0])
                self.rfile.read(int(self.headers["Content-Length"]))
                try:
                    if mode[0] == "headers-stall":
                        release.wait(2)
                        return
                    self.send_response(307 if mode[0] == "redirect" else 200)
                    self.send_header("Location", "/gremlin")
                    self.end_headers()
                    if mode[0] == "body-stall":
                        self.wfile.write(b"{")
                        self.wfile.flush()
                        release.wait(2)
                    else:
                        self.wfile.write(b"x" * 1024)
                except OSError:
                    pass
        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        server.daemon_threads = True
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            for case in ("redirect", "oversize", "headers-stall", "body-stall"):
                mode[0] = case
                client = r.Transport("http://127.0.0.1:%d/gremlin" % server.server_port, 0.15, 128)
                before = len(received)
                start = time.monotonic()
                with self.assertRaises(Exception):
                    client.post({"test": True})
                self.assertLess(time.monotonic() - start, 1.5)
                self.assertEqual(len(received), before + 1)
        finally:
            release.set()
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def test_transport_gzip_preserves_wire_hash_and_bounds_decoded_content(self):
        raw = b'{"status":{"code":200},"result":{"data":[{"id":9007199254740993}]}}'
        response = [gzip.compress(raw, mtime=0)]
        encoding = ["gzip"]
        requests = []
        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass
            def do_POST(self):
                self.rfile.read(int(self.headers["Content-Length"]))
                requests.append(self.headers.get("Accept-Encoding"))
                self.send_response(200)
                self.send_header("Content-Encoding", encoding[0])
                self.send_header("Content-Length", str(len(response[0])))
                self.end_headers()
                self.wfile.write(response[0])
        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            client = r.Transport("http://127.0.0.1:%d/gremlin" % server.server_port, 1, 128)
            data, wire = client.post({"test": True})
            self.assertEqual(9007199254740993, data[0]["id"])
            self.assertEqual(r.digest(response[0]), wire)
            self.assertEqual(["gzip"], requests)
            response[0] = gzip.compress(b'x' * 4096, mtime=0)
            with self.assertRaisesRegex(ValueError, 'Decoded response'):
                client.post({"test": True})
            response[0] = gzip.compress(raw, mtime=0)[:-4]
            with self.assertRaises(EOFError):
                client.post({"test": True})
            response[0] = raw
            encoding[0] = "br"
            with self.assertRaisesRegex(ValueError, 'Unsupported response encoding'):
                client.post({"test": True})
            self.assertEqual(4, len(requests))
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def test_http_failure_keeps_safe_codes_and_never_raw_messages(self):
        secret = "SECRET-only-in-provider-message"
        raw = r.encoded({"exception": "java.util.concurrent.TimeoutException", "message": secret,
                         "status": {"code": 598, "message": secret}, "stacktrace": [secret]})
        payload = [raw]
        encoding = ["identity"]
        status = [500]
        received = []
        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass
            def do_POST(self):
                received.append(1)
                self.rfile.read(int(self.headers["Content-Length"]))
                self.send_response(status[0])
                self.send_header("Content-Encoding", encoding[0])
                self.send_header("Content-Length", str(len(payload[0])))
                self.end_headers()
                try:
                    self.wfile.write(payload[0])
                except OSError:
                    pass
        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            client = r.Transport("http://127.0.0.1:%d/gremlin" % server.server_port, 1, 1024)
            for response_status, compressed in [(400, False), (400, True), (500, False), (500, True)]:
                status[0] = response_status
                payload[0] = gzip.compress(raw, mtime=0) if compressed else raw
                encoding[0] = "gzip" if compressed else "identity"
                with self.assertRaisesRegex(ValueError, '^HTTP status %d$' % response_status) as caught:
                    client.post({"request": secret})
                detail = caught.exception.http
                self.assertEqual(detail['status'], response_status)
                self.assertEqual(detail['providerCode'], 598)
                self.assertEqual(detail['providerException'], 'TimeoutException')
                self.assertEqual(detail['bodyState'], 'complete-json')
                self.assertEqual(detail['wirePrefixSha256'], r.digest(payload[0]))
                self.assertNotIn(secret, json.dumps(detail))
            self.assertEqual(len(received), 4)
            payload[0] = gzip.compress(b'x' * 4096, mtime=0)
            with self.assertRaises(ValueError) as caught:
                client.post({})
            self.assertEqual(caught.exception.http['bodyState'], 'decoded-limit')
            payload[0] = b'x' * 4096
            encoding[0] = 'identity'
            with self.assertRaises(ValueError) as caught:
                client.post({})
            self.assertEqual(caught.exception.http['bodyState'], 'wire-limit')
            self.assertEqual(caught.exception.http['wirePrefixBytes'], 1024)
            self.assertEqual(len(received), 6)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def test_slow_error_body_preserves_status_under_one_whole_deadline(self):
        released = threading.Event()
        received = []
        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass
            def do_POST(self):
                received.append(1)
                self.rfile.read(int(self.headers['Content-Length']))
                self.send_response(400)
                self.send_header('Content-Length', '10000')
                self.end_headers()
                try:
                    for _ in range(30):
                        self.wfile.write(b'x')
                        self.wfile.flush()
                        if released.wait(0.03):
                            break
                except OSError:
                    pass
        server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        server.daemon_threads = True
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            client = r.Transport('http://127.0.0.1:%d/gremlin' % server.server_port, 0.12, 1024)
            start = time.monotonic()
            with self.assertRaisesRegex(ValueError, '^HTTP status 400$') as caught:
                client.post({})
            self.assertLess(time.monotonic() - start, 0.8)
            self.assertEqual(caught.exception.http['bodyState'], 'incomplete')
            self.assertLess(caught.exception.http['wirePrefixBytes'], 10000)
            self.assertEqual(received, [1])
        finally:
            released.set()
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def test_entrypoint_http_failure_is_unknown_exit4_with_safe_evidence(self):
        received = []
        body = r.encoded({'status': {'code': 598, 'message': 'SECRET-script-and-credentials'},
                          'exception': 'java.util.concurrent.TimeoutException'})
        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass
            def do_POST(self):
                received.append(1)
                self.rfile.read(int(self.headers['Content-Length']))
                self.send_response(500)
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)
        server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            port = server.server_port
            self.context['job']['config']['target'].update(host='http://127.0.0.1', port=port)
            self.context['job']['request']['outputBudgetBytes'] = '32768'
            self.context['job']['request']['shard']['id'] = 'unit-shard'
            lease = self.root / 'http-lease.json'
            lease.write_bytes(r.encoded(self.lease))
            out = self.root / 'http-failure'
            argv = ['ldbc-reconcile.py', '--job', str(self.root / 'job.json'), '--quiescence', str(lease),
                    '--alias', 'g', '--endpoint', 'http://127.0.0.1:%d/gremlin' % port, '--out', str(out)]
            with patch('sys.argv', argv), patch.object(r.subprocess, 'run', return_value=SimpleNamespace(
                    stdout=r.encoded(self.context))), redirect_stdout(io.StringIO()):
                self.assertEqual(r.main(), 4)
            report = json.loads((out / 'reconciliation.json').read_text())
            self.assertEqual(report['disposition'], 'unknown')
            self.assertIsNone(report['counts'])
            self.assertEqual(report['error']['message'], 'HTTP status 500')
            self.assertEqual(report['error']['http']['providerCode'], 598)
            self.assertEqual((out / 'batches.jsonl').read_bytes(), b'')
            for path in out.iterdir():
                self.assertNotIn(b'SECRET', path.read_bytes())
            self.assertEqual(received, [1])
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def test_edge_full_identity_and_numeric_type(self):
        selector, item = r.expected({"sourceId": "person:1", "targetId": "person:2", "edgeKey": 9007199254740993,
                                     "creationDate": 3}, "edges", "person_knows_person")
        self.assertEqual(selector["edgeKey"], 9007199254740993)
        self.assertEqual(selector["targetId"], "person:2")
        self.assertEqual(item["targetId"], "person:2")
        self.assertNotEqual(r.exact({"v": 1}), r.exact({"v": True}))
        self.assertNotEqual(r.exact({"v": 1}), r.exact({"v": 1.0}))
        with self.assertRaises(ValueError):
            r.decode('{"x":1,"x":2}')

    def test_edge_scan_uses_full_producer_identity(self):
        source, target, key = "person:9007199254740993>甲", "person:2", 9007199254740993
        stored = {"sourceId": source, "targetId": target, "label": "person_knows_person",
                  "properties": {"edgeKey": key, "creationDate": 3}}
        for lookup in ("traversal", "native-batch"):
            class Store:
                def post(self, body):
                    bindings = body["bindings"]
                    selector = bindings["selectors"][0]
                    # Independent consumer contract: native EdgeId needs both endpoints and the sort key.
                    if lookup == "native-batch":
                        found = ((selector["sourceId"], selector["targetId"], selector["edgeKey"]) ==
                                 (source, target, key))
                    else:
                        found = (selector["sourceId"], selector["edgeKey"]) == (source, key)
                    return [{"graphName": bindings["graphName"], "epochMarker": bindings["epochMarker"],
                             "rows": [[copy.deepcopy(stored)] if found else []]}], "c" * 64
            for declared_target in (target, "person:missing"):
                self.row = {"sourceId": source, "targetId": declared_target, "edgeKey": key, "creationDate": 3}
                self.raw = r.encoded(self.row) + b"\n"
                (self.root / "shard.json").write_bytes(self.raw)
                job = self.context["job"]
                job["mapping"] = {"vertices": [], "edges": [{"label": "person_knows_person"}]}
                job["request"]["inputs"] = [{"path": "shard.json", "sha256": r.digest(self.raw),
                                              "bytes": str(len(self.raw))}]
                with self.subTest(lookup=lookup, target=declared_target):
                    disposition, counts = r.scan(self.context, self.lease, Store(), "g", 256,
                                                 io.BytesIO(), 10000, lookup)
                    if declared_target == target:
                        self.assertEqual(disposition, "committed")
                        self.assertEqual(counts["matched"], 1)
                    else:
                        self.assertNotEqual(disposition, "committed")


if __name__ == "__main__":
    unittest.main()
