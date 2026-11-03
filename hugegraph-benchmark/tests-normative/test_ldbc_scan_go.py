# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Native Go CLI, real frozen-job validator, and independent Python evidence reader.

Only fixed HTTP responses; no graph service or SF100 performance claim.
Set HGBENCH_SCAN_GO_BINARY explicitly. Optional HGBENCH_SCAN_GO_EVIDENCE keeps all artifacts.
"""
import copy
import datetime as dt
import gzip
import hashlib
import http.server
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import threading
import time
import unittest

ROOT = Path(os.environ.get("HGBENCH_PACKAGE_ROOT", Path(__file__).resolve().parents[1]))
BINARY = os.environ.get("HGBENCH_SCAN_GO_BINARY")
spec = importlib.util.spec_from_file_location("scan_go_reference", ROOT / "tools/ldbc-scan.py")
s = importlib.util.module_from_spec(spec)
spec.loader.exec_module(s)

JOB_SCRIPT = r'''
import {readFile,writeFile} from 'node:fs/promises';
import {pathToFileURL} from 'node:url';
const [root,base,port]=process.argv.slice(1);
const {hash}=await import(pathToFileURL(root+'/dist/io.js'));
const {planLoaderJobs}=await import(pathToFileURL(root+'/dist/sf100-loader.js'));
const prepared=JSON.parse(await readFile(base+'/prepare-manifest.json','utf8'));
const config={schemaVersion:1,planDigest:'1'.repeat(64),datasetDigest:'2'.repeat(64),
 mappingDigest:hash({schema:prepared.files['schema.groovy'],converter:prepared.sourceConverterSha256}),
 componentManifest:'component.json',componentSha256:prepared.files['component.json'].sha256,
 target:{host:'http://127.0.0.1',port:Number(port),graph:'unit',epoch:'unit-epoch',topologyDigest:'3'.repeat(64)},
 schemaAlreadyApplied:true,batchSize:500,batchThreads:1,parserThreads:2,heapMiB:256,
 timeoutSeconds:5,processTimeoutMs:5000,maxLogBytes:4096};
await writeFile(base+'/config.json',JSON.stringify(config));
const result=await planLoaderJobs(base+'/prepare-manifest.json',base+'/config.json');
await writeFile(base+'/job.json',JSON.stringify(result.jobs[0]));
process.stdout.write(JSON.stringify({job:result.jobs[0],requestDigest:hash(result.jobs[0].request)}));
'''


@unittest.skipUnless(BINARY, "Set HGBENCH_SCAN_GO_BINARY to a pinned native Go verifier")
class NativeScanGoTests(unittest.TestCase):
    def setUp(self):
        if os.environ.get("HGBENCH_SCAN_GO_EVIDENCE"):
            self.base = Path(os.environ["HGBENCH_SCAN_GO_EVIDENCE"]) / self._testMethodName
            self.base.mkdir(parents=True, exist_ok=False)
        else:
            temp = tempfile.TemporaryDirectory(prefix="hgbench-scan-go-")
            self.addCleanup(temp.cleanup)
            self.base = Path(temp.name)
        self.binary = str(Path(BINARY).resolve())
        self.node = os.environ.get("HGBENCH_SCAN_GO_NODE", shutil.which("node"))
        self.vertex = {"id": "person:9007199254740993>甲", "label": "person",
                       "properties": {"ldbcId": 9007199254740993, "firstName": " \t甲\n ", "emails": ["b", "a"]}}
        self.edge = {"sourceId": self.vertex["id"], "targetId": "person:2", "label": "knows",
                     "properties": {"edgeKey": (1 << 63)-1, "stamp": "甲>"}}
        self.expected = {"schemaVersion": 1, "scope": "public-ldbc-initial-graph", "schema": {
            "propertyKeys": {}, "vertexLabels": {"person": {}}, "edgeLabels": {"knows": {}}, "indexLabels": {}},
            "counts": {"vertices": {"person": "1"}, "edges": {"knows": "1"}}}
        self.marker = "hgb_epoch_" + s.r.digest(b"unit-epoch")[:32]
        self.calls, self.encoding, self.mutate = [], "identity", None
        self.entered, self.release = threading.Event(), threading.Event()
        self.hold = False
        owner = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                body = s.r.decode(self.rfile.read(int(self.headers["Content-Length"])))
                owner.calls.append(body)
                binding = body["bindings"]
                if owner.hold:
                    owner.entered.set()
                    owner.release.wait(10)
                row = {"graphName": "unit", "epochMarker": owner.marker, "kind": "schema",
                       "schema": copy.deepcopy(owner.expected["schema"])}
                if binding["category"] != "schema":
                    category = binding["category"]
                    row = {"graphName": "unit", "epochMarker": owner.marker, "kind": "scan-page",
                           "category": category, "page": binding["page"], "nextPage": None,
                           "scanned": "1", "hiddenRecords": "0",
                           "rows": [copy.deepcopy(owner.vertex if category == "vertices" else owner.edge)]}
                if owner.mutate:
                    owner.mutate(row)
                raw = s.r.encoded({"status": {"code": 200}, "result": {"data": [row]}})
                if owner.encoding == "gzip":
                    raw = gzip.compress(raw, mtime=0)
                self.send_response(200)
                self.send_header("Content-Encoding", owner.encoding)
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                try:
                    self.wfile.write(raw)
                except (BrokenPipeError, ConnectionResetError):
                    pass

            def log_message(self, *_):
                pass

        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        self.addCleanup(self.release.set)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.endpoint = "http://127.0.0.1:" + str(server.server_port) + "/gremlin"
        files, shards, struct = {}, [], {"vertices": [], "edges": []}

        def put(name, value):
            raw = value if isinstance(value, bytes) else s.r.encoded(value)
            (self.base / name).write_bytes(raw)
            fact = {"sha256": s.r.digest(raw), "bytes": len(raw)}
            files[name] = fact
            return fact

        for cat, label, row in [("vertices", "person", {"vertexId": self.vertex["id"], **self.vertex["properties"]}),
                                ("edges", "knows", {"sourceId": self.edge["sourceId"],
                                                    "targetId": self.edge["targetId"], **self.edge["properties"]})]:
            raw = s.r.encoded(row) + b"\n"
            name = cat + ".jsonl.gz"
            fact = put(name, gzip.compress(raw, mtime=0))
            shards.append({"id": cat + "-0", "category": cat, "label": label, "path": name,
                           "sha256": fact["sha256"], "bytes": str(fact["bytes"]), "records": "1",
                           "encoding": {"compression": "gzip", "uncompressedBytes": str(len(raw)),
                                        "uncompressedSha256": s.r.digest(raw)}})
            mapping = {"label": label, "input": {"type": "file", "path": name, "format": "JSON", "compression": "GZIP"}}
            mapping.update({"id": "vertexId"} if cat == "vertices" else {"source": ["sourceId"], "target": ["targetId"]})
            struct[cat].append(mapping)
        put("struct.json", struct)
        put("schema.groovy", b"// protocol fixture; never executed\n")
        launcher = put("loader.sh", b"#!/bin/sh\nexit 99\n")
        (self.base / "loader.sh").chmod(0o700)
        put("component.json", {"protocolVersion": 1, "kind": "sf100-component", "role": "loader",
            "id": "fixed-response-never-a-database", "version": "test", "command": ["./loader.sh"],
            "trackedFiles": [{"path": "loader.sh", "sha256": launcher["sha256"]}],
            "capabilities": {"scale": "100", "serializer": "CsvBasic", "operations": [], "checkpoint": "reconcile-before-retry"}})
        put("inventory.expected.json", self.expected)
        self.prepared = {"schemaVersion": 2, "scope": "sharded-loader-input", "sourceConverterSha256": "b"*64,
            "sourceAsset": {"scale": "100", "role": "initial", "sha256": "a"*64}, "files": files,
            "shards": shards, "vertices": 1, "edges": 1, "counts": {"person": 1, "knows": 1}}
        prepared_raw = s.r.encoded(self.prepared)
        (self.base / "prepare-manifest.json").write_bytes(prepared_raw)
        result = subprocess.run([self.node, "--input-type=module", "-e", JOB_SCRIPT,
                                 str(ROOT), str(self.base), str(server.server_port)],
                                capture_output=True, text=True, timeout=20)
        (self.base / "job-create.stdout.json").write_text(result.stdout)
        (self.base / "job-create.stderr.log").write_text(result.stderr)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.context = json.loads(result.stdout)
        lease = {"schemaVersion": 1, "kind": "sf100-writer-quiescence", "requestDigest": self.context["requestDigest"],
            "target": self.context["job"]["request"]["target"], "graphName": "unit", "epochMarker": self.marker,
            "validUntil": (dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=5)).isoformat(),
            "writersStopped": True, "inflightWritesDrained": True, "exclusiveAccess": True,
            "attestor": "native-cli-protocol-fixture", "basis": "Fixed HTTP responses; no graph service or Loader execution"}
        (self.base / "lease.json").write_bytes(s.r.encoded(lease))
        self.deadline = time.time() + 60
        self.args = [self.binary, "--root", str(ROOT), "--node", self.node, "--job", str(self.base / "job.json"),
            "--prepared", str(self.base / "prepare-manifest.json"), "--prepared-sha256", s.r.digest(prepared_raw),
            "--quiescence", str(self.base / "lease.json"), "--endpoint", self.endpoint, "--alias", "g",
            "--scope", "vertices", "--deadline-unix", str(self.deadline), "--max-pages", "20"]

    def execute(self, name, extra=()):
        out = self.base / name
        result = subprocess.run([*self.args, *extra, "--out", str(out)], capture_output=True, text=True, timeout=30)
        (self.base / (name + ".stdout.json")).write_text(result.stdout)
        (self.base / (name + ".stderr.log")).write_text(result.stderr)
        return result, out

    def consume(self, out, categories):
        report = json.loads((out / "scan.json").read_text())
        self.assertEqual(report["outcome"], "matched")
        self.assertFalse(report["performanceAccepted"])
        self.assertEqual(report["componentSha256"], hashlib.sha256(Path(self.binary).read_bytes()).hexdigest())
        challenge = bytes.fromhex(json.loads((out / "challenge.json").read_text())["challengeHex"])
        fp = s.Fingerprint({"vertices": ["person"], "edges": ["knows"]}, challenge=challenge)
        s.source_fingerprint(self.prepared, self.base, categories, fp, lambda: None, lambda _: None)
        for name in ["expected.json", "actual.json"]:
            self.assertEqual((out / name).read_bytes(), s.r.encoded(fp.snapshot()) + b"\n")
        for fact in report["evidence"]:
            self.assertEqual(s.artifact(out / fact["path"]), fact)
        return report

    def test_identity_default_scopes_and_python_prior_reader(self):
        result, out = self.execute("vertices")
        self.assertEqual(result.returncode, 0, result.stderr)
        vertex = self.consume(out, ("vertices",))
        self.assertFalse(vertex["datasetVerified"])
        self.assertEqual(s.prior_usage(out / "scan.json", s.r.digest((out / "scan.json").read_bytes()),
            vertex["binding"], vertex["componentSha256"], vertex["budgets"])[:2], (3, vertex["ledgerBytes"]))
        self.encoding = "gzip"
        result, full_out = self.execute("full", ["--scope", "full", "--prior-scan", str(out / "scan.json"),
                                                  "--prior-scan-sha256", s.r.digest((out / "scan.json").read_bytes())])
        self.assertEqual(result.returncode, 0, result.stderr)
        full = self.consume(full_out, ("vertices", "edges"))
        self.assertTrue(full["datasetVerified"])
        self.assertEqual(full["wholeTaskPages"], 7)
        self.assertNotEqual(vertex["challengeCommitment"], full["challengeCommitment"])

    def test_gzip_source_and_response_full_alternative(self):
        self.encoding = "gzip"
        result, out = self.execute("full", ["--scope", "full", "--vertex-admission", "acknowledged-writes"])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(self.consume(out, ("vertices", "edges"))["datasetVerified"])

    def test_wrong_epoch_refuses_without_retry(self):
        self.mutate = lambda row: row.update(epochMarker="wrong")
        result, out = self.execute("wrong")
        self.assertEqual(result.returncode, 4)
        report = json.loads((out / "scan.json").read_text())
        self.assertEqual(report["outcome"], "unknown")
        self.assertFalse(report["datasetVerified"])
        self.assertEqual(len(self.calls), 1)

    def test_sigterm_keeps_partial_receipts(self):
        self.hold = True
        out = self.base / "cancelled"
        with (self.base / "cancelled.stdout.json").open("x") as stdout, \
                (self.base / "cancelled.stderr.log").open("x") as stderr:
            process = subprocess.Popen([*self.args, "--out", str(out)], stdout=stdout, stderr=stderr)
            try:
                self.assertTrue(self.entered.wait(10), "CLI did not reach the fixed HTTP service")
                process.terminate()
                self.assertEqual(process.wait(timeout=5), 4)
            finally:
                self.release.set()
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=5)
        report = json.loads((out / "scan.json").read_text())
        self.assertEqual(report["outcome"], "unknown")
        self.assertFalse(report["datasetVerified"])
        rows = [json.loads(v) for v in (out / "pages.jsonl").read_text().splitlines()]
        self.assertEqual(len(rows), 1)
        self.assertIsNotNone(rows[0]["errorType"])
        self.assertEqual(len(self.calls), 1)

    def test_real_job_validator_refuses_changed_request_and_source(self):
        job_path = self.base / "job.json"
        original = job_path.read_bytes()
        job = json.loads(original)
        job["config"]["target"]["graph"] = "changed-without-a-new-frozen-job"
        job_path.write_bytes(s.r.encoded(job))
        result, out = self.execute("bad-job")
        self.assertEqual(result.returncode, 4)
        self.assertFalse(out.exists())
        self.assertEqual(len(self.calls), 0)
        job_path.write_bytes(original)
        path = self.base / self.prepared["shards"][0]["path"]
        path.write_bytes(path.read_bytes() + b"changed")
        result, out = self.execute("bad-source")
        self.assertEqual(result.returncode, 4)
        self.assertFalse(out.exists())
        self.assertEqual(len(self.calls), 0)

    def test_original_deadline_bounds_the_whole_response(self):
        self.hold = True
        started = time.monotonic()
        try:
            result, out = self.execute("deadline", ["--deadline-unix", str(time.time() + 3),
                                                    "--timeout-seconds", "10"])
        finally:
            self.release.set()
        self.assertEqual(result.returncode, 4)
        self.assertTrue(self.entered.is_set(), "Deadline expired before the HTTP phase")
        self.assertLess(time.monotonic() - started, 5)
        report = json.loads((out / "scan.json").read_text())
        self.assertEqual(report["outcome"], "unknown")
        self.assertFalse(report["datasetVerified"])
        self.assertEqual(len(self.calls), 1)


if __name__ == "__main__":
    unittest.main()
