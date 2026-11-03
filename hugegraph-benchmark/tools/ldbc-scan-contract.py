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

"""Check production scan scripts on an isolated local RocksDB fixture and consume pages in Python.

No server, network, HStore or SF100 experiment. Requires a local HugeGraph runtime classpath.
"""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--classpath-report", type=Path, required=True, help="Surefire-format java.class.path XML")
    parser.add_argument("--java-home", type=Path, help="Local Java 17/21; otherwise PATH")
    parser.add_argument("--out", type=Path, required=True, help="NEW evidence directory")
    args = parser.parse_args()
    cp = next(p.attrib["value"] for p in ET.parse(args.classpath_report).findall(".//property")
              if p.attrib["name"] == "java.class.path")
    entries = list(dict.fromkeys(p for p in cp.split(os.pathsep) if p))
    if any(not Path(p).exists() for p in entries):
        raise ValueError("Classpath contains unavailable files")
    args.out.mkdir()
    output = args.out.resolve()
    source = ROOT / "tests-normative/java/ScanPageCheck.java"
    scripts = [ROOT / "adapters/ldbc-gremlin" / name for name in ("scan-page.groovy", "inventory-page.groovy")]
    def binary(name):
        return str(args.java_home / "bin" / name) if args.java_home else name
    run = None
    with tempfile.TemporaryDirectory(prefix="hgbench-scan-contract-") as temp:
        with (output / "compile.log").open("x") as log:
            build = subprocess.run([binary("javac"), "-J-Xmx256m", "--release", "17", "-encoding", "UTF-8",
                                    "-cp", cp, "-d", temp, str(source)],
                                   cwd=temp, stdout=log, stderr=subprocess.STDOUT, timeout=60)
        if build.returncode == 0:
            with (output / "execution.log").open("x") as log:
                run = subprocess.run([binary("java"), "-Xmx512m", "-XX:ActiveProcessorCount=2",
                                      "-Djava.security.manager=allow", "-cp", temp + os.pathsep + cp,
                                      "ScanPageCheck", *map(str, scripts), str(Path(temp) / "data"),
                                      str(output / "pages.jsonl")],
                                     cwd=temp, stdout=log, stderr=subprocess.STDOUT, timeout=90)
    text = (output / "execution.log").read_text() if run is not None else ""
    match = re.search(r"SCAN_PAGE_CHECK_PASSED=(\d+)", text)
    passed = int(match[1]) if match else 0
    code = build.returncode or (run.returncode if run is not None else 2)
    consumer = False
    if code == 0 and passed == 18:
        spec = importlib.util.spec_from_file_location("scan", ROOT / "tools/ldbc-scan.py")
        scan = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(scan)
        labels = {"vertices": ["person"], "edges": ["knows"]}
        actual, expected = [scan.Fingerprint(labels, challenge=bytes(range(32))) for _ in range(2)]
        a, b = "person:9007199254740993>甲", "person:2"
        expected.add("vertices", {"id": a, "label": "person", "properties": {
            "ldbcId": 9007199254740993, "firstName": " \t甲\n ", "emails": ["a", "b"]}})
        expected.add("vertices", {"id": b, "label": "person", "properties": {
            "ldbcId": 2, "firstName": "B", "emails": ["c"]}})
        for key in [0, -1, 9007199254740993, (1 << 63)-1]:
            expected.add("edges", {"sourceId": a, "targetId": b, "label": "knows",
                                   "properties": {"edgeKey": key, "stamp": "甲>" + str(key)}})
        expected.add("edges", {"sourceId": b, "targetId": a, "label": "knows",
                               "properties": {"edgeKey": 7, "stamp": "reverse"}})
        pages = [scan.r.decode(line) for line in (output / "pages.jsonl").read_bytes().splitlines()]
        for category in scan.CATEGORIES:
            fold = scan.PageFold(category, "scan_page_check", "epoch_marker", actual, page_size=1, max_pages=12)
            for page in pages:
                if page["category"] == category:
                    fold.accept(fold.next_page, page)
            if not fold.complete():
                raise ValueError("Actual backend pages did not terminate")
        consumer = expected.snapshot() == actual.snapshot()
        (output / "expected.json").write_bytes(scan.r.encoded(expected.snapshot()) + b"\n")
        (output / "actual.json").write_bytes(scan.r.encoded(actual.snapshot()) + b"\n")
        code = 0 if consumer else 2
    elif code == 0:
        code = 2
    paths = [source, Path(__file__), ROOT / "tools/ldbc-scan.py", ROOT / "tools/ldbc-reconcile.py", *scripts]
    result = {"scope": "actual-local-rocksdb-pages-to-python-consumer", "ok": code == 0,
              "javaChecks": passed, "pythonConsumerMatched": consumer, "exitCode": code,
              "buildExitCode": build.returncode, "runtimeExitCode": run.returncode if run else None,
              "classpathReportSha256": hashlib.sha256(args.classpath_report.read_bytes()).hexdigest(),
              "hugegraphClassSource": next((line for line in text.splitlines()
                                           if line.startswith("HUGEGRAPH_CLASS_SOURCE=")), None),
              "sources": [{"path": str(p.relative_to(ROOT)), "sha256": hashlib.sha256(p.read_bytes()).hexdigest()}
                          for p in paths],
              "hstoreExecuted": False, "sf100Executed": False, "performanceAccepted": False}
    (output / "result.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result))
    return code


if __name__ == "__main__":
    raise SystemExit(main())

