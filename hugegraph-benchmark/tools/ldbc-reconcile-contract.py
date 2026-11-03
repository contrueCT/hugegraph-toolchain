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

"""Check Python selectors against the real HugeGraph memory/Groovy consumer, offline.

Reuses a local HugeGraph test classpath. Creates no server and contacts no remote system.
This is a functional component check, not HStore, SF100 or performance acceptance.
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
    parser.add_argument("--classpath-report", type=Path, required=True,
                        help="Existing Maven Surefire XML containing java.class.path")
    parser.add_argument("--java-home", type=Path, help="Optional Java 17/21 home; otherwise use PATH")
    parser.add_argument("--out", type=Path, required=True, help="NEW directory for inputs, logs and result")
    args = parser.parse_args()
    cp = next(p.attrib["value"] for p in ET.parse(args.classpath_report).findall(".//property")
              if p.attrib["name"] == "java.class.path")
    entries = list(dict.fromkeys(p for p in cp.split(os.pathsep) if p))
    if any(not Path(p).exists() for p in entries):
        raise ValueError("Classpath contains unavailable files")
    args.out.mkdir()
    output = args.out.resolve()
    spec = importlib.util.spec_from_file_location("reconcile", ROOT / "tools/ldbc-reconcile.py")
    reconcile = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(reconcile)
    a, b = "person:9007199254740993>甲", "person:2"
    keys = [0, -1, 9007199254740993, (1 << 63) - 1]
    selectors = [reconcile.expected({"sourceId": a, "targetId": b, "edgeKey": key,
                                    "stamp": "甲>" + str(key)}, "edges", "knows")[0] for key in keys]
    selectors += [selectors[0], reconcile.expected({"sourceId": a, "targetId": b, "edgeKey": 77},
                                                  "edges", "knows")[0]]
    selectors_file = output / "selectors.json"
    selectors_file.write_bytes(reconcile.encoded(selectors) + b"\n")
    source = ROOT / "tests-normative/java/BatchReconcileCheck.java"
    def binary(name):
        return str(args.java_home / "bin" / name) if args.java_home else name
    with tempfile.TemporaryDirectory(prefix="hgbench-batch-contract-") as temp:
        with (output / "compile.log").open("x") as log:
            build = subprocess.run([binary("javac"), "-J-Xmx256m", "--release", "17", "-encoding", "UTF-8",
                                    "-cp", cp, "-d", temp, str(source)],
                                   cwd=temp, stdout=log, stderr=subprocess.STDOUT, timeout=60)
        run = None
        if build.returncode == 0:
            with (output / "execution.log").open("x") as log:
                run = subprocess.run([binary("java"), "-Xmx512m", "-XX:ActiveProcessorCount=2",
                                      "-Djava.security.manager=allow", "-cp", temp + os.pathsep + cp,
                                      "BatchReconcileCheck", str(reconcile.reconciliation_script("traversal")),
                                      str(reconcile.reconciliation_script("native-batch")), str(selectors_file)],
                                     cwd=temp, stdout=log, stderr=subprocess.STDOUT, timeout=60)
    text = (output / "execution.log").read_text() if run is not None else ""
    match = re.search(r"BATCH_RECONCILE_CHECK_PASSED=(\d+)", text)
    passed = int(match[1]) if match else 0
    code = build.returncode or (run.returncode if run is not None else 2)
    if code == 0 and passed != 11:
        code = 2
    paths = [source, ROOT / "tools/ldbc-reconcile.py", Path(__file__),
             reconcile.reconciliation_script("traversal"), reconcile.reconciliation_script("native-batch")]
    result = {"scope": "python-selector-to-native-hugegraph-memory-contract", "ok": code == 0,
              "passed": passed, "exitCode": code, "buildExitCode": build.returncode,
              "runtimeExitCode": run.returncode if run is not None else None,
              "classpathReportSha256": hashlib.sha256(args.classpath_report.read_bytes()).hexdigest(),
              "runtimeIdentityScope": "Existing classpath report and observed class source, not a full runtime lock",
              "hugegraphClassSource": next((s for s in text.splitlines()
                                           if s.startswith("HUGEGRAPH_CLASS_SOURCE=")), None),
              "selectorsSha256": hashlib.sha256(selectors_file.read_bytes()).hexdigest(),
              "sources": [{"path": str(p.relative_to(ROOT)), "sha256": hashlib.sha256(p.read_bytes()).hexdigest()}
                          for p in paths],
              "hstoreExecuted": False, "sf100Executed": False, "performanceAccepted": False}
    (output / "result.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
