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

"""Compile and round-trip pinned official operation/result classes, offline, without a database."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]
SUITE = ROOT / "benchmarks/ldbc-snb-interactive-v1"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--maven-repo", type=Path, default=Path.home() / ".m2/repository")
    args = parser.parse_args()
    paths = [f"com/fasterxml/jackson/core/{name}/2.13.2/{name}-2.13.2.jar"
             for name in ("jackson-databind", "jackson-core", "jackson-annotations")]
    paths += ["com/google/guava/guava/18.0/guava-18.0.jar"]
    jars = [args.maven_repo / path for path in paths]
    missing = [str(path) for path in jars if not path.is_file()]
    if missing:
        print(json.dumps({"ok": False, "error": "MISSING_CACHED_DEPENDENCIES", "files": missing}))
        return 3
    upstream = SUITE / "reference/driver/src/main/java/org/ldbcouncil/snb/driver"
    tasks = json.loads((SUITE / "tasks.json").read_text())["tasks"]
    sources = [SUITE / t["operationSource"] for t in tasks]
    sources += [SUITE / t["resultSource"] for t in tasks if t["resultSource"]]
    sources += [upstream / name for name in ["Operation.java", "util/ListUtils.java", "temporal/TemporalUtil.java",
                "validation/ValidationEquality.java", "workloads/interactive/LdbcNoResult.java",
                "workloads/interactive/LdbcNoResultJsonSerializer.java"]]
    source_lock = json.loads((SUITE / "reference/sources.lock.json").read_text())
    indexed = {str(SUITE / "reference" / e["path"]): e["sha256"] for e in source_lock["files"]}
    for source in sources:
        if hashlib.sha256(source.read_bytes()).hexdigest() != indexed[str(source)]:
            raise ValueError("Pinned source drift: " + str(source))
    check = ROOT / "adapters/ldbc-contract/DriverContractCheck.java"
    cp = os.pathsep.join(str(p) for p in jars)
    with tempfile.TemporaryDirectory(prefix="hgbench-java-contract-") as temp:
        # Isolate DTO serialization from scheduler dependencies. Only the no-result serializer's
        # compile-time constant is provided; all operation/result/Operation sources stay byte-identical.
        configuration = upstream / "workloads/interactive/LdbcSnbInteractiveWorkloadConfiguration.java"
        if hashlib.sha256(configuration.read_bytes()).hexdigest() != indexed[str(configuration)]:
            raise ValueError("Pinned configuration source drift")
        constant = re.search(r"WRITE_OPERATION_NO_RESULT_DEFAULT_RESULT = (-?\d+);", configuration.read_text())[1]
        shim = Path(temp) / "LdbcSnbInteractiveWorkloadConfiguration.java"
        shim.write_text("package org.ldbcouncil.snb.driver.workloads.interactive;\n"
                        "public final class LdbcSnbInteractiveWorkloadConfiguration {\n"
                        "public static final int WRITE_OPERATION_NO_RESULT_DEFAULT_RESULT = " + constant + ";\n}\n")
        build = subprocess.run(["javac", "-J-Xmx256m", "--release", "17", "-encoding", "UTF-8", "-cp", cp,
                                "-d", temp, *map(str, sources), str(shim), str(check)],
                               capture_output=True, text=True, timeout=60)
        if build.returncode:
            print(json.dumps({"ok": False, "error": "COMPILE_FAILED", "diagnostic": build.stderr[-4096:]}))
            return 2
        result = subprocess.run(["java", "-Xmx128m", "-cp", temp + os.pathsep + cp, "DriverContractCheck",
                                 str(SUITE / "tasks.json")], capture_output=True, text=True, timeout=30)
        report = json.loads(result.stdout)
        report["dependencies"] = [{"artifact": name, "sha256": hashlib.sha256(p.read_bytes()).hexdigest()}
                                  for name, p in zip(paths, jars)]
        report["checkSha256"] = hashlib.sha256(check.read_bytes()).hexdigest()
        report["fullDriverBuilt"] = False
        report["isolatedCompileTimeConstant"] = {"WRITE_OPERATION_NO_RESULT_DEFAULT_RESULT": int(constant)}
        print(json.dumps(report, ensure_ascii=False))
        return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
