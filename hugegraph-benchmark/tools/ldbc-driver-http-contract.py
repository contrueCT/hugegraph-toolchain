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

"""Exercise the complete official validator and adapter against loopback HTTP/Groovy unit fixtures."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("short_runtime", ROOT / "tools/ldbc-short-read-contract.py")
runtime = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runtime)


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build", type=Path, required=True, help="Adapter build with locked driver and adapter jars")
    parser.add_argument("--vectors", type=Path, required=True, help="Unit vectors exported by ldbc-full-contract.py")
    parser.add_argument("--maven-repo", type=Path, default=Path.home() / ".m2/repository")
    args = parser.parse_args()
    build = args.build.resolve()
    manifest = json.loads((build / "build-manifest.json").read_text())
    jars = []
    for entry in manifest["files"]:
        path = build / entry["path"]
        if sha(path) != entry["sha256"]:
            raise ValueError("Build artifact changed")
        jars.append(path)
    for entry in manifest["sources"]:
        if sha(ROOT / entry["path"]) != entry["sha256"]:
            raise ValueError("Source changed after build: " + entry["path"])
    jars += [args.maven_repo / g.replace('.', '/') / a / v / f"{a}-{v}.jar" for g, a, v in runtime.COORDINATES]
    missing = [str(p) for p in jars if not p.is_file()]
    if missing:
        print(json.dumps({"ok": False, "error": "MISSING_CACHED_DEPENDENCIES", "files": missing}))
        return 3
    cp = os.pathsep.join(map(str, jars))
    source = ROOT / "adapters/ldbc-contract/DriverHttpCheck.java"
    retry_source = ROOT / "adapters/ldbc-contract/TransportRetryCheck.java"
    with tempfile.TemporaryDirectory(prefix="hgbench-driver-http-") as temp:
        compiled = subprocess.run(["javac", "-J-Xmx256m", "--release", "17", "-encoding", "UTF-8",
                                   "-cp", cp, "-d", temp, str(source), str(retry_source)],
                                  capture_output=True, text=True, timeout=30)
        if compiled.returncode:
            print(json.dumps({"ok": False, "error": "COMPILE_FAILED", "diagnostic": compiled.stderr[-12000:]}))
            return 2
        result = subprocess.run(["java", "-Xmx384m", "-Djdk.httpclient.disableRetryConnect=true",
                    "-Djdk.httpclient.enableAllMethodRetry=false",
                    "-cp", temp + os.pathsep + cp, "org.apache.hugegraph.benchmark.ldbc.DriverHttpCheck",
                    str(args.vectors.resolve()), str(ROOT / "benchmarks/ldbc-snb-interactive-v1/tasks.json"),
                    str(ROOT / "adapters/ldbc-gremlin/snb-read.groovy"),
                    str(ROOT / "adapters/ldbc-gremlin/snb-update.groovy")],
                    capture_output=True, text=True, timeout=60)
        if result.returncode:
            print(json.dumps({"ok": False, "error": "HTTP_CONTRACT_FAILED", "stdout": result.stdout[-16000:],
                              "diagnostic": result.stderr[-16000:]}))
            return result.returncode
        report = json.loads(result.stdout)
        retry_checks = []
        cases = [("off", "false", "true", "accept"), ("uppercase-off", "FALSE", "true", "accept"),
                 ("on", "true", "true", "reject"), ("blank", "", "true", "reject"),
                 ("missing", None, "true", "reject"), ("connect-retry", "false", "false", "reject")]
        for name, retry, connect, expected in cases:
            command = ["java", "-Xmx256m", "-XX:ActiveProcessorCount=2",
                       "-Djdk.httpclient.disableRetryConnect=" + connect]
            if retry is not None:
                command.append("-Djdk.httpclient.enableAllMethodRetry=" + retry)
            command += ["-cp", temp + os.pathsep + cp,
                        "org.apache.hugegraph.benchmark.ldbc.TransportRetryCheck", expected]
            checked = subprocess.run(command, capture_output=True, text=True, timeout=20)
            if checked.returncode:
                raise ValueError("Retry policy contract failed: " + name + ": " + checked.stderr[-4000:])
            retry_checks.append({"name": name, **json.loads(checked.stdout)})
        report["retryPolicyChecks"] = retry_checks
        report["retryCheckSha256"] = sha(retry_source)
        report["buildManifestSha256"] = sha(build / "build-manifest.json")
        report["checkSha256"] = sha(source)
        report["vectorsSha256"] = sha(args.vectors)
        print(json.dumps(report))
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
