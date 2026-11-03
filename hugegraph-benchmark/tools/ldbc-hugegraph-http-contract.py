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

"""Run official DbValidator through a real HugeGraph ContextGremlinServer on bounded loopback unit data."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import time
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("http_contract", ROOT / "tools/ldbc-driver-http-contract.py")
contract = importlib.util.module_from_spec(spec)
spec.loader.exec_module(contract)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--classpath-report", type=Path, required=True, help="Actual prebuilt HugeGraph Surefire classpath")
    p.add_argument("--build", type=Path, required=True, help="Locked official driver/adapter build")
    p.add_argument("--vectors", type=Path, required=True, help="Hand-authored unit data; never an SF100 benchmark")
    p.add_argument("--maven-repo", type=Path, default=Path.home() / ".m2/repository")
    p.add_argument("--out", type=Path, required=True, help="New directory for client/server logs and result")
    a = p.parse_args()
    cp = next(e.attrib["value"] for e in ET.parse(a.classpath_report).findall(".//property")
              if e.attrib["name"] == "java.class.path")
    entries = list(dict.fromkeys(entry for entry in cp.split(os.pathsep) if entry))
    if any(not Path(entry).exists() for entry in entries):
        raise ValueError("Missing HugeGraph classpath entry")
    manifest = json.loads((a.build / "build-manifest.json").read_text())
    jars = []
    for entry in manifest["files"]:
        path = a.build / entry["path"]
        if contract.sha(path) != entry["sha256"]:
            raise ValueError("Runtime artifact changed")
        jars.append(path.resolve())
    for entry in manifest["sources"]:
        if contract.sha(ROOT / entry["path"]) != entry["sha256"]:
            raise ValueError("Adapter source drift")
    jars += [a.maven_repo / g.replace('.', '/') / name / version / f"{name}-{version}.jar"
             for g, name, version in contract.runtime.COORDINATES]
    client_cp = os.pathsep.join(map(str, jars))
    a.out.mkdir()
    output = a.out.resolve()
    sources = [ROOT / "adapters/ldbc-contract" / name for name in ("DriverHttpCheck.java", "DriverRemoteCheck.java")]
    with tempfile.TemporaryDirectory(prefix="hgbench-real-http-") as temp:
        base = Path(temp)
        ready, done = base / "ready.json", base / "done"
        compile_result = subprocess.run(["javac", "-J-Xmx256m", "--release", "17", "-cp", client_cp,
            "-d", temp, *map(str, sources)], capture_output=True, text=True, timeout=30)
        if compile_result.returncode:
            raise ValueError(compile_result.stderr)
        script = ROOT / "tests-normative/ldbc-hugegraph.groovy"
        command = ["java", "-Xmx512m", "-XX:ActiveProcessorCount=2", "-cp", os.pathsep.join(entries),
            "groovy.ui.GroovyMain", str(script), str(a.vectors.resolve()),
            str(ROOT / "adapters/ldbc-gremlin/snb-read.groovy"), str(ROOT / "adapters/ldbc-gremlin/snb-update.groovy"),
            str(output / "server-result.json"), str(ROOT / "adapters/ldbc-gremlin/reconcile.groovy"),
            str(ROOT / "adapters/ldbc-gremlin/inventory.groovy"), str(ready), str(done)]
        with (output / "server.log").open("w") as log:
            server = subprocess.Popen(command, cwd=temp, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            try:
                deadline = time.monotonic() + 30
                while not ready.exists():
                    if server.poll() is not None or time.monotonic() >= deadline:
                        raise ValueError("HugeGraph HTTP server did not become ready; see server.log")
                    time.sleep(0.05)
                with (output / "client.log").open("w") as client_log:
                    run = subprocess.run(["java", "-Xmx384m", "-XX:ActiveProcessorCount=2",
                        "-Djdk.httpclient.disableRetryConnect=true", "-Djdk.httpclient.enableAllMethodRetry=false",
                        "-cp", temp + os.pathsep + client_cp,
                        "org.apache.hugegraph.benchmark.ldbc.DriverRemoteCheck", str(a.vectors.resolve()),
                        str(ROOT / "benchmarks/ldbc-snb-interactive-v1/tasks.json"), str(ready),
                        str(output / "result.json")], cwd=temp, stdout=client_log, stderr=subprocess.STDOUT, timeout=40)
                if run.returncode:
                    raise ValueError("Official validator failed against real HugeGraph HTTP; see client/server logs")
            finally:
                done.touch()
                try:
                    server.wait(timeout=12)
                except subprocess.TimeoutExpired:
                    os.killpg(server.pid, signal.SIGTERM)
                    try:
                        server.wait(timeout=3)
                    except subprocess.TimeoutExpired:
                        os.killpg(server.pid, signal.SIGKILL)
                        server.wait()
            if server.returncode:
                raise ValueError("HugeGraph server exited abnormally; see server.log")
        report = json.loads((output / "result.json").read_text())
        server_config = json.loads(ready.read_text())
        report["serverConfiguration"] = {key: value for key, value in server_config.items() if key not in ("endpoint", "alias")}
        report["buildManifestSha256"] = contract.sha(a.build / "build-manifest.json")
        report["vectorsSha256"] = contract.sha(a.vectors)
        report["classpathReportSha256"] = contract.sha(a.classpath_report)
        runtime = hashlib.sha256()
        for item in entries:
            path = Path(item)
            for file in sorted(path.rglob("*")) if path.is_dir() else [path]:
                if file.is_file():
                    runtime.update(str(file).encode() + b"\0")
                    runtime.update(bytes.fromhex(contract.sha(file)))
        report["runtimeSha256"] = runtime.hexdigest()
        report["sources"] = [{"path": str(path.relative_to(ROOT)), "sha256": contract.sha(path)}
                             for path in sources + [script, Path(__file__)]]
        (output / "result.json").write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps(report))
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
