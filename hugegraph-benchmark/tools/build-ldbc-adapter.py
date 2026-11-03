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

"""Build the pinned official driver and HG adapter; never connects to a database or downloads graph data."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import zipfile

ROOT = Path(__file__).resolve().parents[1]
COMMIT = "8c6e849a60397eaced9f3cdd08b0efa430366cb9"


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def verified_source(source):
    lock = json.loads((source / "hgbench-source-lock.json").read_text())
    if lock["commit"] != COMMIT or lock["repository"] != "ldbc/ldbc_snb_interactive_v1_driver":
        raise ValueError("Wrong official driver source identity")
    if lock != json.loads((ROOT / "adapters/ldbc-driver/source.lock.json").read_text()):
        raise ValueError("Driver source lock differs from the reviewed HG-Bench lock")
    expected = set()
    for entry in lock["files"]:
        relative = Path(entry["path"])
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError("Unsafe source lock path")
        path = source / relative
        if path.is_symlink() or not path.resolve().is_relative_to(source.resolve()) or sha(path) != entry["sha256"]:
            raise ValueError("Driver source drift: " + entry["path"])
        if relative.parts[0] == "src":
            expected.add(entry["path"])
    observed = {p.relative_to(source).as_posix() for p in (source / "src").rglob("*") if p.is_file()}
    if observed != expected:
        raise ValueError("Unexpected additional driver source/resource files")
    return sha(source / "hgbench-source-lock.json")


def bounded(command, cwd, logfile, seconds):
    environment = os.environ.copy()
    environment["MAVEN_OPTS"] = "-Xmx512m"
    with logfile.open("x") as log:
        process = subprocess.Popen(command, cwd=cwd, stdout=log, stderr=subprocess.STDOUT,
                                   env=environment, start_new_session=True)
        try:
            code = process.wait(timeout=seconds)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
            raise ValueError("Build time budget exceeded; see " + str(logfile))
    if code:
        raise ValueError("Build failed; see " + str(logfile))


def build(source, output, offline):
    source = Path(source).resolve()
    output = Path(output).resolve()
    source_digest = verified_source(source)
    output.mkdir()  # A new output directory, including failed-build logs, is never silently reused.
    command = ["mvn", "-B", "-ntp", "-DskipTests", "clean", "package"]
    if offline:
        command.insert(1, "-o")
    bounded(command, source, output / "driver-build.log", 180)
    if verified_source(source) != source_digest:
        raise ValueError("Driver source changed during build")
    driver = source / "target/driver-standalone.jar"
    java_sources = sorted((ROOT / "adapters/ldbc-driver/src").rglob("*.java"))
    resources = {
        "snb-read.groovy": ROOT / "adapters/ldbc-gremlin/snb-read.groovy",
        "snb-update.groovy": ROOT / "adapters/ldbc-gremlin/snb-update.groovy",
        "tasks.json": ROOT / "benchmarks/ldbc-snb-interactive-v1/tasks.json",
        "META-INF/LICENSE": ROOT / "LICENSE",
        "META-INF/NOTICE": ROOT / "NOTICE",
    }
    with tempfile.TemporaryDirectory(prefix="hgbench-adapter-build-") as temp:
        bounded(["javac", "-J-Xmx256m", "--release", "17", "-encoding", "UTF-8", "-cp", str(driver),
                 "-d", temp, *map(str, java_sources)], ROOT, output / "adapter-build.log", 60)
        entries = {p.relative_to(temp).as_posix(): p for p in Path(temp).rglob("*.class")}
        entries.update(resources)
        jar = output / "hgbench-ldbc-adapter.jar"
        with zipfile.ZipFile(jar, "x", compression=zipfile.ZIP_DEFLATED) as archive:
            for name, path in sorted(entries.items()):
                info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
                info.external_attr = 0o100644 << 16
                info.compress_type = zipfile.ZIP_DEFLATED
                archive.writestr(info, path.read_bytes())
    # Runtime needs these two jars and Java 17+; no adapter Maven install or mutable SNAPSHOT resolution.
    import shutil
    shutil.copyfile(driver, output / "ldbc-driver-standalone.jar")
    files = [{"path": p.name, "bytes": p.stat().st_size, "sha256": sha(p)} for p in
             [output / "ldbc-driver-standalone.jar", output / "hgbench-ldbc-adapter.jar"]]
    manifest = {"schemaVersion": 1, "component": "hgbench-ldbc-http-adapter", "javaRelease": 17,
                "driverRepository": "ldbc/ldbc_snb_interactive_v1_driver", "driverCommit": COMMIT,
                "driverSourceLockSha256": source_digest, "files": files,
                "sources": [{"path": str(p.relative_to(ROOT)), "sha256": sha(p)}
                            for p in java_sources + list(resources.values())],
                "mainClass": "org.ldbcouncil.snb.driver.Client",
                "databaseClass": "org.apache.hugegraph.benchmark.ldbc.HugeGraphDb",
                "requiredJvmOptions": ["-Djdk.httpclient.disableRetryConnect=true",
                                       "-Djdk.httpclient.enableAllMethodRetry=false"],
                "operations": list(range(1, 15)) + list(range(101, 108)) + list(range(1001, 1009)),
                "hugegraphExecuted": False}
    (output / "build-manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return {"output": str(output), "files": files, "operations": 29, "databaseExecuted": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True, help="Pinned official source with hgbench-source-lock.json")
    parser.add_argument("--out", type=Path, required=True, help="NEW output directory; build logs remain on failure")
    parser.add_argument("--offline", action="store_true", help="Do not let Maven fetch missing build dependencies")
    args = parser.parse_args()
    try:
        print(json.dumps({"ok": True, "data": build(args.source, args.out, args.offline)}))
        return 0
    except (OSError, ValueError, KeyError) as error:
        print(json.dumps({"ok": False, "error": str(error)}))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
