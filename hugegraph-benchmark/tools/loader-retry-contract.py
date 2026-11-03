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

"""Compile real Loader option/HTTP classes and run targeted JUnit checks without Hadoop dependency resolution."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--classpath-report", type=Path, required=True)
    p.add_argument("--client-classes", type=Path, required=True)
    p.add_argument("--jcommander", type=Path, required=True, help="Actual Loader dependency version 1.78 jar")
    p.add_argument("--log4j", type=Path, required=True, help="Log4j 1.2 API needed to compile existing test class")
    p.add_argument("--lombok", type=Path, required=True, help="Actual Loader compile dependency 1.18.32 jar")
    p.add_argument("--http-library", type=Path, action="append", default=[],
                   help="Optional repeatable complete OkHttp/Okio/Kotlin classpath override for version compatibility")
    a = p.parse_args()
    cp = next(e.attrib["value"] for e in ET.parse(a.classpath_report).findall(".//property")
              if e.attrib["name"] == "java.class.path")
    paths = [str(a.jcommander.resolve()), str(a.log4j.resolve()), str(a.lombok.resolve()),
             str(a.client_classes.resolve())]
    paths += [str(path.resolve()) for path in a.http_library]
    excluded = ["jcommander"]
    if a.http_library:
        excluded += ["/com/squareup/okhttp3/okhttp/", "/com/squareup/okio/", "/org/jetbrains/kotlin/"]
    paths += [entry for entry in cp.split(os.pathsep) if entry and not any(part in entry for part in excluded)]
    if any(not Path(entry).exists() for entry in paths):
        raise ValueError("Classpath contains missing files")
    cp = os.pathsep.join(dict.fromkeys(paths))
    base = ROOT.parent / "hugegraph-loader/src"
    sources = [base / "main/java/org/apache/hugegraph/loader" / name for name in
               ["executor/LoadOptions.java", "util/LoadUtil.java", "constant/Constants.java",
                "mapping/BackendStoreInfo.java", "filter/util/ShortIdConfig.java", "exception/LoadException.java",
                "util/HugeClientHolder.java", "util/UrlParseUtil.java", "util/NoRetryHttpClient.java"]]
    sources += [base / "test/java/org/apache/hugegraph/loader/test/unit" / name
                for name in ["LoadOptionsTest.java", "NoRetryHttpClientTest.java"]]
    with tempfile.TemporaryDirectory(prefix="hgbench-loader-junit-") as temp:
        runner = Path(temp) / "RetryCheck.java"
        runner.write_text('public class RetryCheck { public static void main(String[] a) { '
            'org.junit.runner.Result r = new org.junit.runner.JUnitCore().run(org.junit.runner.Request.method('
            'org.apache.hugegraph.loader.test.unit.LoadOptionsTest.class, "testRetriesCanBeDisabled")); '
            'if (!r.wasSuccessful() || r.getRunCount() != 1) throw new AssertionError(r.getFailures()); '
            'org.junit.runner.Result h = org.junit.runner.JUnitCore.runClasses('
            'org.apache.hugegraph.loader.test.unit.NoRetryHttpClientTest.class); '
            'if (!h.wasSuccessful() || h.getRunCount() != 4) throw new AssertionError(h.getFailures()); }}')
        compile_result = subprocess.run(["javac", "-J-Xmx256m", "--release", "17", "-cp", cp, "-d", temp,
                                        *map(str, sources), str(runner)], capture_output=True, text=True, timeout=30)
        if compile_result.returncode:
            raise ValueError(compile_result.stderr)
        result = subprocess.run(["java", "-Xmx256m", "-cp", temp + os.pathsep + cp, "RetryCheck"],
                                cwd=temp, capture_output=True, text=True, timeout=45)
        if result.returncode:
            raise ValueError(result.stdout + result.stderr)
    print(json.dumps({"scope": "actual-loader-option-and-http-source-targeted-junit", "tests": 5, "passed": True,
        "compileRelease": 17,
        "javaVersion": subprocess.run(["java", "--version"], capture_output=True, text=True, timeout=10).stdout.strip(),
        "javacVersion": subprocess.run(["javac", "--version"], capture_output=True, text=True, timeout=10).stdout.strip(),
        "method": "LoadOptionsTest.testRetriesCanBeDisabled", "stubbedClasses": [], "fullMavenBuild": False,
        "httpTests": "NoRetryHttpClientTest: real loopback HTTP, retry-after-503, redirect, lost acknowledgement, independent calls",
        "httpDependencies": [{"path": entry, "sha256": hashlib.sha256(Path(entry).read_bytes()).hexdigest()}
                             for entry in paths if "/okhttp/" in entry and entry.endswith(".jar")],
        "httpLibraryOverrides": [{"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
                                 for path in a.http_library],
        "sources": [{"path": str(f.relative_to(ROOT.parent)), "sha256": hashlib.sha256(f.read_bytes()).hexdigest()}
                    for f in sources], "jcommanderSha256": hashlib.sha256(a.jcommander.read_bytes()).hexdigest(),
        "lombokSha256": hashlib.sha256(a.lombok.read_bytes()).hexdigest()}))


if __name__ == "__main__":
    main()
