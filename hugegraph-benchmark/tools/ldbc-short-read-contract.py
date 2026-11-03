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

"""Run all seven short-read templates on a bounded hand-authored TinkerGraph unit fixture, offline."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]
COORDINATES = [
    ("org.apache.tinkerpop", "tinkergraph-gremlin", "3.8.1"),
    ("org.apache.tinkerpop", "gremlin-core", "3.8.1"),
    ("org.apache.tinkerpop", "gremlin-shaded", "3.8.1"),
    ("org.apache.tinkerpop", "gremlin-language", "3.8.1"),
    ("org.apache.groovy", "groovy", "4.0.25"),
    ("org.apache.groovy", "groovy-json", "4.0.25"),
    ("org.slf4j", "slf4j-api", "1.7.25"),
    ("org.slf4j", "jcl-over-slf4j", "1.7.25"),
    ("org.apache.commons", "commons-configuration2", "2.9.0"),
    ("org.apache.commons", "commons-lang3", "3.12.0"),
    ("org.apache.commons", "commons-text", "1.10.0"),
    ("commons-beanutils", "commons-beanutils", "1.9.4"),
    ("commons-collections", "commons-collections", "3.2.2"),
    ("org.yaml", "snakeyaml", "2.0"),
    ("com.carrotsearch", "hppc", "0.7.1"),
    ("com.squareup", "javapoet", "1.13.0"),
    ("net.objecthunter", "exp4j", "0.4.8"),
    ("com.github.ben-manes.caffeine", "caffeine", "2.3.1"),
    ("org.antlr", "antlr4-runtime", "4.9.1"),
    ("org.javatuples", "javatuples", "1.2"),
]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--maven-repo", type=Path, default=Path.home() / ".m2/repository")
    args = parser.parse_args()
    names = [f"{g.replace('.', '/')}/{a}/{v}/{a}-{v}.jar" for g, a, v in COORDINATES]
    jars = [args.maven_repo / name for name in names]
    missing = [str(path) for path in jars if not path.is_file()]
    if missing:
        print(json.dumps({"ok": False, "error": "MISSING_CACHED_DEPENDENCIES", "files": missing}))
        return 3
    source = ROOT / "adapters/ldbc-short-reads.mjs"
    check = ROOT / "tests-normative/ldbc-short-reads.groovy"
    with tempfile.TemporaryDirectory(prefix="hgbench-gremlin-contract-") as temp:
        exported = subprocess.run(["node", "--input-type=module", "-e",
                    "import {queries} from './adapters/ldbc-short-reads.mjs'; "
                    "process.stdout.write(JSON.stringify(queries));"],
                    cwd=ROOT, capture_output=True, text=True, check=True, timeout=10)
        templates = Path(temp) / "templates.json"
        templates.write_text(exported.stdout)
        result = subprocess.run(["java", "-Xmx256m", "-cp", os.pathsep.join(str(p) for p in jars),
                                 "groovy.ui.GroovyMain", str(check), str(templates)],
                                 capture_output=True, text=True, timeout=30)
        if result.returncode:
            print(json.dumps({"ok": False, "error": "SEMANTIC_TEST_FAILED", "diagnostic": result.stderr[-12000:]}))
            return result.returncode
        report = json.loads(result.stdout)
        report["dependencies"] = [{"artifact": name, "sha256": hashlib.sha256(p.read_bytes()).hexdigest()}
                                  for name, p in zip(names, jars)]
        report["adapterSha256"] = hashlib.sha256(source.read_bytes()).hexdigest()
        report["checkSha256"] = hashlib.sha256(check.read_bytes()).hexdigest()
        print(json.dumps(report, ensure_ascii=False))
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
