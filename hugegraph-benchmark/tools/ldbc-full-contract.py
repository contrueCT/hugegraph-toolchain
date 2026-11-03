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

"""Run all 29 operation scripts and transactional failure tests on bounded unit fixtures, offline."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("short_runtime", ROOT / "tools/ldbc-short-read-contract.py")
runtime = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runtime)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--maven-repo", type=Path, default=Path.home() / ".m2/repository")
    parser.add_argument("--vectors-out", type=Path, help="NEW unit transport-vector file; never an SF100 reference")
    args = parser.parse_args()
    names = [f"{g.replace('.', '/')}/{a}/{v}/{a}-{v}.jar" for g, a, v in runtime.COORDINATES]
    jars = [args.maven_repo / name for name in names]
    missing = [str(p) for p in jars if not p.is_file()]
    if missing:
        print(json.dumps({"ok": False, "error": "MISSING_CACHED_DEPENDENCIES", "files": missing}))
        return 3
    scripts = [ROOT / "tests-normative/ldbc-full.groovy", ROOT / "adapters/ldbc-gremlin/snb-read.groovy",
               ROOT / "adapters/ldbc-gremlin/snb-update.groovy"]
    result = subprocess.run(["java", "-Xmx256m", "-cp", os.pathsep.join(map(str, jars)),
                             "groovy.ui.GroovyMain", *map(str, scripts),
                             *([str(args.vectors_out)] if args.vectors_out else [])],
                            capture_output=True, text=True, timeout=45)
    if result.returncode:
        print(json.dumps({"ok": False, "error": "SEMANTIC_TEST_FAILED", "diagnostic": result.stderr[-20000:]}))
        return result.returncode
    report = json.loads(result.stdout)
    report["sources"] = [{"path": str(p.relative_to(ROOT)), "sha256": hashlib.sha256(p.read_bytes()).hexdigest()}
                         for p in scripts]
    report["dependencies"] = [{"artifact": name, "sha256": hashlib.sha256(p.read_bytes()).hexdigest()}
                              for name, p in zip(names, jars)]
    print(json.dumps(report, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
