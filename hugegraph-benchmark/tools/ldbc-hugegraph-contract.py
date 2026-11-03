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

"""Run all operation unit vectors on a prebuilt HugeGraph memory backend (no downloads)."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--classpath-report", type=Path, required=True, help="Existing Maven Surefire XML with java.class.path")
    parser.add_argument("--vectors", type=Path, required=True, help="Hand-authored unit transport vectors")
    parser.add_argument("--out", type=Path, required=True, help="New output directory for log and evidence")
    args = parser.parse_args()
    cp = next(p.attrib["value"] for p in ET.parse(args.classpath_report).findall(".//property")
              if p.attrib["name"] == "java.class.path")
    entries = list(dict.fromkeys(p for p in cp.split(os.pathsep) if p))
    missing = [p for p in entries if not Path(p).exists()]
    if missing:
        raise ValueError("Classpath entries unavailable: " + str(missing))
    args.out.mkdir()
    output = args.out.resolve()
    with tempfile.TemporaryDirectory(prefix="hgbench-hg-unit-") as temp:
        command = ["java", "-Xmx512m", "-cp", os.pathsep.join(entries), "groovy.ui.GroovyMain",
            str(ROOT / "tests-normative/ldbc-hugegraph.groovy"), str(args.vectors.resolve()),
            str(ROOT / "adapters/ldbc-gremlin/snb-read.groovy"), str(ROOT / "adapters/ldbc-gremlin/snb-update.groovy"),
            str(output / "result.json"), str(ROOT / "adapters/ldbc-gremlin/reconcile.groovy"), str(ROOT / "adapters/ldbc-gremlin/inventory.groovy")]
        with (output / "execution.log").open("w") as log:
            result = subprocess.run(command, cwd=temp, stdout=log, stderr=subprocess.STDOUT, timeout=60)
        if result.returncode:
            print((output / "execution.log").read_text()[-16000:])
            return result.returncode
        modspec = importlib.util.spec_from_file_location("prepare", ROOT / "tools/ldbc-prepare.py")
        prepare = importlib.util.module_from_spec(modspec)
        modspec.loader.exec_module(prepare)
        vectors = json.loads(args.vectors.read_text())
        fields, relations, counts = {}, {}, {}
        for item in vectors["snapshot"]["vertices"] + vectors["snapshot"]["edges"]:
            label = item["label"]
            fields.setdefault(label, set()).update(item["properties"])
            counts[label] = counts.get(label, 0) + 1
            if "_" in label:
                parts = label.split("_")
                relations[label] = (parts[0], parts[2])
        fields["post"].update(["imageFile", "language", "browserUsed", "locationIP"])
        fields["comment"].update(["browserUsed", "locationIP"])
        for label in ("forum_hasTag_tag", "comment_hasTag_tag"):
            fields.setdefault(label, set()).add("edgeKey")
            parts = label.split("_")
            relations[label] = (parts[0], parts[2])
            counts.setdefault(label, 0)
        expected = prepare.expected_inventory(fields, relations, counts)
        observed = json.loads((output / "result.json.inventory.json").read_text())
        if observed["initial"] != expected:
            (output / "inventory.expected.json").write_text(json.dumps(expected, indent=2))
            raise ValueError("Real HugeGraph schema/count inventory differs from converter contract; see output")
        if observed["extraVertex"]["counts"] == expected["counts"] or observed["extraSchema"]["schema"] == expected["schema"]:
            raise ValueError("Inventory failed to reveal extra records or schema")
        report = json.loads((output / "result.json").read_text())
        # Capture actual runtime bytes, including mutable target/classes rather than relying on a Git label.
        runtime = hashlib.sha256()
        for item in entries:
            path = Path(item)
            files = sorted(path.rglob("*")) if path.is_dir() else [path]
            for file in files:
                if file.is_file():
                    runtime.update(str(file).encode() + b"\0")
                    runtime.update(hashlib.sha256(file.read_bytes()).digest())
        report["runtimeSha256"] = runtime.hexdigest()
        report["classpathReportSha256"] = hashlib.sha256(args.classpath_report.read_bytes()).hexdigest()
        report["vectorsSha256"] = hashlib.sha256(args.vectors.read_bytes()).hexdigest()
        report["sources"] = [{"path": name, "sha256": hashlib.sha256((ROOT / name).read_bytes()).hexdigest()}
            for name in ["tests-normative/ldbc-hugegraph.groovy", "adapters/ldbc-gremlin/snb-read.groovy",
                         "adapters/ldbc-gremlin/snb-update.groovy", "adapters/ldbc-gremlin/reconcile.groovy", "adapters/ldbc-gremlin/inventory.groovy", "tools/ldbc-prepare.py"]]
        (output / "result.json").write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps(report))
        return 0

if __name__ == "__main__":
    raise SystemExit(main())
