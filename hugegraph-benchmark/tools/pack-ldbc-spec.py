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

"""Create a deterministic standalone specification archive; no graph data or database operations."""
import argparse
import gzip
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import tarfile
import tempfile

ROOT = Path(__file__).resolve().parents[1]
SUITE = ROOT / "benchmarks/ldbc-snb-interactive-v1"
module_spec = importlib.util.spec_from_file_location("ldbc_spec", SUITE / "verify.py")
checker = importlib.util.module_from_spec(module_spec)
module_spec.loader.exec_module(checker)


def pack(output):
    report = checker.verify_spec()
    lock = json.loads((SUITE / "bundle.lock.json").read_text())
    entries = lock["files"] + [{"path": "bundle.lock.json", **checker.digest(SUITE / "bundle.lock.json")}]
    if sum(e["bytes"] for e in entries) > 16 * 1024 ** 2:
        raise ValueError("Specification exceeds the 16 MiB packaging budget; inspect unexpected files")
    prefix = report["suite"] + "-" + report["version"]
    output = Path(output)
    with tempfile.NamedTemporaryFile(dir=output.parent, prefix=".hgbench-spec-", delete=False) as stream:
        temporary = Path(stream.name)
        try:
            with gzip.GzipFile(filename="", fileobj=stream, mode="wb", mtime=0) as gz:
                with tarfile.open(fileobj=gz, mode="w|") as archive:
                    for entry in sorted(entries, key=lambda e: e["path"]):
                        data = checker.local_file(SUITE, entry["path"]).read_bytes()
                        if len(data) != entry["bytes"] or hashlib.sha256(data).hexdigest() != entry["sha256"]:
                            raise ValueError("Source changed during packaging")
                        info = tarfile.TarInfo(prefix + "/" + entry["path"])
                        info.size = len(data)
                        info.mode = 0o644
                        info.mtime = 0
                        archive.addfile(info, io.BytesIO(data))
            stream.flush()
            os.fsync(stream.fileno())
            os.link(temporary, output)  # exclusive publication: never replace a previous package
        finally:
            temporary.unlink(missing_ok=True)
    return {"suite": report["suite"], "version": report["version"], "bundleSha256": report["bundleSha256"],
            "archive": str(output), **checker.digest(output), "releaseReady": False, "graphDataIncluded": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True, help="NEW .tar.gz path; existing files are never overwritten")
    args = parser.parse_args()
    try:
        print(json.dumps({"ok": True, "data": pack(args.out)}))
        return 0
    except (OSError, ValueError) as error:
        print(json.dumps({"ok": False, "error": str(error)}))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
