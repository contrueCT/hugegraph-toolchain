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

"""Fetch and verify the small pinned official driver source via gh CLI. No graph data is fetched."""
import argparse
import hashlib
import io
import json
from pathlib import Path, PurePosixPath
import subprocess
import tarfile

ROOT = Path(__file__).resolve().parents[1]


def extract(raw, output):
    lock_raw = (ROOT / "adapters/ldbc-driver/source.lock.json").read_bytes()
    lock = json.loads(lock_raw)
    if len(raw) > 8 * 1024**2 or hashlib.sha256(raw).hexdigest() != lock["archiveSha256"]:
        raise ValueError("Upstream source archive differs from the reviewed lock")
    expected = {e["path"]: e for e in lock["files"]}
    seen = set()
    checked = []
    with tarfile.open(fileobj=io.BytesIO(raw), mode="r:gz") as archive:
        for member in archive:
            if member.isdir():
                continue
            if not member.isfile():
                raise ValueError("Links and special archive entries forbidden")
            parts = PurePosixPath(member.name).parts
            if len(parts) < 2 or ".." in parts or PurePosixPath(member.name).is_absolute():
                raise ValueError("Unsafe source archive path")
            name = "/".join(parts[1:])
            entry = expected.get(name)
            if entry is None or name in seen or member.size != entry["bytes"]:
                raise ValueError("Unexpected/changed source entry: " + name)
            value = archive.extractfile(member).read()
            if hashlib.sha256(value).hexdigest() != entry["sha256"]:
                raise ValueError("Source hash mismatch: " + name)
            seen.add(name)
            checked.append((name, value))
    if seen != set(expected):
        raise ValueError("Incomplete official source archive")
    output.mkdir()
    for name, value in checked:
        path = output / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(value)
    (output / "hgbench-source-lock.json").write_bytes(lock_raw)
    return {"repository": lock["repository"], "commit": lock["commit"], "files": len(seen),
            "archiveBytes": len(raw), "expandedBytes": sum(len(v) for _, v in checked)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True, type=Path, help="New source directory; never replaces previous work")
    parser.add_argument("--archive", type=Path, help="Verify a pre-fetched archive offline instead of using gh")
    args = parser.parse_args()
    if args.out.exists():
        parser.error("Output directory already exists")
    lock = json.loads((ROOT / "adapters/ldbc-driver/source.lock.json").read_text())
    if args.archive:
        raw = args.archive.read_bytes()
    else:
        result = subprocess.run(["gh", "api", "repos/" + lock["repository"] + "/tarball/" + lock["commit"]],
                                capture_output=True, timeout=60, check=True)
        raw = result.stdout
    print(json.dumps(extract(raw, args.out)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
