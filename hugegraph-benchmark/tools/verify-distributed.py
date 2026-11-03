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

"""Disk-backed, independent oracle for the generated distributed benchmark assets."""

import argparse
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile


def rows(path):
    with path.open(encoding="utf-8") as stream:
        for line in stream:
            yield json.loads(line)


def file_facts(path):
    digest = hashlib.sha256()
    count = total = 0
    with path.open("rb") as stream:
        for line in stream:
            digest.update(line)
            count += 1
            total += len(line)
    return {"records": count, "bytes": total, "sha256": digest.hexdigest()}


def fail(message):
    raise ValueError(message)


def validate(directory, scratch=None):
    directory = Path(directory)
    manifest = json.loads((directory / "manifest.json").read_text())
    scales = {"dev": 8192, "s1": 1048576, "s2": 16777216, "s3": 134217728}
    if manifest["suiteId"] != "hgbench-distributed-read-v0.1-draft" or manifest["seed"] != 3346:
        fail("Unknown suite or seed")
    n = scales[manifest["scale"]]
    expected_edges = n * 8 + (n // 1024) * 1016
    if manifest["facts"]["vertices"] != n or manifest["facts"]["edges"] != expected_edges:
        fail("Incorrect scale counts")
    contract_root = Path(__file__).resolve().parent.parent / "benchmarks/distributed-read-v0.1"
    contract_names = ["spec.md", "performance-profile.json", "environment.schema.json", "generator.mjs", "../../tools/verify-distributed.py", "../core-read-v0.1/spec.md", "../core-read-v0.1/reference.mjs"]
    contract = {name: hashlib.sha256((contract_root / name).read_bytes()).hexdigest() for name in contract_names}
    contract_digest = hashlib.sha256(json.dumps(contract, separators=(",", ":")).encode()).hexdigest()
    if manifest["contract"] != {"files": contract, "digest": contract_digest}:
        fail("Benchmark contract mismatch")
    names = ["vertices.ndjson", "edges.ndjson", "validation.ndjson"]
    if set(manifest["files"]) != set(names):
        fail("Unexpected evidence files")
    for name in names:
        if manifest["files"][name] != file_facts(directory / name):
            fail("Checksum or count mismatch: " + name)
    with tempfile.TemporaryDirectory(prefix="hgbench-oracle-", dir=scratch) as temporary:
        with sqlite3.connect(str(Path(temporary) / "graph.sqlite")) as db:
            db.execute("PRAGMA temp_store=FILE")
            db.execute("PRAGMA cache_size=-32768")
            db.execute("CREATE TABLE vertex(id INTEGER PRIMARY KEY, bucket TEXT, body TEXT)")
            db.execute("CREATE INDEX vertex_bucket ON vertex(bucket)")
            db.execute("CREATE TABLE edge(id TEXT UNIQUE, source INTEGER, target INTEGER, PRIMARY KEY(source,target))")
            count = 0
            for v in rows(directory / "vertices.ndjson"):
                count += 1
                if type(v["id"]) is not int or v["id"] != count or v["label"] != "node":
                    fail("Vertex ID, type or order mismatch")
                payload = "".join(hashlib.sha256(f"hgbench:3346:{count}:{block}".encode()).hexdigest() for block in range(4))
                if v["properties"] != {"group": f"bucket-{(count - 1) % 4096}", "rank": count, "payload": payload}:
                    fail("Vertex projection mismatch")
                db.execute("INSERT INTO vertex VALUES (?,?,?)", (v["id"], v["properties"]["group"], json.dumps(v)))
            if count != n:
                fail("Vertex count mismatch")
            count = 0
            for edge in rows(directory / "edges.ndjson"):
                count += 1
                source, target = edge["source"], edge["target"]
                if type(source) is not int or type(target) is not int or not (1 <= source <= n and 1 <= target <= n):
                    fail("Edge endpoint is not a valid numeric ID")
                if source == target or edge["label"] != "link":
                    fail("Self edge or wrong label")
                db.execute("INSERT INTO edge VALUES (?,?,?)", (edge["id"], source, target))
            if count != expected_edges:
                fail("Edge count mismatch")
            if db.execute("SELECT count(*) FROM edge WHERE target = source % ? + 1", (n,)).fetchone()[0] != n:
                fail("Missing connectivity backbone")
            for source, degree in db.execute("SELECT source,count(*) FROM edge GROUP BY source"):
                if degree != (1024 if (source - 1) % 1024 == 0 else 8):
                    fail("Out-degree mismatch")
            db.commit()

            def adjacent(source):
                return {row[0] for row in db.execute("SELECT target FROM edge WHERE source=?", (source,))}

            def layer(sources):
                result = set()
                for source in sources:
                    result.update(adjacent(source))
                return result

            def oracle(p):
                args = p["args"]
                op = p["operation"]
                if op == "point":
                    row = db.execute("SELECT body FROM vertex WHERE id=?", (args["id"],)).fetchone()
                    return json.loads(row[0]) if row else None
                if op in ("neighbors", "paged-neighbors"):
                    return adjacent(args["source"])
                if op in ("filter", "materialize"):
                    result = list(db.execute("SELECT id,body FROM vertex WHERE bucket=? ORDER BY id", (args["group"],)))
                    return {x[0] for x in result} if op == "filter" else [json.loads(x[1]) for x in result]
                if op == "common":
                    return adjacent(args["sources"][0]) & adjacent(args["sources"][1])
                if op == "kout":
                    frontier, seen = {args["source"]}, set()
                    for _ in range(args["depth"]):
                        if args["nearest"]:
                            seen.update(frontier)
                        frontier = layer(frontier) - seen - {args["source"]}
                    if args["limit"] is None:
                        return frontier
                    return {"allowed": sorted(frontier), "count": min(args["limit"], len(frontier))}
                if op == "shortest":
                    frontier, seen = {args["source"]}, {args["source"]}
                    for distance in range(args["maxDepth"] + 1):
                        if args["target"] in frontier:
                            return {"distance": distance}
                        frontier = layer(frontier) - seen
                        seen.update(frontier)
                    return {"distance": None}
                fail("Unknown operation")

            seen = set()
            for item in rows(directory / "validation.ndjson"):
                p, answer = item["parameter"], item["expected"]
                identity = (p["caseId"], p["ordinal"])
                if identity in seen or p["datasetId"] != manifest["scale"]:
                    fail("Duplicate parameter or wrong dataset")
                seen.add(identity)
                actual = oracle(p)
                if isinstance(actual, set):
                    if not isinstance(answer, list) or len(set(answer)) != len(answer) or actual != set(answer):
                        fail("Incorrect set answer: " + p["parameterId"])
                elif p["policy"] == "bounded-set":
                    if sorted(answer["allowed"]) != actual["allowed"] or answer["count"] != actual["count"]:
                        fail("Incorrect bounded answer: " + p["parameterId"])
                elif actual != answer:
                    fail("Incorrect answer: " + p["parameterId"])
            required = {(f"DR{i:02d}", j) for i in range(1, 13) for j in range(16)}
            if seen != required:
                fail("Incomplete validation parameter coverage")
    return {"ok": True, "scope": "independent-sqlite-data-oracle", "scale": manifest["scale"],
            "vertices": n, "edges": expected_edges, "validatedParameters": len(seen),
            "manifestSha256": hashlib.sha256((directory / "manifest.json").read_bytes()).hexdigest(),
            "databasePerformanceValidated": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", help="Generated dataset directory with manifest.json")
    parser.add_argument("--scratch-dir", help="Existing directory with enough disk for a temporary SQLite index")
    parser.add_argument("--report", help="Write the JSON result to a new file, refusing overwrite")
    args = parser.parse_args()
    try:
        result = validate(args.directory, args.scratch_dir)
        text = json.dumps(result, indent=2) + "\n"
        if args.report:
            with open(args.report, "x", encoding="utf-8") as stream:
                stream.write(text)
        print(text, end="")
    except Exception as error:
        print(json.dumps({"ok": False, "error": str(error)}))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
