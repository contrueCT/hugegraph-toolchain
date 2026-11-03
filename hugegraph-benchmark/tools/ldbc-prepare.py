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

"""Convert a verified SNB v1 CsvBasic/LongDateFormatter snapshot to HugeGraph Loader input."""

import argparse
import collections
import csv
import hashlib
import importlib.util
import json
import os
import platform
import signal
from pathlib import Path
import re
import sqlite3
import zlib

ROOT = Path(__file__).resolve().parent.parent
module_spec = importlib.util.spec_from_file_location("assets", ROOT / "tools/ldbc-assets.py")
assets = importlib.util.module_from_spec(module_spec)
module_spec.loader.exec_module(assets)
shard_spec = importlib.util.spec_from_file_location("shards", ROOT / "tools/ldbc-shards.py")
shards = importlib.util.module_from_spec(shard_spec)
shard_spec.loader.exec_module(shards)
ENTITIES = {"person", "post", "comment", "forum", "place", "organisation", "tag", "tagclass"}
NUMERIC = {"id", "ldbcId", "creationDate", "birthday", "length", "joinDate", "classYear", "workFrom", "edgeKey"}


def identity(entity, value):
    if entity not in ENTITIES or not re.fullmatch(r"[0-9]+", value):
        raise ValueError("Unknown entity or invalid ID")
    number = int(value)
    if number > 2 ** 63 - 1:
        raise ValueError("ID exceeds signed 64-bit range")
    prefix = "message" if entity in ("post", "comment") else entity
    return f"{prefix}:{number}"


def typed(name, value):
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9]*", name):
        raise ValueError("Unexpected property name")
    if name in NUMERIC:
        number = int(value)
        if not -(2 ** 63) <= number < 2 ** 63:
            raise ValueError("Integer exceeds signed 64-bit range")
        return number
    return value


def json_line(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")) + "\n"


def verified_extraction(path, role, frozen):
    path = Path(path)
    manifest = json.loads((path / "extraction-manifest.json").read_text())
    if manifest["asset"] not in frozen["assets"] or manifest["asset"]["role"] != role:
        raise ValueError("Extraction is not from the frozen asset catalog")
    for entry in manifest["files"]:
        relative = assets.safe_member(entry["path"])
        file = path.joinpath(*relative.parts)
        if file.is_symlink() or not file.resolve().is_relative_to(path.resolve()):
            raise ValueError("Extraction path escaped its root")
        if assets.digest_file(file) != (entry["sha256"], entry["bytes"]):
            raise ValueError("Extracted input drift: " + entry["path"])
    return manifest


def csv_rows(path):
    with path.open(encoding="utf-8", newline="") as stream:
        reader = csv.reader(stream, delimiter="|", quoting=csv.QUOTE_NONE)
        header = next(reader)
        for row in reader:
            if len(row) != len(header):
                raise ValueError("CSV width mismatch: " + str(path))
            yield header, row


def prepare(initial, output, frozen, shard_rows=None, shard_bytes=64 * 1024 ** 2,
            max_data_bytes=None, max_index_bytes=None, resume=False, compression="none", max_stored_bytes=None):
    if compression not in ("none", "gzip"):
        raise ValueError("Unsupported shard compression")
    if shard_rows is None:
        if resume or compression != "none" or max_stored_bytes is not None:
            raise ValueError("Resume, compression and stored-byte limits require sharded conversion")
        return prepare_data(initial, output, frozen)
    if not max_data_bytes or not max_index_bytes or min(shard_rows, shard_bytes, max_data_bytes, max_index_bytes) <= 0:
        raise ValueError("Sharded conversion requires positive data/index byte budgets")
    max_stored_bytes = max_data_bytes if max_stored_bytes is None else max_stored_bytes
    if max_stored_bytes <= 0:
        raise ValueError("Stored-byte budget must be positive")
    manifest = verified_extraction(initial, "initial", frozen)
    output = Path(output)
    identity = {"input": manifest, "catalog": frozen, "shardRows": shard_rows, "shardBytes": shard_bytes,
                "maxDataBytes": max_data_bytes, "maxIndexBytes": max_index_bytes,
                "maxStoredBytes": max_stored_bytes, "compression": compression,
                "converter": assets.digest_file(__file__)[0], "shardWriter": assets.digest_file(shards.__file__)[0]}
    if compression == "gzip":
        identity["codec"] = {"gzipLevel": 1, "mtime": 0, "python": platform.python_version(),
                             "zlib": zlib.ZLIB_RUNTIME_VERSION}
    if resume:
        if output.is_symlink() or not output.is_dir():
            raise ValueError("Resume output must be an existing conversion directory")
        if json.loads((output / "conversion-state.json").read_text()) != identity:
            raise ValueError("Conversion identity changed; use a new output directory")
    else:
        output.mkdir()
        shards.atomic_json(output / "conversion-state.json", identity)
    lock = output / ".conversion.lock"
    with lock.open("x") as stream:
        stream.write(str(os.getpid()) + "\n")
    try:
        complete = output / "prepare-manifest.json"
        if complete.exists():
            result = json.loads(complete.read_text())
            for name, fact in result["files"].items():
                path = output.joinpath(*assets.safe_member(name).parts)
                if path.is_symlink() or not path.resolve().is_relative_to(output.resolve()):
                    raise ValueError("Prepared path escaped its root")
                if assets.digest_file(path) != (fact["sha256"], fact["bytes"]):
                    raise ValueError("Prepared output drift: " + name)
            return result
        # Only the scratch index is rebuilt. Immutable data shards are verified during deterministic replay.
        for name in ("reference.sqlite", "reference.sqlite-journal"):
            path = output / name
            if path.is_symlink():
                raise ValueError("Symlink in scratch index")
            path.unlink(missing_ok=True)
        writer = shards.ShardSet(output, shard_rows, shard_bytes, max_data_bytes, compression, max_stored_bytes)
        return prepare_data(initial, output, frozen, writer, max_index_bytes, manifest)
    finally:
        lock.unlink()


def expected_inventory(properties, relations, counts):
    """Public schema and exact initial counts. Internal HugeGraph system labels are out of scope."""
    property_keys = {name: {"dataType": "LONG" if name in NUMERIC else "TEXT",
        "cardinality": "SET" if name in ("emails", "languages") else "SINGLE",
        "aggregateType": "NONE", "writeType": "OLTP", "status": "CREATED"}
        for name in sorted(set().union(*properties.values()))}
    common = lambda keys, required: {"properties": sorted(keys), "nullableKeys": sorted(keys - required),
                                     "enableLabelIndex": True, "ttl": "0", "ttlStartTime": None, "status": "CREATED"}
    vertices = {name: {**common(properties[name], {"ldbcId"}), "idStrategy": "CUSTOMIZE_STRING", "primaryKeys": []}
                for name in sorted(ENTITIES)}
    edges = {name: {**common(properties[name], {"edgeKey"}), "frequency": "MULTIPLE", "edgeLabelType": "NORMAL",
                   "sourceLabel": endpoints[0], "targetLabel": endpoints[1], "sortKeys": ["edgeKey"]}
             for name, endpoints in sorted(relations.items())}
    return {"schemaVersion": 1, "scope": "public-ldbc-initial-graph",
            "schema": {"propertyKeys": property_keys, "vertexLabels": vertices, "edgeLabels": edges, "indexLabels": {}},
            "counts": {"vertices": {name: str(counts[name]) for name in vertices},
                       "edges": {name: str(counts[name]) for name in edges}}}


def prepare_data(initial, output, frozen, shard_set=None, max_index_bytes=None, verified_manifest=None):
    manifest = verified_manifest or verified_extraction(initial, "initial", frozen)
    if frozen["serializer"] != "CsvBasic" or frozen["dateFormatter"] != "LongDateFormatter":
        raise ValueError("This importer requires CsvBasic/LongDateFormatter")
    output = Path(output)
    if shard_set is None:
        output.mkdir()
    for category in ("vertices", "edges"):
        path = output / category
        if path.is_symlink():
            raise ValueError("Symlink in conversion output directory")
        path.mkdir(exist_ok=shard_set is not None)
    tables = []
    for entry in manifest["files"]:
        path = Path(initial) / entry["path"]
        if path.parent.name not in ("static", "dynamic") or path.suffix != ".csv":
            continue
        match = re.fullmatch(r"(.+)_\d+_\d+\.csv", path.name)
        if not match:
            raise ValueError("Unknown table filename")
        tables.append((match[1], path))
    if {name for name, _ in tables if name in ENTITIES} != ENTITIES:
        raise ValueError("Missing required vertex tables")
    counts, properties, relations = collections.Counter(), {}, {}
    with sqlite3.connect(output / "reference.sqlite") as db:
        if shard_set:
            db.execute("PRAGMA cache_size=-65536")
            db.execute("PRAGMA temp_store=FILE")
            pages = max_index_bytes // db.execute("PRAGMA page_size").fetchone()[0]
            if pages < 16:
                raise ValueError("Index budget is too small")
            db.execute(f"PRAGMA max_page_count={pages}")
        db.execute("CREATE TABLE vertices(id TEXT PRIMARY KEY, label TEXT, body TEXT)")
        db.execute("CREATE TABLE edges(source TEXT, target TEXT, label TEXT, body TEXT)")
        pending_edges = 0
        def check_edges():
            condition = "s.id IS NULL OR t.id IS NULL"
            if shard_set:
                condition += " OR s.label != substr(e.body,1,instr(e.body,'|')-1) OR t.label != substr(e.body,instr(e.body,'|')+1)"
            missing = db.execute("SELECT count(*) FROM edges e LEFT JOIN vertices s ON e.source=s.id LEFT JOIN vertices t ON e.target=t.id WHERE " + condition).fetchone()[0]
            if missing:
                raise ValueError("Relationship endpoints are missing")
            if shard_set:
                db.execute("DELETE FROM edges")
        db.execute("CREATE TABLE multi(id TEXT, property TEXT, value TEXT, PRIMARY KEY(id,property,value))")
        for name, path in tables:
            if name not in ("person_email_emailaddress", "person_speaks_language"):
                continue
            prop = "emails" if name == "person_email_emailaddress" else "languages"
            for header, row in csv_rows(path):
                if header != ["Person.id", "email" if prop == "emails" else "language"]:
                    raise ValueError("Unexpected multi-property header")
                db.execute("INSERT INTO multi VALUES (?,?,?)", (identity("person", row[0]), prop, row[1]))
                counts[name] += 1
        handles = {}
        succeeded = False
        try:
            for name, path in sorted(tables):
                if name not in ENTITIES:
                    continue
                if name not in handles:
                    handles[name] = shard_set.writer("vertices", name) if shard_set else (output / "vertices" / (name + ".json")).open("x", encoding="utf-8")
                    properties[name] = {"ldbcId"}
                for header, row in csv_rows(path):
                    if header[0] != "id" or len(set(header)) != len(header):
                        raise ValueError("Unexpected vertex header")
                    identifier = identity(name, row[0])
                    body = {"vertexId": identifier, "ldbcId": int(row[0])}
                    for key, value in zip(header[1:], row[1:]):
                        properties[name].add(key)
                        if value != "":
                            body[key] = typed(key, value)
                    if name == "person":
                        for prop in ("emails", "languages"):
                            values = [r[0] for r in db.execute("SELECT value FROM multi WHERE id=? AND property=? ORDER BY value", (identifier, prop))]
                            if values:
                                body[prop] = values
                            properties[name].add(prop)
                    # Message IDs share one namespace: duplicates across Post/Comment fail here.
                    db.execute("INSERT INTO vertices VALUES (?,?,?)", (identifier, name, "" if shard_set else json_line(body)))
                    handles[name].write(json_line(body))
                    counts[name] += 1
            if db.execute("SELECT count(*) FROM multi m LEFT JOIN vertices v ON m.id=v.id WHERE v.id IS NULL").fetchone()[0]:
                raise ValueError("Multi-property references a missing Person")
            succeeded = True
        finally:
            for handle in handles.values():
                if shard_set and not succeeded:
                    handle.abort()
                else:
                    handle.close()
        handles = {}
        succeeded = False
        try:
            for name, path in sorted(tables):
                if name in ENTITIES or name in ("person_email_emailaddress", "person_speaks_language"):
                    continue
                parts = name.split("_")
                if len(parts) != 3 or parts[0] not in ENTITIES or parts[2] not in ENTITIES:
                    raise ValueError("Unknown relationship table: " + name)
                source_type, _, target_type = parts
                if name not in handles:
                    handles[name] = shard_set.writer("edges", name) if shard_set else (output / "edges" / (name + ".json")).open("x", encoding="utf-8")
                    properties[name] = {"edgeKey"}
                    relations[name] = (source_type, target_type)
                for header, row in csv_rows(path):
                    if [x.lower() for x in header[:2]] != [source_type + ".id", target_type + ".id"]:
                        raise ValueError("Unexpected relationship endpoints")
                    counts[name] += 1
                    source, target = identity(source_type, row[0]), identity(target_type, row[1])
                    body = {"sourceId": source, "targetId": target, "edgeKey": counts[name]}
                    for key, value in zip(header[2:], row[2:]):
                        properties[name].add(key)
                        if value != "":
                            body[key] = typed(key, value)
                    db.execute("INSERT INTO edges VALUES (?,?,?,?)", (source, target, name, source_type + "|" + target_type if shard_set else json_line(body)))
                    pending_edges += 1
                    if shard_set and pending_edges >= 10000:
                        check_edges()
                        pending_edges = 0
                    handles[name].write(json_line(body))
            succeeded = True
        finally:
            for handle in handles.values():
                if shard_set and not succeeded:
                    handle.abort()
                else:
                    handle.close()
        check_edges()
        if not shard_set:
            db.execute("CREATE INDEX edges_source ON edges(source,label)")
            db.execute("CREATE INDEX edges_target ON edges(target,label)")
        db.commit()
    return publish_prepared(output, manifest, counts, properties, relations, shard_set)


def publish_prepared(output, manifest, counts, properties, relations, shard_set=None,
                     converter_sha256=None, include_index=True, extra=None, publish=True):
    """Shared HugeGraph mapping metadata; callers complete integrity checks before publication."""
    output = Path(output)
    loader = {"vertices": [], "edges": []}
    schema = ["// Generated HG-Bench mapping for frozen SNB v1 data. Apply only to a new empty graph."]
    for key in sorted(set().union(*properties.values())):
        kind = "asLong()" if key in NUMERIC else "asText()"
        cardinality = ".valueSet()" if key in ("emails", "languages") else ""
        schema.append(f'schema.propertyKey("{key}").{kind}{cardinality}.create();')
    args = lambda values: ", ".join(json.dumps(x) for x in sorted(values))
    for entity in sorted(ENTITIES):
        schema.append(f'schema.vertexLabel("{entity}").useCustomizeStringId().properties({args(properties[entity])}).nullableKeys({args(properties[entity] - {"ldbcId"})}).create();')
        loader["vertices"].append({"label": entity, "id": "vertexId", "input": {"type": "file", "path": "vertices/" + entity + ".json", "format": "JSON"}})
    for label, (source, target) in sorted(relations.items()):
        optional = properties[label] - {"edgeKey"}
        nullable = f'.nullableKeys({args(optional)})' if optional else ""
        schema.append(f'schema.edgeLabel("{label}").sourceLabel("{source}").targetLabel("{target}").multiTimes().properties({args(properties[label])}).sortKeys("edgeKey"){nullable}.create();')
        loader["edges"].append({"label": label, "source": ["sourceId"], "target": ["targetId"], "input": {"type": "file", "path": "edges/" + label + ".json", "format": "JSON"}})
    (output / "schema.groovy").write_text("\n".join(schema) + "\n")
    (output / "inventory.expected.json").write_text(
        json.dumps(expected_inventory(properties, relations, counts), indent=2) + "\n")
    if shard_set:
        templates = {entry["label"]: entry for group in loader.values() for entry in group}
        loader = {"vertices": [], "edges": []}
        for shard in shard_set.files:
            entry = {**templates[shard["label"]], "input": {"type": "file", "path": shard["path"], "format": "JSON"}}
            if "encoding" in shard:
                entry["input"]["compression"] = "GZIP"
            loader[shard["category"]].append(entry)
    (output / "struct.json").write_text(json.dumps(loader, indent=2) + "\n")
    # Shards were hashed while written (or checked against deterministic replay), then fsynced.
    # Reuse those receipts instead of rereading every SF100 output at publication time.
    # Loader dispatch independently rechecks the stored file identity before submission.
    file_hashes = ({shard["path"]: {"sha256": shard["sha256"], "bytes": int(shard["bytes"])}
                   for shard in shard_set.files} if shard_set else {})
    data_paths = ([] if shard_set else
                  [p for category in ("vertices", "edges") for p in (output / category).glob("*.json")])
    for path in sorted(data_paths + [output / "struct.json"]):
        digest, size = assets.digest_file(path)
        file_hashes[str(path.relative_to(output))] = {"sha256": digest, "bytes": size}
    digest, size = assets.digest_file(output / "schema.groovy")
    file_hashes["schema.groovy"] = {"sha256": digest, "bytes": size}
    digest, size = assets.digest_file(output / "inventory.expected.json")
    file_hashes["inventory.expected.json"] = {"sha256": digest, "bytes": size}
    if include_index:
        digest, size = assets.digest_file(output / "reference.sqlite")
        file_hashes["reference.sqlite"] = {"sha256": digest, "bytes": size}
    result = {"schemaVersion": 1, "scope": "initial-snapshot-loader-input", "sourceAsset": manifest["asset"],
              "counts": dict(sorted(counts.items())), "vertices": sum(counts[e] for e in ENTITIES),
              "edges": sum(counts[e] for e in relations), "danglingEdges": 0, "files": file_hashes,
              "sourceConverterSha256": converter_sha256 or assets.digest_file(__file__)[0], "hugegraphImportVerified": False}
    if shard_set:
        result.update({"scope": "sharded-loader-input", "shards": shard_set.files,
                       "reusedShards": shard_set.reused, "indexMode": "vertex-identities-and-bounded-edge-check",
                       "storedDataBytes": shard_set.stored, "uncompressedDataBytes": shard_set.total,
                       "recovery": "deterministic-replay-with-verified-shard-reuse"})
        if shard_set.compression == "gzip":
            result["schemaVersion"] = 2  # Older planners must reject, not silently strip compression metadata.
        if extra:
            result.update(extra)
        if publish:
            shards.atomic_json(output / "prepare-manifest.json", result)
    elif publish:
        (output / "prepare-manifest.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


def parse_validation(line):
    decoder = json.JSONDecoder()
    operation, end = decoder.raw_decode(line)
    if line[end:end + 1] != "|":
        raise ValueError("Invalid official validation record delimiter")
    result = json.loads(line[end + 1:])
    return operation, result


def crosscheck(prepared, validation, frozen):
    validation_manifest = verified_extraction(validation, "validation", frozen)
    prepared = Path(prepared)
    preparation = json.loads((prepared / "prepare-manifest.json").read_text())
    if preparation.get("indexMode"):
        raise ValueError("Compact sharded index is for integrity checks, not the SF0.1 answer oracle")
    if preparation["sourceAsset"] not in frozen["assets"]:
        raise ValueError("Prepared snapshot uses an unrecognized source asset")
    if preparation["sourceAsset"]["scale"] != "0.1":
        raise ValueError("Current local crosscheck is explicitly scoped to SF0.1")
    for name, facts in preparation["files"].items():
        path = prepared.joinpath(*assets.safe_member(name).parts)
        if path.is_symlink() or not path.resolve().is_relative_to(prepared.resolve()):
            raise ValueError("Prepared input path escaped its root")
        if assets.digest_file(path) != (facts["sha256"], facts["bytes"]):
            raise ValueError("Prepared input drift: " + name)
    # Rebuild this scratch SQLite index via prepare if it is missing; it is not a performance backend.
    with sqlite3.connect(f"file:{(prepared / 'reference.sqlite').resolve()}?mode=ro", uri=True) as db:
        def vertex(identifier):
            row = db.execute("SELECT body FROM vertices WHERE id=?", (identifier,)).fetchone()
            return json.loads(row[0]) if row else None

        def linked(identifier, labels):
            placeholders = ",".join("?" for _ in labels)
            rows = list(db.execute(f"SELECT target FROM edges WHERE source=? AND label IN ({placeholders})", (identifier, *labels)))
            if len(rows) != 1:
                raise ValueError("Expected exactly one reference relationship")
            return vertex(rows[0][0])

        counts, mismatches = collections.Counter(), []
        file = Path(validation) / "validation_params-sf0.1.csv"
        if file.name not in {f["path"] for f in validation_manifest["files"]}:
            raise ValueError("Missing frozen SF0.1 validation member")
        with file.open() as stream:
            for line in stream:
                operation, expected = parse_validation(line)
                counts["allOfficialRecords"] += 1
                query = next((q for q in ("personIdSQ1", "messageIdContent", "messageIdCreator") if q in operation), None)
                if query is None:
                    counts["otherOperationsNotValidated"] += 1
                    continue
                identifier = ("person:" if query == "personIdSQ1" else "message:") + str(operation[query])
                v = vertex(identifier)
                if v is None:
                    counts["requiresUpdateReplay"] += 1
                    continue
                if query == "personIdSQ1":
                    place = linked(identifier, ["person_isLocatedIn_place"])
                    actual = {k: v[k] for k in ("firstName", "lastName", "birthday", "browserUsed", "gender", "creationDate")}
                    actual.update(locationIp=v["locationIP"], cityId=place["ldbcId"])
                elif query == "messageIdContent":
                    actual = {"messageContent": v.get("content", v.get("imageFile")), "messageCreationDate": v["creationDate"]}
                else:
                    person = linked(identifier, ["post_hasCreator_person", "comment_hasCreator_person"])
                    actual = {"personId": person["ldbcId"], "firstName": person["firstName"], "lastName": person["lastName"]}
                counts[query] += 1
                if actual != expected:
                    counts["mismatches"] += 1
                    if len(mismatches) < 5:
                        mismatches.append({"operation": operation, "actual": actual, "expected": expected})
    if any(counts[q] == 0 for q in ("personIdSQ1", "messageIdContent", "messageIdCreator")):
        raise ValueError("No eligible initial-state records for one of the required checks")
    return {"scope": "official-initial-state-immutable-short-read-crosscheck", "counts": dict(counts),
            "mismatchExamples": mismatches, "fullWorkloadValidated": False, "hugegraphExecuted": False}


def main():
    def interrupted(signum, frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupted)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", default=str(assets.DEFAULT_CATALOG))
    sub = parser.add_subparsers(dest="command", required=True)
    convert = sub.add_parser("prepare", help="Offline conversion to a NEW output directory; does not connect to HugeGraph")
    convert.add_argument("--initial", required=True)
    convert.add_argument("--out", required=True)
    convert.add_argument("--shard-rows", type=int, help="Stream bounded immutable JSONL shards; does not retain graph bodies in SQLite")
    convert.add_argument("--shard-bytes", type=int, default=64 * 1024 ** 2, help="Maximum uncompressed JSONL bytes per shard (default 64 MiB)")
    convert.add_argument("--max-data-bytes", type=int, help="Required with --shard-rows: total uncompressed JSONL byte budget")
    convert.add_argument("--compression", choices=("none", "gzip"), default="none", help="Shard storage codec; gzip level 1 with fixed header, natively read by HugeGraph Loader")
    convert.add_argument("--max-stored-bytes", type=int, help="Total on-disk shard bytes including gzip headers/trailers; defaults to --max-data-bytes; excludes SQLite and metadata")
    convert.add_argument("--max-index-bytes", type=int, help="Required with --shard-rows: scratch SQLite main-file byte budget")
    convert.add_argument("--resume", action="store_true", help="Replay identical input and reuse verified sealed shards; rebuild scratch index")
    check = sub.add_parser("crosscheck", help="Compare immutable initial-state IS1/IS4/IS5 answers with the official Neo4j validation artifact")
    check.add_argument("--prepared", required=True)
    check.add_argument("--validation", required=True)
    check.add_argument("--report", required=True)
    args = parser.parse_args()
    try:
        frozen = assets.catalog(args.catalog)
        if args.command == "prepare":
            result = prepare(args.initial, args.out, frozen, args.shard_rows, args.shard_bytes, args.max_data_bytes,
                             args.max_index_bytes, args.resume, args.compression, args.max_stored_bytes)
            print(json.dumps({"ok": True, "vertices": result["vertices"], "edges": result["edges"], "hugegraphImportVerified": False}))
        else:
            result = crosscheck(args.prepared, args.validation, frozen)
            with Path(args.report).open("x") as stream:
                json.dump(result, stream, indent=2)
                stream.write("\n")
            print(json.dumps(result, indent=2))
            if result["counts"].get("mismatches", 0):
                raise SystemExit(2)
    except KeyboardInterrupt:
        print(json.dumps({"ok": False, "error": "interrupted", "sealedShardsRetained": True}))
        raise SystemExit(130)
    except Exception as error:
        print(json.dumps({"ok": False, "error": str(error)}))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
