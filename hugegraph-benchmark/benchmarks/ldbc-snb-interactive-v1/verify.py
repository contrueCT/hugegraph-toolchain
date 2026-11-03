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

"""Offline HG-Bench SNB specification and answer-evidence checks (Python standard library only)."""

import argparse
import collections
from decimal import Decimal
import hashlib
import itertools
import json
from pathlib import Path, PurePosixPath
import re
import sys
import urllib.parse

ROOT = Path(__file__).resolve().parent
REQUIRED_IDS = [f"IC{i}" for i in range(1, 15)] + [f"IS{i}" for i in range(1, 8)] + [f"IU{i}" for i in range(1, 9)]
SHA = re.compile(r"[0-9a-f]{64}")


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON field: " + key)
        result[key] = value
    return result


def reject_constant(value):
    raise ValueError("Non-finite JSON number: " + value)


DECODER = json.JSONDecoder(parse_float=Decimal, object_pairs_hook=unique_object, parse_constant=reject_constant)


def loads(text):
    return DECODER.decode(text)


def digest(path):
    value = hashlib.sha256()
    size = 0
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(block)
            size += len(block)
    return {"sha256": value.hexdigest(), "bytes": size}


def local_file(root, name):
    path = PurePosixPath(name)
    if not name or path.is_absolute() or ".." in path.parts or str(path) != name or "\\" in name:
        raise ValueError("Unsafe bundle path: " + name)
    file = root
    for part in path.parts:
        file = file / part
        if file.is_symlink():
            raise ValueError("Symlink in bundle: " + name)
    if not file.is_file():
        raise ValueError("Missing regular bundle file: " + name)
    return file


def read_tasks(root=ROOT):
    return loads((root / "tasks.json").read_text(encoding="utf-8"))


def verify_asset_lock(root, indexed):
    """Check frozen metadata consistency, without reading or claiming to reverify graph data."""
    index_path = "assets/sf100/inventories.json"
    catalog_path = "candidates/sf100.json"
    if index_path not in indexed or catalog_path not in indexed:
        raise ValueError("Missing frozen SF100 asset inventory")
    index = loads(local_file(root, index_path).read_text())
    source = local_file(root, catalog_path)
    if index["catalog"] != catalog_path or index["catalogSha256"] != digest(source)["sha256"]:
        raise ValueError("Frozen SF100 catalog identity differs")
    catalog = loads(source.read_text())
    assets = catalog["assets"]
    if (len(assets) != 3 or {a["role"] for a in assets} != {"initial", "parameters", "updates"} or
            catalog["serializer"] != "CsvBasic" or catalog["dateFormatter"] != "LongDateFormatter"):
        raise ValueError("Exactly three frozen SF100 assets required")
    for asset in assets:
        if (asset["id"] != asset["role"] + "-sf100" or asset["scale"] != "100" or
                type(asset["bytes"]) is not int or asset["bytes"] <= 0 or
                not SHA.fullmatch(str(asset.get("sha256"))) or
                urllib.parse.urlsplit(asset["url"]).scheme != "https" or
                urllib.parse.urlsplit(asset["url"]).hostname != "datasets.ldbcouncil.org"):
            raise ValueError("Invalid or unfrozen SF100 asset")
    if (catalog["driver"]["commit"] != "8c6e849a60397eaced9f3cdd08b0efa430366cb9" or
            catalog["reference"]["commit"] != "11db98cc2ba14c33492f6c0c34e68c8be7e22e5f" or
            next(a for a in assets if a["role"] == "updates")["updatePartitions"] != 1):
        raise ValueError("SF100 driver/reference/update partition identity changed")
    if len(index["inventories"]) != 3 or {v["role"] for v in index["inventories"]} != {a["role"] for a in assets}:
        raise ValueError("Missing or duplicate SF100 inventory role")
    reports = {}
    for entry in index["inventories"]:
        name = entry["path"]
        if name not in indexed or digest(local_file(root, name)) != {k: entry[k] for k in ("sha256", "bytes")}:
            raise ValueError("SF100 inventory differs from its content lock")
        report = loads(local_file(root, name).read_text())
        asset = next(a for a in assets if a["role"] == entry["role"])
        if (report["asset"] != asset or report["compressedBytesRead"] != asset["bytes"] or
                report["archiveVerification"] != "same-stream-sha256-and-length-before-publication"):
            raise ValueError("SF100 inventory asset identity mismatch")
        files = report["files"]
        if not files or len({f["path"] for f in files}) != len(files):
            raise ValueError("Duplicate or empty SF100 member inventory")
        rows, parts = collections.Counter(), collections.Counter()
        for f in files:
            path = PurePosixPath(f["path"])
            if (path.is_absolute() or ".." in path.parts or "\\" in f["path"] or
                    type(f["bytes"]) is not int or f["bytes"] < 0 or not SHA.fullmatch(f["sha256"])):
                raise ValueError("Invalid SF100 archive member metadata")
            if path.parent.name in ("static", "dynamic") and path.suffix == ".csv":
                match = re.fullmatch(r"(.+)_\d+_\d+\.csv", path.name)
                if not match or type(f.get("rows")) is not int or f["rows"] < 0:
                    raise ValueError("Invalid SF100 table member metadata")
                table = match[1]
                if table not in report["tables"] or f["columns"] != report["tables"][table]["columns"]:
                    raise ValueError("SF100 table header metadata differs")
                rows[table] += f["rows"]
                parts[table] += 1
        if set(rows) != set(report["tables"]) or any(
                rows[t] != report["tables"][t]["rows"] or parts[t] != report["tables"][t]["parts"] for t in rows):
            raise ValueError("SF100 member/table totals mismatch")
        if sum(f["bytes"] for f in files) != report["expandedBytes"]:
            raise ValueError("SF100 inventory byte totals mismatch")
        for kind, field in (("vertex", "vertices"), ("edge", "edges")):
            if sum(v["rows"] for v in report["tables"].values() if v["kind"] == kind) != report[field]:
                raise ValueError("SF100 inventory graph totals mismatch")
        reports[entry["role"]] = report
    expected_policy = {"directories": ["static", "dynamic"], "embeddedUpdates": "verify-and-exclude",
                       "executionUpdateAsset": "updates-sf100"}
    if index["initialGraphSelection"] != expected_policy:
        raise ValueError("Initial graph and benchmark update stream roles must remain distinct")
    comparisons = []
    for f in reports["initial"]["files"]:
        name = PurePosixPath(f["path"]).name
        if not re.fullmatch(r"updateStream_\d+_\d+_(person|forum)\.csv", name):
            continue
        matched = [v for v in reports["updates"]["files"] if PurePosixPath(v["path"]).name == name]
        if len(matched) != 1:
            raise ValueError("Embedded update has no unique selected counterpart")
        selected = matched[0]
        comparisons.append({"file": name, "embeddedSha256": f["sha256"], "selectedUpdatesSha256": selected["sha256"],
                            "rowsEqual": f["rows"] == selected["rows"], "bytesEqual": f["bytes"] == selected["bytes"],
                            "sha256Equal": f["sha256"] == selected["sha256"]})
    if comparisons != index["embeddedUpdateComparison"]:
        raise ValueError("Embedded update comparison differs from frozen member identities")
    return {"catalogFrozen": True, "archiveCount": 3, "memberCount": sum(len(r["files"]) for r in reports.values()),
            "contentReverifiedByThisCommand": False, "referenceAnswersFrozen": False,
            "updateSemanticCompatibilityVerified": False, "embeddedUpdateComparison": comparisons}


def verify_spec(root=ROOT):
    root = Path(root)
    lock = loads((root / "bundle.lock.json").read_text(encoding="utf-8"))
    files = lock["files"]
    if not files or len({v["path"] for v in files}) != len(files):
        raise ValueError("Empty or duplicate bundle inventory")
    indexed = {v["path"]: v for v in files}
    for name in ["tasks.json", "spec.md", "scoring-policy.json", "scorecard.schema.json",
                 "calibration-plan.json", "release.json", "verify.py", "reference/sources.lock.json"]:
        if name not in indexed:
            raise ValueError("Required file is not locked: " + name)
    for entry in files:
        if digest(local_file(root, entry["path"])) != {k: entry[k] for k in ("sha256", "bytes")}:
            raise ValueError("Bundle checksum mismatch: " + entry["path"])
    sources = loads((root / "reference/sources.lock.json").read_text())
    seen = set()
    pins = {"ldbc/ldbc_snb_interactive_v1_impls": "11db98cc2ba14c33492f6c0c34e68c8be7e22e5f",
            "ldbc/ldbc_snb_interactive_v1_driver": "8c6e849a60397eaced9f3cdd08b0efa430366cb9"}
    for entry in sources["files"]:
        name = "reference/" + entry["path"]
        if name in seen or entry["commit"] != pins.get(entry["repository"]):
            raise ValueError("Duplicate or unexpected upstream source")
        seen.add(name)
        if name not in indexed or any(indexed[name][k] != entry[k] for k in ("sha256", "bytes")):
            raise ValueError("Upstream source differs from bundle lock: " + name)
    catalog = read_tasks(root)
    if lock["schemaVersion"] != 1 or lock["id"] != catalog["id"] or lock["version"] != catalog["version"]:
        raise ValueError("Bundle identity differs from task catalog")
    tasks = catalog["tasks"]
    if catalog["scale"] != "100" or catalog["operationCount"] != 29 or [t["id"] for t in tasks] != REQUIRED_IDS:
        raise ValueError("Expected SF100 and all 29 unique ordered operations")
    if len({t["driverClass"] for t in tasks}) != 29 or len({t["driverType"] for t in tasks}) != 29:
        raise ValueError("Duplicate driver operation identity")
    for task in tasks:
        for key in ["referenceQuery", "operationSource", "resultSource"]:
            if task[key] is not None and task[key] not in seen:
                raise ValueError("Operation references an unlocked upstream source")
        if not task["parameters"] or (task["family"] != "update" and not task["resultFields"]):
            raise ValueError("Missing parameter/result contract: " + task["id"])
    policy = loads((root / "scoring-policy.json").read_text())
    if policy["requiredOperations"] != REQUIRED_IDS or policy["aggregateScore"] is not None:
        raise ValueError("Unexpected scoring policy")
    release = loads((root / "release.json").read_text())
    if release["suiteVersion"] != catalog["version"]:
        raise ValueError("Release declaration differs from task catalog version")
    asset_lock = verify_asset_lock(root, indexed)
    # Readiness cannot be promoted by editing a JSON boolean: this implementation has no live evidence auditor.
    return {"suite": catalog["id"], "version": catalog["version"], "scale": "100", "specValid": True,
            "bundleSha256": digest(root / "bundle.lock.json")["sha256"], "files": len(files),
            "operations": len(tasks), "releaseReady": False, "performanceVerdict": "not-evaluated",
            "assetLock": asset_lock, "releaseGates": release["gates"], "officialBenchmarkClaimAllowed": False}


def integer(value, bits):
    if type(value) is not int or not -(2 ** (bits - 1)) <= value < 2 ** (bits - 1):
        raise ValueError(f"Expected exact signed {bits}-bit integer")
    return value


def typed(value, kind, catalog):
    if kind in ("long", "int"):
        return integer(value, 64 if kind == "long" else 32)
    if kind == "String":
        if type(value) is not str:
            raise ValueError("Expected string; null/missing and empty string are distinct")
        return value
    if kind == "boolean":
        if type(value) is not bool:
            raise ValueError("Expected boolean")
        return value
    if kind == "double":
        # IC14 weights are sums of 1 and 0.5. Compare exactly without float tolerance masking errors.
        if type(value) not in (int, Decimal) or value < 0 or value * 2 != int(value * 2):
            raise ValueError("Expected a nonnegative half-integer IC14 weight")
        return int(value * 2)
    if kind.startswith("Iterable<"):
        if type(value) is not list:
            raise ValueError("Expected array")
        child = kind[9:-1]
        if child == "? extends Number":
            child = "long"
        return [row(v, catalog["resultOrganizationFields"], catalog) if child == "Organization"
                else typed(v, child, catalog) for v in value]
    raise ValueError("Unsupported result type: " + kind)


def row(value, fields, catalog):
    if type(value) is not dict or set(value) != {f["name"] for f in fields}:
        raise ValueError("Missing or unexpected result fields")
    return {f["name"]: typed(value[f["name"]], f["javaType"], catalog) for f in fields}


def key(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def canonical_result(task, result, catalog):
    if task["resultShape"] == "none":
        if result is not None:
            raise ValueError("Update result must be null in HG-Bench evidence")
        return None
    values = result if task["resultShape"] == "rows" else [result]
    if type(values) is not list:
        raise ValueError("Expected result rows")
    normalized = []
    for value in values:
        item = row(value, task["resultFields"], catalog)
        for name in task["unorderedFields"]:
            # Multiset: preserve duplicates. Never turn arrays into sets to conceal extra/missing results.
            item[name] = sorted(item[name], key=key)
        normalized.append(item)
    if task["id"] == "IC14":
        weights = [v["pathWeight"] for v in normalized]
        if weights != sorted(weights, reverse=True):
            raise ValueError("IC14 paths must be ordered by descending weight")
        for item in normalized:
            ids = item["personIdsInPath"]
            if not ids or len(set(ids)) != len(ids):
                raise ValueError("IC14 must contain a nonempty simple path")
        # Canonical order only inside equal-weight ties; sequence inside a path always matters.
        normalized = sorted(normalized, key=lambda v: (-v["pathWeight"], v["personIdsInPath"]))
    return normalized


def operation_id(operation):
    if type(operation) is not dict:
        raise ValueError("Expected official operation object")
    candidates = []
    for i in range(1, 13):
        if f"personIdQ{i}" in operation:
            candidates.append(f"IC{i}")
    for i in (13, 14):
        if f"person1IdQ{i}StartNode" in operation:
            candidates.append(f"IC{i}")
    for i, field in enumerate(["personIdSQ1", "personIdSQ2", "personIdSQ3", "messageIdContent",
                               "messageIdCreator", "messageForumId", "messageRepliesId"], 1):
        if field in operation:
            candidates.append(f"IS{i}")
    signatures = {"IU1": {"personFirstName"}, "IU2": {"personId", "postId"},
                  "IU3": {"personId", "commentId"}, "IU4": {"forumTitle"}, "IU5": {"joinDate"},
                  "IU6": {"postId", "authorPersonId"}, "IU7": {"commentId", "replyToPostId"},
                  "IU8": {"person1Id", "person2Id"}}
    candidates.extend(name for name, fields in signatures.items() if fields <= operation.keys())
    if len(candidates) != 1:
        raise ValueError("Unknown/ambiguous official operation")
    return candidates[0]


def parse_reference(raw):
    text = raw.decode("utf-8")
    operation, end = DECODER.raw_decode(text)
    if text[end:end + 1] != "|":
        raise ValueError("Expected operation JSON | result JSON")
    return operation_id(operation), hashlib.sha256(text[:end].encode("utf-8")).hexdigest(), loads(text[end + 1:])


def lines(path, hasher, max_line_bytes):
    with Path(path).open("rb") as stream:
        while True:
            raw = stream.readline(max_line_bytes + 1)
            if not raw:
                return
            if len(raw) > max_line_bytes:
                raise ValueError("Record exceeds max-line-bytes; do not truncate it")
            hasher.update(raw)
            if not raw.strip():
                raise ValueError("Blank records are not allowed")
            yield raw


def check_answers(reference, actual, reference_sha256, root=ROOT, max_line_bytes=4 * 1024 ** 2):
    if not SHA.fullmatch(reference_sha256) or max_line_bytes < 1:
        raise ValueError("Expected SHA-256 and positive line budget")
    catalog = read_tasks(root)
    tasks = {t["id"]: t for t in catalog["tasks"]}
    expected_hash, actual_hash = hashlib.sha256(), hashlib.sha256()
    counts = {name: collections.Counter(required=0, matched=0, wrong=0, incomplete=0, updateAcknowledged=0)
              for name in REQUIRED_IDS}
    examples = []
    pairs = itertools.zip_longest(lines(reference, expected_hash, max_line_bytes),
                                 lines(actual, actual_hash, max_line_bytes))
    for sequence, (expected_raw, actual_raw) in enumerate(pairs, 1):
        if expected_raw is None:
            raise ValueError("Unexpected extra actual record")
        name, request_sha256, expected = parse_reference(expected_raw)
        count = counts[name]
        count["required"] += 1
        task = tasks[name]
        # Validate reference answers too; a malformed oracle must not become a correctness failure in the SUT.
        if task["family"] == "update" and expected != "-1":
            raise ValueError("Expected pinned driver's serialized no-result marker: string -1")
        expected_normalized = None if task["family"] == "update" else canonical_result(task, expected, catalog)
        if actual_raw is None:
            count["incomplete"] += 1
            continue
        actual_row = loads(actual_raw.decode("utf-8"))
        fields = {"sequence", "operationId", "requestSha256", "status", "result", "committed"}
        if type(actual_row) is not dict or set(actual_row) != fields:
            raise ValueError("Unexpected actual record schema")
        if (type(actual_row["sequence"]) is not int or actual_row["sequence"] != sequence or
                actual_row["operationId"] != name or actual_row["requestSha256"] != request_sha256):
            raise ValueError("Reordered, duplicated or mismatched operation evidence")
        if actual_row["status"] not in ("success", "timeout", "error", "unsupported", "cancelled"):
            raise ValueError("Unknown actual status")
        if type(actual_row["committed"]) is not bool:
            raise ValueError("committed must be boolean")
        if actual_row["status"] != "success":
            count["incomplete"] += 1
            continue
        if task["family"] == "update":
            if actual_row["committed"] and actual_row["result"] is None:
                count["updateAcknowledged"] += 1
            else:
                count["incomplete"] += 1
            continue
        if actual_row["committed"]:
            raise ValueError("Read operation cannot be a committed update")
        try:
            matched = canonical_result(task, actual_row["result"], catalog) == expected_normalized
        except ValueError:
            matched = False
        count["matched" if matched else "wrong"] += 1
        if not matched and len(examples) < 10:
            examples.append({"sequence": sequence, "operationId": name, "reason": "answer-mismatch"})
    if expected_hash.hexdigest() != reference_sha256:
        raise ValueError("Reference SHA-256 mismatch")
    missing = [name for name, value in counts.items() if value["required"] == 0]
    wrong = sum(v["wrong"] for v in counts.values())
    incomplete = sum(v["incomplete"] for v in counts.values())
    outcome = "correctness-failed" if wrong else "incomplete" if missing or incomplete else "answers-match"
    return {"scope": "ordered-answer-evidence", "outcome": outcome, "counts": counts,
            "missingOperations": missing, "mismatchExamples": examples,
            "referenceSha256": expected_hash.hexdigest(), "actualSha256": actual_hash.hexdigest(),
            "updateStateVerified": False, "fullWorkloadValidated": False,
            "datasetIdentityVerified": False, "referenceScale": None,
            "performanceVerdict": "not-evaluated", "officialBenchmarkClaimAllowed": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT, help="Suite directory (default: next to this script)")
    sub = parser.add_subparsers(dest="command", required=True)
    inspect = sub.add_parser("verify", help="Check locked files and source identities; no network or database access")
    inspect.add_argument("--require-release", action="store_true",
                         help="Exit 3 while real release prerequisites remain")
    describe = sub.add_parser("questions", help="List 29 tasks, or inspect one complete parameter/result contract")
    describe.add_argument("--id", choices=REQUIRED_IDS)
    sub.add_parser("schema", help="Print the answer scorecard JSON Schema after verifying the bundle")
    answers = sub.add_parser("answers", help="Stream-check ordered evidence against frozen official-format validation")
    answers.add_argument("--reference", type=Path, required=True)
    answers.add_argument("--reference-sha256", required=True,
                         help="SHA-256 from the independently reviewed reference manifest")
    answers.add_argument("--actual", type=Path, required=True,
                         help="NDJSON observations; see spec.md; never reads saved verdicts")
    answers.add_argument("--max-line-bytes", type=int, default=4 * 1024 ** 2)
    args = parser.parse_args()
    try:
        verified = verify_spec(args.root)
        code = 0
        if args.command == "verify":
            result = verified
            if args.require_release and not result["releaseReady"]:
                code = 3
        elif args.command == "questions":
            tasks = read_tasks(args.root)["tasks"]
            result = next(t for t in tasks if t["id"] == args.id) if args.id else [
                {k: t[k] for k in ("id", "title", "family", "resultShape")} for t in tasks]
        elif args.command == "schema":
            result = loads((args.root / "scorecard.schema.json").read_text())
        else:
            result = check_answers(args.reference, args.actual, args.reference_sha256, args.root, args.max_line_bytes)
            if result["outcome"] != "answers-match":
                code = 10
        print(json.dumps({"schemaVersion": 1, "ok": True, "command": args.command, "data": result}, ensure_ascii=False))
        return code
    except (OSError, ValueError, KeyError, TypeError, StopIteration) as error:
        failure = {"schemaVersion": 1, "ok": False, "error": {"code": "INVALID_EVIDENCE", "message": str(error)}}
        print(json.dumps(failure))
        return 2


if __name__ == "__main__":
    sys.exit(main())
