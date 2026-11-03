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

"""Adversarial tests for the published specification and ordered answer checker."""
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import tarfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
SUITE = ROOT / "benchmarks/ldbc-snb-interactive-v1"
spec = importlib.util.spec_from_file_location("ldbc_spec", SUITE / "verify.py")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def encoded(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def value(kind):
    if kind == "String": return "张|Unicode\n测试"
    if kind == "long": return 9223372036854775807
    if kind == "int": return 3
    if kind == "boolean": return False
    if kind == "double": return 2.5
    if kind == "Iterable<String>": return ["a", "a", "b"]
    if kind == "Iterable<? extends Number>": return [1, 9007199254740993, 9223372036854775807]
    if kind == "Iterable<Organization>":
        return [{"organizationName": "学校", "year": 2010, "placeName": "北京"}]
    raise AssertionError(kind)


def observations():
    catalog = module.read_tasks()
    expected, actual = [], []
    for sequence, task in enumerate(catalog["tasks"], 1):
        # Minimal identifiers select the official operation type. These are unit vectors, not workload parameters.
        op = {p["name"]: 1 for p in task["parameters"]}
        body = None if task["resultShape"] == "none" else {f["name"]: value(f["javaType"]) for f in task["resultFields"]}
        if task["resultShape"] == "rows": body = [body]
        text = encoded(op)
        expected.append(text + "|" + encoded("-1" if task["resultShape"] == "none" else body) + "\n")
        actual.append({"sequence": sequence, "operationId": task["id"],
                       "requestSha256": hashlib.sha256(text.encode()).hexdigest(),
                       "status": "success", "result": body, "committed": task["family"] == "update"})
    return expected, actual


class LdbcSpecificationTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="hgbench-spec-")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.reference = self.root / "reference.csv"
        self.actual = self.root / "actual.ndjson"

    def check(self, expected, actual):
        self.reference.write_text("".join(expected), encoding="utf-8")
        self.actual.write_text("".join(encoded(v) + "\n" for v in actual), encoding="utf-8")
        return module.check_answers(self.reference, self.actual, module.digest(self.reference)["sha256"])

    def test_locked_package_is_complete_and_relocation_preserves_identity(self):
        report = module.verify_spec()
        self.assertEqual(report["operations"], 29)
        self.assertFalse(report["releaseReady"])
        self.assertTrue(report["assetLock"]["catalogFrozen"])
        self.assertEqual(report["assetLock"]["memberCount"], 119)
        self.assertFalse(report["assetLock"]["contentReverifiedByThisCommand"])
        self.assertFalse(report["assetLock"]["referenceAnswersFrozen"])
        copy_root = self.root / "relocated"
        shutil.copytree(SUITE, copy_root, ignore=shutil.ignore_patterns("__pycache__"))
        self.assertEqual(module.verify_spec(copy_root), report)
        with (copy_root / "tasks.json").open("a") as stream: stream.write(" ")
        with self.assertRaisesRegex(ValueError, "checksum"):
            module.verify_spec(copy_root)

    def test_frozen_asset_metadata_rejects_unfrozen_counts_and_update_role_changes(self):
        for damage in ('unfrozen', 'counts', 'update-role', 'comparison'):
            with self.subTest(damage=damage):
                root=self.root/damage
                shutil.copytree(SUITE,root,ignore=shutil.ignore_patterns('__pycache__'))
                index_path=root/'assets/sf100/inventories.json'
                index=json.loads(index_path.read_text())
                if damage=='unfrozen':
                    path=root/'candidates/sf100.json';data=json.loads(path.read_text())
                    data['assets'][0]['sha256']=None;path.write_text(json.dumps(data))
                    index['catalogSha256']=module.digest(path)['sha256']
                elif damage=='counts':
                    entry=next(e for e in index['inventories'] if e['role']=='initial')
                    path=root/entry['path'];data=json.loads(path.read_text())
                    data['vertices']+=1;path.write_text(json.dumps(data));entry.update(module.digest(path))
                elif damage=='update-role':
                    index['initialGraphSelection']['executionUpdateAsset']='embedded-updates'
                else:
                    index['embeddedUpdateComparison'][-1]['sha256Equal']=True
                index_path.write_text(json.dumps(index))
                # Recompute byte locks so rejection proves semantic consistency, not just changed bytes.
                lock_path=root/'bundle.lock.json';lock=json.loads(lock_path.read_text())
                for entry in lock['files']:entry.update(module.digest(root/entry['path']))
                lock_path.write_text(json.dumps(lock))
                with self.assertRaises(ValueError):module.verify_spec(root)

    def test_all_29_answers_and_update_acks_are_not_live_validation(self):
        expected, actual = observations()
        report = self.check(expected, actual)
        self.assertEqual(report["outcome"], "answers-match")
        self.assertEqual(len(report["counts"]), 29)
        self.assertFalse(report["fullWorkloadValidated"])
        self.assertFalse(report["updateStateVerified"])
        self.assertEqual(report["performanceVerdict"], "not-evaluated")
        actual[0]["result"][0]["friendId"] -= 1
        self.assertEqual(self.check(expected, actual)["outcome"], "correctness-failed")

    def test_missing_failed_updates_and_unsupported_remain_in_denominator(self):
        expected, actual = observations()
        self.assertEqual(self.check(expected, actual[:-1])["outcome"], "incomplete")
        self.assertEqual(self.check(expected[:-1], actual[:-1])["missingOperations"], ["IU8"])
        for status in ["timeout", "error", "unsupported", "cancelled"]:
            changed = copy.deepcopy(actual)
            changed[0]["status"] = status
            self.assertEqual(self.check(expected, changed)["outcome"], "incomplete")
        actual[-1]["committed"] = False
        self.assertEqual(self.check(expected, actual)["outcome"], "incomplete")

    def test_identity_order_duplicates_and_oracle_corruption_are_invalid(self):
        expected, actual = observations()
        for changed in [actual + [actual[0]], [actual[1], actual[0]] + actual[2:]]:
            with self.assertRaises(ValueError): self.check(expected, changed)
        changed = copy.deepcopy(actual)
        changed[0]["requestSha256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "mismatched"):
            self.check(expected, changed)
        self.check(expected, actual)
        with self.assertRaisesRegex(ValueError, "SHA-256"):
            module.check_answers(self.reference, self.actual, "0" * 64)
        with self.assertRaisesRegex(ValueError, "max-line"):
            module.check_answers(self.reference, self.actual, module.digest(self.reference)["sha256"], max_line_bytes=10)

    def test_strict_fields_types_and_64bit_precision(self):
        expected, actual = observations()
        for field, bad in [("friendId", 9223372036854775808), ("friendId", "9223372036854775807"),
                           ("friendId", True), ("friendId", 1.0), ("friendLastName", None)]:
            changed = copy.deepcopy(actual)
            changed[0]["result"][0][field] = bad
            self.assertEqual(self.check(expected, changed)["outcome"], "correctness-failed")
        changed = copy.deepcopy(actual)
        del changed[0]["result"][0]["friendLastName"]
        self.assertEqual(self.check(expected, changed)["outcome"], "correctness-failed")
        for text in ['{"a":1,"a":2}', '{"a":NaN}', '{"a":Infinity}']:
            with self.assertRaises(ValueError): module.loads(text)

    def test_collections_preserve_multiplicity_and_row_order(self):
        catalog = module.read_tasks()
        task = catalog["tasks"][0]
        row = observations()[1][0]["result"][0]
        a = copy.deepcopy(row)
        a["friendEmails"] = ["b", "a", "a"]
        self.assertEqual(module.canonical_result(task, [row], catalog), module.canonical_result(task, [a], catalog))
        a["friendEmails"] = ["a", "b", "b"]
        self.assertNotEqual(module.canonical_result(task, [row], catalog), module.canonical_result(task, [a], catalog))
        a = copy.deepcopy(row); a["friendId"] -= 1
        self.assertNotEqual(module.canonical_result(task, [row, a], catalog), module.canonical_result(task, [a, row], catalog))

    def test_ic14_ties_allow_reordering_but_path_direction_and_weights_matter(self):
        catalog = module.read_tasks(); task = catalog["tasks"][13]
        a = {"personIdsInPath": [1, 2, 9], "pathWeight": module.Decimal("2.5")}
        b = {"personIdsInPath": [1, 3, 9], "pathWeight": module.Decimal("2.5")}
        self.assertEqual(module.canonical_result(task, [a, b], catalog), module.canonical_result(task, [b, a], catalog))
        reverse = {**a, "personIdsInPath": [9, 2, 1]}
        self.assertNotEqual(module.canonical_result(task, [a], catalog), module.canonical_result(task, [reverse], catalog))
        for bad in [{**a, "pathWeight": module.Decimal("2.50000000001")}, {**a, "personIdsInPath": [1, 2, 1]}]:
            with self.assertRaises(ValueError): module.canonical_result(task, [bad], catalog)
        with self.assertRaises(ValueError): module.canonical_result(task, [{**a, "pathWeight": 1}, b], catalog)

    def test_command_exit_codes_and_local_help(self):
        def run(*args):
            return subprocess.run(["python3", str(SUITE / "verify.py"), *args], text=True, capture_output=True)
        self.assertEqual(run("--help").returncode, 0)
        self.assertEqual(run("verify", "--require-release").returncode, 3)
        result = run("questions", "--id", "IU7")
        self.assertEqual(result.returncode, 0)
        self.assertTrue(json.loads(result.stdout)["data"]["compatibilityNotes"])
        expected, actual = observations(); self.check(expected, actual)
        args = ["answers", "--reference", str(self.reference), "--reference-sha256", module.digest(self.reference)["sha256"], "--actual", str(self.actual)]
        self.assertEqual(run(*args).returncode, 0)
        self.check(expected, actual[:-1])
        self.assertEqual(run(*args).returncode, 10)

    def test_archive_is_deterministic_relocatable_and_exclusive(self):
        outputs = [self.root / "first.tar.gz", self.root / "second.tar.gz"]
        for path in outputs:
            result = subprocess.run(["python3", str(ROOT / "tools/pack-ldbc-spec.py"), "--out", str(path)],
                                    capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(module.digest(outputs[0]), module.digest(outputs[1]))
        result = subprocess.run(["python3", str(ROOT / "tools/pack-ldbc-spec.py"), "--out", str(outputs[0])],
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 2)
        # Extract only our just-produced archive; verify every path explicitly, including on Python 3.9.
        unpacked = self.root / "unpacked"
        unpacked.mkdir()
        with tarfile.open(outputs[0]) as archive:
            for member in archive.getmembers():
                target = unpacked / member.name
                self.assertTrue(target.resolve().is_relative_to(unpacked.resolve()))
                self.assertTrue(member.isfile())
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(archive.extractfile(member).read())
        self.assertTrue(module.verify_spec(next(unpacked.iterdir()))["specValid"])


if __name__ == "__main__":
    unittest.main()
