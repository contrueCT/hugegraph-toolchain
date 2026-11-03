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

import copy
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("inventory", ROOT / "tools/ldbc-inventory.py")
i = importlib.util.module_from_spec(spec)
spec.loader.exec_module(i)


class InventoryTests(unittest.TestCase):
    def setUp(self):
        self.expected = {"schemaVersion": 1, "scope": "public-ldbc-initial-graph", "schema": {
            "propertyKeys": {"ldbcId": {"dataType": "LONG", "cardinality": "SINGLE"}},
            "vertexLabels": {"person": {"idStrategy": "CUSTOMIZE_STRING", "ttl": "0"}},
            "edgeLabels": {"person_knows_person": {"frequency": "MULTIPLE", "sortKeys": ["edgeKey"]}},
            "indexLabels": {}}, "counts": {"vertices": {"person": "9007199254740993"},
                                           "edges": {"person_knows_person": "1"}}}

    def test_schema_properties_counts_and_extra_labels_are_not_ignored(self):
        self.assertEqual(i.compare(self.expected, copy.deepcopy(self.expected)), {"schemaMatch": True, "countsMatch": True})
        for path, value in [(("counts", "vertices", "person"), "9007199254740992"),
                            (("counts", "vertices", "person"), 9007199254740993),
                            (("counts", "vertices", "unexpected"), "1"),
                            (("schema", "indexLabels", "extra"), {}),
                            (("schema", "vertexLabels", "person", "ttl"), "60"),
                            (("schema", "edgeLabels", "person_knows_person", "sortKeys"), []),
                            (("schema", "propertyKeys", "ldbcId", "dataType"), "DOUBLE")]:
            changed = copy.deepcopy(self.expected)
            target = changed
            for key in path[:-1]:
                target = target[key]
            target[path[-1]] = value
            self.assertFalse(all(i.compare(self.expected, changed).values()), path)
        for bad in (True, 1.0, "1"):
            changed = copy.deepcopy(self.expected)
            changed["schemaVersion"] = bad
            with self.assertRaises(ValueError):
                i.compare(self.expected, changed)

    def test_expected_inventory_is_bound_to_prepared_hash_and_all_shards(self):
        with tempfile.TemporaryDirectory() as temp:
            base = Path(temp)
            raw = i.r.encoded(self.expected)
            (base / "inventory.expected.json").write_bytes(raw)
            prepared = {"files": {"inventory.expected.json": {"sha256": i.r.digest(raw), "bytes": len(raw)}},
                "shards": [{"category": "vertices", "label": "person", "records": "9007199254740993"},
                           {"category": "edges", "label": "person_knows_person", "records": "1"}],
                "counts": {"person": 9007199254740993, "person_knows_person": 1},
                "vertices": 9007199254740993, "edges": 1}
            def save():
                value = i.r.encoded(prepared)
                file = base / "prepare-manifest.json"
                file.write_bytes(value)
                return file, i.r.digest(value)
            file, sha = save()
            self.assertEqual(i.load_expected(file, sha)[0], self.expected)
            with self.assertRaises(ValueError):
                i.load_expected(file, "0" * 64)
            prepared["shards"][0]["records"] = "9007199254740992"
            file, sha = save()
            with self.assertRaises(ValueError):
                i.load_expected(file, sha)
            prepared["shards"][0]["records"] = "9007199254740993"
            file, sha = save()
            (base / "inventory.expected.json").write_text("{}")
            with self.assertRaises(ValueError):
                i.load_expected(file, sha)

    def test_server_receives_no_expected_values_and_epoch_or_expiry_fail(self):
        owner = self
        class Server:
            def post(self, body):
                owner.assertEqual(body["bindings"], {"graphName": "unit", "epochMarker": "epoch"})
                return [{"graphName": "unit", "epochMarker": "epoch", "inventory": copy.deepcopy(owner.expected)}], "a"*64
        with patch.object(i.r, "lease_check", return_value=("unit", "epoch")):
            result, actual = i.observe({}, {}, self.expected, Server(), "g")
            self.assertTrue(result["matches"])
            self.assertEqual(actual, self.expected)
        with patch.object(i.r, "lease_check", side_effect=[("unit", "epoch"), ValueError("expired")]):
            with self.assertRaisesRegex(ValueError, "expired"):
                i.observe({}, {}, self.expected, Server(), "g")
        with patch.object(i.r, "lease_check", return_value=("another", "epoch")):
            class WrongTarget:
                def post(self, body):
                    return [{"graphName": "unit", "epochMarker": "epoch", "inventory": owner.expected}], "a"*64
            with self.assertRaisesRegex(ValueError, "Target"):
                i.observe({}, {}, self.expected, WrongTarget(), "g")

    def paged_server(self, mutation=None):
        owner = self
        class Server:
            calls = 0
            def post(self, body):
                self.calls += 1
                bindings = body['bindings']
                owner.assertEqual(set(bindings), {'graphName', 'epochMarker', 'category', 'page', 'pageSize'})
                category, page = bindings['category'], bindings['page']
                row = {'graphName': 'unit', 'epochMarker': 'epoch'}
                if category == 'schema':
                    row.update(kind='schema', schema=copy.deepcopy(owner.expected['schema']))
                else:
                    count = '2' if category == 'vertices' and not page else '1'
                    row.update(kind='page', category=category, page=page, scanned=count,
                               nextPage='next' if category == 'vertices' and not page else None,
                               counts={'person' if category == 'vertices' else 'person_knows_person': count})
                if mutation:
                    mutation(row, self.calls)
                return [row], 'a' * 64
        return Server()

    def test_paged_scan_keeps_all_counts_and_evidence_without_sending_expected_counts(self):
        expected = copy.deepcopy(self.expected)
        expected['counts']['vertices']['person'] = '3'
        ledger = io.BytesIO()
        with patch.object(i.r, 'lease_check', return_value=('unit', 'epoch')):
            report, actual = i.observe_paged({}, {}, expected, self.paged_server(), 'g', 2, 10, ledger, 100000)
        self.assertTrue(report['matches'])
        self.assertEqual(expected, actual)
        self.assertEqual({'vertices': 2, 'edges': 1}, report['pages'])
        rows = [json.loads(line) for line in ledger.getvalue().splitlines()]
        self.assertEqual([1, 2, 3, 4, 5], [row['sequence'] for row in rows])
        self.assertEqual(len(ledger.getvalue()), report['ledgerBytes'])

    def test_paged_scan_rejects_cursor_cycle_bad_counts_epoch_and_schema_drift(self):
        cases = [
            (lambda row, n: row.update(nextPage='next') if n == 3 else None, 'cursor repeated'),
            (lambda row, n: row.update(page='other') if n == 2 else None, 'identity'),
            (lambda row, n: row.update(scanned='3') if n == 2 else None, 'page limit'),
            (lambda row, n: row.update(counts={'person': '3'}) if n == 2 else None, 'exceeds scanned'),
            (lambda row, n: row.update(epochMarker='changed') if n == 2 else None, 'epoch'),
            (lambda row, n: row['schema']['indexLabels'].update(extra={}) if n == 5 else None, 'Schema changed'),
        ]
        for mutation, error in cases:
            with self.subTest(error=error), patch.object(i.r, 'lease_check', return_value=('unit', 'epoch')):
                with self.assertRaisesRegex(ValueError, error):
                    i.observe_paged({}, {}, self.expected, self.paged_server(mutation), 'g', 2, 10, io.BytesIO(), 100000)

    def test_paged_scan_has_no_partial_success_at_page_evidence_or_lease_limit(self):
        for pages, budget, error in [(1, 100000, 'terminal cursor'), (10, 1, 'evidence budget')]:
            with patch.object(i.r, 'lease_check', return_value=('unit', 'epoch')):
                with self.assertRaisesRegex(ValueError, error):
                    i.observe_paged({}, {}, self.expected, self.paged_server(), 'g', 2, pages, io.BytesIO(), budget)
        with patch.object(i.r, 'lease_check', side_effect=[('unit', 'epoch')] * 4 + [ValueError('expired')]):
            with self.assertRaisesRegex(ValueError, 'expired'):
                i.observe_paged({}, {}, self.expected, self.paged_server(), 'g', 2, 10, io.BytesIO(), 100000)


if __name__ == "__main__":
    unittest.main()
