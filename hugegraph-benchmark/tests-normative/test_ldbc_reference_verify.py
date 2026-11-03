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

"""Exact-verifier planning contracts, using hand-authored records only."""
import argparse
import copy
import datetime
import importlib.util
import json
from pathlib import Path
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


verify = module('reference_verify', ROOT/'tools/ldbc-reference-verify.py')
fixture = module('verify_fixture', ROOT/'tests-normative/test_ldbc_reference_import.py')
BUDGET = {'maxSeconds': 60, 'querySeconds': 10, 'reserveFreeBytes': 0, 'maxRowBytes': 1024,
          'maxBatchBytes': 4096, 'batchSize': 100, 'maxCandidatesPerRow': 4,
          'maxNodeId': 31, 'maxRelationshipId': 31}


class ReferenceVerificationPlanTest(unittest.TestCase):
    def setUp(self):
        fixture.ReferenceImportContractTest.setUp(self)
        self.facts = verify.imp.validate_manifest(self.manifest, self.catalog, self.inventory)
        self.imported = {'digest': 'a'*64, 'metadata': {'manifest': self.manifest,
                         'catalog': self.catalog, 'inventory': self.inventory},
                         'facts': self.facts, 'inputRoot': str(self.prepared)}
        self.identity = {'id': 'unit-store-id', 'name': 'neo4j', 'creationDate': 'unit-creation-date'}
        self.inspect = self.root/'inspection.json'
        self.inspect.write_text(json.dumps({'databaseInfo': self.identity, **self.facts['expected']}))
        self.lease = self.root/'lease.json'
        self.lease.write_text(json.dumps({'kind': 'reference-writer-quiescence', 'importPlanDigest': 'a'*64,
             'databaseInfo': self.identity, 'writersStopped': True, 'inflightWritesDrained': True,
             'exclusiveAccess': True, 'validUntil': (datetime.datetime.now(datetime.timezone.utc) +
             datetime.timedelta(minutes=10)).isoformat()}))
        self.budget_path = self.root/'budget.json'
        self.budget_path.write_text(json.dumps(BUDGET))
        self.args = argparse.Namespace(import_plan=self.root/'import-plan.json', inspection=self.inspect,
              lease=self.lease, budget=self.budget_path, runtime=self.root/'runtime', uri='bolt://127.0.0.1:7687',
              database='neo4j')
        self.status = {'importProcessAccepted': True, 'containerState': {'Running': False},
                       'receipt': {'unit': True}}
        self.addCleanup(patch.stopall)
        patch.object(verify.imp, 'load_plan', return_value=self.imported).start()
        patch.object(verify.imp, 'inspect_attempt', side_effect=lambda *a: self.status).start()
        patch.object(verify, 'runtime_files', return_value=({'dependencies': []}, 'b'*64)).start()

    def test_plan_covers_every_table_and_keeps_raw_identity(self):
        p = verify.plan(self.args)
        self.assertEqual(len(p['tables']), 31)
        self.assertEqual(p['expected'], {'nodes': 8, 'relationships': 5})
        self.assertEqual(p['bitmapLogicalBytes'], 8)
        self.assertEqual(p['importPlanDigest'], 'a'*64)
        self.assertEqual(p['databaseInfo'], self.identity)
        self.assertTrue(all(t['columns'] for t in p['tables']))
        self.assertFalse(p['databaseWrites'])
        self.assertFalse(p['performanceAccepted'])

    def test_unfinished_or_failed_import_is_rejected(self):
        self.status['importProcessAccepted'] = False
        with self.assertRaisesRegex(ValueError, 'accepted terminal'):
            verify.plan(self.args)
        self.status['importProcessAccepted'] = True
        self.status['containerState']['Running'] = True
        with self.assertRaisesRegex(ValueError, 'accepted terminal'):
            verify.plan(self.args)

    def test_converted_input_drift_is_rejected(self):
        self.imported['facts'] = {**self.facts, 'manifestSha256': '0'*64}
        with self.assertRaisesRegex(ValueError, 'differs from the accepted import'):
            verify.plan(self.args)

    def test_database_identity_counts_and_lease_are_bound(self):
        original = self.inspect.read_text()
        for mutate in [lambda x: x.update(nodes=9), lambda x: x['databaseInfo'].update(name='other')]:
            value = json.loads(original); mutate(value); self.inspect.write_text(json.dumps(value))
            with self.assertRaisesRegex(ValueError, 'identity/count'):
                verify.plan(self.args)
        self.inspect.write_text(original)
        original = self.lease.read_text()
        for mutate in [lambda x: x.update(exclusiveAccess=False), lambda x: x.update(importPlanDigest='0'*64),
                       lambda x: x['databaseInfo'].update(id='changed')]:
            value = json.loads(original); mutate(value); self.lease.write_text(json.dumps(value))
            with self.assertRaisesRegex(ValueError, 'lease'):
                verify.plan(self.args)

    def test_lease_must_span_the_full_window(self):
        value = json.loads(self.lease.read_text())
        value['validUntil'] = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=20)).isoformat()
        self.lease.write_text(json.dumps(value))
        with self.assertRaisesRegex(ValueError, 'complete frozen'):
            verify.plan(self.args)

    def test_bitmap_must_fit_full_graph_not_just_the_current_batch(self):
        self.budget_path.write_text(json.dumps({**BUDGET, 'maxNodeId': 1}))
        with self.assertRaisesRegex(ValueError, 'complete expected graph'):
            verify.plan(self.args)

    def test_endpoint_has_no_credentials_or_external_network(self):
        for uri in ['bolt://10.0.0.1:7687', 'neo4j://127.0.0.1:7687', 'bolt://user:pass@127.0.0.1:7687',
                    'bolt://127.0.0.1:7687/path', 'bolt://127.0.0.1:7687?x=y']:
            self.args.uri = uri
            with self.assertRaisesRegex(ValueError, 'loopback'):
                verify.plan(self.args)

    def test_budget_types_and_caps(self):
        for key, value in [('batchSize', True), ('maxCandidatesPerRow', 0), ('maxNodeId', 2**31),
                           ('maxBatchBytes', 100), ('querySeconds', 61)]:
            with self.assertRaises(ValueError):
                verify.budget({**BUDGET, key: value})
        with self.assertRaises(ValueError):
            verify.budget({**BUDGET, 'extra': 1})


if __name__ == '__main__':
    unittest.main()
