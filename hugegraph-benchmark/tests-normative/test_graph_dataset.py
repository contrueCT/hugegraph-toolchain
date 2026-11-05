# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import argparse
import importlib.util
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


g = module('graph_dataset_tests', ROOT / 'tools/graph-dataset.py')
unit = module('graph_dataset_archive_fixture', ROOT / 'tests-normative/test_ldbc_reference_prepare.py')


@unittest.skipUnless(shutil.which('zstd') and shutil.which('curl'), 'archive components required')
class GraphDatasetTests(unittest.TestCase):
    def test_real_archive_preparation_projection_and_metadata_only_reuse(self):
        # Hand-authored framing/semantics fixture, not an SF1 benchmark result.
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            person = '|'.join(unit.bridge.schema()[0]['person']['basic']) + '\n' + unit.ROWS['person'] + '\n'
            person += '2|Grace|Hopper|female|0|1|127.0.0.1|Browser\n'
            source = unit.fixture(root, {'graph/dynamic/person_0_0.csv': person,
                'graph/dynamic/person_knows_person_0_0.csv': 'Person.id|Person.id|creationDate\n' + unit.PERSON + '|2|1\n'})
            original = json.loads(source[0].read_text())['assets'][0]
            descriptor = g.read(ROOT / 'benchmarks/graph-query-v0.1/datasets.json')
            asset = {**original, 'id': 'initial-sf1', 'scale': '1', 'url': descriptor['datasets'][0]['asset']['url']}
            descriptor['datasets'][0]['asset'] = asset
            reader = g.read
            def read(path):
                return descriptor if Path(path).name == 'datasets.json' else reader(path)
            args = argparse.Namespace(scale=1, out=str(root/'prepared-data'), cache=str(source[2]),
                max_seconds=60, max_bytes=8*1024**2, reserve_free_bytes=0, max_download_bytes=1024**2,
                seed=3346, pool_size=2, dry_run=False, resume=False)
            with patch.object(g, 'read', side_effect=read):
                result = g.prepare(args)
                self.assertEqual(result['scaleFactor'], 1)
                self.assertEqual(result['vertices'], 9)
                self.assertFalse(result['targetImported'])
                projection = Path(args.out)/'reference-csv'
                self.assertEqual(sorted(p.name for p in projection.rglob('*.csv')),
                    ['person_0_0.csv', 'person_knows_person_0_0.csv'])
                answers = reader(Path(args.out)/'reference.json')
                self.assertEqual(answers['dataset']['scaleFactor'], 1)
                self.assertEqual(len(answers['pools']), 5)
                args.resume = True
                with patch.object(g.converter, 'convert', side_effect=AssertionError('Do not re-read completed data')):
                    self.assertTrue(g.prepare(args)['reused'])
                (Path(args.out)/'reference.json').write_text('{}')
                with self.assertRaisesRegex(ValueError, 'metadata changed'):
                    g.prepare(args)

    def test_default_source_and_dry_run_have_no_disk_or_network_effects(self):
        with tempfile.TemporaryDirectory() as temporary:
            out = Path(temporary)/'absent'
            args = argparse.Namespace(scale=1, out=str(out), cache=None, max_seconds=None, max_bytes=None,
                reserve_free_bytes=None, max_download_bytes=None, seed=3346, pool_size=128, dry_run=True, resume=False)
            with patch.object(g.assets, 'acquire_resumable', side_effect=AssertionError('No download')):
                result = g.prepare(args)
            self.assertFalse(out.exists())
            self.assertEqual(result['scaleFactor'], 1)
            self.assertEqual(result['targetRequests'], 0)
            self.assertEqual(result['descriptor']['asset']['scale'], '1')


if __name__ == '__main__':
    unittest.main()
