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

import csv
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile
import unittest

ROOT = Path(__file__).resolve().parent.parent

def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result

bridge = module('reference_prepare', ROOT / 'tools/ldbc-reference-prepare.py')
assets = module('reference_assets', ROOT / 'tools/ldbc-assets.py')

# Hand-authored records; these are boundary tests, not a generated or downloaded benchmark dataset.
PERSON = '9007199254740993'
ROWS = {
    'person': f'{PERSON}|Ada|Lovelace|female|-86400000|1262304000000|127.0.0.1|Browser',
    'post': '42||1262304000001|127.0.0.2|Browser|en|He said "你好"; yes|19',
    'comment': '43|1262304000002|127.0.0.3|Browser|reply|5',
    'forum': '8|Forum "title"|1262304000000',
    'place': '7|Cambridge|https://example.test|city',
    'organisation': '9|company|Org|https://example.test',
    'tag': '10|Tag|https://example.test',
    'tagclass': '11|Class|https://example.test',
    'person_speaks_language': f'{PERSON}|zh\n{PERSON}|en',
    'person_email_emailaddress': f'{PERSON}|b@example.test\n{PERSON}|a@example.test',
    'person_knows_person': f'{PERSON}|{PERSON}|1262304000003',
    'person_likes_post': f'{PERSON}|42|1262304000004\n{PERSON}|42|1262304000005',
    'comment_replyOf_post': '43|42',
    'post_hasCreator_person': f'42|{PERSON}',
}


def fixture(root, changes=None, reverse=False, multipart=False, frozen_sha=None):
    specs, _ = bridge.schema()
    content = {}
    for table, spec in specs.items():
        content[f'graph/{spec["category"]}/{table}_0_0.csv'] = '|'.join(spec['basic']) + '\n'
        if table in ROWS:
            content[f'graph/{spec["category"]}/{table}_0_0.csv'] += ROWS[table] + '\n'
    for table, (field, _) in bridge.MULTI.items():
        content[f'graph/dynamic/{table}_0_0.csv'] = 'Person.id|' + field + '\n' + ROWS[table] + '\n'
    content['graph/about.txt'] = 'hand-written fixture, not SF100\n'
    if multipart:
        content['graph/dynamic/person_likes_post_1_0.csv'] = (
            'Person.id|Post.id|creationDate\n' + PERSON + '|42|1262304000006\n')
    for name, value in (changes or {}).items():
        if value is None:
            content.pop(name)
        else:
            content[name] = value
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode='w') as archive:
        for name in sorted(content, reverse=reverse):
            body = content[name].encode()
            info = tarfile.TarInfo(name); info.size = len(body)
            archive.addfile(info, io.BytesIO(body))
    compressed = subprocess.run(['zstd', '-cq'], input=stream.getvalue(), capture_output=True, check=True).stdout
    digest = hashlib.sha256(compressed).hexdigest()
    cache = root / 'cache'; cache.mkdir()
    (cache / (digest + '.tar.zst')).write_bytes(compressed)
    asset = {'id': 'initial-hand-written-unit', 'role': 'initial', 'scale': 'unit-only',
             'bytes': len(compressed), 'sha256': digest}
    catalog = {'serializer': 'CsvBasic', 'dateFormatter': 'LongDateFormatter', 'assets': [asset],
               'reference': {'commit': '11db98cc2ba14c33492f6c0c34e68c8be7e22e5f'}}
    inv_path = root / 'inventory.json'
    assets.inspect_asset(asset, cache, inv_path, 1024 * 1024)
    if frozen_sha:
        (cache / (frozen_sha + '.tar.zst')).write_bytes(compressed)
        asset['sha256'] = frozen_sha
        inv = json.loads(inv_path.read_text()); inv['asset'] = asset; inv_path.write_text(json.dumps(inv))
    catalog_path = root / 'catalog.json'; catalog_path.write_text(json.dumps(catalog))
    return catalog_path, inv_path, cache


def run(root, source, **changes):
    args = {'max_output_bytes': 1024 * 1024, 'max_index_bytes': 1024 * 1024,
            'reserve_free_bytes': 0, 'max_seconds': 30}
    args.update(changes)
    return bridge.convert(*source, root / 'out', **args)


def output_rows(root, path):
    with (root / 'out' / path).open(newline='') as stream:
        return list(csv.reader(stream, delimiter='|'))


@unittest.skipUnless(shutil.which('zstd'), 'zstd executable required for real archive tests')
class ReferenceBridgeTests(unittest.TestCase):
    def test_embedded_updates_are_excluded_from_initial_import_and_still_verified(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            source=fixture(root, {'graph/updateStream_0_0_forum.csv':'unit-update-not-an-initial-row\n'})
            result=run(root,source)
            self.assertEqual(len(result['embeddedUpdatesExcludedFromInitialGraph']),1)
            self.assertFalse(result['updateStreamCompatibilityVerified'])
            self.assertEqual(result['counts']['person'],1)
            self.assertNotIn('updateStream', ' '.join(f['path'] for f in result['files']))
            inventory=json.loads(source[1].read_text())
            next(f for f in inventory['files'] if f['path']=='graph/updateStream_0_0_forum.csv')['sha256']='0'*64
            source[1].write_text(json.dumps(inventory))
            with self.assertRaisesRegex(ValueError,'member identity drift'):
                bridge.convert(*source,root/'damaged',max_output_bytes=1024**2,max_index_bytes=1024**2,
                               reserve_free_bytes=0,max_seconds=30)
            self.assertFalse((root/'damaged/reference-manifest.json').exists())

    def test_pinned_schema_matches_reference_importer_and_queries(self):
        specs, source = bridge.schema()
        self.assertEqual(len(specs), 31)
        self.assertEqual(specs['comment']['label'], 'Comment:Message')
        self.assertEqual(specs['place']['columns'], ['id:ID(Place)', 'name:STRING', 'url:STRING', ':LABEL'])
        self.assertEqual(specs['person_knows_person']['basic'], ['Person.id', 'Person.id', 'creationDate'])
        self.assertEqual(source['commit'], '11db98cc2ba14c33492f6c0c34e68c8be7e22e5f')

    def test_real_archive_preserves_types_quotes_arrays_multipart_edges_and_directions(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); source = fixture(root, reverse=True, multipart=True)
            result = run(root, source)
            person = output_rows(root, 'dynamic/person_0_0.csv')
            self.assertEqual(person[1], [PERSON, 'Ada', 'Lovelace', 'female', '-86400000',
                '1262304000000', '127.0.0.1', 'Browser', 'en;zh', 'a@example.test;b@example.test'])
            self.assertEqual(person[0][-2:], ['speaks:STRING[]', 'email:STRING[]'])
            self.assertEqual(output_rows(root, 'static/place_0_0.csv')[1][-1], 'City')
            self.assertEqual(output_rows(root, 'static/organisation_0_0.csv')[1][1], 'Company')
            self.assertEqual(output_rows(root, 'dynamic/post_0_0.csv')[1][6], 'He said "你好"; yes')
            self.assertEqual(output_rows(root, 'dynamic/comment_replyOf_post_0_0.csv')[1], ['43', '42'])
            self.assertEqual(output_rows(root, 'dynamic/person_knows_person_0_0.csv')[1][:2], [PERSON, PERSON])
            self.assertEqual(len(output_rows(root, 'dynamic/person_likes_post_0_0.csv')), 4)
            self.assertFalse((root / 'out/person-arrays.sqlite').exists())
            self.assertEqual(result['status'], 'converted-not-imported')
            self.assertFalse(result['databaseImported'])
            self.assertFalse(result['endpointsValidated'])
            self.assertFalse(result['executionReady'])
            self.assertIn('--legacy-style-quoting=false', result['referenceImportArguments'])
            self.assertIn('--bad-tolerance=0', result['referenceImportArguments'])
            self.assertEqual(bridge.encode_row(['a\\\"b']), b'"a\\""b"\n')
            self.assertIn('--nodes=Comment:Message=/import/dynamic/comment_0_0.csv', result['referenceImportArguments'])
            for item in result['files']:
                body = (root / 'out' / item['path']).read_bytes()
                self.assertEqual((hashlib.sha256(body).hexdigest(), len(body)), (item['sha256'], item['bytes']))
            with self.assertRaises(FileExistsError):
                run(root, source)

    def test_missing_unknown_or_wrong_category_tables_fail_before_output(self):
        changes = [
            {'graph/static/tag_0_0.csv': None},
            {'graph/dynamic/post_unknown_person_0_0.csv': 'Post.id|Person.id\n42|1\n'},
            {'graph/static/person_speaks_language_0_0.csv': 'Person.id|language\n'},
        ]
        for change in changes:
            with self.subTest(change=change), tempfile.TemporaryDirectory() as directory:
                root = Path(directory); source = fixture(root, change)
                with self.assertRaises(ValueError): run(root, source)
                self.assertFalse((root / 'out/reference-manifest.json').exists())

    def test_source_or_member_identity_drift_never_publishes_import_manifest(self):
        for damage in ('compressed', 'member', 'catalog'):
            with self.subTest(damage=damage), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                source = fixture(root, frozen_sha='0' * 64 if damage == 'compressed' else None)
                if damage == 'member':
                    inventory = json.loads(source[1].read_text()); inventory['files'][0]['sha256'] = '0' * 64
                    source[1].write_text(json.dumps(inventory))
                if damage == 'catalog':
                    catalog = json.loads(source[0].read_text()); catalog['reference']['commit'] = '0' * 40
                    source[0].write_text(json.dumps(catalog))
                with self.assertRaises(ValueError): run(root, source)
                self.assertFalse((root / 'out/reference-manifest.json').exists())

    def test_invalid_fields_duplicates_or_orphan_arrays_fail_closed(self):
        cases = [
            ('graph/dynamic/person_speaks_language_0_0.csv', f'Person.id|language\n{PERSON}|en;zh\n'),
            ('graph/dynamic/person_email_emailaddress_0_0.csv', 'Person.id|email\n123|a@example.test\n'),
            ('graph/dynamic/person_speaks_language_0_0.csv', f'Person.id|language\n{PERSON}|en\n{PERSON}|en\n'),
            ('graph/dynamic/person_likes_post_0_0.csv', f'Person.id|Post.id|creationDate\n{PERSON}|42|9223372036854775808\n'),
            ('graph/dynamic/person_likes_post_0_0.csv', f'Person.id|Post.id|creationDate\n{PERSON}|42|5|extra\n'),
            ('graph/static/place_0_0.csv', 'id|name|url|type\n7|City|url|unknown\n'),
        ]
        for name, value in cases:
            with self.subTest(name=name, value=value), tempfile.TemporaryDirectory() as directory:
                root = Path(directory); source = fixture(root, {name: value})
                with self.assertRaises((ValueError, bridge.sqlite3.IntegrityError)): run(root, source)
                self.assertFalse((root / 'out/reference-manifest.json').exists())

    def test_csv_disk_and_wall_budgets_stop_without_a_success_receipt(self):
        for budget in ({'max_output_bytes': 100}, {'max_seconds': 0}, {'reserve_free_bytes': 2 ** 63}):
            with self.subTest(budget=budget), tempfile.TemporaryDirectory() as directory:
                root = Path(directory); source = fixture(root)
                with self.assertRaises((ValueError, TimeoutError)): run(root, source, **budget)
                self.assertFalse((root / 'out/reference-manifest.json').exists())

    def test_scratch_index_bound_and_inventory_omissions_do_not_publish(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            emails = 'Person.id|email\n' + ''.join(
                f'{PERSON}|{i:05d}' + 'x' * 80 + '@example.test\n' for i in range(1000))
            source = fixture(root, {'graph/dynamic/person_email_emailaddress_0_0.csv': emails})
            with self.assertRaisesRegex(bridge.sqlite3.DatabaseError, 'full'):
                run(root, source, max_index_bytes=65536)
            self.assertLessEqual((root / 'out/person-arrays.sqlite').stat().st_size, 65536)
            self.assertFalse((root / 'out/reference-manifest.json').exists())
        for extra in (False, True):
            with self.subTest(extra=extra), tempfile.TemporaryDirectory() as directory:
                root = Path(directory); source = fixture(root)
                inventory = json.loads(source[1].read_text())
                if extra:
                    inventory['files'].append({'path': 'graph/unseen.txt', 'bytes': 0,
                                               'sha256': hashlib.sha256(b'').hexdigest()})
                else:
                    inventory['files'] = [e for e in inventory['files'] if e['path'] != 'graph/about.txt']
                source[1].write_text(json.dumps(inventory))
                with self.assertRaises(ValueError): run(root, source)
                self.assertFalse((root / 'out/reference-manifest.json').exists())

    def test_cli_help_and_controlled_success_return_machine_readable_result(self):
        help_text = subprocess.run([sys_executable(), str(ROOT / 'tools/ldbc-reference-prepare.py'), '--help'],
                                   check=True, capture_output=True, text=True).stdout
        self.assertIn('--inventory', help_text)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); catalog, inventory, cache = fixture(root)
            args = [sys_executable(), str(ROOT / 'tools/ldbc-reference-prepare.py'), '--catalog', str(catalog),
                '--inventory', str(inventory), '--cache', str(cache), '--out', str(root / 'out'),
                '--max-output-bytes', '1048576', '--max-index-bytes', '1048576',
                '--reserve-free-bytes', '0', '--max-seconds', '30']
            result = subprocess.run(args, check=True, capture_output=True, text=True)
            self.assertTrue(json.loads(result.stdout)['ok'])
            self.assertFalse(json.loads(result.stdout)['databaseImported'])


def sys_executable():
    import sys
    return sys.executable


if __name__ == '__main__':
    unittest.main()
