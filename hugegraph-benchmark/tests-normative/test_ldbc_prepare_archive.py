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

import gzip
import importlib.util
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]

def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result

c = module('hg_archive_converter', ROOT / 'tools/ldbc-prepare-archive.py')
unit = module('archive_unit_data', ROOT / 'tests-normative/test_ldbc_reference_prepare.py')
LIMITS = {'shard_rows': 1, 'shard_bytes': 4096, 'max_data_bytes': 1024**2,
          'max_stored_bytes': 1024**2, 'max_index_bytes': 1024**2, 'max_total_bytes': 4*1024**2,
          'reserve_free_bytes': 0, 'max_seconds': 30}


def run(root, source, **kwargs):
    return c.convert(*source, root / 'out', **{**LIMITS, **kwargs})


def rows(root, manifest):
    result = {}
    for shard in manifest['shards']:
        raw = (root / shard['path']).read_bytes()
        logical = gzip.decompress(raw) if shard.get('encoding') else raw
        key = shard['category'] + '/' + shard['label']
        result.setdefault(key, []).extend(json.loads(line) for line in logical.splitlines())
    return {key: sorted(values, key=lambda r: json.dumps(r, sort_keys=True)) for key, values in result.items()}


@unittest.skipUnless(shutil.which('zstd'), 'zstd required for real archive framing checks')
class ArchivePreparationTests(unittest.TestCase):
    def test_two_passes_match_existing_graph_mapping_and_remove_scratch(self):
        for reverse in (False, True):
            with self.subTest(reverse=reverse), tempfile.TemporaryDirectory() as directory:
                root = Path(directory); source = unit.fixture(root, reverse=reverse, multipart=True)
                result = run(root, source)
                catalog = json.loads(source[0].read_text())
                c.mapping.assets.extract(catalog['assets'][0], source[2], root / 'extracted', 1024**2)
                legacy = c.mapping.prepare(root / 'extracted', root / 'legacy', catalog,
                    **{k:v for k,v in LIMITS.items() if k in ('shard_rows','shard_bytes','max_data_bytes','max_index_bytes')})
                self.assertEqual(rows(root/'out', result), rows(root/'legacy', legacy))
                self.assertEqual(result['counts'], {name: legacy['counts'].get(name, 0) for name in result['counts']})
                self.assertEqual(result['archivePasses'], 2)
                self.assertEqual(result['compressedSourceBytesRead'], 2*catalog['assets'][0]['bytes'])
                self.assertEqual(result['newRawCsvCopies'], 0)
                self.assertEqual(list((root/'out').rglob('*.csv')), [])
                self.assertFalse((root/'out/reference.sqlite').exists())
                self.assertTrue(result['scratchIndexRemoved'])
                self.assertEqual(result['vertices'], 8)
                self.assertEqual(result['edges'], 6)
                expected = json.loads((root/'out/inventory.expected.json').read_text())
                self.assertIn('workFrom', expected['schema']['edgeLabels']['person_workAt_organisation']['properties'])
                self.assertEqual(result['schemaVersion'], 2)
                for name, fact in result['files'].items():
                    self.assertEqual(c.mapping.assets.digest_file(root/'out'/name), (fact['sha256'], fact['bytes']))
                self.assertEqual(run(root, source, resume=True), result)
                first = root/'out'/result['shards'][0]['path']
                first.write_bytes(b'corrupt')
                with self.assertRaisesRegex(ValueError, 'output drift'):
                    run(root, source, resume=True)

    def test_interrupted_edges_reuse_sealed_vertices_and_rebuild_index(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory); source=unit.fixture(root, reverse=True, multipart=True)
            original=c.Graph.row
            def interrupted(graph, table, header, row, edge_key):
                if table == 'person_likes_post': raise KeyboardInterrupt()
                return original(graph, table, header, row, edge_key)
            with patch.object(c.Graph, 'row', interrupted), self.assertRaises(KeyboardInterrupt):
                run(root, source)
            self.assertFalse((root/'out/prepare-manifest.json').exists())
            self.assertFalse((root/'out/.conversion.lock').exists())
            self.assertTrue(list((root/'out').glob('failure-*.json')))
            self.assertFalse(list((root/'out').rglob('*.partial')))
            resumed=run(root, source, resume=True)
            self.assertGreaterEqual(resumed['reusedShards'], 8)
            fresh=c.convert(*source,root/'fresh',**LIMITS)
            self.assertEqual(resumed['shards'], fresh['shards'])
            with self.assertRaisesRegex(ValueError, 'identity changed'):
                run(root, source, resume=True, shard_rows=2)

    def test_invalid_endpoint_type_duplicates_and_orphan_values_never_publish(self):
        cases = [
            {'graph/dynamic/person_likes_post_0_0.csv': f'Person.id|Post.id|creationDate\n{unit.PERSON}|43|5\n'},
            {'graph/dynamic/person_likes_post_0_0.csv': f'Person.id|Post.id|creationDate\n{unit.PERSON}|999|5\n'},
            {'graph/dynamic/comment_0_0.csv': 'id|creationDate|locationIP|browserUsed|content|length\n42|3|ip|Browser|x|1\n'},
            {'graph/dynamic/person_email_emailaddress_0_0.csv': 'Person.id|email\n999|x@example.test\n'},
            {'graph/dynamic/person_speaks_language_0_0.csv': f'Person.id|language\n{unit.PERSON}|en\n{unit.PERSON}|en\n'},
            {'graph/dynamic/post_hasCreator_person_0_0.csv': 'Post.id|Person.id\n42|9223372036854775808\n'},
            {'graph/dynamic/person_likes_post_0_0.csv': f'Person.id|Post.id|creationDate\n{unit.PERSON}|42|5|extra\n'},
            {'graph/dynamic/person_workAt_organisation_0_0.csv': f'Person.id|Organisation.id|workFrom\n{unit.PERSON}|9|2147483648\n'},
            {'graph/dynamic/person_likes_post_0_0.csv': f'Person.id|Post.id|creationDate\n{unit.PERSON}|42|\n'},
            {'graph/static/place_0_0.csv': 'id|name|url|type\n7|City|url|unknown\n'},
        ]
        for change in cases:
            with self.subTest(change=change), tempfile.TemporaryDirectory() as directory:
                root=Path(directory); source=unit.fixture(root, change)
                with self.assertRaises((ValueError,c.sqlite3.IntegrityError)): run(root,source)
                self.assertFalse((root/'out/prepare-manifest.json').exists())

    def test_inventory_omission_header_or_source_drift_fail_closed(self):
        for damage in ('missing-table','category','header','compressed','member','total'):
            with self.subTest(damage=damage), tempfile.TemporaryDirectory() as directory:
                root=Path(directory)
                changes = {'graph/static/tag_0_0.csv':None} if damage=='missing-table' else None
                if damage=='category': changes={'graph/static/person_speaks_language_0_0.csv':'Person.id|language\n'}
                if damage=='header': changes={'graph/dynamic/person_0_0.csv':'id|unexpected\n1|x\n'}
                source=unit.fixture(root,changes,frozen_sha='0'*64 if damage=='compressed' else None)
                inventory=json.loads(source[1].read_text())
                if damage=='member': inventory['files'][0]['sha256']='0'*64
                if damage=='total': inventory['expandedBytes']+=1
                source[1].write_text(json.dumps(inventory))
                with self.assertRaises(ValueError): run(root,source)
                self.assertFalse((root/'out/prepare-manifest.json').exists())

    def test_second_pass_revalidates_bytes_before_publication(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory); source=unit.fixture(root)
            original=c.visit
            def changed_between_passes(*args):
                original(*args)
                if args[-2]=='vertices-and-arrays':
                    path=next(source[2].glob('*.tar.zst'))
                    path.write_bytes(b'x'*path.stat().st_size)
            with patch.object(c,'visit',changed_between_passes), self.assertRaises((ValueError,c.tarfile.TarError)):
                run(root,source)
            self.assertFalse((root/'out/prepare-manifest.json').exists())

    def test_embedded_updates_are_hashed_but_never_imported_as_initial_data(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            source=unit.fixture(root,{'graph/updateStream_0_0_forum.csv':'unit-update-not-an-initial-row\n',
                                      'graph/updateStream_0_0_person.csv':'other-unit-update\n'})
            result=run(root,source)
            self.assertEqual((result['vertices'],result['edges']),(8,5))
            self.assertEqual(len(result['embeddedUpdatesExcludedFromInitialGraph']),2)
            self.assertFalse(result['updateStreamCompatibilityVerified'])
            self.assertFalse(list((root/'out').rglob('updateStream*.csv')))
            inventory=json.loads(source[1].read_text())
            next(f for f in inventory['files'] if f['path']=='graph/updateStream_0_0_forum.csv')['sha256']='0'*64
            source[1].write_text(json.dumps(inventory))
            with self.assertRaisesRegex(ValueError,'member identity drift'):
                c.convert(*source,root/'damaged',**LIMITS)
            self.assertFalse((root/'damaged/prepare-manifest.json').exists())

    def test_disk_index_wall_and_record_limits_stop_with_no_success_manifest(self):
        for budget in ({'max_data_bytes':1},{'max_stored_bytes':1},{'max_total_bytes':1},
                       {'reserve_free_bytes':2**63},{'max_seconds':0},{'shard_bytes':32}):
            with self.subTest(budget=budget),tempfile.TemporaryDirectory() as directory:
                root=Path(directory);source=unit.fixture(root)
                with self.assertRaises((ValueError,TimeoutError)):run(root,source,**budget)
                self.assertFalse((root/'out/prepare-manifest.json').exists())
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            emails='Person.id|email\n'+''.join(f'{unit.PERSON}|{i:05d}'+('x'*80)+'@example.test\n' for i in range(1000))
            source=unit.fixture(root,{'graph/dynamic/person_email_emailaddress_0_0.csv':emails})
            with self.assertRaisesRegex(c.sqlite3.DatabaseError,'full'):run(root,source,max_index_bytes=65536)
            self.assertLessEqual((root/'out/reference.sqlite').stat().st_size,65536)
            self.assertFalse((root/'out/prepare-manifest.json').exists())
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);source=unit.fixture(root)
            with patch.object(c.Budget,'check',side_effect=TimeoutError('deadline')),self.assertRaises(TimeoutError):
                run(root,source)
            self.assertFalse((root/'out/prepare-manifest.json').exists())

    def test_cli_help_and_controlled_success_are_machine_readable(self):
        helper=ROOT/'tools/ldbc-prepare-archive.py'
        help_text=subprocess.run([sys.executable,str(helper),'--help'],check=True,capture_output=True,text=True).stdout
        self.assertIn('--max-total-bytes',help_text)
        self.assertIn('Two complete verified reads',help_text)
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);source=unit.fixture(root)
            argv=[sys.executable,str(helper),'--catalog',str(source[0]),'--inventory',str(source[1]),
                  '--cache',str(source[2]),'--out',str(root/'out')]
            for name,value in LIMITS.items():argv.extend(['--'+name.replace('_','-'),str(value)])
            preview=subprocess.run([*argv,'--plan'],check=True,capture_output=True,text=True,timeout=10)
            plan=json.loads(preview.stdout)
            self.assertFalse(plan['executed'])
            self.assertEqual(plan['archivePasses'],2)
            self.assertEqual(plan['compressedBytesToRead'],2*json.loads(source[0].read_text())['assets'][0]['bytes'])
            self.assertFalse((root/'out').exists())
            output=subprocess.run(argv,check=True,capture_output=True,text=True,timeout=10)
            data=json.loads(output.stdout)
            self.assertTrue(data['ok'])
            self.assertEqual(data['vertices'],8)
            self.assertFalse(data['hugegraphImportVerified'])
            self.assertEqual(data['manifestSha256'],c.mapping.assets.digest_file(root/'out/prepare-manifest.json')[0])
            self.assertEqual(output.stderr.count('archive-pass-verified'),2)

    def test_person_array_materialization_obeys_exact_record_limit(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);source=unit.fixture(root)
            result=run(root,source)
            person=next(s for s in result['shards'] if s['label']=='person')
            size=int(person['encoding']['uncompressedBytes'])
            exact=c.convert(*source,root/'exact',**{**LIMITS,'shard_bytes':size})
            self.assertEqual(rows(root/'out',result),rows(root/'exact',exact))
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            emails='Person.id|email\n'+''.join(f'{unit.PERSON}|{i:05d}@example.test\n' for i in range(100))
            source=unit.fixture(root,{'graph/dynamic/person_email_emailaddress_0_0.csv':emails})
            with self.assertRaisesRegex(ValueError,'multi-properties exceed'):
                run(root,source,shard_bytes=512)
            self.assertFalse((root/'out/prepare-manifest.json').exists())

    def test_generated_manifest_is_consumed_by_typescript_loader_planner(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);source=unit.fixture(root)
            # Synthetic contract fixture only: exercise the planner's SF100 metadata type.
            # This retains the hand-written-unit asset ID and proves no real SF100 import.
            catalog=json.loads(source[0].read_text());catalog['assets'][0]['scale']='100'
            source[0].write_text(json.dumps(catalog))
            inventory=json.loads(source[1].read_text());inventory['asset']=catalog['assets'][0]
            source[1].write_text(json.dumps(inventory))
            result=run(root,source)
            script = r'''
import {readFile,writeFile} from 'node:fs/promises';
import {join} from 'node:path';
import {hash,fileHash} from './dist/io.js';
import {planLoaderJobs} from './dist/sf100-loader.js';
const root=process.argv[1];
const preparedFile=join(root,'prepare-manifest.json');
const prepared=JSON.parse(await readFile(preparedFile,'utf8'));
await writeFile(join(root,'unit-launcher.sh'),'#!/bin/sh\nexit 99\n');
const manifest={protocolVersion:1,kind:'sf100-component',role:'loader',id:'unit-not-a-real-loader',version:'unit',
 command:['./unit-launcher.sh'],trackedFiles:[{path:'unit-launcher.sh',sha256:await fileHash(join(root,'unit-launcher.sh'))}],
 capabilities:{scale:'100',serializer:'CsvBasic',operations:[],checkpoint:'reconcile-before-retry'}};
await writeFile(join(root,'component.json'),JSON.stringify(manifest));
const config={schemaVersion:1,planDigest:'1'.repeat(64),datasetDigest:'2'.repeat(64),
 mappingDigest:hash({schema:prepared.files['schema.groovy'],converter:prepared.sourceConverterSha256}),
 componentManifest:'component.json',componentSha256:await fileHash(join(root,'component.json')),
 target:{host:'http://example.invalid',port:8080,graph:'unit_only',epoch:'unit-epoch',topologyDigest:'3'.repeat(64)},
 schemaAlreadyApplied:true,batchSize:500,batchThreads:1,parserThreads:1,heapMiB:256,timeoutSeconds:5,
 processTimeoutMs:5000,maxLogBytes:4096};
await writeFile(join(root,'config.json'),JSON.stringify(config));
const planned=await planLoaderJobs(preparedFile,join(root,'config.json'));
if(planned.executed || planned.jobs.some(j=>j.schemaVersion!==2 || !j.encoding ||
 [...j.mapping.vertices,...j.mapping.edges][0].input.compression!=='GZIP')) throw new Error('Broken compressed plan');
console.log(JSON.stringify({jobs:planned.jobs.length,executed:planned.executed}));
'''
            planned=subprocess.run(['node','--input-type=module','-e',script,str(root/'out')],cwd=ROOT,
                                   capture_output=True,text=True,check=True,timeout=10)
            parsed=json.loads(planned.stdout)
            self.assertEqual(parsed['jobs'],len(result['shards']))
            self.assertFalse(parsed['executed'])


if __name__=='__main__':unittest.main()
