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
import hashlib
import importlib.util
import itertools
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import time
import unittest

spec = importlib.util.spec_from_file_location('graph_reference', Path(__file__).resolve().parents[1] / 'tools/graph-reference.py')
graph = importlib.util.module_from_spec(spec)
spec.loader.exec_module(graph)

class GraphReferenceTest(unittest.TestCase):
    def test_temp_directory_binding_and_sql_budget_error(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp).resolve() / "quoted'workspace"; root.mkdir()
            db = graph.connect(root / 'test.sqlite')
            try:
                self.assertEqual(db.execute('PRAGMA temp_store_directory').fetchone(), (str(root),))
                args = argparse.Namespace(max_seconds=30, min_free_bytes=0, max_bytes=1024)
                budget = graph.Budget(root, args); budget.end = time.monotonic() - 1
                db.set_progress_handler(budget.progress, 1)
                with self.assertRaises(sqlite3.OperationalError): db.execute('CREATE TABLE denied(x)')
                self.assertIsInstance(budget.error, ValueError)
                self.assertIn('deadline', str(budget.error))
            finally: db.close()

    @unittest.skipUnless(Path('/proc/self/fd').exists(), 'Linux open/unlinked SQLite temporary-file accounting')
    def test_unlinked_temporary_file_counts_toward_budget(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp).resolve()
            args = argparse.Namespace(max_seconds=30, min_free_bytes=0, max_bytes=4096)
            budget = graph.Budget(root, args)
            with (root / 'etilqs-test').open('w+b') as stream:
                stream.write(b'x' * 8192); stream.flush()
                self.assertEqual(graph.output_bytes(root), 8192)
                os.unlink(stream.name)
                self.assertEqual(graph.output_bytes(root), 8192)
                with self.assertRaisesRegex(ValueError, 'Output budget'): budget.check()

    def test_all_small_directed_graphs_against_exhaustive_simple_paths(self):
        # Algorithm test only; never starts or benchmarks a database/small graph.
        possible = [(a, b) for a in 'abcd' for b in 'abcd' if a != b]
        db = sqlite3.connect(':memory:')
        db.execute('CREATE TABLE edge(source TEXT,target TEXT,PRIMARY KEY(source,target))')
        db.execute('CREATE INDEX reverse_edge ON edge(target,source)')
        for mask in range(1 << len(possible)):
            edges = {e for i, e in enumerate(possible) if mask & (1 << i)}
            db.execute('DELETE FROM edge'); db.executemany('INSERT INTO edge VALUES (?,?)', sorted(edges))
            for source, target in [('a', 'd'), ('d', 'a')]:
                paths = []
                for n in range(4):
                    for inside in itertools.permutations([v for v in 'abcd' if v not in (source, target)], n):
                        p = [source, *inside, target]
                        if len(p) <= 4 and all(e in edges for e in zip(p, p[1:])): paths.append(p)
                length = min((len(p) - 1 for p in paths), default=-1)
                shortest = [p for p in paths if len(p) - 1 == length]
                expected = sorted({e for p in shortest for e in zip(p, p[1:])})
                actual, witness = graph.shortest(db, source, target, 1000)
                self.assertEqual(actual, length)
                self.assertEqual(witness, [list(e) for e in expected])
        self.assertEqual(graph.shortest(db, 'a', 'a', 1000), (0, []))
        with self.assertRaisesRegex(ValueError, 'budget'): graph.shortest(db, 'a', 'd', 1)

    def test_csv_lock_duplicates_dangling_and_deterministic_export(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp); (root / 'data/dynamic').mkdir(parents=True)
            person = b'id:ID(Person)|firstName:STRING\n1|a\n2|b\n3|c\n4|d\n'
            edge = b':START_ID(Person)|:END_ID(Person)|creationDate:LONG\n1|2|0\n1|3|0\n2|4|0\n3|4|0\n'
            files = []
            for name, raw in [('person', person), ('person_knows_person', edge)]:
                path = 'dynamic/' + name + '_0_0.csv'; (root / 'data' / path).write_bytes(raw)
                files.append({'path': path, 'rows': raw.count(b'\n') - 1, 'bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest()})
            manifest = {'inputAsset': {'sha256': graph.ASSET_SHA, 'scale': '100'}, 'files': files}
            (root / 'manifest.json').write_text(json.dumps(manifest))
            (root / 'hg.json').write_text(json.dumps({'vertices': graph.VERTICES, 'edges': graph.EDGES, 'sourceAsset': {'sha256': graph.ASSET_SHA}, 'danglingEdges': 0}))
            args = argparse.Namespace(out=str(root / 'index'), manifest=str(root / 'manifest.json'), hg_manifest=str(root / 'hg.json'), data=str(root / 'data'), max_seconds=30, min_free_bytes=0, max_bytes=10485760)
            graph.index(args)
            export = argparse.Namespace(index=args.out, out=str(root / 'reference.json'), pool_size=4, seed=3346, max_visited=100, max_seconds=30, min_free_bytes=0, max_bytes=10485760)
            graph.export(export)
            first = (root / 'reference.json').read_bytes()
            export.out = str(root / 'reference2.json'); graph.export(export)
            self.assertEqual(first, (root / 'reference2.json').read_bytes())
            args.out = str(root / 'bad'); (root / 'data' / files[1]['path']).write_bytes(edge + b'1|2|0\n')
            with self.assertRaises((ValueError, sqlite3.IntegrityError)): graph.index(args)
            self.assertTrue((root / 'bad/failure.json').exists())

if __name__ == '__main__': unittest.main()
