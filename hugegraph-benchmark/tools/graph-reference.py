#!/usr/bin/env python3
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
"""Independent directed Person/knows reference from locked full SF100 CSV.

No SUT requests. Build once, export frozen pools repeatedly. Resource limits stop
with a failed receipt; an incomplete reference search never becomes an unreachable answer.
"""
import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import random
import shutil
import sqlite3
import stat
import time

ASSET_SHA = '9116c6ae0f634076eb56881e8f93cdb64e751726e66d644a5b6e6ab76fb16dac'
VERTICES, EDGES = 282637871, 1775513741

def digest(path, check=lambda: None):
    h = hashlib.sha256()
    with Path(path).open('rb') as f:
        for b in iter(lambda: f.read(1048576), b''):
            check(); h.update(b)
    return h.hexdigest()

def save(path, value):
    with Path(path).open('x', encoding='utf-8') as f:
        json.dump(value, f, ensure_ascii=False, indent=2)
        f.write('\n')

def output_bytes(root):
    # SQLite unlinks Unix sort files while their descriptors remain open. Count
    # them too; directory entries alone miss their space until the sort closes.
    files = {}
    for path in root.iterdir():
        try:
            value = path.stat()
            if stat.S_ISREG(value.st_mode): files[(value.st_dev, value.st_ino)] = value.st_size
        except FileNotFoundError: pass
    descriptors = Path('/proc/self/fd')
    if descriptors.exists():
        for path in descriptors.iterdir():
            try:
                target = os.readlink(path)
                if not target.endswith(' (deleted)') or Path(target[:-10]).parent != root: continue
                value = path.stat()
                if stat.S_ISREG(value.st_mode): files[(value.st_dev, value.st_ino)] = value.st_size
            except FileNotFoundError: pass
    return sum(files.values())

class Budget:
    def __init__(self, root, args):
        self.root, self.args, self.end = root, args, time.monotonic() + args.max_seconds
        self.next = 0
        self.initial = output_bytes(root)
        self.error = None
    def check(self):
        now = time.monotonic()
        if now > self.end: raise ValueError('Whole-task deadline reached')
        if now < self.next: return
        self.next = now + .2
        if shutil.disk_usage(self.root).free < self.args.min_free_bytes: raise ValueError('Free-space reserve reached')
        if output_bytes(self.root) - self.initial > self.args.max_bytes: raise ValueError('Output budget reached')
    def progress(self):
        # Exceptions escaping a SQLite callback become an opaque "interrupted".
        try: self.check(); return 0
        except Exception as error: self.error = error; return 1

def connect(path):
    db = sqlite3.connect(path)
    try:
        # This standalone, single-connection CLI owns SQLite's process-global
        # temporary directory. Some SQLite builds cache environment variables
        # at import: setting SQLITE_TMPDIR inside index() can be too late.
        folder = str(Path(path).resolve().parent)
        db.execute("PRAGMA temp_store_directory='" + folder.replace("'", "''") + "'")
        actual = db.execute('PRAGMA temp_store_directory').fetchone()
        if actual != (folder,): raise ValueError('SQLite cannot bind temporary files to the output budget')
        db.execute('PRAGMA cache_size=-65536')
        db.execute('PRAGMA temp_store=FILE')
    except BaseException:
        db.close(); raise
    return db

def finish_index(db, budget):
    budget.check()
    db.execute('CREATE INDEX edge_target ON edge(target,source)')
    for column in ['source', 'target']:
        if db.execute('SELECT 1 FROM edge e LEFT JOIN person p ON p.id=e.' + column + ' WHERE p.id IS NULL LIMIT 1').fetchone(): raise ValueError('Dangling reference edge')
    db.execute('CREATE TABLE degree AS SELECT p.id, count(e.target) AS n FROM person p LEFT JOIN edge e ON e.source=p.id GROUP BY p.id')
    db.execute('CREATE INDEX degree_count ON degree(n,id)')
    db.commit(); budget.check()

def ingest(db, path, receipt, kind, budget):
    h, size, rows = hashlib.sha256(), 0, 0
    def lines():
        nonlocal size
        with path.open('rb') as f:
            for raw in f:
                budget.check()
                if len(raw) > 1048576: raise ValueError('CSV row bound exceeded')
                h.update(raw); size += len(raw)
                yield raw.decode('utf-8')
    reader = csv.reader(lines(), delimiter='|', strict=True)
    header = next(reader)
    expected = ['id:ID(Person)'] if kind == 'person' else [':START_ID(Person)', ':END_ID(Person)']
    if header[:len(expected)] != expected: raise ValueError('Reference CSV header mismatch')
    for row in reader:
        if len(row) != len(header) or any(not s.isdigit() or str(int(s)) != s or int(s) > 2**63-1 for s in row[:len(expected)]): raise ValueError('Invalid CSV identity or row width')
        ids = ['person:' + s for s in row[:len(expected)]]
        if kind == 'person': db.execute('INSERT INTO person VALUES (?)', ids)
        else:
            if ids[0] == ids[1]: raise ValueError('Self-edge unsupported by frozen one-hop semantics')
            db.execute('INSERT INTO edge VALUES (?,?)', ids)
        rows += 1
        if rows % 10000 == 0: db.commit()
    db.commit()
    if (rows, size, h.hexdigest()) != (receipt['rows'], receipt['bytes'], receipt['sha256']): raise ValueError('CSV count/content lock differs')
    return {'path': receipt['path'], 'rows': rows, 'bytes': size, 'sha256': h.hexdigest()}

def index(args):
    out = Path(args.out).resolve(); out.mkdir(parents=True, exist_ok=False)
    budget = Budget(out, args)
    manifest_path = Path(args.manifest).resolve()
    manifest = json.loads(manifest_path.read_text())
    if manifest['inputAsset']['sha256'] != ASSET_SHA or manifest['inputAsset']['scale'] != '100': raise ValueError('Requires locked official SF100 initial asset')
    hg = Path(args.hg_manifest).resolve()
    hg_manifest = json.loads(hg.read_text())
    # Counts are bound to the full converted graph; this tool verifies only the
    # independent Person/knows projection, never the loaded target database.
    if hg_manifest.get('vertices') != VERTICES or hg_manifest.get('edges') != EDGES or hg_manifest.get('sourceAsset', {}).get('sha256') != ASSET_SHA or hg_manifest.get('danglingEdges') != 0: raise ValueError('HG manifest must declare the full SF100 vertex/edge counts')
    receipts = []
    os.environ['SQLITE_TMPDIR'] = str(out)
    db = connect(out / 'adjacency.sqlite')
    db.set_progress_handler(budget.progress, 10000)
    try:
        db.execute('CREATE TABLE person(id TEXT PRIMARY KEY) WITHOUT ROWID')
        db.execute('CREATE TABLE edge(source TEXT, target TEXT, PRIMARY KEY(source,target)) WITHOUT ROWID')
        for name in ['person', 'person_knows_person']:
            matches = [r for r in manifest['files'] if r['path'] == 'dynamic/' + name + '_0_0.csv']
            if len(matches) != 1: raise ValueError('Missing unique reference CSV')
            r = matches[0]
            root = Path(args.data).resolve(); path = (root / r['path']).resolve()
            if root not in path.parents: raise ValueError('CSV path escape')
            receipts.append(ingest(db, path, r, name, budget))
        finish_index(db, budget)
        save(out / 'index.json', {'schemaVersion': 1, 'status': 'complete', 'dataset': {'id': 'LDBC-SNB-Interactive-v1-SF100-initial', 'scaleFactor': 100, 'manifestSha256': digest(hg), 'vertices': VERTICES, 'edges': EDGES}, 'csvManifestSha256': digest(manifest_path), 'files': receipts, 'generatorSha256': digest(__file__), 'adjacencySha256': digest(out / 'adjacency.sqlite', budget.check), 'scope': 'Directed Person/knows projection from independent CSV; target and all properties unverified'})
    except BaseException as e:
        if budget.error is not None: e = budget.error
        save(out / 'failure.json', {'status': 'failed', 'error': str(e)})
        raise e
    finally: db.close()
    return {'index': str(out), 'status': 'complete', 'scope': 'Independent CSV Person/knows projection; target not verified', 'performanceAccepted': False}

def shortest(db, source, target, max_visited, check=lambda: None):
    # Exact <=3-hop directed joins. Check all possible witnesses; never infer
    # unreachable from a truncated search. This avoids expanding a full third
    # BFS layer for each pair while retaining every tied shortest path.
    if source == target: return 0, []
    outgoing = [r[0] for r in db.execute('SELECT target FROM edge WHERE source=? ORDER BY target', (source,))]
    incoming = {r[0] for r in db.execute('SELECT source FROM edge WHERE target=?', (target,))}
    visits = len(outgoing) + len(incoming)
    if visits > max_visited: raise ValueError('Reference search budget exhausted; no answer emitted')
    if target in outgoing: return 1, [[source, target]]
    middle = sorted(set(outgoing).intersection(incoming))
    if middle: return 2, sorted([[source, v] for v in middle] + [[v, target] for v in middle])
    edges = set()
    for u in outgoing:
        check()
        for v, in db.execute('SELECT target FROM edge WHERE source=? ORDER BY target', (u,)):
            visits += 1
            if visits > max_visited: raise ValueError('Reference search budget exhausted; no answer emitted')
            if v in incoming:
                edges.update([(source, u), (u, v), (v, target)])
                if len(edges) > 100000: raise ValueError('Reference witness bound exceeded')
    return (3, [list(e) for e in sorted(edges)]) if edges else (-1, [])

def export(args):
    root = Path(args.index).resolve()
    out = Path(args.out).resolve()
    if out.exists(): raise ValueError('Reference export exists; never overwrite')
    out.parent.mkdir(parents=True, exist_ok=True)
    budget = Budget(out.parent, args)
    meta = json.loads((root / 'index.json').read_text())
    if meta['status'] != 'complete' or digest(root / 'adjacency.sqlite', budget.check) != meta['adjacencySha256']: raise ValueError('Independent adjacency identity changed')
    db = sqlite3.connect('file:' + str(root / 'adjacency.sqlite') + '?mode=ro', uri=True)
    db.execute('PRAGMA cache_size=-65536')
    db.set_progress_handler(lambda: (budget.check() or 0), 10000)
    rng = random.Random(args.seed)
    # Deterministic reservoir per degree stratum, in lexical string-ID order.
    buckets = {s: [] for s in ['zero', 'low', 'medium', 'high']}; seen = dict.fromkeys(buckets, 0)
    for identifier, degree in db.execute('SELECT id,n FROM degree ORDER BY id'):
        budget.check()
        s = 'zero' if degree == 0 else 'low' if degree <= 10 else 'medium' if degree <= 100 else 'high'
        seen[s] += 1; b = buckets[s]
        if len(b) < args.pool_size: b.append(identifier)
        else:
            j = rng.randrange(seen[s])
            if j < len(b): b[j] = identifier
    ids, strata = [], {}
    for i in range(args.pool_size):
        for s, bucket in buckets.items():
            if i < len(bucket): ids.append(bucket[i]); strata[bucket[i]] = s
    if len(ids) < 2: raise ValueError('Insufficient independent parameters')
    ids = ids[:args.pool_size]
    neighbors = lambda v: [r[0] for r in db.execute('SELECT target FROM edge WHERE source=? ORDER BY target', (v,))]
    pools = {k: [] for k in ['point', 'neighbors', 'common', 'shortest', 'gremlin-neighbors']}
    for i, v in enumerate(ids):
        budget.check(); missing = i % 10 == 9
        point = 'hgbench-missing:' + str(args.seed) + ':' + str(i) if missing else v
        pools['point'].append({'id': 'p' + str(i), 'stratum': 'miss' if missing else 'hit-' + strata[v], 'source': point, 'rule': {'kind': 'vertex', 'id': point, 'label': 'person', 'absent': missing}})
        allowed = neighbors(v)
        if len(allowed) > 100000: raise ValueError('Reference degree exceeds declared pool bound')
        for k in ['neighbors', 'gremlin-neighbors']:
            pools[k].append({'id': 'p' + str(i), 'stratum': strata[v], 'source': v, 'rule': {'kind': 'id-set', 'pointer': '/vertices', 'allowed': allowed, 'count': min(100, len(allowed))}})
        other = ids[rng.randrange(len(ids))]
        if i % 2 == 0 and allowed:
            candidates = [x[0] for x in db.execute('SELECT source FROM edge WHERE target=? AND source<>? ORDER BY source LIMIT 1000', (allowed[0], v))]
            if candidates: other = candidates[rng.randrange(len(candidates))]
        if other == v: other = ids[(i + 1) % len(ids)]
        intersection = sorted(set(allowed).intersection(neighbors(other)))
        pools['common'].append({'id': 'p' + str(i), 'stratum': 'nonempty' if intersection else 'empty', 'source': v, 'target': other, 'rule': {'kind': 'id-set', 'pointer': '/same_neighbors', 'allowed': intersection, 'count': min(100, len(intersection))}})
        target = other
        if i % 4 < 3:
            target = v
            for _ in range(1 + i % 3):
                choices = neighbors(target)
                if not choices: break
                target = choices[rng.randrange(len(choices))]
            if target == v: target = other
        distance, edges = shortest(db, v, target, args.max_visited, budget.check)
        if len(edges) > 100000: raise ValueError('Reference shortest-path witness bound exceeded')
        pools['shortest'].append({'id': 'p' + str(i), 'stratum': 'unreachable' if distance < 0 else 'distance-' + str(distance), 'source': v, 'target': target, 'rule': {'kind': 'shortest-path', 'pointer': '/path', 'source': v, 'target': target, 'distance': distance, 'edges': edges}})
    value = {'schemaVersion': 1, 'kind': 'graph-query-reference', 'dataset': meta['dataset'], 'origin': 'independent-csv', 'provenance': {'indexManifestSha256': digest(root / 'index.json'), 'adjacencySha256': meta['adjacencySha256'], 'generatorSha256': digest(__file__), 'seed': args.seed, 'poolSize': args.pool_size, 'degreePopulation': seen, 'weights': 'round-robin nonempty degree strata; point every tenth misses; common/path strata observed, not guaranteed balanced', 'maxVisited': args.max_visited}, 'pools': pools}
    payload = json.dumps(value, ensure_ascii=False, indent=2).encode() + b'\n'
    if len(payload) > min(args.max_bytes, 3 * 1024 * 1024): raise ValueError('Reference export exceeds 3 MiB; configure a smaller pool before a new export')
    with out.open('xb') as f: f.write(payload)
    db.close()
    return {'reference': str(out), 'sha256': digest(out), 'bytes': len(payload), 'sizes': {k: len(v) for k, v in pools.items()}, 'performanceAccepted': False}

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    p = sub.add_parser('index', help='Verify the complete Person and directed knows CSV; build independent SQLite reference')
    p.add_argument('--manifest', required=True); p.add_argument('--data', required=True); p.add_argument('--hg-manifest', required=True); p.add_argument('--out', required=True)
    e = sub.add_parser('export', help='Export deterministic degree/intersection/path-stratified pools; no target requests')
    e.add_argument('--index', required=True); e.add_argument('--out', required=True); e.add_argument('--seed', type=int, default=3346); e.add_argument('--pool-size', type=int, default=128); e.add_argument('--max-visited', type=int, default=1000000)
    for x in [p, e]:
        x.add_argument('--max-seconds', type=int, default=7200); x.add_argument('--min-free-bytes', type=int, default=80 * 1024**3); x.add_argument('--max-bytes', type=int, default=8 * 1024**3)
    a = parser.parse_args()
    if a.max_seconds < 1 or a.min_free_bytes < 0 or a.max_bytes < 4096: parser.error('Invalid resource bounds')
    if a.command == 'export' and not (2 <= a.pool_size <= 100000 and 1 <= a.max_visited <= 10000000): parser.error('Invalid pool/search bounds')
    print(json.dumps(index(a) if a.command == 'index' else export(a)))

if __name__ == '__main__': main()
