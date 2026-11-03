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

"""Convert a frozen SNB v1 initial archive into HugeGraph shards without expanded CSV copies.

Two complete verified reads: vertices/Person arrays, then edges with exact endpoint checks.
Requires explicit budgets. Does not start a database. Resume replays the same frozen archive.
"""
import argparse
from collections import Counter
from contextlib import closing
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import platform
import shutil
import signal
import sqlite3
import sys
import tarfile
import threading
import time
import uuid
import zlib

ROOT = Path(__file__).resolve().parent.parent

def module(name, filename):
    spec = importlib.util.spec_from_file_location(name, ROOT / 'tools' / filename)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result

mapping = module('hg_mapping', 'ldbc-prepare.py')
archive_io = module('archive_io', 'ldbc-archive.py')
shards = mapping.shards
STATIC = {'place', 'organisation', 'tag', 'tagclass'}
VERTICES = {
    'person': 'id firstName lastName gender birthday creationDate locationIP browserUsed',
    'post': 'id imageFile creationDate locationIP browserUsed language content length',
    'comment': 'id creationDate locationIP browserUsed content length',
    'forum': 'id title creationDate', 'place': 'id name url type',
    'organisation': 'id type name url', 'tag': 'id name url', 'tagclass': 'id name url',
}
EDGES = {
    'person_isLocatedIn_place': '', 'person_hasInterest_tag': '', 'person_workAt_organisation': 'workFrom',
    'person_studyAt_organisation': 'classYear', 'person_knows_person': 'creationDate',
    'forum_containerOf_post': '', 'forum_hasMember_person': 'joinDate', 'forum_hasModerator_person': '',
    'forum_hasTag_tag': '', 'person_likes_post': 'creationDate', 'person_likes_comment': 'creationDate',
    'post_hasCreator_person': '', 'post_hasTag_tag': '', 'post_isLocatedIn_place': '',
    'comment_hasCreator_person': '', 'comment_hasTag_tag': '', 'comment_isLocatedIn_place': '',
    'comment_replyOf_post': '', 'comment_replyOf_comment': '', 'tag_hasType_tagclass': '',
    'tagclass_isSubclassOf_tagclass': '', 'place_isPartOf_place': '', 'organisation_isLocatedIn_place': '',
}
MULTI = {'person_email_emailaddress': ('email', 'emails'), 'person_speaks_language': ('language', 'languages')}
MAX_ROW_BYTES = 1024**2

def property_value(table, key, value):
    result = mapping.typed(key, value)
    if key in ('length', 'workFrom', 'classYear') and not -(2**31) <= result < 2**31:
        raise ValueError('Property exceeds the SNB signed 32-bit contract: ' + key)
    if key == 'type' and table in ('place', 'organisation'):
        allowed = ('city', 'country', 'continent') if table == 'place' else ('company', 'university')
        if result not in allowed:
            raise ValueError('Unknown SNB subtype: ' + value)
    return result

def encoded(value):
    return (json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':')) + '\n').encode()

def read_json(path):
    raw = Path(path).read_bytes()
    return json.loads(raw), hashlib.sha256(raw).hexdigest()

def validate_header(table, header):
    if table in VERTICES:
        expected = VERTICES[table].split()
        valid = header[:1] == ['id'] and len(header) == len(expected) and set(header) == set(expected)
    elif table in MULTI:
        valid = header == ['Person.id', MULTI[table][0]]
    else:
        source, _, target = table.split('_')
        valid = ([v.lower() for v in header[:2]] == [source + '.id', target + '.id'] and
                 header[2:] == EDGES[table].split())
    if not valid:
        raise ValueError('Unexpected CsvBasic table header: ' + table)

def inventory_tables(inventory, members):
    counts, offsets, table_counts = {}, {}, Counter()
    embedded = archive_io.embedded_updates(members)
    for name in sorted(members):
        entry, path = members[name], archive_io.member_path(name)
        table = archive_io.table_of(path)
        if table is None:
            if path.suffix == '.csv' and name not in embedded:
                raise ValueError('CSV outside a graph table: ' + name)
            continue
        if table not in VERTICES and table not in EDGES and table not in MULTI:
            raise ValueError('Unknown graph table: ' + table)
        category = 'static' if table.split('_')[0] in STATIC else 'dynamic'
        if path.parent.name != category:
            raise ValueError('Incorrect graph table category: ' + table)
        if type(entry.get('rows')) is not int or entry['rows'] < 0 or not isinstance(entry.get('columns'), list):
            raise ValueError('Inventoried CSV header and exact row count required')
        validate_header(table, entry['columns'])
        offsets[name] = table_counts[table]
        table_counts[table] += entry['rows']
        counts[table] = counts.get(table, 0) + 1
    if set(counts) != set(VERTICES) | set(EDGES) | set(MULTI):
        raise ValueError('All 8 vertex, 23 relationship and 2 multi-property tables are required')
    if sum(f['bytes'] for f in members.values()) != inventory.get('expandedBytes'):
        raise ValueError('Inventory expanded byte count mismatch')
    if set(table_counts) != set(inventory.get('tables', {})) or any(
            table_counts[t] != inventory['tables'][t]['rows'] or counts[t] != inventory['tables'][t]['parts']
            for t in table_counts):
        raise ValueError('Inventory table totals mismatch')
    if (sum(table_counts[t] for t in VERTICES) != inventory.get('vertices') or
            sum(table_counts[t] for t in EDGES) != inventory.get('edges')):
        raise ValueError('Inventory graph totals mismatch')
    return table_counts, offsets

class Budget:
    def __init__(self, path, limits):
        self.path, self.limits = Path(path), limits
        self.deadline = time.monotonic() + limits['max_seconds']
        self.next_space, self.next_size, self.peak = 0, 0, 0
        self.owner_thread = threading.get_ident()

    def size(self):
        total = 0
        for path in self.path.rglob('*'):
            if path.is_symlink():
                raise ValueError('Symlink in conversion output')
            if path.is_file():
                total += path.stat().st_size
        self.peak = max(self.peak, total)
        return total

    def check(self, extra_bytes=0, force=False):
        now = time.monotonic()
        if now >= self.deadline:
            raise TimeoutError('Conversion wall budget exhausted')
        if threading.get_ident() != self.owner_thread:
            return  # Feeder only checks the deadline; the writer owns directory/space sampling.
        if force or now >= self.next_space:
            if shutil.disk_usage(self.path).free < self.limits['reserve_free_bytes'] + extra_bytes:
                raise ValueError('Conversion free-space reserve reached')
            self.next_space = now + 0.2
        if force or now >= self.next_size:
            if self.size() + extra_bytes > self.limits['max_total_bytes']:
                raise ValueError('Whole conversion directory byte budget exceeded')
            self.next_size = now + 5

class Graph:
    def __init__(self, db, writers, budget):
        self.db, self.writers, self.budget = db, writers, budget
        self.counts, self.properties, self.relations = Counter(), {}, {}
        self.handles, self.pending, self.processed = {}, 0, 0
        db.execute('CREATE TABLE vertices(id TEXT PRIMARY KEY, label TEXT NOT NULL) WITHOUT ROWID')
        db.execute('CREATE TABLE persons(sequence INTEGER PRIMARY KEY, id TEXT NOT NULL, body TEXT NOT NULL)')
        db.execute('CREATE TABLE multi(id TEXT, property TEXT, value TEXT, PRIMARY KEY(id,property,value)) WITHOUT ROWID')
        db.execute('CREATE TABLE pending_edges(source TEXT, target TEXT, source_label TEXT, target_label TEXT)')

    def header(self, table, header):
        self.counts[table] += 0
        if table in MULTI:
            return
        if table in VERTICES:
            keys = {'ldbcId', *header[1:]}
            if table == 'person':
                keys.update(('emails', 'languages'))
            category = 'vertices'
        else:
            keys, category = {'edgeKey', *header[2:]}, 'edges'
            source, _, target = table.split('_')
            self.relations[table] = (source, target)
        if table in self.properties and self.properties[table] != keys:
            raise ValueError('Inconsistent table columns')
        self.properties[table] = keys
        if table not in self.handles:
            self.handles[table] = self.writers.writer(category, table)

    def row(self, table, header, row, edge_key):
        self.budget.check()
        if table in MULTI:
            if not row[1]:
                raise ValueError('Empty multi-property value')
            self.db.execute('INSERT INTO multi VALUES (?,?,?)',
                            (mapping.identity('person', row[0]), MULTI[table][1], row[1]))
        elif table in VERTICES:
            identifier = mapping.identity(table, row[0])
            body = {'vertexId': identifier, 'ldbcId': int(row[0])}
            for key, value in zip(header[1:], row[1:]):
                if key in mapping.NUMERIC and value == '':
                    raise ValueError('Missing numeric property: ' + key)
                if value != '':
                    body[key] = property_value(table, key, value)
            self.db.execute('INSERT INTO vertices VALUES (?,?)', (identifier, table))
            if table == 'person':
                self.db.execute('INSERT INTO persons(id,body) VALUES (?,?)', (identifier, mapping.json_line(body)))
            else:
                self.handles[table].write(mapping.json_line(body))
        else:
            source_type, target_type = self.relations[table]
            source, target = mapping.identity(source_type, row[0]), mapping.identity(target_type, row[1])
            if edge_key > 2**63 - 1:
                raise ValueError('Synthetic edge key exceeds signed 64-bit range')
            body = {'sourceId': source, 'targetId': target, 'edgeKey': edge_key}
            for key, value in zip(header[2:], row[2:]):
                if key in mapping.NUMERIC and value == '':
                    raise ValueError('Missing numeric property: ' + key)
                if value != '':
                    body[key] = property_value(table, key, value)
            self.db.execute('INSERT INTO pending_edges VALUES (?,?,?,?)', (source, target, source_type, target_type))
            self.pending += 1
            if self.pending >= 10000:
                self.check_edges()
            self.handles[table].write(mapping.json_line(body))
        self.counts[table] += 1
        self.processed += 1
        if self.processed % 100000 == 0:
            self.db.commit()
            print(json.dumps({'event': 'conversion-progress', 'table': table,
                              'records': self.processed, 'storedShardBytes': self.writers.stored}), file=sys.stderr, flush=True)

    def persons(self):
        if self.db.execute('SELECT 1 FROM multi m LEFT JOIN vertices v ON m.id=v.id '
                           "WHERE v.id IS NULL OR v.label!='person' LIMIT 1").fetchone():
            raise ValueError('Multi-property references a missing Person')
        for identifier, raw in self.db.execute('SELECT id,body FROM persons ORDER BY sequence'):
            self.budget.check()
            body = json.loads(raw)
            record_bytes = len(raw.encode())
            for prop in ('emails', 'languages'):
                values, array_bytes = [], 2
                for value, in self.db.execute('SELECT value FROM multi WHERE id=? AND property=? ORDER BY value',
                                              (identifier, prop)):
                    array_bytes += len(json.dumps(value, ensure_ascii=False).encode()) + (1 if values else 0)
                    if record_bytes + len(prop) + 4 + array_bytes > self.writers.shard_bytes:
                        raise ValueError('Person multi-properties exceed the shard record byte limit')
                    values.append(value)
                if values:
                    record_bytes += len(prop) + 4 + array_bytes
                    body[prop] = values
            self.handles['person'].write(mapping.json_line(body))
        self.db.execute('DELETE FROM persons')
        self.db.execute('DELETE FROM multi')
        self.db.commit()

    def check_edges(self):
        if self.db.execute('SELECT 1 FROM pending_edges e LEFT JOIN vertices s ON s.id=e.source '
                           'LEFT JOIN vertices t ON t.id=e.target WHERE s.id IS NULL OR t.id IS NULL '
                           'OR s.label!=e.source_label OR t.label!=e.target_label LIMIT 1').fetchone():
            raise ValueError('Relationship endpoints are missing or have incorrect labels')
        self.db.execute('DELETE FROM pending_edges')
        self.pending = 0

    def close_category(self, category):
        for table, writer in self.handles.items():
            if writer.category == category:
                writer.close()

    def abort(self):
        for writer in self.handles.values():
            writer.abort()

def consume(source, entry, table, graph, offset, selected, budget):
    digest, size, rows = hashlib.sha256(), 0, 0
    if not selected:
        for raw in iter(lambda: source.read(1024**2), b''):
            budget.check()
            digest.update(raw)
            size += len(raw)
    else:
        header = None
        for raw in iter(lambda: source.readline(MAX_ROW_BYTES + 1), b''):
            budget.check()
            if len(raw) > MAX_ROW_BYTES or b'\0' in raw:
                raise ValueError('Oversized or NUL-containing CSV row')
            digest.update(raw)
            size += len(raw)
            line = raw.decode('utf-8').removesuffix('\n').removesuffix('\r')
            if '\r' in line:
                raise ValueError('Embedded CR is not supported in CsvBasic')
            row = line.split('|')
            if header is None:
                if row != entry['columns']:
                    raise ValueError('CSV header differs from frozen inventory')
                header = row
                validate_header(table, header)
                graph.header(table, header)
                continue
            if len(row) != len(header):
                raise ValueError('CSV width mismatch: ' + table)
            rows += 1
            if rows > entry['rows']:
                raise ValueError('CSV row count exceeds frozen inventory')
            graph.row(table, header, row, offset + rows)
        if header is None or rows != entry['rows']:
            raise ValueError('CSV row count differs from frozen inventory')
    if size != entry['bytes'] or digest.hexdigest() != entry['sha256']:
        raise ValueError('Archive member identity drift: ' + entry['path'])

def visit(asset, members, offsets, cache, graph, phase, budget):
    seen = set()
    print(json.dumps({'event': 'archive-pass-started', 'phase': phase}), file=sys.stderr, flush=True)
    with archive_io.archive_stream(Path(cache) / (asset['sha256'] + '.tar.zst'), asset, budget) as stream:
        with tarfile.open(fileobj=stream, mode='r|', ignore_zeros=True) as archive:
            for item in archive:
                if item.isdir():
                    if item.name.rstrip('/') not in ('', '.'):
                        archive_io.member_path(item.name)
                    continue
                name = str(archive_io.member_path(item.name))
                if not item.isfile() or name not in members or name in seen or item.size != members[name]['bytes']:
                    raise ValueError('Unexpected, duplicate or changed archive member: ' + name)
                seen.add(name)
                table = archive_io.table_of(archive_io.member_path(name))
                selected = table in EDGES if phase == 'edges' else table in VERTICES or table in MULTI
                with archive.extractfile(item) as source:
                    consume(source, members[name], table, graph, offsets.get(name, 0), selected, budget)
    if seen != set(members):
        raise ValueError('Archive missing inventoried members')
    print(json.dumps({'event': 'archive-pass-verified', 'phase': phase, 'members': len(seen),
                      'compressedBytes': asset['bytes']}), file=sys.stderr, flush=True)

def convert(catalog_path, inventory_path, cache, output, *, shard_rows, shard_bytes,
            max_data_bytes, max_stored_bytes, max_index_bytes, max_total_bytes,
            reserve_free_bytes, max_seconds, compression='gzip', resume=False, plan_only=False):
    limits = {k: v for k, v in locals().copy().items() if k in (
        'shard_rows', 'shard_bytes', 'max_data_bytes', 'max_stored_bytes', 'max_index_bytes',
        'max_total_bytes', 'reserve_free_bytes', 'max_seconds')}
    if any(type(v) is not int or v < (0 if k == 'reserve_free_bytes' else 1) for k, v in limits.items()):
        raise ValueError('Explicit positive limits and nonnegative free-space reserve are required')
    if max_index_bytes < 65536 or compression not in ('none', 'gzip'):
        raise ValueError('Index budget or compression is invalid')
    catalog, catalog_sha = read_json(catalog_path)
    inventory, inventory_sha = read_json(inventory_path)
    asset, members = archive_io.validated_inputs(catalog, inventory)
    expected_counts, offsets = inventory_tables(inventory, members)
    embedded = list(archive_io.embedded_updates(members).values())
    files = {Path(path).name: mapping.assets.digest_file(path)[0] for path in
             (__file__, mapping.__file__, archive_io.__file__, shards.__file__, mapping.assets.__file__)}
    converter_sha = hashlib.sha256(encoded(files)).hexdigest()
    identity = {'schemaVersion': 1, 'kind': 'archive-conversion', 'catalogSha256': catalog_sha,
                'inventorySha256': inventory_sha, 'converterFiles': files, 'limits': limits,
                'compression': compression, 'ordering': 'archive-member-order; edgeKey uses sorted member-path row offsets'}
    if compression == 'gzip':
        identity['codec'] = {'gzipLevel': 1, 'mtime': 0, 'python': platform.python_version(), 'zlib': zlib.ZLIB_RUNTIME_VERSION}
    output = Path(output)
    if plan_only:
        return {'ok': True, 'executed': False, 'scope': 'catalog-and-inventory-metadata-only',
                'conversionIdentitySha256': hashlib.sha256(encoded(identity)).hexdigest(),
                'sourceAsset': asset, 'archivePasses': 2, 'compressedBytesToRead': 2 * asset['bytes'],
                'vertices': sum(expected_counts[t] for t in VERTICES), 'edges': sum(expected_counts[t] for t in EDGES),
                'tables': len(expected_counts), 'limits': limits, 'compression': compression,
                'output': str(output), 'resume': resume, 'cacheAndOutputContentsChecked': False,
                'rawCsvCopiesToCreate': 0, 'databaseStarted': False,
                'embeddedUpdatesExcludedFromInitialGraph': embedded}
    if resume:
        if output.is_symlink() or not output.is_dir() or read_json(output / 'conversion-state.json')[0] != identity:
            raise ValueError('Conversion identity changed; use a new output directory')
    else:
        output.mkdir()
        shards.atomic_json(output / 'conversion-state.json', identity)
    lock = output / '.conversion.lock'
    with lock.open('x') as stream:
        stream.write(str(os.getpid()) + '\n')
    budget = Budget(output, limits)
    graph = None
    try:
        budget.check(force=True)
        complete = output / 'prepare-manifest.json'
        if complete.exists():
            result = read_json(complete)[0]
            if (result.get('sourceConverterSha256') != converter_sha or result.get('counts') != dict(expected_counts) or
                    result.get('sourceInventorySha256') != inventory_sha or result.get('sourceAsset') != asset):
                raise ValueError('Prepared manifest differs from frozen conversion identity')
            for name, fact in result['files'].items():
                path = output.joinpath(*archive_io.member_path(name).parts)
                if path.is_symlink() or not path.resolve().is_relative_to(output.resolve()):
                    raise ValueError('Prepared file escapes output directory')
                if mapping.assets.digest_file(path) != (fact['sha256'], fact['bytes']):
                    raise ValueError('Prepared output drift')
                budget.check()
            return result
        for category in ('vertices', 'edges'):
            directory = output / category
            if directory.is_symlink():
                raise ValueError('Symlink in shard directory')
            directory.mkdir(exist_ok=True)
        index = output / 'reference.sqlite'
        for suffix in ('', '-journal', '-wal', '-shm'):
            path = index.with_name(index.name + suffix)
            if path.is_symlink():
                raise ValueError('Symlink in scratch index')
            path.unlink(missing_ok=True)
        writers = shards.ShardSet(output, shard_rows, shard_bytes, max_data_bytes, compression, max_stored_bytes)
        with closing(sqlite3.connect(index)) as db:
            db.execute('PRAGMA journal_mode=OFF')
            db.execute('PRAGMA synchronous=OFF')
            db.execute('PRAGMA cache_size=-65536')
            db.execute('PRAGMA temp_store=MEMORY')
            page_size = db.execute('PRAGMA page_size').fetchone()[0]
            db.execute('PRAGMA max_page_count=' + str(max_index_bytes // page_size))
            graph = Graph(db, writers, budget)
            visit(asset, members, offsets, cache, graph, 'vertices-and-arrays', budget)
            graph.persons()
            graph.close_category('vertices')
            budget.check(force=True)
            visit(asset, members, offsets, cache, graph, 'edges', budget)
            graph.check_edges()
            graph.close_category('edges')
            db.commit()
            if graph.counts != expected_counts:
                raise ValueError('Converted table totals differ from frozen inventory')
        index_bytes = index.stat().st_size
        budget.check(force=True)
        index.unlink()
        extra = {'sourceConverterDigestKind': 'sha256-of-canonical-source-file-map-with-final-newline',
                 'converterFiles': files, 'inputMode': 'two-pass-frozen-archive', 'archivePasses': 2,
                 'compressedSourceBytesRead': 2 * asset['bytes'], 'sourceInventorySha256': inventory_sha,
                 'sourceCatalogSha256': catalog_sha, 'scratchIndexRemoved': True,
                 'scratchIndexBytesBeforeCleanup': index_bytes, 'newRawCsvCopies': 0,
                 'peakOutputBytesObservedBeforeMetadata': budget.peak,
                 'sourceVerification': 'both-passes-compressed-and-all-member-sha256; all-table-rows-and-headers',
                 'ordering': identity['ordering'], 'embeddedUpdatesExcludedFromInitialGraph': embedded,
                 'updateStreamCompatibilityVerified': False}
        result = mapping.publish_prepared(output, {'asset': asset}, graph.counts, graph.properties,
            graph.relations, writers, converter_sha256=converter_sha, include_index=False, extra=extra, publish=False)
        budget.check(extra_bytes=len(json.dumps(result, ensure_ascii=False, indent=2).encode()) + 1, force=True)
        shards.atomic_json(complete, result)
        return result
    except BaseException as error:
        if graph:
            graph.abort()
        # Failure files are tiny and preserved per attempt; no success receipt is manufactured.
        failure = {'ok': False, 'type': type(error).__name__, 'error': str(error)[:1024], 'sealedShardsRetained': True}
        try:
            path = output / ('failure-' + uuid.uuid4().hex + '.json')
            path.write_bytes(encoded(failure))
        except OSError:
            pass
        raise
    finally:
        lock.unlink()

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name, help_text in [('catalog', 'Frozen content catalog'), ('inventory', 'Verified initial archive inventory'),
                            ('cache', 'Read-only content-addressed tar.zst cache'), ('out', 'New output directory, or identical --resume directory')]:
        parser.add_argument('--' + name, required=True, help=help_text)
    for name, help_text in [('shard-rows', 'Maximum records per shard'), ('shard-bytes', 'Maximum uncompressed JSONL bytes per shard'),
        ('max-data-bytes', 'Total uncompressed JSONL byte budget'), ('max-stored-bytes', 'Total stored shard byte budget'),
        ('max-index-bytes', 'Scratch SQLite main-file cap; index is removed on success'),
        ('max-total-bytes', 'Whole output directory byte guard including index, shards, metadata and failures'),
        ('reserve-free-bytes', 'Shared filesystem free-space reserve'), ('max-seconds', 'Whole attempt wall budget; use an outer cgroup/service too')]:
        parser.add_argument('--' + name, type=int, required=True, help=help_text)
    parser.add_argument('--compression', choices=('none', 'gzip'), default='gzip')
    parser.add_argument('--resume', action='store_true', help='Verify complete output or replay same input and reuse sealed shards; rebuild scratch index')
    parser.add_argument('--plan', dest='plan_only', action='store_true',
                        help='Validate only catalog/inventory and print two-pass cost/limits as JSON; no archive read or output directory creation')
    args = vars(parser.parse_args())
    args['catalog_path'], args['inventory_path'], args['output'] = args.pop('catalog'), args.pop('inventory'), args.pop('out')
    def stop(signum, frame):
        raise TimeoutError('Conversion cancelled or wall budget exhausted')
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGALRM, stop)
    try:
        if args['max_seconds'] <= 0:
            raise ValueError('Wall budget must be positive')
        signal.alarm(args['max_seconds'])
        result = convert(**args)
        if args['plan_only']:
            print(json.dumps(result))
            return
        print(json.dumps({'ok': True, 'manifest': str(Path(args['output']) / 'prepare-manifest.json'),
                          'manifestSha256': mapping.assets.digest_file(Path(args['output']) / 'prepare-manifest.json')[0],
                          'vertices': result['vertices'], 'edges': result['edges'], 'storedDataBytes': result['storedDataBytes'],
                          'reusedShards': result['reusedShards'], 'hugegraphImportVerified': False}))
    except (Exception, KeyboardInterrupt) as error:
        print(json.dumps({'ok': False, 'error': str(error), 'type': type(error).__name__}))
        raise SystemExit(1)
    finally:
        signal.alarm(0)

if __name__ == '__main__':
    main()
