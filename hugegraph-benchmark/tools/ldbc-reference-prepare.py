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

"""Stream a frozen CsvBasic initial archive into the pinned Neo4j reference import format.

No database is started. This mapping is independent of HugeGraph's importer and queries.
Requires a verified archive inventory, explicit disk budgets and a new output directory.
"""
import argparse
from collections import Counter
from contextlib import closing
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import signal
import sqlite3
import sys
import tarfile
import time

ROOT = Path(__file__).resolve().parent.parent
UPSTREAM = ROOT / 'adapters/ldbc-reference/upstream'
_archive_spec = importlib.util.spec_from_file_location('ldbc_archive', ROOT / 'tools/ldbc-archive.py')
archive_io = importlib.util.module_from_spec(_archive_spec)
_archive_spec.loader.exec_module(archive_io)
archive_stream, validated_inputs = archive_io.archive_stream, archive_io.validated_inputs
member_path, table_of = archive_io.member_path, archive_io.table_of
MULTI = {'person_speaks_language': ('language', 'speaks'), 'person_email_emailaddress': ('email', 'email')}
MAX_ROW_BYTES = 1024 * 1024


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def read_json(path):
    raw = Path(path).read_bytes()
    return json.loads(raw), sha(raw)


def schema():
    lock, lock_sha = read_json(UPSTREAM / 'sources.lock.json')
    for entry in lock['files']:
        raw = (UPSTREAM / entry['path']).read_bytes()
        if len(raw) != entry['bytes'] or sha(raw) != entry['sha256']:
            raise ValueError('Pinned reference source drift: ' + entry['path'])
    script = (UPSTREAM / 'cypher/scripts/import-to-neo4j.sh').read_text()
    labels = {}
    for kind, label, category, table in re.findall(
            r'--(nodes|relationships)=([A-Za-z_:]+)="/import/(static|dynamic)/(\w+)\$', script):
        labels[table] = (kind, label, category)
    specs = {}
    for line in (UPSTREAM / 'cypher/scripts/headers.txt').read_text().splitlines():
        name, header = line.split()
        category, table = name.split('/')
        kind, label, expected_category = labels[table]
        if category != expected_category:
            raise ValueError('Reference import/header mismatch')
        columns = header.split('|')
        if kind == 'nodes':
            fields = ['type' if c == ':LABEL' else c.split(':', 1)[0] for c in columns]
            basic = [f for f in fields if table != 'person' or f not in ('speaks', 'email')]
        else:
            fields = [re.search(r'\((\w+)\)', c)[1] + '.id' if c.startswith((':START_ID', ':END_ID'))
                      else c.split(':', 1)[0] for c in columns]
            basic = fields
        specs[table] = {'category': category, 'kind': kind, 'label': label,
                        'columns': columns, 'fields': fields, 'basic': basic}
    if len(specs) != len(labels) or not specs:
        raise ValueError('Incomplete reference schema')
    return specs, {'commit': lock['commit'], 'sourceLockSha256': lock_sha}



def integer(value, bits=64, nonnegative=False):
    if not re.fullmatch(r'-?(0|[1-9][0-9]*)', value):
        raise ValueError('Non-canonical integer: ' + value[:80])
    number = int(value)
    if not -(2 ** (bits - 1)) <= number < 2 ** (bits - 1) or nonnegative and number < 0:
        raise ValueError('Integer outside range')
    return number


def encode_row(values):
    def quote(value):
        if any(c in value for c in ('|', '"', '\r', '\n')):
            return '"' + value.replace('"', '""') + '"'
        return value
    return ('|'.join(map(quote, values)) + '\n').encode('utf-8')


class Budget:
    def __init__(self, path, data_bytes, reserve_bytes, seconds):
        if data_bytes <= 0 or reserve_bytes < 0 or seconds <= 0:
            raise ValueError('Invalid conversion budget')
        self.path, self.limit, self.reserve = Path(path), data_bytes, reserve_bytes
        self.used, self.deadline, self.next_space = 0, time.monotonic() + seconds, 0

    def check(self, added=0):
        now = time.monotonic()
        if now >= self.deadline:
            raise TimeoutError('Reference conversion wall budget exhausted')
        if self.used + added > self.limit:
            raise ValueError('Reference CSV output byte budget exhausted')
        if now >= self.next_space:
            disk = os.statvfs(self.path)
            if disk.f_bavail * disk.f_frsize < self.reserve:
                raise ValueError('Reference conversion free-space reserve reached')
            self.next_space = now + 0.2
        if added:
            self.used += added


class Writer:
    def __init__(self, root, table, spec, budget):
        self.path = Path(spec['category']) / (table + '_0_0.csv')
        target = root / self.path
        target.parent.mkdir(exist_ok=True)
        self.handle, self.digest, self.size, self.rows = target.open('xb'), hashlib.sha256(), 0, 0
        self.budget = budget
        try:
            self.write(spec['columns'], header=True)
        except BaseException:
            self.handle.close()
            raise

    def write(self, values, header=False):
        raw = encode_row(values)
        self.budget.check(len(raw))
        self.handle.write(raw)
        self.digest.update(raw)
        self.size += len(raw)
        if not header:
            self.rows += 1

    def seal(self):
        self.budget.check()
        self.handle.flush()
        os.fsync(self.handle.fileno())
        self.budget.check()
        self.handle.close()

    def close(self):
        self.handle.close()

    def receipt(self):
        return {'path': str(self.path), 'sha256': self.digest.hexdigest(), 'bytes': self.size, 'rows': self.rows}



def mapped_values(table, spec, header, row):
    # Positional access preserves repeated names such as Person.id|Person.id on KNOWS.
    if len(set(spec['basic'])) != len(spec['basic']):
        if header != spec['basic']:
            raise ValueError('Relationship endpoint header order differs: ' + table)
        values = row
    else:
        if len(set(header)) != len(header) or set(header) != set(spec['basic']):
            raise ValueError('Unexpected CsvBasic fields: ' + table)
        fields = dict(zip(header, row))
        values = [fields[f] for f in spec['basic']]
    for column, value in zip(spec['columns'], values):
        if ':ID(' in column or column.startswith((':START_ID', ':END_ID')):
            integer(value, nonnegative=True)
        elif column.endswith((':LONG', ':INT')):
            integer(value, 32 if column.endswith(':INT') else 64)
        elif column == ':LABEL':
            mapping = ({'city': 'City', 'country': 'Country', 'continent': 'Continent'} if table == 'place'
                       else {'company': 'Company', 'university': 'University'})
            if value not in mapping:
                raise ValueError('Unknown reference subtype label: ' + value)
            values[spec['columns'].index(column)] = mapping[value]
    return values


def consume_member(source, entry, table, spec, db, writers, counts, budget):
    digest, size, rows, header = hashlib.sha256(), 0, 0, None
    if table is None:
        for raw in iter(lambda: source.read(1024 * 1024), b''):
            budget.check()
            digest.update(raw)
            size += len(raw)
    else:
        for raw in iter(lambda: source.readline(MAX_ROW_BYTES + 1), b''):
            budget.check()
            if len(raw) > MAX_ROW_BYTES or b'\0' in raw:
                raise ValueError('Oversized or NUL-containing CSV row')
            digest.update(raw)
            size += len(raw)
            line = raw.decode('utf-8').removesuffix('\n').removesuffix('\r')
            if '\r' in line:
                raise ValueError('Embedded CR requires an unsupported multiline CSV field')
            row = line.split('|')
            if header is None:
                header = row
                if header != entry.get('columns'):
                    raise ValueError('CSV header differs from inventory')
                expected = ['Person.id', MULTI[table][0]] if table in MULTI else spec['basic']
                if sorted(header) != sorted(expected):
                    raise ValueError('Unexpected CsvBasic header: ' + table)
                continue
            if len(row) != len(header):
                raise ValueError('CSV row width mismatch: ' + table)
            if table in MULTI:
                if header != ['Person.id', MULTI[table][0]]:
                    raise ValueError('Unexpected multi-property header')
                person = integer(row[0], nonnegative=True)
                if not row[1] or ';' in row[1]:
                    raise ValueError('Empty or ambiguous array element')
                db.execute('INSERT INTO multi VALUES (?,?,?)', (person, MULTI[table][1], row[1]))
            else:
                values = mapped_values(table, spec, header, row)
                if table == 'person':
                    db.execute('INSERT INTO person VALUES (?,?)',
                               (integer(values[0], nonnegative=True), json.dumps(values, ensure_ascii=False)))
                else:
                    writers[table].write(values)
            rows += 1
            if rows % 100000 == 0:
                print(json.dumps({'event': 'reference-conversion-progress', 'table': table, 'rowsInMember': rows}),
                      file=sys.stderr, flush=True)
        if header is None or rows != entry.get('rows'):
            raise ValueError('CSV row count differs from inventory')
        counts[table] += rows
    if size != entry['bytes'] or digest.hexdigest() != entry['sha256']:
        raise ValueError('Archive member identity drift: ' + entry['path'])


def convert(catalog_path, inventory_path, cache, output, max_output_bytes, max_index_bytes,
            reserve_free_bytes, max_seconds):
    catalog, catalog_sha = read_json(catalog_path)
    inventory, inventory_sha = read_json(inventory_path)
    asset, members = validated_inputs(catalog, inventory)
    specs, reference = schema()
    if catalog.get('reference', {}).get('commit') != reference['commit']:
        raise ValueError('Reference implementation commit differs from frozen catalog')
    present = set()
    embedded = archive_io.embedded_updates(members)
    for name in members:
        path = member_path(name)
        table = table_of(path)
        if table:
            if table not in specs and table not in MULTI:
                raise ValueError('Unmapped graph table: ' + table)
            category = 'dynamic' if table in MULTI else specs[table]['category']
            if path.parent.name != category:
                raise ValueError('Graph table category mismatch')
            present.add(table)
        elif path.suffix == '.csv' and name not in embedded:
            raise ValueError('Unmapped CSV member: ' + name)
    if present != set(specs) | set(MULTI):
        raise ValueError('Incomplete reference input table set')
    if max_index_bytes < 65536:
        raise ValueError('Reference scratch index budget is too small')
    output = Path(output)
    output.mkdir()
    budget = Budget(output, max_output_bytes, reserve_free_bytes, max_seconds)
    state = {'status': 'incomplete', 'catalogSha256': catalog_sha, 'inventorySha256': inventory_sha,
             'converterSha256': sha(Path(__file__).read_bytes()),
             'archiveReaderSha256': sha(Path(archive_io.__file__).read_bytes()), 'reference': reference,
             'limits': {'maxCsvBytes': max_output_bytes, 'maxIndexBytes': max_index_bytes,
                        'reserveFreeBytes': reserve_free_bytes, 'maxSeconds': max_seconds}}
    (output / 'conversion-state.json').write_text(json.dumps(state, indent=2) + '\n')
    writers, counts, seen = {}, Counter(), set()
    index = output / 'person-arrays.sqlite'
    try:
        with closing(sqlite3.connect(index)) as db:
            db.execute('PRAGMA journal_mode=OFF')
            db.execute('PRAGMA synchronous=OFF')
            db.execute('PRAGMA cache_size=-32768')
            db.execute('PRAGMA temp_store=FILE')
            page_size = db.execute('PRAGMA page_size').fetchone()[0]
            db.execute('PRAGMA max_page_count=' + str(max_index_bytes // page_size))
            db.execute('CREATE TABLE person(id INTEGER PRIMARY KEY, body TEXT NOT NULL)')
            db.execute('CREATE TABLE multi(person INTEGER, field TEXT, value TEXT, '
                       'PRIMARY KEY(person,field,value)) WITHOUT ROWID')
            for table, spec in specs.items():
                writers[table] = Writer(output, table, spec, budget)
            with archive_stream(Path(cache) / (asset['sha256'] + '.tar.zst'), asset, budget) as stream:
                with tarfile.open(fileobj=stream, mode='r|', ignore_zeros=True) as archive:
                    for item in archive:
                        if item.isdir():
                            if item.name.rstrip('/') not in ('', '.'):
                                member_path(item.name)
                            continue
                        path = member_path(item.name)
                        name = str(path)
                        if not item.isfile() or name not in members or name in seen:
                            raise ValueError('Unexpected or duplicate archive member: ' + name)
                        entry = members[name]
                        if item.size != entry['bytes']:
                            raise ValueError('Archive member size differs from inventory')
                        seen.add(name)
                        table = table_of(path)
                        with archive.extractfile(item) as source:
                            consume_member(source, entry, table, specs.get(table), db, writers, counts, budget)
                        db.commit()
            if seen != set(members):
                raise ValueError('Archive missing inventoried members')
            if db.execute('SELECT 1 FROM multi m LEFT JOIN person p ON m.person=p.id '
                          'WHERE p.id IS NULL LIMIT 1').fetchone():
                raise ValueError('Multi-property references missing Person')
            for person, body in db.execute('SELECT id,body FROM person ORDER BY id'):
                values = json.loads(body)
                for field in ('speaks', 'email'):
                    values.append(';'.join(r[0] for r in db.execute(
                        'SELECT value FROM multi WHERE person=? AND field=? ORDER BY value', (person, field))))
                writers['person'].write(values)
            peak_index_bytes = index.stat().st_size
        for writer in writers.values():
            writer.seal()
        index.unlink()
        imports = [f'--{specs[t]["kind"]}={specs[t]["label"]}=/import/{w.path}' for t, w in writers.items()]
        files = [w.receipt() for w in writers.values()]
        manifest = {**state, 'schemaVersion': 1, 'status': 'converted-not-imported', 'inputAsset': asset,
                    'memberCount': len(seen), 'counts': dict(sorted(counts.items())), 'files': files,
                    'csvBytes': sum(f['bytes'] for f in files), 'scratchIndexBytesBeforeCleanup': peak_index_bytes,
                    'sourceVerification': 'same-stream-compressed-and-all-member-sha256',
                    'referenceImportArguments': ['neo4j-admin', 'import', '--id-type=INTEGER', *imports,
                        '--delimiter=|', '--array-delimiter=;', '--input-encoding=UTF-8',
                        '--legacy-style-quoting=false', '--trim-strings=false', '--ignore-extra-columns=false',
                        '--skip-bad-relationships=false', '--skip-duplicate-nodes=false', '--bad-tolerance=0',
                        '--multiline-fields=false'],
                    'referenceImporterVersion': '4.4.24-source-options-checked-runtime-pending',
                    'executionReady': False,
                    'arrayDelimiter': ';', 'databaseImported': False, 'endpointsValidated': False,
                    'embeddedUpdatesExcludedFromInitialGraph': list(embedded.values()),
                    'updateStreamCompatibilityVerified': False,
                    'allNodeIdsValidatedUnique': False, 'referenceAnswersGenerated': False,
                    'requiredAfterImport': ['strict importer with zero skipped nodes or relationships',
                        'unique Message.id across Post and Comment', 'full label/relationship counts',
                        'full mapped property and typed endpoint checks', 'pinned official driver and ordered answers']}
        temporary = output / 'reference-manifest.json.tmp'
        temporary.write_text(json.dumps(manifest, indent=2) + '\n')
        temporary.replace(output / 'reference-manifest.json')
        return manifest
    except BaseException as error:
        (output / 'failure.json').write_text(json.dumps({'ok': False, 'error': str(error),
             'usableImportManifestPublished': False, 'partialOutputRetained': True}) + '\n')
        raise
    finally:
        for writer in writers.values():
            writer.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--catalog', required=True, help='Frozen SF100 content lock (no network access)')
    parser.add_argument('--inventory', required=True, help='Verified initial-archive inventory from ldbc-assets inspect')
    parser.add_argument('--cache', required=True, help='Directory containing SHA256.tar.zst assets')
    parser.add_argument('--out', required=True, help='New output directory; existing directories are never overwritten')
    parser.add_argument('--max-output-bytes', type=int, required=True, help='Total converted CSV byte budget')
    parser.add_argument('--max-index-bytes', type=int, required=True, help='Bounded Person/array scratch SQLite budget')
    parser.add_argument('--reserve-free-bytes', type=int, required=True, help='Filesystem free-space reserve')
    parser.add_argument('--max-seconds', type=int, required=True, help='Total wall limit; use an outer cgroup/timeout too')
    args = parser.parse_args()
    def stop(signum, frame):
        raise TimeoutError('Reference conversion cancelled or wall limit reached')
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGALRM, stop)
    try:
        if args.max_seconds <= 0:
            raise ValueError('Wall limit must be positive')
        signal.alarm(args.max_seconds)
        result = convert(args.catalog, args.inventory, args.cache, args.out, args.max_output_bytes,
                         args.max_index_bytes, args.reserve_free_bytes, args.max_seconds)
        print(json.dumps({'ok': True, 'manifest': str(Path(args.out) / 'reference-manifest.json'),
                          'csvBytes': result['csvBytes'], 'databaseImported': False}))
    except (OSError, ValueError, TimeoutError, KeyError, TypeError, sqlite3.Error, tarfile.TarError) as error:
        print(json.dumps({'ok': False, 'error': str(error)}))
        raise SystemExit(1)
    finally:
        signal.alarm(0)


if __name__ == '__main__':
    main()
