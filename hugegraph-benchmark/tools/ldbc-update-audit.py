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

"""Audit complete frozen SNB update streams without materializing expanded assets.

Compares byte order, operation payload order and equal-scheduled-time groups.
The selected standalone stream is never replaced or reordered. These checks do
not establish database replay correctness or dependency independence of ties.
"""
import argparse
from collections import Counter
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import shutil
import struct
import tarfile
import threading
import time

spec = importlib.util.spec_from_file_location('archive_io', Path(__file__).with_name('ldbc-archive.py'))
archive_io = importlib.util.module_from_spec(spec)
spec.loader.exec_module(archive_io)
UPDATE_TYPES = {str(n).encode(): str(n) for n in range(1, 9)}


def sha(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as source:
        for raw in iter(lambda: source.read(1024 * 1024), b''):
            h.update(raw)
    return h.hexdigest()


def put(path, value):
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(value, indent=2) + '\n')
    temporary.replace(path)


class Budget:
    def __init__(self, output, timeout, reserve):
        self.output, self.deadline, self.reserve = output, time.monotonic() + timeout, reserve
        self.next_check, self.lock = 0, threading.Lock()

    def check(self):
        with self.lock:
            now = time.monotonic()
            if now < self.next_check:
                return
            self.next_check = now + 0.5
            if now > self.deadline:
                raise ValueError('Audit time budget exhausted')
            if shutil.disk_usage(self.output).free < self.reserve:
                raise ValueError('Free-disk reserve reached')


class StreamAudit:
    def __init__(self, max_group_bytes=16 * 1024 * 1024, max_line_bytes=1024 * 1024):
        self.max_group_bytes, self.max_line_bytes = max_group_bytes, max_line_bytes
        self.rows, self.bytes, self.groups = 0, 0, 0
        self.raw, self.payload_ordered = hashlib.sha256(), hashlib.sha256()
        self.grouped_raw, self.grouped_payload = hashlib.sha256(), hashlib.sha256()
        self.types, self.column_counts = Counter(), Counter()
        self.current_time, self.first_time, self.last_time = None, None, None
        self.minimum_time, self.maximum_time = None, None
        self.out_of_order, self.future_dependencies = 0, 0
        self.group, self.group_bytes = [], 0
        self.max_observed_group_bytes, self.max_observed_group_rows = 0, 0
        self.first_records = []

    def flush(self):
        if not self.group:
            return
        prefix = struct.pack('>qQ', self.current_time, len(self.group))
        self.grouped_raw.update(prefix)
        self.grouped_payload.update(prefix)
        # Keep duplicates. Separate hashes expose dependency changes versus ordering.
        for raw, _ in sorted(self.group):
            self.grouped_raw.update(raw)
        for payload in sorted(item[1] for item in self.group):
            self.grouped_payload.update(payload)
        self.groups += 1
        self.max_observed_group_bytes = max(self.max_observed_group_bytes, self.group_bytes)
        self.max_observed_group_rows = max(self.max_observed_group_rows, len(self.group))
        self.group, self.group_bytes = [], 0

    def row(self, raw):
        if not raw.endswith(b'\n') or len(raw) > self.max_line_bytes:
            raise ValueError('Update row lacks LF or exceeds line budget')
        fields = raw.split(b'|', 3)
        if len(fields) != 4 or fields[2] not in UPDATE_TYPES:
            raise ValueError('Invalid update metadata/type prefix')
        if any(not 1 <= len(part.lstrip(b'-')) <= 19 or not part.removeprefix(b'-').isdigit() for part in fields[:2]):
            raise ValueError('Invalid integer update timestamps')
        scheduled, dependency = map(int, fields[:2])
        if any(not -(2**63) <= value < 2**63 for value in (scheduled, dependency)):
            raise ValueError('Timestamp outside driver signed-64-bit range')
        if self.last_time is not None and scheduled < self.last_time:
            self.out_of_order += 1
        if dependency > scheduled:
            self.future_dependencies += 1
        if self.current_time is not None and self.current_time != scheduled:
            self.flush()
        self.current_time = scheduled
        if self.first_time is None:
            self.first_time = scheduled
        self.last_time = scheduled
        self.minimum_time = scheduled if self.minimum_time is None else min(self.minimum_time, scheduled)
        self.maximum_time = scheduled if self.maximum_time is None else max(self.maximum_time, scheduled)
        self.group_bytes += len(raw)
        if self.group_bytes > self.max_group_bytes or len(self.group) >= 1000000:
            raise ValueError('Equal-time group exceeds memory budget; no partial comparison published')
        payload = fields[2] + b'|' + fields[3]
        self.group.append((raw, payload))
        self.raw.update(raw)
        self.payload_ordered.update(payload)
        self.types[UPDATE_TYPES[fields[2]]] += 1
        self.column_counts[(UPDATE_TYPES[fields[2]], raw.count(b"|") + 1)] += 1
        self.rows += 1
        self.bytes += len(raw)
        if len(self.first_records) < 8:
            self.first_records.append({'scheduled': scheduled, 'dependency': dependency,
                                       'operationType': int(fields[2]), 'rowSha256': hashlib.sha256(raw).hexdigest()})

    def finish(self):
        self.flush()
        return {'rows': self.rows, 'bytes': self.bytes, 'rawSha256': self.raw.hexdigest(),
                'orderedOperationPayloadSha256': self.payload_ordered.hexdigest(),
                'equalTimeGroupedRawSha256': self.grouped_raw.hexdigest(),
                'equalTimeGroupedPayloadSha256': self.grouped_payload.hexdigest(),
                'operationCounts': dict(sorted(self.types.items())),
                'observedColumnCounts': {f'{op}:{count}': rows for (op, count), rows in sorted(self.column_counts.items())},
                'scheduledRange': [self.minimum_time, self.maximum_time],
                'firstScheduled': self.first_time, 'lastScheduled': self.last_time,
                'outOfOrderRows': self.out_of_order, 'futureDependencyRows': self.future_dependencies,
                'timeGroups': self.groups, 'maxEqualTimeGroupBytes': self.max_observed_group_bytes,
                'maxEqualTimeGroupRows': self.max_observed_group_rows, 'firstRecordMetadata': self.first_records}


def validate_inventory(asset, inventory):
    if inventory.get('asset') != asset or inventory.get('archiveVerification') != 'same-stream-sha256-and-length-before-publication':
        raise ValueError('Inventory is not bound to the verified asset')
    members = {}
    for entry in inventory['files']:
        name = str(archive_io.member_path(entry['path']))
        if (name in members or type(entry.get('bytes')) is not int or entry['bytes'] < 0 or
                not re.fullmatch('[0-9a-f]{64}', entry.get('sha256', ''))):
            raise ValueError('Invalid inventory member')
        members[name] = entry
    if not members or len(members) > 100000:
        raise ValueError('Inventory outside member budget')
    return members


def inspect_asset(asset, inventory, cache, output, budget, group_bytes):
    members = validate_inventory(asset, inventory)
    selected = {name for name in members if re.fullmatch(r'updateStream_\d+_\d+_(person|forum)\.csv', Path(name).name)}
    if not selected:
        raise ValueError('No update streams in archive')
    streams, properties, seen = {}, {}, set()
    path = cache / (asset['sha256'] + '.tar.zst')
    started, last_progress = time.monotonic(), 0
    def progress(name, rows, count):
        nonlocal last_progress
        now = time.monotonic()
        if now - last_progress < 30:
            return
        last_progress = now
        value = {'asset': asset['id'], 'member': name, 'rows': rows, 'memberBytesRead': count,
                 'elapsedSeconds': round(now - started, 3), 'atUnix': time.time()}
        put(output / 'status.json', value)
        print(json.dumps(value), flush=True)
    with archive_io.archive_stream(path, asset, budget) as decoded:
        with tarfile.open(fileobj=decoded, mode='r|') as archive:
            for member in archive:
                budget.check()
                name = str(archive_io.member_path(member.name))
                if member.isdir():
                    continue
                if not member.isfile() or name not in members or name in seen:
                    raise ValueError('Unexpected/duplicate archive member')
                expected = members[name]
                if member.size != expected['bytes']:
                    raise ValueError('Archive member size drift')
                seen.add(name)
                source = archive.extractfile(member)
                if name in selected:
                    audit = StreamAudit(group_bytes)
                    while True:
                        raw = source.readline(audit.max_line_bytes + 1)
                        if not raw:
                            break
                        audit.row(raw)
                        if audit.rows % 8192 == 0:
                            budget.check()
                            progress(name, audit.rows, audit.bytes)
                    result = audit.finish()
                    if (result['bytes'], result['rawSha256'], result['rows']) != (expected['bytes'], expected['sha256'], expected['rows']):
                        raise ValueError('Update stream identity/row count drift')
                    streams[Path(name).name] = {'path': name, **result}
                else:
                    h, count, small = hashlib.sha256(), 0, bytearray()
                    is_properties = Path(name).name == 'updateStream.properties'
                    if is_properties and member.size > 16384:
                        raise ValueError('Update properties exceed metadata budget')
                    while True:
                        raw = source.read(1024 * 1024)
                        if not raw:
                            break
                        budget.check()
                        h.update(raw)
                        count += len(raw)
                        if is_properties:
                            small.extend(raw)
                        progress(name, None, count)
                    if (count, h.hexdigest()) != (expected['bytes'], expected['sha256']):
                        raise ValueError('Non-update member identity drift')
                    if is_properties:
                        properties[name] = {'sha256': h.hexdigest(), 'text': small.decode('utf-8')}
        if seen != set(members):
            raise ValueError('Missing inventoried members')
    return {'asset': asset, 'archiveIntegrityVerified': True, 'allMembersVerified': len(seen),
            'streams': streams, 'properties': properties, 'elapsedSeconds': time.monotonic() - started}


def compare(left, right):
    if set(left['streams']) != set(right['streams']):
        raise ValueError('Update partition/member sets differ; no implicit repartitioning')
    rows = {}
    fields = ['rows', 'bytes', 'rawSha256', 'orderedOperationPayloadSha256',
              'equalTimeGroupedRawSha256', 'equalTimeGroupedPayloadSha256', 'operationCounts', 'scheduledRange']
    for name in left['streams']:
        a, b = left['streams'][name], right['streams'][name]
        equality = {field: a[field] == b[field] for field in fields}
        monotonic = a['outOfOrderRows'] == b['outOfOrderRows'] == 0
        if equality['rawSha256']:
            classification = 'byte-identical'
        elif monotonic and equality['equalTimeGroupedRawSha256']:
            classification = 'only-order-within-equal-scheduled-time-groups-differs'
        elif monotonic and equality['equalTimeGroupedPayloadSha256']:
            classification = 'same-operation-payloads-per-scheduled-time-dependencies-or-tie-order-differ'
        elif equality['orderedOperationPayloadSha256']:
            classification = 'same-operation-payload-order-timestamps-or-dependencies-differ'
        else:
            classification = 'operation-content-or-cross-time-order-differs'
        rows[name] = {'classification': classification, 'equal': equality, 'bothMonotonic': monotonic,
                      'initial': a, 'standalone': b}
    return {'schemaVersion': 1, 'kind': 'ldbc-update-stream-audit', 'streams': rows,
            'propertiesEqual': sorted(p['sha256'] for p in left['properties'].values()) == sorted(p['sha256'] for p in right['properties'].values()),
            'selectedUpdateRole': 'standalone-catalog-updates', 'inputsModified': False,
            'databaseReplayValidated': False, 'initialGraphCompatibilityAccepted': False,
            'limits': ['Hashes and metadata/group comparison do not prove database reference integrity',
                       'Equal-time operations are not declared independent or reordered for execution',
                       'Full official-driver/reference replay is still required for semantic acceptance']}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ['catalog', 'initial-inventory', 'updates-inventory', 'cache', 'out']:
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--timeout-seconds', type=int, default=21600)
    parser.add_argument('--max-group-bytes', type=int, default=16 * 1024 * 1024)
    parser.add_argument('--reserve-free-bytes', type=int, default=80 * 1024**3)
    args = parser.parse_args()
    if not (1 <= args.timeout_seconds <= 86400 and 1024 <= args.max_group_bytes <= 64 * 1024**2 and args.reserve_free_bytes >= 0):
        parser.error('Budgets outside supported range')
    args.out.mkdir(parents=True, exist_ok=False)
    budget = Budget(args.out, args.timeout_seconds, args.reserve_free_bytes)
    budget.check()
    inputs = [args.catalog, args.initial_inventory, args.updates_inventory, Path(__file__), Path(__file__).with_name('ldbc-archive.py')]
    put(args.out / 'inputs.lock.json', {'files': [{'path': str(p.resolve()), 'sha256': sha(p)} for p in inputs],
                                     'startedAtUnix': time.time(), 'budget': {'seconds': args.timeout_seconds, 'maxGroupBytes': args.max_group_bytes, 'reserveFreeBytes': args.reserve_free_bytes}})
    try:
        catalog = json.loads(args.catalog.read_text())
        results = {}
        for role, inventory_file in [('initial', args.initial_inventory), ('updates', args.updates_inventory)]:
            matches = [a for a in catalog['assets'] if a['role'] == role]
            if len(matches) != 1 or matches[0].get('scale') != '100':
                raise ValueError('Exactly one frozen SF100 asset per role required')
            asset = matches[0]
            result = inspect_asset(asset, json.loads(inventory_file.read_text()), args.cache, args.out, budget, args.max_group_bytes)
            put(args.out / (role + '.json'), result)
            results[role] = result
        report = compare(results['initial'], results['updates'])
        report['finishedAtUnix'] = time.time()
        put(args.out / 'result.json', report)
        print(json.dumps({'ok': True, 'report': str(args.out / 'result.json'), 'databaseReplayValidated': False}))
        return 0
    except Exception as error:
        put(args.out / 'failure.json', {'error': type(error).__name__, 'message': str(error), 'atUnix': time.time()})
        raise


if __name__ == '__main__':
    raise SystemExit(main())
