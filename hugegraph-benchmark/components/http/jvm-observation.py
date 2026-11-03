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

"""Plan bounded HotSpot 17 observations and parse closed logs offline.

No JVM launch, attach, Docker, phase alignment, or performance acceptance.
The capture receipt is evidence supplied by a supervisor, not live attestation.
"""
import argparse
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import stat
import sys
import time

sys.dont_write_bytecode = True
MiB = 1024 ** 2
PRESET = 'hotspot17-gc-jit-v1'
ARGUMENT = ('-Xlog:gc*=info,safepoint=info,jit+compilation=debug:stdout:'
            'timenanos,uptimenanos,pid,tid,level,tags')
CANCELLED = 0
SHA = re.compile(r'[a-f0-9]{64}')
DECIMAL = re.compile(r'0|[1-9][0-9]{0,18}')
PREFIX = re.compile(rb'^\[([0-9]+)ns( *)\]\[([0-9]+)ns( *)\]\[([0-9]+)( *)\]\[([0-9]+)( *)\]'
                    rb'\[(trace|debug|info|warning|error) *\]\[([a-z, ]+)\] (.*)\n$')
GC_PAUSE = re.compile(r'^GC\(([0-9]+)\) (Pause .+) ([0-9]+\.[0-9]{3})ms$')
GC_START = re.compile(r'^GC\(([0-9]+)\) (Pause .+)$')
HEAP = re.compile(r' ([0-9]+)M->([0-9]+)M\(([0-9]+)M\)$')
JIT = re.compile(r'^ *([0-9]+) ([% ][s ][! ][b ][n ]) (?:([0-4-]) )? +(.+)$')
NATIVE_WRAPPER = re.compile(r'^.+::.+ \(native\)   (?:\(static\))?$')
SAFEPOINT = re.compile(r'^Safepoint "([^"]+)", Time since last: ([0-9]+) ns, '
                       r'Reaching safepoint: ([0-9]+) ns, Cleanup: ([0-9]+) ns, '
                       r'At safepoint: ([0-9]+) ns, Total: ([0-9]+) ns$')


class ObservationError(Exception):
    pass


def need(value, code):
    if not value:
        raise ObservationError(code)


def exact(value, fields, code):
    need(isinstance(value, dict) and set(value) == set(fields), code)


def integer(value, low, high, code):
    need(type(value) is int and low <= value <= high, code)


def decimal(value, code):
    need(isinstance(value, str) and DECIMAL.fullmatch(value) and int(value) <= (1 << 63) - 1, code)
    return int(value)


def digest(value):
    return hashlib.sha256(value).hexdigest()


def encoded(value):
    return (json.dumps(value, sort_keys=True, ensure_ascii=True, allow_nan=False, indent=2) + '\n').encode()


def compact(value):
    raw = json.dumps(value, sort_keys=True, ensure_ascii=True, allow_nan=False, separators=(',', ':'))
    return (raw + '\n').encode()


def unique(pairs):
    result = {}
    for key, value in pairs:
        need(key not in result, 'duplicate-json-key')
        result[key] = value
    return result


def parse_json(raw):
    try:
        return json.loads(raw, object_pairs_hook=unique,
                          parse_constant=lambda _: (_ for _ in ()).throw(ObservationError('nonfinite-json')))
    except (ValueError, UnicodeError, RecursionError):
        raise ObservationError('invalid-json') from None


def open_regular(path):
    fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
    try:
        info = os.fstat(fd)
        need(stat.S_ISREG(info.st_mode), 'regular-file-required')
        return os.fdopen(fd, 'rb'), info
    except BaseException:
        os.close(fd)
        raise


def unchanged(before, after):
    return (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) == (
        after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)


def read(path, cap=65536):
    stream, before = open_regular(path)
    with stream:
        need(before.st_size <= cap, 'metadata-byte-budget')
        raw = stream.read(cap + 1)
        need(len(raw) == before.st_size and unchanged(before, os.fstat(stream.fileno())), 'input-changed')
    return raw


def entry(name, raw):
    return {'path': name, 'bytes': len(raw), 'sha256': digest(raw)}


def write(root, name, raw):
    with (Path(root) / name).open('xb') as stream:
        stream.write(raw)
    return entry(name, raw)


def validate_spec(spec):
    exact(spec, ('schemaVersion', 'kind', 'preset', 'capture', 'maxRawBytes', 'windowMs', 'parser'), 'spec-fields')
    need(type(spec['schemaVersion']) is int and spec['schemaVersion'] == 1 and
         spec['kind'] == 'http-jvm-observation-spec', 'spec-identity')
    need(spec['preset'] == PRESET and spec['capture'] == 'supervisor-stdout', 'unsupported-preset-or-capture')
    integer(spec['maxRawBytes'], 4096, 256 * MiB, 'raw-byte-budget')
    integer(spec['windowMs'], 100, 60000, 'window-size')
    parser = spec['parser']
    exact(parser, ('maxLineBytes', 'maxEvents', 'maxWindows', 'maxOutputBytes', 'timeoutMs'), 'parser-fields')
    for name, low, high in [('maxLineBytes', 256, 65536), ('maxEvents', 1, 1000000),
                           ('maxWindows', 1, 10000), ('maxOutputBytes', spec['maxRawBytes'] + 2 * MiB, 512 * MiB),
                           ('timeoutMs', 100, 300000)]:
        integer(parser[name], low, high, 'invalid-' + name)


def plan(spec_path, output):
    raw = read(spec_path)
    spec = parse_json(raw)
    validate_spec(spec)
    value = {'schemaVersion': 1, 'kind': 'http-jvm-observation-plan', 'specSha256': digest(raw),
             'javaArguments': [ARGUMENT], 'capture': {'file': 'server.log', 'rotation': 'none', 'maxFiles': 1,
             'maxRawBytes': spec['maxRawBytes'], 'overflow': 'stop-and-invalidate', 'synchronous': True},
             'componentSha256': digest(read(__file__, MiB)), 'clockAlignment': 'unverified',
             'performanceAccepted': False}
    output = Path(output)
    output.mkdir(mode=0o700)
    files = [write(output, 'spec.json', raw), write(output, 'plan.json', encoded(value))]
    write(output, 'plan.lock.json', encoded({'schemaVersion': 1, 'kind': 'http-jvm-observation-plan-lock',
                                           'files': files}))
    return {'directory': str(output), 'planSha256': files[1]['sha256'], 'executed': False,
            'javaArguments': [ARGUMENT], 'performanceAccepted': False}


def load_plan(root):
    root = Path(root)
    raw_spec, raw_plan, raw_lock = (read(root / name) for name in ('spec.json', 'plan.json', 'plan.lock.json'))
    spec, value, lock = map(parse_json, (raw_spec, raw_plan, raw_lock))
    validate_spec(spec)
    exact(lock, ('schemaVersion', 'kind', 'files'), 'plan-lock-fields')
    need(type(lock['schemaVersion']) is int and lock['schemaVersion'] == 1 and
         lock['kind'] == 'http-jvm-observation-plan-lock' and
         lock['files'] == [entry('spec.json', raw_spec), entry('plan.json', raw_plan)], 'plan-lock-digest')
    expected = {'schemaVersion': 1, 'kind': 'http-jvm-observation-plan', 'specSha256': digest(raw_spec),
                'javaArguments': [ARGUMENT], 'capture': {'file': 'server.log', 'rotation': 'none', 'maxFiles': 1,
                'maxRawBytes': spec['maxRawBytes'], 'overflow': 'stop-and-invalidate', 'synchronous': True},
                'componentSha256': digest(read(__file__, MiB)), 'clockAlignment': 'unverified',
                'performanceAccepted': False}
    need(encoded(value) == encoded(expected), 'plan-content-or-component-changed')
    return spec, digest(raw_plan), {'spec.json': raw_spec, 'plan.json': raw_plan, 'plan.lock.json': raw_lock}


def validate_evidence(launch, capture, plan_sha, launch_sha):
    exact(launch, ('schemaVersion', 'kind', 'planSha256', 'argv', 'process', 'jdk'), 'launch-fields')
    need(type(launch['schemaVersion']) is int and launch['schemaVersion'] == 1 and
         launch['kind'] == 'http-jvm-observation-launch' and launch['planSha256'] == plan_sha, 'launch-identity')
    argv = launch['argv']
    need(isinstance(argv, list) and 2 <= len(argv) <= 256 and
         all(isinstance(a, str) and 0 < len(a) <= 65536 and '\0' not in a for a in argv), 'launch-argv')
    need([a for a in argv if a.startswith('-Xlog')] == [ARGUMENT], 'launch-logging-options-differ')
    process, jdk = launch['process'], launch['jdk']
    exact(process, ('pid', 'startTimeTicks', 'hostBootId'), 'process-fields')
    integer(process['pid'], 1, 4194304, 'process-pid')
    decimal(process['startTimeTicks'], 'process-start-ticks')
    need(isinstance(process['hostBootId'], str) and
         re.fullmatch(r'[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}', process['hostBootId']), 'host-boot-id')
    exact(jdk, ('javaSha256', 'libjvmSha256', 'version'), 'jdk-fields')
    need(all(isinstance(jdk[k], str) and SHA.fullmatch(jdk[k]) for k in ('javaSha256', 'libjvmSha256')) and
         isinstance(jdk['version'], str) and re.fullmatch(r'17\.[0-9.]+(?:\+[A-Za-z0-9._-]+)?', jdk['version']),
         'jdk17-identity')
    exact(capture, ('schemaVersion', 'kind', 'planSha256', 'launchSha256', 'raw', 'startedMonotonicNs',
                    'finishedMonotonicNs', 'eof', 'capHit', 'forcedKill', 'cleanupComplete', 'error',
                    'readBytes', 'retainedBytes', 'discardedBytes'), 'capture-fields')
    need(type(capture['schemaVersion']) is int and capture['schemaVersion'] == 1 and
         capture['kind'] == 'http-jvm-observation-capture' and capture['planSha256'] == plan_sha and
         capture['launchSha256'] == launch_sha, 'capture-identity')
    exact(capture['raw'], ('bytes', 'sha256'), 'capture-raw-fields')
    integer(capture['raw']['bytes'], 0, 1 << 40, 'capture-byte-count')
    need(isinstance(capture['raw']['sha256'], str) and SHA.fullmatch(capture['raw']['sha256']), 'capture-sha256')
    need(decimal(capture['startedMonotonicNs'], 'capture-start') <=
         decimal(capture['finishedMonotonicNs'], 'capture-finish'), 'capture-time-order')
    need(all(type(capture[k]) is bool for k in ('eof', 'capHit', 'forcedKill', 'cleanupComplete')),
         'capture-state-fields')
    for name in ('readBytes', 'retainedBytes', 'discardedBytes'):
        integer(capture[name], 0, 1 << 40, 'capture-' + name)
    need(capture['readBytes'] == capture['retainedBytes'] + capture['discardedBytes'] and
         capture['raw']['bytes'] == capture['retainedBytes'], 'capture-byte-accounting-differs')
    need(capture['error'] is None or isinstance(capture['error'], str) and 0 < len(capture['error']) <= 500,
         'capture-error-field')


def decode_event(raw, offset, pid):
    match = PREFIX.fullmatch(raw)
    if match is None:
        # A malformed candidate must not silently become an ignored application line.
        need(not re.match(rb'^\[-?[0-9]+(?:\.[0-9]+)?(?:ns|ms|s)\]', raw),
             'unrecognized-unified-log-prefix')
        return None
    nano, nano_pad, uptime, uptime_pad, actual_pid, pid_pad, tid, tid_pad, level, raw_tags, payload = match.groups()
    # LogFileStreamOutput uses [%-*s] and remembers the widest value per decorator.
    # Only ASCII right padding fits that format. Bound widths by the accepted
    # signed 64-bit nanoseconds and Linux PID/TID domains; never strip digits.
    need(len(nano) + 2 + len(nano_pad) <= 21 and len(uptime) + 2 + len(uptime_pad) <= 21 and
         len(actual_pid) + len(pid_pad) <= 7 and len(tid) + len(tid_pad) <= 7,
         'invalid-decorator-padding')
    nano, uptime = nano.decode(), uptime.decode()
    decimal(nano, 'invalid-timenanos')
    decimal(uptime, 'invalid-uptimenanos')
    need(decimal(actual_pid.decode(), 'invalid-pid') == pid, 'log-process-identity-differs')
    tid = decimal(tid.decode(), 'invalid-tid')
    integer(tid, 1, 4194304, 'invalid-tid')
    tags = raw_tags.decode().strip().split(',')
    need(all(re.fullmatch(r'[a-z]+', tag) for tag in tags), 'invalid-log-tags')
    body = payload.decode('utf-8')
    result = {'source': {'offset': offset, 'bytes': len(raw)}, 'timenanos': nano, 'uptimenanos': uptime,
              'pid': pid, 'tid': tid, 'level': level.decode(), 'tags': tags, 'kind': 'diagnostic'}
    if tags == ['gc', 'start'] and (match := GC_START.fullmatch(body)):
        result.update(kind='gc-pause-start', gcId=match[1], title=match[2])
    elif tags == ['gc'] and (match := GC_PAUSE.fullmatch(body)):
        title = match[2]
        if (heap := HEAP.search(title)):
            result['heapApproxMiB'] = {'before': int(heap[1]), 'after': int(heap[2]), 'capacity': int(heap[3])}
            title = title[:heap.start()]
        result.update(kind='gc-pause-end', gcId=match[1], title=title,
                      durationNs=str(int(Decimal(match[3]) * 1000000)))
    elif tags in (['gc'], ['gc', 'start']) and re.match(r'^GC\([0-9]+\) Pause ', body):
        result['kind'] = 'unknown-gc-pause'
    elif set(tags) == {'jit', 'compilation'}:
        match = JIT.fullmatch(body)
        if match is None:
            result['kind'] = 'unknown-jit'
        else:
            tail = match[4]
            state = 'jit-begin'
            if tail.endswith('made not entrant'):
                state = 'jit-not-entrant'
            elif tail.endswith('made zombie'):
                state = 'jit-zombie'
            elif tail.endswith('NO CODE INSTALLED'):
                state = 'jit-repeat-no-install'
            elif NATIVE_WRAPPER.fullmatch(tail) and match[2] in ('    n', ' s  n') and match[3] in (None, '0'):
                # Native wrappers are emitted by create_native_wrapper after
                # installation, with an empty or "(static)" message. They are
                # distinct from the compiler-entry activity counted below.
                state = 'jit-native-wrapper'
            elif match[2][4] == 'n' or not re.search(r'\([0-9]+ bytes\)$', tail):
                state = 'unknown-jit'
            result.update(kind=state, compileId=match[1], flags=match[2].strip(), compilationLevel=match[3],
                          method=tail[:512], methodTruncated=len(tail) > 512)
    elif tags == ['safepoint']:
        match = SAFEPOINT.fullmatch(body)
        if match:
            result.update(kind='safepoint', operation=match[1], totalNs=match[6],
                          reachingNs=match[3], cleanupNs=match[4], atSafepointNs=match[5])
        else:
            result['kind'] = 'unknown-safepoint'
    # Payload text can contain unusual method names. Raw bytes remain in server.log.
    if 'title' in result:
        need(len(result['title']) <= 1024, 'gc-title-budget')
    return result


def cancel(signum, _frame):
    global CANCELLED
    CANCELLED = 128 + signum


def parse(plan_root, raw_path, launch_path, capture_path, output):
    started = time.monotonic()
    spec, plan_sha, metadata = load_plan(plan_root)
    component_sha = parse_json(metadata['plan.json'])['componentSha256']
    launch_raw, capture_raw = read(launch_path, 256 * 1024), read(capture_path)
    launch, capture = parse_json(launch_raw), parse_json(capture_raw)
    validate_evidence(launch, capture, plan_sha, digest(launch_raw))
    metadata.update({'launch.json': launch_raw, 'capture.json': capture_raw})
    output = Path(output)
    output.mkdir(mode=0o700)
    files = [write(output, name, raw) for name, raw in metadata.items()]
    used = sum(f['bytes'] for f in files)
    parser = spec['parser']
    deadline = started + parser['timeoutMs'] / 1000
    summary = {'schemaVersion': 1, 'kind': 'http-jvm-observation-summary', 'status': 'partial',
               'planSha256': plan_sha, 'clockAlignment': 'unverified', 'phaseAttribution': 'unavailable',
               'performanceAccepted': False, 'warmupStable': None, 'compilationSuccesses': None,
               'compilationCpuNs': None, 'issues': [], 'events': 0, 'applicationLines': 0, 'counts': {},
               'gcPauseReportedNs': '0', 'safepointReportedNs': '0', 'rawComplete': False,
               'captureComplete': capture['eof'] and not capture['capHit'] and not capture['forcedKill'] and
               capture['cleanupComplete'] and capture['error'] is None and capture['discardedBytes'] == 0,
               'firstTimenanos': None, 'lastTimenanos': None, 'windows': 0,
               'windowDomain': 'jvm-uptime', 'windowSemantics': 'log-event-time; no zero-filled coverage claim'}
    bins, pending_gc = {}, {}
    raw_hash, event_hash = hashlib.sha256(), hashlib.sha256()
    raw_count = event_count = 0

    def issue(code):
        if code not in summary['issues'] and len(summary['issues']) < 32:
            summary['issues'].append(code)

    def check():
        need(not CANCELLED, 'cancelled')
        need(time.monotonic() < deadline, 'parse-deadline')

    def bounded_write(stream, raw):
        nonlocal used
        check()
        need(used + len(raw) <= parser['maxOutputBytes'] - MiB, 'output-byte-budget')
        stream.write(raw)
        used += len(raw)

    for name in ('eof', 'capHit', 'forcedKill'):
        if capture[name] != (name == 'eof'):
            issue('capture-' + ('missing-eof' if name == 'eof' else name))
    if capture['error'] is not None:
        issue('capture-io-error')
    if capture['discardedBytes']:
        issue('capture-discarded-bytes')
    if not capture['cleanupComplete']:
        issue('capture-cleanup-incomplete')
    try:
        stream, before = open_regular(raw_path)
        with stream, (output / 'server.log').open('xb') as raw_out, (output / 'events.ndjson').open('xb') as events:
            need(before.st_size <= spec['maxRawBytes'], 'raw-byte-budget')
            while True:
                check()
                raw = stream.readline(parser['maxLineBytes'] + 1)
                if not raw:
                    break
                need(raw_count + len(raw) <= spec['maxRawBytes'], 'raw-byte-budget')
                bounded_write(raw_out, raw)
                offset = raw_count
                raw_count += len(raw)
                raw_hash.update(raw)
                need(len(raw) <= parser['maxLineBytes'], 'line-byte-budget')
                need(raw.endswith(b'\n'), 'truncated-final-line')
                event = decode_event(raw, offset, launch['process']['pid'])
                if event is None:
                    summary['applicationLines'] += 1
                    continue
                need(summary['events'] < parser['maxEvents'], 'event-count-budget')
                kind = event['kind']
                if kind.startswith('unknown-'):
                    issue(kind)
                if kind.startswith('gc-pause-'):
                    key = (event['gcId'], event['title'])
                    if kind == 'gc-pause-start':
                        if key in pending_gc:
                            issue('duplicate-gc-start')
                        pending_gc[key] = int(event['uptimenanos'])
                    else:
                        start = pending_gc.pop(key, None)
                        event['startObserved'] = start is not None
                        if start is None or start > int(event['uptimenanos']):
                            issue('unmatched-or-reversed-gc-pause')
                index = int(event['uptimenanos']) // (spec['windowMs'] * 1000000)
                if index not in bins:
                    need(len(bins) < parser['maxWindows'], 'window-count-budget')
                    bins[index] = {'index': index, 'startUptimeNs': str(index * spec['windowMs'] * 1000000),
                                   'endUptimeNs': str((index + 1) * spec['windowMs'] * 1000000), 'counts': {},
                                   'gcPauseReportedNs': '0', 'safepointReportedNs': '0'}
                encoded_event = compact(event)
                bounded_write(events, encoded_event)
                event_hash.update(encoded_event)
                event_count += len(encoded_event)
                summary['events'] += 1
                for target in (summary, bins[index]):
                    target['counts'][kind] = target['counts'].get(kind, 0) + 1
                    for event_key, total_key in [('durationNs', 'gcPauseReportedNs'),
                                                  ('totalNs', 'safepointReportedNs')]:
                        if event_key in event:
                            target[total_key] = str(int(target[total_key]) + int(event[event_key]))
                nano = int(event['timenanos'])
                summary['firstTimenanos'] = str(min(nano, int(summary['firstTimenanos'] or nano)))
                summary['lastTimenanos'] = str(max(nano, int(summary['lastTimenanos'] or nano)))
            need(unchanged(before, os.fstat(stream.fileno())) and unchanged(before, Path(raw_path).lstat()),
                 'raw-input-changed')
            need(raw_count == capture['raw']['bytes'] and raw_hash.hexdigest() == capture['raw']['sha256'],
                 'capture-raw-digest-differs')
            summary['rawComplete'] = True
    except BaseException as error:
        issue(str(error) if isinstance(error, ObservationError) else type(error).__name__)
    if pending_gc:
        issue('unfinished-gc-pause')
    if not summary['events']:
        issue('no-unified-log-events')
    for name, count, hashed in [('server.log', raw_count, raw_hash), ('events.ndjson', event_count, event_hash)]:
        if (output / name).exists():
            files.append({'path': name, 'bytes': count, 'sha256': hashed.hexdigest()})
    window_hash, window_count = hashlib.sha256(), 0
    with (output / 'windows.ndjson').open('xb') as stream:
        try:
            for index in sorted(bins):
                raw = compact(bins[index])
                bounded_write(stream, raw)
                window_hash.update(raw)
                window_count += len(raw)
                summary['windows'] += 1
        except BaseException as error:
            issue(str(error) if isinstance(error, ObservationError) else type(error).__name__)
    files.append({'path': 'windows.ndjson', 'bytes': window_count, 'sha256': window_hash.hexdigest()})
    component_reverified = False
    try:
        component_reverified = digest(read(__file__, MiB)) == component_sha
        if not component_reverified:
            issue('component-changed')
    except BaseException:
        issue('component-reverification-failed')
    try:
        check()
    except ObservationError as error:
        issue(str(error))
    summary['status'] = 'completed' if not summary['issues'] else 'partial'
    summary['componentReverified'] = component_reverified
    files.append(write(output, 'summary.json', encoded(summary)))
    write(output, 'manifest.json', encoded({'schemaVersion': 1, 'kind': 'http-jvm-observation-evidence',
          'status': summary['status'], 'planSha256': plan_sha, 'componentSha256': component_sha,
          'componentReverified': component_reverified,
          'files': sorted(files, key=lambda f: f['path']), 'performanceAccepted': False}))
    return {'directory': str(output), 'status': summary['status'], 'issues': summary['issues'],
            'clockAlignment': 'unverified', 'performanceAccepted': False}


def describe():
    return {'schemaVersion': 1, 'kind': 'http-jvm-observation-capability', 'preset': PRESET,
            'commands': ['plan --spec JSON --out NEW', 'parse --plan DIR --raw FILE --launch JSON '
                         '--capture JSON --out NEW', 'describe'], 'javaArguments': [ARGUMENT],
            'capture': 'synchronous stdout; supervisor hard byte cap; no rotation; drain to EOF',
            'clockAlignment': 'unverified', 'liveOperations': False, 'performanceAccepted': False,
            'exitCodes': {'0': 'plan created or complete observation parsed', '2': 'partial observation',
                          '3': 'invalid input or I/O failure', '130/143': 'cancelled with partial evidence if created'}}


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument('--json', action='store_true', help='Output is always structured JSON.')
    commands = cli.add_subparsers(dest='command', required=True)
    commands.add_parser('describe', help='Show fixed preset, capabilities, and exit codes.')
    create = commands.add_parser('plan', help='Freeze a fixed startup preset; launches no process.')
    create.add_argument('--spec', required=True)
    create.add_argument('--out', required=True)
    analyze = commands.add_parser('parse', help='Stream closed captured bytes offline; retains partial evidence.')
    for name in ('plan', 'raw', 'launch', 'capture', 'out'):
        analyze.add_argument('--' + name, required=True)
    args = cli.parse_args()
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, cancel)
    try:
        if args.command == 'describe':
            value = describe()
        elif args.command == 'plan':
            value = plan(args.spec, args.out)
        else:
            value = parse(args.plan, args.raw, args.launch, args.capture, args.out)
        status = CANCELLED or (2 if value.get('status') == 'partial' else 0)
        print(json.dumps({'ok': status == 0, 'data': value}))
        return status
    except Exception as error:
        print(json.dumps({'ok': False, 'error': {'code': str(error) if isinstance(error, ObservationError)
                           else type(error).__name__, 'retryable': False}, 'performanceAccepted': False}))
        return CANCELLED or 3


if __name__ == '__main__':
    sys.exit(main())
