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

"""Collect bounded Linux host and cgroup v2 resource evidence, without generating load.

describe prints the input/output contract. collect requires a frozen specification
and a NEW output directory. Run in the namespace where the declared host PIDs and
cgroups are visible. Never resumes, restarts services, invokes Docker/jcmd, or forces
GC. SIGTERM/SIGINT preserve partial evidence. Collection overhead is NOT calibrated.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import resource
import signal
import stat
import sys
import time

SCHEMA = 1
RESERVE = 65536
CLOCK_EXECUTABLE_MAX_BYTES = 256 * 1024 * 1024
CLOCK_HASH_MAX_NS = 2_000_000_000
CPU_FIELDS = ('user', 'nice', 'system', 'idle', 'iowait', 'irq', 'softirq', 'steal')
MEM_FIELDS = ('MemTotal', 'MemAvailable', 'MemFree', 'Buffers', 'Cached', 'SwapTotal', 'SwapFree')


class CollectionError(Exception):
    def __init__(self, code):
        super().__init__(code)
        self.code = code


def require(value, code):
    if not value:
        raise CollectionError(code)


def json_bytes(value):
    raw = json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(',', ':'), allow_nan=False)
    return (raw + '\n').encode()


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def utc():
    return datetime.now(timezone.utc).isoformat()


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, 'duplicate-json-key')
        result[key] = value
    return result


def load_spec(path):
    raw = read_file(path, 65536)
    try:
        value = json.loads(raw, object_pairs_hook=unique_object,
                           parse_constant=lambda _: (_ for _ in ()).throw(CollectionError('invalid-json-number')))
    except (ValueError, UnicodeError, RecursionError):
        raise CollectionError('invalid-spec-json') from None
    validate_spec(value)
    return value


def exact(value, fields):
    require(isinstance(value, dict) and set(value) == set(fields), 'invalid-spec-fields')


def absolute_path(value):
    require(isinstance(value, str) and value.startswith('/') and len(value) <= 2048 and
            not any(ord(c) < 32 for c in value), 'invalid-absolute-path')


def validate_spec(value):
    fields = ('schemaVersion', 'kind', 'intervalMs', 'durationMs', 'maxSamples', 'maxOutputBytes',
              'filesystemPath', 'roles')
    exact(value, fields + (('controlledStop',) if isinstance(value, dict) and 'controlledStop' in value else ()))
    require(type(value['schemaVersion']) is int and value['schemaVersion'] == SCHEMA and
            value['kind'] == 'http-resource-collection', 'invalid-spec-identity')
    for key, low, high in [('intervalMs', 100, 60000), ('durationMs', 100, 86400000),
                           ('maxSamples', 1, 100000), ('maxOutputBytes', 131072, 67108864)]:
        require(type(value[key]) is int and low <= value[key] <= high, 'invalid-' + key)
    require(value['intervalMs'] <= value['durationMs'], 'interval-exceeds-duration')
    absolute_path(value['filesystemPath'])
    if 'controlledStop' in value:
        stop = value['controlledStop']
        exact(stop, ('path', 'token'))
        absolute_path(stop['path'])
        require(isinstance(stop['token'], str) and re.fullmatch(r'[0-9a-f]{32}', stop['token']),
                'invalid-stop-token')
    require(isinstance(value['roles'], list) and 1 <= len(value['roles']) <= 8, 'invalid-roles')
    names = set()
    for entry in value['roles']:
        require(isinstance(entry, dict), 'invalid-role')
        if 'hostPid' in entry:
            exact(entry, ('role', 'hostPid'))
            pid = entry['hostPid']
        else:
            exact(entry, ('role', 'cgroupPath', 'anchorPid'))
            absolute_path(entry['cgroupPath'])
            pid = entry['anchorPid']
        require(type(pid) is int and 1 <= pid <= 4194304, 'invalid-anchor-pid')
        require(isinstance(entry['role'], str) and re.fullmatch(r'[a-z][a-z0-9_-]{0,31}', entry['role']) and
                entry['role'] not in names, 'invalid-or-duplicate-role')
        names.add(entry['role'])


def read_file(path, limit=262144):
    # Constant kernel filenames only; do not wait on FIFOs/devices or follow file symlinks.
    try:
        fd = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NONBLOCK | os.O_NOFOLLOW)
        with os.fdopen(fd, 'rb') as source:
            require(stat.S_ISREG(os.fstat(source.fileno()).st_mode), 'not-regular-file')
            raw = source.read(limit + 1)
        require(raw is not None and len(raw) <= limit, 'read-byte-limit')
        return raw
    except FileNotFoundError:
        raise CollectionError('missing') from None
    except PermissionError:
        raise CollectionError('permission-denied') from None
    except OSError:
        raise CollectionError('read-unavailable') from None


def clock_file_metadata(info):
    return {'bytes': str(info.st_size), 'device': str(info.st_dev), 'inode': str(info.st_ino),
            'mtimeNs': str(info.st_mtime_ns), 'ctimeNs': str(info.st_ctime_ns)}


def clock_executable_identity(path, hash_content):
    """Only /proc/self/exe is followed in production; an open descriptor binds the running binary."""
    result = {'sourcePath': str(path), 'metadata': None, 'sha256': None, 'digestRead': hash_content}
    unavailable = None
    deadline = time.monotonic_ns() + CLOCK_HASH_MAX_NS
    try:
        fd = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NONBLOCK)
        with os.fdopen(fd, 'rb') as source:
            before = os.fstat(source.fileno())
            require(stat.S_ISREG(before.st_mode), 'not-regular-file')
            result['metadata'] = clock_file_metadata(before)
            require(0 < before.st_size <= CLOCK_EXECUTABLE_MAX_BYTES, 'clock-executable-byte-budget')
            if hash_content:
                digest = hashlib.sha256()
                count = 0
                while True:
                    require(time.monotonic_ns() < deadline, 'clock-executable-hash-deadline')
                    raw = source.read(1024 * 1024)
                    if not raw:
                        break
                    count += len(raw)
                    require(count <= before.st_size, 'clock-executable-changed-during-read')
                    digest.update(raw)
                require(count == before.st_size, 'clock-executable-changed-during-read')
                result['sha256'] = digest.hexdigest()
            require(clock_file_metadata(before) == clock_file_metadata(os.fstat(source.fileno())),
                    'clock-executable-changed-during-read')
    except CollectionError as error:
        unavailable = error.code
    except FileNotFoundError:
        unavailable = 'missing'
    except PermissionError:
        unavailable = 'permission-denied'
    except OSError:
        unavailable = 'read-unavailable'
    return result, unavailable


def collector_clock_identity(origin, hash_executable):
    """Raw before/after observations only; never certifies a shared Node/JVM/host clock."""
    result = {'sourceOrigin': origin, 'availability': 'unknown', 'pid': None, 'startTimeTicks': None,
              'bootId': None, 'statRaw': None, 'timeNamespace': None, 'timeForChildrenNamespace': None,
              'timeOffsetsRaw': None, 'timeOffsets': None, 'python': None, 'clock': None,
              'executable': None, 'unavailable': {}, 'captureDrift': [],
              'startedMonotonicNs': str(time.monotonic_ns())}
    def finish():
        result['finishedMonotonicNs'] = str(time.monotonic_ns())
        return result
    if origin != 'linux-procfs':
        result['unavailable']['source'] = 'fixture-has-no-production-clock-identity'
        return finish()
    if sys.platform != 'linux':
        result['unavailable']['source'] = 'platform-not-linux'
        return finish()

    def observed(key, action):
        try:
            return action()
        except CollectionError as error:
            result['unavailable'][key] = error.code
        except (OSError, ValueError, UnicodeError, IndexError, AttributeError):
            result['unavailable'][key] = 'read-unavailable-or-malformed'
        return None

    def text(path, cap):
        return read_file(path, cap).decode('utf-8')

    def identity_stat():
        raw = text('/proc/self/stat', 4096)
        require(raw.startswith(str(os.getpid()) + ' ('), 'clock-proc-pid-mismatch')
        return raw, proc_identity(raw)

    def namespace(name):
        value = os.readlink('/proc/self/ns/' + name)
        require(re.fullmatch(r'time:\[[0-9]{1,20}\]', value), 'clock-time-namespace-malformed')
        return value

    def offsets():
        raw = text('/proc/self/timens_offsets', 256)
        lines = [line.split() for line in raw.strip().splitlines()]
        require(len(lines) == 2 and [line[0] for line in lines] == ['monotonic', 'boottime'] and
                all(len(line) == 3 and re.fullmatch(r'-?[0-9]{1,12}', line[1]) and
                    re.fullmatch(r'[0-9]{1,9}', line[2]) and int(line[2]) < 1000000000 for line in lines),
                'clock-time-offsets-malformed')
        return raw, {line[0]: {'seconds': str(int(line[1])), 'nanoseconds': str(int(line[2]))} for line in lines}

    def clock_info():
        info = time.get_clock_info('monotonic')
        require(type(info.implementation) is str and len(info.implementation) <= 256 and
                type(info.monotonic) is bool and type(info.adjustable) is bool and
                math.isfinite(info.resolution) and info.resolution > 0, 'clock-info-malformed')
        return {'api': 'time.monotonic_ns', 'implementation': info.implementation,
                'monotonic': info.monotonic, 'adjustable': info.adjustable, 'resolutionSeconds': info.resolution}

    first = observed('stat', identity_stat)
    result['pid'] = os.getpid()
    if first is not None:
        result['statRaw'], result['startTimeTicks'] = first
    result['bootId'] = observed('bootId', lambda: text('/proc/sys/kernel/random/boot_id', 128).strip())
    if result['bootId'] is not None and not re.fullmatch(r'[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}',
                                                       result['bootId']):
        result['unavailable']['bootId'] = 'clock-boot-id-malformed'
        result['bootId'] = None
    result['timeNamespace'] = observed('timeNamespace', lambda: namespace('time'))
    result['timeForChildrenNamespace'] = observed('timeForChildrenNamespace', lambda: namespace('time_for_children'))
    raw_offsets = observed('timeOffsets', offsets)
    if raw_offsets is not None:
        result['timeOffsetsRaw'], result['timeOffsets'] = raw_offsets
    result['clock'] = observed('clock', clock_info)
    result['python'] = {'reportedExecutable': sys.executable, 'version': sys.version,
                        'implementation': sys.implementation.name}
    result['executable'], error = clock_executable_identity('/proc/self/exe', hash_executable)
    if error:
        result['unavailable']['executable'] = error
        if error == 'clock-executable-changed-during-read':
            result['captureDrift'].append('executable')
    last = observed('statAfter', identity_stat)
    if first is not None and (last is None or first[1] != last[1]):
        result['captureDrift'].append('process')
    if not result['unavailable'] and not result['captureDrift']:
        result['availability'] = 'observed'
    return finish()


def collector_clock_changes(before, after):
    # Dynamic fields in /proc/self/stat and the one-time SHA are deliberately excluded.
    changed = list(before['captureDrift']) + list(after['captureDrift'])
    for field in ('sourceOrigin', 'pid', 'startTimeTicks', 'bootId', 'timeNamespace',
                  'timeForChildrenNamespace', 'timeOffsets', 'python', 'clock'):
        if before[field] is not None and before[field] != after[field]:
            changed.append(field)
    old_file, new_file = before['executable'], after['executable']
    if old_file and old_file['metadata'] is not None and (
            not new_file or old_file['sourcePath'] != new_file['sourcePath'] or
            old_file['metadata'] != new_file['metadata']):
        changed.append('executable.metadata')
    return sorted(set(changed))


def unsigned(value):
    require(isinstance(value, str) and re.fullmatch(r'[0-9]{1,20}', value) and
            int(value) <= 18446744073709551615, 'malformed')
    return str(int(value))


def scalar(raw, unlimited=False):
    value = raw.strip()
    return 'max' if unlimited and value == 'max' else unsigned(value)


def pairs(raw):
    result = {}
    for line in raw.splitlines():
        pieces = line.split()
        require(len(pieces) == 2 and pieces[0] not in result, 'malformed')
        result[pieces[0]] = unsigned(pieces[1])
    require(bool(result), 'malformed')
    return result


def required_pairs(*keys):
    def parse(raw):
        value = pairs(raw)
        require(all(key in value for key in keys), 'malformed')
        return value
    return parse


def pressure(raw):
    result = {}
    for line in raw.splitlines():
        fields = line.split()
        require(fields and fields[0] in ('some', 'full') and fields[0] not in result, 'malformed')
        values = {}
        for item in fields[1:]:
            require(item.count('=') == 1, 'malformed')
            key, value = item.split('=')
            require(key not in values and key in ('avg10', 'avg60', 'avg300', 'total'), 'malformed')
            if key == 'total':
                values[key] = unsigned(value)
            else:
                number = float(value)
                require(math.isfinite(number) and 0 <= number <= 100, 'malformed')
                values[key] = number
        require(set(values) == {'avg10', 'avg60', 'avg300', 'total'}, 'malformed')
        result[fields[0]] = values
    require(bool(result), 'malformed')
    return result


def cpu_max(raw):
    fields = raw.split()
    require(len(fields) == 2, 'malformed')
    result = {'quotaMicros': scalar(fields[0], True), 'periodMicros': unsigned(fields[1])}
    require(int(result['periodMicros']) > 0 and result['quotaMicros'] != '0', 'malformed')
    return result


def io_stat(raw):
    result = {}
    for line in raw.splitlines():
        fields = line.split()
        require(fields and re.fullmatch(r'[0-9]+:[0-9]+', fields[0]) and fields[0] not in result, 'malformed')
        values = {}
        for item in fields[1:]:
            require(item.count('=') == 1, 'malformed')
            key, value = item.split('=')
            require(re.fullmatch(r'[a-z][a-z0-9_.-]{0,63}', key) and key not in values, 'malformed')
            values[key] = unsigned(value)
        result[fields[0]] = values
    return result


def proc_identity(raw):
    tail = raw.rsplit(')', 1)[-1].split()
    require(len(tail) >= 20 and tail[0] not in ('Z', 'X', 'x'), 'anchor-process-not-live')
    return unsigned(tail[19])


def unescape_mount(value):
    return re.sub(r'\\([0-7]{3})', lambda match: chr(int(match.group(1), 8)), value)


class Kernel:
    def __init__(self, proc=Path('/proc'), fixture=False):
        require(fixture or platform.system() == 'Linux', 'linux-required')
        self.proc = Path(proc)
        self.origin = 'fixture' if fixture else 'linux-procfs'
        self.check = None
        self.mounts = []
        for line in self.text(self.proc / 'self/mountinfo').splitlines():
            fields = line.split()
            require('-' in fields, 'invalid-mountinfo')
            divider = fields.index('-')
            if fields[divider + 1] == 'cgroup2':
                self.mounts.append({'root': unescape_mount(fields[3]),
                                    'path': unescape_mount(fields[4]), 'mountId': fields[0]})
        require(bool(self.mounts), 'cgroup-v2-mount-unavailable')
        self.boot = self.text(self.proc / 'sys/kernel/random/boot_id').strip()
        require(re.fullmatch(r'[a-f0-9-]{36}', self.boot), 'invalid-boot-identity')
        self.namespace = os.readlink(self.proc / 'self/ns/pid')

    def text(self, path):
        try:
            if self.check:
                self.check()
            raw = read_file(path).decode('utf-8')
            if self.check:
                self.check()
            return raw
        except UnicodeError:
            raise CollectionError('malformed') from None

    def metric(self, path, parse, unavailable, name):
        try:
            value = parse(self.text(path))
            if self.check:
                self.check()
            return value
        except (CollectionError, ValueError, OverflowError, IndexError) as error:
            if isinstance(error, CollectionError) and (error.code == 'deadline-during-sample' or
                                                     error.code.startswith('cancelled-')):
                raise
            unavailable[name] = error.code if isinstance(error, CollectionError) else 'malformed'
            return None

    def process(self, pid):
        start = proc_identity(self.text(self.proc / str(pid) / 'stat'))
        entries = [line[3:] for line in self.text(self.proc / str(pid) / 'cgroup').splitlines()
                   if line.startswith('0::')]
        require(len(entries) == 1 and entries[0].startswith('/'), 'anchor-cgroup-v2-unavailable')
        member = Path(entries[0])
        candidates = []
        for mount in self.mounts:
            try:
                relative = member.relative_to(mount['root'])
            except ValueError:
                continue
            candidates.append((len(Path(mount['root']).parts), mount, relative))
        require(bool(candidates), 'anchor-cgroup-mount-unavailable')
        candidates.sort(key=lambda item: (-item[0], len(item[1]['path'])))
        _, mount, relative = candidates[0]
        path = (Path(mount['path']) / relative).resolve(strict=True)
        require(path == Path(mount['path']).resolve() or Path(mount['path']).resolve() in path.parents,
                'cgroup-path-escapes-mount')
        return {'pid': pid, 'startTimeTicks': start, 'cgroupPath': str(path), 'mountId': mount['mountId']}

    def identity(self, entry):
        process = self.process(entry.get('hostPid', entry.get('anchorPid')))
        if 'cgroupPath' in entry:
            require(str(Path(entry['cgroupPath']).resolve(strict=True)) == process['cgroupPath'],
                    'anchor-cgroup-path-mismatch')
        path = Path(process['cgroupPath'])
        info = path.stat()
        require(stat.S_ISDIR(info.st_mode), 'cgroup-not-directory')
        self.text(path / 'cgroup.controllers')
        kind = self.text(path / 'cgroup.type').strip()
        require(kind in ('domain', 'domain threaded', 'domain invalid', 'threaded'), 'invalid-cgroup-type')
        return {**process, 'bootId': self.boot, 'device': str(info.st_dev), 'inode': str(info.st_ino),
                'cgroupType': kind}

    def group(self, identity):
        root, unavailable = Path(identity['cgroupPath']), {}
        specs = [('cpu.stat', required_pairs('usage_usec')), ('cpu.max', cpu_max),
                 ('memory.current', scalar), ('memory.peak', scalar),
                 ('memory.max', lambda raw: scalar(raw, True)), ('memory.swap.current', scalar),
                 ('memory.swap.max', lambda raw: scalar(raw, True)), ('memory.stat', pairs),
                 ('memory.events', required_pairs('oom', 'oom_kill')), ('pids.current', scalar),
                 ('pids.max', lambda raw: scalar(raw, True)), ('pids.events', required_pairs('max')),
                 ('io.stat', io_stat), ('cpu.pressure', pressure), ('memory.pressure', pressure),
                 ('io.pressure', pressure), ('cgroup.events', required_pairs('populated'))]
        values = {name: self.metric(root / name, parser, unavailable, name) for name, parser in specs}
        return {'values': values, 'unavailable': unavailable}

    def host(self, filesystem):
        unavailable = {}

        def cpu(raw):
            fields = raw.splitlines()[0].split()
            require(fields[0] == 'cpu' and len(fields) >= 9, 'malformed')
            return dict(zip(CPU_FIELDS, map(unsigned, fields[1:9])))

        def memory(raw):
            result = {key: None for key in MEM_FIELDS}
            for line in raw.splitlines():
                fields = line.split()
                key = fields[0].rstrip(':')
                if key in result:
                    require(len(fields) == 3 and fields[2] == 'kB', 'malformed')
                    result[key] = str(int(unsigned(fields[1])) * 1024)
            for key, value in result.items():
                if value is None:
                    unavailable['memory.' + key] = 'missing'
            return result

        def disks(raw):
            result = {}
            for line in raw.splitlines():
                fields = line.split()
                require(len(fields) >= 14, 'malformed')
                if fields[2].startswith(('loop', 'ram')):
                    continue
                key = fields[0] + ':' + fields[1]
                require(key not in result and len(result) < 256, 'disk-inventory-limit-or-duplicate')
                result[key] = {'name': fields[2], 'readOperations': unsigned(fields[3]),
                               'readSectors512': unsigned(fields[5]), 'readMillis': unsigned(fields[6]),
                               'writeOperations': unsigned(fields[7]), 'writeSectors512': unsigned(fields[9]),
                               'writeMillis': unsigned(fields[10]), 'inflight': unsigned(fields[11]),
                               'ioMillis': unsigned(fields[12]), 'weightedIoMillis': unsigned(fields[13])}
            return result

        def load(raw):
            fields = raw.split()
            values = [float(value) for value in fields[:3]]
            require(len(values) == 3 and all(math.isfinite(value) and value >= 0 for value in values), 'malformed')
            return values

        boot = self.text(self.proc / 'sys/kernel/random/boot_id').strip()
        require(boot == self.boot and os.readlink(self.proc / 'self/ns/pid') == self.namespace,
                'host-identity-changed')
        fs = os.statvfs(filesystem)
        return {'cpuTicks': self.metric(self.proc / 'stat', cpu, unavailable, 'cpuTicks'),
                'memoryBytes': self.metric(self.proc / 'meminfo', memory, unavailable, 'memoryBytes'),
                'loadAverage': self.metric(self.proc / 'loadavg', load, unavailable, 'loadAverage'),
                'disks': self.metric(self.proc / 'diskstats', disks, unavailable, 'disks'),
                'pressure': {name: self.metric(self.proc / 'pressure' / name, pressure, unavailable, name + '.pressure')
                             for name in ('cpu', 'memory', 'io')},
                'filesystem': {'path': str(filesystem), 'device': str(Path(filesystem).stat().st_dev),
                               'totalBytes': str(fs.f_blocks * fs.f_frsize),
                               'availableBytes': str(fs.f_bavail * fs.f_frsize)}, 'unavailable': unavailable}


def counters_delta(before, after, prefix, resets):
    if before is None or after is None:
        return None
    result = {}
    for key in sorted(before.keys() | after.keys()):
        if key not in before or key not in after:
            result[key] = None
        else:
            change = int(after[key]) - int(before[key])
            if change < 0:
                resets.append(prefix + '.' + key)
                result[key] = None
            else:
                result[key] = str(change)
    return result


def group_delta(before, after, elapsed_ns):
    require(elapsed_ns > 0, 'nonpositive-sample-interval')
    resets, deltas = [], {}
    a, b = before['values'], after['values']
    for key in ('cpu.max', 'memory.max', 'memory.swap.max', 'pids.max'):
        if a[key] is not None and b[key] is not None:
            require(a[key] == b[key], 'cgroup-controls-changed')
    for key in ('cpu.stat', 'memory.events', 'pids.events'):
        deltas[key] = counters_delta(a[key], b[key], key, resets)
    for key in ('cpu.pressure', 'memory.pressure', 'io.pressure'):
        first = None if a[key] is None else {k: v['total'] for k, v in a[key].items()}
        last = None if b[key] is None else {k: v['total'] for k, v in b[key].items()}
        deltas[key] = counters_delta(first, last, key, resets)
    io = None
    if a['io.stat'] is not None and b['io.stat'] is not None:
        io = {device: counters_delta(a['io.stat'].get(device), b['io.stat'].get(device), 'io.' + device, resets)
              for device in sorted(a['io.stat'].keys() | b['io.stat'].keys())}
    deltas['io.stat'] = io
    require(not resets, 'cgroup-counter-reset')
    usage = (deltas['cpu.stat'] or {}).get('usage_usec')
    return {'elapsedNs': str(elapsed_ns), 'counters': deltas,
            'averageCpuCores': int(usage) * 1000 / elapsed_ns if usage is not None else None}


class Cancellation:
    def __init__(self):
        self.code = None

    def signal(self, number, _frame):
        # No Event/condition locks in a signal handler, including repeated signals.
        if self.code is None:
            self.code = 'cancelled-sigterm' if number == signal.SIGTERM else 'cancelled-sigint'


def self_usage():
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return {'processCpuNs': str(time.process_time_ns()),
            'peakRssBytes': str(usage.ru_maxrss * (1 if sys.platform == 'darwin' else 1024))}


def summarize_role(summary, role):
    observed = summary.setdefault(role['role'], {'samples': 0, 'maximumObservedMemoryCurrentBytes': None,
                         'maximumObservedPids': None, 'cumulativeMemoryPeakBytesAtLastSample': None,
                         'cpuUsageMicros': None, 'cpuCoveredIntervalNs': '0', 'cpuObservedIntervals': 0,
                         'averageCpuCoresOverObservedIntervals': None, 'oomKillsWithinObservedIntervals': None,
                         'pidFailuresWithinObservedIntervals': None})
    observed['samples'] += 1
    for source, target in [('memory.current', 'maximumObservedMemoryCurrentBytes'),
                           ('pids.current', 'maximumObservedPids')]:
        value = role['values'][source]
        if value is not None:
            observed[target] = str(max(int(value), int(observed[target] or '0')))
    observed['cumulativeMemoryPeakBytesAtLastSample'] = role['values']['memory.peak']
    interval = role['interval']
    if interval is None:
        return
    counters = interval['counters']
    usage = (counters['cpu.stat'] or {}).get('usage_usec')
    if usage is not None:
        observed['cpuUsageMicros'] = str(int(observed['cpuUsageMicros'] or '0') + int(usage))
        observed['cpuCoveredIntervalNs'] = str(int(observed['cpuCoveredIntervalNs']) + int(interval['elapsedNs']))
        observed['cpuObservedIntervals'] += 1
        observed['averageCpuCoresOverObservedIntervals'] = (int(observed['cpuUsageMicros']) * 1000 /
                                                          int(observed['cpuCoveredIntervalNs']))
    for family, key, target in [('memory.events', 'oom_kill', 'oomKillsWithinObservedIntervals'),
                                ('pids.events', 'max', 'pidFailuresWithinObservedIntervals')]:
        value = (counters[family] or {}).get(key)
        if value is not None:
            observed[target] = str(int(observed[target] or '0') + int(value))


def file_entry(path):
    digest = hashlib.sha256()
    size = 0
    with Path(path).open('rb') as source:
        for block in iter(lambda: source.read(262144), b''):
            digest.update(block)
            size += len(block)
    return {'path': Path(path).name, 'bytes': size, 'sha256': digest.hexdigest()}


def write_new(path, value):
    raw = json_bytes(value)
    with Path(path).open('xb') as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    return len(raw)


def stop_request(path, stop, spec_sha, boot_id, start):
    try:
        raw = read_file(path, 4096)
    except CollectionError as error:
        if error.code == 'missing':
            return None
        raise CollectionError('stop-file-' + error.code) from None
    try:
        value = json.loads(raw, object_pairs_hook=unique_object,
                           parse_constant=lambda _: (_ for _ in ()).throw(CollectionError('invalid-json-number')))
    except (ValueError, UnicodeError, RecursionError):
        raise CollectionError('invalid-stop-json') from None
    exact(value, ('schemaVersion', 'kind', 'token', 'specSha256', 'hostBootId',
                  'workflowFinishedMonotonicNs', 'stopRequestedMonotonicNs'))
    require(type(value['schemaVersion']) is int and value['schemaVersion'] == SCHEMA and
            value['kind'] == 'http-resource-stop', 'invalid-stop-identity')
    require(value['token'] == stop['token'] and value['specSha256'] == spec_sha and value['hostBootId'] == boot_id,
            'stop-binding-mismatch')
    for key in ('workflowFinishedMonotonicNs', 'stopRequestedMonotonicNs'):
        require(isinstance(value[key], str) and re.fullmatch(r'0|[1-9][0-9]{0,24}', value[key]),
                'invalid-stop-timestamp')
    require(start <= int(value['workflowFinishedMonotonicNs']) <= int(value['stopRequestedMonotonicNs']) <=
            time.monotonic_ns(), 'stop-time-order')
    return {'request': value, 'sha256': sha(raw), 'bytes': len(raw)}


def collect(spec, output, kernel=None, cancellation=None):
    validate_spec(spec)
    cancellation = cancellation or Cancellation()
    kernel = kernel or Kernel()
    component_sha = sha(read_file(__file__, 1048576))
    output = Path(output).absolute()
    stop = spec.get('controlledStop')
    stop_path = None
    if stop:
        supplied = Path(stop['path'])
        stop_path = supplied.parent.resolve(strict=True) / supplied.name
        require(not stop_path.is_relative_to(output.resolve()), 'stop-path-inside-output')
        require(not os.path.lexists(stop_path), 'stop-file-already-exists')
    output.mkdir(mode=0o700, exist_ok=False)
    # Hash before the sampling window; only metadata is re-read after sampling.
    clock_before = collector_clock_identity(kernel.origin, hash_executable=True)
    start, cpu_start = time.monotonic_ns(), time.process_time_ns()
    deadline = start + spec['durationMs'] * 1000000
    spec_size = write_new(output / 'spec.json', spec)
    spec_sha = sha(json_bytes(spec))
    summary = {'schemaVersion': SCHEMA, 'kind': 'http-resource-summary', 'status': 'partial', 'reason': None,
               'startedAt': utc(), 'startedMonotonicNs': str(start), 'sourceOrigin': kernel.origin,
               'clock': {'implementation': time.get_clock_info('monotonic').implementation,
                         'resolutionSeconds': time.get_clock_info('monotonic').resolution,
                         'adjustable': time.get_clock_info('monotonic').adjustable},
               'host': {'bootId': kernel.boot, 'pidNamespace': kernel.namespace, 'hostname': platform.node(),
                        'kernel': platform.release(), 'architecture': platform.machine(),
                        'clockTicksPerSecond': os.sysconf('SC_CLK_TCK'), 'cpuLogical': os.cpu_count(),
                        'cpuAffinityCount': len(os.sched_getaffinity(0)) if hasattr(os, 'sched_getaffinity') else None},
               'samples': 0, 'skippedSlots': 0, 'sampleBytes': 0, 'identities': [], 'missingMetrics': {},
               'observedResources': {},
               'maximumCaptureNs': '0', 'maximumLatenessNs': '0', 'overheadCalibrated': False,
               'performanceAccepted': False, 'identityContinuous': False,
               'collectorPid': os.getpid(), 'pythonVersion': platform.python_version(),
               'pythonExecutable': sys.executable, 'componentSha256': component_sha,
               'clockIdentity': {'schemaVersion': 1, 'kind': 'http-collector-clock-identity',
                                 'sourceOrigin': kernel.origin, 'availability': 'unknown', 'verified': False,
                                 'hostAssociation': 'unverified', 'before': clock_before, 'after': None,
                                 'changedFields': [], 'executableHashMaxBytes': CLOCK_EXECUTABLE_MAX_BYTES,
                                 'executableHashMaxNs': str(CLOCK_HASH_MAX_NS)},
               'jvmMetrics': {'available': False, 'reason': 'not-collected-no-jcmd-or-gc-operations'},
               'ownership': 'caller-selected-cgroups; no Docker or host-exclusivity attestation'}
    previous, pending, slot, accepted_stop = None, None, 0, None
    identities = []
    try:
        require(not clock_before['captureDrift'], 'collector-clock-identity-changed')
        def check():
            if cancellation.code:
                raise CollectionError(cancellation.code)
            require(time.monotonic_ns() < deadline, 'deadline-during-sample')
        kernel.check = check
        def verify_identity(entry, expected):
            observed = kernel.identity(entry)
            if observed != expected:
                summary['terminalIdentityMismatch'] = {'role': entry['role'], 'observed': observed}
                raise CollectionError('role-identity-changed')
        filesystem = Path(spec['filesystemPath']).resolve(strict=True)
        require(filesystem.is_dir(), 'filesystem-path-not-directory')
        identities = [kernel.identity(entry) for entry in spec['roles']]
        summary['identities'] = [{'role': entry['role'], **identity}
                                 for entry, identity in zip(spec['roles'], identities)]
        with (output / 'samples.ndjson').open('xb') as stream:
            while True:
                if cancellation.code:
                    raise CollectionError(cancellation.code)
                now = time.monotonic_ns()
                if now >= deadline:
                    kernel.check = None
                    require(summary['samples'] > 0, 'deadline-before-first-sample')
                    require(not stop, 'controlled-stop-deadline')
                    for entry, expected in zip(spec['roles'], identities):
                        verify_identity(entry, expected)
                    if cancellation.code:
                        raise CollectionError(cancellation.code)
                    summary.update(status='completed', reason='duration-ended', identityContinuous=True)
                    break
                if stop:
                    observed_stop = stop_request(stop_path, stop, spec_sha, kernel.boot, start)
                    check()
                    require(accepted_stop is None or observed_stop == accepted_stop, 'stop-file-changed')
                    if observed_stop is not None:
                        accepted_stop = observed_stop
                        summary['controlledStop'] = accepted_stop.copy()
                        if previous and int(previous['startedMonotonicNs']) >= int(
                                accepted_stop['request']['stopRequestedMonotonicNs']):
                            for entry, expected in zip(spec['roles'], identities):
                                verify_identity(entry, expected)
                            check()
                            require(stop_request(stop_path, stop, spec_sha, kernel.boot, start) == accepted_stop,
                                    'stop-file-changed')
                            check()
                            summary['controlledStop']['coveringSampleIndex'] = previous['index']
                            summary.update(status='completed', reason='controlled-stop', identityContinuous=True)
                            break
                scheduled = start + slot * spec['intervalMs'] * 1000000
                if now < scheduled:
                    time.sleep(min((scheduled - now) / 1e9, (deadline - now) / 1e9, 0.05))
                    continue
                require(summary['samples'] < spec['maxSamples'], 'sample-count-budget')
                begun = time.monotonic_ns()
                record = {'schemaVersion': SCHEMA, 'kind': 'http-resource-sample', 'index': summary['samples'],
                          'scheduledMonotonicNs': str(scheduled), 'startedMonotonicNs': str(begun),
                          'observedAt': utc(), 'roles': []}
                pending = record
                record['host'] = kernel.host(filesystem)
                for entry, expected in zip(spec['roles'], identities):
                    if cancellation.code:
                        raise CollectionError(cancellation.code)
                    require(time.monotonic_ns() < deadline, 'deadline-during-sample')
                    verify_identity(entry, expected)
                    group_start = time.monotonic_ns()
                    group = kernel.group(expected)
                    group_end = time.monotonic_ns()
                    verify_identity(entry, expected)
                    role = {'role': entry['role'], 'identity': expected, 'startedMonotonicNs': str(group_start),
                            'finishedMonotonicNs': str(group_end), **group, 'interval': None}
                    record['roles'].append(role)
                    if previous:
                        old = next(item for item in previous['roles'] if item['role'] == entry['role'])
                        role['interval'] = group_delta(old, role, group_end - int(old['finishedMonotonicNs']))
                finished = time.monotonic_ns()
                require(finished < deadline, 'deadline-during-sample')
                if cancellation.code:
                    raise CollectionError(cancellation.code)
                record.update(finishedMonotonicNs=str(finished), captureDurationNs=str(finished - begun),
                              latenessNs=str(begun - scheduled), collector=self_usage())
                if previous:
                    resets = []
                    record['hostCpuDeltaTicks'] = counters_delta(previous['host']['cpuTicks'],
                                                                 record['host']['cpuTicks'], 'host.cpu', resets)
                    require(not resets, 'host-counter-reset')
                else:
                    record['hostCpuDeltaTicks'] = None
                raw = json_bytes(record)
                check()
                require(spec_size + summary['sampleBytes'] + len(raw) + RESERVE <= spec['maxOutputBytes'],
                        'output-byte-budget')
                stream.write(raw)
                stream.flush()
                summary['samples'] += 1
                summary['sampleBytes'] += len(raw)
                if summary['samples'] == 1:
                    summary['firstSampleStartedMonotonicNs'] = str(begun)
                summary['lastSampleFinishedMonotonicNs'] = str(finished)
                summary['maximumCaptureNs'] = str(max(int(summary['maximumCaptureNs']), finished - begun))
                summary['maximumLatenessNs'] = str(max(int(summary['maximumLatenessNs']), begun - scheduled))
                missing = {'host.' + key: value for key, value in record['host']['unavailable'].items()}
                for role in record['roles']:
                    missing.update({role['role'] + '.' + key: value for key, value in role['unavailable'].items()})
                    summarize_role(summary['observedResources'], role)
                for key, reason in missing.items():
                    item = summary['missingMetrics'].setdefault(key, {'samples': 0, 'lastReason': reason})
                    item.update(samples=item['samples'] + 1, lastReason=reason)
                previous = record
                pending = None
                elapsed_slots = (time.monotonic_ns() - start) // (spec['intervalMs'] * 1000000)
                next_slot = max(slot + 1, elapsed_slots + (1 if elapsed_slots > slot else 0))
                summary['skippedSlots'] += max(0, next_slot - slot - 1)
                slot = next_slot
            os.fsync(stream.fileno())
    except CollectionError as error:
        summary['reason'] = error.code
    except (OSError, ValueError, UnicodeError, KeyError, IndexError):
        summary['reason'] = 'collection-source-or-output-unavailable'
    finally:
        kernel.check = None
        if not (output / 'samples.ndjson').exists():
            (output / 'samples.ndjson').touch(mode=0o600, exist_ok=False)
        summary['rejectedObservationPreserved'] = False
        if pending is not None:
            pending.update(kind='http-resource-rejected-sample', rejection=summary['reason'])
            raw = json_bytes(pending)
            if spec_size + summary['sampleBytes'] + len(raw) + RESERVE <= spec['maxOutputBytes']:
                with (output / 'samples.ndjson').open('ab') as stream:
                    stream.write(raw)
                    stream.flush()
                    os.fsync(stream.fileno())
                summary['sampleBytes'] += len(raw)
                summary['rejectedObservationPreserved'] = True
        clock_after = collector_clock_identity(kernel.origin, hash_executable=False)
        clock_changes = collector_clock_changes(clock_before, clock_after)
        summary['clockIdentity'].update(
            after=clock_after, changedFields=clock_changes,
            availability=('observed' if not clock_changes and
                          clock_before['availability'] == clock_after['availability'] == 'observed' else 'unknown'))
        if clock_changes:
            if summary['status'] == 'completed' or summary['reason'] is None:
                summary['reason'] = 'collector-clock-identity-changed'
            summary.update(status='partial', identityContinuous=False)
        summary.update(finishedAt=utc(), finishedMonotonicNs=str(time.monotonic_ns()),
                       collectorCpuNs=str(time.process_time_ns() - cpu_start), collector=self_usage(),
                       sealed=True, coverage=('no-complete-samples' if not summary['samples'] else
                                              'partial' if summary['missingMetrics'] or summary['status'] != 'completed'
                                              else 'observed-metrics-present'))
        summary['specSha256'] = spec_sha
        try:
            unchanged = sha(read_file(__file__, 1048576)) == component_sha
        except CollectionError:
            unchanged = False
        if not unchanged:
            summary.update(status='partial', reason='collector-component-changed', identityContinuous=False,
                           coverage='partial')
        with (output / 'samples.ndjson').open('ab') as stream:
            stream.flush()
            os.fsync(stream.fileno())
        summary['sampleFile'] = file_entry(output / 'samples.ndjson')
        summary['sampleFileComplete'] = summary['sampleFile']['bytes'] == summary['sampleBytes']
        if not summary['sampleFileComplete']:
            summary.update(status='partial', reason='sample-write-incomplete', identityContinuous=False,
                           coverage='partial')
        if cancellation.code:
            summary.update(status='partial', reason=cancellation.code, identityContinuous=False, coverage='partial')
        raw = json_bytes(summary)
        require(spec_size + summary['sampleFile']['bytes'] + len(raw) + 4096 <= spec['maxOutputBytes'],
                'metadata-reserve-exhausted')
        write_new(output / 'summary.json', summary)
        manifest = {'schemaVersion': SCHEMA, 'kind': 'http-resource-evidence', 'sealed': True,
                    'collectionStatus': summary['status'], 'performanceAccepted': False,
                    'files': [{'path': 'spec.json', 'bytes': spec_size, 'sha256': summary['specSha256']},
                              summary['sampleFile'], file_entry(output / 'summary.json')]}
        write_new(output / 'manifest.json', manifest)
    return summary


def describe():
    return {'schemaVersion': SCHEMA, 'kind': 'http-resource-collector-contract',
            'commands': ['describe [--json]', 'collect --spec SPEC.json --out NEW_DIRECTORY [--json]'],
            'exampleSpec': {'schemaVersion': SCHEMA, 'kind': 'http-resource-collection', 'intervalMs': 1000,
                            'durationMs': 60000, 'maxSamples': 61, 'maxOutputBytes': 8388608,
                            'filesystemPath': '/home/contrue/hgbench',
                            'roles': [{'role': 'target', 'hostPid': 1234},
                                      {'role': 'driver', 'cgroupPath': '/sys/fs/cgroup/example', 'anchorPid': 5678}]},
            'bounds': {'roles': [1, 8], 'intervalMs': [100, 60000], 'durationMs': [100, 86400000],
                       'maxSamples': [1, 100000], 'maxOutputBytes': [131072, 67108864]},
            'clockIdentity': {'location': 'summary.clockIdentity', 'verified': False,
                              'capture': 'own proc/clock identity before and after; no per-sample hashes',
                              'executable': '/proc/self/exe; SHA-256 before samples, metadata recheck after',
                              'maxExecutableBytes': CLOCK_EXECUTABLE_MAX_BYTES,
                              'maxHashNs': str(CLOCK_HASH_MAX_NS),
                              'unavailable': 'unknown with reason; fixture never supplies production identity',
                              'drift': 'partial; raw samples remain; host clock association stays unverified'},
            'controlledStop': {'optionalSpec': {'path': '/absolute/controller/stop.json', 'token': 'a' * 32},
                               'requestFields': ['schemaVersion', 'kind', 'token', 'specSha256', 'hostBootId',
                                                 'workflowFinishedMonotonicNs', 'stopRequestedMonotonicNs'],
                               'kind': 'http-resource-stop', 'maxRequestBytes': 4096,
                               'publication': 'absent at startup; atomic exclusive publication; keep immutable',
                               'completion': 'full sample starts after stop request; final identities match',
                               'durationMs': 'maximum budget; missing or uncovered stop remains partial'},
            'exitCodes': {'0': 'duration or controlled stop completed; not performance acceptance',
                          '2': 'partial, cancelled, identity change, or collection budget/error',
                          '64': 'invalid input'},
            'outputs': ['spec.json', 'samples.ndjson', 'summary.json', 'manifest.json'],
            'restartPolicy': 'never resume/overwrite; use a new attempt; missing manifest means unsealed evidence',
            'limitations': ['Linux and cgroup v2 required', 'anchor PID must remain alive in the exact selected cgroup',
                            'counters are decimal strings; null is unavailable, never zero',
                            'host PID selection is caller asserted; no Docker ownership/exclusivity proof',
                            'no jcmd, forced GC or JVM internals', 'sampling/validation/output costs are uncalibrated',
                            'external task/cgroup supervisor owns hard CPU, memory and uninterruptible-I/O limits'],
            'performanceAccepted': False}


class Parser(argparse.ArgumentParser):
    def error(self, _message):
        error = {'ok': False, 'errorCode': 'invalid-arguments', 'nextAction': 'Run --help or describe'}
        self.exit(64, json.dumps(error) + '\n')


def main(argv=None):
    parser = Parser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    discovery = commands.add_parser('describe', help='Print strict specification and evidence contract as JSON')
    discovery.add_argument('--json', action='store_true', help='JSON is always enabled')
    command = commands.add_parser('collect', help='Collect without load; new output only; SIGTERM/SIGINT seal partial')
    command.add_argument('--spec', type=Path, required=True, help='JSON specification; see describe and docs')
    command.add_argument('--out', type=Path, required=True, help='New directory, existing parent; never resumes')
    command.add_argument('--json', action='store_true', help='stdout is always one terminal JSON object')
    args = parser.parse_args(argv)
    if args.command == 'describe':
        print(json.dumps(describe(), sort_keys=True))
        return 0
    previous = {}
    cancellation = Cancellation()
    try:
        spec = load_spec(args.spec)
        for number in (signal.SIGTERM, signal.SIGINT):
            previous[number] = signal.signal(number, cancellation.signal)
        result = collect(spec, args.out, cancellation=cancellation)
        print(json.dumps({'ok': result['status'] == 'completed', 'result': result}, sort_keys=True))
        return 0 if result['status'] == 'completed' else 2
    except (CollectionError, OSError, ValueError) as error:
        code = error.code if isinstance(error, CollectionError) else 'input-or-evidence-unavailable'
        print(json.dumps({'ok': False, 'errorCode': code, 'sealed': False, 'performanceAccepted': False}))
        return 64 if not Path(args.out).exists() else 2
    finally:
        for number, handler in previous.items():
            signal.signal(number, handler)


if __name__ == '__main__':
    raise SystemExit(main())
