#!/usr/bin/env python3
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
#

"""Fixed bounded JVM/capture Python clock receipts. No CLI, network or process launch."""
import hashlib
import math
import os
from pathlib import Path
import re
import stat
import sys
import time

CLOCK_EXECUTABLE_MAX_BYTES = 256*1024*1024
CLOCK_HASH_MAX_NS = 2000000000


class ClockCaptureError(Exception):
    def __init__(self, code):
        super().__init__(code)
        self.code = code


def bounded_file(path, limit):
    with Path(path).open('rb') as source:
        raw = source.read(limit+1)
    if len(raw) > limit:
        raise ClockCaptureError('clock-record-too-large')
    return raw


def proc_stat(raw):
    tail = raw.rsplit(')', 1)[-1].split()
    if len(tail) < 20 or not tail[19].isdigit() or tail[0] in ('Z', 'X', 'x'):
        raise ClockCaptureError('clock-process-not-live')
    return tail[19]


def clock_component_sha256():
    path = Path(__file__)
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as source:
        before = os.fstat(source.fileno())
        if not stat.S_ISREG(before.st_mode) or not 0 < before.st_size <= 1024*1024:
            raise ClockCaptureError('clock-component-byte-budget')
        raw = source.read(1024*1024+1)
        if len(raw) != before.st_size or clock_file_metadata(before) != clock_file_metadata(os.fstat(source.fileno())):
            raise ClockCaptureError('clock-component-changed-during-read')
    return hashlib.sha256(raw).hexdigest()


def clock_file_metadata(value):
    return {'bytes': str(value.st_size), 'deviceMajor': os.major(value.st_dev),
            'deviceMinor': os.minor(value.st_dev), 'inode': str(value.st_ino),
            'mtimeNs': str(value.st_mtime_ns), 'ctimeNs': str(value.st_ctime_ns)}


def clock_executable(path, hash_content):
    result = {'sourcePath': str(path), 'path': os.readlink(path), 'metadata': None,
              'sha256': None, 'digestRead': hash_content}
    deadline = time.monotonic_ns()+CLOCK_HASH_MAX_NS
    fd = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as stream:
        before = os.fstat(stream.fileno())
        if not stat.S_ISREG(before.st_mode) or not 0 < before.st_size <= CLOCK_EXECUTABLE_MAX_BYTES:
            raise ClockCaptureError('clock-executable-byte-budget')
        result['metadata'] = clock_file_metadata(before)
        if hash_content:
            digest, count = hashlib.sha256(), 0
            while True:
                if time.monotonic_ns() >= deadline:
                    raise ClockCaptureError('clock-executable-hash-deadline')
                raw = stream.read(1024*1024)
                if time.monotonic_ns() >= deadline:
                    raise ClockCaptureError('clock-executable-hash-deadline')
                if not raw:
                    break
                count += len(raw)
                if count > before.st_size:
                    raise ClockCaptureError('clock-executable-changed-during-read')
                digest.update(raw)
            if count != before.st_size:
                raise ClockCaptureError('clock-executable-changed-during-read')
            result['sha256'] = digest.hexdigest()
        if clock_file_metadata(before) != clock_file_metadata(os.fstat(stream.fileno())):
            raise ClockCaptureError('clock-executable-changed-during-read')
    return result


def clock_snapshot(java_pid, hash_python, _proc_root=Path('/proc')):
    """Bounded raw provenance, outside measurement; failures never change the mode proof."""
    origin = 'linux-procfs' if _proc_root == Path('/proc') else 'fixture'
    result = {'sourceOrigin': origin, 'startedMonotonicNs': str(time.monotonic_ns()),
              'captureComponentSha256': None, 'capture': None, 'jvm': None, 'unavailable': {}}

    def observed(key, action):
        try:
            return action()
        except ClockCaptureError as error:
            result['unavailable'][key] = error.code
        except (OSError, ValueError, UnicodeError, AttributeError, IndexError):
            result['unavailable'][key] = 'read-unavailable-or-malformed'
        return None

    def text(path, cap):
        return bounded_file(path, cap).decode('utf-8', errors='strict')

    def boot():
        value = text(_proc_root/'sys/kernel/random/boot_id', 128).strip()
        if not re.fullmatch(r'[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}', value):
            raise ClockCaptureError('clock-boot-id-malformed')
        return value

    def namespace(path):
        value = os.readlink(path)
        if not re.fullmatch(r'time:\[[0-9]{1,20}\]', value):
            raise ClockCaptureError('clock-time-namespace-malformed')
        return value

    def offsets(path):
        raw = text(path, 256)
        rows = [line.split() for line in raw.strip().splitlines()]
        if (len(rows) != 2 or [line[0] for line in rows] != ['monotonic', 'boottime'] or
                not all(len(line) == 3 and re.fullmatch(r'-?[0-9]{1,12}', line[1]) and
                        re.fullmatch(r'[0-9]{1,9}', line[2]) and int(line[2]) < 1000000000 for line in rows)):
            raise ClockCaptureError('clock-time-offsets-malformed')
        return raw

    def process(label, pid, root, digest):
        value = {'pid': pid}
        for key, action in [
                ('statRaw', lambda: text(root/'stat', 4096)),
                ('bootId', boot),
                ('timeNamespace', lambda: namespace(root/'ns/time')),
                ('timeForChildrenNamespace', lambda: namespace(root/'ns/time_for_children')),
                ('timeOffsetsRaw', lambda: offsets(root/'timens_offsets')),
                ('executable', lambda: clock_executable(root/'exe', digest)),
                ('statAfterRaw', lambda: text(root/'stat', 4096))]:
            value[key] = observed(label+'.'+key, action)
        for raw, field in [('statRaw', 'startTimeTicks'), ('statAfterRaw', 'startTimeTicksAfter')]:
            def parse(raw=raw):
                if not value[raw].startswith(str(pid)+' ('):
                    raise ClockCaptureError('clock-process-pid-mismatch')
                return proc_stat(value[raw])
            value[field] = observed(label+'.'+field, parse) if value[raw] is not None else None
        return value

    if origin == 'fixture' or sys.platform != 'linux':
        result['unavailable']['source'] = ('fixture-has-no-production-clock-identity' if origin == 'fixture'
                                           else 'platform-not-linux')
    else:
        result['captureComponentSha256'] = observed('component', clock_component_sha256)
        result['capture'] = process('capture', os.getpid(), _proc_root/'self', hash_python)
        result['capture']['python'] = {'reportedExecutable': sys.executable, 'version': sys.version,
                                       'implementation': sys.implementation.name}
        def info():
            clock = time.get_clock_info('monotonic')
            if not math.isfinite(clock.resolution) or clock.resolution <= 0:
                raise ClockCaptureError('clock-info-malformed')
            return {'api': 'time.monotonic_ns', 'implementation': clock.implementation,
                    'monotonic': clock.monotonic, 'adjustable': clock.adjustable,
                    'resolutionSeconds': clock.resolution}
        result['capture']['clock'] = observed('capture.clock', info)
        result['jvm'] = process('jvm', java_pid, _proc_root/str(java_pid), False)
    result['finishedMonotonicNs'] = str(time.monotonic_ns())
    return result


def clock_snapshot_changes(before, after):
    """Raw stat CPU/RSS/state fields may change; only stable invariants are compared."""
    changed = []
    if before['captureComponentSha256'] is not None and (
            before['captureComponentSha256'] != after['captureComponentSha256']):
        changed.append('captureComponentSha256')
    for role in ('capture', 'jvm'):
        first, last = before[role], after[role]
        if first is None or last is None:
            continue
        for field in ('pid', 'startTimeTicks', 'bootId', 'timeNamespace', 'timeForChildrenNamespace',
                      'timeOffsetsRaw', 'python', 'clock'):
            if first.get(field) is not None and first.get(field) != last.get(field):
                changed.append(role+'.'+field)
        for point in (first, last):
            if point['startTimeTicks'] is not None and point['startTimeTicks'] != point['startTimeTicksAfter']:
                changed.append(role+'.process')
        if first['executable'] and last['executable']:
            for field in ('sourcePath', 'path', 'metadata'):
                if first['executable'][field] != last['executable'][field]:
                    changed.append(role+'.executable.'+field)
    return sorted(set(changed))


