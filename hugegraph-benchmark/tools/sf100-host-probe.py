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

"""Read-only Linux resource snapshot; no benchmark, install, download or file writes.

Run locally or over SSH: ssh HOST python3 - --path / --sample-seconds 3 < tools/sf100-host-probe.py
This observes one guest/OS host; it cannot attest physical-host exclusivity or benchmark readiness.
"""

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import platform
import time


def read(path):
    try:
        with Path(path).open() as source:
            return source.read(262144).strip()
    except (OSError, UnicodeError):
        return None


def pressure(name):
    raw = read('/proc/pressure/' + name)
    if raw is None:
        return None
    result = {}
    for line in raw.splitlines():
        kind, *fields = line.split()
        values = dict(item.split('=', 1) for item in fields)
        result[kind] = {key: int(value) if key == 'total' else float(value) for key, value in values.items()}
    return result


def counters():
    fields = Path('/proc/stat').read_text().splitlines()[0].split()[1:9]
    disks = {}
    for line in Path('/proc/diskstats').read_text().splitlines():
        values = line.split()
        if len(values) >= 14 and not values[2].startswith(('loop', 'ram')):
            disks[values[2]] = {
                'readOperations': int(values[3]), 'readSectors': int(values[5]), 'readMillis': int(values[6]),
                'writeOperations': int(values[7]), 'writeSectors': int(values[9]), 'writeMillis': int(values[10]),
                'inflight': int(values[11]), 'ioMillis': int(values[12]), 'weightedIoMillis': int(values[13])}
    return {'cpu': list(map(int, fields)), 'disks': disks, 'ioPressure': pressure('io')}


def cgroup_snapshot(path):
    """Read cgroup v2 counters without treating missing metrics as zero."""
    path = Path(path)
    try:
        path = path.resolve(strict=True)
        identity = path.stat()
    except OSError as error:
        return {'path': str(path), 'available': False, 'error': type(error).__name__}
    unavailable = []

    def scalar(name, unlimited=False):
        raw = read(path / name)
        if unlimited and raw == 'max':
            return 'max'
        if raw is not None and raw.isascii() and raw.isdigit():
            return raw
        unavailable.append(name)
        return None

    def counters_file(name):
        raw = read(path / name)
        if raw is None:
            unavailable.append(name)
            return None
        result = {}
        for line in raw.splitlines():
            parts = line.split()
            if len(parts) != 2 or not parts[1].isascii() or not parts[1].isdigit():
                unavailable.append(name + ':malformed')
                return None
            result[parts[0]] = parts[1]
        return result

    cpu_max = (read(path / 'cpu.max') or '').split()
    if (len(cpu_max) != 2 or not cpu_max[1].isascii() or not cpu_max[1].isdigit()
            or int(cpu_max[1]) == 0 or not (cpu_max[0] == 'max'
            or (cpu_max[0].isascii() and cpu_max[0].isdigit() and int(cpu_max[0]) > 0))):
        unavailable.append('cpu.max')
        cpu_max = [None, None]
    return {
        'path': str(path), 'available': True,
        'identity': {'device': str(identity.st_dev), 'inode': str(identity.st_ino)},
        'controls': {'cpuQuotaMicros': cpu_max[0], 'cpuPeriodMicros': cpu_max[1],
                     'memoryMaxBytes': scalar('memory.max', True),
                     'swapMaxBytes': scalar('memory.swap.max', True),
                     'pidsMax': scalar('pids.max', True)},
        'memoryCurrentBytes': scalar('memory.current'),
        'memoryPeakBytes': scalar('memory.peak'),
        'swapCurrentBytes': scalar('memory.swap.current'),
        'pidsCurrent': scalar('pids.current'),
        'pidsEvents': counters_file('pids.events'),
        'memoryStatBytesAndCounters': counters_file('memory.stat'),
        'memoryEvents': counters_file('memory.events'),
        'cpuStatMicrosAndCounters': counters_file('cpu.stat'),
        'unavailable': unavailable,
    }


def cgroup_delta(before, after, elapsed):
    if (not before.get('available') or not after.get('available') or elapsed <= 0
            or before.get('path') != after.get('path') or before.get('identity') != after.get('identity')):
        return {'valid': False, 'reason': 'missing-or-replaced-cgroup-or-invalid-interval'}
    changes = {}
    resets = []
    for field in ['cpuStatMicrosAndCounters', 'memoryEvents', 'pidsEvents']:
        first, last = before.get(field), after.get(field)
        if first is None or last is None:
            changes[field] = None
            continue
        delta = {}
        for key in sorted(first.keys() & last.keys()):
            value = int(last[key]) - int(first[key])
            if value < 0:
                resets.append(field + '.' + key)
            else:
                delta[key] = str(value)
        changes[field] = delta
    if resets:
        return {'valid': False, 'reason': 'counter-reset', 'resetCounters': resets}
    cpu = changes['cpuStatMicrosAndCounters'] or {}
    return {'valid': True, 'controlsChanged': before['controls'] != after['controls'],
            'elapsedSeconds': elapsed, 'counterDeltas': changes,
            'averageCpuCores': int(cpu['usage_usec']) / 1e6 / elapsed if 'usage_usec' in cpu else None}


def resource_signals(before, after, interval):
    """Expose kernel failure evidence; a valid counter interval does not imply a healthy service."""
    fields = {'pidAllocationFailures': ('pidsEvents', 'max'),
              'oomEvents': ('memoryEvents', 'oom'),
              'oomKills': ('memoryEvents', 'oom_kill'),
              'oomGroupKills': ('memoryEvents', 'oom_group_kill')}
    cumulative, changes = {}, {}
    deltas = interval.get('counterDeltas', {}) if interval.get('valid') else {}
    for name, (field, key) in fields.items():
        cumulative[name] = (after.get(field) or {}).get(key)
        changes[name] = (deltas.get(field) or {}).get(key)

    def at_limit(sample):
        current, maximum = sample.get('pidsCurrent'), sample.get('controls', {}).get('pidsMax')
        if current is None or maximum is None:
            return None
        return maximum != 'max' and int(current) >= int(maximum)

    return {'pidsAtLimitBefore': at_limit(before), 'pidsAtLimitAfter': at_limit(after),
            'cumulativeFailureEventsAtEnd': cumulative, 'failureEventsWithinInterval': changes,
            'scope': 'kernel-signals-only; cumulative-events-may-predate-window; not-service-health-or-performance-acceptance'}


def snapshot(path, seconds, cgroups=()):
    if platform.system() != 'Linux':
        raise ValueError('This probe requires Linux /proc; it does not emulate other hosts')
    path = Path(path).resolve(strict=True)
    started = datetime.now(timezone.utc).isoformat()
    first, begin = counters(), time.monotonic()
    groups_before = [(cgroup_snapshot(p), time.monotonic()) for p in cgroups]
    time.sleep(seconds)
    last, elapsed = counters(), time.monotonic() - begin
    groups_after = [(cgroup_snapshot(p), time.monotonic()) for p in cgroups]
    cpu_delta = [b - a for a, b in zip(first['cpu'], last['cpu'])]
    total = sum(cpu_delta)
    labels = ['user', 'nice', 'system', 'idle', 'iowait', 'irq', 'softirq', 'steal']
    io = {}
    for name, end in last['disks'].items():
        start = first['disks'].get(name)
        if start is None:
            continue
        delta = {key: end[key] - start[key] for key in start if key != 'inflight'}
        io[name] = {**delta, 'inflightAtEnd': end['inflight'],
                    'readBytesPerSecond': delta['readSectors'] * 512 / elapsed,
                    'writeBytesPerSecond': delta['writeSectors'] * 512 / elapsed}
    mem = {}
    for line in Path('/proc/meminfo').read_text().splitlines():
        key, value = line.split(':', 1)
        if key in ('MemTotal', 'MemAvailable', 'SwapTotal', 'SwapFree'):
            mem[key] = str(int(value.split()[0]) * 1024)
    disk = os.statvfs(path)
    groups = []
    for (a, start), (b, end) in zip(groups_before, groups_after):
        interval = cgroup_delta(a, b, end - start)
        groups.append({'before': a, 'after': b, 'interval': interval,
                       'resourceSignals': resource_signals(a, b, interval)})
    return {
        'schemaVersion': 3, 'kind': 'sf100-host-observation', 'observedAt': started,
        'host': platform.node(), 'architecture': platform.machine(), 'kernel': platform.release(),
        'cpuAvailable': len(os.sched_getaffinity(0)), 'cpuLogical': os.cpu_count(),
        'memoryBytes': mem, 'filesystem': {'path': str(path), 'device': str(path.stat().st_dev),
            'totalBytes': str(disk.f_blocks * disk.f_frsize),
            'availableBytes': str(disk.f_bavail * disk.f_frsize)},
        'virtualizationHint': read('/sys/class/dmi/id/product_name'),
        'cgroup': read('/proc/self/cgroup'), 'mounts': read('/proc/mounts'),
        'sampleSeconds': elapsed,
        'cpuPercent': {key: value * 100 / total if total else None for key, value in zip(labels, cpu_delta)},
        'ioPressureBefore': first['ioPressure'], 'ioPressureAfter': last['ioPressure'],
        'memoryPressure': pressure('memory'), 'diskDelta': io,
        'cgroups': groups,
        'benchmarkExecuted': False, 'performanceReady': False,
        'scope': 'read-only-short-observation; no-exclusivity-or-capacity-attestation; do-not-sum-mapper-and-device-IO'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--path', default='.', help='Existing data filesystem path; read-only statvfs')
    parser.add_argument('--sample-seconds', type=int, choices=range(1, 11), default=3,
                        help='Passive counter interval, 1-10 seconds (default: 3); generates no load')
    parser.add_argument('--cgroup', action='append', default=[],
                        help='Existing cgroup v2 directory; repeat up to 16 times. Read-only; missing fields remain null')
    args = parser.parse_args()
    try:
        if len(args.cgroup) > 16:
            raise ValueError('At most 16 cgroups per bounded snapshot')
        print(json.dumps(snapshot(args.path, args.sample_seconds, args.cgroup), indent=2))
    except (OSError, ValueError) as error:
        print(json.dumps({'ok': False, 'error': str(error)}))
        raise SystemExit(1)


if __name__ == '__main__':
    main()
