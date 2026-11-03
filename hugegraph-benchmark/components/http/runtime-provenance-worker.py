#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more contributor
# license agreements. See the NOTICE file distributed with this work for
# additional information regarding copyright ownership. The ASF licenses this
# file to you under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed
# under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.
"""Private fixed two-frame runtime observer; bounded stdin handshake, no commands or environment values."""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import re
import selectors
import signal
import stat
import sys
import time

FLAGS = ('LD_PRELOAD', 'LD_AUDIT', 'LD_LIBRARY_PATH', 'NODE_OPTIONS', 'PYTHONHOME', 'PYTHONPATH',
         'PYTHONSTARTUP', 'PYTHONINSPECT', 'PYTHONUSERBASE')
MAX_FRAME = 32768


def need(value, code):
    if not value:
        raise ValueError(code)


def parse(raw):
    def unique(pairs):
        out = {}
        for key, value in pairs:
            need(key not in out, 'PROBE_DUPLICATE_KEY')
            out[key] = value
        return out
    return json.loads(raw, object_pairs_hook=unique, parse_constant=lambda _: need(False, 'PROBE_NONFINITE'))


def raw_file(path, maximum):
    fd = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as stream:
        raw = stream.read(maximum + 1)
    need(len(raw) <= maximum, 'PROBE_RAW_BUDGET')
    return raw.decode('utf-8')


def metadata(info):
    return {'bytes': info.st_size, 'device': str(info.st_dev), 'inode': str(info.st_ino),
            'mtimeNs': str(info.st_mtime_ns), 'ctimeNs': str(info.st_ctime_ns)}


def executable(maximum, hash_content):
    fd = os.open('/proc/self/exe', os.O_RDONLY | os.O_CLOEXEC)
    with os.fdopen(fd, 'rb') as stream:
        before = os.fstat(stream.fileno())
        need(stat.S_ISREG(before.st_mode) and 0 < before.st_size <= maximum, 'PROBE_EXECUTABLE_BUDGET')
        result = metadata(before)
        if hash_content:
            digest, count = hashlib.sha256(), 0
            while True:
                block = stream.read(1024 * 1024)
                if not block:
                    break
                count += len(block)
                need(count <= maximum, 'PROBE_HASH_BUDGET')
                digest.update(block)
            need(count == before.st_size, 'PROBE_EXECUTABLE_CHANGED')
            result['sha256'] = digest.hexdigest()
        need(metadata(os.fstat(stream.fileno())) == metadata(before), 'PROBE_EXECUTABLE_CHANGED')
    return result


def identity():
    need(sys.platform == 'linux', 'PROBE_LINUX_REQUIRED')
    raw = raw_file('/proc/self/stat', 4096)
    tail = raw.rsplit(')', 1)[-1].split()
    need(raw.startswith(str(os.getpid()) + ' (') and len(tail) >= 20 and
         re.fullmatch(r'[0-9]+', tail[19]) and tail[0] not in ('Z', 'X', 'x'), 'PROBE_STAT')
    info = time.get_clock_info('monotonic')
    need(math.isfinite(info.resolution) and info.resolution > 0, 'PROBE_CLOCK')
    return {'pid': os.getpid(), 'parentPid': os.getppid(), 'statRaw': raw, 'startTimeTicks': tail[19],
        'bootId': raw_file('/proc/sys/kernel/random/boot_id', 128).strip(),
        'namespaces': {key: os.readlink('/proc/self/ns/' + key)
                       for key in ('time', 'time_for_children', 'pid', 'mnt', 'user')},
        'timeOffsetsRaw': raw_file('/proc/self/timens_offsets', 256),
        'clock': {'api': 'time.monotonic_ns', 'implementation': info.implementation,
            'monotonic': info.monotonic, 'adjustable': info.adjustable, 'resolutionSeconds': info.resolution},
        'runtime': {'name': 'python', 'version': '.'.join(map(str, sys.version_info[:3])),
            'versionText': sys.version, 'implementation': sys.implementation.name,
            'reportedExecutable': sys.executable,
            'flags': {key: getattr(sys.flags, key) for key in
                      ('isolated', 'ignore_environment', 'no_site', 'no_user_site', 'dont_write_bytecode')},
            'injectionEnvironmentPresent': {key: bool(os.environ.get(key)) for key in FLAGS}}}


def read_action(expected, token, seconds):
    deadline, buffer = time.monotonic() + seconds, bytearray()
    with selectors.DefaultSelector() as selector:
        selector.register(0, selectors.EVENT_READ)
        while b'\n' not in buffer:
            left = deadline - time.monotonic()
            need(left > 0 and selector.select(left), 'PROBE_HANDSHAKE_DEADLINE')
            raw = os.read(0, 1025)
            need(raw, 'PROBE_HANDSHAKE_EOF')
            buffer.extend(raw)
            need(len(buffer) <= 1024, 'PROBE_HANDSHAKE_FRAME')
    raw = bytes(buffer)
    need(len(raw) <= 1024 and raw.endswith(b'\n') and raw.count(b'\n') == 1, 'PROBE_HANDSHAKE_FRAME')
    need(parse(raw) == {'kind': expected, 'token': token}, 'PROBE_HANDSHAKE_IDENTITY')


def write(value):
    raw = (json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False) + '\n').encode()
    need(len(raw) <= MAX_FRAME, 'PROBE_FRAME_BUDGET')
    os.write(1, raw)


def run(request):
    need(type(request) is dict and set(request) == {'schemaVersion', 'kind', 'role', 'token', 'runtimeDigest',
         'wallMs', 'handshakeMs', 'maxExecutableBytes'}, 'PROBE_REQUEST_FIELDS')
    need(request['schemaVersion'] == 1 and request['kind'] == 'http-runtime-observer-request' and
         request['role'] in ('host-python', 'driver-python') and
         re.fullmatch('[a-f0-9]{32}', request['token']) and
         re.fullmatch('[a-f0-9]{64}', request['runtimeDigest']), 'PROBE_REQUEST')
    need(type(request['wallMs']) is int and 5000 <= request['wallMs'] <= 120000 and
         type(request['handshakeMs']) is int and 500 <= request['handshakeMs'] <= 20000 and
         type(request['maxExecutableBytes']) is int and 1 <= request['maxExecutableBytes'] <= 512 * 1024**2,
         'PROBE_REQUEST_BUDGET')
    signal.signal(signal.SIGALRM, lambda *_: (_ for _ in ()).throw(ValueError('PROBE_WATCHDOG')))
    signal.setitimer(signal.ITIMER_REAL, request['wallMs'] / 1000)
    try:
        before = identity()
        hashed = executable(request['maxExecutableBytes'], True)
        write({'kind': 'ready', 'request': request, 'identity': before, 'executable': hashed})
        read_action('observe', request['token'], request['handshakeMs'] / 1000)
        after = identity()
        final = executable(request['maxExecutableBytes'], False)
        write({'kind': 'final', 'request': request, 'identity': after, 'executable': final})
        read_action('finish', request['token'], request['handshakeMs'] / 1000)
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument('--request', type=Path, required=True)
    args = cli.parse_args()
    try:
        run(parse(raw_file(args.request, 4096)))
        return 0
    except BaseException as error:
        code = str(error) if isinstance(error, ValueError) else 'PROBE_OBSERVER_FAILED'
        os.write(2, (code[:128] + '\n').encode())
        return 6


if __name__ == '__main__':
    raise SystemExit(main())
