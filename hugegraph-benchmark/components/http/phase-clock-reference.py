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
"""Fixed stdio reference peer for two actual-caller Node clock sanity exchanges.

Private protocol; no commands, networking, Node subprocesses or background sampling.
S is read before challenge output and R immediately after the complete reply frame.
Raw observations remain unverified until independent source/runtime/namespace gates.
"""
import hashlib
import json
import math
import os
from pathlib import Path
import re
import selectors
import stat
import sys
import time
import uuid

MAX_FILE = 256 * 1024 * 1024


def need(value, code):
    if not value:
        raise ValueError(code)


def encoded(value):
    return (json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False) + '\n').encode()


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        need(key not in result, 'SR_DUPLICATE_JSON_KEY')
        result[key] = value
    return result


def parsed(raw):
    return json.loads(raw, object_pairs_hook=unique_object,
                      parse_constant=lambda _: (_ for _ in ()).throw(ValueError('SR_NONFINITE_JSON')))


def binding_check(value):
    need(type(value) is dict and set(value) == {'sessionId', 'runIdentity', 'profileDigest'}, 'SR_BINDING_FIELDS')
    need(type(value['sessionId']) is str and re.fullmatch(r'[a-f0-9]{32}', value['sessionId']) and
         type(value['profileDigest']) is str and re.fullmatch(r'[a-f0-9]{64}', value['profileDigest']), 'SR_BINDING')
    run = value['runIdentity']
    need(type(run) is dict and set(run) == {'id', 'caseId', 'loadId', 'repeat', 'probePhase'}, 'SR_BINDING')
    for key in ('id', 'caseId', 'loadId'):
        need(type(run[key]) is str and re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,63}', run[key]), 'SR_BINDING')
    need(type(run['repeat']) is int and 0 <= run['repeat'] <= 20 and
         run['probePhase'] in (None, 'preflight', 'postcheck'), 'SR_BINDING')
    return value


def spec_check(value):
    need(type(value) is dict and set(value) == {'schemaVersion', 'kind', 'sourceOrigin', 'startupMs',
         'exchangeMs', 'finishMs', 'wallMs', 'maxFrameBytes', 'maxRawBytes', 'maxStderrBytes'}, 'SR_SPEC_FIELDS')
    need(type(value['schemaVersion']) is int and value['schemaVersion'] == 1 and
         value['kind'] == 'http-runner-sanity-spec', 'SR_SPEC_KIND')
    need(value['sourceOrigin'] in ('fixture', 'linux-procfs'), 'SR_SOURCE_ORIGIN')
    for key, low, high in [('startupMs', 100, 10000), ('exchangeMs', 100, 10000), ('finishMs', 50, 2000),
                           ('wallMs', 1000, 600000), ('maxFrameBytes', 4096, 16384),
                           ('maxRawBytes', 16384, 262144), ('maxStderrBytes', 0, 4096)]:
        need(type(value[key]) is int and low <= value[key] <= high, 'SR_SPEC_BUDGET')
    need(value['startupMs'] + 2 * value['exchangeMs'] + value['finishMs'] <= value['wallMs'], 'SR_SPEC_BUDGET')
    need(4 * value['maxFrameBytes'] <= value['maxRawBytes'], 'SR_SPEC_BUDGET')
    return value


def metadata(info):
    return {'bytes': str(info.st_size), 'device': str(info.st_dev), 'inode': str(info.st_ino),
            'mtimeNs': str(info.st_mtime_ns), 'ctimeNs': str(info.st_ctime_ns)}


def file_identity(path, digest, deadline):
    fd = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as stream:
        before = os.fstat(stream.fileno())
        need(stat.S_ISREG(before.st_mode) and 0 < before.st_size <= MAX_FILE, 'SR_RUNTIME_FILE_BUDGET')
        value = {'path': str(path), 'metadata': metadata(before)}
        if digest:
            hash_value, count = hashlib.sha256(), 0
            while True:
                need(time.monotonic_ns() < deadline, 'SR_RUNTIME_HASH_DEADLINE')
                raw = stream.read(1024 * 1024)
                if not raw:
                    break
                count += len(raw)
                need(count <= before.st_size, 'SR_RUNTIME_FILE_DRIFT')
                hash_value.update(raw)
            need(count == before.st_size, 'SR_RUNTIME_FILE_DRIFT')
            value['sha256'] = hash_value.hexdigest()
        need(metadata(before) == metadata(os.fstat(stream.fileno())), 'SR_RUNTIME_FILE_DRIFT')
    return value


def text(path, cap):
    fd = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as stream:
        raw = stream.read(cap + 1)
    need(len(raw) <= cap, 'SR_IDENTITY_BUDGET')
    return raw.decode('utf-8')


def identity(origin):
    info = time.get_clock_info('monotonic')
    need(math.isfinite(info.resolution) and info.resolution > 0, 'SR_CLOCK_INFO')
    result = {'sourceOrigin': origin, 'platform': sys.platform, 'pid': os.getpid(), 'parentPid': os.getppid(),
              'bootId': None, 'startTimeTicks': None, 'timeNamespace': None, 'timeForChildrenNamespace': None,
              'timeOffsets': None, 'statRaw': None,
              'python': {'version': sys.version, 'executable': sys.executable,
                         'implementation': sys.implementation.name},
              'clock': {'api': 'time.monotonic_ns', 'implementation': info.implementation,
                        'monotonic': info.monotonic, 'adjustable': info.adjustable,
                        'resolutionSeconds': info.resolution}}
    if origin == 'fixture':
        return result
    need(sys.platform == 'linux', 'SR_LINUX_REQUIRED')

    def ticks():
        raw = text('/proc/self/stat', 4096)
        tail = raw.rsplit(')', 1)[-1].split()
        need(raw.startswith(str(os.getpid()) + ' (') and len(tail) >= 20 and
             tail[0] not in ('Z', 'X', 'x') and re.fullmatch(r'[0-9]{1,20}', tail[19]), 'SR_PROCESS_IDENTITY')
        return raw, tail[19]

    result['statRaw'], result['startTimeTicks'] = ticks()
    result['bootId'] = text('/proc/sys/kernel/random/boot_id', 128).strip()
    for key, name in [('timeNamespace', 'time'), ('timeForChildrenNamespace', 'time_for_children')]:
        result[key] = os.readlink('/proc/self/ns/' + name)
    result['timeOffsets'] = text('/proc/self/timens_offsets', 256)
    need(ticks()[1] == result['startTimeTicks'], 'SR_PROCESS_IDENTITY_DRIFT')
    return result


def invariant(value):
    return {key: item for key, item in value.items() if key != 'statRaw'}


class Channel:
    def __init__(self, spec, deadline):
        self.spec, self.deadline, self.buffer, self.count = spec, deadline, bytearray(), 0
        os.set_blocking(0, False)
        os.set_blocking(1, False)

    def wait(self, fd, event, deadline):
        remaining = min(deadline, self.deadline) - time.monotonic_ns()
        need(remaining > 0, 'SR_DEADLINE')
        with selectors.DefaultSelector() as selector:
            selector.register(fd, event)
            need(selector.select(remaining / 1e9), 'SR_DEADLINE')
        need(time.monotonic_ns() < min(deadline, self.deadline), 'SR_DEADLINE')

    def read(self, deadline):
        while b'\n' not in self.buffer:
            self.wait(0, selectors.EVENT_READ, deadline)
            raw = os.read(0, self.spec['maxFrameBytes'] + 1)
            need(raw, 'SR_INPUT_EOF')
            self.count += len(raw)
            need(self.count <= self.spec['maxRawBytes'], 'SR_RAW_BYTE_BUDGET')
            self.buffer.extend(raw)
            need(len(self.buffer) <= self.spec['maxFrameBytes'], 'SR_FRAME_BYTE_BUDGET')
        end = self.buffer.index(b'\n') + 1
        raw = bytes(self.buffer[:end])
        del self.buffer[:end]
        need(not self.buffer, 'SR_UNSOLICITED_INPUT')
        # R must precede JSON decoding/validation and any process/file observations.
        received = time.monotonic_ns()
        return raw, received

    def write(self, value, deadline):
        raw = encoded(value)
        need(len(raw) <= self.spec['maxFrameBytes'], 'SR_FRAME_BYTE_BUDGET')
        self.count += len(raw)
        need(self.count <= self.spec['maxRawBytes'], 'SR_RAW_BYTE_BUDGET')
        offset = 0
        while offset < len(raw):
            self.wait(1, selectors.EVENT_WRITE, deadline)
            offset += os.write(1, raw[offset:])
        return raw


def run(spec):
    begun = time.monotonic_ns()
    deadline = begun + spec['wallMs'] * 1000000
    channel = Channel(spec, deadline)
    startup = min(deadline, begun + spec['startupMs'] * 1000000)
    first, _ = channel.read(startup)
    init = parsed(first)
    need(type(init) is dict and set(init) == {'kind', 'binding'} and init['kind'] == 'init', 'SR_INIT')
    binding = binding_check(init['binding'])
    need(len(encoded(binding)) <= 2048, 'SR_BINDING')
    paths = [Path(__file__).resolve(), Path('/proc/self/exe') if sys.platform == 'linux' else
             Path(sys.executable).resolve()]
    files = [file_identity(path, True, startup) for path in paths]
    before = identity(spec['sourceOrigin'])
    channel.write({'kind': 'ready', 'binding': binding, 'identity': before, 'files': files}, startup)
    for sequence, stage in enumerate(('pre', 'post')):
        request, _ = channel.read(deadline)
        need(parsed(request) == {'kind': 'sample', 'binding': binding, 'stage': stage}, 'SR_STAGE')
        end = min(deadline, time.monotonic_ns() + spec['exchangeMs'] * 1000000)
        need(invariant(identity(spec['sourceOrigin'])) == invariant(before), 'SR_REFERENCE_IDENTITY_DRIFT')
        challenge = {'kind': 'challenge', 'binding': binding, 'stage': stage, 'sequence': sequence,
                     'nonce': uuid.uuid4().hex}
        # Encoding is preparation, outside S/R. S precedes every byte of the challenge.
        challenge_raw = encoded(challenge)
        sent = time.monotonic_ns()
        written = channel.write(challenge, end)
        need(written == challenge_raw, 'SR_CHALLENGE_BYTES')
        response_raw, received = channel.read(end)
        response = parsed(response_raw)
        need(type(response) is dict and set(response) == {'kind', 'binding', 'stage', 'sequence', 'nonce', 'anchor'}
             and response['kind'] == 'anchor' and response['binding'] == binding and
             response['stage'] == stage and response['sequence'] == sequence and
             response['nonce'] == challenge['nonce'], 'SR_RESPONSE_BINDING')
        need(invariant(identity(spec['sourceOrigin'])) == invariant(before), 'SR_REFERENCE_IDENTITY_DRIFT')
        channel.write({'kind': 'receipt', 'binding': binding, 'stage': stage, 'sequence': sequence,
                       'sentMonotonicNs': str(sent), 'receivedMonotonicNs': str(received),
                       'challengeRaw': challenge_raw.decode(), 'responseRaw': response_raw.decode(),
                       'challengeSha256': hashlib.sha256(challenge_raw).hexdigest(),
                       'responseSha256': hashlib.sha256(response_raw).hexdigest()}, end)
    final_raw, _ = channel.read(min(deadline, time.monotonic_ns() + spec['finishMs'] * 1000000))
    need(parsed(final_raw) == {'kind': 'finish', 'binding': binding}, 'SR_FINISH')
    after = identity(spec['sourceOrigin'])
    need(invariant(after) == invariant(before), 'SR_REFERENCE_IDENTITY_DRIFT')
    after_files = [file_identity(path, False, deadline) for path in paths]
    need([{key: value for key, value in file.items() if key != 'sha256'} for file in files] == after_files,
         'SR_REFERENCE_FILE_DRIFT')
    channel.write({'kind': 'finished', 'binding': binding, 'identity': after, 'files': after_files,
                   'performanceAccepted': False}, deadline)


def main():
    if sys.argv[1:] == ['--help']:
        print(__doc__ + '\nPrivate: --spec-json STRICT_JSON; exactly init/pre/post/finish; never verified.')
        return 0
    try:
        need(len(sys.argv) == 3 and sys.argv[1] == '--spec-json' and len(sys.argv[2].encode()) <= 4096,
             'SR_ARGUMENTS')
        run(spec_check(parsed(sys.argv[2])))
        return 0
    except (ValueError, OSError, UnicodeError, KeyError, TypeError, IndexError) as error:
        # No traceback/environment values; parent owns bounded stderr capture and partial transcript.
        code = str(error) if isinstance(error, ValueError) else 'SR_REFERENCE_INPUT_OR_RUNTIME'
        os.write(2, (code[:512] + '\n').encode())
        return 6


if __name__ == '__main__':
    raise SystemExit(main())
