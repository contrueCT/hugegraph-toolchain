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

"""Container-side watchdog for one immutable HugeGraph HTTP arm.

The watchdog stays in the container with Java, independently of its host
controller. CPU/memory/PID/swap limits belong to Docker; aggregate disk usage
is sampled and shutdown can overshoot by one sampling/shutdown interval.
"""
import argparse
import base64
import gzip
import hashlib
import http.client
import importlib.util
import json
import os
from pathlib import Path
import re
import selectors
import signal
import socket
import stat
import subprocess
import sys
import threading
import time
import uuid

sys.dont_write_bytecode = True
_CLOCK_SPEC = importlib.util.spec_from_file_location('hgbench_jvm_clock_capture',
                                                    Path(__file__).with_name('jvm-clock-capture.py'))
CLOCK = importlib.util.module_from_spec(_CLOCK_SPEC)
_CLOCK_SPEC.loader.exec_module(CLOCK)


class EnvironmentError(Exception):
    pass


def require(value, message):
    if not value:
        raise EnvironmentError(message)


def check_deadline(deadline, cancelled):
    require(not cancelled[0], 'stop-requested')
    require(time.monotonic() < deadline, 'startup-budget-exhausted')


def digest(path, check=None):
    result = hashlib.sha256()
    if check:
        check()
    with Path(path).open('rb') as stream:
        while True:
            if check:
                check()
            chunk = stream.read(1024 * 1024)
            if check:
                check()
            if not chunk:
                break
            result.update(chunk)
    return result.hexdigest()


def read_json(path, maximum=16 * 1024 * 1024):
    path = Path(path)
    require(stat.S_ISREG(path.lstat().st_mode) and path.stat().st_size <= maximum, 'invalid-json-file')
    with path.open('rb') as stream:
        raw = stream.read(maximum + 1)
    require(len(raw) <= maximum, 'json-file-too-large')
    return json.loads(raw)


def write_json(path, value, exclusive=False):
    path = Path(path)
    raw = json.dumps(value, sort_keys=True, ensure_ascii=True, indent=2, allow_nan=False) + '\n'
    if exclusive:
        with path.open('x', encoding='utf-8') as stream:
            os.chmod(path, 0o600)
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
    else:
        temporary = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
        write_json(temporary, value, exclusive=True)
        os.replace(temporary, path)


def inventory(root, limit=10000, check=None):
    """Freeze complete file inventory, executable bits and contained symlinks."""
    root = Path(root).resolve(strict=True)
    require(root.is_dir(), 'source-is-not-directory')
    if check:
        check()
    result = []
    for folder, directories, files in os.walk(root, followlinks=False):
        if check:
            check()
        for name in sorted(directories + files):
            if check:
                check()
            path = Path(folder) / name
            relative = path.relative_to(root).as_posix()
            info = path.lstat()
            item = {'path': relative, 'mode': stat.S_IMODE(info.st_mode)}
            if stat.S_ISLNK(info.st_mode):
                require(not os.path.isabs(os.readlink(path)), 'absolute-source-symlink-not-portable')
                resolved = path.resolve(strict=True)
                require(resolved == root or root in resolved.parents, 'source-symlink-escapes-root')
                item.update(type='symlink', target=os.readlink(path))
            elif stat.S_ISDIR(info.st_mode):
                item['type'] = 'directory'
            else:
                require(stat.S_ISREG(info.st_mode), 'source-special-file')
                item.update(type='file', bytes=info.st_size, sha256=digest(path, check))
            result.append(item)
            require(len(result) <= limit, 'source-inventory-too-large')
    return sorted(result, key=lambda value: value['path'])


def verify_inventory(root, expected, check=None):
    require(inventory(root, check=check) == expected, 'frozen-source-content-changed')


def _directory_usage(root, maximum_files):
    logical = allocated = count = 0
    for folder, directories, files in os.walk(root, followlinks=False):
        for name in directories + files:
            info = (Path(folder) / name).lstat()
            require(not stat.S_ISLNK(info.st_mode), 'runtime-symlink-not-allowed')
            require(stat.S_ISREG(info.st_mode) or stat.S_ISDIR(info.st_mode), 'runtime-special-file')
            count += 1
            require(count <= maximum_files, 'runtime-file-count-exceeded')
            allocated += info.st_blocks * 512
            if stat.S_ISREG(info.st_mode):
                logical += info.st_size
    return {'logicalBytes': logical, 'allocatedBytes': allocated, 'files': count,
            'freeBytes': os.statvfs(root).f_bavail * os.statvfs(root).f_frsize}


def directory_usage(root, maximum_files=10000):
    # Atomic replacement/compaction may remove a name after scandir but before lstat.
    for retry in range(3):
        try:
            return dict(_directory_usage(root, maximum_files), unstable=retry > 0, resamples=retry)
        except FileNotFoundError:
            continue
    raise EnvironmentError('runtime-inventory-remained-unstable')


def budget_reason(usage, budget):
    if usage['logicalBytes'] > budget['maxLogicalBytes']:
        return 'directory-logical-budget'
    if usage['allocatedBytes'] > budget['maxAllocatedBytes']:
        return 'directory-allocated-budget'
    if usage['freeBytes'] < budget['minFreeBytes']:
        return 'disk-reserve-budget'
    return None


def terminate(process, grace, drain=None):
    """The new session's process group belongs exclusively to this wrapper."""
    result = {'termSent': False, 'killSent': False, 'exitCode': None,
              'mainReaped': False, 'groupGone': False, 'cleanupComplete': False}
    started = time.monotonic()
    grace_deadline, final_deadline = started + grace, started + grace + 5
    for sig in (signal.SIGTERM, signal.SIGKILL):
        try:
            os.killpg(process.pid, sig)
            result['termSent' if sig == signal.SIGTERM else 'killSent'] = True
        except ProcessLookupError:
            pass
        if sig == signal.SIGTERM and drain is not None:
            until = grace_deadline
            while time.monotonic() < until:
                pending = drain(min(0.025, max(0, until - time.monotonic())))
                if process.poll() is not None and not pending:
                    break
        elif sig == signal.SIGTERM:
            try:
                process.wait(timeout=grace)
            except subprocess.TimeoutExpired:
                pass
    until = final_deadline
    if drain is not None:
        while time.monotonic() < until:
            pending = drain(min(0.025, max(0, until - time.monotonic())))
            if process.poll() is not None and not pending:
                break
    try:
        result['exitCode'] = process.wait(timeout=max(0, until - time.monotonic()))
        result['mainReaped'] = True
    except subprocess.TimeoutExpired:
        return result
    try:
        os.killpg(process.pid, 0)
    except ProcessLookupError:
        result['groupGone'] = True
    result['cleanupComplete'] = result['mainReaped'] and result['groupGone']
    return result


class PipeCapture:
    """Drain even after the retained-byte cap; a blocked pipe must not defeat shutdown."""

    def __init__(self, selector, log, state, maximum):
        self.selector, self.log, self.state, self.maximum = selector, log, state, maximum
        self.sha = hashlib.sha256()
        self.record = {'schemaVersion': 1, 'kind': 'hugegraph-http-pipe-capture',
                       'stream': 'stdout+stderr',
                       'startedMonotonicNs': str(time.monotonic_ns()),
                       'readBytes': 0, 'retainedBytes': 0, 'discardedBytes': 0,
                       'eof': False, 'capHit': False, 'error': None}

    def drain(self, timeout):
        try:
            events = self.selector.select(timeout=timeout)
        except (OSError, ValueError) as error:
            self.record['error'] = str(error)[:500]
            return bool(self.selector.get_map())
        for key, _ in events:
            try:
                chunk = os.read(key.fileobj.fileno(), 65536)
            except BlockingIOError:
                continue
            except OSError as error:
                self.record['error'] = str(error)[:500]
                self.selector.unregister(key.fileobj)
                continue
            if not chunk:
                self.record['eof'] = True
                self.selector.unregister(key.fileobj)
                continue
            self.record['readBytes'] += len(chunk)
            remaining = max(0, self.maximum - self.state['logBytes'])
            keep = chunk[:remaining] if self.record['error'] is None else b''
            # An I/O error invalidates the receipt. Continue consuming without retaining
            # so that shutdown remains bounded even if the output filesystem fails.
            if keep:
                confirmed = 0
                try:
                    written = self.log.write(keep)
                    require(isinstance(written, int) and 0 <= written <= len(keep), 'invalid-log-write-count')
                    confirmed = written
                    self.sha.update(keep[:written])
                    self.state['logBytes'] += written
                    self.record['retainedBytes'] += written
                    complete_write = written == len(keep)
                    keep = keep[:written]
                    require(complete_write, 'short-log-write')
                    self.log.flush()
                except (OSError, EnvironmentError) as error:
                    self.record['error'] = str(error)[:500]
                    # All supervisor files are unbuffered. A failed write retains no
                    # confirmed bytes; a short write retains its confirmed prefix.
                    keep = keep[:confirmed]
            self.record['discardedBytes'] += len(chunk) - len(keep)
            self.record['capHit'] |= len(chunk) > remaining
        return bool(self.selector.get_map())

    def finish(self, termination):
        self.record.update(finishedMonotonicNs=str(time.monotonic_ns()), sha256=self.sha.hexdigest(),
                           forcedKill=termination['killSent'])
        self.record['complete'] = (self.record['eof'] and not self.record['capHit'] and
                                   self.record['error'] is None and not termination['killSent'] and
                                   termination['cleanupComplete'])
        return self.record


def expire_budget(state):
    if state.get('cleanupInProgress'):
        state['deadlineExpiredDuringCleanup'] = True
        return
    raise EnvironmentError('wall-clock-budget')


def unforced_stop(outcome):
    termination = outcome.get('termination', {})
    # HotSpot commonly exits 143 after SIGTERM/shutdown hooks; Python represents raw SIGTERM as -15.
    return (outcome.get('reason') == 'stop-requested' and termination.get('cleanupComplete') is True and
            termination.get('termSent') is True and not termination.get('killSent') and
            termination.get('exitCode') in (0, 143, -signal.SIGTERM))


def stop_requested(control, attempt, plan_sha):
    path = Path(control) / 'stop-request.json'
    try:
        value = read_json(path, 65536)
    except FileNotFoundError:
        return False
    require(value.get('attemptId') == attempt and value.get('planSha256') == plan_sha,
            'stop-marker-identity-mismatch')
    return True


def healthy(deadline):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        return False
    connection = http.client.HTTPConnection('127.0.0.1', 18080, timeout=min(0.4, remaining))
    timer = None
    try:
        connection.connect()
        if time.monotonic() >= deadline:
            return False
        # Keep the actual socket: HTTPConnection can detach it after Connection: close.
        active_socket = connection.sock
        def expire_socket():
            try:
                active_socket.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        timer = threading.Timer(max(0, deadline - time.monotonic()), expire_socket)
        timer.daemon = True
        timer.start()
        connection.request('POST', '/gremlin', json.dumps({'gremlin': '1+1', 'language': 'gremlin-groovy'}),
                           {'Content-Type': 'application/json', 'Accept-Encoding': 'gzip'})
        response = connection.getresponse()
        raw = response.read(65537)
        require(len(raw) <= 65536, 'health-response-too-large')
        if response.getheader('Content-Encoding', '').lower() == 'gzip':
            # Read only a bounded decoded prefix, even if a server returns a compressed expansion.
            import io
            with gzip.GzipFile(fileobj=io.BytesIO(raw)) as stream:
                raw = stream.read(65537)
            require(len(raw) <= 65536, 'health-decoded-response-too-large')
        body = json.loads(raw)
        return (time.monotonic() < deadline and response.status == 200 and
                body['status']['code'] == 200 and body['result']['data'] == [2])
    except (OSError, ValueError, KeyError, EnvironmentError, http.client.HTTPException):
        return False
    finally:
        connection.close()
        if timer:
            timer.cancel()
            timer.join(timeout=0.1)


def proc_start(pid):
    # Linux /proc comm is parenthesized and may itself contain spaces/parentheses.
    return (Path('/proc') / str(pid) / 'stat').read_text().rsplit(')', 1)[1].split()[19]


def bounded_bytes(path, maximum):
    with Path(path).open('rb') as stream:
        value = stream.read(maximum + 1)
    require(len(value) <= maximum, 'jvm-runtime-record-too-large')
    return value


def jdk_file_proof(path, expected, check):
    """Hash one opened regular file, preserving its exact device/inode identity."""
    with os.fdopen(os.open(str(path), os.O_RDONLY | os.O_NOFOLLOW), 'rb') as stream:
        before = os.fstat(stream.fileno())
        require(stat.S_ISREG(before.st_mode), 'jvm-runtime-member-not-regular')
        hashed = hashlib.sha256()
        while True:
            check()
            value = stream.read(1024 * 1024)
            if not value:
                break
            hashed.update(value)
        after = os.fstat(stream.fileno())
    require((before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) ==
            (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns) and
            hashed.hexdigest() == expected, 'jvm-runtime-member-content-changed')
    return {'deviceMajor': os.major(before.st_dev), 'deviceMinor': os.minor(before.st_dev),
            'inode': str(before.st_ino), 'bytes': before.st_size, 'sha256': hashed.hexdigest()}


def clock_namespace(root):
    result = {'domain': 'linux-clock-monotonic', 'alignment': 'unverified'}
    for key, path, link in [('namespace', root / 'ns/time', True),
                            ('timeForChildren', root / 'ns/time_for_children', True),
                            ('offsets', root / 'timens_offsets', False)]:
        try:
            value = os.readlink(path) if link else bounded_bytes(path, 4096).decode('ascii')
            require(len(value) <= 4096, 'clock-namespace-record-too-large')
            result[key] = {'available': True, 'value': value, 'reason': None}
        except (OSError, UnicodeError, EnvironmentError) as error:
            result[key] = {'available': False, 'value': None, 'reason': str(error)[:500]}
    return result


def inspect_jvm_process(pid, expected_start, argv, observation, check, proc_root=Path('/proc'),
                        java_home=Path('/opt/jdk')):
    """Return only when the launched process has mapped the frozen libjvm."""
    require(sys.platform.startswith('linux'), 'jvm-observation-linux-required')
    process = proc_root / str(pid)
    before_ns = str(time.monotonic_ns())
    raw_before = bounded_bytes(process / 'stat', 65536)
    before = raw_before.decode().rsplit(')', 1)[1].split()
    require(raw_before.split(b' ', 1)[0] == str(pid).encode() and before[19] == expected_start and
            before[0] not in ('Z', 'X'), 'jvm-runtime-process-replaced')
    raw_cmdline = bounded_bytes(process / 'cmdline', 131072)
    require(raw_cmdline.endswith(b'\0'), 'jvm-runtime-cmdline-not-terminated')
    actual_argv = [arg.decode('utf-8') for arg in raw_cmdline[:-1].split(b'\0')]
    require(actual_argv == argv, 'jvm-runtime-argv-mismatch')
    java = java_home / 'bin/java'
    java_stat, exe_stat = java.stat(), (process / 'exe').stat()
    require((java_stat.st_dev, java_stat.st_ino) == (exe_stat.st_dev, exe_stat.st_ino),
            'jvm-runtime-executable-mismatch')
    raw_maps = bounded_bytes(process / 'maps', 1024 * 1024)
    # Paths are decoded losslessly for matching and retained as base64 for audit.
    mappings = []
    for line in raw_maps.decode('utf-8', errors='surrogateescape').splitlines():
        fields = line.split(None, 5)
        if len(fields) == 6 and re.search(r'(?:^|/)libjvm\.so(?: \(deleted\))?$', fields[5]):
            require(fields[5] == str(java_home / 'lib/server/libjvm.so'), 'jvm-runtime-libjvm-path-mismatch')
            require(re.fullmatch(r'[0-9a-f]+-[0-9a-f]+', fields[0]) and
                    re.fullmatch(r'[r-][w-][x-][ps]', fields[1]) and
                    re.fullmatch(r'[0-9a-f]+', fields[2]) and
                    re.fullmatch(r'[0-9a-f]+:[0-9a-f]+', fields[3]) and fields[4].isdecimal(),
                    'jvm-runtime-malformed-libjvm-mapping')
            mappings.append(fields)
    if not mappings:
        return None
    require(any('x' in item[1] for item in mappings), 'jvm-runtime-libjvm-not-executable')
    java_proof = jdk_file_proof(java, observation['jdk']['javaSha256'], check)
    require((java_proof['deviceMajor'], java_proof['deviceMinor'], int(java_proof['inode'])) ==
            (os.major(exe_stat.st_dev), os.minor(exe_stat.st_dev), exe_stat.st_ino),
            'jvm-runtime-opened-executable-mismatch')
    lib_proof = jdk_file_proof(java_home / 'lib/server/libjvm.so', observation['jdk']['libjvmSha256'], check)
    require(all([int(part, 16) for part in item[3].split(':')] ==
                [lib_proof['deviceMajor'], lib_proof['deviceMinor']] and
                int(item[4]) == int(lib_proof['inode']) for item in mappings), 'jvm-runtime-loaded-libjvm-mismatch')
    raw_after = bounded_bytes(process / 'stat', 65536)
    after = raw_after.decode().rsplit(')', 1)[1].split()
    final_exe_stat = (process / 'exe').stat()
    require(raw_after.split(b' ', 1)[0] == str(pid).encode() and
            after[19] == expected_start and after[0] not in ('Z', 'X') and
            bounded_bytes(process / 'cmdline', 131072) == raw_cmdline and
            (final_exe_stat.st_dev, final_exe_stat.st_ino, final_exe_stat.st_size,
             final_exe_stat.st_mtime_ns, final_exe_stat.st_ctime_ns) ==
            (exe_stat.st_dev, exe_stat.st_ino, exe_stat.st_size, exe_stat.st_mtime_ns, exe_stat.st_ctime_ns),
            'jvm-runtime-process-changed-during-proof')
    boot = bounded_bytes(proc_root / 'sys/kernel/random/boot_id', 64).decode('ascii').strip()
    require(re.fullmatch(r'[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}', boot), 'jvm-runtime-invalid-boot-id')
    launch = {'schemaVersion': 1, 'kind': 'http-jvm-observation-launch',
              'planSha256': observation['planSha256'], 'argv': actual_argv,
              'process': {'pid': pid, 'startTimeTicks': expected_start, 'hostBootId': boot},
              'jdk': observation['jdk']}
    clock_started = str(time.monotonic_ns())
    snapshot = CLOCK.clock_snapshot(pid, True, proc_root)
    capture_identity = {'schemaVersion': 1, 'kind': 'http-jvm-initial-clock-identity',
                        'sourceOrigin': snapshot['sourceOrigin'],
                        'captureComponentSha256': snapshot['captureComponentSha256'], 'snapshot': snapshot,
                        'verified': False, 'hostAssociation': 'unverified',
                        'executableHashMaxBytes': CLOCK.CLOCK_EXECUTABLE_MAX_BYTES,
                        'executableHashMaxNs': str(CLOCK.CLOCK_HASH_MAX_NS)}
    clock = {'observedBeforeMonotonicNs': clock_started, 'capture': clock_namespace(proc_root/'self'),
             'jvm': clock_namespace(process), 'phaseMapping': 'unverified'}
    clock['observedAfterMonotonicNs'] = str(time.monotonic_ns())
    runtime = {'schemaVersion': 1, 'kind': 'http-jvm-observation-runtime',
               'planSha256': observation['planSha256'], 'observedBeforeMonotonicNs': before_ns,
               'process': {'pid': pid, 'startTimeTicksBefore': before[19], 'startTimeTicksAfter': after[19],
                           'hostBootId': boot, 'cmdlineBase64': base64.b64encode(raw_cmdline).decode('ascii'),
                           'statBeforeBase64': base64.b64encode(raw_before).decode('ascii'),
                           'statAfterBase64': base64.b64encode(raw_after).decode('ascii'),
                           'mapsBase64': base64.b64encode(raw_maps).decode('ascii'),
                           'executablePath': os.readlink(process / 'exe'), 'java': java_proof,
                           'libjvm': lib_proof},
               'clock': clock, 'captureIdentity': capture_identity}
    runtime['observedAfterMonotonicNs'] = str(time.monotonic_ns())
    check()
    return launch, runtime


def jvm_capture_receipt(work, observation, capture, termination):
    launch = Path(work) / 'jvm-launch.json'
    if not launch.exists():
        return None
    receipt = {'schemaVersion': 1, 'kind': 'http-jvm-observation-capture',
               'planSha256': observation['planSha256'], 'launchSha256': digest(launch),
               'raw': {'bytes': capture['retainedBytes'], 'sha256': capture['sha256']},
               'cleanupComplete': termination['cleanupComplete']}
    for name in ('startedMonotonicNs', 'finishedMonotonicNs', 'eof', 'capHit', 'forcedKill', 'error',
                 'readBytes', 'retainedBytes', 'discardedBytes'):
        receipt[name] = capture[name]
    write_json(Path(work) / 'jvm-capture.json', receipt, exclusive=True)
    return receipt


def supervise(command, work, budget, deadline, phase, cancelled, state, readiness=None, stop_check=None,
              jvm_observation=None):
    """Keep bounded logs and stop exactly the launched child group on every exit."""
    work = Path(work)
    require(not budget_reason(directory_usage(work), budget), 'initial-directory-budget-exceeded')
    process = None
    reason = None
    selector = selectors.DefaultSelector()
    started = time.monotonic()
    last_sample = last_probe = 0.0
    ready = readiness is None
    outcome = None
    log = capture = None
    observed_jvm = jvm_observation is None
    try:
        if stop_check and stop_check():
            cancelled[0] = True
        require(not cancelled[0] and time.monotonic() < deadline, 'cancelled-or-expired-before-java-launch')
        log = (work / (phase + '.log')).open('xb', buffering=0)
        capture = PipeCapture(selector, log, state, budget['maxLogBytes'])
        process = subprocess.Popen(command, cwd=work, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT, start_new_session=True,
                                   env={'PATH': '/opt/jdk/bin:/usr/bin:/bin', 'JAVA_HOME': '/opt/jdk',
                                        'HOME': '/work', 'LANG': 'C.UTF-8', 'TMPDIR': '/tmp'})
        os.set_blocking(process.stdout.fileno(), False)
        selector.register(process.stdout, selectors.EVENT_READ)
        identity = {'pid': process.pid, 'phase': phase, 'startedMonotonic': started}
        if Path('/proc').exists():
            try:
                identity['startTimeTicks'] = proc_start(process.pid)
            except FileNotFoundError:
                identity['startTimeTicks'] = None
        write_json(work / 'java-process.json', identity)
        state['phase'] = phase
        while True:
            now = time.monotonic()
            if stop_check and stop_check():
                cancelled[0] = True
            if cancelled[0]:
                reason = 'stop-requested'
            elif now >= deadline:
                reason = 'wall-clock-budget'
            elif not ready and now - started >= budget['startupSeconds']:
                reason = 'startup-timeout'
            if now - last_sample >= budget['pollSeconds']:
                last_sample = now
                state['usage'] = directory_usage(work)
                state['unstableSamples'] = state.get('unstableSamples', 0) + int(state['usage']['unstable'])
                reason = reason or budget_reason(state['usage'], budget)
                write_json(work / 'service-status.json',
                           dict(state, elapsedSeconds=now - state['startedMonotonic']))
            if reason:
                break
            if not observed_jvm:
                def observation_check():
                    if stop_check and stop_check():
                        cancelled[0] = True
                    check_deadline(min(deadline, started + budget['startupSeconds']), cancelled)
                evidence = inspect_jvm_process(process.pid, identity.get('startTimeTicks'), command,
                                               jvm_observation, observation_check)
                if evidence:
                    launch, runtime = evidence
                    write_json(work / 'jvm-launch.json', launch, exclusive=True)
                    runtime.update(environmentPlanSha256=state['planSha256'], attemptId=state['attemptId'],
                                   launchSha256=digest(work / 'jvm-launch.json'))
                    write_json(work / 'jvm-runtime.json', runtime, exclusive=True)
                    observed_jvm = True
            capture.drain(min(budget['pollSeconds'], 0.1))
            if capture.record['error']:
                reason = 'log-capture-error'
            elif capture.record['capHit']:
                reason = 'log-byte-budget'
            if reason:
                break
            if process.poll() is not None and not selector.get_map():
                reason = 'process-exit'
                break
            if not ready and now - last_probe >= 0.5:
                last_probe = now
                reached = readiness(min(deadline, started + budget['startupSeconds'], time.monotonic() + 0.4))
                # A delayed callback must never create ready evidence after cancellation or expiry.
                after = time.monotonic()
                if stop_check and stop_check():
                    cancelled[0] = True
                if cancelled[0]:
                    reason = 'stop-requested'
                    break
                if after >= deadline:
                    reason = 'wall-clock-budget'
                    break
                if after >= started + budget['startupSeconds']:
                    reason = 'startup-timeout'
                    break
                if reached and observed_jvm:
                    ready = True
                    write_json(work / 'ready.json', {'schemaVersion': 1, 'kind': 'hugegraph-http-ready',
                               'attemptId': state['attemptId'], 'planSha256': state['planSha256'],
                               'declaredMode': state['mode'], 'actualModeVerified': False,
                               'java': identity, 'endpoint': 'http://127.0.0.1:18080/gremlin',
                               'javaExecutable': '/opt/jdk/bin/java', 'jcmdExecutable': '/opt/jdk/bin/jcmd',
                               'javaIdentityFile': '/work/java-process.json',
                               'performanceAccepted': False}, exclusive=True)
        outcome = {'phase': phase, 'reason': reason, 'exitCodeBeforeShutdown': process.poll(), 'ready': ready}
        if jvm_observation:
            outcome['jvmObservationRecorded'] = observed_jvm
        return outcome
    except Exception as failure:
        outcome = {'phase': phase, 'reason': 'watchdog-error', 'error': str(failure)[:1000],
                   'exitCodeBeforeShutdown': process.poll() if process else None, 'ready': ready}
        return outcome
    finally:
        termination = None
        state['cleanupInProgress'] = True
        try:
            if process is not None:
                termination = terminate(process, budget['shutdownSeconds'], capture.drain if capture else None)
                if outcome is not None:
                    outcome['termination'] = termination
        finally:
            selector.close()
            if process is not None:
                process.stdout.close()
            if log is not None:
                try:
                    log.flush()
                    os.fsync(log.fileno())
                except OSError as error:
                    capture.record['error'] = str(error)[:500]
                finally:
                    log.close()
            if capture is not None and termination is not None:
                receipt = capture.finish(termination)
                if outcome is not None:
                    outcome['capture'] = receipt
                write_json(work / (phase + '-capture.json'), receipt, exclusive=True)
                if jvm_observation:
                    jvm_capture_receipt(work, jvm_observation, receipt, termination)
            state['cleanupInProgress'] = False


def java_commands(plan):
    settings = plan['settings']
    jvm = settings['jvm']
    server_cp = ':'.join('/opt/hugegraph/' + path for path in plan['classpath'])
    init_cp = ':'.join('/opt/hugegraph/' + path for path in plan['initClasspath'])
    common = ['/opt/jdk/bin/java', '-Xms' + str(jvm['heapMinBytes']), '-Xmx' + str(jvm['heapMaxBytes']),
              '-XX:MaxDirectMemorySize=' + str(jvm['maxDirectMemoryBytes']),
              '-XX:ActiveProcessorCount=' + str(jvm['activeProcessorCount']),
              '@/opt/hugegraph/bin/jvm-module.options', '-Djava.io.tmpdir=/tmp',
              '-Dlog4j.configurationFile=/control/conf/log4j2.xml']
    observation_args = []
    if 'jvmObservation' in plan:
        spec = importlib.util.spec_from_file_location('hgbench_jvm_observation',
                                                    Path(__file__).with_name('jvm-observation.py'))
        component = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(component)
        observation_args = [component.ARGUMENT]
    return {'init': common + ['-cp', init_cp, 'org.apache.hugegraph.cmd.InitStore',
                              '/control/conf/rest-server.properties'],
            'server': common + observation_args + ['-Djava.security.properties=/control/conf/java-security.properties',
                                '-cp', server_cp, 'org.apache.hugegraph.bootstrap.HugeGraphServerBootstrap',
                                'true', '/control/conf/gremlin-server.yaml', '/control/conf/rest-server.properties']}


def serve(plan_path, plan_sha, work):
    work = Path(work)
    require(digest(plan_path) == plan_sha, 'plan-digest-mismatch')
    plan = read_json(plan_path)
    require(plan['schemaVersion'] == 1 and plan['kind'] == 'hugegraph-http-environment-plan', 'invalid-plan')
    require(os.geteuid() == plan['owner']['uid'] and os.getegid() == plan['owner']['gid'] and os.geteuid() != 0,
            'service-owner-mismatch')
    require(work.resolve() == Path('/work') and not work.is_symlink(), 'invalid-service-workspace')
    write_json(work / 'service-started.json', {'attemptId': plan['attemptId'], 'planSha256': plan_sha}, exclusive=True)
    started = time.monotonic()
    deadline = started + plan['settings']['budget']['wallSeconds']
    cancelled = [False]
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda _signum, _frame: cancelled.__setitem__(0, True))
    state = {'schemaVersion': 1, 'attemptId': plan['attemptId'], 'planSha256': plan_sha,
             'mode': plan['settings']['mode'], 'startedMonotonic': started, 'logBytes': 0}
    outcomes = []
    error = None
    def expired(_signum, _frame):
        expire_budget(state)
    signal.signal(signal.SIGALRM, expired)
    signal.setitimer(signal.ITIMER_REAL, plan['settings']['budget']['wallSeconds'])
    try:
        startup_deadline = min(deadline, started + plan['settings']['budget']['startupSeconds'])
        stopped = lambda: stop_requested(Path(plan_path).parent, plan['attemptId'], plan_sha)
        def check():
            if stopped():
                cancelled[0] = True
            check_deadline(startup_deadline, cancelled)
        check()
        verify_inventory('/opt/hugegraph', plan['sources']['distribution']['files'], check)
        verify_inventory('/opt/jdk', plan['sources']['javaHome']['files'], check)
        for entry in plan['controlFiles']:
            check()
            path = Path('/control') / entry['path']
            require(path.is_file() and not path.is_symlink() and digest(path, check) == entry['sha256'],
                    'control-file-changed')
        for folder in ('data', 'plugins', 'logs'):
            check()
            (work / folder).mkdir()
        commands = java_commands(plan)
        observation = plan.get('jvmObservation')
        if observation is not None:
            spec = importlib.util.spec_from_file_location('hgbench_jvm_observation',
                                                        Path(__file__).with_name('jvm-observation.py'))
            component = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(component)
            settings, observation_sha, _ = component.load_plan(Path(plan_path).parent / 'jvm-observation')
            require(set(observation) == {'planDirectory', 'planSha256', 'jdk'} and
                    observation['planDirectory'] == 'jvm-observation' and
                    settings == plan['settings'].get('jvmObservation') and
                    observation_sha == observation['planSha256'] and
                    settings['maxRawBytes'] == plan['settings']['budget']['maxLogBytes'],
                    'jvm-observation-runtime-plan-binding-mismatch')
            frozen_jdk = {item['path']: item for item in plan['sources']['javaHome']['files']}
            require(observation['jdk']['javaSha256'] == frozen_jdk['bin/java']['sha256'] and
                    observation['jdk']['libjvmSha256'] == frozen_jdk['lib/server/libjvm.so']['sha256'],
                    'jvm-observation-runtime-jdk-binding-mismatch')
        init_budget = dict(plan['settings']['budget'])
        init_deadline = startup_deadline
        require(not cancelled[0] and time.monotonic() < init_deadline, 'startup-budget-exhausted')
        version = supervise(['/opt/jdk/bin/java', '-version'], work, init_budget, init_deadline,
                            'java-version', cancelled, state, stop_check=stopped)
        outcomes.append(version)
        require(version['reason'] == 'process-exit' and version['exitCodeBeforeShutdown'] == 0 and
                re.search(r'version "17\.', (work / 'java-version.log').read_text()), 'runtime-jdk17-required')
        if observation is not None:
            version_log = (work / 'java-version.log').read_text()
            require(re.search(r'(?:OpenJDK|Java HotSpot\(TM\)) 64-Bit Server VM', version_log) and
                    observation['jdk']['version'] in version_log, 'runtime-hotspot17-identity-required')
        require(not cancelled[0] and time.monotonic() < init_deadline, 'startup-budget-exhausted')
        initialized = supervise(commands['init'], work, init_budget, init_deadline, 'init', cancelled, state,
                                stop_check=stopped)
        outcomes.append(initialized)
        require(initialized['reason'] == 'process-exit' and initialized['exitCodeBeforeShutdown'] == 0,
                'store-initialization-failed')
        require(not cancelled[0] and time.monotonic() < init_deadline, 'startup-budget-exhausted')
        server_budget = dict(init_budget, startupSeconds=max(0.1, init_deadline - time.monotonic()))
        outcomes.append(supervise(commands['server'], work, server_budget, deadline, 'server', cancelled,
                                  state, readiness=healthy, stop_check=stopped, jvm_observation=observation))
    except Exception as failure:
        error = str(failure)[:1000]
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
    result = {'schemaVersion': 1, 'kind': 'hugegraph-http-service-result', 'attemptId': plan['attemptId'],
              'planSha256': plan_sha, 'outcomes': outcomes, 'error': error, 'logBytes': state['logBytes'],
              'unstableSamples': state.get('unstableSamples', 0),
              'elapsedSeconds': time.monotonic() - started, 'performanceAccepted': False}
    write_json(work / 'service-result.json', result, exclusive=True)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--plan', type=Path, required=True)
    parser.add_argument('--plan-sha256', required=True)
    parser.add_argument('--work', type=Path, default=Path('/work'))
    args = parser.parse_args()
    try:
        result = serve(args.plan, args.plan_sha256, args.work)
        ok = result['error'] is None and bool(result['outcomes']) and unforced_stop(result['outcomes'][-1])
        print(json.dumps({'ok': ok, 'result': result}, sort_keys=True))
        return 0 if ok else 6
    except Exception as error:
        print(json.dumps({'ok': False, 'error': str(error)[:1000]}))
        return 6


if __name__ == '__main__':
    raise SystemExit(main())
